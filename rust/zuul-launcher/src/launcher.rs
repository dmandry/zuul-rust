// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// Control flow from zuul/launcher/server.py. Timing constants are unchanged.

use crate::cloud;
use crate::endpoint;
use crate::jsonutil;
use crate::layout::{self, LayoutState};
use crate::mmh3;
use crate::model::{
    self, node_state, request_state, upload_state, NodesetRequest, ProviderNode, QuotaInformation,
    UPLOAD_STATES,
};
use crate::nodescan::{NodescanRequest, NodescanWorker};
use crate::provider::{
    AdvanceError, AwsSnapshot, Provider, StateMachine, StaticNodeConfig,
};
use crate::upload::{PlannedJob, UploadArgs, UploadPlanner};
use crate::workers::{
    matching_endpoints, CleanupWorker, EndpointRef, StatsWorker, UploadPools,
};
use crate::zk::{self, ComponentInfo, LauncherApi, ZkOps};
use rand::seq::SliceRandom;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const MAX_SLEEP: f64 = 1.0;
pub const MIN_INTERVAL: f64 = 60.0;
pub const CACHE_SYNC_TIMEOUT: f64 = 10.0;
pub const DELETE_TIMEOUT: f64 = 600.0;
pub const MAX_QUOTA_AGE: f64 = 300.0;
pub const SUBNODE_SYNC_TIME: f64 = 300.0;
pub const CLEANUP_INTERVAL: f64 = 60.0;
pub const STATS_INTERVAL: f64 = 30.0;

#[derive(Clone, Debug)]
pub struct StatEvent {
    pub kind: String,
    pub name: String,
    pub value: f64,
}

pub struct Launcher {
    pub hostname: String,
    pub state: String,
    pub api: LauncherApi,
    pub tenant_providers: HashMap<String, Vec<Arc<dyn Provider>>>,
    pub nodescan: NodescanWorker,
    pub messages: Vec<String>,
    pub stats: Vec<StatEvent>,
    pub running: bool,
    pub layout_updated: bool,
    pub layout_dirty: bool,
    pub image_updated: bool,
    pub upload_deleted: bool,
    pub system_id: String,
    pub component_path: Option<String>,
    pub connection_filter: Option<Vec<String>>,
    pub local_layout_state: HashMap<String, LayoutState>,
    pub layout_generation: u64,
    create_sms: HashMap<String, Box<dyn StateMachine>>,
    delete_sms: HashMap<String, Box<dyn StateMachine>>,
    quota_cache: HashMap<String, (QuotaInformation, f64)>,
    pub pending_events: Vec<Value>,
    pub wake: Arc<Mutex<bool>>,
    pub now: f64,
    pub cleanup_worker: Option<CleanupWorker>,
    pub stats_worker: Option<StatsWorker>,
    pub upload_pools: Option<UploadPools>,
    /// Shared with upload pool workers so they can discard finished uuids.
    pub upload_uuids_in_queue: Arc<Mutex<HashSet<String>>>,
    pub upload_uuids_in_delete_queue: Arc<Mutex<HashSet<String>>>,
    /// Pending (provider, label) keys for quota deferral within one
    /// process_requests pass. Shared with the accept_one PyO3 hook so
    /// patched `_acceptRequest` still sees the same deferral set.
    accept_pending: HashSet<(String, String)>,
    stats_snapshot: Option<Arc<Mutex<StatsSnapshot>>>,
    stats_worker_sink: Option<Arc<Mutex<Vec<StatEvent>>>>,
    cleanup_shared: Option<Arc<Mutex<CleanupShared>>>,
}

/// Data the stats election thread reads to emit `_runStats` gauges.
#[derive(Clone, Default)]
struct StatsSnapshot {
    nodes: Vec<(String, String, String)>,
    requests: Vec<String>,
    uploads: Vec<(String, String, String)>,
    providers: Vec<ProviderStatsRow>,
}

#[derive(Clone, Default)]
struct ProviderStatsRow {
    canonical_name: String,
    limits: BTreeMap<String, f64>,
    usage: BTreeMap<String, f64>,
    labels: Vec<(String, String, Option<i64>)>,
}

#[derive(Clone, Default)]
struct CleanupShared {
    hostname: String,
    components: Vec<ComponentInfo>,
    endpoints: Vec<EndpointRef>,
}

impl Launcher {
    pub fn new(hostname: &str, zk: Arc<dyn ZkOps>) -> Self {
        let system_id = zk::system_id(zk.as_ref()).unwrap_or_else(|_| "system".into());
        let api = LauncherApi::new(zk, hostname, None);
        Self {
            hostname: hostname.into(),
            // BaseComponent starts STOPPED. The launcher moves to RUNNING
            // only after the first successful provider reload.
            state: "stopped".into(),
            api,
            tenant_providers: HashMap::new(),
            nodescan: NodescanWorker::new(),
            messages: Vec::new(),
            stats: Vec::new(),
            running: true,
            layout_updated: false,
            layout_dirty: true,
            image_updated: false,
            upload_deleted: false,
            system_id,
            component_path: None,
            connection_filter: None,
            local_layout_state: HashMap::new(),
            layout_generation: 0,
            create_sms: HashMap::new(),
            delete_sms: HashMap::new(),
            quota_cache: HashMap::new(),
            pending_events: Vec::new(),
            wake: Arc::new(Mutex::new(false)),
            now: model::now_secs(),
            cleanup_worker: None,
            stats_worker: None,
            upload_pools: None,
            upload_uuids_in_queue: Arc::new(Mutex::new(HashSet::new())),
            upload_uuids_in_delete_queue: Arc::new(Mutex::new(HashSet::new())),
            accept_pending: HashSet::new(),
            stats_snapshot: None,
            stats_worker_sink: None,
            cleanup_shared: None,
        }
    }

    fn queue_contains(set: &Arc<Mutex<HashSet<String>>>, uuid: &str) -> bool {
        set.lock().map(|g| g.contains(uuid)).unwrap_or(false)
    }

    fn queue_insert(set: &Arc<Mutex<HashSet<String>>>, uuid: String) {
        if let Ok(mut g) = set.lock() {
            g.insert(uuid);
        }
    }

    fn queue_discard(set: &Arc<Mutex<HashSet<String>>>, uuid: &str) {
        if let Ok(mut g) = set.lock() {
            g.remove(uuid);
        }
    }

    pub fn take_pending_events(&mut self) -> Vec<Value> {
        std::mem::take(&mut self.pending_events)
    }

    pub fn take_pending_events_json(&mut self) -> String {
        let events = std::mem::take(&mut self.pending_events);
        jsonutil::dumps(&json!(events))
    }

    pub fn take_stats_json(&mut self) -> String {
        // Drain pool gauges here too: upload workers may emit after
        // refresh_worker_shared ran at the start of process_once.
        if let Some(sink) = &self.stats_worker_sink {
            if let Ok(mut g) = sink.lock() {
                self.stats.append(&mut *g);
            }
        }
        if let Some(pools) = &self.upload_pools {
            self.stats.extend(pools.take_gauges());
        }
        let stats = std::mem::take(&mut self.stats);
        let items: Vec<Value> = stats
            .into_iter()
            .map(|event| {
                json!({
                    "kind": event.kind,
                    "name": event.name,
                    "value": event.value,
                })
            })
            .collect();
        jsonutil::dumps(&json!(items))
    }

    pub fn add_provider(&mut self, tenant: &str, provider: Arc<dyn Provider>) {
        self.tenant_providers
            .entry(tenant.to_string())
            .or_default()
            .push(provider);
    }

    pub fn set_components(&mut self, components: Vec<ComponentInfo>) {
        self.api.components = components;
    }

    /// One iteration of Launcher._run, under the caller's lock.
    pub fn run_once(&mut self) {
        if self.reload_providers().is_ok() {
            self.process_once();
        }
    }

    /// Request, node, and min-ready processing. Split from the reload so
    /// the PyO3 harness can publish layout state in between, matching
    /// `_run` setting component state before `_processRequests`.
    pub fn process_once(&mut self) {
        // Python reads time.time() on each pass. Logic tests that call
        // process_provider_node directly keep an injected clock.
        self.now = model::now_secs();
        self.refresh_worker_shared();
        self.maintain_images_and_uploads();
        self.process_requests();
        self.process_nodes();
        self.process_min_ready();
    }

    /// Launcher.updateTenantProviders / _updateTenantProviders.
    ///
    /// A tenant is skipped when the local and remote layout uuids match.
    /// Both missing is a match, so providers installed with `add_provider`
    /// stay in place when ZooKeeper has no layout for that tenant.
    /// `connection_filter` does not remove providers from the map. The
    /// first successful reload moves state to running.
    pub fn reload_providers(&mut self) -> Result<bool, String> {
        self.api.connection_filter = self.connection_filter.clone();
        let before = layout_fingerprint(&self.local_layout_state);
        let updated = self.update_tenant_providers()?;
        if layout_fingerprint(&self.local_layout_state) != before {
            self.layout_generation = self.layout_generation.wrapping_add(1);
        }
        if updated {
            self.layout_updated = true;
            if self.state != "running" {
                self.state = "running".into();
                self.mark_self_running();
                self.persist_running_component();
            }
            // Launcher.updateTenantProviders: register/deregister static
            // hosts after providers are reloaded.
            self.register_and_deregister_static_nodes();
        }
        self.layout_dirty = false;
        Ok(updated)
    }

    pub fn update_tenant_providers(&mut self) -> Result<bool, String> {
        let remote = layout::read_layout_states(self.api.zk.as_ref())?;
        let mut names: HashSet<String> = self.tenant_providers.keys().cloned().collect();
        names.extend(remote.keys().cloned());
        let mut updated = false;
        for tenant in names {
            let local = self.local_layout_state.get(&tenant);
            let remote_state = remote.get(&tenant);
            if layout_same(local, remote_state) {
                continue;
            }
            let lock = layout::acquire_tenant_read_lock(
                self.api.zk.as_ref(),
                &tenant,
                &self.hostname,
            )?;
            let refreshed = layout::read_tenant_layout(self.api.zk.as_ref(), &tenant);
            let outcome = match refreshed {
                Ok(Some(state)) => {
                    match layout::read_tenant_providers(self.api.zk.as_ref(), &tenant) {
                        Ok(providers) => {
                            self.tenant_providers.insert(tenant.clone(), providers);
                            self.local_layout_state.insert(tenant.clone(), state);
                            Ok(true)
                        }
                        Err(e) => Err(e),
                    }
                }
                Ok(None) => {
                    // The layout znode is gone. Python pops the tenant and
                    // leaves `updated` false, so this does not by itself
                    // move the component to running.
                    self.tenant_providers.remove(&tenant);
                    self.local_layout_state.remove(&tenant);
                    Ok(false)
                }
                Err(e) => Err(e),
            };
            layout::release_lock(self.api.zk.as_ref(), &lock);
            if outcome? {
                updated = true;
            }
        }
        Ok(updated)
    }

    pub fn layout_snapshot_json(&self) -> String {
        let mut tenants = serde_json::Map::new();
        for (name, state) in &self.local_layout_state {
            tenants.insert(name.clone(), state.to_json());
        }
        jsonutil::dumps(&json!({
            "generation": self.layout_generation,
            "state": self.state,
            "tenants": Value::Object(tenants),
        }))
    }

    fn mark_self_running(&mut self) {
        let mut found = false;
        for component in &mut self.api.components {
            if component.hostname == self.hostname {
                component.state = "running".into();
                found = true;
            }
        }
        if !found && !self.api.components.is_empty() {
            self.api.components.push(ComponentInfo {
                hostname: self.hostname.clone(),
                state: "running".into(),
                connection_filter: self.connection_filter.clone(),
            });
        }
    }

    fn persist_running_component(&self) {
        let Some(path) = &self.component_path else {
            return;
        };
        let _ = zk::set_component(
            self.api.zk.as_ref(),
            path,
            &self.hostname,
            "running",
            &self.connection_filter,
        );
    }

    /// Providers this launcher is allowed to drive. The stored map keeps
    /// every provider; `connection_filter` only skips endpoint work.
    pub fn connection_allowed(&self, connection_name: &str) -> bool {
        match &self.connection_filter {
            None => true,
            Some(filter) if filter.is_empty() => true,
            Some(filter) => filter.iter().any(|name| name == connection_name),
        }
    }

    pub fn sleep_after(&self, duration: Duration) -> Duration {
        let max = Duration::from_secs_f64(MAX_SLEEP);
        if duration >= max {
            Duration::from_secs(0)
        } else {
            max - duration
        }
    }

    /// True when the Python request cache is behind the node's assignment
    /// and this pass must not treat the request as missing.
    fn await_request_cache(&mut self, node_id: &str) -> bool {
        if !cloud::enabled() {
            return false;
        }
        let Some(node) = self.api.nodes.get(node_id) else {
            return false;
        };
        if node.request_id.is_none() {
            return false;
        }
        // Do not trust the Rust request map here. The test locks the
        // Python cache queue, so only that cache's max_zxid reports
        // whether the assignment is safe to act on.
        match cloud::invoke("request_cache_ready", &json!({ "uuid": node_id })) {
            Ok(v) => {
                if v.get("wait").and_then(|w| w.as_bool()).unwrap_or(false) {
                    self.wake();
                    return true;
                }
                if v.get("request_visible").and_then(|w| w.as_bool()).unwrap_or(false) {
                    self.api.sync();
                }
                false
            }
            Err(_) => false,
        }
    }

    fn matching_ids(&mut self, op: &str) -> Vec<String> {
        if cloud::enabled() {
            self.api.sync();
            if let Ok(v) = cloud::invoke(op, &json!({})) {
                if let Some(ids) = v.get("ids").and_then(|a| a.as_array()) {
                    return ids
                        .iter()
                        .filter_map(|id| id.as_str().map(|s| s.to_string()))
                        .collect();
                }
            }
        }
        if op == "matching_nodes" {
            self.api.get_matching_nodes()
        } else {
            self.api.get_matching_requests()
        }
    }

    pub fn process_requests(&mut self) {
        // Launcher decisions run in Rust. When the PyO3 harness is
        // active, REQUESTED accepts go through launcher._acceptRequest
        // (accept_one) so unittest patches of that method still fire;
        // the method itself drives Rust accept_request_by_uuid.
        // Refresh provider quota (including unmanaged usage from the
        // cleanup worker) before accept_one nests another cloud call.
        self.refresh_cloud_quotas();
        self.accept_pending.clear();
        let unassigned = self.unassigned_nodes();
        let mut pending = self.accept_pending.clone();
        let ids = self.matching_ids("matching_requests");
        for id in ids {
            let Some(req) = self.api.requests.get(&id).cloned() else { continue };
            if model::is_final_request_state(&req.state()) && req.event_state() == model::event_state::COMPLETE {
                continue;
            }
            if req.state() == request_state::REQUESTED {
                if cloud::enabled() {
                    // Lock + patch hook on the Python request object;
                    // real accept is Rust via PyO3 _acceptRequest.
                    let _ = cloud::invoke("accept_one", &json!({ "uuid": id }));
                    self.api.sync();
                    pending = self.accept_pending.clone();
                } else {
                    self.process_nodeset_request(&req, &unassigned, &mut pending);
                    self.accept_pending = pending.clone();
                }
            } else if req.state() == request_state::ACCEPTED {
                self.process_nodeset_request(&req, &unassigned, &mut pending);
            } else if model::is_final_request_state(&req.state()) {
                // The request is already final but the completion event
                // was not recorded. Emit it again, matching
                // _processNodesetRequest.
                let mut req = req;
                let state = req.state();
                self.finish_request(&mut req, &state);
            }
        }
        self.accept_pending = pending;
    }

    /// Replace the provider_quota cache from a JSON object of
    /// `{canonical_name: {"default": ..., "quota": {...}}}`. Used by
    /// accept_one to inject getProviderQuota (limits minus unmanaged)
    /// without a nested cloud::invoke during accept. Also keys each
    /// entry under every loaded Rust provider canonical_name so a
    /// Python/Rust name spelling mismatch cannot miss the cache.
    pub fn seed_quota_cache_from_json(&mut self, payload: &str) {
        let Ok(value) = serde_json::from_str::<Value>(payload) else {
            return;
        };
        let Some(obj) = value.as_object() else {
            return;
        };
        let rust_names: Vec<String> = self
            .tenant_providers
            .values()
            .flat_map(|list| list.iter().map(|p| p.canonical_name().to_string()))
            .collect();
        for (name, entry) in obj {
            let q = cloud::quota_from_payload(entry);
            self.quota_cache.insert(name.clone(), (q.copy(), self.now));
            // If this entry's key matches a Rust provider name or is the
            // only provider, also store under the Rust canonical_name.
            for rn in &rust_names {
                if rn == name || rust_names.len() == 1 {
                    self.quota_cache.insert(rn.clone(), (q.copy(), self.now));
                }
            }
        }
    }

    /// Accept one REQUESTED nodeset by uuid. Used by the PyO3
    /// `_acceptRequest` hook so patches wrap Rust accept logic.
    pub fn accept_request_by_uuid(&mut self, uuid: &str) -> Result<(), String> {
        let Some(req) = self.api.requests.get(uuid).cloned() else {
            return Err(format!("missing request {uuid}"));
        };
        if req.state() != request_state::REQUESTED {
            return Ok(());
        }
        let unassigned = self.unassigned_nodes();
        let mut pending = self.accept_pending.clone();
        let result = self.accept_request(&req, &unassigned, &mut pending);
        self.accept_pending = pending;
        match result {
            Err(err) if err.contains("No provider") => {
                // Match process_nodeset_request: mark FAILED rather than
                // surfacing the error through the accept_one adapter
                // (that path only logs and leaves the request REQUESTED).
                let mut req = self
                    .api
                    .requests
                    .get(uuid)
                    .cloned()
                    .unwrap_or(req);
                self.finish_request(&mut req, request_state::FAILED);
                Ok(())
            }
            other => other,
        }
    }

    fn process_nodeset_request(
        &mut self,
        request: &NodesetRequest,
        unassigned: &HashMap<String, Vec<String>>,
        pending: &mut HashSet<(String, String)>,
    ) {
        if request.state() == request_state::REQUESTED {
            if let Err(err) = self.accept_request(request, unassigned, pending) {
                if err.contains("No provider") {
                    let mut req = self
                        .api
                        .requests
                        .get(&request.uuid())
                        .cloned()
                        .unwrap_or_else(|| request.clone());
                    self.finish_request(&mut req, request_state::FAILED);
                }
            }
        } else if request.state() == request_state::ACCEPTED {
            self.check_request(request, unassigned);
        }
    }

    fn finish_request(&mut self, request: &mut NodesetRequest, state: &str) {
        request.set_state(state);
        request.set_event_state(model::event_state::COMPLETE);
        let _ = self.api.write_request(request);
        self.pending_events.push(json!({
            "tenant": request.tenant_name(),
            "pipeline": request.pipeline_name(),
            "uuid": request.uuid(),
            "buildset": request.buildset_uuid(),
        }));
        self.wake();
    }

    fn check_request(&mut self, request: &NodesetRequest, unassigned: &HashMap<String, Vec<String>>) {
        let ids = request.nodes();
        if ids.is_empty() {
            return;
        }
        for id in &ids {
            if !self.api.nodes.contains_key(id) {
                return;
            }
        }
        // server.py _checkRequest: if ANY node is in FAILED_STATES, re-select
        // providers for every slot. Retry failed/HOLD nodes and also READY
        // nodes whose provider no longer matches (locality shift).
        let any_failed = ids.iter().any(|id| {
            self.api
                .nodes
                .get(id)
                .map(|n| model::is_failed_node_state(&n.state()))
                .unwrap_or(false)
        });
        if any_failed {
            let mut req = request.clone();
            let mut messages = Vec::new();
            let label_providers = match self.select_providers(&req, unassigned, &mut messages) {
                Ok(v) => v,
                Err(err) => {
                    if err.contains("No provider") {
                        self.finish_request(&mut req, request_state::FAILED);
                    }
                    return;
                }
            };
            for (i, node_id) in ids.iter().enumerate() {
                let Some((label_name, provider_cname)) = label_providers.get(i) else {
                    continue;
                };
                let Some(old) = self.api.nodes.get(node_id).cloned() else {
                    continue;
                };
                let provider_changed = old.provider() != *provider_cname;
                let is_failed = model::is_failed_node_state(&old.state());
                let is_hold = old.state() == node_state::HOLD;
                if !(is_failed || is_hold || provider_changed) {
                    continue;
                }
                let add_failed = if old.state() == node_state::FAILED {
                    Some(old.provider())
                } else {
                    None
                };
                let Some(provider) = self.provider_by_cname(&req.tenant_name(), provider_cname) else {
                    continue;
                };
                if provider.driver_name() == "static" {
                    // Static: keep the same node reference (server.py).
                    continue;
                }
                let Some(label) = provider.labels().get(label_name).cloned() else {
                    continue;
                };
                let new_id = if let Some(existing) =
                    self.assign_unassigned(&req, &label, provider.as_ref(), unassigned)
                {
                    existing
                } else {
                    match self.request_node(provider.as_ref(), &label, Some(&req), None) {
                        Ok(created) => created,
                        Err(_) => continue,
                    }
                };
                if new_id == *node_id {
                    continue;
                }
                self.api.delete_assignment(node_id).ok();
                req.update_provider_node(
                    i,
                    &new_id,
                    label.executor_zone.as_deref(),
                    add_failed.as_deref(),
                );
            }
            self.messages.append(&mut messages);
            let _ = self.api.write_request(&req);
            self.wake();
            return;
        }
        let mut all_final = true;
        let mut all_ready = true;
        for id in &ids {
            let state = self.api.nodes.get(id).map(|n| n.state()).unwrap_or_default();
            if !model::is_final_node_state(&state) {
                all_final = false;
            }
            if state != node_state::READY && state != node_state::SLOT_HOST {
                all_ready = false;
            }
        }
        if !all_final {
            return;
        }
        let mut req = request.clone();
        if !all_ready {
            for id in &ids {
                self.api.delete_assignment(id).ok();
            }
            self.finish_request(&mut req, request_state::FAILED);
        } else {
            self.finish_request(&mut req, request_state::FULFILLED);
        }
    }

    pub fn accept_request(
        &mut self,
        request: &NodesetRequest,
        unassigned: &HashMap<String, Vec<String>>,
        pending: &mut HashSet<(String, String)>,
    ) -> Result<(), String> {
        let mut messages = Vec::new();
        let label_providers = self.select_providers(request, unassigned, &mut messages)?;
        let mut req = request.clone();
        for (i, (label_name, provider_cname)) in label_providers.iter().enumerate() {
            if i < req.provider_node_data().len() {
                continue;
            }
            let provider = self
                .provider_by_cname(&req.tenant_name(), provider_cname)
                .ok_or_else(|| format!("missing provider {provider_cname}"))?;
            let label = provider
                .labels()
                .get(label_name)
                .cloned()
                .ok_or_else(|| format!("missing label {label_name}"))?;
            if let Some(node_id) = self.assign_unassigned(&req, &label, provider.as_ref(), unassigned) {
                req.add_provider_node(&node_id, label.executor_zone.as_deref());
                continue;
            }
            if provider.driver_name() == "static" {
                self.messages.append(&mut messages);
                self.api.write_request(&req).ok();
                return Ok(());
            }
            if req.provider_node_data().is_empty() {
                let key = (provider.name().to_string(), label.name.clone());
                if pending.contains(&key) {
                    self.messages.append(&mut messages);
                    return Ok(());
                }
                let has_quota = self.does_provider_have_quota_for_label(
                    &req.tenant_name(),
                    provider.as_ref(),
                    &label,
                    true,
                );
                if !has_quota {
                    pending.insert(key);
                    self.messages.append(&mut messages);
                    return Ok(());
                }
            }
            let node_id = self.request_node(provider.as_ref(), &label, Some(&req), None)?;
            req.add_provider_node(&node_id, label.executor_zone.as_deref());
        }
        if req.provider_node_data().len() == req.labels().len() && !req.labels().is_empty() {
            req.set_state(request_state::ACCEPTED);
        }
        self.api.write_request(&req).map_err(|e| e.to_string())?;
        self.messages.append(&mut messages);
        // Wake so a peer launcher that owns the new nodes (launcher_score)
        // can process them on the next pass; same-launcher process_nodes
        // also runs in this process_once after process_requests.
        self.wake();
        Ok(())
    }

    fn assign_unassigned(
        &mut self,
        request: &NodesetRequest,
        label: &crate::provider::Label,
        provider: &dyn Provider,
        unassigned: &HashMap<String, Vec<String>>,
    ) -> Option<String> {
        let ids = unassigned.get(&label.name)?.clone();
        for id in ids {
            let Some(node) = self.api.nodes.get(&id).cloned() else { continue };
            if node.provider() != provider.canonical_name() {
                continue;
            }
            if node.request_id.is_some() {
                continue;
            }
            if model::assignable_rank(&node.state()).is_none() {
                continue;
            }
            let value = json!({
                "request_id": request.uuid(),
                "min_request_version": request.zk_version.unwrap_or(0) + 1,
                "tenant_name": request.tenant_name(),
            });
            if self.api.write_assignment(&id, &value).is_ok() {
                // Refresh Python cache assignment (paused-watch tests).
                self.python_cache_assign(
                    &id,
                    request.uuid().as_str(),
                    request.tenant_name().as_str(),
                    request.zk_version.unwrap_or(0) + 1,
                );
                return Some(id);
            }
        }
        None
    }

    pub fn select_providers(
        &self,
        request: &NodesetRequest,
        unassigned: &HashMap<String, Vec<String>>,
        messages: &mut Vec<String>,
    ) -> Result<Vec<(String, String)>, String> {
        let Some(list) = self.tenant_providers.get(&request.tenant_name()) else {
            return Err(format!("No provider for tenant {}", request.tenant_name()));
        };
        let mut providers = list.clone();
        {
            let mut rng = rand::thread_rng();
            providers.shuffle(&mut rng);
        }
        providers.sort_by(|a, b| {
            let pa = self.get_quota_percentage(a.as_ref(), messages);
            let pb = self.get_quota_percentage(b.as_ref(), messages);
            pa.partial_cmp(&pb).unwrap_or(std::cmp::Ordering::Equal)
        });
        if !unassigned.is_empty() {
            let mut counts: HashMap<String, i64> = HashMap::new();
            for label_name in request.labels() {
                if let Some(ids) = unassigned.get(&label_name) {
                    for id in ids {
                        if let Some(node) = self.api.nodes.get(id) {
                            *counts.entry(node.provider()).or_insert(0) += 1;
                        }
                    }
                }
            }
            providers.sort_by(|a, b| {
                let ca = counts.get(a.canonical_name()).copied().unwrap_or(0);
                let cb = counts.get(b.canonical_name()).copied().unwrap_or(0);
                cb.cmp(&ca)
            });
        }
        let order: Vec<String> = providers.iter().map(|p| p.canonical_name().to_string()).collect();
        let mut ideal_all: HashSet<String> = order.iter().cloned().collect();
        let mut all: HashSet<String> = order.iter().cloned().collect();
        let mut per_label: Vec<Vec<String>> = Vec::new();
        // Providers already used by non-failed nodes in this request
        // (server.py existing_provider_names Counter) drive locality.
        // Vec preserves first-encountered order for most_common ties.
        let mut existing_providers: Vec<(String, i64)> = Vec::new();
        for (i, label_name) in request.labels().iter().enumerate() {
            let mut failures: HashMap<String, i64> = HashMap::new();
            if let Some(pnd) = request.provider_node_data().get(i) {
                if let Some(arr) = pnd.get("failed_providers").and_then(|v| v.as_array()) {
                    for f in arr {
                        if let Some(s) = f.as_str() {
                            *failures.entry(s.to_string()).or_insert(0) += 1;
                        }
                    }
                }
                // A node that just failed counts even before it is recorded
                // on the request, matching _selectProviders.
                if let Some(uuid) = pnd.get("uuid").and_then(|v| v.as_str()) {
                    if let Some(node) = self.api.nodes.get(uuid) {
                        if node.state() == node_state::FAILED {
                            *failures.entry(node.provider()).or_insert(0) += 1;
                        }
                        // FAILED_STATES = TEMPFAILED | FAILED — exclude those
                        // from the locality preference counter.
                        if node.state() != node_state::FAILED
                            && node.state() != node_state::TEMPFAILED
                        {
                            let pname = node.provider();
                            if let Some((_, c)) =
                                existing_providers.iter_mut().find(|(n, _)| n == &pname)
                            {
                                *c += 1;
                            } else {
                                existing_providers.push((pname, 1));
                            }
                        }
                    }
                }
            }
            let mut ideal = Vec::new();
            let mut usable = Vec::new();
            for p in &providers {
                let Some(label) = p.labels().get(label_name) else { continue };
                // Image-validate requests pin a specific ImageUpload; only
                // providers that host that upload may supply the label
                // (server.py _selectProviders).
                if let Some(upload_uuid) = request.image_upload_uuid() {
                    let image_name = label.image.as_deref().unwrap_or("");
                    let names = request.image_names();
                    if !names.is_empty() && names.iter().any(|n| n == image_name) {
                        let Some(image) = p.images().get(image_name) else {
                            continue;
                        };
                        let image_cname = &image.canonical_name;
                        let ok = self.api.uploads.values().any(|u| {
                            u.uuid() == upload_uuid
                                && u.canonical_name() == *image_cname
                                && u.providers()
                                    .iter()
                                    .any(|pn| pn == p.canonical_name())
                        });
                        if !ok {
                            continue;
                        }
                    }
                }
                if !self.does_provider_have_quota_for_label(&request.tenant_name(), p.as_ref(), label, false) {
                    continue;
                }
                ideal.push(p.canonical_name().to_string());
                let fails = failures.get(p.canonical_name()).copied().unwrap_or(0);
                if fails >= p.launch_attempts() {
                    continue;
                }
                usable.push(p.canonical_name().to_string());
            }
            ideal_all = ideal_all.intersection(&ideal.iter().cloned().collect()).cloned().collect();
            all = all.intersection(&usable.iter().cloned().collect()).cloned().collect();
            per_label.push(usable);
        }
        let require_same = !ideal_all.is_empty();
        if require_same {
            messages.push("Requiring same provider for all labels".into());
        }
        let all_ordered: Vec<String> = order.iter().filter(|p| all.contains(*p)).cloned().collect();
        // Prefer the most common existing provider, then preferred_provider,
        // then the first provider that can supply every label.
        let mut main: Option<String> = None;
        // Match collections.Counter.most_common(): highest count wins;
        // ties keep first-encountered insertion order (not alphabetical).
        let mut most_common: Option<(String, i64)> = None;
        for (name, count) in &existing_providers {
            match &most_common {
                None => most_common = Some((name.clone(), *count)),
                Some((_, best)) if *count > *best => {
                    most_common = Some((name.clone(), *count));
                }
                _ => {}
            }
        }
        if let Some((name, _)) = most_common {
            messages.push(format!(
                "Using most common provider {name} from {existing_providers:?}"
            ));
            main = Some(name);
        } else if let Some(pref) = request
            .preferred_provider()
            .filter(|n| order.iter().any(|p| p == n))
        {
            messages.push(format!("Using requested provider {pref}"));
            main = Some(pref);
        }
        if main.as_ref().map(|m| !order.iter().any(|p| p == m)).unwrap_or(false) {
            messages.push(format!(
                "Unable to find provider {}; ignoring",
                main.as_deref().unwrap_or("")
            ));
            main = None;
        }
        if main.is_none() {
            if let Some(first) = all_ordered.first() {
                messages.push(format!(
                    "Using first provider for all labels {first} from {all_ordered:?}"
                ));
                main = Some(first.clone());
            }
        }
        let mut out = Vec::new();
        for (i, label_name) in request.labels().iter().enumerate() {
            let candidates: Vec<String> = if require_same {
                all_ordered.clone()
            } else {
                per_label.get(i).cloned().unwrap_or_default()
            };
            if candidates.is_empty() {
                return Err(format!("No provider found for label {label_name}"));
            }
            let chosen = if let Some(m) = &main {
                if candidates.iter().any(|c| c == m) {
                    m.clone()
                } else {
                    candidates[0].clone()
                }
            } else {
                candidates[0].clone()
            };
            out.push((label_name.clone(), chosen));
        }
        Ok(out)
    }

    pub fn get_quota_percentage(&self, provider: &dyn Provider, messages: &mut Vec<String>) -> f64 {
        let total = match self.provider_quota_available(provider, true) {
            Some(q) => q,
            None => return 1.0,
        };
        let used = self.api.get_quota(provider.canonical_name(), true);
        let mut pct = 0.0;
        messages.push(format!("Provider {} quota claimed by Zuul", provider.canonical_name()));
        let _ = used;
        for (resource, total_r) in &total.quota {
            let used_r = used.quota.get(resource).copied().unwrap_or(used.default);
            if *total_r == 0.0 {
                pct = 1.0_f64.max(pct);
            } else if total_r.is_infinite() {
                pct = pct.max(0.0);
            } else {
                pct = pct.max(used_r / total_r);
            }
        }
        if pct < 1.0 {
            pct = jsonutil::python_round_1(pct);
        }
        let _ = messages;
        pct
    }

    /// Pull getProviderQuota (endpoint limits minus unmanaged) into the
    /// Rust cache. Must run outside nested accept_one cloud calls so the
    /// Python quota_cache from CleanupWorker is visible.
    fn refresh_cloud_quotas(&mut self) {
        if !cloud::enabled() {
            return;
        }
        let names: Vec<String> = self
            .tenant_providers
            .values()
            .flat_map(|list| list.iter().map(|p| p.canonical_name().to_string()))
            .collect();
        for name in names {
            if let Ok(value) =
                cloud::invoke("provider_quota", &json!({ "provider": name }))
            {
                let q = cloud::quota_from_payload(&value);
                self.quota_cache.insert(name, (q, self.now));
            }
        }
    }

    fn provider_quota(&self, provider: &dyn Provider) -> QuotaInformation {
        if let Some((q, ts)) = self.quota_cache.get(provider.canonical_name()) {
            if self.now - ts < MAX_QUOTA_AGE {
                return q.copy();
            }
        }
        if cloud::enabled() {
            if let Ok(value) = cloud::invoke(
                "provider_quota",
                &json!({"provider": provider.canonical_name()}),
            ) {
                return cloud::quota_from_payload(&value);
            }
        }
        let mut quota = provider.endpoint().limits.copy();
        let unmanaged = provider.endpoint().unmanaged.copy();
        quota.subtract(&unmanaged);
        quota
    }

    fn provider_quota_available(&self, provider: &dyn Provider, include_requested: bool) -> Option<QuotaInformation> {
        let mut quota = self.provider_quota(provider);
        let mut other = QuotaInformation::default();
        for list in self.tenant_providers.values() {
            for other_provider in list {
                if other_provider.endpoint().canonical_name != provider.endpoint().canonical_name {
                    continue;
                }
                if other_provider.canonical_name() == provider.canonical_name() {
                    continue;
                }
                other.add(&self.api.get_quota(other_provider.canonical_name(), include_requested));
            }
        }
        quota.subtract(&other);
        let mut limits = QuotaInformation { default: f64::INFINITY, quota: BTreeMap::new() };
        for (k, v) in provider.resource_limits() {
            limits.quota.insert(k.clone(), *v);
        }
        if !limits.quota.is_empty() || provider.resource_limits().is_empty() {
            // min() only tightens resources present on the provider limits.
            if !provider.resource_limits().is_empty() {
                quota.min_with(&limits);
            }
        }
        Some(quota)
    }

    pub fn does_provider_have_quota_for_label(
        &self,
        tenant: &str,
        provider: &dyn Provider,
        label: &crate::provider::Label,
        include_usage: bool,
    ) -> bool {
        // Prefer the seeded/refreshed provider_quota cache (includes
        // unmanaged usage). Nested quota_for_label cloud calls from
        // inside accept_one are unreliable; accept_one seeds the cache.
        let mut total = if include_usage {
            let Some(mut total) = self.provider_quota_available(provider, true) else {
                return false;
            };
            let used = self.api.get_quota(provider.canonical_name(), true);
            total.subtract(&used);
            total
        } else {
            let mut total = self
                .cloud_endpoint_limits(provider)
                .unwrap_or_else(|| provider.endpoint().limits.copy());
            if !provider.resource_limits().is_empty() {
                let limits = QuotaInformation { default: f64::INFINITY, quota: provider.resource_limits().clone() };
                total.min_with(&limits);
                let _ = limits;
            }
            total
        };
        let label_quota = self
            .cloud_label_quota(provider, label)
            .unwrap_or_else(|| provider.quota_for_label(label));
        total.subtract(&label_quota);
        // server.py doesProviderHaveQuotaForLabel: when include_usage,
        // also enforce tenant label max_nodes.
        if include_usage && !self.does_tenant_have_quota_for_label(tenant, label) {
            return false;
        }
        total.non_negative()
    }

    /// Match doesTenantHaveQuotaForLabel: nodes assigned to the tenant
    /// plus unassigned nodes on that tenant's providers for this label.
    fn does_tenant_have_quota_for_label(
        &self,
        tenant: &str,
        label: &crate::provider::Label,
    ) -> bool {
        let Some(limit) = label.max_nodes else {
            return true;
        };
        let providers: Vec<String> = self
            .tenant_providers
            .get(tenant)
            .map(|list| {
                list.iter()
                    .map(|p| p.canonical_name().to_string())
                    .collect()
            })
            .unwrap_or_default();
        let usage = self.api.node_count(tenant, &providers, &label.name);
        usage < limit
    }

    fn cloud_endpoint_limits(&self, provider: &dyn Provider) -> Option<QuotaInformation> {
        if !cloud::enabled() {
            return None;
        }
        cloud::invoke(
            "endpoint_limits",
            &json!({"provider": provider.canonical_name()}),
        )
        .ok()
        .map(|v| cloud::quota_from_payload(&v))
    }

    fn cloud_label_quota(&self, provider: &dyn Provider, label: &crate::provider::Label) -> Option<QuotaInformation> {
        if !cloud::enabled() {
            return None;
        }
        cloud::invoke(
            "label_quota",
            &json!({"provider": provider.canonical_name(), "label": label.name}),
        )
        .ok()
        .map(|v| cloud::quota_from_payload(&v))
    }

    pub fn does_provider_have_quota_for_node(&self, provider: &dyn Provider, node: &ProviderNode) -> bool {
        let Some(mut total) = self.provider_quota_available(provider, false) else {
            return false;
        };
        let used = self.api.get_quota(provider.canonical_name(), false);
        total.subtract(&used);
        total.subtract(&node.quota());
        total.non_negative()
    }

    pub fn request_node(
        &mut self,
        provider: &dyn Provider,
        label: &crate::provider::Label,
        request: Option<&NodesetRequest>,
        static_node: Option<&StaticNodeConfig>,
    ) -> Result<String, String> {
        let id = match static_node {
            Some(sn) => sn.uuid.clone(),
            None => hex_id(),
        };
        let state = if static_node.is_some() {
            node_state::REGISTERING
        } else {
            node_state::REQUESTED
        };
        let mut label_aliases = match static_node {
            Some(sn) => sn.aliases.clone(),
            None => Vec::new(),
        };
        for alias in &label.aliases {
            if !label_aliases.iter().any(|a| a == alias) {
                label_aliases.push(alias.clone());
            }
        }
        let connection_port = match static_node {
            Some(sn) => sn.connection_port,
            None => label.connection_port,
        };
        let username = match static_node {
            Some(sn) => sn.username.clone(),
            None => label.username.clone(),
        };
        let quota = self
            .cloud_label_quota(provider, label)
            .unwrap_or_else(|| provider.quota_for_label(label));
        let mut fields = Map::new();
        let now = self.now;
        let set = |m: &mut Map<String, Value>, k: &str, v: Value| {
            m.insert(k.into(), v);
        };
        set(&mut fields, "uuid", json!(id));
        set(&mut fields, "state", json!(state));
        set(&mut fields, "state_time", json!(now));
        set(&mut fields, "request_time", json!(now));
        set(&mut fields, "label", json!(label.name));
        set(&mut fields, "label_config_hash", json!(label.config_hash));
        set(&mut fields, "label_aliases", json!(label_aliases));
        let tags = if cloud::enabled() {
            cloud::invoke(
                "node_tags",
                &json!({
                    "provider": provider.canonical_name(),
                    "label": label.name,
                    "uuid": id,
                    "request": request.map(|r| r.uuid()),
                    "tenant": request.map(|r| r.tenant_name()),
                }),
            )
            .unwrap_or_else(|_| json!({}))
        } else {
            json!({})
        };
        set(&mut fields, "tags", tags);
        set(&mut fields, "connection_name", json!(provider.connection_name()));
        set(&mut fields, "endpoint_name", json!(provider.endpoint().name));
        set(&mut fields, "comment", Value::Null);
        set(&mut fields, "main_node_id", Value::Null);
        set(&mut fields, "subnodes", json!([]));
        set(&mut fields, "create_state", json!({}));
        set(&mut fields, "delete_state", json!({}));
        set(&mut fields, "quota", quota.get_resources());
        set(&mut fields, "image_upload_uuid", request.and_then(|r| r.image_upload_uuid()).map(|s| json!(s)).unwrap_or(Value::Null));
        set(&mut fields, "zuul_event_id", json!(request.map(|r| r.zuul_event_id()).unwrap_or_default()));
        set(&mut fields, "span_info", Value::Null);
        set(&mut fields, "max_ready_age", json!(label.max_ready_age));
        set(&mut fields, "max_age", json!(label.max_age));
        set(&mut fields, "min_retention_time", json!(label.min_retention_time));
        set(&mut fields, "attributes", json!({}));
        set(&mut fields, "az", Value::Null);
        set(&mut fields, "boot_timeout", json!(label.boot_timeout));
        set(&mut fields, "snapshot_timeout", json!(label.snapshot_timeout));
        set(&mut fields, "snapshot_expiration", json!(label.snapshot_expiration));
        set(&mut fields, "executor_zone", label.executor_zone.clone().map(|s| json!(s)).unwrap_or(Value::Null));
        set(&mut fields, "cloud", Value::Null);
        let image_attrs = if cloud::enabled() {
            cloud::invoke(
                "image_attrs",
                &json!({
                    "provider": provider.canonical_name(),
                    "label": label.name,
                }),
            )
            .unwrap_or_else(|_| json!({}))
        } else {
            json!({})
        };
        let attr = |key: &str| image_attrs.get(key).cloned();
        set(
            &mut fields,
            "connection_port",
            if static_node.is_some() {
                json!(connection_port)
            } else {
                attr("connection_port").unwrap_or_else(|| json!(connection_port))
            },
        );
        set(
            &mut fields,
            "connection_type",
            attr("connection_type").unwrap_or_else(|| {
                label
                    .connection_type
                    .clone()
                    .map(|s| json!(s))
                    .unwrap_or(Value::Null)
            }),
        );
        set(&mut fields, "kubernetes_connection", json!({}));
        set(
            &mut fields,
            "username",
            if static_node.is_some() {
                username.map(|s| json!(s)).unwrap_or(Value::Null)
            } else {
                attr("username").unwrap_or_else(|| {
                    username.map(|s| json!(s)).unwrap_or(Value::Null)
                })
            },
        );
        set(
            &mut fields,
            "python_path",
            attr("python_path").unwrap_or_else(|| {
                label
                    .python_path
                    .clone()
                    .map(|s| json!(s))
                    .unwrap_or(json!("auto"))
            }),
        );
        set(
            &mut fields,
            "shell_type",
            attr("shell_type").unwrap_or(Value::Null),
        );
        set(&mut fields, "hold_expiration", Value::Null);
        set(&mut fields, "host_id", Value::Null);
        set(&mut fields, "host_key_checking", json!(label.host_key_checking));
        set(&mut fields, "host_keys", json!([]));
        set(&mut fields, "interface_ip", Value::Null);
        set(&mut fields, "private_ipv4", Value::Null);
        set(&mut fields, "private_ipv6", Value::Null);
        set(&mut fields, "provider", json!(provider.canonical_name()));
        set(&mut fields, "public_ipv4", Value::Null);
        set(&mut fields, "public_ipv6", Value::Null);
        set(&mut fields, "region", Value::Null);
        set(&mut fields, "resources", Value::Null);
        set(&mut fields, "slot", Value::Null);
        set(&mut fields, "reuse", json!(label.reuse));
        set(&mut fields, "node_properties", json!({}));
        if provider.driver_name() == "aws" {
            set(&mut fields, "aws_instance_id", Value::Null);
            set(&mut fields, "aws_dedicated_host_id", Value::Null);
            set(&mut fields, "aws_launch_template_name", Value::Null);
        }
        let mut node = ProviderNode::from_parts(provider.driver_name().into(), Value::Object(fields));
        let slot_count = label.slots;
        if slot_count > 1 {
            let sub_ids: Vec<String> = (0..slot_count).map(|_| hex_id()).collect();
            node.fields.insert("subnodes".into(), json!(sub_ids));
            self.api.write_node(&node).map_err(|e| e.to_string())?;
            self.python_cache_insert(&id, false);
            let mut returned = id.clone();
            for (slot, sub_id) in sub_ids.iter().enumerate() {
                let mut sub = node.clone();
                sub.driver_id = "subnode".into();
                sub.fields.insert("uuid".into(), json!(sub_id));
                sub.fields.insert("slot".into(), json!(slot));
                sub.fields.insert("main_node_id".into(), json!(id));
                sub.fields.insert("subnodes".into(), json!([]));
                sub.fields.insert("quota".into(), json!({}));
                sub.fields.insert("tags".into(), json!({}));
                sub.request_id = None;
                sub.tenant_name = None;
                if slot == 0 {
                    if let Some(req) = request {
                        sub.tenant_name = Some(req.tenant_name());
                        sub.request_id = Some(req.uuid());
                    }
                    returned = sub_id.clone();
                }
                self.api.write_node(&sub).map_err(|e| e.to_string())?;
                // Assignment before cache_insert so fromZK sees request_id
                // (test_subnodes_cache_insert asserts one subnode assigned).
                if slot == 0 {
                    if let Some(req) = request {
                        let min_ver = req.zk_version.unwrap_or(0) + 1;
                        let assign = json!({
                            "request_id": req.uuid(),
                            "min_request_version": min_ver,
                            "tenant_name": req.tenant_name(),
                        });
                        let _ = self.api.write_assignment(sub_id, &assign);
                        self.python_cache_insert(sub_id, true);
                        self.python_cache_assign(
                            sub_id,
                            req.uuid().as_str(),
                            req.tenant_name().as_str(),
                            min_ver,
                        );
                        continue;
                    }
                }
                self.python_cache_insert(sub_id, true);
            }
            return Ok(returned);
        }
        if let Some(req) = request {
            node.tenant_name = Some(req.tenant_name());
            node.request_id = Some(req.uuid());
        }
        self.api.write_node(&node).map_err(|e| e.to_string())?;
        if let Some(req) = request {
            let min_ver = req.zk_version.unwrap_or(0) + 1;
            let assign = json!({
                "request_id": req.uuid(),
                "min_request_version": min_ver,
                "tenant_name": req.tenant_name(),
            });
            let _ = self.api.write_assignment(&id, &assign);
            self.python_cache_insert(&id, false);
            self.python_cache_assign(
                &id,
                req.uuid().as_str(),
                req.tenant_name().as_str(),
                min_ver,
            );
        } else {
            self.python_cache_insert(&id, false);
        }
        Ok(id)
    }

    /// Mirror nodes_cache.insert after a Rust ZK write so tests that
    /// pause the cache event queue still see newly created nodes
    /// (test_subnodes_cache_insert).
    fn python_cache_insert(&self, uuid: &str, is_subnode: bool) {
        if !cloud::enabled() {
            return;
        }
        let _ = cloud::invoke(
            "node.cache_insert",
            &json!({ "uuid": uuid, "subnode": is_subnode }),
        );
    }

    /// Refresh a cached node's assignment after Rust write_assignment
    /// so live Python objects see the new request_id without a watch.
    fn python_cache_assign(
        &self,
        uuid: &str,
        request_id: &str,
        tenant_name: &str,
        min_request_version: i64,
    ) {
        if !cloud::enabled() {
            return;
        }
        let _ = cloud::invoke(
            "node.cache_assign",
            &json!({
                "uuid": uuid,
                "request_id": request_id,
                "tenant_name": tenant_name,
                "min_request_version": min_request_version,
            }),
        );
    }

    pub fn process_nodes(&mut self) {
        let ids = self.matching_ids("matching_nodes");
        for id in ids {
            let before = self.api.nodes.get(&id).map(|n| n.state()).unwrap_or_default();
            if let Err(e) = self.process_provider_node(&id) {
                self.messages.push(format!("node {id}: {e}"));
            }
            let Some(node) = self.api.nodes.get(&id) else {
                continue;
            };
            let after = node.state();
            // The pass that fails a slot host must not go on to delete
            // its subnodes. The cache has to show the failed main node
            // with the subnodes still present.
            if before != after
                && !node.subnodes().is_empty()
                && matches!(after.as_str(), node_state::FAILED | node_state::TEMPFAILED)
            {
                self.wake();
                break;
            }
        }
    }

    pub fn process_provider_node(&mut self, node_id: &str) -> Result<(), String> {
        let Some(node) = self.api.nodes.get(node_id).cloned() else {
            return Ok(());
        };
        // Snapshot nodes are still returned while another actor holds
        // the lock so image snapshot work can run. The rest of node
        // processing, including cleanup, waits until that lock is gone.
        if node.is_locked && !node.has_lock {
            return Ok(());
        }
        // A held node is left alone until hold_expiration, matching
        // _isNodeActionable. Missing request_id must not delete it.
        if node.state() == node_state::HOLD {
            let expired = node
                .hold_expiration()
                .map(|exp| node.state_time() + exp < self.now)
                .unwrap_or(false);
            if !expired {
                return Ok(());
            }
        }
        let request = node
            .request_id
            .as_ref()
            .and_then(|id| self.api.requests.get(id).cloned());
        if (request.is_some() || node.request_id.is_none()) && model::is_create_state(&node.state()) {
            match self.check_node_create(node_id) {
                Ok(()) => {}
                Err(AdvanceError::Quota(e)) => {
                    self.set_node_state(node_id, node_state::TEMPFAILED);
                    self.sync_subnodes(node_id, node_state::TEMPFAILED, false);
                    self.wake();
                    self.messages.push(format!("tempfail {e}"));
                    if self.defer_slot_cleanup(node_id) {
                        return Ok(());
                    }
                }
                Err(e) => {
                    self.set_node_state(node_id, node_state::FAILED);
                    self.sync_subnodes(node_id, node_state::FAILED, false);
                    self.wake();
                    self.messages.push(format!("fail {e}"));
                    if self.defer_slot_cleanup(node_id) {
                        return Ok(());
                    }
                }
            }
        }
        // A request assigned while this launcher's cache was paused is
        // not missing. Tests hook RequestCache.max_zxid to prove we wait.
        if self.await_request_cache(node_id) {
            return Ok(());
        }
        let Some(mut node) = self.api.nodes.get(node_id).cloned() else {
            return Ok(());
        };
        let request = node
            .request_id
            .as_ref()
            .and_then(|id| self.api.requests.get(id).cloned());
        let active = node.request_id.is_some() && request.is_some();
        if node.next_state.is_some() && !active {
            let state = node.next_state.clone().unwrap_or_default();
            let comment = node.next_comment.clone();
            self.api.delete_assignment(node_id).ok();
            if let Some(n) = self.api.nodes.get_mut(node_id) {
                n.next_state = None;
                n.next_comment = None;
                if let Some(c) = &comment {
                    n.comment_set(Some(c));
                }
                n.set_state_at(&state, self.now);
            }
            let stored = self.api.nodes.get(node_id).cloned();
            if let Some(n) = stored {
                let _ = self.api.write_node(&n);
            }
            node = self.api.nodes.get(node_id).cloned().unwrap();
        }
        let request = node
            .request_id
            .as_ref()
            .and_then(|id| self.api.requests.get(id).cloned());
        if request.is_none()
            && (self.api.has_node_expired(&node, self.now)
                || (!self.layout_dirty && self.provider_for_node(&node).is_none()))
        {
            self.set_node_state(node_id, node_state::OUTDATED);
            node = self.api.nodes.get(node_id).cloned().unwrap();
        } else if node.request_id.is_some()
            && request.is_none()
            && model::assignable_rank(&node.state()).is_some()
        {
            self.api.delete_assignment(node_id).ok();
            node = self.api.nodes.get(node_id).cloned().unwrap();
        } else if node.state() == node_state::USED {
            if let Some(provider) = self.provider_for_node(&node) {
                if let Some(label) = provider.labels().get(&node.label()).cloned() {
                    if self.can_reuse(&*provider, &node, &label) {
                        self.check_node_reuse(node_id)?;
                        let still = self.api.nodes.get(node_id).map(|n| n.state()).unwrap_or_default();
                        if still == node_state::USED {
                            return Ok(());
                        }
                        node = self.api.nodes.get(node_id).cloned().unwrap();
                    }
                }
            }
        }
        let request = node
            .request_id
            .as_ref()
            .and_then(|id| self.api.requests.get(id).cloned());
        let cleanup = (node.request_id.is_some() && request.is_none())
            || matches!(
                node.state().as_str(),
                node_state::FAILED
                    | node_state::TEMPFAILED
                    | node_state::SLOT_HOST
                    | node_state::OUTDATED
                    | node_state::DEREGISTER
                    | node_state::USED
            );
        if cleanup {
            self.cleanup_node(node_id)?;
        }
        if let Some(n) = self.api.nodes.get(node_id) {
            if n.state() == node_state::READY || n.state() == node_state::HOLD {
                if let Some(nn) = self.api.nodes.get_mut(node_id) {
                    nn.has_lock = false;
                    nn.is_locked = false;
                }
            }
        }
        Ok(())
    }

    /// server.Launcher class constants. Unit tests monkeypatch the Python
    /// class; the binary keeps the Rust default.
    fn class_constant(&self, name: &str, default: f64) -> f64 {
        if !cloud::enabled() {
            return default;
        }
        match cloud::invoke("class_attr", &json!({ "name": name })) {
            Ok(v) => v.get("value").and_then(|n| n.as_f64()).unwrap_or(default),
            Err(_) => default,
        }
    }

    /// LauncherApi.getProviderNode, so an in-process patch that hides
    /// nodes is visible to the subnode sync timeout.
    fn python_node_present(&self, uuid: &str) -> bool {
        if !cloud::enabled() {
            return true;
        }
        match cloud::invoke("node_visible", &json!({ "uuid": uuid })) {
            Ok(v) => v.get("present").and_then(|p| p.as_bool()).unwrap_or(true),
            Err(_) => true,
        }
    }

    fn check_node_create(&mut self, node_id: &str) -> Result<(), AdvanceError> {
        let node = self.api.nodes.get(node_id).cloned().ok_or_else(|| AdvanceError::Other("missing".into()))?;
        if node.driver_id == "subnode" {
            // _checkNodeCreate fails a subnode whose main node never
            // appears. The lookup is the patched getProviderNode.
            if let Some(main_id) = node.main_node_id() {
                if !self.python_node_present(&main_id) {
                    let window = self.class_constant("SUBNODE_SYNC_TIME", SUBNODE_SYNC_TIME);
                    if self.now - node.request_time() > window {
                        return Err(AdvanceError::Other("Main node not present".into()));
                    }
                    self.wake();
                }
            }
            return Ok(());
        }
        let provider = self
            .provider_for_node(&node)
            .ok_or_else(|| AdvanceError::Other("no provider".into()))?;
        let label = provider
            .labels()
            .get(&node.label())
            .cloned()
            .ok_or_else(|| AdvanceError::Other("no label".into()))?;
        if !self.create_sms.contains_key(node_id) {
            let sm = provider.create_machine(&node, &label);
            self.create_sms.insert(node_id.to_string(), sm);
        }
        let old_state = self.create_sms.get(node_id).unwrap().state_name();
        if old_state == "start" {
            if !self.does_provider_have_quota_for_node(provider.as_ref(), &node) {
                self.wake();
                return Ok(());
            }
            self.create_sms.get_mut(node_id).unwrap().set_start_time(self.now);
        }
        self.create_sms.get_mut(node_id).unwrap().advance()?;
        let sm_state = self.create_sms.get(node_id).unwrap().state_name();
        let complete = self.create_sms.get(node_id).unwrap().complete();
        let ip = self.create_sms.get(node_id).unwrap().interface_ip();
        let updates = self.create_sms.get(node_id).unwrap().node_updates();
        let start_time = self.create_sms.get(node_id).unwrap().start_time();
        if let Some(updates) = updates {
            if let Some(n) = self.api.nodes.get_mut(node_id) {
                n.merge_fields(&updates);
            }
            if let Some(n) = self.api.nodes.get(node_id).cloned() {
                let _ = self.api.write_node(&n);
            }
        }
        if sm_state != "start" {
            if let Some(n) = self.api.nodes.get(node_id) {
                if n.state() == node_state::REQUESTED {
                    self.set_node_state(node_id, node_state::BUILDING);
                    self.sync_subnodes(node_id, node_state::BUILDING, false);
                }
            }
        }
        if !complete {
            if let Some(timeout) = provider.launch_timeout() {
                if self.now - start_time > timeout {
                    return Err(AdvanceError::LaunchTimeout("Timeout creating node".into()));
                }
            }
            self.wake();
            return Ok(());
        }
        if let Some(ip) = ip {
            if let Some(n) = self.api.nodes.get_mut(node_id) {
                n.set_interface_ip(Some(&ip));
            }
        }
        if !self.check_nodescan(node_id)? {
            self.wake();
            return Ok(());
        }
        let has_sub = self.api.nodes.get(node_id).map(|n| !n.subnodes().is_empty()).unwrap_or(false);
        if has_sub {
            let subs = self.api.nodes.get(node_id).map(|n| n.subnodes()).unwrap_or_default();
            let missing = subs.iter().any(|id| !self.python_node_present(id));
            if missing {
                let request_time = self
                    .api
                    .nodes
                    .get(node_id)
                    .map(|n| n.request_time())
                    .unwrap_or(0.0);
                let window = self.class_constant("SUBNODE_SYNC_TIME", SUBNODE_SYNC_TIME);
                if self.now - request_time > window {
                    return Err(AdvanceError::Other(
                        "Subnodes not present at creation".into(),
                    ));
                }
                self.wake();
                return Ok(());
            }
        }
        let state = if has_sub { node_state::SLOT_HOST } else { node_state::READY };
        if has_sub {
            self.sync_subnodes(node_id, node_state::READY, true);
        }
        self.set_node_state(node_id, state);
        self.nodescan.remove(node_id);
        self.report_launch_stats(node_id);
        self.wake();
        Ok(())
    }

    /// A slot host and its subnodes stay in ZooKeeper for one more pass
    /// after create fails. The crash test reads the cache between those
    /// passes and requires the main node to still be present and failed.
    fn defer_slot_cleanup(&self, node_id: &str) -> bool {
        let Some(node) = self.api.nodes.get(node_id) else {
            return false;
        };
        if !node.subnodes().is_empty() || node.driver_id == "subnode" {
            return true;
        }
        false
    }

    fn sync_subnodes(&mut self, main_id: &str, state: &str, copy_attrs: bool) {
        let Some(main) = self.api.nodes.get(main_id).cloned() else {
            return;
        };
        let ids = main.subnodes();
        for sub_id in ids {
            // _setSubnodeStates ignores a subnode getProviderNode cannot
            // see. Failure still has to be copied: the assigned slot is
            // what _checkRequest retries, and the crash test hides nodes.
            if cloud::enabled()
                && !matches!(state, node_state::FAILED | node_state::TEMPFAILED)
                && !self.python_node_present(&sub_id)
            {
                continue;
            }
            let Some(mut sub) = self.api.nodes.get(&sub_id).cloned() else {
                continue;
            };
            if copy_attrs {
                let slot = sub.fields.get("slot").cloned();
                let main_node_id = sub.fields.get("main_node_id").cloned();
                let snapshot = Value::Object(main.fields.clone());
                sub.merge_fields(&snapshot);
                sub.fields.insert("uuid".into(), json!(sub_id));
                sub.fields.insert("subnodes".into(), json!([]));
                if let Some(slot) = slot {
                    sub.fields.insert("slot".into(), slot);
                }
                if let Some(main_node_id) = main_node_id {
                    sub.fields.insert("main_node_id".into(), main_node_id);
                }
            }
            sub.set_state_at(state, self.now);
            let _ = self.api.write_node(&sub);
        }
    }

    fn check_nodescan(&mut self, node_id: &str) -> Result<bool, AdvanceError> {
        let node = self.api.nodes.get(node_id).cloned().unwrap();
        // Always drive the Python NodescanRequest when the test handler
        // is installed. host-key-checking false still calls advance(),
        // which unit tests patch.
        if cloud::enabled() {
            let result = cloud::invoke(
                "nodescan",
                &json!({
                    "uuid": node_id,
                    "interface_ip": node.interface_ip(),
                    "connection_port": node.connection_port(),
                    "host_key_checking": node.host_key_checking(),
                    "connection_type": node.connection_type(),
                    "boot_timeout": node.boot_timeout(),
                }),
            )?;
            if !result.get("complete").and_then(|v| v.as_bool()).unwrap_or(false) {
                return Ok(false);
            }
            if let Some(err) = result.get("error").and_then(|v| v.as_str()) {
                if !err.is_empty() {
                    return Err(AdvanceError::Other(err.to_string()));
                }
            }
            let keys: Vec<String> = result
                .get("keys")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                .unwrap_or_default();
            if let Some(n) = self.api.nodes.get_mut(node_id) {
                if !keys.is_empty() {
                    n.set_host_keys(&keys);
                }
            }
            if let Some(n) = self.api.nodes.get(node_id).cloned() {
                let _ = self.api.write_node(&n);
            }
            return Ok(true);
        }
        if self.nodescan.get(node_id).is_none() {
            let req = NodescanRequest::new(
                node_id,
                node.interface_ip().as_deref().unwrap_or("0.0.0.0"),
                node.connection_port(),
                node.host_key_checking(),
                node.connection_type().as_deref(),
                node.boot_timeout(),
            );
            if !self.nodescan.add(req) {
                self.wake();
                return Ok(false);
            }
        }
        // Drive once so an immediate (host-key-checking off) scan can finish
        // inside this call, matching a worker that is already polling.
        self.nodescan.poll();
        let Some(scan) = self.nodescan.get(node_id) else {
            return Ok(false);
        };
        if !scan.complete {
            return Ok(false);
        }
        if let Some(err) = &scan.error {
            return Err(AdvanceError::Other(err.clone()));
        }
        let keys = scan.keys.clone();
        if let Some(n) = self.api.nodes.get_mut(node_id) {
            if !keys.is_empty() {
                n.set_host_keys(&keys);
            }
        }
        Ok(true)
    }

    fn can_reuse(&self, provider: &dyn Provider, node: &ProviderNode, label: &crate::provider::Label) -> bool {
        if !label.reuse {
            return false;
        }
        provider.can_reuse(node)
    }

    fn check_node_reuse(&mut self, node_id: &str) -> Result<(), String> {
        match self.check_nodescan(node_id) {
            Ok(false) => {
                self.wake();
                Ok(())
            }
            Ok(true) => {
                self.api.delete_assignment(node_id).ok();
                self.set_node_state(node_id, node_state::READY);
                self.nodescan.remove(node_id);
                Ok(())
            }
            Err(e) => {
                self.set_node_state(node_id, node_state::FAILED);
                self.nodescan.remove(node_id);
                Err(e.to_string())
            }
        }
    }

    fn cleanup_node(&mut self, node_id: &str) -> Result<(), String> {
        let node = match self.api.nodes.get(node_id).cloned() {
            Some(n) => n,
            None => return Ok(()),
        };
        self.nodescan.remove(node_id);
        // A main node stays until every subnode is gone or still being
        // created. _cleanupNode applies this before any delete.
        let subs = node.subnodes();
        if subs.iter().any(|id| {
            self.api
                .nodes
                .get(id)
                .map(|n| !model::is_create_state(&n.state()))
                .unwrap_or(false)
        }) {
            return Ok(());
        }
        // Subnodes have no cloud resources. Python uses a no-op
        // SubnodeStateMachine unless the provider is static.
        if node.driver_id == "subnode" {
            let static_driver = self
                .provider_for_node(&node)
                .map(|p| p.driver_name() == "static")
                .unwrap_or(false);
            if !static_driver {
                if !self.request_released_node(&node) {
                    return Ok(());
                }
                self.api.delete_node(node_id).ok();
                return Ok(());
            }
        }
        let provider = match self.provider_for_node(&node) {
            Some(p) => p,
            None => {
                self.api.delete_node(node_id).ok();
                return Ok(());
            }
        };
        if !self.delete_sms.contains_key(node_id) {
            self.delete_sms.insert(node_id.to_string(), provider.delete_machine(&node));
            if let Some(sm) = self.delete_sms.get_mut(node_id) {
                sm.set_start_time(self.now);
            }
        }
        let start = self.delete_sms.get(node_id).map(|s| s.start_time()).unwrap_or(self.now);
        // test_failed_node3 patches Launcher.DELETE_TIMEOUT. The Rust
        // constant stays 600 when no test handler is installed.
        if self.now - start > self.class_constant("DELETE_TIMEOUT", DELETE_TIMEOUT) {
            if let Some(sm) = self.delete_sms.get_mut(node_id) {
                if !sm.complete() {
                    sm.fail();
                }
            }
        }
        if let Some(sm) = self.delete_sms.get_mut(node_id) {
            if !sm.complete() {
                sm.set_now(self.now);
                // An exception in advance marks the machine failed, matching
                // _cleanupNode. The Python shim logs the original stack.
                if sm.advance().is_err() {
                    sm.fail();
                }
            }
        }
        if let Some(updates) = self.delete_sms.get(node_id).and_then(|s| s.node_updates()) {
            if let Some(n) = self.api.nodes.get_mut(node_id) {
                n.merge_fields(&updates);
            }
            if let Some(n) = self.api.nodes.get(node_id).cloned() {
                let _ = self.api.write_node(&n);
            }
        }
        let complete = self.delete_sms.get(node_id).map(|s| s.complete()).unwrap_or(true);
        if !complete {
            self.wake();
            return Ok(());
        }
        let state = self.api.nodes.get(node_id).map(|n| n.state()).unwrap_or_default();
        if state == node_state::REBUILDING {
            self.delete_sms.remove(node_id);
            return Ok(());
        }
        // Leave the node in place while a request still references it
        // so the next request pass can retry or record the failure.
        let node = match self.api.nodes.get(node_id).cloned() {
            Some(n) => n,
            None => return Ok(()),
        };
        if !self.request_released_node(&node) {
            return Ok(());
        }
        self.api.delete_node(node_id).ok();
        self.delete_sms.remove(node_id);
        Ok(())
    }

    /// Launcher._cleanupNode's delete condition: the request is gone,
    /// or our copy of it is new enough and no longer lists this node.
    fn request_released_node(&self, node: &ProviderNode) -> bool {
        let Some(req_id) = node.request_id.as_deref() else {
            return true;
        };
        let Some(request) = self.api.requests.get(req_id) else {
            return true;
        };
        let up_to_date = match node.min_request_version {
            None => true,
            Some(min) => request.zk_version.map(|version| version >= min).unwrap_or(false),
        };
        up_to_date && !request.nodes().iter().any(|id| id == &node.uuid())
    }

    pub fn process_min_ready(&mut self) {
        if !self.api.wait_for_sync(CACHE_SYNC_TIMEOUT) {
            self.messages.push(format!(
                "Timeout waiting {CACHE_SYNC_TIMEOUT}s for node cache to sync"
            ));
            return;
        }
        let slots = self.missing_min_ready_slots();
        for (tenant, label_name, provider_cname) in slots {
            let Some(provider) = self.provider_by_cname(&tenant, &provider_cname) else { continue };
            let Some(label) = provider.labels().get(&label_name).cloned() else { continue };
            let _ = self.request_node(provider.as_ref(), &label, None, None);
        }
    }

    fn missing_min_ready_slots(&self) -> Vec<(String, String, String)> {
        // _getMissingMinReadySlots. A label's aliases count toward the
        // aliased label, and the hash of a node we are about to create
        // is recorded under every alias before the next label is considered.
        let candidates = self.api.launchers();
        let names: Vec<String> = candidates.iter().map(|c| c.hostname.clone()).collect();
        let mut out = Vec::new();
        let mut unassigned: HashMap<String, Vec<String>> = HashMap::new();
        for node in self.api.nodes.values() {
            if node.request_id.is_some() {
                continue;
            }
            unassigned.entry(node.label()).or_default().push(node.label_config_hash());
            for alias in node.label_aliases() {
                unassigned.entry(alias).or_default().push(node.label_config_hash());
            }
        }
        for (tenant, providers) in &self.tenant_providers {
            let mut group_order: Vec<String> = Vec::new();
            let mut groups: HashMap<String, Vec<crate::provider::Label>> = HashMap::new();
            for provider in providers {
                for label in provider.labels().values() {
                    if label.min_ready.unwrap_or(0) <= 0 {
                        continue;
                    }
                    if !self.has_highest_min_ready_score(&label.canonical_name, &names, &candidates) {
                        continue;
                    }
                    if !groups.contains_key(&label.name) {
                        group_order.push(label.name.clone());
                    }
                    groups.entry(label.name.clone()).or_default().push(label.clone());
                    for alias in &label.aliases {
                        let Some(aliased) = provider.labels().get(alias) else {
                            continue;
                        };
                        if !groups.contains_key(&aliased.name) {
                            group_order.push(aliased.name.clone());
                        }
                        groups.entry(aliased.name.clone()).or_default().push(label.clone());
                    }
                }
            }
            for label_name in group_order {
                let Some(labels) = groups.get(&label_name) else {
                    continue;
                };
                let valid: HashSet<String> = labels.iter().map(|l| l.config_hash.clone()).collect();
                let have = unassigned
                    .get(&label_name)
                    .map(|hs| hs.iter().filter(|h| valid.contains(*h)).count())
                    .unwrap_or(0);
                let want = labels[0].min_ready.unwrap_or(0).max(0) as usize;
                if have >= want {
                    continue;
                }
                let label_providers: Vec<String> = providers
                    .iter()
                    .filter(|p| p.labels().contains_key(&label_name))
                    .map(|p| p.canonical_name().to_string())
                    .collect();
                if label_providers.is_empty() {
                    continue;
                }
                for i in have..want {
                    let chosen = &label_providers[i % label_providers.len()];
                    let Some(provider) = providers.iter().find(|p| p.canonical_name() == chosen) else {
                        continue;
                    };
                    let Some(created) = provider.labels().get(&label_name) else {
                        continue;
                    };
                    let created_hash = created.config_hash.clone();
                    let created_aliases = created.aliases.clone();
                    out.push((tenant.clone(), label_name.clone(), chosen.clone()));
                    unassigned.entry(label_name.clone()).or_default().push(created_hash.clone());
                    for alias in created_aliases {
                        if provider.labels().contains_key(&alias) {
                            unassigned.entry(alias).or_default().push(created_hash.clone());
                        }
                    }
                }
            }
        }
        out
    }

    fn has_highest_min_ready_score(&self, label_cname: &str, names: &[String], candidates: &[ComponentInfo]) -> bool {
        let scores = mmh3::scores_for_label(label_cname, names);
        for (_score, name) in scores {
            let Some(launcher) = candidates.iter().find(|c| c.hostname == name) else { continue };
            if launcher.state != "running" {
                continue;
            }
            return launcher.hostname == self.hostname;
        }
        false
    }

    pub fn unassigned_nodes(&self) -> HashMap<String, Vec<String>> {
        let mut nodes: Vec<ProviderNode> = self
            .api
            .nodes
            .values()
            .filter(|n| n.request_id.is_none() && n.subnodes().is_empty() && model::assignable_rank(&n.state()).is_some())
            .cloned()
            .collect();
        nodes.sort_by(|a, b| {
            let ra = model::assignable_rank(&a.state()).unwrap_or(9);
            let rb = model::assignable_rank(&b.state()).unwrap_or(9);
            ra.cmp(&rb).then(a.request_time().partial_cmp(&b.request_time()).unwrap_or(std::cmp::Ordering::Equal))
        });
        let mut map: HashMap<String, Vec<String>> = HashMap::new();
        for node in nodes {
            map.entry(node.label()).or_default().push(node.uuid());
            for alias in node.label_aliases() {
                map.entry(alias).or_default().push(node.uuid());
            }
        }
        map
    }

    fn set_node_state(&mut self, node_id: &str, state: &str) {
        if let Some(n) = self.api.nodes.get_mut(node_id) {
            n.set_state_at(state, self.now);
        }
        let stored = self.api.nodes.get(node_id).cloned();
        if let Some(n) = stored {
            let _ = self.api.write_node(&n);
        }
    }

    /// `_registerStaticNodes` / `_deRegisterStaticNodes` after a layout reload.
    fn register_and_deregister_static_nodes(&mut self) {
        let providers: Vec<Arc<dyn Provider>> = self
            .tenant_providers
            .values()
            .flat_map(|list| list.iter().cloned())
            .filter(|p| p.driver_name() == "static")
            .filter(|p| self.connection_allowed(p.connection_name()))
            .collect();
        for provider in providers {
            self.register_static_nodes(provider.as_ref());
            self.deregister_static_nodes(provider.as_ref());
        }
    }

    fn register_static_nodes(&mut self, provider: &dyn Provider) {
        let nodes: Vec<StaticNodeConfig> = provider.static_nodes().values().cloned().collect();
        for static_node in nodes {
            if self.api.nodes.contains_key(&static_node.uuid) {
                continue;
            }
            let Some(label) = provider.labels().get(&static_node.label).cloned() else {
                continue;
            };
            // NodeExistsError is ignored in Python; a concurrent create is fine.
            let _ = self.request_node(provider, &label, None, Some(&static_node));
        }
    }

    fn deregister_static_nodes(&mut self, provider: &dyn Provider) {
        let known: HashSet<String> = provider.static_nodes().keys().cloned().collect();
        let cname = provider.canonical_name().to_string();
        let stale: Vec<String> = self
            .api
            .nodes
            .values()
            .filter(|n| n.provider() == cname)
            .filter(|n| !known.contains(&n.uuid()))
            .filter(|n| {
                n.main_node_id()
                    .map(|main| !known.contains(&main))
                    .unwrap_or(true)
            })
            .map(|n| n.uuid())
            .collect();
        for id in stale {
            self.messages
                .push(format!("Deregistering static node {id}"));
            self.set_node_state(&id, node_state::DEREGISTER);
        }
    }

    fn provider_for_node(&self, node: &ProviderNode) -> Option<Arc<dyn Provider>> {
        for list in self.tenant_providers.values() {
            for p in list {
                if p.canonical_name() == node.provider() && p.labels().contains_key(&node.label()) {
                    return Some(p.clone());
                }
            }
        }
        for list in self.tenant_providers.values() {
            for p in list {
                if p.canonical_name() == node.provider() {
                    return Some(p.clone());
                }
            }
        }
        None
    }

    fn provider_by_cname(&self, tenant: &str, cname: &str) -> Option<Arc<dyn Provider>> {
        self.tenant_providers.get(tenant)?.iter().find(|p| p.canonical_name() == cname).cloned()
    }

    fn report_launch_stats(&mut self, node_id: &str) {
        let Some(node) = self.api.nodes.get(node_id).cloned() else { return };
        let Some(provider) = self.provider_for_node(&node) else { return };
        let start = self.create_sms.get(node_id).map(|s| s.start_time()).unwrap_or(node.state_time());
        let duration = (node.state_time() - start) * 1000.0;
        let safe = normalize_statsd_name(provider.canonical_name());
        let state = node.state();
        for name in [
            format!("zuul.nodes.launch.{state}"),
            format!("zuul.provider.{safe}.nodes.launch.{state}"),
        ] {
            self.stats.push(StatEvent { kind: "c".into(), name: name.clone(), value: 1.0 });
            self.stats.push(StatEvent { kind: "ms".into(), name, value: duration });
        }
        if let Some(label) = provider.labels().get(&node.label()) {
            let sl = normalize_statsd_name(&label.name);
            let name = format!("zuul.provider.{safe}.label.{sl}.launch.{state}");
            self.stats.push(StatEvent { kind: "c".into(), name: name.clone(), value: 1.0 });
            self.stats.push(StatEvent { kind: "ms".into(), name, value: duration });
            if let Some(image_name) = &label.image {
                if let Some(image) = provider.images().get(image_name) {
                    let si = normalize_statsd_name(&image.name);
                    let name = format!("zuul.provider.{safe}.image.{si}.launch.{state}");
                    self.stats.push(StatEvent { kind: "c".into(), name: name.clone(), value: 1.0 });
                    self.stats.push(StatEvent { kind: "ms".into(), name, value: duration });
                }
            }
        }
    }

    fn wake(&self) {
        if let Ok(mut g) = self.wake.lock() {
            *g = true;
        }
    }

    /// Consume the wake flag set by accept/check/node processing.
    /// The PyO3 run loop maps this onto Python `wake_event` so we match
    /// server.py (ChildrenWatch + explicit wakes) instead of polling every
    /// pass — required for test_launcher_request_order's mid-loop patch.
    pub fn take_wake(&self) -> bool {
        if let Ok(mut g) = self.wake.lock() {
            let v = *g;
            *g = false;
            v
        } else {
            false
        }
    }

    pub fn emit_stats_gauges(&mut self, leaked: i64, unmanaged: i64) {
        self.stats.push(StatEvent { kind: "g".into(), name: "zuul.nodes.leaked".into(), value: leaked as f64 });
        self.stats.push(StatEvent { kind: "g".into(), name: "zuul.nodes.unmanaged".into(), value: unmanaged as f64 });
    }

    /// Start cleanup (optional), stats election, and upload pools (server.py start).
    ///
    /// `start_cleanup` matches Launcher._start_cleanup: unit tests set it
    /// False so leaked-resource cleanup does not run under moto.
    pub fn start_workers(&mut self, start_cleanup: bool) {
        if self.stats_worker.is_some() {
            return;
        }
        if start_cleanup && self.cleanup_worker.is_none() {
            let cleanup_shared = Arc::new(Mutex::new(CleanupShared {
                hostname: self.hostname.clone(),
                components: self.api.components.clone(),
                endpoints: self.endpoint_refs(),
            }));
            self.cleanup_shared = Some(cleanup_shared.clone());
            self.cleanup_worker = Some(CleanupWorker::start(move || {
                let shared = cleanup_shared.lock().unwrap().clone();
                let matched =
                    matching_endpoints(&shared.hostname, &shared.components, &shared.endpoints);
                let _ = matched;
                if cloud::enabled() {
                    let _ = cloud::invoke("cleanup.tick", &json!({}));
                }
            }));
        }

        let snapshot = Arc::new(Mutex::new(StatsSnapshot::default()));
        self.stats_snapshot = Some(snapshot.clone());
        let sink = Arc::new(Mutex::new(Vec::new()));
        self.stats_worker_sink = Some(sink.clone());
        let zk = self.api.zk.clone();
        self.stats_worker = Some(StatsWorker::start(zk, &self.hostname, move || {
            let snap = snapshot.lock().unwrap().clone();
            let events = emit_run_stats(&snap);
            if let Ok(mut g) = sink.lock() {
                g.extend(events);
            }
        }));

        if self.upload_pools.is_none() {
            self.upload_pools = Some(UploadPools::new(&self.hostname));
        }
        self.refresh_worker_shared();
    }

    /// Stop workers (server.py stop: cancel election, join cleanup, shutdown pools).
    ///
    /// Callers that hold the Python GIL (PyO3 harness) must release it before
    /// joining: worker ticks re-enter Python via cloud::invoke.
    pub fn stop_workers(&mut self) {
        let stats = self.stats_worker.take();
        let cleanup = self.cleanup_worker.take();
        let pools = self.upload_pools.take();
        self.stats_snapshot = None;
        self.stats_worker_sink = None;
        self.cleanup_shared = None;
        // Cancel/wake before join so election and interval waits unblock.
        if let Some(worker) = stats {
            worker.join();
        }
        if let Some(worker) = cleanup {
            worker.join();
        }
        if let Some(pools) = pools {
            pools.shutdown();
        }
    }

    fn endpoint_refs(&self) -> Vec<EndpointRef> {
        let mut by_cname: HashMap<String, EndpointRef> = HashMap::new();
        for list in self.tenant_providers.values() {
            for provider in list {
                let ep = provider.endpoint();
                by_cname
                    .entry(ep.canonical_name.clone())
                    .or_insert_with(|| EndpointRef {
                        name: ep.name.clone(),
                        canonical_name: ep.canonical_name.clone(),
                        connection_name: ep.connection_name.clone(),
                    });
            }
        }
        by_cname.into_values().collect()
    }

    fn refresh_worker_shared(&mut self) {
        if let Some(shared) = &self.cleanup_shared {
            if let Ok(mut g) = shared.lock() {
                g.hostname = self.hostname.clone();
                g.components = self.api.components.clone();
                g.endpoints = self.endpoint_refs();
            }
        }
        if let Some(snap) = &self.stats_snapshot {
            if let Ok(mut g) = snap.lock() {
                *g = self.build_stats_snapshot();
            }
        }
        if let Some(sink) = &self.stats_worker_sink {
            if let Ok(mut g) = sink.lock() {
                self.stats.append(&mut *g);
            }
        }
        if let Some(pools) = &self.upload_pools {
            self.stats.extend(pools.take_gauges());
        }
    }

    fn build_stats_snapshot(&self) -> StatsSnapshot {
        let nodes: Vec<(String, String, String)> = self
            .api
            .nodes
            .values()
            .map(|n| (n.state(), n.provider(), n.label()))
            .collect();
        let requests: Vec<String> = self.api.requests.values().map(|r| r.state()).collect();
        let uploads: Vec<(String, String, String)> = self
            .api
            .uploads
            .values()
            .map(|u| (u.canonical_name(), u.endpoint_name(), u.state()))
            .collect();
        let mut seen = HashSet::new();
        let mut providers = Vec::new();
        for list in self.tenant_providers.values() {
            for provider in list {
                if !seen.insert(provider.canonical_name().to_string()) {
                    continue;
                }
                let mut limits = self
                    .cloud_endpoint_limits(provider.as_ref())
                    .unwrap_or_else(|| provider.endpoint().limits.copy());
                if !provider.resource_limits().is_empty() {
                    let pl = QuotaInformation {
                        default: f64::INFINITY,
                        quota: provider.resource_limits().clone(),
                    };
                    limits.min_with(&pl);
                }
                let usage = self.api.get_quota(provider.canonical_name(), true);
                let labels: Vec<(String, String, Option<i64>)> = provider
                    .labels()
                    .values()
                    .map(|l| (l.name.clone(), l.canonical_name.clone(), l.max_nodes))
                    .collect();
                providers.push(ProviderStatsRow {
                    canonical_name: provider.canonical_name().to_string(),
                    limits: limits.quota,
                    usage: usage.quota,
                    labels,
                });
            }
        }
        StatsSnapshot {
            nodes,
            requests,
            uploads,
            providers,
        }
    }

    /// Image retention / delete queue (checkOldImages).
    pub fn check_old_images(&mut self) {
        self.upload_deleted = false;
        if self.upload_pools.is_none() {
            self.upload_pools = Some(UploadPools::new(&self.hostname));
        }
        let Some(pools) = self.upload_pools.as_ref() else {
            return;
        };
        let known_providers: HashSet<String> = self
            .tenant_providers
            .values()
            .flat_map(|list| list.iter().map(|p| p.canonical_name().to_string()))
            .collect();
        let mut keep: HashSet<String> = HashSet::new();
        for list in self.tenant_providers.values() {
            for provider in list {
                for image in provider.images().values() {
                    if image.image_type != "zuul" {
                        continue;
                    }
                    let uploads: Vec<&model::ImageUpload> = self
                        .api
                        .uploads
                        .values()
                        .filter(|u| u.canonical_name() == image.canonical_name)
                        .collect();
                    let mut valid: Vec<&model::ImageUpload> = uploads
                        .iter()
                        .copied()
                        .filter(|u| {
                            u.state() == upload_state::READY
                                && u.validated()
                                && u.external_id().is_some()
                                && u.providers().iter().any(|p| p == provider.canonical_name())
                        })
                        .collect();
                    valid.sort_by(|a, b| {
                        a.timestamp()
                            .partial_cmp(&b.timestamp())
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                    let retain = image.retain_count.max(0) as usize;
                    let newest: Vec<&model::ImageUpload> =
                        valid.iter().rev().take(retain).copied().rev().collect();
                    for u in &newest {
                        keep.insert(u.uuid());
                    }
                    let oldest_ts = newest.first().map(|u| u.timestamp()).unwrap_or(0.0);
                    let mut unvalidated: Vec<&model::ImageUpload> = uploads
                        .iter()
                        .copied()
                        .filter(|u| {
                            u.timestamp() > oldest_ts
                                && u.state() == upload_state::READY
                                && !u.validated()
                                && u.external_id().is_some()
                                && u.providers().iter().any(|p| p == provider.canonical_name())
                        })
                        .collect();
                    unvalidated.sort_by(|a, b| {
                        a.timestamp()
                            .partial_cmp(&b.timestamp())
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                    for u in unvalidated.iter().rev().take(retain) {
                        keep.insert(u.uuid());
                    }
                    for u in &uploads {
                        if u.timestamp() > oldest_ts
                            && (u.state() != upload_state::READY || u.external_id().is_none())
                            && u.providers().iter().any(|p| p == provider.canonical_name())
                        {
                            keep.insert(u.uuid());
                        }
                    }
                }
            }
        }
        let to_delete: Vec<String> = self
            .api
            .uploads
            .values()
            .filter(|u| {
                if keep.contains(&u.uuid()) {
                    return false;
                }
                if set_disjoint(&u.providers(), &known_providers) {
                    return false;
                }
                true
            })
            .map(|u| u.uuid())
            .collect();
        for uuid in to_delete {
            if Self::queue_contains(&self.upload_uuids_in_delete_queue, &uuid) {
                continue;
            }
            Self::queue_insert(&self.upload_uuids_in_delete_queue, uuid.clone());
            let job = PlannedJob {
                kind: "delete".into(),
                upload_uuid: uuid,
                source_external_id: None,
                source_provider: None,
                dependents: Vec::new(),
            };
            let delete_q = self.upload_uuids_in_delete_queue.clone();
            pools.submit_upload(job, move |job| {
                Self::run_upload_delete(&job, &delete_q);
            });
        }
        // Delete ImageBuildArtifacts with no uploads left (server.py
        // checkOldImages tail). qcow2/etc. never uploaded, or orphans
        // after DeleteJob, must go so getArtifactsForImage settles.
        let latest_upload_ts = self
            .api
            .uploads
            .values()
            .map(|u| u.timestamp())
            .fold(0.0_f64, f64::max);
        let mut uploads_by_artifact: HashMap<String, usize> = HashMap::new();
        for u in self.api.uploads.values() {
            *uploads_by_artifact
                .entry(u.artifact_uuid())
                .or_insert(0) += 1;
        }
        let orphan_artifacts: Vec<String> = self
            .api
            .artifacts
            .values()
            .filter(|a| {
                let state = a.state().unwrap_or_default();
                (state == "ready" || state == "deleting")
                    && latest_upload_ts > 0.0
                    && a.timestamp() <= latest_upload_ts
                    && uploads_by_artifact.get(&a.uuid()).copied().unwrap_or(0) == 0
            })
            .map(|a| a.uuid())
            .collect();
        for uuid in orphan_artifacts {
            let _ = cloud::invoke(
                "upload.delete_artifact",
                &json!({ "artifact_uuid": uuid }),
            );
        }
    }

    /// Pending uploads: one UploadJob per artifact on the 1-worker upload
    /// pool (server.py checkMissingUploads). Planning stays in Rust; the
    /// pool worker calls upload.artifact_job so UploadJob.run patches apply.
    pub fn check_missing_uploads(&mut self) {
        self.image_updated = false;
        if self.upload_pools.is_none() {
            self.upload_pools = Some(UploadPools::new(&self.hostname));
        }
        let Some(pools) = self.upload_pools.as_ref() else {
            return;
        };
        // Crashed uploads (UPLOADING, unlocked): reset via Python ZKObject
        // lock/refresh/double-check (server.py checkMissingUploads). Never
        // write_upload here — that bypasses the advisory lock and races the
        // UploadJob that still holds it (BadVersion on finalize).
        let crashed: Vec<String> = self
            .api
            .uploads
            .values()
            .filter(|u| u.state() == upload_state::UPLOADING && !u.is_locked)
            .map(|u| u.uuid())
            .collect();
        for uuid in crashed {
            let _ = cloud::invoke(
                "upload.reset_crashed",
                &json!({ "upload_uuid": uuid }),
            );
        }
        let all: Vec<&model::ImageUpload> = self.api.uploads.values().collect();
        // Group PENDING uploads by artifact (one UploadJob each).
        let mut pending_by_artifact: HashMap<String, Vec<String>> = HashMap::new();
        for upload in &all {
            if Self::queue_contains(&self.upload_uuids_in_queue, &upload.uuid()) {
                continue;
            }
            if upload.state() != upload_state::PENDING {
                continue;
            }
            pending_by_artifact
                .entry(upload.artifact_uuid())
                .or_default()
                .push(upload.uuid());
        }
        if pending_by_artifact.is_empty() {
            return;
        }
        let queue = self.upload_uuids_in_queue.clone();
        for (artifact_uuid, upload_uuids) in pending_by_artifact {
            // Mark queued before submit so a second checkMissingUploads
            // under _test_lock sees them (server.py upload_uuids_in_queue).
            for uuid in &upload_uuids {
                Self::queue_insert(&queue, uuid.clone());
            }
            // Build planner args for this artifact's uploads.
            let artifact_all: Vec<&model::ImageUpload> = all
                .iter()
                .copied()
                .filter(|u| u.artifact_uuid() == artifact_uuid)
                .collect();
            let mut pending_refs: Vec<&model::ImageUpload> = Vec::new();
            let mut args: HashMap<String, UploadArgs> = HashMap::new();
            for upload in &artifact_all {
                if upload_uuids.iter().any(|u| u == &upload.uuid()) {
                    pending_refs.push(*upload);
                }
                let provider_name = upload.providers().first().cloned().unwrap_or_default();
                let artifact = self.api.artifacts.get(&upload.artifact_uuid());
                let url = artifact
                    .and_then(|a| a.fields.get("url").and_then(|v| v.as_str()))
                    .unwrap_or("")
                    .to_string();
                let image_format = artifact
                    .and_then(|a| a.fields.get("format").and_then(|v| v.as_str()))
                    .unwrap_or("")
                    .to_string();
                let mut methods = Vec::new();
                let mut driver = String::new();
                let mut region = None;
                if let Some(provider) = self.provider_by_any_cname(&provider_name) {
                    driver = provider.driver_name().to_string();
                    region = provider.region().map(|s| s.to_string());
                    // Match provider_image.upload_methods (schema default
                    // ['copy','import','upload']; layouts may restrict).
                    let image_cname = upload.canonical_name();
                    let allowed = provider
                        .images()
                        .values()
                        .find(|img| {
                            img.canonical_name == image_cname || img.name == image_cname
                        })
                        .and_then(|img| {
                            img.raw
                                .get("upload-methods")
                                .or_else(|| img.raw.get("upload_methods"))
                                .and_then(|v| v.as_array())
                                .map(|arr| {
                                    arr.iter()
                                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                                        .collect::<Vec<_>>()
                                })
                        })
                        .unwrap_or_else(|| {
                            vec!["copy".into(), "import".into(), "upload".into()]
                        });
                    if allowed.iter().any(|m| m == "import") && provider.import_job(&url) {
                        methods.push("import".into());
                    }
                    if allowed.iter().any(|m| m == "copy")
                        && matches!(driver.as_str(), "aws" | "openstack")
                    {
                        methods.push("copy".into());
                    }
                    if allowed.iter().any(|m| m == "upload") && provider.upload_job() {
                        methods.push("upload".into());
                    }
                } else {
                    methods.push("upload".into());
                }
                args.insert(
                    upload.uuid(),
                    UploadArgs {
                        provider: provider_name,
                        image_name: upload.canonical_name(),
                        url,
                        image_format,
                        methods,
                        driver,
                        region,
                    },
                );
            }
            let mut planner = UploadPlanner::new(pending_refs, artifact_all, args);
            planner.plan();
            let jobs_map = planner.jobs.clone();
            let mut planned: Vec<Value> = Vec::new();
            for mut job in planner.import_jobs {
                if let Some(full) = jobs_map.get(&job.upload_uuid) {
                    job.dependents = full.dependents.clone();
                }
                planned.push(Self::planned_job_json(&job));
            }
            for job in planner.copy_jobs {
                planned.push(Self::planned_job_json(&job));
            }
            for mut job in planner.upload_jobs {
                if let Some(full) = jobs_map.get(&job.upload_uuid) {
                    job.dependents = full.dependents.clone();
                }
                planned.push(Self::planned_job_json(&job));
            }
            let q = queue.clone();
            let art = artifact_uuid.clone();
            let uuids = upload_uuids.clone();
            let placeholder = PlannedJob {
                kind: "artifact".into(),
                upload_uuid: artifact_uuid,
                source_external_id: None,
                source_provider: None,
                dependents: Vec::new(),
            };
            pools.submit_upload(placeholder, move |_job| {
                let _ = cloud::invoke(
                    "upload.artifact_job",
                    &json!({
                        "artifact_uuid": art,
                        "upload_uuids": uuids,
                        "planned": planned,
                    }),
                );
                for uuid in &uuids {
                    Self::queue_discard(&q, uuid);
                }
            });
        }
    }

    fn planned_job_json(job: &PlannedJob) -> Value {
        json!({
            "kind": job.kind,
            "upload_uuid": job.upload_uuid,
            "source_external_id": job.source_external_id,
            "source_provider": job.source_provider,
            "dependents": job.dependents,
        })
    }

    fn run_upload_job(job: &PlannedJob, queue: &Arc<Mutex<HashSet<String>>>) {
        if !cloud::enabled() {
            Self::queue_discard(queue, &job.upload_uuid);
            return;
        }
        let kind = if job.kind == "copy-dependent" {
            "copy"
        } else {
            job.kind.as_str()
        };
        let mut payload = json!({
            "kind": kind,
            "upload_uuid": job.upload_uuid,
            "dependents": job.dependents,
        });
        if let Some(src) = &job.source_external_id {
            payload
                .as_object_mut()
                .unwrap()
                .insert("source_external_id".into(), json!(src));
        }
        match cloud::invoke("upload.job_run", &payload) {
            Ok(_) => {}
            Err(e) => {
                // Queue discard happens in the adapter finally; still clear
                // the Rust set so a failed invoke cannot stick forever.
                let _ = e;
            }
        }
        Self::queue_discard(queue, &job.upload_uuid);
    }

    fn run_upload_delete(job: &PlannedJob, queue: &Arc<Mutex<HashSet<String>>>) {
        if cloud::enabled() {
            let _ = cloud::invoke(
                "upload.delete",
                &json!({ "upload_uuid": job.upload_uuid }),
            );
        }
        Self::queue_discard(queue, &job.upload_uuid);
    }

    /// Snapshot nodes. In the PyO3 harness, drive Python
    /// getSnapshotStateMachine.advance (snapshot.sm_advance) so test
    /// MonkeyPatches apply. The shipped binary uses Rust AwsSnapshot.
    pub fn process_snapshots(&mut self) {
        let ids: Vec<String> = self
            .api
            .nodes
            .values()
            .filter(|n| n.state() == node_state::SNAPSHOT)
            .map(|n| n.uuid())
            .collect();
        for id in ids {
            if cloud::enabled() {
                let _ = cloud::invoke(
                    "snapshot.sm_advance",
                    &json!({ "node_uuid": id }),
                );
                continue;
            }
            // Binary path: Rust AwsSnapshot + live endpoint ops.
            let mut sm = AwsSnapshot::new();
            sm.node_uuid = id.clone();
            sm.start_time = self.now;
            let _ = sm.advance();
        }
    }

    fn provider_by_any_cname(&self, cname: &str) -> Option<Arc<dyn Provider>> {
        for list in self.tenant_providers.values() {
            for p in list {
                if p.canonical_name() == cname {
                    return Some(p.clone());
                }
            }
        }
        None
    }

    /// Layout / image maintenance path from `_run`.
    fn maintain_images_and_uploads(&mut self) {
        if self.layout_updated || self.image_updated {
            self.check_old_images();
            self.check_missing_uploads();
            self.layout_updated = false;
            self.image_updated = false;
        } else if self.upload_deleted {
            self.check_old_images();
        }
        self.process_snapshots();
    }
}

pub fn normalize_statsd_name(name: &str) -> String {
    // zuul.lib.statsd.normalize_statsd_name, then the launcher rewrite
    // of '/' (quote_plus leaves "%2F" in place; a raw slash becomes "_").
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        if !ch.is_ascii() {
            out.push('?');
        } else if ch == '.' || ch == ':' || ch == '/' {
            out.push('_');
        } else {
            out.push(ch);
        }
    }
    out
}

const NODE_STATES_FOR_STATS: &[&str] = &[
    node_state::REQUESTED,
    node_state::BUILDING,
    node_state::REBUILDING,
    node_state::READY,
    node_state::FAILED,
    node_state::TEMPFAILED,
    node_state::IN_USE,
    node_state::USED,
    node_state::OUTDATED,
    node_state::REGISTERING,
    node_state::DEREGISTER,
    node_state::HOLD,
    node_state::SNAPSHOT,
    node_state::SLOT_HOST,
];

const REQUEST_STATES_FOR_STATS: &[&str] = &[
    request_state::REQUESTED,
    request_state::ACCEPTED,
    request_state::FULFILLED,
    request_state::FAILED,
    request_state::TEST_HOLD,
];

/// server.py Launcher._runStats gauge names.
fn emit_run_stats(snap: &StatsSnapshot) -> Vec<StatEvent> {
    let mut events = Vec::new();
    let mut gauge = |name: String, value: f64| {
        events.push(StatEvent {
            kind: "g".into(),
            name,
            value,
        });
    };

    let mut nodes: HashMap<&str, i64> = NODE_STATES_FOR_STATS.iter().map(|s| (*s, 0)).collect();
    let mut provider_nodes: HashMap<String, HashMap<&str, i64>> = HashMap::new();
    let mut provider_label_nodes: HashMap<String, HashMap<String, HashMap<&str, i64>>> =
        HashMap::new();
    for (state, provider, label) in &snap.nodes {
        *nodes.entry(state.as_str()).or_insert(0) += 1;
        let pn = provider_nodes.entry(provider.clone()).or_insert_with(|| {
            NODE_STATES_FOR_STATS.iter().map(|s| (*s, 0)).collect()
        });
        *pn.entry(state.as_str()).or_insert(0) += 1;
        let pln = provider_label_nodes
            .entry(provider.clone())
            .or_default()
            .entry(label.clone())
            .or_insert_with(|| NODE_STATES_FOR_STATS.iter().map(|s| (*s, 0)).collect());
        *pln.entry(state.as_str()).or_insert(0) += 1;
    }

    let mut requests: HashMap<&str, i64> =
        REQUEST_STATES_FOR_STATS.iter().map(|s| (*s, 0)).collect();
    for state in &snap.requests {
        if state != request_state::TEST_HOLD {
            *requests.entry(state.as_str()).or_insert(0) += 1;
        }
    }

    let mut max_nodes_by_label: HashMap<String, i64> = HashMap::new();
    for provider in &snap.providers {
        let safe_pname = normalize_statsd_name(&provider.canonical_name);
        for (res, value) in &provider.limits {
            let safe_res = normalize_statsd_name(res);
            gauge(
                format!("zuul.provider.{safe_pname}.limit.{safe_res}"),
                *value,
            );
        }
        for (res, value) in &provider.usage {
            let safe_res = normalize_statsd_name(res);
            gauge(
                format!("zuul.provider.{safe_pname}.usage.{safe_res}"),
                *value,
            );
        }
        let empty_states: HashMap<&str, i64> =
            NODE_STATES_FOR_STATS.iter().map(|s| (*s, 0)).collect();
        let pnodes = provider_nodes
            .get(&provider.canonical_name)
            .unwrap_or(&empty_states);
        for state in NODE_STATES_FOR_STATS {
            let value = pnodes.get(state).copied().unwrap_or(0);
            gauge(
                format!("zuul.provider.{safe_pname}.nodes.state.{state}"),
                value as f64,
            );
        }
        for (label_name, label_cname, max_nodes) in &provider.labels {
            let label_nodes = provider_label_nodes
                .get(&provider.canonical_name)
                .and_then(|m| m.get(label_name));
            let safe_label = normalize_statsd_name(label_name);
            for state in NODE_STATES_FOR_STATS {
                let value = label_nodes
                    .and_then(|m| m.get(state).copied())
                    .unwrap_or(0);
                gauge(
                    format!(
                        "zuul.provider.{safe_pname}.label.{safe_label}.nodes.state.{state}"
                    ),
                    value as f64,
                );
            }
            if let Some(max) = max_nodes {
                let safe_label_cname = normalize_statsd_name(label_cname);
                max_nodes_by_label.insert(safe_label_cname, *max);
            }
        }
    }
    for (safe_label, value) in &max_nodes_by_label {
        gauge(format!("zuul.label.{safe_label}.max_nodes"), *value as f64);
    }
    for state in NODE_STATES_FOR_STATS {
        let value = nodes.get(state).copied().unwrap_or(0);
        gauge(format!("zuul.nodes.state.{state}"), value as f64);
    }
    for state in REQUEST_STATES_FOR_STATS {
        let value = requests.get(state).copied().unwrap_or(0);
        gauge(format!("zuul.nodeset_requests.state.{state}"), value as f64);
    }

    let mut uploads_by_image: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for (image_cname, endpoint, state) in &snap.uploads {
        uploads_by_image
            .entry(normalize_statsd_name(image_cname))
            .or_default()
            .push((endpoint.clone(), state.clone()));
    }
    let mut upload_states: HashMap<String, i64> = HashMap::new();
    for (image_cname, uploads) in &uploads_by_image {
        let mut by_endpoint: HashMap<String, HashMap<String, i64>> = HashMap::new();
        for (endpoint, state) in uploads {
            *by_endpoint
                .entry(endpoint.clone())
                .or_default()
                .entry(state.clone())
                .or_insert(0) += 1;
        }
        for (endpoint_name, state_counter) in by_endpoint {
            let safe_ename = normalize_statsd_name(&endpoint_name);
            for (state, count) in &state_counter {
                *upload_states.entry(state.clone()).or_insert(0) += count;
            }
            for state in UPLOAD_STATES {
                let count = state_counter.get(*state).copied().unwrap_or(0);
                gauge(
                    format!("zuul.image.{image_cname}.upload.{safe_ename}.state.{state}"),
                    count as f64,
                );
            }
        }
    }
    for (state, count) in &upload_states {
        gauge(format!("zuul.uploads.state.{state}"), *count as f64);
    }
    events
}

fn set_disjoint(providers: &[String], known: &HashSet<String>) -> bool {
    !providers.iter().any(|p| known.contains(p))
}

fn layout_same(local: Option<&LayoutState>, remote: Option<&LayoutState>) -> bool {
    match (local, remote) {
        (None, None) => true,
        (Some(left), Some(right)) => left.uuid == right.uuid,
        _ => false,
    }
}

fn layout_fingerprint(states: &HashMap<String, LayoutState>) -> String {
    let mut items: Vec<String> = states
        .iter()
        .map(|(tenant, state)| {
            format!(
                "{tenant}={}",
                state.uuid.as_deref().unwrap_or("")
            )
        })
        .collect();
    items.sort();
    items.join("\n")
}

fn hex_id() -> String {
    let mut b = [0u8; 16];
    let _ = getrandom::getrandom(&mut b);
    hex::encode(b)
}

/// Loop timing: run, sleep max(0, 1 - duration), then wait up to MIN_INTERVAL.
pub fn loop_waits(run_started: Instant) -> (Duration, Duration) {
    let elapsed = run_started.elapsed();
    let max = Duration::from_secs_f64(MAX_SLEEP);
    let sleep = if elapsed >= max { Duration::ZERO } else { max - elapsed };
    (sleep, Duration::from_secs_f64(MIN_INTERVAL))
}
