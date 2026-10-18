// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// Control flow from zuul/launcher/server.py. Timing constants are unchanged.

use crate::cloud;
use crate::jsonutil;
use crate::layout::{self, LayoutState};
use crate::mmh3;
use crate::model::{self, node_state, request_state, NodesetRequest, ProviderNode, QuotaInformation};
use crate::nodescan::{NodescanRequest, NodescanWorker};
use crate::provider::{AdvanceError, Provider, StateMachine};
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
        // Unit tests patch Launcher._acceptRequest and launcher_score.
        // Drive Python acceptance and _checkRequest so retries keep
        // every label in a request on the same provider.
        let python_accept = cloud::enabled()
            && cloud::invoke("accept_requested", &json!({})).is_ok();
        if python_accept {
            return;
        }
        let unassigned = self.unassigned_nodes();
        let mut pending: HashSet<(String, String)> = HashSet::new();
        let ids = self.matching_ids("matching_requests");
        for id in ids {
            let Some(req) = self.api.requests.get(&id).cloned() else { continue };
            if model::is_final_request_state(&req.state()) && req.event_state() == model::event_state::COMPLETE {
                continue;
            }
            if req.state() == request_state::REQUESTED {
                self.process_nodeset_request(&req, &unassigned, &mut pending);
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
        let failed: Vec<usize> = ids
            .iter()
            .enumerate()
            .filter(|(_, id)| {
                self.api
                    .nodes
                    .get(*id)
                    .map(|n| model::is_failed_node_state(&n.state()))
                    .unwrap_or(false)
            })
            .map(|(i, _)| i)
            .collect();
        if !failed.is_empty() {
            let mut req = request.clone();
            let label_providers = match self.select_providers(&req, unassigned, &mut Vec::new()) {
                Ok(v) => v,
                Err(err) => {
                    if err.contains("No provider") {
                        self.finish_request(&mut req, request_state::FAILED);
                    }
                    return;
                }
            };
            for i in failed {
                let Some((label_name, provider_cname)) = label_providers.get(i) else {
                    continue;
                };
                let Some(old) = self.api.nodes.get(&ids[i]).cloned() else {
                    continue;
                };
                let add_failed = if old.state() == node_state::FAILED {
                    Some(old.provider())
                } else {
                    None
                };
                let Some(provider) = self.provider_by_cname(&req.tenant_name(), provider_cname) else {
                    continue;
                };
                if provider.driver_name() == "static" {
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
                    match self.request_node(provider.as_ref(), &label, Some(&req)) {
                        Ok(created) => created,
                        Err(_) => continue,
                    }
                };
                self.api.delete_assignment(&ids[i]).ok();
                req.update_provider_node(
                    i,
                    &new_id,
                    label.executor_zone.as_deref(),
                    add_failed.as_deref(),
                );
            }
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
            let node_id = self.request_node(provider.as_ref(), &label, Some(&req))?;
            req.add_provider_node(&node_id, label.executor_zone.as_deref());
        }
        if req.provider_node_data().len() == req.labels().len() && !req.labels().is_empty() {
            req.set_state(request_state::ACCEPTED);
        }
        self.api.write_request(&req).map_err(|e| e.to_string())?;
        self.messages.append(&mut messages);
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
                    }
                }
            }
            let mut ideal = Vec::new();
            let mut usable = Vec::new();
            for p in &providers {
                let Some(label) = p.labels().get(label_name) else { continue };
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
        let main = request
            .preferred_provider()
            .filter(|n| order.iter().any(|p| p == n))
            .or_else(|| all_ordered.first().cloned());
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
        if cloud::enabled() {
            if let Ok(value) = cloud::invoke(
                "quota_percentage",
                &json!({"provider": provider.canonical_name()}),
            ) {
                if let Some(pct) = value.get("pct").and_then(|v| v.as_f64()) {
                    return pct;
                }
            }
        }
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

    fn provider_quota(&self, provider: &dyn Provider) -> QuotaInformation {
        if cloud::enabled() {
            if let Ok(value) = cloud::invoke(
                "provider_quota",
                &json!({"provider": provider.canonical_name()}),
            ) {
                return cloud::quota_from_payload(&value);
            }
        }
        if let Some((q, ts)) = self.quota_cache.get(provider.canonical_name()) {
            if self.now - ts < MAX_QUOTA_AGE {
                return q.copy();
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
        if cloud::enabled() {
            if let Ok(value) = cloud::invoke(
                "quota_for_label",
                &json!({
                    "provider": provider.canonical_name(),
                    "label": label.name,
                    "tenant": tenant,
                    "include_usage": include_usage,
                }),
            ) {
                if let Some(ok) = value.get("ok").and_then(|v| v.as_bool()) {
                    return ok;
                }
            }
        }
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
        total.non_negative()
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
        if cloud::enabled() {
            if let Ok(value) = cloud::invoke(
                "quota_for_node",
                &json!({
                    "provider": provider.canonical_name(),
                    "uuid": node.uuid(),
                }),
            ) {
                if let Some(ok) = value.get("ok").and_then(|v| v.as_bool()) {
                    return ok;
                }
            }
        }
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
    ) -> Result<String, String> {
        let id = hex_id();
        let quota = self
            .cloud_label_quota(provider, label)
            .unwrap_or_else(|| provider.quota_for_label(label));
        let mut fields = Map::new();
        let now = self.now;
        let set = |m: &mut Map<String, Value>, k: &str, v: Value| {
            m.insert(k.into(), v);
        };
        set(&mut fields, "uuid", json!(id));
        set(&mut fields, "state", json!(node_state::REQUESTED));
        set(&mut fields, "state_time", json!(now));
        set(&mut fields, "request_time", json!(now));
        set(&mut fields, "label", json!(label.name));
        set(&mut fields, "label_config_hash", json!(label.config_hash));
        set(&mut fields, "label_aliases", json!(label.aliases));
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
            attr("connection_port").unwrap_or_else(|| json!(label.connection_port)),
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
            attr("username").unwrap_or_else(|| {
                label.username.clone().map(|s| json!(s)).unwrap_or(Value::Null)
            }),
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
                if slot == 0 {
                    if let Some(req) = request {
                        let assign = json!({
                            "request_id": req.uuid(),
                            "min_request_version": req.zk_version.unwrap_or(0) + 1,
                            "tenant_name": req.tenant_name(),
                        });
                        let _ = self.api.write_assignment(sub_id, &assign);
                    }
                }
            }
            return Ok(returned);
        }
        if let Some(req) = request {
            node.tenant_name = Some(req.tenant_name());
            node.request_id = Some(req.uuid());
        }
        self.api.write_node(&node).map_err(|e| e.to_string())?;
        if let Some(req) = request {
            let assign = json!({
                "request_id": req.uuid(),
                "min_request_version": req.zk_version.unwrap_or(0) + 1,
                "tenant_name": req.tenant_name(),
            });
            let _ = self.api.write_assignment(&id, &assign);
        }
        Ok(id)
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
            let _ = self.request_node(provider.as_ref(), &label, None);
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

    pub fn emit_stats_gauges(&mut self, leaked: i64, unmanaged: i64) {
        self.stats.push(StatEvent { kind: "g".into(), name: "zuul.nodes.leaked".into(), value: leaked as f64 });
        self.stats.push(StatEvent { kind: "g".into(), name: "zuul.nodes.unmanaged".into(), value: unmanaged as f64 });
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
