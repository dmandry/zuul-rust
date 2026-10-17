// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// Control flow from zuul/launcher/server.py. Timing constants are unchanged.

use crate::jsonutil;
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
    create_sms: HashMap<String, Box<dyn StateMachine>>,
    delete_sms: HashMap<String, Box<dyn StateMachine>>,
    quota_cache: HashMap<String, (QuotaInformation, f64)>,
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
            create_sms: HashMap::new(),
            delete_sms: HashMap::new(),
            quota_cache: HashMap::new(),
            wake: Arc::new(Mutex::new(false)),
            now: model::now_secs(),
        }
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
        if self.layout_dirty {
            let updated = self.update_tenant_providers();
            if updated {
                if self.state != "running" {
                    self.state = "running".into();
                }
                self.layout_dirty = false;
                self.layout_updated = true;
            }
        }
        self.process_requests();
        self.process_nodes();
        self.process_min_ready();
    }

    pub fn update_tenant_providers(&mut self) -> bool {
        // Tests install providers directly. A real reload that finds any
        // tenant counts as the first successful update.
        !self.tenant_providers.is_empty() || self.layout_updated
    }

    pub fn sleep_after(&self, duration: Duration) -> Duration {
        let max = Duration::from_secs_f64(MAX_SLEEP);
        if duration >= max {
            Duration::from_secs(0)
        } else {
            max - duration
        }
    }

    pub fn process_requests(&mut self) {
        let unassigned = self.unassigned_nodes();
        let mut pending: HashSet<(String, String)> = HashSet::new();
        let ids = self.api.get_matching_requests();
        for id in ids {
            let Some(req) = self.api.requests.get(&id).cloned() else { continue };
            if req.state() == request_state::REQUESTED || req.state() == request_state::ACCEPTED {
                self.process_nodeset_request(&req, &unassigned, &mut pending);
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
            let _ = self.accept_request(request, unassigned, pending);
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
        _tenant: &str,
        provider: &dyn Provider,
        label: &crate::provider::Label,
        include_usage: bool,
    ) -> bool {
        let mut total = if include_usage {
            let Some(mut total) = self.provider_quota_available(provider, true) else {
                return false;
            };
            let used = self.api.get_quota(provider.canonical_name(), true);
            total.subtract(&used);
            total
        } else {
            let mut total = provider.endpoint().limits.copy();
            if !provider.resource_limits().is_empty() {
                let limits = QuotaInformation { default: f64::INFINITY, quota: provider.resource_limits().clone() };
                total.min_with(&limits);
                let _ = limits;
            }
            total
        };
        let label_quota = provider.quota_for_label(label);
        total.subtract(&label_quota);
        total.non_negative()
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
    ) -> Result<String, String> {
        let id = hex_id();
        let quota = provider.quota_for_label(label);
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
        set(&mut fields, "tags", json!({}));
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
        set(&mut fields, "connection_port", json!(label.connection_port));
        set(&mut fields, "connection_type", label.connection_type.clone().map(|s| json!(s)).unwrap_or(Value::Null));
        set(&mut fields, "kubernetes_connection", json!({}));
        set(&mut fields, "username", label.username.clone().map(|s| json!(s)).unwrap_or(Value::Null));
        set(&mut fields, "python_path", label.python_path.clone().map(|s| json!(s)).unwrap_or(json!("auto")));
        set(&mut fields, "shell_type", Value::Null);
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
        let ids = self.api.get_matching_nodes();
        for id in ids {
            if let Err(e) = self.process_provider_node(&id) {
                self.messages.push(format!("node {id}: {e}"));
            }
        }
    }

    pub fn process_provider_node(&mut self, node_id: &str) -> Result<(), String> {
        let Some(node) = self.api.nodes.get(node_id).cloned() else {
            return Ok(());
        };
        let request = node
            .request_id
            .as_ref()
            .and_then(|id| self.api.requests.get(id).cloned());
        if (request.is_some() || node.request_id.is_none()) && model::is_create_state(&node.state()) {
            match self.check_node_create(node_id) {
                Ok(()) => {}
                Err(AdvanceError::Quota(e)) => {
                    self.set_node_state(node_id, node_state::TEMPFAILED);
                    self.wake();
                    self.messages.push(format!("tempfail {e}"));
                }
                Err(e) => {
                    self.set_node_state(node_id, node_state::FAILED);
                    self.wake();
                    self.messages.push(format!("fail {e}"));
                }
            }
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

    fn check_node_create(&mut self, node_id: &str) -> Result<(), AdvanceError> {
        let node = self.api.nodes.get(node_id).cloned().ok_or_else(|| AdvanceError::Other("missing".into()))?;
        if node.driver_id == "subnode" {
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
        let start_time = self.create_sms.get(node_id).unwrap().start_time();
        if sm_state != "start" {
            if let Some(n) = self.api.nodes.get(node_id) {
                if n.state() == node_state::REQUESTED {
                    self.set_node_state(node_id, node_state::BUILDING);
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
        let state = if has_sub { node_state::SLOT_HOST } else { node_state::READY };
        self.set_node_state(node_id, state);
        self.nodescan.remove(node_id);
        self.report_launch_stats(node_id);
        self.wake();
        Ok(())
    }

    fn check_nodescan(&mut self, node_id: &str) -> Result<bool, AdvanceError> {
        let node = self.api.nodes.get(node_id).cloned().unwrap();
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
        if self.now - start > DELETE_TIMEOUT {
            self.delete_sms.get_mut(node_id).map(|s| {
                // fail() in Python marks complete.
                let _ = s.advance();
            });
        }
        if let Some(sm) = self.delete_sms.get_mut(node_id) {
            if !sm.complete() {
                sm.set_now(self.now);
                let _ = sm.advance();
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
        self.api.delete_node(node_id).ok();
        self.delete_sms.remove(node_id);
        Ok(())
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
        let candidates = self.api.launchers();
        let names: Vec<String> = candidates.iter().map(|c| c.hostname.clone()).collect();
        let mut out = Vec::new();
        let mut seen_hashes: HashMap<String, Vec<String>> = HashMap::new();
        for node in self.api.nodes.values() {
            if node.request_id.is_some() {
                continue;
            }
            seen_hashes
                .entry(node.label())
                .or_default()
                .push(node.label_config_hash());
        }
        for (tenant, providers) in &self.tenant_providers {
            let mut groups: HashMap<String, Vec<crate::provider::Label>> = HashMap::new();
            for provider in providers {
                for label in provider.labels().values() {
                    if label.min_ready.unwrap_or(0) <= 0 {
                        continue;
                    }
                    if !self.has_highest_min_ready_score(&label.canonical_name, &names, &candidates) {
                        continue;
                    }
                    groups.entry(label.name.clone()).or_default().push(label.clone());
                }
            }
            for (label_name, labels) in groups {
                let valid: HashSet<String> = labels.iter().map(|l| l.config_hash.clone()).collect();
                let have = seen_hashes
                    .get(&label_name)
                    .map(|hs| hs.iter().filter(|h| valid.contains(*h)).count())
                    .unwrap_or(0);
                let want = labels[0].min_ready.unwrap_or(0).max(0) as usize;
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
                    out.push((tenant.clone(), label_name.clone(), chosen.clone()));
                    seen_hashes.entry(label_name.clone()).or_default().push(labels[0].config_hash.clone());
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
    name.replace(['.', ':', ' '], "_")
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
