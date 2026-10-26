// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// States and JSON keys copied from zuul/model.py (14.3.0). Unknown keys are
// kept on the decoded value and omitted when a 14.3.0 serializer writes.

use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

use crate::jsonutil;

pub const MODEL_API: i64 = 38;
pub const ZUUL_VERSION: &str = "14.3.0";

pub mod node_state {
    pub const REQUESTED: &str = "requested";
    pub const BUILDING: &str = "building";
    pub const REBUILDING: &str = "rebuilding";
    pub const READY: &str = "ready";
    pub const FAILED: &str = "failed";
    pub const TEMPFAILED: &str = "tempfailed";
    pub const IN_USE: &str = "in-use";
    pub const USED: &str = "used";
    pub const OUTDATED: &str = "outdated";
    pub const REGISTERING: &str = "registering";
    pub const DEREGISTER: &str = "deregister";
    pub const HOLD: &str = "hold";
    pub const SNAPSHOT: &str = "snapshot";
    pub const SLOT_HOST: &str = "slot-host";
}

pub fn assignable_rank(state: &str) -> Option<i32> {
    match state {
        node_state::READY => Some(0),
        node_state::BUILDING => Some(1),
        node_state::REQUESTED => Some(2),
        _ => None,
    }
}

pub fn is_allocated_state(state: &str) -> bool {
    matches!(
        state,
        node_state::BUILDING
            | node_state::REBUILDING
            | node_state::READY
            | node_state::IN_USE
            | node_state::USED
            | node_state::OUTDATED
            | node_state::REGISTERING
            | node_state::DEREGISTER
            | node_state::HOLD
            | node_state::SNAPSHOT
            | node_state::SLOT_HOST
    )
}

pub fn is_requested_quota_state(state: &str) -> bool {
    is_allocated_state(state) || state == node_state::REQUESTED
}

pub fn is_final_node_state(state: &str) -> bool {
    matches!(
        state,
        node_state::READY | node_state::FAILED | node_state::SLOT_HOST
    )
}

pub fn is_failed_node_state(state: &str) -> bool {
    matches!(state, node_state::TEMPFAILED | node_state::FAILED)
}

pub fn is_create_state(state: &str) -> bool {
    matches!(
        state,
        node_state::REQUESTED
            | node_state::REBUILDING
            | node_state::REGISTERING
            | node_state::BUILDING
    )
}

pub fn is_launcher_state(state: &str) -> bool {
    matches!(
        state,
        node_state::REQUESTED
            | node_state::REBUILDING
            | node_state::BUILDING
            | node_state::FAILED
            | node_state::USED
            | node_state::OUTDATED
            | node_state::REGISTERING
            | node_state::DEREGISTER
    )
}

pub mod request_state {
    pub const REQUESTED: &str = "requested";
    pub const ACCEPTED: &str = "accepted";
    pub const FULFILLED: &str = "fulfilled";
    pub const FAILED: &str = "failed";
    pub const TEST_HOLD: &str = "test-hold";
}

pub fn is_final_request_state(state: &str) -> bool {
    matches!(
        state,
        request_state::FULFILLED | request_state::FAILED
    )
}

pub mod event_state {
    pub const PENDING: i64 = 0;
    pub const COMPLETE: i64 = 1;
}

pub mod upload_state {
    pub const READY: &str = "ready";
    pub const DELETING: &str = "deleting";
    pub const PENDING: &str = "pending";
    pub const UPLOADING: &str = "uploading";
    pub const FAILED: &str = "failed";
}

pub const UPLOAD_STATES: &[&str] = &[
    upload_state::READY,
    upload_state::DELETING,
    upload_state::PENDING,
    upload_state::UPLOADING,
    upload_state::FAILED,
];

#[derive(Clone, Debug)]
pub struct QuotaInformation {
    pub default: f64,
    pub quota: BTreeMap<String, f64>,
}

impl Default for QuotaInformation {
    fn default() -> Self {
        Self {
            default: 0.0,
            quota: BTreeMap::new(),
        }
    }
}

impl QuotaInformation {
    pub fn new(default: f64, quota: BTreeMap<String, f64>) -> Self {
        Self { default, quota }
    }

    pub fn from_value(v: &Value) -> Self {
        let mut q = QuotaInformation::default();
        if let Some(obj) = v.as_object() {
            for (k, val) in obj {
                if let Some(n) = val.as_f64() {
                    q.quota.insert(k.clone(), n);
                } else if let Some(n) = val.as_i64() {
                    q.quota.insert(k.clone(), n as f64);
                }
            }
        }
        q
    }

    pub fn get_resources(&self) -> Value {
        let mut m = Map::new();
        for (k, v) in &self.quota {
            m.insert(k.clone(), number_like(*v));
        }
        Value::Object(m)
    }

    pub fn copy(&self) -> Self {
        self.clone()
    }

    fn ensure_keys(&mut self, other: &QuotaInformation) {
        for resource in other.quota.keys() {
            self.quota.entry(resource.clone()).or_insert(self.default);
        }
    }

    pub fn add(&mut self, other: &QuotaInformation) {
        self.ensure_keys(other);
        let keys: Vec<String> = self.quota.keys().cloned().collect();
        for resource in keys {
            let other_value = other.quota.get(&resource).copied().unwrap_or(other.default);
            *self.quota.get_mut(&resource).unwrap() += other_value;
        }
    }

    pub fn subtract(&mut self, other: &QuotaInformation) {
        self.ensure_keys(other);
        let keys: Vec<String> = self.quota.keys().cloned().collect();
        for resource in keys {
            let other_value = other.quota.get(&resource).copied().unwrap_or(other.default);
            *self.quota.get_mut(&resource).unwrap() -= other_value;
        }
    }

    pub fn min_with(&mut self, other: &QuotaInformation) {
        for (resource, theirs) in &other.quota {
            let ours = self.quota.get(resource).copied().unwrap_or(self.default);
            self.quota.insert(resource.clone(), ours.min(*theirs));
        }
    }

    /// Raise each resource to `other`'s value when that is larger.
    /// Missing keys count as zero so a source that recorded `instances`
    /// still counts when this side only has cores/ram.
    pub fn max_cover(&mut self, other: &QuotaInformation) {
        for (resource, theirs) in &other.quota {
            let ours = self.quota.get(resource).copied().unwrap_or(0.0);
            if *theirs > ours {
                self.quota.insert(resource.clone(), *theirs);
            }
        }
    }

    pub fn non_negative(&self) -> bool {
        self.quota.values().all(|v| *v >= 0.0)
    }
}

fn number_like(v: f64) -> Value {
    if v.is_finite() && v.fract() == 0.0 && v.abs() < (i64::MAX as f64) {
        json!(v as i64)
    } else if v.is_finite() {
        json!(v)
    } else if v.is_nan() {
        Value::String("NaN".into())
    } else if v.is_sign_positive() {
        Value::String("Infinity".into())
    } else {
        Value::String("-Infinity".into())
    }
}

#[derive(Clone, Debug)]
pub struct NodesetRequest {
    pub fields: Map<String, Value>,
    pub extra: Map<String, Value>,
    pub is_locked: bool,
    pub lock_holder: Option<String>,
    pub has_lock: bool,
    pub zk_version: Option<i64>,
    pub revision_priority: Option<i64>,
    pub lscores: Option<BTreeMap<u32, String>>,
}

impl NodesetRequest {
    pub fn from_value(v: Value) -> Self {
        let obj = v.as_object().cloned().unwrap_or_default();
        let mut fields = Map::new();
        let mut extra = Map::new();
        for (k, val) in obj {
            if REQUEST_KEYS.iter().any(|n| *n == k) {
                fields.insert(k, val);
            } else {
                extra.insert(k, val);
            }
        }
        Self {
            fields,
            extra,
            is_locked: false,
            lock_holder: None,
            has_lock: false,
            zk_version: None,
            revision_priority: None,
            lscores: None,
        }
    }

    pub fn state(&self) -> String {
        self.fields
            .get("state")
            .and_then(|v| v.as_str())
            .unwrap_or(request_state::REQUESTED)
            .to_string()
    }

    pub fn set_state(&mut self, state: &str) {
        self.fields.insert("state".into(), json!(state));
    }

    pub fn uuid(&self) -> String {
        self.str_field("uuid")
    }

    pub fn tenant_name(&self) -> String {
        self.str_field("tenant_name")
    }

    pub fn pipeline_name(&self) -> String {
        self.str_field("pipeline_name")
    }

    pub fn buildset_uuid(&self) -> String {
        self.str_field("buildset_uuid")
    }

    pub fn event_state(&self) -> i64 {
        self.fields
            .get("event_state")
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
    }

    pub fn set_event_state(&mut self, v: i64) {
        self.fields.insert("event_state".into(), json!(v));
    }

    pub fn labels(&self) -> Vec<String> {
        self.fields
            .get("labels")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn priority(&self) -> i64 {
        self.fields.get("priority").and_then(|v| v.as_i64()).unwrap_or(0)
    }

    pub fn request_time(&self) -> f64 {
        self.fields
            .get("request_time")
            .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
            .unwrap_or(0.0)
    }

    pub fn relative_priority(&self) -> i64 {
        if let Some(p) = self.revision_priority {
            return p;
        }
        self.fields
            .get("_relative_priority")
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
    }

    pub fn hash_key(&self) -> String {
        self.fields
            .get("hash_key")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| self.uuid())
    }

    pub fn image_upload_uuid(&self) -> Option<String> {
        self.opt_str("image_upload_uuid")
    }

    pub fn image_names(&self) -> Vec<String> {
        self.fields
            .get("image_names")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn preferred_provider(&self) -> Option<String> {
        self.opt_str("preferred_provider")
    }

    pub fn zuul_event_id(&self) -> String {
        self.str_field("zuul_event_id")
    }

    pub fn provider_node_data(&self) -> Vec<Value> {
        self.fields
            .get("provider_node_data")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
    }

    pub fn set_provider_node_data(&mut self, data: Vec<Value>) {
        self.fields
            .insert("provider_node_data".into(), Value::Array(data));
    }

    pub fn nodes(&self) -> Vec<String> {
        self.provider_node_data()
            .iter()
            .filter_map(|n| n.get("uuid").and_then(|u| u.as_str()).map(|s| s.to_string()))
            .collect()
    }

    pub fn add_provider_node(&mut self, uuid: &str, executor_zone: Option<&str>) {
        let mut data = self.provider_node_data();
        data.push(json!({
            "uuid": uuid,
            "executor_zone": executor_zone,
            "failed_providers": [],
        }));
        self.set_provider_node_data(data);
    }

    pub fn update_provider_node(
        &mut self,
        index: usize,
        uuid: &str,
        executor_zone: Option<&str>,
        add_failed_provider: Option<&str>,
    ) {
        let mut data = self.provider_node_data();
        if let Some(slot) = data.get_mut(index) {
            if let Some(obj) = slot.as_object_mut() {
                obj.insert("uuid".into(), json!(uuid));
                obj.insert(
                    "executor_zone".into(),
                    match executor_zone {
                        Some(z) => json!(z),
                        None => Value::Null,
                    },
                );
                if let Some(failed) = add_failed_provider {
                    let arr = obj
                        .entry("failed_providers")
                        .or_insert_with(|| json!([]));
                    if let Some(a) = arr.as_array_mut() {
                        a.push(json!(failed));
                    }
                }
            }
        }
        self.set_provider_node_data(data);
    }

    pub fn serialize(&self) -> Value {
        let mut m = Map::new();
        for key in REQUEST_KEYS {
            if let Some(v) = self.fields.get(*key) {
                m.insert((*key).to_string(), v.clone());
            }
        }
        // Serializer writes the live relative priority, not a stale copy.
        m.insert(
            "_relative_priority".into(),
            json!(self.relative_priority()),
        );
        Value::Object(m)
    }

    fn str_field(&self, key: &str) -> String {
        self.fields
            .get(key)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    }

    fn opt_str(&self, key: &str) -> Option<String> {
        self.fields.get(key).and_then(|v| {
            if v.is_null() {
                None
            } else {
                v.as_str().map(|s| s.to_string())
            }
        })
    }
}

const REQUEST_KEYS: &[&str] = &[
    "uuid",
    "state",
    "event_state",
    "tenant_name",
    "pipeline_name",
    "buildset_uuid",
    "job_uuid",
    "job_name",
    "labels",
    "priority",
    "request_time",
    "zuul_event_id",
    "span_info",
    "image_names",
    "image_upload_uuid",
    "preferred_provider",
    "provider_node_data",
    "_relative_priority",
    "hash_key",
];

#[derive(Clone, Debug)]
pub struct ProviderNode {
    pub driver_id: String,
    pub fields: Map<String, Value>,
    pub extra: Map<String, Value>,
    pub is_locked: bool,
    pub lock_holder: Option<String>,
    pub has_lock: bool,
    pub zk_version: Option<i64>,
    pub assignment_czxid: Option<i64>,
    pub assignment_version: Option<i64>,
    pub request_id: Option<String>,
    pub min_request_version: Option<i64>,
    pub tenant_name: Option<String>,
    pub next_state: Option<String>,
    pub next_comment: Option<String>,
    pub lscores: Option<BTreeMap<u32, String>>,
    /// Runtime-only, mirrored into create_state on serialize.
    pub create_runtime: Option<StateMachineSnap>,
    pub delete_runtime: Option<StateMachineSnap>,
    pub nodescan_complete: bool,
    pub nodescan_keys: Vec<String>,
    pub nodescan_error: Option<String>,
    pub nodescan_pending: bool,
}

#[derive(Clone, Debug)]
pub struct StateMachineSnap {
    pub state: String,
    pub start_time: f64,
    pub complete: bool,
    pub attempts: i64,
    pub image_external_id: Option<String>,
    pub extra: Map<String, Value>,
}

impl ProviderNode {
    pub fn from_parts(driver_id: String, v: Value) -> Self {
        let obj = v.as_object().cloned().unwrap_or_default();
        let known = node_keys_for(&driver_id);
        let mut fields = Map::new();
        let mut extra = Map::new();
        for (k, val) in obj {
            if known.iter().any(|n| *n == k) {
                fields.insert(k, val);
            } else {
                extra.insert(k, val);
            }
        }
        Self {
            driver_id,
            fields,
            extra,
            is_locked: false,
            lock_holder: None,
            has_lock: false,
            zk_version: None,
            assignment_czxid: None,
            min_request_version: None,
            assignment_version: None,
            request_id: None,
            tenant_name: None,
            next_state: None,
            next_comment: None,
            lscores: None,
            create_runtime: None,
            delete_runtime: None,
            nodescan_complete: false,
            nodescan_keys: Vec::new(),
            nodescan_error: None,
            nodescan_pending: false,
        }
    }

    pub fn state(&self) -> String {
        self.fields
            .get("state")
            .and_then(|v| v.as_str())
            .unwrap_or(node_state::REQUESTED)
            .to_string()
    }

    pub fn set_state(&mut self, state: &str) {
        if self.state() == state {
            return;
        }
        self.fields.insert("state".into(), json!(state));
        self.fields
            .insert("state_time".into(), json!(now_secs()));
    }

    pub fn set_state_at(&mut self, state: &str, when: f64) {
        if self.state() == state {
            return;
        }
        self.fields.insert("state".into(), json!(state));
        self.fields.insert("state_time".into(), json!(when));
    }

    pub fn uuid(&self) -> String {
        self.str_field("uuid")
    }

    pub fn label(&self) -> String {
        self.str_field("label")
    }

    pub fn label_config_hash(&self) -> String {
        self.str_field("label_config_hash")
    }

    pub fn label_aliases(&self) -> Vec<String> {
        string_list(self.fields.get("label_aliases"))
    }

    pub fn all_labels(&self) -> Vec<String> {
        let mut v = vec![self.label()];
        v.extend(self.label_aliases());
        v
    }

    pub fn provider(&self) -> String {
        self.str_field("provider")
    }

    pub fn connection_name(&self) -> String {
        self.str_field("connection_name")
    }

    pub fn request_time(&self) -> f64 {
        self.fields
            .get("request_time")
            .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
            .unwrap_or(0.0)
    }

    pub fn state_time(&self) -> f64 {
        self.fields
            .get("state_time")
            .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
            .unwrap_or(0.0)
    }

    pub fn subnodes(&self) -> Vec<String> {
        string_list(self.fields.get("subnodes"))
    }

    pub fn main_node_id(&self) -> Option<String> {
        self.opt_str("main_node_id")
    }

    pub fn host_key_checking(&self) -> bool {
        self.fields
            .get("host_key_checking")
            .and_then(|v| v.as_bool())
            .unwrap_or(true)
    }

    pub fn interface_ip(&self) -> Option<String> {
        self.opt_str("interface_ip")
    }

    pub fn set_interface_ip(&mut self, ip: Option<&str>) {
        self.fields.insert(
            "interface_ip".into(),
            match ip {
                Some(s) => json!(s),
                None => Value::Null,
            },
        );
    }

    pub fn connection_port(&self) -> u16 {
        self.fields
            .get("connection_port")
            .and_then(|v| v.as_u64().or_else(|| v.as_i64().map(|i| i as u64)))
            .unwrap_or(22) as u16
    }

    pub fn connection_type(&self) -> Option<String> {
        self.opt_str("connection_type")
    }

    pub fn boot_timeout(&self) -> f64 {
        self.fields
            .get("boot_timeout")
            .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
            .unwrap_or(300.0)
    }

    pub fn quota(&self) -> QuotaInformation {
        self.fields
            .get("quota")
            .map(QuotaInformation::from_value)
            .unwrap_or_default()
    }

    pub fn merge_fields(&mut self, updates: &Value) {
        let Some(obj) = updates.as_object() else {
            return;
        };
        for (k, v) in obj {
            if k == "uuid" || k == "slot" {
                continue;
            }
            self.fields.insert(k.clone(), v.clone());
        }
    }

    pub fn set_host_keys(&mut self, keys: &[String]) {
        self.fields.insert(
            "host_keys".into(),
            Value::Array(keys.iter().cloned().map(|k| json!(k)).collect()),
        );
    }

    pub fn max_age(&self) -> Option<f64> {
        positive_opt(self.fields.get("max_age"))
    }

    pub fn max_ready_age(&self) -> Option<f64> {
        positive_opt(self.fields.get("max_ready_age"))
    }

    pub fn min_retention_time(&self) -> Option<f64> {
        positive_opt(self.fields.get("min_retention_time"))
    }

    pub fn hold_expiration(&self) -> Option<f64> {
        positive_opt(self.fields.get("hold_expiration"))
    }

    pub fn reuse_flag(&self) -> bool {
        self.fields
            .get("reuse")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    pub fn image_upload_uuid(&self) -> Option<String> {
        self.opt_str("image_upload_uuid")
    }

    pub fn comment_set(&mut self, comment: Option<&str>) {
        self.fields.insert(
            "comment".into(),
            match comment {
                Some(c) => json!(c),
                None => Value::Null,
            },
        );
    }

    pub fn has_expired(&self, now: f64) -> bool {
        if let Some(min) = self.min_retention_time() {
            if self.request_time() + min > now {
                return false;
            }
        }
        if let Some(max_age) = self.max_age() {
            if self.request_time() + max_age < now {
                return true;
            }
        }
        if let Some(max_ready) = self.max_ready_age() {
            if self.state() == node_state::READY && self.state_time() + max_ready < now {
                return true;
            }
        }
        false
    }

    pub fn has_hold_expired(&self, now: f64) -> bool {
        if self.state() != node_state::HOLD {
            return false;
        }
        match self.hold_expiration() {
            Some(exp) => self.state_time() + exp < now,
            None => false,
        }
    }

    pub fn serialize(&self) -> Value {
        let mut fields = self.fields.clone();
        if let Some(sm) = &self.create_runtime {
            fields.insert("create_state".into(), sm.to_value());
        }
        if let Some(sm) = &self.delete_runtime {
            fields.insert("delete_state".into(), sm.to_value());
        }
        let mut m = Map::new();
        for key in node_keys_for(&self.driver_id) {
            if let Some(v) = fields.get(key) {
                m.insert(key.to_string(), v.clone());
            }
        }
        Value::Object(m)
    }

    fn str_field(&self, key: &str) -> String {
        self.fields
            .get(key)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    }

    fn opt_str(&self, key: &str) -> Option<String> {
        self.fields.get(key).and_then(|v| {
            if v.is_null() {
                None
            } else {
                v.as_str().map(|s| s.to_string())
            }
        })
    }
}

impl StateMachineSnap {
    pub fn to_value(&self) -> Value {
        let mut m = Map::new();
        m.insert("state".into(), json!(self.state));
        m.insert("start_time".into(), json!(self.start_time));
        if self.attempts != 0 {
            m.insert("attempts".into(), json!(self.attempts));
        }
        if let Some(id) = &self.image_external_id {
            m.insert("image_external_id".into(), json!(id));
        }
        for (k, v) in &self.extra {
            m.insert(k.clone(), v.clone());
        }
        Value::Object(m)
    }
}

fn positive_opt(v: Option<&Value>) -> Option<f64> {
    let n = v?.as_f64().or_else(|| v?.as_i64().map(|i| i as f64))?;
    if n > 0.0 {
        Some(n)
    } else {
        None
    }
}

fn string_list(v: Option<&Value>) -> Vec<String> {
    v.and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

fn node_keys_for(driver: &str) -> Vec<&'static str> {
    let mut keys: Vec<&'static str> = BASE_NODE_KEYS.to_vec();
    match driver {
        "aws" => keys.extend_from_slice(&[
            "aws_instance_id",
            "aws_dedicated_host_id",
            "aws_launch_template_name",
        ]),
        "azure" => keys.push("azure_vm_id"),
        "openstack" => keys.extend_from_slice(&[
            "openstack_server_id",
            "openstack_floating_ip_id",
        ]),
        "kubernetes" => keys.push("kubernetes_namespace_id"),
        _ => {}
    }
    keys
}

const BASE_NODE_KEYS: &[&str] = &[
    "zuul_event_id",
    "span_info",
    "max_ready_age",
    "max_age",
    "min_retention_time",
    "request_time",
    "state",
    "state_time",
    "label",
    "label_config_hash",
    "label_aliases",
    "tags",
    "connection_name",
    "endpoint_name",
    "comment",
    "main_node_id",
    "subnodes",
    "create_state",
    "delete_state",
    "quota",
    "image_upload_uuid",
    "uuid",
    "attributes",
    "az",
    "boot_timeout",
    "snapshot_timeout",
    "snapshot_expiration",
    "executor_zone",
    "cloud",
    "connection_port",
    "connection_type",
    "kubernetes_connection",
    "username",
    "python_path",
    "shell_type",
    "hold_expiration",
    "host_id",
    "host_key_checking",
    "host_keys",
    "interface_ip",
    "private_ipv4",
    "private_ipv6",
    "provider",
    "public_ipv4",
    "public_ipv6",
    "region",
    "resources",
    "slot",
    "reuse",
    "node_properties",
];

pub fn now_secs() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

pub fn component_json(
    hostname: &str,
    state: &str,
    connection_filter: &Option<Vec<String>>,
) -> Value {
    let filter = match connection_filter {
        Some(v) => Value::Array(v.iter().cloned().map(|s| json!(s)).collect()),
        None => Value::Null,
    };
    json!({
        "hostname": hostname,
        "state": state,
        "kind": "launcher",
        "version": ZUUL_VERSION,
        "model_api": MODEL_API,
        "connection_filter": filter,
    })
}

pub fn component_bytes(
    hostname: &str,
    state: &str,
    connection_filter: &Option<Vec<String>>,
) -> Vec<u8> {
    jsonutil::dumps_bytes(&component_json(hostname, state, connection_filter))
}

#[derive(Clone, Debug)]
pub struct ImageBuildArtifact {
    pub fields: Map<String, Value>,
    pub extra: Map<String, Value>,
    pub is_locked: bool,
}

impl ImageBuildArtifact {
    pub fn from_value(v: Value) -> Self {
        split_known(v, ARTIFACT_KEYS)
    }
    pub fn uuid(&self) -> String {
        str_of(&self.fields, "uuid")
    }
    pub fn state(&self) -> Option<String> {
        self.fields
            .get("_state")
            .and_then(|v| v.as_str().map(|s| s.to_string()))
    }
    pub fn canonical_name(&self) -> String {
        str_of(&self.fields, "canonical_name")
    }
    pub fn timestamp(&self) -> f64 {
        self.fields
            .get("timestamp")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0)
    }
    pub fn serialize(&self) -> Value {
        known_only(&self.fields, ARTIFACT_KEYS)
    }
}

#[derive(Clone, Debug)]
pub struct ImageUpload {
    pub fields: Map<String, Value>,
    pub extra: Map<String, Value>,
    pub is_locked: bool,
}

impl ImageUpload {
    pub fn from_value(v: Value) -> Self {
        let (fields, extra) = split_maps(v, UPLOAD_KEYS);
        Self {
            fields,
            extra,
            is_locked: false,
        }
    }
    pub fn uuid(&self) -> String {
        str_of(&self.fields, "uuid")
    }
    pub fn state(&self) -> String {
        self.fields
            .get("_state")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    }
    pub fn set_state(&mut self, state: &str) {
        self.fields.insert("_state".into(), json!(state));
        self.fields
            .insert("state_time".into(), json!(now_secs()));
    }
    pub fn canonical_name(&self) -> String {
        str_of(&self.fields, "canonical_name")
    }
    pub fn artifact_uuid(&self) -> String {
        str_of(&self.fields, "artifact_uuid")
    }
    pub fn endpoint_name(&self) -> String {
        str_of(&self.fields, "endpoint_name")
    }
    pub fn providers(&self) -> Vec<String> {
        string_list(self.fields.get("providers"))
    }
    pub fn external_id(&self) -> Option<String> {
        self.fields.get("external_id").and_then(|v| {
            if v.is_null() {
                None
            } else {
                v.as_str().map(|s| s.to_string())
            }
        })
    }
    pub fn set_external_id(&mut self, external_id: Option<&str>) {
        self.fields.insert(
            "external_id".into(),
            match external_id {
                Some(id) => json!(id),
                None => Value::Null,
            },
        );
    }
    pub fn timestamp(&self) -> f64 {
        self.fields
            .get("timestamp")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0)
    }
    pub fn validated(&self) -> bool {
        self.fields
            .get("validated")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }
    pub fn config_hash(&self) -> String {
        str_of(&self.fields, "config_hash")
    }
    pub fn attempt(&self) -> i64 {
        self.fields.get("attempt").and_then(|v| v.as_i64()).unwrap_or(0)
    }
    pub fn serialize(&self) -> Value {
        known_only(&self.fields, UPLOAD_KEYS)
    }
}

fn split_known(v: Value, keys: &[&str]) -> ImageBuildArtifact {
    let (fields, extra) = split_maps(v, keys);
    ImageBuildArtifact {
        fields,
        extra,
        is_locked: false,
    }
}

fn split_maps(v: Value, keys: &[&str]) -> (Map<String, Value>, Map<String, Value>) {
    let obj = v.as_object().cloned().unwrap_or_default();
    let mut fields = Map::new();
    let mut extra = Map::new();
    for (k, val) in obj {
        if keys.iter().any(|n| *n == k) {
            fields.insert(k, val);
        } else {
            extra.insert(k, val);
        }
    }
    (fields, extra)
}

fn known_only(fields: &Map<String, Value>, keys: &[&str]) -> Value {
    let mut m = Map::new();
    for key in keys {
        if let Some(v) = fields.get(*key) {
            m.insert((*key).to_string(), v.clone());
        }
    }
    Value::Object(m)
}

fn str_of(fields: &Map<String, Value>, key: &str) -> String {
    fields
        .get(key)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

const ARTIFACT_KEYS: &[&str] = &[
    "uuid",
    "name",
    "canonical_name",
    "project_canonical_name",
    "project_branch",
    "build_tenant_name",
    "build_uuid",
    "format",
    "md5sum",
    "sha256",
    "url",
    "timestamp",
    "validated",
    "_state",
    "state_time",
];

const UPLOAD_KEYS: &[&str] = &[
    "uuid",
    "canonical_name",
    "artifact_uuid",
    "build_uuid",
    "endpoint_name",
    "providers",
    "config_hash",
    "external_id",
    "timestamp",
    "validated",
    "attempt",
    "_state",
    "state_time",
];
