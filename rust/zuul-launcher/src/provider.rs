// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// Provider / endpoint / state-machine surface from zuul/provider/__init__.py
// and zuul/provider/statemachine.py. Driver advance() methods follow the
// Python call order in each driver's statemachine module.

use crate::cloud;
use crate::config::quote_plus;
use crate::endpoint;
use crate::model::{ProviderNode, QuotaInformation};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use uuid::Uuid;

/// Namespace for static node UUIDs from zuul/driver/static/staticprovider.py.
pub const STATIC_NODE_NAMESPACE: Uuid =
    Uuid::from_u128(0x913b8645_cc07_4e1c_88e6_7ba14fdd286f);

/// Deterministic uuid5 hex matching Python `uuid.uuid5(NODE_NAMESPACE, key).hex`.
pub fn static_node_uuid(
    canonical_name: &str,
    host: &str,
    connection_port: i64,
    username: Option<&str>,
) -> String {
    let username = username.unwrap_or("None");
    let key = format!("{canonical_name} {host} {connection_port} {username}");
    Uuid::new_v5(&STATIC_NODE_NAMESPACE, key.as_bytes())
        .as_simple()
        .to_string()
}

/// A configured static host from the provider `nodes` list.
#[derive(Clone, Debug)]
pub struct StaticNodeConfig {
    pub uuid: String,
    pub label: String,
    pub host: String,
    pub connection_port: i64,
    pub username: Option<String>,
    pub aliases: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct Label {
    pub name: String,
    pub canonical_name: String,
    pub image: Option<String>,
    pub flavor: Option<String>,
    pub min_ready: Option<i64>,
    pub max_ready_age: i64,
    pub max_age: i64,
    pub min_retention_time: i64,
    pub host_key_checking: bool,
    pub boot_timeout: i64,
    pub snapshot_timeout: i64,
    pub snapshot_expiration: i64,
    pub executor_zone: Option<String>,
    pub reuse: bool,
    pub slots: i64,
    pub aliases: Vec<String>,
    pub config_hash: String,
    pub max_nodes: Option<i64>,
    pub tags: BTreeMap<String, String>,
    pub connection_type: Option<String>,
    pub connection_port: i64,
    pub username: Option<String>,
    pub python_path: Option<String>,
    pub shell_type: Option<String>,
    pub key_name: Option<String>,
    pub raw: Value,
}

#[derive(Clone, Debug)]
pub struct Image {
    pub name: String,
    pub canonical_name: String,
    pub image_type: String,
    pub config_hash: String,
    pub username: Option<String>,
    pub connection_type: Option<String>,
    pub connection_port: Option<i64>,
    pub python_path: Option<String>,
    pub shell_type: Option<String>,
    pub retain_count: i64,
    pub raw: Value,
}

#[derive(Clone, Debug)]
pub struct Flavor {
    pub name: String,
    pub raw: Value,
}

#[derive(Debug)]
pub enum AdvanceError {
    Quota(String),
    Capacity(String),
    LaunchTimeout(String),
    Other(String),
}

impl std::fmt::Display for AdvanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            AdvanceError::Quota(s)
            | AdvanceError::Capacity(s)
            | AdvanceError::LaunchTimeout(s)
            | AdvanceError::Other(s) => write!(f, "{s}"),
        }
    }
}

pub trait StateMachine: Send + Sync {
    fn advance(&mut self) -> Result<(), AdvanceError>;
    fn complete(&self) -> bool;
    fn state_name(&self) -> String;
    fn start_time(&self) -> f64;
    fn set_start_time(&mut self, t: f64);
    fn external_id(&self) -> Option<String> {
        None
    }
    fn interface_ip(&self) -> Option<String> {
        None
    }
    fn to_dict(&self) -> Value;
    /// Endpoint methods invoked so far, in order. Empty for scripted test machines.
    fn calls(&self) -> Vec<String> {
        Vec::new()
    }
    /// Launcher clock, used by the static delete recycle wait.
    fn set_now(&mut self, _t: f64) {}
    /// Node fields copied back from a live cloud call.
    fn node_updates(&self) -> Option<Value> {
        None
    }
    /// Mark the machine complete after an error or a delete timeout.
    fn fail(&mut self) {}
}

pub struct EndpointInfo {
    pub name: String,
    pub canonical_name: String,
    pub connection_name: String,
    pub unmanaged: QuotaInformation,
    pub limits: QuotaInformation,
}

pub trait Provider: Send + Sync {
    fn name(&self) -> &str;
    fn canonical_name(&self) -> &str;
    fn driver_name(&self) -> &str;
    fn connection_name(&self) -> &str;
    fn endpoint(&self) -> &EndpointInfo;
    fn labels(&self) -> &BTreeMap<String, Label>;
    fn images(&self) -> &BTreeMap<String, Image>;
    fn flavors(&self) -> &BTreeMap<String, Flavor>;
    fn launch_attempts(&self) -> i64;
    fn launch_timeout(&self) -> Option<f64>;
    fn resource_limits(&self) -> &BTreeMap<String, f64>;
    fn quota_for_label(&self, label: &Label) -> QuotaInformation;
    fn can_reuse(&self, node: &ProviderNode) -> bool;
    fn create_machine(&self, node: &ProviderNode, label: &Label) -> Box<dyn StateMachine>;
    fn delete_machine(&self, node: &ProviderNode) -> Box<dyn StateMachine>;
    fn get_node_tags(&self, system_id: &str, node: &ProviderNode, label: &Label, provider_cname: &str) -> BTreeMap<String, String> {
        let mut tags = label.tags.clone();
        tags.insert("zuul_system_id".into(), system_id.into());
        tags.insert("zuul_node_uuid".into(), node.uuid());
        tags.insert("zuul_provider".into(), provider_cname.into());
        tags
    }
    fn import_job(&self, _url: &str) -> bool {
        false
    }
    /// Whether this provider can copy an image from a source with the
    /// given driver and region (Aws: different region; OpenStack: same
    /// region, different project — project check is runtime-only).
    fn copy_job(&self, _source_driver: &str, _source_region: Option<&str>) -> bool {
        false
    }
    fn region(&self) -> Option<&str> {
        None
    }
    fn upload_job(&self) -> bool {
        true
    }
    fn delete_image(&self, _external_id: &str) -> Result<(), String> {
        Ok(())
    }
    fn list_instances(&self) -> Vec<CloudResource> {
        Vec::new()
    }
    fn list_resources(&self) -> Vec<CloudResource> {
        Vec::new()
    }
    fn list_required_resources(&self) -> Vec<CloudResource> {
        Vec::new()
    }
    fn delete_resource(&self, _resource: &CloudResource) -> Result<(), String> {
        Ok(())
    }
    fn unmanaged_quota(&self) -> Result<QuotaInformation, String> {
        Ok(self.endpoint().unmanaged.copy())
    }
    fn notify_nodescan_failure(&self, _node: &ProviderNode) {}
    /// Static provider hosts keyed by uuid5 node id. Empty for other drivers.
    fn static_nodes(&self) -> &BTreeMap<String, StaticNodeConfig> {
        static EMPTY: std::sync::OnceLock<BTreeMap<String, StaticNodeConfig>> =
            std::sync::OnceLock::new();
        EMPTY.get_or_init(BTreeMap::new)
    }
}

#[derive(Clone, Debug)]
pub struct CloudResource {
    pub unique_id: String,
    pub metadata: BTreeMap<String, String>,
    pub quota: QuotaInformation,
    pub kind: String,
}

/// In-memory provider used by launcher tests and as the static driver's
/// create path when no live cloud is required.
pub struct MemoryProvider {
    pub name: String,
    pub canonical_name: String,
    pub driver_name: String,
    pub connection_name: String,
    pub endpoint_info: EndpointInfo,
    pub labels: BTreeMap<String, Label>,
    pub images: BTreeMap<String, Image>,
    pub flavors: BTreeMap<String, Flavor>,
    pub launch_attempts: i64,
    pub launch_timeout: Option<f64>,
    pub resource_limits: BTreeMap<String, f64>,
    pub label_quota: QuotaInformation,
    pub reuse: bool,
    pub create_mode: CreateMode,
    pub calls: std::sync::Mutex<Vec<String>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateMode {
    Ready,
    QuotaFail,
    Error,
    Hang,
}

impl MemoryProvider {
    pub fn static_like(name: &str, label_name: &str) -> Self {
        let mut labels = BTreeMap::new();
        labels.insert(label_name.to_string(), default_label(label_name, true));
        let mut images = BTreeMap::new();
        images.insert(
            "img".into(),
            Image {
                name: "img".into(),
                canonical_name: format!("example%2Forg/{label_name}"),
                image_type: "cloud".into(),
                config_hash: "h".into(),
                username: Some("zuul".into()),
                connection_type: Some("ssh".into()),
                connection_port: Some(22),
                python_path: Some("auto".into()),
                shell_type: None,
                retain_count: 2,
                raw: json!({}),
            },
        );
        let mut q = QuotaInformation::default();
        q.quota.insert("instances".into(), 1.0);
        let mut limits = QuotaInformation {
            default: f64::INFINITY,
            quota: BTreeMap::new(),
        };
        limits.quota.insert("instances".into(), 100.0);
        Self {
            name: name.into(),
            canonical_name: name.into(),
            driver_name: "static".into(),
            connection_name: "static-conn".into(),
            endpoint_info: EndpointInfo {
                name: "static-conn".into(),
                canonical_name: quote_plus("static-conn"),
                connection_name: "static-conn".into(),
                unmanaged: QuotaInformation::default(),
                limits,
            },
            labels,
            images,
            flavors: BTreeMap::new(),
            launch_attempts: 3,
            launch_timeout: Some(600.0),
            resource_limits: BTreeMap::new(),
            label_quota: q,
            reuse: true,
            create_mode: CreateMode::Ready,
            calls: std::sync::Mutex::new(Vec::new()),
        }
    }
}

pub fn default_label(name: &str, reuse: bool) -> Label {
    Label {
        name: name.into(),
        canonical_name: format!("example%2Forg/{name}"),
        image: Some("img".into()),
        flavor: None,
        min_ready: None,
        max_ready_age: 0,
        max_age: 0,
        min_retention_time: 0,
        host_key_checking: true,
        boot_timeout: 300,
        snapshot_timeout: 3600,
        snapshot_expiration: 3600 * 24 * 7,
        executor_zone: None,
        reuse,
        slots: 1,
        aliases: Vec::new(),
        config_hash: "abc123".into(),
        max_nodes: None,
        tags: BTreeMap::new(),
        connection_type: Some("ssh".into()),
        connection_port: 22,
        username: Some("zuul".into()),
        python_path: Some("auto".into()),
        shell_type: None,
        key_name: None,
        raw: json!({}),
    }
}

impl Provider for MemoryProvider {
    fn name(&self) -> &str { &self.name }
    fn canonical_name(&self) -> &str { &self.canonical_name }
    fn driver_name(&self) -> &str { &self.driver_name }
    fn connection_name(&self) -> &str { &self.connection_name }
    fn endpoint(&self) -> &EndpointInfo { &self.endpoint_info }
    fn labels(&self) -> &BTreeMap<String, Label> { &self.labels }
    fn images(&self) -> &BTreeMap<String, Image> { &self.images }
    fn flavors(&self) -> &BTreeMap<String, Flavor> { &self.flavors }
    fn launch_attempts(&self) -> i64 { self.launch_attempts }
    fn launch_timeout(&self) -> Option<f64> { self.launch_timeout }
    fn resource_limits(&self) -> &BTreeMap<String, f64> { &self.resource_limits }
    fn quota_for_label(&self, _label: &Label) -> QuotaInformation { self.label_quota.copy() }
    fn can_reuse(&self, node: &ProviderNode) -> bool {
        self.reuse && node.reuse_flag()
    }
    fn create_machine(&self, node: &ProviderNode, _label: &Label) -> Box<dyn StateMachine> {
        self.calls.lock().unwrap().push("create".into());
        Box::new(ScriptedMachine {
            state: "start".into(),
            start_time: 0.0,
            complete: false,
            mode: self.create_mode,
            ip: Some("127.0.0.1".into()),
            external_id: Some(node.uuid()),
        })
    }
    fn delete_machine(&self, _node: &ProviderNode) -> Box<dyn StateMachine> {
        self.calls.lock().unwrap().push("delete".into());
        Box::new(ScriptedMachine {
            state: "start".into(),
            start_time: 0.0,
            complete: false,
            mode: CreateMode::Ready,
            ip: None,
            external_id: None,
        })
    }
    fn upload_job(&self) -> bool { false }
}

struct ScriptedMachine {
    state: String,
    start_time: f64,
    complete: bool,
    mode: CreateMode,
    ip: Option<String>,
    external_id: Option<String>,
}

impl StateMachine for ScriptedMachine {
    fn advance(&mut self) -> Result<(), AdvanceError> {
        match self.mode {
            CreateMode::QuotaFail if self.state == "start" => {
                return Err(AdvanceError::Quota("quota".into()));
            }
            CreateMode::Error => return Err(AdvanceError::Other("boom".into())),
            CreateMode::Hang => {
                self.state = "pending".into();
                return Ok(());
            }
            _ => {}
        }
        self.state = "complete".into();
        self.complete = true;
        Ok(())
    }
    fn complete(&self) -> bool { self.complete }
    fn state_name(&self) -> String { self.state.clone() }
    fn start_time(&self) -> f64 { self.start_time }
    fn set_start_time(&mut self, t: f64) { self.start_time = t; }
    fn external_id(&self) -> Option<String> { self.external_id.clone() }
    fn interface_ip(&self) -> Option<String> { self.ip.clone() }
    fn to_dict(&self) -> Value {
        json!({"state": self.state, "start_time": self.start_time})
    }
}

/// AWS create/delete from zuul/driver/aws/awsendpoint.py.
/// Offline: a stub endpoint returns success immediately, so one
/// advance() falls through the same states Python would when every
/// future is done. Live: each endpoint method is invoked through
/// cloud::invoke so moto and unittest patches apply.
pub struct AwsCreate {
    pub state: String,
    pub start_time: f64,
    pub complete: bool,
    pub attempts: i64,
    pub image_external_id: Option<String>,
    pub host_id: Option<String>,
    pub instance_id: Option<String>,
    pub dedicated_host: bool,
    pub calls: Vec<String>,
    pub visited: Vec<String>,
    host_future: bool,
    create_future: bool,
    /// Stub: host refresh reports available.
    pub host_ready: bool,
    /// Stub: instance refresh reports running.
    pub instance_running: bool,
    /// When set, advance() drives real endpoint ops via cloud::invoke.
    live: bool,
    provider: String,
    label_name: String,
    node_uuid: String,
    hostname: String,
    tags: Value,
    fleet: bool,
    spot: bool,
    launch_template_name: Option<String>,
    host_future_id: Option<String>,
    create_future_id: Option<String>,
    host_obj: Option<Value>,
    instance_obj: Option<Value>,
    node_properties: Map<String, Value>,
    interface_ip: Option<String>,
    updates: Option<Value>,
    /// Node fields from a patched `_updateNode`, merged into updates.
    patch_node: Option<Value>,
    quota: Option<Value>,
    setup_done: bool,
}

impl AwsCreate {
    pub fn new(dedicated_host: bool) -> Self {
        let mut machine = Self {
            state: "start".into(),
            start_time: 0.0,
            complete: false,
            attempts: 0,
            image_external_id: None,
            host_id: None,
            instance_id: None,
            dedicated_host,
            calls: Vec::new(),
            visited: vec!["start".into()],
            host_future: false,
            create_future: false,
            host_ready: true,
            instance_running: true,
            live: false,
            provider: String::new(),
            label_name: String::new(),
            node_uuid: String::new(),
            hostname: String::new(),
            tags: json!({}),
            fleet: false,
            spot: false,
            launch_template_name: None,
            host_future_id: None,
            create_future_id: None,
            host_obj: None,
            instance_obj: None,
            node_properties: Map::new(),
            interface_ip: None,
            updates: None,
            patch_node: None,
            quota: None,
            setup_done: true,
        };
        // AwsCreateStateMachine.__init__ always prices the label.
        machine.calls.push("getQuotaForLabel".into());
        machine
    }

    pub fn from_provider(provider: &CloudProvider, node: &ProviderNode, label: &Label) -> Self {
        let dedicated = label
            .flavor
            .as_ref()
            .and_then(|name| provider.flavors.get(name))
            .and_then(|flavor| {
                flavor
                    .raw
                    .get("dedicated-host")
                    .or_else(|| flavor.raw.get("dedicated_host"))
                    .and_then(|v| v.as_bool())
            })
            .unwrap_or(false);
        let mut machine = Self::new(dedicated);
        machine.provider = provider.canonical_name.clone();
        machine.label_name = label.name.clone();
        machine.node_uuid = node.uuid();
        let uuid = node.uuid();
        let host_len = uuid.len().min(13);
        machine.hostname = format!("np{}", &uuid[..host_len]);
        machine.tags = node.fields.get("tags").cloned().unwrap_or_else(|| json!({}));
        if let Some(obj) = machine.tags.as_object_mut() {
            obj.insert("Name".into(), json!(machine.hostname));
        }
        machine.instance_id = node
            .fields
            .get("aws_instance_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        machine.host_id = node
            .fields
            .get("aws_dedicated_host_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        machine.launch_template_name = node
            .fields
            .get("aws_launch_template_name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        if let Some(props) = node.fields.get("node_properties").and_then(|v| v.as_object()) {
            machine.node_properties = props.clone();
        }
        if let Some(cs) = node.fields.get("create_state") {
            if let Some(state) = cs.get("state").and_then(|v| v.as_str()) {
                if state != "start" {
                    machine.state = state.to_string();
                    machine.visited = vec![state.to_string()];
                }
            }
            machine.attempts = cs
                .get("attempts")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            if let Some(id) = cs.get("image_external_id").and_then(|v| v.as_str()) {
                machine.image_external_id = Some(id.to_string());
            }
            if let Some(t) = cs.get("start_time").and_then(|v| v.as_f64()) {
                machine.start_time = t;
            }
        }
        if let Some(flavor_name) = &label.flavor {
            if let Some(flavor) = provider.flavors.get(flavor_name) {
                machine.fleet = flavor
                    .raw
                    .get("fleet")
                    .map(|v| !v.is_null() && v != &json!(false))
                    .unwrap_or(false);
                let market = flavor
                    .raw
                    .get("market-type")
                    .or_else(|| flavor.raw.get("market_type"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                machine.spot = market == "spot";
            }
        }
        machine.live = true;
        machine.setup_done = !machine.live;
        if machine.live {
            // Quota call is recorded once live_setup runs.
            machine.calls.clear();
            machine.visited = vec![machine.state.clone()];
        }
        machine
    }

    fn note(&mut self, state: &str) {
        self.state = state.into();
        self.visited.push(state.into());
    }

    fn live_setup(&mut self) -> Result<(), AdvanceError> {
        if self.setup_done {
            return Ok(());
        }
        let quota = endpoint::quota_for_label(&self.provider, &self.label_name, None)?;
        self.calls.push("getQuotaForLabel".into());
        self.quota = Some(quota);
        if let Some(host_id) = self.host_id.clone() {
            let obj = json!({
                "HostId": host_id,
                "State": {"Name": "pending"},
            });
            self.host_obj = Some(endpoint::refresh(&self.provider, &obj)?);
            self.calls.push("_refresh".into());
        }
        if let Some(instance_id) = self.instance_id.clone() {
            let obj = json!({
                "InstanceId": instance_id,
                "State": {"Name": "pending"},
            });
            self.instance_obj = Some(endpoint::refresh(&self.provider, &obj)?);
            self.calls.push("_refresh".into());
        }
        self.setup_done = true;
        self.export_updates();
        Ok(())
    }

    fn apply_patched(&mut self, result: &Value) {
        if let Some(state) = result.get("state").and_then(|v| v.as_str()) {
            self.state = state.to_string();
        }
        self.complete = result
            .get("complete")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if let Some(ip) = result.get("interface_ip").and_then(|v| v.as_str()) {
            self.interface_ip = Some(ip.to_string());
        }
        if let Some(id) = result.get("aws_instance_id").and_then(|v| v.as_str()) {
            self.instance_id = Some(id.to_string());
        }
        if let Some(node) = result.get("node") {
            self.updates = Some(node.clone());
        } else {
            self.export_updates();
        }
    }

    fn export_updates(&mut self) {
        let mut m = Map::new();
        m.insert("create_state".into(), self.to_dict());
        if let Some(id) = &self.instance_id {
            m.insert("aws_instance_id".into(), json!(id));
        }
        if let Some(id) = &self.host_id {
            m.insert("aws_dedicated_host_id".into(), json!(id));
        }
        if let Some(name) = &self.launch_template_name {
            m.insert("aws_launch_template_name".into(), json!(name));
        }
        if !self.node_properties.is_empty() {
            m.insert(
                "node_properties".into(),
                Value::Object(self.node_properties.clone()),
            );
        }
        if let Some(ip) = &self.interface_ip {
            m.insert("interface_ip".into(), json!(ip));
        }
        if let Some(q) = &self.quota {
            if let Some(quota) = q.get("quota") {
                m.insert("quota".into(), quota.clone());
            } else {
                m.insert("quota".into(), q.clone());
            }
        }
        // Fill address fields from the instance when complete.
        if let Some(inst) = &self.instance_obj {
            if let Some(v) = inst.get("PublicIpAddress") {
                m.insert("public_ipv4".into(), v.clone());
            }
            if let Some(v) = inst.get("PrivateIpAddress") {
                m.insert("private_ipv4".into(), v.clone());
            }
            if let Some(az) = inst
                .get("Placement")
                .and_then(|p| p.get("AvailabilityZone"))
            {
                m.insert("az".into(), az.clone());
            }
            m.insert("cloud".into(), json!("AWS"));
        }
        if let Some(Value::Object(patch)) = &self.patch_node {
            for (k, v) in patch {
                if k == "create_state" {
                    continue;
                }
                // Deep-merge node_properties so a Python _updateNode
                // export (ebs_volume_id) does not drop instance_id set
                // earlier on the Rust state machine.
                if k == "node_properties" {
                    let mut merged = self.node_properties.clone();
                    if let Some(obj) = v.as_object() {
                        for (pk, pv) in obj {
                            merged.insert(pk.clone(), pv.clone());
                        }
                    }
                    m.insert(k.clone(), Value::Object(merged));
                } else {
                    m.insert(k.clone(), v.clone());
                }
            }
        }
        self.updates = Some(Value::Object(m));
    }

    fn advance_offline(&mut self) -> Result<(), AdvanceError> {
        if self.state == "start" {
            self.calls.push("provider_label_template_names".into());
            if self.dedicated_host {
                self.note("start allocating host");
            } else {
                self.note("start creating instance");
            }
        }
        if self.state == "start allocating host" {
            if !self.host_future {
                self.calls.push("_submitAllocateHost".into());
                self.host_future = true;
            }
            self.calls.push("_completeAllocateHost".into());
            self.host_id = Some("h-stub".into());
            self.note("allocating host");
        }
        if self.state == "allocating host" {
            self.calls.push("_refresh".into());
            if self.host_ready {
                self.note("start creating instance");
            } else {
                return Ok(());
            }
        }
        if self.state == "start creating instance" {
            if !self.create_future {
                self.calls.push("_submitCreateInstance".into());
                self.create_future = true;
            }
            self.calls.push("_completeCreateInstance".into());
            self.instance_id = Some("i-stub".into());
            self.note("creating instance");
        }
        if self.state == "creating instance" {
            self.calls.push("_refresh".into());
            if self.instance_running {
                self.calls.push("getQuotaForLabel".into());
                self.note("complete");
            } else {
                return Ok(());
            }
        }
        if self.state == "complete" {
            self.complete = true;
        }
        Ok(())
    }

    fn advance_live(&mut self) -> Result<(), AdvanceError> {
        self.live_setup()?;
        if self.state == "start" {
            self.node_properties
                .insert("fleet".into(), json!(self.fleet));
            self.node_properties
                .insert("spot".into(), json!(self.spot));
            self.calls.push("provider_label_template_names".into());
            if self.dedicated_host {
                self.note("start allocating host");
            } else {
                self.note("start creating instance");
            }
        }
        if self.state == "start allocating host" {
            if self.host_future_id.is_none() {
                let result = endpoint::submit_allocate_host(
                    &self.provider,
                    json!({
                        "label": self.label_name,
                        "tags": self.tags,
                        "hostname": self.hostname,
                    }),
                )?;
                self.calls.push("_submitAllocateHost".into());
                self.host_future = true;
                self.host_future_id = result
                    .get("future_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
            }
            let future_id = self
                .host_future_id
                .clone()
                .ok_or_else(|| AdvanceError::Other("missing host future".into()))?;
            self.calls.push("_completeAllocateHost".into());
            let host = endpoint::complete_allocate_host(&self.provider, &future_id)?;
            if host.is_none() {
                self.export_updates();
                return Ok(());
            }
            let host = host.unwrap();
            self.host_id = host
                .get("HostId")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            self.host_obj = Some(host);
            self.note("allocating host");
        }
        if self.state == "allocating host" {
            let obj = self
                .host_obj
                .clone()
                .ok_or_else(|| AdvanceError::Other("missing host".into()))?;
            self.calls.push("_refresh".into());
            let host = endpoint::refresh(&self.provider, &obj)?;
            self.host_obj = Some(host.clone());
            let state = host
                .get("State")
                .and_then(|v| {
                    v.as_str()
                        .map(|s| s.to_string())
                        .or_else(|| v.get("Name").and_then(|n| n.as_str()).map(|s| s.to_string()))
                })
                .unwrap_or_default()
                .to_lowercase();
            if state == "available" {
                if let Some(id) = host.get("HostId").and_then(|v| v.as_str()) {
                    self.host_id = Some(id.to_string());
                    self.node_properties
                        .insert("host_id".into(), json!(id));
                }
                self.note("start creating instance");
            } else if matches!(
                state.as_str(),
                "permanent-failure" | "released" | "released-permanent-failure"
            ) {
                return Err(AdvanceError::Other(format!("Host in {state} state")));
            } else {
                self.export_updates();
                return Ok(());
            }
        }
        if self.state == "start creating instance" {
            if self.create_future_id.is_none() {
                let mut payload = json!({
                    "label": self.label_name,
                    "tags": self.tags,
                    "hostname": self.hostname,
                    "node_uuid": self.node_uuid,
                    "dedicated_host_id": self.host_id,
                    "launch_template_name": self.launch_template_name,
                });
                if let Some(id) = &self.image_external_id {
                    payload
                        .as_object_mut()
                        .unwrap()
                        .insert("image_external_id".into(), json!(id));
                }
                let result = endpoint::submit_create_instance(&self.provider, payload)?;
                self.calls.push("_submitCreateInstance".into());
                self.create_future = true;
                self.create_future_id = result
                    .get("future_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                if let Some(name) = result
                    .get("launch_template_name")
                    .and_then(|v| v.as_str())
                {
                    self.launch_template_name = Some(name.to_string());
                }
                if let Some(id) = result
                    .get("image_external_id")
                    .and_then(|v| v.as_str())
                {
                    self.image_external_id = Some(id.to_string());
                }
            }
            let future_id = self
                .create_future_id
                .clone()
                .ok_or_else(|| AdvanceError::Other("missing create future".into()))?;
            self.calls.push("_completeCreateInstance".into());
            let instance = endpoint::complete_create_instance(&self.provider, &future_id)?;
            if instance.is_none() {
                self.export_updates();
                return Ok(());
            }
            let instance = instance.unwrap();
            self.instance_id = instance
                .get("InstanceId")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            if let Some(id) = &self.instance_id {
                self.node_properties
                    .insert("instance_id".into(), json!(id));
            }
            self.instance_obj = Some(instance);
            self.note("creating instance");
        }
        if self.state == "creating instance" {
            let obj = self
                .instance_obj
                .clone()
                .ok_or_else(|| AdvanceError::Other("missing instance".into()))?;
            self.calls.push("_refresh".into());
            let instance = endpoint::refresh(&self.provider, &obj)?;
            self.instance_obj = Some(instance.clone());
            let state = instance
                .get("State")
                .and_then(|v| v.get("Name"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_lowercase();
            if state == "running" {
                let instance_type = instance
                    .get("InstanceType")
                    .and_then(|v| v.as_str());
                let quota = endpoint::quota_for_label(
                    &self.provider,
                    &self.label_name,
                    instance_type,
                )?;
                self.calls.push("getQuotaForLabel".into());
                self.quota = Some(quota);
                self.apply_instance_fields(&instance);
                self.note("complete");
                self.complete = true;
                self.export_updates();
                return Ok(());
            } else if state == "terminated" {
                return Err(AdvanceError::Other(
                    "Instance in terminated state".into(),
                ));
            } else {
                self.export_updates();
                return Ok(());
            }
        }
        if self.state == "complete" {
            self.complete = true;
        }
        self.export_updates();
        Ok(())
    }

    fn apply_instance_fields(&mut self, instance: &Value) {
        let public_v4 = instance
            .get("PublicIpAddress")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let private_v4 = instance
            .get("PrivateIpAddress")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let public_v6 = instance
            .get("NetworkInterfaces")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|iface| iface.get("Ipv6Addresses"))
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|a| a.get("Ipv6Address"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        self.interface_ip = public_v4
            .clone()
            .or(public_v6)
            .or(private_v4);
        if let (Some(mappings), Some(root)) = (
            instance.get("BlockDeviceMappings").and_then(|v| v.as_array()),
            instance.get("RootDeviceName").and_then(|v| v.as_str()),
        ) {
            for attachment in mappings {
                if attachment.get("DeviceName").and_then(|v| v.as_str()) == Some(root) {
                    if let Some(vol) = attachment
                        .get("Ebs")
                        .and_then(|e| e.get("VolumeId"))
                        .and_then(|v| v.as_str())
                    {
                        self.node_properties
                            .insert("ebs_volume_id".into(), json!(vol));
                    }
                    break;
                }
            }
        }
        // Allow a Python _updateNode patch to adjust the exported node.
        let patched = endpoint::patched_advance(
            "aws",
            "update_node",
            &self.state,
            false,
            json!({
                "provider": self.provider,
                "node_uuid": self.node_uuid,
                "label": self.label_name,
                "instance": instance,
                "host": self.host_obj,
                "aws_instance_id": self.instance_id,
                "aws_dedicated_host_id": self.host_id,
            }),
        );
        if let Ok(Some(result)) = patched {
            if let Some(node) = result.get("node") {
                if let Some(ip) = node.get("interface_ip").and_then(|v| v.as_str()) {
                    self.interface_ip = Some(ip.to_string());
                }
                self.patch_node = Some(node.clone());
            }
        }
    }
}

impl StateMachine for AwsCreate {
    fn advance(&mut self) -> Result<(), AdvanceError> {
        if self.live || cloud::enabled() {
            self.live = true;
            if let Some(result) = endpoint::patched_advance(
                "aws",
                "create",
                &self.state,
                self.complete,
                json!({
                    "provider": self.provider,
                    "node_uuid": self.node_uuid,
                    "label": self.label_name,
                }),
            )? {
                self.apply_patched(&result);
                return Ok(());
            }
            return self.advance_live();
        }
        self.advance_offline()
    }
    fn complete(&self) -> bool { self.complete }
    fn state_name(&self) -> String { self.state.clone() }
    fn start_time(&self) -> f64 { self.start_time }
    fn set_start_time(&mut self, t: f64) { self.start_time = t; }
    fn external_id(&self) -> Option<String> { self.instance_id.clone() }
    fn interface_ip(&self) -> Option<String> {
        if self.live {
            return self.interface_ip.clone();
        }
        if self.complete { Some("192.0.2.10".into()) } else { None }
    }
    fn calls(&self) -> Vec<String> { self.calls.clone() }
    fn node_updates(&self) -> Option<Value> {
        if self.live {
            self.updates.clone()
        } else {
            None
        }
    }
    fn to_dict(&self) -> Value {
        let mut m = Map::new();
        m.insert("state".into(), json!(self.state));
        m.insert("start_time".into(), json!(self.start_time));
        m.insert("attempts".into(), json!(self.attempts));
        if let Some(id) = &self.image_external_id {
            m.insert("image_external_id".into(), json!(id));
        }
        Value::Object(m)
    }
}

pub struct AwsDelete {
    pub state: String,
    pub start_time: f64,
    pub complete: bool,
    pub calls: Vec<String>,
    pub visited: Vec<String>,
    pub instance_id: Option<String>,
    pub host_id: Option<String>,
    /// Stub: _refreshDelete reports the resource gone.
    pub gone: bool,
    live: bool,
    provider: String,
    node_uuid: String,
    instance_obj: Option<Value>,
    host_obj: Option<Value>,
    updates: Option<Value>,
    setup_done: bool,
}

impl AwsDelete {
    pub fn new(instance_id: Option<String>, host_id: Option<String>) -> Self {
        let mut machine = Self {
            state: "start".into(),
            start_time: 0.0,
            complete: false,
            calls: Vec::new(),
            visited: vec!["start".into()],
            instance_id,
            host_id,
            gone: true,
            live: false,
            provider: String::new(),
            node_uuid: String::new(),
            instance_obj: None,
            host_obj: None,
            updates: None,
            setup_done: true,
        };
        if machine.host_id.is_some() {
            machine.calls.push("_refreshDelete".into());
            machine.host_obj = Some(json!({"HostId": machine.host_id}));
        }
        if machine.instance_id.is_some() {
            machine.calls.push("_refreshDelete".into());
            machine.instance_obj = Some(json!({"InstanceId": machine.instance_id}));
        }
        machine
    }

    pub fn from_provider(provider: &CloudProvider, node: &ProviderNode) -> Self {
        let instance_id = node
            .fields
            .get("aws_instance_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        let host_id = node
            .fields
            .get("aws_dedicated_host_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        let mut machine = Self::new(instance_id, host_id);
        machine.provider = provider.canonical_name.clone();
        machine.node_uuid = node.uuid();
        if let Some(ds) = node.fields.get("delete_state") {
            if let Some(state) = ds.get("state").and_then(|v| v.as_str()) {
                if state != "start" {
                    machine.state = state.to_string();
                    machine.visited = vec![state.to_string()];
                }
            }
            if let Some(t) = ds.get("start_time").and_then(|v| v.as_f64()) {
                machine.start_time = t;
            }
        }
        machine.live = true;
        machine.setup_done = !machine.live;
        if machine.live {
            machine.calls.clear();
        }
        machine
    }

    fn note(&mut self, state: &str) {
        self.state = state.into();
        self.visited.push(state.into());
    }

    fn live_setup(&mut self) -> Result<(), AdvanceError> {
        if self.setup_done {
            return Ok(());
        }
        if let Some(host_id) = self.host_id.clone() {
            let obj = json!({ "HostId": host_id });
            self.host_obj = endpoint::refresh_delete(&self.provider, &obj)?;
            self.calls.push("_refreshDelete".into());
        }
        if let Some(instance_id) = self.instance_id.clone() {
            let obj = json!({ "InstanceId": instance_id });
            self.instance_obj = endpoint::refresh_delete(&self.provider, &obj)?;
            self.calls.push("_refreshDelete".into());
        }
        self.setup_done = true;
        self.export_updates();
        Ok(())
    }

    fn export_updates(&mut self) {
        let mut m = Map::new();
        m.insert("delete_state".into(), self.to_dict());
        if let Some(id) = &self.instance_id {
            m.insert("aws_instance_id".into(), json!(id));
        }
        if let Some(id) = &self.host_id {
            m.insert("aws_dedicated_host_id".into(), json!(id));
        }
        self.updates = Some(Value::Object(m));
    }

    fn apply_patched(&mut self, result: &Value) {
        if let Some(state) = result.get("state").and_then(|v| v.as_str()) {
            self.state = state.to_string();
        }
        self.complete = result
            .get("complete")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if let Some(node) = result.get("node") {
            self.updates = Some(node.clone());
        } else {
            self.export_updates();
        }
    }

    fn advance_offline(&mut self) -> Result<(), AdvanceError> {
        if self.state == "start" {
            if self.instance_id.is_some() {
                self.note("start deleting instance");
            } else if self.host_id.is_some() {
                self.note("start releasing host");
            } else {
                self.note("complete");
            }
        }
        if self.state == "start deleting instance" {
            self.calls.push("_deleteInstance".into());
            self.note("deleting instance");
        }
        if self.state == "deleting instance" {
            self.calls.push("_refreshDelete".into());
            if self.gone {
                if self.host_id.is_some() {
                    self.note("start releasing host");
                } else {
                    self.note("complete");
                }
            } else {
                return Ok(());
            }
        }
        if self.state == "start releasing host" {
            self.calls.push("_releaseHost".into());
            self.note("releasing host");
        }
        if self.state == "releasing host" {
            self.calls.push("_refreshDelete".into());
            if self.gone {
                self.note("complete");
            } else {
                return Ok(());
            }
        }
        if self.state == "complete" {
            self.complete = true;
        }
        Ok(())
    }

    fn advance_live(&mut self) -> Result<(), AdvanceError> {
        self.live_setup()?;
        if self.state == "start" {
            if self.instance_obj.is_some() {
                self.note("start deleting instance");
            } else if self.host_obj.is_some() {
                self.note("start releasing host");
            } else {
                self.note("complete");
            }
        }
        if self.state == "start deleting instance" {
            let id = self
                .instance_id
                .clone()
                .ok_or_else(|| AdvanceError::Other("missing instance id".into()))?;
            self.calls.push("_deleteInstance".into());
            self.instance_obj = endpoint::delete_instance(&self.provider, &id)?;
            self.note("deleting instance");
        }
        if self.state == "deleting instance" {
            if let Some(obj) = self.instance_obj.clone() {
                self.calls.push("_refreshDelete".into());
                self.instance_obj = endpoint::refresh_delete(&self.provider, &obj)?;
            }
            if self.instance_obj.is_none() {
                if self.host_obj.is_some() || self.host_id.is_some() {
                    // Re-check host presence for the release path.
                    if self.host_obj.is_none() {
                        if let Some(host_id) = self.host_id.clone() {
                            let obj = json!({ "HostId": host_id });
                            self.host_obj = endpoint::refresh_delete(&self.provider, &obj)?;
                            self.calls.push("_refreshDelete".into());
                        }
                    }
                    if self.host_obj.is_some() {
                        self.note("start releasing host");
                    } else {
                        self.note("complete");
                    }
                } else {
                    self.note("complete");
                }
            } else {
                self.export_updates();
                return Ok(());
            }
        }
        if self.state == "start releasing host" {
            let id = self
                .host_id
                .clone()
                .ok_or_else(|| AdvanceError::Other("missing host id".into()))?;
            self.calls.push("_releaseHost".into());
            self.host_obj = endpoint::release_host(&self.provider, &id)?;
            self.note("releasing host");
        }
        if self.state == "releasing host" {
            if let Some(obj) = self.host_obj.clone() {
                self.calls.push("_refreshDelete".into());
                self.host_obj = endpoint::refresh_delete(&self.provider, &obj)?;
            }
            if self.host_obj.is_none() {
                self.note("complete");
            } else {
                self.export_updates();
                return Ok(());
            }
        }
        if self.state == "complete" {
            self.complete = true;
        }
        self.export_updates();
        Ok(())
    }
}

impl StateMachine for AwsDelete {
    fn advance(&mut self) -> Result<(), AdvanceError> {
        if self.live || cloud::enabled() {
            self.live = true;
            if let Some(result) = endpoint::patched_advance(
                "aws",
                "delete",
                &self.state,
                self.complete,
                json!({
                    "provider": self.provider,
                    "node_uuid": self.node_uuid,
                }),
            )? {
                self.apply_patched(&result);
                return Ok(());
            }
            return self.advance_live();
        }
        self.advance_offline()
    }
    fn fail(&mut self) {
        // StateMachine.fail marks the machine complete so cleanup can
        // drop the node. DELETE_TIMEOUT and advance exceptions both
        // call this (server.py _cleanupNode).
        self.state = "complete".into();
        self.complete = true;
        self.export_updates();
    }
    fn complete(&self) -> bool { self.complete }
    fn state_name(&self) -> String { self.state.clone() }
    fn start_time(&self) -> f64 { self.start_time }
    fn set_start_time(&mut self, t: f64) { self.start_time = t; }
    fn calls(&self) -> Vec<String> { self.calls.clone() }
    fn node_updates(&self) -> Option<Value> {
        if self.live {
            self.updates.clone()
        } else {
            None
        }
    }
    fn to_dict(&self) -> Value { json!({"state": self.state, "start_time": self.start_time}) }
}

/// AWS snapshot from zuul/driver/aws/awsendpoint.py.
pub struct AwsSnapshot {
    pub state: String,
    pub start_time: f64,
    pub complete: bool,
    pub snapshot_id: Option<String>,
    pub external_id: Option<String>,
    pub volume_id: Option<String>,
    pub tags: Value,
    live: bool,
    pub provider: String,
    pub node_uuid: String,
    pub snapshot_obj: Option<Value>,
    pub account_id: Option<String>,
    pub region: Option<String>,
}

impl AwsSnapshot {
    pub fn new() -> Self {
        Self {
            state: "start".into(),
            start_time: 0.0,
            complete: false,
            snapshot_id: None,
            external_id: None,
            volume_id: None,
            tags: json!({}),
            live: cloud::enabled(),
            provider: String::new(),
            node_uuid: String::new(),
            snapshot_obj: None,
            account_id: None,
            region: None,
        }
    }

    /// The binary snapshot path is live even when no PyO3 handler is
    /// registered. `new` stays offline so driver_call_order is unchanged.
    pub fn enable_live(&mut self) {
        self.live = true;
    }
}

impl StateMachine for AwsSnapshot {
    fn advance(&mut self) -> Result<(), AdvanceError> {
        if self.live || cloud::enabled() {
            self.live = true;
            if let Some(result) = endpoint::patched_advance(
                "aws",
                "snapshot",
                &self.state,
                self.complete,
                json!({
                    "provider": self.provider,
                    "node_uuid": self.node_uuid,
                    "snapshot_id": self.snapshot_id,
                }),
            )? {
                if let Some(state) = result.get("state").and_then(|v| v.as_str()) {
                    self.state = state.to_string();
                }
                self.complete = result
                    .get("complete")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if let Some(ext) = result.get("external_id").and_then(|v| v.as_str()) {
                    self.external_id = Some(ext.to_string());
                }
                return Ok(());
            }
            if self.state == "start" {
                let volume_id = self
                    .volume_id
                    .clone()
                    .ok_or_else(|| AdvanceError::Other("missing volume id".into()))?;
                let snap = endpoint::create_snapshot(&self.provider, &volume_id, &self.tags)?;
                self.snapshot_id = snap
                    .get("SnapshotId")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                self.snapshot_obj = Some(snap);
                self.state = "snapshot pending".into();
            }
            if self.state == "snapshot pending" {
                let obj = self
                    .snapshot_obj
                    .clone()
                    .ok_or_else(|| AdvanceError::Other("missing snapshot".into()))?;
                let snap = endpoint::refresh_snapshot(&self.provider, &obj)?;
                self.snapshot_obj = Some(snap.clone());
                let state = snap
                    .get("State")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_lowercase();
                if state == "completed" {
                    self.state = "complete".into();
                } else if state == "pending" {
                    return Ok(());
                } else {
                    return Err(AdvanceError::Other(format!("Snapshot in {state} state")));
                }
            }
            if self.state == "complete" {
                if let (Some(region), Some(account), Some(snap_id)) =
                    (&self.region, &self.account_id, &self.snapshot_id)
                {
                    self.external_id = Some(format!(
                        "arn:aws:ec2:{region}:{account}:snapshot/{snap_id}"
                    ));
                }
                self.complete = true;
            }
            return Ok(());
        }
        // Offline stub.
        self.state = "complete".into();
        self.complete = true;
        self.external_id = Some("snap-stub".into());
        Ok(())
    }
    fn complete(&self) -> bool { self.complete }
    fn state_name(&self) -> String { self.state.clone() }
    fn start_time(&self) -> f64 { self.start_time }
    fn set_start_time(&mut self, t: f64) { self.start_time = t; }
    fn external_id(&self) -> Option<String> { self.external_id.clone() }
    fn to_dict(&self) -> Value {
        let mut m = Map::new();
        m.insert("state".into(), json!(self.state));
        m.insert("start_time".into(), json!(self.start_time));
        if let Some(id) = &self.snapshot_id {
            m.insert("snapshot_id".into(), json!(id));
        }
        Value::Object(m)
    }
}

/// Azure order from zuul/driver/azure/azureendpoint.py.
pub struct AzureCreate {
    pub state: String,
    pub start_time: f64,
    pub complete: bool,
    pub attempts: i64,
    pub image_external_id: Option<String>,
    pub calls: Vec<String>,
    pub visited: Vec<String>,
    create_future: bool,
    pub vm_ready: bool,
    pub nic_ready: bool,
    live: bool,
    provider: String,
    label_name: String,
    node_uuid: String,
    hostname: String,
    tags: Value,
    create_future_id: Option<String>,
    image_reference: Option<Value>,
    subnet_id: Option<String>,
    vm_obj: Option<Value>,
    nic_obj: Option<Value>,
    public_ipv4: Option<Value>,
    public_ipv6: Option<Value>,
    vm_id: Option<String>,
    interface_ip: Option<String>,
    quota: Option<Value>,
    updates: Option<Value>,
    patch_node: Option<Value>,
    setup_done: bool,
}

impl AzureCreate {
    pub fn new() -> Self {
        let mut machine = Self {
            state: "start".into(),
            start_time: 0.0,
            complete: false,
            attempts: 0,
            image_external_id: None,
            calls: Vec::new(),
            visited: vec!["start".into()],
            create_future: false,
            vm_ready: true,
            nic_ready: true,
            live: false,
            provider: String::new(),
            label_name: String::new(),
            node_uuid: String::new(),
            hostname: String::new(),
            tags: json!({}),
            create_future_id: None,
            image_reference: None,
            subnet_id: None,
            vm_obj: None,
            nic_obj: None,
            public_ipv4: None,
            public_ipv6: None,
            vm_id: None,
            interface_ip: None,
            quota: None,
            updates: None,
            patch_node: None,
            setup_done: true,
        };
        machine.calls.push("getQuotaForLabel".into());
        machine
    }

    pub fn from_provider(provider: &CloudProvider, node: &ProviderNode, label: &Label) -> Self {
        let mut machine = Self::new();
        machine.provider = provider.canonical_name.clone();
        machine.label_name = label.name.clone();
        machine.node_uuid = node.uuid();
        let uuid = node.uuid();
        let host_len = uuid.len().min(13);
        machine.hostname = format!("np{}", &uuid[..host_len]);
        machine.tags = node.fields.get("tags").cloned().unwrap_or_else(|| json!({}));
        machine.vm_id = node
            .fields
            .get("azure_vm_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        if let Some(cs) = node.fields.get("create_state") {
            if let Some(state) = cs.get("state").and_then(|v| v.as_str()) {
                if state != "start" {
                    machine.state = state.to_string();
                    machine.visited = vec![state.to_string()];
                }
            }
            machine.attempts = cs
                .get("attempts")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            if let Some(id) = cs.get("image_external_id").and_then(|v| v.as_str()) {
                machine.image_external_id = Some(id.to_string());
            }
            if let Some(t) = cs.get("start_time").and_then(|v| v.as_f64()) {
                machine.start_time = t;
            }
        }
        if machine.image_external_id.is_some() {
            machine.image_reference = Some(json!({
                "id": machine.image_external_id.clone().unwrap()
            }));
        }
        machine.live = true;
        machine.setup_done = !machine.live;
        if machine.live {
            machine.calls.clear();
            machine.visited = vec![machine.state.clone()];
        }
        machine
    }

    fn note(&mut self, state: &str) {
        self.state = state.into();
        self.visited.push(state.into());
    }

    fn live_setup(&mut self) -> Result<(), AdvanceError> {
        if self.setup_done {
            return Ok(());
        }
        let quota = endpoint::quota_for_label(&self.provider, &self.label_name, None)?;
        self.calls.push("getQuotaForLabel".into());
        self.quota = Some(quota);
        if self.state == "submit creating vm" {
            if let Some(id) = endpoint::azure_list_instance_id(&self.provider, &self.node_uuid)? {
                self.vm_id = Some(id);
                self.state = "creating vm".into();
                self.visited = vec![self.state.clone()];
            }
        }
        if let Some(vm_id) = self.vm_id.clone() {
            let obj = json!({
                "type": "Microsoft.Compute/virtualMachines",
                "id": vm_id,
            });
            self.vm_obj = Some(endpoint::refresh(&self.provider, &obj)?);
            self.calls.push("_refresh".into());
        }
        self.setup_done = true;
        self.export_updates();
        Ok(())
    }

    fn export_updates(&mut self) {
        let mut m = Map::new();
        m.insert("create_state".into(), self.to_dict());
        if let Some(id) = &self.vm_id {
            m.insert("azure_vm_id".into(), json!(id));
        }
        if let Some(ip) = &self.interface_ip {
            m.insert("interface_ip".into(), json!(ip));
        }
        if let Some(q) = &self.quota {
            if let Some(quota) = q.get("quota") {
                m.insert("quota".into(), quota.clone());
            } else {
                m.insert("quota".into(), q.clone());
            }
        }
        if let Some(Value::Object(patch)) = &self.patch_node {
            for (k, v) in patch {
                if k != "create_state" {
                    m.insert(k.clone(), v.clone());
                }
            }
        }
        self.updates = Some(Value::Object(m));
    }

    fn apply_patched(&mut self, result: &Value) {
        if let Some(state) = result.get("state").and_then(|v| v.as_str()) {
            self.state = state.to_string();
        }
        self.complete = result
            .get("complete")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if let Some(ip) = result.get("interface_ip").and_then(|v| v.as_str()) {
            self.interface_ip = Some(ip.to_string());
        }
        if let Some(id) = result.get("azure_vm_id").and_then(|v| v.as_str()) {
            self.vm_id = Some(id.to_string());
        }
        if let Some(node) = result.get("node") {
            self.updates = Some(node.clone());
        } else {
            self.export_updates();
        }
    }

    fn advance_offline(&mut self) -> Result<(), AdvanceError> {
        if self.state == "start" {
            self.calls.push("_getImageReferenceForImage".into());
            self.calls.push("_getSubnetIdForLabel".into());
            self.note("submit creating vm");
        }
        if self.state == "submit creating vm" {
            if !self.create_future {
                self.calls.push("_submitApi(_createVirtualMachine)".into());
                self.create_future = true;
            }
            self.calls.push("_completeApi".into());
            self.note("creating vm");
        }
        if self.state == "creating vm" {
            self.calls.push("_refresh".into());
            if self.vm_ready {
                self.calls.push("_succeeded".into());
                self.note("querying nic");
            } else {
                return Ok(());
            }
        }
        if self.state == "querying nic" {
            self.calls.push("_refresh".into());
            if self.nic_ready {
                self.calls.push("_succeeded".into());
                self.note("querying pip");
            } else {
                return Ok(());
            }
        }
        if self.state == "querying pip" {
            self.note("complete");
        }
        if self.state == "complete" {
            self.complete = true;
        }
        Ok(())
    }

    fn advance_live(&mut self) -> Result<(), AdvanceError> {
        self.live_setup()?;
        if self.state == "start" {
            if self.image_reference.is_none() {
                let image_ref = endpoint::azure_get_image_reference(
                    &self.provider,
                    &self.label_name,
                    self.image_external_id.as_deref(),
                )?;
                self.calls.push("_getImageReferenceForImage".into());
                self.image_reference = Some(image_ref);
            }
            if self.subnet_id.is_none() {
                let subnet = endpoint::azure_get_subnet_id(&self.provider, &self.label_name)?;
                self.calls.push("_getSubnetIdForLabel".into());
                self.subnet_id = Some(subnet);
            }
            self.note("submit creating vm");
        }
        if self.state == "submit creating vm" {
            if self.create_future_id.is_none() {
                let mut payload = json!({
                    "label": self.label_name,
                    "tags": self.tags,
                    "hostname": self.hostname,
                    "node_uuid": self.node_uuid,
                    "subnet_id": self.subnet_id,
                    "image_reference": self.image_reference,
                });
                if let Some(id) = &self.image_external_id {
                    payload
                        .as_object_mut()
                        .unwrap()
                        .insert("image_external_id".into(), json!(id));
                }
                let result = endpoint::azure_submit_create_vm(&self.provider, payload)?;
                self.calls.push("_submitApi(_createVirtualMachine)".into());
                self.create_future = true;
                self.create_future_id = result
                    .get("future_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
            }
            let future_id = self
                .create_future_id
                .clone()
                .ok_or_else(|| AdvanceError::Other("missing azure create future".into()))?;
            self.calls.push("_completeApi".into());
            let vm = endpoint::azure_complete_create_vm(&self.provider, &future_id)?;
            if vm.is_none() {
                self.export_updates();
                return Ok(());
            }
            let vm = vm.unwrap();
            self.vm_id = vm
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            self.vm_obj = Some(vm);
            self.note("creating vm");
        }
        if self.state == "creating vm" {
            let obj = self
                .vm_obj
                .clone()
                .ok_or_else(|| AdvanceError::Other("missing azure vm".into()))?;
            self.calls.push("_refresh".into());
            let vm = endpoint::refresh(&self.provider, &obj)?;
            self.vm_obj = Some(vm.clone());
            if endpoint::azure_succeeded(&vm) {
                self.calls.push("_succeeded".into());
                let mut nic = vm
                    .pointer("/properties/networkProfile/networkInterfaces/0")
                    .cloned()
                    .unwrap_or(Value::Null);
                if let Some(obj) = nic.as_object_mut() {
                    obj.insert(
                        "type".into(),
                        json!("Microsoft.Network/networkInterfaces"),
                    );
                }
                self.nic_obj = Some(nic);
                self.note("querying nic");
            } else if endpoint::azure_failed(&vm) {
                return Err(AdvanceError::Other("VM in failed state".into()));
            } else {
                self.export_updates();
                return Ok(());
            }
        }
        if self.state == "querying nic" {
            let nic = self
                .nic_obj
                .clone()
                .ok_or_else(|| AdvanceError::Other("missing azure nic".into()))?;
            self.calls.push("_refresh".into());
            let nic = endpoint::refresh_force(&self.provider, &nic, true)?;
            self.nic_obj = Some(nic.clone());
            if endpoint::azure_failed(&nic) {
                return Err(AdvanceError::Other("NIC in failed state".into()));
            } else if !endpoint::azure_succeeded(&nic) {
                self.export_updates();
                return Ok(());
            }
            self.calls.push("_succeeded".into());
            let mut all_found = true;
            if let Some(configs) = nic
                .pointer("/properties/ipConfigurations")
                .and_then(|v| v.as_array())
            {
                for ip_config in configs {
                    if ip_config
                        .pointer("/properties/privateIPAddress")
                        .is_none()
                    {
                        all_found = false;
                    }
                }
                if all_found {
                    for ip_config in configs {
                        let props = ip_config.get("properties");
                        let version = props
                            .and_then(|p| p.get("privateIPAddressVersion"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        if let Some(public_ip) = props.and_then(|p| p.get("publicIPAddress")) {
                            let mut public_ip = public_ip.clone();
                            if let Some(obj) = public_ip.as_object_mut() {
                                obj.insert(
                                    "type".into(),
                                    json!("Microsoft.Network/publicIPAddresses"),
                                );
                            }
                            if version == "IPv4" {
                                self.public_ipv4 = Some(public_ip);
                            } else if version == "IPv6" {
                                self.public_ipv6 = Some(public_ip);
                            }
                        }
                    }
                    self.note("querying pip");
                } else {
                    self.export_updates();
                    return Ok(());
                }
            } else {
                self.export_updates();
                return Ok(());
            }
        }
        if self.state == "querying pip" {
            let mut all_found = true;
            if let Some(pip) = self.public_ipv4.clone() {
                let pip = endpoint::refresh_force(&self.provider, &pip, true)?;
                self.public_ipv4 = Some(pip.clone());
                if pip.pointer("/properties/ipAddress").is_none() {
                    all_found = false;
                }
            }
            if let Some(pip) = self.public_ipv6.clone() {
                let pip = endpoint::refresh_force(&self.provider, &pip, true)?;
                self.public_ipv6 = Some(pip.clone());
                if pip.pointer("/properties/ipAddress").is_none() {
                    all_found = false;
                }
            }
            if !all_found {
                self.export_updates();
                return Ok(());
            }
            let result = endpoint::azure_update_node(
                &self.provider,
                json!({
                    "node_uuid": self.node_uuid,
                    "label": self.label_name,
                    "vm": self.vm_obj,
                    "nic": self.nic_obj,
                    "public_ipv4": self.public_ipv4,
                    "public_ipv6": self.public_ipv6,
                    "azure_vm_id": self.vm_id,
                    "quota": self.quota,
                }),
            )?;
            if let Some(node) = result.get("node") {
                if let Some(ip) = node.get("interface_ip").and_then(|v| v.as_str()) {
                    self.interface_ip = Some(ip.to_string());
                }
                self.patch_node = Some(node.clone());
            }
            self.note("complete");
            self.complete = true;
            self.export_updates();
            return Ok(());
        }
        if self.state == "complete" {
            self.complete = true;
        }
        self.export_updates();
        Ok(())
    }
}

impl StateMachine for AzureCreate {
    fn advance(&mut self) -> Result<(), AdvanceError> {
        if self.live || cloud::enabled() {
            self.live = true;
            if let Some(result) = endpoint::patched_advance(
                "azure",
                "create",
                &self.state,
                self.complete,
                json!({
                    "provider": self.provider,
                    "node_uuid": self.node_uuid,
                    "label": self.label_name,
                }),
            )? {
                self.apply_patched(&result);
                return Ok(());
            }
            return self.advance_live();
        }
        self.advance_offline()
    }
    fn complete(&self) -> bool { self.complete }
    fn state_name(&self) -> String { self.state.clone() }
    fn start_time(&self) -> f64 { self.start_time }
    fn set_start_time(&mut self, t: f64) { self.start_time = t; }
    fn external_id(&self) -> Option<String> { self.vm_id.clone() }
    fn interface_ip(&self) -> Option<String> { self.interface_ip.clone() }
    fn calls(&self) -> Vec<String> { self.calls.clone() }
    fn node_updates(&self) -> Option<Value> {
        if self.live { self.updates.clone() } else { None }
    }
    fn to_dict(&self) -> Value {
        let mut m = Map::new();
        m.insert("state".into(), json!(self.state));
        m.insert("start_time".into(), json!(self.start_time));
        m.insert("attempts".into(), json!(self.attempts));
        if let Some(id) = &self.image_external_id {
            m.insert("image_external_id".into(), json!(id));
        }
        Value::Object(m)
    }
}

pub struct AzureDelete {
    pub state: String,
    pub start_time: f64,
    pub complete: bool,
    pub calls: Vec<String>,
    pub visited: Vec<String>,
    pub vm_id: Option<String>,
    pub gone: bool,
    live: bool,
    provider: String,
    node_uuid: String,
    vm_obj: Option<Value>,
    updates: Option<Value>,
    setup_done: bool,
}

impl AzureDelete {
    pub fn new(vm_id: Option<String>) -> Self {
        Self {
            state: "start".into(),
            start_time: 0.0,
            complete: false,
            calls: Vec::new(),
            visited: vec!["start".into()],
            vm_id,
            gone: true,
            live: false,
            provider: String::new(),
            node_uuid: String::new(),
            vm_obj: None,
            updates: None,
            setup_done: true,
        }
    }

    pub fn from_provider(provider: &CloudProvider, node: &ProviderNode) -> Self {
        let vm_id = node
            .fields
            .get("azure_vm_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        let mut machine = Self::new(vm_id);
        machine.provider = provider.canonical_name.clone();
        machine.node_uuid = node.uuid();
        if let Some(ds) = node.fields.get("delete_state") {
            if let Some(state) = ds.get("state").and_then(|v| v.as_str()) {
                if state != "start" {
                    machine.state = state.to_string();
                    machine.visited = vec![state.to_string()];
                }
            }
            if let Some(t) = ds.get("start_time").and_then(|v| v.as_f64()) {
                machine.start_time = t;
            }
        }
        machine.live = true;
        machine.setup_done = !machine.live;
        if machine.live {
            machine.calls.clear();
        }
        machine
    }

    fn note(&mut self, state: &str) {
        self.state = state.into();
        self.visited.push(state.into());
    }

    fn live_setup(&mut self) -> Result<(), AdvanceError> {
        if self.setup_done {
            return Ok(());
        }
        self.setup_done = true;
        self.export_updates();
        Ok(())
    }

    fn export_updates(&mut self) {
        let mut m = Map::new();
        m.insert("delete_state".into(), self.to_dict());
        if let Some(id) = &self.vm_id {
            m.insert("azure_vm_id".into(), json!(id));
        }
        self.updates = Some(Value::Object(m));
    }

    fn apply_patched(&mut self, result: &Value) {
        if let Some(state) = result.get("state").and_then(|v| v.as_str()) {
            self.state = state.to_string();
        }
        self.complete = result
            .get("complete")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if let Some(node) = result.get("node") {
            self.updates = Some(node.clone());
        } else {
            self.export_updates();
        }
    }

    fn advance_offline(&mut self) -> Result<(), AdvanceError> {
        if self.state == "start" {
            self.calls.push("_deleteVirtualMachine".into());
            self.note("deleting vm");
        }
        if self.state == "deleting vm" {
            self.calls.push("_refreshDelete".into());
            if self.gone {
                self.note("complete");
            } else {
                return Ok(());
            }
        }
        if self.state == "complete" {
            self.complete = true;
        }
        Ok(())
    }

    fn advance_live(&mut self) -> Result<(), AdvanceError> {
        self.live_setup()?;
        if self.state == "start" {
            if let Some(vm_id) = self.vm_id.clone() {
                self.calls.push("_deleteVirtualMachine".into());
                self.vm_obj = endpoint::azure_delete_vm(&self.provider, &vm_id)?;
                self.note("deleting vm");
            } else {
                self.note("complete");
            }
        }
        if self.state == "deleting vm" {
            if let Some(obj) = self.vm_obj.clone() {
                self.calls.push("_refreshDelete".into());
                self.vm_obj = endpoint::refresh_delete(&self.provider, &obj)?;
            }
            if self.vm_obj.is_none() {
                self.note("complete");
            } else {
                self.export_updates();
                return Ok(());
            }
        }
        if self.state == "complete" {
            self.complete = true;
        }
        self.export_updates();
        Ok(())
    }
}

impl StateMachine for AzureDelete {
    fn advance(&mut self) -> Result<(), AdvanceError> {
        if self.live || cloud::enabled() {
            self.live = true;
            if let Some(result) = endpoint::patched_advance(
                "azure",
                "delete",
                &self.state,
                self.complete,
                json!({
                    "provider": self.provider,
                    "node_uuid": self.node_uuid,
                }),
            )? {
                self.apply_patched(&result);
                return Ok(());
            }
            return self.advance_live();
        }
        self.advance_offline()
    }
    fn fail(&mut self) {
        self.state = "complete".into();
        self.complete = true;
        self.export_updates();
    }
    fn complete(&self) -> bool { self.complete }
    fn state_name(&self) -> String { self.state.clone() }
    fn start_time(&self) -> f64 { self.start_time }
    fn set_start_time(&mut self, t: f64) { self.start_time = t; }
    fn calls(&self) -> Vec<String> { self.calls.clone() }
    fn node_updates(&self) -> Option<Value> {
        if self.live { self.updates.clone() } else { None }
    }
    fn to_dict(&self) -> Value { json!({"state": self.state, "start_time": self.start_time}) }
}


/// OpenStack order from zuul/driver/openstack/openstackendpoint.py.
pub struct OpenstackCreate {
    pub state: String,
    pub start_time: f64,
    pub complete: bool,
    pub attempts: i64,
    pub image_external_id: Option<String>,
    pub calls: Vec<String>,
    pub visited: Vec<String>,
    create_future: bool,
    pub active: bool,
    pub auto_floating_ip: bool,
    pub needs_floating_ip: bool,
    pub fip_active: bool,
    pub fip_attached: bool,
    live: bool,
    provider: String,
    label_name: String,
    node_uuid: String,
    hostname: String,
    tags: Value,
    create_future_id: Option<String>,
    server_id: Option<String>,
    floating_ip_id: Option<String>,
    server_obj: Option<Value>,
    floating_ip: Option<Value>,
    interface_ip: Option<String>,
    quota: Option<Value>,
    updates: Option<Value>,
    patch_node: Option<Value>,
    setup_done: bool,
}

impl OpenstackCreate {
    pub fn new() -> Self {
        let mut machine = Self {
            state: "start".into(),
            start_time: 0.0,
            complete: false,
            attempts: 0,
            image_external_id: None,
            calls: Vec::new(),
            visited: vec!["start".into()],
            create_future: false,
            active: true,
            auto_floating_ip: false,
            needs_floating_ip: false,
            fip_active: true,
            fip_attached: true,
            live: false,
            provider: String::new(),
            label_name: String::new(),
            node_uuid: String::new(),
            hostname: String::new(),
            tags: json!({}),
            create_future_id: None,
            server_id: None,
            floating_ip_id: None,
            server_obj: None,
            floating_ip: None,
            interface_ip: None,
            quota: None,
            updates: None,
            patch_node: None,
            setup_done: true,
        };
        machine.calls.push("_findFlavor".into());
        machine
    }

    pub fn from_provider(provider: &CloudProvider, node: &ProviderNode, label: &Label) -> Self {
        let mut machine = Self::new();
        machine.provider = provider.canonical_name.clone();
        machine.label_name = label.name.clone();
        machine.node_uuid = node.uuid();
        let uuid = node.uuid();
        let host_len = uuid.len().min(13);
        machine.hostname = format!("np{}", &uuid[..host_len]);
        machine.tags = node.fields.get("tags").cloned().unwrap_or_else(|| json!({}));
        machine.server_id = node
            .fields
            .get("openstack_server_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        machine.floating_ip_id = node
            .fields
            .get("openstack_floating_ip_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        machine.auto_floating_ip = label
            .raw
            .get("auto-floating-ip")
            .or_else(|| label.raw.get("auto_floating_ip"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if let Some(cs) = node.fields.get("create_state") {
            if let Some(state) = cs.get("state").and_then(|v| v.as_str()) {
                if state != "start" {
                    machine.state = state.to_string();
                    machine.visited = vec![state.to_string()];
                }
            }
            machine.attempts = cs
                .get("attempts")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            if let Some(id) = cs.get("image_external_id").and_then(|v| v.as_str()) {
                machine.image_external_id = Some(id.to_string());
            }
            if let Some(t) = cs.get("start_time").and_then(|v| v.as_f64()) {
                machine.start_time = t;
            }
        }
        machine.live = true;
        machine.setup_done = !machine.live;
        if machine.live {
            machine.calls.clear();
            machine.visited = vec![machine.state.clone()];
        }
        machine
    }

    fn note(&mut self, state: &str) {
        self.state = state.into();
        self.visited.push(state.into());
    }

    fn live_setup(&mut self) -> Result<(), AdvanceError> {
        if self.setup_done {
            return Ok(());
        }
        let flavor = endpoint::os_find_flavor(&self.provider, &self.label_name)?;
        self.calls.push("_findFlavor".into());
        if let Some(quota) = flavor.get("quota") {
            self.quota = Some(quota.clone());
        }
        if let Some(auto) = flavor.get("auto_floating_ip").and_then(|v| v.as_bool()) {
            self.auto_floating_ip = auto;
        }
        if self.state == "submit creating server" {
            if let Some(id) = endpoint::os_list_instance_id(&self.provider, &self.node_uuid)? {
                self.server_id = Some(id);
                self.state = "creating server".into();
                self.visited = vec![self.state.clone()];
            }
        }
        if let Some(server_id) = self.server_id.clone() {
            let obj = json!({ "id": server_id, "status": "_unknown" });
            self.server_obj = Some(endpoint::os_refresh_server(&self.provider, &obj)?);
        }
        if let Some(fip_id) = self.floating_ip_id.clone() {
            let obj = json!({ "id": fip_id });
            self.floating_ip = Some(endpoint::os_refresh_floating_ip(&self.provider, &obj)?);
        }
        self.setup_done = true;
        self.export_updates();
        Ok(())
    }

    fn export_updates(&mut self) {
        let mut m = Map::new();
        m.insert("create_state".into(), self.to_dict());
        if let Some(id) = &self.server_id {
            m.insert("openstack_server_id".into(), json!(id));
        }
        if let Some(id) = &self.floating_ip_id {
            m.insert("openstack_floating_ip_id".into(), json!(id));
        }
        if let Some(ip) = &self.interface_ip {
            m.insert("interface_ip".into(), json!(ip));
        }
        if let Some(q) = &self.quota {
            if let Some(quota) = q.get("quota") {
                m.insert("quota".into(), quota.clone());
            } else {
                m.insert("quota".into(), q.clone());
            }
        }
        if let Some(Value::Object(patch)) = &self.patch_node {
            for (k, v) in patch {
                if k != "create_state" {
                    m.insert(k.clone(), v.clone());
                }
            }
        }
        self.updates = Some(Value::Object(m));
    }

    fn apply_patched(&mut self, result: &Value) {
        if let Some(state) = result.get("state").and_then(|v| v.as_str()) {
            self.state = state.to_string();
        }
        self.complete = result
            .get("complete")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if let Some(ip) = result.get("interface_ip").and_then(|v| v.as_str()) {
            self.interface_ip = Some(ip.to_string());
        }
        if let Some(node) = result.get("node") {
            self.updates = Some(node.clone());
        } else {
            self.export_updates();
        }
    }

    fn finish_with_instance(&mut self) -> Result<(), AdvanceError> {
        let server = self
            .server_obj
            .clone()
            .ok_or_else(|| AdvanceError::Other("missing openstack server".into()))?;
        let result = endpoint::os_get_instance(
            &self.provider,
            json!({
                "node_uuid": self.node_uuid,
                "label": self.label_name,
                "server": server,
                "quota": self.quota,
                "openstack_server_id": self.server_id,
                "openstack_floating_ip_id": self.floating_ip_id,
            }),
        )?;
        self.calls.push("_getInstance".into());
        if let Some(node) = result.get("node") {
            if let Some(ip) = node.get("interface_ip").and_then(|v| v.as_str()) {
                self.interface_ip = Some(ip.to_string());
            }
            self.patch_node = Some(node.clone());
        }
        self.note("complete");
        self.complete = true;
        self.export_updates();
        Ok(())
    }

    fn advance_offline(&mut self) -> Result<(), AdvanceError> {
        if self.state == "start" {
            self.note("submit creating server");
        }
        if self.state == "submit creating server" {
            if !self.create_future {
                self.calls.push("_submitApi(_createServer)".into());
                self.create_future = true;
            }
            self.calls.push("_completeApi".into());
            self.note("creating server");
        }
        if self.state == "creating server" {
            self.calls.push("_refreshServer".into());
            if self.active {
                if self.auto_floating_ip && self.needs_floating_ip {
                    self.calls.push("_createFloatingIp".into());
                    self.note("creating floating ip");
                } else {
                    self.calls.push("_getInstance".into());
                    self.note("complete");
                }
            } else {
                return Ok(());
            }
        }
        if self.state == "creating floating ip" {
            self.calls.push("_refreshFloatingIp".into());
            if self.fip_active {
                self.calls.push("_attachIpToServer".into());
                self.note("attaching floating ip");
            } else {
                return Ok(());
            }
        }
        if self.state == "attaching floating ip" {
            self.calls.push("_refreshServer".into());
            if self.fip_attached {
                self.calls.push("_getInstance".into());
                self.note("complete");
            } else {
                return Ok(());
            }
        }
        if self.state == "complete" {
            self.complete = true;
        }
        Ok(())
    }

    fn advance_live(&mut self) -> Result<(), AdvanceError> {
        self.live_setup()?;
        if self.state == "start" {
            self.note("submit creating server");
        }
        if self.state == "submit creating server" {
            if self.create_future_id.is_none() {
                let mut payload = json!({
                    "label": self.label_name,
                    "hostname": self.hostname,
                    "tags": self.tags,
                    "node_uuid": self.node_uuid,
                });
                if let Some(id) = &self.image_external_id {
                    payload
                        .as_object_mut()
                        .unwrap()
                        .insert("image_external_id".into(), json!(id));
                }
                let result = endpoint::os_submit_create_server(&self.provider, payload)?;
                self.calls.push("_submitApi(_createServer)".into());
                self.create_future = true;
                self.create_future_id = result
                    .get("future_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
            }
            let future_id = self
                .create_future_id
                .clone()
                .ok_or_else(|| AdvanceError::Other("missing os create future".into()))?;
            self.calls.push("_completeApi".into());
            let result = endpoint::os_complete_create_server(&self.provider, &future_id)?;
            if let Some(id) = result.get("resource_id").and_then(|v| v.as_str()) {
                self.server_id = Some(id.to_string());
                self.export_updates();
                if result.get("quota_fault").and_then(|v| v.as_bool()) == Some(true) {
                    return Err(AdvanceError::Quota("Quota exceeded".into()));
                }
                let msg = result
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("OpenStack create failed");
                return Err(AdvanceError::Other(msg.to_string()));
            }
            match result.get("server") {
                None | Some(Value::Null) => {
                    self.export_updates();
                    return Ok(());
                }
                Some(server) => {
                    self.server_id = server
                        .get("id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    self.server_obj = Some(server.clone());
                    self.note("creating server");
                }
            }
        }
        if self.state == "creating server" {
            let obj = self
                .server_obj
                .clone()
                .ok_or_else(|| AdvanceError::Other("missing openstack server".into()))?;
            self.calls.push("_refreshServer".into());
            let server = endpoint::os_refresh_server(&self.provider, &obj)?;
            self.server_obj = Some(server.clone());
            let status = server
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_uppercase();
            if status == "ACTIVE" {
                let needs = if self.auto_floating_ip {
                    endpoint::os_needs_floating_ip(&self.provider, &server)?
                } else {
                    false
                };
                if self.auto_floating_ip && needs {
                    let fip = endpoint::os_create_floating_ip(&self.provider, &server)?;
                    self.calls.push("_createFloatingIp".into());
                    self.floating_ip_id = fip
                        .get("id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    self.floating_ip = Some(fip);
                    self.note("creating floating ip");
                } else {
                    return self.finish_with_instance();
                }
            } else if status == "ERROR" {
                let fault = server
                    .pointer("/fault/message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Server in error state");
                if let Some(server_id) = &self.server_id {
                    if endpoint::os_handle_server_fault(&self.provider, server_id)? {
                        return Err(AdvanceError::Quota("Quota exceeded".into()));
                    }
                }
                return Err(AdvanceError::Other(fault.to_string()));
            } else {
                self.export_updates();
                return Ok(());
            }
        }
        if self.state == "creating floating ip" {
            let fip = self
                .floating_ip
                .clone()
                .ok_or_else(|| AdvanceError::Other("missing floating ip".into()))?;
            self.calls.push("_refreshFloatingIp".into());
            let fip = endpoint::os_refresh_floating_ip(&self.provider, &fip)?;
            self.floating_ip = Some(fip.clone());
            if fip.get("port_id").and_then(|v| if v.is_null() { None } else { Some(v) }).is_some() {
                let status = fip
                    .get("status")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_uppercase();
                if status == "ACTIVE" {
                    self.note("attaching floating ip");
                } else {
                    self.export_updates();
                    return Ok(());
                }
            } else {
                let server = self
                    .server_obj
                    .clone()
                    .ok_or_else(|| AdvanceError::Other("missing openstack server".into()))?;
                endpoint::os_attach_ip(&self.provider, &server, &fip)?;
                self.calls.push("_attachIpToServer".into());
                self.note("attaching floating ip");
            }
        }
        if self.state == "attaching floating ip" {
            let obj = self
                .server_obj
                .clone()
                .ok_or_else(|| AdvanceError::Other("missing openstack server".into()))?;
            self.calls.push("_refreshServer".into());
            let server = endpoint::os_refresh_server(&self.provider, &obj)?;
            self.server_obj = Some(server.clone());
            let ext_ip = endpoint::os_server_floating_ip(&self.provider, &server)?;
            let expected = self
                .floating_ip
                .as_ref()
                .and_then(|f| f.get("floating_ip_address"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            if ext_ip == expected {
                return self.finish_with_instance();
            }
            self.export_updates();
            return Ok(());
        }
        if self.state == "complete" {
            self.complete = true;
        }
        self.export_updates();
        Ok(())
    }
}

impl StateMachine for OpenstackCreate {
    fn advance(&mut self) -> Result<(), AdvanceError> {
        if self.live || cloud::enabled() {
            self.live = true;
            if let Some(result) = endpoint::patched_advance(
                "openstack",
                "create",
                &self.state,
                self.complete,
                json!({
                    "provider": self.provider,
                    "node_uuid": self.node_uuid,
                    "label": self.label_name,
                }),
            )? {
                self.apply_patched(&result);
                return Ok(());
            }
            return self.advance_live();
        }
        self.advance_offline()
    }
    fn complete(&self) -> bool { self.complete }
    fn state_name(&self) -> String { self.state.clone() }
    fn start_time(&self) -> f64 { self.start_time }
    fn set_start_time(&mut self, t: f64) { self.start_time = t; }
    fn external_id(&self) -> Option<String> { self.server_id.clone() }
    fn interface_ip(&self) -> Option<String> { self.interface_ip.clone() }
    fn calls(&self) -> Vec<String> { self.calls.clone() }
    fn node_updates(&self) -> Option<Value> {
        if self.live { self.updates.clone() } else { None }
    }
    fn to_dict(&self) -> Value {
        let mut m = Map::new();
        m.insert("state".into(), json!(self.state));
        m.insert("start_time".into(), json!(self.start_time));
        m.insert("attempts".into(), json!(self.attempts));
        if let Some(id) = &self.image_external_id {
            m.insert("image_external_id".into(), json!(id));
        }
        Value::Object(m)
    }
}

pub struct OpenstackDelete {
    pub state: String,
    pub start_time: f64,
    pub complete: bool,
    pub calls: Vec<String>,
    pub visited: Vec<String>,
    pub server_id: Option<String>,
    pub floating_ips: bool,
    pub fips_down: bool,
    pub server_gone: bool,
    delete_future: bool,
    live: bool,
    provider: String,
    node_uuid: String,
    server_obj: Option<Value>,
    floating_ip_objs: Vec<Value>,
    delete_future_id: Option<String>,
    updates: Option<Value>,
    setup_done: bool,
}

impl OpenstackDelete {
    pub fn new(server_id: Option<String>, floating_ips: bool) -> Self {
        let mut machine = Self {
            state: "start".into(),
            start_time: 0.0,
            complete: false,
            calls: Vec::new(),
            visited: vec!["start".into()],
            server_id,
            floating_ips,
            fips_down: true,
            server_gone: true,
            delete_future: false,
            live: false,
            provider: String::new(),
            node_uuid: String::new(),
            server_obj: None,
            floating_ip_objs: Vec::new(),
            delete_future_id: None,
            updates: None,
            setup_done: true,
        };
        if machine.server_id.is_some() {
            machine.calls.push("_getServer".into());
        }
        machine
    }

    pub fn from_provider(provider: &CloudProvider, node: &ProviderNode) -> Self {
        let server_id = node
            .fields
            .get("openstack_server_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        let mut machine = Self::new(server_id, false);
        machine.provider = provider.canonical_name.clone();
        machine.node_uuid = node.uuid();
        if let Some(ds) = node.fields.get("delete_state") {
            if let Some(state) = ds.get("state").and_then(|v| v.as_str()) {
                if state != "start" {
                    machine.state = state.to_string();
                    machine.visited = vec![state.to_string()];
                }
            }
            if let Some(t) = ds.get("start_time").and_then(|v| v.as_f64()) {
                machine.start_time = t;
            }
        }
        machine.live = true;
        machine.setup_done = !machine.live;
        if machine.live {
            machine.calls.clear();
        }
        machine
    }

    fn note(&mut self, state: &str) {
        self.state = state.into();
        self.visited.push(state.into());
    }

    fn live_setup(&mut self) -> Result<(), AdvanceError> {
        if self.setup_done {
            return Ok(());
        }
        if let Some(server_id) = self.server_id.clone() {
            self.calls.push("_getServer".into());
            self.server_obj = endpoint::os_get_server(&self.provider, &server_id)?;
            if let Some(server) = self.server_obj.clone() {
                if endpoint::os_has_floating_ips(&self.provider)?
                    && server.get("addresses").is_some()
                {
                    self.floating_ip_objs =
                        endpoint::os_get_floating_ips(&self.provider, &server)?;
                    self.floating_ips = !self.floating_ip_objs.is_empty();
                }
            }
        }
        self.setup_done = true;
        self.export_updates();
        Ok(())
    }

    fn export_updates(&mut self) {
        let mut m = Map::new();
        m.insert("delete_state".into(), self.to_dict());
        if let Some(id) = &self.server_id {
            m.insert("openstack_server_id".into(), json!(id));
        }
        self.updates = Some(Value::Object(m));
    }

    fn apply_patched(&mut self, result: &Value) {
        if let Some(state) = result.get("state").and_then(|v| v.as_str()) {
            self.state = state.to_string();
        }
        self.complete = result
            .get("complete")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if let Some(node) = result.get("node") {
            self.updates = Some(node.clone());
        } else {
            self.export_updates();
        }
    }

    fn advance_offline(&mut self) -> Result<(), AdvanceError> {
        if self.state == "start" {
            if self.server_id.is_some() {
                if self.floating_ips {
                    self.calls.push("_deleteFloatingIp".into());
                    self.note("deleting floating ip");
                } else {
                    self.note("delete server");
                }
            } else {
                self.note("complete");
            }
        }
        if self.state == "deleting floating ip" {
            self.calls.push("_refreshFloatingIpDelete".into());
            if self.fips_down {
                self.note("delete server");
            } else {
                return Ok(());
            }
        }
        if self.state == "delete server" {
            if !self.delete_future {
                self.calls.push("_submitApi(_deleteServer)".into());
                self.delete_future = true;
            }
            self.calls.push("_completeApi".into());
            self.note("deleting server");
        }
        if self.state == "deleting server" {
            self.calls.push("_refreshServerDelete".into());
            if self.server_gone {
                self.note("complete");
            } else {
                return Ok(());
            }
        }
        if self.state == "complete" {
            self.complete = true;
        }
        Ok(())
    }

    fn advance_live(&mut self) -> Result<(), AdvanceError> {
        self.live_setup()?;
        if self.state == "start" {
            if self.server_id.is_some() {
                if !self.floating_ip_objs.is_empty() {
                    for fip in self.floating_ip_objs.clone() {
                        endpoint::os_delete_floating_ip(&self.provider, &fip)?;
                        self.calls.push("_deleteFloatingIp".into());
                    }
                    self.note("deleting floating ip");
                } else {
                    self.note("delete server");
                }
            } else {
                self.note("complete");
            }
        }
        if self.state == "deleting floating ip" {
            let mut remaining = Vec::new();
            for fip in self.floating_ip_objs.clone() {
                self.calls.push("_refreshFloatingIpDelete".into());
                if let Some(fip) = endpoint::os_refresh_floating_ip_delete(&self.provider, &fip)? {
                    let status = fip
                        .get("status")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    if status != "DOWN" {
                        remaining.push(fip);
                    }
                }
            }
            self.floating_ip_objs = remaining;
            if !self.floating_ip_objs.is_empty() {
                self.export_updates();
                return Ok(());
            }
            self.note("delete server");
        }
        if self.state == "delete server" {
            if self.delete_future_id.is_none() {
                let server_id = self
                    .server_id
                    .clone()
                    .ok_or_else(|| AdvanceError::Other("missing openstack server id".into()))?;
                let result = endpoint::os_submit_delete_server(&self.provider, &server_id)?;
                self.calls.push("_submitApi(_deleteServer)".into());
                self.delete_future = true;
                self.delete_future_id = result
                    .get("future_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
            }
            let future_id = self
                .delete_future_id
                .clone()
                .ok_or_else(|| AdvanceError::Other("missing os delete future".into()))?;
            self.calls.push("_completeApi".into());
            if endpoint::os_complete_delete_server(&self.provider, &future_id)? {
                self.note("deleting server");
            } else {
                self.export_updates();
                return Ok(());
            }
        }
        if self.state == "deleting server" {
            if let Some(server) = self.server_obj.clone() {
                self.calls.push("_refreshServerDelete".into());
                self.server_obj = endpoint::os_refresh_server_delete(&self.provider, &server)?;
            }
            if self.server_obj.is_none() {
                self.note("complete");
            } else {
                self.export_updates();
                return Ok(());
            }
        }
        if self.state == "complete" {
            self.complete = true;
        }
        self.export_updates();
        Ok(())
    }
}

impl StateMachine for OpenstackDelete {
    fn advance(&mut self) -> Result<(), AdvanceError> {
        if self.live || cloud::enabled() {
            self.live = true;
            if let Some(result) = endpoint::patched_advance(
                "openstack",
                "delete",
                &self.state,
                self.complete,
                json!({
                    "provider": self.provider,
                    "node_uuid": self.node_uuid,
                }),
            )? {
                self.apply_patched(&result);
                return Ok(());
            }
            return self.advance_live();
        }
        self.advance_offline()
    }
    fn fail(&mut self) {
        self.state = "complete".into();
        self.complete = true;
        self.export_updates();
    }
    fn complete(&self) -> bool { self.complete }
    fn state_name(&self) -> String { self.state.clone() }
    fn start_time(&self) -> f64 { self.start_time }
    fn set_start_time(&mut self, t: f64) { self.start_time = t; }
    fn calls(&self) -> Vec<String> { self.calls.clone() }
    fn node_updates(&self) -> Option<Value> {
        if self.live { self.updates.clone() } else { None }
    }
    fn to_dict(&self) -> Value { json!({"state": self.state, "start_time": self.start_time}) }
}


/// Kubernetes order from zuul/driver/kubernetes/kubernetesendpoint.py.
pub struct K8sCreate {
    pub state: String,
    pub start_time: f64,
    pub complete: bool,
    pub calls: Vec<String>,
    pub visited: Vec<String>,
    pub use_openshift_projects: bool,
    /// "pod" or "namespace".
    pub kind: String,
    pub access_allowed: bool,
    pub token_ready: bool,
    pub pod_ready: bool,
    live: bool,
    provider: String,
    label_name: String,
    node_uuid: String,
    hostname: String,
    tags: Value,
    namespace: String,
    user: String,
    restricted_access: bool,
    containers: Option<Value>,
    namespace_id: Option<String>,
    kubernetes_connection: Map<String, Value>,
    quota: Option<Value>,
    updates: Option<Value>,
    patch_node: Option<Value>,
    setup_done: bool,
}

impl K8sCreate {
    pub fn new() -> Self {
        Self {
            state: "start".into(),
            start_time: 0.0,
            complete: false,
            calls: Vec::new(),
            visited: vec!["start".into()],
            use_openshift_projects: false,
            kind: "pod".into(),
            access_allowed: true,
            token_ready: true,
            pod_ready: true,
            live: false,
            provider: String::new(),
            label_name: String::new(),
            node_uuid: String::new(),
            hostname: String::new(),
            tags: json!({}),
            namespace: String::new(),
            user: "zuul-worker".into(),
            restricted_access: true,
            containers: None,
            namespace_id: None,
            kubernetes_connection: Map::new(),
            quota: None,
            updates: None,
            patch_node: None,
            setup_done: true,
        }
    }

    pub fn from_provider(provider: &CloudProvider, node: &ProviderNode, label: &Label) -> Self {
        let mut machine = Self::new();
        machine.provider = provider.canonical_name.clone();
        machine.label_name = label.name.clone();
        machine.node_uuid = node.uuid();
        let uuid = node.uuid();
        let host_len = uuid.len().min(13);
        machine.hostname = format!("np{}", &uuid[..host_len]);
        machine.namespace = machine.hostname.clone();
        machine.tags = node.fields.get("tags").cloned().unwrap_or_else(|| json!({}));
        machine.kind = label
            .raw
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or("pod")
            .to_string();
        machine.restricted_access = machine.kind == "pod";
        if let Some(cs) = node.fields.get("create_state") {
            if let Some(state) = cs.get("state").and_then(|v| v.as_str()) {
                if state != "start" {
                    machine.state = state.to_string();
                    machine.visited = vec![state.to_string()];
                }
            }
            if let Some(t) = cs.get("start_time").and_then(|v| v.as_f64()) {
                machine.start_time = t;
            }
        }
        machine.namespace_id = node
            .fields
            .get("kubernetes_namespace_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        if let Some(conn) = node
            .fields
            .get("kubernetes_connection")
            .and_then(|v| v.as_object())
        {
            machine.kubernetes_connection = conn.clone();
        }
        machine.live = true;
        machine.setup_done = !machine.live;
        if machine.live {
            machine.calls.clear();
            machine.visited = vec![machine.state.clone()];
        }
        machine
    }

    fn note(&mut self, state: &str) {
        self.state = state.into();
        self.visited.push(state.into());
    }

    fn live_setup(&mut self) -> Result<(), AdvanceError> {
        if self.setup_done {
            return Ok(());
        }
        let setup = endpoint::k8s_setup(&self.provider, &self.label_name)?;
        self.use_openshift_projects = setup
            .get("use_openshift_projects")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if let Some(kind) = setup.get("kind").and_then(|v| v.as_str()) {
            self.kind = kind.to_string();
            self.restricted_access = self.kind == "pod";
        }
        if let Some(quota) = setup.get("quota") {
            self.quota = Some(quota.clone());
        }
        self.containers = setup.get("containers").cloned();
        self.kubernetes_connection
            .insert("user".into(), json!(self.user));
        if let Some(host) = setup.get("host") {
            self.kubernetes_connection.insert("host".into(), host.clone());
        }
        if let Some(skiptls) = setup.get("skiptls") {
            self.kubernetes_connection
                .insert("skiptls".into(), skiptls.clone());
        }
        self.kubernetes_connection
            .insert("namespace".into(), json!(self.namespace));
        if self.kind == "pod" {
            self.kubernetes_connection
                .insert("pod".into(), json!(self.namespace));
        } else {
            self.kubernetes_connection
                .insert("pod".into(), Value::Null);
        }
        self.kubernetes_connection.insert(
            "containers".into(),
            self.containers.clone().unwrap_or(Value::Null),
        );
        self.setup_done = true;
        self.export_updates();
        Ok(())
    }

    fn export_updates(&mut self) {
        let mut m = Map::new();
        m.insert("create_state".into(), self.to_dict());
        if let Some(id) = &self.namespace_id {
            m.insert("kubernetes_namespace_id".into(), json!(id));
        }
        m.insert(
            "kubernetes_connection".into(),
            Value::Object(self.kubernetes_connection.clone()),
        );
        if let Some(q) = &self.quota {
            if let Some(quota) = q.get("quota") {
                m.insert("quota".into(), quota.clone());
            } else {
                m.insert("quota".into(), q.clone());
            }
        }
        if let Some(Value::Object(patch)) = &self.patch_node {
            for (k, v) in patch {
                if k != "create_state" {
                    m.insert(k.clone(), v.clone());
                }
            }
        }
        self.updates = Some(Value::Object(m));
    }

    fn apply_patched(&mut self, result: &Value) {
        if let Some(state) = result.get("state").and_then(|v| v.as_str()) {
            self.state = state.to_string();
        }
        self.complete = result
            .get("complete")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if let Some(node) = result.get("node") {
            self.updates = Some(node.clone());
        } else {
            self.export_updates();
        }
    }

    fn finish_with_instance(&mut self) -> Result<(), AdvanceError> {
        let result = endpoint::k8s_update_node(
            &self.provider,
            json!({
                "node_uuid": self.node_uuid,
                "label": self.label_name,
                "hostname": self.hostname,
                "kind": self.kind,
                "tags": self.tags,
                "quota": self.quota,
                "kubernetes_namespace_id": self.namespace_id,
                "kubernetes_connection": self.kubernetes_connection,
            }),
        )?;
        self.calls.push("updateFromInstance".into());
        if let Some(node) = result.get("node") {
            self.patch_node = Some(node.clone());
        }
        self.note("complete");
        self.complete = true;
        self.export_updates();
        Ok(())
    }

    fn advance_offline(&mut self) -> Result<(), AdvanceError> {
        if self.state == "start" {
            if self.use_openshift_projects {
                self.calls.push("_createProject".into());
            } else {
                self.calls.push("_createNamespace".into());
            }
            self.note("creating namespace");
        }
        if self.state == "creating namespace" {
            self.calls.push("_getSubjectAccessReview".into());
            if !self.access_allowed {
                return Ok(());
            }
            self.calls.push("_createImagePullSecrets".into());
            self.calls.push("_createServiceAccount".into());
            self.note("creating service account");
        }
        if self.state == "creating service account" {
            self.calls.push("_getToken".into());
            if !self.token_ready {
                return Ok(());
            }
            self.calls.push("_createRole".into());
            self.note("creating rbac");
        }
        if self.state == "creating rbac" {
            if self.kind == "pod" {
                self.calls.push("_createPod".into());
                self.note("creating pod");
            } else {
                self.calls.push("updateFromInstance".into());
                self.note("complete");
            }
        }
        if self.state == "creating pod" {
            self.calls.push("_getPod".into());
            if self.pod_ready {
                self.calls.push("updateFromInstance".into());
                self.note("complete");
            } else {
                return Ok(());
            }
        }
        if self.state == "complete" {
            self.complete = true;
        }
        Ok(())
    }

    fn advance_live(&mut self) -> Result<(), AdvanceError> {
        self.live_setup()?;
        if self.state == "start" {
            if self.use_openshift_projects {
                endpoint::k8s_create_project(&self.provider, &self.hostname, &self.tags)?;
                self.calls.push("_createProject".into());
            } else {
                endpoint::k8s_create_namespace(&self.provider, &self.hostname, &self.tags)?;
                self.calls.push("_createNamespace".into());
            }
            self.note("creating namespace");
        }
        if self.state == "creating namespace" {
            self.calls.push("_getSubjectAccessReview".into());
            match endpoint::k8s_get_subject_access_review(&self.provider, &self.namespace)? {
                None => {
                    self.export_updates();
                    return Ok(());
                }
                Some(false) => {
                    self.export_updates();
                    return Ok(());
                }
                Some(true) => {}
            }
            endpoint::k8s_create_image_pull_secrets(
                &self.provider,
                &self.namespace,
                &self.label_name,
            )?;
            self.calls.push("_createImagePullSecrets".into());
            endpoint::k8s_create_service_account(
                &self.provider,
                &self.namespace,
                &self.user,
            )?;
            self.calls.push("_createServiceAccount".into());
            self.namespace_id = Some(self.namespace.clone());
            self.note("creating service account");
        }
        if self.state == "creating service account" {
            self.calls.push("_getToken".into());
            match endpoint::k8s_get_token(&self.provider, &self.namespace, &self.user)? {
                None => {
                    self.export_updates();
                    return Ok(());
                }
                Some((token, ca_crt)) => {
                    self.kubernetes_connection
                        .insert("token".into(), json!(token));
                    let skiptls = self
                        .kubernetes_connection
                        .get("skiptls")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    if !skiptls {
                        if let Some(ca) = ca_crt {
                            self.kubernetes_connection
                                .insert("ca_crt".into(), json!(ca));
                        }
                    }
                }
            }
            endpoint::k8s_create_role(
                &self.provider,
                &self.namespace,
                &self.user,
                self.restricted_access,
            )?;
            self.calls.push("_createRole".into());
            self.note("creating rbac");
        }
        if self.state == "creating rbac" {
            if self.kind == "pod" {
                endpoint::k8s_create_pod(
                    &self.provider,
                    &self.namespace,
                    &self.hostname,
                    &self.label_name,
                    &self.tags,
                )?;
                self.calls.push("_createPod".into());
                self.note("creating pod");
            } else {
                return self.finish_with_instance();
            }
        }
        if self.state == "creating pod" {
            self.calls.push("_getPod".into());
            if !endpoint::k8s_get_pod(&self.provider, &self.namespace, &self.hostname)? {
                self.export_updates();
                return Ok(());
            }
            return self.finish_with_instance();
        }
        if self.state == "complete" {
            self.complete = true;
        }
        self.export_updates();
        Ok(())
    }
}

impl StateMachine for K8sCreate {
    fn advance(&mut self) -> Result<(), AdvanceError> {
        if self.live || cloud::enabled() {
            self.live = true;
            if let Some(result) = endpoint::patched_advance(
                "kubernetes",
                "create",
                &self.state,
                self.complete,
                json!({
                    "provider": self.provider,
                    "node_uuid": self.node_uuid,
                    "label": self.label_name,
                }),
            )? {
                self.apply_patched(&result);
                return Ok(());
            }
            return self.advance_live();
        }
        self.advance_offline()
    }
    fn complete(&self) -> bool { self.complete }
    fn state_name(&self) -> String { self.state.clone() }
    fn start_time(&self) -> f64 { self.start_time }
    fn set_start_time(&mut self, t: f64) { self.start_time = t; }
    fn external_id(&self) -> Option<String> { self.namespace_id.clone() }
    fn calls(&self) -> Vec<String> { self.calls.clone() }
    fn node_updates(&self) -> Option<Value> {
        if self.live { self.updates.clone() } else { None }
    }
    fn to_dict(&self) -> Value { json!({"state": self.state, "start_time": self.start_time}) }
}

pub struct K8sDelete {
    pub state: String,
    pub start_time: f64,
    pub complete: bool,
    pub calls: Vec<String>,
    pub visited: Vec<String>,
    pub namespace_id: Option<String>,
    pub use_openshift_projects: bool,
    live: bool,
    provider: String,
    node_uuid: String,
    updates: Option<Value>,
    setup_done: bool,
}

impl K8sDelete {
    pub fn new(namespace_id: Option<String>) -> Self {
        Self {
            state: "start".into(),
            start_time: 0.0,
            complete: false,
            calls: Vec::new(),
            visited: vec!["start".into()],
            namespace_id,
            use_openshift_projects: false,
            live: false,
            provider: String::new(),
            node_uuid: String::new(),
            updates: None,
            setup_done: true,
        }
    }

    pub fn from_provider(provider: &CloudProvider, node: &ProviderNode) -> Self {
        let namespace_id = node
            .fields
            .get("kubernetes_namespace_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        let mut machine = Self::new(namespace_id);
        machine.provider = provider.canonical_name.clone();
        machine.node_uuid = node.uuid();
        if let Some(ds) = node.fields.get("delete_state") {
            if let Some(state) = ds.get("state").and_then(|v| v.as_str()) {
                if state != "start" {
                    machine.state = state.to_string();
                    machine.visited = vec![state.to_string()];
                }
            }
            if let Some(t) = ds.get("start_time").and_then(|v| v.as_f64()) {
                machine.start_time = t;
            }
        }
        machine.live = true;
        machine.setup_done = !machine.live;
        if machine.live {
            machine.calls.clear();
        }
        machine
    }

    fn note(&mut self, state: &str) {
        self.state = state.into();
        self.visited.push(state.into());
    }

    fn live_setup(&mut self) -> Result<(), AdvanceError> {
        if self.setup_done {
            return Ok(());
        }
        self.use_openshift_projects = endpoint::k8s_use_openshift(&self.provider)?;
        self.setup_done = true;
        self.export_updates();
        Ok(())
    }

    fn export_updates(&mut self) {
        let mut m = Map::new();
        m.insert("delete_state".into(), self.to_dict());
        if let Some(id) = &self.namespace_id {
            m.insert("kubernetes_namespace_id".into(), json!(id));
        }
        self.updates = Some(Value::Object(m));
    }

    fn apply_patched(&mut self, result: &Value) {
        if let Some(state) = result.get("state").and_then(|v| v.as_str()) {
            self.state = state.to_string();
        }
        self.complete = result
            .get("complete")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if let Some(node) = result.get("node") {
            self.updates = Some(node.clone());
        } else {
            self.export_updates();
        }
    }

    fn advance_offline(&mut self) -> Result<(), AdvanceError> {
        if self.state == "start" {
            if self.namespace_id.is_some() {
                if self.use_openshift_projects {
                    self.calls.push("_deleteProject".into());
                } else {
                    self.calls.push("_deleteNamespace".into());
                }
                self.note("deleting namespace");
            } else {
                self.note("complete");
            }
        }
        if self.state == "deleting namespace" {
            self.note("complete");
        }
        if self.state == "complete" {
            self.complete = true;
        }
        Ok(())
    }

    fn advance_live(&mut self) -> Result<(), AdvanceError> {
        self.live_setup()?;
        if self.state == "start" {
            if let Some(namespace_id) = self.namespace_id.clone() {
                if self.use_openshift_projects {
                    endpoint::k8s_delete_project(&self.provider, &namespace_id)?;
                    self.calls.push("_deleteProject".into());
                } else {
                    endpoint::k8s_delete_namespace(&self.provider, &namespace_id)?;
                    self.calls.push("_deleteNamespace".into());
                }
                self.note("deleting namespace");
            } else {
                self.note("complete");
            }
        }
        if self.state == "deleting namespace" {
            self.note("complete");
        }
        if self.state == "complete" {
            self.complete = true;
        }
        self.export_updates();
        Ok(())
    }
}

impl StateMachine for K8sDelete {
    fn advance(&mut self) -> Result<(), AdvanceError> {
        if self.live || cloud::enabled() {
            self.live = true;
            if let Some(result) = endpoint::patched_advance(
                "kubernetes",
                "delete",
                &self.state,
                self.complete,
                json!({
                    "provider": self.provider,
                    "node_uuid": self.node_uuid,
                }),
            )? {
                self.apply_patched(&result);
                return Ok(());
            }
            return self.advance_live();
        }
        self.advance_offline()
    }
    fn fail(&mut self) {
        self.state = "complete".into();
        self.complete = true;
        self.export_updates();
    }
    fn complete(&self) -> bool { self.complete }
    fn state_name(&self) -> String { self.state.clone() }
    fn start_time(&self) -> f64 { self.start_time }
    fn set_start_time(&mut self, t: f64) { self.start_time = t; }
    fn calls(&self) -> Vec<String> { self.calls.clone() }
    fn node_updates(&self) -> Option<Value> {
        if self.live { self.updates.clone() } else { None }
    }
    fn to_dict(&self) -> Value { json!({"state": self.state, "start_time": self.start_time}) }
}


/// Static order from zuul/driver/static/staticendpoint.py.
/// Create finishes in the constructor via updateFromInstance.
pub const DELETE_RECYCLE_TIME: f64 = 300.0;

pub struct StaticCreate {
    pub state: String,
    pub start_time: f64,
    pub complete: bool,
    pub calls: Vec<String>,
    pub ip: Option<String>,
}

impl StaticCreate {
    pub fn new() -> Self {
        Self {
            state: "complete".into(),
            start_time: 0.0,
            complete: true,
            calls: vec!["getQuotaForLabel".into(), "updateFromInstance".into()],
            ip: None,
        }
    }
}

impl StateMachine for StaticCreate {
    fn advance(&mut self) -> Result<(), AdvanceError> {
        if self.state == "complete" {
            self.complete = true;
        }
        Ok(())
    }
    fn complete(&self) -> bool { self.complete }
    fn state_name(&self) -> String { self.state.clone() }
    fn start_time(&self) -> f64 { self.start_time }
    fn set_start_time(&mut self, t: f64) { self.start_time = t; }
    fn interface_ip(&self) -> Option<String> { self.ip.clone() }
    fn calls(&self) -> Vec<String> { self.calls.clone() }
    fn to_dict(&self) -> Value { json!({"state": self.state, "start_time": self.start_time}) }
}

pub struct StaticDelete {
    pub state: String,
    pub start_time: f64,
    pub complete: bool,
    pub calls: Vec<String>,
    pub reuse: bool,
    pub now: f64,
    /// fail() sets the node to REBUILDING when the node is reusable.
    pub rebuild: bool,
}

impl StaticDelete {
    pub fn new(reuse: bool) -> Self {
        Self {
            state: "start".into(),
            start_time: 0.0,
            complete: false,
            calls: Vec::new(),
            reuse,
            now: 0.0,
            rebuild: false,
        }
    }

    fn fail(&mut self) {
        self.calls.push("fail".into());
        if self.reuse {
            self.rebuild = true;
        }
        self.state = "complete".into();
        self.complete = true;
    }
}

impl StateMachine for StaticDelete {
    fn advance(&mut self) -> Result<(), AdvanceError> {
        if self.state == "start" {
            if self.reuse {
                if self.now - self.start_time > DELETE_RECYCLE_TIME {
                    self.fail();
                }
            } else {
                self.fail();
            }
        }
        if self.state == "complete" {
            self.complete = true;
        }
        Ok(())
    }
    fn complete(&self) -> bool { self.complete }
    fn state_name(&self) -> String { self.state.clone() }
    fn start_time(&self) -> f64 { self.start_time }
    fn set_start_time(&mut self, t: f64) { self.start_time = t; }
    fn set_now(&mut self, t: f64) { self.now = t; }
    fn calls(&self) -> Vec<String> { self.calls.clone() }
    fn to_dict(&self) -> Value { json!({"state": self.state, "start_time": self.start_time}) }
}

pub fn driver_create(driver: &str) -> Box<dyn StateMachine> {
    match driver {
        "aws" => Box::new(AwsCreate::new(false)),
        "azure" => Box::new(AzureCreate::new()),
        "openstack" => Box::new(OpenstackCreate::new()),
        "kubernetes" => Box::new(K8sCreate::new()),
        _ => Box::new(StaticCreate::new()),
    }
}

pub fn driver_delete(driver: &str) -> Box<dyn StateMachine> {
    match driver {
        "aws" => Box::new(AwsDelete::new(Some("i-stub".into()), None)),
        "azure" => Box::new(AzureDelete::new(Some("vm-stub".into()))),
        "openstack" => Box::new(OpenstackDelete::new(Some("server-stub".into()), false)),
        "kubernetes" => Box::new(K8sDelete::new(Some("ns-stub".into()))),
        _ => Box::new(StaticDelete::new(false)),
    }
}

/// Legacy full-SM adapter. Unused now that Azure / OpenStack /
/// Kubernetes drive fine-grained ep.* ops like AWS. Kept so leftover
/// step/fail bootstrap handlers remain reachable if needed.
pub struct RemoteMachine {
    pub kind: String,
    pub driver: String,
    pub provider: String,
    pub node_uuid: String,
    pub label: String,
    pub state: String,
    pub start_time: f64,
    pub complete: bool,
    pub interface_ip: Option<String>,
    pub updates: Option<Value>,
}

impl RemoteMachine {
    pub fn new(kind: &str, driver: &str, provider: &str, node: &ProviderNode, label: &Label) -> Self {
        Self {
            kind: kind.into(),
            driver: driver.into(),
            provider: provider.into(),
            node_uuid: node.uuid(),
            label: label.name.clone(),
            state: "start".into(),
            start_time: 0.0,
            complete: false,
            interface_ip: None,
            updates: None,
        }
    }
}

impl StateMachine for RemoteMachine {
    fn advance(&mut self) -> Result<(), AdvanceError> {
        let result = cloud::invoke(
            "step",
            &json!({
                "kind": self.kind,
                "driver": self.driver,
                "provider": self.provider,
                "node_uuid": self.node_uuid,
                "label": self.label,
            }),
        )?;
        if let Some(state) = result.get("state").and_then(|v| v.as_str()) {
            self.state = state.to_string();
        }
        self.complete = result.get("complete").and_then(|v| v.as_bool()).unwrap_or(false);
        self.interface_ip = result
            .get("interface_ip")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        self.updates = result.get("node").cloned();
        Ok(())
    }
    fn fail(&mut self) {
        let result = cloud::invoke(
            "fail",
            &json!({
                "kind": self.kind,
                "driver": self.driver,
                "provider": self.provider,
                "node_uuid": self.node_uuid,
                "label": self.label,
            }),
        );
        self.complete = true;
        self.state = "complete".into();
        if let Ok(value) = result {
            if let Some(state) = value.get("state").and_then(|v| v.as_str()) {
                self.state = state.to_string();
            }
            self.complete = value.get("complete").and_then(|v| v.as_bool()).unwrap_or(true);
            self.updates = value.get("node").cloned();
        }
    }
    fn complete(&self) -> bool { self.complete }
    fn state_name(&self) -> String { self.state.clone() }
    fn start_time(&self) -> f64 { self.start_time }
    fn set_start_time(&mut self, t: f64) { self.start_time = t; }
    fn interface_ip(&self) -> Option<String> { self.interface_ip.clone() }
    fn node_updates(&self) -> Option<Value> { self.updates.clone() }
    fn to_dict(&self) -> Value { json!({"state": self.state, "start_time": self.start_time}) }
}

/// Cloud-backed provider. AWS / Azure / OpenStack / Kubernetes create and
/// delete state machines run in Rust and call fine-grained endpoint ops
/// through `cloud`. Static stays fully Rust.
pub struct CloudProvider {
    pub name: String,
    pub canonical_name: String,
    pub driver_name: String,
    pub connection_name: String,
    pub endpoint_info: EndpointInfo,
    pub labels: BTreeMap<String, Label>,
    pub images: BTreeMap<String, Image>,
    pub flavors: BTreeMap<String, Flavor>,
    pub launch_attempts: i64,
    pub launch_timeout: Option<f64>,
    pub resource_limits: BTreeMap<String, f64>,
    pub region: Option<String>,
    pub options: BTreeMap<String, String>,
    pub static_nodes: BTreeMap<String, StaticNodeConfig>,
}

impl Provider for CloudProvider {
    fn name(&self) -> &str { &self.name }
    fn canonical_name(&self) -> &str { &self.canonical_name }
    fn driver_name(&self) -> &str { &self.driver_name }
    fn connection_name(&self) -> &str { &self.connection_name }
    fn endpoint(&self) -> &EndpointInfo { &self.endpoint_info }
    fn labels(&self) -> &BTreeMap<String, Label> { &self.labels }
    fn images(&self) -> &BTreeMap<String, Image> { &self.images }
    fn flavors(&self) -> &BTreeMap<String, Flavor> { &self.flavors }
    fn launch_attempts(&self) -> i64 { self.launch_attempts }
    fn launch_timeout(&self) -> Option<f64> { self.launch_timeout }
    fn resource_limits(&self) -> &BTreeMap<String, f64> { &self.resource_limits }
    fn quota_for_label(&self, _label: &Label) -> QuotaInformation {
        let mut q = QuotaInformation::default();
        q.quota.insert("instances".into(), 1.0);
        q
    }
    fn can_reuse(&self, node: &ProviderNode) -> bool {
        // StaticProvider.canReuseNode: only nodes still in provider.nodes.
        if self.driver_name == "static" {
            let uuid = node.main_node_id().unwrap_or_else(|| node.uuid());
            return self.static_nodes.contains_key(&uuid);
        }
        node.reuse_flag()
    }
    fn static_nodes(&self) -> &BTreeMap<String, StaticNodeConfig> {
        &self.static_nodes
    }
    fn create_machine(&self, node: &ProviderNode, label: &Label) -> Box<dyn StateMachine> {
        if self.driver_name == "static" {
            let mut sm = StaticCreate::new();
            if let Some(sn) = self.static_nodes.get(&node.uuid()) {
                sm.ip = Some(sn.host.clone());
            }
            return Box::new(sm);
        }
        match self.driver_name.as_str() {
            "aws" => Box::new(AwsCreate::from_provider(self, node, label)),
            "azure" => Box::new(AzureCreate::from_provider(self, node, label)),
            "openstack" => Box::new(OpenstackCreate::from_provider(self, node, label)),
            "kubernetes" => Box::new(K8sCreate::from_provider(self, node, label)),
            _ => driver_create(&self.driver_name),
        }
    }
    fn delete_machine(&self, node: &ProviderNode) -> Box<dyn StateMachine> {
        if self.driver_name == "static" {
            return Box::new(StaticDelete::new(node.reuse_flag()));
        }
        match self.driver_name.as_str() {
            "aws" => Box::new(AwsDelete::from_provider(self, node)),
            "azure" => Box::new(AzureDelete::from_provider(self, node)),
            "openstack" => Box::new(OpenstackDelete::from_provider(self, node)),
            "kubernetes" => Box::new(K8sDelete::from_provider(self, node)),
            _ => driver_delete(&self.driver_name),
        }
    }
    fn import_job(&self, url: &str) -> bool {
        // Match AwsProviderEndpoint.getImageImportJob: only s3:// or arn:
        // URLs can import. OpenStack has no import job implementation.
        match self.driver_name.as_str() {
            "aws" => url.starts_with("s3://") || url.starts_with("arn:"),
            _ => false,
        }
    }
    fn copy_job(&self, source_driver: &str, source_region: Option<&str>) -> bool {
        // Match AwsProviderEndpoint.getImageCopyJob / OpenStack equivalent.
        match self.driver_name.as_str() {
            "aws" => {
                source_driver == "aws"
                    && self.region.is_some()
                    && source_region.is_some()
                    && self.region.as_deref() != source_region
            }
            "openstack" => {
                // Same region required; different project checked at job build.
                source_driver == "openstack"
                    && self.region.is_some()
                    && source_region.is_some()
                    && self.region.as_deref() == source_region
            }
            _ => false,
        }
    }
    fn region(&self) -> Option<&str> {
        self.region.as_deref()
    }
    fn upload_job(&self) -> bool {
        // AwsProvider.getImageUploadJob returns None without object_storage.
        match self.driver_name.as_str() {
            "aws" => self.options.contains_key("bucket_name"),
            "openstack" | "azure" => true,
            _ => false,
        }
    }
}

pub fn parse_provider_config(driver: &str, connection: &str, canonical: &str, config: &Value) -> CloudProvider {
    let obj = config.as_object();
    let name = obj
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or(canonical)
        .to_string();
    let mut labels = BTreeMap::new();
    if let Some(list) = obj.and_then(|m| m.get("labels")).and_then(|v| v.as_array()) {
        for item in list {
            if let Some(n) = item.get("name").and_then(|v| v.as_str()) {
                let mut label = default_label(n, driver == "static");
                if let Some(img) = item.get("image").and_then(|v| v.as_str()) {
                    label.image = Some(img.to_string());
                }
                if let Some(v) = item.get("config_hash").or_else(|| item.get("config-hash")) {
                    if let Some(h) = v.as_str() {
                        label.config_hash = h.to_string();
                    } else if v.is_null() {
                        label.config_hash.clear();
                    }
                }
                if let Some(pcn) = item.get("project_canonical_name").or_else(|| item.get("project-canonical-name")).and_then(|v| v.as_str()) {
                    label.canonical_name = format!("{}/{}", quote_plus(pcn), quote_plus(n));
                }
                if let Some(flavor) = item.get("flavor").and_then(|v| v.as_str()) {
                    label.flavor = Some(flavor.to_string());
                }
                if let Some(mr) = json_i64_field(item, &["min-ready", "min_ready"]) {
                    label.min_ready = Some(mr);
                }
                if let Some(n) = json_i64_field(item, &["boot-timeout", "boot_timeout"]) {
                    label.boot_timeout = n;
                }
                if let Some(n) = json_i64_field(item, &["max-ready-age", "max_ready_age"]) {
                    label.max_ready_age = n;
                }
                if let Some(n) = json_i64_field(item, &["max-age", "max_age"]) {
                    label.max_age = n;
                }
                if let Some(n) = json_i64_field(item, &["min-retention-time", "min_retention_time"]) {
                    label.min_retention_time = n;
                }
                if let Some(n) = json_i64_field(item, &["snapshot-timeout", "snapshot_timeout"]) {
                    label.snapshot_timeout = n;
                }
                if let Some(n) = json_i64_field(item, &["snapshot-expiration", "snapshot_expiration"]) {
                    label.snapshot_expiration = n;
                }
                if let Some(n) = json_i64_field(item, &["slots"]) {
                    label.slots = n;
                }
                if let Some(n) = json_i64_field(item, &["connection-port", "connection_port"]) {
                    label.connection_port = n;
                }
                if let Some(v) = item.get("max-nodes").or_else(|| item.get("max_nodes")) {
                    if v.is_null() {
                        label.max_nodes = None;
                    } else if let Some(n) = v.as_i64() {
                        label.max_nodes = Some(n);
                    }
                }
                if let Some(zone) = item.get("executor-zone").or_else(|| item.get("executor_zone")).and_then(|v| v.as_str()) {
                    label.executor_zone = Some(zone.to_string());
                }
                if let Some(user) = item.get("username").and_then(|v| v.as_str()) {
                    label.username = Some(user.to_string());
                }
                if let Some(kind) = item.get("connection-type").or_else(|| item.get("connection_type")).and_then(|v| v.as_str()) {
                    label.connection_type = Some(kind.to_string());
                }
                if let Some(path) = item.get("python-path").or_else(|| item.get("python_path")).and_then(|v| v.as_str()) {
                    label.python_path = Some(path.to_string());
                }
                if let Some(shell) = item.get("shell-type").or_else(|| item.get("shell_type")).and_then(|v| v.as_str()) {
                    label.shell_type = Some(shell.to_string());
                }
                if let Some(key) = item.get("key-name").or_else(|| item.get("key_name")).and_then(|v| v.as_str()) {
                    label.key_name = Some(key.to_string());
                }
                // Schema AsList(str) coerces a bare string to a one-element
                // list; accept both forms from ZK provider configs.
                if let Some(arr) = item.get("aliases").and_then(|v| v.as_array()) {
                    label.aliases = arr
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect();
                } else if let Some(s) = item.get("aliases").and_then(|v| v.as_str()) {
                    label.aliases = vec![s.to_string()];
                }
                if let Some(tags) = item.get("tags").and_then(|v| v.as_object()) {
                    label.tags = tags
                        .iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect();
                }
                if let Some(b) = item.get("reuse").and_then(|v| v.as_bool()) {
                    label.reuse = b;
                }
                if let Some(b) = item.get("host-key-checking").or_else(|| item.get("host_key_checking")).and_then(|v| v.as_bool()) {
                    label.host_key_checking = b;
                }
                label.raw = item.clone();
                labels.insert(n.to_string(), label);
            }
        }
    }
    let mut images = BTreeMap::new();
    if let Some(list) = obj.and_then(|m| m.get("images")).and_then(|v| v.as_array()) {
        for item in list {
            if let Some(n) = item.get("name").and_then(|v| v.as_str()) {
                let canonical_name = item
                    .get("project_canonical_name")
                    .or_else(|| item.get("project-canonical-name"))
                    .and_then(|v| v.as_str())
                    .map(|pcn| format!("{}/{}", quote_plus(pcn), quote_plus(n)))
                    .unwrap_or_else(|| n.to_string());
                images.insert(n.to_string(), Image {
                    name: n.into(),
                    canonical_name,
                    image_type: item.get("type").and_then(|v| v.as_str()).unwrap_or("cloud").into(),
                    config_hash: item.get("config_hash").and_then(|v| v.as_str()).unwrap_or("").into(),
                    username: item.get("username").and_then(|v| v.as_str()).map(|s| s.to_string()),
                    connection_type: item.get("connection-type").and_then(|v| v.as_str()).map(|s| s.to_string()),
                    connection_port: item.get("connection-port").and_then(|v| v.as_i64()),
                    python_path: item.get("python-path").and_then(|v| v.as_str()).map(|s| s.to_string()),
                    shell_type: item.get("shell-type").and_then(|v| v.as_str()).map(|s| s.to_string()),
                    retain_count: item.get("retain-count").and_then(|v| v.as_i64()).unwrap_or(2),
                    raw: item.clone(),
                });
            }
        }
    }
    let region = obj.and_then(|m| m.get("region")).and_then(|v| v.as_str()).map(|s| s.to_string());
    let endpoint_name = region.clone().unwrap_or_else(|| connection.to_string());
    let endpoint_cname = if driver == "static" {
        quote_plus(connection)
    } else {
        format!("{}/{}", quote_plus(connection), quote_plus(&endpoint_name))
    };
    let mut limits = QuotaInformation { default: f64::INFINITY, quota: BTreeMap::new() };
    let mut resource_limits = BTreeMap::new();
    if let Some(rl) = obj.and_then(|m| m.get("resource-limits").or_else(|| m.get("resource_limits"))).and_then(|v| v.as_object()) {
        for (k, v) in rl {
            if let Some(n) = v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)) {
                limits.quota.insert(k.clone(), n);
                resource_limits.insert(k.clone(), n);
            }
        }
    }
    let mut flavors = BTreeMap::new();
    if let Some(list) = obj.and_then(|m| m.get("flavors")).and_then(|v| v.as_array()) {
        for item in list {
            if let Some(n) = item.get("name").and_then(|v| v.as_str()) {
                flavors.insert(n.to_string(), Flavor { name: n.into(), raw: item.clone() });
            }
        }
    }
    // _requestNode copies username and connection fields from the image.
    // Label objects only inherit them when the label config omits the key.
    for label in labels.values_mut() {
        let Some(image_name) = label.image.clone() else { continue };
        let Some(image) = images.get(&image_name) else { continue };
        if label.raw.get("username").is_none() {
            label.username = image.username.clone();
        }
        if label.raw.get("connection-type").is_none() && label.raw.get("connection_type").is_none() {
            if let Some(kind) = &image.connection_type {
                label.connection_type = Some(kind.clone());
            }
        }
        if label.raw.get("connection-port").is_none() && label.raw.get("connection_port").is_none() {
            if let Some(port) = image.connection_port {
                label.connection_port = port;
            }
        }
        if label.raw.get("python-path").is_none() && label.raw.get("python_path").is_none() {
            if let Some(path) = &image.python_path {
                label.python_path = Some(path.clone());
            }
        }
        if label.raw.get("shell-type").is_none() && label.raw.get("shell_type").is_none() {
            label.shell_type = image.shell_type.clone();
        }
    }
    let launch_attempts = obj
        .and_then(|m| m.get("launch-attempts").or_else(|| m.get("launch_attempts")))
        .and_then(|v| v.as_i64())
        .unwrap_or(3);
    let launch_timeout = obj
        .and_then(|m| m.get("launch-timeout").or_else(|| m.get("launch_timeout")))
        .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
        .or(Some(600.0));
    // StaticProvider.parseConfig: uuid5 from canonical + host + port + user.
    let mut static_nodes = BTreeMap::new();
    if driver == "static" {
        if let Some(list) = obj.and_then(|m| m.get("nodes")).and_then(|v| v.as_array()) {
            for item in list {
                let Some(host) = item.get("name").and_then(|v| v.as_str()) else {
                    continue;
                };
                let Some(label_name) = item.get("label").and_then(|v| v.as_str()) else {
                    continue;
                };
                let label = labels.get(label_name);
                let image = label
                    .and_then(|l| l.image.as_ref())
                    .and_then(|name| images.get(name));
                let mut connection_port =
                    json_i64_field(item, &["connection-port", "connection_port"]);
                let mut username = item
                    .get("username")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                // StaticNodeConfig.inheritFrom(image, flavor): fill nulls.
                if connection_port.is_none() {
                    connection_port = label
                        .map(|l| l.connection_port)
                        .or_else(|| image.and_then(|i| i.connection_port));
                }
                if username.is_none() {
                    username = label
                        .and_then(|l| l.username.clone())
                        .or_else(|| image.and_then(|i| i.username.clone()));
                }
                // StaticProviderImage defaults connection_port to 22.
                let connection_port = connection_port.unwrap_or(22);
                let aliases = item
                    .get("aliases")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default();
                let uuid = static_node_uuid(
                    canonical,
                    host,
                    connection_port,
                    username.as_deref(),
                );
                static_nodes.insert(
                    uuid.clone(),
                    StaticNodeConfig {
                        uuid,
                        label: label_name.to_string(),
                        host: host.to_string(),
                        connection_port,
                        username,
                        aliases,
                    },
                );
            }
        }
    }
    let mut options = BTreeMap::new();
    // provider[aws].object-storage.bucket-name (also accept snake_case).
    if let Some(os) = obj
        .and_then(|m| m.get("object-storage").or_else(|| m.get("object_storage")))
        .and_then(|v| v.as_object())
    {
        if let Some(bucket) = os
            .get("bucket-name")
            .or_else(|| os.get("bucket_name"))
            .and_then(|v| v.as_str())
        {
            options.insert("bucket_name".into(), bucket.to_string());
        }
    }
    CloudProvider {
        name,
        canonical_name: canonical.into(),
        driver_name: driver.into(),
        connection_name: connection.into(),
        endpoint_info: EndpointInfo {
            name: endpoint_name,
            canonical_name: endpoint_cname,
            connection_name: connection.into(),
            unmanaged: QuotaInformation::default(),
            limits,
        },
        labels,
        images,
        flavors,
        launch_attempts,
        launch_timeout,
        resource_limits,
        region,
        options,
        static_nodes,
    }
}

fn json_i64_field(item: &Value, keys: &[&str]) -> Option<i64> {
    for key in keys {
        if let Some(v) = item.get(*key) {
            if let Some(n) = v.as_i64() {
                return Some(n);
            }
            if let Some(n) = v.as_u64() {
                return Some(n as i64);
            }
            if let Some(n) = v.as_f64() {
                return Some(n as i64);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_node_uuid5_matches_python() {
        // Python: uuid.uuid5(NODE_NAMESPACE, key).hex
        assert_eq!(
            static_node_uuid("tenant/static-provider", "host.example.com", 22, Some("zuul")),
            "0d6392cb7c6f53b2bbda6aaa8b5904fe"
        );
        assert_eq!(
            static_node_uuid("tenant-a/static", "host1", 22, Some("zuul")),
            "072a1749be0c5ae2a7223de6f946dbce"
        );
        // str(None) in Python when username is missing.
        assert_eq!(
            static_node_uuid("org/p/static", "n1", 22, None),
            Uuid::new_v5(
                &STATIC_NODE_NAMESPACE,
                b"org/p/static n1 22 None"
            )
            .as_simple()
            .to_string()
        );
    }

    #[test]
    fn parse_static_provider_nodes() {
        let config = json!({
            "name": "static-main",
            "images": [{
                "name": "debian",
                "type": "cloud",
                "username": "debian",
                "connection-port": 22
            }],
            "labels": [{
                "name": "debian-normal",
                "image": "debian",
                "config_hash": "abc",
                "reuse": true
            }],
            "nodes": [{
                "name": "host.example.com",
                "label": "debian-normal",
                "aliases": ["special"]
            }]
        });
        let provider = parse_provider_config(
            "static",
            "static-conn",
            "org/project/static-main",
            &config,
        );
        assert_eq!(provider.static_nodes.len(), 1);
        let node = provider.static_nodes.values().next().unwrap();
        assert_eq!(node.host, "host.example.com");
        assert_eq!(node.label, "debian-normal");
        assert_eq!(node.connection_port, 22);
        assert_eq!(node.username.as_deref(), Some("debian"));
        assert_eq!(node.aliases, vec!["special".to_string()]);
        assert_eq!(
            node.uuid,
            static_node_uuid(
                "org/project/static-main",
                "host.example.com",
                22,
                Some("debian")
            )
        );
    }
}
