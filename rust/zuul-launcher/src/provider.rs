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
    fn copy_job(&self, _source_driver: &str) -> bool {
        false
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
        machine.live = cloud::enabled();
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
                if k != "create_state" {
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
        machine.live = cloud::enabled();
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
    provider: String,
    node_uuid: String,
    snapshot_obj: Option<Value>,
    account_id: Option<String>,
    region: Option<String>,
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
    pub calls: Vec<String>,
    pub visited: Vec<String>,
    create_future: bool,
    pub vm_ready: bool,
    pub nic_ready: bool,
}

impl AzureCreate {
    pub fn new() -> Self {
        let mut machine = Self {
            state: "start".into(),
            start_time: 0.0,
            complete: false,
            calls: Vec::new(),
            visited: vec!["start".into()],
            create_future: false,
            vm_ready: true,
            nic_ready: true,
        };
        machine.calls.push("getQuotaForLabel".into());
        machine
    }
    fn note(&mut self, state: &str) {
        self.state = state.into();
        self.visited.push(state.into());
    }
}

impl StateMachine for AzureCreate {
    fn advance(&mut self) -> Result<(), AdvanceError> {
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
            // No public IP on the stub NIC, so the query is already satisfied.
            self.note("complete");
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
    fn calls(&self) -> Vec<String> { self.calls.clone() }
    fn to_dict(&self) -> Value { json!({"state": self.state, "start_time": self.start_time}) }
}

pub struct AzureDelete {
    pub state: String,
    pub start_time: f64,
    pub complete: bool,
    pub calls: Vec<String>,
    pub visited: Vec<String>,
    pub vm_id: Option<String>,
    pub gone: bool,
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
        }
    }
    fn note(&mut self, state: &str) {
        self.state = state.into();
        self.visited.push(state.into());
    }
}

impl StateMachine for AzureDelete {
    fn advance(&mut self) -> Result<(), AdvanceError> {
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
    fn complete(&self) -> bool { self.complete }
    fn state_name(&self) -> String { self.state.clone() }
    fn start_time(&self) -> f64 { self.start_time }
    fn set_start_time(&mut self, t: f64) { self.start_time = t; }
    fn calls(&self) -> Vec<String> { self.calls.clone() }
    fn to_dict(&self) -> Value { json!({"state": self.state, "start_time": self.start_time}) }
}

/// OpenStack order from zuul/driver/openstack/openstackendpoint.py.
pub struct OpenstackCreate {
    pub state: String,
    pub start_time: f64,
    pub complete: bool,
    pub calls: Vec<String>,
    pub visited: Vec<String>,
    create_future: bool,
    pub active: bool,
    pub auto_floating_ip: bool,
    pub needs_floating_ip: bool,
    pub fip_active: bool,
    pub fip_attached: bool,
}

impl OpenstackCreate {
    pub fn new() -> Self {
        let mut machine = Self {
            state: "start".into(),
            start_time: 0.0,
            complete: false,
            calls: Vec::new(),
            visited: vec!["start".into()],
            create_future: false,
            active: true,
            auto_floating_ip: false,
            needs_floating_ip: false,
            fip_active: true,
            fip_attached: true,
        };
        machine.calls.push("_findFlavor".into());
        machine
    }
    fn note(&mut self, state: &str) {
        self.state = state.into();
        self.visited.push(state.into());
    }
}

impl StateMachine for OpenstackCreate {
    fn advance(&mut self) -> Result<(), AdvanceError> {
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
    fn complete(&self) -> bool { self.complete }
    fn state_name(&self) -> String { self.state.clone() }
    fn start_time(&self) -> f64 { self.start_time }
    fn set_start_time(&mut self, t: f64) { self.start_time = t; }
    fn calls(&self) -> Vec<String> { self.calls.clone() }
    fn to_dict(&self) -> Value { json!({"state": self.state, "start_time": self.start_time}) }
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
        };
        if machine.server_id.is_some() {
            machine.calls.push("_getServer".into());
        }
        machine
    }
    fn note(&mut self, state: &str) {
        self.state = state.into();
        self.visited.push(state.into());
    }
}

impl StateMachine for OpenstackDelete {
    fn advance(&mut self) -> Result<(), AdvanceError> {
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
    fn complete(&self) -> bool { self.complete }
    fn state_name(&self) -> String { self.state.clone() }
    fn start_time(&self) -> f64 { self.start_time }
    fn set_start_time(&mut self, t: f64) { self.start_time = t; }
    fn calls(&self) -> Vec<String> { self.calls.clone() }
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
        }
    }
    fn note(&mut self, state: &str) {
        self.state = state.into();
        self.visited.push(state.into());
    }
}

impl StateMachine for K8sCreate {
    fn advance(&mut self) -> Result<(), AdvanceError> {
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
    fn complete(&self) -> bool { self.complete }
    fn state_name(&self) -> String { self.state.clone() }
    fn start_time(&self) -> f64 { self.start_time }
    fn set_start_time(&mut self, t: f64) { self.start_time = t; }
    fn calls(&self) -> Vec<String> { self.calls.clone() }
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
        }
    }
    fn note(&mut self, state: &str) {
        self.state = state.into();
        self.visited.push(state.into());
    }
}

impl StateMachine for K8sDelete {
    fn advance(&mut self) -> Result<(), AdvanceError> {
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
        // Python does not wait for the namespace to disappear.
        if self.state == "deleting namespace" {
            self.note("complete");
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
    fn calls(&self) -> Vec<String> { self.calls.clone() }
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

/// Temporary adapter for Azure / OpenStack / Kubernetes while those
/// drivers still lack Rust endpoint-op state machines. One `step` /
/// `fail` call runs the Python SM so azul and the SDK fakes stay
/// reachable. AWS does not use this.
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

/// Cloud-backed provider. AWS create/delete state machines run in Rust
/// and call fine-grained endpoint ops through `cloud`. Other non-static
/// drivers keep a temporary RemoteMachine adapter until they gain the
/// same endpoint-op path. Static stays fully Rust.
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
        // Static defaults reuse True; the others default False.
        if self.driver_name == "static" {
            node.reuse_flag()
        } else {
            node.reuse_flag()
        }
    }
    fn create_machine(&self, node: &ProviderNode, label: &Label) -> Box<dyn StateMachine> {
        if self.driver_name == "aws" {
            return Box::new(AwsCreate::from_provider(self, node, label));
        }
        if cloud::enabled() && self.driver_name != "static" {
            return Box::new(RemoteMachine::new(
                "create",
                &self.driver_name,
                &self.canonical_name,
                node,
                label,
            ));
        }
        driver_create(&self.driver_name)
    }
    fn delete_machine(&self, node: &ProviderNode) -> Box<dyn StateMachine> {
        if self.driver_name == "aws" {
            return Box::new(AwsDelete::from_provider(self, node));
        }
        if cloud::enabled() && self.driver_name != "static" {
            let label_name = node.label();
            let label = self
                .labels
                .get(&label_name)
                .cloned()
                .unwrap_or_else(|| default_label(&label_name, false));
            return Box::new(RemoteMachine::new(
                "delete",
                &self.driver_name,
                &self.canonical_name,
                node,
                &label,
            ));
        }
        if self.driver_name == "static" {
            return Box::new(StaticDelete::new(node.reuse_flag()));
        }
        driver_delete(&self.driver_name)
    }
    fn import_job(&self, _url: &str) -> bool {
        matches!(self.driver_name.as_str(), "aws" | "openstack")
    }
    fn copy_job(&self, source_driver: &str) -> bool {
        source_driver == self.driver_name && matches!(self.driver_name.as_str(), "aws" | "openstack")
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
                if let Some(arr) = item.get("aliases").and_then(|v| v.as_array()) {
                    label.aliases = arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
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
        options: BTreeMap::new(),
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
