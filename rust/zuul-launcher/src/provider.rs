// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// Provider / endpoint / state-machine surface from zuul/provider/__init__.py
// and zuul/provider/statemachine.py. Driver advance() methods follow the
// Python call order in each driver's statemachine module.

use crate::config::quote_plus;
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
/// A stub endpoint returns success immediately, so one advance() falls
/// through the same states Python would when every future is done.
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
        };
        // AwsCreateStateMachine.__init__ always prices the label.
        machine.calls.push("getQuotaForLabel".into());
        machine
    }

    fn note(&mut self, state: &str) {
        self.state = state.into();
        self.visited.push(state.into());
    }
}

impl StateMachine for AwsCreate {
    fn advance(&mut self) -> Result<(), AdvanceError> {
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
    fn complete(&self) -> bool { self.complete }
    fn state_name(&self) -> String { self.state.clone() }
    fn start_time(&self) -> f64 { self.start_time }
    fn set_start_time(&mut self, t: f64) { self.start_time = t; }
    fn external_id(&self) -> Option<String> { self.instance_id.clone() }
    fn interface_ip(&self) -> Option<String> {
        if self.complete { Some("192.0.2.10".into()) } else { None }
    }
    fn calls(&self) -> Vec<String> { self.calls.clone() }
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
        };
        if machine.host_id.is_some() {
            machine.calls.push("_refreshDelete".into());
        }
        if machine.instance_id.is_some() {
            machine.calls.push("_refreshDelete".into());
        }
        machine
    }

    fn note(&mut self, state: &str) {
        self.state = state.into();
        self.visited.push(state.into());
    }
}

impl StateMachine for AwsDelete {
    fn advance(&mut self) -> Result<(), AdvanceError> {
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
    fn complete(&self) -> bool { self.complete }
    fn state_name(&self) -> String { self.state.clone() }
    fn start_time(&self) -> f64 { self.start_time }
    fn set_start_time(&mut self, t: f64) { self.start_time = t; }
    fn calls(&self) -> Vec<String> { self.calls.clone() }
    fn to_dict(&self) -> Value { json!({"state": self.state, "start_time": self.start_time}) }
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

/// Cloud-backed provider. State machines are the Python ones; endpoint
/// methods are invoked in that order. Live HTTP is performed by `cloud`.
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
    fn create_machine(&self, _node: &ProviderNode, label: &Label) -> Box<dyn StateMachine> {
        if self.driver_name == "aws" {
            let dedicated = label
                .flavor
                .as_ref()
                .and_then(|name| self.flavors.get(name))
                .and_then(|flavor| {
                    flavor
                        .raw
                        .get("dedicated-host")
                        .or_else(|| flavor.raw.get("dedicated_host"))
                        .and_then(|v| v.as_bool())
                })
                .unwrap_or(false);
            return Box::new(AwsCreate::new(dedicated));
        }
        driver_create(&self.driver_name)
    }
    fn delete_machine(&self, _node: &ProviderNode) -> Box<dyn StateMachine> {
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
