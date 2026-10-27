// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// Native AWS, Azure, OpenStack, and Kubernetes clients for the
// zuul-launcher binary. These run only when no PyO3 cloud handler is
// installed. Request shapes follow Zuul 14.3.0:
//   awsconnection / awsendpoint (boto3 EC2 query, S3 REST, EBS REST,
//   service quotas, standard retries, rate limit, 10s list cache)
//   azureconnection / azul.py
//   openstackconnection (clouds.yaml + Keystone v3 + Nova/Neutron)
//   kubernetesconnection (kubeconfig + the core/rbac calls in
//   kubernetesendpoint.py)

use crate::cloud_http::{
    amz_now, aws_encode, ec2_query, ec2_xml_to_boto, exchange, parse_yaml, sha256_hex, RateLimiter,
    SigV4,
};
use crate::config::ConnectionConfig;
use crate::provider::{AdvanceError, Image, Label, Provider};
use base64::Engine;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const EC2_VERSION: &str = "2016-11-15";
const LIST_TTL: Duration = Duration::from_secs(10);
const QUOTA_TTL: Duration = Duration::from_secs(300);
const HTTP_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_ATTEMPTS: u32 = 5;

struct Registry {
    connections: BTreeMap<String, ConnectionConfig>,
    providers: BTreeMap<String, ProviderSnap>,
    futures: BTreeMap<String, FutureJob>,
    aws: BTreeMap<String, Arc<AwsEndpoint>>,
    azure_tokens: BTreeMap<String, AzureToken>,
    os_tokens: BTreeMap<String, OsToken>,
    limiters: BTreeMap<String, Arc<RateLimiter>>,
    /// Test-only Azure hosts. Production leaves both unset and uses
    /// login.microsoftonline.com and management.azure.com, matching azul.
    azure_login: Option<String>,
    azure_arm: Option<String>,
    /// Zuul system uuid, used in launch-template and upload tags.
    system_id: String,
    /// Provider options the cleanup worker needs (buckets, port interval).
    extras: BTreeMap<String, ProviderExtra>,
    /// Endpoint canonical names whose postConfig finished.
    ready: BTreeMap<String, bool>,
    /// provider canonical → label → launch template name.
    templates: BTreeMap<String, BTreeMap<String, String>>,
    /// Unmanaged quota recorded by the native cleanup worker.
    unmanaged: BTreeMap<String, BTreeMap<String, f64>>,
    /// OpenStack DOWN-port sets, keyed by endpoint canonical name.
    ports: BTreeMap<String, PortBook>,
}

#[derive(Clone)]
struct ProviderExtra {
    endpoint_canonical: String,
    #[allow(dead_code)]
    endpoint_name: String,
    bucket: Option<String>,
    port_interval: Option<i64>,
    floating_ip_cleanup: bool,
}

struct PortBook {
    last: Option<Instant>,
    down: HashSet<String>,
}

struct FutureJob {
    #[allow(dead_code)]
    kind: String,
    provider: String,
    payload: Value,
}

#[derive(Clone)]
struct ProviderSnap {
    driver: String,
    connection: String,
    region: String,
    labels: BTreeMap<String, Value>,
    flavors: BTreeMap<String, Value>,
    images: BTreeMap<String, Value>,
}

struct AzureToken {
    token: String,
    expiration: f64,
}

struct OsToken {
    token: String,
    compute: String,
    network: String,
    image: String,
    volume: String,
    limit_key: String,
    rate: f64,
}

struct AwsEndpoint {
    region: String,
    endpoint: String,
    sig: SigV4,
    mutating: RateLimiter,
    reading: RateLimiter,
    quota_limit: RateLimiter,
    lists: Mutex<BTreeMap<String, CachedList>>,
    not_our_images: Mutex<HashSet<String>>,
    not_our_snapshots: Mutex<HashSet<String>>,
}

struct CachedList {
    at: Instant,
    value: Value,
    refreshing: bool,
}

fn registry() -> &'static Mutex<Registry> {
    static REG: std::sync::OnceLock<Mutex<Registry>> = std::sync::OnceLock::new();
    REG.get_or_init(|| {
        Mutex::new(Registry {
            connections: BTreeMap::new(),
            providers: BTreeMap::new(),
            futures: BTreeMap::new(),
            aws: BTreeMap::new(),
            azure_tokens: BTreeMap::new(),
            os_tokens: BTreeMap::new(),
            limiters: BTreeMap::new(),
            azure_login: None,
            azure_arm: None,
            system_id: "system".into(),
            extras: BTreeMap::new(),
            ready: BTreeMap::new(),
            templates: BTreeMap::new(),
            unmanaged: BTreeMap::new(),
            ports: BTreeMap::new(),
        })
    })
}

fn conn_rate(conn: &ConnectionConfig) -> f64 {
    opt(conn, "rate").and_then(|s| s.parse().ok()).unwrap_or(2.0)
}

fn enter_limit(key: &str, rate: f64) {
    let lim = {
        let mut reg = registry().lock().unwrap();
        reg.limiters
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(RateLimiter::new(rate)))
            .clone()
    };
    lim.enter();
}

fn azure_login_base() -> String {
    registry()
        .lock()
        .unwrap()
        .azure_login
        .clone()
        .unwrap_or_else(|| "https://login.microsoftonline.com".into())
}

fn azure_arm_base() -> String {
    registry()
        .lock()
        .unwrap()
        .azure_arm
        .clone()
        .unwrap_or_else(|| "https://management.azure.com".into())
}

#[cfg(test)]
fn set_test_azure_endpoints(login: &str, arm: &str) {
    let mut reg = registry().lock().unwrap();
    reg.azure_login = Some(login.to_string());
    reg.azure_arm = Some(arm.to_string());
}

pub fn set_system_id(id: &str) {
    if !id.is_empty() {
        registry().lock().unwrap().system_id = id.to_string();
    }
}

fn system_id() -> String {
    registry().lock().unwrap().system_id.clone()
}

pub fn set_connections(connections: Vec<ConnectionConfig>) {
    let mut reg = registry().lock().unwrap();
    reg.connections.clear();
    reg.aws.clear();
    for c in connections {
        reg.connections.insert(c.name.clone(), c);
    }
}

pub fn sync_providers(tenants: &std::collections::HashMap<String, Vec<Arc<dyn Provider>>>) {
    let mut reg = registry().lock().unwrap();
    reg.providers.clear();
    for list in tenants.values() {
        for provider in list {
            let mut labels = BTreeMap::new();
            for (name, label) in provider.labels() {
                labels.insert(name.clone(), label_value(label));
            }
            let mut flavors = BTreeMap::new();
            for (name, flavor) in provider.flavors() {
                flavors.insert(name.clone(), flavor.raw.clone());
            }
            let mut images = BTreeMap::new();
            for (name, image) in provider.images() {
                images.insert(name.clone(), image_value(image));
            }
            let options = provider.provider_options();
            reg.extras.insert(
                provider.canonical_name().to_string(),
                ProviderExtra {
                    endpoint_canonical: provider.endpoint().canonical_name.clone(),
                    endpoint_name: provider.endpoint().name.clone(),
                    bucket: options.get("bucket_name").cloned(),
                    port_interval: options.get("port_cleanup_interval").and_then(|s| s.parse().ok()),
                    floating_ip_cleanup: options.get("floating_ip_cleanup").map(|s| s == "true").unwrap_or(false),
                },
            );
            reg.providers.insert(
                provider.canonical_name().to_string(),
                ProviderSnap {
                    driver: provider.driver_name().to_string(),
                    connection: provider.connection_name().to_string(),
                    region: provider.region().unwrap_or("").to_string(),
                    labels,
                    flavors,
                    images,
                },
            );
        }
    }
}

fn label_value(label: &Label) -> Value {
    let mut raw = match &label.raw {
        Value::Object(m) => m.clone(),
        _ => Map::new(),
    };
    raw.entry("name").or_insert(json!(label.name));
    if let Some(v) = &label.key_name {
        raw.entry("key-name").or_insert(json!(v));
    }
    if let Some(v) = &label.flavor {
        raw.entry("flavor").or_insert(json!(v));
    }
    if let Some(v) = &label.image {
        raw.entry("image").or_insert(json!(v));
    }
    Value::Object(raw)
}

fn image_value(image: &Image) -> Value {
    let mut raw = match &image.raw {
        Value::Object(m) => m.clone(),
        _ => Map::new(),
    };
    raw.entry("name").or_insert(json!(image.name));
    if let Some(v) = &image.username {
        raw.entry("username").or_insert(json!(v));
    }
    Value::Object(raw)
}

fn new_id() -> String {
    let mut buf = [0u8; 16];
    let _ = getrandom::getrandom(&mut buf);
    hex::encode(buf)
}

fn random_index(len: usize) -> usize {
    if len <= 1 {
        return 0;
    }
    let mut buf = [0u8; 8];
    let _ = getrandom::getrandom(&mut buf);
    (u64::from_le_bytes(buf) as usize) % len
}

pub fn invoke(op: &str, args: &Value) -> Result<Value, AdvanceError> {
    if op == "sm.patched_advance" {
        return Ok(json!({"patched": false}));
    }
    match op {
        "ep.refresh" => refresh_any(args),
        "ep.refresh_delete" => refresh_delete_any(args),
        "ep.submit_create_instance" => submit("aws-create", args),
        "ep.complete_create_instance" => complete_aws_create(args),
        "ep.submit_allocate_host" => submit("aws-host", args),
        "ep.complete_allocate_host" => complete_aws_host(args),
        "ep.delete_instance" => aws_delete_instance(args),
        "ep.release_host" => aws_release_host(args),
        "ep.quota_for_label" => quota_for_label(args),
        "ep.create_snapshot" => aws_create_snapshot(args),
        "ep.refresh_snapshot" => refresh_any(args),
        "ep.azure_list_instance_id" => azure_list_instance_id(args),
        "ep.azure_get_image_reference" => azure_image_reference(args),
        "ep.azure_get_subnet_id" => azure_subnet_id(args),
        "ep.azure_submit_create_vm" => submit("azure-create", args),
        "ep.azure_complete_create_vm" => complete_azure_create(args),
        "ep.azure_delete_vm" => azure_delete_vm(args),
        "ep.azure_update_node" => azure_update_node(args),
        "ep.os_find_flavor" => os_find_flavor(args),
        "ep.os_list_instance_id" => os_list_instance_id(args),
        "ep.os_submit_create_server" => submit("os-create", args),
        "ep.os_complete_create_server" => complete_os_create(args),
        "ep.os_refresh_server" => os_refresh_server(args),
        "ep.os_get_server" => os_get_server(args),
        "ep.os_needs_floating_ip" => os_needs_floating_ip(args),
        "ep.os_create_floating_ip" => os_create_floating_ip(args),
        "ep.os_refresh_floating_ip" => os_refresh_floating_ip(args),
        "ep.os_attach_ip" => os_attach_ip(args),
        "ep.os_server_floating_ip" => os_server_floating_ip(args),
        "ep.os_get_instance" => os_get_instance(args),
        "ep.os_has_floating_ips" => os_has_floating_ips(args),
        "ep.os_get_floating_ips" => os_get_floating_ips(args),
        "ep.os_delete_floating_ip" => os_delete_floating_ip(args),
        "ep.os_refresh_floating_ip_delete" => os_refresh_floating_ip_delete(args),
        "ep.os_submit_delete_server" => submit("os-delete", args),
        "ep.os_complete_delete_server" => complete_os_delete(args),
        "ep.os_refresh_server_delete" => os_refresh_server_delete(args),
        "ep.os_handle_server_fault" => os_handle_server_fault(args),
        "ep.k8s_setup" => k8s_setup(args),
        "ep.k8s_use_openshift" => k8s_use_openshift(args),
        "ep.k8s_create_namespace" => k8s_create_namespace(args),
        "ep.k8s_create_project" => k8s_create_project(args),
        "ep.k8s_get_subject_access_review" => k8s_subject_access(args),
        "ep.k8s_create_image_pull_secrets" => k8s_image_pull_secrets(args),
        "ep.k8s_create_service_account" => k8s_service_account(args),
        "ep.k8s_get_token" => k8s_get_token(args),
        "ep.k8s_create_role" => k8s_create_role(args),
        "ep.k8s_create_pod" => k8s_create_pod(args),
        "ep.k8s_get_pod" => k8s_get_pod(args),
        "ep.k8s_delete_namespace" => k8s_delete_namespace(args),
        "ep.k8s_delete_project" => k8s_delete_project(args),
        "ep.k8s_update_node" => k8s_update_node(args),
        other => Err(AdvanceError::Other(format!(
            "native cloud op not implemented: {other}"
        ))),
    }
}

fn submit(kind: &str, args: &Value) -> Result<Value, AdvanceError> {
    let id = new_id();
    let provider = args
        .get("provider")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    registry().lock().unwrap().futures.insert(
        id.clone(),
        FutureJob {
            kind: kind.into(),
            provider,
            payload: args.clone(),
        },
    );
    Ok(json!({"future_id": id}))
}

fn take_future(args: &Value) -> Result<FutureJob, AdvanceError> {
    let id = args
        .get("future_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AdvanceError::Other("missing future_id".into()))?;
    registry()
        .lock()
        .unwrap()
        .futures
        .remove(id)
        .ok_or_else(|| AdvanceError::Other(format!("unknown future {id}")))
}

fn provider_of(args: &Value) -> Result<ProviderSnap, AdvanceError> {
    let name = args
        .get("provider")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AdvanceError::Other("missing provider".into()))?;
    registry()
        .lock()
        .unwrap()
        .providers
        .get(name)
        .cloned()
        .ok_or_else(|| AdvanceError::Other(format!("no provider {name}")))
}

fn connection_of(name: &str) -> Result<ConnectionConfig, AdvanceError> {
    registry()
        .lock()
        .unwrap()
        .connections
        .get(name)
        .cloned()
        .ok_or_else(|| AdvanceError::Other(format!("no connection {name}")))
}

fn opt<'a>(cfg: &'a ConnectionConfig, key: &str) -> Option<String> {
    cfg.options.get(key).filter(|s| !s.is_empty()).cloned()
}

fn field<'a>(v: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    let obj = v.as_object()?;
    for k in keys {
        if let Some(found) = obj.get(*k) {
            if !found.is_null() {
                return Some(found);
            }
        }
    }
    None
}

fn field_str(v: &Value, keys: &[&str]) -> Option<String> {
    field(v, keys).and_then(|x| x.as_str()).map(|s| s.to_string())
}

fn as_str_list(v: Option<&Value>) -> Vec<String> {
    match v {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|i| i.as_str().map(|s| s.to_string()))
            .collect(),
        Some(Value::String(s)) => vec![s.clone()],
        _ => Vec::new(),
    }
}

fn err_other(e: String) -> AdvanceError {
    let lower = e.to_ascii_lowercase();
    if e.contains("VolumeLimitExceeded") || lower.contains("quota") {
        AdvanceError::Quota(e)
    } else if e.contains("InsufficientInstanceCapacity") || lower.contains("capacity") {
        AdvanceError::Capacity(e)
    } else {
        AdvanceError::Other(e)
    }
}

// ---------------------------------------------------------------------------
// AWS
// ---------------------------------------------------------------------------

struct AwsCreds {
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    endpoint: Option<String>,
}

fn aws_creds(conn: &ConnectionConfig) -> Result<AwsCreds, AdvanceError> {
    let mut access = opt(conn, "access_key_id");
    let mut secret = opt(conn, "secret_access_key");
    let mut token = None;
    let mut role = opt(conn, "role_arn");
    let mut web = opt(conn, "web_identity_token_file");
    let mut endpoint = opt(conn, "endpoint_url");
    let profile = opt(conn, "profile")
        .or_else(|| std::env::var("AWS_PROFILE").ok())
        .unwrap_or_else(|| "default".into());
    if let Some(path) = opt(conn, "shared_credentials_file") {
        apply_aws_file(&expand_user(&path), &profile, &mut access, &mut secret, &mut role, &mut web, &mut endpoint);
    }
    if access.is_none() {
        if let Ok(v) = std::env::var("AWS_ACCESS_KEY_ID") {
            access = Some(v);
        }
        if let Ok(v) = std::env::var("AWS_SECRET_ACCESS_KEY") {
            secret = Some(v);
        }
        if let Ok(v) = std::env::var("AWS_SESSION_TOKEN") {
            token = Some(v);
        }
    }
    if access.is_none() {
        let path = std::env::var("AWS_SHARED_CREDENTIALS_FILE")
            .unwrap_or_else(|_| expand_user("~/.aws/credentials"));
        apply_aws_file(&path, &profile, &mut access, &mut secret, &mut role, &mut web, &mut endpoint);
    }
    if role.is_none() {
        role = std::env::var("AWS_ROLE_ARN").ok();
    }
    if web.is_none() {
        web = std::env::var("AWS_WEB_IDENTITY_TOKEN_FILE").ok();
    }
    if endpoint.is_none() {
        endpoint = std::env::var("AWS_ENDPOINT_URL").ok();
    }
    if let (Some(role_arn), Some(token_file)) = (role, web) {
        let region = std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|_| "us-east-1".into());
        let assumed = assume_web_identity(&region, endpoint.as_deref(), &role_arn, &token_file, access.as_deref(), secret.as_deref())?;
        access = Some(assumed.0);
        secret = Some(assumed.1);
        token = Some(assumed.2);
    }
    let access = access.ok_or_else(|| {
        AdvanceError::Other("AWS credentials not found in config, env, or shared credentials file".into())
    })?;
    let secret = secret.ok_or_else(|| {
        AdvanceError::Other("AWS secret access key not found".into())
    })?;
    Ok(AwsCreds {
        access_key: access,
        secret_key: secret,
        session_token: token,
        endpoint,
    })
}

fn expand_user(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{home}/{rest}");
        }
    }
    path.to_string()
}

fn apply_aws_file(
    path: &str,
    profile: &str,
    access: &mut Option<String>,
    secret: &mut Option<String>,
    role: &mut Option<String>,
    web: &mut Option<String>,
    endpoint: &mut Option<String>,
) {
    let Ok(text) = std::fs::read_to_string(path) else {
        return;
    };
    let mut section = String::new();
    let mut values = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if line.starts_with('[') && line.ends_with(']') {
            if section == profile {
                break;
            }
            section = line[1..line.len() - 1].to_string();
            values.clear();
            continue;
        }
        if section == profile {
            if let Some((k, v)) = line.split_once('=') {
                values.insert(k.trim().to_string(), v.trim().to_string());
            }
        }
    }
    if section != profile && values.is_empty() {
        return;
    }
    if let Some(v) = values.get("aws_access_key_id").or_else(|| values.get("access_key_id")) {
        *access = Some(v.clone());
    }
    if let Some(v) = values.get("aws_secret_access_key").or_else(|| values.get("secret_access_key")) {
        *secret = Some(v.clone());
    }
    if let Some(v) = values.get("role_arn") {
        *role = Some(v.clone());
    }
    if let Some(v) = values.get("web_identity_token_file") {
        *web = Some(v.clone());
    }
    if let Some(v) = values.get("endpoint_url") {
        *endpoint = Some(v.clone());
    }
}

fn assume_web_identity(
    region: &str,
    endpoint: Option<&str>,
    role_arn: &str,
    token_file: &str,
    access: Option<&str>,
    secret: Option<&str>,
) -> Result<(String, String, String), AdvanceError> {
    let token = std::fs::read_to_string(token_file).map_err(|e| AdvanceError::Other(format!("web identity token: {e}")))?;
    let host = endpoint
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("https://sts.{region}.amazonaws.com/"));
    let body = format!(
        "Action=AssumeRoleWithWebIdentity&Version=2011-06-15&RoleArn={}&RoleSessionName=zuul&WebIdentityToken={}",
        aws_encode(role_arn),
        aws_encode(token.trim())
    );
    // Anonymous call when the connection has no static keys. When keys
    // exist, boto still signs the STS call with them.
    let headers = vec![("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")];
    let resp = if let (Some(ak), Some(sk)) = (access, secret) {
        signed_request(
            "POST",
            &host,
            "sts",
            region,
            ak,
            sk,
            None,
            &headers,
            body.as_bytes(),
            true,
        )?
    } else {
        let url = if host.ends_with('/') { host.clone() } else { format!("{host}/") };
        exchange("POST", &url, &headers, body.as_bytes(), HTTP_TIMEOUT).map_err(err_other)?
    };
    let parsed = ec2_xml_to_boto(&resp.text()).map_err(err_other)?;
    let creds = parsed
        .pointer("/AssumeRoleWithWebIdentityResult/Credentials")
        .cloned()
        .or_else(|| parsed.get("Credentials").cloned())
        .ok_or_else(|| AdvanceError::Other(format!("STS response missing credentials: {}", resp.text())))?;
    Ok((
        field_str(&creds, &["AccessKeyId"]).unwrap_or_default(),
        field_str(&creds, &["SecretAccessKey"]).unwrap_or_default(),
        field_str(&creds, &["SessionToken"]).unwrap_or_default(),
    ))
}

fn signed_request(
    method: &str,
    url: &str,
    service: &str,
    region: &str,
    access: &str,
    secret: &str,
    session: Option<&str>,
    headers: &[(&str, &str)],
    body: &[u8],
    mutating: bool,
) -> Result<crate::cloud_http::HttpResponse, AdvanceError> {
    let _ = mutating;
    let (host, path, query) = split_url(url)?;
    let sig = SigV4 {
        access_key: access.into(),
        secret_key: secret.into(),
        session_token: session.map(|s| s.to_string()),
        region: region.into(),
        service: service.into(),
    };
    let date = amz_now();
    let (_auth, signed) = sig.authorization(method, &host, &path, query, headers, body, &date);
    let mut all: Vec<(String, String)> = headers
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect();
    for (k, v) in signed {
        if !all.iter().any(|(ek, _)| ek.eq_ignore_ascii_case(&k)) {
            all.push((k, v));
        }
    }
    let refs: Vec<(&str, &str)> = all.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let mut last_err = String::new();
    for attempt in 0..MAX_ATTEMPTS {
        match exchange(method, url, &refs, body, HTTP_TIMEOUT) {
            Ok(resp) if resp.status < 400 => return Ok(resp),
            Ok(resp) => {
                let text = resp.text();
                let retry = aws_retryable(resp.status, &text);
                last_err = format!("HTTP {} {text}", resp.status);
                if !retry || attempt + 1 == MAX_ATTEMPTS {
                    return Err(err_other(last_err));
                }
                std::thread::sleep(standard_backoff(attempt + 1));
            }
            Err(e) => {
                last_err = e;
                if attempt + 1 == MAX_ATTEMPTS {
                    return Err(err_other(last_err));
                }
                std::thread::sleep(standard_backoff(attempt + 1));
            }
        }
    }
    Err(err_other(last_err))
}

/// botocore standard mode: `min(20, random() * 2**attempt_number)`.
fn standard_backoff(attempt_number: u32) -> Duration {
    let mut buf = [0u8; 8];
    let _ = getrandom::getrandom(&mut buf);
    let unit = (u64::from_le_bytes(buf) as f64) / (u64::MAX as f64);
    let exp = 2.0_f64.powi(attempt_number as i32);
    Duration::from_secs_f64((unit * exp).min(20.0))
}

fn aws_retryable(status: u16, body: &str) -> bool {
    if matches!(status, 429 | 500 | 502 | 503 | 504) {
        return true;
    }
    if status < 400 {
        return false;
    }
    // Parsed error codes botocore standard mode retries, including
    // TooManyRequestsException (the reason Zuul sets mode=standard).
    const CODES: &[&str] = &[
        "BandwidthLimitExceeded",
        "EC2ThrottledException",
        "IDPCommunicationError",
        "InternalError",
        "InternalFailure",
        "LimitExceededException",
        "PriorRequestNotComplete",
        "ProvisionedThroughputExceededException",
        "RequestLimitExceeded",
        "RequestThrottled",
        "RequestThrottledException",
        "RequestTimeout",
        "RequestTimeoutException",
        "ServiceUnavailable",
        "SlowDown",
        "ThrottledException",
        "Throttling",
        "ThrottlingException",
        "TooManyRequestsException",
        "TransactionInProgressException",
        "Unavailable",
    ];
    CODES.iter().any(|code| body.contains(code))
}

fn split_url(url: &str) -> Result<(String, String, &'static str), AdvanceError> {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .ok_or_else(|| AdvanceError::Other(format!("bad url {url}")))?;
    let (hostport, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let (path, _query) = path.split_once('?').unwrap_or((path, ""));
    let host = if let Some((h, port)) = hostport.rsplit_once(':') {
        if (url.starts_with("https://") && port == "443") || (url.starts_with("http://") && port == "80") {
            h.to_string()
        } else {
            hostport.to_string()
        }
    } else {
        hostport.to_string()
    };
    let path = if path.is_empty() { "/" } else { path };
    // Query string is returned empty; callers that sign a query put it
    // in the URL path for S3 and pass an empty canonical query when the
    // parameters are in the body (EC2).
    let _ = _query;
    Ok((host, path.to_string(), ""))
}

fn endpoint_arc(ep: &AwsEndpoint) -> Option<Arc<AwsEndpoint>> {
    registry()
        .lock()
        .unwrap()
        .aws
        .values()
        .find(|cand| std::ptr::eq(Arc::as_ptr(cand), ep as *const AwsEndpoint))
        .cloned()
}

fn aws_endpoint(provider: &ProviderSnap) -> Result<Arc<AwsEndpoint>, AdvanceError> {
    let key = format!("{}:{}", provider.connection, provider.region);
    if let Some(existing) = registry().lock().unwrap().aws.get(&key).cloned() {
        return Ok(existing);
    }
    let conn = connection_of(&provider.connection)?;
    let creds = aws_creds(&conn)?;
    let region = if provider.region.is_empty() {
        std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|_| "us-east-1".into())
    } else {
        provider.region.clone()
    };
    let endpoint = creds
        .endpoint
        .clone()
        .unwrap_or_else(|| format!("https://ec2.{region}.amazonaws.com"));
    let rate = conn_rate(&conn);
    let ep = Arc::new(AwsEndpoint {
        region: region.clone(),
        endpoint,
        sig: SigV4 {
            access_key: creds.access_key,
            secret_key: creds.secret_key,
            session_token: creds.session_token,
            region,
            service: "ec2".into(),
        },
        mutating: RateLimiter::new(rate),
        reading: RateLimiter::new(rate * 10.0),
        quota_limit: RateLimiter::new(rate),
        lists: Mutex::new(BTreeMap::new()),
        not_our_images: Mutex::new(HashSet::new()),
        not_our_snapshots: Mutex::new(HashSet::new()),
    });
    registry().lock().unwrap().aws.insert(key, ep.clone());
    Ok(ep)
}

fn ec2(ep: &AwsEndpoint, action: &str, params: Map<String, Value>, reading: bool) -> Result<Value, AdvanceError> {
    if reading {
        ep.reading.enter();
    } else {
        ep.mutating.enter();
    }
    let mut params = params;
    params.insert("Action".into(), json!(action));
    params.insert("Version".into(), json!(EC2_VERSION));
    let body = ec2_query(&params);
    let url = if ep.endpoint.ends_with('/') {
        ep.endpoint.clone()
    } else {
        format!("{}/", ep.endpoint)
    };
    let resp = signed_request(
        "POST",
        &url,
        "ec2",
        &ep.region,
        &ep.sig.access_key,
        &ep.sig.secret_key,
        ep.sig.session_token.as_deref(),
        &[("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")],
        body.as_bytes(),
        !reading,
    )?;
    let parsed = ec2_xml_to_boto(&resp.text()).map_err(err_other)?;
    if let Some(code) = parsed.pointer("/Errors/Error/Code").and_then(|v| v.as_str())
        .or_else(|| parsed.pointer("/Error/Code").and_then(|v| v.as_str()))
    {
        let message = parsed
            .pointer("/Errors/Error/Message")
            .or_else(|| parsed.pointer("/Error/Message"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        return Err(err_other(format!("{code}: {message}")));
    }
    // Errors come back as an array after the item fold.
    if let Some(code) = parsed.pointer("/Errors/0/Code").and_then(|v| v.as_str()) {
        let message = parsed.pointer("/Errors/0/Message").and_then(|v| v.as_str()).unwrap_or("");
        return Err(err_other(format!("{code}: {message}")));
    }
    Ok(parsed)
}

enum CacheHit {
    Fresh(Value),
    Stale(Value),
    Miss,
}

/// LazyExecutorTTLCache shape: a hit inside the TTL returns the cached
/// value. After the TTL, the caller gets the stale value and a refresh
/// runs in the background.
fn cache_hit(ep: &AwsEndpoint, key: &str, ttl: Duration) -> CacheHit {
    let mut lists = ep.lists.lock().unwrap();
    if let Some(entry) = lists.get_mut(key) {
        if entry.at.elapsed() < ttl {
            return CacheHit::Fresh(entry.value.clone());
        }
        let stale = entry.value.clone();
        if !entry.refreshing {
            entry.refreshing = true;
            return CacheHit::Stale(stale);
        }
        return CacheHit::Fresh(stale);
    }
    CacheHit::Miss
}

fn cache_store(ep: &AwsEndpoint, key: &str, value: Value) {
    ep.lists.lock().unwrap().insert(
        key.into(),
        CachedList {
            at: Instant::now(),
            value,
            refreshing: false,
        },
    );
}

fn cache_clear_inflight(ep: &AwsEndpoint, key: &str) {
    if let Some(entry) = ep.lists.lock().unwrap().get_mut(key) {
        entry.refreshing = false;
    }
}

fn cache_forget(ep: &AwsEndpoint, key: &str) {
    ep.lists.lock().unwrap().remove(key);
}

fn ttl_lists() -> &'static Mutex<BTreeMap<String, CachedList>> {
    static LISTS: std::sync::OnceLock<Mutex<BTreeMap<String, CachedList>>> = std::sync::OnceLock::new();
    LISTS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn clear_ttl_cache() {
    ttl_lists().lock().unwrap().clear();
}

/// LazyExecutorTTLCache for OpenStack and Azure lists. A hit inside the
/// TTL returns the cached value. After that the caller gets the stale
/// value and a refresh runs in the background. The first call is synchronous.
fn with_ttl_cache(
    key: &str,
    ttl: Duration,
    load: std::sync::Arc<dyn Fn() -> Result<Value, AdvanceError> + Send + Sync>,
) -> Result<Value, AdvanceError> {
    let stale = {
        let mut guard = ttl_lists().lock().unwrap();
        if let Some(entry) = guard.get_mut(key) {
            if entry.at.elapsed() < ttl {
                return Ok(entry.value.clone());
            }
            let stale = entry.value.clone();
            if entry.refreshing {
                return Ok(stale);
            }
            entry.refreshing = true;
            Some(stale)
        } else {
            None
        }
    };
    if let Some(stale) = stale {
        let key = key.to_string();
        let load = load.clone();
        std::thread::spawn(move || match load() {
            Ok(fresh) => {
                ttl_lists().lock().unwrap().insert(
                    key,
                    CachedList {
                        at: Instant::now(),
                        value: fresh,
                        refreshing: false,
                    },
                );
            }
            Err(_) => {
                if let Some(entry) = ttl_lists().lock().unwrap().get_mut(&key) {
                    entry.refreshing = false;
                }
            }
        });
        return Ok(stale);
    }
    let fresh = load()?;
    ttl_lists().lock().unwrap().insert(
        key.to_string(),
        CachedList {
            at: Instant::now(),
            value: fresh.clone(),
            refreshing: false,
        },
    );
    Ok(fresh)
}

fn cached_ec2_items(
    ep: &Arc<AwsEndpoint>,
    cache_key: &str,
    action: &str,
    item_key: &str,
    base: Map<String, Value>,
) -> Result<Vec<Value>, AdvanceError> {
    let fetch = |ep: &AwsEndpoint, action: &str, item_key: &str, base: &Map<String, Value>| -> Result<Vec<Value>, AdvanceError> {
        let mut params = base.clone();
        let mut all = Vec::new();
        loop {
            let page = ec2(ep, action, params.clone(), true)?;
            if let Some(items) = page.get(item_key).and_then(|v| v.as_array()) {
                all.extend(items.clone());
            }
            let token = page.get("NextToken").and_then(|v| v.as_str()).unwrap_or("");
            if token.is_empty() {
                break;
            }
            params.insert("NextToken".into(), json!(token));
        }
        Ok(all)
    };
    match cache_hit(ep, cache_key, LIST_TTL) {
        CacheHit::Fresh(value) => Ok(value.as_array().cloned().unwrap_or_default()),
        CacheHit::Stale(value) => {
            let bg = ep.clone();
            let action = action.to_string();
            let item_key = item_key.to_string();
            let base = base.clone();
            let key = cache_key.to_string();
            std::thread::spawn(move || match fetch(&bg, &action, &item_key, &base) {
                Ok(items) => cache_store(&bg, &key, Value::Array(items)),
                Err(_) => cache_clear_inflight(&bg, &key),
            });
            Ok(value.as_array().cloned().unwrap_or_default())
        }
        CacheHit::Miss => {
            let items = fetch(ep, action, item_key, &base)?;
            cache_store(ep, cache_key, Value::Array(items.clone()));
            Ok(items)
        }
    }
}

fn list_volumes_cached(ep: &Arc<AwsEndpoint>) -> Result<Vec<Value>, AdvanceError> {
    cached_ec2_items(ep, "volumes", "DescribeVolumes", "Volumes", Map::new())
}

fn list_images_cached(ep: &Arc<AwsEndpoint>) -> Result<Vec<Value>, AdvanceError> {
    let mut params = Map::new();
    params.insert("Owners".into(), json!(["self"]));
    cached_ec2_items(ep, "images", "DescribeImages", "Images", params)
}

fn list_snapshots_cached(ep: &Arc<AwsEndpoint>) -> Result<Vec<Value>, AdvanceError> {
    let mut params = Map::new();
    params.insert("OwnerIds".into(), json!(["self"]));
    cached_ec2_items(ep, "snapshots", "DescribeSnapshots", "Snapshots", params)
}

fn list_launch_templates_cached(ep: &Arc<AwsEndpoint>) -> Result<Vec<Value>, AdvanceError> {
    let mut params = Map::new();
    params.insert(
        "Filters".into(),
        json!([{"Name": "launch-template-name", "Value": ["zuul-launch-template-*"]}]),
    );
    let listed = cached_ec2_items(ep, "launch-templates", "DescribeLaunchTemplates", "LaunchTemplates", params)?;
    Ok(listed
        .into_iter()
        .filter(|t| {
            t.get("LaunchTemplateName")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .starts_with("zuul-launch-template-")
        })
        .collect())
}

fn instances_of(page: &Value) -> Vec<Value> {
    let mut instances = Vec::new();
    if let Some(reservations) = page.as_array() {
        for res in reservations {
            if let Some(arr) = res.get("Instances").and_then(|v| v.as_array()) {
                instances.extend(arr.clone());
            }
        }
    }
    instances
}

fn fetch_reservations(ep: &AwsEndpoint) -> Result<Value, AdvanceError> {
    let mut token: Option<String> = None;
    let mut reservations = Vec::new();
    loop {
        let mut params = Map::new();
        if let Some(t) = &token {
            params.insert("NextToken".into(), json!(t));
        }
        let page = ec2(ep, "DescribeInstances", params, true)?;
        if let Some(arr) = page.get("Reservations").and_then(|v| v.as_array()) {
            reservations.extend(arr.clone());
        }
        token = page
            .get("NextToken")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        if token.is_none() {
            break;
        }
    }
    Ok(Value::Array(reservations))
}

fn list_instances(ep: &Arc<AwsEndpoint>) -> Result<Vec<Value>, AdvanceError> {
    match cache_hit(ep, "instances", LIST_TTL) {
        CacheHit::Fresh(page) => Ok(instances_of(&page)),
        CacheHit::Stale(page) => {
            let bg = ep.clone();
            std::thread::spawn(move || match fetch_reservations(&bg) {
                Ok(fresh) => cache_store(&bg, "instances", fresh),
                Err(_) => cache_clear_inflight(&bg, "instances"),
            });
            Ok(instances_of(&page))
        }
        CacheHit::Miss => {
            let page = fetch_reservations(ep)?;
            cache_store(ep, "instances", page.clone());
            Ok(instances_of(&page))
        }
    }
}

fn list_hosts(ep: &Arc<AwsEndpoint>) -> Result<Vec<Value>, AdvanceError> {
    cached_ec2_items(ep, "hosts", "DescribeHosts", "Hosts", Map::new())
}

fn aws_refresh(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let ep = aws_endpoint(&snap)?;
    let obj = args.get("obj").cloned().unwrap_or(Value::Null);
    if let Some(id) = obj.get("InstanceId").and_then(|v| v.as_str()) {
        for instance in list_instances(&ep)? {
            if instance.get("InstanceId").and_then(|v| v.as_str()) == Some(id) {
                return Ok(json!({"obj": instance}));
            }
        }
        return Ok(json!({"obj": obj}));
    }
    if let Some(id) = obj.get("HostId").and_then(|v| v.as_str()) {
        for host in list_hosts(&ep)? {
            if host.get("HostId").and_then(|v| v.as_str()) == Some(id) {
                return Ok(json!({"obj": host}));
            }
        }
    }
    if let Some(id) = obj.get("SnapshotId").and_then(|v| v.as_str()) {
        let mut params = Map::new();
        params.insert("SnapshotId".into(), json!([id]));
        let resp = ec2(&ep, "DescribeSnapshots", params, true)?;
        if let Some(snap) = resp.get("Snapshots").and_then(|v| v.as_array()).and_then(|a| a.first()) {
            return Ok(json!({"obj": snap.clone()}));
        }
    }
    Ok(json!({"obj": obj}))
}

fn refresh_any(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    if snap.driver == "azure" {
        return azure_refresh_obj(args, &snap, false);
    }
    aws_refresh(args)
}

fn refresh_delete_any(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    if snap.driver == "azure" {
        return azure_refresh_obj(args, &snap, true);
    }
    aws_refresh_delete(args)
}

fn aws_refresh_delete(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let ep = aws_endpoint(&snap)?;
    let obj = args.get("obj").cloned().unwrap_or(Value::Null);
    if obj.is_null() {
        return Ok(json!({"obj": null}));
    }
    if let Some(id) = obj.get("InstanceId").and_then(|v| v.as_str()) {
        for instance in list_instances(&ep)? {
            if instance.get("InstanceId").and_then(|v| v.as_str()) == Some(id) {
                let state = instance.pointer("/State/Name").and_then(|v| v.as_str()).unwrap_or("");
                if state.eq_ignore_ascii_case("terminated") {
                    return Ok(json!({"obj": null}));
                }
                return Ok(json!({"obj": instance}));
            }
        }
        return Ok(json!({"obj": null}));
    }
    if let Some(id) = obj.get("HostId").and_then(|v| v.as_str()) {
        for host in list_hosts(&ep)? {
            if host.get("HostId").and_then(|v| v.as_str()) == Some(id) {
                let state = host.get("State").and_then(|v| v.as_str()).unwrap_or("");
                if state.eq_ignore_ascii_case("released") || state.eq_ignore_ascii_case("released-permanent-failure") {
                    return Ok(json!({"obj": null}));
                }
                return Ok(json!({"obj": host}));
            }
        }
    }
    Ok(json!({"obj": null}))
}

fn label_flavor_image(snap: &ProviderSnap, label_name: &str) -> Result<(Value, Value, Value), AdvanceError> {
    let label = snap
        .labels
        .get(label_name)
        .cloned()
        .ok_or_else(|| AdvanceError::Other(format!("no label {label_name}")))?;
    let flavor_name = field_str(&label, &["flavor"]).unwrap_or_default();
    let image_name = field_str(&label, &["image"]).unwrap_or_default();
    let flavor = snap.flavors.get(&flavor_name).cloned().unwrap_or(json!({}));
    let image = snap.images.get(&image_name).cloned().unwrap_or(json!({}));
    Ok((label, flavor, image))
}

fn complete_aws_create(args: &Value) -> Result<Value, AdvanceError> {
    let job = take_future(args)?;
    let snap = {
        let mut lookup = job.payload.clone();
        if lookup.get("provider").is_none() {
            lookup["provider"] = json!(job.provider);
        }
        provider_of(&lookup)?
    };
    let ep = aws_endpoint(&snap)?;
    let label_name = field_str(&job.payload, &["label"]).unwrap_or_default();
    let (label, flavor, image) = label_flavor_image(&snap, &label_name)?;
    let image_id = if let Some(id) = field_str(&job.payload, &["image_external_id"]) {
        id
    } else if let Some(filters) = field(&image, &["image-filters", "image_filters"]) {
        latest_image_id(&ep, filters)?
    } else {
        field_str(&image, &["image-id", "image_id"]).unwrap_or_default()
    };
    let tags = job.payload.get("tags").cloned().unwrap_or(json!({}));
    let hostname = field_str(&job.payload, &["hostname"]).unwrap_or_default();
    let host_id = field_str(&job.payload, &["dedicated_host_id"]);
    let instance = run_instances(
        &ep,
        &job.provider,
        &label,
        &flavor,
        &image_id,
        &tags,
        &hostname,
        host_id.as_deref(),
    )?;
    Ok(json!({"instance": instance}))
}

fn tag_list(tags: &Value) -> Value {
    let mut out = Vec::new();
    if let Some(obj) = tags.as_object() {
        for (k, v) in obj {
            let value = match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            out.push(json!({"Key": k, "Value": value}));
        }
    }
    Value::Array(out)
}

fn run_instances(
    ep: &AwsEndpoint,
    provider_name: &str,
    label: &Value,
    flavor: &Value,
    image_id: &str,
    tags: &Value,
    _hostname: &str,
    dedicated_host_id: Option<&str>,
) -> Result<Value, AdvanceError> {
    if flavor.get("fleet").and_then(|v| v.as_object()).is_some() {
        return create_fleet(ep, provider_name, label, flavor, image_id, tags);
    }
    let mut nic = Map::new();
    let public_v4 = field(flavor, &["public-ipv4", "public_ipv4"]).and_then(|v| v.as_bool()).unwrap_or(false);
    nic.insert("AssociatePublicIpAddress".into(), json!(public_v4));
    nic.insert("DeviceIndex".into(), json!(0));
    let groups = as_str_list(field(label, &["security-group-id", "security_group_ids", "security-group-ids"]));
    if !groups.is_empty() {
        nic.insert("Groups".into(), json!(groups));
    }
    let subnets = as_str_list(field(label, &["subnet-id", "subnet_ids", "subnet-ids"]));
    if !subnets.is_empty() {
        // awsendpoint._getInstanceConfiguration uses random.choice.
        let idx = random_index(subnets.len());
        nic.insert("SubnetId".into(), json!(subnets[idx]));
    }
    if field(flavor, &["public-ipv6", "public_ipv6"]).and_then(|v| v.as_bool()) == Some(true) {
        nic.insert("Ipv6AddressCount".into(), json!(1));
    }
    let mut params = Map::new();
    params.insert("ImageId".into(), json!(image_id));
    params.insert("MinCount".into(), json!(1));
    params.insert("MaxCount".into(), json!(1));
    if let Some(key) = field_str(label, &["key-name", "key_name"]) {
        params.insert("KeyName".into(), json!(key));
    }
    let ebs_opt = field(flavor, &["ebs-optimized", "ebs_optimized"]).and_then(|v| v.as_bool()).unwrap_or(false);
    params.insert("EbsOptimized".into(), json!(ebs_opt));
    if let Some(itype) = field_str(flavor, &["instance-type", "instance_type"]) {
        params.insert("InstanceType".into(), json!(itype));
    }
    params.insert("NetworkInterfaces".into(), json!([Value::Object(nic)]));
    let tag_spec = json!([
        {"ResourceType": "instance", "Tag": tag_list(tags)},
        {"ResourceType": "volume", "Tag": tag_list(tags)},
    ]);
    params.insert("TagSpecifications".into(), tag_spec);
    params.insert("MaintenanceOptions".into(), json!({"AutoRecovery": "disabled"}));
    if field(flavor, &["nested-virtualization", "nested_virtualization"]).and_then(|v| v.as_bool()) == Some(true) {
        params.insert("CpuOptions".into(), json!({"NestedVirtualization": "enabled"}));
    }
    if let Some(user) = field_str(label, &["userdata", "user-data"]) {
        params.insert("UserData".into(), json!(user));
    }
    if let Some(profile) = field(label, &["iam-instance-profile", "iam_instance_profile"]) {
        if let Some(name) = profile.get("name").and_then(|v| v.as_str()) {
            params.insert("IamInstanceProfile".into(), json!({"Name": name}));
        } else if let Some(arn) = profile.get("arn").and_then(|v| v.as_str()) {
            params.insert("IamInstanceProfile".into(), json!({"Arn": arn}));
        }
    }
    if let Some(market) = field_str(flavor, &["market-type", "market_type"]) {
        if market == "spot" {
            params.insert(
                "InstanceMarketOptions".into(),
                json!({"MarketType": "spot", "SpotOptions": {"SpotInstanceType": "one-time", "InstanceInterruptionBehavior": "terminate"}}),
            );
        }
    }
    if let Some(tokens) = field_str(label, &["imds-http-tokens", "imds_http_tokens"]) {
        if tokens == "required" || tokens == "optional" {
            params.insert(
                "MetadataOptions".into(),
                json!({"HttpTokens": tokens, "HttpEndpoint": "enabled", "HttpProtocolIpv6": "enabled"}),
            );
        }
    }
    let mut placement = Map::new();
    if let Some(host) = dedicated_host_id {
        placement.insert("Tenancy".into(), json!("host"));
        placement.insert("HostId".into(), json!(host));
        placement.insert("Affinity".into(), json!("host"));
    }
    if let Some(az) = field_str(label, &["az", "availability-zone"]) {
        placement.insert("AvailabilityZone".into(), json!(az));
    }
    if !placement.is_empty() {
        params.insert("Placement".into(), Value::Object(placement));
    }
    // Describe the AMI and copy the first block device mapping with
    // DeleteOnTermination, matching _getInstanceConfiguration.
    if let Ok(image) = describe_image(ep, image_id) {
        if let Some(bdm) = image.get("BlockDeviceMappings").and_then(|v| v.as_array()).and_then(|a| a.first()) {
            let mut mapping = bdm.clone();
            if let Some(ebs) = mapping.get_mut("Ebs").and_then(|v| v.as_object_mut()) {
                ebs.insert("DeleteOnTermination".into(), json!(true));
                ebs.remove("Encrypted");
                if let Some(size) = field(label, &["volume-size", "volume_size"]) {
                    ebs.insert("VolumeSize".into(), size.clone());
                }
                if let Some(kind) = field_str(label, &["volume-type", "volume_type"]) {
                    ebs.insert("VolumeType".into(), json!(kind));
                }
                if let Some(iops) = field(label, &["iops"]) {
                    ebs.insert("Iops".into(), iops.clone());
                }
                if let Some(tp) = field(label, &["throughput"]) {
                    ebs.insert("Throughput".into(), tp.clone());
                }
                if let Some(kms) = field_str(label, &["kms-key-id", "kms_key_id"]) {
                    ebs.insert("KmsKeyId".into(), json!(kms));
                    ebs.insert("Encrypted".into(), json!(true));
                }
            }
            params.insert("BlockDeviceMappings".into(), json!([mapping]));
        }
    }
    let resp = ec2(ep, "RunInstances", params, false)?;
    resp.get("Instances")
        .and_then(|v| v.as_array())
        .and_then(|a| a.first())
        .cloned()
        .ok_or_else(|| AdvanceError::Other(format!("RunInstances returned no instance: {resp}")))
}

fn describe_image(ep: &AwsEndpoint, image_id: &str) -> Result<Value, AdvanceError> {
    let mut params = Map::new();
    params.insert("ImageId".into(), json!([image_id]));
    let resp = ec2(ep, "DescribeImages", params, true)?;
    resp.get("Images")
        .and_then(|v| v.as_array())
        .and_then(|a| a.first())
        .cloned()
        .ok_or_else(|| AdvanceError::Other(format!("image {image_id} not found")))
}

fn latest_image_id(ep: &AwsEndpoint, filters: &Value) -> Result<String, AdvanceError> {
    let mut params = Map::new();
    let mut encoded = Vec::new();
    if let Some(arr) = filters.as_array() {
        for fltr in arr {
            let mut one = Map::new();
            if let Some(obj) = fltr.as_object() {
                for (k, v) in obj {
                    let name = camel_first(k);
                    one.insert(name, v.clone());
                }
            }
            encoded.push(Value::Object(one));
        }
    }
    params.insert("Filter".into(), Value::Array(encoded));
    let resp = ec2(ep, "DescribeImages", params, true)?;
    let mut images = resp.get("Images").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    images.sort_by(|a, b| {
        let da = a.get("CreationDate").and_then(|v| v.as_str()).unwrap_or("");
        let db = b.get("CreationDate").and_then(|v| v.as_str()).unwrap_or("");
        db.cmp(da)
    });
    images
        .first()
        .and_then(|i| i.get("ImageId"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| AdvanceError::Other("No cloud-image (AMI) matches supplied image filters".into()))
}

fn camel_first(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        Some(f) => format!("{}{}", f.to_ascii_uppercase(), c.as_str()),
        None => String::new(),
    }
}

fn complete_aws_host(args: &Value) -> Result<Value, AdvanceError> {
    let job = take_future(args)?;
    let snap = provider_of(&job.payload)?;
    let ep = aws_endpoint(&snap)?;
    let label_name = field_str(&job.payload, &["label"]).unwrap_or_default();
    let (label, flavor, _) = label_flavor_image(&snap, &label_name)?;
    let mut params = Map::new();
    params.insert("AutoPlacement".into(), json!("off"));
    params.insert("Quantity".into(), json!(1));
    if let Some(az) = field_str(&label, &["az"]) {
        params.insert("AvailabilityZone".into(), json!(az));
    }
    if let Some(ht) = field_str(&flavor, &["instance-type", "instance_type"]) {
        params.insert("InstanceType".into(), json!(ht));
    }
    params.insert("TagSpecification".into(), json!([{"ResourceType": "dedicated-host", "Tag": tag_list(job.payload.get("tags").unwrap_or(&json!({})))}]));
    let resp = ec2(&ep, "AllocateHosts", params, false)?;
    let host_id = resp
        .get("HostIds")
        .and_then(|v| v.as_array())
        .and_then(|a| a.first())
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    Ok(json!({"host": {"HostId": host_id, "State": "pending"}}))
}

fn aws_delete_instance(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let ep = aws_endpoint(&snap)?;
    let id = args.get("instance_id").and_then(|v| v.as_str()).unwrap_or("");
    let mut params = Map::new();
    params.insert("InstanceId".into(), json!([id]));
    params.insert("SkipOsShutdown".into(), json!(true));
    ec2(&ep, "TerminateInstances", params, false)?;
    ep.lists.lock().unwrap().remove("instances");
    Ok(json!({"instance": {"InstanceId": id, "State": {"Name": "shutting-down"}}}))
}

fn aws_release_host(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let ep = aws_endpoint(&snap)?;
    let id = args.get("host_id").and_then(|v| v.as_str()).unwrap_or("");
    let mut params = Map::new();
    params.insert("HostId".into(), json!([id]));
    ec2(&ep, "ReleaseHosts", params, false)?;
    Ok(json!({"host": {"HostId": id, "State": "released"}}))
}

fn aws_create_snapshot(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let ep = aws_endpoint(&snap)?;
    let mut params = Map::new();
    params.insert("VolumeId".into(), json!(args.get("volume_id").and_then(|v| v.as_str()).unwrap_or("")));
    params.insert("TagSpecification".into(), json!([{"ResourceType": "snapshot", "Tag": tag_list(args.get("tags").unwrap_or(&json!({})))}]));
    let resp = ec2(&ep, "CreateSnapshot", params, false)?;
    Ok(json!({"snapshot": resp}))
}

fn quota_for_label(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    match snap.driver.as_str() {
        "kubernetes" => {
            let label_name = args.get("label").and_then(|v| v.as_str()).unwrap_or("");
            let kind = label_flavor_image(&snap, label_name)
                .ok()
                .and_then(|(label, _, _)| field_str(&label, &["kind"]))
                .unwrap_or_default();
            let mut quota = Map::new();
            quota.insert("namespaces".into(), json!(1));
            if kind == "pod" {
                quota.insert("pods".into(), json!(1));
            }
            Ok(json!({"default": null, "quota": quota}))
        }
        "aws" => aws_quota(&snap, args),
        "openstack" => os_find_flavor(args).map(|v| {
            v.get("quota")
                .cloned()
                .unwrap_or(json!({"default": null, "quota": {"instances": 1}}))
        }),
        "azure" => azure_quota_for_label(&snap, args),
        _ => Ok(json!({"default": null, "quota": {}})),
    }
}

fn aws_quota(snap: &ProviderSnap, args: &Value) -> Result<Value, AdvanceError> {
    let ep = aws_endpoint(snap)?;
    let label_name = args.get("label").and_then(|v| v.as_str()).unwrap_or("");
    let (label, flavor, _) = label_flavor_image(snap, label_name)?;
    let instance_type = args
        .get("instance_type")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| field_str(&flavor, &["instance-type", "instance_type"]))
        .unwrap_or_default();
    let dedicated = field(&flavor, &["dedicated-host", "dedicated_host"])
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let fleet = flavor.get("fleet").and_then(|v| v.as_object()).is_some();
    let mut quota = Map::new();
    if dedicated {
        // _getQuotaForHostType: one host, plus the host quota code.
        quota.insert("instances".into(), json!(1));
        if let Some(code) = host_quota_code(&instance_type) {
            quota.insert(code, json!(1));
        }
    } else if fleet && args.get("instance_type").and_then(|v| v.as_str()).is_none() {
        quota.insert("instances".into(), json!(1));
    } else {
        let mut params = Map::new();
        params.insert("InstanceType".into(), json!([instance_type.clone()]));
        ep.quota_limit.enter();
        let resp = ec2(&ep, "DescribeInstanceTypes", params, true)?;
        let info = resp.pointer("/InstanceTypes/0").cloned().unwrap_or(Value::Null);
        let cores = info.pointer("/VCpuInfo/DefaultCores").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let vcpus = info.pointer("/VCpuInfo/DefaultVCpus").and_then(|v| v.as_f64()).unwrap_or(cores);
        let ram = info.pointer("/MemoryInfo/SizeInMiB").and_then(|v| v.as_f64()).unwrap_or(0.0);
        quota.insert("cores".into(), json!(cores));
        quota.insert("ram".into(), json!(ram));
        quota.insert("instances".into(), json!(1));
        let market = field_str(&flavor, &["market-type", "market_type"]).unwrap_or_default();
        if let Some(code) = instance_quota_code(&instance_type, market == "spot") {
            if !code.is_empty() {
                quota.insert(code, json!(vcpus));
            }
        }
    }
    apply_volume_quota(&mut quota, &label);
    let _ = QUOTA_TTL;
    Ok(json!({"default": null, "quota": quota}))
}

fn instance_quota_code(instance_type: &str, spot: bool) -> Option<String> {
    // Same family prefixes as zuul/driver/aws/const.py INSTANCE_QUOTA_CODES.
    // Longer prefixes win. Spot is the second code.
    const CODES: &[(&str, &str, &str)] = &[
        ("a", "L-1216C47A", "L-34B43A08"),
        ("c", "L-1216C47A", "L-34B43A08"),
        ("d", "L-1216C47A", "L-34B43A08"),
        ("h", "L-1216C47A", "L-34B43A08"),
        ("i", "L-1216C47A", "L-34B43A08"),
        ("m", "L-1216C47A", "L-34B43A08"),
        ("r", "L-1216C47A", "L-34B43A08"),
        ("t", "L-1216C47A", "L-34B43A08"),
        ("z", "L-1216C47A", "L-34B43A08"),
        ("dl", "L-6E869C2A", "L-85EED4F7"),
        ("f", "L-74FC7D96", "L-88CF9481"),
        ("g", "L-DB2E81BA", "L-3819A6DF"),
        ("vt", "L-DB2E81BA", "L-3819A6DF"),
        ("u-", "L-43DA4232", ""),
        ("inf", "L-1945791B", "L-B5D1601B"),
        ("p", "L-417A185B", "L-7212CCBC"),
        ("x", "L-7295265B", "L-E3A00192"),
        ("trn", "L-2C3B7624", "L-6B0D517C"),
        ("hpc", "L-F7808C92", ""),
    ];
    let family = instance_type
        .chars()
        .take_while(|c| !c.is_ascii_digit())
        .collect::<String>();
    let mut best: Option<&(&str, &str, &str)> = None;
    for row in CODES {
        if family.starts_with(row.0) && best.map(|b| row.0.len() > b.0.len()).unwrap_or(true) {
            best = Some(row);
        }
    }
    best.map(|row| if spot { row.2 } else { row.1 }.to_string())
}

/// S3 PutObject. Custom endpoints (moto) use path-style; AWS uses
/// virtual-hosted `{bucket}.s3.{region}.amazonaws.com`.
pub fn s3_put(provider_name: &str, bucket: &str, key: &str, body: &[u8], content_type: &str) -> Result<(), AdvanceError> {
    s3_put_tagged(provider_name, bucket, key, body, content_type, None)
}

/// PutObject. `tagging` is the `x-amz-tagging` header boto sets from
/// `ExtraArgs={'Tagging': urlencode(metadata)}`.
pub fn s3_put_tagged(
    provider_name: &str,
    bucket: &str,
    key: &str,
    body: &[u8],
    content_type: &str,
    tagging: Option<&str>,
) -> Result<(), AdvanceError> {
    let snap = provider_by_name(provider_name)?;
    let ep = aws_endpoint(&snap)?;
    let (url, _host_for_sign) = s3_url(&ep, bucket, key);
    let payload_hash = sha256_hex(body);
    ep.mutating.enter();
    let mut headers: Vec<(&str, &str)> = vec![
        ("Content-Type", content_type),
        ("x-amz-content-sha256", payload_hash.as_str()),
    ];
    if let Some(tags) = tagging {
        headers.push(("x-amz-tagging", tags));
    }
    signed_request(
        "PUT",
        &url,
        "s3",
        &ep.region,
        &ep.sig.access_key,
        &ep.sig.secret_key,
        ep.sig.session_token.as_deref(),
        &headers,
        body,
        true,
    )?;
    Ok(())
}

pub fn s3_delete(provider_name: &str, bucket: &str, key: &str) -> Result<(), AdvanceError> {
    let snap = provider_by_name(provider_name)?;
    let ep = aws_endpoint(&snap)?;
    let (url, _) = s3_url(&ep, bucket, key);
    ep.mutating.enter();
    signed_request(
        "DELETE",
        &url,
        "s3",
        &ep.region,
        &ep.sig.access_key,
        &ep.sig.secret_key,
        ep.sig.session_token.as_deref(),
        &[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")],
        b"",
        true,
    )?;
    Ok(())
}

fn s3_url(ep: &AwsEndpoint, bucket: &str, key: &str) -> (String, String) {
    let key = key.trim_start_matches('/');
    if ep.endpoint.contains("amazonaws.com") && !ep.endpoint.contains(bucket) {
        let host = format!("{bucket}.s3.{}.amazonaws.com", ep.region);
        (format!("https://{host}/{key}"), host)
    } else {
        let base = ep.endpoint.trim_end_matches('/');
        (format!("{base}/{bucket}/{key}"), String::new())
    }
}

/// EBS direct snapshot upload used by EbsSnapshotUploader: StartSnapshot,
/// PutSnapshotBlock (512 KiB, SHA256 checksum), CompleteSnapshot.
pub fn ebs_put_snapshot(provider_name: &str, volume_gib: i64, blocks: &[Vec<u8>], tags: &Value) -> Result<String, AdvanceError> {
    let snap = provider_by_name(provider_name)?;
    let ep = aws_endpoint(&snap)?;
    // boto sends every client to endpoint_url when one is configured
    // (moto). Otherwise the EBS direct API is the regional host.
    let base = if ep.endpoint.contains("amazonaws.com") {
        format!("https://ebs.{}.amazonaws.com", ep.region)
    } else {
        ep.endpoint.trim_end_matches('/').to_string()
    };
    let start_body = serde_json::to_vec(&json!({
        "VolumeSize": volume_gib,
        "ClientToken": new_id(),
        "Tags": tag_list(tags),
    }))
    .map_err(|e| AdvanceError::Other(e.to_string()))?;
    ep.mutating.enter();
    let started = signed_request(
        "POST",
        &format!("{base}/snapshots"),
        "ebs",
        &ep.region,
        &ep.sig.access_key,
        &ep.sig.secret_key,
        ep.sig.session_token.as_deref(),
        &[("Content-Type", "application/json")],
        &start_body,
        true,
    )?;
    let started = started.json().map_err(err_other)?;
    let snapshot_id = started
        .get("SnapshotId")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AdvanceError::Other(format!("StartSnapshot: {started}")))?
        .to_string();
    for (index, block) in blocks.iter().enumerate() {
        let mut data = block.clone();
        if data.len() < 512 * 1024 {
            data.resize(512 * 1024, 0);
        }
        let digest = {
            use sha2::{Digest, Sha256};
            Sha256::digest(&data)
        };
        let checksum = base64::engine::general_purpose::STANDARD.encode(digest);
        let len = data.len().to_string();
        ep.mutating.enter();
        let url = format!("{base}/snapshots/{snapshot_id}/blocks/{index}");
        let put = signed_request(
            "PUT",
            &url,
            "ebs",
            &ep.region,
            &ep.sig.access_key,
            &ep.sig.secret_key,
            ep.sig.session_token.as_deref(),
            &[
                ("x-amz-checksum", &checksum),
                ("x-amz-checksum-algorithm", "SHA256"),
                ("Content-Length", &len),
            ],
            &data,
            true,
        );
        let put = match put {
            Ok(resp) => resp,
            Err(err) => {
                delete_snapshot_id(&ep, &snapshot_id);
                return Err(err);
            }
        };
        let echoed = put.json().ok().and_then(|v| {
            v.get("Checksum")
                .or_else(|| v.get("checksum"))
                .and_then(|c| c.as_str())
                .map(|s| s.to_string())
        });
        if let Some(got) = echoed {
            if got != checksum {
                delete_snapshot_id(&ep, &snapshot_id);
                return Err(AdvanceError::Other(format!(
                    "Checksums do not match; received {got} expected {checksum}"
                )));
            }
        }
    }
    let done = serde_json::to_vec(&json!({"ChangedBlocksCount": blocks.len()})).unwrap_or_default();
    // EbsSnapshotUploader.finishUpload polls CompleteSnapshot until
    // Status is completed, and abortUpload deletes the snapshot.
    loop {
        ep.mutating.enter();
        let resp = match signed_request(
            "POST",
            &format!("{base}/snapshots/completion/{snapshot_id}"),
            "ebs",
            &ep.region,
            &ep.sig.access_key,
            &ep.sig.secret_key,
            ep.sig.session_token.as_deref(),
            &[("Content-Type", "application/json")],
            &done,
            true,
        ) {
            Ok(resp) => resp,
            Err(err) => {
                delete_snapshot_id(&ep, &snapshot_id);
                return Err(err);
            }
        };
        let status = resp
            .json()
            .ok()
            .and_then(|v| v.get("Status").or_else(|| v.get("status")).and_then(|s| s.as_str()).map(|s| s.to_string()))
            .unwrap_or_else(|| "completed".into());
        if status == "completed" {
            break;
        }
        if status == "error" {
            delete_snapshot_id(&ep, &snapshot_id);
            return Err(AdvanceError::Other("Snapshot in error state".into()));
        }
    }
    Ok(snapshot_id)
}

fn delete_snapshot_id(ep: &AwsEndpoint, snapshot_id: &str) {
    let mut params = Map::new();
    params.insert("SnapshotId".into(), json!(snapshot_id));
    let _ = ec2(ep, "DeleteSnapshot", params, false);
}

fn provider_by_name(name: &str) -> Result<ProviderSnap, AdvanceError> {
    provider_of(&json!({"provider": name}))
}

// ---------------------------------------------------------------------------
// Azure (azul.py)
// ---------------------------------------------------------------------------

fn azure_cloud(snap: &ProviderSnap) -> Result<(ConnectionConfig, String), AdvanceError> {
    let conn = connection_of(&snap.connection)?;
    Ok((conn, snap.region.clone()))
}

fn azure_token(conn: &ConnectionConfig) -> Result<String, AdvanceError> {
    let key = conn.name.clone();
    if let Some(tok) = registry().lock().unwrap().azure_tokens.get(&key) {
        if tok.expiration - now_secs() >= 60.0 {
            return Ok(tok.token.clone());
        }
    }
    let mut cred = Map::new();
    if let Some(path) = opt(conn, "shared_credentials_file") {
        if let Ok(text) = std::fs::read_to_string(&path) {
            if let Ok(Value::Object(m)) = serde_json::from_str::<Value>(&text) {
                cred = m;
            }
        }
    }
    let tenant = opt(conn, "tenant_id").or_else(|| cred.get("tenantId").and_then(|v| v.as_str()).map(|s| s.to_string()));
    let client = opt(conn, "client_id").or_else(|| cred.get("clientId").and_then(|v| v.as_str()).map(|s| s.to_string()));
    let secret = opt(conn, "client_secret").or_else(|| cred.get("clientSecret").and_then(|v| v.as_str()).map(|s| s.to_string()));
    let tenant = tenant.ok_or_else(|| AdvanceError::Other("azure tenant_id missing".into()))?;
    let client = client.ok_or_else(|| AdvanceError::Other("azure client_id missing".into()))?;
    let url = format!("{}/{tenant}/oauth2/token", azure_login_base());
    let body = if let Some(path) = opt(conn, "federated_token_file") {
        let assertion = std::fs::read_to_string(&path).map_err(|e| AdvanceError::Other(e.to_string()))?;
        format!(
            "grant_type=client_credentials&client_id={}&client_assertion_type={}&client_assertion={}&resource={}",
            aws_encode(&client),
            aws_encode("urn:ietf:params:oauth:client-assertion-type:jwt-bearer"),
            aws_encode(assertion.trim()),
            aws_encode("https://management.azure.com/")
        )
    } else {
        let secret = secret.ok_or_else(|| AdvanceError::Other("azure client_secret missing".into()))?;
        format!(
            "grant_type=client_credentials&client_id={}&client_secret={}&resource={}",
            aws_encode(&client),
            aws_encode(&secret),
            aws_encode("https://management.azure.com/")
        )
    };
    let resp = exchange(
        "POST",
        &url,
        &[("Content-Type", "application/x-www-form-urlencoded")],
        body.as_bytes(),
        HTTP_TIMEOUT,
    )
    .map_err(err_other)?;
    let parsed = resp.json().map_err(err_other)?;
    let token = parsed
        .get("access_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AdvanceError::Other(format!("azure auth: {}", resp.text())))?
        .to_string();
    let exp = parsed.get("expires_on").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(now_secs() + 3600.0);
    registry().lock().unwrap().azure_tokens.insert(key, AzureToken { token: token.clone(), expiration: exp });
    Ok(token)
}

fn now_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

fn azure_sub(conn: &ConnectionConfig) -> Result<String, AdvanceError> {
    if let Some(id) = opt(conn, "subscription_id") {
        return Ok(id);
    }
    if let Some(path) = opt(conn, "shared_credentials_file") {
        if let Ok(text) = std::fs::read_to_string(path) {
            if let Ok(v) = serde_json::from_str::<Value>(&text) {
                if let Some(id) = v.get("subscriptionId").and_then(|x| x.as_str()) {
                    return Ok(id.to_string());
                }
            }
        }
    }
    Err(AdvanceError::Other("azure subscription_id missing".into()))
}

fn azure_api_version(id: &str) -> &'static str {
    let id = id.to_ascii_lowercase();
    if id.contains("microsoft.network/") {
        "2020-11-01"
    } else if id.contains("/disks/") {
        "2020-06-30"
    } else if id.contains("/images/") {
        "2020-12-01"
    } else {
        "2025-11-01"
    }
}

fn azure_get(conn: &ConnectionConfig, id: &str) -> Result<Option<Value>, AdvanceError> {
    enter_limit(&format!("azure:{}", conn.name), conn_rate(conn));
    let token = azure_token(conn)?;
    let auth = format!("Bearer {token}");
    let url = if id.starts_with("http://") || id.starts_with("https://") {
        format!("{id}?api-version={}", azure_api_version(id))
    } else {
        format!("{}{id}?api-version={}", azure_arm_base(), azure_api_version(id))
    };
    let resp = exchange(
        "GET",
        &url,
        &[("Authorization", &auth), ("Accept", "application/json")],
        b"",
        HTTP_TIMEOUT,
    )
    .map_err(err_other)?;
    if resp.status == 404 {
        return Ok(None);
    }
    if !(200..300).contains(&resp.status) {
        return Err(err_other(format!("azure HTTP {} {}", resp.status, resp.text())));
    }
    resp.json().map(Some).map_err(err_other)
}

fn azure_refresh_obj(args: &Value, snap: &ProviderSnap, deleting: bool) -> Result<Value, AdvanceError> {
    let obj = args.get("obj").cloned().unwrap_or(Value::Null);
    if obj.is_null() {
        return Ok(json!({"obj": null}));
    }
    let Some(id) = obj.get("id").and_then(|v| v.as_str()).filter(|s| !s.is_empty()) else {
        return Ok(json!({"obj": obj}));
    };
    let (conn, _) = azure_cloud(snap)?;
    match azure_get(&conn, id)? {
        None if deleting => Ok(json!({"obj": null})),
        None => Ok(json!({"obj": obj})),
        Some(found) if deleting => {
            let state = found
                .pointer("/properties/provisioningState")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if state.eq_ignore_ascii_case("succeeded") && found.get("id").is_none() {
                Ok(json!({"obj": null}))
            } else if state.eq_ignore_ascii_case("deleted") {
                Ok(json!({"obj": null}))
            } else {
                // A successful GET means the resource is still there.
                // Delete polling ends when Azure returns 404.
                Ok(json!({"obj": found}))
            }
        }
        Some(found) => Ok(json!({"obj": found})),
    }
}

fn azure_request(conn: &ConnectionConfig, method: &str, url: &str, body: Option<&Value>) -> Result<Value, AdvanceError> {
    enter_limit(&format!("azure:{}", conn.name), conn_rate(conn));
    let token = azure_token(conn)?;
    let auth = format!("Bearer {token}");
    let raw = body.map(|v| serde_json::to_vec(v).unwrap_or_default()).unwrap_or_default();
    let resp = exchange(
        method,
        url,
        &[
            ("Authorization", &auth),
            ("Accept", "application/json"),
            ("Content-Type", "application/json"),
        ],
        &raw,
        HTTP_TIMEOUT,
    )
    .map_err(err_other)?;
    if !(200..300).contains(&resp.status) {
        return Err(err_other(format!("azure HTTP {} {}", resp.status, resp.text())));
    }
    if resp.body.is_empty() {
        return Ok(json!({}));
    }
    resp.json().map_err(err_other)
}

fn azure_vm_url(conn: &ConnectionConfig, rg: &str, name: &str) -> Result<String, AdvanceError> {
    let sub = azure_sub(conn)?;
    Ok(format!(
        "{}/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Compute/virtualMachines/{name}?api-version=2025-11-01",
        azure_arm_base()
    ))
}

fn azure_image_reference(args: &Value) -> Result<Value, AdvanceError> {
    if let Some(id) = args.get("image_external_id").and_then(|v| v.as_str()) {
        return Ok(json!({"image_reference": {"id": id}}));
    }
    let snap = provider_of(args)?;
    let label_name = args.get("label").and_then(|v| v.as_str()).unwrap_or("");
    let (_, _, image) = label_flavor_image(&snap, label_name)?;
    let reference = field(&image, &["image-reference", "image_reference"]).cloned().unwrap_or(json!({}));
    Ok(json!({"image_reference": reference}))
}

fn azure_subnet_id(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let label_name = args.get("label").and_then(|v| v.as_str()).unwrap_or("");
    let (label, _, _) = label_flavor_image(&snap, label_name)?;
    let id = field_str(&label, &["subnet-id", "subnet_id"]).unwrap_or_default();
    Ok(json!({"subnet_id": id}))
}

fn azure_list_instance_id(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let (conn, _) = azure_cloud(&snap)?;
    let sub = azure_sub(&conn)?;
    let url = format!(
        "{}/subscriptions/{sub}/providers/Microsoft.Compute/virtualMachines?api-version=2025-11-01",
        azure_arm_base()
    );
    let listed = azure_request(&conn, "GET", &url, None)?;
    let want = args.get("node_uuid").and_then(|v| v.as_str()).unwrap_or("");
    let values = listed.get("value").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    for vm in values {
        let tags = vm.get("tags").cloned().unwrap_or(json!({}));
        if tags.get("zuul_node_uuid").and_then(|v| v.as_str()) == Some(want) {
            return Ok(json!({"azure_vm_id": vm.get("id").and_then(|v| v.as_str()).unwrap_or("")}));
        }
    }
    Ok(json!({"azure_vm_id": null}))
}

fn complete_azure_create(args: &Value) -> Result<Value, AdvanceError> {
    let job = take_future(args)?;
    let snap = provider_of(&job.payload)?;
    let (conn, region) = azure_cloud(&snap)?;
    let label_name = field_str(&job.payload, &["label"]).unwrap_or_default();
    let (label, flavor, image) = label_flavor_image(&snap, &label_name)?;
    let hostname = field_str(&job.payload, &["hostname"]).unwrap_or_else(|| "zuul".into());
    let rg = field_str(&label, &["resource-group", "resource_group"]).unwrap_or_else(|| "zuul".into());
    let subnet = field_str(&job.payload, &["subnet_id"]).unwrap_or_default();
    let image_reference = job.payload.get("image_reference").cloned().unwrap_or(json!({}));
    let tags = job.payload.get("tags").cloned().unwrap_or(json!({}));
    let sub = azure_sub(&conn)?;
    let spec = azure_vm_body(
        &region,
        &hostname,
        &label,
        &flavor,
        &image,
        &image_reference,
        &subnet,
        &tags,
        &sub,
        &rg,
    );
    let url = azure_vm_url(&conn, &rg, &hostname)?;
    let vm = azure_request(&conn, "PUT", &url, Some(&spec))?;
    Ok(json!({"vm": vm}))
}

fn azure_delete_vm(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let (conn, _) = azure_cloud(&snap)?;
    let id = args.get("vm_id").and_then(|v| v.as_str()).unwrap_or("");
    let url = if id.starts_with("http://") || id.starts_with("https://") {
        format!("{id}?api-version=2025-11-01")
    } else {
        format!("{}{id}?api-version=2025-11-01", azure_arm_base())
    };
    azure_request(&conn, "DELETE", &url, None)?;
    Ok(json!({"vm": null}))
}

// ---------------------------------------------------------------------------
// OpenStack
// ---------------------------------------------------------------------------

fn os_config(snap: &ProviderSnap) -> Result<(Value, ConnectionConfig), AdvanceError> {
    let conn = connection_of(&snap.connection)?;
    let path = opt(&conn, "client_config_file")
        .or_else(|| std::env::var("OS_CLIENT_CONFIG_FILE").ok())
        .unwrap_or_else(|| expand_user("~/.config/openstack/clouds.yaml"));
    let text = std::fs::read_to_string(&path).map_err(|e| AdvanceError::Other(format!("clouds.yaml {path}: {e}")))?;
    let doc = parse_yaml(&text);
    let cloud_name = opt(&conn, "cloud").ok_or_else(|| AdvanceError::Other("openstack connection requires cloud".into()))?;
    let cloud = doc
        .pointer(&format!("/clouds/{cloud_name}"))
        .cloned()
        .ok_or_else(|| AdvanceError::Other(format!("cloud {cloud_name} not in {path}")))?;
    Ok((cloud, conn))
}

fn os_token(snap: &ProviderSnap) -> Result<OsToken, AdvanceError> {
    let key = format!("{}:{}", snap.connection, snap.region);
    if let Some(tok) = registry().lock().unwrap().os_tokens.get(&key).cloned() {
        return Ok(tok);
    }
    let (cloud, _conn) = os_config(snap)?;
    let auth = cloud.get("auth").cloned().unwrap_or(json!({}));
    let auth_url = field_str(&auth, &["auth_url"]).ok_or_else(|| AdvanceError::Other("auth_url missing".into()))?;
    let username = field_str(&auth, &["username"]).or_else(|| std::env::var("OS_USERNAME").ok()).unwrap_or_default();
    let password = field_str(&auth, &["password"]).or_else(|| std::env::var("OS_PASSWORD").ok()).unwrap_or_default();
    let project = field_str(&auth, &["project_name"]).or_else(|| std::env::var("OS_PROJECT_NAME").ok()).unwrap_or_default();
    let user_domain = field_str(&auth, &["user_domain_name"]).unwrap_or_else(|| "Default".into());
    let project_domain = field_str(&auth, &["project_domain_name"]).unwrap_or_else(|| "Default".into());
    let body = json!({"auth": {"identity": {"methods": ["password"], "password": {"user": {"name": username, "password": password, "domain": {"name": user_domain}}}}, "scope": {"project": {"name": project, "domain": {"name": project_domain}}}}});
    let trimmed = auth_url.trim_end_matches('/');
    let url = if trimmed.ends_with("/v3") {
        format!("{trimmed}/auth/tokens")
    } else {
        format!("{trimmed}/v3/auth/tokens")
    };
    let raw = serde_json::to_vec(&body).unwrap_or_default();
    let resp = exchange("POST", &url, &[("Content-Type", "application/json")], &raw, HTTP_TIMEOUT).map_err(err_other)?;
    if resp.status >= 400 {
        return Err(err_other(format!("keystone HTTP {} {}", resp.status, resp.text())));
    }
    let token = resp
        .header("X-Subject-Token")
        .ok_or_else(|| AdvanceError::Other("keystone did not return X-Subject-Token".into()))?
        .to_string();
    let doc = resp.json().map_err(err_other)?;
    let region = if snap.region.is_empty() {
        field_str(&cloud, &["region_name"]).unwrap_or_default()
    } else {
        snap.region.clone()
    };
    let catalog = doc.pointer("/token/catalog").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    let compute = catalog_endpoint(&catalog, "compute", &region);
    let network = catalog_endpoint(&catalog, "network", &region);
    let image = catalog_endpoint(&catalog, "image", &region);
    let volume = catalog_endpoint_any(&catalog, &["volumev3", "volume", "block-storage"], &region);
    let tok = OsToken {
        token,
        compute,
        network,
        image,
        volume,
        limit_key: format!("os:{}", snap.connection),
        rate: conn_rate(&_conn),
    };
    registry().lock().unwrap().os_tokens.insert(key, tok.clone());
    Ok(tok)
}

impl Clone for OsToken {
    fn clone(&self) -> Self {
        Self {
            token: self.token.clone(),
            compute: self.compute.clone(),
            network: self.network.clone(),
            image: self.image.clone(),
            volume: self.volume.clone(),
            limit_key: self.limit_key.clone(),
            rate: self.rate,
        }
    }
}

fn catalog_endpoint_any(catalog: &[Value], kinds: &[&str], region: &str) -> String {
    for kind in kinds {
        let url = catalog_endpoint(catalog, kind, region);
        if !url.is_empty() {
            return url;
        }
    }
    String::new()
}

fn catalog_endpoint(catalog: &[Value], kind: &str, region: &str) -> String {
    for svc in catalog {
        let ty = svc.get("type").and_then(|v| v.as_str()).unwrap_or("");
        if ty != kind {
            continue;
        }
        let endpoints = svc.get("endpoints").and_then(|v| v.as_array()).cloned().unwrap_or_default();
        for ep in &endpoints {
            let iface = ep.get("interface").and_then(|v| v.as_str()).unwrap_or("");
            let reg = ep.get("region").or_else(|| ep.get("region_id")).and_then(|v| v.as_str()).unwrap_or("");
            if iface == "public" && (region.is_empty() || reg == region) {
                return ep.get("url").and_then(|v| v.as_str()).unwrap_or("").trim_end_matches('/').to_string();
            }
        }
    }
    String::new()
}

fn os_http(tok: &OsToken, method: &str, url: &str, body: Option<&Value>) -> Result<Value, AdvanceError> {
    enter_limit(&tok.limit_key, tok.rate);
    let raw = body.map(|v| serde_json::to_vec(v).unwrap_or_default()).unwrap_or_default();
    let resp = exchange(
        method,
        url,
        &[
            ("X-Auth-Token", &tok.token),
            ("Content-Type", "application/json"),
            ("Accept", "application/json"),
        ],
        &raw,
        HTTP_TIMEOUT,
    )
    .map_err(err_other)?;
    if resp.status == 404 {
        return Ok(Value::Null);
    }
    if resp.status >= 400 {
        return Err(err_other(format!("openstack HTTP {} {}", resp.status, resp.text())));
    }
    if resp.body.is_empty() {
        return Ok(json!({}));
    }
    resp.json().map_err(err_other)
}

fn os_find_flavor(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let tok = os_token(&snap)?;
    let label_name = args.get("label").and_then(|v| v.as_str()).unwrap_or("");
    let (label, _, _) = label_flavor_image(&snap, label_name)?;
    let want = field_str(&label, &["flavor"]).unwrap_or(label_name.into());
    let listed = os_http(&tok, "GET", &format!("{}/flavors/detail", tok.compute), None)?;
    let flavors = listed.get("flavors").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    for f in flavors {
        if f.get("name").and_then(|v| v.as_str()) == Some(want.as_str()) || f.get("id").and_then(|v| v.as_str()) == Some(want.as_str()) {
            let cores = f.get("vcpus").and_then(|v| v.as_i64()).unwrap_or(0);
            let ram = f.get("ram").and_then(|v| v.as_i64()).unwrap_or(0);
            let mut resources = Map::new();
            resources.insert("instances".into(), json!(1));
            resources.insert("cores".into(), json!(cores));
            resources.insert("ram".into(), json!(ram));
            if field(&label, &["boot-from-volume", "boot_from_volume"]).and_then(|v| v.as_bool()) == Some(true) {
                resources.insert("volumes".into(), json!(1));
                resources.insert(
                    "volume-gb".into(),
                    field(&label, &["volume-size", "volume_size"]).cloned().unwrap_or(json!(0)),
                );
            }
            let auto = field(&label, &["auto-floating-ip", "auto_floating_ip"])
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            return Ok(json!({
                "flavor": f,
                "quota": {"default": null, "quota": resources},
                "auto_floating_ip": auto,
            }));
        }
    }
    Err(AdvanceError::Other(format!("Unable to find flavor: {want}")))
}

fn os_list_instance_id(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let tok = os_token(&snap)?;
    let want = args.get("node_uuid").and_then(|v| v.as_str()).unwrap_or("");
    let listed = os_http(&tok, "GET", &format!("{}/servers/detail", tok.compute), None)?;
    let servers = listed.get("servers").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    for server in servers {
        if server.pointer("/metadata/zuul_node_uuid").and_then(|v| v.as_str()) == Some(want) {
            return Ok(json!({"openstack_server_id": server.get("id").and_then(|v| v.as_str()).unwrap_or("")}));
        }
    }
    Ok(json!({"openstack_server_id": null}))
}

fn complete_os_create(args: &Value) -> Result<Value, AdvanceError> {
    let job = take_future(args)?;
    let snap = provider_of(&job.payload)?;
    let tok = os_token(&snap)?;
    let name = field_str(&job.payload, &["hostname"]).unwrap_or_default();
    let label_name = field_str(&job.payload, &["label"]).unwrap_or_default();
    let (label, flavor, image) = label_flavor_image(&snap, &label_name)?;
    let image_ref = field_str(&job.payload, &["image_external_id"])
        .or_else(|| field_str(&image, &["image-id", "image_id"]))
        .unwrap_or_default();
    let flavor_ref = field_str(&flavor, &["flavor-id", "id"]).or_else(|| field_str(&label, &["flavor"])).unwrap_or_default();
    let mut server = Map::new();
    server.insert("name".into(), json!(name));
    server.insert("imageRef".into(), json!(image_ref));
    server.insert("flavorRef".into(), json!(flavor_ref));
    server.insert("config_drive".into(), json!(field(&label, &["config-drive", "config_drive"]).and_then(|v| v.as_bool()).unwrap_or(true)));
    if let Some(key) = field_str(&label, &["key-name", "key_name"]) {
        server.insert("key_name".into(), json!(key));
    }
    if let Some(az) = field_str(&label, &["az", "availability-zone"]) {
        server.insert("availability_zone".into(), json!(az));
    }
    let groups = as_str_list(field(&label, &["security-groups", "security_groups"]));
    if !groups.is_empty() {
        let spec: Vec<Value> = groups.into_iter().map(|n| json!({"name": n})).collect();
        server.insert("security_groups".into(), Value::Array(spec));
    }
    let networks = as_str_list(field(&label, &["networks"]));
    if !networks.is_empty() {
        let nics: Vec<Value> = networks.into_iter().map(|n| json!({"uuid": n})).collect();
        server.insert("networks".into(), Value::Array(nics));
    }
    if let Some(meta) = job.payload.get("metadata") {
        server.insert("metadata".into(), meta.clone());
    }
    if field(&label, &["boot-from-volume", "boot_from_volume"]).and_then(|v| v.as_bool()) == Some(true) {
        let size = field(&label, &["volume-size", "volume_size"]).and_then(|v| v.as_i64()).unwrap_or(50);
        server.insert(
            "block_device_mapping_v2".into(),
            json!([{"boot_index": 0, "uuid": server.get("imageRef").cloned().unwrap_or(Value::Null), "source_type": "image", "destination_type": "volume", "volume_size": size, "delete_on_termination": true}]),
        );
        server.remove("imageRef");
    }
    let created = os_http(&tok, "POST", &format!("{}/servers", tok.compute), Some(&json!({"server": server})))?;
    Ok(json!({"server": created.get("server").cloned().unwrap_or(created)}))
}

fn os_refresh_server(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let tok = os_token(&snap)?;
    let server = args.get("server").cloned().unwrap_or(Value::Null);
    let id = server.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let got = os_http(&tok, "GET", &format!("{}/servers/{id}", tok.compute), None)?;
    Ok(json!({"server": got.get("server").cloned().unwrap_or(got)}))
}

fn os_get_server(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let tok = os_token(&snap)?;
    let id = args.get("server_id").and_then(|v| v.as_str()).unwrap_or("");
    let got = os_http(&tok, "GET", &format!("{}/servers/{id}", tok.compute), None)?;
    if got.is_null() {
        return Ok(json!({"server": null}));
    }
    Ok(json!({"server": got.get("server").cloned().unwrap_or(got)}))
}

fn os_needs_floating_ip(args: &Value) -> Result<Value, AdvanceError> {
    let server = args.get("server").cloned().unwrap_or(Value::Null);
    let addresses = server.get("addresses").cloned().unwrap_or(json!({}));
    let mut has_floating = false;
    if let Some(nets) = addresses.as_object() {
        for (_net, addrs) in nets {
            if let Some(arr) = addrs.as_array() {
                for addr in arr {
                    let typ = addr.get("OS-EXT-IPS:type").or_else(|| addr.get("type")).and_then(|v| v.as_str()).unwrap_or("");
                    if typ == "floating" {
                        has_floating = true;
                    }
                }
            }
        }
    }
    Ok(json!({"needs": !has_floating}))
}

fn os_create_floating_ip(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let tok = os_token(&snap)?;
    let (cloud, _) = os_config(&snap)?;
    let network = field_str(&cloud, &["external_network", "floating_ip_source"]).unwrap_or_default();
    let server = args.get("server").cloned().unwrap_or(Value::Null);
    if !tok.network.is_empty() {
        // Neutron. create_floating_ip(server=server) allocates and attaches.
        let body = json!({"floatingip": {"floating_network_id": network}});
        let created = os_http(&tok, "POST", &format!("{}/v2.0/floatingips", tok.network), Some(&body))?;
        let mut fip = created.get("floatingip").cloned().unwrap_or(created);
        let sid = server.get("id").and_then(|v| v.as_str()).unwrap_or("");
        if !sid.is_empty() {
            if let Some(port_id) = neutron_port_for_server(&tok, sid) {
                let id = fip.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let updated = os_http(
                    &tok,
                    "PUT",
                    &format!("{}/v2.0/floatingips/{id}", tok.network),
                    Some(&json!({"floatingip": {"port_id": port_id}})),
                )?;
                fip = updated.get("floatingip").cloned().unwrap_or(updated);
            }
        }
        return Ok(json!({"floating_ip": fip}));
    }
    // Nova. The address is allocated here and attached later by addFloatingIp.
    let created = os_http(
        &tok,
        "POST",
        &format!("{}/os-floating-ips", tok.compute),
        Some(&json!({"pool": network})),
    )?;
    Ok(json!({"floating_ip": created.get("floating_ip").cloned().unwrap_or(created)}))
}

fn neutron_port_for_server(tok: &OsToken, server_id: &str) -> Option<String> {
    let listed = os_http(
        tok,
        "GET",
        &format!("{}/v2.0/ports?device_id={server_id}", tok.network),
        None,
    )
    .ok()?;
    listed
        .get("ports")?
        .as_array()?
        .first()?
        .get("id")?
        .as_str()
        .map(|s| s.to_string())
}

fn floating_ip_source(cloud: &Value) -> String {
    // openstacksdk defaults.json floating_ip_source is "neutron".
    // "None" disables floating IPs. The source, not merely a network
    // catalog entry, chooses Neutron versus Nova.
    let raw = field_str(cloud, &["floating_ip_source"]).unwrap_or_else(|| "neutron".into());
    let lower = raw.to_ascii_lowercase();
    if lower == "none" {
        String::new()
    } else {
        lower
    }
}

fn use_neutron_floating(tok: &OsToken, cloud: &Value) -> bool {
    floating_ip_source(cloud) == "neutron" && !tok.network.is_empty()
}

fn list_floating_ips(tok: &OsToken, cloud: &Value) -> Result<Vec<Value>, AdvanceError> {
    let cache_key = format!("fips:{}:{}", tok.compute, floating_ip_source(cloud));
    let tok = tok.clone();
    let neutron = use_neutron_floating(&tok, cloud);
    let load = std::sync::Arc::new(move || {
        // os_http maps 404 to Null. That is the NotFound fall-through
        // list_floating_ips uses when no filters were pushed down.
        if neutron {
            let doc = os_http(&tok, "GET", &format!("{}/v2.0/floatingips", tok.network), None)?;
            if !doc.is_null() {
                return Ok(doc.get("floatingips").cloned().unwrap_or(json!([])));
            }
        }
        let doc = os_http(&tok, "GET", &format!("{}/os-floating-ips", tok.compute), None)?;
        if doc.is_null() {
            return Ok(json!([]));
        }
        Ok(doc
            .get("floating_ips")
            .or_else(|| doc.get("floatingips"))
            .cloned()
            .unwrap_or(json!([])))
    });
    let listed = with_ttl_cache(&cache_key, LIST_TTL, load)?;
    Ok(listed.as_array().cloned().unwrap_or_default())
}

fn os_refresh_floating_ip(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let tok = os_token(&snap)?;
    let (cloud, _) = os_config(&snap)?;
    let fip = args.get("floating_ip").cloned().unwrap_or(Value::Null);
    let id = fip.get("id").and_then(|v| v.as_str()).unwrap_or("");
    for item in list_floating_ips(&tok, &cloud)? {
        if item.get("id").and_then(|v| v.as_str()) == Some(id) {
            return Ok(json!({"floating_ip": item}));
        }
    }
    Ok(json!({"floating_ip": fip}))
}

fn os_attach_ip(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let tok = os_token(&snap)?;
    let server = args.get("server").cloned().unwrap_or(Value::Null);
    let fip = args.get("floating_ip").cloned().unwrap_or(Value::Null);
    let sid = server.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let address = fip.get("floating_ip_address").or_else(|| fip.get("ip")).and_then(|v| v.as_str()).unwrap_or("");
    let body = json!({"addFloatingIp": {"address": address}});
    os_http(&tok, "POST", &format!("{}/servers/{sid}/action", tok.compute), Some(&body))?;
    Ok(json!({"ok": true}))
}

fn os_server_floating_ip(args: &Value) -> Result<Value, AdvanceError> {
    let server = args.get("server").cloned().unwrap_or(Value::Null);
    let addresses = server.get("addresses").cloned().unwrap_or(json!({}));
    if let Some(nets) = addresses.as_object() {
        for (_net, addrs) in nets {
            if let Some(arr) = addrs.as_array() {
                for addr in arr {
                    let typ = addr
                        .get("OS-EXT-IPS:type")
                        .or_else(|| addr.get("type"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    if typ == "floating" {
                        let ip = addr.get("addr").or_else(|| addr.get("ip")).and_then(|v| v.as_str());
                        if let Some(ip) = ip {
                            return Ok(json!({"ip": ip}));
                        }
                    }
                }
            }
        }
    }
    Ok(json!({"ip": null}))
}

fn os_get_instance(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let mut args = args.clone();
    if let Ok(found) = os_find_flavor(&args) {
        if let Some(q) = found.get("quota") {
            if let Some(obj) = args.as_object_mut() {
                obj.insert("quota".into(), q.clone());
            }
        }
    }
    Ok(json!({"node": node_from_os(&args, &snap)}))
}

fn azure_update_node(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let mut args = args.clone();
    if let Ok(q) = azure_quota_for_label(&snap, &args) {
        if let Some(obj) = args.as_object_mut() {
            obj.insert("quota".into(), q);
        }
    }
    Ok(json!({"node": node_from_azure(&args)}))
}

fn k8s_update_node(args: &Value) -> Result<Value, AdvanceError> {
    let mut args = args.clone();
    if let Ok(q) = quota_for_label(&args) {
        if let Some(obj) = args.as_object_mut() {
            obj.insert("quota".into(), q);
        }
    }
    Ok(json!({"node": node_from_k8s(&args)}))
}

fn server_interface_ip(server: &Value) -> Option<String> {
    let addresses = server.get("addresses")?.as_object()?;
    let mut fallback = None;
    for addrs in addresses.values() {
        let Some(arr) = addrs.as_array() else { continue };
        for addr in arr {
            let ip = addr.get("addr").or_else(|| addr.get("ip")).and_then(|v| v.as_str());
            let typ = addr
                .get("OS-EXT-IPS:type")
                .or_else(|| addr.get("type"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if typ == "floating" {
                if let Some(ip) = ip {
                    return Some(ip.to_string());
                }
            }
            if fallback.is_none() {
                fallback = ip.map(|s| s.to_string());
            }
        }
    }
    fallback
}

fn os_has_floating_ips(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let (cloud, _) = os_config(&snap)?;
    let has = field_str(&cloud, &["external_network", "floating_ip_source"]).is_some();
    Ok(json!({"has": has}))
}

fn os_get_floating_ips(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let tok = os_token(&snap)?;
    let (cloud, _) = os_config(&snap)?;
    let server = args.get("server").cloned().unwrap_or(Value::Null);
    let mut wanted = Vec::new();
    if let Some(nets) = server.get("addresses").and_then(|v| v.as_object()) {
        for addrs in nets.values() {
            let Some(arr) = addrs.as_array() else { continue };
            for addr in arr {
                let typ = addr
                    .get("OS-EXT-IPS:type")
                    .or_else(|| addr.get("type"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if typ == "floating" {
                    if let Some(ip) = addr.get("addr").or_else(|| addr.get("ip")).and_then(|v| v.as_str()) {
                        wanted.push(ip.to_string());
                    }
                }
            }
        }
    }
    let mut matched = Vec::new();
    if !wanted.is_empty() {
        for item in list_floating_ips(&tok, &cloud)? {
            let ip = item
                .get("floating_ip_address")
                .or_else(|| item.get("ip"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if wanted.iter().any(|w| w == ip) {
                matched.push(item);
            }
        }
    }
    Ok(json!({"floating_ips": matched}))
}

fn os_delete_floating_ip(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let tok = os_token(&snap)?;
    let (cloud, _) = os_config(&snap)?;
    let id = args.get("floating_ip").and_then(|v| v.get("id")).and_then(|v| v.as_str()).unwrap_or("");
    // delete_floating_ip(..., retry=0) does not GET the address again.
    // A 404 from Neutron falls through to Nova. os_http reports that as Null.
    if use_neutron_floating(&tok, &cloud) {
        let got = os_http(&tok, "DELETE", &format!("{}/v2.0/floatingips/{id}", tok.network), None)?;
        if !got.is_null() {
            return Ok(json!({"ok": true}));
        }
    }
    let got = os_http(&tok, "DELETE", &format!("{}/os-floating-ips/{id}", tok.compute), None)?;
    Ok(json!({"ok": !got.is_null()}))
}

fn os_refresh_floating_ip_delete(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let tok = os_token(&snap)?;
    let (cloud, _) = os_config(&snap)?;
    let id = args.get("floating_ip").and_then(|v| v.get("id")).and_then(|v| v.as_str()).unwrap_or("");
    for item in list_floating_ips(&tok, &cloud)? {
        if item.get("id").and_then(|v| v.as_str()) != Some(id) {
            continue;
        }
        if item.get("status").and_then(|v| v.as_str()) == Some("DOWN") {
            return Ok(json!({"floating_ip": null}));
        }
        return Ok(json!({"floating_ip": item}));
    }
    Ok(json!({"floating_ip": null}))
}

fn complete_os_delete(args: &Value) -> Result<Value, AdvanceError> {
    let job = take_future(args)?;
    let snap = provider_of(&job.payload)?;
    let tok = os_token(&snap)?;
    let id = field_str(&job.payload, &["server_id"]).unwrap_or_default();
    os_http(&tok, "DELETE", &format!("{}/servers/{id}", tok.compute), None)?;
    Ok(json!({"done": true}))
}

fn os_refresh_server_delete(args: &Value) -> Result<Value, AdvanceError> {
    let got = os_get_server(args)?;
    let gone = got.get("server").map(|v| v.is_null()).unwrap_or(true);
    Ok(json!({"gone": gone, "server": got.get("server").cloned().unwrap_or(Value::Null)}))
}

fn os_handle_server_fault(args: &Value) -> Result<Value, AdvanceError> {
    let server = os_get_server(args)?;
    let message = server.pointer("/server/fault/message").and_then(|v| v.as_str()).unwrap_or("");
    let quota = message.to_ascii_lowercase().contains("quota");
    Ok(json!({"quota": quota, "message": message}))
}

// ---------------------------------------------------------------------------
// Kubernetes
// ---------------------------------------------------------------------------

struct Kube {
    server: String,
    token: String,
    openshift: bool,
    skiptls: bool,
    limit_key: String,
    rate: f64,
}

fn kube_of(snap: &ProviderSnap) -> Result<Kube, AdvanceError> {
    let conn = connection_of(&snap.connection)?;
    let path = opt(&conn, "kubeconfig_file")
        .or_else(|| std::env::var("KUBECONFIG").ok())
        .unwrap_or_else(|| expand_user("~/.kube/config"));
    let text = std::fs::read_to_string(&path).map_err(|e| AdvanceError::Other(format!("kubeconfig {path}: {e}")))?;
    let doc = parse_yaml(&text);
    let context_name = opt(&conn, "context")
        .or_else(|| doc.get("current-context").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .unwrap_or_default();
    let contexts = doc.get("contexts").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    let mut cluster_name = String::new();
    let mut user_name = String::new();
    for ctx in &contexts {
        if ctx.get("name").and_then(|v| v.as_str()) == Some(context_name.as_str()) || context_name.is_empty() {
            cluster_name = ctx.pointer("/context/cluster").and_then(|v| v.as_str()).unwrap_or("").to_string();
            user_name = ctx.pointer("/context/user").and_then(|v| v.as_str()).unwrap_or("").to_string();
            if !context_name.is_empty() {
                break;
            }
        }
    }
    let clusters = doc.get("clusters").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    let mut server = String::new();
    let mut skiptls = false;
    for c in &clusters {
        if c.get("name").and_then(|v| v.as_str()) == Some(cluster_name.as_str()) || cluster_name.is_empty() {
            server = c.pointer("/cluster/server").and_then(|v| v.as_str()).unwrap_or("").to_string();
            skiptls = c
                .pointer("/cluster/insecure-skip-tls-verify")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            break;
        }
    }
    let users = doc.get("users").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    let mut token = String::new();
    for u in &users {
        if u.get("name").and_then(|v| v.as_str()) == Some(user_name.as_str()) || user_name.is_empty() {
            token = u.pointer("/user/token").and_then(|v| v.as_str()).unwrap_or("").to_string();
            break;
        }
    }
    if server.is_empty() {
        return Err(AdvanceError::Other(format!("kubeconfig {path} has no server")));
    }
    let openshift = opt(&conn, "use_openshift_projects")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
        .unwrap_or(false);
    Ok(Kube {
        server: server.trim_end_matches('/').into(),
        token,
        openshift,
        skiptls,
        limit_key: format!("k8s:{}", conn.name),
        rate: conn_rate(&conn),
    })
}

fn k8s_http(kube: &Kube, method: &str, path: &str, body: Option<&Value>) -> Result<Value, AdvanceError> {
    enter_limit(&kube.limit_key, kube.rate);
    let raw = body.map(|v| serde_json::to_vec(v).unwrap_or_default()).unwrap_or_default();
    let auth = format!("Bearer {}", kube.token);
    let resp = exchange(
        method,
        &format!("{}{path}", kube.server),
        &[
            ("Authorization", &auth),
            ("Accept", "application/json"),
            ("Content-Type", "application/json"),
        ],
        &raw,
        HTTP_TIMEOUT,
    )
    .map_err(err_other)?;
    if resp.status == 404 {
        return Ok(Value::Null);
    }
    if resp.status >= 400 {
        return Err(err_other(format!("k8s HTTP {} {}", resp.status, resp.text())));
    }
    if resp.body.is_empty() {
        return Ok(json!({}));
    }
    resp.json().map_err(err_other)
}

fn k8s_setup(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let kube = kube_of(&snap)?;
    let label_name = args.get("label").and_then(|v| v.as_str()).unwrap_or("");
    let (label, _, _) = label_flavor_image(&snap, label_name).unwrap_or((json!({}), json!({}), json!({})));
    let kind = field_str(&label, &["kind"]).unwrap_or_else(|| "pod".into());
    let containers = if kind == "pod" {
        field(&label, &["spec"])
            .and_then(|v| v.get("containers"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                Value::Array(
                    arr.iter()
                        .filter_map(|c| c.get("name").cloned())
                        .collect(),
                )
            })
    } else {
        None
    };
    let quota = quota_for_label(args)?;
    Ok(json!({
        "use_openshift_projects": kube.openshift,
        "kind": kind,
        "host": kube.server,
        "skiptls": kube.skiptls,
        "containers": containers,
        "quota": quota,
    }))
}

fn k8s_use_openshift(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let kube = kube_of(&snap)?;
    Ok(json!({"use_openshift_projects": kube.openshift}))
}

fn k8s_create_namespace(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let kube = kube_of(&snap)?;
    let name = args.get("hostname").and_then(|v| v.as_str()).unwrap_or("zuul");
    let labels = args.get("tags").cloned().unwrap_or(json!({}));
    let body = json!({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name, "labels": labels}});
    k8s_http(&kube, "POST", "/api/v1/namespaces", Some(&body))?;
    Ok(json!({"ok": true}))
}

fn k8s_create_project(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let kube = kube_of(&snap)?;
    let name = args.get("hostname").and_then(|v| v.as_str()).unwrap_or("zuul");
    let labels = args.get("tags").cloned().unwrap_or(json!({}));
    let body = json!({"apiVersion": "project.openshift.io/v1", "kind": "ProjectRequest", "metadata": {"name": name, "labels": labels}});
    k8s_http(&kube, "POST", "/apis/project.openshift.io/v1/projectrequests", Some(&body))?;
    Ok(json!({"ok": true}))
}

fn k8s_subject_access(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let kube = kube_of(&snap)?;
    let ns = args.get("namespace").and_then(|v| v.as_str()).unwrap_or("");
    let body = json!({
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "SelfSubjectAccessReview",
        "spec": {"resourceAttributes": {"namespace": ns, "verb": "create", "resource": "serviceaccounts"}}
    });
    match k8s_http(&kube, "POST", "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews", Some(&body)) {
        Ok(resp) => {
            match resp.pointer("/status/allowed").and_then(|v| v.as_bool()) {
                Some(allowed) => Ok(json!({"allowed": allowed})),
                None => Ok(json!({"allowed": null})),
            }
        }
        Err(_) => Ok(json!({"allowed": null})),
    }
}

fn k8s_image_pull_secrets(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let kube = kube_of(&snap)?;
    let ns = args.get("namespace").and_then(|v| v.as_str()).unwrap_or("default");
    let label_name = args.get("label").and_then(|v| v.as_str()).unwrap_or("");
    let (label, _, _) = label_flavor_image(&snap, label_name).unwrap_or((json!({}), json!({}), json!({})));
    let secrets = field(&label, &["image-pull-secrets", "image_pull_secrets"])
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    for secret in secrets {
        let name = secret.get("name").and_then(|v| v.as_str()).unwrap_or("zuul-image-pull");
        let source_ns = secret.get("namespace").and_then(|v| v.as_str()).unwrap_or(ns);
        let old = k8s_http(&kube, "GET", &format!("/api/v1/namespaces/{source_ns}/secrets/{name}"), None)?;
        let body = json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "type": "kubernetes.io/dockerconfigjson",
            "metadata": {"name": name},
            "data": old.get("data").cloned().unwrap_or(json!({}))
        });
        k8s_http(&kube, "POST", &format!("/api/v1/namespaces/{ns}/secrets"), Some(&body))?;
    }
    Ok(json!({"ok": true}))
}

fn k8s_service_account(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let kube = kube_of(&snap)?;
    let ns = args.get("namespace").and_then(|v| v.as_str()).unwrap_or("default");
    let user = args.get("user").and_then(|v| v.as_str()).unwrap_or("zuul-worker");
    let body = json!({"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": user}});
    k8s_http(&kube, "POST", &format!("/api/v1/namespaces/{ns}/serviceaccounts"), Some(&body))?;
    let secret = json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "kubernetes.io/service-account-token",
        "metadata": {
            "name": user,
            "annotations": {"kubernetes.io/service-account.name": user}
        }
    });
    k8s_http(&kube, "POST", &format!("/api/v1/namespaces/{ns}/secrets"), Some(&secret))?;
    Ok(json!({"ok": true}))
}

fn k8s_get_token(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let kube = kube_of(&snap)?;
    let ns = args.get("namespace").and_then(|v| v.as_str()).unwrap_or("default");
    let user = args.get("user").and_then(|v| v.as_str()).unwrap_or("zuul-worker");
    let secret = k8s_http(&kube, "GET", &format!("/api/v1/namespaces/{ns}/secrets/{user}"), None)?;
    let token_b64 = secret.pointer("/data/token").and_then(|v| v.as_str());
    let ca = secret.pointer("/data/ca.crt").and_then(|v| v.as_str());
    match (token_b64, ca) {
        (Some(token_b64), Some(ca)) => {
            let raw = base64::engine::general_purpose::STANDARD
                .decode(token_b64)
                .unwrap_or_default();
            let token = String::from_utf8_lossy(&raw).into_owned();
            if token.is_empty() {
                return Ok(json!({"token": null, "ca_crt": null}));
            }
            Ok(json!({"token": token, "ca_crt": ca}))
        }
        _ => Ok(json!({"token": null, "ca_crt": null})),
    }
}

fn k8s_create_role(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let kube = kube_of(&snap)?;
    let ns = args.get("namespace").and_then(|v| v.as_str()).unwrap_or("default");
    let user = args.get("user").and_then(|v| v.as_str()).unwrap_or("zuul-worker");
    let restricted = args.get("restricted_access").and_then(|v| v.as_bool()).unwrap_or(false);
    let all_verbs = ["create", "delete", "get", "list", "patch", "update", "watch"];
    let (role_name, rules) = if restricted {
        (
            "zuul-restricted",
            json!([
                {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]},
                {"apiGroups": [""], "resources": ["pods/exec"], "verbs": all_verbs},
                {"apiGroups": [""], "resources": ["pods/log"], "verbs": all_verbs},
                {"apiGroups": [""], "resources": ["pods/portforward"], "verbs": all_verbs}
            ]),
        )
    } else {
        (
            "zuul",
            json!([
                {"apiGroups": ["apps"], "resources": ["deployments", "replicasets"], "verbs": all_verbs},
                {"apiGroups": ["batch"], "resources": ["cronjobs", "jobs"], "verbs": all_verbs},
                {"apiGroups": [""], "resources": ["pods", "pods/exec", "pods/log", "pods/portforward", "pods/attach", "services", "endpoints", "configmaps", "secrets"], "verbs": all_verbs}
            ]),
        )
    };
    let role = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": {"name": role_name},
        "rules": rules
    });
    k8s_http(&kube, "POST", &format!("/apis/rbac.authorization.k8s.io/v1/namespaces/{ns}/roles"), Some(&role))?;
    let binding = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {"name": "zuul-role"},
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": role_name},
        "subjects": [{"kind": "ServiceAccount", "name": user, "namespace": ns}],
        "userNames": [format!("system:serviceaccount:{ns}:zuul-worker")]
    });
    k8s_http(&kube, "POST", &format!("/apis/rbac.authorization.k8s.io/v1/namespaces/{ns}/rolebindings"), Some(&binding))?;
    Ok(json!({"ok": true}))
}

fn k8s_create_pod(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let kube = kube_of(&snap)?;
    let ns = args.get("namespace").and_then(|v| v.as_str()).unwrap_or("default");
    let name = args.get("hostname").and_then(|v| v.as_str()).unwrap_or("zuul");
    let label_name = args.get("label").and_then(|v| v.as_str()).unwrap_or("");
    let (label, _, _) = label_flavor_image(&snap, label_name).unwrap_or((json!({}), json!({}), json!({})));
    let spec = field(&label, &["spec"]).cloned().unwrap_or(json!({}));
    let annotations = field(&label, &["annotations"]).cloned().unwrap_or(json!({}));
    let labels = args.get("tags").cloned().unwrap_or(json!({}));
    let body = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "labels": labels, "annotations": annotations},
        "spec": spec,
        "restartPolicy": "Never"
    });
    k8s_http(&kube, "POST", &format!("/api/v1/namespaces/{ns}/pods"), Some(&body))?;
    Ok(json!({"ok": true}))
}

fn k8s_get_pod(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let kube = kube_of(&snap)?;
    let ns = args.get("namespace").and_then(|v| v.as_str()).unwrap_or("default");
    let name = args.get("hostname").and_then(|v| v.as_str()).unwrap_or("");
    let pod = k8s_http(&kube, "GET", &format!("/api/v1/namespaces/{ns}/pods/{name}"), None)?;
    let ready = pod.pointer("/status/phase").and_then(|v| v.as_str()) == Some("Running");
    Ok(json!({"ready": ready}))
}

fn k8s_delete_namespace(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let kube = kube_of(&snap)?;
    let ns = args.get("namespace_id").or_else(|| args.get("namespace")).and_then(|v| v.as_str()).unwrap_or("");
    let body = json!({"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Background"});
    k8s_http(&kube, "DELETE", &format!("/api/v1/namespaces/{ns}"), Some(&body))?;
    Ok(json!({"ok": true}))
}

fn k8s_delete_project(args: &Value) -> Result<Value, AdvanceError> {
    let snap = provider_of(args)?;
    let kube = kube_of(&snap)?;
    let name = args.get("namespace_id").and_then(|v| v.as_str()).unwrap_or("");
    k8s_http(&kube, "DELETE", &format!("/apis/project.openshift.io/v1/projects/{name}"), None)?;
    Ok(json!({"ok": true}))
}

include!("leak.rs");
include!("parity_impl.rs");

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex as StdMutex};
    use std::thread;

    fn test_lock() -> &'static StdMutex<()> {
        static LOCK: std::sync::OnceLock<StdMutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| StdMutex::new(()))
    }

    fn content_length(header: &str) -> usize {
        header
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
            .and_then(|l| l.split(':').nth(1))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0)
    }

    fn read_request(sock: &mut std::net::TcpStream) -> String {
        let mut got = Vec::new();
        let mut buf = [0u8; 8192];
        let _ = sock.set_read_timeout(Some(Duration::from_secs(2)));
        loop {
            match sock.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    got.extend_from_slice(&buf[..n]);
                    if let Some(end) = got.windows(4).position(|w| w == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&got[..end]);
                        let need = end + 4 + content_length(&header);
                        if got.len() >= need {
                            break;
                        }
                    }
                }
            }
        }
        String::from_utf8_lossy(&got).into_owned()
    }

    fn serve(
        handler: impl Fn(u16, &str) -> (u16, Vec<(String, String)>, String) + Send + 'static,
    ) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        thread::spawn(move || loop {
            let Ok((mut sock, _)) = listener.accept() else { break };
            let req = read_request(&mut sock);
            let (status, headers, body) = handler(port, &req);
            let mut resp = format!("HTTP/1.1 {status} OK\r\nContent-Length: {}\r\n", body.len());
            let mut has_type = false;
            for (k, v) in &headers {
                if k.eq_ignore_ascii_case("content-type") {
                    has_type = true;
                }
                resp.push_str(k);
                resp.push_str(": ");
                resp.push_str(v);
                resp.push_str("\r\n");
            }
            if !has_type {
                resp.push_str("Content-Type: application/json\r\n");
            }
            resp.push_str("Connection: close\r\n\r\n");
            resp.push_str(&body);
            let _ = sock.write_all(resp.as_bytes());
        });
        port
    }

    fn json_ok(body: &str) -> (u16, Vec<(String, String)>, String) {
        (200, Vec::new(), body.to_string())
    }

    #[test]
    fn aws_run_instances_hits_ec2_query_api() {
        let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
        let port = serve(|_port, req| {
            if req.contains("Action=DescribeImages") {
                assert!(req.contains("ImageId.1=ami-1"), "{req}");
                return json_ok(
                    r#"<?xml version="1.0"?><DescribeImagesResponse><imagesSet><item><imageId>ami-1</imageId><blockDeviceMapping><item><deviceName>/dev/sda1</deviceName><ebs><volumeSize>8</volumeSize></ebs></item></blockDeviceMapping></item></imagesSet></DescribeImagesResponse>"#,
                );
            }
            assert!(req.contains("Action=RunInstances"), "{req}");
            assert!(req.contains("InstanceType=t3.medium"), "{req}");
            assert!(req.contains("ImageId=ami-1"), "{req}");
            assert!(req.contains("TagSpecifications.1.ResourceType=instance"), "{req}");
            assert!(req.contains("TagSpecifications.1.Tag.1.Key=Name"), "{req}");
            assert!(req.starts_with("POST /"), "{req}");
            json_ok(
                r#"<?xml version="1.0"?><RunInstancesResponse><instancesSet><item><instanceId>i-from-mock</instanceId><instanceState><code>0</code><name>pending</name></instanceState><instanceType>t3.medium</instanceType></item></instancesSet></RunInstancesResponse>"#,
            )
        });
        let mut options = BTreeMap::new();
        options.insert("driver".into(), "aws".into());
        options.insert("access_key_id".into(), "AKID".into());
        options.insert("secret_access_key".into(), "SECRET".into());
        options.insert("endpoint_url".into(), format!("http://127.0.0.1:{port}"));
        options.insert("rate".into(), "1000".into());
        set_connections(vec![ConnectionConfig {
            name: "aws-test".into(),
            driver: "aws".into(),
            options,
        }]);
        {
            let mut reg = registry().lock().unwrap();
            reg.providers.insert(
                "tenant/aws-main".into(),
                ProviderSnap {
                    driver: "aws".into(),
                    connection: "aws-test".into(),
                    region: "us-east-1".into(),
                    labels: BTreeMap::from([(
                        "debian".into(),
                        json!({"name": "debian", "flavor": "small", "image": "img", "key-name": "zuul"}),
                    )]),
                    flavors: BTreeMap::from([(
                        "small".into(),
                        json!({"instance-type": "t3.medium", "public-ipv4": true}),
                    )]),
                    images: BTreeMap::from([("img".into(), json!({"image-id": "ami-1"}))]),
                },
            );
            reg.aws.clear();
        }
        let submitted = invoke(
            "ep.submit_create_instance",
            &json!({"provider": "tenant/aws-main", "label": "debian", "hostname": "npabc", "tags": {"Name": "npabc", "zuul_node_uuid": "u1"}}),
        )
        .unwrap();
        let done = invoke(
            "ep.complete_create_instance",
            &json!({"provider": "tenant/aws-main", "future_id": submitted["future_id"]}),
        )
        .unwrap();
        assert_eq!(done["instance"]["InstanceId"], "i-from-mock");
        assert_eq!(done["instance"]["State"]["Name"], "pending");
    }

    #[test]
    fn s3_put_and_ebs_snapshot_hit_local_endpoints() {
        let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
        let port = serve(|_port, req| {
            if req.starts_with("PUT /bucket/obj") {
                assert!(req.to_ascii_lowercase().contains("x-amz-content-sha256:"), "{req}");
                assert!(req.contains("hello-image"), "{req}");
                return (200, Vec::new(), String::new());
            }
            if req.starts_with("POST /snapshots ") || req.starts_with("POST /snapshots\r") {
                assert!(req.contains("\"VolumeSize\":1"), "{req}");
                return json_ok(r#"{"SnapshotId":"snap-mock"}"#);
            }
            if req.contains("/snapshots/snap-mock/blocks/") {
                assert!(req.to_ascii_lowercase().contains("x-amz-checksum-algorithm: sha256"), "{req}");
                assert!(req.to_ascii_lowercase().contains("x-amz-checksum:"), "{req}");
                return (200, Vec::new(), String::new());
            }
            if req.contains("/snapshots/completion/snap-mock") {
                assert!(req.contains("\"ChangedBlocksCount\":1"), "{req}");
                return json_ok(r#"{"Status":"completed"}"#);
            }
            panic!("unexpected {req}");
        });
        let mut options = BTreeMap::new();
        options.insert("access_key_id".into(), "AKID".into());
        options.insert("secret_access_key".into(), "SECRET".into());
        options.insert("endpoint_url".into(), format!("http://127.0.0.1:{port}"));
        options.insert("rate".into(), "1000".into());
        set_connections(vec![ConnectionConfig {
            name: "aws-s3".into(),
            driver: "aws".into(),
            options,
        }]);
        {
            let mut reg = registry().lock().unwrap();
            reg.providers.insert(
                "tenant/aws-s3".into(),
                ProviderSnap {
                    driver: "aws".into(),
                    connection: "aws-s3".into(),
                    region: "us-east-1".into(),
                    labels: BTreeMap::new(),
                    flavors: BTreeMap::new(),
                    images: BTreeMap::new(),
                },
            );
            reg.aws.clear();
        }
        s3_put("tenant/aws-s3", "bucket", "obj", b"hello-image", "application/octet-stream").unwrap();
        let snap = ebs_put_snapshot("tenant/aws-s3", 1, &[b"block".to_vec()], &json!({"Name": "img"})).unwrap();
        assert_eq!(snap, "snap-mock");
    }

    #[test]
    fn azure_create_puts_azul_vm() {
        let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
        let port = serve(|_port, req| {
            if req.contains("/oauth2/token") {
                assert!(req.contains("grant_type=client_credentials"), "{req}");
                assert!(req.contains("client_id=cid"), "{req}");
                assert!(req.contains("resource=https%3A%2F%2Fmanagement.azure.com%2F"), "{req}");
                return json_ok(r#"{"access_token":"tok","expires_on":"9999999999"}"#);
            }
            assert!(req.starts_with("PUT "), "{req}");
            assert!(req.contains("/virtualMachines/npabc?api-version=2025-11-01"), "{req}");
            assert!(req.contains("Standard_B1s"), "{req}");
            assert!(req.contains("\"location\":\"eastus\""), "{req}");
            (
                201,
                Vec::new(),
                r#"{"id":"/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/npabc","properties":{"provisioningState":"Creating"}}"#.into(),
            )
        });
        set_test_azure_endpoints(
            &format!("http://127.0.0.1:{port}"),
            &format!("http://127.0.0.1:{port}"),
        );
        let mut options = BTreeMap::new();
        options.insert("tenant_id".into(), "tid".into());
        options.insert("client_id".into(), "cid".into());
        options.insert("client_secret".into(), "sec".into());
        options.insert("subscription_id".into(), "sub".into());
        options.insert("rate".into(), "1000".into());
        set_connections(vec![ConnectionConfig {
            name: "azure-test".into(),
            driver: "azure".into(),
            options,
        }]);
        {
            let mut reg = registry().lock().unwrap();
            reg.azure_tokens.clear();
            reg.providers.insert(
                "tenant/azure".into(),
                ProviderSnap {
                    driver: "azure".into(),
                    connection: "azure-test".into(),
                    region: "eastus".into(),
                    labels: BTreeMap::from([(
                        "debian".into(),
                        json!({"name": "debian", "flavor": "small", "image": "img", "resource-group": "rg", "key-data": "ssh-rsa A"}),
                    )]),
                    flavors: BTreeMap::from([(
                        "small".into(),
                        json!({"vm-size": "Standard_B1s", "public-ipv4": true}),
                    )]),
                    images: BTreeMap::from([("img".into(), json!({"username": "zuul"}))]),
                },
            );
        }
        let submitted = invoke(
            "ep.azure_submit_create_vm",
            &json!({
                "provider": "tenant/azure",
                "label": "debian",
                "hostname": "npabc",
                "subnet_id": "/subscriptions/sub/subnets/s",
                "image_reference": {"id": "/subscriptions/sub/images/img"},
                "tags": {"zuul_node_uuid": "u1"}
            }),
        )
        .unwrap();
        let done = invoke(
            "ep.azure_complete_create_vm",
            &json!({"provider": "tenant/azure", "future_id": submitted["future_id"]}),
        )
        .unwrap();
        assert!(done["vm"]["id"].as_str().unwrap().contains("virtualMachines/npabc"));
        assert_eq!(done["vm"]["properties"]["provisioningState"], "Creating");
    }

    #[test]
    fn openstack_server_create_posts_nova_body() {
        let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
        let port = serve(|port, req| {
            if req.contains("/auth/tokens") {
                assert!(req.contains("\"name\":\"demo\""), "{req}");
                assert!(req.contains("\"password\":\"secret\""), "{req}");
                let body = format!(
                    "{{\"token\":{{\"catalog\":[{{\"type\":\"compute\",\"endpoints\":[{{\"interface\":\"public\",\"region\":\"RegionOne\",\"url\":\"http://127.0.0.1:{port}/compute\"}}]}},{{\"type\":\"network\",\"endpoints\":[{{\"interface\":\"public\",\"region\":\"RegionOne\",\"url\":\"http://127.0.0.1:{port}/network\"}}]}}]}}}}"
                );
                return (
                    201,
                    vec![("X-Subject-Token".into(), "tok-os".into())],
                    body,
                );
            }
            if req.contains("POST /compute/servers") {
                assert!(req.contains("X-Auth-Token: tok-os") || req.contains("x-auth-token: tok-os"), "{req}");
                assert!(req.contains("\"name\":\"np1\""), "{req}");
                assert!(req.contains("\"imageRef\":\"img-1\""), "{req}");
                assert!(req.contains("\"flavorRef\":\"flv-1\""), "{req}");
                assert!(req.contains("\"config_drive\":true"), "{req}");
                return json_ok(r#"{"server":{"id":"srv-1","status":"BUILD"}}"#);
            }
            (404, Vec::new(), format!("no match {req}"))
        });
        let dir = std::env::temp_dir().join(format!("os-{}", new_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let clouds = dir.join("clouds.yaml");
        std::fs::write(
            &clouds,
            format!(
                "clouds:\n  demo:\n    region_name: RegionOne\n    auth:\n      auth_url: http://127.0.0.1:{port}/v3\n      username: demo\n      password: secret\n      project_name: demo\n      user_domain_name: Default\n      project_domain_name: Default\n"
            ),
        )
        .unwrap();
        let mut options = BTreeMap::new();
        options.insert("cloud".into(), "demo".into());
        options.insert("client_config_file".into(), clouds.display().to_string());
        options.insert("rate".into(), "1000".into());
        set_connections(vec![ConnectionConfig {
            name: "os-test".into(),
            driver: "openstack".into(),
            options,
        }]);
        {
            let mut reg = registry().lock().unwrap();
            reg.os_tokens.clear();
            reg.providers.insert(
                "tenant/os".into(),
                ProviderSnap {
                    driver: "openstack".into(),
                    connection: "os-test".into(),
                    region: "RegionOne".into(),
                    labels: BTreeMap::from([(
                        "debian".into(),
                        json!({"name": "debian", "flavor": "small", "image": "img", "config-drive": true}),
                    )]),
                    flavors: BTreeMap::from([("small".into(), json!({"flavor-id": "flv-1"}))]),
                    images: BTreeMap::from([("img".into(), json!({"image-id": "img-1"}))]),
                },
            );
        }
        let submitted = invoke(
            "ep.os_submit_create_server",
            &json!({"provider": "tenant/os", "label": "debian", "hostname": "np1", "metadata": {"zuul_node_uuid": "u1"}}),
        )
        .unwrap();
        let done = invoke(
            "ep.os_complete_create_server",
            &json!({"provider": "tenant/os", "future_id": submitted["future_id"]}),
        )
        .unwrap();
        assert_eq!(done["server"]["id"], "srv-1");
        assert_eq!(done["server"]["status"], "BUILD");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn kubernetes_namespace_and_pod_match_endpoint() {
        let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
        let port = serve(|_port, req| {
            if req.starts_with("POST /api/v1/namespaces ") || req.starts_with("POST /api/v1/namespaces\r") {
                assert!(req.contains("\"kind\":\"Namespace\""), "{req}");
                assert!(req.contains("\"name\":\"npk\""), "{req}");
                return json_ok(r#"{"kind":"Namespace"}"#);
            }
            if req.contains("POST /api/v1/namespaces/npk/pods") {
                assert!(req.contains("\"kind\":\"Pod\""), "{req}");
                assert!(req.contains("\"restartPolicy\":\"Never\""), "{req}");
                return json_ok(r#"{"kind":"Pod","status":{"phase":"Pending"}}"#);
            }
            if req.contains("GET /api/v1/namespaces/npk/pods/npk") {
                return json_ok(r#"{"status":{"phase":"Running"}}"#);
            }
            (404, Vec::new(), format!("no {req}"))
        });
        let dir = std::env::temp_dir().join(format!("kube-{}", new_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let kubeconfig = dir.join("config");
        std::fs::write(
            &kubeconfig,
            format!(
                "apiVersion: v1\ncurrent-context: ctx\ncontexts:\n- name: ctx\n  context:\n    cluster: c\n    user: u\nclusters:\n- name: c\n  cluster:\n    server: http://127.0.0.1:{port}\nusers:\n- name: u\n  user:\n    token: t-1\n"
            ),
        )
        .unwrap();
        let mut options = BTreeMap::new();
        options.insert("driver".into(), "kubernetes".into());
        options.insert("kubeconfig_file".into(), kubeconfig.display().to_string());
        options.insert("rate".into(), "1000".into());
        set_connections(vec![ConnectionConfig {
            name: "k8s-test".into(),
            driver: "kubernetes".into(),
            options,
        }]);
        {
            let mut reg = registry().lock().unwrap();
            reg.providers.insert(
                "tenant/k8s".into(),
                ProviderSnap {
                    driver: "kubernetes".into(),
                    connection: "k8s-test".into(),
                    region: String::new(),
                    labels: BTreeMap::from([(
                        "pod".into(),
                        json!({"name": "pod", "kind": "pod", "spec": {"containers": [{"name": "c", "image": "debian"}]}}),
                    )]),
                    flavors: BTreeMap::new(),
                    images: BTreeMap::new(),
                },
            );
        }
        invoke(
            "ep.k8s_create_namespace",
            &json!({"provider": "tenant/k8s", "hostname": "npk", "tags": {"zuul": "1"}}),
        )
        .unwrap();
        invoke(
            "ep.k8s_create_pod",
            &json!({"provider": "tenant/k8s", "namespace": "npk", "hostname": "npk", "label": "pod", "tags": {"app": "zuul"}}),
        )
        .unwrap();
        let pod = invoke(
            "ep.k8s_get_pod",
            &json!({"provider": "tenant/k8s", "namespace": "npk", "hostname": "npk"}),
        )
        .unwrap();
        assert_eq!(pod["ready"], true);
        let _ = std::fs::remove_dir_all(dir);
    }

    fn aws_provider(name: &str, connection: &str, port: u16, labels: Value, flavors: Value, images: Value) {
        let mut options = BTreeMap::new();
        options.insert("access_key_id".into(), "AKID".into());
        options.insert("secret_access_key".into(), "SECRET".into());
        options.insert("endpoint_url".into(), format!("http://127.0.0.1:{port}"));
        options.insert("rate".into(), "1000".into());
        set_connections(vec![ConnectionConfig {
            name: connection.into(),
            driver: "aws".into(),
            options,
        }]);
        let mut reg = registry().lock().unwrap();
        reg.aws.clear();
        reg.providers.insert(
            name.into(),
            ProviderSnap {
                driver: "aws".into(),
                connection: connection.into(),
                region: "us-east-1".into(),
                labels: BTreeMap::from([("debian".into(), labels)]),
                flavors: BTreeMap::from([("small".into(), flavors)]),
                images: BTreeMap::from([("img".into(), images)]),
            },
        );
    }

    #[test]
    fn aws_retries_match_botocore_standard_mode() {
        let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
        let hits = Arc::new(StdMutex::new(0u32));
        let hits_bg = hits.clone();
        let port = serve(move |_port, req| {
            if req.contains("Action=DescribeInstances") {
                let n = {
                    let mut h = hits_bg.lock().unwrap();
                    *h += 1;
                    *h
                };
                if n == 1 {
                    return (
                        503,
                        Vec::new(),
                        r#"<?xml version="1.0"?><Response><Errors><Error><Code>RequestLimitExceeded</Code><Message>slow</Message></Error></Errors></Response>"#.into(),
                    );
                }
                return json_ok(
                    r#"<?xml version="1.0"?><DescribeInstancesResponse><reservationSet></reservationSet></DescribeInstancesResponse>"#,
                );
            }
            (404, Vec::new(), String::new())
        });
        aws_provider(
            "tenant/retry",
            "aws-retry",
            port,
            json!({"name": "debian", "flavor": "small", "image": "img"}),
            json!({"instance-type": "t3.micro"}),
            json!({"image-id": "ami-1"}),
        );
        let snap = provider_by_name("tenant/retry").unwrap();
        let ep = aws_endpoint(&snap).unwrap();
        let page = fetch_reservations(&ep).expect("retry then success");
        assert!(page.as_array().unwrap().is_empty());
        assert_eq!(*hits.lock().unwrap(), 2);
    }

    #[test]
    fn instance_list_cache_returns_stale_and_refreshes() {
        let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
        let hits = Arc::new(StdMutex::new(0u32));
        let hold = Arc::new(StdMutex::new(true));
        let hits_bg = hits.clone();
        let hold_bg = hold.clone();
        let port = serve(move |_port, req| {
            if req.contains("Action=DescribeInstances") {
                let n = {
                    let mut h = hits_bg.lock().unwrap();
                    *h += 1;
                    *h
                };
                if n >= 2 {
                    while *hold_bg.lock().unwrap() {
                        thread::sleep(Duration::from_millis(10));
                    }
                    return json_ok(
                        r#"<?xml version="1.0"?><DescribeInstancesResponse><reservationSet><item><instancesSet><item><instanceId>i-new</instanceId></item></instancesSet></item></reservationSet></DescribeInstancesResponse>"#,
                    );
                }
                return json_ok(
                    r#"<?xml version="1.0"?><DescribeInstancesResponse><reservationSet><item><instancesSet><item><instanceId>i-old</instanceId></item></instancesSet></item></reservationSet></DescribeInstancesResponse>"#,
                );
            }
            (404, Vec::new(), String::new())
        });
        aws_provider(
            "tenant/cache",
            "aws-cache",
            port,
            json!({"name": "debian", "flavor": "small", "image": "img"}),
            json!({"instance-type": "t3.micro"}),
            json!({"image-id": "ami-1"}),
        );
        let args = json!({"provider": "tenant/cache", "obj": {"InstanceId": "i-old"}});
        let first = aws_refresh(&args).unwrap();
        assert_eq!(first["obj"]["InstanceId"], "i-old");
        let snap = provider_by_name("tenant/cache").unwrap();
        let ep = aws_endpoint(&snap).unwrap();
        ep.lists.lock().unwrap().get_mut("instances").unwrap().at =
            Instant::now() - Duration::from_secs(30);
        let stale = aws_refresh(&args).unwrap();
        assert_eq!(stale["obj"]["InstanceId"], "i-old");
        *hold.lock().unwrap() = false;
        for _ in 0..50 {
            if *hits.lock().unwrap() >= 2 {
                thread::sleep(Duration::from_millis(50));
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }
        assert!(*hits.lock().unwrap() >= 2, "background refresh did not run");
    }

    #[test]
    fn host_and_volume_quota_codes_match_const() {
        let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
        aws_provider(
            "tenant/quota",
            "aws-quota",
            9,
            json!({"name": "debian", "flavor": "small", "image": "img", "volume-type": "gp3", "volume-size": 20}),
            json!({"instance-type": "c5.metal", "dedicated-host": true}),
            json!({"image-id": "ami-1"}),
        );
        let quota = quota_for_label(&json!({"provider": "tenant/quota", "label": "debian"})).unwrap();
        assert_eq!(quota["quota"]["instances"].as_f64(), Some(1.0));
        assert_eq!(quota["quota"]["L-81657574"].as_f64(), Some(1.0));
        assert_eq!(quota["quota"]["L-7A658B76"].as_f64(), Some(20.0));
        assert!(quota["quota"].get("cores").is_none());
    }

    #[test]
    fn list_service_quotas_translates_ebs_storage_to_gb() {
        let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
        let port = serve(|_port, req| {
            assert!(req.contains("ServiceQuotasV20190624.ListServiceQuotas"), "{req}");
            json_ok(r#"{"Quotas":[{"QuotaCode":"L-1216C47A","Value":64},{"QuotaCode":"L-7A658B76","Value":5}]}"#)
        });
        aws_provider(
            "tenant/limits",
            "aws-limits",
            port,
            json!({"name": "debian", "flavor": "small", "image": "img"}),
            json!({"instance-type": "t3.micro"}),
            json!({"image-id": "ami-1"}),
        );
        let limits = endpoint_limits("tenant/limits").expect("limits");
        assert_eq!(limits.quota.get("L-1216C47A").copied().unwrap_or(0.0), 64.0);
        assert_eq!(limits.quota.get("L-7A658B76").copied().unwrap_or(0.0), 5000.0);
    }

    #[test]
    fn create_fleet_uses_launch_template() {
        let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
        let seen = Arc::new(StdMutex::new(String::new()));
        let seen_bg = seen.clone();
        let port = serve(move |_port, req| {
            if req.contains("Action=DescribeLaunchTemplates") {
                return json_ok(r#"<?xml version="1.0"?><DescribeLaunchTemplatesResponse><launchTemplateSet></launchTemplateSet></DescribeLaunchTemplatesResponse>"#);
            }
            if req.contains("Action=CreateLaunchTemplate") {
                assert!(req.contains("LaunchTemplateName=zuul-launch-template-"), "{req}");
                assert!(req.contains("AutoRecovery=disabled"), "{req}");
                *seen_bg.lock().unwrap() = req.to_string();
                return json_ok(r#"<?xml version="1.0"?><CreateLaunchTemplateResponse><launchTemplate><launchTemplateName>zuul</launchTemplateName></launchTemplate></CreateLaunchTemplateResponse>"#);
            }
            if req.contains("Action=CreateFleet") {
                assert!(req.contains("Type=instant"), "{req}");
                assert!(req.contains("Version=%24Latest") || req.contains("Version=$Latest"), "{req}");
                assert!(req.contains("InstanceType=t3.micro"), "{req}");
                return json_ok(r#"<?xml version="1.0"?><CreateFleetResponse><instancesSet><item><instanceIds><item>i-fleet</item></instanceIds></item></instancesSet></CreateFleetResponse>"#);
            }
            panic!("unexpected {req}");
        });
        set_system_id("sys-1");
        aws_provider(
            "tenant/fleet",
            "aws-fleet",
            port,
            json!({"name": "debian", "flavor": "small", "image": "img", "key-name": "zuul"}),
            json!({"fleet": {"instance_types": ["t3.micro"], "allocation_strategy": "lowest-price"}}),
            json!({"image-id": "ami-1"}),
        );
        let submitted = invoke(
            "ep.submit_create_instance",
            &json!({"provider": "tenant/fleet", "label": "debian", "hostname": "npf", "tags": {"Name": "npf"}}),
        )
        .unwrap();
        let done = invoke(
            "ep.complete_create_instance",
            &json!({"provider": "tenant/fleet", "future_id": submitted["future_id"]}),
        )
        .unwrap();
        assert_eq!(done["instance"]["InstanceId"], "i-fleet");
        assert!(seen.lock().unwrap().contains("zuul_system_id"));
        assert!(seen.lock().unwrap().contains("sys-1"));
    }

    #[test]
    fn s3_put_sends_tagging_header() {
        let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
        let port = serve(|_port, req| {
            let lower = req.to_ascii_lowercase();
            assert!(lower.contains("x-amz-tagging:"), "{req}");
            assert!(req.contains("zuul_upload_uuid"), "{req}");
            (200, Vec::new(), String::new())
        });
        aws_provider(
            "tenant/tag",
            "aws-tag",
            port,
            json!({"name": "debian", "flavor": "small", "image": "img"}),
            json!({}),
            json!({}),
        );
        s3_put_tagged("tenant/tag", "bucket", "obj", b"hello-image", "application/octet-stream", Some("zuul_upload_uuid=abc")).unwrap();
    }

    #[test]
    fn ebs_checksum_mismatch_deletes_snapshot() {
        let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
        let deleted = Arc::new(StdMutex::new(false));
        let deleted_bg = deleted.clone();
        let port = serve(move |_port, req| {
            if req.starts_with("POST /snapshots ") || req.starts_with("POST /snapshots\r") {
                return json_ok(r#"{"SnapshotId":"snap-bad"}"#);
            }
            if req.contains("/blocks/") {
                return json_ok(r#"{"Checksum":"not-the-digest"}"#);
            }
            if req.contains("Action=DeleteSnapshot") && req.contains("snap-bad") {
                *deleted_bg.lock().unwrap() = true;
                return json_ok(r#"<?xml version="1.0"?><DeleteSnapshotResponse></DeleteSnapshotResponse>"#);
            }
            (404, Vec::new(), format!("no {req}"))
        });
        aws_provider(
            "tenant/ebs",
            "aws-ebs",
            port,
            json!({"name": "debian", "flavor": "small", "image": "img"}),
            json!({}),
            json!({}),
        );
        let err = ebs_put_snapshot("tenant/ebs", 1, &[b"block".to_vec()], &json!({})).unwrap_err();
        assert!(err.to_string().to_ascii_lowercase().contains("checksum"), "{err}");
        assert!(*deleted.lock().unwrap());
    }

    #[test]
    fn import_snapshot_registers_ami() {
        let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
        let port = serve(|_port, req| {
            if req.contains("Action=ImportSnapshot") {
                assert!(req.contains("S3Bucket=bucket"), "{req}");
                assert!(req.contains("S3Key=img.raw"), "{req}");
                return json_ok(r#"<?xml version="1.0"?><ImportSnapshotResponse><importTaskId>import-1</importTaskId></ImportSnapshotResponse>"#);
            }
            if req.contains("Action=DescribeImportSnapshotTasks") {
                return json_ok(r#"<?xml version="1.0"?><DescribeImportSnapshotTasksResponse><importSnapshotTaskSet><item><importTaskId>import-1</importTaskId><snapshotTaskDetail><status>completed</status><snapshotId>snap-imp</snapshotId></snapshotTaskDetail></item></importSnapshotTaskSet></DescribeImportSnapshotTasksResponse>"#);
            }
            if req.contains("Action=CreateTags") {
                return json_ok(r#"<?xml version="1.0"?><CreateTagsResponse></CreateTagsResponse>"#);
            }
            if req.contains("Action=RegisterImage") {
                assert!(req.contains("SnapshotId=snap-imp"), "{req}");
                assert!(req.contains("DeviceName=%2Fdev%2Fsda1") || req.contains("DeviceName=/dev/sda1"), "{req}");
                return json_ok(r#"<?xml version="1.0"?><RegisterImageResponse><imageId>ami-imp</imageId></RegisterImageResponse>"#);
            }
            panic!("unexpected {req}");
        });
        aws_provider(
            "tenant/import",
            "aws-import",
            port,
            json!({}),
            json!({}),
            json!({"name": "debian", "canonical_name": "debian-img", "import-method": "snapshot", "architecture": "x86_64", "volume-size": 8}),
        );
        let result = upload_sdk_run(&json!({
            "provider": "tenant/import",
            "kind": "import",
            "upload_uuid": "up-1",
            "image_name": "debian-img",
            "image_format": "raw",
            "artifact_url": "s3://bucket/img.raw"
        }))
        .unwrap();
        assert_eq!(result["external_id"], "ami-imp");
    }

    #[test]
    fn azure_vm_body_covers_14_3_0_fields() {
        let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
        let body = Arc::new(StdMutex::new(String::new()));
        let body_bg = body.clone();
        let port = serve(move |_port, req| {
            if req.contains("/oauth2/token") {
                return json_ok(r#"{"access_token":"tok","expires_on":"9999999999"}"#);
            }
            if req.starts_with("PUT ") {
                *body_bg.lock().unwrap() = req.to_string();
                return json_ok(r#"{"id":"/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/npabc","location":"eastus","properties":{"provisioningState":"Creating"}}"#);
            }
            (404, Vec::new(), format!("no {req}"))
        });
        set_test_azure_endpoints(&format!("http://127.0.0.1:{port}"), &format!("http://127.0.0.1:{port}"));
        let mut options = BTreeMap::new();
        options.insert("tenant_id".into(), "tid".into());
        options.insert("client_id".into(), "cid".into());
        options.insert("client_secret".into(), "sec".into());
        options.insert("subscription_id".into(), "sub".into());
        options.insert("rate".into(), "1000".into());
        set_connections(vec![ConnectionConfig {
            name: "azure-full".into(),
            driver: "azure".into(),
            options,
        }]);
        {
            let mut reg = registry().lock().unwrap();
            reg.azure_tokens.clear();
            reg.providers.insert(
                "tenant/azure-full".into(),
                ProviderSnap {
                    driver: "azure".into(),
                    connection: "azure-full".into(),
                    region: "eastus".into(),
                    labels: BTreeMap::from([(
                        "debian".into(),
                        json!({
                            "name": "debian",
                            "flavor": "small",
                            "image": "img",
                            "resource-group": "rg",
                            "key-data": "ssh-rsa A",
                            "userdata": "#!/bin/sh",
                            "ephemeral-disk": true,
                            "user-assigned-identities": [{"name": "zuul-id", "resource_group": "rg"}]
                        }),
                    )]),
                    flavors: BTreeMap::from([(
                        "small".into(),
                        json!({"vm-size": "Standard_B1s", "ipv6": true, "public-ipv6": true, "ipv4": false, "priority": "spot"}),
                    )]),
                    images: BTreeMap::from([("img".into(), json!({"username": "zuul"}))]),
                },
            );
        }
        let submitted = invoke(
            "ep.azure_submit_create_vm",
            &json!({
                "provider": "tenant/azure-full",
                "label": "debian",
                "hostname": "npabc",
                "subnet_id": "/subscriptions/sub/subnets/s",
                "image_reference": {"id": "/images/img"},
                "tags": {"zuul_node_uuid": "u1"}
            }),
        )
        .unwrap();
        invoke(
            "ep.azure_complete_create_vm",
            &json!({"provider": "tenant/azure-full", "future_id": submitted["future_id"]}),
        )
        .unwrap();
        let req = body.lock().unwrap().clone();
        assert!(req.contains("IPv6"), "{req}");
        assert!(!req.contains("\"publicIPAddressConfiguration\":null"), "{req}");
        assert!(req.contains("diffDiskSettings"), "{req}");
        assert!(req.contains("#!/bin/sh"), "{req}");
        assert!(req.contains("userAssignedIdentities"), "{req}");
        assert!(req.contains("\"priority\":\"Spot\""), "{req}");
        assert!(req.contains("\"evictionPolicy\":\"Delete\""), "{req}");
    }

    #[test]
    fn azure_label_quota_comes_from_sku() {
        let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
        let port = serve(|_port, req| {
            if req.contains("/oauth2/token") {
                return json_ok(r#"{"access_token":"tok","expires_on":"9999999999"}"#);
            }
            if req.contains("/skus?") {
                return json_ok(r#"{"value":[{"name":"Standard_B1s","locations":["eastus"],"capabilities":[{"name":"vCPUs","value":"1"},{"name":"MemoryGB","value":"1"}]}]}"#);
            }
            (404, Vec::new(), format!("no {req}"))
        });
        set_test_azure_endpoints(&format!("http://127.0.0.1:{port}"), &format!("http://127.0.0.1:{port}"));
        let mut options = BTreeMap::new();
        options.insert("tenant_id".into(), "tid".into());
        options.insert("client_id".into(), "cid".into());
        options.insert("client_secret".into(), "sec".into());
        options.insert("subscription_id".into(), "sub".into());
        options.insert("rate".into(), "1000".into());
        set_connections(vec![ConnectionConfig {
            name: "azure-sku".into(),
            driver: "azure".into(),
            options,
        }]);
        {
            let mut reg = registry().lock().unwrap();
            reg.azure_tokens.clear();
            reg.providers.insert(
                "tenant/azure-sku".into(),
                ProviderSnap {
                    driver: "azure".into(),
                    connection: "azure-sku".into(),
                    region: "eastus".into(),
                    labels: BTreeMap::from([("debian".into(), json!({"flavor": "small", "image": "img"}))]),
                    flavors: BTreeMap::from([("small".into(), json!({"vm-size": "Standard_B1s", "priority": "spot"}))]),
                    images: BTreeMap::new(),
                },
            );
        }
        let quota = quota_for_label(&json!({"provider": "tenant/azure-sku", "label": "debian"})).unwrap();
        assert_eq!(quota["quota"]["spot_cores"].as_f64(), Some(1.0));
        assert_eq!(quota["quota"]["ram"].as_f64(), Some(1024.0));
        assert_eq!(quota["quota"]["instances"].as_f64(), Some(1.0));
    }

    #[test]
    fn azure_and_k8s_update_node_return_instance_fields() {
        let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
        {
            let mut reg = registry().lock().unwrap();
            reg.providers.insert(
                "tenant/k8s-upd".into(),
                ProviderSnap {
                    driver: "kubernetes".into(),
                    connection: "k8s-test".into(),
                    region: String::new(),
                    labels: BTreeMap::from([("pod".into(), json!({"kind": "pod", "flavor": "f", "image": "i"}))]),
                    flavors: BTreeMap::new(),
                    images: BTreeMap::new(),
                },
            );
        }
        let node = k8s_update_node(&json!({
            "provider": "tenant/k8s-upd",
            "label": "pod",
            "hostname": "npk",
            "kind": "pod",
            "kubernetes_namespace_id": "npk",
            "kubernetes_connection": {"token": "t"}
        }))
        .unwrap();
        assert_eq!(node["node"]["kubernetes_namespace_id"], "npk");
        assert_eq!(node["node"]["quota"]["pods"], 1);
        assert_eq!(node["node"]["quota"]["namespaces"], 1);
    }

    fn os_cloud(port: u16, with_network: bool) -> String {
        let dir = std::env::temp_dir().join(format!("os-{}", new_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("clouds.yaml");
        let network = if with_network {
            format!(",{{\"type\":\"network\",\"endpoints\":[{{\"interface\":\"public\",\"region\":\"RegionOne\",\"url\":\"http://127.0.0.1:{port}/network\"}}]}}")
        } else {
            String::new()
        };
        std::fs::write(
            &path,
            format!("clouds:\n  demo:\n    region_name: RegionOne\n    external_network: public\n    auth:\n      auth_url: http://127.0.0.1:{port}\n      username: demo\n      password: secret\n      project_name: demo\n"),
        )
        .unwrap();
        let _ = network;
        path.display().to_string()
    }

    #[test]
    fn neutron_floating_ip_attaches_by_port() {
        let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
        let port = serve(|port, req| {
            if req.contains("/auth/tokens") {
                let body = format!(
                    "{{\"token\":{{\"catalog\":[{{\"type\":\"compute\",\"endpoints\":[{{\"interface\":\"public\",\"region\":\"RegionOne\",\"url\":\"http://127.0.0.1:{port}/compute\"}}]}},{{\"type\":\"network\",\"endpoints\":[{{\"interface\":\"public\",\"region\":\"RegionOne\",\"url\":\"http://127.0.0.1:{port}/network\"}}]}}]}}}}"
                );
                return (201, vec![("X-Subject-Token".into(), "tok".into())], body);
            }
            if req.contains("GET /network/v2.0/ports") {
                return json_ok(r#"{"ports":[{"id":"port-1","device_id":"srv-1"}]}"#);
            }
            if req.contains("POST /network/v2.0/floatingips") {
                return json_ok(r#"{"floatingip":{"id":"fip-1","floating_ip_address":"203.0.113.5","status":"DOWN"}}"#);
            }
            if req.contains("PUT /network/v2.0/floatingips/fip-1") {
                assert!(req.contains("\"port_id\":\"port-1\""), "{req}");
                return json_ok(r#"{"floatingip":{"id":"fip-1","floating_ip_address":"203.0.113.5","port_id":"port-1","status":"ACTIVE"}}"#);
            }
            (404, Vec::new(), format!("no {req}"))
        });
        let clouds = os_cloud(port, true);
        let mut options = BTreeMap::new();
        options.insert("cloud".into(), "demo".into());
        options.insert("client_config_file".into(), clouds.clone());
        options.insert("rate".into(), "1000".into());
        set_connections(vec![ConnectionConfig {
            name: "os-n".into(),
            driver: "openstack".into(),
            options,
        }]);
        {
            let mut reg = registry().lock().unwrap();
            reg.os_tokens.clear();
            reg.providers.insert(
                "tenant/os-n".into(),
                ProviderSnap {
                    driver: "openstack".into(),
                    connection: "os-n".into(),
                    region: "RegionOne".into(),
                    labels: BTreeMap::new(),
                    flavors: BTreeMap::new(),
                    images: BTreeMap::new(),
                },
            );
        }
        let fip = os_create_floating_ip(&json!({"provider": "tenant/os-n", "server": {"id": "srv-1"}})).unwrap();
        assert_eq!(fip["floating_ip"]["port_id"], "port-1");
        let _ = std::fs::remove_file(&clouds);
    }

    #[test]
    fn nova_floating_ip_uses_add_floating_ip() {
        let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
        let port = serve(|port, req| {
            if req.contains("/auth/tokens") {
                let body = format!(
                    "{{\"token\":{{\"catalog\":[{{\"type\":\"compute\",\"endpoints\":[{{\"interface\":\"public\",\"region\":\"RegionOne\",\"url\":\"http://127.0.0.1:{port}/compute\"}}]}}]}}}}"
                );
                return (201, vec![("X-Subject-Token".into(), "tok".into())], body);
            }
            if req.contains("POST /compute/os-floating-ips") {
                assert!(req.contains("\"pool\":\"public\""), "{req}");
                return json_ok(r#"{"floating_ip":{"id":"fip-n","ip":"203.0.113.9"}}"#);
            }
            if req.contains("/servers/srv-1/action") {
                assert!(req.contains("addFloatingIp"), "{req}");
                assert!(req.contains("203.0.113.9"), "{req}");
                return json_ok("{}");
            }
            (404, Vec::new(), format!("no {req}"))
        });
        let dir = std::env::temp_dir().join(format!("os-{}", new_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let clouds = dir.join("clouds.yaml");
        std::fs::write(
            &clouds,
            format!("clouds:\n  demo:\n    region_name: RegionOne\n    external_network: public\n    auth:\n      auth_url: http://127.0.0.1:{port}\n      username: demo\n      password: secret\n      project_name: demo\n"),
        )
        .unwrap();
        let mut options = BTreeMap::new();
        options.insert("cloud".into(), "demo".into());
        options.insert("client_config_file".into(), clouds.display().to_string());
        options.insert("rate".into(), "1000".into());
        set_connections(vec![ConnectionConfig {
            name: "os-nova".into(),
            driver: "openstack".into(),
            options,
        }]);
        {
            let mut reg = registry().lock().unwrap();
            reg.os_tokens.clear();
            reg.providers.insert(
                "tenant/os-nova".into(),
                ProviderSnap {
                    driver: "openstack".into(),
                    connection: "os-nova".into(),
                    region: "RegionOne".into(),
                    labels: BTreeMap::new(),
                    flavors: BTreeMap::new(),
                    images: BTreeMap::new(),
                },
            );
        }
        let fip = os_create_floating_ip(&json!({"provider": "tenant/os-nova", "server": {"id": "srv-1"}})).unwrap();
        assert!(fip["floating_ip"].get("port_id").is_none());
        os_attach_ip(&json!({
            "provider": "tenant/os-nova",
            "server": {"id": "srv-1"},
            "floating_ip": {"ip": "203.0.113.9"}
        }))
        .unwrap();
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn aws_run_instances_against_moto_server() {
        let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
        let python = std::env::var("ZUUL_PYTHON").unwrap_or_else(|_| "python3".into());
        let probe = std::process::Command::new(&python)
            .args(["-c", "import moto.server"])
            .status();
        if !probe.map(|s| s.success()).unwrap_or(false) {
            eprintln!("moto is not importable with {python}; the query mock covers RunInstances");
            return;
        }
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let mut child = std::process::Command::new(&python)
            .args([
                "-m",
                "moto.server",
                "-H",
                "127.0.0.1",
                "-p",
                &port.to_string(),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("moto.server");
        let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let ready = (0..50).any(|_| {
            if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
                true
            } else {
                thread::sleep(Duration::from_millis(100));
                false
            }
        });
        if !ready {
            let _ = child.kill();
            panic!("moto.server did not accept connections");
        }
        let mut options = BTreeMap::new();
        options.insert("access_key_id".into(), "testing".into());
        options.insert("secret_access_key".into(), "testing".into());
        options.insert("endpoint_url".into(), format!("http://127.0.0.1:{port}"));
        options.insert("rate".into(), "1000".into());
        set_connections(vec![ConnectionConfig {
            name: "aws-moto".into(),
            driver: "aws".into(),
            options,
        }]);
        {
            let mut reg = registry().lock().unwrap();
            reg.aws.clear();
            reg.providers.insert(
                "tenant/moto".into(),
                ProviderSnap {
                    driver: "aws".into(),
                    connection: "aws-moto".into(),
                    region: "us-east-1".into(),
                    labels: BTreeMap::from([(
                        "debian".into(),
                        json!({"name": "debian", "flavor": "small", "image": "img"}),
                    )]),
                    flavors: BTreeMap::from([(
                        "small".into(),
                        json!({"instance-type": "t3.micro"}),
                    )]),
                    images: BTreeMap::from([("img".into(), json!({"image-id": "ami-12345678"}))]),
                },
            );
        }
        let ep = aws_endpoint(&provider_of(&json!({"provider": "tenant/moto"})).unwrap()).unwrap();
        let mut reg_params = Map::new();
        reg_params.insert("Name".into(), json!("debian"));
        let registered = ec2(&ep, "RegisterImage", reg_params, false).expect("moto RegisterImage");
        let ami = registered
            .get("ImageId")
            .and_then(|v| v.as_str())
            .unwrap_or("ami-12345678")
            .to_string();
        let submitted = invoke(
            "ep.submit_create_instance",
            &json!({
                "provider": "tenant/moto",
                "label": "debian",
                "hostname": "npmoto",
                "image_external_id": ami,
                "tags": {"Name": "npmoto"}
            }),
        )
        .unwrap();
        let done = invoke(
            "ep.complete_create_instance",
            &json!({"provider": "tenant/moto", "future_id": submitted["future_id"]}),
        );
        let done = done.expect("moto RunInstances");
        let id = done["instance"]["InstanceId"].as_str().unwrap_or("").to_string();
        assert!(id.starts_with('i'), "{done}");
        let snap = provider_by_name("tenant/moto").unwrap();
        let ep = aws_endpoint(&snap).unwrap();
        let bucket_url = format!("{}/zuul-bucket", ep.endpoint.trim_end_matches('/'));
        signed_request(
            "PUT",
            &bucket_url,
            "s3",
            &ep.region,
            &ep.sig.access_key,
            &ep.sig.secret_key,
            None,
            &[("Content-Type", "application/xml")],
            b"",
            true,
        )
        .expect("moto CreateBucket");
        s3_put("tenant/moto", "zuul-bucket", "img.raw", b"raw-bytes", "application/octet-stream")
            .expect("moto S3 PutObject");
        invoke(
            "ep.delete_instance",
            &json!({"provider": "tenant/moto", "instance_id": id}),
        )
        .expect("moto TerminateInstances");
        delete_uploaded_image("tenant/moto", &ami).expect("moto DeregisterImage");
        let _ = child.kill();
    }

    #[test]
    fn service_quota_cache_returns_stale_and_refreshes() {
        let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
        let hits = Arc::new(StdMutex::new(0u32));
        let hold = Arc::new(StdMutex::new(true));
        let hits_bg = hits.clone();
        let hold_bg = hold.clone();
        let port = serve(move |_port, req| {
            if req.contains("ServiceQuotasV20190624.ListServiceQuotas") {
                let n = {
                    let mut h = hits_bg.lock().unwrap();
                    *h += 1;
                    *h
                };
                if n >= 3 {
                    while *hold_bg.lock().unwrap() {
                        thread::sleep(Duration::from_millis(10));
                    }
                    return json_ok(r#"{"Quotas":[{"QuotaCode":"L-1216C47A","Value":9},{"QuotaCode":"L-7A658B76","Value":9}]}"#);
                }
                return json_ok(r#"{"Quotas":[{"QuotaCode":"L-1216C47A","Value":64},{"QuotaCode":"L-7A658B76","Value":5}]}"#);
            }
            (404, Vec::new(), String::new())
        });
        aws_provider(
            "tenant/qcache",
            "aws-qcache",
            port,
            json!({"name": "debian", "flavor": "small", "image": "img"}),
            json!({"instance-type": "t3.micro"}),
            json!({"image-id": "ami-1"}),
        );
        let first = endpoint_limits("tenant/qcache").expect("limits");
        assert_eq!(first.quota.get("L-1216C47A").copied().unwrap_or(0.0), 64.0);
        assert_eq!(first.quota.get("L-7A658B76").copied().unwrap_or(0.0), 5000.0);
        let snap = provider_by_name("tenant/qcache").unwrap();
        let ep = aws_endpoint(&snap).unwrap();
        for key in ["quotas-ec2", "quotas-ebs"] {
            ep.lists.lock().unwrap().get_mut(key).unwrap().at = Instant::now() - Duration::from_secs(400);
        }
        let stale = endpoint_limits("tenant/qcache").expect("stale limits");
        assert_eq!(stale.quota.get("L-1216C47A").copied().unwrap_or(0.0), 64.0);
        assert_eq!(stale.quota.get("L-7A658B76").copied().unwrap_or(0.0), 5000.0);
        *hold.lock().unwrap() = false;
        for _ in 0..50 {
            if *hits.lock().unwrap() >= 4 {
                thread::sleep(Duration::from_millis(50));
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }
        assert!(*hits.lock().unwrap() >= 4, "background quota refresh did not run");
    }

    #[test]
    fn nova_floating_ip_refresh_and_delete_ignore_neutron_catalog() {
        let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
        let hits = Arc::new(StdMutex::new(Vec::<String>::new()));
        let hits_bg = hits.clone();
        let port = serve(move |port, req| {
            let line = req.lines().next().unwrap_or("").to_string();
            hits_bg.lock().unwrap().push(line.clone());
            if req.contains("/auth/tokens") {
                let body = format!(
                    "{{\"token\":{{\"catalog\":[{{\"type\":\"compute\",\"endpoints\":[{{\"interface\":\"public\",\"region\":\"RegionOne\",\"url\":\"http://127.0.0.1:{port}/compute\"}}]}},{{\"type\":\"network\",\"endpoints\":[{{\"interface\":\"public\",\"region\":\"RegionOne\",\"url\":\"http://127.0.0.1:{port}/network\"}}]}}]}}}}"
                );
                return (201, vec![("X-Subject-Token".into(), "tok".into())], body);
            }
            if line.contains("GET /compute/os-floating-ips") {
                return json_ok(r#"{"floating_ips":[{"id":"fip-n","ip":"203.0.113.9","status":"ACTIVE"}]}"#);
            }
            if line.contains("DELETE /compute/os-floating-ips/fip-n") {
                return (204, Vec::new(), String::new());
            }
            if line.contains("/network/") {
                return (500, Vec::new(), "neutron must not be used".into());
            }
            (404, Vec::new(), format!("no {line}"))
        });
        let dir = std::env::temp_dir().join(format!("os-{}", new_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let clouds = dir.join("clouds.yaml");
        std::fs::write(
            &clouds,
            format!("clouds:\n  demo:\n    region_name: RegionOne\n    floating_ip_source: nova\n    external_network: public\n    auth:\n      auth_url: http://127.0.0.1:{port}\n      username: demo\n      password: secret\n      project_name: demo\n"),
        )
        .unwrap();
        let mut options = BTreeMap::new();
        options.insert("cloud".into(), "demo".into());
        options.insert("client_config_file".into(), clouds.display().to_string());
        options.insert("rate".into(), "1000".into());
        set_connections(vec![ConnectionConfig {
            name: "os-src".into(),
            driver: "openstack".into(),
            options,
        }]);
        {
            let mut reg = registry().lock().unwrap();
            reg.os_tokens.clear();
            reg.providers.insert(
                "tenant/os-src".into(),
                ProviderSnap {
                    driver: "openstack".into(),
                    connection: "os-src".into(),
                    region: "RegionOne".into(),
                    labels: BTreeMap::new(),
                    flavors: BTreeMap::new(),
                    images: BTreeMap::new(),
                },
            );
        }
        clear_ttl_cache();
        let refreshed = os_refresh_floating_ip(&json!({
            "provider": "tenant/os-src",
            "floating_ip": {"id": "fip-n"}
        }))
        .unwrap();
        assert_eq!(refreshed["floating_ip"]["ip"], "203.0.113.9");
        os_delete_floating_ip(&json!({
            "provider": "tenant/os-src",
            "floating_ip": {"id": "fip-n"}
        }))
        .unwrap();
        let blob = hits.lock().unwrap().join("\n");
        assert!(blob.contains("GET /compute/os-floating-ips"), "{blob}");
        assert!(blob.contains("DELETE /compute/os-floating-ips/fip-n"), "{blob}");
        assert!(!blob.contains("/network/"), "{blob}");
        let _ = std::fs::remove_dir_all(dir);
    }

    include!("leak_tests.rs");
}
