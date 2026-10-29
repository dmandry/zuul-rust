// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// Fine-grained cloud endpoint operations used by the Rust AWS, Azure,
// OpenStack, and Kubernetes state machines. Each helper forwards through
// cloud::invoke to the matching Python ProviderEndpoint method so
// moto / azul / SDK fakes and unittest patches still apply.

use crate::cloud;
use crate::provider::AdvanceError;
use serde_json::{json, Value};

fn dispatch(op: &str, args: &Value) -> Result<Value, AdvanceError> {
    if let Some(hooked) = crate::hooks::consult(op, args) {
        return hooked;
    }
    if cloud::enabled() {
        cloud::invoke(op, args)
    } else {
        crate::native::invoke(op, args)
    }
}

fn args(provider: &str, extra: Value) -> Value {
    let mut obj = match extra {
        Value::Object(m) => m,
        _ => serde_json::Map::new(),
    };
    obj.insert("provider".into(), json!(provider));
    Value::Object(obj)
}

pub fn refresh(provider: &str, obj: &Value) -> Result<Value, AdvanceError> {
    refresh_force(provider, obj, false)
}

pub fn refresh_force(provider: &str, obj: &Value, force: bool) -> Result<Value, AdvanceError> {
    let result = dispatch(
        "ep.refresh",
        &args(provider, json!({ "obj": obj, "force": force })),
    )?;
    Ok(result.get("obj").cloned().unwrap_or(Value::Null))
}

pub fn refresh_delete(provider: &str, obj: &Value) -> Result<Option<Value>, AdvanceError> {
    let result = dispatch("ep.refresh_delete", &args(provider, json!({ "obj": obj })))?;
    match result.get("obj") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(v.clone())),
    }
}

pub fn submit_create_instance(provider: &str, payload: Value) -> Result<Value, AdvanceError> {
    dispatch("ep.submit_create_instance", &args(provider, payload))
}

pub fn complete_create_instance(provider: &str, future_id: &str) -> Result<Option<Value>, AdvanceError> {
    let result = dispatch(
        "ep.complete_create_instance",
        &args(provider, json!({ "future_id": future_id })),
    )?;
    match result.get("instance") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(v.clone())),
    }
}

pub fn submit_allocate_host(provider: &str, payload: Value) -> Result<Value, AdvanceError> {
    dispatch("ep.submit_allocate_host", &args(provider, payload))
}

pub fn complete_allocate_host(provider: &str, future_id: &str) -> Result<Option<Value>, AdvanceError> {
    let result = dispatch(
        "ep.complete_allocate_host",
        &args(provider, json!({ "future_id": future_id })),
    )?;
    match result.get("host") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(v.clone())),
    }
}

pub fn delete_instance(provider: &str, instance_id: &str) -> Result<Option<Value>, AdvanceError> {
    let result = dispatch(
        "ep.delete_instance",
        &args(provider, json!({ "instance_id": instance_id })),
    )?;
    match result.get("instance") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(v.clone())),
    }
}

pub fn release_host(provider: &str, host_id: &str) -> Result<Option<Value>, AdvanceError> {
    let result = dispatch(
        "ep.release_host",
        &args(provider, json!({ "host_id": host_id })),
    )?;
    match result.get("host") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(v.clone())),
    }
}

pub fn quota_for_label(
    provider: &str,
    label: &str,
    instance_type: Option<&str>,
) -> Result<Value, AdvanceError> {
    let mut payload = json!({ "label": label });
    if let Some(it) = instance_type {
        payload
            .as_object_mut()
            .unwrap()
            .insert("instance_type".into(), json!(it));
    }
    dispatch("ep.quota_for_label", &args(provider, payload))
}

pub fn create_snapshot(provider: &str, volume_id: &str, tags: &Value) -> Result<Value, AdvanceError> {
    let result = dispatch(
        "ep.create_snapshot",
        &args(
            provider,
            json!({
                "volume_id": volume_id,
                "tags": tags,
            }),
        ),
    )?;
    Ok(result.get("snapshot").cloned().unwrap_or(Value::Null))
}

pub fn refresh_snapshot(provider: &str, obj: &Value) -> Result<Value, AdvanceError> {
    refresh(provider, obj)
}

/// If the Python state-machine class `advance` is a Mock or otherwise
/// replaced, run that callable and return its result. When not patched,
/// returns None so the Rust state machine continues.
pub fn patched_advance(
    driver: &str,
    kind: &str,
    state: &str,
    complete: bool,
    extra: Value,
) -> Result<Option<Value>, AdvanceError> {
    let mut payload = match extra {
        Value::Object(m) => m,
        _ => serde_json::Map::new(),
    };
    payload.insert("driver".into(), json!(driver));
    payload.insert("kind".into(), json!(kind));
    payload.insert("state".into(), json!(state));
    payload.insert("complete".into(), json!(complete));
    let result = dispatch("sm.patched_advance", &Value::Object(payload))?;
    if result.get("patched").and_then(|v| v.as_bool()) == Some(true) {
        Ok(Some(result))
    } else {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Azure
// ---------------------------------------------------------------------------

pub fn azure_list_instance_id(provider: &str, node_uuid: &str) -> Result<Option<String>, AdvanceError> {
    let result = dispatch(
        "ep.azure_list_instance_id",
        &args(provider, json!({ "node_uuid": node_uuid })),
    )?;
    Ok(result
        .get("azure_vm_id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string()))
}

pub fn azure_get_image_reference(
    provider: &str,
    label: &str,
    image_external_id: Option<&str>,
) -> Result<Value, AdvanceError> {
    let mut payload = json!({ "label": label });
    if let Some(id) = image_external_id {
        payload
            .as_object_mut()
            .unwrap()
            .insert("image_external_id".into(), json!(id));
    }
    let result = dispatch("ep.azure_get_image_reference", &args(provider, payload))?;
    Ok(result.get("image_reference").cloned().unwrap_or(Value::Null))
}

pub fn azure_get_subnet_id(provider: &str, label: &str) -> Result<String, AdvanceError> {
    let result = dispatch(
        "ep.azure_get_subnet_id",
        &args(provider, json!({ "label": label })),
    )?;
    result
        .get("subnet_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| AdvanceError::Other("missing azure subnet id".into()))
}

pub fn azure_submit_create_vm(provider: &str, payload: Value) -> Result<Value, AdvanceError> {
    dispatch("ep.azure_submit_create_vm", &args(provider, payload))
}

pub fn azure_complete_create_vm(provider: &str, future_id: &str) -> Result<Option<Value>, AdvanceError> {
    let result = dispatch(
        "ep.azure_complete_create_vm",
        &args(provider, json!({ "future_id": future_id })),
    )?;
    match result.get("vm") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(v.clone())),
    }
}

pub fn azure_delete_vm(provider: &str, vm_id: &str) -> Result<Option<Value>, AdvanceError> {
    let result = dispatch(
        "ep.azure_delete_vm",
        &args(provider, json!({ "vm_id": vm_id })),
    )?;
    match result.get("vm") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(v.clone())),
    }
}

pub fn azure_update_node(provider: &str, payload: Value) -> Result<Value, AdvanceError> {
    dispatch("ep.azure_update_node", &args(provider, payload))
}

pub fn azure_succeeded(obj: &Value) -> bool {
    obj.get("properties")
        .and_then(|p| p.get("provisioningState"))
        .and_then(|v| v.as_str())
        == Some("Succeeded")
}

pub fn azure_failed(obj: &Value) -> bool {
    obj.get("properties")
        .and_then(|p| p.get("provisioningState"))
        .and_then(|v| v.as_str())
        == Some("Failed")
}

// ---------------------------------------------------------------------------
// OpenStack
// ---------------------------------------------------------------------------

pub fn os_find_flavor(provider: &str, label: &str) -> Result<Value, AdvanceError> {
    dispatch("ep.os_find_flavor", &args(provider, json!({ "label": label })))
}

pub fn os_list_instance_id(provider: &str, node_uuid: &str) -> Result<Option<String>, AdvanceError> {
    let result = dispatch(
        "ep.os_list_instance_id",
        &args(provider, json!({ "node_uuid": node_uuid })),
    )?;
    Ok(result
        .get("openstack_server_id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string()))
}

pub fn os_submit_create_server(provider: &str, payload: Value) -> Result<Value, AdvanceError> {
    dispatch("ep.os_submit_create_server", &args(provider, payload))
}

pub fn os_complete_create_server(provider: &str, future_id: &str) -> Result<Value, AdvanceError> {
    dispatch(
        "ep.os_complete_create_server",
        &args(provider, json!({ "future_id": future_id })),
    )
}

pub fn os_refresh_server(provider: &str, server: &Value) -> Result<Value, AdvanceError> {
    let result = dispatch(
        "ep.os_refresh_server",
        &args(provider, json!({ "server": server })),
    )?;
    Ok(result.get("server").cloned().unwrap_or(Value::Null))
}

pub fn os_get_server(provider: &str, server_id: &str) -> Result<Option<Value>, AdvanceError> {
    let result = dispatch(
        "ep.os_get_server",
        &args(provider, json!({ "server_id": server_id })),
    )?;
    match result.get("server") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(v.clone())),
    }
}

pub fn os_needs_floating_ip(provider: &str, server: &Value) -> Result<bool, AdvanceError> {
    let result = dispatch(
        "ep.os_needs_floating_ip",
        &args(provider, json!({ "server": server })),
    )?;
    Ok(result.get("needs").and_then(|v| v.as_bool()).unwrap_or(false))
}

pub fn os_create_floating_ip(provider: &str, server: &Value) -> Result<Value, AdvanceError> {
    let result = dispatch(
        "ep.os_create_floating_ip",
        &args(provider, json!({ "server": server })),
    )?;
    Ok(result.get("floating_ip").cloned().unwrap_or(Value::Null))
}

pub fn os_refresh_floating_ip(provider: &str, fip: &Value) -> Result<Value, AdvanceError> {
    let result = dispatch(
        "ep.os_refresh_floating_ip",
        &args(provider, json!({ "floating_ip": fip })),
    )?;
    Ok(result.get("floating_ip").cloned().unwrap_or(Value::Null))
}

pub fn os_attach_ip(provider: &str, server: &Value, fip: &Value) -> Result<(), AdvanceError> {
    dispatch(
        "ep.os_attach_ip",
        &args(provider, json!({ "server": server, "floating_ip": fip })),
    )?;
    Ok(())
}

pub fn os_server_floating_ip(
    provider: &str,
    server: &Value,
) -> Result<Option<String>, AdvanceError> {
    let result = dispatch(
        "ep.os_server_floating_ip",
        &args(provider, json!({ "server": server })),
    )?;
    Ok(result
        .get("ip")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string()))
}

pub fn os_get_instance(provider: &str, payload: Value) -> Result<Value, AdvanceError> {
    dispatch("ep.os_get_instance", &args(provider, payload))
}

pub fn os_has_floating_ips(provider: &str) -> Result<bool, AdvanceError> {
    let result = dispatch("ep.os_has_floating_ips", &args(provider, json!({})))?;
    Ok(result.get("has").and_then(|v| v.as_bool()).unwrap_or(false))
}

pub fn os_get_floating_ips(provider: &str, server: &Value) -> Result<Vec<Value>, AdvanceError> {
    let result = dispatch(
        "ep.os_get_floating_ips",
        &args(provider, json!({ "server": server })),
    )?;
    Ok(result
        .get("floating_ips")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default())
}

pub fn os_delete_floating_ip(provider: &str, fip: &Value) -> Result<(), AdvanceError> {
    dispatch(
        "ep.os_delete_floating_ip",
        &args(provider, json!({ "floating_ip": fip })),
    )?;
    Ok(())
}

pub fn os_refresh_floating_ip_delete(
    provider: &str,
    fip: &Value,
) -> Result<Option<Value>, AdvanceError> {
    let result = dispatch(
        "ep.os_refresh_floating_ip_delete",
        &args(provider, json!({ "floating_ip": fip })),
    )?;
    match result.get("floating_ip") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(v.clone())),
    }
}

pub fn os_submit_delete_server(provider: &str, server_id: &str) -> Result<Value, AdvanceError> {
    dispatch(
        "ep.os_submit_delete_server",
        &args(provider, json!({ "server_id": server_id })),
    )
}

pub fn os_complete_delete_server(provider: &str, future_id: &str) -> Result<bool, AdvanceError> {
    let result = dispatch(
        "ep.os_complete_delete_server",
        &args(provider, json!({ "future_id": future_id })),
    )?;
    Ok(result.get("done").and_then(|v| v.as_bool()).unwrap_or(false))
}

pub fn os_refresh_server_delete(
    provider: &str,
    server: &Value,
) -> Result<Option<Value>, AdvanceError> {
    let result = dispatch(
        "ep.os_refresh_server_delete",
        &args(provider, json!({ "server": server })),
    )?;
    match result.get("server") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(v.clone())),
    }
}

pub fn os_handle_server_fault(provider: &str, server_id: &str) -> Result<bool, AdvanceError> {
    let result = dispatch(
        "ep.os_handle_server_fault",
        &args(provider, json!({ "server_id": server_id })),
    )?;
    Ok(result.get("quota").and_then(|v| v.as_bool()).unwrap_or(false))
}

// ---------------------------------------------------------------------------
// Kubernetes
// ---------------------------------------------------------------------------

pub fn k8s_setup(provider: &str, label: &str) -> Result<Value, AdvanceError> {
    dispatch("ep.k8s_setup", &args(provider, json!({ "label": label })))
}

pub fn k8s_create_namespace(provider: &str, hostname: &str, tags: &Value) -> Result<(), AdvanceError> {
    dispatch(
        "ep.k8s_create_namespace",
        &args(provider, json!({ "hostname": hostname, "tags": tags })),
    )?;
    Ok(())
}

pub fn k8s_create_project(provider: &str, hostname: &str, tags: &Value) -> Result<(), AdvanceError> {
    dispatch(
        "ep.k8s_create_project",
        &args(provider, json!({ "hostname": hostname, "tags": tags })),
    )?;
    Ok(())
}

pub fn k8s_get_subject_access_review(
    provider: &str,
    namespace: &str,
) -> Result<Option<bool>, AdvanceError> {
    let result = dispatch(
        "ep.k8s_get_subject_access_review",
        &args(provider, json!({ "namespace": namespace })),
    )?;
    match result.get("allowed") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(v.as_bool()),
    }
}

pub fn k8s_create_image_pull_secrets(
    provider: &str,
    namespace: &str,
    label: &str,
) -> Result<(), AdvanceError> {
    dispatch(
        "ep.k8s_create_image_pull_secrets",
        &args(
            provider,
            json!({ "namespace": namespace, "label": label }),
        ),
    )?;
    Ok(())
}

pub fn k8s_create_service_account(
    provider: &str,
    namespace: &str,
    user: &str,
) -> Result<(), AdvanceError> {
    dispatch(
        "ep.k8s_create_service_account",
        &args(
            provider,
            json!({ "namespace": namespace, "user": user }),
        ),
    )?;
    Ok(())
}

pub fn k8s_get_token(
    provider: &str,
    namespace: &str,
    user: &str,
) -> Result<Option<(String, Option<String>)>, AdvanceError> {
    let result = dispatch(
        "ep.k8s_get_token",
        &args(
            provider,
            json!({ "namespace": namespace, "user": user }),
        ),
    )?;
    match result.get("token") {
        None | Some(Value::Null) => Ok(None),
        Some(token) => {
            let token = token
                .as_str()
                .ok_or_else(|| AdvanceError::Other("k8s token not a string".into()))?
                .to_string();
            let ca = result
                .get("ca_crt")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            Ok(Some((token, ca)))
        }
    }
}

pub fn k8s_create_role(
    provider: &str,
    namespace: &str,
    user: &str,
    restricted_access: bool,
) -> Result<(), AdvanceError> {
    dispatch(
        "ep.k8s_create_role",
        &args(
            provider,
            json!({
                "namespace": namespace,
                "user": user,
                "restricted_access": restricted_access,
            }),
        ),
    )?;
    Ok(())
}

pub fn k8s_create_pod(
    provider: &str,
    namespace: &str,
    hostname: &str,
    label: &str,
    tags: &Value,
) -> Result<(), AdvanceError> {
    dispatch(
        "ep.k8s_create_pod",
        &args(
            provider,
            json!({
                "namespace": namespace,
                "hostname": hostname,
                "label": label,
                "tags": tags,
            }),
        ),
    )?;
    Ok(())
}

pub fn k8s_get_pod(provider: &str, namespace: &str, hostname: &str) -> Result<bool, AdvanceError> {
    let result = dispatch(
        "ep.k8s_get_pod",
        &args(
            provider,
            json!({ "namespace": namespace, "hostname": hostname }),
        ),
    )?;
    Ok(result.get("ready").and_then(|v| v.as_bool()).unwrap_or(false))
}

pub fn k8s_delete_namespace(provider: &str, namespace_id: &str) -> Result<(), AdvanceError> {
    dispatch(
        "ep.k8s_delete_namespace",
        &args(provider, json!({ "namespace_id": namespace_id })),
    )?;
    Ok(())
}

pub fn k8s_delete_project(provider: &str, namespace_id: &str) -> Result<(), AdvanceError> {
    dispatch(
        "ep.k8s_delete_project",
        &args(provider, json!({ "namespace_id": namespace_id })),
    )?;
    Ok(())
}

pub fn k8s_update_node(provider: &str, payload: Value) -> Result<Value, AdvanceError> {
    dispatch("ep.k8s_update_node", &args(provider, payload))
}

pub fn k8s_use_openshift(provider: &str) -> Result<bool, AdvanceError> {
    let result = dispatch("ep.k8s_use_openshift", &args(provider, json!({})))?;
    Ok(result
        .get("use_openshift_projects")
        .and_then(|v| v.as_bool())
        .unwrap_or(false))
}
