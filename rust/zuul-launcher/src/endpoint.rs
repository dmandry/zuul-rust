// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// Fine-grained cloud endpoint operations used by the Rust AWS state
// machines. Each helper forwards through cloud::invoke to the Python
// AwsProviderEndpoint method of the same name so moto and unittest
// patches of those methods still apply.

use crate::cloud;
use crate::provider::AdvanceError;
use serde_json::{json, Value};

fn args(provider: &str, extra: Value) -> Value {
    let mut obj = match extra {
        Value::Object(m) => m,
        _ => serde_json::Map::new(),
    };
    obj.insert("provider".into(), json!(provider));
    Value::Object(obj)
}

pub fn refresh(provider: &str, obj: &Value) -> Result<Value, AdvanceError> {
    let result = cloud::invoke("ep.refresh", &args(provider, json!({ "obj": obj })))?;
    Ok(result.get("obj").cloned().unwrap_or(Value::Null))
}

pub fn refresh_delete(provider: &str, obj: &Value) -> Result<Option<Value>, AdvanceError> {
    let result = cloud::invoke("ep.refresh_delete", &args(provider, json!({ "obj": obj })))?;
    match result.get("obj") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(v.clone())),
    }
}

pub fn submit_create_instance(provider: &str, payload: Value) -> Result<Value, AdvanceError> {
    cloud::invoke("ep.submit_create_instance", &args(provider, payload))
}

pub fn complete_create_instance(provider: &str, future_id: &str) -> Result<Option<Value>, AdvanceError> {
    let result = cloud::invoke(
        "ep.complete_create_instance",
        &args(provider, json!({ "future_id": future_id })),
    )?;
    match result.get("instance") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(v.clone())),
    }
}

pub fn submit_allocate_host(provider: &str, payload: Value) -> Result<Value, AdvanceError> {
    cloud::invoke("ep.submit_allocate_host", &args(provider, payload))
}

pub fn complete_allocate_host(provider: &str, future_id: &str) -> Result<Option<Value>, AdvanceError> {
    let result = cloud::invoke(
        "ep.complete_allocate_host",
        &args(provider, json!({ "future_id": future_id })),
    )?;
    match result.get("host") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(v.clone())),
    }
}

pub fn delete_instance(provider: &str, instance_id: &str) -> Result<Option<Value>, AdvanceError> {
    let result = cloud::invoke(
        "ep.delete_instance",
        &args(provider, json!({ "instance_id": instance_id })),
    )?;
    match result.get("instance") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(v.clone())),
    }
}

pub fn release_host(provider: &str, host_id: &str) -> Result<Option<Value>, AdvanceError> {
    let result = cloud::invoke(
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
    cloud::invoke("ep.quota_for_label", &args(provider, payload))
}

pub fn create_snapshot(provider: &str, volume_id: &str, tags: &Value) -> Result<Value, AdvanceError> {
    let result = cloud::invoke(
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
    let result = cloud::invoke("sm.patched_advance", &Value::Object(payload))?;
    if result.get("patched").and_then(|v| v.as_bool()) == Some(true) {
        Ok(Some(result))
    } else {
        Ok(None)
    }
}
