// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// Cloud operations used by the live driver state machines.
//
// The unit-test fakes (moto, the `responses` fixture, the OpenStack and
// Kubernetes fake clients, and unittest/fixtures patches of endpoint
// methods) live inside the test process. The PyO3 crate installs the
// handler below and forwards each call to the Python provider object the
// tests already constructed. The shipped binary can install its own
// handler; with none installed the state machines keep the offline path
// exercised by the Rust unit tests.

use crate::model::QuotaInformation;
use crate::provider::AdvanceError;
use serde_json::Value;
use std::sync::{Arc, Mutex, OnceLock};

pub type Handler = Arc<dyn Fn(&str, &Value) -> Result<Value, String> + Send + Sync>;

fn slot() -> &'static Mutex<Option<Handler>> {
    static HANDLER: OnceLock<Mutex<Option<Handler>>> = OnceLock::new();
    HANDLER.get_or_init(|| Mutex::new(None))
}

pub fn set_handler(handler: Handler) {
    *slot().lock().unwrap() = Some(handler);
}

pub fn clear_handler() {
    *slot().lock().unwrap() = None;
}

pub fn enabled() -> bool {
    slot().lock().unwrap().is_some()
}

pub fn invoke(op: &str, args: &Value) -> Result<Value, AdvanceError> {
    let handler = {
        let guard = slot().lock().unwrap();
        guard.clone().ok_or_else(|| AdvanceError::Other("no cloud handler".into()))?
    };
    match handler(op, args) {
        Ok(value) => match value.get("error_kind").and_then(|v| v.as_str()) {
            Some("quota") => Err(AdvanceError::Quota(message_of(&value))),
            Some("capacity") => Err(AdvanceError::Capacity(message_of(&value))),
            Some(_) => Err(AdvanceError::Other(message_of(&value))),
            None => Ok(value),
        },
        Err(err) => Err(AdvanceError::Other(err)),
    }
}

fn message_of(value: &Value) -> String {
    value
        .get("message")
        .and_then(|v| v.as_str())
        .unwrap_or("cloud error")
        .to_string()
}

/// Payload from the Python quota helpers: {"default": null|number, "quota": {...}}.
pub fn quota_from_payload(value: &Value) -> QuotaInformation {
    let default = match value.get("default") {
        Some(Value::Null) | None => f64::INFINITY,
        Some(v) => v.as_f64().or_else(|| v.as_i64().map(|n| n as f64)).unwrap_or(f64::INFINITY),
    };
    let mut quota = QuotaInformation {
        default,
        quota: Default::default(),
    };
    if let Some(obj) = value.get("quota").and_then(|v| v.as_object()) {
        for (k, val) in obj {
            if let Some(n) = val.as_f64() {
                quota.quota.insert(k.clone(), n);
            } else if let Some(n) = val.as_i64() {
                quota.quota.insert(k.clone(), n as f64);
            }
        }
    }
    quota
}
