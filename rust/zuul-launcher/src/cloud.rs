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
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

pub type Handler = Arc<dyn Fn(&str, &Value) -> Result<Value, String> + Send + Sync>;

fn handlers() -> &'static Mutex<HashMap<String, Handler>> {
    static HANDLERS: OnceLock<Mutex<HashMap<String, Handler>>> = OnceLock::new();
    HANDLERS.get_or_init(|| Mutex::new(HashMap::new()))
}

thread_local! {
    static CURRENT: RefCell<Option<String>> = RefCell::new(None);
    /// When process_once holds the PyO3 mutex, `_acceptRequest` re-enters
    /// on the same thread. Point at the active Launcher so accept can
    /// run without trying to lock the mutex again.
    static ACTIVE_LAUNCHER: RefCell<Option<*mut ()>> = RefCell::new(None);
}

/// Binds cloud calls on this thread to one launcher. Two launchers in
/// one process (test_launcher_request_order) each install a handler;
/// a single global would run both threads against the last one.
pub struct CurrentGuard;

impl Drop for CurrentGuard {
    fn drop(&mut self) {
        CURRENT.with(|c| *c.borrow_mut() = None);
    }
}

pub fn enter(id: &str) -> CurrentGuard {
    CURRENT.with(|c| *c.borrow_mut() = Some(id.to_string()));
    CurrentGuard
}

/// Bind `launcher` as the re-entrant target for accept_one → `_acceptRequest`.
pub struct ActiveLauncherGuard;

impl Drop for ActiveLauncherGuard {
    fn drop(&mut self) {
        ACTIVE_LAUNCHER.with(|c| *c.borrow_mut() = None);
    }
}

pub fn enter_launcher(ptr: *mut ()) -> ActiveLauncherGuard {
    ACTIVE_LAUNCHER.with(|c| *c.borrow_mut() = Some(ptr));
    ActiveLauncherGuard
}

pub fn active_launcher() -> Option<*mut ()> {
    ACTIVE_LAUNCHER.with(|c| *c.borrow())
}

pub fn register_handler(id: &str, handler: Handler) {
    handlers().lock().unwrap().insert(id.to_string(), handler);
}

pub fn unregister_handler(id: &str) {
    handlers().lock().unwrap().remove(id);
}

pub fn current_id() -> Option<String> {
    CURRENT.with(|c| c.borrow().clone())
}

pub fn enabled() -> bool {
    !handlers().lock().unwrap().is_empty()
}

fn current_handler() -> Option<Handler> {
    let guard = handlers().lock().unwrap();
    let current = CURRENT.with(|c| c.borrow().clone());
    if let Some(id) = current {
        return guard.get(&id).cloned();
    }
    if guard.len() == 1 {
        return guard.values().next().cloned();
    }
    None
}

pub fn invoke(op: &str, args: &Value) -> Result<Value, AdvanceError> {
    let handler = current_handler()
        .ok_or_else(|| AdvanceError::Other("no cloud handler".into()))?;
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
/// Null resource values mean unlimited (Python math.inf after JSON sanitization).
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
            if val.is_null() {
                quota.quota.insert(k.clone(), f64::INFINITY);
            } else if let Some(n) = val.as_f64() {
                quota.quota.insert(k.clone(), n);
            } else if let Some(n) = val.as_i64() {
                quota.quota.insert(k.clone(), n as f64);
            } else if let Some(s) = val.as_str() {
                if s.eq_ignore_ascii_case("infinity") || s == "inf" {
                    quota.quota.insert(k.clone(), f64::INFINITY);
                } else if s.eq_ignore_ascii_case("-infinity") {
                    quota.quota.insert(k.clone(), f64::NEG_INFINITY);
                }
            }
        }
    }
    quota
}
