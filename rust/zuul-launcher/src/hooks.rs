// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// Test-only bridge to the Python fixture. Production builds leave the
// feature off, so every function here is a no-op and the launcher talks
// only to ZooKeeper and the native cloud clients.
//
// The fixture listens on a unix socket. Each launcher thread has its own
// connection. A request can nest one callback (the fixture's patched
// `_acceptRequest` asks this thread to run the Rust accept) before the
// response comes back.

use crate::provider::AdvanceError;
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::sync::{Mutex, OnceLock};
use std::cell::RefCell;

#[cfg(feature = "test-hooks")]
thread_local! {
    static CONN: RefCell<Option<Conn>> = RefCell::new(None);
    static HOST: RefCell<Option<String>> = RefCell::new(None);
}

pub fn bind_hostname(name: &str) {
    #[cfg(feature = "test-hooks")]
    HOST.with(|h| *h.borrow_mut() = Some(name.to_string()));
    #[cfg(not(feature = "test-hooks"))]
    {
        let _ = name;
    }
}

#[cfg_attr(not(feature = "test-hooks"), allow(dead_code))]
struct Conn {
    stream: UnixStream,
    reader: BufReader<UnixStream>,
}

fn socket_path() -> &'static Mutex<Option<String>> {
    static PATH: OnceLock<Mutex<Option<String>>> = OnceLock::new();
    PATH.get_or_init(|| Mutex::new(None))
}

pub fn set_socket(path: impl Into<String>) {
    if let Ok(mut slot) = socket_path().lock() {
        *slot = Some(path.into());
    }
}

pub fn active() -> bool {
    #[cfg(not(feature = "test-hooks"))]
    {
        false
    }
    #[cfg(feature = "test-hooks")]
    {
        socket_path()
            .lock()
            .ok()
            .and_then(|g| g.clone())
            .is_some()
    }
}

/// Ask the fixture. `None` means the op is not hooked and the caller
/// should keep the native path. Errors from a hooked call are `Some(Err)`.
pub fn consult(op: &str, args: &Value) -> Option<Result<Value, AdvanceError>> {
    #[cfg(not(feature = "test-hooks"))]
    {
        let _ = (op, args);
        None
    }
    #[cfg(feature = "test-hooks")]
    {
        if !active() {
            return None;
        }
        match transact(op, args, &mut |_name, _args| {
            json!({"ok": false, "message": "no callback"})
        }) {
            Ok(resp) => decode(resp),
            Err(err) => {
                eprintln!("hook {op}: {err}");
                None
            }
        }
    }
}

/// Like `consult`, but the fixture may call back into `on_callback`
/// before it answers. Used by accept_one.
pub fn consult_cb(
    op: &str,
    args: &Value,
    on_callback: &mut dyn FnMut(&str, &Value) -> Value,
) -> Option<Result<Value, AdvanceError>> {
    #[cfg(not(feature = "test-hooks"))]
    {
        let _ = (op, args, on_callback);
        None
    }
    #[cfg(feature = "test-hooks")]
    {
        if !active() {
            return None;
        }
        match transact(op, args, on_callback) {
            Ok(resp) => decode(resp),
            Err(err) => Some(Err(AdvanceError::Other(err))),
        }
    }
}

pub fn flag(op: &str) -> bool {
    match consult(op, &json!({})) {
        Some(Ok(v)) => v.get("patched").and_then(|p| p.as_bool()).unwrap_or(false),
        _ => false,
    }
}

fn decode(resp: Value) -> Option<Result<Value, AdvanceError>> {
    if resp.get("hooked").and_then(|v| v.as_bool()) != Some(true) {
        return None;
    }
    if let Some(kind) = resp.get("error_kind").and_then(|v| v.as_str()) {
        let message = resp
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("hook error")
            .to_string();
        return Some(match kind {
            "quota" => Err(AdvanceError::Quota(message)),
            "capacity" => Err(AdvanceError::Capacity(message)),
            _ => Err(AdvanceError::Other(message)),
        });
    }
    let result = resp.get("result").cloned().unwrap_or(Value::Null);
    if let Some(kind) = result.get("error_kind").and_then(|v| v.as_str()) {
        let message = result
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("hook error")
            .to_string();
        return Some(match kind {
            "quota" => Err(AdvanceError::Quota(message)),
            "capacity" => Err(AdvanceError::Capacity(message)),
            _ => Err(AdvanceError::Other(message)),
        });
    }
    Some(Ok(result))
}

#[cfg(feature = "test-hooks")]
fn ensure_conn() -> Result<(), String> {
    CONN.with(|slot| {
        let missing = slot.borrow().is_none();
        if !missing {
            return Ok(());
        }
        let path = socket_path()
            .lock()
            .ok()
            .and_then(|g| g.clone())
            .ok_or_else(|| "hook socket unset".to_string())?;
        let stream = UnixStream::connect(&path).map_err(|e| format!("hook connect: {e}"))?;
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(120)))
            .ok();
        let reader_stream = stream.try_clone().map_err(|e| e.to_string())?;
        *slot.borrow_mut() = Some(Conn {
            stream,
            reader: BufReader::new(reader_stream),
        });
        Ok(())
    })
}

#[cfg(feature = "test-hooks")]
fn with_conn<T>(body: impl FnOnce(&mut Conn) -> Result<T, String>) -> Result<T, String> {
    ensure_conn()?;
    CONN.with(|slot| {
        let mut conn = slot.borrow_mut();
        let conn = conn
            .as_mut()
            .ok_or_else(|| "hook connection dropped".to_string())?;
        body(conn)
    })
}

#[cfg(feature = "test-hooks")]
fn transact(
    op: &str,
    args: &Value,
    on_callback: &mut dyn FnMut(&str, &Value) -> Value,
) -> Result<Value, String> {
    let mut body = args.clone();
    if let Some(obj) = body.as_object_mut() {
        HOST.with(|h| {
            if let Some(name) = h.borrow().clone() {
                obj.entry("hostname").or_insert(json!(name));
            }
        });
    }
    let line = json!({"op": op, "args": body}).to_string();
    with_conn(|conn| {
        writeln!(conn.stream, "{line}").map_err(|e| e.to_string())?;
        conn.stream.flush().map_err(|e| e.to_string())
    })?;
    loop {
        // Drop the connection borrow before the callback. accept_one
        // calls back into this thread, and that second transact would
        // panic on the RefCell if this read still held it.
        let msg: Value = with_conn(|conn| {
            let mut response = String::new();
            let n = conn
                .reader
                .read_line(&mut response)
                .map_err(|e| e.to_string())?;
            if n == 0 {
                return Err("hook socket closed".into());
            }
            serde_json::from_str(response.trim()).map_err(|e| e.to_string())
        })?;
        if msg.get("callback").is_some() {
            let name = msg.get("callback").and_then(|v| v.as_str()).unwrap_or("");
            let cb_args = msg.get("args").cloned().unwrap_or(json!({}));
            let result = on_callback(name, &cb_args);
            let reply = json!({"callback_result": result}).to_string();
            with_conn(|conn| {
                writeln!(conn.stream, "{reply}").map_err(|e| e.to_string())?;
                conn.stream.flush().map_err(|e| e.to_string())
            })?;
            continue;
        }
        return Ok(msg);
    }
}

/// Cloud call that prefers the PyO3 handler and otherwise the test hook.
/// `None` from the hook means "not patched".
pub fn bridge(op: &str, args: &Value) -> Option<Result<Value, AdvanceError>> {
    if crate::cloud::enabled() {
        return Some(crate::cloud::invoke(op, args));
    }
    consult(op, args)
}
