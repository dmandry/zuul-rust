// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// The same Rust suite bodies, driven through PyO3 against the Python
// launcher, scheduler-side client, ZooKeeper model, and test fixtures.

use std::fs::OpenOptions;
use std::io::Write;
use std::time::Instant;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde::Deserialize;
use serde_json::Value;
use zuul_launcher::suite::{self, Check};

#[derive(Deserialize)]
struct Begin {
    ok: bool,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
struct ExecReply {
    #[serde(default)]
    checks: Vec<Check>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
struct EndReply {
    #[serde(default)]
    error: Option<String>,
}

fn selected(class_name: &str, method: &str) -> bool {
    match std::env::var("ZUUL_SUITE_FILTER") {
        Ok(filter) if !filter.is_empty() => {
            let id = format!("{class_name}.{method}");
            filter.split(',').any(|item| id.contains(item.trim()))
        }
        _ => true,
    }
}

fn record_time(class_name: &str, method: &str, seconds: f64) {
    let path = std::env::var("ZUUL_SUITE_TIMES")
        .unwrap_or_else(|_| "/tmp/zuul-suite-times-bridge.txt".into());
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{class_name}.{method} {seconds:.4}");
    }
    eprintln!("{class_name}.{method} {seconds:.4}s");
}

fn dumps(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<String> {
    let json = py.import_bound("json")?;
    json.call_method1("dumps", (value,))?.extract()
}

fn call_json(py: Python<'_>, value: &Bound<'_, PyAny>, name: &str) -> Value {
    let text = dumps(py, value).unwrap_or_else(|err| panic!("json dumps {name}: {err}"));
    serde_json::from_str(&text).unwrap_or_else(|err| panic!("decode {name}: {err}: {text}"))
}

fn prepare_python() {
    // PyO3 embeds libpython. The stdlib resolves from the interpreter
    // prefix; only the venv site-packages need to be added. PYTHONHOME
    // breaks Ansible's own Python, so leave it unset.
    let sites = "/home/ubuntu/zuul-nox-311/lib/python3.11/site-packages";
    let path = match std::env::var("PYTHONPATH") {
        Ok(existing) if !existing.is_empty() => format!("{sites}:/workspace:{existing}"),
        _ => format!("{sites}:/workspace"),
    };
    std::env::set_var("PYTHONPATH", path);
    pyo3::prepare_freethreaded_python();
}

fn with_session(body: impl FnOnce(Python<'_>, &Bound<'_, PyAny>)) {
    // The interpreter copies the process environment at startup.
    // ZUUL_LAUNCHER_IMPL must already be absent, or tests.base binds
    // TestLauncher to the PyO3 class instead of the Python server.
    std::env::remove_var("ZUUL_LAUNCHER_IMPL");
    prepare_python();
    std::env::set_var("CI", "1");
    std::env::set_var("ZUUL_ZK_CA", "/opt/zookeeper/ca/certs/cacert.pem");
    std::env::set_var("ZUUL_ZK_CERT", "/opt/zookeeper/ca/certs/client.pem");
    std::env::set_var("ZUUL_ZK_KEY", "/opt/zookeeper/ca/keys/clientkey.pem");
    std::env::set_var("ZUUL_TEST_ROOT", "/tmp/zuul-test");
    std::env::set_var("OS_LOG_CAPTURE", "1");
    std::env::set_var("OS_STDERR_CAPTURE", "1");
    std::env::set_var("OS_STDOUT_CAPTURE", "1");
    std::env::set_var("OS_TEST_TIMEOUT", "360");
    std::env::set_var("PYTHONUNBUFFERED", "1");
    Python::with_gil(|py| {
        let sys = py.import_bound("sys").expect("sys");
        sys.getattr("path")
            .expect("path")
            .call_method1("insert", (0, "/workspace"))
            .expect("insert path");
        let support = py
            .import_bound("tests.launcher_suite_support")
            .expect("support");
        let session = support.getattr("Session").expect("Session").call0().expect("session");
        body(py, &session);
    });
}

fn run_with_prelude(class_name: &str, method: &str, prelude: &str, honor_filter: bool) {
    if honor_filter && !selected(class_name, method) {
        return;
    }
    let started = Instant::now();
    let _ = std::fs::create_dir_all("/tmp/zuul-test");
    with_session(|py, session| {
        let begun = session
            .call_method1("begin", (class_name, method, "bridge"))
            .unwrap_or_else(|err| {
                err.print(py);
                panic!("begin failed");
            });
        let begin: Begin = serde_json::from_value(call_json(py, &begun, "begin")).expect("begin");
        if !begin.ok {
            record_time(class_name, method, started.elapsed().as_secs_f64());
            panic!(
                "setUp failed for {class_name}.{method}: {}",
                begin.error.unwrap_or_default()
            );
        }
        let case = suite::find_case(class_name, method);
        let kwargs = PyDict::new_bound(py);
        kwargs.set_item("source", &case.body).unwrap();
        kwargs.set_item("patches", &case.patches).unwrap();
        kwargs.set_item("prelude", prelude).unwrap();
        let reply_value = session
            .call_method("execute", (), Some(&kwargs))
            .unwrap_or_else(|err| {
                err.print(py);
                panic!("execute failed");
            });
        let reply: ExecReply =
            serde_json::from_value(call_json(py, &reply_value, "execute")).expect("exec reply");
        let ended_value = session.call_method0("end").unwrap_or_else(|err| {
            err.print(py);
            panic!("end failed");
        });
        let ended: EndReply =
            serde_json::from_value(call_json(py, &ended_value, "end")).expect("end");
        record_time(class_name, method, started.elapsed().as_secs_f64());
        suite::finish(&reply.checks, reply.error.as_deref());
        if let Some(err) = ended.error.as_deref() {
            if !err.is_empty() {
                panic!("cleanup failed for {class_name}.{method}:\n{err}");
            }
        }
    });
}

fn run(class_name: &str, method: &str) {
    run_with_prelude(class_name, method, "", true);
}

/// Break the Python launcher behavior test_node_image_attributes covers
/// (image connection_port copied onto the node) and require the Rust
/// assertion to fail. The patch lives only in this test.
#[test]
fn mutation_connection_port_fails_node_image_attributes() {
    if let Ok(filter) = std::env::var("ZUUL_SUITE_FILTER") {
        if !filter.is_empty() && !filter.contains("mutation") {
            return;
        }
    }
    let prelude = r#"
import tests.base as base
import zuul.launcher.server as server
_orig = None
_cls = None
for cls in base.TestLauncher.__mro__:
    if "_requestNode" in cls.__dict__:
        _orig = cls.__dict__["_requestNode"]
        _cls = cls
        break
server._suite_orig_request_node = _orig
server._suite_orig_request_node_cls = _cls
def _mut(self, label, request, provider, log, ctx, static_node=None):
    image = provider.images[label.image]
    image.connection_port = 22
    return _orig(self, label, request, provider, log, ctx, static_node=static_node)
_cls._requestNode = _mut
"#;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_with_prelude(
            "TestLauncher",
            "test_node_image_attributes",
            prelude,
            false,
        );
    }));
    prepare_python();
    Python::with_gil(|py| {
        let _ = py.run_bound(
            r#"
import zuul.launcher.server as server
orig = getattr(server, "_suite_orig_request_node", None)
cls = getattr(server, "_suite_orig_request_node_cls", None)
if orig is not None and cls is not None:
    cls._requestNode = orig
"#,
            None,
            None,
        );
    });
    let message = match result {
        Ok(()) => String::new(),
        Err(payload) => payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()))
            .unwrap_or_else(|| "non-string panic".into()),
    };
    assert!(
        message.contains("shell_type") || message.contains("8080"),
        "mutating connection_port to 22 should fail the port assertion, got: {message}"
    );
}

include!("generated_bridge.rs");
