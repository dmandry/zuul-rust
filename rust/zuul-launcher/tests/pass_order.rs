// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// Pass order for the two-sibling launch-attempt failure. The Python
// launcher and the native launcher each record _checkRequest's node
// state and provider. The collapsed sequences have to match: locality
// stays on the first provider while the sibling is still building, and
// both move only once launch-attempts is exhausted.

#[path = "harness/mod.rs"]
mod harness;

use serde_json::Value;
use std::fs;
use std::process::Command;

const PYTHON: &str = "/home/ubuntu/zuul-nox-311/bin/python";

#[test]
fn two_sibling_failure_pass_order_matches_python() {
    let py_path = "/tmp/pass-trace-python.jsonl";
    let rs_path = "/tmp/pass-trace-native.jsonl";
    let _ = fs::remove_file(py_path);
    let _ = fs::remove_file(rs_path);
    python_trace(py_path);
    let previous_filter = std::env::var("ZUUL_SUITE_FILTER").ok();
    let previous_trace = std::env::var("ZUUL_PASS_TRACE").ok();
    std::env::remove_var("ZUUL_SUITE_FILTER");
    std::env::set_var("ZUUL_PASS_TRACE", rs_path);
    harness::run(
        "TestLauncherLocality",
        "test_provider_selection_locality_exhaustion",
    );
    match previous_filter {
        Some(value) => std::env::set_var("ZUUL_SUITE_FILTER", value),
        None => std::env::remove_var("ZUUL_SUITE_FILTER"),
    }
    match previous_trace {
        Some(value) => std::env::set_var("ZUUL_PASS_TRACE", value),
        None => std::env::remove_var("ZUUL_PASS_TRACE"),
    }
    let python = collapsed(py_path);
    let rust = collapsed(rs_path);
    assert!(
        !python.is_empty(),
        "python _checkRequest trace is empty ({py_path})"
    );
    assert!(
        !rust.is_empty(),
        "native _checkRequest trace is empty ({rs_path})"
    );
    assert_eq!(
        python, rust,
        "collapsed _checkRequest (state, provider) sequence differs\npython: {python:?}\nnative: {rust:?}"
    );
}

fn collapsed(path: &str) -> Vec<Vec<(String, String)>> {
    let text = fs::read_to_string(path).unwrap_or_else(|err| panic!("read {path}: {err}"));
    // _shuffleProviders randomizes the first provider independently in
    // each process. Compare the role (first provider seen, then the
    // other), which is the locality decision, not the region name.
    let mut roles: Vec<String> = Vec::new();
    let mut out = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(line).unwrap_or_else(|err| panic!("{path}: {err}: {line}"));
        let Some(slots) = value.get("slots").and_then(|v| v.as_array()) else {
            continue;
        };
        if slots.len() != 2 {
            continue;
        }
        let row: Vec<(String, String)> = slots
            .iter()
            .map(|slot| {
                let state = slot.get("state").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let provider = slot.get("provider").and_then(|v| v.as_str()).unwrap_or("");
                if !provider.is_empty() && !roles.iter().any(|r| r == provider) {
                    roles.push(provider.to_string());
                }
                let role = roles
                    .iter()
                    .position(|r| r == provider)
                    .map(|i| format!("p{i}"))
                    .unwrap_or_default();
                (state, role)
            })
            .collect();
        if out.last() != Some(&row) {
            out.push(row);
        }
    }
    out
}

fn python_trace(path: &str) {
    let case = zuul_launcher::suite::find_case(
        "TestLauncherLocality",
        "test_provider_selection_locality_exhaustion",
    );
    let payload = serde_json::json!({
        "source": case.body,
        "patches": case.patches,
        "trace": path,
    });
    let payload_path = "/tmp/pass-order-payload.json";
    fs::write(payload_path, payload.to_string()).expect("payload");
    let script = r#"
import json, os, sys
os.chdir("/workspace")
sys.path.insert(0, "/workspace")
os.environ.pop("ZUUL_LAUNCHER_IMPL", None)
os.environ.pop("PYTHONHOME", None)
payload = json.load(open(sys.argv[1], encoding="utf-8"))
trace = payload["trace"]
open(trace, "w", encoding="utf-8").close()
from tests.launcher_suite_support import Session
import zuul.launcher.server as server

_orig = server.Launcher._checkRequest

def _traced(self, request, log, unassigned_nodes):
    slots = []
    for node_id in list(request.nodes):
        node = self.api.getProviderNode(node_id)
        if node is None:
            return _orig(self, request, log, unassigned_nodes)
        state = node.state if isinstance(node.state, str) else str(node.state)
        slots.append({"state": state, "provider": node.provider or ""})
    with open(trace, "a", encoding="utf-8") as handle:
        handle.write(json.dumps({"slots": slots}) + "\n")
    return _orig(self, request, log, unassigned_nodes)

server.Launcher._checkRequest = _traced
session = Session()
begun = session.begin(
    "TestLauncherLocality",
    "test_provider_selection_locality_exhaustion",
    "bridge",
)
if not begun.get("ok"):
    sys.stderr.write(begun.get("error") or "begin failed")
    raise SystemExit(1)
result = session.execute(payload["source"], payload["patches"] or [], "")
ended = session.end()
if not result.get("ok"):
    sys.stderr.write(result.get("error") or "exec failed")
    raise SystemExit(1)
if not ended.get("ok"):
    sys.stderr.write(ended.get("error") or "end failed")
    raise SystemExit(1)
"#;
    let status = Command::new(PYTHON)
        .arg("-c")
        .arg(script)
        .arg(payload_path)
        .current_dir("/workspace")
        .env_remove("ZUUL_LAUNCHER_IMPL")
        .env_remove("PYTHONHOME")
        .env("PYTHONPATH", "/workspace")
        .env("CI", "1")
        .env("ZUUL_ZK_CA", "/opt/zookeeper/ca/certs/cacert.pem")
        .env("ZUUL_ZK_CERT", "/opt/zookeeper/ca/certs/client.pem")
        .env("ZUUL_ZK_KEY", "/opt/zookeeper/ca/keys/clientkey.pem")
        .env("ZUUL_TEST_ROOT", "/tmp/zuul-test")
        .env("OS_LOG_CAPTURE", "1")
        .env("OS_STDERR_CAPTURE", "1")
        .env("OS_STDOUT_CAPTURE", "1")
        .env("OS_TEST_TIMEOUT", "360")
        .env("PYTHONUNBUFFERED", "1")
        .status()
        .expect("spawn python trace");
    assert!(status.success(), "python pass trace failed: {status}");
}
