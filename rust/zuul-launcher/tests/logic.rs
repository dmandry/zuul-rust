// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::sync::Arc;
use zuul_launcher::codec::{self, decode_polymorphic, decode_zkobject};
use zuul_launcher::config::Config;
use zuul_launcher::jsonutil::{self, python_round_1};
use zuul_launcher::launcher::Launcher;
use zuul_launcher::mmh3;
use zuul_launcher::model::{self, node_state, request_state, NodesetRequest, ProviderNode};
use zuul_launcher::nodescan::{self, NodescanRequest, NodescanWorker};
use zuul_launcher::provider::{CreateMode, MemoryProvider};
use zuul_launcher::zk::{lock_winner, ComponentInfo, MemoryZk};
use serde_json::json;

fn fixtures() -> serde_json::Value {
    let text = include_str!("fixtures/fixtures.json");
    serde_json::from_str(text).unwrap()
}

#[test]
fn mmh3_matches_python() {
    let doc = fixtures();
    for row in doc["mmh3"].as_array().unwrap() {
        let input = row["input"].as_str().unwrap();
        let expect = row["hash"].as_u64().unwrap() as u32;
        assert_eq!(mmh3::hash_str(input), expect, "{input}");
    }
}

#[test]
fn zlib_json_round_trip() {
    let doc = fixtures();
    for (name, blob) in doc["blobs"].as_object().unwrap() {
        let raw = base64_decode(blob["zlib_b64"].as_str().unwrap());
        let value = decode_zkobject(&raw).unwrap_or_else(|e| panic!("{name}: {e}"));
        let again = codec::dumps_compressed(&value);
        assert_eq!(again, raw, "{name} zlib bytes");
        let Some(plain_b64) = blob.get("uncompressed_b64").and_then(|v| v.as_str()) else {
            continue;
        };
        let plain = base64_decode(plain_b64);
        let from_plain = decode_zkobject(&plain).unwrap();
        assert_eq!(jsonutil::dumps(&from_plain), jsonutil::dumps(&value));
        if let Some(pref) = blob.get("prefixed_b64") {
            let bytes = base64_decode(pref.as_str().unwrap());
            let (driver, decoded) = decode_polymorphic(&bytes).unwrap();
            assert_eq!(driver, blob["driver_id"].as_str().unwrap());
            assert_eq!(jsonutil::dumps(&decoded), blob["json"].as_str().unwrap());
        }
    }
}

#[test]
fn unknown_key_round_trips_in_json_and_serializer_omits_it() {
    let doc = fixtures();
    let blob = &doc["blobs"]["nodeset_request_unknown_key"];
    let raw = base64_decode(blob["zlib_b64"].as_str().unwrap());
    let value = decode_zkobject(&raw).unwrap();
    assert_eq!(value["future_field"], json!("keep-me"));
    let req = NodesetRequest::from_value(value);
    assert!(req.extra.get("future_field").is_some());
    let written = req.serialize();
    assert!(written.get("future_field").is_none());
}

#[test]
fn kazoo_lock_child_and_winner_rule() {
    let doc = fixtures();
    let lock = &doc["kazoo_lock"];
    let child = lock["child"].as_str().unwrap();
    assert!(child.contains("__lock__"), "{child}");
    assert!(child.ends_with("0000000042"));
    assert_eq!(lock["payload"], "launcher-a.example.com");
    let mut contenders = BTreeMap::new();
    contenders.insert(child.to_string(), b"launcher-a.example.com".to_vec());
    contenders.insert(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa__lock__0000000099".into(),
        b"other".to_vec(),
    );
    contenders.insert(
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb__lock__0000000007".into(),
        b"winner".to_vec(),
    );
    assert_eq!(lock_winner(&contenders).as_deref(), Some("winner"));
}

#[test]
fn quota_rounding_matches_python() {
    let doc = fixtures();
    for row in doc["quota_rounding"].as_array().unwrap() {
        let input = row["input"].as_f64().unwrap();
        let expect = row["output"].as_f64().unwrap();
        let got = if input < 1.0 { python_round_1(input) } else { input };
        assert!((got - expect).abs() < 1e-9, "{input}: {got} != {expect}");
    }
}

#[test]
fn unassigned_sort_ready_before_building_before_requested() {
    let zk = MemoryZk::new();
    let mut launcher = Launcher::new("launcher-a.example.com", zk);
    let provider = Arc::new(MemoryProvider::static_like("p1", "debian"));
    launcher.add_provider("tenant-a", provider);
    launcher.state = "running".into();
    for (id, state, when) in [
        ("n-req", node_state::REQUESTED, 1.0),
        ("n-ready-late", node_state::READY, 5.0),
        ("n-bldg", node_state::BUILDING, 2.0),
        ("n-ready-early", node_state::READY, 3.0),
    ] {
        let mut node = sample_node(id, state);
        node.fields.insert("request_time".into(), json!(when));
        launcher.api.write_node(&node).unwrap();
        // unassigned: no assignment
        launcher.api.delete_assignment(id).ok();
    }
    let map = launcher.unassigned_nodes();
    let ids = &map["debian-bookworm"];
    assert_eq!(
        ids,
        &vec![
            "n-ready-early".to_string(),
            "n-ready-late".to_string(),
            "n-bldg".to_string(),
            "n-req".to_string(),
        ]
    );
}

#[test]
fn process_provider_node_branches() {
    let zk = MemoryZk::new();
    let mut launcher = Launcher::new("launcher-a.example.com", zk);
    launcher.state = "running".into();
    launcher.now = 1_700_000_100.0;

    // Quota short while still in START returns without leaving requested.
    let mut paused = Arc::new(MemoryProvider::static_like("paused", "debian-bookworm"));
    {
        let p = Arc::get_mut(&mut paused).unwrap();
        p.canonical_name = "paused".into();
        p.endpoint_info.limits.quota.insert("instances".into(), 0.0);
        p.driver_name = "aws".into();
    }
    launcher.add_provider("tenant-a", paused);
    let mut node = sample_node("quota-node", node_state::REQUESTED);
    node.fields.insert("provider".into(), json!("paused"));
    node.driver_id = "aws".into();
    launcher.api.write_node(&node).unwrap();
    launcher.process_provider_node("quota-node").unwrap();
    assert_eq!(launcher.api.nodes["quota-node"].state(), node_state::REQUESTED);

    // Create completes, host key checking off, node becomes ready.
    let mut ready = Arc::new(MemoryProvider::static_like("cloud", "debian-bookworm"));
    {
        let p = Arc::get_mut(&mut ready).unwrap();
        p.canonical_name = "cloud".into();
        p.driver_name = "aws".into();
        p.labels.get_mut("debian-bookworm").unwrap().host_key_checking = false;
    }
    launcher.add_provider("tenant-a", ready);
    let mut node = sample_node("create-node", node_state::REQUESTED);
    node.fields.insert("provider".into(), json!("cloud"));
    node.driver_id = "aws".into();
    node.fields.insert("host_key_checking".into(), json!(false));
    launcher.api.write_node(&node).unwrap();
    launcher.process_provider_node("create-node").unwrap();
    assert_eq!(launcher.api.nodes["create-node"].state(), node_state::READY);
    assert!(launcher.stats.iter().any(|s| s.name == "zuul.nodes.launch.ready"));

    // Generic error -> failed.
    let mut bad = Arc::new(MemoryProvider::static_like("bad", "debian-bookworm"));
    {
        let p = Arc::get_mut(&mut bad).unwrap();
        p.canonical_name = "bad".into();
        p.driver_name = "aws".into();
        p.create_mode = CreateMode::Error;
    }
    launcher.add_provider("tenant-a", bad);
    let mut node = sample_node("err-node", node_state::REQUESTED);
    node.fields.insert("provider".into(), json!("bad"));
    node.driver_id = "aws".into();
    launcher.api.write_node(&node).unwrap();
    launcher.process_provider_node("err-node").unwrap();
    assert!(launcher.api.nodes.get("err-node").is_none());

    // Quota exception during advance -> tempfailed, then cleanup deletes it
    // because tempfailed is a cleanup state. Check the message.
    let mut qfail = Arc::new(MemoryProvider::static_like("qfail", "debian-bookworm"));
    {
        let p = Arc::get_mut(&mut qfail).unwrap();
        p.canonical_name = "qfail".into();
        p.driver_name = "aws".into();
        p.create_mode = CreateMode::QuotaFail;
    }
    launcher.add_provider("tenant-a", qfail);
    let mut node = sample_node("qfail-node", node_state::REQUESTED);
    node.fields.insert("provider".into(), json!("qfail"));
    node.driver_id = "aws".into();
    launcher.api.write_node(&node).unwrap();
    launcher.process_provider_node("qfail-node").unwrap();
    assert!(launcher.messages.iter().any(|m| m.contains("tempfail")));

    // next_state when the request is gone.
    let mut node = sample_node("hold-node", node_state::READY);
    node.fields.insert("provider".into(), json!("cloud"));
    node.next_state = Some(node_state::HOLD.into());
    node.next_comment = Some("paused".into());
    node.request_id = Some("missing-req".into());
    launcher.api.write_node(&node).unwrap();
    launcher.api.write_assignment("hold-node", &json!({
        "request_id": "missing-req",
        "min_request_version": 1,
        "tenant_name": "tenant-a",
    })).unwrap();
    launcher.process_provider_node("hold-node").unwrap();
    let held = &launcher.api.nodes["hold-node"];
    assert_eq!(held.state(), node_state::HOLD);
    assert!(held.request_id.is_none());

    // Expired unassigned node becomes outdated and is deleted by cleanup.
    let mut node = sample_node("old-node", node_state::READY);
    node.fields.insert("provider".into(), json!("cloud"));
    node.fields.insert("max_age".into(), json!(10));
    node.fields.insert("request_time".into(), json!(1.0));
    launcher.api.write_node(&node).unwrap();
    launcher.api.delete_assignment("old-node").ok();
    launcher.now = 100.0;
    launcher.layout_dirty = false;
    launcher.process_provider_node("old-node").unwrap();
    assert!(launcher.api.nodes.get("old-node").is_none());
}

#[test]
fn fake_zk_assign_reuse_and_min_ready() {
    let zk = MemoryZk::new();
    let mut launcher = Launcher::new("launcher-a.example.com", zk.clone());
    launcher.state = "running".into();
    launcher.now = 1_700_000_200.0;
    launcher.api.components = vec![
        ComponentInfo {
            hostname: "launcher-a.example.com".into(),
            state: "running".into(),
            connection_filter: None,
        },
        ComponentInfo {
            hostname: "launcher-b.example.com".into(),
            state: "running".into(),
            connection_filter: None,
        },
    ];
    let mut provider = MemoryProvider::static_like("aws-main", "debian");
    provider.driver_name = "aws".into();
    provider.canonical_name = "aws-main".into();
    provider.labels.get_mut("debian").unwrap().host_key_checking = false;
    provider.labels.get_mut("debian").unwrap().min_ready = Some(1);
    provider.labels.get_mut("debian").unwrap().reuse = true;
    let provider = Arc::new(provider);
    launcher.add_provider("tenant-a", provider);
    launcher.layout_dirty = true;
    launcher.run_once();
    assert_eq!(launcher.state, "running");

    // Min-ready ownership: only the lowest score creates the node.
    let label_cname = launcher.tenant_providers["tenant-a"][0].labels()["debian"].canonical_name.clone();
    let scores = mmh3::scores_for_label(
        &label_cname,
        &[
            "launcher-a.example.com".into(),
            "launcher-b.example.com".into(),
        ],
    );
    let winner = &scores[0].1;
    let created: Vec<_> = launcher
        .api
        .nodes
        .values()
        .filter(|n| n.request_id.is_none())
        .map(|n| n.uuid())
        .collect();
    if winner == "launcher-a.example.com" {
        assert_eq!(created.len(), 1, "winner should create one min-ready node");
    } else {
        assert!(created.is_empty(), "loser must not create min-ready nodes");
        // Run the winner as a second launcher sharing the same zk.
        let mut other = Launcher::new("launcher-b.example.com", zk.clone());
        other.api.sync();
        other.state = "running".into();
        other.now = launcher.now;
        other.api.components = launcher.api.components.clone();
        let provider = launcher.tenant_providers["tenant-a"][0].clone();
        other.add_provider("tenant-a", provider);
        other.layout_dirty = false;
        other.process_min_ready();
        other.api.sync();
        // The second launcher wrote into its own api; re-read zk.
        launcher.api.sync();
        let created: Vec<_> = launcher.api.nodes.values().filter(|n| n.request_id.is_none()).collect();
        assert_eq!(created.len(), 1);
    }

    // Request assign.
    let req = NodesetRequest::from_value(json!({
        "uuid": "req-1",
        "state": "requested",
        "event_state": 0,
        "tenant_name": "tenant-a",
        "pipeline_name": "check",
        "buildset_uuid": "bs",
        "job_uuid": "job",
        "job_name": "test",
        "labels": ["debian"],
        "priority": 200,
        "request_time": 10.0,
        "zuul_event_id": "evt",
        "span_info": null,
        "image_names": null,
        "image_upload_uuid": null,
        "preferred_provider": null,
        "provider_node_data": [],
        "_relative_priority": 0,
        "hash_key": null
    }));
    launcher.api.components = vec![ComponentInfo {
        hostname: "launcher-a.example.com".into(),
        state: "running".into(),
        connection_filter: None,
    }];
    launcher.api.write_request(&req).unwrap();
    launcher.process_requests();
    let stored = launcher.api.requests.get("req-1").unwrap();
    assert_eq!(stored.state(), request_state::ACCEPTED);
    assert_eq!(stored.nodes().len(), 1);

    // Reuse: used -> nodescan -> ready, with a local SSH server.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let host_key = sample_ed25519_blob();
    let expected = nodescan::format_host_key(&host_key).unwrap();
    std::thread::spawn(move || nodescan::serve_ed25519(&host_key, listener));

    let mut node = sample_node("used-node", node_state::USED);
    node.fields.insert("provider".into(), json!("aws-main"));
    node.fields.insert("label".into(), json!("debian"));
    node.fields.insert("reuse".into(), json!(true));
    node.fields.insert("host_key_checking".into(), json!(true));
    node.fields.insert("connection_type".into(), json!("ssh"));
    node.fields.insert("interface_ip".into(), json!("127.0.0.1"));
    node.fields.insert("connection_port".into(), json!(port));
    node.fields.insert("boot_timeout".into(), json!(10));
    node.driver_id = "aws".into();
    launcher.api.write_node(&node).unwrap();
    launcher.api.delete_assignment("used-node").ok();
    launcher.process_provider_node("used-node").unwrap();
    assert_eq!(launcher.api.nodes["used-node"].state(), node_state::USED);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
    while std::time::Instant::now() < deadline {
        launcher.nodescan.poll();
        if launcher.nodescan.get("used-node").map(|s| s.complete).unwrap_or(false) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    launcher.process_provider_node("used-node").unwrap();
    let done = &launcher.api.nodes["used-node"];
    let scan_dbg = launcher.nodescan.get("used-node").map(|s| {
        format!(
            "state={} complete={} err={:?} attempts={} keyfail={} negfail={} keys={:?}",
            s.state, s.complete, s.error, s.init_connection_attempts, s.key_connection_failures, s.key_negotiation_failures, s.keys
        )
    });
    assert_eq!(done.state(), node_state::READY, "messages {:?} scan {:?}", launcher.messages, scan_dbg);
    assert!(done.request_id.is_none());
    let keys = done.fields.get("host_keys").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    assert!(keys.iter().any(|k| k.as_str() == Some(expected.as_str())), "{keys:?}");
}

#[test]
fn nodescan_cap_and_states() {
    let mut worker = NodescanWorker::new();
    for i in 0..nodescan::MAX_REQUESTS {
        assert!(worker.add(NodescanRequest::new(
            &format!("n{i}"),
            "127.0.0.1",
            1,
            false,
            Some("ssh"),
            5.0,
        )));
    }
    assert!(!worker.add(NodescanRequest::new("overflow", "127.0.0.1", 1, false, Some("ssh"), 5.0)));
    worker.poll();
    assert!(worker.get("n0").unwrap().complete);
    assert_eq!(worker.get("n0").unwrap().state, nodescan::COMPLETE);
}

#[test]
fn config_parser_interpolation_and_bools() {
    let mut defaults = BTreeMap::new();
    defaults.insert("ZUUL_USER".into(), "zuul".into());
    let cfg = Config::parse(
        r#"
[zookeeper]
hosts = localhost:2281
tls_key = /tmp/k
tls_cert = /tmp/c
tls_ca = /tmp/ca
session_timeout = 10

[launcher]
command_socket = /tmp/%(name)s.socket
name = launcher
debug = yes

[connection aws]
driver = aws
access_key_id = %(ZUUL_USER)s
"#,
        &defaults,
    )
    .unwrap();
    assert_eq!(cfg.get("launcher", "command_socket").unwrap(), "/tmp/launcher.socket");
    assert_eq!(cfg.getboolean("launcher", "debug").unwrap(), true);
    assert_eq!(cfg.getint("zookeeper", "session_timeout").unwrap(), 10);
    assert_eq!(cfg.get("connection aws", "access_key_id").unwrap(), "zuul");
    let zk = zuul_launcher::config::zookeeper_config(&cfg).unwrap();
    assert_eq!(zk.session_timeout, 10.0);
    let cons = zuul_launcher::config::provider_connections(&cfg).unwrap();
    assert_eq!(cons[0].driver, "aws");
}

#[test]
fn component_json_is_uncompressed_sort_keys() {
    let doc = fixtures();
    let bytes = model::component_bytes(
        "launcher-a.example.com",
        "stopped",
        &Some(vec!["aws-conn".into()]),
    );
    assert_eq!(String::from_utf8(bytes).unwrap(), doc["component_json"].as_str().unwrap());
}

#[test]
fn version_flag() {
    let bin = env!("CARGO_BIN_EXE_zuul-launcher");
    let out = std::process::Command::new(bin)
        .arg("--version")
        .output()
        .unwrap();
    assert!(out.status.success());
    assert_eq!(String::from_utf8(out.stdout).unwrap(), "Zuul version: 14.3.0\n");
}

#[test]
fn driver_call_order() {
    use zuul_launcher::provider::{
        driver_create, driver_delete, AwsCreate, AwsDelete, AzureCreate, AzureDelete, K8sCreate,
        K8sDelete, OpenstackCreate, OpenstackDelete, StateMachine, StaticCreate, StaticDelete,
    };

    let mut aws = AwsCreate::new(false);
    aws.advance().unwrap();
    assert!(aws.complete());
    assert_eq!(
        aws.visited,
        vec![
            "start",
            "start creating instance",
            "creating instance",
            "complete",
        ]
    );
    assert_eq!(
        aws.calls,
        vec![
            "getQuotaForLabel",
            "provider_label_template_names",
            "_submitCreateInstance",
            "_completeCreateInstance",
            "_refresh",
            "getQuotaForLabel",
        ]
    );

    let mut host = AwsCreate::new(true);
    host.advance().unwrap();
    assert_eq!(
        host.visited,
        vec![
            "start",
            "start allocating host",
            "allocating host",
            "start creating instance",
            "creating instance",
            "complete",
        ]
    );
    assert!(host.calls.windows(2).any(|w| w == ["_submitAllocateHost", "_completeAllocateHost"]));

    let mut aws_del = AwsDelete::new(Some("i-1".into()), Some("h-1".into()));
    aws_del.advance().unwrap();
    assert_eq!(
        aws_del.visited,
        vec![
            "start",
            "start deleting instance",
            "deleting instance",
            "start releasing host",
            "releasing host",
            "complete",
        ]
    );
    assert_eq!(
        aws_del.calls.iter().filter(|c| *c != "_refreshDelete").cloned().collect::<Vec<_>>(),
        vec!["_deleteInstance", "_releaseHost"]
    );

    let mut azure = AzureCreate::new();
    azure.advance().unwrap();
    assert_eq!(
        azure.visited,
        vec!["start", "submit creating vm", "creating vm", "querying nic", "querying pip", "complete"]
    );
    assert_eq!(
        azure.calls,
        vec![
            "getQuotaForLabel",
            "_getImageReferenceForImage",
            "_getSubnetIdForLabel",
            "_submitApi(_createVirtualMachine)",
            "_completeApi",
            "_refresh",
            "_succeeded",
            "_refresh",
            "_succeeded",
        ]
    );
    let mut azure_del = AzureDelete::new(Some("vm".into()));
    azure_del.advance().unwrap();
    assert_eq!(azure_del.calls, vec!["_deleteVirtualMachine", "_refreshDelete"]);
    assert_eq!(azure_del.state_name(), "complete");

    let mut os = OpenstackCreate::new();
    os.advance().unwrap();
    assert_eq!(
        os.visited,
        vec!["start", "submit creating server", "creating server", "complete"]
    );
    assert_eq!(
        os.calls,
        vec!["_findFlavor", "_submitApi(_createServer)", "_completeApi", "_refreshServer", "_getInstance"]
    );
    let mut os_fip = OpenstackCreate::new();
    os_fip.auto_floating_ip = true;
    os_fip.needs_floating_ip = true;
    os_fip.advance().unwrap();
    assert_eq!(
        os_fip.visited,
        vec![
            "start",
            "submit creating server",
            "creating server",
            "creating floating ip",
            "attaching floating ip",
            "complete",
        ]
    );
    let mut os_del = OpenstackDelete::new(Some("srv".into()), true);
    os_del.advance().unwrap();
    assert_eq!(
        os_del.visited,
        vec!["start", "deleting floating ip", "delete server", "deleting server", "complete"]
    );
    assert!(os_del.calls.contains(&"_deleteFloatingIp".into()));
    assert!(os_del.calls.contains(&"_submitApi(_deleteServer)".into()));
    assert!(os_del.calls.contains(&"_refreshServerDelete".into()));

    let mut k8s = K8sCreate::new();
    k8s.advance().unwrap();
    assert_eq!(
        k8s.visited,
        vec![
            "start",
            "creating namespace",
            "creating service account",
            "creating rbac",
            "creating pod",
            "complete",
        ]
    );
    assert_eq!(
        k8s.calls,
        vec![
            "_createNamespace",
            "_getSubjectAccessReview",
            "_createImagePullSecrets",
            "_createServiceAccount",
            "_getToken",
            "_createRole",
            "_createPod",
            "_getPod",
            "updateFromInstance",
        ]
    );
    let mut openshift = K8sCreate::new();
    openshift.use_openshift_projects = true;
    openshift.kind = "namespace".into();
    openshift.advance().unwrap();
    assert!(openshift.calls.contains(&"_createProject".into()));
    assert!(!openshift.calls.iter().any(|c| c == "_createPod"));
    let mut k8s_del = K8sDelete::new(Some("ns".into()));
    k8s_del.advance().unwrap();
    assert_eq!(k8s_del.calls, vec!["_deleteNamespace"]);
    assert!(k8s_del.complete());
    assert_eq!(k8s_del.visited, vec!["start", "deleting namespace", "complete"]);

    let static_create = StaticCreate::new();
    assert!(static_create.complete());
    assert_eq!(static_create.calls, vec!["getQuotaForLabel", "updateFromInstance"]);
    let mut static_del = StaticDelete::new(false);
    static_del.advance().unwrap();
    assert!(static_del.complete());
    assert!(!static_del.rebuild);
    let mut recycle = StaticDelete::new(true);
    recycle.set_start_time(1_000.0);
    recycle.set_now(1_100.0);
    recycle.advance().unwrap();
    assert!(!recycle.complete(), "reuse waits DELETE_RECYCLE_TIME");
    recycle.set_now(1_301.0);
    recycle.advance().unwrap();
    assert!(recycle.complete());
    assert!(recycle.rebuild);

    // Factory entry points used by CloudProvider.
    let mut created = driver_create("aws");
    created.advance().unwrap();
    assert!(created.complete());
    let mut k8s_factory = driver_create("kubernetes");
    k8s_factory.advance().unwrap();
    assert!(k8s_factory.complete());
    let mut del = driver_delete("aws");
    del.advance().unwrap();
    assert!(del.complete());
    assert_eq!(del.state_name(), "complete");
}

fn sample_node(id: &str, state: &str) -> ProviderNode {
    let doc = fixtures();
    let text = doc["blobs"]["provider_node_static"]["json"].as_str().unwrap();
    let mut value: serde_json::Value = serde_json::from_str(text).unwrap();
    value["uuid"] = json!(id);
    value["state"] = json!(state);
    value["label"] = json!("debian-bookworm");
    value["provider"] = json!("p1");
    value["tags"]["zuul_node_uuid"] = json!(id);
    ProviderNode::from_parts("static".into(), value)
}

fn sample_ed25519_blob() -> Vec<u8> {
    let mut blob = Vec::new();
    let name = b"ssh-ed25519";
    blob.extend_from_slice(&(name.len() as u32).to_be_bytes());
    blob.extend_from_slice(name);
    let key = [0x11u8; 32];
    blob.extend_from_slice(&(key.len() as u32).to_be_bytes());
    blob.extend_from_slice(&key);
    blob
}

fn base64_decode(s: &str) -> Vec<u8> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(s).unwrap()
}
