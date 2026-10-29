// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use zuul_launcher::codec;
use zuul_launcher::config::quote_plus;
use zuul_launcher::jsonutil;
use zuul_launcher::launcher::Launcher;
use zuul_launcher::provider::MemoryProvider;
use zuul_launcher::zk::{CreateMode, MemoryZk};
use serde_json::json;

fn put_layout(zk: &MemoryZk, tenant: &str, uuid: &str) {
    zk.ensure_path("/zuul/layout").unwrap();
    let path = format!("/zuul/layout/{tenant}");
    let body = json!({
        "tenant_name": tenant,
        "hostname": "scheduler.example.com",
        "last_reconfigured": 1700000000,
        "uuid": uuid,
        "branch_cache_min_ltimes": {"gerrit": 3},
        "last_reconfigure_event_ltime": 9
    });
    let bytes = jsonutil::dumps_bytes(&body);
    if zk.exists(&path).unwrap().is_some() {
        zk.set(&path, &bytes, -1).unwrap();
    } else {
        zk.create(&path, &bytes, CreateMode::Persistent).unwrap();
    }
    let min_ltimes = codec::compress(&jsonutil::dumps_bytes(&json!({
        "min_ltimes": {"gerrit": 5}
    })));
    let data_root = format!("/zuul/layout-data/{uuid}");
    zk.ensure_path(&data_root).unwrap();
    if zk.get_children(&data_root).unwrap().is_empty() {
        zk.create(
            &format!("{data_root}/"),
            &min_ltimes,
            CreateMode::PersistentSequential,
        )
        .unwrap();
    }
}

fn put_provider(zk: &MemoryZk, tenant: &str, project: &str, name: &str, connection: &str) {
    let canonical = format!("{project}/{name}");
    let config = json!({
        "name": name,
        "connection": connection,
        "region": "us-east-1",
        "launch-timeout": 600,
        "launch-attempts": 2,
        "resource-limits": {"instances": 10},
        "flavors": [{"name": "dedicated", "dedicated-host": true}],
        "images": [{
            "name": "debian",
            "type": "cloud",
            "image-id": "ami-1e749f67",
            "username": "debian"
        }],
        "labels": [{
            "name": "debian-normal",
            "image": "debian",
            "flavor": "dedicated",
            "config_hash": "abc",
            "project_canonical_name": "gerrit.example.com/org/project",
            "min-ready": 1,
            "host-key-checking": false
        }]
    });
    let payload = json!({
        "tenant_name": tenant,
        "canonical_name": canonical,
        "connection_name": connection,
        "config": config,
    });
    let raw = codec::encode_polymorphic("aws", &payload);
    let path = format!("/zuul/tenant/{tenant}/provider/{project}/{name}/config");
    zk.ensure_path(&path).unwrap();
    zk.create(&format!("{path}/"), &raw, CreateMode::PersistentSequential)
        .unwrap();
}

#[test]
fn reload_reads_layout_lock_and_provider_shards() {
    let zk = MemoryZk::new();
    let tenant = "tenant one";
    put_layout(&zk, tenant, "layout-1");
    put_provider(&zk, tenant, "org%2Fproject", "aws-main", "aws");
    put_provider(&zk, tenant, "org%2Fproject", "other-main", "other");

    let mut launcher = Launcher::new("launcher-a.example.com", Arc::new(zk.clone()));
    launcher.connection_filter = Some(vec!["aws".into()]);
    let extra = Arc::new(MemoryProvider::static_like("hand", "debian"));
    launcher.add_provider("hand-added", extra);
    assert_eq!(launcher.state, "stopped");

    launcher.run_once();

    assert_eq!(launcher.state, "running");
    let state = &launcher.local_layout_state[tenant];
    assert_eq!(state.uuid.as_deref(), Some("layout-1"));
    assert_eq!(state.ltime > 0, true);
    assert_eq!(state.min_ltimes.as_ref().and_then(|v| v.get("gerrit")).and_then(|v| v.as_i64()), Some(5));
    assert!(launcher.local_layout_state.get("hand-added").is_none());
    assert!(launcher.tenant_providers.contains_key("hand-added"));

    let providers = &launcher.tenant_providers[tenant];
    assert_eq!(providers.len(), 2, "connection_filter keeps every provider");
    assert!(launcher.connection_allowed("aws"));
    assert!(!launcher.connection_allowed("other"));
    let aws = providers.iter().find(|p| p.name() == "aws-main").unwrap();
    assert_eq!(aws.connection_name(), "aws");
    assert_eq!(aws.launch_attempts(), 2);
    assert_eq!(aws.resource_limits().get("instances"), Some(&10.0));
    let label = &aws.labels()["debian-normal"];
    assert_eq!(label.flavor.as_deref(), Some("dedicated"));
    assert_eq!(label.min_ready, Some(1));
    assert!(!label.host_key_checking);
    assert_eq!(label.username.as_deref(), Some("debian"));
    assert_eq!(
        label.canonical_name,
        format!(
            "{}/{}",
            quote_plus("gerrit.example.com/org/project"),
            quote_plus("debian-normal")
        )
    );
    assert_eq!(
        aws.flavors()["dedicated"].raw["dedicated-host"],
        json!(true)
    );

    let lock_root = format!("/zuul/locks/tenant/{}", quote_plus(tenant));
    assert!(zk.exists(&lock_root).unwrap().is_some());
    assert!(
        zk.get_children(&lock_root).unwrap().is_empty(),
        "read lock is released"
    );
    assert!(zk
        .exists(&format!("/zuul/locks/tenant/{tenant}"))
        .unwrap()
        .is_none());

    // Same uuid is not a reload, so the provider list is not duplicated.
    launcher.run_once();
    assert_eq!(launcher.tenant_providers[tenant].len(), 2);
    assert_eq!(launcher.layout_generation, 1);

    put_layout(&zk, tenant, "layout-2");
    launcher.run_once();
    assert_eq!(
        launcher.local_layout_state[tenant].uuid.as_deref(),
        Some("layout-2")
    );
    assert_eq!(launcher.tenant_providers[tenant].len(), 2);
    assert_eq!(launcher.state, "running");

    zk.delete(&format!("/zuul/layout/{tenant}"), false).unwrap();
    launcher.run_once();
    assert!(launcher.local_layout_state.get(tenant).is_none());
    assert!(launcher.tenant_providers.get(tenant).is_none());
    assert!(launcher.tenant_providers.contains_key("hand-added"));
    assert_eq!(launcher.state, "running");
}
