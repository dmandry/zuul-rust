use crate::workers::{native_cleanup_pass, CleanupCtx, CleanupNode, EndpointRef, LeakBook};
use crate::zk::ComponentInfo;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

fn reset_cloud() {
    let mut reg = registry().lock().unwrap();
    reg.providers.clear();
    reg.aws.clear();
    reg.ready.clear();
    reg.templates.clear();
    reg.unmanaged.clear();
    reg.ports.clear();
    reg.extras.clear();
    reg.os_tokens.clear();
    reg.azure_tokens.clear();
    reg.futures.clear();
    reg.label_orders.clear();
    drop(reg);
    clear_ttl_cache();
}

fn cleanup_ctx(
    hostname: &str,
    system_id: &str,
    name: &str,
    canonical: &str,
    connection: &str,
    nodes: Vec<CleanupNode>,
    uploads: Vec<String>,
) -> CleanupCtx {
    CleanupCtx {
        hostname: hostname.into(),
        system_id: system_id.into(),
        components: vec![ComponentInfo {
            hostname: hostname.into(),
            state: "running".into(),
            connection_filter: None,
        }],
        endpoints: vec![EndpointRef {
            name: name.into(),
            canonical_name: canonical.into(),
            connection_name: connection.into(),
        }],
        nodes,
        upload_ids: uploads,
    }
}

fn node(uuid: &str, endpoint: &str, required: &[&str]) -> CleanupNode {
    CleanupNode {
        uuid: uuid.into(),
        endpoint_name: endpoint.into(),
        required: required.iter().map(|s| (*s).to_string()).collect(),
    }
}

#[test]
fn aws_leak_scan_skips_other_system_and_live_nodes() {
    let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
    reset_cloud();
    set_system_id("sys-1");
    let hits = Arc::new(StdMutex::new(Vec::<String>::new()));
    let hits_bg = hits.clone();
    let port = serve(move |_port, req| {
        let line = req.lines().next().unwrap_or("").to_string();
        if line.starts_with("DELETE ") || req.contains("Action=TerminateInstances") || req.contains("Action=DeleteVolume")
            || req.contains("Action=DeleteSnapshot") || req.contains("Action=ReleaseHosts")
            || req.contains("Action=DeregisterImage") || req.contains("Action=DeleteLaunchTemplate")
        {
            hits_bg.lock().unwrap().push(req.to_string());
        }
        if req.contains("Action=DescribeInstanceTypes") {
            return json_ok(concat!(
                r#"<?xml version="1.0"?><DescribeInstanceTypesResponse><instanceTypeSet><item>"#,
                r#"<vCpuInfo><defaultCores>1</defaultCores><defaultVCpus>2</defaultVCpus></vCpuInfo>"#,
                r#"<memoryInfo><sizeInMiB>2048</sizeInMiB></memoryInfo>"#,
                r#"</item></instanceTypeSet></DescribeInstanceTypesResponse>"#,
            ));
        }
        if req.contains("Action=DescribeInstances") {
            return json_ok(concat!(
                r#"<?xml version="1.0"?><DescribeInstancesResponse><reservationSet><item><instancesSet>"#,
                r#"<item><instanceId>i-leaked</instanceId><instanceType>t3.small</instanceType><instanceState><name>running</name></instanceState><blockDeviceMapping><item><ebs><volumeId>vol-none</volumeId></ebs></item></blockDeviceMapping><tagSet><item><key>zuul_system_id</key><value>sys-1</value></item><item><key>zuul_node_uuid</key><value>gone</value></item></tagSet></item>"#,
                r#"<item><instanceId>i-other</instanceId><instanceType>t3.small</instanceType><instanceState><name>running</name></instanceState><blockDeviceMapping><item><ebs><volumeId>vol-none</volumeId></ebs></item></blockDeviceMapping><tagSet><item><key>zuul_system_id</key><value>other</value></item><item><key>zuul_node_uuid</key><value>gone</value></item></tagSet></item>"#,
                r#"<item><instanceId>i-live</instanceId><instanceType>t3.small</instanceType><instanceState><name>running</name></instanceState><blockDeviceMapping><item><ebs><volumeId>vol-none</volumeId></ebs></item></blockDeviceMapping><tagSet><item><key>zuul_system_id</key><value>sys-1</value></item><item><key>zuul_node_uuid</key><value>live-node</value></item></tagSet></item>"#,
                r#"<item><instanceId>i-term</instanceId><instanceType>t3.small</instanceType><instanceState><name>terminated</name></instanceState><blockDeviceMapping><item><ebs><volumeId>vol-none</volumeId></ebs></item></blockDeviceMapping><tagSet><item><key>zuul_system_id</key><value>sys-1</value></item><item><key>zuul_node_uuid</key><value>gone</value></item></tagSet></item>"#,
                r#"</instancesSet></item></reservationSet></DescribeInstancesResponse>"#,
            ));
        }
        if req.contains("Action=DescribeVolumes") {
            return json_ok(concat!(
                r#"<?xml version="1.0"?><DescribeVolumesResponse><volumeSet>"#,
                r#"<item><volumeId>vol-expired</volumeId><status>available</status><tagSet><item><key>zuul_system_id</key><value>sys-1</value></item><item><key>zuul_expiration</key><value>1</value></item></tagSet></item>"#,
                r#"<item><volumeId>vol-future</volumeId><status>available</status><tagSet><item><key>zuul_system_id</key><value>sys-1</value></item><item><key>zuul_expiration</key><value>9999999999</value></item></tagSet></item>"#,
                r#"<item><volumeId>vol-live</volumeId><status>in-use</status><tagSet><item><key>zuul_system_id</key><value>sys-1</value></item><item><key>zuul_node_uuid</key><value>live-node</value></item></tagSet></item>"#,
                r#"<item><volumeId>vol-deleted</volumeId><status>deleted</status><tagSet><item><key>zuul_system_id</key><value>sys-1</value></item><item><key>zuul_node_uuid</key><value>gone</value></item></tagSet></item>"#,
                r#"</volumeSet></DescribeVolumesResponse>"#,
            ));
        }
        if req.contains("Action=DescribeSnapshots") {
            return json_ok(concat!(
                r#"<?xml version="1.0"?><DescribeSnapshotsResponse><snapshotSet>"#,
                r#"<item><snapshotId>snap-leaked</snapshotId><status>completed</status><tagSet><item><key>zuul_system_id</key><value>sys-1</value></item><item><key>zuul_upload_uuid</key><value>missing-upload</value></item></tagSet></item>"#,
                r#"<item><snapshotId>snap-kept</snapshotId><status>completed</status><tagSet><item><key>zuul_system_id</key><value>sys-1</value></item><item><key>zuul_upload_uuid</key><value>up-live</value></item></tagSet></item>"#,
                r#"</snapshotSet></DescribeSnapshotsResponse>"#,
            ));
        }
        if req.contains("Action=DescribeHosts") {
            return json_ok(concat!(
                r#"<?xml version="1.0"?><DescribeHostsResponse><hostSet>"#,
                r#"<item><hostId>h-leaked</hostId><state>available</state><tagSet><item><key>zuul_system_id</key><value>sys-1</value></item><item><key>zuul_node_uuid</key><value>gone-host</value></item></tagSet></item>"#,
                r#"<item><hostId>h-released</hostId><state>released</state><tagSet><item><key>zuul_system_id</key><value>sys-1</value></item><item><key>zuul_node_uuid</key><value>gone-host</value></item></tagSet></item>"#,
                r#"</hostSet></DescribeHostsResponse>"#,
            ));
        }
        if req.contains("Action=DescribeImages") {
            return json_ok(concat!(
                r#"<?xml version="1.0"?><DescribeImagesResponse><imagesSet>"#,
                r#"<item><imageId>ami-kept</imageId><imageState>available</imageState><tagSet><item><key>zuul_system_id</key><value>sys-1</value></item><item><key>zuul_upload_uuid</key><value>up-live</value></item></tagSet></item>"#,
                r#"</imagesSet></DescribeImagesResponse>"#,
            ));
        }
        if line.contains("GET /") && line.contains("list-type=2") {
            return json_ok(concat!(
                r#"<?xml version="1.0"?><ListBucketResult>"#,
                r#"<Contents><Key>leaked.raw</Key></Contents>"#,
                r#"<Contents><Key>kept.raw</Key></Contents>"#,
                r#"<Contents><Key>other.raw</Key></Contents>"#,
                r#"</ListBucketResult>"#,
            ));
        }
        if line.contains("GET /") && line.contains("tagging") {
            let body = if line.contains("leaked.raw") {
                r#"<?xml version="1.0"?><Tagging><TagSet><Tag><Key>zuul_system_id</Key><Value>sys-1</Value></Tag><Tag><Key>zuul_upload_uuid</Key><Value>missing-upload</Value></Tag></TagSet></Tagging>"#
            } else if line.contains("other.raw") {
                r#"<?xml version="1.0"?><Tagging><TagSet><Tag><Key>zuul_system_id</Key><Value>other</Value></Tag><Tag><Key>zuul_upload_uuid</Key><Value>missing-upload</Value></Tag></TagSet></Tagging>"#
            } else {
                r#"<?xml version="1.0"?><Tagging><TagSet><Tag><Key>zuul_system_id</Key><Value>sys-1</Value></Tag><Tag><Key>zuul_upload_uuid</Key><Value>up-live</Value></Tag></TagSet></Tagging>"#
            };
            return json_ok(body);
        }
        json_ok(r#"<?xml version="1.0"?><Response></Response>"#)
    });
    aws_provider(
        "tenant/aws",
        "aws-leak",
        port,
        json!({"name": "debian", "flavor": "small", "image": "img"}),
        json!({"instance-type": "t3.small"}),
        json!({"name": "img"}),
    );
    testing_set_extra("tenant/aws", Some("bucket"), None, false);
    post_config();
    let ctx = cleanup_ctx(
        "launcher",
        "sys-1",
        "us-east-1",
        "aws-leak/us-east-1",
        "aws-leak",
        vec![node("live-node", "us-east-1", &[])],
        vec!["up-live".into()],
    );
    let mut book = LeakBook::default();
    let first = native_cleanup_pass(&mut book, &ctx);
    let first_blob = hits.lock().unwrap().join("\n");
    assert!(first_blob.contains("vol-expired"), "{first_blob}");
    assert!(!first_blob.contains("i-leaked"), "{first_blob}");
    assert!(!first_blob.contains("i-other"), "{first_blob}");
    assert!(!first_blob.contains("i-live"), "{first_blob}");
    assert!(!first_blob.contains("i-term"), "{first_blob}");
    assert!(!first_blob.contains("vol-future"), "{first_blob}");
    assert!(!first_blob.contains("vol-live"), "{first_blob}");
    assert!(!first_blob.contains("vol-deleted"), "{first_blob}");
    assert!(!first_blob.contains("snap-leaked"), "{first_blob}");
    assert!(!first_blob.contains("h-leaked"), "{first_blob}");
    assert!(!first_blob.contains("h-released"), "{first_blob}");
    assert!(!first_blob.contains("leaked.raw"), "{first_blob}");
    assert!(first.iter().any(|ev| ev.kind == "g" && ev.name.contains("leaked.volume")));
    assert!(first.iter().any(|ev| ev.kind == "g" && ev.name.contains("unmanaged.instance")));
    hits.lock().unwrap().clear();
    let second = native_cleanup_pass(&mut book, &ctx);
    let blob = hits.lock().unwrap().join("\n");
    assert!(blob.contains("Action=TerminateInstances") && blob.contains("i-leaked"), "{blob}");
    assert!(blob.contains("snap-leaked"), "{blob}");
    assert!(blob.contains("h-leaked") && blob.contains("Action=ReleaseHosts"), "{blob}");
    assert!(blob.contains("DELETE ") && blob.contains("leaked.raw"), "{blob}");
    assert!(!blob.contains("i-other") && !blob.contains("i-live") && !blob.contains("i-term"), "{blob}");
    assert!(!blob.contains("vol-future") && !blob.contains("vol-live"), "{blob}");
    assert!(!blob.contains("snap-kept") && !blob.contains("kept.raw") && !blob.contains("other.raw"), "{blob}");
    assert!(!blob.contains("h-released") && !blob.contains("ami-kept"), "{blob}");
    assert!(second.iter().any(|ev| ev.kind == "c" && ev.name == "zuul.provider_endpoint.aws-leak_us-east-1.leaked.instance"));
    assert!(second.iter().any(|ev| ev.kind == "c" && ev.name.contains("leaked.snapshot")));
    assert!(second.iter().any(|ev| ev.kind == "c" && ev.name.contains("leaked.object")));
    assert!(second.iter().any(|ev| ev.kind == "c" && ev.name.contains("leaked.host")));
}

#[test]
fn launch_templates_are_created_at_startup_and_unused_are_deleted() {
    let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
    reset_cloud();
    set_system_id("sys-1");
    let created = Arc::new(StdMutex::new(Vec::<String>::new()));
    let created_bg = created.clone();
    let deletes = Arc::new(StdMutex::new(Vec::<String>::new()));
    let deletes_bg = deletes.clone();
    let port = serve(move |_port, req| {
        if req.contains("Action=CreateLaunchTemplate") {
            let name = req.split("LaunchTemplateName=").nth(1).unwrap_or("").split('&').next().unwrap_or("").to_string();
            created_bg.lock().unwrap().push(name);
            return json_ok(r#"<?xml version="1.0"?><CreateLaunchTemplateResponse></CreateLaunchTemplateResponse>"#);
        }
        if req.contains("Action=DeleteLaunchTemplate") {
            deletes_bg.lock().unwrap().push(req.to_string());
            return json_ok(r#"<?xml version="1.0"?><DeleteLaunchTemplateResponse></DeleteLaunchTemplateResponse>"#);
        }
        if req.contains("Action=DescribeLaunchTemplates") {
            let names = created_bg.lock().unwrap().clone();
            let mut items = String::new();
            for name in &names {
                items.push_str(&format!(
                    r#"<item><launchTemplateName>{name}</launchTemplateName><tagSet><item><key>zuul_system_id</key><value>sys-1</value></item><item><key>zuul_provider_name</key><value>tenant/fleet</value></item></tagSet></item>"#
                ));
            }
            items.push_str(r#"<item><launchTemplateName>zuul-launch-template-unused</launchTemplateName><tagSet><item><key>zuul_system_id</key><value>sys-1</value></item><item><key>zuul_provider_name</key><value>tenant/fleet</value></item></tagSet></item>"#);
            items.push_str(r#"<item><launchTemplateName>zuul-launch-template-foreign</launchTemplateName><tagSet><item><key>zuul_system_id</key><value>other</value></item></tagSet></item>"#);
            return json_ok(&format!(
                r#"<?xml version="1.0"?><DescribeLaunchTemplatesResponse><launchTemplateSet>{items}</launchTemplateSet></DescribeLaunchTemplatesResponse>"#
            ));
        }
        json_ok(r#"<?xml version="1.0"?><Response></Response>"#)
    });
    let mut options = BTreeMap::new();
    options.insert("access_key_id".into(), "AKID".into());
    options.insert("secret_access_key".into(), "SECRET".into());
    options.insert("endpoint_url".into(), format!("http://127.0.0.1:{port}"));
    options.insert("rate".into(), "1000".into());
    set_connections(vec![ConnectionConfig {
        name: "aws-fleet".into(),
        driver: "aws".into(),
        options,
    }]);
    {
        let mut reg = registry().lock().unwrap();
        reg.providers.insert(
            "tenant/fleet".into(),
            ProviderSnap {
                driver: "aws".into(),
                connection: "aws-fleet".into(),
                region: "us-east-1".into(),
                labels: IndexMap::from([
                    ("fleet-label".into(), json!({"name": "fleet-label", "flavor": "big", "key-name": "pair"})),
                    ("plain".into(), json!({"name": "plain", "flavor": "small", "key-name": "pair"})),
                ]),
                flavors: BTreeMap::from([
                    ("big".into(), json!({"fleet": {"instance-types": ["t3.large"]}, "instance-type": "t3.large"})),
                    ("small".into(), json!({"instance-type": "t3.small"})),
                ]),
                images: BTreeMap::new(),
            },
        );
    }
    post_config();
    let names = created.lock().unwrap().clone();
    assert_eq!(names.len(), 1, "{names:?}");
    assert!(names[0].starts_with("zuul-launch-template-"), "{names:?}");
    let stored = stored_launch_template("tenant/fleet", "fleet-label").unwrap();
    assert_eq!(stored, names[0]);
    assert!(stored_launch_template("tenant/fleet", "plain").is_none());
    let ctx = cleanup_ctx(
        "launcher",
        "sys-1",
        "us-east-1",
        "aws-fleet/us-east-1",
        "aws-fleet",
        vec![node("n1", "us-east-1", &[&stored])],
        Vec::new(),
    );
    let mut book = LeakBook::default();
    native_cleanup_pass(&mut book, &ctx);
    assert!(deletes.lock().unwrap().is_empty(), "{:?}", deletes.lock().unwrap());
    let events = native_cleanup_pass(&mut book, &ctx);
    let blob = deletes.lock().unwrap().join("\n");
    assert!(blob.contains("zuul-launch-template-unused"), "{blob}");
    assert!(!blob.contains("zuul-launch-template-foreign"), "{blob}");
    assert!(!blob.contains(&stored), "{blob}");
    assert!(events.iter().any(|ev| ev.kind == "c" && ev.name.contains("leaked.launch-template")));
}

#[test]
fn openstack_ports_and_floating_ips_follow_14_3_0() {
    let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
    reset_cloud();
    set_system_id("sys-1");
    let hits = Arc::new(StdMutex::new(Vec::<String>::new()));
    let hits_bg = hits.clone();
    let port = serve(move |port, req| {
        let line = req.lines().next().unwrap_or("").to_string();
        if line.starts_with("DELETE ") {
            hits_bg.lock().unwrap().push(line.clone());
        }
        if req.contains("/auth/tokens") {
            let body = format!(
                "{{\"token\":{{\"catalog\":[{{\"type\":\"compute\",\"endpoints\":[{{\"interface\":\"public\",\"region\":\"RegionOne\",\"url\":\"http://127.0.0.1:{port}/compute\"}}]}},{{\"type\":\"network\",\"endpoints\":[{{\"interface\":\"public\",\"region\":\"RegionOne\",\"url\":\"http://127.0.0.1:{port}/network\"}}]}}]}}}}"
            );
            return (201, vec![("X-Subject-Token".into(), "tok".into())], body);
        }
        if line.contains("GET /compute/servers/detail") {
            return json_ok(concat!(
                r#"{"servers":["#,
                r#"{"id":"srv-leaked","status":"ACTIVE","flavor":{"vcpus":1,"ram":512},"metadata":{"zuul_system_id":"sys-1","zuul_node_uuid":"gone"}},"#,
                r#"{"id":"srv-live","status":"ACTIVE","flavor":{"vcpus":1,"ram":512},"metadata":{"zuul_system_id":"sys-1","zuul_node_uuid":"live"}},"#,
                r#"{"id":"srv-other","status":"ACTIVE","flavor":{"vcpus":1,"ram":512},"metadata":{"zuul_system_id":"other","zuul_node_uuid":"gone"}},"#,
                r#"{"id":"srv-dead","status":"DELETED","flavor":{"vcpus":1,"ram":512},"metadata":{"zuul_system_id":"sys-1","zuul_node_uuid":"gone"}}"#,
                r#"]}"#,
            ));
        }
        if line.contains("GET /network/v2.0/ports") {
            return json_ok(concat!(
                r#"{"ports":["#,
                r#"{"id":"p-old","status":"DOWN","device_owner":"compute:nova"},"#,
                r#"{"id":"p-router","status":"DOWN","device_owner":"network:router_interface"},"#,
                r#"{"id":"p-free","status":"DOWN","device_owner":""}"#,
                r#"]}"#,
            ));
        }
        if line.contains("GET /network/v2.0/floatingips") {
            return json_ok(concat!(
                r#"{"floatingips":["#,
                r#"{"id":"fip-free","port_id":null,"floating_ip_address":"203.0.113.5"},"#,
                r#"{"id":"fip-used","port_id":"p-live","floating_ip_address":"203.0.113.6"}"#,
                r#"]}"#,
            ));
        }
        json_ok("{}")
    });
    let dir = std::env::temp_dir().join(format!("os-leak-{}", new_id()));
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
        name: "os-leak".into(),
        driver: "openstack".into(),
        options,
    }]);
    {
        let mut reg = registry().lock().unwrap();
        reg.providers.insert(
            "tenant/os".into(),
            ProviderSnap {
                driver: "openstack".into(),
                connection: "os-leak".into(),
                region: "RegionOne".into(),
                labels: IndexMap::new(),
                flavors: BTreeMap::new(),
                images: BTreeMap::new(),
            },
        );
    }
    testing_set_extra("tenant/os", None, Some(10), true);
    let ctx = cleanup_ctx(
        "launcher",
        "sys-1",
        "RegionOne",
        "os-leak/RegionOne",
        "os-leak",
        vec![node("live", "RegionOne", &[])],
        Vec::new(),
    );
    let mut book = LeakBook::default();
    native_cleanup_pass(&mut book, &ctx);
    let first = hits.lock().unwrap().join("\n");
    assert!(first.contains("floatingips/fip-free"), "{first}");
    assert!(!first.contains("fip-used"), "{first}");
    assert!(!first.contains("/ports/"), "{first}");
    assert!(!first.contains("srv-leaked"), "{first}");
    testing_age_ports("os-leak/RegionOne", 10);
    hits.lock().unwrap().clear();
    native_cleanup_pass(&mut book, &ctx);
    let held = hits.lock().unwrap().join("\n");
    assert!(!held.contains("/ports/"), "interval boundary must not delete: {held}");
    testing_age_ports("os-leak/RegionOne", 11);
    hits.lock().unwrap().clear();
    native_cleanup_pass(&mut book, &ctx);
    let blob = hits.lock().unwrap().join("\n");
    assert!(blob.contains("/ports/p-old"), "{blob}");
    assert!(blob.contains("/ports/p-free"), "{blob}");
    assert!(!blob.contains("p-router"), "{blob}");
    assert!(blob.contains("/servers/srv-leaked"), "{blob}");
    assert!(!blob.contains("srv-live") && !blob.contains("srv-other") && !blob.contains("srv-dead"), "{blob}");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn azure_leak_scan_skips_other_system_and_live_ids() {
    let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
    reset_cloud();
    set_system_id("sys-1");
    let hits = Arc::new(StdMutex::new(Vec::<String>::new()));
    let hits_bg = hits.clone();
    let port = serve(move |_port, req| {
        let line = req.lines().next().unwrap_or("").to_string();
        if line.starts_with("DELETE ") {
            hits_bg.lock().unwrap().push(line.clone());
        }
        if req.contains("/oauth2/token") {
            return json_ok(r#"{"access_token":"tok","expires_in":"3600"}"#);
        }
        if line.contains("virtualMachines?") {
            return json_ok(concat!(
                r#"{"value":["#,
                r#"{"id":"/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/leaked","tags":{"zuul_system_id":"sys-1","zuul_node_uuid":"gone"}},"#,
                r#"{"id":"/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/live","tags":{"zuul_system_id":"sys-1","zuul_node_uuid":"live"}},"#,
                r#"{"id":"/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/other","tags":{"zuul_system_id":"other","zuul_node_uuid":"gone"}}"#,
                r#"]}"#,
            ));
        }
        if line.contains("networkInterfaces?") || line.contains("publicIPAddresses?") || line.contains("/disks?") || line.contains("/images?") {
            return json_ok(r#"{"value":[]}"#);
        }
        json_ok("{}")
    });
    set_test_azure_endpoints(&format!("http://127.0.0.1:{port}"), &format!("http://127.0.0.1:{port}"));
    let mut options = BTreeMap::new();
    options.insert("tenant_id".into(), "tenant".into());
    options.insert("client_id".into(), "client".into());
    options.insert("client_secret".into(), "secret".into());
    options.insert("subscription_id".into(), "sub".into());
    options.insert("rate".into(), "1000".into());
    set_connections(vec![ConnectionConfig {
        name: "azure-leak".into(),
        driver: "azure".into(),
        options,
    }]);
    {
        let mut reg = registry().lock().unwrap();
        reg.providers.insert(
            "tenant/azure".into(),
            ProviderSnap {
                driver: "azure".into(),
                connection: "azure-leak".into(),
                region: "eastus".into(),
                labels: IndexMap::new(),
                flavors: BTreeMap::new(),
                images: BTreeMap::new(),
            },
        );
    }
    let ctx = cleanup_ctx(
        "launcher",
        "sys-1",
        "eastus",
        "azure-leak/eastus",
        "azure-leak",
        vec![node("live", "eastus", &[])],
        Vec::new(),
    );
    let mut book = LeakBook::default();
    native_cleanup_pass(&mut book, &ctx);
    assert!(hits.lock().unwrap().is_empty(), "{:?}", hits.lock().unwrap());
    let events = native_cleanup_pass(&mut book, &ctx);
    let blob = hits.lock().unwrap().join("\n");
    assert!(blob.contains("virtualMachines/leaked"), "{blob}");
    assert!(!blob.contains("virtualMachines/live") && !blob.contains("virtualMachines/other"), "{blob}");
    assert!(events.iter().any(|ev| ev.name.contains("leaked.instance") && ev.kind == "c"));
    assert!(events.iter().any(|ev| ev.name.contains("unmanaged.instance") && ev.kind == "g"));
}

#[test]
fn kubernetes_namespace_leak_uses_labels() {
    let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
    reset_cloud();
    set_system_id("sys-1");
    let hits = Arc::new(StdMutex::new(Vec::<String>::new()));
    let hits_bg = hits.clone();
    let port = serve(move |_port, req| {
        let line = req.lines().next().unwrap_or("").to_string();
        if line.starts_with("DELETE ") {
            hits_bg.lock().unwrap().push(line.clone());
        }
        if line.contains("GET /api/v1/namespaces") {
            return json_ok(concat!(
                r#"{"items":["#,
                r#"{"metadata":{"name":"leaked","labels":{"zuul_system_id":"sys-1","zuul_node_uuid":"gone"}}},"#,
                r#"{"metadata":{"name":"live","labels":{"zuul_system_id":"sys-1","zuul_node_uuid":"live"}}},"#,
                r#"{"metadata":{"name":"other","labels":{"zuul_system_id":"other","zuul_node_uuid":"gone"}}}"#,
                r#"]}"#,
            ));
        }
        json_ok("{}")
    });
    let dir = std::env::temp_dir().join(format!("k8s-leak-{}", new_id()));
    std::fs::create_dir_all(&dir).unwrap();
    let kube = dir.join("kube");
    std::fs::write(
        &kube,
        format!(
            "apiVersion: v1\nkind: Config\ncurrent-context: c\ncontexts:\n- name: c\n  context:\n    cluster: cl\n    user: u\nclusters:\n- name: cl\n  cluster:\n    server: http://127.0.0.1:{port}\nusers:\n- name: u\n  user:\n    token: tok\n"
        ),
    )
    .unwrap();
    let mut options = BTreeMap::new();
    options.insert("kubeconfig_file".into(), kube.display().to_string());
    options.insert("rate".into(), "1000".into());
    set_connections(vec![ConnectionConfig {
        name: "k8s-leak".into(),
        driver: "kubernetes".into(),
        options,
    }]);
    {
        let mut reg = registry().lock().unwrap();
        reg.providers.insert(
            "tenant/k8s".into(),
            ProviderSnap {
                driver: "kubernetes".into(),
                connection: "k8s-leak".into(),
                region: String::new(),
                labels: IndexMap::new(),
                flavors: BTreeMap::new(),
                images: BTreeMap::new(),
            },
        );
    }
    let ctx = cleanup_ctx(
        "launcher",
        "sys-1",
        "k8s-leak",
        "k8s-leak/k8s-leak",
        "k8s-leak",
        vec![node("live", "k8s-leak", &[])],
        Vec::new(),
    );
    let mut book = LeakBook::default();
    native_cleanup_pass(&mut book, &ctx);
    assert!(hits.lock().unwrap().is_empty(), "{:?}", hits.lock().unwrap());
    native_cleanup_pass(&mut book, &ctx);
    let blob = hits.lock().unwrap().join("\n");
    assert!(blob.contains("/api/v1/namespaces/leaked"), "{blob}");
    assert!(!blob.contains("/namespaces/live") && !blob.contains("/namespaces/other"), "{blob}");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn import_reuses_tagged_task_and_retries_resource_count_limit() {
    let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
    reset_cloud();
    set_system_id("sys-1");
    let imports = Arc::new(AtomicUsize::new(0));
    let imports_bg = imports.clone();
    let saw_import = Arc::new(AtomicUsize::new(0));
    let saw_bg = saw_import.clone();
    let port = serve(move |_port, req| {
        if req.contains("Action=ImportSnapshot") {
            let n = imports_bg.fetch_add(1, AtomicOrdering::SeqCst);
            if n == 0 {
                return (
                    400,
                    Vec::new(),
                    r#"<?xml version="1.0"?><Response><Errors><Error><Code>ResourceCountLimitExceeded</Code><Message>limit</Message></Error></Errors></Response>"#.into(),
                );
            }
            return json_ok(r#"<?xml version="1.0"?><ImportSnapshotResponse><importTaskId>import-new</importTaskId></ImportSnapshotResponse>"#);
        }
        if req.contains("Action=DescribeImportSnapshotTasks") {
            saw_bg.fetch_add(1, AtomicOrdering::SeqCst);
            if req.contains("ImportTaskIds") {
                return json_ok(r#"<?xml version="1.0"?><DescribeImportSnapshotTasksResponse><importSnapshotTaskSet><item><importTaskId>import-new</importTaskId><snapshotTaskDetail><status>completed</status><snapshotId>snap-new</snapshotId></snapshotTaskDetail></item></importSnapshotTaskSet></DescribeImportSnapshotTasksResponse>"#);
            }
            return json_ok(r#"<?xml version="1.0"?><DescribeImportSnapshotTasksResponse><importSnapshotTaskSet></importSnapshotTaskSet></DescribeImportSnapshotTasksResponse>"#);
        }
        if req.contains("Action=RegisterImage") || req.contains("Action=CreateTags") || req.contains("Action=DeleteSnapshot") {
            return json_ok(r#"<?xml version="1.0"?><Response><imageId>ami-imp</imageId></Response>"#);
        }
        json_ok(r#"<?xml version="1.0"?><Response></Response>"#)
    });
    aws_provider(
        "tenant/imp",
        "aws-imp",
        port,
        json!({"name": "debian"}),
        json!({}),
        json!({"name": "img", "import-timeout": 300}),
    );
    let err = upload_sdk_run(&json!({
        "provider": "tenant/imp",
        "kind": "import",
        "upload_uuid": "up-1",
        "image_name": "img",
        "image_format": "raw",
        "bucket": "bucket",
        "object_key": "obj.raw",
    }));
    assert!(err.is_ok(), "{err:?}");
    assert!(imports.load(AtomicOrdering::SeqCst) >= 2, "limit must be retried");

    reset_cloud();
    set_system_id("sys-1");
    let reused = Arc::new(AtomicUsize::new(0));
    let reused_bg = reused.clone();
    let port = serve(move |_port, req| {
        if req.contains("Action=ImportSnapshot") {
            reused_bg.fetch_add(1, AtomicOrdering::SeqCst);
            return json_ok(r#"<?xml version="1.0"?><ImportSnapshotResponse><importTaskId>should-not</importTaskId></ImportSnapshotResponse>"#);
        }
        if req.contains("Action=DescribeImportSnapshotTasks") {
            return json_ok(concat!(
                r#"<?xml version="1.0"?><DescribeImportSnapshotTasksResponse><importSnapshotTaskSet><item>"#,
                r#"<importTaskId>import-existing</importTaskId>"#,
                r#"<snapshotTaskDetail><status>completed</status><snapshotId>snap-old</snapshotId></snapshotTaskDetail>"#,
                r#"<tagSet><item><key>zuul_system_id</key><value>sys-1</value></item><item><key>zuul_upload_uuid</key><value>up-9</value></item><item><key>extra</key><value>1</value></item></tagSet>"#,
                r#"</item></importSnapshotTaskSet></DescribeImportSnapshotTasksResponse>"#,
            ));
        }
        if req.contains("Action=RegisterImage") || req.contains("Action=CreateTags") {
            return json_ok(r#"<?xml version="1.0"?><Response><imageId>ami-old</imageId></Response>"#);
        }
        json_ok(r#"<?xml version="1.0"?><Response></Response>"#)
    });
    aws_provider(
        "tenant/reuse",
        "aws-reuse",
        port,
        json!({"name": "debian"}),
        json!({}),
        json!({"name": "img"}),
    );
    let done = upload_sdk_run(&json!({
        "provider": "tenant/reuse",
        "kind": "import",
        "upload_uuid": "up-9",
        "image_name": "img",
        "image_format": "raw",
        "bucket": "bucket",
        "object_key": "obj.raw",
    }))
    .unwrap();
    assert_eq!(done["external_id"], "ami-old");
    assert_eq!(reused.load(AtomicOrdering::SeqCst), 0);
}

#[test]
fn ebs_direct_upload_registers_the_snapshot() {
    let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
    reset_cloud();
    let saw = Arc::new(StdMutex::new(Vec::<String>::new()));
    let saw_bg = saw.clone();
    let port = serve(move |_port, req| {
        saw_bg.lock().unwrap().push(req.lines().next().unwrap_or("").to_string() + "\n" + req);
        if req.contains("POST ") && req.contains("/snapshots") && !req.contains("completion") && !req.contains("Action=") {
            return json_ok(r#"{"SnapshotId":"snap-ebs"}"#);
        }
        if req.contains("/blocks/") {
            return json_ok(r#"{}"#);
        }
        if req.contains("/snapshots/completion/") {
            return json_ok(r#"{"Status":"completed"}"#);
        }
        if req.contains("Action=RegisterImage") {
            assert!(req.contains("SnapshotId=snap-ebs") || req.contains("SnapshotId"), "{req}");
            assert!(req.contains("DeviceName=%2Fdev%2Fsda1") || req.contains("DeviceName=/dev/sda1") || req.contains("sda1"), "{req}");
            return json_ok(r#"<?xml version="1.0"?><RegisterImageResponse><imageId>ami-ebs</imageId></RegisterImageResponse>"#);
        }
        json_ok(r#"<?xml version="1.0"?><Response></Response>"#)
    });
    aws_provider(
        "tenant/ebs",
        "aws-ebs",
        port,
        json!({"name": "debian"}),
        json!({}),
        json!({"name": "img", "import-method": "ebs-direct"}),
    );
    let path = std::env::temp_dir().join(format!("ebs-{}", new_id()));
    std::fs::write(&path, b"raw-image").unwrap();
    let done = upload_sdk_run(&json!({
        "provider": "tenant/ebs",
        "kind": "upload",
        "upload_uuid": "up-ebs",
        "image_name": "img",
        "image_format": "raw",
        "path": path,
    }))
    .unwrap();
    assert_eq!(done["external_id"], "ami-ebs");
    let blob = saw.lock().unwrap().join("\n");
    assert!(blob.contains("PUT ") && blob.contains("/blocks/0"), "{blob}");
    assert!(blob.to_ascii_lowercase().contains("x-amz-checksum-algorithm: sha256"), "{blob}");
    assert!(blob.contains("snap-ebs"), "{blob}");
    let _ = std::fs::remove_file(path);
}

#[test]
fn azure_sas_page_upload_builds_a_managed_image() {
    let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
    reset_cloud();
    let saw = Arc::new(StdMutex::new(Vec::<String>::new()));
    let saw_bg = saw.clone();
    let port = serve(move |port, req| {
        saw_bg.lock().unwrap().push(req.to_string());
        if req.contains("/oauth2/token") {
            return json_ok(r#"{"access_token":"tok","expires_in":"3600"}"#);
        }
        if req.contains("beginGetAccess") {
            return json_ok(&format!(
                r#"{{"properties":{{"output":{{"accessSAS":"http://127.0.0.1:{port}/sas?sig=1"}}}}}}"#
            ));
        }
        if req.contains("comp=page") {
            assert!(req.contains("x-ms-page-write: Update"), "{req}");
            assert!(req.contains("Range: bytes=0-"), "{req}");
            return json_ok("");
        }
        if req.contains("PUT ") && req.contains("/images/") {
            assert!(req.contains("Generalized"), "{req}");
            assert!(req.contains("zoneResilient"), "{req}");
            return json_ok(r#"{"id":"/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/images/img-up"}"#);
        }
        if req.contains("PUT ") && req.contains("/disks/") {
            assert!(req.contains("Upload"), "{req}");
            return json_ok(r#"{"id":"/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/disks/img-up"}"#);
        }
        json_ok("{}")
    });
    set_test_azure_endpoints(&format!("http://127.0.0.1:{port}"), &format!("http://127.0.0.1:{port}"));
    let mut options = BTreeMap::new();
    options.insert("tenant_id".into(), "tenant".into());
    options.insert("client_id".into(), "client".into());
    options.insert("client_secret".into(), "secret".into());
    options.insert("subscription_id".into(), "sub".into());
    options.insert("rate".into(), "1000".into());
    set_connections(vec![ConnectionConfig {
        name: "azure-up".into(),
        driver: "azure".into(),
        options,
    }]);
    {
        let mut reg = registry().lock().unwrap();
        reg.providers.insert(
            "tenant/azure-up".into(),
            ProviderSnap {
                driver: "azure".into(),
                connection: "azure-up".into(),
                region: "eastus".into(),
                labels: IndexMap::new(),
                flavors: BTreeMap::new(),
                images: BTreeMap::from([("img".into(), json!({"name": "img"}))]),
            },
        );
    }
    let path = std::env::temp_dir().join(format!("az-{}", new_id()));
    std::fs::write(&path, b"page-bytes").unwrap();
    let done = upload_sdk_run(&json!({
        "provider": "tenant/azure-up",
        "kind": "upload",
        "upload_uuid": "up-az",
        "image_name": "img-up",
        "image_format": "vhd",
        "path": path,
        "resource_group": "rg",
    }))
    .unwrap();
    assert!(done["external_id"].as_str().unwrap_or("").contains("/images/"), "{done}");
    let blob = saw.lock().unwrap().join("\n");
    assert!(blob.contains("comp=page"), "{blob}");
    assert!(blob.contains("DELETE ") && blob.contains("/disks/"), "{blob}");
    let _ = std::fs::remove_file(path);
}

#[test]
fn glance_upload_posts_the_image_then_the_file() {
    let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
    reset_cloud();
    let saw = Arc::new(StdMutex::new(Vec::<String>::new()));
    let saw_bg = saw.clone();
    let port = serve(move |port, req| {
        saw_bg.lock().unwrap().push(req.to_string());
        if req.contains("/auth/tokens") {
            let body = format!(
                "{{\"token\":{{\"catalog\":[{{\"type\":\"image\",\"endpoints\":[{{\"interface\":\"public\",\"region\":\"RegionOne\",\"url\":\"http://127.0.0.1:{port}/image\"}}]}}]}}}}"
            );
            return (201, vec![("X-Subject-Token".into(), "tok".into())], body);
        }
        if req.contains("POST ") && req.contains("/v2/images") && !req.contains("/file") {
            assert!(req.contains("\"disk_format\":\"raw\""), "{req}");
            assert!(req.contains("\"name\":\"img-"), "{req}");
            return (201, Vec::new(), r#"{"id":"glance-1"}"#.into());
        }
        if req.contains("PUT ") && req.contains("/v2/images/glance-1/file") {
            assert!(req.contains("application/octet-stream"), "{req}");
            assert!(req.contains("glance-bytes"), "{req}");
            return json_ok("");
        }
        json_ok("{}")
    });
    let dir = std::env::temp_dir().join(format!("glance-{}", new_id()));
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
        name: "os-up".into(),
        driver: "openstack".into(),
        options,
    }]);
    {
        let mut reg = registry().lock().unwrap();
        reg.providers.insert(
            "tenant/glance".into(),
            ProviderSnap {
                driver: "openstack".into(),
                connection: "os-up".into(),
                region: "RegionOne".into(),
                labels: IndexMap::new(),
                flavors: BTreeMap::new(),
                images: BTreeMap::from([("img".into(), json!({"name": "img"}))]),
            },
        );
    }
    let path = std::env::temp_dir().join(format!("glance-file-{}", new_id()));
    std::fs::write(&path, b"glance-bytes").unwrap();
    let done = upload_sdk_run(&json!({
        "provider": "tenant/glance",
        "kind": "upload",
        "upload_uuid": "up-gl",
        "image_name": "img",
        "image_format": "raw",
        "path": &path,
    }))
    .unwrap();
    assert_eq!(done["external_id"], "glance-1");
    let _ = std::fs::remove_dir_all(dir);
    let _ = std::fs::remove_file(path);
    let _ = saw;
}

#[test]
fn copy_image_polls_until_available() {
    let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
    reset_cloud();
    let port = serve(|_port, req| {
        if req.contains("Action=CopyImage") {
            assert!(req.contains("SourceImageId=ami-src"), "{req}");
            assert!(req.contains("SourceRegion=us-west-2"), "{req}");
            assert!(req.contains("CopyImageTags=false"), "{req}");
            return json_ok(r#"<?xml version="1.0"?><CopyImageResponse><imageId>ami-copy</imageId></CopyImageResponse>"#);
        }
        if req.contains("Action=DescribeImages") {
            return json_ok(r#"<?xml version="1.0"?><DescribeImagesResponse><imagesSet><item><imageId>ami-copy</imageId><imageState>available</imageState></item></imagesSet></DescribeImagesResponse>"#);
        }
        json_ok(r#"<?xml version="1.0"?><Response></Response>"#)
    });
    aws_provider(
        "tenant/copy",
        "aws-copy",
        port,
        json!({"name": "debian"}),
        json!({}),
        json!({"name": "img"}),
    );
    let done = upload_sdk_run(&json!({
        "provider": "tenant/copy",
        "kind": "copy",
        "upload_uuid": "up-copy",
        "image_name": "img",
        "source_external_id": "ami-src",
        "source_region": "us-west-2",
    }))
    .unwrap();
    assert_eq!(done["external_id"], "ami-copy");
}

#[test]
fn snapshot_znode_matches_python_provider_node_snapshot() {
    let mut sm = crate::provider::AwsSnapshot::new();
    sm.state = "complete".into();
    sm.start_time = 1700000000.5;
    sm.snapshot_id = Some("snap-1".into());
    sm.complete = true;
    sm.external_id = Some("arn:aws:ec2:us-east-1:123456789012:snapshot/snap-1".into());
    sm.tags = json!({"Name": "node", "zuul_expiration": "99"});
    let rust_bytes = sm.znode_bytes();
    let py = std::process::Command::new("/home/ubuntu/zuul-nox-311/bin/python")
        .env("PYTHONPATH", "/workspace")
        .arg("-c")
        .arg(r#"
import json, zlib
from zuul.model import ProviderNodeSnapshot
snap = ProviderNodeSnapshot()
class SM:
    def toDict(self):
        return {"state": "complete", "start_time": 1700000000.5, "snapshot_id": "snap-1"}
snap._set(
    state_machine=SM(),
    complete=True,
    external_id="arn:aws:ec2:us-east-1:123456789012:snapshot/snap-1",
    tags={"Name": "node", "zuul_expiration": "99"},
)
raw = snap.serialize(None)
open("/tmp/py-snap.json","wb").write(raw)
open("/tmp/py-snap.zlib","wb").write(zlib.compress(raw))
"#)
        .output()
        .expect("python");
    assert!(py.status.success(), "{}", String::from_utf8_lossy(&py.stderr));
    let py_json = std::fs::read("/tmp/py-snap.json").unwrap();
    let py_zlib = std::fs::read("/tmp/py-snap.zlib").unwrap();
    let rust_json = crate::codec::decompress(&rust_bytes);
    assert_eq!(rust_json, py_json, "json {}", String::from_utf8_lossy(&rust_json));
    assert_eq!(rust_bytes, py_zlib);
}

#[test]
fn unmanaged_quotas_match_14_3_0() {
    let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
    reset_cloud();
    set_system_id("sys-1");
    let port = serve(|_port, req| {
        if req.contains("Action=DescribeInstanceTypes") {
            assert!(req.contains("t3.medium"), "{req}");
            return json_ok(concat!(
                r#"<?xml version="1.0"?><DescribeInstanceTypesResponse><instanceTypeSet><item>"#,
                r#"<vCpuInfo><defaultCores>1</defaultCores><defaultVCpus>2</defaultVCpus></vCpuInfo>"#,
                r#"<memoryInfo><sizeInMiB>4096</sizeInMiB></memoryInfo>"#,
                r#"</item></instanceTypeSet></DescribeInstanceTypesResponse>"#,
            ));
        }
        if req.contains("Action=DescribeInstances") {
            return json_ok(concat!(
                r#"<?xml version="1.0"?><DescribeInstancesResponse><reservationSet><item><instancesSet>"#,
                r#"<item><instanceId>i-vol</instanceId><instanceType>t3.medium</instanceType><instanceState><name>running</name></instanceState>"#,
                r#"<blockDeviceMapping><item><ebs><volumeId>vol-1</volumeId></ebs></item></blockDeviceMapping>"#,
                r#"<tagSet><item><key>zuul_system_id</key><value>sys-1</value></item></tagSet></item>"#,
                r#"<item><instanceId>i-host</instanceId><instanceType>c5.metal</instanceType><instanceState><name>running</name></instanceState>"#,
                r#"<placement><hostId>h-1</hostId></placement>"#,
                r#"<blockDeviceMapping><item><ebs><volumeId>vol-missing</volumeId></ebs></item></blockDeviceMapping>"#,
                r#"<tagSet><item><key>zuul_system_id</key><value>sys-1</value></item></tagSet></item>"#,
                r#"</instancesSet></item></reservationSet></DescribeInstancesResponse>"#,
            ));
        }
        if req.contains("Action=DescribeVolumes") {
            return json_ok(concat!(
                r#"<?xml version="1.0"?><DescribeVolumesResponse><volumeSet>"#,
                r#"<item><volumeId>vol-1</volumeId><volumeType>gp3</volumeType><size>20</size><status>in-use</status></item>"#,
                r#"</volumeSet></DescribeVolumesResponse>"#,
            ));
        }
        json_ok(r#"<?xml version="1.0"?><Response></Response>"#)
    });
    aws_provider(
        "tenant/q",
        "aws-q",
        port,
        json!({"name": "debian", "flavor": "small"}),
        json!({"instance-type": "t3.medium"}),
        json!({"name": "img"}),
    );
    post_config();
    let inv = endpoint_inventory("aws-q/us-east-1");
    assert!(!inv.error, "aws inventory failed");
    let vol = inv
        .instances
        .iter()
        .find(|i| i["quota"].get("L-7A658B76").is_some())
        .unwrap_or_else(|| panic!("volume quota {:?}", inv.instances));
    assert_eq!(vol["quota"]["instances"], 1);
    assert_eq!(vol["quota"]["cores"].as_f64(), Some(1.0));
    assert_eq!(vol["quota"]["ram"].as_f64(), Some(4096.0));
    assert_eq!(vol["quota"]["L-1216C47A"].as_f64(), Some(2.0));
    assert_eq!(vol["quota"]["L-7A658B76"].as_f64(), Some(20.0));
    let host = inv
        .instances
        .iter()
        .find(|i| i["quota"].get("L-81657574").is_some())
        .expect("host quota");
    assert_eq!(host["quota"]["instances"], 1);
    assert_eq!(host["quota"]["L-81657574"], 1);
    assert!(host["quota"].get("cores").is_none());
    assert!(host["quota"].get("ram").is_none());

    reset_cloud();
    set_system_id("sys-1");
    let port = serve(move |port, req| {
        let line = req.lines().next().unwrap_or("").to_string();
        if req.contains("/auth/tokens") {
            let body = format!(
                "{{\"token\":{{\"catalog\":[{{\"type\":\"compute\",\"endpoints\":[{{\"interface\":\"public\",\"region\":\"RegionOne\",\"url\":\"http://127.0.0.1:{port}/compute\"}}]}},{{\"type\":\"volumev3\",\"endpoints\":[{{\"interface\":\"public\",\"region\":\"RegionOne\",\"url\":\"http://127.0.0.1:{port}/volume\"}}]}},{{\"type\":\"network\",\"endpoints\":[{{\"interface\":\"public\",\"region\":\"RegionOne\",\"url\":\"http://127.0.0.1:{port}/network\"}}]}}]}}}}"
            );
            return (201, vec![("X-Subject-Token".into(), "tok".into())], body);
        }
        if line.contains("GET /compute/servers/detail") {
            return json_ok(concat!(
                r#"{"servers":[{"id":"srv-1","status":"ACTIVE","flavor":{"id":"f1"},"#,
                r#""os-extended-volumes:volumes_attached":[{"id":"vol-os"}],"#,
                r#""metadata":{"zuul_system_id":"sys-1"}}]}"#,
            ));
        }
        if line.contains("GET /compute/flavors/detail") {
            return json_ok(r#"{"flavors":[{"id":"f1","name":"m1","vcpus":2,"ram":2048}]}"#);
        }
        if line.contains("GET /volume/volumes/detail") {
            return json_ok(r#"{"volumes":[{"id":"vol-os","size":10}]}"#);
        }
        if line.contains("GET /network/v2.0/ports") {
            return json_ok(r#"{"ports":[]}"#);
        }
        json_ok("{}")
    });
    let dir = std::env::temp_dir().join(format!("os-quota-{}", new_id()));
    std::fs::create_dir_all(&dir).unwrap();
    let clouds = dir.join("clouds.yaml");
    std::fs::write(
        &clouds,
        format!(
            "clouds:\n  demo:\n    region_name: RegionOne\n    auth:\n      auth_url: http://127.0.0.1:{port}/v3\n      username: demo\n      password: secret\n      project_id: proj-id\n      project_name: demo\n"
        ),
    )
    .unwrap();
    let mut options = BTreeMap::new();
    options.insert("cloud".into(), "demo".into());
    options.insert("client_config_file".into(), clouds.display().to_string());
    options.insert("rate".into(), "1000".into());
    set_connections(vec![ConnectionConfig {
        name: "os-q".into(),
        driver: "openstack".into(),
        options,
    }]);
    {
        let mut reg = registry().lock().unwrap();
        reg.providers.insert(
            "tenant/os-q".into(),
            ProviderSnap {
                driver: "openstack".into(),
                connection: "os-q".into(),
                region: "RegionOne".into(),
                labels: IndexMap::new(),
                flavors: BTreeMap::new(),
                images: BTreeMap::new(),
            },
        );
    }
    let inv = endpoint_inventory("os-q/RegionOne");
    assert!(!inv.error, "openstack inventory failed");
    let quota = &inv.instances[0]["quota"];
    assert_eq!(quota["instances"], 1);
    assert_eq!(quota["cores"], 2);
    assert_eq!(quota["ram"], 2048);
    assert_eq!(quota["volumes"], 1);
    assert_eq!(quota["volume-gb"], 10);
    let _ = std::fs::remove_dir_all(dir);

    reset_cloud();
    set_system_id("sys-1");
    let port = serve(|_port, req| {
        if req.contains("/oauth2/token") {
            return json_ok(r#"{"access_token":"tok","expires_in":"3600"}"#);
        }
        if req.contains("virtualMachines?") {
            return json_ok(concat!(
                r#"{"value":[{"id":"/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/spot","#,
                r#""location":"eastus","tags":{"zuul_system_id":"sys-1"},"#,
                r#""properties":{"priority":"Spot","hardwareProfile":{"vmSize":"Standard_B1s"}}}]}"#,
            ));
        }
        if req.contains("/skus?") {
            return json_ok(
                r#"{"value":[{"name":"Standard_B1s","locations":["eastus"],"capabilities":[{"name":"vCPUs","value":"1"},{"name":"MemoryGB","value":"1"}]}]}"#,
            );
        }
        json_ok(r#"{"value":[]}"#)
    });
    set_test_azure_endpoints(&format!("http://127.0.0.1:{port}"), &format!("http://127.0.0.1:{port}"));
    let mut options = BTreeMap::new();
    options.insert("tenant_id".into(), "tenant".into());
    options.insert("client_id".into(), "client".into());
    options.insert("client_secret".into(), "secret".into());
    options.insert("subscription_id".into(), "sub".into());
    options.insert("rate".into(), "1000".into());
    set_connections(vec![ConnectionConfig {
        name: "azure-q".into(),
        driver: "azure".into(),
        options,
    }]);
    {
        let mut reg = registry().lock().unwrap();
        reg.providers.insert(
            "tenant/azure-q".into(),
            ProviderSnap {
                driver: "azure".into(),
                connection: "azure-q".into(),
                region: "eastus".into(),
                labels: IndexMap::new(),
                flavors: BTreeMap::new(),
                images: BTreeMap::new(),
            },
        );
    }
    let inv = endpoint_inventory("azure-q/eastus");
    assert!(!inv.error, "azure inventory failed");
    let quota = &inv.instances[0]["quota"];
    assert_eq!(quota["instances"], 1);
    assert_eq!(quota["cores"], 0);
    assert_eq!(quota["spot_cores"], 1);
    assert_eq!(quota["ram"], 1024);

    reset_cloud();
    set_system_id("sys-1");
    let port = serve(|_port, req| {
        let line = req.lines().next().unwrap_or("").to_string();
        if line.contains("GET /api/v1/namespaces?") || line.contains("GET /api/v1/namespaces ") {
            return json_ok(concat!(
                r#"{"items":["#,
                r#"{"metadata":{"name":"open","labels":{"zuul_system_id":"sys-1"}}},"#,
                r#"{"metadata":{"name":"locked","labels":{"zuul_system_id":"sys-1"}}}"#,
                r#"]}"#,
            ));
        }
        if line.contains("/namespaces/locked/pods") {
            return (403, Vec::new(), r#"{"reason":"Forbidden"}"#.into());
        }
        if line.contains("/namespaces/open/pods") {
            return json_ok(r#"{"items":[{"metadata":{"name":"pod-1","labels":{"app":"one"}}}]}"#);
        }
        json_ok("{}")
    });
    let dir = std::env::temp_dir().join(format!("k8s-quota-{}", new_id()));
    std::fs::create_dir_all(&dir).unwrap();
    let kube = dir.join("kube");
    std::fs::write(
        &kube,
        format!(
            "apiVersion: v1\nkind: Config\ncurrent-context: c\ncontexts:\n- name: c\n  context:\n    cluster: cl\n    user: u\nclusters:\n- name: cl\n  cluster:\n    server: http://127.0.0.1:{port}\nusers:\n- name: u\n  user:\n    token: tok\n"
        ),
    )
    .unwrap();
    let mut options = BTreeMap::new();
    options.insert("kubeconfig_file".into(), kube.display().to_string());
    options.insert("rate".into(), "1000".into());
    set_connections(vec![ConnectionConfig {
        name: "k8s-q".into(),
        driver: "kubernetes".into(),
        options,
    }]);
    {
        let mut reg = registry().lock().unwrap();
        reg.providers.insert(
            "tenant/k8s-q".into(),
            ProviderSnap {
                driver: "kubernetes".into(),
                connection: "k8s-q".into(),
                region: String::new(),
                labels: IndexMap::new(),
                flavors: BTreeMap::new(),
                images: BTreeMap::new(),
            },
        );
    }
    let inv = endpoint_inventory("k8s-q/k8s-q");
    assert!(!inv.error, "k8s inventory failed");
    assert_eq!(inv.instances.len(), 3, "{:?}", inv.instances);
    assert_eq!(inv.instances[0]["quota"]["namespaces"], 1);
    assert_eq!(inv.instances[1]["quota"]["pods"], 1);
    assert_eq!(inv.instances[2]["quota"]["namespaces"], 1);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn launch_templates_are_listed_once_per_provider() {
    let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
    reset_cloud();
    set_system_id("sys-1");
    let describes = Arc::new(AtomicUsize::new(0));
    let describes_bg = describes.clone();
    let created = Arc::new(StdMutex::new(Vec::<String>::new()));
    let created_bg = created.clone();
    let port = serve(move |_port, req| {
        if req.contains("Action=DescribeLaunchTemplates") {
            describes_bg.fetch_add(1, AtomicOrdering::SeqCst);
            return json_ok(r#"<?xml version="1.0"?><DescribeLaunchTemplatesResponse><launchTemplateSet></launchTemplateSet></DescribeLaunchTemplatesResponse>"#);
        }
        if req.contains("Action=CreateLaunchTemplate") {
            let key = if req.contains("KeyName=two") {
                "two"
            } else if req.contains("KeyName=one") {
                "one"
            } else {
                "other"
            };
            created_bg.lock().unwrap().push(key.to_string());
            return json_ok(r#"<?xml version="1.0"?><CreateLaunchTemplateResponse></CreateLaunchTemplateResponse>"#);
        }
        json_ok(r#"<?xml version="1.0"?><Response></Response>"#)
    });
    let mut options = BTreeMap::new();
    options.insert("access_key_id".into(), "AKID".into());
    options.insert("secret_access_key".into(), "SECRET".into());
    options.insert("endpoint_url".into(), format!("http://127.0.0.1:{port}"));
    options.insert("rate".into(), "1000".into());
    set_connections(vec![ConnectionConfig {
        name: "aws-once".into(),
        driver: "aws".into(),
        options,
    }]);
    {
        let mut reg = registry().lock().unwrap();
        reg.providers.insert(
            "tenant/once".into(),
            ProviderSnap {
                driver: "aws".into(),
                connection: "aws-once".into(),
                region: "us-east-1".into(),
                labels: IndexMap::from([
                    ("a".into(), json!({"name": "a", "flavor": "big", "key-name": "one"})),
                    ("z-fleet".into(), json!({"name": "z-fleet", "flavor": "big", "key-name": "two"})),
                ]),
                flavors: BTreeMap::from([(
                    "big".into(),
                    json!({"fleet": {"instance-types": ["t3.large"]}, "instance-type": "t3.large"}),
                )]),
                images: BTreeMap::new(),
            },
        );
        reg.label_orders.insert(
            "tenant/once".into(),
            vec!["z-fleet".into(), "a".into()],
        );
    }
    post_config();
    assert_eq!(describes.load(AtomicOrdering::SeqCst), 1);
    assert_eq!(
        created.lock().unwrap().as_slice(),
        ["two", "one"],
        "fleet labels followed sorted name order"
    );
}

#[test]
fn ami_tag_in_the_same_pass_stays_invisible() {
    let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
    reset_cloud();
    set_system_id("sys-1");
    let describes = Arc::new(AtomicUsize::new(0));
    let describes_bg = describes.clone();
    let tagged = Arc::new(StdMutex::new(false));
    let tagged_bg = tagged.clone();
    let port = serve(move |_port, req| {
        if req.contains("Action=CreateTags") {
            *tagged_bg.lock().unwrap() = true;
            return json_ok(r#"<?xml version="1.0"?><CreateTagsResponse></CreateTagsResponse>"#);
        }
        if req.contains("Action=DescribeImages") {
            describes_bg.fetch_add(1, AtomicOrdering::SeqCst);
            if *tagged_bg.lock().unwrap() {
                return json_ok(concat!(
                    r#"<?xml version="1.0"?><DescribeImagesResponse><imagesSet><item>"#,
                    r#"<imageId>ami-new</imageId><name>import-ami-task1</name><imageState>available</imageState>"#,
                    r#"<tagSet><item><key>zuul_system_id</key><value>sys-1</value></item></tagSet>"#,
                    r#"</item></imagesSet></DescribeImagesResponse>"#,
                ));
            }
            return json_ok(concat!(
                r#"<?xml version="1.0"?><DescribeImagesResponse><imagesSet><item>"#,
                r#"<imageId>ami-new</imageId><name>import-ami-task1</name><imageState>available</imageState>"#,
                r#"</item></imagesSet></DescribeImagesResponse>"#,
            ));
        }
        if req.contains("Action=DescribeImportImageTasks") {
            return json_ok(concat!(
                r#"<?xml version="1.0"?><DescribeImportImageTasksResponse><importImageTaskSet><item>"#,
                r#"<importTaskId>import-ami-task1</importTaskId>"#,
                r#"<tagSet><item><key>zuul_system_id</key><value>sys-1</value></item></tagSet>"#,
                r#"</item></importImageTaskSet></DescribeImportImageTasksResponse>"#,
            ));
        }
        json_ok(r#"<?xml version="1.0"?><Response></Response>"#)
    });
    aws_provider(
        "tenant/ami",
        "aws-ami",
        port,
        json!({"name": "debian", "flavor": "small"}),
        json!({}),
        json!({"name": "img"}),
    );
    post_config();
    let inv = endpoint_inventory("aws-ami/us-east-1");
    assert!(!inv.error);
    assert!(*tagged.lock().unwrap(), "tag was not copied");
    assert_eq!(describes.load(AtomicOrdering::SeqCst), 1, "image list must be cached inside the pass");
    let ami = inv.resources.iter().find(|r| r["type"] == "ami").expect("ami");
    assert!(ami["metadata"].get("zuul_system_id").is_none(), "{ami}");
}

#[test]
fn glance_share_uses_project_id_not_project_name() {
    let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
    reset_cloud();
    let seen = Arc::new(StdMutex::new(String::new()));
    let seen_bg = seen.clone();
    let port = serve(move |port, req| {
        if req.contains("/auth/tokens") {
            let body = format!(
                "{{\"token\":{{\"catalog\":[{{\"type\":\"image\",\"endpoints\":[{{\"interface\":\"public\",\"region\":\"RegionOne\",\"url\":\"http://127.0.0.1:{port}/image\"}}]}}]}}}}"
            );
            return (201, vec![("X-Subject-Token".into(), "tok".into())], body);
        }
        if req.contains("POST ") && req.contains("/members") {
            *seen_bg.lock().unwrap() = req.to_string();
            return (200, Vec::new(), r#"{"member_id":"proj-id"}"#.into());
        }
        if req.contains("PUT ") && req.contains("/members/") {
            return (200, Vec::new(), r#"{"status":"accepted"}"#.into());
        }
        json_ok("{}")
    });
    let dir = std::env::temp_dir().join(format!("glance-share-{}", new_id()));
    std::fs::create_dir_all(&dir).unwrap();
    let clouds = dir.join("clouds.yaml");
    std::fs::write(
        &clouds,
        format!(
            "clouds:\n  demo:\n    region_name: RegionOne\n    auth:\n      auth_url: http://127.0.0.1:{port}/v3\n      username: demo\n      password: secret\n      project_id: proj-id\n      project_name: proj-name\n"
        ),
    )
    .unwrap();
    let mut options = BTreeMap::new();
    options.insert("cloud".into(), "demo".into());
    options.insert("client_config_file".into(), clouds.display().to_string());
    options.insert("rate".into(), "1000".into());
    set_connections(vec![ConnectionConfig {
        name: "os-share".into(),
        driver: "openstack".into(),
        options,
    }]);
    {
        let mut reg = registry().lock().unwrap();
        reg.providers.insert(
            "tenant/share".into(),
            ProviderSnap {
                driver: "openstack".into(),
                connection: "os-share".into(),
                region: "RegionOne".into(),
                labels: IndexMap::new(),
                flavors: BTreeMap::new(),
                images: BTreeMap::from([(
                    "img".into(),
                    json!({"name": "img", "tags": {"visibility": "shared"}}),
                )]),
            },
        );
    }
    let done = upload_sdk_run(&json!({
        "provider": "tenant/share",
        "kind": "copy",
        "upload_uuid": "up-share",
        "image_name": "img",
        "source_external_id": "img-src",
    }))
    .unwrap();
    assert_eq!(done["external_id"], "img-src");
    let body = seen.lock().unwrap().clone();
    assert!(body.contains("\"member\":\"proj-id\""), "{body}");
    assert!(!body.contains("proj-name"), "{body}");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn missing_describe_fields_fail_like_keyerror() {
    let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
    reset_cloud();
    let port = serve(|_port, req| {
        if req.contains("Action=DescribeVolumes") {
            return json_ok(r#"<?xml version="1.0"?><DescribeVolumesResponse><volumeSet></volumeSet></DescribeVolumesResponse>"#);
        }
        if req.contains("Action=DescribeInstances") {
            return json_ok(concat!(
                r#"<?xml version="1.0"?><DescribeInstancesResponse><reservationSet><item><instancesSet>"#,
                r#"<item><instanceId>i-bare</instanceId><instanceState><name>running</name></instanceState></item>"#,
                r#"</instancesSet></item></reservationSet></DescribeInstancesResponse>"#,
            ));
        }
        json_ok(r#"<?xml version="1.0"?><Response></Response>"#)
    });
    aws_provider(
        "tenant/key",
        "aws-key",
        port,
        json!({}),
        json!({}),
        json!({}),
    );
    let snap = registry().lock().unwrap().providers.get("tenant/key").unwrap().clone();
    let err = list_aws_instances(&snap).unwrap_err();
    let text = err.to_string();
    assert!(text.contains("InstanceType"), "{text}");
}

#[test]
fn missing_openstack_flavor_fails_like_keyerror() {
    let tok = OsToken {
        token: String::new(),
        compute: String::new(),
        network: String::new(),
        image: String::new(),
        volume: String::new(),
        limit_key: String::new(),
        rate: 1.0,
    };
    let err = os_server_quota(&tok, &json!({"id": "srv", "status": "ACTIVE"}), &BTreeMap::new()).unwrap_err();
    assert!(err.to_string().contains("flavor"), "{err}");
}

#[test]
fn flavor_cache_does_not_refresh() {
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_bg = hits.clone();
    let load = std::sync::Arc::new(move || {
        hits_bg.fetch_add(1, AtomicOrdering::SeqCst);
        Ok(json!([{"id": "f1", "vcpus": 1, "ram": 512}]))
    });
    let key = "flavors-once-test";
    cache_once(key, load.clone()).unwrap();
    cache_once(key, load).unwrap();
    assert_eq!(hits.load(AtomicOrdering::SeqCst), 1);
    ttl_lists().lock().unwrap().remove(key);
}

#[test]
fn delete_existence_uses_the_list_cache() {
    let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
    reset_cloud();
    let describes = Arc::new(AtomicUsize::new(0));
    let describes_bg = describes.clone();
    let port = serve(move |_port, req| {
        if req.contains("Action=DescribeInstances") {
            describes_bg.fetch_add(1, AtomicOrdering::SeqCst);
            return json_ok(r#"<?xml version="1.0"?><DescribeInstancesResponse><reservationSet></reservationSet></DescribeInstancesResponse>"#);
        }
        json_ok(r#"<?xml version="1.0"?><Response></Response>"#)
    });
    aws_provider(
        "tenant/del",
        "aws-del",
        port,
        json!({}),
        json!({}),
        json!({}),
    );
    let snap = registry().lock().unwrap().providers.get("tenant/del").unwrap().clone();
    let resource = json!({"type": "instance", "id": "i-missing"});
    delete_aws(&snap, &resource).unwrap();
    delete_aws(&snap, &resource).unwrap();
    assert_eq!(describes.load(AtomicOrdering::SeqCst), 1);
}

#[test]
fn fleet_labels_without_a_config_array_follow_insertion_order() {
    let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
    reset_cloud();
    set_system_id("sys-1");
    let created = Arc::new(StdMutex::new(Vec::<String>::new()));
    let created_bg = created.clone();
    let port = serve(move |_port, req| {
        if req.contains("Action=DescribeLaunchTemplates") {
            return json_ok(r#"<?xml version="1.0"?><DescribeLaunchTemplatesResponse><launchTemplateSet></launchTemplateSet></DescribeLaunchTemplatesResponse>"#);
        }
        if req.contains("Action=CreateLaunchTemplate") {
            let key = if req.contains("KeyName=two") {
                "two"
            } else if req.contains("KeyName=one") {
                "one"
            } else {
                "other"
            };
            created_bg.lock().unwrap().push(key.to_string());
            return json_ok(r#"<?xml version="1.0"?><CreateLaunchTemplateResponse></CreateLaunchTemplateResponse>"#);
        }
        json_ok(r#"<?xml version="1.0"?><Response></Response>"#)
    });
    let mut options = BTreeMap::new();
    options.insert("access_key_id".into(), "AKID".into());
    options.insert("secret_access_key".into(), "SECRET".into());
    options.insert("endpoint_url".into(), format!("http://127.0.0.1:{port}"));
    options.insert("rate".into(), "1000".into());
    set_connections(vec![ConnectionConfig {
        name: "aws-order".into(),
        driver: "aws".into(),
        options,
    }]);
    {
        let mut reg = registry().lock().unwrap();
        reg.providers.insert(
            "tenant/order".into(),
            ProviderSnap {
                driver: "aws".into(),
                connection: "aws-order".into(),
                region: "us-east-1".into(),
                labels: IndexMap::from([
                    ("z-fleet".into(), json!({"name": "z-fleet", "flavor": "big", "key-name": "two"})),
                    ("a".into(), json!({"name": "a", "flavor": "big", "key-name": "one"})),
                ]),
                flavors: BTreeMap::from([(
                    "big".into(),
                    json!({"fleet": {"instance-types": ["t3.large"]}, "instance-type": "t3.large"}),
                )]),
                images: BTreeMap::new(),
            },
        );
    }
    post_config();
    assert_eq!(
        created.lock().unwrap().as_slice(),
        ["two", "one"],
        "in-memory labels follow insertion order, not sorted names"
    );
}

#[test]
fn empty_instance_type_is_passed_to_describe() {
    let _guard = test_lock().lock().unwrap_or_else(|err| err.into_inner());
    reset_cloud();
    set_system_id("sys-1");
    let saw = Arc::new(AtomicUsize::new(0));
    let saw_bg = saw.clone();
    let port = serve(move |_port, req| {
        if req.contains("Action=DescribeInstanceTypes") {
            saw_bg.fetch_add(1, AtomicOrdering::SeqCst);
            return json_ok(concat!(
                r#"<?xml version="1.0"?><DescribeInstanceTypesResponse><instanceTypeSet><item>"#,
                r#"<vCpuInfo><defaultCores>1</defaultCores><defaultVCpus>2</defaultVCpus></vCpuInfo>"#,
                r#"<memoryInfo><sizeInMiB>512</sizeInMiB></memoryInfo>"#,
                r#"</item></instanceTypeSet></DescribeInstanceTypesResponse>"#,
            ));
        }
        if req.contains("Action=DescribeInstances") {
            return json_ok(concat!(
                r#"<?xml version="1.0"?><DescribeInstancesResponse><reservationSet><item><instancesSet>"#,
                r#"<item><instanceId>i-empty</instanceId><instanceType></instanceType><instanceState><name>running</name></instanceState>"#,
                r#"<blockDeviceMapping><item><ebs><volumeId>vol-missing</volumeId></ebs></item></blockDeviceMapping>"#,
                r#"<tagSet><item><key>zuul_system_id</key><value>sys-1</value></item></tagSet></item>"#,
                r#"</instancesSet></item></reservationSet></DescribeInstancesResponse>"#,
            ));
        }
        if req.contains("Action=DescribeVolumes") {
            return json_ok(r#"<?xml version="1.0"?><DescribeVolumesResponse><volumeSet></volumeSet></DescribeVolumesResponse>"#);
        }
        json_ok(r#"<?xml version="1.0"?><Response></Response>"#)
    });
    aws_provider(
        "tenant/empty-type",
        "aws-empty",
        port,
        json!({"name": "debian", "flavor": "small"}),
        json!({"instance-type": "t3.micro"}),
        json!({"name": "img"}),
    );
    post_config();
    let inv = endpoint_inventory("aws-empty/us-east-1");
    assert!(!inv.error, "empty InstanceType failed the quota pass: {:?}", inv.instances);
    assert_eq!(saw.load(AtomicOrdering::SeqCst), 1, "DescribeInstanceTypes was not called");
    assert_eq!(inv.instances[0]["quota"]["instances"], 1);
    assert_eq!(inv.instances[0]["quota"]["cores"].as_f64(), Some(1.0));
}
