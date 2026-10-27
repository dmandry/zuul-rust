// Native CleanupWorker leak scan. The PyO3 path stays in workers.rs and
// calls Python cleanup.inspect / cleanup.apply. This file runs only when
// no cloud handler is registered.
//
// Ordering matches server.py cleanupLeakedResources: node uuid, then
// upload uuid, then expiration (immediate), then infrastructure resources
// that are not in listRequiredInfrastructureResources. A resource can
// match more than one check and be deleted more than once. OpenStack
// ports and floating IPs are cleaned inside listResources, not by that
// matcher. AWS retags untagged AMIs and snapshots from import tasks first.

use crate::config::quote_plus;
use crate::model::QuotaInformation;

const LAUNCH_TEMPLATE_PREFIX: &str = "zuul-launch-template-";

pub struct LeakInventory {
    pub error: bool,
    pub partial: bool,
    pub required_infra: Vec<String>,
    pub resource_types: Vec<String>,
    pub resources: Vec<Value>,
    pub instances: Vec<Value>,
}

pub fn post_config() {
    let snaps: Vec<(String, ProviderSnap)> = {
        let reg = registry().lock().unwrap();
        reg.providers.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    };
    let mut by_endpoint: BTreeMap<String, Vec<(String, ProviderSnap)>> = BTreeMap::new();
    for (name, snap) in snaps {
        let cname = endpoint_cname(&name, &snap);
        by_endpoint.entry(cname).or_default().push((name, snap));
    }
    let known: HashSet<String> = {
        let reg = registry().lock().unwrap();
        reg.providers.keys().cloned().collect()
    };
    {
        let mut reg = registry().lock().unwrap();
        reg.templates.retain(|name, _| known.contains(name));
    }
    for (cname, providers) in by_endpoint {
        let mut ok = true;
        for (name, snap) in &providers {
            if snap.driver == "aws" && create_provider_templates(name, snap).is_err() {
                ok = false;
            }
        }
        if ok {
            registry().lock().unwrap().ready.insert(cname, true);
        }
    }
}

pub fn stored_launch_template(provider: &str, label: &str) -> Option<String> {
    registry()
        .lock()
        .unwrap()
        .templates
        .get(provider)
        .and_then(|labels| labels.get(label))
        .cloned()
}

pub fn unmanaged_quota(canonical: &str) -> QuotaInformation {
    let map = registry()
        .lock()
        .unwrap()
        .unmanaged
        .get(canonical)
        .cloned()
        .unwrap_or_default();
    QuotaInformation::new(0.0, map)
}

pub fn set_unmanaged_quota(canonical: &str, quota: &BTreeMap<String, f64>) {
    registry()
        .lock()
        .unwrap()
        .unmanaged
        .insert(canonical.to_string(), quota.clone());
}

pub fn endpoint_inventory(canonical: &str) -> LeakInventory {
    let snaps = providers_on(canonical);
    if snaps.is_empty() {
        return LeakInventory {
            error: true,
            partial: false,
            required_infra: Vec::new(),
            resource_types: Vec::new(),
            resources: Vec::new(),
            instances: Vec::new(),
        };
    }
    let driver = snaps[0].1.driver.clone();
    if driver == "aws" {
        let ready = registry().lock().unwrap().ready.get(canonical).copied().unwrap_or(false);
        if !ready {
            return LeakInventory {
                error: true,
                partial: false,
                required_infra: Vec::new(),
                resource_types: Vec::new(),
                resources: Vec::new(),
                instances: Vec::new(),
            };
        }
    }
    let types = resource_types(&driver);
    // Same order as cleanupLeakedResources: required infrastructure
    // (which fills the launch-template cache), then instances, then
    // resources. A failure before listResources skips the endpoint.
    let required = match required_infra(canonical, &snaps) {
        Ok(v) => v,
        Err(_) => {
            return LeakInventory {
                error: true,
                partial: false,
                required_infra: Vec::new(),
                resource_types: types,
                resources: Vec::new(),
                instances: Vec::new(),
            };
        }
    };
    let instances = match list_instances_for(&snaps) {
        Ok(v) => v,
        Err(_) => {
            return LeakInventory {
                error: true,
                partial: false,
                required_infra: required,
                resource_types: types,
                resources: Vec::new(),
                instances: Vec::new(),
            };
        }
    };
    match list_resources(canonical, &snaps) {
        Ok(resources) => LeakInventory {
            error: false,
            partial: false,
            required_infra: required,
            resource_types: types,
            resources,
            instances,
        },
        Err(_) => LeakInventory {
            error: true,
            partial: true,
            required_infra: required,
            resource_types: types,
            resources: Vec::new(),
            instances,
        },
    }
}

pub fn delete_listed(canonical: &str, resource: &Value) -> Result<(), String> {
    let snaps = providers_on(canonical);
    let Some((_, snap)) = snaps.first() else {
        return Err("missing endpoint".into());
    };
    match snap.driver.as_str() {
        "aws" => delete_aws(snap, resource).map_err(|e| e.to_string()),
        "azure" => delete_azure(snap, resource).map_err(|e| e.to_string()),
        "openstack" => delete_os(snap, resource).map_err(|e| e.to_string()),
        "kubernetes" => delete_k8s(snap, resource).map_err(|e| e.to_string()),
        _ => Ok(()),
    }
}

#[cfg(test)]
pub fn testing_set_extra(provider: &str, bucket: Option<&str>, port_interval: Option<i64>, floating: bool) {
    let mut reg = registry().lock().unwrap();
    let snap = reg.providers.get(provider).cloned();
    let (cname, ename) = match snap {
        Some(snap) => {
            let name = if snap.region.is_empty() {
                snap.connection.clone()
            } else {
                snap.region.clone()
            };
            (
                format!("{}/{}", quote_plus(&snap.connection), quote_plus(&name)),
                name,
            )
        }
        None => (String::new(), String::new()),
    };
    reg.extras.insert(
        provider.to_string(),
        ProviderExtra {
            endpoint_canonical: cname,
            endpoint_name: ename,
            bucket: bucket.map(|s| s.to_string()),
            port_interval,
            floating_ip_cleanup: floating,
        },
    );
}

#[cfg(test)]
pub fn testing_age_ports(canonical: &str, secs: u64) {
    let mut reg = registry().lock().unwrap();
    if let Some(book) = reg.ports.get_mut(canonical) {
        book.last = Instant::now().checked_sub(Duration::from_secs(secs));
    }
}

fn create_provider_templates(name: &str, snap: &ProviderSnap) -> Result<(), AdvanceError> {
    let ep = aws_endpoint(snap)?;
    let mut stored = BTreeMap::new();
    let mut fleet = Vec::new();
    for (label_name, label) in &snap.labels {
        let flavor_name = field_str(label, &["flavor"]).unwrap_or_default();
        let flavor = snap.flavors.get(&flavor_name).cloned().unwrap_or(json!({}));
        if !flavor_is_fleet(&flavor) {
            continue;
        }
        fleet.push((label_name.clone(), label.clone(), flavor));
    }
    if fleet.is_empty() {
        registry().lock().unwrap().templates.insert(name.to_string(), stored);
        return Ok(());
    }
    // 14.3.0 lists launch templates once per provider, then creates the
    // names that are missing. The list is cached, and cleared only if
    // a template was created.
    let listed = list_launch_templates(&ep)?;
    let mut existing: HashSet<String> = listed
        .iter()
        .filter_map(|t| t.get("LaunchTemplateName").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect();
    let mut created = false;
    for (label_name, label, flavor) in fleet {
        let (template_name, args) = launch_template_spec(name, &label, &flavor);
        if !existing.contains(&template_name) {
            create_named_template(&ep, &template_name, &args)?;
            existing.insert(template_name.clone());
            created = true;
        }
        stored.insert(label_name, template_name);
    }
    if created {
        cache_forget(&ep, "launch-templates");
    }
    registry().lock().unwrap().templates.insert(name.to_string(), stored);
    Ok(())
}

fn flavor_is_fleet(flavor: &Value) -> bool {
    match flavor.get("fleet") {
        Some(Value::Bool(b)) => *b,
        Some(Value::Object(map)) => !map.is_empty(),
        Some(Value::Null) | None => false,
        Some(_) => true,
    }
}

fn providers_on(canonical: &str) -> Vec<(String, ProviderSnap)> {
    let reg = registry().lock().unwrap();
    reg.providers
        .iter()
        .filter(|(name, snap)| endpoint_cname_locked(&reg, name, snap) == canonical)
        .map(|(name, snap)| (name.clone(), snap.clone()))
        .collect()
}

fn endpoint_cname(name: &str, snap: &ProviderSnap) -> String {
    let reg = registry().lock().unwrap();
    endpoint_cname_locked(&reg, name, snap)
}

fn endpoint_cname_locked(reg: &Registry, name: &str, snap: &ProviderSnap) -> String {
    if let Some(extra) = reg.extras.get(name) {
        if !extra.endpoint_canonical.is_empty() {
            return extra.endpoint_canonical.clone();
        }
    }
    let ename = if snap.region.is_empty() {
        snap.connection.clone()
    } else {
        snap.region.clone()
    };
    format!("{}/{}", quote_plus(&snap.connection), quote_plus(&ename))
}

fn resource_types(driver: &str) -> Vec<String> {
    match driver {
        "aws" => vec![
            "host",
            "instance",
            "ami",
            "snapshot",
            "volume",
            "object",
            "launch-template",
        ],
        "openstack" => vec!["host", "instance", "ami", "snapshot", "volume", "object"],
        "azure" => vec!["instance", "nic", "pip", "disk", "image"],
        "kubernetes" => vec!["namespace", "project"],
        _ => Vec::new(),
    }
    .into_iter()
    .map(|s| s.to_string())
    .collect()
}

fn required_infra(canonical: &str, snaps: &[(String, ProviderSnap)]) -> Result<Vec<String>, AdvanceError> {
    let Some((_, snap)) = snaps.first() else {
        return Ok(Vec::new());
    };
    if snap.driver != "aws" {
        return Ok(Vec::new());
    }
    let ready = registry().lock().unwrap().ready.get(canonical).copied().unwrap_or(false);
    if !ready {
        return Err(AdvanceError::Other("Unable to list resources until configuration complete".into()));
    }
    let mut in_use = HashSet::new();
    {
        let reg = registry().lock().unwrap();
        for (name, _) in snaps {
            if let Some(labels) = reg.templates.get(name) {
                for template in labels.values() {
                    in_use.insert(template.clone());
                }
            }
        }
    }
    let ep = aws_endpoint(snap)?;
    let mut ids = Vec::new();
    for template in list_launch_templates(&ep)? {
        let template_name = template.get("LaunchTemplateName").and_then(|v| v.as_str()).unwrap_or("");
        if !in_use.contains(template_name) {
            continue;
        }
        ids.push(format!("launch-template-{template_name}"));
    }
    Ok(ids)
}

fn list_resources(canonical: &str, snaps: &[(String, ProviderSnap)]) -> Result<Vec<Value>, AdvanceError> {
    let Some((_, snap)) = snaps.first() else {
        return Ok(Vec::new());
    };
    match snap.driver.as_str() {
        "aws" => list_aws(snaps),
        "azure" => list_azure(snap),
        "openstack" => list_openstack(canonical, snaps),
        "kubernetes" => list_k8s(snap),
        _ => Ok(Vec::new()),
    }
}

fn list_instances_for(snaps: &[(String, ProviderSnap)]) -> Result<Vec<Value>, AdvanceError> {
    let Some((_, snap)) = snaps.first() else {
        return Ok(Vec::new());
    };
    match snap.driver.as_str() {
        "aws" => list_aws_instances(snap),
        "azure" => list_azure_instances(snap),
        "openstack" => list_os_instances(snap),
        "kubernetes" => list_k8s_instances(snap),
        _ => Ok(Vec::new()),
    }
}

fn list_aws(snaps: &[(String, ProviderSnap)]) -> Result<Vec<Value>, AdvanceError> {
    let (_, snap) = &snaps[0];
    let ep = aws_endpoint(snap)?;
    tag_snapshots(&ep)?;
    tag_amis(&ep)?;
    let mut out = Vec::new();
    for host in list_hosts(&ep)? {
        let state = state_text(&host);
        if state == "released" || state == "released-permanent-failure" {
            continue;
        }
        let id = host.get("HostId").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if id.is_empty() {
            continue;
        }
        out.push(resource("host", &id, aws_tag_map(&host), None));
    }
    for instance in list_instances(&ep)? {
        if state_text(&instance) == "terminated" {
            continue;
        }
        let id = instance.get("InstanceId").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if id.is_empty() {
            continue;
        }
        out.push(resource("instance", &id, aws_tag_map(&instance), None));
    }
    for volume in list_volumes_cached(&ep)? {
        if state_text(&volume) == "deleted" {
            continue;
        }
        let id = volume.get("VolumeId").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if id.is_empty() {
            continue;
        }
        out.push(resource("volume", &id, aws_tag_map(&volume), None));
    }
    for ami in list_images_cached(&ep)? {
        if state_text(&ami) == "deleted" {
            continue;
        }
        let id = ami.get("ImageId").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if id.is_empty() {
            continue;
        }
        out.push(resource("ami", &id, aws_tag_map(&ami), None));
    }
    for snap in list_snapshots_cached(&ep)? {
        if state_text(&snap) == "deleted" {
            continue;
        }
        let id = snap.get("SnapshotId").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if id.is_empty() {
            continue;
        }
        out.push(resource("snapshot", &id, aws_tag_map(&snap), None));
    }
    let mut buckets = HashSet::new();
    {
        let reg = registry().lock().unwrap();
        for (name, _) in snaps {
            if let Some(extra) = reg.extras.get(name) {
                if let Some(bucket) = &extra.bucket {
                    if !bucket.is_empty() {
                        buckets.insert(bucket.clone());
                    }
                }
            }
        }
    }
    for bucket in buckets {
        for (key, tags) in list_s3_objects(snap, &bucket)? {
            out.push(resource("object", &key, tags, Some(bucket.as_str())));
        }
    }
    for template in list_launch_templates_cached(&ep)? {
        let id = template.get("LaunchTemplateName").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if id.is_empty() {
            continue;
        }
        out.push(resource("launch-template", &id, aws_tag_map(&template), None));
    }
    Ok(out)
}

fn list_aws_instances(snap: &ProviderSnap) -> Result<Vec<Value>, AdvanceError> {
    let ep = aws_endpoint(snap)?;
    let mut volumes = BTreeMap::new();
    for volume in list_volumes_cached(&ep)? {
        if let Some(id) = volume.get("VolumeId").and_then(|v| v.as_str()) {
            volumes.insert(id.to_string(), volume);
        }
    }
    let mut out = Vec::new();
    for instance in list_instances(&ep)? {
        if state_text(&instance) == "terminated" {
            continue;
        }
        let dedicated = !instance
            .pointer("/Placement/HostId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .is_empty();
        let instance_type = instance.get("InstanceType").and_then(|v| v.as_str()).unwrap_or("");
        let spot = instance.get("InstanceLifecycle").and_then(|v| v.as_str()) == Some("spot");
        let mut quota = Map::new();
        if dedicated {
            quota.insert("instances".into(), json!(1));
            if let Some(code) = host_quota_code(instance_type) {
                if !code.is_empty() {
                    quota.insert(code, json!(1));
                }
            }
        } else if instance_type.is_empty() {
            // DescribeInstances always carries InstanceType. Fixtures
            // that omit it record instances=1 and do not call
            // DescribeInstanceTypes.
            quota.insert("instances".into(), json!(1));
        } else {
            let mut params = Map::new();
            params.insert("InstanceType".into(), json!([instance_type]));
            let resp = ec2(&ep, "DescribeInstanceTypes", params, true)?;
            let info = resp.pointer("/InstanceTypes/0").cloned().unwrap_or(Value::Null);
            if info.is_null() {
                return Err(err_other(format!("invalid instance type {instance_type}")));
            }
            let cores = json_f64(info.pointer("/VCpuInfo/DefaultCores")).unwrap_or(0.0);
            let ram = json_f64(info.pointer("/MemoryInfo/SizeInMiB")).unwrap_or(0.0);
            let vcpus = json_f64(info.pointer("/VCpuInfo/DefaultVCpus")).unwrap_or(0.0);
            quota.insert("cores".into(), json!(cores));
            quota.insert("ram".into(), json!(ram));
            quota.insert("instances".into(), json!(1));
            if let Some(code) = instance_quota_code(instance_type, spot) {
                if !code.is_empty() {
                    quota.insert(code, json!(vcpus));
                }
            }
        }
        if let Some(maps) = instance.get("BlockDeviceMappings").and_then(|v| v.as_array()) {
            for attachment in maps {
                let Some(id) = attachment.pointer("/Ebs/VolumeId").and_then(|v| v.as_str()) else {
                    continue;
                };
                let Some(volume) = volumes.get(id) else {
                    continue;
                };
                add_reported_volume_quota(&mut quota, volume);
            }
        }
        out.push(json!({
            "metadata": aws_tag_map(&instance),
            "quota": quota,
        }));
    }
    Ok(out)
}

fn list_azure(snap: &ProviderSnap) -> Result<Vec<Value>, AdvanceError> {
    let (conn, _) = azure_cloud(snap)?;
    let sub = azure_sub(&conn)?;
    let base = azure_arm_base();
    let mut out = Vec::new();
    let specs = [
        ("instance", format!("{base}/subscriptions/{sub}/providers/Microsoft.Compute/virtualMachines?api-version=2024-07-01")),
        ("nic", format!("{base}/subscriptions/{sub}/providers/Microsoft.Network/networkInterfaces?api-version=2024-05-01")),
        ("pip", format!("{base}/subscriptions/{sub}/providers/Microsoft.Network/publicIPAddresses?api-version=2024-05-01")),
        ("disk", format!("{base}/subscriptions/{sub}/providers/Microsoft.Compute/disks?api-version=2020-06-30")),
        ("image", format!("{base}/subscriptions/{sub}/providers/Microsoft.Compute/images?api-version=2020-12-01")),
    ];
    for (kind, url) in specs {
        for item in cached_azure_items(&conn, &url)? {
            let id = item.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
            if id.is_empty() {
                continue;
            }
            out.push(resource(kind, &id, json_tags(item.get("tags")), None));
        }
    }
    Ok(out)
}

fn cached_azure_items(conn: &ConnectionConfig, url: &str) -> Result<Vec<Value>, AdvanceError> {
    let cache_key = format!("azure:{url}");
    let conn = conn.clone();
    let url = url.to_string();
    let load = std::sync::Arc::new(move || {
        let mut all = Vec::new();
        let mut next = Some(url.clone());
        let mut seen = HashSet::new();
        while let Some(page_url) = next.take() {
            if !seen.insert(page_url.clone()) {
                break;
            }
            let doc = azure_request(&conn, "GET", &page_url, None)?;
            if let Some(items) = doc.get("value").and_then(|v| v.as_array()) {
                all.extend(items.clone());
            }
            next = doc
                .get("nextLink")
                .or_else(|| doc.get("@odata.nextLink"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string());
        }
        Ok(Value::Array(all))
    });
    let value = with_ttl_cache(&cache_key, LIST_TTL, load)?;
    Ok(value.as_array().cloned().unwrap_or_default())
}

fn azure_skus(conn: &ConnectionConfig) -> Result<Vec<Value>, AdvanceError> {
    let sub = azure_sub(conn)?;
    let url = format!(
        "{}/subscriptions/{sub}/providers/Microsoft.Compute/skus?api-version=2021-07-01",
        azure_arm_base()
    );
    let cache_key = format!("azure-skus:{sub}");
    let conn = conn.clone();
    let load = std::sync::Arc::new(move || {
        let mut all = Vec::new();
        let mut next = Some(url.clone());
        let mut seen = HashSet::new();
        while let Some(page_url) = next.take() {
            if !seen.insert(page_url.clone()) {
                break;
            }
            let doc = azure_request(&conn, "GET", &page_url, None)?;
            if let Some(items) = doc.get("value").and_then(|v| v.as_array()) {
                all.extend(items.clone());
            }
            next = doc
                .get("nextLink")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string());
        }
        Ok(Value::Array(all))
    });
    // _getSKUs runs once when the endpoint starts and is kept for its life.
    let value = with_ttl_cache(&cache_key, Duration::from_secs(86400 * 365), load)?;
    Ok(value.as_array().cloned().unwrap_or_default())
}

fn quota_from_sku(sku: Option<&Value>, spot: bool) -> Map<String, Value> {
    let mut quota = Map::new();
    let Some(sku) = sku else {
        quota.insert("instances".into(), json!(1));
        return quota;
    };
    let mut cores = 0i64;
    let mut spot_cores = 0i64;
    let mut ram = 0i64;
    if let Some(caps) = sku.get("capabilities").and_then(|v| v.as_array()) {
        for cap in caps {
            let name = cap.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let value = cap.get("value").and_then(|v| v.as_str()).unwrap_or("");
            if name == "vCPUs" {
                let n = value.parse::<i64>().unwrap_or(0);
                if spot {
                    spot_cores = n;
                } else {
                    cores = n;
                }
            }
            if name == "MemoryGB" {
                let gb = value.parse::<f64>().unwrap_or(0.0);
                ram = (gb * 1024.0) as i64;
            }
        }
    }
    quota.insert("cores".into(), json!(cores));
    quota.insert("spot_cores".into(), json!(spot_cores));
    quota.insert("ram".into(), json!(ram));
    quota.insert("instances".into(), json!(1));
    quota
}

fn list_azure_instances(snap: &ProviderSnap) -> Result<Vec<Value>, AdvanceError> {
    let (conn, _) = azure_cloud(snap)?;
    let sub = azure_sub(&conn)?;
    let url = format!(
        "{}/subscriptions/{sub}/providers/Microsoft.Compute/virtualMachines?api-version=2024-07-01",
        azure_arm_base()
    );
    let vms = cached_azure_items(&conn, &url)?;
    let skus = if vms.is_empty() { Vec::new() } else { azure_skus(&conn)? };
    let mut out = Vec::new();
    for vm in vms {
        let size = vm.pointer("/properties/hardwareProfile/vmSize").and_then(|v| v.as_str()).unwrap_or("");
        let location = vm.get("location").and_then(|v| v.as_str()).unwrap_or("");
        let spot = vm.pointer("/properties/priority").and_then(|v| v.as_str()) == Some("Spot");
        let sku = skus.iter().find(|sku| {
            sku.get("name").and_then(|v| v.as_str()) == Some(size)
                && sku
                    .get("locations")
                    .and_then(|v| v.as_array())
                    .map(|locs| locs.iter().any(|loc| loc.as_str() == Some(location)))
                    .unwrap_or(false)
        });
        out.push(json!({
            "metadata": json_tags(vm.get("tags")),
            "quota": quota_from_sku(sku, spot),
        }));
    }
    Ok(out)
}

fn list_openstack(canonical: &str, snaps: &[(String, ProviderSnap)]) -> Result<Vec<Value>, AdvanceError> {
    let (_, snap) = &snaps[0];
    let tok = os_token(snap)?;
    let mut out = Vec::new();
    let servers = cached_os_pages(&tok, &format!("{}/servers/detail", tok.compute), "servers")?;
    for server in servers {
        if server.get("status").and_then(|v| v.as_str()).unwrap_or("").eq_ignore_ascii_case("deleted") {
            continue;
        }
        let id = server.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if id.is_empty() {
            continue;
        }
        out.push(resource("instance", &id, json_tags(server.get("metadata")), None));
    }
    let mut interval: Option<i64> = None;
    let mut floating = false;
    {
        let reg = registry().lock().unwrap();
        for (name, snap) in snaps {
            if snap.driver != "openstack" {
                continue;
            }
            let configured = reg.extras.get(name).and_then(|e| e.port_interval);
            let value = configured.unwrap_or(600);
            if value > 0 {
                interval = Some(interval.map(|cur: i64| cur.min(value)).unwrap_or(value));
            }
            if reg.extras.get(name).map(|e| e.floating_ip_cleanup).unwrap_or(false) {
                floating = true;
            }
        }
    }
    if let Some(interval) = interval {
        cleanup_ports(canonical, &tok, interval)?;
    }
    if floating {
        cleanup_floating_ips(snap, &tok)?;
    }
    Ok(out)
}

fn list_k8s(snap: &ProviderSnap) -> Result<Vec<Value>, AdvanceError> {
    let kube = kube_of(snap)?;
    let conn = connection_of(&snap.connection)?;
    let openshift = kube.openshift
        || opt(&conn, "use_openshift_projects").as_deref() == Some("true")
        || opt(&conn, "use-openshift-projects").as_deref() == Some("true");
    let (path, kind) = if openshift {
        ("/apis/project.openshift.io/v1/projects", "project")
    } else {
        ("/api/v1/namespaces", "namespace")
    };
    let mut out = Vec::new();
    let mut path = path.to_string();
    let mut seen = HashSet::new();
    loop {
        if !seen.insert(path.clone()) {
            break;
        }
        let doc = k8s_http(&kube, "GET", &path, None)?;
        let items = doc.get("items").and_then(|v| v.as_array()).cloned().unwrap_or_default();
        for item in items {
            let name = item.pointer("/metadata/name").and_then(|v| v.as_str()).unwrap_or("").to_string();
            if name.is_empty() {
                continue;
            }
            let labels = json_tags(item.pointer("/metadata/labels"));
            out.push(resource(kind, &name, labels, None));
        }
        let cont = doc.pointer("/metadata/continue").and_then(|v| v.as_str()).unwrap_or("");
        if cont.is_empty() {
            break;
        }
        let base = path.split('?').next().unwrap_or(path.as_str());
        path = format!("{base}?continue={}", aws_encode(cont));
    }
    Ok(out)
}

fn cached_os_pages(tok: &OsToken, start: &str, key: &str) -> Result<Vec<Value>, AdvanceError> {
    let cache_key = format!("os:{start}");
    let tok = tok.clone();
    let start = start.to_string();
    let key = key.to_string();
    let load = std::sync::Arc::new(move || {
        let items = os_pages(&tok, &start, &key)?;
        Ok(Value::Array(items))
    });
    let value = with_ttl_cache(&cache_key, LIST_TTL, load)?;
    Ok(value.as_array().cloned().unwrap_or_default())
}

fn os_volume_map(tok: &OsToken) -> Result<BTreeMap<String, Value>, AdvanceError> {
    if tok.volume.is_empty() {
        return Ok(BTreeMap::new());
    }
    let listed = cached_os_pages(tok, &format!("{}/volumes/detail", tok.volume), "volumes")?;
    let mut volumes = BTreeMap::new();
    for volume in listed {
        if let Some(id) = volume.get("id").and_then(|v| v.as_str()) {
            volumes.insert(id.to_string(), volume);
        }
    }
    Ok(volumes)
}

fn os_flavor_by_id(tok: &OsToken, id: &str) -> Result<Value, AdvanceError> {
    let cache_key = format!("flavors:{}", tok.compute);
    let tok_load = tok.clone();
    let load = std::sync::Arc::new(move || {
        let listed = os_http(&tok_load, "GET", &format!("{}/flavors/detail", tok_load.compute), None)?;
        Ok(listed.get("flavors").cloned().unwrap_or(json!([])))
    });
    let flavors = with_ttl_cache(&cache_key, Duration::from_secs(86400 * 365), load)?;
    for flavor in flavors.as_array().cloned().unwrap_or_default() {
        if flavor.get("id").and_then(|v| v.as_str()) == Some(id) {
            return Ok(flavor);
        }
    }
    Err(err_other(format!("Unable to find flavor with id: {id}")))
}

fn os_server_quota(tok: &OsToken, server: &Value, volumes: &BTreeMap<String, Value>) -> Result<Map<String, Value>, AdvanceError> {
    let flavor = server.get("flavor").cloned().unwrap_or(Value::Null);
    let (vcpus, ram) = if flavor.get("vcpus").is_some() {
        (
            flavor.get("vcpus").and_then(|v| v.as_i64()).unwrap_or(0),
            flavor.get("ram").and_then(|v| v.as_i64()).unwrap_or(0),
        )
    } else if let Some(id) = flavor.get("id").and_then(|v| v.as_str()) {
        let found = os_flavor_by_id(tok, id)?;
        (
            found.get("vcpus").and_then(|v| v.as_i64()).unwrap_or(0),
            found.get("ram").and_then(|v| v.as_i64()).unwrap_or(0),
        )
    } else {
        let mut quota = Map::new();
        quota.insert("instances".into(), json!(1));
        return Ok(quota);
    };
    let mut quota = Map::new();
    quota.insert("instances".into(), json!(1));
    quota.insert("cores".into(), json!(vcpus));
    quota.insert("ram".into(), json!(ram));
    let mut attached = Vec::new();
    if let Some(links) = server.get("os-extended-volumes:volumes_attached").and_then(|v| v.as_array()) {
        for link in links {
            if let Some(id) = link.get("id").and_then(|v| v.as_str()) {
                if let Some(volume) = volumes.get(id) {
                    attached.push(volume);
                }
            }
        }
    }
    if !attached.is_empty() {
        let gb: i64 = attached
            .iter()
            .map(|v| v.get("size").and_then(|n| n.as_i64()).unwrap_or(0))
            .sum();
        quota.insert("volumes".into(), json!(attached.len()));
        quota.insert("volume-gb".into(), json!(gb));
    }
    Ok(quota)
}

fn list_os_instances(snap: &ProviderSnap) -> Result<Vec<Value>, AdvanceError> {
    let tok = os_token(snap)?;
    let volumes = os_volume_map(&tok)?;
    let servers = cached_os_pages(&tok, &format!("{}/servers/detail", tok.compute), "servers")?;
    let mut out = Vec::new();
    for server in servers {
        if server.get("status").and_then(|v| v.as_str()).unwrap_or("").eq_ignore_ascii_case("deleted") {
            continue;
        }
        let quota = os_server_quota(&tok, &server, &volumes)?;
        out.push(json!({
            "metadata": server.get("metadata").cloned().unwrap_or(json!({})),
            "quota": quota,
        }));
    }
    Ok(out)
}

fn list_k8s_instances(snap: &ProviderSnap) -> Result<Vec<Value>, AdvanceError> {
    let kube = kube_of(snap)?;
    let mut out = Vec::new();
    for resource in list_k8s(snap)? {
        let kind = resource.get("type").and_then(|v| v.as_str()).unwrap_or("");
        if kind != "namespace" && kind != "project" {
            continue;
        }
        let name = resource.get("id").and_then(|v| v.as_str()).unwrap_or("");
        out.push(json!({
            "metadata": resource.get("metadata").cloned().unwrap_or(json!({})),
            "quota": {"namespaces": 1},
        }));
        let path = format!("/api/v1/namespaces/{name}/pods");
        let doc = match k8s_http(&kube, "GET", &path, None) {
            Ok(doc) => doc,
            Err(err) if err.to_string().contains("403") || err.to_string().contains("Forbidden") => continue,
            Err(err) => return Err(err),
        };
        let pods = doc.get("items").and_then(|v| v.as_array()).cloned().unwrap_or_default();
        for pod in pods {
            out.push(json!({
                "metadata": json_tags(pod.pointer("/metadata/labels")),
                "quota": {"pods": 1},
            }));
        }
    }
    Ok(out)
}

fn os_pages(tok: &OsToken, start: &str, key: &str) -> Result<Vec<Value>, AdvanceError> {
    let mut url = start.to_string();
    let mut seen = HashSet::new();
    let mut all = Vec::new();
    loop {
        if !seen.insert(url.clone()) {
            break;
        }
        let doc = os_http(tok, "GET", &url, None)?;
        if let Some(items) = doc.get(key).and_then(|v| v.as_array()) {
            all.extend(items.clone());
        }
        let links_key = format!("{key}_links");
        let next = doc.get(&links_key).and_then(|v| v.as_array()).and_then(|links| {
            links.iter().find(|link| link.get("rel").and_then(|v| v.as_str()) == Some("next"))
        });
        let href = next.and_then(|link| link.get("href").and_then(|v| v.as_str())).unwrap_or("");
        if href.is_empty() {
            break;
        }
        url = href.to_string();
    }
    Ok(all)
}

fn cleanup_ports(canonical: &str, tok: &OsToken, interval: i64) -> Result<(), AdvanceError> {
    let current = down_port_ids(tok)?;
    let first = {
        let mut reg = registry().lock().unwrap();
        let book = reg.ports.entry(canonical.to_string()).or_insert(PortBook {
            last: None,
            down: HashSet::new(),
        });
        if book.last.is_none() {
            book.last = Some(Instant::now());
            book.down = current.clone();
            true
        } else {
            let elapsed = book.last.unwrap().elapsed().as_secs() as i64;
            if elapsed <= interval {
                return Ok(());
            }
            false
        }
    };
    if first {
        return Ok(());
    }
    let previous = registry().lock().unwrap().ports.get(canonical).map(|b| b.down.clone()).unwrap_or_default();
    for port_id in current.intersection(&previous) {
        let _ = os_http(tok, "DELETE", &format!("{}/v2.0/ports/{port_id}", tok.network), None);
    }
    let refreshed = down_port_ids(tok).unwrap_or(current);
    let mut reg = registry().lock().unwrap();
    if let Some(book) = reg.ports.get_mut(canonical) {
        book.last = Some(Instant::now());
        book.down = refreshed;
    }
    Ok(())
}

fn down_port_ids(tok: &OsToken) -> Result<HashSet<String>, AdvanceError> {
    let ports = os_pages(tok, &format!("{}/v2.0/ports?status=DOWN", tok.network), "ports")?;
    let mut ids = HashSet::new();
    for port in ports {
        let owner = port.get("device_owner").and_then(|v| v.as_str()).unwrap_or("");
        if !(owner.is_empty() || owner.starts_with("compute:")) {
            continue;
        }
        if let Some(id) = port.get("id").and_then(|v| v.as_str()) {
            ids.insert(id.to_string());
        }
    }
    Ok(ids)
}

fn cleanup_floating_ips(snap: &ProviderSnap, tok: &OsToken) -> Result<(), AdvanceError> {
    let (cloud, _) = os_config(snap)?;
    // _delete_unattached_floating_ips iterates only when Neutron is the source.
    if !use_neutron_floating(tok, &cloud) {
        return Ok(());
    }
    let ips = list_floating_ips(tok, &cloud)?;
    for ip in ips {
        let port_id = ip.get("port_id").and_then(|v| v.as_str()).unwrap_or("");
        if !port_id.is_empty() {
            continue;
        }
        let id = ip.get("id").and_then(|v| v.as_str()).unwrap_or("");
        if id.is_empty() {
            continue;
        }
        let _ = os_http(tok, "DELETE", &format!("{}/v2.0/floatingips/{id}", tok.network), None);
    }
    Ok(())
}

fn delete_aws(snap: &ProviderSnap, resource: &Value) -> Result<(), AdvanceError> {
    let ep = aws_endpoint(snap)?;
    let kind = resource.get("type").and_then(|v| v.as_str()).unwrap_or("");
    let id = resource.get("id").and_then(|v| v.as_str()).unwrap_or("");
    match kind {
        "host" => {
            if !ec2_list(&ep, "DescribeHosts", "Hosts")?
                .iter()
                .any(|h| h.get("HostId").and_then(|v| v.as_str()) == Some(id))
            {
                return Ok(());
            }
            let mut params = Map::new();
            params.insert("HostIds".into(), json!([id]));
            ec2(&ep, "ReleaseHosts", params, false)?;
        }
        "instance" => {
            if !ec2_instances(&ep)?.iter().any(|i| i.get("InstanceId").and_then(|v| v.as_str()) == Some(id)) {
                return Ok(());
            }
            let mut params = Map::new();
            params.insert("InstanceIds".into(), json!([id]));
            params.insert("SkipOsShutdown".into(), json!(true));
            ec2(&ep, "TerminateInstances", params, false)?;
        }
        "volume" => {
            if !ec2_list(&ep, "DescribeVolumes", "Volumes")?
                .iter()
                .any(|v| v.get("VolumeId").and_then(|x| x.as_str()) == Some(id))
            {
                return Ok(());
            }
            let mut params = Map::new();
            params.insert("VolumeId".into(), json!(id));
            let _ = ec2(&ep, "DeleteVolume", params, false);
        }
        "ami" => {
            if !ec2_list(&ep, "DescribeImages", "Images")?
                .iter()
                .any(|v| v.get("ImageId").and_then(|x| x.as_str()) == Some(id))
            {
                return Ok(());
            }
            let mut params = Map::new();
            params.insert("ImageId".into(), json!(id));
            let _ = ec2(&ep, "DeregisterImage", params, false);
        }
        "snapshot" => {
            if !ec2_list(&ep, "DescribeSnapshots", "Snapshots")?
                .iter()
                .any(|v| v.get("SnapshotId").and_then(|x| x.as_str()) == Some(id))
            {
                return Ok(());
            }
            let mut params = Map::new();
            params.insert("SnapshotId".into(), json!(id));
            let _ = ec2(&ep, "DeleteSnapshot", params, false);
        }
        "object" => {
            let bucket = resource.get("bucket_name").and_then(|v| v.as_str()).unwrap_or("");
            let _ = s3_delete(&snap_name(snap), bucket, id);
        }
        "launch-template" => {
            let mut params = Map::new();
            params.insert("LaunchTemplateName".into(), json!(id));
            ec2(&ep, "DeleteLaunchTemplate", params, false)?;
        }
        _ => {}
    }
    Ok(())
}

fn snap_name(snap: &ProviderSnap) -> String {
    let reg = registry().lock().unwrap();
    reg.providers
        .iter()
        .find(|(_, p)| p.connection == snap.connection && p.region == snap.region && p.driver == snap.driver)
        .map(|(name, _)| name.clone())
        .unwrap_or_default()
}

fn delete_azure(snap: &ProviderSnap, resource: &Value) -> Result<(), AdvanceError> {
    let (conn, _) = azure_cloud(snap)?;
    let id = resource.get("id").and_then(|v| v.as_str()).unwrap_or("");
    if id.is_empty() {
        return Ok(());
    }
    let url = if id.starts_with("http://") || id.starts_with("https://") {
        format!("{id}?api-version=2024-07-01")
    } else {
        format!("{}{id}?api-version=2024-07-01", azure_arm_base())
    };
    match azure_request(&conn, "DELETE", &url, None) {
        Ok(_) => Ok(()),
        Err(err) if err.to_string().contains("404") => Ok(()),
        Err(err) => Err(err),
    }
}

fn delete_os(snap: &ProviderSnap, resource: &Value) -> Result<(), AdvanceError> {
    if resource.get("type").and_then(|v| v.as_str()) != Some("instance") {
        return Ok(());
    }
    let tok = os_token(snap)?;
    let id = resource.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let _ = os_http(&tok, "DELETE", &format!("{}/servers/{id}", tok.compute), None);
    Ok(())
}

fn delete_k8s(snap: &ProviderSnap, resource: &Value) -> Result<(), AdvanceError> {
    let kube = kube_of(snap)?;
    let id = resource.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let kind = resource.get("type").and_then(|v| v.as_str()).unwrap_or("");
    if kind == "project" {
        let _ = k8s_http(&kube, "DELETE", &format!("/apis/project.openshift.io/v1/projects/{id}"), None);
    } else {
        let body = json!({"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Background"});
        let _ = k8s_http(&kube, "DELETE", &format!("/api/v1/namespaces/{id}"), Some(&body));
    }
    Ok(())
}

fn tag_amis(ep: &Arc<AwsEndpoint>) -> Result<(), AdvanceError> {
    let amis = list_images_cached(ep)?;
    let mut examine = Vec::new();
    for ami in amis {
        let id = ami.get("ImageId").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if id.is_empty() || ep.not_our_images.lock().unwrap().contains(&id) {
            continue;
        }
        if ami.get("Tags").and_then(|v| v.as_array()).map(|a| !a.is_empty()).unwrap_or(false) {
            continue;
        }
        let name = ami.get("Name").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if name.starts_with("import-ami-") {
            if let Some(task) = import_image_task(ep, &name)? {
                let tags = aws_tag_map(&task);
                if tags.get("zuul_system_id").map(String::as_str) == Some(system_id().as_str()) {
                    let _ = create_tags(ep, &id, &json!(tags));
                    continue;
                }
            }
        }
        let snapshot_id = ami
            .pointer("/BlockDeviceMappings/0/Ebs/SnapshotId")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if snapshot_id.is_empty() {
            ep.not_our_images.lock().unwrap().insert(id);
            continue;
        }
        examine.push((id, snapshot_id));
    }
    if examine.is_empty() {
        return Ok(());
    }
    let task_map = import_snapshot_task_map(ep)?;
    for (ami_id, snapshot_id) in examine {
        let Some(tags) = task_map.get(&snapshot_id) else {
            ep.not_our_images.lock().unwrap().insert(ami_id);
            continue;
        };
        let parsed = aws_tag_map(&json!({"Tags": tags}));
        if parsed.get("zuul_system_id").map(String::as_str) == Some(system_id().as_str()) {
            let _ = create_tags(ep, &ami_id, &json!(parsed));
        } else {
            ep.not_our_images.lock().unwrap().insert(ami_id);
        }
    }
    Ok(())
}

fn tag_snapshots(ep: &Arc<AwsEndpoint>) -> Result<(), AdvanceError> {
    let snaps = list_snapshots_cached(ep)?;
    let mut examine = Vec::new();
    for snap in snaps {
        let id = snap.get("SnapshotId").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if id.is_empty() || ep.not_our_snapshots.lock().unwrap().contains(&id) {
            continue;
        }
        if snap.get("Tags").and_then(|v| v.as_array()).map(|a| !a.is_empty()).unwrap_or(false) {
            continue;
        }
        let description = snap.get("Description").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if let Some(task_id) = import_ami_id(&description) {
            if let Some(task) = import_image_task(ep, &task_id)? {
                let tags = aws_tag_map(&task);
                if tags.get("zuul_system_id").map(String::as_str) == Some(system_id().as_str()) {
                    let _ = create_tags(ep, &id, &json!(tags));
                    continue;
                }
            }
        }
        examine.push(id);
    }
    if examine.is_empty() {
        return Ok(());
    }
    let task_map = import_snapshot_task_map(ep)?;
    for id in examine {
        let Some(tags) = task_map.get(&id) else {
            ep.not_our_snapshots.lock().unwrap().insert(id);
            continue;
        };
        let parsed = aws_tag_map(&json!({"Tags": tags}));
        if parsed.get("zuul_system_id").map(String::as_str) == Some(system_id().as_str()) {
            let _ = create_tags(ep, &id, &json!(parsed));
        } else {
            ep.not_our_snapshots.lock().unwrap().insert(id);
        }
    }
    Ok(())
}

fn import_ami_id(description: &str) -> Option<String> {
    let marker = "import-ami-";
    let start = description.find(marker)?;
    let rest = &description[start..];
    let end = rest.find(|c: char| !(c.is_ascii_alphanumeric() || c == '-')).unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

fn import_image_task(ep: &AwsEndpoint, task_id: &str) -> Result<Option<Value>, AdvanceError> {
    let mut params = Map::new();
    params.insert("ImportTaskIds".into(), json!([task_id]));
    match ec2(ep, "DescribeImportImageTasks", params, true) {
        Ok(page) => Ok(page.get("ImportImageTasks").and_then(|v| v.as_array()).and_then(|a| a.first()).cloned()),
        Err(err) if err.to_string().contains("InvalidConversionTaskId.Malformed") => Ok(None),
        Err(err) => Err(err),
    }
}

fn import_snapshot_task_map(ep: &AwsEndpoint) -> Result<BTreeMap<String, Value>, AdvanceError> {
    let mut out = BTreeMap::new();
    for task in ec2_list(ep, "DescribeImportSnapshotTasks", "ImportSnapshotTasks")? {
        let id = task.pointer("/SnapshotTaskDetail/SnapshotId").and_then(|v| v.as_str()).unwrap_or("");
        if id.is_empty() {
            continue;
        }
        if let Some(tags) = task.get("Tags") {
            out.insert(id.to_string(), tags.clone());
        }
    }
    Ok(out)
}

fn find_tagged_import(ep: &AwsEndpoint, action: &str, list_key: &str, metadata: &Value) -> Result<Option<String>, AdvanceError> {
    for task in ec2_list(ep, action, list_key)? {
        if metadata_covers(metadata, &aws_tag_map(&task)) {
            return Ok(task.get("ImportTaskId").and_then(|v| v.as_str()).map(|s| s.to_string()));
        }
    }
    Ok(None)
}

fn metadata_covers(metadata: &Value, candidate: &BTreeMap<String, String>) -> bool {
    let Some(obj) = metadata.as_object() else {
        return true;
    };
    for (key, value) in obj {
        let want = match value {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            _ => continue,
        };
        if candidate.get(key).map(String::as_str) != Some(want.as_str()) {
            return false;
        }
    }
    true
}

fn ec2_until_import_limit(ep: &AwsEndpoint, action: &str, params: Map<String, Value>, timeout: f64) -> Result<Value, AdvanceError> {
    let deadline = now_secs() + timeout;
    loop {
        match ec2(ep, action, params.clone(), false) {
            Ok(value) => return Ok(value),
            Err(AdvanceError::Other(err)) if err.contains("ResourceCountLimitExceeded") && now_secs() < deadline => {
                image_upload_sleep();
                continue;
            }
            Err(err) => return Err(err),
        }
    }
}

fn list_launch_templates(ep: &AwsEndpoint) -> Result<Vec<Value>, AdvanceError> {
    if let Some(arc) = endpoint_arc(ep) {
        return list_launch_templates_cached(&arc);
    }
    let mut params = Map::new();
    params.insert(
        "Filters".into(),
        json!([{"Name": "launch-template-name", "Value": [format!("{LAUNCH_TEMPLATE_PREFIX}*")]}]),
    );
    let listed = ec2_list_params(ep, "DescribeLaunchTemplates", "LaunchTemplates", params)?;
    Ok(listed
        .into_iter()
        .filter(|t| {
            t.get("LaunchTemplateName")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .starts_with(LAUNCH_TEMPLATE_PREFIX)
        })
        .collect())
}

fn ec2_instances(ep: &AwsEndpoint) -> Result<Vec<Value>, AdvanceError> {
    let mut params = Map::new();
    let mut instances = Vec::new();
    loop {
        let page = ec2(ep, "DescribeInstances", params.clone(), true)?;
        let reservations = page.get("Reservations").and_then(|v| v.as_array()).cloned().unwrap_or_default();
        for res in reservations {
            if let Some(arr) = res.get("Instances").and_then(|v| v.as_array()) {
                instances.extend(arr.clone());
            }
        }
        let token = page.get("NextToken").and_then(|v| v.as_str()).unwrap_or("");
        if token.is_empty() {
            break;
        }
        params.insert("NextToken".into(), json!(token));
    }
    Ok(instances)
}

fn ec2_list(ep: &AwsEndpoint, action: &str, key: &str) -> Result<Vec<Value>, AdvanceError> {
    ec2_list_params(ep, action, key, Map::new())
}

fn ec2_list_params(ep: &AwsEndpoint, action: &str, key: &str, params: Map<String, Value>) -> Result<Vec<Value>, AdvanceError> {
    let mut params = params;
    let mut all = Vec::new();
    loop {
        let page = ec2(ep, action, params.clone(), true)?;
        if let Some(items) = page.get(key).and_then(|v| v.as_array()) {
            all.extend(items.clone());
        }
        let token = page.get("NextToken").and_then(|v| v.as_str()).unwrap_or("");
        if token.is_empty() {
            break;
        }
        params.insert("NextToken".into(), json!(token));
    }
    Ok(all)
}

fn fetch_object_keys(ep: &AwsEndpoint, bucket: &str) -> Result<Vec<String>, AdvanceError> {
    let base = ep.endpoint.trim_end_matches('/');
    let mut keys = Vec::new();
    let mut token = String::new();
    loop {
        let mut url = format!("{base}/{bucket}?list-type=2");
        if !token.is_empty() {
            url.push_str("&continuation-token=");
            url.push_str(&aws_encode(&token));
        }
        ep.reading.enter();
        let resp = signed_request(
            "GET",
            &url,
            "s3",
            &ep.region,
            &ep.sig.access_key,
            &ep.sig.secret_key,
            ep.sig.session_token.as_deref(),
            &[],
            b"",
            false,
        )?;
        let doc = ec2_xml_to_boto(&resp.text()).unwrap_or(json!({}));
        match doc.get("Contents") {
            Some(Value::Array(items)) => {
                for item in items {
                    if let Some(key) = item.get("Key").and_then(|v| v.as_str()) {
                        keys.push(key.to_string());
                    }
                }
            }
            Some(item) => {
                if let Some(key) = item.get("Key").and_then(|v| v.as_str()) {
                    keys.push(key.to_string());
                }
            }
            None => {}
        }
        let truncated = doc
            .get("IsTruncated")
            .and_then(|v| v.as_str())
            .map(|s| s.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        token = doc
            .get("NextContinuationToken")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if !truncated || token.is_empty() {
            break;
        }
    }
    Ok(keys)
}

fn cached_object_keys(ep: &Arc<AwsEndpoint>, bucket: &str) -> Result<Vec<String>, AdvanceError> {
    let as_strings = |value: &Value| -> Vec<String> {
        value
            .as_array()
            .map(|items| items.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
            .unwrap_or_default()
    };
    // The wrapped _listObjects ignores its arguments after the first
    // call, the same way LazyExecutorTTLCache does. A refresh uses
    // the bucket from the call that started it.
    match cache_hit(ep, "objects", LIST_TTL) {
        CacheHit::Fresh(value) => Ok(as_strings(&value)),
        CacheHit::Stale(value) => {
            let bg = ep.clone();
            let bucket = bucket.to_string();
            std::thread::spawn(move || match fetch_object_keys(&bg, &bucket) {
                Ok(keys) => cache_store(&bg, "objects", json!(keys)),
                Err(_) => cache_clear_inflight(&bg, "objects"),
            });
            Ok(as_strings(&value))
        }
        CacheHit::Miss => {
            let keys = fetch_object_keys(ep, bucket)?;
            cache_store(ep, "objects", json!(keys.clone()));
            Ok(keys)
        }
    }
}

fn list_s3_objects(snap: &ProviderSnap, bucket: &str) -> Result<Vec<(String, BTreeMap<String, String>)>, AdvanceError> {
    let ep = aws_endpoint(snap)?;
    let base = ep.endpoint.trim_end_matches('/').to_string();
    let keys = cached_object_keys(&ep, bucket)?;
    let mut out = Vec::new();
    for key in keys {
        let tag_url = format!("{base}/{bucket}/{key}?tagging");
        ep.reading.enter();
        let tagged = signed_request(
            "GET",
            &tag_url,
            "s3",
            &ep.region,
            &ep.sig.access_key,
            &ep.sig.secret_key,
            ep.sig.session_token.as_deref(),
            &[],
            b"",
            false,
        )?;
        let tags = s3_tag_map(&tagged.text());
        out.push((key, tags));
    }
    Ok(out)
}

fn s3_tag_map(xml: &str) -> BTreeMap<String, String> {
    let doc = ec2_xml_to_boto(xml).unwrap_or(json!({}));
    let mut tags = Vec::new();
    if let Some(set) = doc.get("TagSet") {
        match set.get("Tag") {
            Some(Value::Array(items)) => tags.extend(items.clone()),
            Some(item) => tags.push(item.clone()),
            None => {}
        }
    }
    aws_tag_map(&json!({"Tags": tags}))
}

fn aws_tag_map(value: &Value) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let Some(tags) = value.get("Tags").and_then(|v| v.as_array()) else {
        return out;
    };
    for tag in tags {
        let key = tag.get("Key").and_then(|v| v.as_str()).unwrap_or("");
        if key.is_empty() {
            continue;
        }
        let val = match tag.get("Value") {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Number(n)) => n.to_string(),
            Some(Value::Bool(b)) => b.to_string(),
            _ => String::new(),
        };
        out.insert(key.to_string(), val);
    }
    out
}

fn json_tags(value: Option<&Value>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let Some(obj) = value.and_then(|v| v.as_object()) else {
        return out;
    };
    for (key, val) in obj {
        let text = match val {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            _ => continue,
        };
        out.insert(key.clone(), text);
    }
    out
}

fn state_text(value: &Value) -> String {
    // Query XML uses status/imageState; boto exposes those as State.
    let raw = value
        .get("State")
        .or_else(|| value.get("Status"))
        .or_else(|| value.get("ImageState"));
    match raw {
        Some(Value::String(s)) => s.to_ascii_lowercase(),
        Some(obj) => obj.get("Name").and_then(|v| v.as_str()).unwrap_or("").to_ascii_lowercase(),
        _ => String::new(),
    }
}

fn resource(kind: &str, id: &str, metadata: BTreeMap<String, String>, bucket: Option<&str>) -> Value {
    json!({
        "type": kind,
        "id": id,
        "unique_id": format!("{kind}-{id}"),
        "bucket_name": bucket,
        "metadata": metadata,
    })
}
