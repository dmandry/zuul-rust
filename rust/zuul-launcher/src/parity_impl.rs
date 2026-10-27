// Native parity with Zuul 14.3.0 drivers. Included from native.rs.

include!("host_codes.inc");

fn host_quota_code(host_type: &str) -> Option<String> {
    // host_key_re = ([a-z\d\-]+)\..*
    let key = host_type.split_once('.')?.0;
    HOST_CODES.iter().find(|(k, _)| *k == key).map(|(_, code)| (*code).to_string())
}

fn apply_volume_quota(quota: &mut Map<String, Value>, label: &Value) {
    let vtype = field_str(label, &["volume-type", "volume_type"]).unwrap_or_default();
    let size = json_f64(field(label, &["volume-size", "volume_size"]));
    let iops = json_f64(field(label, &["iops"]));
    let (iops_code, storage_code) = match vtype.as_str() {
        "io1" => (Some("L-B3A130E6"), Some("L-FD252861")),
        "io2" => (Some("L-8D977E7E"), Some("L-09BD8365")),
        "sc1" => (None, Some("L-17AF77E8")),
        "gp2" => (None, Some("L-D18FCD1D")),
        "gp3" => (None, Some("L-7A658B76")),
        "standard" => (None, Some("L-9CF3C2EB")),
        "st1" => (None, Some("L-82ACEF56")),
        _ => (None, None),
    };
    if let (Some(code), Some(n)) = (iops_code, iops) {
        quota.insert(code.into(), json!(n));
    }
    if let (Some(code), Some(n)) = (storage_code, size) {
        quota.insert(code.into(), json!(n));
    }
}

fn strip_nulls(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut out = Map::new();
            for (k, child) in map {
                if !child.is_null() {
                    out.insert(k.clone(), strip_nulls(child));
                }
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(strip_nulls).collect()),
        other => other.clone(),
    }
}

fn json_f64(v: Option<&Value>) -> Option<f64> {
    v.and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|n| n as f64)))
}

fn image_upload_sleep() {
    // AwsProviderEndpoint.IMAGE_UPLOAD_SLEEP is 30s. Crate tests skip the
    // wait; the binary sleeps.
    #[cfg(not(test))]
    std::thread::sleep(Duration::from_secs(30));
}

fn sorted_json(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut keys: Vec<String> = map.keys().cloned().collect();
            keys.sort();
            let mut out = Map::new();
            for key in keys {
                out.insert(key.clone(), sorted_json(&map[&key]));
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(sorted_json).collect()),
        other => other.clone(),
    }
}

fn sha256_hex_bytes(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(data))
}

fn create_fleet(
    ep: &AwsEndpoint,
    provider_name: &str,
    label: &Value,
    flavor: &Value,
    image_id: &str,
    tags: &Value,
) -> Result<Value, AdvanceError> {
    let template = ensure_launch_template(ep, provider_name, label, flavor)?;
    let fleet = flavor.get("fleet").cloned().unwrap_or(json!({}));
    let types = as_str_list(fleet.get("instance_types").or_else(|| fleet.get("instance-types")));
    let strategy = fleet
        .get("allocation_strategy")
        .or_else(|| fleet.get("allocation-strategy"))
        .and_then(|v| v.as_str())
        .unwrap_or("lowest-price")
        .to_string();
    let subnets = as_str_list(field(label, &["subnet-id", "subnet_ids", "subnet-ids"]));
    let mut overrides = Vec::new();
    let mut priority = 0i64;
    for instance_type in &types {
        let mut ebs = Map::new();
        ebs.insert("DeleteOnTermination".into(), json!(true));
        if let Some(size) = field(label, &["volume-size", "volume_size"]) {
            ebs.insert("VolumeSize".into(), size.clone());
        }
        if let Some(vt) = field_str(label, &["volume-type", "volume_type"]) {
            ebs.insert("VolumeType".into(), json!(vt));
        }
        if let Some(iops) = field(label, &["iops"]) {
            ebs.insert("Iops".into(), iops.clone());
        }
        if let Some(tp) = field(label, &["throughput"]) {
            ebs.insert("Throughput".into(), tp.clone());
        }
        if let Some(kms) = field_str(label, &["kms-key-id", "kms_key_id"]) {
            ebs.insert("KmsKeyId".into(), json!(kms));
            ebs.insert("Encrypted".into(), json!(true));
        }
        let mut over = Map::new();
        over.insert("ImageId".into(), json!(image_id));
        over.insert("InstanceType".into(), json!(instance_type));
        over.insert(
            "BlockDeviceMappings".into(),
            json!([{"DeviceName": "/dev/sda1", "Ebs": ebs}]),
        );
        if strategy == "prioritized" || strategy == "capacity-optimized-prioritized" {
            over.insert("Priority".into(), json!(priority));
            priority += 1;
        }
        if subnets.is_empty() {
            overrides.push(Value::Object(over));
        } else {
            for subnet in &subnets {
                let mut one = over.clone();
                one.insert("SubnetId".into(), json!(subnet));
                overrides.push(Value::Object(one));
            }
        }
    }
    let spot = field_str(flavor, &["market-type", "market_type"]).unwrap_or_default() == "spot";
    let mut params = Map::new();
    if spot {
        params.insert(
            "SpotOptions".into(),
            json!({"AllocationStrategy": strategy}),
        );
        params.insert(
            "TargetCapacitySpecification".into(),
            json!({"TotalTargetCapacity": 1, "DefaultTargetCapacityType": "spot"}),
        );
    } else {
        params.insert(
            "OnDemandOptions".into(),
            json!({"AllocationStrategy": strategy}),
        );
        params.insert(
            "TargetCapacitySpecification".into(),
            json!({"TotalTargetCapacity": 1, "DefaultTargetCapacityType": "on-demand"}),
        );
    }
    params.insert(
        "LaunchTemplateConfigs".into(),
        json!([{
            "LaunchTemplateSpecification": {
                "LaunchTemplateName": template,
                "Version": "$Latest"
            },
            "Overrides": overrides
        }]),
    );
    params.insert("Type".into(), json!("instant"));
    params.insert(
        "TagSpecifications".into(),
        json!([
            {"ResourceType": "instance", "Tag": tag_list(tags)},
            {"ResourceType": "volume", "Tag": tag_list(tags)}
        ]),
    );
    let resp = ec2(ep, "CreateFleet", params, false)?;
    if let Some(id) = resp
        .pointer("/Instances/0/InstanceIds/0")
        .and_then(|v| v.as_str())
    {
        return Ok(json!({"InstanceId": id, "State": {"Name": "creating"}}));
    }
    let code = resp
        .pointer("/Errors/0/ErrorCode")
        .or_else(|| resp.pointer("/ErrorSet/0/ErrorCode"))
        .and_then(|v| v.as_str())
        .unwrap_or("CreateFleet");
    let message = resp
        .pointer("/Errors/0/ErrorMessage")
        .or_else(|| resp.pointer("/ErrorSet/0/ErrorMessage"))
        .and_then(|v| v.as_str())
        .unwrap_or("empty instance list");
    Err(AdvanceError::Other(format!("Couldn't create fleet instance because of {code}: {message}")))
}

fn ensure_launch_template(
    ep: &AwsEndpoint,
    provider_name: &str,
    label: &Value,
    flavor: &Value,
) -> Result<String, AdvanceError> {
    let mut data = Map::new();
    data.insert(
        "KeyName".into(),
        field_str(label, &["key-name", "key_name"]).map(|k| json!(k)).unwrap_or(Value::Null),
    );
    data.insert(
        "MaintenanceOptions".into(),
        json!({"AutoRecovery": "disabled"}),
    );
    let groups = as_str_list(field(label, &["security-group-id", "security_group_ids", "security-group-ids"]));
    if !groups.is_empty() {
        data.insert("SecurityGroupIds".into(), json!(groups));
    }
    if let Some(tokens) = field_str(label, &["imds-http-tokens", "imds_http_tokens"]) {
        if tokens == "required" || tokens == "optional" {
            data.insert(
                "MetadataOptions".into(),
                json!({
                    "HttpTokens": tokens,
                    "HttpEndpoint": "enabled",
                    "HttpProtocolIpv6": "enabled"
                }),
            );
        }
    }
    if field(flavor, &["nested-virtualization", "nested_virtualization"]).and_then(|v| v.as_bool()) == Some(true) {
        data.insert("CpuOptions".into(), json!({"NestedVirtualization": "enabled"}));
    }
    if let Some(user) = field_str(label, &["userdata", "user-data"]) {
        data.insert(
            "UserData".into(),
            json!(base64::engine::general_purpose::STANDARD.encode(user.as_bytes())),
        );
    }
    // Hash matches json.dumps(args, sort_keys=True) before the name is
    // added. Tag order is insertion order: system id, then provider.
    let args = json!({
        "LaunchTemplateData": data,
        "TagSpecifications": [{
            "ResourceType": "launch-template",
            "Tags": [
                {"Key": "zuul_system_id", "Value": system_id()},
                {"Key": "zuul_provider_name", "Value": provider_name}
            ]
        }]
    });
    let name = format!(
        "zuul-launch-template-{}",
        sha256_hex_bytes(serde_json::to_string(&sorted_json(&args)).unwrap_or_default().as_bytes())
    );
    let listed = ec2(ep, "DescribeLaunchTemplates", Map::new(), true).unwrap_or(json!({}));
    let exists = listed
        .get("LaunchTemplates")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter().any(|t| t.get("LaunchTemplateName").and_then(|v| v.as_str()) == Some(name.as_str()))
        })
        .unwrap_or(false);
    if exists {
        return Ok(name);
    }
    let mut params = Map::new();
    if let Some(obj) = args.as_object() {
        for (k, v) in obj {
            params.insert(k.clone(), strip_nulls(v));
        }
    }
    params.insert("LaunchTemplateName".into(), json!(name));
    match ec2(ep, "CreateLaunchTemplate", params, false) {
        Ok(_) => Ok(name),
        Err(err) => {
            if err.to_string().contains("InvalidLaunchTemplateName.AlreadyExistsException") {
                Ok(name)
            } else {
                Err(err)
            }
        }
    }
}

fn service_quotas(ep: &AwsEndpoint, service: &str) -> Result<Map<String, Value>, AdvanceError> {
    let cache_key = format!("quotas-{service}");
    let load = |ep: &AwsEndpoint, service: &str| -> Result<Value, AdvanceError> {
        let url = if ep.endpoint.contains("amazonaws.com") {
            format!("https://servicequotas.{}.amazonaws.com/", ep.region)
        } else {
            ep.endpoint.trim_end_matches('/').to_string()
        };
        let mut token: Option<String> = None;
        let mut quotas = Map::new();
        loop {
            let mut body = Map::new();
            body.insert("ServiceCode".into(), json!(service));
            body.insert("MaxResults".into(), json!(100));
            if let Some(t) = &token {
                body.insert("NextToken".into(), json!(t));
            }
            let raw = serde_json::to_vec(&Value::Object(body)).unwrap_or_default();
            ep.quota_limit.enter();
            let resp = signed_request(
                "POST",
                &url,
                "servicequotas",
                &ep.region,
                &ep.sig.access_key,
                &ep.sig.secret_key,
                ep.sig.session_token.as_deref(),
                &[
                    ("Content-Type", "application/x-amz-json-1.1"),
                    ("X-Amz-Target", "ServiceQuotasV20190624.ListServiceQuotas"),
                ],
                &raw,
                false,
            )?;
            let doc = resp.json().map_err(err_other)?;
            if let Some(arr) = doc.get("Quotas").and_then(|v| v.as_array()) {
                for quota in arr {
                    if let (Some(code), Some(value)) = (
                        quota.get("QuotaCode").and_then(|v| v.as_str()),
                        quota.get("Value"),
                    ) {
                        quotas.insert(code.to_string(), value.clone());
                    }
                }
            }
            token = doc
                .get("NextToken")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string());
            if token.is_none() {
                break;
            }
        }
        Ok(Value::Object(quotas))
    };
    let page = match cache_hit(ep, &cache_key, QUOTA_TTL) {
        CacheHit::Fresh(v) => v,
        CacheHit::Stale(v) => {
            let service = service.to_string();
            // Endpoint is not behind Arc here. Refresh on the next call
            // by storing the stale value with refreshing cleared after a
            // synchronous update when the caller is the limits path.
            let fresh = load(ep, &service)?;
            cache_store(ep, &cache_key, fresh.clone());
            let _ = v;
            fresh
        }
        CacheHit::Miss => {
            let fresh = load(ep, service)?;
            cache_store(ep, &cache_key, fresh.clone());
            fresh
        }
    };
    Ok(page.as_object().cloned().unwrap_or_default())
}

pub fn endpoint_limits(provider_name: &str) -> Option<crate::model::QuotaInformation> {
    let snap = provider_by_name(provider_name).ok()?;
    let mut quota = BTreeMap::new();
    match snap.driver.as_str() {
        "aws" => {
            let ep = aws_endpoint(&snap).ok()?;
            for service in ["ec2", "ebs"] {
                if let Ok(codes) = service_quotas(&ep, service) {
                    for (code, value) in codes {
                        if let Some(n) = value.as_f64().or_else(|| value.as_i64().map(|i| i as f64)) {
                            let stored = if service == "ebs" && code_is_storage(&code) {
                                n * 1000.0
                            } else {
                                n
                            };
                            quota.insert(code, stored);
                        }
                    }
                }
            }
        }
        "azure" => {
            if let Ok(limits) = azure_usage_limits(&snap) {
                quota = limits;
            }
        }
        "kubernetes" => {}
        "openstack" => {
            if let Ok(limits) = os_absolute_limits(&snap) {
                quota = limits;
            }
        }
        _ => return None,
    }
    Some(crate::model::QuotaInformation {
        default: f64::INFINITY,
        quota,
    })
}

fn code_is_storage(code: &str) -> bool {
    matches!(
        code,
        "L-FD252861" | "L-09BD8365" | "L-17AF77E8" | "L-D18FCD1D" | "L-7A658B76" | "L-9CF3C2EB" | "L-82ACEF56"
    )
}

fn azure_usage_limits(snap: &ProviderSnap) -> Result<BTreeMap<String, f64>, AdvanceError> {
    let (conn, region) = azure_cloud(snap)?;
    let sub = azure_sub(&conn)?;
    let url = format!(
        "{}/subscriptions/{sub}/providers/Microsoft.Compute/locations/{region}/usages?api-version=2020-12-01",
        azure_arm_base()
    );
    let doc = azure_request(&conn, "GET", &url, None)?;
    let mut cores = f64::INFINITY;
    let mut spot_cores = f64::INFINITY;
    let mut instances = f64::INFINITY;
    let items = doc.get("value").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    for item in items {
        let name = item.pointer("/name/value").and_then(|v| v.as_str()).unwrap_or("");
        let limit = item.get("limit").and_then(|v| v.as_f64()).unwrap_or(f64::INFINITY);
        if name == "cores" {
            cores = limit;
        } else if name == "lowPriorityCores" {
            spot_cores = limit;
        } else if name == "virtualMachines" {
            instances = limit;
        }
    }
    let mut out = BTreeMap::new();
    out.insert("cores".into(), cores);
    out.insert("spot_cores".into(), spot_cores);
    out.insert("instances".into(), instances);
    Ok(out)
}

fn azure_quota_for_label(snap: &ProviderSnap, args: &Value) -> Result<Value, AdvanceError> {
    let label_name = args.get("label").and_then(|v| v.as_str()).unwrap_or("");
    let (_label, flavor, _) = label_flavor_image(snap, label_name)?;
    let vm_size = field_str(&flavor, &["vm-size", "vm_size"]).unwrap_or_default();
    let spot = field_str(&flavor, &["priority"]).unwrap_or_default() == "spot";
    let (conn, region) = azure_cloud(snap)?;
    let sub = azure_sub(&conn)?;
    let url = format!(
        "{}/subscriptions/{sub}/providers/Microsoft.Compute/skus?api-version=2019-04-01",
        azure_arm_base()
    );
    let doc = azure_request(&conn, "GET", &url, None).unwrap_or(json!({"value": []}));
    let mut cores = 0.0;
    let mut spot_cores = 0.0;
    let mut ram = 0.0;
    let mut found = false;
    if let Some(arr) = doc.get("value").and_then(|v| v.as_array()) {
        for sku in arr {
            let name = sku.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let locations = sku.get("locations").and_then(|v| v.as_array());
            let in_region = locations
                .map(|locs| {
                    locs.iter().any(|l| l.as_str().map(|s| s.eq_ignore_ascii_case(&region)).unwrap_or(false))
                })
                .unwrap_or(true);
            if name != vm_size || !in_region {
                continue;
            }
            found = true;
            if let Some(caps) = sku.get("capabilities").and_then(|v| v.as_array()) {
                for cap in caps {
                    let cname = cap.get("name").and_then(|v| v.as_str()).unwrap_or("");
                    let raw = cap.get("value").and_then(|v| v.as_str()).unwrap_or("0");
                    if cname == "vCPUs" {
                        let n = raw.parse::<f64>().unwrap_or(0.0);
                        if spot {
                            spot_cores = n;
                        } else {
                            cores = n;
                        }
                    }
                    if cname == "MemoryGB" {
                        ram = raw.parse::<f64>().unwrap_or(0.0) * 1024.0;
                    }
                }
            }
            break;
        }
    }
    if !found {
        return Ok(json!({"default": null, "quota": {"instances": 1}}));
    }
    let mut quota = Map::new();
    quota.insert("cores".into(), json!(cores));
    quota.insert("spot_cores".into(), json!(spot_cores));
    quota.insert("ram".into(), json!(ram));
    quota.insert("instances".into(), json!(1));
    Ok(json!({"default": null, "quota": quota}))
}

fn os_absolute_limits(snap: &ProviderSnap) -> Result<BTreeMap<String, f64>, AdvanceError> {
    let tok = os_token(snap)?;
    if tok.compute.is_empty() {
        return Ok(BTreeMap::new());
    }
    let doc = os_http(&tok, "GET", &format!("{}/limits", tok.compute), None)?;
    let abs = doc.get("limits").and_then(|v| v.get("absolute")).cloned().unwrap_or(json!({}));
    let mut out = BTreeMap::new();
    if let Some(n) = json_f64(abs.get("maxTotalInstances")) {
        out.insert("instances".into(), n);
    }
    if let Some(n) = json_f64(abs.get("maxTotalCores")) {
        out.insert("cores".into(), n);
    }
    if let Some(n) = json_f64(abs.get("maxTotalRAMSize")) {
        out.insert("ram".into(), n);
    }
    Ok(out)
}

pub fn root_volume(provider_name: &str, instance_id: &str) -> Result<Option<String>, AdvanceError> {
    let snap = provider_by_name(provider_name)?;
    let ep = aws_endpoint(&snap)?;
    let mut params = Map::new();
    params.insert("InstanceId".into(), json!([instance_id]));
    let resp = ec2(&ep, "DescribeInstances", params, true)?;
    let instance = resp
        .pointer("/Reservations/0/Instances/0")
        .cloned()
        .unwrap_or(Value::Null);
    let root = instance.get("RootDeviceName").and_then(|v| v.as_str()).unwrap_or("");
    let maps = instance
        .get("BlockDeviceMappings")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    for attachment in maps {
        if attachment.get("DeviceName").and_then(|v| v.as_str()) == Some(root) {
            if let Some(id) = attachment.pointer("/Ebs/VolumeId").and_then(|v| v.as_str()) {
                return Ok(Some(id.to_string()));
            }
        }
    }
    Ok(None)
}

pub fn account_id(provider_name: &str) -> Result<String, AdvanceError> {
    let snap = provider_by_name(provider_name)?;
    let ep = aws_endpoint(&snap)?;
    let url = if ep.endpoint.contains("amazonaws.com") {
        format!("https://sts.{}.amazonaws.com/", ep.region)
    } else {
        format!("{}/", ep.endpoint.trim_end_matches('/'))
    };
    let body = "Action=GetCallerIdentity&Version=2011-06-15";
    let resp = signed_request(
        "POST",
        &url,
        "sts",
        &ep.region,
        &ep.sig.access_key,
        &ep.sig.secret_key,
        ep.sig.session_token.as_deref(),
        &[("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")],
        body.as_bytes(),
        true,
    )?;
    let text = resp.text();
    if let Some(start) = text.find("<Account>") {
        if let Some(end) = text[start + 9..].find("</Account>") {
            return Ok(text[start + 9..start + 9 + end].to_string());
        }
    }
    Err(AdvanceError::Other("GetCallerIdentity returned no account".into()))
}

pub fn delete_uploaded_image(provider_name: &str, external_id: &str) -> Result<(), AdvanceError> {
    let snap = provider_by_name(provider_name)?;
    match snap.driver.as_str() {
        "aws" => delete_aws_image(&snap, external_id),
        "azure" => {
            let (conn, _) = azure_cloud(&snap)?;
            let url = if external_id.starts_with("http") {
                format!("{external_id}?api-version=2020-12-01")
            } else {
                format!("{}{external_id}?api-version=2020-12-01", azure_arm_base())
            };
            azure_request(&conn, "DELETE", &url, None)?;
            Ok(())
        }
        "openstack" => {
            let tok = os_token(&snap)?;
            os_http(&tok, "DELETE", &format!("{}/v2/images/{external_id}", tok.image), None)?;
            Ok(())
        }
        _ => Ok(()),
    }
}

fn delete_aws_image(snap: &ProviderSnap, external_id: &str) -> Result<(), AdvanceError> {
    let ep = aws_endpoint(snap)?;
    let mut params = Map::new();
    params.insert("ImageId".into(), json!([external_id]));
    let listed = ec2(&ep, "DescribeImages", params, true).unwrap_or(json!({}));
    let mut snaps = Vec::new();
    if let Some(images) = listed.get("Images").and_then(|v| v.as_array()) {
        for image in images {
            if image.get("ImageId").and_then(|v| v.as_str()) != Some(external_id) {
                continue;
            }
            if let Some(bdm) = image.get("BlockDeviceMappings").and_then(|v| v.as_array()) {
                for disk in bdm {
                    if let Some(id) = disk.pointer("/Ebs/SnapshotId").and_then(|v| v.as_str()) {
                        snaps.push(id.to_string());
                    }
                }
            }
        }
    }
    let mut dereg = Map::new();
    dereg.insert("ImageId".into(), json!(external_id));
    let _ = ec2(&ep, "DeregisterImage", dereg, false);
    for id in snaps {
        delete_snapshot_id(&ep, &id);
    }
    Ok(())
}

pub fn delete_volume(provider_name: &str, volume_id: &str) -> Result<(), AdvanceError> {
    let snap = provider_by_name(provider_name)?;
    let ep = aws_endpoint(&snap)?;
    let mut params = Map::new();
    params.insert("VolumeId".into(), json!(volume_id));
    match ec2(&ep, "DeleteVolume", params, false) {
        Err(err) if err.to_string().contains("NotFound") => Ok(()),
        other => other.map(|_| ()),
    }
}

fn upload_tags(image: &Value, upload_uuid: &str) -> Value {
    let mut tags = Map::new();
    if let Some(obj) = image.get("tags").and_then(|v| v.as_object()) {
        for (k, v) in obj {
            tags.insert(k.clone(), v.clone());
        }
    }
    tags.insert("zuul_system_id".into(), json!(system_id()));
    tags.insert("zuul_upload_uuid".into(), json!(upload_uuid));
    Value::Object(tags)
}

fn tagging_header(tags: &Value) -> String {
    let mut pairs = Vec::new();
    if let Some(obj) = tags.as_object() {
        for (k, v) in obj {
            let value = match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            pairs.push(format!("{}={}", aws_encode(k), aws_encode(&value)));
        }
    }
    pairs.join("&")
}

pub fn upload_sdk_run(args: &Value) -> Result<Value, AdvanceError> {
    let provider_name = args.get("provider").and_then(|v| v.as_str()).unwrap_or("");
    let snap = provider_by_name(provider_name)?;
    let mut args = args.clone();
    let kind = args.get("kind").and_then(|v| v.as_str()).unwrap_or("upload").to_string();
    let upload_uuid = args.get("upload_uuid").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let wanted = args.get("image_name").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let image = snap
        .images
        .values()
        .find(|img| {
            let canon = img.get("canonical_name").and_then(|v| v.as_str()).unwrap_or("");
            let name = img.get("name").and_then(|v| v.as_str()).unwrap_or("");
            !wanted.is_empty() && (canon == wanted || name == wanted)
        })
        .cloned()
        .unwrap_or(json!({}));
    let short = image.get("name").and_then(|v| v.as_str()).unwrap_or(upload_uuid.as_str());
    let image_name = if upload_uuid.is_empty() || short.ends_with(upload_uuid.as_str()) {
        short.to_string()
    } else {
        format!("{short}-{upload_uuid}")
    };
    if let Some(url) = args.get("artifact_url").and_then(|v| v.as_str()).map(|s| s.to_string()) {
        let obj = args.as_object_mut().unwrap();
        if let Some(rest) = url.strip_prefix("s3://") {
            if let Some((bucket, key)) = rest.split_once('/') {
                obj.insert("bucket".into(), json!(bucket));
                obj.insert("object_key".into(), json!(key));
            }
        } else if url.starts_with("arn:") {
            let parts: Vec<&str> = url.split(':').collect();
            if parts.len() > 5 {
                if let Some(id) = parts[5].split('/').nth(1) {
                    obj.insert("snapshot_id".into(), json!(id));
                }
            }
        }
    }
    let source_region_missing = args.get("source_region").and_then(|v| v.as_str()).unwrap_or("").is_empty();
    let source_provider = args.get("source_provider").and_then(|v| v.as_str()).map(|s| s.to_string());
    if source_region_missing {
        if let Some(sp) = source_provider.as_deref() {
            if let Ok(src) = provider_by_name(sp) {
                args.as_object_mut().unwrap().insert("source_region".into(), json!(src.region));
            }
        }
    }
    let format = args.get("image_format").and_then(|v| v.as_str()).unwrap_or("raw").to_string();
    let tags = upload_tags(&image, &upload_uuid);
    let external = match snap.driver.as_str() {
        "aws" => aws_upload_job(&snap, provider_name, &kind, &args, &image_name, &format, &image, &tags)?,
        "azure" => azure_upload_job(&snap, &args, &image_name, &tags)?,
        "openstack" => os_upload_job(&snap, &kind, &args, &image_name, &format, &tags)?,
        _ => return Err(AdvanceError::Other(format!("no upload for {}", snap.driver))),
    };
    Ok(json!({"external_id": external}))
}

fn provider_image_method(image: &Value) -> String {
    field_str(image, &["import-method", "import_method"]).unwrap_or_else(|| "snapshot".into())
}

fn aws_upload_job(
    snap: &ProviderSnap,
    provider_name: &str,
    kind: &str,
    args: &Value,
    image_name: &str,
    format: &str,
    image: &Value,
    tags: &Value,
) -> Result<String, AdvanceError> {
    let ep = aws_endpoint(snap)?;
    if kind == "copy" {
        let source = args.get("source_external_id").and_then(|v| v.as_str()).unwrap_or("");
        let source_region = args
            .get("source_region")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        return copy_image(&ep, image_name, source, source_region, tags);
    }
    if kind == "import" {
        let snapshot_id = args.get("snapshot_id").and_then(|v| v.as_str());
        if let Some(id) = snapshot_id {
            return register_from_snapshot(&ep, image, image_name, id, tags);
        }
        let bucket = args.get("bucket").and_then(|v| v.as_str()).unwrap_or("");
        let key = args.get("object_key").and_then(|v| v.as_str()).unwrap_or("");
        let method = provider_image_method(image);
        if method == "image" && field_str(image, &["imds-support", "imds_support"]).as_deref() == Some("v2.0") {
            return Err(AdvanceError::Other("IMDSv2 requires 'snapshot' import method".into()));
        }
        if method == "ebs-direct" {
            return Err(AdvanceError::Other("no import job for ebs-direct".into()));
        }
        return if method == "image" {
            import_image(provider_name, &ep, image, image_name, format, bucket, key, tags, false)
        } else {
            import_snapshot(provider_name, &ep, image, image_name, format, bucket, key, tags, false)
        };
    }
    let path = args.get("path").and_then(|v| v.as_str()).unwrap_or("");
    let method = provider_image_method(image);
    if method == "ebs-direct" {
        let bytes = std::fs::read(path).map_err(|e| AdvanceError::Other(e.to_string()))?;
        let gib = ((bytes.len() as i64) + (1024 * 1024 * 1024) - 1) / (1024 * 1024 * 1024);
        let snapshot_id = ebs_put_snapshot(provider_name, gib.max(1), &[bytes], tags)?;
        return register_from_snapshot(&ep, image, image_name, &snapshot_id, tags);
    }
    let conn = connection_of(&snap.connection)?;
    let bucket = opt(&conn, "bucket_name").unwrap_or_default();
    let key = format!("{image_name}.{format}");
    let bytes = std::fs::read(path).map_err(|e| AdvanceError::Other(e.to_string()))?;
    s3_put_tagged(provider_name, &bucket, &key, &bytes, "application/octet-stream", Some(&tagging_header(tags)))?;
    let image_id = if method == "image" {
        import_image(provider_name, &ep, image, image_name, format, &bucket, &key, tags, true)?
    } else {
        import_snapshot(provider_name, &ep, image, image_name, format, &bucket, &key, tags, true)?
    };
    Ok(image_id)
}

fn import_snapshot(
    provider_name: &str,
    ep: &AwsEndpoint,
    image: &Value,
    image_name: &str,
    format: &str,
    bucket: &str,
    key: &str,
    tags: &Value,
    delete_object: bool,
) -> Result<String, AdvanceError> {
    let task_id = start_import_snapshot(ep, format, bucket, key, tags)?;
    let task = wait_import(ep, "DescribeImportSnapshotTasks", "ImportSnapshotTasks", &task_id, "SnapshotTaskDetail")?;
    if delete_object {
        let _ = s3_delete(provider_name, bucket, key);
    }
    let status = task
        .pointer("/SnapshotTaskDetail/Status")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if status != "completed" {
        return Err(AdvanceError::Other(format!("Error uploading image: {task}")));
    }
    let snapshot_id = task
        .pointer("/SnapshotTaskDetail/SnapshotId")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let _ = create_tags(ep, &snapshot_id, tags);
    register_from_snapshot(ep, image, image_name, &snapshot_id, tags)
}

fn import_image(
    provider_name: &str,
    ep: &AwsEndpoint,
    image: &Value,
    _image_name: &str,
    format: &str,
    bucket: &str,
    key: &str,
    tags: &Value,
    delete_object: bool,
) -> Result<String, AdvanceError> {
    let arch = field_str(image, &["architecture"]).unwrap_or_else(|| "x86_64".into());
    let mut params = Map::new();
    params.insert("Architecture".into(), json!(arch));
    params.insert(
        "DiskContainers".into(),
        json!([{"Format": format, "UserBucket": {"S3Bucket": bucket, "S3Key": key}}]),
    );
    params.insert(
        "TagSpecifications".into(),
        json!([{"ResourceType": "import-image-task", "Tag": tag_list(tags)}]),
    );
    let started = ec2(ep, "ImportImage", params, false)?;
    let task_id = started
        .get("ImportTaskId")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let task = wait_import(ep, "DescribeImportImageTasks", "ImportImageTasks", &task_id, "")?;
    if delete_object {
        let _ = s3_delete(provider_name, bucket, key);
    }
    let status = task.get("Status").and_then(|v| v.as_str()).unwrap_or("").to_ascii_lowercase();
    if status != "completed" {
        return Err(AdvanceError::Other(format!("Error uploading image: {task}")));
    }
    let image_id = task.get("ImageId").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let _ = create_tags(ep, &image_id, tags);
    if let Some(snap) = task.pointer("/SnapshotDetails/0/SnapshotId").and_then(|v| v.as_str()) {
        let _ = create_tags(ep, snap, tags);
    }
    Ok(image_id)
}

fn start_import_snapshot(
    ep: &AwsEndpoint,
    format: &str,
    bucket: &str,
    key: &str,
    tags: &Value,
) -> Result<String, AdvanceError> {
    let mut params = Map::new();
    params.insert(
        "DiskContainer".into(),
        json!({"Format": format, "UserBucket": {"S3Bucket": bucket, "S3Key": key}}),
    );
    params.insert(
        "TagSpecifications".into(),
        json!([{"ResourceType": "import-snapshot-task", "Tag": tag_list(tags)}]),
    );
    let started = ec2(ep, "ImportSnapshot", params, false)?;
    Ok(started.get("ImportTaskId").and_then(|v| v.as_str()).unwrap_or("").to_string())
}

fn wait_import(
    ep: &AwsEndpoint,
    action: &str,
    list_key: &str,
    task_id: &str,
    _detail: &str,
) -> Result<Value, AdvanceError> {
    loop {
        image_upload_sleep();
        let mut params = Map::new();
        params.insert("ImportTaskIds".into(), json!([task_id]));
        let page = ec2(ep, action, params, true)?;
        if let Some(task) = page.get(list_key).and_then(|v| v.as_array()).and_then(|a| a.first()) {
            let status = task
                .pointer("/SnapshotTaskDetail/Status")
                .or_else(|| task.get("Status"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_ascii_lowercase();
            if status == "completed" || status == "deleted" {
                return Ok(task.clone());
            }
        }
    }
}

fn copy_image(
    ep: &AwsEndpoint,
    image_name: &str,
    source_ami: &str,
    source_region: &str,
    tags: &Value,
) -> Result<String, AdvanceError> {
    let mut params = Map::new();
    params.insert("Name".into(), json!(image_name));
    params.insert("SourceImageId".into(), json!(source_ami));
    params.insert("SourceRegion".into(), json!(source_region));
    params.insert("CopyImageTags".into(), json!(false));
    params.insert(
        "TagSpecifications".into(),
        json!([{"ResourceType": "image", "Tag": tag_list(tags)}]),
    );
    let resp = ec2(ep, "CopyImage", params, false)?;
    let image_id = resp.get("ImageId").and_then(|v| v.as_str()).unwrap_or("").to_string();
    loop {
        image_upload_sleep();
        let mut q = Map::new();
        q.insert("ImageId".into(), json!([image_id.clone()]));
        let listed = ec2(ep, "DescribeImages", q, true)?;
        if let Some(ami) = listed.get("Images").and_then(|v| v.as_array()).and_then(|a| a.first()) {
            let state = ami.get("State").and_then(|v| v.as_str()).unwrap_or("").to_ascii_lowercase();
            if state == "available" {
                return Ok(image_id);
            }
            if state == "failed" {
                return Err(AdvanceError::Other(format!("Image copy failed: {ami}")));
            }
        }
    }
}

fn register_from_snapshot(
    ep: &AwsEndpoint,
    image: &Value,
    image_name: &str,
    snapshot_id: &str,
    tags: &Value,
) -> Result<String, AdvanceError> {
    let mut ebs = Map::new();
    ebs.insert("DeleteOnTermination".into(), json!(true));
    ebs.insert("SnapshotId".into(), json!(snapshot_id));
    if let Some(size) = field(image, &["volume-size", "volume_size"]) {
        ebs.insert("VolumeSize".into(), size.clone());
    } else {
        ebs.insert("VolumeSize".into(), json!(8));
    }
    if let Some(vt) = field_str(image, &["volume-type", "volume_type"]) {
        ebs.insert("VolumeType".into(), json!(vt));
    }
    if let Some(iops) = field(image, &["iops"]) {
        ebs.insert("Iops".into(), iops.clone());
    }
    if let Some(tp) = field(image, &["throughput"]) {
        ebs.insert("Throughput".into(), tp.clone());
    }
    let mut params = Map::new();
    params.insert(
        "Architecture".into(),
        json!(field_str(image, &["architecture"]).unwrap_or_else(|| "x86_64".into())),
    );
    params.insert(
        "BlockDeviceMapping".into(),
        json!([{"DeviceName": "/dev/sda1", "Ebs": ebs}]),
    );
    params.insert("RootDeviceName".into(), json!("/dev/sda1"));
    params.insert("VirtualizationType".into(), json!("hvm"));
    let ena = field(image, &["ena-support", "ena_support"]).and_then(|v| v.as_bool()).unwrap_or(true);
    params.insert("EnaSupport".into(), json!(ena));
    params.insert("Name".into(), json!(image_name));
    params.insert(
        "TagSpecifications".into(),
        json!([{"ResourceType": "image", "Tag": tag_list(tags)}]),
    );
    if field_str(image, &["imds-support", "imds_support"]).as_deref() == Some("v2.0") {
        params.insert("ImdsSupport".into(), json!("v2.0"));
    }
    let resp = ec2(ep, "RegisterImage", params, false)?;
    Ok(resp.get("ImageId").and_then(|v| v.as_str()).unwrap_or("").to_string())
}

fn create_tags(ep: &AwsEndpoint, resource: &str, tags: &Value) -> Result<(), AdvanceError> {
    let mut params = Map::new();
    params.insert("ResourceId".into(), json!([resource]));
    params.insert("Tag".into(), tag_list(tags));
    ec2(ep, "CreateTags", params, false)?;
    Ok(())
}

fn azure_upload_job(snap: &ProviderSnap, args: &Value, image_name: &str, tags: &Value) -> Result<String, AdvanceError> {
    let path = args.get("path").and_then(|v| v.as_str()).unwrap_or("");
    let bytes = std::fs::read(path).map_err(|e| AdvanceError::Other(e.to_string()))?;
    let (conn, region) = azure_cloud(snap)?;
    let sub = azure_sub(&conn)?;
    let rg = args.get("resource_group").and_then(|v| v.as_str()).unwrap_or("zuul");
    let disk_url = format!(
        "{}/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Compute/disks/{image_name}?api-version=2020-06-30",
        azure_arm_base()
    );
    let disk = json!({
        "location": region,
        "tags": tags,
        "properties": {"creationData": {"createOption": "Upload", "uploadSizeBytes": bytes.len()}}
    });
    azure_request(&conn, "PUT", &disk_url, Some(&disk))?;
    let grant = json!({"access": "Write", "durationInSeconds": 86400});
    let access = azure_request(
        &conn,
        "POST",
        &format!(
            "{}/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Compute/disks/{image_name}/beginGetAccess?api-version=2020-06-30",
            azure_arm_base()
        ),
        Some(&grant),
    )?;
    let sas = access
        .pointer("/properties/output/accessSAS")
        .or_else(|| access.get("accessSAS"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if !sas.is_empty() {
        azure_page_upload(&sas, &bytes)?;
    }
    let _ = azure_request(
        &conn,
        "POST",
        &format!(
            "{}/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Compute/disks/{image_name}/endGetAccess?api-version=2020-06-30",
            azure_arm_base()
        ),
        Some(&json!({})),
    );
    let disk_id = format!("/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Compute/disks/{image_name}");
    let image_body = json!({
        "location": region,
        "tags": tags,
        "properties": {
            "hyperVGeneration": "V2",
            "storageProfile": {
                "osDisk": {
                    "osType": "Linux",
                    "managedDisk": {"id": disk_id},
                    "osState": "Generalized"
                },
                "zoneResilient": true
            }
        }
    });
    let image_url = format!(
        "{}/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Compute/images/{image_name}?api-version=2020-12-01",
        azure_arm_base()
    );
    let image = azure_request(&conn, "PUT", &image_url, Some(&image_body))?;
    let _ = azure_request(&conn, "DELETE", &disk_url, None);
    Ok(image
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or(&disk_id)
        .to_string())
}

fn azure_page_upload(sas: &str, bytes: &[u8]) -> Result<(), AdvanceError> {
    let page = 4 * 1024 * 1024;
    let mut start = 0usize;
    while start < bytes.len() {
        let end = (start + page).min(bytes.len());
        let chunk = &bytes[start..end];
        let mut url = sas.to_string();
        if !url.contains("comp=page") {
            url.push(if url.contains('?') { '&' } else { '?' });
            url.push_str("comp=page");
        }
        let range = format!("bytes={start}-{}", end - 1);
        let len = chunk.len().to_string();
        let mut attempt = 0;
        loop {
            let resp = exchange(
                "PUT",
                &url,
                &[
                    ("x-ms-blob-type", "PageBlob"),
                    ("x-ms-page-write", "Update"),
                    ("Content-Length", len.as_str()),
                    ("Range", range.as_str()),
                ],
                chunk,
                HTTP_TIMEOUT,
            );
            match resp {
                Ok(r) if r.status < 400 => break,
                Ok(r) if attempt < 2 => {
                    attempt += 1;
                    std::thread::sleep(Duration::from_secs(2 * (attempt as u64 - 1).max(0)));
                    let _ = r;
                }
                Ok(r) => return Err(err_other(format!("azure page HTTP {}", r.status))),
                Err(e) if attempt < 2 => {
                    attempt += 1;
                    std::thread::sleep(Duration::from_secs(2 * (attempt as u64 - 1).max(0)));
                    let _ = e;
                }
                Err(e) => return Err(err_other(e)),
            }
        }
        start = end;
    }
    Ok(())
}

fn os_upload_job(
    snap: &ProviderSnap,
    kind: &str,
    args: &Value,
    image_name: &str,
    format: &str,
    tags: &Value,
) -> Result<String, AdvanceError> {
    let tok = os_token(snap)?;
    if kind == "copy" {
        let source = args.get("source_external_id").and_then(|v| v.as_str()).unwrap_or("");
        let visibility = tags.get("visibility").and_then(|v| v.as_str()).unwrap_or("");
        if visibility == "public" {
            return Ok(source.to_string());
        }
        let (cloud, _) = os_config(snap)?;
        let project = cloud
            .pointer("/auth/project_id")
            .or_else(|| cloud.pointer("/auth/project_name"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let share_tok = if let Some(sp) = args.get("source_provider").and_then(|v| v.as_str()) {
            provider_by_name(sp).and_then(|src| os_token(&src)).unwrap_or_else(|_| tok.clone())
        } else {
            tok.clone()
        };
        let created = os_http(
            &share_tok,
            "POST",
            &format!("{}/v2/images/{source}/members", share_tok.image),
            Some(&json!({"member": project})),
        )?;
        let member = created
            .get("member_id")
            .and_then(|v| v.as_str())
            .unwrap_or(project.as_str());
        let _ = os_http(
            &tok,
            "PUT",
            &format!("{}/v2/images/{source}/members/{member}", tok.image),
            Some(&json!({"status": "accepted"})),
        );
        return Ok(source.to_string());
    }
    let path = args.get("path").and_then(|v| v.as_str()).unwrap_or("");
    let bytes = std::fs::read(path).map_err(|e| AdvanceError::Other(e.to_string()))?;
    let mut extra = Map::new();
    if let Some(obj) = tags.as_object() {
        for (k, v) in obj {
            extra.insert(k.clone(), v.clone());
        }
    }
    extra.insert("disk_format".into(), json!(format));
    extra.insert("name".into(), json!(image_name));
    extra.insert("visibility".into(), json!("private"));
    extra.insert("container_format".into(), json!("bare"));
    let created = os_http(&tok, "POST", &format!("{}/v2/images", tok.image), Some(&Value::Object(extra)))?;
    let id = created.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let url = format!("{}/v2/images/{id}/file", tok.image);
    enter_limit(&tok.limit_key, tok.rate);
    let resp = exchange(
        "PUT",
        &url,
        &[
            ("X-Auth-Token", tok.token.as_str()),
            ("Content-Type", "application/octet-stream"),
        ],
        &bytes,
        HTTP_TIMEOUT,
    )
    .map_err(err_other)?;
    if resp.status >= 400 {
        return Err(err_other(format!("glance upload HTTP {}", resp.status)));
    }
    Ok(id)
}

fn azure_vm_body(
    region: &str,
    hostname: &str,
    label: &Value,
    flavor: &Value,
    image: &Value,
    image_reference: &Value,
    subnet: &str,
    tags: &Value,
    sub: &str,
    rg: &str,
) -> Value {
    let username = field_str(image, &["username"]).unwrap_or_else(|| "zuul".into());
    let mut os_profile = Map::new();
    os_profile.insert("computerName".into(), json!(hostname));
    if !username.is_empty() {
        os_profile.insert("adminUsername".into(), json!(username));
    }
    if let Some(key) = field_str(label, &["key-data", "key_data"]) {
        os_profile.insert(
            "linuxConfiguration".into(),
            json!({
                "disablePasswordAuthentication": true,
                "ssh": {"publicKeys": [{
                    "path": format!("/home/{username}/.ssh/authorized_keys"),
                    "keyData": key
                }]}
            }),
        );
    }
    if field(image, &["generate-password", "generate_password"]).and_then(|v| v.as_bool()) == Some(true) {
        os_profile.insert("adminPassword".into(), json!(azure_password()));
    }
    let mut os_disk = Map::new();
    os_disk.insert("createOption".into(), json!("FromImage"));
    os_disk.insert("deleteOption".into(), json!("Delete"));
    if let Some(size) = field(label, &["volume-size", "volume_size"]) {
        os_disk.insert("diskSizeGB".into(), size.clone());
    }
    if field(label, &["ephemeral-disk", "ephemeral_disk"]).and_then(|v| v.as_bool()) == Some(true) {
        os_disk.insert("caching".into(), json!("ReadOnly"));
        os_disk.insert("diffDiskSettings".into(), json!({"option": "Local"}));
    }
    let ipv6 = field(flavor, &["ipv6"]).and_then(|v| v.as_bool()).unwrap_or(false)
        || field(flavor, &["public-ipv6", "public_ipv6"]).and_then(|v| v.as_bool()).unwrap_or(false);
    let ipv4 = field(flavor, &["ipv4"]).and_then(|v| v.as_bool()).unwrap_or(false)
        || field(flavor, &["public-ipv4", "public_ipv4"]).and_then(|v| v.as_bool()).unwrap_or(false)
        || !ipv6;
    let public_v4 = field(flavor, &["public-ipv4", "public_ipv4"]).and_then(|v| v.as_bool()).unwrap_or(false);
    let public_v6 = field(flavor, &["public-ipv6", "public_ipv6"]).and_then(|v| v.as_bool()).unwrap_or(false);
    let mut ip_configs = Vec::new();
    if ipv4 {
        ip_configs.push(azure_ip_config(&format!("{hostname}-v4"), "IPv4", subnet, public_v4, tags));
    }
    if ipv6 {
        ip_configs.push(azure_ip_config(&format!("{hostname}-v6"), "IPv6", subnet, public_v6, tags));
    }
    let nic = json!({
        "name": format!("{hostname}-nic"),
        "tags": tags,
        "properties": {"deleteOption": "Delete", "ipConfigurations": ip_configs}
    });
    let vm_size = field_str(flavor, &["vm-size", "vm_size", "instance-type"]).unwrap_or_default();
    let mut properties = Map::new();
    properties.insert("osProfile".into(), Value::Object(os_profile));
    properties.insert("hardwareProfile".into(), json!({"vmSize": vm_size}));
    properties.insert(
        "storageProfile".into(),
        json!({"imageReference": image_reference, "osDisk": os_disk}),
    );
    properties.insert(
        "networkProfile".into(),
        json!({
            "networkApiVersion": "2020-11-01",
            "networkInterfaceConfigurations": [nic]
        }),
    );
    if let Some(user) = field_str(label, &["userdata", "user-data"]) {
        properties.insert("userData".into(), json!(user));
    }
    if field_str(flavor, &["priority"]).as_deref() == Some("spot") {
        properties.insert("evictionPolicy".into(), json!("Delete"));
        properties.insert("priority".into(), json!("Spot"));
    }
    let mut spec = Map::new();
    spec.insert("location".into(), json!(region));
    spec.insert("tags".into(), tags.clone());
    spec.insert("properties".into(), Value::Object(properties));
    let identities = label
        .get("user-assigned-identities")
        .or_else(|| label.get("user_assigned_identities"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    if !identities.is_empty() {
        let mut assigned = Map::new();
        for uai in identities {
            let name = uai.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let uai_rg = uai
                .get("resource_group")
                .or_else(|| uai.get("resource-group"))
                .and_then(|v| v.as_str())
                .unwrap_or(rg);
            let id = format!(
                "/subscriptions/{sub}/resourceGroups/{uai_rg}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/{name}"
            );
            assigned.insert(id, json!({}));
        }
        spec.insert(
            "identity".into(),
            json!({"type": "UserAssigned", "userAssignedIdentities": assigned}),
        );
    }
    Value::Object(spec)
}

fn azure_ip_config(name: &str, version: &str, subnet: &str, public: bool, tags: &Value) -> Value {
    let mut properties = Map::new();
    properties.insert("privateIPAddressVersion".into(), json!(version));
    properties.insert("subnet".into(), json!({"id": subnet}));
    if public {
        properties.insert(
            "publicIPAddressConfiguration".into(),
            json!({
                "name": format!("{name}-ip"),
                "tags": tags,
                "properties": {
                    "deleteOption": "Delete",
                    "publicIPAllocationMethod": "Static",
                    "publicIPAddressVersion": version
                },
                "sku": {"name": "Standard"}
            }),
        );
    }
    json!({"name": format!("{name}-config"), "properties": properties})
}

fn azure_password() -> String {
    const LOWER: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
    const UPPER: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    const DIGIT: &[u8] = b"0123456789";
    let alphabet = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    loop {
        let mut buf = [0u8; 64];
        let _ = getrandom::getrandom(&mut buf);
        let mut chars = Vec::with_capacity(64);
        let mut has_l = false;
        let mut has_u = false;
        let mut has_d = false;
        for b in buf {
            let c = alphabet[(b as usize) % alphabet.len()];
            has_l |= LOWER.contains(&c);
            has_u |= UPPER.contains(&c);
            has_d |= DIGIT.contains(&c);
            chars.push(c);
        }
        if has_l && has_u && has_d {
            return String::from_utf8(chars).unwrap_or_default();
        }
    }
}

fn node_from_azure(args: &Value) -> Value {
    let vm = args.get("vm").cloned().unwrap_or(json!({}));
    let nic = args.get("nic").cloned().unwrap_or(json!({}));
    let mut private_v4 = None;
    let mut private_v6 = None;
    if let Some(cfgs) = nic.pointer("/properties/ipConfigurations").and_then(|v| v.as_array()) {
        for cfg in cfgs {
            let version = cfg.pointer("/properties/privateIPAddressVersion").and_then(|v| v.as_str()).unwrap_or("");
            let ip = cfg.pointer("/properties/privateIPAddress").and_then(|v| v.as_str());
            if version == "IPv4" {
                private_v4 = ip.map(|s| s.to_string());
            }
            if version == "IPv6" {
                private_v6 = ip.map(|s| s.to_string());
            }
        }
    }
    let public_v4 = args.pointer("/public_ipv4/properties/ipAddress").and_then(|v| v.as_str()).map(|s| s.to_string());
    let public_v6 = args.pointer("/public_ipv6/properties/ipAddress").and_then(|v| v.as_str()).map(|s| s.to_string());
    let interface = public_v4.clone().or(public_v6.clone()).or(private_v4.clone()).or(private_v6.clone());
    let mut node = Map::new();
    if let Some(id) = args.get("azure_vm_id").filter(|v| !v.is_null()) {
        node.insert("azure_vm_id".into(), id.clone());
    } else if let Some(id) = vm.get("id") {
        node.insert("azure_vm_id".into(), id.clone());
    }
    if let Some(ip) = interface {
        node.insert("interface_ip".into(), json!(ip));
    }
    if let Some(ip) = public_v4 {
        node.insert("public_ipv4".into(), json!(ip));
    }
    if let Some(ip) = private_v4 {
        node.insert("private_ipv4".into(), json!(ip));
    }
    if let Some(ip) = public_v6 {
        node.insert("public_ipv6".into(), json!(ip));
    }
    if let Some(ip) = private_v6 {
        node.insert("private_ipv6".into(), json!(ip));
    }
    node.insert("cloud".into(), json!("Azure"));
    if let Some(region) = vm.get("location") {
        node.insert("region".into(), region.clone());
    }
    let az = vm.get("zones").and_then(|v| v.as_array()).and_then(|a| a.first()).cloned().unwrap_or(json!(""));
    node.insert("az".into(), az);
    if let Some(q) = args.get("quota") {
        let inner = q.get("quota").cloned().unwrap_or_else(|| q.clone());
        node.insert("quota".into(), inner);
    }
    Value::Object(node)
}

fn node_from_k8s(args: &Value) -> Value {
    let mut node = Map::new();
    if let Some(id) = args.get("kubernetes_namespace_id") {
        node.insert("kubernetes_namespace_id".into(), id.clone());
    }
    if let Some(conn) = args.get("kubernetes_connection") {
        node.insert("kubernetes_connection".into(), conn.clone());
    }
    if let Some(q) = args.get("quota") {
        let inner = q.get("quota").cloned().unwrap_or_else(|| q.clone());
        node.insert("quota".into(), inner);
    }
    node.insert("interface_ip".into(), Value::Null);
    node.insert("host_id".into(), Value::Null);
    node.insert("cloud".into(), Value::Null);
    node.insert("region".into(), Value::Null);
    node.insert("az".into(), Value::Null);
    Value::Object(node)
}

fn node_from_os(args: &Value, snap: &ProviderSnap) -> Value {
    let server = args.get("server").cloned().unwrap_or(json!({}));
    let mut node = Map::new();
    let server_id = server.get("id").cloned().or_else(|| args.get("openstack_server_id").cloned());
    if let Some(id) = server_id {
        if !id.is_null() {
            node.insert("openstack_server_id".into(), id);
        }
    }
    if let Some(id) = args.get("openstack_floating_ip_id").filter(|v| !v.is_null()) {
        node.insert("openstack_floating_ip_id".into(), id.clone());
    }
    if let Some(ip) = server.get("interface_ip").and_then(|v| v.as_str()).map(|s| s.to_string()).or_else(|| server_interface_ip(&server)) {
        node.insert("interface_ip".into(), json!(ip));
    }
    if let Some(ip) = server.get("public_v4").and_then(|v| v.as_str()) {
        node.insert("public_ipv4".into(), json!(ip));
    }
    if let Some(ip) = server.get("private_v4").and_then(|v| v.as_str()) {
        node.insert("private_ipv4".into(), json!(ip));
    }
    if let Some(ip) = server.get("public_v6").and_then(|v| v.as_str()) {
        node.insert("public_ipv6".into(), json!(ip));
    }
    if let Some(host) = server.get("hostId").and_then(|v| v.as_str()) {
        node.insert("host_id".into(), json!(host));
    }
    if let Some(az) = server.get("OS-EXT-AZ:availability_zone").and_then(|v| v.as_str()) {
        node.insert("az".into(), json!(az));
    }
    node.insert("region".into(), json!(snap.region));
    if let Ok((cloud, _)) = os_config(snap) {
        if let Some(name) = cloud.get("cloud_name").and_then(|v| v.as_str()) {
            node.insert("cloud".into(), json!(name));
        }
    }
    if let Some(q) = args.get("quota") {
        let inner = q.get("quota").cloned().unwrap_or_else(|| q.clone());
        node.insert("quota".into(), inner);
    }
    Value::Object(node)
}
