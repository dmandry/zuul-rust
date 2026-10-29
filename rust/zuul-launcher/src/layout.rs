// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// Layout and provider loading from zuul/zk/layout.py, zuul/zk/locks.py,
// and Launcher._updateTenantProviders. Layout znodes are uncompressed
// JSON. Layout-data and provider configs are sharded; provider configs
// are polymorphic (`{driver}\0` + zlib JSON).

use crate::codec::{self, decode_polymorphic};
use crate::config::quote_plus;
use crate::jsonutil;
use crate::provider::{parse_provider_config, Provider};
use crate::zk::{lock_is_winner, CreateMode, ZkError, ZkOps};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

pub const LAYOUT_ROOT: &str = "/zuul/layout";
pub const LAYOUT_DATA_ROOT: &str = "/zuul/layout-data";
pub const TENANT_ROOT: &str = "/zuul/tenant";
pub const TENANT_LOCK_ROOT: &str = "/zuul/locks/tenant";

/// Same fields as zuul.zk.layout.LayoutState. Equality is uuid only,
/// matching LayoutState.__eq__.
#[derive(Clone, Debug)]
pub struct LayoutState {
    pub tenant_name: String,
    pub hostname: String,
    pub last_reconfigured: f64,
    pub uuid: Option<String>,
    pub ltime: i64,
    pub branch_cache_min_ltimes: Value,
    pub last_reconfigure_event_ltime: i64,
    /// From /zuul/layout-data/{uuid}. None when the znode is missing.
    pub min_ltimes: Option<Value>,
}

impl PartialEq for LayoutState {
    fn eq(&self, other: &Self) -> bool {
        self.uuid == other.uuid
    }
}

impl LayoutState {
    pub fn to_json(&self) -> Value {
        json!({
            "tenant_name": self.tenant_name,
            "hostname": self.hostname,
            "last_reconfigured": self.last_reconfigured,
            "uuid": self.uuid,
            "ltime": self.ltime,
            "branch_cache_min_ltimes": self.branch_cache_min_ltimes,
            "last_reconfigure_event_ltime": self.last_reconfigure_event_ltime,
            "min_ltimes": self.min_ltimes,
        })
    }
}

/// GET /zuul/layout/{tenant}. Empty data and NoNode are a missing tenant.
pub fn read_tenant_layout(zk: &dyn ZkOps, tenant: &str) -> Result<Option<LayoutState>, String> {
    let path = format!("{LAYOUT_ROOT}/{tenant}");
    let (data, stat) = match zk.get(&path) {
        Ok(pair) => pair,
        Err(ZkError::NoNode) => return Ok(None),
        Err(e) => return Err(format!("{path}: {e}")),
    };
    if data.is_empty() {
        return Ok(None);
    }
    let text = String::from_utf8(data).map_err(|e| format!("{path}: {e}"))?;
    let mut body: Value = jsonutil::loads(&text)?;
    let obj = body
        .as_object_mut()
        .ok_or_else(|| format!("{path}: layout state is not an object"))?;
    // json.loads wins over the stat ltime when the payload itself has ltime.
    let ltime = obj
        .get("ltime")
        .and_then(json_i64)
        .unwrap_or(stat.mzxid);
    let uuid = obj.get("uuid").and_then(|v| {
        if v.is_null() {
            None
        } else {
            v.as_str().map(|s| s.to_string())
        }
    });
    let min_ltimes = match &uuid {
        Some(id) if !id.is_empty() => read_min_ltimes(zk, id),
        _ => None,
    };
    Ok(Some(LayoutState {
        tenant_name: json_string(obj, "tenant_name").unwrap_or_else(|| tenant.to_string()),
        hostname: json_string(obj, "hostname").unwrap_or_default(),
        last_reconfigured: obj.get("last_reconfigured").and_then(json_f64).unwrap_or(0.0),
        uuid,
        ltime,
        branch_cache_min_ltimes: obj
            .get("branch_cache_min_ltimes")
            .cloned()
            .unwrap_or(Value::Null),
        last_reconfigure_event_ltime: obj
            .get("last_reconfigure_event_ltime")
            .and_then(json_i64)
            .unwrap_or(-1),
        min_ltimes,
    }))
}

pub fn read_layout_states(zk: &dyn ZkOps) -> Result<HashMap<String, LayoutState>, String> {
    let names = match zk.get_children(LAYOUT_ROOT) {
        Ok(names) => names,
        Err(ZkError::NoNode) => return Ok(HashMap::new()),
        Err(e) => return Err(format!("{LAYOUT_ROOT}: {e}")),
    };
    let mut out = HashMap::new();
    for tenant in names {
        if let Some(state) = read_tenant_layout(zk, &tenant)? {
            out.insert(tenant, state);
        }
    }
    Ok(out)
}

/// /zuul/layout-data/{uuid} is a sharded zlib JSON object. A missing node
/// is None, matching LayoutStateStore.getMinLtimes.
fn read_min_ltimes(zk: &dyn ZkOps, uuid: &str) -> Option<Value> {
    let path = format!("{LAYOUT_DATA_ROOT}/{uuid}");
    let raw = read_shard_bytes(zk, &path).ok()?;
    let text = String::from_utf8(codec::decompress(&raw)).ok()?;
    let value = jsonutil::loads(&text).ok()?;
    value.get("min_ltimes").cloned()
}

/// Children of /zuul/tenant/{tenant}/provider/{project}/{provider}/config.
pub fn read_tenant_providers(
    zk: &dyn ZkOps,
    tenant: &str,
) -> Result<Vec<Arc<dyn Provider>>, String> {
    let root = format!("{TENANT_ROOT}/{tenant}/provider");
    let repos = match zk.get_children(&root) {
        Ok(names) => names,
        Err(ZkError::NoNode) => return Ok(Vec::new()),
        Err(e) => return Err(format!("{root}: {e}")),
    };
    let mut providers = Vec::new();
    for repo in repos {
        let repo_path = format!("{root}/{repo}");
        let names = match zk.get_children(&repo_path) {
            Ok(names) => names,
            Err(ZkError::NoNode) => continue,
            Err(e) => return Err(format!("{repo_path}: {e}")),
        };
        for name in names {
            let config_path = format!("{repo_path}/{name}/config");
            let raw = read_shard_bytes(zk, &config_path)?;
            let (driver, blob) = decode_polymorphic(&raw)?;
            let connection = blob
                .get("connection_name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let canonical = blob
                .get("canonical_name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let config = blob.get("config").cloned().unwrap_or(Value::Null);
            let parsed = parse_provider_config(&driver, &connection, &canonical, &config);
            providers.push(Arc::new(parsed) as Arc<dyn Provider>);
        }
    }
    Ok(providers)
}

/// Concatenate shard children in sorted order. A second shard that is its
/// own zlib blob is the pre-MODEL_API-31 format: decompress each shard.
fn read_shard_bytes(zk: &dyn ZkOps, path: &str) -> Result<Vec<u8>, String> {
    let mut names = match zk.get_children(path) {
        Ok(names) => names,
        Err(ZkError::NoNode) => return Err(format!("{path}: no node")),
        Err(e) => return Err(format!("{path}: {e}")),
    };
    names.sort();
    let mut parts = Vec::with_capacity(names.len());
    for name in &names {
        let shard = format!("{path}/{name}");
        let (data, _) = zk
            .get(&shard)
            .map_err(|e| format!("{shard}: {e}"))?;
        parts.push(data);
    }
    if parts.len() >= 2 && zlib_payload(&parts[1]).is_some() {
        let mut joined = Vec::new();
        for part in &parts {
            match zlib_payload(part) {
                Some(plain) => joined.extend(plain),
                None => joined.extend(part),
            }
        }
        return Ok(joined);
    }
    Ok(codec::unshard(&parts))
}

fn zlib_payload(data: &[u8]) -> Option<Vec<u8>> {
    if data.len() < 2 || data[0] != 0x78 || data[1] != 0x9c {
        return None;
    }
    let plain = codec::decompress(data);
    if plain == data {
        None
    } else {
        Some(plain)
    }
}

/// Blocking tenant read lock. The child is `{uuid}__rlock__{seq:010}`.
/// Readers do not block readers. Returns the full child path.
pub fn acquire_tenant_read_lock(
    zk: &dyn ZkOps,
    tenant: &str,
    identifier: &str,
) -> Result<String, String> {
    let lock_path = format!("{TENANT_LOCK_ROOT}/{}", quote_plus(tenant));
    zk.ensure_path(&lock_path)
        .map_err(|e| format!("{lock_path}: {e}"))?;
    let mut nonce = [0u8; 16];
    let _ = getrandom::getrandom(&mut nonce);
    let prefix = format!("{lock_path}/{}__rlock__", hex::encode(nonce));
    let full = zk
        .create(&prefix, identifier.as_bytes(), CreateMode::EphemeralSequential)
        .map_err(|e| format!("tenant read lock {lock_path}: {e}"))?;
    let child = full[lock_path.len() + 1..].to_string();
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        match lock_is_winner(zk, &lock_path, &child, true) {
            Ok(true) => return Ok(full),
            Ok(false) => {
                if Instant::now() >= deadline {
                    let _ = zk.delete(&full, false);
                    return Err(format!("timed out waiting for {lock_path} read lock"));
                }
                thread::sleep(Duration::from_millis(20));
            }
            Err(e) => {
                let _ = zk.delete(&full, false);
                return Err(format!("{lock_path}: {e}"));
            }
        }
    }
}

pub fn release_lock(zk: &dyn ZkOps, path: &str) {
    let _ = zk.delete(path, false);
}

fn json_string(obj: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
    obj.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
}

fn json_i64(v: &Value) -> Option<i64> {
    v.as_i64()
        .or_else(|| v.as_u64().map(|n| n as i64))
        .or_else(|| v.as_f64().map(|n| n as i64))
}

fn json_f64(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_i64().map(|n| n as f64))
        .or_else(|| v.as_u64().map(|n| n as f64))
}
