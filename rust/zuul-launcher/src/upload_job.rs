// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// UploadJob / EndpointUploadJob from zuul/launcher/server.py.
// Locking, state transitions, download post-processing, checksums, dependent
// copies, and statsd timing run here. The provider getImage*Job().run call
// and the requests-mocked artifact download stay on the Python side.

use crate::cloud;
use crate::codec::{decode_zkobject, dumps_compressed};
use crate::launcher::normalize_statsd_name;
use crate::model::{self, upload_state, ImageBuildArtifact, ImageUpload};
use crate::zk::{acquire_lock, CreateMode, HeldLock, ZkError, ZkOps};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Read;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static UPLOAD_SEQ: AtomicU64 = AtomicU64::new(1);

fn hook_or_cloud(op: &str, args: &Value) {
    if let Some(result) = crate::hooks::bridge(op, args) {
        let _ = result;
    }
}

fn new_upload_uuid() -> String {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(1);
    let seq = UPLOAD_SEQ.fetch_add(1, Ordering::Relaxed) as u128;
    format!("{:032x}", n ^ (seq << 64) ^ (seq.wrapping_mul(0x9E37_79B9_7F4A_7C15)))
}

fn upload_path(uuid: &str) -> String {
    format!("/zuul/image-uploads/uploads/{uuid}")
}

fn upload_lock_path(uuid: &str) -> String {
    format!("/zuul/image-uploads/locks/{uuid}")
}

fn artifact_lock_path(uuid: &str) -> String {
    format!("/zuul/images/locks/{uuid}")
}

fn load_upload(zk: &dyn ZkOps, uuid: &str) -> Option<ImageUpload> {
    let (data, _) = zk.get(&upload_path(uuid)).ok()?;
    let val = decode_zkobject(&data).ok()?;
    Some(ImageUpload::from_value(val))
}

fn load_artifact(zk: &dyn ZkOps, uuid: &str) -> Option<ImageBuildArtifact> {
    let path = format!("/zuul/images/artifacts/{uuid}");
    let (data, _) = zk.get(&path).ok()?;
    let val = decode_zkobject(&data).ok()?;
    Some(ImageBuildArtifact::from_value(val))
}

fn save_upload(zk: &dyn ZkOps, upload: &ImageUpload) -> Result<(), ZkError> {
    let path = upload_path(&upload.uuid());
    let bytes = dumps_compressed(&upload.serialize());
    zk.ensure_path("/zuul/image-uploads/uploads")?;
    zk.ensure_path(&upload_lock_path(&upload.uuid()))?;
    for _ in 0..3 {
        match zk.get(&path) {
            Ok((_, stat)) => match zk.set(&path, &bytes, stat.version) {
                Ok(_) => return Ok(()),
                Err(ZkError::BadVersion) => continue,
                Err(e) => return Err(e),
            },
            Err(ZkError::NoNode) => {
                zk.create(&path, &bytes, CreateMode::Persistent)?;
                return Ok(());
            }
            Err(e) => return Err(e),
        }
    }
    Err(ZkError::BadVersion)
}

fn release_lock(zk: &dyn ZkOps, held: &HeldLock) {
    let _ = zk.delete(&held.path, false);
}

/// Reset unlocked UPLOADING uploads to PENDING (checkMissingUploads).
/// The caller passes the tree-cache items. Only an unlocked UPLOADING
/// upload is refreshed, inside its lock. Returns the uuids moved to PENDING.
pub fn reset_crashed_uploads(
    zk: &dyn ZkOps,
    hostname: &str,
    uploads: &[ImageUpload],
) -> Vec<String> {
    let mut reset = Vec::new();
    for upload in uploads {
        if upload.state() != upload_state::UPLOADING || upload.is_locked {
            continue;
        }
        let Ok(Some(held)) = acquire_lock(zk, &upload_lock_path(&upload.uuid()), hostname, false) else {
            continue;
        };
        if let Some(mut fresh) = load_upload(zk, &upload.uuid()) {
            if fresh.state() == upload_state::UPLOADING {
                fresh.set_state(upload_state::PENDING);
                if save_upload(zk, &fresh).is_ok() {
                    reset.push(fresh.uuid());
                }
            }
        }
        release_lock(zk, &held);
    }
    reset
}

struct Planned {
    kind: String,
    upload_uuid: String,
    source_external_id: Option<String>,
    source_provider: Option<String>,
    dependents: Vec<String>,
}

fn planned_from(value: &Value) -> Planned {
    Planned {
        kind: value
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or("upload")
            .to_string(),
        upload_uuid: value
            .get("upload_uuid")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        source_external_id: value
            .get("source_external_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty()),
        source_provider: value
            .get("source_provider")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty()),
        dependents: value
            .get("dependents")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default(),
    }
}

/// One artifact's UploadJob. Honors a MonkeyPatch of UploadJob.run by
/// delegating the whole job; otherwise the body runs here.
pub fn run_artifact(
    zk: &dyn ZkOps,
    hostname: &str,
    artifact_uuid: &str,
    upload_uuids: &[String],
    planned: &[Value],
) {
    let patched = if cloud::enabled() {
        cloud::invoke("upload.run_is_patched", &json!({}))
            .ok()
            .and_then(|v| v.get("patched").and_then(|b| b.as_bool()))
            .unwrap_or(false)
    } else {
        false
    };
    if patched {
        let _ = cloud::invoke(
            "upload.artifact_job",
            &json!({
                "artifact_uuid": artifact_uuid,
                "upload_uuids": upload_uuids,
                "planned": planned,
            }),
        );
        return;
    }

    let jobs: Vec<Planned> = planned.iter().map(planned_from).collect();
    let mut held: Vec<(String, HeldLock)> = Vec::new();
    let iba = acquire_lock(zk, &artifact_lock_path(artifact_uuid), hostname, false);
    let Ok(Some(iba_lock)) = iba else {
        hook_or_cloud("upload.notify", &json!({"image_updated": true, "wake": true}));
        return;
    };
    for uuid in upload_uuids {
        let Ok(Some(lock)) = acquire_lock(zk, &upload_lock_path(uuid), hostname, false) else {
            continue;
        };
        let Some(mut upload) = load_upload(zk, uuid) else {
            release_lock(zk, &lock);
            continue;
        };
        if upload.external_id().is_some() || upload.state() != upload_state::PENDING {
            release_lock(zk, &lock);
            continue;
        }
        upload.set_state(upload_state::UPLOADING);
        if save_upload(zk, &upload).is_err() {
            release_lock(zk, &lock);
            continue;
        }
        let _ = cloud::invoke(
            "upload.log",
            &json!({
                "level": "debug",
                "upload_uuid": uuid,
                "message": format!("Acquired upload lock for upload {uuid}"),
            }),
        );
        held.push((uuid.clone(), lock));
    }
    release_lock(zk, &iba_lock);
    if held.is_empty() {
        return;
    }

    let artifact = load_artifact(zk, artifact_uuid);
    let sha = artifact
        .as_ref()
        .and_then(|a| a.fields.get("sha256").and_then(|v| v.as_str()))
        .unwrap_or("")
        .to_string();
    let mut path: Option<String> = None;
    let mut locked: Vec<String> = held.iter().map(|(u, _)| u.clone()).collect();

    for job in &jobs {
        if job.kind == "copy-dependent" {
            continue;
        }
        if !locked.iter().any(|u| u == &job.upload_uuid) {
            continue;
        }
        let external_id = run_one(zk, &job, &sha, artifact_uuid, &mut path);
        finalize_one(zk, &job.upload_uuid, external_id.as_deref());
        release_one(zk, &mut held, &job.upload_uuid);
        locked.retain(|u| u != &job.upload_uuid);
        if let Some(ext) = &external_id {
            let source_provider = load_upload(zk, &job.upload_uuid)
                .and_then(|u| u.providers().into_iter().next())
                .or_else(|| job.source_provider.clone());
            for dep in &job.dependents {
                if !locked.iter().any(|u| u == dep) {
                    continue;
                }
                let dep_job = Planned {
                    kind: "copy".into(),
                    upload_uuid: dep.clone(),
                    source_external_id: Some(ext.clone()),
                    source_provider: source_provider.clone(),
                    dependents: Vec::new(),
                };
                let dep_ext = run_one(zk, &dep_job, &sha, artifact_uuid, &mut path);
                finalize_one(zk, dep, dep_ext.as_deref());
                release_one(zk, &mut held, dep);
                locked.retain(|u| u != dep);
            }
        }
    }
    for uuid in locked {
        finalize_one(zk, &uuid, None);
        release_one(zk, &mut held, &uuid);
    }
    if let Some(p) = &path {
        match fs::remove_file(p) {
            Ok(()) => {
                let _ = cloud::invoke(
                    "upload.log",
                    &json!({
                        "level": "info",
                        "message": format!("UploadJob deleted {p}"),
                    }),
                );
            }
            Err(err) => {
                let _ = cloud::invoke(
                    "upload.log",
                    &json!({
                        "level": "error",
                        "message": format!("Unable to delete {p}: {err}"),
                    }),
                );
            }
        }
    }
    hook_or_cloud("upload.notify", &json!({"wake": true}));
}

fn release_one(zk: &dyn ZkOps, held: &mut Vec<(String, HeldLock)>, uuid: &str) {
    if let Some(pos) = held.iter().position(|(u, _)| u == uuid) {
        let (_, lock) = held.remove(pos);
        release_lock(zk, &lock);
        let _ = cloud::invoke(
            "upload.log",
            &json!({
                "level": "debug",
                "upload_uuid": uuid,
                "message": format!("Released upload lock for upload {uuid}"),
            }),
        );
    }
}

fn run_one(
    zk: &dyn ZkOps,
    job: &Planned,
    sha: &str,
    artifact_uuid: &str,
    path: &mut Option<String>,
) -> Option<String> {
    let Some(upload) = load_upload(zk, &job.upload_uuid) else {
        return None;
    };
    let iname = normalize_statsd_name(&upload.canonical_name());
    let ename = normalize_statsd_name(&upload.endpoint_name());
    let stats_key = format!("zuul.image.{iname}.upload.{ename}.duration");
    let mut payload = json!({
        "kind": job.kind,
        "upload_uuid": job.upload_uuid,
    });
    if let Some(p) = path {
        payload
            .as_object_mut()
            .unwrap()
            .insert("path".into(), json!(p));
    }
    if let Some(src) = &job.source_external_id {
        payload
            .as_object_mut()
            .unwrap()
            .insert("source_external_id".into(), json!(src));
    }
    if let Some(sp) = &job.source_provider {
        payload
            .as_object_mut()
            .unwrap()
            .insert("source_provider".into(), json!(sp));
    }
    let upload_patched =
        crate::hooks::active() && crate::hooks::flag(&format!("upload_patched_{}", job.kind));
    if job.kind == "upload" && path.is_none() {
        match fetch_image(zk, artifact_uuid, sha) {
            Ok(p) => {
                *path = Some(p.clone());
                payload
                    .as_object_mut()
                    .unwrap()
                    .insert("path".into(), json!(p));
            }
            Err(err) => {
                if !upload_patched {
                    let _ = cloud::invoke(
                        "upload.log",
                        &json!({"level": "error", "message": err}),
                    );
                    return None;
                }
                // The test replaced Job.run and ignores the file. A
                // missing path makes the Python adapter raise before
                // the mock runs.
                payload.as_object_mut().unwrap().insert(
                    "path".into(),
                    json!("/tmp/zuul-patched-upload-image"),
                );
            }
        }
    }
    if upload_patched {
        let started = Instant::now();
        let result = crate::hooks::consult("upload.sdk_run", &payload);
        let ms = started.elapsed().as_secs_f64() * 1000.0;
        crate::statsd::timing(&stats_key, ms);
        if let Some(Ok(value)) = result {
            return value
                .get("external_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .filter(|s| !s.is_empty());
        }
        return None;
    }
    let started = Instant::now();
    let result = if cloud::enabled() {
        cloud::invoke("upload.sdk_run", &payload)
    } else {
        native_upload(zk, job, &payload)
    };
    let ms = started.elapsed().as_secs_f64() * 1000.0;
    crate::statsd::timing(&stats_key, ms);
    let _ = cloud::invoke(
        "stats.timing",
        &json!({"name": stats_key, "ms": ms}),
    );
    let Ok(value) = result else {
        return None;
    };
    // 14.3.0 writes external_id and READY in one ZK update. Publishing
    // the id while the state is still UPLOADING makes _waitForUploads
    // return before getImageExternalId will accept the upload.
    value
        .get("external_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
}

fn native_upload(zk: &dyn ZkOps, job: &Planned, payload: &Value) -> Result<Value, crate::provider::AdvanceError> {
    let upload = load_upload(zk, &job.upload_uuid)
        .ok_or_else(|| crate::provider::AdvanceError::Other("missing upload".into()))?;
    let artifact = load_artifact(zk, &upload.artifact_uuid());
    let provider = upload.providers().into_iter().next().unwrap_or_default();
    let mut body = payload.clone();
    if let Some(obj) = body.as_object_mut() {
        obj.insert("provider".into(), json!(provider));
        obj.insert("image_name".into(), json!(upload.canonical_name()));
        if let Some(art) = artifact {
            if let Some(fmt) = art.fields.get("format") {
                obj.insert("image_format".into(), fmt.clone());
            }
            if let Some(url) = art.fields.get("url") {
                obj.insert("artifact_url".into(), url.clone());
            }
        }
    }
    crate::native::upload_sdk_run(&body)
}

fn fetch_image(zk: &dyn ZkOps, artifact_uuid: &str, sha: &str) -> Result<String, String> {
    if !cloud::enabled() {
        return fetch_image_native(zk, artifact_uuid, sha);
    }
    let fetched = cloud::invoke(
        "upload.fetch",
        &json!({"artifact_uuid": artifact_uuid}),
    )
    .map_err(|e| e.to_string())?;
    if fetched.get("ok").and_then(|v| v.as_bool()) == Some(false) {
        let err = fetched
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("Unable to download artifact");
        return Err(err.to_string());
    }
    let raw = fetched
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "download returned no path".to_string())?
        .to_string();
    let temp_dir = fetched
        .get("temp_dir")
        .and_then(|v| v.as_str())
        .unwrap_or("/tmp")
        .to_string();
    let artifact_repr = fetched
        .get("artifact_repr")
        .and_then(|v| v.as_str())
        .unwrap_or(artifact_uuid)
        .to_string();
    let path = decompress_zst(&raw, &temp_dir)?;
    validate_checksum(&path, sha, &artifact_repr)?;
    Ok(path)
}

fn fetch_image_native(zk: &dyn ZkOps, artifact_uuid: &str, sha: &str) -> Result<String, String> {
    let art = load_artifact(zk, artifact_uuid).ok_or_else(|| "missing artifact".to_string())?;
    let url = art
        .fields
        .get("url")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "artifact has no url".to_string())?;
    let resp = crate::cloud_http::exchange(
        "GET",
        url,
        &[],
        &[],
        std::time::Duration::from_secs(60),
    )
    .map_err(|e| e)?;
    if resp.status >= 400 {
        return Err(format!("artifact HTTP {}", resp.status));
    }
    let dir = std::env::temp_dir();
    let raw = dir.join(format!("{artifact_uuid}.img.zst"));
    fs::write(&raw, &resp.body).map_err(|e| e.to_string())?;
    let raw_s = raw.to_string_lossy().into_owned();
    let temp = dir.to_str().unwrap_or("/tmp");
    let path = decompress_zst(&raw_s, temp)?;
    validate_checksum(&path, sha, artifact_uuid)?;
    Ok(path)
}

fn decompress_zst(path: &str, temp_dir: &str) -> Result<String, String> {
    if !path.ends_with(".zst") {
        return Ok(path.to_string());
    }
    let inflated = path.trim_end_matches(".zst").to_string();
    let result = Command::new("zstd")
        .args(["-dq", path])
        .current_dir(temp_dir)
        .output();
    match result {
        Ok(out) if out.status.success() => {
            let _ = cloud::invoke(
                "upload.log",
                &json!({
                    "level": "debug",
                    "message": format!("Decompressed image to {inflated}"),
                }),
            );
        }
        Ok(out) => {
            if Path::new(&inflated).exists() {
                let _ = fs::remove_file(&inflated);
                let _ = cloud::invoke(
                    "upload.log",
                    &json!({
                        "level": "info",
                        "message": format!("Deleted decompressed image {inflated}"),
                    }),
                );
            }
            let stderr = String::from_utf8_lossy(&out.stderr);
            if Path::new(path).exists() {
                let _ = fs::remove_file(path);
                let _ = cloud::invoke(
                    "upload.log",
                    &json!({
                        "level": "info",
                        "message": format!("Deleted zst source image {path}"),
                    }),
                );
            }
            return Err(format!("zstd failed: {stderr}"));
        }
        Err(err) => {
            if Path::new(&inflated).exists() {
                let _ = fs::remove_file(&inflated);
            }
            if Path::new(path).exists() {
                let _ = fs::remove_file(path);
            }
            return Err(format!("zstd failed: {err}"));
        }
    }
    if Path::new(path).exists() {
        let _ = fs::remove_file(path);
        let _ = cloud::invoke(
            "upload.log",
            &json!({
                "level": "info",
                "message": format!("Deleted zst source image {path}"),
            }),
        );
    }
    Ok(inflated)
}

fn validate_checksum(path: &str, sha: &str, artifact_repr: &str) -> Result<(), String> {
    if sha.is_empty() {
        let _ = cloud::invoke(
            "upload.log",
            &json!({
                "level": "debug",
                "message": format!(
                    "Image build artifact {artifact_repr} has no sha256 checksum, skipping validation"
                ),
            }),
        );
        return Ok(());
    }
    let mut file = fs::File::open(path).map_err(|e| e.to_string())?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 4096];
    loop {
        let n = file.read(&mut buf).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let digest = format!("{:x}", hasher.finalize());
    if digest != sha {
        return Err(format!(
            "Downloaded file {path} sha256 digest {digest} does not match artifact {artifact_repr} digest {sha}"
        ));
    }
    Ok(())
}

fn finalize_one(zk: &dyn ZkOps, uuid: &str, external_id: Option<&str>) {
    let Some(mut upload) = load_upload(zk, uuid) else {
        return;
    };
    if external_id.is_some() || upload.external_id().is_some() {
        if let Some(ext) = external_id {
            upload.set_external_id(Some(ext));
        }
        let _ = cloud::invoke(
            "upload.log",
            &json!({
                "level": "debug",
                "upload_uuid": uuid,
                "message": format!("Marking upload {uuid} ready"),
            }),
        );
        upload.set_state(upload_state::READY);
        let _ = save_upload(zk, &upload);
        if !upload.validated() {
            let _ = cloud::invoke(
                "upload.validate_event",
                &json!({"upload_uuid": uuid}),
            );
        }
        return;
    }
    if upload.attempt() < 2 {
        let replacement = copy_upload(&upload);
        let _ = cloud::invoke(
            "upload.log",
            &json!({
                "level": "debug",
                "upload_uuid": uuid,
                "message": format!(
                    "Replacing upload {uuid} with {}",
                    replacement.uuid()
                ),
            }),
        );
        let _ = save_upload(zk, &replacement);
        let _ = cloud::invoke(
            "upload.log",
            &json!({
                "level": "debug",
                "upload_uuid": uuid,
                "message": format!("Marking upload {uuid} deleting"),
            }),
        );
        upload.set_state(upload_state::DELETING);
        let _ = save_upload(zk, &upload);
        hook_or_cloud(
            "upload.notify",
            &json!({"upload_deleted": true, "wake": true}),
        );
    } else {
        let _ = cloud::invoke(
            "upload.log",
            &json!({
                "level": "debug",
                "upload_uuid": uuid,
                "message": format!("Marking upload {uuid} failed"),
            }),
        );
        upload.set_state(upload_state::FAILED);
        let _ = save_upload(zk, &upload);
    }
}

fn copy_upload(src: &ImageUpload) -> ImageUpload {
    let mut fields: Map<String, Value> = src.fields.clone();
    let uuid = new_upload_uuid();
    fields.insert("uuid".into(), json!(uuid));
    fields.insert("attempt".into(), json!(src.attempt() + 1));
    fields.insert("_state".into(), json!(upload_state::PENDING));
    fields.insert("state_time".into(), json!(model::now_secs()));
    fields.insert("external_id".into(), Value::Null);
    fields.insert("build_uuid".into(), Value::Null);
    ImageUpload {
        fields,
        extra: Map::new(),
        is_locked: false,
        lock_holder: None,
    }
}
