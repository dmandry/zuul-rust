// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// Port of zuul/launcher/uploadplanner.py. Pool sizes match the launcher:
// 1 upload worker, 10 endpoint-upload workers, 20 import workers.

use crate::model::ImageUpload;
use std::collections::HashMap;

pub const UPLOAD_WORKERS: usize = 1;
pub const ENDPOINT_UPLOAD_WORKERS: usize = 10;
pub const IMPORT_WORKERS: usize = 20;

#[derive(Clone, Debug)]
pub struct UploadArgs {
    pub provider: String,
    pub image_name: String,
    pub url: String,
    pub image_format: String,
    pub methods: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct PlannedJob {
    pub kind: String,
    pub upload_uuid: String,
    pub source_external_id: Option<String>,
    pub dependents: Vec<String>,
}

pub struct UploadPlanner<'a> {
    pub pending: Vec<&'a ImageUpload>,
    pub all: Vec<&'a ImageUpload>,
    pub args: HashMap<String, UploadArgs>,
    pub import_jobs: Vec<PlannedJob>,
    pub copy_jobs: Vec<PlannedJob>,
    pub upload_jobs: Vec<PlannedJob>,
    pub jobs: HashMap<String, PlannedJob>,
}

impl<'a> UploadPlanner<'a> {
    pub fn new(pending: Vec<&'a ImageUpload>, all: Vec<&'a ImageUpload>, args: HashMap<String, UploadArgs>) -> Self {
        Self {
            pending,
            all,
            args,
            import_jobs: Vec::new(),
            copy_jobs: Vec::new(),
            upload_jobs: Vec::new(),
            jobs: HashMap::new(),
        }
    }

    pub fn plan(&mut self) {
        let mut pending_ids: Vec<String> = self.pending.iter().map(|u| u.uuid()).collect();
        self.handle_imports(&mut pending_ids);
        self.handle_copies_and_uploads(&mut pending_ids);
    }

    fn handle_imports(&mut self, pending_ids: &mut Vec<String>) {
        let ids = pending_ids.clone();
        for id in ids {
            let Some(args) = self.args.get(&id) else { continue };
            if !args.methods.iter().any(|m| m == "import") {
                continue;
            }
            pending_ids.retain(|u| u != &id);
            let job = PlannedJob {
                kind: "import".into(),
                upload_uuid: id.clone(),
                source_external_id: None,
                dependents: Vec::new(),
            };
            self.import_jobs.push(job.clone());
            self.jobs.insert(id, job);
        }
    }

    fn handle_copies_and_uploads(&mut self, pending_ids: &mut Vec<String>) {
        let mut by_hash: HashMap<String, Vec<String>> = HashMap::new();
        for upload in &self.all {
            by_hash
                .entry(upload.config_hash())
                .or_default()
                .push(upload.uuid());
        }
        let hashes: Vec<String> = by_hash.keys().cloned().collect();
        for hash in hashes {
            let hash_uploads = by_hash.get(&hash).cloned().unwrap_or_default();
            self.handle_copies_inner(&hash, &hash_uploads, pending_ids);
        }
    }

    fn handle_copies_inner(&mut self, config_hash: &str, hash_uploads: &[String], pending_ids: &mut Vec<String>) {
        let mut pending_hash: Vec<String> = pending_ids
            .iter()
            .filter(|id| {
                self.all.iter().any(|u| u.uuid() == **id && u.config_hash() == config_hash)
                    || self.pending.iter().any(|u| u.uuid() == **id && u.config_hash() == config_hash)
            })
            .cloned()
            .collect();
        let mut targets: HashMap<String, Vec<String>> = HashMap::new();
        for upload in &pending_hash {
            for source in hash_uploads {
                targets.entry(source.clone()).or_default();
                if source == upload {
                    continue;
                }
                let Some(args) = self.args.get(upload) else { continue };
                if !args.methods.iter().any(|m| m == "copy") {
                    continue;
                }
                targets.get_mut(source).unwrap().push(upload.clone());
            }
        }
        let sources: Vec<String> = targets.keys().cloned().collect();
        for source in &sources {
            let ext = self.external_id(source);
            if ext.is_none() {
                continue;
            }
            let jobs = targets.get(source).cloned().unwrap_or_default();
            for upload in jobs {
                if !pending_hash.iter().any(|u| u == &upload) {
                    continue;
                }
                let job = PlannedJob {
                    kind: "copy".into(),
                    upload_uuid: upload.clone(),
                    source_external_id: ext.clone(),
                    dependents: Vec::new(),
                };
                self.copy_jobs.push(job.clone());
                self.jobs.insert(upload.clone(), job);
                pending_hash.retain(|u| u != &upload);
                pending_ids.retain(|u| u != &upload);
            }
        }
        let mut ranked: Vec<(String, Vec<String>)> = targets.into_iter().collect();
        ranked.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then(a.0.cmp(&b.0)));
        for (source, jobs) in ranked {
            if self.external_id(&source).is_some() {
                continue;
            }
            if !self.pending.iter().any(|u| u.uuid() == source) {
                continue;
            }
            if !self.jobs.contains_key(&source) {
                let Some(args) = self.args.get(&source) else { continue };
                if !args.methods.iter().any(|m| m == "upload") {
                    continue;
                }
                let job = PlannedJob {
                    kind: "upload".into(),
                    upload_uuid: source.clone(),
                    source_external_id: None,
                    dependents: Vec::new(),
                };
                self.upload_jobs.push(job.clone());
                self.jobs.insert(source.clone(), job);
                pending_hash.retain(|u| u != &source);
                pending_ids.retain(|u| u != &source);
            }
            let Some(mut source_job) = self.jobs.get(&source).cloned() else { continue };
            for upload in jobs {
                if !pending_hash.iter().any(|u| u == &upload) {
                    continue;
                }
                source_job.dependents.push(upload.clone());
                let job = PlannedJob {
                    kind: "copy-dependent".into(),
                    upload_uuid: upload.clone(),
                    source_external_id: None,
                    dependents: Vec::new(),
                };
                self.jobs.insert(upload.clone(), job);
                pending_hash.retain(|u| u != &upload);
                pending_ids.retain(|u| u != &upload);
            }
            self.jobs.insert(source, source_job);
        }
    }

    fn external_id(&self, uuid: &str) -> Option<String> {
        self.all
            .iter()
            .chain(self.pending.iter())
            .find(|u| u.uuid() == uuid)
            .and_then(|u| u.external_id())
    }
}
