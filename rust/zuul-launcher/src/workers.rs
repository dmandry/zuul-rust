// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// Cleanup, stats election, and upload worker pools from zuul/launcher/server.py.
// Prefer std::thread; pool sizes and statsd names match 14.3.0.

use crate::cloud;
use crate::launcher::{normalize_statsd_name, StatEvent, CLEANUP_INTERVAL, STATS_INTERVAL};
use crate::mmh3;
use crate::upload::{
    PlannedJob, ENDPOINT_UPLOAD_WORKERS, IMPORT_WORKERS, UPLOAD_WORKERS,
};
use crate::zk::{ComponentInfo, CreateMode, ZkError, ZkOps};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub const STATS_ELECTION_PATH: &str = "/zuul/launcher/stats-election";

/// Endpoint identity used by CleanupWorker matching (server.py getMatchingEndpoints).
#[derive(Clone, Debug)]
pub struct EndpointRef {
    pub name: String,
    pub canonical_name: String,
    pub connection_name: String,
}

/// Endpoints this launcher wins for cleanup, scored with mmh3 like Python.
pub fn matching_endpoints<'a>(
    hostname: &str,
    components: &[ComponentInfo],
    endpoints: &'a [EndpointRef],
) -> Vec<&'a EndpointRef> {
    let all_launchers: HashMap<&str, &ComponentInfo> = components
        .iter()
        .map(|c| (c.hostname.as_str(), c))
        .collect();
    let mut won = Vec::new();
    for endpoint in endpoints {
        let candidate_names: Vec<String> = all_launchers
            .values()
            .filter(|c| match &c.connection_filter {
                None => true,
                Some(filter) if filter.is_empty() => true,
                Some(filter) => filter.iter().any(|n| n == &endpoint.connection_name),
            })
            .map(|c| c.hostname.clone())
            .collect();
        let mut scores: Vec<(u32, String)> = candidate_names
            .iter()
            .map(|n| (mmh3::endpoint_score(n, &endpoint.canonical_name), n.clone()))
            .collect();
        scores.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        for (_score, launcher_name) in scores {
            let Some(launcher) = all_launchers.get(launcher_name.as_str()) else {
                continue;
            };
            if launcher.state != "running" {
                continue;
            }
            if launcher.hostname == hostname {
                won.push(endpoint);
            }
            break;
        }
    }
    won
}

/// Periodic leaked-resource cleanup (CleanupWorker, INTERVAL=60).
pub struct CleanupWorker {
    running: Arc<AtomicBool>,
    wake: Arc<(Mutex<bool>, Condvar)>,
    thread: Option<JoinHandle<()>>,
}

impl CleanupWorker {
    pub const INTERVAL_SECS: f64 = CLEANUP_INTERVAL;

    pub fn start<F>(mut tick: F) -> Self
    where
        F: FnMut() + Send + 'static,
    {
        let running = Arc::new(AtomicBool::new(true));
        let wake = Arc::new((Mutex::new(false), Condvar::new()));
        let running_t = running.clone();
        let wake_t = wake.clone();
        let interval = Duration::from_secs_f64(CLEANUP_INTERVAL);
        let thread = thread::Builder::new()
            .name("CleanupWorker".into())
            .spawn(move || {
                while running_t.load(Ordering::SeqCst) {
                    // Wait before the first cleanup, matching Python.
                    let (lock, cv) = &*wake_t;
                    let signaled = lock.lock().unwrap();
                    let (mut signaled, _) = cv
                        .wait_timeout_while(signaled, interval, |s| {
                            !*s && running_t.load(Ordering::SeqCst)
                        })
                        .unwrap();
                    *signaled = false;
                    drop(signaled);
                    if !running_t.load(Ordering::SeqCst) {
                        break;
                    }
                    tick();
                }
            })
            .expect("spawn CleanupWorker");
        Self {
            running,
            wake,
            thread: Some(thread),
        }
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        self.wake();
    }

    pub fn wake(&self) {
        let (lock, cv) = &*self.wake;
        if let Ok(mut g) = lock.lock() {
            *g = true;
            cv.notify_all();
        }
    }

    pub fn join(mut self) {
        self.stop();
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// Kazoo-compatible ephemeral sequential election (Election over Lock).
pub struct StatsElection {
    zk: Arc<dyn ZkOps>,
    path: String,
    cancelled: Arc<AtomicBool>,
    wake: Arc<(Mutex<()>, Condvar)>,
    held: Mutex<Option<String>>,
    identifier: String,
}

impl StatsElection {
    pub fn new(zk: Arc<dyn ZkOps>, identifier: &str) -> Self {
        Self {
            zk,
            path: STATS_ELECTION_PATH.into(),
            cancelled: Arc::new(AtomicBool::new(false)),
            wake: Arc::new((Mutex::new(()), Condvar::new())),
            held: Mutex::new(None),
            identifier: identifier.into(),
        }
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        if let Ok(mut held) = self.held.lock() {
            if let Some(path) = held.take() {
                let _ = self.zk.delete(&path, false);
            }
        }
        let (_lock, cv) = &*self.wake;
        cv.notify_all();
    }

    pub fn is_still_valid(&self) -> bool {
        !self.cancelled.load(Ordering::SeqCst)
            && self
                .held
                .lock()
                .map(|h| h.is_some())
                .unwrap_or(false)
    }

    /// Block until cancelled or leadership is won; then run `func`.
    /// Matches kazoo.recipe.election.Election.run (CancelledError -> return).
    pub fn run<F>(&self, mut func: F)
    where
        F: FnMut(),
    {
        self.cancelled.store(false, Ordering::SeqCst);
        match self.acquire() {
            Ok(true) => {
                func();
                self.release();
            }
            Ok(false) | Err(_) => {
                self.release();
            }
        }
    }

    fn acquire(&self) -> Result<bool, ZkError> {
        self.zk.ensure_path(&self.path)?;
        let uuid_hex = format!(
            "{:032x}",
            self.zk.session_id().unsigned_abs() as u128
                ^ std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(1)
        );
        let prefix = format!("{}/{uuid_hex}__lock__", self.path);
        let full = self
            .zk
            .create(&prefix, self.identifier.as_bytes(), CreateMode::EphemeralSequential)?;
        {
            let mut held = self.held.lock().unwrap();
            *held = Some(full.clone());
        }
        let child = full[self.path.len() + 1..].to_string();
        loop {
            if self.cancelled.load(Ordering::SeqCst) {
                return Ok(false);
            }
            if self.is_winner(&child)? {
                return Ok(true);
            }
            let (lock, cv) = &*self.wake;
            let guard = lock.lock().unwrap();
            let (_g, _) = cv
                .wait_timeout(guard, Duration::from_millis(200))
                .unwrap();
        }
    }

    fn is_winner(&self, child: &str) -> Result<bool, ZkError> {
        let kids = self.zk.get_children(&self.path)?;
        let Some(mine) = sequence_tail(child) else {
            return Ok(false);
        };
        for other in kids {
            if other == child {
                continue;
            }
            if !other.contains("__lock__") {
                continue;
            }
            let Some(seq) = sequence_tail(&other) else {
                continue;
            };
            if seq < mine {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn release(&self) {
        if let Ok(mut held) = self.held.lock() {
            if let Some(path) = held.take() {
                let _ = self.zk.delete(&path, false);
            }
        }
    }
}

fn sequence_tail(child: &str) -> Option<&str> {
    if child.len() < 10 {
        return None;
    }
    let tail = &child[child.len() - 10..];
    if tail.chars().all(|c| c.is_ascii_digit()) {
        Some(tail)
    } else {
        None
    }
}

/// Stats election thread (runStatsElection / runStats).
pub struct StatsWorker {
    running: Arc<AtomicBool>,
    election: Arc<StatsElection>,
    stop_wake: Arc<(Mutex<bool>, Condvar)>,
    thread: Option<JoinHandle<()>>,
}

impl StatsWorker {
    pub fn start<F>(zk: Arc<dyn ZkOps>, hostname: &str, mut run_stats: F) -> Self
    where
        F: FnMut() + Send + 'static,
    {
        let running = Arc::new(AtomicBool::new(true));
        let election = Arc::new(StatsElection::new(zk, hostname));
        let stop_wake = Arc::new((Mutex::new(false), Condvar::new()));
        let running_t = running.clone();
        let election_t = election.clone();
        let stop_wake_t = stop_wake.clone();
        let interval = Duration::from_secs_f64(STATS_INTERVAL);
        let thread = thread::Builder::new()
            .name("StatsElection".into())
            .spawn(move || {
                while running_t.load(Ordering::SeqCst) {
                    election_t.run(|| {
                        // Won stats election: emit every STATS_INTERVAL until
                        // stop or the election is cancelled / lost.
                        // Matches runStats: while not stop_event.wait(interval).
                        loop {
                            let (lock, cv) = &*stop_wake_t;
                            let signaled = lock.lock().unwrap();
                            let (mut signaled, wait_result) = cv
                                .wait_timeout_while(signaled, interval, |s| {
                                    !*s && running_t.load(Ordering::SeqCst)
                                })
                                .unwrap();
                            let stop_set = *signaled;
                            *signaled = false;
                            drop(signaled);
                            if stop_set || !running_t.load(Ordering::SeqCst) {
                                return;
                            }
                            if !wait_result.timed_out() {
                                continue;
                            }
                            if !election_t.is_still_valid() {
                                return;
                            }
                            run_stats();
                        }
                    });
                    if !running_t.load(Ordering::SeqCst) {
                        break;
                    }
                    thread::sleep(Duration::from_secs(1));
                }
            })
            .expect("spawn StatsWorker");
        Self {
            running,
            election,
            stop_wake,
            thread: Some(thread),
        }
    }

    /// Unblock Election.run like Python stats_election.cancel().
    pub fn cancel(&self) {
        self.running.store(false, Ordering::SeqCst);
        self.election.cancel();
        let (lock, cv) = &*self.stop_wake;
        if let Ok(mut g) = lock.lock() {
            *g = true;
            cv.notify_all();
        }
    }

    pub fn join(mut self) {
        self.cancel();
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

type JobFn = Box<dyn FnOnce() + Send + 'static>;

struct MonitoringPool {
    tx: Mutex<Option<Sender<JobFn>>>,
    workers: Vec<JoinHandle<()>>,
    #[allow(dead_code)]
    current: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
    metric: String,
    gauge_sink: Arc<Mutex<Vec<StatEvent>>>,
}

impl MonitoringPool {
    fn new(
        name: &str,
        workers: usize,
        metric: String,
        gauge_sink: Arc<Mutex<Vec<StatEvent>>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<JobFn>();
        let rx = Arc::new(Mutex::new(rx));
        let current = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        emit_gauge(&gauge_sink, &metric, 0.0);
        let mut handles = Vec::with_capacity(workers);
        for i in 0..workers {
            let rx = rx.clone();
            let current = current.clone();
            let shutdown = shutdown.clone();
            let metric = metric.clone();
            let gauge_sink = gauge_sink.clone();
            let handle = thread::Builder::new()
                .name(format!("{name}-{i}"))
                .spawn(move || pool_worker(rx, current, shutdown, metric, gauge_sink))
                .expect("spawn pool worker");
            handles.push(handle);
        }
        Self {
            tx: Mutex::new(Some(tx)),
            workers: handles,
            current,
            shutdown,
            metric,
            gauge_sink,
        }
    }

    fn submit<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if self.shutdown.load(Ordering::SeqCst) {
            return;
        }
        if let Ok(guard) = self.tx.lock() {
            if let Some(tx) = guard.as_ref() {
                let _ = tx.send(Box::new(job));
            }
        }
    }

    fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Ok(mut guard) = self.tx.lock() {
            guard.take();
        }
        for t in self.workers.drain(..) {
            let _ = t.join();
        }
        emit_gauge(&self.gauge_sink, &self.metric, 0.0);
    }
}

fn pool_worker(
    rx: Arc<Mutex<Receiver<JobFn>>>,
    current: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
    metric: String,
    gauge_sink: Arc<Mutex<Vec<StatEvent>>>,
) {
    loop {
        if shutdown.load(Ordering::SeqCst) {
            // Drain remaining without accepting new work after channel close.
        }
        let job = {
            let guard = rx.lock().unwrap();
            guard.recv()
        };
        match job {
            Ok(job) => {
                let n = current.fetch_add(1, Ordering::SeqCst) + 1;
                emit_gauge(&gauge_sink, &metric, n as f64);
                job();
                let n = current.fetch_sub(1, Ordering::SeqCst) - 1;
                emit_gauge(&gauge_sink, &metric, n as f64);
            }
            Err(_) => break,
        }
    }
}

fn emit_gauge(sink: &Mutex<Vec<StatEvent>>, name: &str, value: f64) {
    let event = StatEvent {
        kind: "g".into(),
        name: name.into(),
        value,
    };
    crate::statsd::emit(&event);
    if let Ok(mut g) = sink.lock() {
        g.push(event);
    }
}

/// Three MonitoringThreadPoolExecutor pools (1 / 10 / 20).
pub struct UploadPools {
    upload: MonitoringPool,
    endpoint_upload: MonitoringPool,
    endpoint_import: MonitoringPool,
    pub gauge_sink: Arc<Mutex<Vec<StatEvent>>>,
}

impl UploadPools {
    pub fn metric_upload(hostname: &str) -> String {
        format!(
            "zuul.launcher.server.{}.upload_workers",
            normalize_statsd_name(hostname)
        )
    }

    pub fn metric_endpoint_upload(hostname: &str) -> String {
        format!(
            "zuul.launcher.server.{}.endpoint_upload_workers",
            normalize_statsd_name(hostname)
        )
    }

    pub fn metric_endpoint_import(hostname: &str) -> String {
        format!(
            "zuul.launcher.server.{}.endpoint_import_workers",
            normalize_statsd_name(hostname)
        )
    }

    pub fn new(hostname: &str) -> Self {
        let gauge_sink = Arc::new(Mutex::new(Vec::new()));
        Self {
            upload: MonitoringPool::new(
                "UploadWorker",
                UPLOAD_WORKERS,
                Self::metric_upload(hostname),
                gauge_sink.clone(),
            ),
            endpoint_upload: MonitoringPool::new(
                "EndpointUploadWorker",
                ENDPOINT_UPLOAD_WORKERS,
                Self::metric_endpoint_upload(hostname),
                gauge_sink.clone(),
            ),
            endpoint_import: MonitoringPool::new(
                "EndpointImportWorker",
                IMPORT_WORKERS,
                Self::metric_endpoint_import(hostname),
                gauge_sink.clone(),
            ),
            gauge_sink,
        }
    }

    pub fn submit_upload<F>(&self, job: PlannedJob, body: F)
    where
        F: FnOnce(PlannedJob) + Send + 'static,
    {
        self.upload.submit(move || body(job));
    }

    pub fn submit_endpoint_upload<F>(&self, job: PlannedJob, body: F)
    where
        F: FnOnce(PlannedJob) + Send + 'static,
    {
        self.endpoint_upload.submit(move || body(job));
    }

    pub fn submit_import<F>(&self, job: PlannedJob, body: F)
    where
        F: FnOnce(PlannedJob) + Send + 'static,
    {
        self.endpoint_import.submit(move || body(job));
    }

    pub fn shutdown(mut self) {
        self.upload.shutdown();
        self.endpoint_upload.shutdown();
        self.endpoint_import.shutdown();
    }

    pub fn take_gauges(&self) -> Vec<StatEvent> {
        self.gauge_sink
            .lock()
            .map(|mut g| std::mem::take(&mut *g))
            .unwrap_or_default()
    }
}

/// Two-cycle leak ids, keyed by endpoint canonical name.
#[derive(Clone, Debug, Default)]
pub struct LeakBook {
    pub nodes: HashMap<String, HashSet<String>>,
    pub uploads: HashMap<String, HashSet<String>>,
    pub resources: HashMap<String, HashSet<String>>,
}

/// One CleanupWorker._run. SDK list/delete stay in Python; matching is here.
pub fn cleanup_pass(book: &mut LeakBook) {
    if !cloud::enabled() {
        return;
    }
    let Ok(roster) = cloud::invoke("cleanup.roster", &json!({})) else {
        return;
    };
    let launchers = parse_launchers(roster.get("launchers"));
    let endpoints = parse_endpoints(roster.get("endpoints"));
    let mine = match cloud_hostname() {
        Some(id) if !id.is_empty() => id,
        _ => {
            let running: Vec<&ComponentInfo> =
                launchers.iter().filter(|c| c.state == "running").collect();
            if running.len() == 1 {
                running[0].hostname.clone()
            } else {
                return;
            }
        }
    };
    let matched = matching_endpoints(&mine, &launchers, &endpoints);
    let now = model_now();
    for endpoint in matched {
        let canonical = endpoint.canonical_name.clone();
        let Ok(inv) = cloud::invoke(
            "cleanup.inspect",
            &json!({"canonical_name": canonical}),
        ) else {
            continue;
        };
        if inv.get("error").and_then(|v| v.as_bool()).unwrap_or(false) {
            if inv.get("partial").and_then(|v| v.as_bool()).unwrap_or(false) {
                let system_id = inv
                    .get("system_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let node_ids = node_id_set(inv.get("nodes"));
                let quota = unmanaged_quota_for(inv.get("instances"), system_id, &node_ids);
                let _ = cloud::invoke(
                    "cleanup.apply",
                    &json!({
                        "canonical_name": canonical,
                        "deletes": [],
                        "unmanaged_quota": quota,
                        "gauges": [],
                    }),
                );
            }
            continue;
        }
        let (deletes, gauges, quota) = consume_inventory(book, &canonical, &inv, now);
        let _ = cloud::invoke(
            "cleanup.apply",
            &json!({
                "canonical_name": canonical,
                "deletes": deletes,
                "unmanaged_quota": quota,
                "gauges": gauges,
            }),
        );
    }
}

/// One endpoint's leak decision. Book updates and stat names match
/// CleanupWorker.cleanupLeakedResources.
fn consume_inventory(book: &mut LeakBook, canonical: &str, inv: &Value, now: i64) -> (Vec<Value>, Vec<Value>, Value) {
    let system_id = inv
        .get("system_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let node_ids = node_id_set(inv.get("nodes"));
    let upload_ids = string_set(inv.get("upload_ids"));
    let mut required = string_set(inv.get("required_infra"));
    if let Some(nodes) = inv.get("nodes").and_then(|v| v.as_array()) {
        for node in nodes {
            if let Some(req) = node.get("required").and_then(|v| v.as_array()) {
                for item in req {
                    if let Some(s) = item.as_str() {
                        required.insert(s.to_string());
                    }
                }
            }
        }
    }
    let quota = unmanaged_quota_for(inv.get("instances"), &system_id, &node_ids);
    let safe = normalize_statsd_name(canonical);
    let types: Vec<String> = inv
        .get("resource_types")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
        .unwrap_or_default();
    let mut unmanaged: BTreeMap<String, i64> = types.iter().map(|t| (t.clone(), 0)).collect();
    let mut leaked: BTreeMap<String, i64> = types.iter().map(|t| (t.clone(), 0)).collect();
    let prev_nodes = book.nodes.get(canonical).cloned().unwrap_or_default();
    let prev_uploads = book.uploads.get(canonical).cloned().unwrap_or_default();
    let prev_resources = book.resources.get(canonical).cloned().unwrap_or_default();
    let mut new_nodes = HashSet::new();
    let mut new_uploads = HashSet::new();
    let mut new_resources = HashSet::new();
    let mut deletes: Vec<Value> = Vec::new();
    let resources = inv.get("resources").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    for resource in resources {
        let meta = resource.get("metadata").and_then(|v| v.as_object());
        let meta_str = |key: &str| -> Option<String> {
            meta.and_then(|m| m.get(key)).and_then(|v| match v {
                Value::String(s) => Some(s.clone()),
                Value::Number(n) => Some(n.to_string()),
                Value::Bool(b) => Some(b.to_string()),
                _ => None,
            })
        };
        let rtype = resource
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let unique_id = resource
            .get("unique_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if meta_str("zuul_system_id").as_deref() != Some(system_id.as_str()) {
            *unmanaged.entry(rtype).or_insert(0) += 1;
            continue;
        }
        let metric = format!("zuul.provider_endpoint.{safe}.leaked.{rtype}");
        let node_id = meta_str("zuul_node_uuid");
        let upload_id = meta_str("zuul_upload_uuid");
        let expiration = meta_str("zuul_expiration");
        if let Some(node_id) = node_id.clone() {
            if !node_id.is_empty() && !node_ids.contains(&node_id) {
                new_nodes.insert(node_id.clone());
                if prev_nodes.contains(&node_id) {
                    *leaked.entry(rtype.clone()).or_insert(0) += 1;
                    deletes.push(delete_item(
                        &resource,
                        true,
                        &metric,
                        &format!("Unable to delete leaked resource for node {node_id}"),
                    ));
                }
            }
        }
        if let Some(upload_id) = upload_id.clone() {
            if !upload_id.is_empty() && !upload_ids.contains(&upload_id) {
                new_uploads.insert(upload_id.clone());
                if prev_uploads.contains(&upload_id) {
                    *leaked.entry(rtype.clone()).or_insert(0) += 1;
                    deletes.push(delete_item(
                        &resource,
                        true,
                        &metric,
                        &format!("Unable to delete leaked resource for upload {upload_id}"),
                    ));
                }
            }
        }
        if let Some(exp) = expiration.clone() {
            if let Ok(exp_i) = exp.parse::<i64>() {
                if now > exp_i {
                    *leaked.entry(rtype.clone()).or_insert(0) += 1;
                    deletes.push(delete_item(
                        &resource,
                        false,
                        &metric,
                        "Unable to delete leaked resource",
                    ));
                }
            }
        }
        let has_tag = node_id.as_ref().map(|s| !s.is_empty()).unwrap_or(false)
            || upload_id.as_ref().map(|s| !s.is_empty()).unwrap_or(false)
            || expiration.as_ref().map(|s| !s.is_empty()).unwrap_or(false);
        if !has_tag && !required.contains(&unique_id) {
            new_resources.insert(unique_id.clone());
            if prev_resources.contains(&unique_id) {
                *leaked.entry(rtype).or_insert(0) += 1;
                deletes.push(delete_item(
                    &resource,
                    true,
                    &metric,
                    "Unable to delete leaked resource",
                ));
            }
        }
    }
    book.nodes.insert(canonical.to_string(), new_nodes);
    book.uploads.insert(canonical.to_string(), new_uploads);
    book.resources.insert(canonical.to_string(), new_resources);
    let mut gauges = Vec::new();
    let mut seen = HashSet::new();
    for rtype in &types {
        seen.insert(rtype.clone());
        gauges.push(json!({
            "name": format!("zuul.provider_endpoint.{safe}.unmanaged.{rtype}"),
            "value": unmanaged.get(rtype).copied().unwrap_or(0),
        }));
    }
    for (rtype, value) in &unmanaged {
        if seen.contains(rtype) {
            continue;
        }
        gauges.push(json!({
            "name": format!("zuul.provider_endpoint.{safe}.unmanaged.{rtype}"),
            "value": value,
        }));
    }
    for rtype in &types {
        gauges.push(json!({
            "name": format!("zuul.provider_endpoint.{safe}.leaked.{rtype}"),
            "value": leaked.get(rtype).copied().unwrap_or(0),
        }));
    }
    for (rtype, value) in &leaked {
        if seen.contains(rtype) {
            continue;
        }
        gauges.push(json!({
            "name": format!("zuul.provider_endpoint.{safe}.leaked.{rtype}"),
            "value": value,
        }));
    }
    (deletes, gauges, quota)
}

/// Node the native cleanup worker uses when deciding required resources.
#[derive(Clone, Debug)]
pub struct CleanupNode {
    pub uuid: String,
    pub endpoint_name: String,
    pub required: Vec<String>,
}

/// Launcher state for one native cleanup pass.
#[derive(Clone, Debug)]
pub struct CleanupCtx {
    pub hostname: String,
    pub system_id: String,
    pub components: Vec<ComponentInfo>,
    pub endpoints: Vec<EndpointRef>,
    pub nodes: Vec<CleanupNode>,
    pub upload_ids: Vec<String>,
}

/// CleanupWorker._run when the binary has no Python cloud handler.
pub fn native_cleanup_pass(book: &mut LeakBook, ctx: &CleanupCtx) -> Vec<StatEvent> {
    let matched = matching_endpoints(&ctx.hostname, &ctx.components, &ctx.endpoints);
    let now = model_now();
    let mut events = Vec::new();
    for endpoint in matched {
        let canonical = endpoint.canonical_name.clone();
        let listed = crate::native::endpoint_inventory(&canonical);
        if listed.error && !listed.partial {
            continue;
        }
        let mut nodes = Vec::new();
        for node in &ctx.nodes {
            let required = if node.endpoint_name == endpoint.name {
                node.required.clone()
            } else {
                Vec::new()
            };
            nodes.push(json!({
                "uuid": node.uuid,
                "endpoint_name": node.endpoint_name,
                "required": required,
            }));
        }
        let inv = json!({
            "error": false,
            "system_id": ctx.system_id,
            "nodes": nodes,
            "upload_ids": ctx.upload_ids,
            "required_infra": listed.required_infra,
            "resource_types": listed.resource_types,
            "resources": if listed.partial { Vec::<Value>::new() } else { listed.resources },
            "instances": listed.instances,
        });
        if listed.partial {
            let node_ids = node_id_set(inv.get("nodes"));
            let quota = unmanaged_quota_for(inv.get("instances"), &ctx.system_id, &node_ids);
            let mut map = BTreeMap::new();
            if let Some(obj) = quota.as_object() {
                for (k, v) in obj {
                    if let Some(n) = v.as_f64() {
                        map.insert(k.clone(), n);
                    }
                }
            }
            crate::native::set_unmanaged_quota(&canonical, &map);
            continue;
        }
        let (deletes, gauges, quota) = consume_inventory(book, &canonical, &inv, now);
        for item in &deletes {
            if item.get("count").and_then(|v| v.as_bool()).unwrap_or(false) {
                if let Some(metric) = item.get("metric").and_then(|v| v.as_str()) {
                    events.push(StatEvent {
                        kind: "c".into(),
                        name: metric.to_string(),
                        value: 1.0,
                    });
                }
            }
            let _ = crate::native::delete_listed(&canonical, item);
        }
        for gauge in &gauges {
            if let (Some(name), Some(value)) = (
                gauge.get("name").and_then(|v| v.as_str()),
                gauge.get("value").and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|n| n as f64))),
            ) {
                events.push(StatEvent {
                    kind: "g".into(),
                    name: name.to_string(),
                    value,
                });
            }
        }
        let mut map = BTreeMap::new();
        if let Some(obj) = quota.as_object() {
            for (k, v) in obj {
                if let Some(n) = v.as_f64() {
                    map.insert(k.clone(), n);
                }
            }
        }
        crate::native::set_unmanaged_quota(&canonical, &map);
    }
    events
}

fn cloud_hostname() -> Option<String> {
    // enter() binds the handler id, which is the launcher hostname.
    crate::cloud::current_id()
}

fn model_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn delete_item(resource: &Value, count: bool, metric: &str, error: &str) -> Value {
    json!({
        "type": resource.get("type"),
        "id": resource.get("id"),
        "unique_id": resource.get("unique_id"),
        "bucket_name": resource.get("bucket_name"),
        "metadata": resource.get("metadata"),
        "count": count,
        "metric": metric,
        "error": error,
    })
}

fn parse_launchers(value: Option<&Value>) -> Vec<ComponentInfo> {
    let mut out = Vec::new();
    let Some(list) = value.and_then(|v| v.as_array()) else {
        return out;
    };
    for item in list {
        let hostname = item.get("hostname").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if hostname.is_empty() {
            continue;
        }
        let state = item.get("state").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let connection_filter = match item.get("connection_filter") {
            Some(Value::Array(a)) => Some(
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect(),
            ),
            _ => None,
        };
        out.push(ComponentInfo {
            hostname,
            state,
            connection_filter,
        });
    }
    out
}

fn parse_endpoints(value: Option<&Value>) -> Vec<EndpointRef> {
    let mut out = Vec::new();
    let Some(list) = value.and_then(|v| v.as_array()) else {
        return out;
    };
    for item in list {
        out.push(EndpointRef {
            name: item.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
            canonical_name: item
                .get("canonical_name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            connection_name: item
                .get("connection_name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        });
    }
    out
}

fn node_id_set(value: Option<&Value>) -> HashSet<String> {
    let mut out = HashSet::new();
    let Some(list) = value.and_then(|v| v.as_array()) else {
        return out;
    };
    for item in list {
        if let Some(uuid) = item.get("uuid").and_then(|v| v.as_str()) {
            out.insert(uuid.to_string());
        }
    }
    out
}

fn string_set(value: Option<&Value>) -> HashSet<String> {
    let mut out = HashSet::new();
    let Some(list) = value.and_then(|v| v.as_array()) else {
        return out;
    };
    for item in list {
        if let Some(s) = item.as_str() {
            out.insert(s.to_string());
        }
    }
    out
}

fn unmanaged_quota_for(inv_instances: Option<&Value>, system_id: &str, node_ids: &HashSet<String>) -> Value {
    let mut quota: BTreeMap<String, f64> = BTreeMap::new();
    let Some(list) = inv_instances.and_then(|v| v.as_array()) else {
        return json!({});
    };
    for instance in list {
        let meta = instance.get("metadata");
        let sys = meta
            .and_then(|m| m.get("zuul_system_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let node = meta
            .and_then(|m| m.get("zuul_node_uuid"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if sys == system_id && node_ids.contains(node) {
            continue;
        }
        add_quota(&mut quota, instance.get("quota"));
    }
    json!(quota)
}

fn add_quota(quota: &mut BTreeMap<String, f64>, other: Option<&Value>) {
    let Some(obj) = other.and_then(|v| v.as_object()) else {
        return;
    };
    for key in obj.keys() {
        quota.entry(key.clone()).or_insert(0.0);
    }
    let keys: Vec<String> = quota.keys().cloned().collect();
    for key in keys {
        let other_value = obj.get(&key).and_then(|v| v.as_f64()).unwrap_or(0.0);
        if let Some(slot) = quota.get_mut(&key) {
            *slot += other_value;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zk::MemoryZk;

    #[test]
    fn matching_endpoints_picks_lowest_score_running() {
        let endpoints = vec![EndpointRef {
            name: "ep".into(),
            canonical_name: "conn/ep".into(),
            connection_name: "conn".into(),
        }];
        let components = vec![
            ComponentInfo {
                hostname: "a".into(),
                state: "running".into(),
                connection_filter: None,
            },
            ComponentInfo {
                hostname: "b".into(),
                state: "running".into(),
                connection_filter: None,
            },
        ];
        let scores_a = mmh3::endpoint_score("a", "conn/ep");
        let scores_b = mmh3::endpoint_score("b", "conn/ep");
        let winner = if scores_a <= scores_b { "a" } else { "b" };
        let won = matching_endpoints(winner, &components, &endpoints);
        assert_eq!(won.len(), 1);
        let lost = matching_endpoints(
            if winner == "a" { "b" } else { "a" },
            &components,
            &endpoints,
        );
        assert!(lost.is_empty());
    }

    #[test]
    fn stats_election_cancel_unblocks() {
        let zk: Arc<dyn ZkOps> = Arc::new(MemoryZk::new());
        // Hold leadership with a second contender so the test election
        // blocks in acquire(); cancel must unblock that wait.
        let blocker = StatsElection::new(zk.clone(), "blocker");
        assert!(blocker.acquire().unwrap());
        let election = Arc::new(StatsElection::new(zk, "host"));
        let election2 = election.clone();
        let handle = thread::spawn(move || {
            election2.run(|| {
                panic!("should not win while blocker holds election");
            });
        });
        thread::sleep(Duration::from_millis(100));
        election.cancel();
        handle.join().unwrap();
        blocker.release();
    }
}
