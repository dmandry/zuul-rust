// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// Cleanup, stats election, and upload worker pools from zuul/launcher/server.py.
// Prefer std::thread; pool sizes and statsd names match 14.3.0.

use crate::launcher::{normalize_statsd_name, StatEvent, CLEANUP_INTERVAL, STATS_INTERVAL};
use crate::mmh3;
use crate::upload::{
    PlannedJob, ENDPOINT_UPLOAD_WORKERS, IMPORT_WORKERS, UPLOAD_WORKERS,
};
use crate::zk::{ComponentInfo, CreateMode, ZkError, ZkOps};
use std::collections::HashMap;
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
    if let Ok(mut g) = sink.lock() {
        g.push(StatEvent {
            kind: "g".into(),
            name: name.into(),
            value,
        });
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
