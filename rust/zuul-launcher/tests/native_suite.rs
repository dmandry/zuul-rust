// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// Native launcher suite. The Rust launcher runs in this process. Python is a
// separate fixture process (ZooKeeper chroot, gerrit, moto server) and is not
// linked into the launcher.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{json, Value};
use zuul_launcher::config::{provider_connections, zookeeper_config, Config};
use zuul_launcher::launcher::Launcher;
use zuul_launcher::native;
use zuul_launcher::suite::{self, Check};
use zuul_launcher::tlszk::TlsZk;
use zuul_launcher::zk::{self, ZkOps};

const PYTHON: &str = "/home/ubuntu/zuul-nox-311/bin/python";
const ROOT: &str = "/workspace";

#[derive(Deserialize)]
struct Begin {
    ok: bool,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    config_path: String,
    #[serde(default)]
    launchers_path: String,
    #[serde(default)]
    env: serde_json::Map<String, Value>,
}

#[derive(Deserialize)]
struct ExecReply {
    #[serde(default)]
    checks: Vec<Check>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
struct EndReply {
    #[serde(default)]
    error: Option<String>,
}

#[derive(Clone, Deserialize)]
struct LaunchRecord {
    hostname: String,
    connection_filter: Option<Vec<String>>,
}

struct Fixture {
    child: Child,
    stdin: std::process::ChildStdin,
    stdout: BufReader<std::process::ChildStdout>,
}

impl Fixture {
    fn spawn(log_path: &std::path::Path) -> Self {
        let _ = fs::create_dir_all("/tmp/zuul-test");
        let _ = fs::create_dir_all("/tmp/zuul-suite-logs");
        let log = File::create(log_path).expect("suite log");
        let mut cmd = Command::new(PYTHON);
        cmd.arg(format!("{ROOT}/tools/launcher_suite_ctl.py"))
            .current_dir(ROOT)
            .env_remove("ZUUL_LAUNCHER_IMPL")
            .env("CI", "1")
            .env("ZUUL_ZK_CA", "/opt/zookeeper/ca/certs/cacert.pem")
            .env("ZUUL_ZK_CERT", "/opt/zookeeper/ca/certs/client.pem")
            .env("ZUUL_ZK_KEY", "/opt/zookeeper/ca/keys/clientkey.pem")
            .env("ZUUL_TEST_ROOT", "/tmp/zuul-test")
            .env("OS_LOG_CAPTURE", "1")
            .env("OS_STDERR_CAPTURE", "1")
            .env("OS_STDOUT_CAPTURE", "1")
            .env("OS_TEST_TIMEOUT", "360")
            .env("PYTHONTRACEMALLOC", "0")
            .env("PYTHONUNBUFFERED", "1")
            .env(
                "PATH",
                "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/home/ubuntu/zuul-nox-311/bin:/home/ubuntu/.local/bin",
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(log));
        let mut child = cmd.spawn().expect("spawn launcher fixture");
        let stdin = child.stdin.take().expect("stdin");
        let stdout = BufReader::new(child.stdout.take().expect("stdout"));
        Self { child, stdin, stdout }
    }

    fn call(&mut self, payload: Value) -> Value {
        let line = payload.to_string();
        writeln!(self.stdin, "{line}").expect("write fixture command");
        let _ = self.stdin.flush();
        let mut response = String::new();
        self.stdout
            .read_line(&mut response)
            .expect("read fixture response");
        if response.is_empty() {
            panic!("fixture closed stdout");
        }
        serde_json::from_str(&response).unwrap_or_else(|err| {
            panic!("fixture reply is not json ({err}): {response}")
        })
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn selected(class_name: &str, method: &str) -> bool {
    match std::env::var("ZUUL_SUITE_FILTER") {
        Ok(filter) if !filter.is_empty() => {
            let id = format!("{class_name}.{method}");
            filter.split(',').any(|item| id.contains(item.trim()))
        }
        _ => true,
    }
}

fn record_time(class_name: &str, method: &str, seconds: f64) {
    let path = std::env::var("ZUUL_SUITE_TIMES")
        .unwrap_or_else(|_| "/tmp/zuul-suite-times-native.txt".into());
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{class_name}.{method} {seconds:.4}");
    }
    eprintln!("{class_name}.{method} {seconds:.4}s");
}

fn launcher_loop(
    spec: LaunchRecord,
    config_path: String,
    stop: Arc<std::sync::atomic::AtomicBool>,
    ready: Arc<std::sync::atomic::AtomicBool>,
    wakes: Arc<Mutex<Vec<(Arc<Mutex<bool>>, Arc<Condvar>)>>>,
) {
    let cfg_path = PathBuf::from(&config_path);
    let cfg = match Config::load_zuul(Some(cfg_path.as_path())) {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("launcher config: {err}");
            ready.store(true, std::sync::atomic::Ordering::SeqCst);
            return;
        }
    };
    let zk_cfg = match zookeeper_config(&cfg) {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("zookeeper config: {err}");
            ready.store(true, std::sync::atomic::Ordering::SeqCst);
            return;
        }
    };
    static CONNECTIONS: OnceLock<()> = OnceLock::new();
    CONNECTIONS.get_or_init(|| {
        if let Ok(connections) = provider_connections(&cfg) {
            native::set_connections(connections);
        }
    });
    let timeout_ms = (zk_cfg.session_timeout * 1000.0) as i32;
    let zk = match TlsZk::connect(
        &zk_cfg.hosts,
        &zk_cfg.tls_cert,
        &zk_cfg.tls_key,
        &zk_cfg.tls_ca,
        timeout_ms.max(1),
    ) {
        Ok(zk) => zk,
        Err(err) => {
            eprintln!("zk connect {}: {err}", spec.hostname);
            ready.store(true, std::sync::atomic::Ordering::SeqCst);
            return;
        }
    };
    let ops: Arc<dyn ZkOps> = zk;
    let mut launcher = Launcher::new(&spec.hostname, ops.clone());
    launcher.connection_filter = spec.connection_filter.clone();
    launcher.api.connection_filter = spec.connection_filter.clone();
    match zk::register_component(
        ops.as_ref(),
        &spec.hostname,
        "stopped",
        &launcher.connection_filter,
    ) {
        Ok(path) => launcher.component_path = Some(path),
        Err(err) => eprintln!("register {}: {err}", spec.hostname),
    }
    // Unit tests set Launcher._start_cleanup = False.
    launcher.start_workers(false);
    launcher.install_waker(None);
    launcher.arm_watches();
    wakes.lock().unwrap().push((launcher.wake.clone(), launcher.wake_cv.clone()));
    let mut announced = false;
    while !stop.load(std::sync::atomic::Ordering::SeqCst) {
        let started = Instant::now();
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            launcher.api.prepare();
            if launcher.api.take_layout_event() {
                launcher.layout_dirty = true;
            }
            if launcher.api.take_image_event() {
                launcher.image_updated = true;
            }
            if launcher.layout_dirty {
                if let Err(err) = launcher.reload_providers() {
                    eprintln!("reload {}: {err}", spec.hostname);
                }
            }
            launcher.process_once();
        }));
        if let Err(err) = caught {
            eprintln!("launcher {} panicked: {err:?}", spec.hostname);
            break;
        }
        if !announced {
            ready.store(true, std::sync::atomic::Ordering::SeqCst);
            announced = true;
        }
        // tests/base.py sets Launcher.MAX_SLEEP = 0.1. The production
        // loop stays at MAX_SLEEP (1s); this suite matches the fixture.
        let elapsed = started.elapsed();
        let max = Duration::from_millis(100);
        if elapsed < max && !stop.load(std::sync::atomic::Ordering::SeqCst) {
            std::thread::sleep(max - elapsed);
        }
        if !stop.load(std::sync::atomic::Ordering::SeqCst) {
            launcher.wait_wake(Duration::from_secs(60));
        }
    }
}

fn run(class_name: &str, method: &str) {
    if !selected(class_name, method) {
        return;
    }
    let started = Instant::now();
    let log_path = PathBuf::from(format!(
        "/tmp/zuul-suite-logs/native-{class_name}.{method}.log"
    ));
    let mut fixture = Fixture::spawn(&log_path);
    let begin: Begin = serde_json::from_value(fixture.call(json!({
        "cmd": "begin",
        "class": class_name,
        "method": method,
        "mode": "native",
    })))
    .unwrap_or_else(|err| panic!("begin decode {err}"));
    if !begin.ok {
        record_time(class_name, method, started.elapsed().as_secs_f64());
        panic!(
            "setUp failed for {class_name}.{method}: {}",
            begin.error.unwrap_or_default()
        );
    }
    for (key, value) in &begin.env {
        if let Some(text) = value.as_str() {
            std::env::set_var(key, text);
        }
    }
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let wakes = Arc::new(Mutex::new(Vec::new()));
    let watch_stop = stop.clone();
    let watch_ready = ready.clone();
    let watch_wakes = wakes.clone();
    let launchers_path = begin.launchers_path.clone();
    let config_path = begin.config_path.clone();
    let supervisor = std::thread::spawn(move || {
        let mut seen = HashSet::new();
        let mut joins = Vec::new();
        while !watch_stop.load(std::sync::atomic::Ordering::SeqCst) {
            if let Ok(text) = fs::read_to_string(&launchers_path) {
                for line in text.lines() {
                    if line.is_empty() || !seen.insert(line.to_string()) {
                        continue;
                    }
                    let spec: LaunchRecord = match serde_json::from_str(line) {
                        Ok(spec) => spec,
                        Err(err) => {
                            eprintln!("launcher record: {err}");
                            continue;
                        }
                    };
                    let config_path = config_path.clone();
                    let stop = watch_stop.clone();
                    let ready = watch_ready.clone();
                    let wakes = watch_wakes.clone();
                    joins.push(std::thread::spawn(move || {
                        launcher_loop(spec, config_path, stop, ready, wakes);
                    }));
                }
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        for (flag, cv) in watch_wakes.lock().unwrap().iter() {
            if let Ok(mut guard) = flag.lock() {
                *guard = true;
            }
            cv.notify_all();
        }
        for handle in joins {
            let _ = handle.join();
        }
    });
    let wait_started = Instant::now();
    while !ready.load(std::sync::atomic::Ordering::SeqCst) {
        if wait_started.elapsed() > Duration::from_secs(60) {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let case = suite::find_case(class_name, method);
    let reply: ExecReply = serde_json::from_value(fixture.call(json!({
        "cmd": "exec",
        "source": case.body,
        "patches": case.patches,
    })))
    .unwrap_or_else(|err| panic!("exec decode {err}"));
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    for (flag, cv) in wakes.lock().unwrap().iter() {
        if let Ok(mut guard) = flag.lock() {
            *guard = true;
        }
        cv.notify_all();
    }
    let _ = supervisor.join();
    let ended: EndReply = serde_json::from_value(fixture.call(json!({"cmd": "end"})))
        .unwrap_or_else(|err| panic!("end decode {err}"));
    record_time(class_name, method, started.elapsed().as_secs_f64());
    suite::finish(&reply.checks, reply.error.as_deref());
    if let Some(err) = ended.error.as_deref() {
        if !err.is_empty() {
            panic!("cleanup failed for {class_name}.{method}:\n{err}");
        }
    }
}

include!("generated_native.rs");
