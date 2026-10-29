// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// Native launcher suite. The Rust launcher runs in this process. Python is a
// separate fixture process (ZooKeeper chroot, gerrit, moto server) and is not
// linked into the launcher.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{json, Value};
use zuul_launcher::config::{provider_connections, statsd_config, zookeeper_config, Config};
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

fn pass_dir() -> PathBuf {
    let root = std::env::var("ZUUL_TEST_ROOT").unwrap_or_else(|_| "/tmp/zuul-test".into());
    let dir = PathBuf::from(root).join("native-pass");
    let _ = fs::create_dir_all(&dir);
    dir
}

/// One launcher iteration. The Python thread's run() holds `_run_lock`
/// and then asks for this, so a test that takes the lock pauses the
/// native launcher the way it pauses server.Launcher.
fn run_pass(launcher: &mut Launcher, hostname: &str) {
    let started_pass = Instant::now();
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
                eprintln!("reload {hostname}: {err}");
            }
        }
        launcher.process_once();
        zuul_launcher::statsd::timing(
            "zuul.launcher.run_handler",
            started_pass.elapsed().as_secs_f64() * 1000.0,
        );
    }));
    if let Err(err) = caught {
        eprintln!("launcher {hostname} panicked: {err:?}");
    }
}

fn mark_component_stopped(launcher: &Launcher) {
    let Some(path) = &launcher.component_path else {
        return;
    };
    let _ = zk::set_component(
        launcher.api.zk.as_ref(),
        path,
        &launcher.hostname,
        "stopped",
        &launcher.connection_filter,
    );
}

/// Test-hooks pass socket. The production binary does not link this.
/// One blocking read replaces the 5ms poll of `{hostname}.req`.
struct PassOut {
    stream: Mutex<Option<UnixStream>>,
}

fn send_pass(out: &PassOut, msg: &Value) {
    let mut guard = match out.stream.lock() {
        Ok(guard) => guard,
        Err(err) => err.into_inner(),
    };
    let Some(stream) = guard.as_mut() else {
        return;
    };
    let line = format!("{msg}\n");
    let _ = stream.write_all(line.as_bytes());
    let _ = stream.flush();
}

fn pass_readable(stream: &UnixStream, timeout_ms: i32) -> bool {
    let mut pollfd = libc::pollfd {
        fd: stream.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let rc = unsafe { libc::poll(&mut pollfd, 1, timeout_ms) };
    rc > 0
}

fn handle_pass_message(launcher: &mut Launcher, hostname: &str, out: &PassOut, msg: &Value) -> bool {
    let op = msg.get("op").and_then(|v| v.as_str()).unwrap_or("");
    let seq = msg.get("seq").and_then(|v| v.as_u64()).unwrap_or(0);
    match op {
        "pass" => {
            run_pass(launcher, hostname);
            let wake = launcher.take_wake();
            send_pass(out, &json!({ "op": "done", "seq": seq, "wake": wake }));
            false
        }
        "cleanup" => {
            launcher.run_cleanup_now();
            send_pass(out, &json!({ "op": "cleanup_done", "seq": seq }));
            false
        }
        "stats" => {
            launcher.run_stats_now();
            send_pass(out, &json!({ "op": "stats_done", "seq": seq }));
            false
        }
        "stop" => true,
        _ => false,
    }
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
    // Each test starts a new moto proxy. The endpoint URL lives in that
    // test's zuul.conf, so connections are replaced for every launcher.
    if let Ok(connections) = provider_connections(&cfg) {
        native::set_connections(connections);
    }
    if let Ok(stats) = statsd_config(&cfg) {
        zuul_launcher::statsd::install(&stats);
    }
    if let Ok(path) = std::env::var("ZUUL_HOOK_SOCK") {
        if !path.is_empty() {
            zuul_launcher::hooks::set_socket(path);
        }
    }
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
    zuul_launcher::hooks::bind_hostname(&spec.hostname);
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
    let pass_out = Arc::new(PassOut {
        stream: Mutex::new(None),
    });
    let poke_out = pass_out.clone();
    // Playback-thread poke. install_waker also sets the wake flag,
    // which the following pass reports in the done message.
    launcher.install_waker(Some(Arc::new(move || {
        send_pass(&poke_out, &json!({ "op": "poke" }));
    })));
    launcher.arm_watches();
    wakes
        .lock()
        .unwrap()
        .push((launcher.wake.clone(), launcher.wake_cv.clone()));
    let dir = pass_dir();
    let ready_path = dir.join(format!("{}.ready", spec.hostname));
    let stop_path = dir.join(format!("{}.stop", spec.hostname));
    let sock_path = dir.join(format!("{}.sock", spec.hostname));
    let _ = fs::remove_file(&sock_path);
    let listener = match UnixListener::bind(&sock_path) {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!("pass socket {}: {err}", spec.hostname);
            launcher.stop_workers();
            launcher.api.zk.close_session();
            ready.store(true, std::sync::atomic::Ordering::SeqCst);
            return;
        }
    };
    let _ = listener.set_nonblocking(true);
    // Register and load providers before the test body. The lock
    // protocol starts only after the ready file exists.
    run_pass(&mut launcher, &spec.hostname);
    let _ = fs::write(&ready_path, "1");
    ready.store(true, std::sync::atomic::Ordering::SeqCst);
    let mut handoff = false;
    let mut accepted = None;
    while !stop.load(std::sync::atomic::Ordering::SeqCst) {
        if stop_path.exists() {
            mark_component_stopped(&launcher);
            handoff = true;
            break;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                accepted = Some(stream);
                break;
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                // Startup only. The timed path blocks on the accepted
                // socket instead of polling a seq file.
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(err) => {
                eprintln!("pass accept {}: {err}", spec.hostname);
                break;
            }
        }
    }
    if let Some(stream) = accepted {
        let _ = stream.set_nonblocking(false);
        if let Ok(reader_stream) = stream.try_clone() {
            let _ = reader_stream.set_read_timeout(Some(Duration::from_secs(1)));
            if let Ok(mut slot) = pass_out.stream.lock() {
                *slot = Some(stream);
            }
            let mut reader = BufReader::new(reader_stream);
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                if stop_path.exists() {
                    mark_component_stopped(&launcher);
                    handoff = true;
                    break;
                }
                // poll waits up to 1s. A command already queued returns
                // immediately, so the pass itself is not on a timer.
                let buffered = reader.buffer().contains(&b'\n');
                if !buffered && !pass_readable(reader.get_ref(), 1000) {
                    launcher.api.claim_watches();
                    send_pass(&pass_out, &json!({ "op": "poke" }));
                    continue;
                }
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) => break,
                    Ok(_) => {
                        let msg: Value = match serde_json::from_str(line.trim()) {
                            Ok(msg) => msg,
                            Err(_) => continue,
                        };
                        if handle_pass_message(&mut launcher, &spec.hostname, &pass_out, &msg) {
                            mark_component_stopped(&launcher);
                            handoff = true;
                            break;
                        }
                    }
                    Err(err) => {
                        eprintln!("pass read {}: {err} kind={:?}", spec.hostname, err.kind());
                        break;
                    }
                }
            }
        }
    }
    mark_component_stopped(&launcher);
    // Drop the session before removing the stop file. createLauncher
    // waits for that file to disappear, then starts the replacement
    // thread. Ephemeral node locks die with the session, so the new
    // launcher can take the node.
    if let Ok(mut slot) = pass_out.stream.lock() {
        *slot = None;
    }
    drop(listener);
    let _ = fs::remove_file(&sock_path);
    launcher.stop_workers();
    // The watch waker holds Playback, and Playback holds this session.
    // Clearing it breaks the cycle so Drop closes the socket and
    // ZooKeeper deletes this launcher's ephemeral node locks.
    launcher.api.zk.set_waker(None);
    // CloseSession deletes ephemeral node locks and this component.
    // Drop alone does not run while the playback waker still holds an Arc.
    launcher.api.zk.close_session();
    drop(launcher);
    drop(ops);
    if handoff {
        // The replacement thread starts after this file disappears.
        // Give ZooKeeper a moment to drop the ephemeral locks first.
        std::thread::sleep(Duration::from_millis(200));
        let _ = fs::remove_file(&stop_path);
    }
}

struct EnvRestore(Vec<(String, Option<String>)>);

impl Drop for EnvRestore {
    fn drop(&mut self) {
        for (key, previous) in self.0.drain(..) {
            match previous {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

pub fn run(class_name: &str, method: &str) {
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
    let mut previous_env = Vec::new();
    for (key, value) in &begin.env {
        if let Some(text) = value.as_str() {
            previous_env.push((key.clone(), std::env::var(key).ok()));
            std::env::set_var(key, text);
        }
    }
    let _env_restore = EnvRestore(previous_env);
    // quota_external_usage records unmanaged instances on the shared
    // endpoint name. The next test, relative_priority, allows one
    // instance and would wait out requestNodes if that usage survived.
    zuul_launcher::native::reset_endpoint_runtime();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let wakes = Arc::new(Mutex::new(Vec::new()));
    let watch_stop = stop.clone();
    let watch_ready = ready.clone();
    let watch_wakes = wakes.clone();
    let launchers_path = begin.launchers_path.clone();
    let config_path = begin.config_path.clone();
    let supervisor = std::thread::spawn(move || {
        let _ = fs::write(pass_dir().join("supervisor"), "1");
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
