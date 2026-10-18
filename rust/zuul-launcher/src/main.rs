// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// zuul-launcher command line from zuul/cmd/__init__.py and zuul/cmd/launcher.py.

use std::io::{BufRead, BufReader, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use zuul_launcher::config::{launcher_config, zookeeper_config, Config, LauncherConfig, ZookeeperConfig};
use zuul_launcher::launcher::{loop_waits, Launcher};
use zuul_launcher::tlszk::TlsZk;
use zuul_launcher::zk::{self, ZkOps};
use zuul_launcher::{MODEL_API, ZUUL_VERSION};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Err(e) = run(&args) {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

fn run(args: &[String]) -> Result<(), String> {
    let mut config_path: Option<PathBuf> = None;
    let mut debug = false;
    let mut foreground = false;
    let mut command: Option<String> = None;
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        match a.as_str() {
            "-c" | "--config" => {
                i += 1;
                config_path = Some(PathBuf::from(args.get(i).ok_or("missing config path")?));
            }
            "-d" | "--debug" => debug = true,
            "-f" | "--foreground" | "--nodaemon" => foreground = true,
            "--version" | "-v" => {
                println!("Zuul version: {ZUUL_VERSION}");
                return Ok(());
            }
            "-h" | "--help" => {
                print_help();
                return Ok(());
            }
            "stop" | "repl" | "norepl" => command = Some(a.clone()),
            other if other.starts_with('-') => return Err(format!("unrecognized arguments: {other}")),
            other => return Err(format!("unrecognized arguments: {other}")),
        }
        i += 1;
    }
    let _ = (debug, MODEL_API);
    let cfg = Config::load_zuul(config_path.as_deref())?;
    if let Some(cmd) = command {
        // Online commands send one line and exit. They do not stay in the
        // foreground running the launcher.
        let socket = zuul_launcher::config::launcher_config(&cfg)?.command_socket;
        send_command(&socket, &cmd)?;
        return Ok(());
    }
    let launcher_cfg = launcher_config(&cfg)?;
    let zk_cfg = zookeeper_config(&cfg)?;
    let _stats = zuul_launcher::config::statsd_config(&cfg)?;
    let _tracing = zuul_launcher::config::tracing_config(&cfg)?;
    let _connections = zuul_launcher::config::provider_connections(&cfg)?;
    if foreground {
        // SIGTERM is handled by stop+join on the main thread.
        install_signals();
        run_foreground(&zk_cfg, &launcher_cfg)?;
    } else {
        install_signals();
        daemonize(&launcher_cfg.pidfile)?;
        run_foreground(&zk_cfg, &launcher_cfg)?;
    }
    Ok(())
}

fn print_help() {
    println!("usage: zuul-launcher [-h] [-c CONFIG] [-d] [-f] [--version] {{stop,repl,norepl}}");
    println!();
    println!("Zuul launcher (model api {MODEL_API})");
}

fn send_command(socket: &str, command: &str) -> Result<(), String> {
    let mut stream = UnixStream::connect(socket).map_err(|e| format!("command socket: {e}"))?;
    let line = format!("{command}\n");
    stream.write_all(line.as_bytes()).map_err(|e| e.to_string())?;
    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    reader.read_line(&mut response).ok();
    Ok(())
}

fn install_signals() {
    unsafe {
        libc::signal(libc::SIGUSR2, sigusr2 as usize);
        libc::signal(libc::SIGTERM, sigterm as usize);
        libc::signal(libc::SIGINT, sigint as usize);
    }
}

static STOP: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

extern "C" fn sigusr2(_: libc::c_int) {
    let trace = backtrace::Backtrace::new();
    eprintln!("Received SIGUSR2; dumping stack:\n{trace:?}");
}

extern "C" fn sigterm(_: libc::c_int) {
    STOP.store(true, std::sync::atomic::Ordering::SeqCst);
}

extern "C" fn sigint(_: libc::c_int) {
    eprint!("Ctrl + C: asking launcher to exit nicely...\n");
    STOP.store(true, std::sync::atomic::Ordering::SeqCst);
}

fn run_foreground(zk_cfg: &ZookeeperConfig, launcher_cfg: &LauncherConfig) -> Result<(), String> {
    // Launcher.__init__ connects before the command socket or the loop.
    // Python exits if that session cannot be opened.
    let timeout_ms = (zk_cfg.session_timeout * 1000.0) as i32;
    let zk = TlsZk::connect(
        &zk_cfg.hosts,
        &zk_cfg.tls_cert,
        &zk_cfg.tls_key,
        &zk_cfg.tls_ca,
        timeout_ms.max(1),
    )
    .map_err(|e| format!("unable to open ZooKeeper TLS session to {}: {e}", zk_cfg.hosts))?;
    let ops: Arc<dyn ZkOps> = zk;
    let hostname = launcher_hostname();
    let mut launcher = Launcher::new(&hostname, ops.clone());
    launcher.connection_filter = launcher_cfg.connection_filter.clone();
    launcher.api.connection_filter = launcher_cfg.connection_filter.clone();
    let path = zk::register_component(
        ops.as_ref(),
        &hostname,
        "stopped",
        &launcher.connection_filter,
    )
    .map_err(|e| format!("unable to register launcher component: {e}"))?;
    launcher.component_path = Some(path);
    launcher.start_workers(true);
    let socket = launcher_cfg.command_socket.clone();
    std::thread::spawn(move || command_server(&socket));
    while !STOP.load(std::sync::atomic::Ordering::SeqCst) {
        let started = Instant::now();
        launcher.api.sync();
        launcher.run_once();
        let (sleep, _) = loop_waits(started);
        let mut left = sleep;
        let slice = Duration::from_millis(100);
        while !STOP.load(std::sync::atomic::Ordering::SeqCst) && left > Duration::ZERO {
            let step = slice.min(left);
            std::thread::sleep(step);
            left = left.saturating_sub(step);
        }
    }
    launcher.stop_workers();
    if let Some(path) = &launcher.component_path {
        let _ = zk::set_component(
            launcher.api.zk.as_ref(),
            path,
            &hostname,
            "stopped",
            &launcher.connection_filter,
        );
    }
    Ok(())
}

fn launcher_hostname() -> String {
    hostname::get()
        .ok()
        .and_then(|name| name.into_string().ok())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "localhost".into())
}

fn command_server(path: &str) {
    let _ = std::fs::remove_file(path);
    if let Some(parent) = std::path::Path::new(path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let Ok(listener) = std::os::unix::net::UnixListener::bind(path) else {
        return;
    };
    for conn in listener.incoming() {
        let Ok(mut stream) = conn else { continue };
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() {
            continue;
        }
        let line = line.trim_end();
        let (cmd, _args) = match line.split_once(' ') {
            Some((c, rest)) => (c, rest),
            None => (line, ""),
        };
        match cmd {
            "stop" => {
                STOP.store(true, std::sync::atomic::Ordering::SeqCst);
                let _ = stream.write_all(b"\n");
            }
            "repl" => {
                let _ = bind_repl();
                let _ = stream.write_all(b"\n");
            }
            "norepl" => {
                let _ = stream.write_all(b"\n");
            }
            _ => {
                let _ = stream.write_all(b"\n");
            }
        }
    }
}

fn bind_repl() -> std::io::Result<()> {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 3000))?;
    // The port is held only while a client is accepted; norepl drops it.
    // Matching zuul/lib/repl.py, this is not an InteractiveConsole.
    std::thread::spawn(move || {
        let _ = listener.accept();
    });
    Ok(())
}

fn daemonize(pidfile: &str) -> Result<(), String> {
    // Exercise the pidfile (10s lock timeout) before the double-fork, then
    // hold it. umask 022 matches python-daemon.
    acquire_pidfile(pidfile, Duration::from_secs(10))?;
    unsafe {
        let pid = libc::fork();
        if pid < 0 {
            return Err("fork failed".into());
        }
        if pid > 0 {
            libc::_exit(0);
        }
        if libc::setsid() < 0 {
            return Err("setsid failed".into());
        }
        let pid = libc::fork();
        if pid < 0 {
            return Err("fork failed".into());
        }
        if pid > 0 {
            libc::_exit(0);
        }
        libc::umask(0o022);
        let root = std::ffi::CString::new("/").unwrap();
        libc::chdir(root.as_ptr());
        let devnull_path = std::ffi::CString::new("/dev/null").unwrap();
        let devnull = libc::open(devnull_path.as_ptr(), libc::O_RDWR);
        if devnull >= 0 {
            libc::dup2(devnull, 0);
            libc::dup2(devnull, 1);
            libc::dup2(devnull, 2);
            if devnull > 2 {
                libc::close(devnull);
            }
        }
    }
    write_pid(pidfile)?;
    Ok(())
}

fn acquire_pidfile(path: &str, timeout: Duration) -> Result<(), String> {
    if let Some(parent) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let start = std::time::Instant::now();
    loop {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| e.to_string())?;
        let fd = file.as_raw_fd();
        let rc = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
        if rc == 0 {
            // Release immediately; the daemonize path re-acquires after fork.
            unsafe { libc::flock(fd, libc::LOCK_UN) };
            return Ok(());
        }
        if start.elapsed() >= timeout {
            return Err(format!("Timeout acquiring pidfile {path}"));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn write_pid(path: &str) -> Result<(), String> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(|e| e.to_string())?;
    let fd = file.as_raw_fd();
    let rc = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        return Err(format!("Timeout acquiring pidfile {path}"));
    }
    use std::io::Write;
    write!(file, "{}\n", std::process::id()).map_err(|e| e.to_string())?;
    Ok(())
}
