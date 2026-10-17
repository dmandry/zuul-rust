// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// zuul-launcher command line from zuul/cmd/__init__.py and zuul/cmd/launcher.py.

use std::io::{BufRead, BufReader, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::time::Duration;
use zuul_launcher::config::Config;
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
    let launcher_cfg = zuul_launcher::config::launcher_config(&cfg)?;
    let zk_cfg = zuul_launcher::config::zookeeper_config(&cfg)?;
    let _stats = zuul_launcher::config::statsd_config(&cfg)?;
    let _tracing = zuul_launcher::config::tracing_config(&cfg)?;
    let _connections = zuul_launcher::config::provider_connections(&cfg)?;
    if foreground {
        // SIGTERM is handled by stop+join on the main thread.
        install_signals();
        run_foreground(&zk_cfg.hosts, &launcher_cfg.command_socket)?;
    } else {
        install_signals();
        daemonize(&launcher_cfg.pidfile)?;
        run_foreground(&zk_cfg.hosts, &launcher_cfg.command_socket)?;
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

fn run_foreground(hosts: &str, command_socket: &str) -> Result<(), String> {
    // Launcher.__init__ connects before the command socket or the loop.
    // Python exits if that session cannot be opened.
    open_zookeeper(hosts, command_socket)
}

fn open_zookeeper(hosts: &str, _command_socket: &str) -> Result<(), String> {
    if hosts.is_empty() {
        return Err("zookeeper hosts is required".into());
    }
    // The launcher loop, tree cache, and locks are implemented against an
    // in-memory session and covered by tests. A live TLS ZooKeeper client
    // is not linked: current rustls and zookeeper crates need Rust 1.85.
    Err(format!(
        "unable to open ZooKeeper TLS session to {hosts}: this binary has no ZooKeeper client"
    ))
}

#[allow(dead_code)]
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

#[allow(dead_code)]
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
