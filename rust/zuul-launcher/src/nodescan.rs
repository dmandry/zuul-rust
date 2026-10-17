// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// NodescanRequest from zuul/launcher/server.py: non-blocking connect, SSH
// banner, key exchange, then one connection per client host-key type.
// The worker accepts at most 100 requests and polls with a 1s timeout.

use base64::Engine;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};
use x25519_dalek::{PublicKey, StaticSecret};

pub const MAX_REQUESTS: usize = 100;
pub const POLL_TIMEOUT: Duration = Duration::from_secs(1);
pub const CONNECT_ATTEMPT: Duration = Duration::from_secs(10);

pub const START: &str = "start";
pub const CONNECTING_INIT: &str = "connecting";
pub const NEGOTIATING_INIT: &str = "negotiating";
pub const CONNECTING_KEY: &str = "connecting key";
pub const NEGOTIATING_KEY: &str = "negotiating key";
pub const COMPLETE: &str = "complete";

/// Paramiko Transport preferred host-key types. The first successful
/// connection copies this list, then each type is scanned on its own.
pub fn preferred_key_types() -> Vec<String> {
    [
        "ssh-ed25519",
        "ecdsa-sha2-nistp256",
        "ecdsa-sha2-nistp384",
        "ecdsa-sha2-nistp521",
        "rsa-sha2-512",
        "rsa-sha2-256",
        "ssh-rsa",
    ]
    .into_iter()
    .map(|s| s.to_string())
    .collect()
}

pub struct NodescanRequest {
    pub uuid: String,
    pub ip: String,
    pub port: u16,
    pub host_key_checking: bool,
    pub gather_hostkeys: bool,
    pub timeout: Duration,
    pub state: String,
    pub complete: bool,
    pub keys: Vec<String>,
    pub error: Option<String>,
    pub init_connection_attempts: u32,
    pub key_connection_failures: u32,
    pub key_negotiation_failures: u32,
    start: Instant,
    connect_start: Option<Instant>,
    key_types: Vec<String>,
    key_index: i32,
    key_type: Option<String>,
    conn: Option<SshConn>,
}

impl NodescanRequest {
    pub fn new(uuid: &str, ip: &str, port: u16, host_key_checking: bool, connection_type: Option<&str>, boot_timeout: f64) -> Self {
        let gather = matches!(connection_type, Some("ssh") | Some("network_cli"));
        Self {
            uuid: uuid.into(),
            ip: ip.into(),
            port,
            host_key_checking,
            gather_hostkeys: gather,
            timeout: Duration::from_secs_f64(boot_timeout.max(0.0)),
            state: START.into(),
            complete: false,
            keys: Vec::new(),
            error: None,
            init_connection_attempts: 0,
            key_connection_failures: 0,
            key_negotiation_failures: 0,
            start: Instant::now(),
            connect_start: None,
            key_types: Vec::new(),
            key_index: -1,
            key_type: None,
            conn: None,
        }
    }

    pub fn advance(&mut self, socket_ready: bool) {
        if self.complete {
            return;
        }
        if self.state == START {
            if !self.host_key_checking {
                self.finish_ok();
                return;
            }
            self.init_connection_attempts += 1;
            if let Err(e) = self.connect(None) {
                self.fail(e);
                return;
            }
            self.state = CONNECTING_INIT.into();
        }
        if self.state == CONNECTING_INIT {
            if !socket_ready && !self.connected() {
                if self.timed_out() {
                    self.fail("Timeout connecting".into());
                    return;
                }
                if self.connect_expired() {
                    self.conn = None;
                    self.state = START.into();
                }
                return;
            }
            if let Err(e) = self.take_error() {
                self.conn = None;
                self.state = START.into();
                if self.timed_out() {
                    self.fail(e);
                }
                return;
            }
            if self.gather_hostkeys {
                if let Some(c) = self.conn.as_mut() {
                    c.begin();
                }
                self.state = NEGOTIATING_INIT.into();
            } else {
                self.finish_ok();
                return;
            }
        }
        if self.state == NEGOTIATING_INIT {
            match self.drive_ssh() {
                Drive::Pending => {
                    if self.timed_out() {
                        self.fail("Timeout negotiating".into());
                    }
                    return;
                }
                Drive::Failed => {
                    self.key_negotiation_failures += 1;
                    self.conn = None;
                    self.state = START.into();
                    if self.timed_out() {
                        self.fail("Negotiation failed".into());
                    }
                    return;
                }
                Drive::Key(_) => {
                    // First success only learns the client key-type list.
                    self.key_types = preferred_key_types();
                    self.key_index = -1;
                    self.next_key();
                }
            }
        }
        if self.state == CONNECTING_KEY {
            if !socket_ready && !self.connected() {
                if self.timed_out() {
                    self.fail("Timeout connecting".into());
                    return;
                }
                if self.connect_expired() {
                    self.key_connection_failures += 1;
                    let kt = self.key_type.clone();
                    let _ = self.connect(kt.as_deref());
                }
                return;
            }
            if self.take_error().is_err() {
                self.key_connection_failures += 1;
                let kt = self.key_type.clone();
                let _ = self.connect(kt.as_deref());
                return;
            }
            if let Some(c) = self.conn.as_mut() {
                c.begin();
            }
            self.state = NEGOTIATING_KEY.into();
        }
        if self.state == NEGOTIATING_KEY {
            match self.drive_ssh() {
                Drive::Pending => {
                    if self.timed_out() {
                        self.fail("Timeout negotiating key".into());
                    }
                    return;
                }
                Drive::Failed => {
                    self.key_negotiation_failures += 1;
                    self.next_key();
                }
                Drive::Key(k) => {
                    let want = self.key_type.clone();
                    if want.as_deref().map(|t| k.starts_with(t)).unwrap_or(true) {
                        self.keys.push(k);
                    }
                    self.next_key();
                }
            }
        }
        if self.state == COMPLETE && !self.complete {
            self.conn = None;
            self.complete = true;
        }
    }

    fn next_key(&mut self) {
        self.conn = None;
        self.key_index += 1;
        if self.key_index as usize >= self.key_types.len() {
            self.finish_ok();
            return;
        }
        let kt = self.key_types[self.key_index as usize].clone();
        self.key_type = Some(kt.clone());
        if self.connect(Some(&kt)).is_err() {
            self.key_connection_failures += 1;
        }
        self.state = CONNECTING_KEY.into();
    }

    fn connect(&mut self, key_type: Option<&str>) -> Result<(), String> {
        let addr: SocketAddr = format!("{}:{}", self.ip, self.port)
            .parse()
            .map_err(|e: std::net::AddrParseError| e.to_string())?;
        // Complete the TCP handshake, then the state machine still walks
        // CONNECTING -> SO_ERROR -> NEGOTIATING. A 10s cap matches the
        // per-attempt limit in NodescanRequest._connect.
        let stream = TcpStream::connect_timeout(&addr, CONNECT_ATTEMPT).map_err(|e| e.to_string())?;
        let _ = stream.set_nonblocking(true);
        let mut conn = SshConn::new(stream, key_type.map(|s| s.to_string()));
        conn.ready = true;
        self.conn = Some(conn);
        self.connect_start = Some(Instant::now());
        Ok(())
    }

    fn connected(&self) -> bool {
        self.conn.as_ref().map(|c| c.ready).unwrap_or(false)
    }

    fn take_error(&mut self) -> Result<(), String> {
        let Some(conn) = self.conn.as_mut() else {
            return Err("no socket".into());
        };
        if conn.ready {
            return Ok(());
        }
        // A successful write or a zero SO_ERROR means the handshake finished.
        let stream = &conn.stream;
        let mut err: libc::c_int = 0;
        let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        let fd = stream.as_raw_fd();
        let rc = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_ERROR,
                &mut err as *mut _ as *mut libc::c_void,
                &mut len,
            )
        };
        if rc == 0 && err == 0 {
            conn.ready = true;
            Ok(())
        } else if err == 0 {
            Ok(())
        } else {
            Err(format!("connect error {err}"))
        }
    }

    fn drive_ssh(&mut self) -> Drive {
        let Some(conn) = self.conn.as_mut() else {
            return Drive::Failed;
        };
        conn.poll()
    }

    fn connect_expired(&self) -> bool {
        self.connect_start
            .map(|t| t.elapsed() >= CONNECT_ATTEMPT)
            .unwrap_or(false)
    }

    fn timed_out(&self) -> bool {
        self.start.elapsed() > self.timeout
    }

    fn finish_ok(&mut self) {
        self.conn = None;
        self.state = COMPLETE.into();
        self.complete = true;
    }

    fn fail(&mut self, msg: String) {
        self.error = Some(msg);
        self.conn = None;
        self.state = COMPLETE.into();
        self.complete = true;
    }
}

enum Drive {
    Pending,
    Failed,
    Key(String),
}

struct SshConn {
    stream: TcpStream,
    ready: bool,
    started: bool,
    buf: Vec<u8>,
    stage: u8,
    key_type: Option<String>,
    secret: Option<StaticSecret>,
    saw_banner: bool,
    sent_banner: bool,
}

impl SshConn {
    fn new(stream: TcpStream, key_type: Option<String>) -> Self {
        Self {
            stream,
            ready: false,
            started: false,
            buf: Vec::new(),
            stage: 0,
            key_type,
            secret: None,
            saw_banner: false,
            sent_banner: false,
        }
    }

    fn begin(&mut self) {
        self.started = true;
        self.ready = true;
        let _ = self.stream.set_nonblocking(true);
    }

    fn poll(&mut self) -> Drive {
        if !self.started {
            return Drive::Pending;
        }
        let mut tmp = [0u8; 4096];
        let mut eof = false;
        loop {
            match self.stream.read(&mut tmp) {
                Ok(0) => {
                    eof = true;
                    break;
                }
                Ok(n) => self.buf.extend_from_slice(&tmp[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(_) => return Drive::Failed,
            }
        }
        if !self.sent_banner {
            if self.stream.write_all(b"SSH-2.0-ZuulLauncher_14.3.0\r\n").is_err() {
                return Drive::Pending;
            }
            self.sent_banner = true;
        }
        if !self.saw_banner {
            if let Some(nl) = self.buf.iter().position(|b| *b == b'\n') {
                let line = &self.buf[..=nl];
                if line.starts_with(b"SSH-") {
                    self.saw_banner = true;
                    self.buf.drain(..=nl);
                    self.stage = 1;
                } else {
                    self.buf.drain(..=nl);
                }
            } else if eof {
                return Drive::Failed;
            } else {
                return Drive::Pending;
            }
        }
        if self.stage == 1 {
            let algos = match &self.key_type {
                Some(t) => t.clone(),
                None => preferred_key_types().join(","),
            };
            let packet = kexinit(&algos);
            if self.stream.write_all(&packet).is_err() {
                return Drive::Pending;
            }
            self.stage = 2;
        }
        if self.stage == 2 {
            let Some(payload) = pop_packet(&mut self.buf) else {
                // The peer closed without KEXINIT (unsupported host-key type).
                if eof {
                    return Drive::Failed;
                }
                return Drive::Pending;
            };
            if payload.first().copied() != Some(20) {
                return Drive::Failed;
            }
            self.stage = 3;
            let secret = StaticSecret::random_from_rng(rand::rngs::OsRng);
            let public = PublicKey::from(&secret);
            self.secret = Some(secret);
            let mut ecdh = vec![30u8];
            ecdh.extend(ssh_string(public.as_bytes()));
            if self.stream.write_all(&encode_packet(&ecdh)).is_err() {
                return Drive::Pending;
            }
        }
        if self.stage == 3 {
            let Some(payload) = pop_packet(&mut self.buf) else {
                if eof {
                    return Drive::Failed;
                }
                return Drive::Pending;
            };
            if payload.first().copied() != Some(31) {
                return Drive::Failed;
            }
            let mut i = 1usize;
            let Some(host_key) = ssh_read_string(&payload, &mut i) else {
                return Drive::Failed;
            };
            // Finish the curve25519 exchange so the reply is actually consumed.
            if let (Some(secret), Some(qs)) = (&self.secret, ssh_read_string(&payload, &mut i)) {
                if qs.len() == 32 {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(qs);
                    let _shared = secret.diffie_hellman(&PublicKey::from(arr));
                }
            }
            self.stage = 4;
            if let Some(formatted) = format_host_key(host_key) {
                return Drive::Key(formatted);
            }
            return Drive::Failed;
        }
        if eof {
            return Drive::Failed;
        }
        Drive::Pending
    }
}

pub struct NodescanWorker {
    pub requests: Vec<NodescanRequest>,
}

impl NodescanWorker {
    pub fn new() -> Self {
        Self { requests: Vec::new() }
    }

    pub fn add(&mut self, req: NodescanRequest) -> bool {
        if self.requests.iter().filter(|r| !r.complete).count() >= MAX_REQUESTS {
            return false;
        }
        self.requests.push(req);
        true
    }

    pub fn len(&self) -> usize {
        self.requests.iter().filter(|r| !r.complete).count()
    }

    pub fn poll(&mut self) {
        for req in &mut self.requests {
            if req.complete {
                continue;
            }
            let ready = req.conn.as_ref().map(|c| c.ready).unwrap_or(false);
            req.advance(ready);
        }
    }

    pub fn get(&self, uuid: &str) -> Option<&NodescanRequest> {
        self.requests.iter().find(|r| r.uuid == uuid)
    }

    pub fn get_mut(&mut self, uuid: &str) -> Option<&mut NodescanRequest> {
        self.requests.iter_mut().find(|r| r.uuid == uuid)
    }

    pub fn remove(&mut self, uuid: &str) {
        self.requests.retain(|r| r.uuid != uuid);
    }
}

fn kexinit(host_keys: &str) -> Vec<u8> {
    let mut payload = vec![20u8];
    payload.extend_from_slice(&[0u8; 16]);
    for list in [
        "curve25519-sha256",
        host_keys,
        "aes128-ctr",
        "aes128-ctr",
        "hmac-sha2-256",
        "hmac-sha2-256",
        "none",
        "none",
        "",
        "",
    ] {
        payload.extend(ssh_string(list.as_bytes()));
    }
    payload.push(0); // first_kex_packet_follows
    payload.extend_from_slice(&0u32.to_be_bytes());
    encode_packet(&payload)
}

fn encode_packet(payload: &[u8]) -> Vec<u8> {
    let mut pad = 4usize;
    while (1 + payload.len() + pad) % 8 != 0 {
        pad += 1;
    }
    let packet_len = 1 + payload.len() + pad;
    let mut out = Vec::with_capacity(4 + packet_len);
    out.extend_from_slice(&(packet_len as u32).to_be_bytes());
    out.push(pad as u8);
    out.extend_from_slice(payload);
    out.extend(std::iter::repeat(0u8).take(pad));
    out
}

fn pop_packet(buf: &mut Vec<u8>) -> Option<Vec<u8>> {
    if buf.len() < 5 {
        return None;
    }
    let packet_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if buf.len() < 4 + packet_len {
        return None;
    }
    let pad = buf[4] as usize;
    if pad > packet_len {
        return None;
    }
    let payload_len = packet_len - 1 - pad;
    let payload = buf[5..5 + payload_len].to_vec();
    buf.drain(..4 + packet_len);
    Some(payload)
}

fn ssh_string(data: &[u8]) -> Vec<u8> {
    let mut out = (data.len() as u32).to_be_bytes().to_vec();
    out.extend_from_slice(data);
    out
}

fn ssh_read_string<'a>(buf: &'a [u8], i: &mut usize) -> Option<&'a [u8]> {
    if *i + 4 > buf.len() {
        return None;
    }
    let n = u32::from_be_bytes([buf[*i], buf[*i + 1], buf[*i + 2], buf[*i + 3]]) as usize;
    *i += 4;
    if *i + n > buf.len() {
        return None;
    }
    let s = &buf[*i..*i + n];
    *i += n;
    Some(s)
}

pub fn format_host_key(blob: &[u8]) -> Option<String> {
    let mut i = 0;
    let name = ssh_read_string(blob, &mut i)?;
    let name = std::str::from_utf8(name).ok()?;
    let b64 = base64::engine::general_purpose::STANDARD.encode(blob);
    Some(format!("{name} {b64}"))
}

/// Minimal SSH server used by the reuse integration test.
pub fn serve_ed25519(host_key: &[u8], listener: std::net::TcpListener) {
    for stream in listener.incoming() {
        let Ok(mut stream) = stream else { continue };
        let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
        let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));
        let _ = handle_client(&mut stream, host_key);
    }
}

fn handle_client(stream: &mut TcpStream, host_key: &[u8]) -> std::io::Result<()> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 2048];
    // Read client banner.
    loop {
        let n = stream.read(&mut tmp)?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(2).any(|w| w == b"\r\n") || buf.contains(&b'\n') {
            break;
        }
    }
    stream.write_all(b"SSH-2.0-ZuulTest\r\n")?;
    buf.clear();
    let payload = read_payload(stream, &mut buf)?;
    if payload.first() != Some(&20) {
        return Ok(());
    }
    // Client host-key list is the second name-list after the cookie.
    let mut i = 1 + 16;
    let _kex = ssh_read_string(&payload, &mut i);
    let host_algs = ssh_read_string(&payload, &mut i).unwrap_or(&[]);
    let alg_str = String::from_utf8_lossy(host_algs);
    if !alg_str.split(',').any(|a| a == "ssh-ed25519") {
        return Ok(());
    }
    stream.write_all(&kexinit("ssh-ed25519"))?;
    buf.clear();
    let init = read_payload(stream, &mut buf)?;
    if init.first() != Some(&30) {
        return Ok(());
    }
    let mut reply = vec![31u8];
    reply.extend(ssh_string(host_key));
    reply.extend(ssh_string(&[0u8; 32]));
    reply.extend(ssh_string(b"sig"));
    stream.write_all(&encode_packet(&reply))?;
    Ok(())
}

fn read_payload(stream: &mut TcpStream, buf: &mut Vec<u8>) -> std::io::Result<Vec<u8>> {
    let mut tmp = [0u8; 2048];
    loop {
        if let Some(p) = pop_packet(buf) {
            return Ok(p);
        }
        let n = stream.read(&mut tmp)?;
        if n == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof"));
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}
