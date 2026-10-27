// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// ZooKeeper TLS client that speaks the same length-prefixed Jute framing
// Kazoo 2.11 uses. The shipped binary and the PyO3 test harness share it.

use openssl::ssl::{SslConnector, SslFiletype, SslMethod};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::Duration;
use crate::zk::{CreateMode, ZkError, ZkOps, ZkStat};

const OP_CREATE: i32 = 1;
const OP_DELETE: i32 = 2;
const OP_EXISTS: i32 = 3;
const OP_GET_DATA: i32 = 4;
const OP_SET_DATA: i32 = 5;
const OP_GET_CHILDREN: i32 = 8;
const OP_PING: i32 = 11;
/// Read-only multi. Opcode 14 is a transaction and rejects getData.
const OP_MULTI_READ: i32 = 22;
const OP_CLOSE: i32 = -11;

const ERR_NONODE: i32 = -101;
const ERR_NODEEXISTS: i32 = -110;
const ERR_NOTEMPTY: i32 = -111;
const ERR_BADVERSION: i32 = -103;

const XID_PING: i32 = -2;

struct Conn {
    stream: SslStreamGuard,
    xid: i32,
    chroot: String,
}

struct SslStreamGuard {
    stream: openssl::ssl::SslStream<TcpStream>,
}

pub struct TlsZk {
    inner: Mutex<Conn>,
    session_id: AtomicI64,
    closed: AtomicBool,
}

impl TlsZk {
    /// `hosts` is `host:port` or `host:port/chroot`, matching Zuul's config.
    pub fn connect(
        hosts: &str,
        cert: &str,
        key: &str,
        ca: &str,
        timeout_ms: i32,
    ) -> Result<Arc<Self>, ZkError> {
        let (host, port, chroot) = parse_hosts(hosts)?;
        let tcp = TcpStream::connect((host.as_str(), port)).map_err(|e| {
            ZkError::Message(format!("connect {host}:{port}: {e}"))
        })?;
        tcp.set_nodelay(true).ok();
        let handshake_timeout = Duration::from_secs(30);
        tcp.set_read_timeout(Some(handshake_timeout)).ok();
        tcp.set_write_timeout(Some(handshake_timeout)).ok();

        let mut builder = SslConnector::builder(SslMethod::tls())
            .map_err(|e| ZkError::Message(format!("ssl: {e}")))?;
        builder
            .set_certificate_file(cert, SslFiletype::PEM)
            .map_err(|e| ZkError::Message(format!("tls cert: {e}")))?;
        builder
            .set_private_key_file(key, SslFiletype::PEM)
            .map_err(|e| ZkError::Message(format!("tls key: {e}")))?;
        builder
            .set_ca_file(ca)
            .map_err(|e| ZkError::Message(format!("tls ca: {e}")))?;
        let connector = builder.build();
        let mut stream = connector.connect(&host, tcp).map_err(|e| {
            ZkError::Message(format!("tls handshake {host}:{port}: {e}"))
        })?;

        let (session_id, negotiated_ms) =
            handshake(&mut stream, timeout_ms.max(1))?;
        let read_timeout = Duration::from_millis((negotiated_ms as u64 / 3).max(1000));
        stream.get_mut().set_read_timeout(Some(read_timeout)).ok();
        stream.get_mut().set_write_timeout(Some(read_timeout)).ok();

        let zk = Arc::new(Self {
            inner: Mutex::new(Conn {
                stream: SslStreamGuard { stream },
                xid: 1,
                chroot,
            }),
            session_id: AtomicI64::new(session_id),
            closed: AtomicBool::new(false),
        });
        spawn_ping(Arc::downgrade(&zk), read_timeout);
        Ok(zk)
    }

    fn ping(&self) -> Result<(), ZkError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(ZkError::Message("session closed".into()));
        }
        let mut conn = self.inner.lock().unwrap();
        let mut body = Vec::new();
        write_i32(&mut body, XID_PING);
        write_i32(&mut body, OP_PING);
        write_frame(&mut conn.stream.stream, &body)?;
        let reply = read_frame(&mut conn.stream.stream)?;
        let (xid, _zxid, err, _rest) = split_reply(&reply)?;
        if xid != XID_PING {
            return Err(ZkError::Message(format!("ping xid {xid}")));
        }
        map_err(err)?;
        Ok(())
    }

    fn full(&self, conn: &Conn, path: &str) -> String {
        qualify(&conn.chroot, path)
    }
}

impl Drop for TlsZk {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Relaxed);
        if let Ok(mut conn) = self.inner.lock() {
            let mut body = Vec::new();
            write_i32(&mut body, conn.xid);
            write_i32(&mut body, OP_CLOSE);
            let _ = write_frame(&mut conn.stream.stream, &body);
            let _ = conn.stream.stream.shutdown();
        }
    }
}

fn spawn_ping(zk: Weak<TlsZk>, interval: Duration) {
    thread::spawn(move || {
        while let Some(zk) = zk.upgrade() {
            if zk.closed.load(Ordering::Relaxed) {
                break;
            }
            thread::sleep(interval);
            if zk.ping().is_err() {
                break;
            }
        }
    });
}

impl ZkOps for TlsZk {
    fn session_id(&self) -> i64 {
        self.session_id.load(Ordering::Relaxed)
    }

    fn ensure_path(&self, path: &str) -> Result<(), ZkError> {
        let mut acc = String::new();
        for part in path.split('/').filter(|p| !p.is_empty()) {
            acc.push('/');
            acc.push_str(part);
            match self.create(&acc, b"", CreateMode::Persistent) {
                Ok(_) | Err(ZkError::NodeExists) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn create(&self, path: &str, data: &[u8], mode: CreateMode) -> Result<String, ZkError> {
        let mut conn = self.inner.lock().unwrap();
        let full = self.full(&conn, path);
        let flags = match mode {
            CreateMode::Persistent => 0,
            CreateMode::Ephemeral => 1,
            CreateMode::PersistentSequential => 2,
            CreateMode::EphemeralSequential => 3,
        };
        let mut payload = Vec::new();
        write_string(&mut payload, &full);
        write_buffer(&mut payload, data);
        write_i32(&mut payload, 1); // one ACL
        write_i32(&mut payload, 0x1f); // all perms
        write_string(&mut payload, "world");
        write_string(&mut payload, "anyone");
        write_i32(&mut payload, flags);
        let reply = invoke(&mut conn, OP_CREATE, &payload)?;
        let (created, _) = read_string(&reply, 0)?;
        let created = created.unwrap_or_default();
        Ok(strip_chroot(&conn.chroot, &created))
    }

    fn delete(&self, path: &str, recursive: bool) -> Result<(), ZkError> {
        match self.delete_one(path) {
            Err(ZkError::NotEmpty) if recursive => {
                let kids = self.get_children(path)?;
                for kid in kids {
                    let child = if path == "/" {
                        format!("/{kid}")
                    } else {
                        format!("{path}/{kid}")
                    };
                    self.delete(&child, true)?;
                }
                self.delete_one(path)
            }
            other => other,
        }
    }

    fn exists(&self, path: &str) -> Result<Option<ZkStat>, ZkError> {
        let mut conn = self.inner.lock().unwrap();
        let full = self.full(&conn, path);
        let mut payload = Vec::new();
        write_string(&mut payload, &full);
        payload.push(0); // no watch
        match invoke(&mut conn, OP_EXISTS, &payload) {
            Err(ZkError::NoNode) => Ok(None),
            Err(e) => Err(e),
            Ok(reply) => Ok(Some(parse_stat(&reply, 0)?.0)),
        }
    }

    fn get(&self, path: &str) -> Result<(Vec<u8>, ZkStat), ZkError> {
        let mut conn = self.inner.lock().unwrap();
        let full = self.full(&conn, path);
        let mut payload = Vec::new();
        write_string(&mut payload, &full);
        payload.push(0);
        let reply = invoke(&mut conn, OP_GET_DATA, &payload)?;
        let (data, offset) = read_buffer(&reply, 0)?;
        let (stat, _) = parse_stat(&reply, offset)?;
        Ok((data.unwrap_or_default(), stat))
    }

    fn set(&self, path: &str, data: &[u8], version: i32) -> Result<ZkStat, ZkError> {
        let mut conn = self.inner.lock().unwrap();
        let full = self.full(&conn, path);
        let mut payload = Vec::new();
        write_string(&mut payload, &full);
        write_buffer(&mut payload, data);
        write_i32(&mut payload, version);
        let reply = invoke(&mut conn, OP_SET_DATA, &payload)?;
        Ok(parse_stat(&reply, 0)?.0)
    }

    fn get_children(&self, path: &str) -> Result<Vec<String>, ZkError> {
        let mut conn = self.inner.lock().unwrap();
        let full = self.full(&conn, path);
        let mut payload = Vec::new();
        write_string(&mut payload, &full);
        payload.push(0);
        let reply = invoke(&mut conn, OP_GET_CHILDREN, &payload)?;
        let mut offset = 0;
        let count = read_i32(&reply, &mut offset)?;
        if count < 0 {
            return Ok(Vec::new());
        }
        let mut kids = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let (child, next) = read_string(&reply, offset)?;
            offset = next;
            if let Some(child) = child {
                kids.push(child);
            }
        }
        kids.sort();
        Ok(kids)
    }

    fn multi_get(&self, paths: &[String]) -> Result<Vec<Result<(Vec<u8>, ZkStat), ZkError>>, ZkError> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }
        self.multi(paths, OP_GET_DATA, true)
    }

    fn multi_children(&self, paths: &[String]) -> Result<Vec<Result<Vec<String>, ZkError>>, ZkError> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }
        let raw = self.multi_raw(paths, OP_GET_CHILDREN)?;
        Ok(raw
            .into_iter()
            .map(|item| match item {
                Ok(body) => Ok(parse_children(&body)),
                Err(err) => Err(err),
            })
            .collect())
    }
}

impl TlsZk {
    fn multi(
        &self,
        paths: &[String],
        opcode: i32,
        data: bool,
    ) -> Result<Vec<Result<(Vec<u8>, ZkStat), ZkError>>, ZkError> {
        let raw = self.multi_raw(paths, opcode)?;
        let mut out = Vec::with_capacity(raw.len());
        for item in raw {
            match item {
                Err(err) => out.push(Err(err)),
                Ok(body) if data => {
                    let (bytes, offset) = read_buffer(&body, 0).map_err(|e| ZkError::Message(e.to_string()))?;
                    let (stat, _) = parse_stat(&body, offset)?;
                    out.push(Ok((bytes.unwrap_or_default(), stat)));
                }
                Ok(_) => out.push(Err(ZkError::Message("unexpected multi payload".into()))),
            }
        }
        Ok(out)
    }

    fn multi_raw(&self, paths: &[String], opcode: i32) -> Result<Vec<Result<Vec<u8>, ZkError>>, ZkError> {
        let mut conn = self.inner.lock().unwrap();
        let mut payload = Vec::new();
        for path in paths {
            let full = self.full(&conn, path);
            write_i32(&mut payload, opcode);
            payload.push(0); // done = false
            write_i32(&mut payload, -1);
            write_string(&mut payload, &full);
            payload.push(0); // watch = false
        }
        write_i32(&mut payload, -1);
        payload.push(1); // done = true
        write_i32(&mut payload, -1);
        let reply = invoke(&mut conn, OP_MULTI_READ, &payload)?;
        let mut offset = 0;
        let mut out = Vec::with_capacity(paths.len());
        for _ in 0..paths.len() {
            if offset + 9 > reply.len() {
                return Err(ZkError::Message("short multi reply".into()));
            }
            let _typ = read_i32(&reply, &mut offset)?;
            let done = reply[offset];
            offset += 1;
            let err = read_i32(&reply, &mut offset)?;
            if done != 0 {
                return Err(ZkError::Message(format!(
                    "multi ended early at {offset} done={done} err={err} bytes=",
                    reply.iter().take(48).map(|b| b.to_string()).collect::<Vec<_>>().join(",")
                )));
            }
            if err != 0 {
                // A failed sub-op is followed by an ErrorResponse int.
                let _ = read_i32(&reply, &mut offset)?;
                out.push(Err(code_err(err)));
                continue;
            }
            if opcode == OP_GET_DATA {
                let start = offset;
                let (_bytes, next) = read_buffer(&reply, offset).map_err(|e| ZkError::Message(e.to_string()))?;
                offset = next;
                let (_stat, next) = parse_stat(&reply, offset)?;
                offset = next;
                out.push(Ok(reply[start..offset].to_vec()));
            } else if opcode == OP_GET_CHILDREN {
                let start = offset;
                let count = read_i32(&reply, &mut offset)?;
                if count > 0 {
                    for _ in 0..count {
                        let (_child, next) = read_string(&reply, offset).map_err(|e| ZkError::Message(e.to_string()))?;
                        offset = next;
                    }
                }
                out.push(Ok(reply[start..offset].to_vec()));
            } else {
                return Err(ZkError::Message("unsupported multi opcode".into()));
            }
        }
        Ok(out)
    }
}

impl TlsZk {
    fn delete_one(&self, path: &str) -> Result<(), ZkError> {
        let mut conn = self.inner.lock().unwrap();
        let full = self.full(&conn, path);
        let mut payload = Vec::new();
        write_string(&mut payload, &full);
        write_i32(&mut payload, -1); // any version
        invoke(&mut conn, OP_DELETE, &payload)?;
        Ok(())
    }
}

fn invoke(conn: &mut Conn, opcode: i32, payload: &[u8]) -> Result<Vec<u8>, ZkError> {
    let xid = conn.xid;
    conn.xid = conn.xid.wrapping_add(1);
    if conn.xid < 1 {
        conn.xid = 1;
    }
    let mut body = Vec::with_capacity(8 + payload.len());
    write_i32(&mut body, xid);
    write_i32(&mut body, opcode);
    body.extend_from_slice(payload);
    write_frame(&mut conn.stream.stream, &body)?;
    let reply = read_frame(&mut conn.stream.stream)?;
    let (got_xid, _zxid, err, rest) = split_reply(&reply)?;
    if got_xid != xid {
        return Err(ZkError::Message(format!(
            "xid mismatch expected {xid} got {got_xid}"
        )));
    }
    map_err(err)?;
    Ok(rest)
}

fn handshake(
    stream: &mut openssl::ssl::SslStream<TcpStream>,
    timeout_ms: i32,
) -> Result<(i64, i32), ZkError> {
    // Kazoo Connect.serialize: !iqiq + password buffer + read_only byte.
    // protocol, last_zxid, timeout_ms, session_id, 16 zero bytes, not read-only.
    let mut payload = Vec::new();
    write_i32(&mut payload, 0);
    write_i64(&mut payload, 0);
    write_i32(&mut payload, timeout_ms);
    write_i64(&mut payload, 0);
    write_buffer(&mut payload, &[0u8; 16]);
    payload.push(0);
    write_frame(stream, &payload)?;
    let reply = read_frame(stream)?;
    // protocol i32, timeout i32, session i64, password buffer, optional bool.
    if reply.len() < 16 {
        return Err(ZkError::Message(format!(
            "short connect reply ({} bytes)",
            reply.len()
        )));
    }
    let mut offset = 0;
    let _proto = read_i32(&reply, &mut offset)?;
    let negotiated = read_i32(&reply, &mut offset)?;
    let session = read_i64(&reply, &mut offset)?;
    if negotiated <= 0 {
        return Err(ZkError::Message("ZooKeeper session expired".into()));
    }
    Ok((session, negotiated))
}

fn parse_hosts(hosts: &str) -> Result<(String, u16, String), ZkError> {
    let first = hosts.split(',').next().unwrap_or(hosts).trim();
    if first.is_empty() {
        return Err(ZkError::Message("empty zookeeper hosts".into()));
    }
    let (hostport, chroot) = match first.find('/') {
        Some(i) => (&first[..i], first[i..].trim_end_matches('/').to_string()),
        None => (first, String::new()),
    };
    let (host, port_str) = match hostport.rfind(':') {
        Some(i) => (&hostport[..i], &hostport[i + 1..]),
        None => {
            return Err(ZkError::Message(format!(
                "zookeeper host missing port: {hostport}"
            )))
        }
    };
    let port: u16 = port_str.parse().map_err(|_| {
        ZkError::Message(format!("bad zookeeper port: {port_str}"))
    })?;
    let host = host.trim_matches(['[', ']']).to_string();
    Ok((host, port, chroot))
}

fn qualify(chroot: &str, path: &str) -> String {
    if chroot.is_empty() || chroot == "/" {
        return path.to_string();
    }
    if path.is_empty() || path == "/" {
        return chroot.to_string();
    }
    format!("{chroot}{path}")
}

fn strip_chroot(chroot: &str, path: &str) -> String {
    if chroot.is_empty() || chroot == "/" {
        return path.to_string();
    }
    path.strip_prefix(chroot)
        .filter(|rest| rest.is_empty() || rest.starts_with('/'))
        .map(|rest| {
            if rest.is_empty() {
                "/".to_string()
            } else {
                rest.to_string()
            }
        })
        .unwrap_or_else(|| path.to_string())
}

fn map_err(err: i32) -> Result<(), ZkError> {
    if err == 0 {
        Ok(())
    } else {
        Err(code_err(err))
    }
}

fn code_err(err: i32) -> ZkError {
    match err {
        ERR_NONODE => ZkError::NoNode,
        ERR_NODEEXISTS => ZkError::NodeExists,
        ERR_NOTEMPTY => ZkError::NotEmpty,
        ERR_BADVERSION => ZkError::BadVersion,
        other => ZkError::Message(format!("zookeeper error {other}")),
    }
}

fn parse_children(body: &[u8]) -> Vec<String> {
    let mut offset = 0;
    let Ok(count) = read_i32(body, &mut offset) else {
        return Vec::new();
    };
    if count <= 0 {
        return Vec::new();
    }
    let mut kids = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let Ok((child, next)) = read_string(body, offset) else {
            break;
        };
        offset = next;
        if let Some(child) = child {
            kids.push(child);
        }
    }
    kids.sort();
    kids
}

fn write_frame(stream: &mut openssl::ssl::SslStream<TcpStream>, body: &[u8]) -> Result<(), ZkError> {
    let mut frame = Vec::with_capacity(4 + body.len());
    write_i32(&mut frame, body.len() as i32);
    frame.extend_from_slice(body);
    stream
        .write_all(&frame)
        .map_err(|e| ZkError::Message(format!("zk write: {e}")))?;
    stream
        .flush()
        .map_err(|e| ZkError::Message(format!("zk flush: {e}")))?;
    Ok(())
}

fn read_frame(stream: &mut openssl::ssl::SslStream<TcpStream>) -> Result<Vec<u8>, ZkError> {
    let mut len_buf = [0u8; 4];
    read_exact(stream, &mut len_buf)?;
    let len = i32::from_be_bytes(len_buf);
    if len < 0 || len > 64 * 1024 * 1024 {
        return Err(ZkError::Message(format!("bad frame length {len}")));
    }
    let mut body = vec![0u8; len as usize];
    read_exact(stream, &mut body)?;
    Ok(body)
}

fn read_exact(
    stream: &mut openssl::ssl::SslStream<TcpStream>,
    buf: &mut [u8],
) -> Result<(), ZkError> {
    stream
        .read_exact(buf)
        .map_err(|e| ZkError::Message(format!("zk read: {e}")))
}

fn split_reply(reply: &[u8]) -> Result<(i32, i64, i32, Vec<u8>), ZkError> {
    // Reply header !iqi : xid, zxid, err. 16 bytes.
    if reply.len() < 16 {
        return Err(ZkError::Message(format!(
            "short reply header ({} bytes)",
            reply.len()
        )));
    }
    let mut offset = 0;
    let xid = read_i32(reply, &mut offset)?;
    let zxid = read_i64(reply, &mut offset)?;
    let err = read_i32(reply, &mut offset)?;
    Ok((xid, zxid, err, reply[offset..].to_vec()))
}

fn parse_stat(buf: &[u8], offset: usize) -> Result<(ZkStat, usize), ZkError> {
    // !qqqqiiiqiiq
    if buf.len() < offset + 68 {
        return Err(ZkError::Message(format!(
            "short stat ({} bytes from {offset})",
            buf.len()
        )));
    }
    let mut off = offset;
    let czxid = read_i64(buf, &mut off)?;
    let mzxid = read_i64(buf, &mut off)?;
    let ctime = read_i64(buf, &mut off)?;
    let _mtime = read_i64(buf, &mut off)?;
    let version = read_i32(buf, &mut off)?;
    let _cversion = read_i32(buf, &mut off)?;
    let _aversion = read_i32(buf, &mut off)?;
    let ephemeral_owner = read_i64(buf, &mut off)?;
    let _data_length = read_i32(buf, &mut off)?;
    let children = read_i32(buf, &mut off)?;
    let _pzxid = read_i64(buf, &mut off)?;
    Ok((
        ZkStat {
            version,
            czxid,
            mzxid,
            ephemeral_owner,
            created: ctime as f64 / 1000.0,
            children_count: children,
        },
        off,
    ))
}

fn read_i32(buf: &[u8], offset: &mut usize) -> Result<i32, ZkError> {
    if buf.len() < *offset + 4 {
        return Err(ZkError::Message("truncated i32".into()));
    }
    let v = i32::from_be_bytes(buf[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    Ok(v)
}

fn read_i64(buf: &[u8], offset: &mut usize) -> Result<i64, ZkError> {
    if buf.len() < *offset + 8 {
        return Err(ZkError::Message("truncated i64".into()));
    }
    let v = i64::from_be_bytes(buf[*offset..*offset + 8].try_into().unwrap());
    *offset += 8;
    Ok(v)
}

fn read_string(buf: &[u8], offset: usize) -> Result<(Option<String>, usize), ZkError> {
    let mut off = offset;
    let len = read_i32(buf, &mut off)?;
    if len < 0 {
        return Ok((None, off));
    }
    let end = off + len as usize;
    if buf.len() < end {
        return Err(ZkError::Message("truncated string".into()));
    }
    let s = String::from_utf8_lossy(&buf[off..end]).into_owned();
    Ok((Some(s), end))
}

fn read_buffer(buf: &[u8], offset: usize) -> Result<(Option<Vec<u8>>, usize), ZkError> {
    let mut off = offset;
    let len = read_i32(buf, &mut off)?;
    if len < 0 {
        return Ok((None, off));
    }
    let end = off + len as usize;
    if buf.len() < end {
        return Err(ZkError::Message("truncated buffer".into()));
    }
    Ok((Some(buf[off..end].to_vec()), end))
}

fn write_i32(buf: &mut Vec<u8>, v: i32) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn write_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_be_bytes());
}

fn write_string(buf: &mut Vec<u8>, s: &str) {
    if s.is_empty() {
        write_i32(buf, -1);
        return;
    }
    let bytes = s.as_bytes();
    write_i32(buf, bytes.len() as i32);
    buf.extend_from_slice(bytes);
}

fn write_buffer(buf: &mut Vec<u8>, data: &[u8]) {
    write_i32(buf, data.len() as i32);
    buf.extend_from_slice(data);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_hosts_with_chroot() {
        let (host, port, chroot) = parse_hosts("localhost:2281/test/example").unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(port, 2281);
        assert_eq!(chroot, "/test/example");
        assert_eq!(qualify(&chroot, "/zuul"), "/test/example/zuul");
        assert_eq!(
            strip_chroot(&chroot, "/test/example/zuul/nodes"),
            "/zuul/nodes"
        );
    }

    #[test]
    #[ignore = "needs the test ZooKeeper on localhost:2281 with tools/ca certs"]
    fn tls_roundtrip() {
        let zk = TlsZk::connect(
            "localhost:2281",
            "/workspace/tools/ca/certs/client.pem",
            "/workspace/tools/ca/keys/clientkey.pem",
            "/workspace/tools/ca/certs/cacert.pem",
            60_000,
        )
        .unwrap();
        let _ = zk.delete("/zuul-harness-smoke", true);
        zk.ensure_path("/zuul-harness-smoke").unwrap();
        let created = zk
            .create(
                "/zuul-harness-smoke/node",
                b"hi",
                CreateMode::Persistent,
            )
            .unwrap();
        assert_eq!(created, "/zuul-harness-smoke/node");
        let (data, stat) = zk.get(&created).unwrap();
        assert_eq!(data, b"hi");
        zk.set(&created, b"yo", stat.version).unwrap();
        let (data, _) = zk.get(&created).unwrap();
        assert_eq!(data, b"yo");
        assert!(zk.exists(&created).unwrap().is_some());
        let missing = "/zuul-harness-smoke/absent".to_string();
        let got = zk
            .multi_get(&[created.clone(), missing.clone()])
            .unwrap();
        assert_eq!(got[0].as_ref().unwrap().0, b"yo");
        assert!(matches!(got[1], Err(ZkError::NoNode)));
        let only_missing = zk
            .multi_get(&[missing.clone(), "/zuul-harness-smoke/also-absent".into()])
            .unwrap();
        assert!(matches!(only_missing[0], Err(ZkError::NoNode)));
        assert!(matches!(only_missing[1], Err(ZkError::NoNode)));
        let mixed = zk
            .multi_get(&[
                missing.clone(),
                created.clone(),
                "/zuul-harness-smoke/also-absent".into(),
            ])
            .unwrap();
        assert!(matches!(mixed[0], Err(ZkError::NoNode)));
        assert_eq!(mixed[1].as_ref().unwrap().0, b"yo");
        assert!(matches!(mixed[2], Err(ZkError::NoNode)));
        zk.create("/zuul-harness-smoke/a", b"", CreateMode::Persistent)
            .unwrap();
        let kids = zk
            .multi_children(&["/zuul-harness-smoke".to_string(), missing.clone()])
            .unwrap();
        assert!(kids[0].as_ref().unwrap().iter().any(|k| k == "a" || k == "node"));
        assert!(matches!(kids[1], Err(ZkError::NoNode)));
        let kids_first_missing = zk
            .multi_children(&[missing.clone(), "/zuul-harness-smoke".into()])
            .unwrap();
        assert!(matches!(kids_first_missing[0], Err(ZkError::NoNode)));
        assert!(kids_first_missing[1].as_ref().unwrap().iter().any(|k| k == "node"));
        zk.delete(&created, false).unwrap();
        assert!(zk.exists(&created).unwrap().is_none());
        zk.delete("/zuul-harness-smoke", true).unwrap();
    }
}
