// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// UDP statsd client matching zuul.lib.statsd + statsd.StatsClient (14.3.0).
// Packet layout is the pystatsd 3.x / 4.x wire format:
//   counter  name:1|c
//   timing   name:0.000000|ms
//   gauge    name:1|g
//   prefix   prefix.name
// A negative gauge is two pipeline lines, `name:0|g` then `name:-N|g`,
// joined with a newline when they fit in the 512-byte UDP payload.

use crate::config::StatsdConfig;
use crate::launcher::StatEvent;
use std::net::UdpSocket;
use std::sync::{Mutex, OnceLock};

const MAX_UDP: usize = 512;

struct Client {
    sock: UdpSocket,
    prefix: Option<String>,
}

fn global() -> &'static Mutex<Option<Client>> {
    static CLIENT: OnceLock<Mutex<Option<Client>>> = OnceLock::new();
    CLIENT.get_or_init(|| Mutex::new(None))
}

pub fn install(cfg: &StatsdConfig) {
    let Some(host) = cfg.server.as_ref().filter(|h| !h.is_empty()) else {
        if let Ok(mut slot) = global().lock() {
            *slot = None;
        }
        return;
    };
    let addr = format!("{host}:{}", cfg.port);
    let sock = match UdpSocket::bind("0.0.0.0:0").and_then(|sock| {
        sock.connect(&addr)?;
        Ok(sock)
    }) {
        Ok(sock) => sock,
        Err(err) => {
            eprintln!("statsd: unable to connect to {addr}: {err}");
            return;
        }
    };
    let prefix = cfg.prefix.clone().filter(|p| !p.is_empty());
    if let Ok(mut slot) = global().lock() {
        *slot = Some(Client { sock, prefix });
    }
}

pub fn enabled() -> bool {
    global().lock().map(|g| g.is_some()).unwrap_or(false)
}

/// Send one metric. The PyO3 harness forwards the in-memory vec itself;
/// sending here as well would duplicate every packet on FakeStatsd.
pub fn emit(event: &StatEvent) {
    if crate::cloud::enabled() {
        return;
    }
    let Ok(slot) = global().lock() else {
        return;
    };
    let Some(client) = slot.as_ref() else {
        return;
    };
    client.send(event);
}

pub fn timing(name: &str, milliseconds: f64) {
    emit(&StatEvent {
        kind: "ms".into(),
        name: name.to_string(),
        value: milliseconds,
    });
}

impl Client {
    fn send(&self, event: &StatEvent) {
        let stat = self.prefixed(&event.name);
        let packet = match event.kind.as_str() {
            "c" => format!("{stat}:{}|c", format_count(event.value)),
            "ms" => format!("{stat}:{:.6}|ms", event.value),
            "g" => return self.send_gauge(&stat, event.value),
            "s" => format!("{stat}:{}|s", format_count(event.value)),
            _ => return,
        };
        let _ = self.sock.send(packet.as_bytes());
    }

    fn send_gauge(&self, stat: &str, value: f64) {
        if value < 0.0 {
            let zero = format!("{stat}:0|g");
            let line = format!("{stat}:{}|g", format_gauge(value));
            let joined = format!("{zero}\n{line}");
            if joined.len() < MAX_UDP {
                let _ = self.sock.send(joined.as_bytes());
            } else {
                let _ = self.sock.send(zero.as_bytes());
                let _ = self.sock.send(line.as_bytes());
            }
            return;
        }
        let packet = format!("{stat}:{}|g", format_gauge(value));
        let _ = self.sock.send(packet.as_bytes());
    }

    fn prefixed(&self, name: &str) -> String {
        match &self.prefix {
            Some(prefix) => format!("{prefix}.{name}"),
            None => name.to_string(),
        }
    }
}

fn format_count(value: f64) -> String {
    if value.is_finite() && value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{}", value as i64)
    } else {
        format_gauge(value)
    }
}

/// Python `str(int)` / `str(float)` for the values the launcher sends.
fn format_gauge(value: f64) -> String {
    if value.is_nan() {
        return "nan".into();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-inf".into()
        } else {
            "inf".into()
        };
    }
    if value.fract() == 0.0 && value.abs() < 1e15 {
        return format!("{}", value as i64);
    }
    let mut text = format!("{value}");
    if text.contains('e') || text.contains('E') {
        text = format!("{value:.6}");
        while text.contains('.') && text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StatsdConfig;
    use std::process::Command;
    use std::time::Duration;

    #[test]
    fn packets_match_pystatsd_layout() {
        assert_eq!(format_count(1.0), "1");
        assert_eq!(format_gauge(1.0), "1");
        assert_eq!(format_gauge(-2.0), "-2");
        assert_eq!(format_gauge(f64::INFINITY), "inf");
        let timing = format!("{:.6}|ms", 12.5_f64);
        assert_eq!(timing, "12.500000|ms");
        let gauge = format!("zuul.nodes.state.ready:{}|g", format_gauge(3.0));
        assert_eq!(gauge, "zuul.nodes.state.ready:3|g");
        let negative = format!("n:0|g\nn:{}|g", format_gauge(-4.0));
        assert_eq!(negative, "n:0|g\nn:-4|g");
    }

    /// Capture one UDP datagram from this process and one from
    /// statsd.StatsClient for the same counter, timing, gauge, and
    /// negative gauge, including the zuul.conf prefix.
    #[test]
    fn udp_packets_match_python_statsd_client() {
        let py = "/home/ubuntu/zuul-nox-311/bin/python";
        if !std::path::Path::new(py).exists() {
            return;
        }
        let sock = UdpSocket::bind("127.0.0.1:0").expect("bind");
        let port = sock.local_addr().expect("addr").port();
        sock.set_read_timeout(Some(Duration::from_secs(3))).ok();
        install(&StatsdConfig {
            server: Some("127.0.0.1".into()),
            port,
            prefix: Some("zuul".into()),
        });
        let cases = [
            ("c", "nodes.launch.ready", 1.0_f64),
            ("ms", "launcher.run_handler", 12.5),
            ("g", "nodes.state.ready", 3.0),
            ("g", "nodes.leaked", -4.0),
        ];
        let mut from_rust = Vec::new();
        for (kind, name, value) in cases {
            emit(&StatEvent {
                kind: kind.into(),
                name: name.into(),
                value,
            });
            from_rust.push(recv_one(&sock));
        }
        let script = format!(
            "import statsd\n\
             c = statsd.StatsClient('127.0.0.1', {port}, prefix='zuul')\n\
             c.incr('nodes.launch.ready', 1)\n\
             c.timing('launcher.run_handler', 12.5)\n\
             c.gauge('nodes.state.ready', 3)\n\
             c.gauge('nodes.leaked', -4)\n"
        );
        let status = Command::new(py)
            .arg("-c")
            .arg(&script)
            .status()
            .expect("python statsd");
        assert!(status.success(), "python statsd client failed");
        let mut from_python = Vec::new();
        for _ in 0..cases.len() {
            from_python.push(recv_one(&sock));
        }
        assert_eq!(
            from_rust, from_python,
            "rust statsd datagrams differ from statsd.StatsClient"
        );
    }

    fn recv_one(sock: &UdpSocket) -> Vec<u8> {
        let mut buf = [0u8; 2048];
        let n = sock.recv(&mut buf).expect("statsd datagram");
        buf[..n].to_vec()
    }
}
