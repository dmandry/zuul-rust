// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// ConfigParser-compatible ini: %(name)s interpolation, getboolean / getint.
// Defaults and lookup order follow zuul/cmd/__init__.py and zuul/lib/config.py.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct Config {
    /// section -> key -> raw value (after interpolation)
    sections: BTreeMap<String, BTreeMap<String, String>>,
}

impl Config {
    pub fn empty() -> Self {
        Self {
            sections: BTreeMap::new(),
        }
    }

    pub fn parse(text: &str, defaults: &BTreeMap<String, String>) -> Result<Self, String> {
        let mut sections: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        let mut current = "DEFAULT".to_string();
        sections.insert(current.clone(), defaults.clone());
        for (lineno, raw) in text.lines().enumerate() {
            let line = strip_comment(raw).trim();
            if line.is_empty() {
                continue;
            }
            if line.starts_with('[') && line.ends_with(']') {
                current = line[1..line.len() - 1].trim().to_string();
                sections.entry(current.clone()).or_default();
                continue;
            }
            let Some((k, v)) = line.split_once('=') else {
                return Err(format!(
                    "config line {}: expected key = value",
                    lineno + 1
                ));
            };
            let key = k.trim().to_string();
            let val = v.trim().to_string();
            sections.entry(current.clone()).or_default().insert(key, val);
        }
        let mut cfg = Self { sections };
        cfg.interpolate()?;
        Ok(cfg)
    }

    pub fn load_zuul(path: Option<&Path>) -> Result<Self, String> {
        let mut defaults = BTreeMap::new();
        for (k, v) in env::vars() {
            if k.starts_with("ZUUL_") {
                defaults.insert(k, v);
            }
        }
        let locations: Vec<PathBuf> = if let Some(p) = path {
            vec![p.to_path_buf()]
        } else {
            let mut home = env::var("HOME").unwrap_or_else(|_| "/root".into());
            if home == "~" {
                home = "/root".into();
            }
            vec![
                PathBuf::from("/etc/zuul/zuul.conf"),
                PathBuf::from(home).join("zuul.conf"),
            ]
        };
        for fp in &locations {
            let expanded = expand_user(fp);
            if expanded.exists() {
                let text = fs::read_to_string(&expanded)
                    .map_err(|e| format!("reading {}: {e}", expanded.display()))?;
                return Self::parse(&text, &defaults);
            }
        }
        Err(format!(
            "Unable to locate config file in {}",
            locations
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ))
    }

    fn interpolate(&mut self) -> Result<(), String> {
        // BasicInterpolation: %(name)s looks in the same section, then DEFAULT.
        let names: Vec<String> = self.sections.keys().cloned().collect();
        for section in names {
            let keys: Vec<String> = self
                .sections
                .get(&section)
                .map(|m| m.keys().cloned().collect())
                .unwrap_or_default();
            for key in keys {
                let mut value = self.get_raw(&section, &key).unwrap_or_default();
                for _ in 0..20 {
                    if !value.contains("%(") {
                        break;
                    }
                    let Some(start) = value.find("%(") else { break };
                    let Some(end) = value[start..].find(")s") else {
                        return Err(format!("bad interpolation in [{section}] {key}"));
                    };
                    let name = &value[start + 2..start + end];
                    let repl = self
                        .get_raw(&section, name)
                        .or_else(|| self.get_raw("DEFAULT", name))
                        .ok_or_else(|| {
                            format!("interpolation key {name} missing in [{section}]")
                        })?;
                    value = format!("{}{}{}", &value[..start], repl, &value[start + end + 2..]);
                }
                if let Some(map) = self.sections.get_mut(&section) {
                    map.insert(key, value);
                }
            }
        }
        Ok(())
    }

    pub fn has_section(&self, section: &str) -> bool {
        self.sections.contains_key(section)
    }

    pub fn has_option(&self, section: &str, option: &str) -> bool {
        self.sections
            .get(section)
            .map(|m| m.contains_key(option))
            .unwrap_or(false)
            || self
                .sections
                .get("DEFAULT")
                .map(|m| m.contains_key(option))
                .unwrap_or(false)
    }

    fn get_raw(&self, section: &str, option: &str) -> Option<String> {
        self.sections
            .get(section)
            .and_then(|m| m.get(option).cloned())
            .or_else(|| {
                if section == "DEFAULT" {
                    None
                } else {
                    self.sections
                        .get("DEFAULT")
                        .and_then(|m| m.get(option).cloned())
                }
            })
    }

    pub fn get(&self, section: &str, option: &str) -> Option<String> {
        self.get_raw(section, option)
    }

    pub fn get_default(&self, section: &str, option: &str, default: Option<&str>) -> Option<String> {
        self.get(section, option)
            .or_else(|| default.map(|s| s.to_string()))
    }

    pub fn getboolean(&self, section: &str, option: &str) -> Result<bool, String> {
        let v = self
            .get(section, option)
            .ok_or_else(|| format!("missing [{section}] {option}"))?;
        parse_bool(&v).ok_or_else(|| format!("invalid boolean [{section}] {option}={v}"))
    }

    pub fn getint(&self, section: &str, option: &str) -> Result<i64, String> {
        let v = self
            .get(section, option)
            .ok_or_else(|| format!("missing [{section}] {option}"))?;
        v.parse::<i64>()
            .map_err(|_| format!("invalid int [{section}] {option}={v}"))
    }

    pub fn getfloat(&self, section: &str, option: &str) -> Result<f64, String> {
        let v = self
            .get(section, option)
            .ok_or_else(|| format!("missing [{section}] {option}"))?;
        v.parse::<f64>()
            .map_err(|_| format!("invalid float [{section}] {option}={v}"))
    }

    pub fn sections(&self) -> impl Iterator<Item = &String> {
        self.sections.keys().filter(|s| s.as_str() != "DEFAULT")
    }

    pub fn items(&self, section: &str) -> BTreeMap<String, String> {
        let mut out = BTreeMap::new();
        if let Some(def) = self.sections.get("DEFAULT") {
            if section != "DEFAULT" {
                out.extend(def.clone());
            }
        }
        if let Some(map) = self.sections.get(section) {
            out.extend(map.clone());
        }
        out
    }
}

fn strip_comment(line: &str) -> &str {
    let mut in_single = false;
    let mut in_double = false;
    for (i, c) in line.char_indices() {
        match c {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            '#' | ';' if !in_single && !in_double => return &line[..i],
            _ => {}
        }
    }
    line
}

pub fn parse_bool(v: &str) -> Option<bool> {
    match v.trim().to_ascii_lowercase().as_str() {
        "1" | "yes" | "true" | "on" => Some(true),
        "0" | "no" | "false" | "off" => Some(false),
        _ => None,
    }
}

/// zuul/lib/config.py any_to_bool: only 1/on/yes/true are true.
pub fn any_to_bool(v: &str) -> bool {
    matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "1" | "on" | "yes" | "true"
    )
}

pub fn expand_user(path: &Path) -> PathBuf {
    let s = path.to_string_lossy();
    if let Some(rest) = s.strip_prefix("~/") {
        if let Ok(home) = env::var("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    path.to_path_buf()
}

/// get_default from zuul/lib/config.py, including type-from-default.
pub fn get_default_string(cfg: &Config, section: &str, option: &str, default: &str) -> String {
    cfg.get(section, option).unwrap_or_else(|| default.to_string())
}

pub fn get_default_opt(cfg: &Config, section: &str, option: &str) -> Option<String> {
    cfg.get(section, option)
}

pub fn get_default_int(cfg: &Config, section: &str, option: &str, default: i64) -> Result<i64, String> {
    if cfg.has_option(section, option) && cfg.get(section, option).is_some() {
        cfg.getint(section, option)
    } else {
        Ok(default)
    }
}

pub fn get_default_float(cfg: &Config, section: &str, option: &str, default: f64) -> Result<f64, String> {
    if cfg.has_option(section, option) && cfg.get(section, option).is_some() {
        cfg.getfloat(section, option)
    } else {
        Ok(default)
    }
}

pub fn get_default_bool(cfg: &Config, section: &str, option: &str, default: bool) -> Result<bool, String> {
    if cfg.has_option(section, option) && cfg.get(section, option).is_some() {
        cfg.getboolean(section, option)
    } else {
        Ok(default)
    }
}

#[derive(Clone, Debug)]
pub struct ZookeeperConfig {
    pub hosts: String,
    pub tls_key: String,
    pub tls_cert: String,
    pub tls_ca: String,
    pub session_timeout: f64,
}

pub fn zookeeper_config(cfg: &Config) -> Result<ZookeeperConfig, String> {
    let hosts = cfg
        .get("zookeeper", "hosts")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "The zookeeper hosts config value is required".to_string())?;
    let tls_key = cfg.get("zookeeper", "tls_key");
    let tls_cert = cfg.get("zookeeper", "tls_cert");
    let tls_ca = cfg.get("zookeeper", "tls_ca");
    if tls_key.as_deref().unwrap_or("").is_empty()
        || tls_cert.as_deref().unwrap_or("").is_empty()
        || tls_ca.as_deref().unwrap_or("").is_empty()
    {
        return Err(
            "A TLS ZooKeeper connection is required; please supply the tls_* zookeeper config values."
                .into(),
        );
    }
    let session_timeout = get_default_float(cfg, "zookeeper", "session_timeout", 120.0)?;
    Ok(ZookeeperConfig {
        hosts,
        tls_key: tls_key.unwrap(),
        tls_cert: tls_cert.unwrap(),
        tls_ca: tls_ca.unwrap(),
        session_timeout,
    })
}

#[derive(Clone, Debug)]
pub struct LauncherConfig {
    pub connection_filter: Option<Vec<String>>,
    pub temp_dir: String,
    pub command_socket: String,
    pub pidfile: String,
    pub log_config: Option<String>,
    pub prometheus_port: Option<u16>,
    pub prometheus_addr: String,
}

pub fn launcher_config(cfg: &Config) -> Result<LauncherConfig, String> {
    let connection_filter = cfg.get("launcher", "connection_filter").map(|s| {
        s.split(',')
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect::<Vec<_>>()
    });
    let connection_filter = match connection_filter {
        Some(v) if v.is_empty() => None,
        other => other,
    };
    let temp_root = get_default_string(cfg, "launcher", "temp_dir", "/tmp");
    let temp_root = expand_user(Path::new(&temp_root));
    let temp_dir = temp_root.join("zuul-launcher").display().to_string();
    let command_socket = get_default_string(
        cfg,
        "launcher",
        "command_socket",
        "/var/lib/zuul/launcher.socket",
    );
    let pidfile = get_default_string(cfg, "launcher", "pidfile", "/var/run/zuul/launcher.pid");
    let log_config = cfg.get("launcher", "log_config").map(|p| {
        expand_user(Path::new(&p)).display().to_string()
    });
    let prometheus_port = if cfg.has_option("launcher", "prometheus_port") {
        Some(cfg.getint("launcher", "prometheus_port")? as u16)
    } else {
        None
    };
    let prometheus_addr = get_default_string(cfg, "launcher", "prometheus_addr", "0.0.0.0");
    Ok(LauncherConfig {
        connection_filter,
        temp_dir,
        command_socket,
        pidfile,
        log_config,
        prometheus_port,
        prometheus_addr,
    })
}

#[derive(Clone, Debug)]
pub struct StatsdConfig {
    pub server: Option<String>,
    pub port: u16,
    pub prefix: Option<String>,
}

pub fn statsd_config(cfg: &Config) -> Result<StatsdConfig, String> {
    let server = cfg.get("statsd", "server");
    let port = get_default_int(cfg, "statsd", "port", 8125)? as u16;
    let prefix = cfg.get("statsd", "prefix");
    Ok(StatsdConfig {
        server,
        port,
        prefix,
    })
}

#[derive(Clone, Debug)]
pub struct TracingConfig {
    pub service_name: String,
    pub enabled: bool,
    pub protocol: String,
    pub endpoint: Option<String>,
    pub tls_key: Option<String>,
    pub tls_cert: Option<String>,
    pub tls_ca: Option<String>,
    pub certificate_file: Option<String>,
    pub insecure: Option<bool>,
    pub timeout: Option<i64>,
    pub compression: Option<String>,
}

pub fn tracing_config(cfg: &Config) -> Result<TracingConfig, String> {
    let service_name = get_default_string(cfg, "tracing", "service_name", "zuul");
    let enabled = cfg
        .get("tracing", "enabled")
        .map(|v| any_to_bool(&v))
        .unwrap_or(false);
    let protocol = get_default_string(cfg, "tracing", "protocol", "grpc");
    let insecure = match cfg.get("tracing", "insecure") {
        Some(v) => Some(any_to_bool(&v)),
        None => None,
    };
    let timeout = match cfg.get("tracing", "timeout") {
        Some(_) => Some(cfg.getint("tracing", "timeout")?),
        None => None,
    };
    if enabled {
        if protocol == "grpc" {
            if cfg.get("tracing", "certificate_file").is_some() {
                return Err(format!(
                    "The certificate_file tracing option is not valid for {protocol} endpoints"
                ));
            }
        } else if protocol == "http/protobuf" {
            if insecure == Some(true)
                && (cfg.get("tracing", "tls_ca").is_some()
                    || cfg.get("tracing", "tls_key").is_some()
                    || cfg.get("tracing", "tls_cert").is_some())
            {
                return Err(
                    "The insecure tracing option is incompatible with tls_* options".into(),
                );
            }
        } else {
            return Err(format!("Unknown tracing protocol {protocol}"));
        }
    }
    Ok(TracingConfig {
        service_name,
        enabled,
        protocol,
        endpoint: cfg.get("tracing", "endpoint"),
        tls_key: cfg.get("tracing", "tls_key"),
        tls_cert: cfg.get("tracing", "tls_cert"),
        tls_ca: cfg.get("tracing", "tls_ca"),
        certificate_file: cfg.get("tracing", "certificate_file"),
        insecure,
        timeout,
        compression: cfg.get("tracing", "compression"),
    })
}

/// Provider connection sections only: aws, azure, openstack, kubernetes, static.
pub const PROVIDER_DRIVERS: &[&str] = &["aws", "azure", "openstack", "kubernetes", "static"];

#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    pub name: String,
    pub driver: String,
    pub options: BTreeMap<String, String>,
}

pub fn provider_connections(cfg: &Config) -> Result<Vec<ConnectionConfig>, String> {
    let mut out = Vec::new();
    for section in cfg.sections() {
        let rest = section
            .strip_prefix("connection ")
            .or_else(|| section.strip_prefix("connection\t"));
        // ConfigParser matches: ^connection (['\"]?)(.*)(\1)$
        let Some(name) = connection_name(section) else {
            continue;
        };
        let items = cfg.items(section);
        let driver = items
            .get("driver")
            .cloned()
            .ok_or_else(|| format!("No driver specified for connection {name}."))?;
        if !PROVIDER_DRIVERS.contains(&driver.as_str()) {
            // The Python launcher still registers every driver, but only
            // constructs connections whose driver implements ProviderInterface.
            // Non-provider drivers are skipped when providers=True, unless the
            // driver name is unknown, which is an error.
            if is_known_non_provider(&driver) {
                continue;
            }
            return Err(format!(
                "Unknown driver, {driver}, for connection {name}"
            ));
        }
        let _ = rest;
        out.push(ConnectionConfig {
            name,
            driver,
            options: items,
        });
    }
    Ok(out)
}

fn is_known_non_provider(driver: &str) -> bool {
    matches!(
        driver,
        "zuul"
            | "gerrit"
            | "git"
            | "github"
            | "smtp"
            | "timer"
            | "sql"
            | "bubblewrap"
            | "nullwrap"
            | "mqtt"
            | "pagure"
            | "gitlab"
            | "elasticsearch"
    )
}

fn connection_name(section: &str) -> Option<String> {
    let lower = section.to_ascii_lowercase();
    let rest = lower.strip_prefix("connection ")?;
    let orig_rest = &section[section.len() - rest.len()..];
    let orig_rest = orig_rest.trim();
    if orig_rest.len() >= 2 {
        let b = orig_rest.as_bytes();
        if (b[0] == b'\'' || b[0] == b'"') && b[0] == b[b.len() - 1] {
            return Some(orig_rest[1..orig_rest.len() - 1].to_string());
        }
    }
    Some(orig_rest.to_string())
}

pub fn quote_plus(s: &str) -> String {
    let mut out = String::new();
    for b in s.as_bytes() {
        match *b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-' => {
                out.push(*b as char);
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
