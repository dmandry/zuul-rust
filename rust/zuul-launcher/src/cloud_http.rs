// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// HTTP, SigV4, a small YAML reader, and the Zuul 14.3.0 rate limiter.
// Used by the native AWS, Azure, OpenStack, and Kubernetes clients in
// the zuul-launcher binary. The PyO3 harness does not call this module.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    pub fn json(&self) -> Result<serde_json::Value, String> {
        serde_json::from_slice(&self.body).map_err(|e| format!("json: {e}: {}", self.text()))
    }
}

/// One HTTP/1.1 exchange. `url` may be http or https. Timeouts apply to
/// connect and to each read/write, matching azul's 60s request timeout.
pub fn exchange(
    method: &str,
    url: &str,
    headers: &[(&str, &str)],
    body: &[u8],
    timeout: Duration,
) -> Result<HttpResponse, String> {
    let parts = parse_url(url)?;
    let addr = format!("{}:{}", parts.host, parts.port);
    let mut addrs = addr
        .to_socket_addrs()
        .map_err(|e| format!("resolve {addr}: {e}"))?;
    let sock = addrs
        .next()
        .ok_or_else(|| format!("resolve {addr}: no address"))?;
    let stream = TcpStream::connect_timeout(&sock, timeout).map_err(|e| format!("connect {addr}: {e}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|e| e.to_string())?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|e| e.to_string())?;
    if parts.https {
        let connector = openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls())
            .map_err(|e| e.to_string())?
            .build();
        let mut tls = connector
            .connect(&parts.host, stream)
            .map_err(|e| format!("tls {}: {e}", parts.host))?;
        write_request(&mut tls, method, &parts, headers, body)?;
        read_response(&mut tls)
    } else {
        let mut stream = stream;
        write_request(&mut stream, method, &parts, headers, body)?;
        read_response(&mut stream)
    }
}

struct UrlParts {
    https: bool,
    host: String,
    port: u16,
    path: String,
}

fn parse_url(url: &str) -> Result<UrlParts, String> {
    let (https, rest) = if let Some(r) = url.strip_prefix("https://") {
        (true, r)
    } else if let Some(r) = url.strip_prefix("http://") {
        (false, r)
    } else {
        return Err(format!("unsupported url {url}"));
    };
    let (hostport, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].to_string()),
        None => (rest, "/".to_string()),
    };
    let path = if path.is_empty() { "/".into() } else { path };
    let (host, port) = if let Some(h) = hostport.strip_prefix('[') {
        let end = h.find(']').ok_or("bad ipv6 url")?;
        let host = h[..end].to_string();
        let port = if let Some(p) = h[end + 1..].strip_prefix(':') {
            p.parse().map_err(|_| format!("bad port in {url}"))?
        } else if https {
            443
        } else {
            80
        };
        (host, port)
    } else if let Some((h, p)) = hostport.rsplit_once(':') {
        if h.contains(':') {
            (hostport.to_string(), if https { 443 } else { 80 })
        } else {
            (h.to_string(), p.parse().map_err(|_| format!("bad port in {url}"))?)
        }
    } else {
        (hostport.to_string(), if https { 443 } else { 80 })
    };
    Ok(UrlParts {
        https,
        host,
        port,
        path,
    })
}

fn write_request<S: Write>(
    stream: &mut S,
    method: &str,
    parts: &UrlParts,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Result<(), String> {
    let mut req = format!("{} {} HTTP/1.1\r\nHost: {}\r\n", method, parts.path, host_header(parts));
    let mut has_len = false;
    let mut has_conn = false;
    for (k, v) in headers {
        if k.eq_ignore_ascii_case("host") {
            continue;
        }
        if k.eq_ignore_ascii_case("content-length") {
            has_len = true;
        }
        if k.eq_ignore_ascii_case("connection") {
            has_conn = true;
        }
        req.push_str(k);
        req.push_str(": ");
        req.push_str(v);
        req.push_str("\r\n");
    }
    if !has_len {
        req.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    if !has_conn {
        req.push_str("Connection: close\r\n");
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes()).map_err(|e| e.to_string())?;
    stream.write_all(body).map_err(|e| e.to_string())?;
    stream.flush().map_err(|e| e.to_string())?;
    Ok(())
}

fn host_header(parts: &UrlParts) -> String {
    let default = if parts.https { 443 } else { 80 };
    if parts.port == default {
        parts.host.clone()
    } else {
        format!("{}:{}", parts.host, parts.port)
    }
}

fn read_response<S: Read>(stream: &mut S) -> Result<HttpResponse, String> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 8192];
    loop {
        let n = stream.read(&mut tmp).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if find_header_end(&buf).is_some() {
            // Keep reading the body below once headers are complete,
            // but only if Content-Length or chunked says there is more.
            break;
        }
    }
    let header_end = find_header_end(&buf).ok_or("short http response")?;
    let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or("");
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| format!("bad status {status_line}"))?;
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }
    let mut body = buf[header_end + 4..].to_vec();
    let chunked = headers.iter().any(|(k, v)| {
        k.eq_ignore_ascii_case("transfer-encoding") && v.to_ascii_lowercase().contains("chunked")
    });
    if chunked {
        loop {
            if decode_chunks(&body).is_ok() {
                // Need the terminating 0 chunk. decode_chunks fails until complete.
                break;
            }
            let n = stream.read(&mut tmp).map_err(|e| e.to_string())?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&tmp[..n]);
        }
        body = decode_chunks(&body)?;
    } else if let Some(len) = headers.iter().find_map(|(k, v)| {
        if k.eq_ignore_ascii_case("content-length") {
            v.parse::<usize>().ok()
        } else {
            None
        }
    }) {
        while body.len() < len {
            let n = stream.read(&mut tmp).map_err(|e| e.to_string())?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&tmp[..n]);
        }
        body.truncate(len);
    } else {
        loop {
            let n = stream.read(&mut tmp).map_err(|e| e.to_string())?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&tmp[..n]);
        }
    }
    Ok(HttpResponse {
        status,
        headers,
        body,
    })
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn decode_chunks(buf: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let mut rest = buf;
    loop {
        let line_end = rest
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or("incomplete chunk")?;
        let line = std::str::from_utf8(&rest[..line_end]).map_err(|e| e.to_string())?;
        let size = usize::from_str_radix(line.split(';').next().unwrap_or("").trim(), 16)
            .map_err(|e| format!("chunk size: {e}"))?;
        rest = &rest[line_end + 2..];
        if rest.len() < size + 2 {
            return Err("incomplete chunk body".into());
        }
        if size == 0 {
            return Ok(out);
        }
        out.extend_from_slice(&rest[..size]);
        rest = &rest[size + 2..];
    }
}

pub fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

pub fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// AWS SigV4 (botocore standard mode, header auth). `amz_date` is
/// `YYYYMMDDThhmmssZ`. The signed header set is host, content-type (if
/// present), x-amz-date, and x-amz-security-token (if a session token
/// is set), which is the set boto signs for EC2, S3, EBS, and STS.
pub struct SigV4 {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
    pub region: String,
    pub service: String,
}

impl SigV4 {
    pub fn authorization(
        &self,
        method: &str,
        host: &str,
        canonical_uri: &str,
        canonical_query: &str,
        extra_headers: &[(&str, &str)],
        payload: &[u8],
        amz_date: &str,
    ) -> (String, Vec<(String, String)>) {
        let date = &amz_date[..8];
        let payload_hash = sha256_hex(payload);
        let mut headers: Vec<(String, String)> = vec![
            ("host".into(), host.to_string()),
            ("x-amz-date".into(), amz_date.to_string()),
        ];
        for (k, v) in extra_headers {
            let lk = k.to_ascii_lowercase();
            if lk == "host" || lk == "x-amz-date" || lk == "authorization" {
                continue;
            }
            headers.push((lk, (*v).to_string()));
        }
        if let Some(token) = &self.session_token {
            headers.push(("x-amz-security-token".into(), token.clone()));
        }
        headers.sort_by(|a, b| a.0.cmp(&b.0));
        let mut canonical_headers = String::new();
        let mut signed = Vec::new();
        for (k, v) in &headers {
            // Only sign the headers boto includes for these services.
            if k == "host"
                || k == "content-type"
                || k == "x-amz-date"
                || k == "x-amz-security-token"
                || k == "x-amz-content-sha256"
                || k == "x-amz-checksum"
                || k == "x-amz-checksum-algorithm"
            {
                canonical_headers.push_str(k);
                canonical_headers.push(':');
                canonical_headers.push_str(v.trim());
                canonical_headers.push('\n');
                signed.push(k.clone());
            }
        }
        let signed_headers = signed.join(";");
        let canonical = format!(
            "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        );
        let scope = format!("{date}/{}/{}/aws4_request", self.region, self.service);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            sha256_hex(canonical.as_bytes())
        );
        let k_date = hmac_sha256(format!("AWS4{}", self.secret_key).as_bytes(), date.as_bytes());
        let k_region = hmac_sha256(&k_date, self.region.as_bytes());
        let k_service = hmac_sha256(&k_region, self.service.as_bytes());
        let k_signing = hmac_sha256(&k_service, b"aws4_request");
        let signature = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()));
        let auth = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.access_key, scope, signed_headers, signature
        );
        let mut out = Vec::new();
        out.push(("Authorization".into(), auth.clone()));
        out.push(("x-amz-date".into(), amz_date.to_string()));
        if let Some(token) = &self.session_token {
            out.push(("x-amz-security-token".into(), token.clone()));
        }
        let _ = payload_hash;
        (auth, out)
    }
}

pub fn aws_encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.as_bytes() {
        match *b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// botocore `ec2` protocol: lists are `Name.1`, structures are `Name.Member`.
pub fn ec2_query(params: &serde_json::Map<String, serde_json::Value>) -> String {
    let mut pairs = Vec::new();
    flatten_query("", params, &mut pairs);
    pairs.sort();
    pairs
        .into_iter()
        .map(|(k, v)| format!("{}={}", aws_encode(&k), aws_encode(&v)))
        .collect::<Vec<_>>()
        .join("&")
}

fn flatten_query(prefix: &str, value: &serde_json::Map<String, serde_json::Value>, out: &mut Vec<(String, String)>) {
    let mut keys: Vec<_> = value.keys().cloned().collect();
    keys.sort();
    for k in keys {
        let v = &value[&k];
        let name = if prefix.is_empty() {
            k.clone()
        } else {
            format!("{prefix}.{k}")
        };
        push_query(&name, v, out);
    }
}

fn push_query(name: &str, value: &serde_json::Value, out: &mut Vec<(String, String)>) {
    match value {
        serde_json::Value::Null => {}
        serde_json::Value::Bool(b) => out.push((name.into(), if *b { "true" } else { "false" }.into())),
        serde_json::Value::Number(n) => out.push((name.into(), n.to_string())),
        serde_json::Value::String(s) => out.push((name.into(), s.clone())),
        serde_json::Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                push_query(&format!("{name}.{}", i + 1), item, out);
            }
        }
        serde_json::Value::Object(map) => flatten_query(name, map, out),
    }
}

/// EC2 query XML → the dict shape boto3 returns (PascalCase, `*Set`/`item`
/// collapsed into arrays, `instanceState` renamed to `State`).
pub fn ec2_xml_to_boto(xml: &str) -> Result<serde_json::Value, String> {
    let raw = parse_xml(xml)?;
    Ok(ec2_fold(raw))
}

fn ec2_fold(v: serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, val) in map {
                let mut child = val;
                if let serde_json::Value::Object(ref obj) = child {
                    if obj.len() == 1 && obj.contains_key("item") {
                        let item = obj.get("item").cloned().unwrap_or(serde_json::Value::Null);
                        child = if item.is_array() {
                            item
                        } else {
                            serde_json::Value::Array(vec![item])
                        };
                    }
                }
                out.insert(ec2_key(&k), ec2_fold(child));
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(ec2_fold).collect())
        }
        other => other,
    }
}

fn ec2_key(k: &str) -> String {
    match k {
        "instanceState" => "State".into(),
        "instancesSet" => "Instances".into(),
        "reservationSet" => "Reservations".into(),
        "hostSet" => "Hosts".into(),
        "imagesSet" | "imageSet" => "Images".into(),
        "volumeSet" => "Volumes".into(),
        "snapshotSet" => "Snapshots".into(),
        "instanceTypeSet" => "InstanceTypes".into(),
        "launchTemplateSet" => "LaunchTemplates".into(),
        "tagSet" => "Tags".into(),
        "groupSet" => "SecurityGroups".into(),
        "blockDeviceMapping" => "BlockDeviceMappings".into(),
        "networkInterfaceSet" => "NetworkInterfaces".into(),
        "importImageTaskSet" => "ImportImageTasks".into(),
        "importSnapshotTaskSet" => "ImportSnapshotTasks".into(),
        other => camel_pascal(other),
    }
}

fn camel_pascal(s: &str) -> String {
    if s.is_empty() {
        return String::new();
    }
    let mut chars = s.chars();
    let first = chars.next().unwrap();
    format!("{}{}", first.to_ascii_uppercase(), chars.as_str())
}

fn parse_xml(xml: &str) -> Result<serde_json::Value, String> {
    let bytes = xml.as_bytes();
    let mut i = 0;
    skip_prolog(bytes, &mut i);
    let (_name, value) = parse_element(bytes, &mut i)?;
    Ok(value)
}

fn skip_prolog(b: &[u8], i: &mut usize) {
    while *i < b.len() {
        while *i < b.len() && b[*i].is_ascii_whitespace() {
            *i += 1;
        }
        if b.get(*i..*i + 2) == Some(b"<?") || b.get(*i..*i + 4) == Some(b"<!--") {
            if b.get(*i..*i + 4) == Some(b"<!--") {
                if let Some(end) = find_slice(b, *i, b"-->") {
                    *i = end + 3;
                    continue;
                }
            }
            if let Some(end) = find_slice(b, *i, b"?>") {
                *i = end + 2;
                continue;
            }
        }
        break;
    }
}

fn find_slice(b: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    b[from..]
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|p| from + p)
}

fn parse_element(b: &[u8], i: &mut usize) -> Result<(String, serde_json::Value), String> {
    while *i < b.len() && b[*i].is_ascii_whitespace() {
        *i += 1;
    }
    if b.get(*i) != Some(&b'<') {
        return Err("expected element".into());
    }
    *i += 1;
    if b.get(*i) == Some(&b'!') {
        // comment or doctype; skip
        if let Some(end) = find_slice(b, *i, b">") {
            *i = end + 1;
        }
        return parse_element(b, i);
    }
    let start = *i;
    while *i < b.len() && b[*i] != b'>' && b[*i] != b' ' && b[*i] != b'/' && b[*i] != b'\t' {
        *i += 1;
    }
    let name = local_name(std::str::from_utf8(&b[start..*i]).unwrap_or(""));
    // A slash inside an attribute (`xmlns="http://..."`) is not an empty
    // element. Only `/>` is.
    while *i < b.len() && b[*i] != b'>' {
        *i += 1;
    }
    let self_close = *i > 0 && b.get(*i - 1) == Some(&b'/');
    if *i < b.len() && b[*i] == b'>' {
        *i += 1;
    }
    if self_close {
        return Ok((name, serde_json::Value::String(String::new())));
    }
    let mut text = String::new();
    let mut children: Vec<(String, serde_json::Value)> = Vec::new();
    loop {
        if *i >= b.len() {
            break;
        }
        if b.get(*i..*i + 2) == Some(b"</") {
            *i += 2;
            while *i < b.len() && b[*i] != b'>' {
                *i += 1;
            }
            if *i < b.len() {
                *i += 1;
            }
            break;
        }
        if b.get(*i..*i + 4) == Some(b"<!--") {
            if let Some(end) = find_slice(b, *i, b"-->") {
                *i = end + 3;
                continue;
            }
        }
        if b.get(*i) == Some(&b'<') {
            let (n, v) = parse_element(b, i)?;
            children.push((n, v));
        } else {
            let tstart = *i;
            while *i < b.len() && b[*i] != b'<' {
                *i += 1;
            }
            text.push_str(std::str::from_utf8(&b[tstart..*i]).unwrap_or(""));
        }
    }
    if children.is_empty() {
        return Ok((name, serde_json::Value::String(decode_xml(&text.trim()))));
    }
    let mut map = serde_json::Map::new();
    for (k, v) in children {
        match map.remove(&k) {
            None => {
                map.insert(k, v);
            }
            Some(serde_json::Value::Array(mut arr)) => {
                arr.push(v);
                map.insert(k, serde_json::Value::Array(arr));
            }
            Some(prev) => {
                map.insert(k, serde_json::Value::Array(vec![prev, v]));
            }
        }
    }
    if !text.trim().is_empty() {
        map.insert("#text".into(), serde_json::Value::String(decode_xml(text.trim())));
    }
    Ok((name, serde_json::Value::Object(map)))
}

fn local_name(name: &str) -> String {
    name.rsplit(':').next().unwrap_or(name).to_string()
}

fn decode_xml(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// Indentation YAML sufficient for kubeconfig and clouds.yaml
/// (maps, lists, scalars, `|` block scalars). Not a general YAML parser.
pub fn parse_yaml(text: &str) -> serde_json::Value {
    let lines: Vec<(usize, String)> = text
        .lines()
        .map(|l| {
            let trimmed = strip_comment(l);
            let indent = trimmed.chars().take_while(|c| *c == ' ').count();
            (indent, trimmed.trim().to_string())
        })
        .filter(|(_, s)| !s.is_empty())
        .collect();
    let mut i = 0;
    parse_yaml_block(&lines, &mut i, 0).unwrap_or(serde_json::Value::Null)
}

fn strip_comment(line: &str) -> String {
    let mut out = String::new();
    let mut quote: Option<char> = None;
    for c in line.chars() {
        if let Some(q) = quote {
            out.push(c);
            if c == q {
                quote = None;
            }
            continue;
        }
        if c == '"' || c == '\'' {
            quote = Some(c);
            out.push(c);
            continue;
        }
        if c == '#' {
            break;
        }
        out.push(c);
    }
    out
}

fn parse_yaml_block(
    lines: &[(usize, String)],
    i: &mut usize,
    indent: usize,
) -> Option<serde_json::Value> {
    if *i >= lines.len() {
        return None;
    }
    if lines[*i].1.starts_with("- ") || lines[*i].1 == "-" {
        let mut arr = Vec::new();
        while *i < lines.len() && lines[*i].0 == indent && (lines[*i].1.starts_with("- ") || lines[*i].1 == "-") {
            let item = lines[*i].1.trim_start_matches("- ").trim().to_string();
            let item_indent = lines[*i].0;
            *i += 1;
            if item.is_empty() {
                if *i < lines.len() && lines[*i].0 > item_indent {
                    arr.push(parse_yaml_block(lines, i, lines[*i].0).unwrap_or(serde_json::Value::Null));
                } else {
                    arr.push(serde_json::Value::Null);
                }
            } else if item.contains(':') && !item.starts_with('"') && !item.starts_with('\'') {
                let (k, v) = split_key(&item);
                let mut map = serde_json::Map::new();
                if v.is_empty() {
                    if *i < lines.len()
                        && (lines[*i].0 > item_indent
                            || lines[*i].1.starts_with("- ")
                            || lines[*i].1 == "-")
                    {
                        let child_indent = lines[*i].0;
                        map.insert(
                            k,
                            parse_yaml_block(lines, i, child_indent).unwrap_or(serde_json::Value::Null),
                        );
                    } else {
                        map.insert(k, serde_json::Value::Null);
                    }
                } else {
                    map.insert(k, yaml_scalar(&v));
                }
                while *i < lines.len() && lines[*i].0 > item_indent && !lines[*i].1.starts_with('-') {
                    // sibling keys of this list item live at item_indent+2 typically,
                    // but any deeper-than-dash indent that is a key belongs here
                    // only when it is not a nested list already consumed.
                    if lines[*i].0 <= item_indent {
                        break;
                    }
                    if lines[*i].1.starts_with("- ") {
                        break;
                    }
                    let key_indent = lines[*i].0;
                    let (k, v) = split_key(&lines[*i].1);
                    *i += 1;
                    if v.is_empty() {
                        if *i < lines.len()
                            && (lines[*i].0 > key_indent
                                || lines[*i].1.starts_with("- ")
                                || lines[*i].1 == "-")
                        {
                            map.insert(
                                k,
                                parse_yaml_block(lines, i, lines[*i].0).unwrap_or(serde_json::Value::Null),
                            );
                        } else {
                            map.insert(k, serde_json::Value::Null);
                        }
                    } else {
                        map.insert(k, yaml_scalar(&v));
                    }
                }
                arr.push(serde_json::Value::Object(map));
            } else {
                arr.push(yaml_scalar(&item));
            }
        }
        return Some(serde_json::Value::Array(arr));
    }
    let mut map = serde_json::Map::new();
    while *i < lines.len() && lines[*i].0 == indent && !lines[*i].1.starts_with("- ") {
        let (k, v) = split_key(&lines[*i].1);
        let key_indent = lines[*i].0;
        *i += 1;
        if v == "|" || v == "|-" || v == ">" {
            let mut block = String::new();
            while *i < lines.len() && lines[*i].0 > key_indent {
                if !block.is_empty() {
                    block.push('\n');
                }
                block.push_str(lines[*i].1.trim());
                *i += 1;
            }
            map.insert(k, serde_json::Value::String(block));
        } else if v.is_empty() {
            // A block sequence may start at the same column as its key
            // (`clusters:\n- name: c`), which is valid YAML.
            if *i < lines.len()
                && (lines[*i].0 > key_indent
                    || lines[*i].1.starts_with("- ")
                    || lines[*i].1 == "-")
            {
                map.insert(
                    k,
                    parse_yaml_block(lines, i, lines[*i].0).unwrap_or(serde_json::Value::Null),
                );
            } else {
                map.insert(k, serde_json::Value::Null);
            }
        } else {
            map.insert(k, yaml_scalar(&v));
        }
    }
    Some(serde_json::Value::Object(map))
}

fn split_key(line: &str) -> (String, String) {
    if let Some((k, v)) = line.split_once(':') {
        (k.trim().to_string(), v.trim().to_string())
    } else {
        (line.to_string(), String::new())
    }
}

fn yaml_scalar(s: &str) -> serde_json::Value {
    let s = s.trim().trim_matches('"').trim_matches('\'');
    match s {
        "true" | "True" | "yes" => serde_json::Value::Bool(true),
        "false" | "False" | "no" => serde_json::Value::Bool(false),
        "null" | "~" | "" => serde_json::Value::Null,
        _ => {
            if let Ok(n) = s.parse::<i64>() {
                serde_json::Value::from(n)
            } else if let Ok(n) = s.parse::<f64>() {
                serde_json::json!(n)
            } else {
                serde_json::Value::String(s.to_string())
            }
        }
    }
}

/// Zuul 14.3.0 `zuul.driver.util.RateLimiter`. The delay is computed and
/// slept while the lock is held, and the timestamp is the time after
/// the wait. `delta` is `1/rate` (0 when rate is 0).
pub struct RateLimiter {
    delta: Duration,
    last: std::sync::Mutex<Option<Instant>>,
}

impl RateLimiter {
    pub fn new(rate: f64) -> Self {
        let delta = if rate <= 0.0 {
            Duration::from_secs(0)
        } else {
            Duration::from_secs_f64(1.0 / rate)
        };
        Self {
            delta,
            last: std::sync::Mutex::new(None),
        }
    }

    pub fn enter(&self) -> f64 {
        // Hold the lock across the sleep. Python's RateLimiter._enter
        // does the same, so a second mutating call stays inside its
        // future until the interval has elapsed.
        let mut last = self.last.lock().unwrap();
        if last.is_none() {
            *last = Some(Instant::now());
            return 0.0;
        }
        let mut total = Duration::from_secs(0);
        loop {
            let prev = (*last).expect("rate limiter timestamp");
            let now = Instant::now();
            let elapsed = now.saturating_duration_since(prev);
            if elapsed >= self.delta {
                break;
            }
            let delay = self.delta - elapsed;
            std::thread::sleep(delay);
            total += delay;
        }
        *last = Some(Instant::now());
        total.as_secs_f64()
    }
}

pub fn amz_now() -> String {
    // UTC timestamp in the form boto uses for x-amz-date.
    let dur = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    let days = secs / 86400;
    let tod = secs % 86400;
    let (y, m, d) = civil_from_days(days as i64);
    let hh = tod / 3600;
    let mm = (tod % 3600) / 60;
    let ss = tod % 60;
    format!("{y:04}{m:02}{d:02}T{hh:02}{mm:02}{ss:02}Z")
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = y + if m <= 2 { 1 } else { 0 };
    (y, m as u32, d as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sigv4_matches_aws_iam_example() {
        // https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html
        let sig = SigV4 {
            access_key: "AKIDEXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(),
            session_token: None,
            region: "us-east-1".into(),
            service: "iam".into(),
        };
        let (auth, _) = sig.authorization(
            "GET",
            "iam.amazonaws.com",
            "/",
            "Action=ListUsers&Version=2010-05-08",
            &[("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")],
            b"",
            "20150830T123600Z",
        );
        assert!(
            auth.contains("Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"),
            "{auth}"
        );
        assert!(auth.contains("SignedHeaders=content-type;host;x-amz-date"));
        assert!(auth.contains("Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request"));
    }

    #[test]
    fn ec2_query_uses_one_based_indexes() {
        let params = serde_json::json!({
            "Action": "RunInstances",
            "Version": "2016-11-15",
            "MinCount": 1,
            "TagSpecifications": [{
                "ResourceType": "instance",
                "Tag": [{"Key": "Name", "Value": "np"}]
            }]
        });
        let q = ec2_query(params.as_object().unwrap());
        assert!(q.contains("Action=RunInstances"));
        assert!(q.contains("TagSpecifications.1.ResourceType=instance"));
        assert!(q.contains("TagSpecifications.1.Tag.1.Key=Name"));
        assert!(q.contains("TagSpecifications.1.Tag.1.Value=np"));
        assert!(q.contains("MinCount=1"));
    }

    #[test]
    fn ec2_xml_becomes_boto_instance() {
        let xml = r#"<?xml version="1.0"?>
            <RunInstancesResponse>
              <instancesSet>
                <item>
                  <instanceId>i-abc</instanceId>
                  <instanceType>t3.medium</instanceType>
                  <instanceState><code>0</code><name>pending</name></instanceState>
                </item>
              </instancesSet>
            </RunInstancesResponse>"#;
        let v = ec2_xml_to_boto(xml).unwrap();
        let inst = &v["Instances"][0];
        assert_eq!(inst["InstanceId"], "i-abc");
        assert_eq!(inst["InstanceType"], "t3.medium");
        assert_eq!(inst["State"]["Name"], "pending");
    }

    #[test]
    fn http_roundtrip_against_local_server() {
        use std::net::TcpListener;
        use std::thread;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        thread::spawn(move || {
            let (mut sock, _) = listener.accept().unwrap();
            let mut buf = [0u8; 2048];
            let _ = sock.read(&mut buf);
            let body = b"{\"ok\":true}";
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n",
                body.len()
            );
            sock.write_all(resp.as_bytes()).unwrap();
            sock.write_all(body).unwrap();
        });
        let resp = exchange(
            "POST",
            &format!("http://127.0.0.1:{port}/v1/test"),
            &[("Content-Type", "application/json")],
            b"{}",
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.json().unwrap()["ok"], true);
    }

    #[test]
    fn yaml_reads_kubeconfig_shape() {
        let text = r#"
apiVersion: v1
clusters:
- name: c
  cluster:
    server: https://127.0.0.1:6443
users:
- name: u
  user:
    token: tok-1
contexts:
- name: ctx
  context:
    cluster: c
    user: u
current-context: ctx
"#;
        let v = parse_yaml(text);
        assert_eq!(v["current-context"], "ctx");
        assert_eq!(v["clusters"][0]["cluster"]["server"], "https://127.0.0.1:6443");
        assert_eq!(v["users"][0]["user"]["token"], "tok-1");
        assert_eq!(v["contexts"][0]["context"]["cluster"], "c");
    }

    #[test]
    fn rate_limiter_spaces_calls() {
        let lim = RateLimiter::new(50.0);
        assert_eq!(lim.enter(), 0.0);
        let delay = lim.enter();
        assert!(delay > 0.0, "{delay}");
    }
}
