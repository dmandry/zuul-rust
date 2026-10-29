// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// ZKObject wire format from zuul/zk/zkobject.py and zuul/zk/sharding.py.
// JSON (sort_keys) then zlib.compress level 6. Readers accept an
// uncompressed blob when zlib fails. Polymorphic objects prefix
// `{driver_id}\0`. Shards are 1_000_000-byte slices.

use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use serde_json::Value;
use std::io::{Read, Write};

use crate::jsonutil;

pub const NODE_BYTE_SIZE_LIMIT: usize = 1_000_000;
pub const ZLIB_LEVEL: u32 = 6;

pub fn dumps_compressed(value: &Value) -> Vec<u8> {
    compress(&jsonutil::dumps_bytes(value))
}

pub fn compress(data: &[u8]) -> Vec<u8> {
    let mut enc = ZlibEncoder::new(Vec::new(), Compression::new(ZLIB_LEVEL));
    enc.write_all(data).expect("zlib encode");
    enc.finish().expect("zlib finish")
}

pub fn decompress(raw: &[u8]) -> Vec<u8> {
    let mut dec = ZlibDecoder::new(raw);
    let mut out = Vec::new();
    match dec.read_to_end(&mut out) {
        Ok(_) => out,
        Err(_) => raw.to_vec(),
    }
}

pub fn encode_polymorphic(driver_id: &str, value: &Value) -> Vec<u8> {
    let compressed = dumps_compressed(value);
    let mut out = Vec::with_capacity(driver_id.len() + 1 + compressed.len());
    out.extend_from_slice(driver_id.as_bytes());
    out.push(0);
    out.extend_from_slice(&compressed);
    out
}

pub fn decode_polymorphic(raw: &[u8]) -> Result<(String, Value), String> {
    let split = raw
        .iter()
        .position(|b| *b == 0)
        .ok_or_else(|| "polymorphic blob missing driver id".to_string())?;
    let driver = String::from_utf8(raw[..split].to_vec())
        .map_err(|e| format!("driver id: {e}"))?;
    let json_bytes = decompress(&raw[split + 1..]);
    let text = String::from_utf8(json_bytes).map_err(|e| format!("utf-8: {e}"))?;
    let value = jsonutil::loads(&text)?;
    Ok((driver, value))
}

pub fn decode_zkobject(raw: &[u8]) -> Result<Value, String> {
    let json_bytes = decompress(raw);
    let text = String::from_utf8(json_bytes).map_err(|e| format!("utf-8: {e}"))?;
    jsonutil::loads(&text)
}

pub fn shard(data: &[u8]) -> Vec<Vec<u8>> {
    if data.is_empty() {
        return vec![Vec::new()];
    }
    data.chunks(NODE_BYTE_SIZE_LIMIT)
        .map(|c| c.to_vec())
        .collect()
}

pub fn unshard(parts: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for part in parts {
        out.extend_from_slice(part);
    }
    out
}

/// Sequential child name ZooKeeper appends: 10 digits.
pub fn sequence_child(seq: i64) -> String {
    format!("{seq:010}")
}
