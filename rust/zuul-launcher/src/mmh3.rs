// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// MurmurHash3 x86 32-bit, seed 0, unsigned. Matches
// mmh3.hash(text, signed=False) used by scores_for_label, endpoint_score,
// and launcher_score.

const C1: u32 = 0xcc9e_2d51;
const C2: u32 = 0x1b87_3593;

pub fn hash32(data: &[u8], seed: u32) -> u32 {
    let mut h: u32 = seed;
    let mut chunks = data.chunks_exact(4);
    for chunk in chunks.by_ref() {
        let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(C1);
        k = k.rotate_left(15);
        k = k.wrapping_mul(C2);
        h ^= k;
        h = h.rotate_left(13);
        h = h.wrapping_mul(5).wrapping_add(0xe654_6b64);
    }
    let rest = chunks.remainder();
    if !rest.is_empty() {
        let mut k = 0u32;
        if rest.len() >= 3 {
            k ^= (rest[2] as u32) << 16;
        }
        if rest.len() >= 2 {
            k ^= (rest[1] as u32) << 8;
        }
        if !rest.is_empty() {
            k ^= rest[0] as u32;
        }
        k = k.wrapping_mul(C1);
        k = k.rotate_left(15);
        k = k.wrapping_mul(C2);
        h ^= k;
    }
    h ^= data.len() as u32;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^= h >> 16;
    h
}

pub fn hash_str(text: &str) -> u32 {
    hash32(text.as_bytes(), 0)
}

/// zuul/launcher/server.py scores_for_label
pub fn scores_for_label(label_cname: &str, candidate_names: &[String]) -> Vec<(u32, String)> {
    let mut scores: Vec<(u32, String)> = candidate_names
        .iter()
        .map(|n| (hash_str(&format!("{n}-{label_cname}")), n.clone()))
        .collect();
    scores.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    scores
}

/// zuul/zk/launcher.py launcher_score
pub fn launcher_score(name: &str, hash_key: &str) -> u32 {
    hash_str(&format!("{name}-{hash_key}"))
}

/// zuul/launcher/server.py endpoint_score
pub fn endpoint_score(name: &str, endpoint_cname: &str) -> u32 {
    hash_str(&format!("{name}-{endpoint_cname}"))
}
