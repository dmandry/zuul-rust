// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// Python json.dumps(..., sort_keys=True) and json.loads.

use serde_json::{Map, Number, Value};

/// Encode `value` with CPython json.dumps defaults plus sort_keys=True:
/// separators (', ', ': '), ensure_ascii, and Python float formatting.
pub fn dumps(value: &Value) -> String {
    let mut out = String::new();
    write_value(&mut out, value);
    out
}

pub fn dumps_bytes(value: &Value) -> Vec<u8> {
    dumps(value).into_bytes()
}

pub fn loads(text: &str) -> Result<Value, String> {
    serde_json::from_str(text).map_err(|e| e.to_string())
}

fn write_value(out: &mut String, value: &Value) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(n) => write_number(out, n),
        Value::String(s) => write_string(out, s),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_value(out, item);
            }
            out.push(']');
        }
        Value::Object(map) => write_object(out, map),
    }
}

fn write_object(out: &mut String, map: &Map<String, Value>) {
    let mut keys: Vec<&String> = map.keys().collect();
    keys.sort();
    out.push('{');
    for (i, key) in keys.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        write_string(out, key);
        out.push_str(": ");
        write_value(out, &map[*key]);
    }
    out.push('}');
}

fn write_number(out: &mut String, n: &Number) {
    if let Some(i) = n.as_i64() {
        out.push_str(&i.to_string());
        return;
    }
    if let Some(u) = n.as_u64() {
        out.push_str(&u.to_string());
        return;
    }
    if let Some(f) = n.as_f64() {
        out.push_str(&format_float(f));
    }
}

/// CPython's json encoder uses the same shortest round-trip as repr(),
/// with a sign on the exponent (`1e+16`).
pub fn format_float(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f.is_sign_positive() {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        };
    }
    let mut buf = ryu::Buffer::new();
    let s = buf.format_finite(f).to_string();
    if let Some(e) = s.find(['e', 'E']) {
        let (mant, exp) = s.split_at(e);
        let exp = &exp[1..];
        let exp = if exp.starts_with('+') || exp.starts_with('-') {
            exp.to_string()
        } else {
            format!("+{exp}")
        };
        format!("{mant}e{exp}")
    } else if !s.contains('.') {
        // Whole numbers produced by ryu without a decimal (rare) still
        // need a fractional part when they came from a float. ryu always
        // emits a decimal or an exponent for finite non-integers; integers
        // that were stored as f64 (1.0) are formatted as "1.0".
        format!("{s}.0")
    } else {
        s
    }
}

fn write_string(out: &mut String, s: &str) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c if (c as u32) > 0x7f => {
                let cp = c as u32;
                if cp <= 0xffff {
                    out.push_str(&format!("\\u{cp:04x}"));
                } else {
                    let v = cp - 0x10000;
                    let hi = 0xD800 + (v >> 10);
                    let lo = 0xDC00 + (v & 0x3ff);
                    out.push_str(&format!("\\u{hi:04x}\\u{lo:04x}"));
                }
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Python 3 round(x, ndigits). Scales by an exact power of ten and applies
/// round-half-even to the binary value, which is what makes round(0.05, 1)
/// == 0.1 while round(0.25, 1) == 0.2.
pub fn python_round_1(x: f64) -> f64 {
    python_round_ndigits(x, 1)
}

pub fn python_round_ndigits(x: f64, ndigits: i32) -> f64 {
    if !x.is_finite() || ndigits < 0 || ndigits > 18 {
        return x;
    }
    let neg = x.is_sign_negative();
    let x = x.abs();
    if x == 0.0 {
        return if neg { -0.0 } else { 0.0 };
    }
    let bits = x.to_bits();
    let raw_exp = ((bits >> 52) & 0x7ff) as i32;
    if raw_exp == 0 {
        return if neg { -0.0 } else { 0.0 };
    }
    let mant = (bits & ((1u64 << 52) - 1)) | (1u64 << 52);
    let exp = raw_exp - 1023 - 52;
    let mut scale = 1u128;
    for _ in 0..ndigits {
        scale *= 10;
    }
    let mut n = (mant as u128).saturating_mul(scale);
    if exp >= 0 {
        n <<= exp as u32;
        let value = (n as f64) / (scale as f64);
        return if neg { -value } else { value };
    }
    let shift = (-exp) as u32;
    if shift >= 127 {
        return if neg { -0.0 } else { 0.0 };
    }
    let q = n >> shift;
    let rem = n & ((1u128 << shift) - 1);
    let half = 1u128 << (shift - 1);
    let rounded = if rem > half {
        q + 1
    } else if rem < half {
        q
    } else if q % 2 == 0 {
        q
    } else {
        q + 1
    };
    let value = (rounded as f64) / (scale as f64);
    if neg { -value } else { value }
}

pub fn object(entries: Vec<(&str, Value)>) -> Value {
    let mut map = Map::new();
    for (k, v) in entries {
        map.insert(k.to_string(), v);
    }
    Value::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorts_and_spaces() {
        let v = serde_json::json!({"b": 1, "a": true, "c": null});
        assert_eq!(dumps(&v), r#"{"a": true, "b": 1, "c": null}"#);
    }
}
