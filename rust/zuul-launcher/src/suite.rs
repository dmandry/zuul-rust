// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// Port of tests/unit/test_launcher.py. The Python source of each test is
// rewritten so assertions record both sides; Rust compares them. The same
// bodies run against the native launcher and, through PyO3, the Python one.

use serde::Deserialize;
use std::sync::OnceLock;

#[derive(Clone, Debug, Deserialize)]
pub struct Case {
    pub class_name: String,
    pub method: String,
    pub body: String,
    pub patches: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Check {
    pub op: String,
    pub left_src: String,
    pub left: String,
    pub right_src: String,
    pub right: String,
}

pub fn cases() -> &'static [Case] {
    static CASES: OnceLock<Vec<Case>> = OnceLock::new();
    CASES
        .get_or_init(|| {
            serde_json::from_str(include_str!("../suite_data/cases.json"))
                .expect("launcher suite cases")
        })
        .as_slice()
}

pub fn find_case(class_name: &str, method: &str) -> &'static Case {
    cases()
        .iter()
        .find(|case| case.class_name == class_name && case.method == method)
        .unwrap_or_else(|| panic!("no launcher suite case {class_name}.{method}"))
}

fn numbers_equal(left: &str, right: &str) -> bool {
    matches!(
        (left, right),
        ("true", "1") | ("1", "true") | ("false", "0") | ("0", "false")
    )
}

fn parse_num(text: &str) -> Option<f64> {
    match text {
        "inf" | "Infinity" => Some(f64::INFINITY),
        "-inf" | "-Infinity" => Some(f64::NEG_INFINITY),
        "nan" => Some(f64::NAN),
        other => other.parse().ok(),
    }
}

pub fn verify(checks: &[Check]) {
    for (index, check) in checks.iter().enumerate() {
        let msg = format!(
            "check {index} {} ({}) vs {} ({})",
            check.left_src, check.left, check.right_src, check.right
        );
        match check.op.as_str() {
            "eq" => {
                assert!(
                    check.left == check.right || numbers_equal(&check.left, &check.right),
                    "{msg}"
                );
            }
            "ne" => {
                assert!(
                    check.left != check.right && !numbers_equal(&check.left, &check.right),
                    "{msg}"
                );
            }
            "ge" | "le" | "gt" | "lt" => {
                let left = parse_num(&check.left).unwrap_or_else(|| panic!("not a number: {msg}"));
                let right = parse_num(&check.right).unwrap_or_else(|| panic!("not a number: {msg}"));
                let ok = match check.op.as_str() {
                    "ge" => left >= right,
                    "le" => left <= right,
                    "gt" => left > right,
                    "lt" => left < right,
                    _ => false,
                };
                assert!(ok, "{msg}");
            }
            other => panic!("unknown check op {other}: {msg}"),
        }
    }
}

/// Rust assertions first. A fixture exception after a passing check is still
/// a failure (the scenario raised, or an assertion kind we re-raise).
pub fn finish(checks: &[Check], error: Option<&str>) {
    verify(checks);
    if let Some(err) = error {
        if !err.is_empty() {
            panic!("fixture error:\n{err}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suite_lists_every_launcher_test() {
        assert_eq!(cases().len(), 104);
        let noop = find_case("TestLauncher", "test_noop_job");
        assert!(noop.body.contains("__suite_cmp"));
        assert!(noop.body.contains("assertHistory") == false);
        assert!(noop.body.contains("__suite_history"));
    }
}
