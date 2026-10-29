#!/usr/bin/env python3
# Copyright 2026 Acme Gating, LLC
# SPDX-License-Identifier: Apache-2.0
"""Rewrite tests/unit/test_launcher.py into Rust suite cases.

Each test keeps its original statements. unittest assertions become
calls that record both sides; the Rust runner compares those values.
"""

import ast
import json
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = ROOT / "tests" / "unit" / "test_launcher.py"
CASES = ROOT / "rust" / "zuul-launcher" / "suite_data" / "cases.json"
NATIVE_TESTS = ROOT / "rust" / "zuul-launcher" / "tests" / "generated_native.rs"
BRIDGE_TESTS = ROOT / "rust" / "zuul-launcher-py" / "tests" / "generated_bridge.rs"

ASSERT_OPS = {
    "assertEqual": "eq",
    "assertNotEqual": "ne",
    "assertGreaterEqual": "ge",
    "assertLessEqual": "le",
    "assertGreater": "gt",
    "assertLess": "lt",
}


def _name(func):
    return ast.Name(id=func, ctx=ast.Load())


def _call(func, args):
    return ast.Call(func=_name(func), args=args, keywords=[])


def _const(value):
    return ast.Constant(value=value)


def _is_self_attr(node):
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "self"
    )


class Rewriter(ast.NodeTransformer):
    def visit_Expr(self, node):
        rewritten = self._rewrite_call(node.value)
        if rewritten is not None:
            return ast.Expr(value=rewritten)
        self.generic_visit(node)
        return node

    def visit_Assert(self, node):
        return ast.Expr(
            value=_call(
                "__suite_truth",
                [_const(ast.unparse(node.test)), node.test, _const(True)],
            )
        )

    def _rewrite_call(self, call):
        if not _is_self_attr(call):
            return None
        name = call.func.attr
        if name in ASSERT_OPS:
            if len(call.args) < 2:
                return None
            return _call(
                "__suite_cmp",
                [
                    _const(ASSERT_OPS[name]),
                    _const(ast.unparse(call.args[0])),
                    call.args[0],
                    _const(ast.unparse(call.args[1])),
                    call.args[1],
                ],
            )
        if name in ("assertTrue", "assertFalse") and call.args:
            return _call(
                "__suite_truth",
                [
                    _const(ast.unparse(call.args[0])),
                    call.args[0],
                    _const(name == "assertTrue"),
                ],
            )
        if name == "assertIsNone" and call.args:
            return _call(
                "__suite_cmp",
                [
                    _const("eq"),
                    _const(ast.unparse(call.args[0])),
                    call.args[0],
                    _const("None"),
                    _const(None),
                ],
            )
        if name == "assertIsNotNone" and call.args:
            return _call(
                "__suite_cmp",
                [
                    _const("ne"),
                    _const(ast.unparse(call.args[0])),
                    call.args[0],
                    _const("None"),
                    _const(None),
                ],
            )
        if name in ("assertIn", "assertNotIn") and len(call.args) >= 2:
            return _call(
                "__suite_in",
                [
                    _const(ast.unparse(call.args[0])),
                    call.args[0],
                    _const(ast.unparse(call.args[1])),
                    call.args[1],
                    _const(name == "assertIn"),
                ],
            )
        if name == "assertHistory" and call.args:
            ordered = _const(True)
            for kw in call.keywords:
                if kw.arg == "ordered":
                    ordered = kw.value
            return _call("__suite_history", [_name("self"), call.args[0], ordered])
        if name == "assertReportedStat":
            return ast.Call(
                func=_name("__suite_reported"),
                args=[_name("self"), *call.args],
                keywords=list(call.keywords),
            )
        if name.startswith("assert"):
            return _call("__suite_opaque", [_const(ast.unparse(call))])
        return None


def _patches(fn):
    found = []
    for deco in fn.decorator_list:
        text = ast.unparse(deco)
        if ".patch(" in text or text.startswith("patch("):
            found.append(text)
    return found


def _fn_name(class_name, method):
    raw = f"{class_name}_{method}".lower()
    return re.sub(r"[^0-9a-z_]", "_", raw)


def main():
    tree = ast.parse(SRC.read_text())
    cases = []
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        for item in node.body:
            if not isinstance(item, ast.FunctionDef) or not item.name.startswith("test_"):
                continue
            method = item.name
            patches = _patches(item)
            rewritten = Rewriter().visit(ast.fix_missing_locations(item))
            rewritten.name = "__case"
            rewritten.decorator_list = []
            ast.fix_missing_locations(rewritten)
            body = ast.unparse(rewritten)
            compile(body, f"{node.name}.{item.name}", "exec")
            cases.append(
                {
                    "class_name": node.name,
                    "method": method,
                    "body": body,
                    "patches": patches,
                    "rust_fn": _fn_name(node.name, method),
                }
            )
    if len(cases) != 104:
        raise SystemExit(f"expected 104 tests, rewrote {len(cases)}")
    CASES.parent.mkdir(parents=True, exist_ok=True)
    # rust_fn is for the generators only; the JSON the runner loads omits it
    # so the deserializer stays small. It is still unique.
    payload = [
        {
            "class_name": c["class_name"],
            "method": c["method"],
            "body": c["body"],
            "patches": c["patches"],
        }
        for c in cases
    ]
    CASES.write_text(json.dumps(payload, indent=1) + "\n")
    _emit_tests(NATIVE_TESTS, cases, "run")
    _emit_tests(BRIDGE_TESTS, cases, "run")
    print(f"wrote {len(cases)} cases")


def _emit_tests(path, cases, runner):
    lines = [
        "// Generated by tools/gen_launcher_suite.py. Do not edit.",
        "// Copyright 2026 Acme Gating, LLC",
        "// SPDX-License-Identifier: Apache-2.0",
        "",
    ]
    seen = set()
    for case in cases:
        name = case["rust_fn"]
        if name in seen:
            raise SystemExit(f"duplicate rust fn {name}")
        seen.add(name)
        lines.append("#[test]")
        lines.append(f"fn {name}() {{")
        lines.append(
            f'    {runner}("{case["class_name"]}", "{case["method"]}");'
        )
        lines.append("}")
        lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines))


if __name__ == "__main__":
    main()
