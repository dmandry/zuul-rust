#!/usr/bin/env python3
# Copyright 2026 Acme Gating, LLC
# SPDX-License-Identifier: Apache-2.0
"""JSON-line fixture server for the native launcher suite.

The Rust-under-test process does not link Python. This process owns the
Zuul test fixtures only.
"""

import os
import sys

# Keep the protocol on a private fd. Zuul and moto log to stdout.
_JSON_OUT = os.fdopen(os.dup(1), "w", buffering=1)
os.dup2(2, 1)
sys.stdout = sys.stderr

os.environ.pop("ZUUL_LAUNCHER_IMPL", None)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tests.launcher_suite_support import serve  # noqa: E402


if __name__ == "__main__":
    serve(_JSON_OUT)
