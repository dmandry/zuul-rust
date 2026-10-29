// Copyright 2026 Acme Gating, LLC
// SPDX-License-Identifier: Apache-2.0
//
// Native launcher suite. The Rust launcher runs in this process. Python is a
// separate fixture process (ZooKeeper chroot, gerrit, moto server) and is not
// linked into the launcher.

#[path = "harness/mod.rs"]
mod harness;

use harness::run;

include!("generated_native.rs");
