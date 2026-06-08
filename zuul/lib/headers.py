# Copyright 2018 Red Hat, Inc.
# Copyright 2026 Acme Gating, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import re


DEPENDS_ON_RE = re.compile(r"^Depends-On:(.*?)\s*$",
                           re.MULTILINE | re.IGNORECASE)
MERGE_MODE_RE = re.compile(r"^Merge-Mode:(.*?)\s*$",
                           re.MULTILINE | re.IGNORECASE)


def find_dependency_headers(message):
    # Search for Depends-On headers
    dependencies = []
    for m in DEPENDS_ON_RE.findall(message):
        m = m.strip()
        if m in dependencies:
            continue
        dependencies.append(m)
    return dependencies


def find_merge_mode_headers(message):
    modes = []
    for m in MERGE_MODE_RE.findall(message):
        m = m.strip()
        if m in modes:
            continue
        modes.append(m)
    return modes
