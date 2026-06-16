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

# Utilities for cgroups2

import pathlib


# This is to facilitate unit testing
CGROUP_ROOT = '/sys/fs/cgroup'
CGROUP_SELF = '/proc/self/cgroup'


def get_cgroup():
    """Return the current cgroup"""
    with open(CGROUP_SELF) as f:
        cgroup = f.read().strip()
        if cgroup[:3] != "0::":
            return None
    return f'{CGROUP_ROOT}{cgroup[3:]}'


def walk_cgroups_tree():
    """A generator that walks up the cgroups tree of the current
    process

    """
    cgroup = get_cgroup()
    if cgroup is None:
        return None

    path = pathlib.Path(cgroup)
    while len(str(path)) >= len(CGROUP_ROOT):
        yield path
        path = path.parent
