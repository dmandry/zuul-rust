# Copyright 2021 Acme Gating, LLC
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

from zuul.exceptions import VariableNameError


VARNAME_RE = re.compile(r'^[A-Za-z0-9_]+$')

RESERVED_NAMES = {
    'zuul',
    'zuul_node',
    'nodepool',
    'unsafe_vars',
    'zuul_user_dir',
}


def check_varnames(var):
    for varname in var.keys():
        if not VARNAME_RE.match(varname):
            raise VariableNameError(
                "Variable names may only contain letters, "
                "numbers, and underscores")
    # We block RESERVED_NAMES in configloader, but block it here too
    # to make sure that a job doesn't pass reserved variables.
    reserved_vars = list(RESERVED_NAMES) + [
        # Block some connection related variables so they cannot be
        # overridden by jobs to bypass security mechanisms.
        'ansible_connection',
        'ansible_host',
        'ansible_python_interpreter',
        'ansible_shell_executable',
        'ansible_user',
    ]
    for reserved_var in reserved_vars:
        if reserved_var in var:
            raise VariableNameError(
                f"Defining a variable named '{reserved_var}' is not allowed")
