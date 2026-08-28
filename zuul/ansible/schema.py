# Copyright 2018-2019 Red Hat, Inc.
# Copyright 2024 Acme Gating, LLC
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

import voluptuous as vs


artifact = {
    vs.Required('name'): str,
    vs.Required('url'): str,
    'metadata': dict,
}

artifact_data = {
    'zuul': {
        'log_url': str,
        'artifacts': [artifact],
        vs.Extra: object,
    },
    vs.Extra: object,
}

warning_data = {
    'zuul': {
        'log_url': str,
        'warnings': [str],
        vs.Extra: object,
    },
    vs.Extra: object,
}

artifact_schema = vs.Schema(artifact_data)
warning_schema = vs.Schema(warning_data)
