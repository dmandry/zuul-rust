# Copyright 2024-2025 Acme Gating, LLC
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

import logging
import os

from zuul.connection import BaseConnection
from zuul.lib.config import any_to_bool


class KubernetesConnection(BaseConnection):
    driver_name = 'kubernetes'
    log = logging.getLogger("zuul.KubernetesConnection")

    def __init__(self, driver, connection_name, connection_config):
        super().__init__(driver, connection_name, connection_config)

        # Allow the user to specify the kubeconfig via the config
        # setting or the environment variable.  Otherwise we will use
        # the client lib default paths.
        self.config_file = self.connection_config.get(
            'kubeconfig_file',
            os.getenv('KUBECONFIG', None))

        self.context = self.connection_config.get('context')

        # Rate limit: requests/second
        self.rate = self.connection_config.get('rate', 2)
        # Use openshift projects instead of namespaces
        self.use_openshift_projects = any_to_bool(self.connection_config.get(
            'use_openshift_projects', False))
        # Use provider name as namespace for pods (a preserving namespace)
        self.use_provider_namespace = any_to_bool(self.connection_config.get(
            'use_provider_namespace', False))
