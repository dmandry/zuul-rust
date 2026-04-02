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

import logging
import os

from zuul.connection import BaseConnection


class OpenstackConnection(BaseConnection):
    driver_name = 'openstack'
    log = logging.getLogger("zuul.OpenstackConnection")

    def __init__(self, driver, connection_name, connection_config):
        super().__init__(driver, connection_name, connection_config)

        # Allow the user to specify the clouds.yaml via the config
        # setting or the environment variable.  Otherwise we will use
        # the client lib default paths.
        self.config_files = self.connection_config.get(
            'client_config_file',
            os.getenv('OS_CLIENT_CONFIG_FILE', None))

        if 'cloud' not in self.connection_config:
            raise Exception('The "cloud" parameter is required for '
                            f'OpenStack connections in {self.connection_name}')
        self.cloud_name = self.connection_config.get('cloud')

        # Rate limit: requests/second
        self.rate = float(self.connection_config.get('rate', 2))
