# Copyright 2026 Red Hat, Inc.
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
from zuul.exceptions import ConfigurationError


# Default rate limit to avoid overwhelming Heat API
# This prevents API throttling and ensures fair resource sharing
DEFAULT_RATE_LIMIT = 2  # requests per second


class HeatConnection(BaseConnection):
    driver_name = 'heat'
    log = logging.getLogger("zuul.HeatConnection")

    def __init__(self, driver, connection_name, connection_config):
        super().__init__(driver, connection_name, connection_config)

        # Validate required parameters
        if 'cloud' not in self.connection_config:
            raise ConfigurationError(
                f'The "cloud" parameter is required for Heat connection '
                f'"{self.connection_name}"')

        # Store cloud name for OpenStack SDK authentication
        self.cloud_name = self.connection_config.get('cloud')

        # Optional region name
        self.region_name = self.connection_config.get('region', None)

        # Allow the user to specify the clouds.yaml via the config
        # setting or the environment variable. Otherwise we will use
        # the client lib default paths.
        self.config_files = self.connection_config.get(
            'client_config_file',
            os.getenv('OS_CLIENT_CONFIG_FILE', None))

        # Rate limit: requests/second
        self.rate = self.connection_config.get('rate', DEFAULT_RATE_LIMIT)
