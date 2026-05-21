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

import configparser
import logging
import os

from zuul.connection import BaseConnection


class AwsConnection(BaseConnection):
    driver_name = 'aws'
    log = logging.getLogger("zuul.AwsConnection")

    def __init__(self, driver, connection_name, connection_config):
        super().__init__(driver, connection_name, connection_config)

        # Users can provide credentials directly in zuul.conf, or in the
        # AWS shared credentials file.  We will check those places in
        # that order.
        self.shared_credentials_file = self.connection_config.get(
            'shared_credentials_file')

        self.access_key_id = self.connection_config.get(
            'access_key_id')
        self.secret_access_key = self.connection_config.get(
            'secret_access_key')
        self.role_arn = self.connection_config.get(
            'role_arn')
        self.web_identity_token_file = self.connection_config.get(
            'web_identity_token_file')
        self.endpoint_url = self.connection_config.get(
            'endpoint_url')

        self.profile = self.connection_config.get('profile')
        # Rate limit: requests/second
        self.rate = self.connection_config.get('rate', 2)

        if self.shared_credentials_file:
            path = os.path.expanduser(self.shared_credentials_file)
            config = configparser.ConfigParser()
            config.read(path)

            section = config[self.profile]
            self.access_key_id = section.get(
                'access_key_id', self.access_key_id)
            self.secret_access_key = section.get(
                'secret_access_key', self.secret_access_key)
            self.role_arn = section.get(
                'role_arn', self.role_arn)
            self.web_identity_token_file = section.get(
                'web_identity_token_file', self.web_identity_token_file)
            self.endpoint_url = section.get(
                'endpoint_url', self.endpoint_url)
