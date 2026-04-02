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

import json
import logging

from zuul.connection import BaseConnection


class AzureConnection(BaseConnection):
    driver_name = 'azure'
    log = logging.getLogger("zuul.AzureConnection")

    def __init__(self, driver, connection_name, connection_config):
        super().__init__(driver, connection_name, connection_config)

        # Users can provide credentials directly in zuul.conf, a
        # credentials file, or a federated token file.  We will check
        # those places in that order.
        self.shared_credentials_file = self.connection_config.get(
            'shared_credentials_file')
        self.federated_token_file = self.connection_config.get(
            'federated_token_file')

        credential = {}
        if self.shared_credentials_file:
            with open(self.shared_credentials_file, encoding="utf-8") as f:
                credential = json.load(f)

        self.tenant_id = self.connection_config.get(
            'tenant_id', credential.get('tenantId'))
        self.client_id = self.connection_config.get(
            'client_id', credential.get('clientId'))
        self.client_secret = self.connection_config.get(
            'client_secret', credential.get('clientSecret'))
        self.subscription_id = self.connection_config.get(
            'subscription_id', credential.get('subscriptionId'))

        # Rate limit: requests/second
        self.rate = float(self.connection_config.get('rate', 2))
