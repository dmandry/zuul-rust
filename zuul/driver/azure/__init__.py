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

import urllib

from zuul.driver import Driver, ConnectionInterface, ProviderInterface
from zuul.driver.azure import (
    azureconnection,
    azuremodel,
    azureprovider,
    azureendpoint,
)
from zuul.provider import EndpointCacheMixin


class AzureDriver(Driver, EndpointCacheMixin,
                  ConnectionInterface, ProviderInterface):
    name = 'azure'
    _endpoint_class = azureendpoint.AzureProviderEndpoint

    def getConnection(self, name, config):
        return azureconnection.AzureConnection(self, name, config)

    def getProvider(self, zk_client, connection, tenant_name,
                    canonical_name, provider_config, system_id):
        return azureprovider.AzureProvider(
            self, zk_client, connection, tenant_name,
            canonical_name, provider_config, system_id)

    def getProviderClass(self):
        return azureprovider.AzureProvider

    def getProviderSchema(self, internal=False):
        return azureprovider.AzureProviderSchema().getProviderSchema(internal)

    def getProviderSchemaClass(self):
        return azureprovider.AzureProviderSchema

    def getProviderNodeClass(self):
        return azuremodel.AzureProviderNode

    def getEndpoint(self, provider):
        # An azure endpoint is a simply a region on the connection.
        endpoint_id = '/'.join([
            urllib.parse.quote_plus(provider.connection.connection_name),
            urllib.parse.quote_plus(provider.region),
        ])
        return self.getEndpointById(
            endpoint_id,
            create_args=(self, provider.zk_client, provider.connection,
                         provider.region, provider.system_id))

    def stop(self):
        self.stopEndpoints()
