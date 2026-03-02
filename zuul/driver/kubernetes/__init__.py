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
from zuul.driver.kubernetes import (
    kubernetesconnection,
    kubernetesmodel,
    kubernetesprovider,
    kubernetesendpoint,
)
from zuul.provider import EndpointCacheMixin


class KubernetesDriver(Driver, EndpointCacheMixin,
                       ConnectionInterface, ProviderInterface):
    name = 'kubernetes'
    _endpoint_class = kubernetesendpoint.KubernetesProviderEndpoint

    def getConnection(self, name, config):
        return kubernetesconnection.KubernetesConnection(self, name, config)

    def getProvider(self, zk_client, connection, tenant_name,
                    canonical_name, provider_config, system_id):
        return kubernetesprovider.KubernetesProvider(
            self, zk_client, connection, tenant_name,
            canonical_name, provider_config, system_id)

    def getProviderClass(self):
        return kubernetesprovider.KubernetesProvider

    def getProviderSchema(self, internal=False):
        return kubernetesprovider.KubernetesProviderSchema().\
            getProviderSchema(internal)

    def getProviderSchemaClass(self):
        return kubernetesprovider.KubernetesProviderSchema

    def getProviderNodeClass(self):
        return kubernetesmodel.KubernetesProviderNode

    def _getEndpoint(self, zk_client, connection, system_id):
        endpoint_id = urllib.parse.quote_plus(connection.connection_name)
        return self.getEndpointById(
            endpoint_id,
            create_args=(self, zk_client, connection, system_id))

    def getEndpoint(self, provider):
        return self._getEndpoint(
            provider.zk_client, provider.connection,
            provider.system_id)

    def stop(self):
        self.stopEndpoints()
