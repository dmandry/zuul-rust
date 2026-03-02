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

import urllib

from zuul.driver import Driver, ConnectionInterface, ProviderInterface
from zuul.driver.openstack import (
    openstackconnection,
    openstackmodel,
    openstackprovider,
    openstackendpoint,
)
from zuul.provider import EndpointCacheMixin


class OpenstackDriver(Driver, EndpointCacheMixin,
                      ConnectionInterface, ProviderInterface):
    name = 'openstack'
    _endpoint_class = openstackendpoint.OpenstackProviderEndpoint

    def getConnection(self, name, config):
        return openstackconnection.OpenstackConnection(self, name, config)

    def getProvider(self, zk_client, connection, tenant_name,
                    canonical_name, provider_config, system_id):
        return openstackprovider.OpenstackProvider(
            self, zk_client, connection, tenant_name,
            canonical_name, provider_config, system_id)

    def getProviderClass(self):
        return openstackprovider.OpenstackProvider

    def getProviderSchema(self, internal=False):
        return openstackprovider.OpenstackProviderSchema().getProviderSchema(
            internal)

    def getProviderSchemaClass(self):
        return openstackprovider.OpenstackProviderSchema

    def getProviderNodeClass(self):
        return openstackmodel.OpenstackProviderNode

    def _getEndpoint(self, zk_client, connection, region, system_id):
        region_str = region or ''
        endpoint_id = '/'.join([
            urllib.parse.quote_plus(connection.connection_name),
            urllib.parse.quote_plus(region_str),
        ])
        return self.getEndpointById(
            endpoint_id,
            create_args=(self, zk_client, connection, region, system_id))

    def getEndpoint(self, provider):
        return self._getEndpoint(
            provider.zk_client, provider.connection, provider.region,
            provider.system_id)

    def stop(self):
        self.stopEndpoints()
