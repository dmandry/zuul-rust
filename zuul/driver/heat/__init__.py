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

import urllib.parse

from zuul.driver import Driver, ConnectionInterface, ProviderInterface
from zuul.driver.heat import (
    heatconnection,
    heatendpoint,
    heatmodel,
    heatprovider,
)
from zuul.provider import EndpointCacheMixin


class HeatDriver(Driver, EndpointCacheMixin,
                 ConnectionInterface, ProviderInterface):
    name = 'heat'
    _endpoint_class = heatendpoint.HeatProviderEndpoint

    def getConnection(self, name, config):
        return heatconnection.HeatConnection(self, name, config)

    def getProvider(self, zk_client, connection, tenant_name,
                    canonical_name, provider_config, system_id):
        return heatprovider.HeatProvider(
            self, zk_client, connection, tenant_name,
            canonical_name, provider_config, system_id)

    def getProviderClass(self):
        return heatprovider.HeatProvider

    def getProviderSchema(self):
        return heatprovider.HeatProviderSchema().getProviderSchema()

    def getProviderSchemaClass(self):
        return heatprovider.HeatProviderSchema

    def getProviderNodeClass(self):
        return heatmodel.HeatProviderNode

    def _getEndpoint(self, zk_client, connection, region, system_id):
        """Get or create a cached endpoint for the given region.

        Endpoints are cached by connection name and region to avoid
        creating multiple connections to the same region.

        :param zk_client: ZooKeeper client
        :param connection: HeatConnection instance
        :param region: OpenStack region name (or None)
        :param system_id: Unique system identifier
        :returns: HeatProviderEndpoint instance
        """
        region_str = region or ''
        endpoint_id = '/'.join([
            urllib.parse.quote_plus(connection.connection_name),
            urllib.parse.quote_plus(region_str),
        ])
        return self.getEndpointById(
            endpoint_id,
            create_args=(self, zk_client, connection, region, system_id))

    def getEndpoint(self, provider):
        """Get the endpoint for a provider.

        :param provider: HeatProvider instance
        :returns: HeatProviderEndpoint instance
        """
        return self._getEndpoint(
            provider.zk_client, provider.connection, provider.region,
            provider.system_id)

    def stop(self):
        self.stopEndpoints()
