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

import voluptuous as vs

from zuul import model
from zuul.driver import Driver, ConnectionInterface, ProviderInterface
from zuul.driver.heat import (
    heatconnection,
    heatendpoint,
    heatmodel,
    heatprovider,
)
from zuul.driver.heat.exceptions import (
    HeatTemplateNotFoundError,
    HeatTemplateSecurityError,
)
from zuul.exceptions import TemplatePolicyViolation
from zuul.driver.heat import heatutils
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

    def getProviderSchema(self, internal=False):
        return heatprovider.HeatProviderSchema().getProviderSchema(internal)

    def getProviderSchemaClass(self):
        return heatprovider.HeatProviderSchema

    def getProviderNodeClass(self):
        return heatmodel.HeatProviderNode

    def getTemplateConfigSchema(self):
        return {
            'heat-template-path': str,
            'heat-parameters': dict,
        }

    def getProjectConfigKeys(self):
        return {
            'heat-template-dirs': vs.Any([str], str),
        }

    def getFileDiscoveryDirs(self, tenant_project_config):
        return list(
            tenant_project_config.provider_template_dirs.get(
                'heat-template-dirs', ()))

    def resolveTemplateConfig(self, node, nodeset, parsed_config,
                              files_cache, source_context):
        if node.template_config.get('heat_template_content'):
            return
        path = node.template_config.get('heat_template_path')
        if not path:
            return
        content = None
        if hasattr(files_cache, 'getFile'):
            content = files_cache.getFile(
                source_context.project_connection_name,
                source_context.project_name,
                source_context.branch, path)
        else:
            try:
                content = files_cache[path]
            except KeyError:
                pass
        if not content:
            exc = HeatTemplateNotFoundError(path, nodeset.name)
            parsed_config.nodeset_errors.append(
                model.ConfigurationError(
                    source_context, None, str(exc),
                    name=exc.zuul_error_name))
            return
        try:
            heatutils.validateSecurity(content)
        except (TemplatePolicyViolation, ValueError) as e:
            exc = HeatTemplateSecurityError(path, nodeset.name, e)
            parsed_config.nodeset_errors.append(
                model.ConfigurationError(
                    source_context, None, str(exc),
                    name=exc.zuul_error_name))
        else:
            node.template_config['heat_template_content'] = content

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
