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

"""Heat Provider Endpoint for Zuul."""

import functools
import logging
import math
from concurrent.futures import ThreadPoolExecutor

import openstack

from zuul.driver.heat.heatmodel import (
    HeatInstance,
    HeatQuotaInstance,
    HeatResource,
)
from zuul.driver.heat import heatstatemachine
from zuul.driver.heat.exceptions import FlavorNotFoundError
from zuul.driver.heat.heatutils import TRACKED_RESOURCE_TYPES
from zuul.driver.util import LazyExecutorTTLCache, RateLimiter, Timer
from zuul.model import QuotaInformation
from zuul.provider import BaseProviderEndpoint


CACHE_TTL = 10  # seconds; matches the OpenStack driver


def quota_from_limits(compute, volume, network):
    """Convert OpenStack quota limits to Zuul QuotaInformation.

    Handles unlimited quotas (value -1) by converting to math.inf.

    :param compute: Nova compute quota/limits object
    :param volume: Cinder volume quota/limits dict or None
    :param network: Neutron network quota dict or None
    :returns: QuotaInformation instance
    """
    def bound_value(value):
        if value == -1:
            return math.inf
        return value

    args = dict(
        instances=bound_value(compute.max_total_instances),
        cores=bound_value(compute.max_total_cores),
        ram=bound_value(compute.max_total_ram_size))

    if volume is not None:
        args['volumes'] = bound_value(
            volume['absolute']['maxTotalVolumes'])
        args['volume-gb'] = bound_value(
            volume['absolute']['maxTotalVolumeGigabytes'])

    if network is not None:
        args['networks'] = bound_value(
            network.get('network', -1))
        args['routers'] = bound_value(
            network.get('router', -1))
        args['floating-ips'] = bound_value(
            network.get('floatingip', -1))
        args['ports'] = bound_value(
            network.get('port', -1))

    return QuotaInformation(**args)


class HeatProviderEndpoint(BaseProviderEndpoint):
    """Heat endpoint for managing Heat stacks in an OpenStack region."""

    def __init__(self, driver, zk_client, connection, region, system_id):
        region_str = region or 'default'
        name = f'{connection.connection_name}-{region_str}'
        super().__init__(driver, zk_client, connection, name, system_id)

        self.log = logging.getLogger(f"zuul.heat.{self.name}")
        self.region = region
        self._client = None
        self.managed_resource_ids = {}

        self._listFlavors = functools.lru_cache(maxsize=None)(
            self._listFlavors)

        self.rate_limiter = RateLimiter(self.name, connection.rate)
        # Non-mutating requests at 10x the mutating rate.
        self.non_mutating_rate_limiter = RateLimiter(
            self.name, connection.rate * 10.0)

        self.api_executor = ThreadPoolExecutor(
            max_workers=10, thread_name_prefix=f'heat-{self.name}')

    def startEndpoint(self):
        """Start the endpoint and initialize OpenStack connection."""
        self.log.debug("Starting Heat endpoint for region %s", self.region)
        self._client = self._getClient()

        self._listServers = LazyExecutorTTLCache(
            CACHE_TTL, self.api_executor)(self._listServers)
        self._listVolumes = LazyExecutorTTLCache(
            CACHE_TTL, self.api_executor)(self._listVolumes)
        self._listNetworks = LazyExecutorTTLCache(
            CACHE_TTL, self.api_executor)(self._listNetworks)
        self._listRouters = LazyExecutorTTLCache(
            CACHE_TTL, self.api_executor)(self._listRouters)
        self._listFloatingIps = LazyExecutorTTLCache(
            CACHE_TTL, self.api_executor)(self._listFloatingIps)

        self.log.info("Heat endpoint started for region %s (actual: %s)",
                      self.region, self.getRegionName())

    def stopEndpoint(self):
        """Stop the endpoint and release resources."""
        self.log.debug("Stopping Heat endpoint for region %s", self.region)

        if self.api_executor:
            self.api_executor.shutdown(wait=True)

        if self._client:
            self._client.close()
            self._client = None

        self.log.info("Heat endpoint stopped for region %s", self.region)

    def _getRegionConfig(self):
        """Load cloud configuration for this region from clouds.yaml.

        :returns: OpenStackConfig instance for this cloud/region
        """
        config = openstack.config.OpenStackConfig(
            config_files=self.connection.config_files,
            load_envvars=False,
            app_name='zuul',
        )
        return config.get_one(
            cloud=self.connection.cloud_name,
            region_name=self.region)

    def _getClient(self):
        """Create and return an OpenStack SDK connection.

        :returns: openstack.connection.Connection instance
        """
        region = self._getRegionConfig()
        return openstack.connection.Connection(
            config=region,
            use_direct_get=False,
            rate_limit=self.connection.rate,
        )

    def getRegionName(self):
        """Get the actual region name in use.

        :returns: str, region name
        """
        return self._client.config.config['region_name']

    def getEndpointLimits(self):
        """Query Nova, Cinder, and Neutron for quota limits.

        :returns: QuotaInformation with quota limits for this region
        """
        self.log.debug("Fetching quota limits for region %s", self.region)

        compute_limits = self._client.get_compute_limits()

        try:
            volume_limits = self._client.get_volume_limits()
        except (openstack.exceptions.SDKException,
                openstack.exceptions.HttpException) as e:
            self.log.warning("Unable to fetch volume limits: %s", e)
            volume_limits = None

        try:
            network_limits = self._client.network.get_quota(
                self._client.current_project_id)
        except (openstack.exceptions.SDKException,
                openstack.exceptions.HttpException) as e:
            self.log.warning("Unable to fetch network limits: %s", e)
            network_limits = None

        quota = quota_from_limits(compute_limits, volume_limits,
                                  network_limits)
        self.log.debug("Region %s quota limits: %s", self.region, quota)
        return quota

    def refreshQuotaLimits(self, update):
        """Query the endpoint for quota limits and store in cache.

        :param bool update: Whether an update should be performed even
            if there are values present.
        :returns: Whether the cache was updated
        :rtype: bool
        """
        if self.quota_cache.hasLimits() and not update:
            return False
        limits = self.getEndpointLimits()
        self.quota_cache.setLimits(limits)
        return True

    def _submitApi(self, api, *args, **kw):
        """Submit an API call to the thread pool executor.

        API calls are executed asynchronously in a thread pool to avoid
        blocking the main launcher thread.

        :param api: API method to call
        :param args: Positional arguments to pass to the method
        :param kw: Keyword arguments to pass to the method
        :returns: Future object
        """
        return self.api_executor.submit(api, *args, **kw)

    def _completeApi(self, future):
        """Check if an API call has completed and return the result.

        :param future: Future object from _submitApi
        :returns: API call result, or None if not yet complete
        """
        if not future.done():
            return None
        return future.result()

    def _createStack(self, stack_name, template, parameters, tags):
        """Create a Heat stack.

        :param stack_name: Name for the stack
        :param template: Heat template YAML content (string)
        :param parameters: Dict of template parameters
        :param tags: Dict of metadata tags
        :returns: Stack object
        """
        with Timer(self.log, f'API call create_stack {stack_name}'):
            with self.rate_limiter:
                return self._client.orchestration.create_stack(
                    name=stack_name,
                    template=template,
                    parameters=parameters,
                    tags=','.join(tags) if tags else None,
                )

    def _getStack(self, stack_id):
        """Get a Heat stack by ID.

        :param stack_id: Stack ID
        :returns: Stack object or None if not found
        """
        try:
            with self.non_mutating_rate_limiter:
                return self._client.orchestration.get_stack(stack_id)
        except openstack.exceptions.ResourceNotFound:
            return None
        except openstack.exceptions.HttpException as e:
            # Transient server errors - let state machine retry
            if e.status_code >= 500:
                self.log.warning(
                    "Transient error getting stack %s (HTTP %s): %s",
                    stack_id, e.status_code, e)
                return None
            raise

    def _deleteStack(self, stack_id):
        """Delete a Heat stack.

        :param stack_id: Stack ID
        """
        with Timer(self.log, f'API call delete_stack {stack_id}'):
            with self.rate_limiter:
                self._client.orchestration.delete_stack(stack_id)
        return True

    def _listStackResources(self, stack_id):
        """List physical resource IDs that contribute to quota tracking.

        :param stack_id: Heat stack ID
        :returns: List of physical resource ID strings
        """
        with Timer(self.log, f'API call list_stack_resources {stack_id}'):
            with self.non_mutating_rate_limiter:
                resources = self._client.orchestration.resources(
                    stack_id, nested_depth=1)
                return [
                    r.physical_resource_id
                    for r in resources
                    if r.resource_type in TRACKED_RESOURCE_TYPES
                ]

    def _getExternalNetwork(self):
        """Discover the external network for floating IPs.

        :returns: Network name (str)
        :raises Exception: if no external network is found
        """
        with Timer(self.log, 'API call get_external_networks'):
            with self.non_mutating_rate_limiter:
                networks = (
                    self._client.get_external_ipv4_floating_networks())
        if networks:
            return networks[0]['name']
        raise Exception(
            f"No external network found in endpoint {self.name}. "
            f"Configure 'external-network' on the label explicitly.")

    def _findFlavor(self, flavor_name, _min_ram=None):
        """Find a Nova flavor by name or ID.

        :param flavor_name: Flavor name or ID
        :param _min_ram: Unused; for parity with other drivers.
        :returns: Flavor object from OpenStack SDK
        :raises FlavorNotFoundError: If no flavor matches
        """
        with Timer(self.log, f'API call find_flavor {flavor_name}'):
            with self.non_mutating_rate_limiter:
                flavor = self._client.compute.find_flavor(flavor_name)
        if flavor is None:
            raise FlavorNotFoundError(f"Unable to find flavor: {flavor_name}")
        return flavor

    def _getInstance(self, stack, quota, outputs):
        """Convert a Heat stack to a HeatInstance.

        :param stack: Stack object from OpenStack SDK
        :param quota: QuotaInformation for this stack
        :param outputs: Dict of stack outputs
        :returns: HeatInstance
        """
        bastion_ip = outputs.get('bastion_ip')

        return HeatInstance(
            cloud=self.connection.cloud_name,
            region=self.region,
            stack_id=stack.id,
            bastion_ip=bastion_ip,
            outputs=outputs,
            quota=quota,
        )

    def _listZuulStacks(self, skip_statuses=None):
        """List Zuul-managed stacks with extracted metadata.

        :param skip_statuses: Stack status strings to skip
        :returns: Generator of (stack, stack_details, metadata, outputs)
        """
        if skip_statuses is None:
            skip_statuses = []

        tag_filter = f'zuul_system_id={self.system_id}'
        stacks = self._client.orchestration.stacks(
            tags=tag_filter)

        for stack in stacks:
            if stack.status in skip_statuses:
                continue

            stack_details = self._client.orchestration.get_stack(stack.id)
            outputs = {o['output_key']: o.get('output_value')
                       for o in stack_details.get('outputs', [])}

            tags = stack_details.get('tags') or []
            metadata = dict(tag.split('=', 1) for tag in tags if '=' in tag)

            yield (stack, stack_details, metadata, outputs)

    def _listServers(self):
        """List all Nova servers in the project.

        :returns: List of server dicts from the compute API
        """
        with Timer(self.log, 'API call list_servers'):
            with self.non_mutating_rate_limiter:
                return list(self._client.compute.servers(details=True))

    def _listVolumes(self):
        """List all Cinder volumes in the project.

        :returns: List of volume objects from the block_storage API
        """
        try:
            with Timer(self.log, 'API call list_volumes'):
                with self.non_mutating_rate_limiter:
                    return list(self._client.block_storage.volumes(
                        details=True))
        except openstack.exceptions.EndpointNotFound:
            return []

    def _listNetworks(self):
        """List project-owned Neutron networks.

        Excludes external and shared provider networks which are not
        counted against the project's network quota.

        :returns: List of network objects
        """
        with Timer(self.log, 'API call list_networks'):
            with self.non_mutating_rate_limiter:
                project_id = self._client.current_project_id
                return list(self._client.network.networks(
                    project_id=project_id,
                    is_router_external=False,
                    is_shared=False,
                ))

    def _listRouters(self):
        """List all Neutron routers in the project.

        :returns: List of router objects
        """
        with Timer(self.log, 'API call list_routers'):
            with self.non_mutating_rate_limiter:
                project_id = self._client.current_project_id
                return list(self._client.network.routers(
                    project_id=project_id))

    def _listFloatingIps(self):
        """List all Neutron floating IPs in the project.

        :returns: List of floating IP objects
        """
        with Timer(self.log, 'API call list_floating_ips'):
            with self.non_mutating_rate_limiter:
                project_id = self._client.current_project_id
                return list(self._client.network.ips(
                    project_id=project_id))

    def _listFlavors(self):
        """List all Nova flavors (LRU-cached in __init__).

        :returns: List of flavor objects from compute API
        """
        with Timer(self.log, 'API call list_flavors'):
            with self.non_mutating_rate_limiter:
                return list(self._client.compute.flavors(details=True))

    def _findFlavorByName(self, flavor_name):
        """Find a flavor by name or ID from the cached flavor list.

        :param flavor_name: Flavor name or UUID
        :returns: Flavor object
        :raises Exception: If no flavor matches
        """
        for f in self._listFlavors():
            if flavor_name in (f['name'], f['id']):
                return f
        raise Exception("Unable to find flavor: %s" % flavor_name)

    def _getFlavorFromServer(self, server):
        """Extract flavor details from a server object.

        Falls back to cached flavor list lookup if the server object
        only contains the flavor ID (older Nova / SDK versions).

        :param server: Server object from compute API
        :returns: Object with vcpus and ram attributes
        """
        flavor = server.get('flavor', {})
        if hasattr(flavor, 'vcpus'):
            return flavor
        return self._findFlavorByName(flavor['id'])

    def _metadataForResource(self, resource_id):
        """Return Zuul metadata dict for a cloud resource.

        Returns metadata linking the resource to a node if the UUID is
        in self.managed_resource_ids, otherwise returns empty dict
        (unmanaged resource).

        :param resource_id: UUID of the cloud resource
        :returns: dict with zuul_system_id + zuul_node_uuid, or empty dict
        """
        node_uuid = self.managed_resource_ids.get(resource_id)
        if node_uuid is None:
            return {}
        return {
            'zuul_system_id': self.system_id,
            'zuul_node_uuid': node_uuid,
        }

    def listInstances(self):
        """List all cloud resources in this endpoint for quota tracking.

        :returns: Generator yielding Instance objects with quota
        """
        self.log.debug("Listing resources for quota in region %s", self.region)

        # 1. Heat stacks -- only Zuul-tagged stacks contribute stacks=1
        skip = [heatstatemachine.STACK_DELETE_COMPLETE]
        for stack, details, metadata, outputs in self._listZuulStacks(skip):
            quota = QuotaInformation(stacks=1)
            instance = self._getInstance(stack, quota, outputs)
            instance.metadata = metadata
            yield instance

        # 2. Nova servers -- instances, cores, ram (+ attached volumes)
        attached_volume_ids = set()
        for server in self._listServers():
            if server['status'].lower() == 'deleted':
                continue
            flavor = self._getFlavorFromServer(server)
            args = dict(instances=1, cores=flavor.vcpus, ram=flavor.ram)

            vol_attachments = server.get(
                'os-extended-volumes:volumes_attached') or []
            if vol_attachments:
                args['volumes'] = len(vol_attachments)
                for va in vol_attachments:
                    attached_volume_ids.add(va['id'])

            quota = QuotaInformation(**args)
            metadata = self._metadataForResource(server.id)
            yield HeatQuotaInstance(server.id, 'server', metadata, quota)

        # 3. Cinder volumes (standalone -- not already counted above)
        for volume in self._listVolumes():
            if volume.id in attached_volume_ids:
                continue
            quota = QuotaInformation(volumes=1, **{'volume-gb': volume.size})
            metadata = self._metadataForResource(volume.id)
            yield HeatQuotaInstance(volume.id, 'volume', metadata, quota)

        # 4. Neutron networks (project-owned, non-external, non-shared)
        for network in self._listNetworks():
            quota = QuotaInformation(networks=1)
            metadata = self._metadataForResource(network.id)
            yield HeatQuotaInstance(network.id, 'network', metadata, quota)

        # 5. Neutron routers
        for router in self._listRouters():
            args = dict(routers=1)
            if router.external_gateway_info:
                args['routers-with-external-gw'] = 1
            quota = QuotaInformation(**args)
            metadata = self._metadataForResource(router.id)
            yield HeatQuotaInstance(router.id, 'router', metadata, quota)

        # 6. Neutron floating IPs
        for fip in self._listFloatingIps():
            quota = QuotaInformation(**{'floating-ips': 1})
            metadata = self._metadataForResource(fip.id)
            yield HeatQuotaInstance(fip.id, 'floatingip', metadata, quota)

    def listResources(self, providers):
        """List Heat stacks for leak detection and cleanup.

        :param providers: List of providers that share this endpoint
        :returns: Generator yielding HeatResource objects
        """
        self.log.debug("Listing Heat resources for cleanup in region %s",
                       self.region)
        skip = [heatstatemachine.STACK_DELETE_COMPLETE,
                heatstatemachine.STACK_DELETE_IN_PROGRESS]
        for stack, details, metadata, outputs in self._listZuulStacks(skip):
            yield HeatResource(
                metadata,
                HeatResource.TYPE_STACK,
                stack.id
            )

    def deleteResource(self, resource):
        """Delete a leaked Heat resource.

        :param resource: HeatResource object to delete
        """
        self.log.info(f"Deleting leaked {resource.type}: {resource.id}")
        if resource.type == HeatResource.TYPE_STACK:
            self._deleteStack(resource.id)
