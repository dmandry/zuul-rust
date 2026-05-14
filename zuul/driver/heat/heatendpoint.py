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

"""Heat Provider Endpoint for Zuul.

This module provides the endpoint implementation for the Heat driver,
managing connections to OpenStack Heat orchestration API and providing
stack lifecycle operations.
"""

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
from zuul.driver.util import LazyExecutorTTLCache, RateLimiter, Timer
from zuul.model import QuotaInformation
from zuul.provider import BaseProviderEndpoint


CACHE_TTL = 10  # seconds; matches the OpenStack driver


def quota_from_limits(compute, volume, network):
    """Convert OpenStack quota limits to Zuul QuotaInformation.

    Converts OpenStack API quota responses into Zuul's QuotaInformation
    format. Handles unlimited quotas (value -1) by converting to math.inf.

    :param compute: Nova compute quota/limits object
    :param volume: Cinder volume quota/limits dict or None
    :param network: Neutron network quota dict or None
    :returns: QuotaInformation instance
    """
    def bound_value(value):
        """Convert -1 (unlimited) to infinity, otherwise return value."""
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
        # Neutron quotas for Heat driver
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
    """Heat endpoint for managing Heat stacks in an OpenStack region.

    A Heat endpoint corresponds to a single OpenStack region and provides
    access to the Heat orchestration API for that region. It manages the
    OpenStack SDK connection, rate limiting, and provides methods for
    stack operations.

    The endpoint is long-lived and cached by the driver to avoid creating
    multiple connections to the same region.
    """

    # Resource types that contribute to quota tracking.  Keep in sync
    # with HeatTemplateParser resource type constants which uses the
    # same set for quota calculation from templates.
    TRACKED_RESOURCE_TYPES = {
        'OS::Nova::Server',
        'OS::Cinder::Volume',
        'OS::Neutron::Net',
        'OS::Neutron::Router',
        'OS::Neutron::RouterGateway',
        'OS::Neutron::FloatingIP',
    }

    def __init__(self, driver, zk_client, connection, region, system_id):
        """Initialize the Heat endpoint.

        :param driver: The Heat driver instance
        :param zk_client: ZooKeeper client for coordination
        :param connection: HeatConnection instance with cloud config
        :param region: OpenStack region name (or None for default)
        :param system_id: Unique system identifier for resource tagging
        """
        # Construct endpoint name from connection and region
        region_str = region or 'default'
        name = f'{connection.connection_name}-{region_str}'
        super().__init__(driver, zk_client, connection, name, system_id)

        self.log = logging.getLogger(f"zuul.heat.{self.name}")
        self.region = region
        self._client = None

        # Wrap with a per-instance LRU cache so that we don't leak
        # memory over time when the endpoint is occasionally replaced.
        self._listFlavors = functools.lru_cache(maxsize=None)(
            self._listFlavors)

        # Rate limiter to control API request frequency
        self.rate_limiter = RateLimiter(self.name, connection.rate)

        # Thread pool for async API operations
        self.api_executor = ThreadPoolExecutor(
            max_workers=10, thread_name_prefix=f'heat-{self.name}')

    def startEndpoint(self):
        """Start the endpoint and initialize OpenStack connection.

        Called when the endpoint is first used. Establishes connection
        to the OpenStack cloud using openstacksdk and prepares the Heat
        client for API operations.
        """
        self.log.debug("Starting Heat endpoint for region %s", self.region)
        # Initialize the OpenStack SDK connection
        self._client = self._getClient()

        # Wrap resource listing methods with TTL cache for efficiency.
        # listInstances() is called frequently by the cleanup worker;
        # caching avoids redundant API calls within the TTL window.
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
        """Stop the endpoint and clean up resources.

        Called when the endpoint is being shut down. Closes the OpenStack
        connection and releases any held resources.
        """
        self.log.debug("Stopping Heat endpoint for region %s", self.region)

        # Shutdown the API executor
        if self.api_executor:
            self.api_executor.shutdown(wait=True)

        # Close the OpenStack SDK connection
        if self._client:
            self._client.close()
            self._client = None

        self.log.info("Heat endpoint stopped for region %s", self.region)

    def _getRegionConfig(self):
        """Get OpenStack cloud configuration for this region.

        Uses openstacksdk to load cloud configuration from clouds.yaml
        (or environment variables) and select the appropriate region.

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

        Creates a new connection to the OpenStack cloud using openstacksdk.
        The connection provides access to all OpenStack services including
        Heat, Nova, Neutron, Cinder, and Glance.

        Note: The connection object holds internal resources that prevent
        garbage collection. We call close() in stopEndpoint() to release
        these resources when the endpoint is shut down.

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

        Users can omit the region in configuration, and openstacksdk may
        supply a default. This method returns the actual region being used
        regardless of what the user configured.

        :returns: str, the actual region name
        """
        if not self._client:
            return self.region or 'unknown'
        return self._client.config.config['region_name']

    def getEndpointLimits(self):
        """Get quota limits from OpenStack for this endpoint.

        Queries Nova (compute), Cinder (volume), and Neutron (network) for
        the current quota limits and converts them to Zuul's
        QuotaInformation format.

        This is used by the scheduler to determine available resources
        before launching new stacks.

        :returns: QuotaInformation with quota limits for this region
        """
        self.log.debug("Fetching quota limits for region %s", self.region)

        # Get compute limits (instances, cores, RAM)
        # Using same method as OpenStack driver for consistency
        compute_limits = self._client.get_compute_limits()

        # Get volume limits (volumes, volume storage)
        try:
            volume_limits = self._client.get_volume_limits()
        except openstack.exceptions.EndpointNotFound:
            self.log.warning("Volume endpoint not found, skipping volume "
                             "limits for region %s", self.region)
            volume_limits = None
        except (openstack.exceptions.SDKException,
                openstack.exceptions.HttpException) as e:
            self.log.warning("Unable to fetch volume limits: %s", e)
            volume_limits = None

        # Get network limits (networks, routers, floating IPs)
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
            with self.rate_limiter:
                return self._client.orchestration.get_stack(stack_id)
        except openstack.exceptions.ResourceNotFound:
            return None
        except openstack.exceptions.HttpException as e:
            # Transient server errors - let state machine retry
            if e.status_code >= 500:
                self.log.warning(
                    "Transient error getting stack %s (HTTP %s): %s",
                    stack_id, e.status_code, e)
                return None  # State machine will retry on next advance()
            raise

    def _deleteStack(self, stack_id):
        """Delete a Heat stack.

        :param stack_id: Stack ID
        """
        with Timer(self.log, f'API call delete_stack {stack_id}'):
            with self.rate_limiter:
                self._client.orchestration.delete_stack(stack_id)

    def _listStackResources(self, stack_id):
        """List physical resource IDs from a Heat stack.

        Queries the Heat API for all resources in the stack (with
        nested_depth=1 for future ResourceGroup support) and returns
        the physical_resource_id values for resource types that
        contribute to quota tracking.

        :param stack_id: Heat stack ID
        :returns: List of physical resource ID strings
        """
        with Timer(self.log,
                   f'API call list_stack_resources {stack_id}'):
            with self.rate_limiter:
                resources = self._client.orchestration.resources(
                    stack_id, nested_depth=1)
                return [
                    r.physical_resource_id
                    for r in resources
                    if r.resource_type in self.TRACKED_RESOURCE_TYPES
                ]

    def _getExternalNetwork(self):
        """Discover the external network for floating IPs.

        Uses openstacksdk's network discovery to find the first
        external IPv4 network available in Neutron.

        :returns: Network name (str)
        :raises: Exception if no external network is found
        """
        with Timer(self.log, 'API call get_external_networks'):
            with self.rate_limiter:
                networks = (
                    self._client.get_external_ipv4_floating_networks())
        if networks:
            return networks[0]['name']
        raise Exception(
            f"No external network found in endpoint {self.name}. "
            f"Configure 'external-network' on the label explicitly.")

    def _findFlavor(self, flavor_name, _min_ram=None):
        """Find a Nova flavor by name or ID.

        Used when parsing Heat templates to resolve flavor references for
        quota calculation.

        :param flavor_name: Flavor name or ID
        :param _min_ram: Reserved for parity with other drivers (unused).
        :returns: Flavor object from OpenStack SDK
        :raises openstack.exceptions.NotFoundException: If no flavor matches
        """
        with Timer(self.log, f'API call find_flavor {flavor_name}'):
            with self.rate_limiter:
                flavor = self._client.compute.find_flavor(flavor_name)
        if flavor is None:
            raise openstack.exceptions.NotFoundException(
                f"Flavor {flavor_name!r} not found in endpoint {self.name}")
        return flavor

    def _getInstance(self, stack, quota, outputs):
        """Convert a Heat stack to a HeatInstance.

        Extracts the necessary information from the stack and outputs to
        create a HeatInstance that Zuul can use to connect to the node.

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
        """Internal helper to list Zuul-managed stacks.

        Fetches stacks tagged with this system's ID and yields stack
        details with extracted metadata.

        :param skip_statuses: List of stack status strings to skip.
                              Defaults to empty list (no filtering).
        :returns: Generator of (stack, stack_details, metadata, outputs)
        """
        if skip_statuses is None:
            skip_statuses = []

        # Filter stacks by zuul_system_id tag for efficiency.
        tag_filter = f'zuul_system_id={self.system_id}'
        stacks = self._client.orchestration.stacks(
            tags=tag_filter)

        for stack in stacks:
            # Skip stacks with specified statuses
            if stack.status in skip_statuses:
                continue

            # Get stack details including outputs and tags
            stack_details = self._client.orchestration.get_stack(stack.id)
            outputs = {o['output_key']: o.get('output_value')
                       for o in stack_details.get('outputs', [])}

            # Extract metadata from tags
            tags = stack_details.get('tags', []) or []
            metadata = {}
            for tag in tags:
                if '=' in tag:
                    key, value = tag.split('=', 1)
                    metadata[key] = value

            yield (stack, stack_details, metadata, outputs)

    def _listServers(self):
        """List all Nova servers in the project.

        :returns: List of server dicts from the compute API
        """
        with Timer(self.log, 'API call list_servers'):
            with self.rate_limiter:
                return list(self._client.compute.servers(details=True))

    def _listVolumes(self):
        """List all Cinder volumes in the project.

        :returns: List of volume objects from the block_storage API
        """
        try:
            with Timer(self.log, 'API call list_volumes'):
                with self.rate_limiter:
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
            with self.rate_limiter:
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
            with self.rate_limiter:
                project_id = self._client.current_project_id
                return list(self._client.network.routers(
                    project_id=project_id))

    def _listFloatingIps(self):
        """List all Neutron floating IPs in the project.

        :returns: List of floating IP objects
        """
        with Timer(self.log, 'API call list_floating_ips'):
            with self.rate_limiter:
                project_id = self._client.current_project_id
                return list(self._client.network.ips(
                    project_id=project_id))

    # This method is wrapped with an LRU cache in the constructor.
    def _listFlavors(self):
        """List all Nova flavors and cache the result.

        Called once per endpoint lifetime; subsequent calls return the
        cached list.  Used by _getFlavorFromServer() to resolve flavor
        IDs without per-server API calls.

        :returns: List of flavor objects from compute API
        """
        with Timer(self.log, 'API call list_flavors'):
            with self.rate_limiter:
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

        Modern Nova / SDK versions embed full flavor details on the
        server object.  Older versions only include the flavor ID,
        requiring a lookup in the cached flavor list.

        :param server: Server object from compute API
        :returns: Object with vcpus and ram attributes
        """
        flavor = server.get('flavor', {})
        if hasattr(flavor, 'vcpus'):
            return flavor
        return self._findFlavorByName(flavor['id'])

    def _metadataForResource(self, resource_id):
        """Return Zuul metadata dict for a cloud resource.

        If the resource UUID is in self.managed_resource_ids (populated
        by the cleanup worker from HeatProviderNode data in ZK), return
        metadata that will cause the cleanup worker to skip this resource
        (it's already accounted for via node.quota in ZK).

        Otherwise return empty metadata -- the resource is unmanaged and
        its quota will be counted as unmanaged usage.

        :param resource_id: UUID of the cloud resource
        :returns: dict with zuul_system_id + zuul_node_uuid, or empty dict
        """
        managed = getattr(self, 'managed_resource_ids', {})
        node_uuid = managed.get(resource_id)
        if node_uuid:
            return {
                'zuul_system_id': self.system_id,
                'zuul_node_uuid': node_uuid,
            }
        return {}

    def listInstances(self):
        """List all cloud resources in this endpoint for quota tracking.

        Yields instances representing each quota-consuming resource in
        the OpenStack project:

        1. Heat stacks (Zuul-tagged) -- contributes stacks=1
        2. Nova servers -- contributes instances, cores, ram, volumes
        3. Cinder volumes (standalone, not attached) -- volumes, volume-gb
        4. Neutron networks (project-owned) -- networks=1
        5. Neutron routers -- routers=1 (+ routers-with-external-gw)
        6. Neutron floating IPs -- floating-ips=1

        For stacks, metadata comes from the stack's own Zuul tags.
        For cloud resources, metadata is set based on whether the
        resource UUID appears in self.managed_resource_ids (populated
        by the cleanup worker from HeatProviderNode data in ZK).

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
        """List all Heat resources for leak detection.

        Iterates over all Heat stacks and yields HeatResource objects
        for cleanup. This method is used by the launcher's cleanup worker
        to identify and remove leaked resources.

        Only lists stacks tagged with this Zuul system's ID for efficiency.

        :param providers: List of providers that share this endpoint
        :returns: Generator yielding HeatResource objects
        """
        self.log.debug("Listing Heat resources for cleanup in region %s",
                       self.region)
        # Skip deleted and deleting stacks (no action needed)
        skip = [heatstatemachine.STACK_DELETE_COMPLETE,
                heatstatemachine.STACK_DELETE_IN_PROGRESS]
        for stack, details, metadata, outputs in self._listZuulStacks(skip):
            yield HeatResource(
                metadata,
                HeatResource.TYPE_STACK,
                stack.id
            )

    def deleteResource(self, resource):
        """Delete a Heat resource.

        Deletes a Heat stack identified by the resource. This is called
        by the cleanup worker when a leaked resource is detected.

        :param resource: HeatResource object to delete
        """
        self.log.info(f"Deleting leaked {resource.type}: {resource.id}")
        if resource.type == HeatResource.TYPE_STACK:
            self._deleteStack(resource.id)
