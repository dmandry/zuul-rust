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

import logging
import math
from concurrent.futures import ThreadPoolExecutor

import openstack

from zuul.driver.heat.heatmodel import HeatInstance, HeatResource
from zuul.driver.heat import heatstatemachine
from zuul.driver.util import RateLimiter, Timer
from zuul.model import QuotaInformation
from zuul.provider import BaseProviderEndpoint


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
        args['floating_ips'] = bound_value(
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

    def listInstances(self):
        """List all Heat stacks in this endpoint.

        Iterates over all stacks in the OpenStack region managed by this
        endpoint and yields HeatInstance objects. This is used by the
        launcher for quota calculation and resource tracking.

        Only lists stacks tagged with this Zuul system's ID for efficiency.

        :returns: Generator yielding HeatInstance objects
        """
        self.log.debug("Listing Heat stacks for region %s", self.region)
        # Only skip fully deleted stacks for quota calculation
        # Stacks that are deleting or failed deletion still consume resources
        skip = [heatstatemachine.STACK_DELETE_COMPLETE]
        for stack, details, metadata, outputs in self._listZuulStacks(skip):
            # For quota calculation, use minimal quota
            quota = QuotaInformation(stacks=1)

            # Use _getInstance to create and validate the instance
            instance = self._getInstance(stack, quota, outputs)
            instance.metadata = metadata

            yield instance

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
