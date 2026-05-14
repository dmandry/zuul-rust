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

"""Heat Template Parser for Zuul.

This module provides functionality to parse Heat Orchestration Template
(HOT) YAML files and extract resource information for quota tracking
and resource policy validation.
"""

import logging
import re
import yaml

import openstack.exceptions

from zuul.model import QuotaInformation


log = logging.getLogger("zuul.heat.parser")


class ResourcePolicyViolation(Exception):
    """Exception raised when a template violates resource policies."""
    pass


class FlavorLimitExceeded(Exception):
    """Exception raised when template resources exceed flavor limits."""
    pass


class HeatTemplateParser:
    """Parser for Heat Orchestration Templates (HOT).

    Parses Heat templates to count resources and calculate quota
    requirements. This enables the scheduler to make informed decisions
    about whether a stack can be created within available quotas.

    Resource types counted:
    - OS::Nova::Server (instances, cores, RAM via flavor lookup)
    - OS::Cinder::Volume (volumes, volume-gb)
    - OS::Neutron::Net (networks)
    - OS::Neutron::Router (routers)
    - OS::Neutron::RouterGateway (routers-with-external-gw)
    - OS::Neutron::FloatingIP (floating-ips)
    """

    # Heat resource types we track
    NOVA_SERVER = 'OS::Nova::Server'
    CINDER_VOLUME = 'OS::Cinder::Volume'
    NEUTRON_NET = 'OS::Neutron::Net'
    NEUTRON_ROUTER = 'OS::Neutron::Router'
    NEUTRON_ROUTER_GATEWAY = 'OS::Neutron::RouterGateway'
    NEUTRON_FLOATING_IP = 'OS::Neutron::FloatingIP'

    def __init__(self, endpoint, resource_policy=None):
        """Initialize the template parser.

        :param endpoint: HeatProviderEndpoint for flavor lookups
        :param resource_policy: Optional dict with allowed resource
                                types for policy validation
        """
        self.endpoint = endpoint
        self.resource_policy = resource_policy or {}

    def validatePolicy(self, template_content):
        """Validate a template against resource policy only.

        This performs policy validation without counting resources or
        looking up flavors.  Used by getCreateStateMachine() to reject
        templates early, before submitting the stack to Heat.

        :param template_content: Heat template YAML string
        :raises: ResourcePolicyViolation if template violates policy
        :raises: ValueError if template is not valid YAML
        """
        if not self.resource_policy:
            return

        try:
            template = yaml.safe_load(template_content)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid Heat template YAML: {e}") from e

        if not isinstance(template, dict):
            raise ValueError("Heat template must be a YAML dict")

        resources = template.get('resources', {})
        if not isinstance(resources, dict):
            resources = {}

        self._validateResourcePolicy(resources)

    def parse(self, template_content, validate_policy=True):
        """Parse a Heat template and calculate resource requirements.

        :param template_content: Heat template YAML string
        :param validate_policy: Whether to validate resource policy
        :returns: QuotaInformation with resource counts
        :raises: ResourcePolicyViolation if template violates policy
        """
        try:
            template = yaml.safe_load(template_content)
        except yaml.YAMLError as e:
            log.error("Failed to parse Heat template: %s", e)
            raise ValueError(f"Invalid Heat template YAML: {e}") from e

        if not isinstance(template, dict):
            raise ValueError("Heat template must be a YAML dict")

        resources = template.get('resources', {})
        if not isinstance(resources, dict):
            log.warning("Heat template 'resources' is not a dict")
            resources = {}

        # Validate resource policy if enabled
        if validate_policy and self.resource_policy:
            self._validateResourcePolicy(resources)

        quota = self._countResources(resources, template)

        return quota

    def _countResources(self, resources, template):
        """Count resources in the template.

        :param resources: Dict of resources from template
        :param template: Full template dict (for parameter resolution)
        :returns: QuotaInformation with resource counts
        """
        counts = {
            'stacks': 1,
            'instances': 0,
            'cores': 0,
            'ram': 0,
            'volumes': 0,
            'volume-gb': 0,
            'networks': 0,
            'routers': 0,
            'routers-with-external-gw': 0,
            'floating-ips': 0,
        }

        for resource_name, resource_def in resources.items():
            if not isinstance(resource_def, dict):
                continue

            resource_type = resource_def.get('type')
            if not resource_type:
                continue

            # Count instances and lookup flavor for cores/RAM
            if resource_type == self.NOVA_SERVER:
                counts['instances'] += 1
                cores, ram = self._getFlavorResources(
                    resource_def, template)
                counts['cores'] += cores
                counts['ram'] += ram

                # Check for block_device_mapping_v2 (boot-from-volume)
                # This creates Cinder volumes as part of server creation
                block_devices = self._getBlockDevices(resource_def)
                if block_devices:
                    for bd in block_devices:
                        volume_size = bd.get('volume_size', 0)
                        if volume_size:
                            counts['volumes'] += 1
                            counts['volume-gb'] += volume_size

            # Count volumes
            elif resource_type == self.CINDER_VOLUME:
                counts['volumes'] += 1
                size = self._getVolumeSize(resource_def, template)
                counts['volume-gb'] += size

            # Count networks
            elif resource_type == self.NEUTRON_NET:
                counts['networks'] += 1

            # Count routers
            elif resource_type == self.NEUTRON_ROUTER:
                counts['routers'] += 1
                # Router external gateways consume an external network port
                # but are NOT Neutron floatingip quota allocations.  Track
                # them separately so floating-ips aligns with Neutron quota.
                if self._hasExternalGateway(resource_def, template):
                    counts['routers-with-external-gw'] += 1

            # RouterGateway attaches external network to router
            elif resource_type == self.NEUTRON_ROUTER_GATEWAY:
                counts['routers-with-external-gw'] += 1

            # Count floating IPs (actual OS::Neutron::FloatingIP resources)
            elif resource_type == self.NEUTRON_FLOATING_IP:
                counts['floating-ips'] += 1

        log.debug("Template resource counts: %s", counts)
        return QuotaInformation(**counts)

    def _getFlavorResources(self, resource_def, template):
        """Get cores and RAM from a Nova server resource.

        Looks up the flavor specified in the resource and returns its
        cores and RAM. If the flavor cannot be determined, returns
        default estimates.

        :param resource_def: Resource definition dict
        :param template: Full template dict
        :returns: Tuple of (cores, ram_mb)
        """
        properties = resource_def.get('properties', {})
        flavor_ref = properties.get('flavor')

        if not flavor_ref:
            log.warning("Nova server missing flavor, using defaults")
            return (1, 1024)

        # Resolve flavor name (may be a parameter reference)
        flavor_name = self._resolveValue(flavor_ref, template)

        if not flavor_name or not isinstance(flavor_name, str):
            log.warning("Unable to resolve flavor %s, using defaults",
                        flavor_ref)
            return (1, 1024)

        # Look up flavor in OpenStack
        try:
            flavor = self.endpoint._findFlavor(flavor_name)
            return (flavor.vcpus, flavor.ram)
        except (openstack.exceptions.NotFoundException,
                openstack.exceptions.SDKException) as e:
            log.warning("Unable to find flavor %s: %s, using defaults",
                        flavor_name, e)
            return (1, 1024)

    def _getVolumeSize(self, resource_def, template):
        """Get size from a Cinder volume resource.

        :param resource_def: Resource definition dict
        :param template: Full template dict
        :returns: Volume size in GB (int)
        """
        properties = resource_def.get('properties', {})
        size_ref = properties.get('size')

        if not size_ref:
            log.warning("Volume missing size, using default 1GB")
            return 1

        size = self._resolveValue(size_ref, template)

        if isinstance(size, int):
            return size
        elif isinstance(size, str) and size.isdigit():
            return int(size)
        else:
            log.warning("Unable to resolve volume size %s, using default",
                        size_ref)
            return 1

    def _hasExternalGateway(self, resource_def, template):
        """Check if a router has an external gateway interface.

        :param resource_def: Router resource definition dict
        :param template: Full template dict
        :returns: True if router has external_gateway_info, False otherwise
        """
        properties = resource_def.get('properties', {})
        return 'external_gateway_info' in properties

    def _getBlockDevices(self, resource_def):
        """Get block device mappings from a Nova server resource.

        Nova servers can use block_device_mapping_v2 to create volumes
        as part of instance creation (boot-from-volume pattern). This
        method extracts those mappings for volume quota counting.

        :param resource_def: Server resource definition dict
        :returns: List of block device dicts, or empty list
        """
        properties = resource_def.get('properties', {})
        block_devices = properties.get('block_device_mapping_v2', [])

        if not isinstance(block_devices, list):
            log.warning("block_device_mapping_v2 is not a list")
            return []

        return block_devices

    def _resolveValue(self, value, template):
        """Resolve a value that may be a parameter reference.

        Heat templates use { get_param: param_name } to reference
        parameters. This method attempts basic resolution.

        :param value: Value to resolve (may be dict, str, int, etc)
        :param template: Full template dict
        :returns: Resolved value, or original if not resolvable
        """
        if isinstance(value, dict):
            # Check for { get_param: name }
            if 'get_param' in value:
                param_name = value['get_param']
                parameters = template.get('parameters', {})
                param_def = parameters.get(param_name, {})
                # Return default value if available
                return param_def.get('default', value)

        return value

    def _validateResourcePolicy(self, resources):
        """Validate template resources against resource policy.

        Checks if resources comply with allowed resource types.
        If allowed_resource_types is configured, every resource type
        in the template must match at least one pattern. Use negative
        lookahead patterns for deny semantics (e.g.
        ``^(?!OS::Heat::ResourceGroup$).*``).

        Policy patterns are treated as Python regular expressions,
        allowing flexible matching. For example:
        - '^OS::(Nova|Neutron|Cinder)::.*' allows compute/network/storage
        - '^(?!OS::Heat::ResourceGroup$).*' allows everything but one type
        - 'OS::Nova::Server' matches exactly

        :param resources: Dict of resources from template
        :raises: ResourcePolicyViolation if policy is violated
        """
        allowed = self.resource_policy.get('allowed_resource_types', [])

        if not allowed:
            log.debug("No allowed-resource-types configured, skipping")
            return

        allowed_patterns = [(p, re.compile(p)) for p in allowed]

        for resource_name, resource_def in resources.items():
            if not isinstance(resource_def, dict):
                continue

            resource_type = resource_def.get('type')
            if not resource_type:
                continue

            matched = False
            for pattern_str, pattern_re in allowed_patterns:
                if pattern_re.fullmatch(resource_type):
                    matched = True
                    break
            if not matched:
                raise ResourcePolicyViolation(
                    f"Resource '{resource_name}' type '{resource_type}' "
                    f"does not match any allowed resource type pattern")

        log.debug("Template passed resource policy validation")

    def _findFlavor(self, flavor_name):
        """Find a flavor by name in OpenStack.

        This is a convenience wrapper around the endpoint's
        _findFlavor method.

        :param flavor_name: Flavor name or ID
        :returns: Flavor object with vcpus and ram attributes
        """
        return self.endpoint._findFlavor(flavor_name)
