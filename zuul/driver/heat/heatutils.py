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

"""Heat template parsing, policy validation, and quota estimation."""

import logging
import re
import yaml

import openstack

from zuul.exceptions import TemplatePolicyViolation
from zuul.model import QuotaInformation


log = logging.getLogger("zuul.heat.parser")


UNSUPPORTED_TYPES = {
    'OS::Heat::ResourceGroup': (
        "OS::Heat::ResourceGroup is not supported. The quota parser "
        "cannot expand resource groups. Define resources explicitly "
        "in the template instead."),
    'OS::Heat::AutoScalingGroup': (
        "OS::Heat::AutoScalingGroup is not supported. Autoscaling "
        "creates and deletes resources dynamically at runtime, making "
        "quota tracking impossible. Define resources explicitly "
        "in the template instead."),
    'OS::Heat::Stack': (
        "OS::Heat::Stack (remote stacks) is not supported. The driver "
        "does not pass environment or files required for nested/remote "
        "stack creation."),
}

FORBIDDEN_DELETION_POLICIES = frozenset({'retain', 'snapshot'})

# HOT intrinsic functions recognized by Heat.
# https://docs.openstack.org/heat/latest/template_guide/hot_spec.html
KNOWN_INTRINSIC_FUNCTIONS = frozenset({
    'get_param', 'get_resource', 'get_attr', 'get_file',
    'str_replace', 'str_replace_strict', 'str_split',
    'list_join', 'list_concat',
    'map_merge', 'map_replace',
    'digest', 'repeat', 'resource_facade',
    'if', 'contains', 'equals', 'not', 'and', 'or',
    'filter', 'make_url', 'yaql',
})


def _load_template_yaml(template_content):
    """Load and validate Heat template YAML.

    :param template_content: Heat template YAML string
    :returns: Parsed template as dict
    :raises: ValueError if template is not valid YAML or not a dict
    """
    try:
        template = yaml.safe_load(template_content)
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid Heat template YAML: {e}") from e

    if not isinstance(template, dict):
        raise ValueError("Heat template must be a YAML dict")

    return template


def _get_resources(template):
    """Extract resources section from template.

    :param template: Parsed Heat template dict
    :returns: Resources dict (empty dict if invalid or missing)
    """
    resources = template.get('resources', {})
    if not isinstance(resources, dict):
        log.warning("Heat template 'resources' is not a dict")
        resources = {}
    return resources


def _findForbiddenKey(obj, key, message):
    """Recursively search a nested structure for a forbidden dict key.

    :param obj: Object to search (dict, list, or scalar)
    :param key: Dict key to reject
    :param message: Error message if key is found
    :raises: TemplatePolicyViolation if key is found
    """
    if isinstance(obj, dict):
        if key in obj:
            raise TemplatePolicyViolation(message)
        for value in obj.values():
            _findForbiddenKey(value, key, message)
    elif isinstance(obj, list):
        for item in obj:
            _findForbiddenKey(item, key, message)


def _validateUnsupportedTypes(resources):
    """Reject resources whose types Zuul cannot safely manage.

    :param resources: Dict of resources from template
    :raises: TemplatePolicyViolation if an unsupported type is found
    """
    for resource_name, resource_def in resources.items():
        if not isinstance(resource_def, dict):
            continue
        resource_type = resource_def.get('type')
        if resource_type in UNSUPPORTED_TYPES:
            raise TemplatePolicyViolation(
                f"Resource '{resource_name}': "
                f"{UNSUPPORTED_TYPES[resource_type]}")


def _validateExternalId(resources):
    """Reject resources that reference pre-existing external resources.

    :param resources: Dict of resources from template
    :raises: TemplatePolicyViolation if any resource uses external_id
    """
    for resource_name, resource_def in resources.items():
        if not isinstance(resource_def, dict):
            continue
        if 'external_id' in resource_def:
            raise TemplatePolicyViolation(
                f"Resource '{resource_name}' uses 'external_id'. "
                f"External resources are not allowed because Zuul "
                f"cannot manage their lifecycle.")


def _validateDeletionPolicy(resources):
    """Reject resources with non-Delete deletion policies.

    :param resources: Dict of resources from template
    :raises: TemplatePolicyViolation if forbidden policy is found
    """
    for resource_name, resource_def in resources.items():
        if not isinstance(resource_def, dict):
            continue
        policy = resource_def.get('deletion_policy')
        if not policy:
            continue
        if str(policy).lower() in FORBIDDEN_DELETION_POLICIES:
            raise TemplatePolicyViolation(
                f"Resource '{resource_name}' has deletion_policy "
                f"'{policy}'. Only 'Delete' is allowed because Zuul "
                f"must be able to clean up all resources.")


def _validateGetFile(template):
    """Reject templates that use the ``get_file`` intrinsic function.

    :param template: Parsed Heat template dict
    :raises: TemplatePolicyViolation if get_file is found
    """
    _findForbiddenKey(
        template, 'get_file',
        "Template uses 'get_file' which is not supported. The Heat "
        "driver does not pass files required for get_file "
        "resolution. Inline the file content in the template "
        "instead.")


def validateSecurity(template_or_content, resources=None):
    """Validate a Heat template's security.

    Runs unconditional safety checks that protect Zuul's resource
    lifecycle guarantees.  These checks do not require provider
    config (endpoint, template-policy) so they can run early,
    before a job is scheduled.

    :param template_or_content: Parsed template dict, or YAML string
    :param resources: Pre-extracted resources dict (required if
                      template_or_content is a dict)
    :raises: TemplatePolicyViolation if any check fails
    :raises: ValueError if template is not valid YAML
    """
    if isinstance(template_or_content, str):
        template = _load_template_yaml(template_or_content)
        resources = _get_resources(template)
    else:
        template = template_or_content
    _validateUnsupportedTypes(resources)
    _validateExternalId(resources)
    _validateDeletionPolicy(resources)
    _validateGetFile(template)


def _collectIntrinsicFunctions(obj, found):
    """Recursively collect intrinsic function names from a template.

    :param obj: Object to search (dict, list, or scalar)
    :param found: Set to add discovered function names to
    """
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in KNOWN_INTRINSIC_FUNCTIONS:
                found.add(key)
            _collectIntrinsicFunctions(value, found)
    elif isinstance(obj, list):
        for item in obj:
            _collectIntrinsicFunctions(item, found)


def _validateAllowedResourceTypes(template_policy, resources):
    """Validate resources against allowed-resource-types policy.

    :param template_policy: Dict with template policy configuration
    :param resources: Dict of resources from template
    :raises: TemplatePolicyViolation if a resource type is not allowed
    """
    allowed = template_policy.get('allowed_resource_types', [])

    if not allowed:
        log.debug("No allowed-resource-types configured, skipping")
        return

    allowed_patterns = [re.compile(p) for p in allowed]

    for resource_name, resource_def in resources.items():
        if not isinstance(resource_def, dict):
            continue

        resource_type = resource_def.get('type')
        if not resource_type:
            continue

        matched = False
        for pattern_re in allowed_patterns:
            if pattern_re.fullmatch(resource_type):
                matched = True
                break
        if not matched:
            raise TemplatePolicyViolation(
                f"Resource '{resource_name}' type '{resource_type}' "
                f"does not match any allowed resource type pattern")


def _validateIntrinsicFunctions(template_policy, template):
    """Reject intrinsic functions not matching allowed-intrinsic-functions

    An empty or absent allowed list means no restriction.

    :param template_policy: Dict with template policy configuration
    :param template: Parsed Heat template dict
    :raises: TemplatePolicyViolation if a disallowed function is found
    """
    allowed = template_policy.get(
        'allowed_intrinsic_functions', [])

    if not allowed:
        log.debug(
            "No allowed-intrinsic-functions configured, skipping")
        return

    allowed_patterns = [re.compile(p) for p in allowed]
    found = set()
    _collectIntrinsicFunctions(template, found)

    for func_name in sorted(found):
        matched = False
        for pattern_re in allowed_patterns:
            if pattern_re.fullmatch(func_name):
                matched = True
                break
        if not matched:
            raise TemplatePolicyViolation(
                f"Intrinsic function '{func_name}' is not allowed "
                f"by the template policy")


def validatePolicy(template_policy, template_content):
    """Validate a template against operator-configurable template policy.

    This performs policy validation without counting resources or
    looking up flavors.

    :param template_policy: Dict with template policy configuration
    :param template_content: Heat template YAML string
    :raises: TemplatePolicyViolation if template violates policy
    :raises: ValueError if template is not valid YAML
    """
    if not template_policy:
        return

    template = _load_template_yaml(template_content)
    resources = _get_resources(template)
    _validateAllowedResourceTypes(template_policy, resources)
    _validateIntrinsicFunctions(template_policy, template)


# Resource types that contribute to quota tracking.
NOVA_SERVER = 'OS::Nova::Server'
CINDER_VOLUME = 'OS::Cinder::Volume'
NEUTRON_NET = 'OS::Neutron::Net'
NEUTRON_ROUTER = 'OS::Neutron::Router'
NEUTRON_ROUTER_GATEWAY = 'OS::Neutron::RouterGateway'
NEUTRON_FLOATING_IP = 'OS::Neutron::FloatingIP'

TRACKED_RESOURCE_TYPES = {
    NOVA_SERVER,
    CINDER_VOLUME,
    NEUTRON_NET,
    NEUTRON_ROUTER,
    NEUTRON_ROUTER_GATEWAY,
    NEUTRON_FLOATING_IP,
}


class HeatTemplateParser:
    """Parse HOT templates to count resources and estimate quota."""

    def __init__(self, endpoint, template_policy=None):
        """Initialize the template parser.

        :param endpoint: HeatProviderEndpoint for flavor lookups
        :param template_policy: Optional dict with allowed resource
                                types for policy validation
        """
        self.endpoint = endpoint
        self.template_policy = template_policy or {}

    def parse(self, template_content, validate_policy=True,
              runtime_parameters=None):
        """Parse a Heat template and calculate resource requirements.

        :param template_content: Heat template YAML string
        :param validate_policy: Whether to validate template policy
        :param runtime_parameters: Dict of runtime parameters from nodeset
        :returns: QuotaInformation with resource counts
        :raises: TemplatePolicyViolation if template violates policy
        """
        template = _load_template_yaml(template_content)
        resources = _get_resources(template)

        validateSecurity(template, resources)

        if validate_policy and self.template_policy:
            _validateAllowedResourceTypes(self.template_policy, resources)
            _validateIntrinsicFunctions(self.template_policy, template)

        quota = self._countResources(resources, template, runtime_parameters)

        return quota

    def _countResources(self, resources, template, runtime_parameters=None):
        """Count resources in the template.

        :param resources: Dict of resources from template
        :param template: Full template dict (for parameter resolution)
        :param runtime_parameters: Dict of runtime parameters from nodeset
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
            if resource_type == NOVA_SERVER:
                counts['instances'] += 1
                cores, ram = self._getFlavorResources(
                    resource_def, template, runtime_parameters)
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
            elif resource_type == CINDER_VOLUME:
                counts['volumes'] += 1
                size = self._getVolumeSize(resource_def, template,
                                           runtime_parameters)
                counts['volume-gb'] += size

            # Count networks
            elif resource_type == NEUTRON_NET:
                counts['networks'] += 1

            # Count routers
            elif resource_type == NEUTRON_ROUTER:
                counts['routers'] += 1
                # Router external gateways consume an external network port
                # but are NOT Neutron floatingip quota allocations.  Track
                # them separately so floating-ips aligns with Neutron quota.
                if self._hasExternalGateway(resource_def, template):
                    counts['routers-with-external-gw'] += 1

            # RouterGateway attaches external network to router
            elif resource_type == NEUTRON_ROUTER_GATEWAY:
                counts['routers-with-external-gw'] += 1

            # Count floating IPs (actual OS::Neutron::FloatingIP resources)
            elif resource_type == NEUTRON_FLOATING_IP:
                counts['floating-ips'] += 1

        log.debug("Template resource counts: %s", counts)
        return QuotaInformation(**counts)

    def _getFlavorResources(self, resource_def, template,
                            runtime_parameters=None):
        """Look up flavor for a Nova server and return (cores, ram_mb).

        :param resource_def: Resource definition dict
        :param template: Full template dict
        :param runtime_parameters: Dict of runtime parameters from nodeset
        :returns: Tuple of (cores, ram_mb)
        :raises: ValueError if flavor is missing or not found
        """
        properties = resource_def.get('properties', {})
        flavor_ref = properties.get('flavor')

        if not flavor_ref:
            raise ValueError(
                "Nova server resource missing required 'flavor' property")

        # Resolve flavor name (may be a parameter reference)
        flavor_name = self._resolveValue(flavor_ref, template,
                                         runtime_parameters)

        if not flavor_name or not isinstance(flavor_name, str):
            raise ValueError(
                f"Unable to resolve flavor reference: {flavor_ref}")

        # Look up flavor in OpenStack
        try:
            flavor = self.endpoint._findFlavor(flavor_name)
            return (flavor.vcpus, flavor.ram)
        except openstack.exceptions.SDKException as e:
            raise ValueError(
                f"Unable to find Nova flavor '{flavor_name}': {e}") from e

    def _getVolumeSize(self, resource_def, template, runtime_parameters=None):
        """Get size from a Cinder volume resource.

        :param resource_def: Resource definition dict
        :param template: Full template dict
        :param runtime_parameters: Dict of runtime parameters from nodeset
        :returns: Volume size in GB (int)
        """
        properties = resource_def.get('properties', {})
        size_ref = properties.get('size')

        if not size_ref:
            raise ValueError(
                "Cinder volume missing required 'size' property")

        size = self._resolveValue(size_ref, template, runtime_parameters)

        if isinstance(size, int):
            return size
        elif isinstance(size, str) and size.isdigit():
            return int(size)
        else:
            raise ValueError(
                f"Unable to resolve volume size reference: {size_ref}")

    def _hasExternalGateway(self, resource_def, template):
        """Check if a router has an external gateway interface.

        :param resource_def: Router resource definition dict
        :param template: Full template dict
        :returns: True if router has external_gateway_info, False otherwise
        """
        properties = resource_def.get('properties', {})
        return 'external_gateway_info' in properties

    def _getBlockDevices(self, resource_def):
        """Extract block_device_mapping_v2 from a server resource.

        :param resource_def: Server resource definition dict
        :returns: List of block device dicts, or empty list
        :raises: ValueError if block_device_mapping_v2 is not a list
        """
        properties = resource_def.get('properties', {})
        block_devices = properties.get('block_device_mapping_v2', [])

        if not isinstance(block_devices, list):
            raise ValueError(
                "block_device_mapping_v2 must be a list")

        return block_devices

    def _resolveValue(self, value, template, runtime_parameters=None):
        """Resolve a get_param reference, trying runtime params then defaults.

        :param value: Value to resolve (may be dict, str, int, etc)
        :param template: Full template dict
        :param runtime_parameters: Dict of runtime parameters from nodeset
        :returns: Resolved value, or original if not resolvable
        """
        if isinstance(value, dict):
            # Check for { get_param: name }
            if 'get_param' in value:
                param_name = value['get_param']

                # First check runtime parameters (e.g., from nodeset)
                if runtime_parameters and param_name in runtime_parameters:
                    return runtime_parameters[param_name]

                # Fall back to template parameter defaults
                parameters = template.get('parameters', {})
                param_def = parameters.get(param_name, {})
                # Return default value if available
                return param_def.get('default', value)

        return value

    def _findFlavor(self, flavor_name):
        """Find a flavor by name in OpenStack.

        :param flavor_name: Flavor name or ID
        :returns: Flavor object with vcpus and ram attributes
        """
        return self.endpoint._findFlavor(flavor_name)
