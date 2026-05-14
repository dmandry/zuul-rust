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

"""Heat Provider implementation for Zuul.

This module provides the Heat driver's provider schema and configuration
classes, enabling Zuul to orchestrate OpenStack Heat stacks for test nodes.
"""

import logging
import yaml

import openstack.exceptions
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
)
from cryptography.hazmat.primitives import serialization

from zuul.driver.heat.heatstatemachine import (
    HeatCreateStateMachine,
    HeatDeleteStateMachine,
)
from zuul.driver.heat.heatparser import (
    HeatTemplateParser,
    FlavorLimitExceeded,
    ResourcePolicyViolation,
)
from zuul.lib.voluputil import (
    Nullable,
    Optional,
    assemble,
)
from zuul.provider import (
    BaseProvider,
    BaseProviderImage,
    BaseProviderFlavor,
    BaseProviderLabel,
    BaseProviderSchema,
)
import zuul.provider.schema as provider_schema
from zuul.model import QuotaInformation

import voluptuous as vs


class HeatProviderImage(BaseProviderImage):
    """Heat-specific image configuration.

    For Heat stacks, images are referenced in the Heat template itself
    (e.g., as parameters or glance image names/IDs). This class provides
    a base schema compatible with Zuul's image management, primarily for
    Glance images that might be used as inputs to Heat templates.
    """
    heat_cloud_schema = vs.Schema({
        Optional(
            'image-id',
            doc="""\
            The Glance image ID or name for the bastion node.
            This is passed to the Heat template as the
            ``zuul_image`` parameter.
            """,
        ): Nullable(str),
    })
    cloud_schema = vs.All(
        assemble(
            BaseProviderImage.cloud_schema,
            heat_cloud_schema,
        ),
    )
    internal_cloud_schema = assemble(
        BaseProviderImage.cloud_schema,
        heat_cloud_schema,
        provider_schema.internal_base_image,
        extra=vs.ALLOW_EXTRA,
    )
    inheritable_cloud_schema = assemble(
        BaseProviderImage.inheritable_cloud_schema,
    )
    schema = cloud_schema
    internal_schema = internal_cloud_schema
    inheritable_schema = inheritable_cloud_schema

    def __init__(self, image_config, provider_config):
        self.image_id = None
        super().__init__(image_config, provider_config)


class HeatProviderFlavor(BaseProviderFlavor):
    """Heat-specific flavor configuration with per-stack resource limits.

    Defines resource limits that apply to each Heat stack created with
    this flavor. These limits are used to track and control resource
    consumption for individual stacks.
    """
    heat_flavor_schema = vs.Schema({
        Optional(
            'max-instances',
            default=vs.UNDEFINED,
            doc="""\
            Maximum number of Nova instances allowed per stack.
            This limit applies to OS::Nova::Server resources in the
            Heat template. Used for quota tracking.
            """,
        ): vs.Any(int, type(vs.UNDEFINED)),
        Optional(
            'max-cores',
            default=vs.UNDEFINED,
            doc="""\
            Maximum total CPU cores allowed per stack (sum of all
            instance flavors). Used for quota tracking.
            """,
        ): vs.Any(int, type(vs.UNDEFINED)),
        Optional(
            'max-ram',
            default=vs.UNDEFINED,
            doc="""\
            Maximum total RAM in MiB allowed per stack (sum of all
            instance flavors). Used for quota tracking.
            """,
        ): vs.Any(int, type(vs.UNDEFINED)),
        Optional(
            'max-volumes',
            default=vs.UNDEFINED,
            doc="""\
            Maximum number of Cinder volumes allowed per stack.
            This limit applies to OS::Cinder::Volume resources.
            Used for quota tracking.
            """,
        ): vs.Any(int, type(vs.UNDEFINED)),
        Optional(
            'max-volume-gb',
            default=vs.UNDEFINED,
            doc="""\
            Maximum total volume storage in GB allowed per stack
            (sum of all volume sizes). Used for quota tracking.
            """,
        ): vs.Any(int, type(vs.UNDEFINED)),
        Optional(
            'max-networks',
            default=vs.UNDEFINED,
            doc="""\
            Maximum number of Neutron networks allowed per stack.
            This limit applies to OS::Neutron::Net resources.
            Used for quota tracking.
            """,
        ): vs.Any(int, type(vs.UNDEFINED)),
        Optional(
            'max-routers',
            default=vs.UNDEFINED,
            doc="""\
            Maximum number of Neutron routers allowed per stack.
            This limit applies to OS::Neutron::Router resources.
            Used for quota tracking.
            """,
        ): vs.Any(int, type(vs.UNDEFINED)),
        Optional(
            'max-routers-with-external-gw',
            default=vs.UNDEFINED,
            doc="""\
            Maximum number of routers with external gateway interfaces
            allowed per stack. Routers with external gateways consume
            floating IPs. Used for quota tracking.
            """,
        ): vs.Any(int, type(vs.UNDEFINED)),
        Optional(
            'max-floating-ips',
            default=vs.UNDEFINED,
            doc="""\
            Maximum number of floating IPs allowed per stack. This
            includes both OS::Neutron::FloatingIP resources and
            routers with external gateway interfaces. Note: The driver
            will log warnings when a stack uses more than 1 floating IP,
            as floating IPs are typically scarce resources.
            """,
        ): vs.Any(int, type(vs.UNDEFINED)),
    })

    inheritable_schema = assemble(
        BaseProviderFlavor.inheritable_schema,
    )
    schema = assemble(
        BaseProviderFlavor.schema,
        heat_flavor_schema,
    )
    internal_schema = assemble(
        schema,
        provider_schema.internal_base_flavor,
        extra=vs.ALLOW_EXTRA,
    )


class HeatProviderLabel(BaseProviderLabel):
    """Heat-specific label configuration.

    Labels in Heat driver represent the type of Heat stack to create.
    The Heat template path is specified in the job's node-properties
    (part of the nodeset definition), not in the label configuration.
    """
    heat_label_schema = vs.Schema({
        Optional(
            'region',
            doc="""\
            The OpenStack region where Heat stacks should be created.
            If omitted, uses the default region from the cloud
            configuration.
            """,
        ): Nullable(str),
        Optional(
            'external-network',
            doc="""\
            The name or ID of the external network for floating IPs.
            Passed to the Heat template as the
            ``zuul_external_network`` parameter.  If omitted, the
            driver auto-discovers the first external IPv4 network
            from Neutron.
            """,
        ): Nullable(str),
    })

    inheritable_schema = assemble(
        BaseProviderLabel.inheritable_schema,
        provider_schema.ssh_label,
        heat_label_schema,
    )
    schema = assemble(
        BaseProviderLabel.schema,
        provider_schema.ssh_label,
        heat_label_schema,
    )
    internal_schema = assemble(
        schema,
        provider_schema.internal_base_label,
        extra=vs.ALLOW_EXTRA,
    )


def _validate_resource_policy(policy):
    """Validate resource policy regex patterns.

    Validates that all patterns in allowed-resource-types are valid
    Python regular expressions.

    :param policy: Resource policy dict
    :returns: The validated policy dict
    :raises: Invalid if regex is invalid
    """
    import re

    allowed = policy.get('allowed_resource_types', [])
    if not allowed:
        allowed = policy.get('allowed-resource-types', [])

    for pattern in allowed:
        try:
            re.compile(pattern)
        except re.error as e:
            raise vs.Invalid(
                f"Invalid regex pattern '{pattern}' in "
                f"allowed-resource-types: {e}") from e

    return policy


class HeatProviderSchema(BaseProviderSchema):
    """Provider-level schema for Heat driver configuration.

    Defines the schema for Heat provider configuration, including
    resource limits and resource policy (allow list for Heat resource
    types).
    """
    def getLabelSchema(self, internal=False):
        if internal:
            return HeatProviderLabel.internal_schema
        return HeatProviderLabel.schema

    def getImageSchema(self, internal=False):
        if internal:
            return HeatProviderImage.internal_schema
        return HeatProviderImage.schema

    def getFlavorSchema(self, internal=False):
        if internal:
            return HeatProviderFlavor.internal_schema
        return HeatProviderFlavor.schema

    def getInheritableLabelSchema(self):
        return HeatProviderLabel.inheritable_schema

    def getInheritableImageSchema(self):
        return HeatProviderImage.inheritable_schema

    def getInheritableCloudImageSchema(self):
        return HeatProviderImage.inheritable_cloud_schema

    def getInheritableFlavorSchema(self):
        return HeatProviderFlavor.inheritable_schema

    def getProviderSchema(self, internal=False):
        """Return the provider-level configuration schema.

        This includes resource-limits (global quota constraints) and
        resource-policy (allowed/disallowed Heat resource types).
        """
        schema = super().getProviderSchema(internal)

        resource_limits = {
            Optional(
                'stacks',
                default=vs.UNDEFINED,
                doc="""\
                The maximum number of active Heat stacks across all
                labels for this provider.
                """,
            ): vs.Any(int, type(vs.UNDEFINED)),
            Optional(
                'instances',
                default=vs.UNDEFINED,
                doc="""\
                The total number of Nova instances across all stacks.
                This is calculated by parsing Heat templates.
                """,
            ): vs.Any(int, type(vs.UNDEFINED)),
            Optional(
                'cores',
                default=vs.UNDEFINED,
                doc="""\
                The total number of CPU cores across all stacks.
                This is calculated by summing instance flavors.
                """,
            ): vs.Any(int, type(vs.UNDEFINED)),
            Optional(
                'ram',
                default=vs.UNDEFINED,
                doc="""\
                The total amount of RAM in MiB across all stacks.
                This is calculated by summing instance flavors.
                """,
            ): vs.Any(int, type(vs.UNDEFINED)),
            Optional(
                'volumes',
                default=vs.UNDEFINED,
                doc="""\
                The total number of Cinder volumes across all stacks.
                This is calculated by parsing Heat templates.
                """,
            ): vs.Any(int, type(vs.UNDEFINED)),
            Optional(
                'volume-gb',
                default=vs.UNDEFINED,
                doc="""\
                The total volume storage in GB across all stacks.
                This is calculated by parsing Heat templates.
                """,
            ): vs.Any(int, type(vs.UNDEFINED)),
            Optional(
                'networks',
                default=vs.UNDEFINED,
                doc="""\
                The total number of Neutron networks across all stacks.
                This is calculated by parsing Heat templates.
                """,
            ): vs.Any(int, type(vs.UNDEFINED)),
            Optional(
                'routers',
                default=vs.UNDEFINED,
                doc="""\
                The total number of Neutron routers across all stacks.
                This is calculated by parsing Heat templates.
                """,
            ): vs.Any(int, type(vs.UNDEFINED)),
            Optional(
                'routers-with-external-gw',
                default=vs.UNDEFINED,
                doc="""\
                The total number of routers with external gateway
                interfaces across all stacks. These consume floating IPs.
                """,
            ): vs.Any(int, type(vs.UNDEFINED)),
            Optional(
                'floating-ips',
                default=vs.UNDEFINED,
                doc="""\
                The total number of floating IPs across all stacks.
                Includes both FloatingIP resources and routers with
                external gateways. This is a scarce resource and should
                be carefully managed.
                """,
            ): vs.Any(int, type(vs.UNDEFINED)),
        }

        heat_provider_schema = vs.Schema({
            Optional(
                'region',
                doc="""\
                The default OpenStack region for this provider.
                Can be overridden at the label level.
                """,
            ): Nullable(str),
            Optional(
                'stack-create-timeout',
                default=600,
                doc="""\
                Maximum time in seconds to wait for a Heat stack to reach
                CREATE_COMPLETE status. If stack creation exceeds this
                timeout, the node request will fail.

                Default: 600 (10 minutes)
                """,
            ): int,
            Optional(
                'resource-limits',
                doc="""\
                Resource limits for this provider. These values control
                the maximum resources Zuul will consume across all stacks.
                Use these to stay within cloud quotas or implement
                cost controls.

                Resource limits can also be specified per-flavor (applying
                per-stack) using the flavor's max-* attributes.
                """,
                default=dict()): resource_limits,
            Optional(
                'resource-policy',
                doc="""\
                Resource policy for validating Heat templates. Use this
                to restrict which Heat resource types are allowed.

                Patterns are treated as Python regular expressions and
                matched against the full resource type string using
                fullmatch semantics. This allows flexible matching:
                - Exact match: 'OS::Nova::Server'
                - Prefix match: '^OS::Nova::.*'
                - Complex patterns: '^OS::(Nova|Cinder)::.*'
                - Negative lookahead: '^(?!OS::Heat::ResourceGroup$).*'
                  (allow everything except ResourceGroup)

                Example use cases:
                - Restrict to compute/network/storage:
                  '^OS::(Nova|Neutron|Cinder)::.*'
                - Allow everything except ResourceGroup:
                  '^(?!OS::Heat::ResourceGroup$).*'
                """,
                default=dict(),
            ): vs.All(
                vs.Schema({
                    Optional(
                        'allowed-resource-types',
                        doc="""\
                        List of allowed Heat resource types. If specified,
                        only resource types matching at least one pattern
                        may appear in Heat templates. If not specified (or
                        empty), all resource types are allowed.

                        Each entry is treated as a Python regular expression
                        matched against the full resource type string. Exact
                        strings work as-is (they match themselves exactly).

                        Use negative lookahead to express deny semantics:
                        '^(?!OS::Heat::ResourceGroup$).*' allows everything
                        except ResourceGroup.

                        Examples:
                        - 'OS::Nova::Server' (exact match)
                        - '^OS::Nova::.*' (all Nova resources)
                        - '^OS::(Nova|Neutron|Cinder)::.*'
                          (compute/network/storage)
                        - '^(?!OS::Heat::ResourceGroup$).*'
                          (everything except ResourceGroup)
                        """,
                        default=[]): [str],
                }),
                _validate_resource_policy
            ),
        })

        return assemble(
            schema,
            heat_provider_schema,
            doc="""\
            The attributes available for configuring a Heat provider
            are below. The Heat driver uses OpenStack Heat orchestration
            to create complex multi-node test environments.
            """,
        )


class HeatProvider(BaseProvider, subclass_id='heat'):
    """Heat provider implementation.

    Manages Heat stack lifecycle for Zuul test nodes, including stack
    creation, deletion, quota tracking, and resource cleanup.
    """
    log = logging.getLogger("zuul.HeatProvider")
    schema = HeatProviderSchema().getProviderSchema()
    internal_schema = HeatProviderSchema().getProviderSchema(internal=True)

    @property
    def endpoint(self):
        """Get the cached endpoint for this provider.

        Lazily initializes the endpoint on first access and caches it
        for subsequent use.

        :returns: HeatProviderEndpoint instance
        """
        ep = getattr(self, '_endpoint', None)
        if ep:
            return ep
        self._set(_endpoint=self.getEndpoint())
        return self._endpoint

    def parseImage(self, image_config, provider_config, connection):
        """Parse an image configuration and create a HeatProviderImage.

        :param image_config: Image configuration dict
        :param provider_config: Provider configuration dict
        :param connection: HeatConnection instance
        :returns: HeatProviderImage instance
        """
        return HeatProviderImage(image_config, provider_config)

    def parseFlavor(self, flavor_config, provider_config, connection):
        """Parse a flavor configuration and create a HeatProviderFlavor.

        :param flavor_config: Flavor configuration dict
        :param provider_config: Provider configuration dict
        :param connection: HeatConnection instance
        :returns: HeatProviderFlavor instance
        """
        return HeatProviderFlavor(flavor_config, provider_config)

    def parseLabel(self, label_config, provider_config, connection):
        """Parse a label configuration and create a HeatProviderLabel.

        :param label_config: Label configuration dict
        :param provider_config: Provider configuration dict
        :param connection: HeatConnection instance
        :returns: HeatProviderLabel instance
        """
        return HeatProviderLabel(label_config, provider_config)

    def getEndpoint(self):
        """Get the Heat endpoint for this provider.

        Delegates to the driver to get or create a cached endpoint
        for the configured region.

        :returns: HeatProviderEndpoint instance
        """
        return self.driver.getEndpoint(self)

    def getQuotaForLabel(self, label, **kwargs):
        """Calculate the quota requirements for a label.

        For Heat stacks, the quota calculation has two modes:
        1. Basic mode (no template): Returns quota for 1 stack only
        2. Detailed mode (with template): Parses template to count
           instances, cores, RAM, volumes, networks, etc.

        When template_config is provided and contains
        'heat_template_content', the template is parsed to report
        accurate per-label quota (instances, cores, ram, volumes,
        networks, floating-ips, etc.). This enables the launcher's
        quota system to enforce provider-level resource-limits.

        :param label: Label name (str)
        :param template_config: Optional dict of template configuration
            (kwarg). If it contains 'heat_template_content', detailed quota
            is calculated by parsing the template.
        :returns: QuotaInformation instance
        """
        template_config = kwargs.get('template_config', {})
        template_content = template_config.get('heat_template_content')
        heat_parameters = template_config.get('heat_parameters', {})

        # Basic quota fallback: count the stack only.
        # This is used when template_config is not provided or doesn't
        # contain a template (e.g., during provider initialization via
        # refreshQuotaForLabel). When the launcher schedules actual nodes,
        # it provides template_config with heat_template_content loaded
        # by the configloader, enabling accurate quota calculation.
        if not template_content:
            return QuotaInformation(stacks=1)

        # Parse the template to calculate detailed quota
        resource_policy = getattr(self, 'config', {}).get(
            'resource_policy', {})
        parser = HeatTemplateParser(self.endpoint, resource_policy)
        try:
            return parser.parse(template_content,
                                runtime_parameters=heat_parameters)
        except ResourcePolicyViolation:
            # Policy violations must fail immediately
            raise
        except (ValueError, yaml.YAMLError) as e:
            # Template validation or YAML parsing errors
            self.log.error("Template parsing failed: %s", e)
            raise ValueError(
                f"Cannot calculate quota without valid template: {e}") from e
        except openstack.exceptions.SDKException as e:
            # Flavor lookup failures during parsing
            self.log.error("Template parsing failed (OpenStack): %s", e)
            raise ValueError(
                f"Cannot calculate quota, OpenStack error: {e}") from e

    def refreshQuotaForLabel(self, label, update):
        """Refresh quota information for a label.

        This is called when the provider needs to update quota after
        changes (e.g., flavor changes, image changes). Without
        template_config, this returns basic stack-only quota.

        :param label: Label name (str)
        :param update: Whether to force quota cache update (bool)
        :returns: QuotaInformation instance
        """
        return self.getQuotaForLabel(label)

    def _getStackName(self, node):
        """Generate a stack name for a node.

        Generates a unique stack name that includes both the Zuul system ID
        and the node UUID to prevent collisions when multiple Zuul deployments
        share the same OpenStack project.

        :param node: HeatProviderNode
        :returns: Stack name string (format: zuul-{system_id[:8]}-{node.uuid})
        """
        return f"zuul-{self.system_id[:8]}-{node.uuid}"

    @staticmethod
    def _generateEphemeralKeypair():
        """Generate an ephemeral Ed25519 SSH keypair.

        Returns the private key in PEM format and the public key in
        OpenSSH format.  These are passed as Heat template parameters
        so that templates can create OS::Nova::KeyPair resources with
        an explicit public_key (required by Nova API >= 2.92) and
        inject the private key onto bastion nodes via cloud-init.

        :returns: (private_key_pem, public_key_openssh) tuple of strings
        """
        private_key = Ed25519PrivateKey.generate()
        private_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.OpenSSH,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode('utf-8')
        public_openssh = private_key.public_key().public_bytes(
            encoding=serialization.Encoding.OpenSSH,
            format=serialization.PublicFormat.OpenSSH,
        ).decode('utf-8')
        return private_pem, public_openssh

    # Mapping from QuotaInformation keys to flavor max-* attribute names
    _FLAVOR_LIMIT_MAP = {
        'instances': 'max_instances',
        'cores': 'max_cores',
        'ram': 'max_ram',
        'volumes': 'max_volumes',
        'volume-gb': 'max_volume_gb',
        'networks': 'max_networks',
        'routers': 'max_routers',
        'routers-with-external-gw': 'max_routers_with_external_gw',
        'floating-ips': 'max_floating_ips',
    }

    def _validateFlavorLimits(self, template_content, flavor,
                              heat_parameters=None):
        """Validate template resource counts against flavor limits.

        Parses the template to count resources and compares each count
        against the corresponding max-* attribute on the flavor. Raises
        FlavorLimitExceeded if any limit is exceeded.

        :param template_content: Heat template YAML string
        :param flavor: HeatProviderFlavor instance
        :param heat_parameters: Dict of heat parameters (e.g. from nodeset)
        :raises: FlavorLimitExceeded if any limit is exceeded
        """
        parser = HeatTemplateParser(self.endpoint, {})
        quota = parser.parse(template_content, validate_policy=False,
                             runtime_parameters=heat_parameters)

        violations = []
        for quota_key, flavor_attr in self._FLAVOR_LIMIT_MAP.items():
            limit = getattr(flavor, flavor_attr, None)
            if not isinstance(limit, int):
                continue
            actual = quota.quota.get(quota_key, 0)
            if actual > limit:
                violations.append(
                    f"{quota_key}: {actual} > {limit}")

        if violations:
            raise FlavorLimitExceeded(
                f"Template exceeds flavor limits: "
                f"{'; '.join(violations)}")

    def getCreateStateMachine(self, node, image_external_id, log):
        """Get a state machine for creating a Heat stack.

        :param node: HeatProviderNode to create
        :param image_external_id: External image ID for Zuul-managed images
        :param log: Logger instance
        :returns: HeatCreateStateMachine instance
        """
        stack_name = self._getStackName(node)

        template_content = node.template_config.get(
            'heat_template_content')
        if not template_content:
            path = node.template_config.get(
                'heat_template_path', '<not set>')
            raise Exception(
                f"No Heat template content for node "
                f"{node.uuid}. heat-template-path: "
                f"{path}. Ensure the nodeset defines "
                f"heat-template-path in template-config "
                f"and the file exists in the repo.")

        resource_policy = getattr(self, 'config', {}).get(
            'resource_policy', {})
        if resource_policy:
            parser = HeatTemplateParser(None, resource_policy)
            parser.validatePolicy(template_content)

        label = self.labels[node.label]
        flavor = getattr(self, 'flavors', {}).get(
            getattr(label, 'flavor', None))
        heat_parameters = node.template_config.get('heat_parameters', {})
        if flavor:
            self._validateFlavorLimits(template_content, flavor,
                                       heat_parameters)

        # Parse the template's declared parameters so we only pass zuul_*
        # parameters that the template actually defines.  Heat rejects
        # stack creation with a 400 error if unknown parameters are supplied.
        try:
            template_dict = yaml.safe_load(template_content)
            template_defined_params = set(
                template_dict.get('parameters', {}).keys()
                if isinstance(template_dict, dict) else [])
        except yaml.YAMLError:
            template_defined_params = set()

        user_params = node.template_config.get('heat_parameters', {})
        parameters = dict(user_params)
        image = self.images[label.image]

        if label.key_name and 'zuul_key_name' in template_defined_params:
            parameters['zuul_key_name'] = label.key_name
        if image.username and 'zuul_username' in template_defined_params:
            parameters['zuul_username'] = image.username
        if 'zuul_image' in template_defined_params:
            if image_external_id:
                parameters['zuul_image'] = image_external_id
            elif getattr(image, 'image_id', None):
                parameters['zuul_image'] = image.image_id

        if 'zuul_external_network' in template_defined_params:
            if label.external_network:
                parameters['zuul_external_network'] = label.external_network
            else:
                parameters['zuul_external_network'] = (
                    self.endpoint._getExternalNetwork())

        private_pem, public_openssh = self._generateEphemeralKeypair()
        if 'zuul_ephemeral_private_key' in template_defined_params:
            parameters['zuul_ephemeral_private_key'] = private_pem
        if 'zuul_ephemeral_public_key' in template_defined_params:
            parameters['zuul_ephemeral_public_key'] = public_openssh

        tags_dict = node.tags.copy()
        tags_dict['zuul_node_uuid'] = node.uuid
        tags_dict['zuul_system_id'] = self.system_id
        tags = [f"{k}={v}" for k, v in tags_dict.items()]

        timeout = getattr(self, 'config', {}).get('stack_create_timeout', 600)

        return HeatCreateStateMachine(
            self.endpoint,
            node,
            stack_name,
            template_content,
            parameters,
            tags,
            timeout,
            log)

    def getDeleteStateMachine(self, node, log):
        """Get a state machine for deleting a Heat stack.

        :param node: HeatProviderNode to delete
        :param log: Logger instance
        :returns: HeatDeleteStateMachine instance
        """
        return HeatDeleteStateMachine(self.endpoint, node, log)
