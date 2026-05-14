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

from zuul.driver.heat.heatstatemachine import (
    HeatCreateStateMachine,
    HeatDeleteStateMachine,
)
from zuul.driver.heat.heatparser import (
    HeatTemplateParser,
    ResourcePolicyViolation,
)
from zuul.lib.voluputil import (
    Nullable,
    Optional,
    assemble,
    discriminate,
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
    # Heat images don't require additional fields beyond base image schema
    # The actual image selection happens in the Heat template
    cloud_schema = vs.All(
        assemble(
            BaseProviderImage.cloud_schema,
        ),
    )
    zuul_schema = assemble(
        BaseProviderImage.zuul_schema,
    )
    inheritable_cloud_schema = assemble(
        BaseProviderImage.inheritable_cloud_schema,
    )
    inheritable_zuul_schema = assemble(
        BaseProviderImage.inheritable_zuul_schema,
    )
    schema = vs.Union(
        cloud_schema, zuul_schema,
        discriminant=discriminate(
            lambda val, alt: val['type'] == alt['type'].validators[0])
    )
    inheritable_schema = assemble(
        inheritable_cloud_schema,
        inheritable_zuul_schema,
    )


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
            'az',
            doc="""\
            Availability zone for resources created by the Heat stack.
            This can be overridden within the Heat template itself.
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
    """Validate resource policy for mutual exclusion.

    Ensures that allowed-resource-types and disallowed-resource-types
    are not both configured, as this would be contradictory.

    :param policy: Resource policy dict
    :returns: The validated policy dict
    :raises: Invalid if both options are specified
    """
    # Keys are hyphenated in YAML but Zuul's schema mutates them to
    # snake_case (allowed_resource_types).
    allowed = policy.get('allowed_resource_types', [])
    if not allowed:
        allowed = policy.get('allowed-resource-types', [])
    disallowed = policy.get('disallowed_resource_types', [])
    if not disallowed:
        disallowed = policy.get('disallowed-resource-types', [])

    # Both lists exist by default (as []), check if both have content
    if allowed and disallowed:
        raise vs.Invalid(
            "Cannot specify both 'allowed-resource-types' and "
            "'disallowed-resource-types' in resource-policy. "
            "Use one or the other.")

    return policy


class HeatProviderSchema(BaseProviderSchema):
    """Provider-level schema for Heat driver configuration.

    Defines the schema for Heat provider configuration, including
    resource limits and resource policy (whitelist/blacklist for
    Heat resource types).
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

    def getInheritableZuulImageSchema(self):
        return HeatProviderImage.inheritable_zuul_schema

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
                to restrict which Heat resource types are allowed or
                disallowed. Either 'allowed-resource-types' or
                'disallowed-resource-types' can be specified, but not both.

                Example use cases:
                - Prevent OS::Heat::ResourceGroup (infinite scaling risk)
                - Restrict to specific resource types for security
                - Block resources that consume scarce cloud resources
                """,
                default=dict(),
            ): vs.All(
                vs.Schema({
                    Optional(
                        'allowed-resource-types',
                        doc="""\
                        Whitelist of allowed Heat resource types. If specified,
                        only these resource types may appear in Heat templates.
                        Cannot be used with disallowed-resource-types.

                        Example: ['OS::Nova::Server', 'OS::Neutron::Net']
                        """,
                        default=[]): [str],
                    Optional(
                        'disallowed-resource-types',
                        doc="""\
                        Blacklist of disallowed Heat resource types. If
                        specified, these resource types are forbidden in
                        Heat templates. Cannot be used with
                        allowed-resource-types.

                        Example: ['OS::Heat::ResourceGroup',
                                  'OS::Neutron::FloatingIP']
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

    def getQuotaForLabel(self, label, template_content=None):
        """Calculate the quota requirements for a label.

        For Heat stacks, the quota calculation has two modes:
        1. Basic mode (no template): Returns quota for 1 stack only
        2. Detailed mode (with template): Parses template to count
           instances, cores, RAM, volumes, networks, etc.

        NOTE: Currently the scheduler calls this without template
        content, so only basic quota is used. To enable detailed quota,
        the template content needs to be available at scheduling time
        (requires merger integration changes).

        :param label: Label name (str)
        :param template_content: Optional Heat template YAML string
        :returns: QuotaInformation instance
        """
        # If template content is provided, parse it for detailed quota
        if template_content:
            # Get resource policy from provider config
            resource_policy = self.config.get('resource_policy', {})
            parser = HeatTemplateParser(self.endpoint, resource_policy)
            try:
                return parser.parse(template_content)
            except ResourcePolicyViolation:
                # Policy violations must fail immediately
                raise
            except (ValueError, yaml.YAMLError) as e:
                # Template validation or YAML parsing errors
                self.log.error("Template parsing failed: %s", e)
                raise ValueError(
                    f"Cannot calculate quota without valid template: {e}")
            except openstack.exceptions.SDKException as e:
                # Flavor lookup failures during parsing
                self.log.error("Template parsing failed (OpenStack): %s", e)
                raise ValueError(
                    f"Cannot calculate quota, OpenStack error: {e}")

        # Basic quota: count the stack only
        # This is used when no template is provided (e.g., during initial
        # provider setup). Once merger integration is complete, templates
        # will be available at scheduling time for accurate quota.
        return QuotaInformation(stacks=1)

    def refreshQuotaForLabel(self, label, update):
        """Refresh quota information for a label.

        This is called when the provider needs to update quota after
        changes (e.g., flavor changes, image changes).

        :param label: Label name (str)
        :param update: Whether to force quota cache update (bool)
        :returns: QuotaInformation instance
        """
        # Delegates to getQuotaForLabel which handles both basic
        # (stacks=1) and detailed (template parsing) quota calculation
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

    def getCreateStateMachine(self, node, template_content, log):
        """Get a state machine for creating a Heat stack.

        :param node: HeatProviderNode to create
        :param template_content: Heat template YAML content (string)
        :param log: Logger instance
        :returns: HeatCreateStateMachine instance
        """
        stack_name = self._getStackName(node)

        # Prepare template parameters
        # The merger will have already resolved the template content
        parameters = {}

        # Prepare metadata tags for the stack
        # Heat API expects tags as a list of "key=value" strings
        tags_dict = node.tags.copy()
        tags_dict['zuul_node_uuid'] = node.uuid
        tags_dict['zuul_system_id'] = self.system_id
        tags = [f"{k}={v}" for k, v in tags_dict.items()]

        return HeatCreateStateMachine(
            self.endpoint,
            node,
            stack_name,
            template_content,
            parameters,
            tags,
            log)

    def getDeleteStateMachine(self, node, log):
        """Get a state machine for deleting a Heat stack.

        :param node: HeatProviderNode to delete
        :param log: Logger instance
        :returns: HeatDeleteStateMachine instance
        """
        return HeatDeleteStateMachine(self.endpoint, node, log)
