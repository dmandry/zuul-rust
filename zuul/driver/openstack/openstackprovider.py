# Copyright 2022-2024 Acme Gating, LLC
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

import logging
import math

import zuul.provider.schema as provider_schema
from zuul.lib.voluputil import (
    AsList,
    Nullable,
    Optional,
    Required,
    assemble,
    discriminate,
)

import voluptuous as vs

from zuul.driver.openstack.openstackendpoint import (
    OpenstackCreateStateMachine,
    OpenstackDeleteStateMachine,
)
from zuul.model import QuotaInformation
from zuul.provider import (
    BaseProvider,
    BaseProviderFlavor,
    BaseProviderImage,
    BaseProviderLabel,
    BaseProviderSchema,
)


class OpenstackProviderImage(BaseProviderImage):
    # This is used here and in flavors and labels
    inheritable_openstack_image_schema = assemble(
        vs.Schema({
            Optional(
                'volume-size',
                doc="""\
                When booting an image from volume, this indicates the
                size of the created volume, in GB.
                """,
            ): Nullable(int),
        }),
        provider_schema.cloud_image,
    )
    openstack_cloud_schema = vs.Schema({
        Required(
            'image-id',
            doc="""\
            The ID of the cloud provider's image.
            """,
        ): str,
        Optional(
            'config-drive',
            doc="""\
            Whether config drive should be used for the cloud
            image.
            """,
            default=True): bool,
    })
    cloud_schema = assemble(
        BaseProviderImage.cloud_schema,
        openstack_cloud_schema,
        inheritable_openstack_image_schema,
    )
    internal_cloud_schema = assemble(
        cloud_schema,
        provider_schema.internal_base_image,
    )
    inheritable_openstack_zuul_schema = vs.Schema({
        Optional(
            'config-drive',
            doc="""\
            Whether config drive should be used for the cloud
            image.
            """,
            default=True): bool,
    })
    zuul_schema = assemble(
        BaseProviderImage.zuul_schema,
        inheritable_openstack_image_schema,
        inheritable_openstack_zuul_schema,
    )
    internal_zuul_schema = assemble(
        zuul_schema,
        provider_schema.internal_base_image,
    )
    inheritable_cloud_schema = assemble(
        BaseProviderImage.inheritable_cloud_schema,
        inheritable_openstack_image_schema,
    )
    inheritable_zuul_schema = assemble(
        BaseProviderImage.inheritable_zuul_schema,
        inheritable_openstack_image_schema,
        inheritable_openstack_zuul_schema,
    )
    schema = vs.Union(
        cloud_schema, zuul_schema,
        discriminant=discriminate(
            lambda val, alt: val['type'] == alt['type'].validators[0])
    )
    internal_schema = vs.Union(
        internal_cloud_schema, internal_zuul_schema,
        discriminant=discriminate(
            lambda val, alt: val['type'] == alt['type'].validators[0])
    )
    inheritable_schema = assemble(
        inheritable_cloud_schema,
        inheritable_zuul_schema,
    )

    def __init__(self, image_config, provider_config, image_format):
        self.image_id = None
        super().__init__(image_config, provider_config)
        self.format = image_format
        # Implement provider defaults
        if self.connection_type is None:
            self.connection_type = 'ssh'
        if self.connection_port is None:
            self.connection_port = 22


class OpenstackProviderFlavor(BaseProviderFlavor):
    openstack_flavor_schema = vs.Schema({
        Required(
            'flavor-name',
            doc="""\
            Name or id of the OpenStack flavor to use.
            """,
        ): str,
    })

    inheritable_schema = assemble(
        BaseProviderFlavor.inheritable_schema,
        OpenstackProviderImage.inheritable_openstack_image_schema,
        provider_schema.cloud_flavor,
    )
    schema = assemble(
        BaseProviderFlavor.schema,
        provider_schema.cloud_flavor,
        OpenstackProviderImage.inheritable_openstack_image_schema,
        openstack_flavor_schema,
    )
    internal_schema = assemble(
        schema,
        provider_schema.internal_base_flavor,
    )


class OpenstackProviderLabel(BaseProviderLabel):
    inheritable_openstack_label_schema = vs.Schema({
        Optional(
            'az',
            doc="""\
            Servers will be assigned to the specified availibility
            zone.  If omitted, one will be chosen at random.
            """,
        ): Nullable(str),
        Optional(
            'auto-floating-ip',
            doc="""\
            Whether to automatically allocate and assign a floating IP
            for each node.
            """,
            default=True): bool,
        Optional(
            'boot-from-volume',
            doc="""\
            Whether to create a volume from the image and boot the
            node from it.
            """,
            default=False): bool,
        Optional(
            'networks', default=[],
            doc="""\
            The OpenStack networks to associate with the node.
            """,
        ): AsList(str),
        Optional(
            'security-groups',
            doc="""\
            Specify custom networks to be attached to each
            node.  Specify the name or id of the network as a string.
            """,
            default=[]): AsList(str),
    })
    inheritable_schema = assemble(
        BaseProviderLabel.inheritable_schema,
        OpenstackProviderImage.inheritable_openstack_image_schema,
        provider_schema.ssh_label,
        inheritable_openstack_label_schema,
    )
    schema = assemble(
        BaseProviderLabel.schema,
        OpenstackProviderImage.inheritable_openstack_image_schema,
        provider_schema.ssh_label,
        inheritable_openstack_label_schema,
    )
    internal_schema = assemble(
        schema,
        provider_schema.internal_base_label,
    )


class OpenstackProviderSchema(BaseProviderSchema):
    def getLabelSchema(self, internal=False):
        if internal:
            return OpenstackProviderLabel.internal_schema
        else:
            return OpenstackProviderLabel.schema

    def getImageSchema(self, internal=False):
        if internal:
            return OpenstackProviderImage.internal_schema
        else:
            return OpenstackProviderImage.schema

    def getFlavorSchema(self, internal=False):
        if internal:
            return OpenstackProviderFlavor.internal_schema
        else:
            return OpenstackProviderFlavor.schema

    def getInheritableLabelSchema(self):
        return OpenstackProviderLabel.inheritable_schema

    def getInheritableImageSchema(self):
        return OpenstackProviderImage.inheritable_schema

    def getInheritableZuulImageSchema(self):
        return OpenstackProviderImage.inheritable_zuul_schema

    def getInheritableCloudImageSchema(self):
        return OpenstackProviderImage.inheritable_cloud_schema

    def getInheritableFlavorSchema(self):
        return OpenstackProviderFlavor.inheritable_schema

    def getProviderSchema(self, internal=False):
        schema = super().getProviderSchema(internal)

        resource_limits = {
            Optional(
                'instances',
                default=vs.UNDEFINED,
                doc="""The number of instances.""",
            ): int,
            Optional(
                'cores',
                default=vs.UNDEFINED,
                doc="""The number of cores.""",
            ): int,
            Optional(
                'ram',
                default=vs.UNDEFINED,
                doc="""The amount of ram, in MiB.""",
            ): int,
            Optional(
                'volumes',
                default=vs.UNDEFINED,
                doc="""The number of volumes.""",
            ): int,
            Optional(
                'volume-gb',
                default=vs.UNDEFINED,
                doc="""The amount of volume storage in GB.""",
            ): int,
        }

        openstack_provider_schema = vs.Schema({
            Optional(
                'region',
                doc="""\
                The region name if the provider cloud has multiple
                regions.
                """,
            ): Nullable(str),
            Optional(
                'resource-limits',
                doc="""\
                Resource limits for this provider.  Configure these
                values to cause Zuul to attempt to limit the resource
                usage.  This can be used to limit Zuul's usage to a
                level below the cloud quota.
                """,
                default=dict()): resource_limits,
            Optional(
                'floating-ip-cleanup',
                doc="""\
                If set to ``true``, Zuul will behave as if it is the
                only user of the OpenStack project and will attempt to
                clean unattached floating IPs that may have leaked.
                """,
                default=False): bool,
            Optional(
                'port-cleanup-interval',
                doc="""\
                If greater than 0, Zuul will behave as if it is the
                only user of the OpenStack project and will attempt to
                clean ports in ``DOWN`` state after the cleanup
                interval has elapsed.  This value may be reduced if
                the instance spawn time on the provider is reliably
                quicker.
                """,
                default=600): int,
        })

        return assemble(
            schema,
            openstack_provider_schema,
            doc="""\
            The attributes available for configuring an OpenStack provider
            are below.
            """,
        )


class OpenstackProvider(BaseProvider, subclass_id='openstack'):
    log = logging.getLogger("zuul.OpenstackProvider")
    schema = OpenstackProviderSchema().getProviderSchema()
    internal_schema = OpenstackProviderSchema().getProviderSchema(
        internal=True)

    @property
    def endpoint(self):
        ep = getattr(self, '_endpoint', None)
        if ep:
            return ep
        self._set(_endpoint=self.getEndpoint())
        return self._endpoint

    def parseImage(self, image_config, provider_config, connection):
        # We are not fully constructed yet at this point, so we need
        # to peek to get the region and endpoint.
        region = provider_config.get('region')
        endpoint = connection.driver._getEndpoint(
            self.zk_client, connection, region, self.system_id)
        return OpenstackProviderImage(
            image_config, provider_config,
            image_format=endpoint.getImageFormat())

    def parseFlavor(self, flavor_config, provider_config, connection):
        return OpenstackProviderFlavor(flavor_config, provider_config)

    def parseLabel(self, label_config, provider_config, connection):
        return OpenstackProviderLabel(label_config, provider_config)

    def getEndpoint(self):
        return self.driver.getEndpoint(self)

    def getCreateStateMachine(self, node, image_external_id, log):
        # TODO: decide on a method of producing a hostname
        # that is max 15 chars.
        hostname = f"np{node.uuid[:13]}"
        label = self.labels[node.label]
        flavor = self.flavors[label.flavor]
        image = self.images[label.image]
        return OpenstackCreateStateMachine(
            self.endpoint,
            node,
            hostname,
            label,
            flavor,
            image,
            image_external_id,
            node.tags,
            log)

    def getDeleteStateMachine(self, node, log):
        return OpenstackDeleteStateMachine(self.endpoint, node, log)

    def getEndpointLimits(self):
        limits = self.endpoint.quota_cache.getLimits()
        if limits is None:
            limits = {}
        else:
            limits = limits.quota
        return QuotaInformation(default=math.inf, **limits)

    def getQuotaForLabel(self, label):
        flavor = self.flavors[label.flavor]
        return self.endpoint.getQuotaForLabel(label, flavor)

    def refreshQuotaForLabel(self, label, update):
        flavor = self.flavors[label.flavor]
        return self.endpoint.refreshQuotaForLabel(label, flavor, update)

    def getImageUploadJob(self, provider_image, image_name,
                          image_format, metadata, md5, sha256):
        return self.endpoint.getImageUploadJob(
            provider_image, image_name,
            image_format, metadata, md5, sha256)

    def deleteImage(self, external_id):
        self.endpoint.deleteImage(external_id)
