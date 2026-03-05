# Copyright 2021-2025 Acme Gating, LLC
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
    Constant,
    Exclusive,
    Nullable,
    Optional,
    Required,
    RequiredExclusive,
    assemble,
    discriminate,
)

import voluptuous as vs

from zuul.driver.azure.azureendpoint import (
    AzureCreateStateMachine,
    AzureDeleteStateMachine,
)
from zuul.model import QuotaInformation
from zuul.provider import (
    BaseProvider,
    BaseProviderFlavor,
    BaseProviderImage,
    BaseProviderLabel,
    BaseProviderSchema,
)

URL_VM_SIZES = 'https://azure.microsoft.com/en-us/global-infrastructure/services/?products=virtual-machines'  # noqa


class AzureProviderImage(BaseProviderImage):
    azure_image_reference = {
        Required(
            'sku',
            doc="""
            The image SKU.
            """,
        ): str,
        Required(
            'publisher',
            doc="""\
            The image Publisher.
            """,
        ): str,
        Required(
            'version',
            doc="""\
            The image version.
            """,
        ): str,
        Required(
            'offer',
            doc="""\
            The image offer.
            """,
        ): str,
    }
    azure_image_filter = {
        Optional(
            'location',
            doc="""\
            The image location.
            """,
        ): Nullable(str),
        Optional(
            'name',
            doc="""\
            The image name.
            """,
        ): Nullable(str),
        Optional(
            'tags',
            doc="""\
            The image tags.
            """,
        ): Nullable({str: str}),
    }
    azure_gallery_image = {
        Required(
            'gallery-name',
            doc="""\
            The image gallery name.
            """,
        ): str,
        Required(
            'name',
            doc="""\
            The image name.
            """,
        ): str,
        Optional(
            'version',
            doc="""\
            The image version.  Omit to use the latest version.
            """,
        ): Nullable(str),
    }
    # This is used here and in flavors and labels
    inheritable_azure_image_schema = assemble(
        vs.Schema({
            Optional(
                'volume-size',
                doc="""\
                The size of the operating system disk, in GiB.
                """,
            ): Nullable(int),
            Optional(
                'ephemeral-disk',
                doc="""\
                If set to ``true``, Azure will create an ephemeral OS disk
                instead of a managed disk.
                """,
            ): Nullable(bool),
            Optional(
                'generate-password',
                doc="""\
                If booting a Windows image, an administrative password is
                required.  If the password is not actually used (e.g., the
                image has key-based authentication enabled), a random
                password can be provided by enabling this option.  The
                password is not stored anywhere and is not retrievable.
                """,
            ): Nullable(bool),
        }),
        provider_schema.cloud_image,
    )
    azure_cloud_schema = vs.Schema({
        Exclusive(Required(
            'image-filter',
            doc="""\
            Specifies a private image to use via filters.  Either this field,
            :attr:`provider[azure].images[cloud].shared-gallery-image`,
            :attr:`provider[azure].images[cloud].community-gallery-image`,
            :attr:`provider[azure].images[cloud].image-reference`, or
            :attr:`provider[azure].images[cloud].image-id` must be
            provided.

            If a filter is provided, Zuul will list all of the images
            in the provider's resource group and reduce the list using
            the supplied filter.  All items specified in the filter must
            match in order for an image to match.  If more than one image
            matches, the images are sorted by name and the last one
            matches.

            The following filters are available:
            """,
        ), 'spec'): azure_image_filter,
        Exclusive(Required(
            'image-id',
            doc="""\
            Specifies a private image to use by ID.  Either this field,
            :attr:`provider[azure].images[cloud].shared-gallery-image`,
            :attr:`provider[azure].images[cloud].community-gallery-image`,
            :attr:`provider[azure].images[cloud].image-reference`, or
            :attr:`provider[azure].images[cloud].image-filter` must be
            provided.
            """,
        ), 'spec'): str,
        Exclusive(Required(
            'image-reference',
            doc="""\
            Specifies a public image to use by reference.  Either this field,
            :attr:`provider[azure].images[cloud].shared-gallery-image`,
            :attr:`provider[azure].images[cloud].community-gallery-image`,
            :attr:`provider[azure].images[cloud].image-id`, or
            :attr:`provider[azure].images[cloud].image-filter` must be
            provided.
            """,
        ), 'spec'): azure_image_reference,
        Exclusive(Required(
            'community-gallery-image',
            doc="""\
            Specifies a community gallery image to use.  Either this field,
            :attr:`provider[azure].images[cloud].shared-gallery-image`,
            :attr:`provider[azure].images[cloud].image-reference`,
            :attr:`provider[azure].images[cloud].image-id`, or
            :attr:`provider[azure].images[cloud].image-filter` must be
            provided.
            """,
        ), 'spec'): azure_gallery_image,
        Exclusive(Required(
            'shared-gallery-image',
            doc="""\
            Specifies a shared gallery image to use.  Either this field,
            :attr:`provider[azure].images[cloud].community-gallery-image`,
            :attr:`provider[azure].images[cloud].image-reference`,
            :attr:`provider[azure].images[cloud].image-id`, or
            :attr:`provider[azure].images[cloud].image-filter` must be
            provided.
            """,
        ), 'spec'): azure_gallery_image,
    })
    main_cloud_schema = assemble(
        BaseProviderImage.cloud_schema,
        azure_cloud_schema,
        inheritable_azure_image_schema,
    )
    internal_main_cloud_schema = assemble(
        main_cloud_schema,
        provider_schema.internal_base_image,
    )
    cloud_schema_exclusion = RequiredExclusive(
        'image_id', 'image_reference', 'image_filter',
        'community_gallery_image', 'shared_gallery_image',
        msg=('Provide one of "image-id", '
             '"image-reference", "image-filter", '
             '"community-gallery-image", or '
             '"shared-gallery-image" keys'))
    cloud_schema = vs.All(
        main_cloud_schema,
        cloud_schema_exclusion,
    )
    internal_cloud_schema = vs.All(
        internal_main_cloud_schema,
        cloud_schema_exclusion,
    )
    zuul_schema = assemble(
        BaseProviderImage.zuul_schema,
        inheritable_azure_image_schema,
    )
    internal_zuul_schema = assemble(
        zuul_schema,
        provider_schema.internal_base_image,
    )
    inheritable_cloud_schema = assemble(
        BaseProviderImage.inheritable_cloud_schema,
        inheritable_azure_image_schema,
    )
    inheritable_zuul_schema = assemble(
        BaseProviderImage.inheritable_zuul_schema,
        inheritable_azure_image_schema,
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

    def __init__(self, image_config, provider_config):
        self.image_id = None
        self.image_reference = None
        self.image_filter = None
        self.community_gallery_image = None
        self.shared_gallery_image = None
        super().__init__(image_config, provider_config)
        self.format = 'vhd'
        # Implement provider defaults
        if self.connection_type is None:
            self.connection_type = 'ssh'
        if self.connection_port is None:
            self.connection_port = 22


class AzureProviderFlavor(BaseProviderFlavor):
    azure_flavor_schema = vs.Schema({
        Required(
            'vm-size',
            doc=f"""\
            Size of the VM to use in Azure.  See the `List of VM
            sizes`_ for the list of sizes availabile in each region.

            .. _`List of VM sizes`: {URL_VM_SIZES}
            """,
        ): str,
        # TODO: add "low" priority
        Optional(
            'priority',
            doc="""\
            Whether to create regular or spot instances.
            """,
            default='regular'
        ): vs.Any(
            Constant(
                'regular',
                doc="""\
                A regular instance.
                """,
            ),
            Constant(
                'spot',
                doc="""\
                A spot instance.
                """,
            ),
        ),
        Optional(
            'ipv4',
            doc="""\
            Whether to enable IPv4 networking.  Defaults to true unless IPv6
            is enabled.  Enabling this will attach a private IPv4 address.
            """,
            default=False): bool,
        Optional(
            'ipv6',
            doc="""\
            Whether to enable IPv6 networking.
            Enabling this will attach a private IPv6 address.
            """,
            default=False): bool,
    })

    inheritable_schema = assemble(
        BaseProviderFlavor.inheritable_schema,
        # This is already included via the image, but listed again
        # here for clarity.
        AzureProviderImage.inheritable_azure_image_schema,
        provider_schema.cloud_flavor,
    )
    schema = assemble(
        BaseProviderFlavor.schema,
        provider_schema.cloud_flavor,
        AzureProviderImage.inheritable_azure_image_schema,
        azure_flavor_schema,
    )
    internal_schema = assemble(
        schema,
        provider_schema.internal_base_flavor,
    )


class AzureProviderLabel(BaseProviderLabel):
    azure_network_reference = {
        Optional(
            'resource-group',
            doc="""\
            The resource group that contains the subnet.
            """,
        ): Nullable(str),
        Required(
            'network',
            doc="""\
            The name of the subnet's network.
            """,
        ): str,
        Optional(
            'subnet',
            doc="""\
            The name of the subnet.
            """,
            default='default'): str,
    }

    azure_identity_reference = {
        Optional(
            'resource-group',
            doc="""\
            The resource group that contains the identity.
            """,
        ): Nullable(str),
        Required(
            'name',
            doc="""\
            The name of the identity.
            """,
        ): str,
    }

    azure_label_schema = vs.Schema({
        Exclusive(Required(
            'subnet-id',
            doc="""\
            Specifies the subnet to use by ID.
            """,
        ), 'subnet'): str,
        Exclusive(Required(
            'subnet-reference',
            doc="""\
            Specifies the subnet to use by reference
            """,
        ), 'subnet'): azure_network_reference,
        Optional(
            'user-assigned-identities',
            doc="""\
            """,
            default=[],
        ): AsList(azure_identity_reference),
        Optional(
            'key-data',
            doc="""\
            The SSH public key that should be installed on the node.
            """,
        ): Nullable(str),
    })

    inheritable_schema = assemble(
        BaseProviderLabel.inheritable_schema,
        # This is already included via the image, but listed again
        # here for clarity.
        AzureProviderImage.inheritable_azure_image_schema,
        provider_schema.host_key_checking,
        azure_label_schema,
    )

    main_schema = assemble(
        BaseProviderLabel.schema,
        AzureProviderImage.inheritable_azure_image_schema,
        provider_schema.host_key_checking,
        azure_label_schema,
    )

    internal_main_schema = assemble(
        main_schema,
        provider_schema.internal_base_label,
    )

    main_schema_exclusion = RequiredExclusive(
        'subnet_id', 'subnet_reference',
        msg=('Provide one of "subnet-id" or '
             '"subnet-reference" keys'))

    schema = vs.All(
        main_schema,
        main_schema_exclusion,
    )

    internal_schema = vs.All(
        internal_main_schema,
        main_schema_exclusion,
    )

    image_flavor_inheritable_schema = assemble(
        AzureProviderImage.inheritable_azure_image_schema,
    )

    def __init__(self, label_config, provider_config):
        self.subnet_id = None
        self.subnet_reference = None
        super().__init__(label_config, provider_config)


class AzureProviderSchema(BaseProviderSchema):
    def getLabelSchema(self, internal=False):
        if internal:
            return AzureProviderLabel.internal_schema
        else:
            return AzureProviderLabel.schema

    def getImageSchema(self, internal=False):
        if internal:
            return AzureProviderImage.internal_schema
        else:
            return AzureProviderImage.schema

    def getFlavorSchema(self, internal=False):
        if internal:
            return AzureProviderFlavor.internal_schema
        else:
            return AzureProviderFlavor.schema

    def getInheritableLabelSchema(self):
        return AzureProviderLabel.inheritable_schema

    def getInheritableImageSchema(self):
        return AzureProviderImage.inheritable_schema

    def getInheritableZuulImageSchema(self):
        return AzureProviderImage.inheritable_zuul_schema

    def getInheritableCloudImageSchema(self):
        return AzureProviderImage.inheritable_cloud_schema

    def getInheritableFlavorSchema(self):
        return AzureProviderFlavor.inheritable_schema

    def getZuulImageSchema(self):
        return AzureProviderImage.zuul_schema

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
                doc="""The number of cores used by regular instances.""",
            ): int,
            Optional(
                'ram',
                default=vs.UNDEFINED,
                doc="""The amount of ram, in MiB.""",
            ): int,
            Optional(
                'lowPriorityCores',
                default=vs.UNDEFINED,
                doc="""\
                The number of low priority cores (including spot instances).
                """,
            ): int,
        }

        azure_provider_schema = vs.Schema({
            Required(
                'region',
                doc="""Name of the Azure region to use.""",
            ): str,
            Required(
                'resource-group',
                doc="""\
                Name of the resource group in which to place nodes.
                """,
            ): str,
            Optional(
                'resource-limits',
                doc="""\
                Resource limits for this provider.  Configure these
                values to cause Zuul to attempt to limit the resource
                usage.  This can be used to limit Zuul's usage to a
                level below the cloud quota.
                """,
                default=dict()): resource_limits,
        })

        return assemble(
            schema,
            azure_provider_schema,
            doc="""\
            The attributes available for configuring an Azure provider
            are below.
            """,
        )


class AzureProvider(BaseProvider, subclass_id='azure'):
    log = logging.getLogger("zuul.AzureProvider")
    schema = AzureProviderSchema().getProviderSchema()
    internal_schema = AzureProviderSchema().getProviderSchema(internal=True)

    @property
    def endpoint(self):
        ep = getattr(self, '_endpoint', None)
        if ep:
            return ep
        self._set(_endpoint=self.getEndpoint())
        return self._endpoint

    def parseImage(self, image_config, provider_config, connection):
        return AzureProviderImage(image_config, provider_config)

    def parseFlavor(self, flavor_config, provider_config, connection):
        return AzureProviderFlavor(flavor_config, provider_config)

    def parseLabel(self, label_config, provider_config, connection):
        return AzureProviderLabel(label_config, provider_config)

    def getEndpoint(self):
        return self.driver.getEndpoint(self)

    def getCreateStateMachine(self, node, image_external_id, log):
        # TODO: decide on a method of producing a hostname
        # that is max 15 chars.
        hostname = f"np{node.uuid[:13]}"
        label = self.labels[node.label]
        flavor = self.flavors[label.flavor]
        image = self.images[label.image]
        return AzureCreateStateMachine(
            self,
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
        return AzureDeleteStateMachine(self.endpoint, node, log)

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
            self.resource_group, provider_image, image_name,
            image_format, metadata, md5, sha256)

    def deleteImage(self, external_id):
        self.endpoint.deleteImage(external_id)
