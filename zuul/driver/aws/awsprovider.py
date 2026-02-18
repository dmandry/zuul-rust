# Copyright 2022-2025 Acme Gating, LLC
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

from zuul.driver.aws.awsendpoint import (
    AwsCreateStateMachine,
    AwsDeleteStateMachine,
    AwsSnapshotStateMachine,
)
from zuul.driver.aws.const import (
    SPOT,
    ON_DEMAND,
    VOLUME_QUOTA_CODES,
    ALL_QUOTA_CODES,
)
from zuul.model import QuotaInformation
from zuul.provider import (
    BaseProvider,
    BaseProviderFlavor,
    BaseProviderImage,
    BaseProviderLabel,
    BaseProviderSchema,
)

URL_BOTO_DESCRIBE_IMAGES = 'https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.describe_images'  # noqa
URL_EBS_VOLUME_TYPE = 'https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSVolumeTypes.html'  # noqa
URL_EBS_DIRECT_API = 'https://docs.aws.amazon.com/ebs/latest/userguide/ebs-accessing-snapshot.html'  # noqa
URL_REGISTER_IMAGE = 'https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_RegisterImage.html'  # noqa
URL_AWS_REGION = 'https://docs.aws.amazon.com/general/latest/gr/rande.html'  # noqa


class AwsProviderImage(BaseProviderImage):
    aws_image_filters = {
        Optional(
            'name',
            doc=f"""\
            The filter name. See `Boto describe images`_ for a list of
            valid filters.

            .. _`Boto describe images`: {URL_BOTO_DESCRIBE_IMAGES}
            """,
        ): Nullable(str),
        Optional(
            'values',
            doc="""A list of string values on which to filter.""",
        ): Nullable(AsList(str)),
    }
    # This is used here and in flavors and labels
    inheritable_aws_image_schema = assemble(
        vs.Schema({
            Optional(
                'volume-size',
                doc="""\
                The size of the root EBS volume, in GiB, for the
                image.  If omitted, the volume size reported for the
                imported snapshot will be used.  Only used with the
                :value:`provider[aws].images[zuul].import-method.snapshot` or
                :value:`provider[aws].images[zuul].import-method.ebs-direct`
                import methods.""",
            ): Nullable(int),
            Optional(
                'volume-type',
                doc=f"""\
                The root `EBS volume type`_ for the image.
                Only used with the
                :value:`provider[aws].images[zuul].import-method.snapshot` or
                :value:`provider[aws].images[zuul].import-method.ebs-direct`
                import methods.

                .. _`EBS volume type`: {URL_EBS_VOLUME_TYPE}
                """,
                default='gp3',
            ): str,
            Optional(
                'iops',
                doc="""\
                The number of I/O operations per second to be provisioned for
                the volume.  The default varies based on the volume type; see
                the documentation under `EBS volume type`_ for the specific
                volume type for details.
                """,
            ): Nullable(int),
            Optional(
                'throughput',
                doc="""\
                The throughput of the volume in MiB/s.  This is only valid for
                ``gp3`` volumes.
                """,
            ): Nullable(int),
            Optional(
                'imds-http-tokens',
                doc="""\
                Specify whether IMDSv2 is required.  If this is omitted,
                then AWS defaults are used (usually equivalent to
                ``optional`` but may be influenced by the image used).
                """,
            ): Nullable(vs.Any(
                Constant(
                    'optional',
                    doc="""\
                    Allows usage of IMDSv2 but do not require it.  This
                    sets the following metadata options:

                    * `HttpTokens` is `optional`
                    * `HttpEndpoint` is `enabled`
                    """),
                Constant(
                    'required',
                    doc="""\
                    Require IMDSv2.  This sets the following metadata
                    options:

                    * `HttpTokens` is `required`
                    * `HttpEndpoint` is `enabled`
                    """),
            )),
        }),
        provider_schema.cloud_image,
    )
    aws_cloud_schema = vs.Schema({
        Exclusive(
            Required(
                'image-id',
                doc="""\
                If this is provided, it is used to select the AMI
                from AWS by ID.  Either this field or
                :attr:`provider[aws].images[cloud].image-filters`
                must be provided.
                """,
            ), 'spec'): str,
        Exclusive(
            Required(
                'image-filters',
                doc="""\
                If provided, this is used to select an AMI by filters.  If
                the filters provided match more than one image, the most
                recent will be returned.  Either this field or
                :attr:`provider[aws].images[cloud].image-id` must be
                provided.

                This field may be provided as a dictionary, or a list
                of dictionaries with the following keys:
                """,
            ), 'spec'): AsList(aws_image_filters),
    })
    cloud_schema = vs.All(
        assemble(
            BaseProviderImage.cloud_schema,
            aws_cloud_schema,
            inheritable_aws_image_schema,
        ),
        RequiredExclusive('image_id', 'image_filters',
                          msg=('Provide either '
                               '"image-filters", or "image-id" keys'))
    )
    inheritable_aws_zuul_schema = vs.Schema({
        Optional(
            'import-method',
            doc="""The method to use when importing the image.""",
            default='snapshot'
        ): vs.Any(
            Constant(
                'snapshot',
                doc="""\
                This method uploads the image file to AWS as a snapshot
                and then registers an AMI directly from the snapshot.
                This is faster compared to the `image` method and may be
                used with operating systems and versions that AWS does not
                otherwise support.  However, it is incompatible with some
                operating systems which require special licensing or other
                metadata in AWS.
                """,
            ),
            Constant(
                'image',
                doc="""\
                This method uploads the image file to AWS and performs
                an "image import" on the file.  This causes AWS to
                boot the image in a temporary VM and then take a
                snapshot of that VM which is then used as the basis of
                the AMI.  This is slower compared to the `snapshot`
                method and may only be used with operating systems and
                versions which AWS already supports.  This may be
                necessary in order to use Windows images.
                """,
            ),
            Constant(
                'ebs-direct',
                doc=f"""\
                This is similar to the `snapshot` method, but uses the
                `EBS direct API`_ instead of S3.  This may be faster and
                more efficient, but it may incur additional costs.

                .. _`EBS direct API`: {URL_EBS_DIRECT_API}
                """,
            ),
        ),
        Optional(
            'image-format',
            doc="""\
            The image format that should be used when building and
            uploading or importing the image.
            """,
            default='raw',
        ): vs.Any(
            Constant(
                'ova',
                doc="""The OVA image format.""",
            ),
            Constant(
                'vhd',
                doc="""The VHD image format.""",
            ),
            Constant(
                'vhdx',
                doc="""The VHDX image format.""",
            ),
            Constant(
                'vmdk',
                doc="""The VMDK image format.""",
            ),
            Constant(
                'raw',
                doc="""A raw image.""",
            ),
            Constant(
                'snapshot',
                doc="""\
                Rather than producing an image artifact and
                uploading or importing it, this image is created by
                snapshotting a running instance.
                """,
            )
        ),
        # None is an acceptable explicit value for imds-support
        Optional(
            'imds-support',
            doc="""\
            Controls the usage of IMDSv2 on instances created from the
            image, This is only supported using the
            :value:`provider[aws].images[zuul].import-method.snapshot` or
            :value:`provider[aws].images[zuul].import-method.ebs-direct`
            import methods.
            """,
            default=None
        ): vs.Any(
            Constant(
                'v2.0',
                doc="""\
                Enforces usage of IMDSv2 by default on instances
                created from the image.
                """,
            ),
            Constant(
                None,
                doc="""IMDSv2 is optional.""",
            ),
        ),
        Optional(
            'architecture',
            doc=f"""\
            The architecture of the image.  See the `AWS RegisterImage API
            documentation`_ for valid values.

            .. _`AWS RegisterImage API documentation`: {URL_REGISTER_IMAGE}
            """,
            default='x86_64'): str,
        Optional(
            'ena-support',
            doc="""\
            Whether the image has support for the AWS Enhanced Networking
            Adapter (ENA).  Many newer operating systems include driver
            support as standard and some AWS instance types require it.
            """,
            default=True): bool,
    })
    zuul_schema = assemble(
        BaseProviderImage.zuul_schema,
        inheritable_aws_image_schema,
        inheritable_aws_zuul_schema,
    )
    inheritable_cloud_schema = assemble(
        BaseProviderImage.inheritable_cloud_schema,
        inheritable_aws_image_schema,
    )
    inheritable_zuul_schema = assemble(
        BaseProviderImage.inheritable_zuul_schema,
        inheritable_aws_image_schema,
        inheritable_aws_zuul_schema,
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

    def __init__(self, image_config, provider_config):
        self.image_id = None
        self.image_filters = None
        super().__init__(image_config, provider_config)
        self.format = getattr(self, 'image_format', None)
        # Implement provider defaults
        if self.connection_type is None:
            self.connection_type = 'ssh'
        if self.connection_port is None:
            self.connection_port = 22


class AwsProviderFlavor(BaseProviderFlavor):
    fleet_schema = vs.Schema({
        Required(
            'instance-types',
            doc="""\
            A list of instance types from which AWS may select when
            launching the instance.""",
        ): AsList(str),
        Required(
            'allocation-strategy',
            doc="""\
            The allocation strategy to use when launching the instance.
            """,
        ): vs.Any(
            Constant(
                'prioritized',
                doc="""\
                The prioritized allocation strategy.  Available for
                on-demand instances""",
            ),
            Constant(
                'price-capacity-optimized',
                doc="""\
                The price-capacity-optimized allocation strategy.
                Available for spot instances""",
            ),
            Constant(
                'capacity-optimized',
                doc="""\
                The capacity-optimized allocation strategy.  Available
                for spot instances""",
            ),
            Constant(
                'diversified',
                doc="""\
                The diversified allocation strategy.  Available for
                spot instances""",
            ),
            Constant(
                'lowest-price',
                doc="""\
                The lowest-price allocation strategy.  Available for
                spot or on-demand instances""",
            ),
        )
    })

    aws_flavor_schema = vs.Schema({
        Exclusive(Required(
            'instance-type',
            doc="""\
            Name of the AWS instance type to use..
            Mutually exclusive with :attr:`provider[aws].flavors.fleet`.
            """,
        ), 'instance'): str,
        Optional(
            'dedicated-host',
            doc="""\
            If set to ``true``, an AWS dedicated host will be
            allocated for the instance.  The host may be used for one
            or more nodes depending on the settings of
            :attr:`provider[aws].labels.slots` and
            :attr:`provider[aws].labels.reuse`.

            If this option is set, the
            :value:`provider[aws].flavors.market-type.spot` option is
            not available, and :attr:`provider[aws].labels.az` option is
            required.""",
            default=False): bool,
        Optional(
            'ebs-optimized',
            doc="""\
            Indicates whether EBS optimization (additional, dedicated
            throughput between Amazon EC2 and Amazon EBS) should be
            enabled for the instance.""",
            default=False): bool,
        Optional(
            'market-type',
            doc="""\
            Whether to request an on-demand or spot instance.
            """,
            default='on-demand'
        ): vs.Any(
            Constant(
                'on-demand',
                doc="""\
                This is the typical EC2 instance where continued
                availability is guaranteed after allocation.""",
            ),
            Constant(
                'spot',
                doc="""\
                Request an Amazon EC2 Spot instance instead of an
                On-Demand instance.  Spot instances take advantage of
                unused EC2 capacity at a discount, but if demand is
                high, instances may be unavailable.  In addition,
                Amazon EC2 may interrupt Spot instances and reclaim
                them.  Alternative nodesets with On-Demand instances
                configured as a fallback may be configured in order to
                mitigate this.""",
            ),
        ),
        Exclusive(Required(
            'fleet',
            doc="""\
            If specified, the EC2 Fleet API will be used for launching
            the instance.  In this case, quota is not checked before
            launching the instance, but is taken into account after
            the instance is launched.  Mutually exclusive with
            :attr:`provider[aws].flavors.instance-type`.
            """,
        ), 'instance'): fleet_schema,
    })

    inheritable_schema = assemble(
        BaseProviderFlavor.inheritable_schema,
        # This is already included via the image, but listed again
        # here for clarity.
        AwsProviderImage.inheritable_aws_image_schema,
        provider_schema.cloud_flavor,
    )
    schema = vs.All(
        assemble(
            BaseProviderFlavor.schema,
            provider_schema.cloud_flavor,
            AwsProviderImage.inheritable_aws_image_schema,
            aws_flavor_schema,
        ),
        RequiredExclusive('instance_type', 'fleet',
                          msg=('Provide either '
                               '"instance-type", or "fleet" keys'))
    )

    def __init__(self, flavor_config, provider_config):
        self.instance_type = None
        self.fleet = None
        super().__init__(flavor_config, provider_config)


class AwsProviderLabel(BaseProviderLabel):
    aws_iam_schema = vs.All(
        vs.Schema({
            Exclusive(Required(
                'name',
                doc="""\
                Name of the instance profile.  Mutually exclusive with
                :attr:`provider[aws].labels.iam-instance-profile.arn`
                """,
            ), 'iam'): str,
            Exclusive(Required(
                'arn',
                doc="""\
                ARN identifier of the profile.  Mutually exclusive
                with
                :attr:`provider[aws].labels.iam-instance-profile.name`
                """,
            ), 'iam'): str
        }),
        RequiredExclusive('name', 'arn',
                          msg=('Provide either "name", or "arn" keys'))
    )
    aws_label_schema = vs.Schema({
        Optional(
            'az',
            doc="""\
            Instances will be assigned to the specified availibility
            zone.  If omitted, AWS will select from the available
            zones.
            """,
        ): Nullable(str),
        Optional(
            'security-group-ids',
            doc="""\
            Specifies the security group IDs to assign to the node's
            network interfaces.
            """,
        ): Nullable(AsList(str)),
        Optional(
            'subnet-ids',
            doc="""\
            Specifies the subnets to assign to the node's network interfaces.
            """,
            default=[]): AsList(str),
        Optional(
            'iam-instance-profile',
            doc="""\
            Used to attach an IAM instance profile.
            Useful for giving access to services without needing any secrets.
            """,
        ): Nullable(aws_iam_schema),
        # This could make sense for image as well, but currently is
        # only included in label.
        Optional(
            'kms-key-id',
            doc="""\
            The KMS key id to use when launching instances from an
            encrypted image.  Typically only necessary when using
            images shared from a different AWS account.
            """,
        ): Nullable(str),
    })

    inheritable_schema = assemble(
        BaseProviderLabel.inheritable_schema,
        # This is already included via the image, but listed again
        # here for clarity.
        AwsProviderImage.inheritable_aws_image_schema,
        provider_schema.ssh_label,
        aws_label_schema,
    )
    schema = assemble(
        BaseProviderLabel.schema,
        AwsProviderImage.inheritable_aws_image_schema,
        provider_schema.ssh_label,
        aws_label_schema,
    )

    image_flavor_inheritable_schema = assemble(
        AwsProviderImage.inheritable_aws_image_schema,
    )


class AwsProviderSchema(BaseProviderSchema):
    def getLabelSchema(self):
        return AwsProviderLabel.schema

    def getImageSchema(self):
        return AwsProviderImage.schema

    def getFlavorSchema(self):
        return AwsProviderFlavor.schema

    def getInheritableLabelSchema(self):
        return AwsProviderLabel.inheritable_schema

    def getInheritableImageSchema(self):
        return AwsProviderImage.inheritable_schema

    def getInheritableZuulImageSchema(self):
        return AwsProviderImage.inheritable_zuul_schema

    def getInheritableCloudImageSchema(self):
        return AwsProviderImage.inheritable_cloud_schema

    def getInheritableFlavorSchema(self):
        return AwsProviderFlavor.inheritable_schema

    def getProviderSchema(self):
        schema = super().getProviderSchema()
        object_storage = {
            Required(
                'bucket-name',
                doc="""\
                The name of the S3 bucket that should be used when
                importing Zuul images.
                """,
            ): str,
        }

        resource_limits = {k: int for k in ALL_QUOTA_CODES}
        resource_limits[Optional(
            'instances',
            default=vs.UNDEFINED,
            doc="""The number of instances.""",
        )] = int
        resource_limits[Optional(
            'cores',
            default=vs.UNDEFINED,
            doc="""The number of cores.""",
        )] = int
        resource_limits[Optional(
            'ram',
            default=vs.UNDEFINED,
            doc="""The amount of ram, in MiB.""",
        )] = int

        aws_provider_schema = vs.Schema({
            Required(
                'region',
                doc=f"""\
                The name of the `AWS region`_ to interact with.

                .. _`AWS region`: {URL_AWS_REGION}
                """,
            ): str,
            Optional(
                'object-storage',
                doc="""\
                Configuration options related to object storage used
                for image management.
                """,
            ): Nullable(object_storage),
            Optional(
                'resource-limits',
                doc="""\
                Resource limits for this provider.  Configure these
                values to cause Zuul to attempt to limit the resource
                usage.  This can be used to limit Zuul's usage to a
                level below the cloud quota.

                In addition to the options listed below, it is
                possible to configure a limit for any of the quota
                codes supported by AWS.  """,
                default=dict(),
            ): resource_limits,
        })

        return assemble(
            schema,
            aws_provider_schema,
            doc="""\
            The attributes available for configuring an AWS provider
            are below.
            """,
        )


class AwsProvider(BaseProvider, subclass_id='aws'):
    log = logging.getLogger("zuul.AwsProvider")
    schema = AwsProviderSchema().getProviderSchema()

    @property
    def endpoint(self):
        ep = getattr(self, '_endpoint', None)
        if ep:
            return ep
        self._set(_endpoint=self.getEndpoint())
        return self._endpoint

    def parseImage(self, image_config, provider_config, connection):
        return AwsProviderImage(image_config, provider_config)

    def parseFlavor(self, flavor_config, provider_config, connection):
        return AwsProviderFlavor(flavor_config, provider_config)

    def parseLabel(self, label_config, provider_config, connection):
        return AwsProviderLabel(label_config, provider_config)

    def getEndpoint(self):
        return self.driver.getEndpoint(self)

    def validateConfig(self, config):
        for label in config['labels'].values():
            flavor = config['flavors'][label.flavor]
            if flavor.dedicated_host:
                if flavor.market_type == 'spot':
                    raise Exception(
                        f'Label "{label.name}": spot instances can not '
                        'be used on dedicated hosts')
                if not label.az:
                    raise Exception(
                        f'Label "{label.name}": availability-zone is '
                        'required for dedicated hosts')

    def getCreateStateMachine(self, node, image_external_id, log):
        # TODO: decide on a method of producing a hostname
        # that is max 15 chars.
        hostname = f"np{node.uuid[:13]}"
        label = self.labels[node.label]
        flavor = self.flavors[label.flavor]
        image = self.images[label.image]
        return AwsCreateStateMachine(
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
        return AwsDeleteStateMachine(self.endpoint, node, log)

    def getSnapshotStateMachine(self, node, log):
        return AwsSnapshotStateMachine(self.endpoint, node, log)

    def getEndpointLimits(self):
        # Get the instance and volume types that this provider handles
        limits = self.endpoint.quota_cache.getLimits()
        if limits is None:
            limits = {}
        else:
            limits = limits.quota
        instance_types = {}
        host_types = set()
        volume_types = set()
        for flavor in self.flavors.values():
            if flavor.dedicated_host:
                host_types.add(flavor.instance_type)
            else:
                flavor_instance_types = []
                if flavor.instance_type:
                    flavor_instance_types.append(flavor.instance_type)
                elif flavor.fleet and flavor.fleet.get('instance-types'):
                    # Include instance-types from fleet config if available
                    flavor_instance_types.extend(
                        flavor.fleet['instance-types'])
                for flavor_instance_type in flavor_instance_types:
                    if flavor_instance_type not in instance_types:
                        instance_types[flavor_instance_type] = set()
                    instance_types[flavor_instance_type].add(
                        SPOT if flavor.market_type == 'spot' else ON_DEMAND)
            if flavor.volume_type:
                volume_types.add(flavor.volume_type)
        args = dict(default=math.inf)
        for instance_type in instance_types:
            for market_type_option in instance_types[instance_type]:
                code = self.endpoint._getQuotaCodeForInstanceType(
                    instance_type, market_type_option)
                if code in args:
                    continue
                if not code:
                    continue
                if code not in limits:
                    self.log.warning(
                        "AWS quota code %s for instance type: %s not known",
                        code, instance_type)
                    continue
                args[code] = limits[code]
        for host_type in host_types:
            code = self.endpoint._getQuotaCodeForHostType(host_type)
            if code in args:
                continue
            if not code:
                continue
            if code not in limits:
                self.log.warning(
                    "AWS quota code %s for host type: %s not known",
                    code, host_type)
                continue
            args[code] = limits[code]
        for volume_type in volume_types:
            vquota_codes = VOLUME_QUOTA_CODES.get(volume_type)
            if not vquota_codes:
                self.log.warning(
                    "Unknown quota code for volume type: %s",
                    volume_type)
                continue
            for resource, code in vquota_codes.items():
                if code in args:
                    continue
                if code not in limits:
                    self.log.warning(
                        "AWS quota code %s for volume type: %s not known",
                        code, volume_type)
                    continue
                value = limits[code]
                # Unit mismatch: storage limit is in TB, but usage
                # is in GB.  Translate the limit to GB.
                if resource == 'storage':
                    value *= 1000
                args[code] = value

        return QuotaInformation(**args)

    def getQuotaForLabel(self, label):
        flavor = self.flavors[label.flavor]
        return self.endpoint.getQuotaForLabel(label, flavor)

    def refreshQuotaForLabel(self, label, update):
        flavor = self.flavors[label.flavor]
        return self.endpoint.refreshQuotaForLabel(label, flavor, update)

    def downloadUrl(self, url, path):
        return self.endpoint.downloadUrl(url, path)

    def getImageImportJob(self, url, provider_image, image_name,
                          image_format, metadata, md5, sha256):
        return self.endpoint.getImageImportJob(
            url, provider_image, image_name,
            image_format, metadata, md5, sha256)

    def getImageCopyJob(self, source_provider, provider_image, image_name,
                        image_format, metadata, md5, sha256):
        return self.endpoint.getImageCopyJob(
            source_provider, provider_image, image_name,
            image_format, metadata, md5, sha256)

    def getImageUploadJob(self, provider_image, image_name,
                          image_format, metadata, md5, sha256):
        # TODO this needs to move to the section or connection config
        # since it's used by endpoints.
        bucket_name = self.object_storage.get('bucket_name')
        return self.endpoint.getImageUploadJob(
            provider_image, image_name,
            image_format, metadata, md5, sha256,
            bucket_name)

    def deleteImage(self, external_id):
        self.endpoint.deleteImage(external_id)
