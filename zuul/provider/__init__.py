# Copyright 2024-2026 Acme Gating, LLC
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

import abc
import json
import math
import threading
import urllib.parse

from zuul.lib.voluputil import (
    Constant,
    Nullable,
    Optional,
    Required,
    assemble,
    discriminate,
)
from zuul import model
from zuul.zk import zkobject
from zuul.zk.quotacache import QuotaCache
import zuul.provider.schema as provider_schema

import voluptuous as vs


class CNameMixin:

    @property
    def canonical_name(self):
        return '/'.join([
            urllib.parse.quote_plus(
                self.project_canonical_name),
            urllib.parse.quote_plus(self.name),
        ])


class BaseProviderImage(CNameMixin, metaclass=abc.ABCMeta):
    inheritable_cloud_schema = assemble(
        provider_schema.common_image,
    )
    inheritable_zuul_schema = assemble(
        provider_schema.common_image,
        provider_schema.common_image_zuul,
    )
    cloud_schema = assemble(
        provider_schema.common_image,
        provider_schema.base_image,
        vs.Schema({Required(
            'type',
            doc="""The type of image.""",
        ): vs.Any(Constant(
            'cloud',
            doc="""An existing image available in the cloud.""",
        ))}),
        doc="These are the attributes available for a cloud image.",
    )
    internal_cloud_schema = assemble(
        cloud_schema,
        provider_schema.internal_base_image,
        extra=vs.ALLOW_EXTRA,
    )
    zuul_schema = assemble(
        provider_schema.common_image,
        provider_schema.common_image_zuul,
        provider_schema.base_image,
        vs.Schema({Required(
            'type',
            doc="""The type of image.""",
        ): vs.Any(Constant(
            'zuul',
            doc="""An image managed by Zuul.""",
        ))}),
        doc="These are the attributes available for a Zuul image.",
    )
    internal_zuul_schema = assemble(
        zuul_schema,
        provider_schema.internal_base_image,
        extra=vs.ALLOW_EXTRA,
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
        new_config = image_config.copy()
        self.__dict__.update(self.internal_schema(new_config))


class BaseProviderFlavor(CNameMixin, metaclass=abc.ABCMeta):
    inheritable_schema = assemble(
        provider_schema.common_flavor,
    )
    schema = assemble(
        provider_schema.common_flavor,
        provider_schema.base_flavor,
    )
    internal_schema = assemble(
        schema,
        provider_schema.internal_base_flavor,
        extra=vs.ALLOW_EXTRA,
    )

    def __init__(self, flavor_config, provider_config):
        new_config = flavor_config.copy()
        self.__dict__.update(self.internal_schema(new_config))


class BaseProviderLabel(CNameMixin, metaclass=abc.ABCMeta):
    inheritable_schema = assemble(
        provider_schema.common_label,
    )
    schema = assemble(
        provider_schema.common_label,
        provider_schema.base_label,
    )
    internal_schema = assemble(
        schema,
        provider_schema.internal_base_label,
        extra=vs.ALLOW_EXTRA,
    )
    image_flavor_inheritable_schema = assemble()

    def __init__(self, label_config, provider_config):
        new_config = label_config.copy()
        self.__dict__.update(self.internal_schema(new_config))

    def __repr__(self):
        return (f"<{self.__class__.__name__} "
                f"canonical_name={self.canonical_name} >")

    def inheritFrom(self, image, flavor):
        # Some label attributes should default to values specified in
        # a flavor or image (for example, volume types) but can be
        # overridden by a label.  This method implements that
        # inheritance, using the attributes specified in
        # image_label_inhertable_schema.
        for attr in self.image_flavor_inheritable_schema.schema:
            # Get the vs.Optional attribute mutation
            if hasattr(attr, 'output'):
                attr = attr.output
            if getattr(self, attr, None) is None:
                setattr(self, attr,
                        getattr(flavor, attr, None) or
                        getattr(image, attr, None))


class BaseProviderEndpoint(metaclass=abc.ABCMeta):
    """Base class for provider endpoints.

    Providers and Sections are combined to describe clouds, and they
    may not correspond exactly with the cloud's topology.  To
    reconcile this, the Endpoint class is used for storing information
    about what we would typically call a region of a cloud.  This is
    the unit of visibility of instances, VPCs, images, etc.
    """

    def __init__(self, driver, zk_client, connection, name, system_id):
        self.driver = driver
        self.zk_client = zk_client
        self.connection = connection
        self.name = name
        self.system_id = system_id
        self.start_lock = threading.Lock()
        self.started = False
        self.stopped = False
        self.quota_cache = QuotaCache(zk_client, name)

    def start(self):
        with self.start_lock:
            if not self.stopped and not self.started:
                self.startEndpoint()
                self.started = True

    def stop(self):
        with self.start_lock:
            if self.started:
                self.stopEndpoint()
            # Set the stopped flag regardless of whether we started so
            # that we won't start after stopping.
            self.stopped = True

    @property
    def canonical_name(self):
        return '/'.join([
            urllib.parse.quote_plus(self.connection.connection_name),
            urllib.parse.quote_plus(self.name),
        ])

    def startEndpoint(self):
        """Start the endpoint

        This method may start any threads necessary for the endpoint.

        """
        raise NotImplementedError()

    def stopEndpoint(self):
        """Stop the endpoint

        This method must stop all endpoint threads.
        """
        raise NotImplementedError()

    def refreshQuotaLimits(self, update):
        """Query the endpoint for quota limits and store them in the quota
        cache

        :param bool update: Whether an update should be performed even
            if there are values present.

        :return: Whether the cache was updated
        :rtype: bool

        """
        raise NotImplementedError()

    def postConfig(self, provider):
        """Perform any endpoint-global actions after reconfiguration

        This will be called any time a tenant layout is updated, once
        for each provider that uses the endpoint.  It may be called
        multiple times for the same update, and it may be called even
        when nothing about the provider or endpoint changed.

        """
        pass

    def listInstances(self):
        """Return an iterator of instances accessible to this endpoint.

        The yielded values should represent all instances accessible
        to this endpoint, not only those under the control of Zuul,
        but all visible instances in order to achive accurate quota
        calculation.

        :returns: A generator of :py:class:`Instance` objects.

        """
        raise NotImplementedError()

    def listResources(self, providers):
        """Return a list of resources accessible to this endpoint.

        The yielded values should represent all resources accessible
        to this endpoint, not only those under the control of Zuul,
        but all visible instances in order for the driver to identify
        leaked resources and instruct the endpoint to remove them.

        :param list providers: A list of providers that share this endpoint.

        :returns: A generator of :py:class:`Resource` objects.

        """
        raise NotImplementedError()

    def listRequiredInfrastructureResources(self):
        """Return a list of infrastructure resources in use by this endpoint.

        This returns the same type of resource objects as
        listResources, but it returns only the resources which are
        currently needed by the provider.  In "infrastructure
        resource" is a resource which is not directly associated with
        a single node or upload.  Instead, it is a resource that may
        be used by multiple nodes or uploads.  Examples include launch
        templates or ssh keys.  Providers may create these on their
        own when needed by their labels.  The launcher will use this
        information to help the provider know when to delete them.

        :returns: A generator of :py:class:`Resource` objects.

        """

        return []

    def deleteResource(self, resource):

        """Delete the supplied resource

        The driver has identified a leaked resource and the adapter
        should delete it.

        :param Resource resource: A Resource object previously
            returned by 'listResources'.
        """
        raise NotImplementedError()


class BaseProviderSchema(metaclass=abc.ABCMeta):
    def getLabelSchema(self, internal=False):
        if internal:
            return BaseProviderLabel.internal_schema
        else:
            return BaseProviderLabel.schema

    def getImageSchema(self, internal=False):
        if internal:
            return BaseProviderImage.internal_schema
        else:
            return BaseProviderImage.schema

    def getFlavorSchema(self, internal=False):
        if internal:
            return BaseProviderFlavor.internal_schema
        else:
            return BaseProviderFlavor.schema

    def getInheritableLabelSchema(self):
        return BaseProviderLabel.inheritable_schema

    def getInheritableImageSchema(self):
        return BaseProviderImage.inheritable_schema

    def getInheritableZuulImageSchema(self):
        return BaseProviderImage.inheritable_zuul_schema

    def getInheritableCloudImageSchema(self):
        return BaseProviderImage.inheritable_cloud_schema

    def getInheritableFlavorSchema(self):
        return BaseProviderFlavor.inheritable_schema

    def getZuulImageSchema(self):
        return BaseProviderImage.zuul_schema

    def getImageConfigKeys(self):
        schema = self.getZuulImageSchema()
        for key in schema.schema.keys():
            if key.config:
                yield key

    def getProviderSchema(self, internal=False):
        schema = vs.Schema({
            '_source_context': model.SourceContext,
            '_start_mark': model.ZuulMark,
            Required(
                'name',
                doc="""\
                The name of the provider.  Used to refer to the
                provider in Zuul configuration.""",
            ): str,
            Required(
                'section',
                doc="""\
                The name of the :attr:`section` from which to inherit.""",
            ): str,
            Required(
                'labels',
                doc="""A list of labels associated with this provider.""",
            ): [self.getLabelSchema(internal)],
            Required(
                'images',
                doc="""A list of images associated with this provider.""",
            ): [self.getImageSchema(internal)],
            Required(
                'flavors',
                doc="""A list of flavors associated with this provider.""",
            ): [self.getFlavorSchema(internal)],
            Optional(
                'label-defaults',
                doc="""\
                Attributes to be set as default values for any label
                used with this provider.  Many attributes which may be
                set on an individual label may be set once in this
                section and used for all the labels in this provider.
                Values set on individual labels may still override the
                values set here.""",
                default={},
            ): self.getInheritableLabelSchema(),
            Optional(
                'image-defaults',
                doc="""\
                Attributes to be set as default values for any image
                used with this provider.  Many attributes which may be
                set on an individual image may be set once in this
                section and used for all the images in this provider.
                Values set on individual images may still override the
                values set here.""",
                default={},
            ): self.getInheritableImageSchema(),
            Optional(
                'flavor-defaults',
                doc="""\
                Attributes to be set as default values for any flavor
                used with this provider.  Many attributes which may be
                set on an individual flavor may be set once in this
                section and used for all the flavors in this provider.
                Values set on individual flavors may still override the
                values set here.""",
                default={},
            ): self.getInheritableFlavorSchema(),
            Optional(
                'abstract',
                doc="""\
                Whether a section is intended to be inherited by
                another :attr:`section` or a :attr:`provider`.  This
                setting is currently unused (but may be used in the
                future).  If a section is used to provide common
                values to other sections, set this to `true`.
                Otherwise, the default of `false` indicates that the
                section should be referenced directly by providers.""",
                default=False,
            ): Nullable(bool),
            Optional(
                'parent',
                doc="""\
                The name of the parent section from which to inherit.
                This attribute is only used by :attr:`section`
                objects.  To indicate which section a provider should
                be attached to, use
                :attr:`provider[$driver].section`""",
            ): Nullable(str),
            Required(
                'connection',
                doc="""\
                The name of the :ref:`connection <connections>` to
                use.  This attribute is only used by :attr:`section`
                objects and may not be changed via inheritance.""",
            ): str,
            Optional(
                'launch-timeout',
                doc="""\
                The time to wait from issuing the command to create a
                new node until the node is reporting as running.  If
                the timeout is exceeded, the node launch is aborted
                and the node deleted.""",
            ): Nullable(int),
            Optional(
                'launch-attempts',
                doc="""\
                The number of times to attempt launching a node before
                considering the request failed.""",
                default=3,
            ): int,
        })
        return schema


class BaseImageJob:
    """Abstract class to encapsulate an image creation"""

    def __init__(self):
        self.dependents = []


class BaseImageImportJob(BaseImageJob):
    """Abstract class to encapsulate an image import

    This class should contain all the information needed to perform an
    image import.  The run method will be executed asynchronously in
    an executor.
    """

    @abc.abstractmethod
    def run(self):
        """Run the import.

        :return: The external id of the image in the cloud
        """
        pass


class BaseImageUploadJob(BaseImageJob):
    """Abstract class to encapsulate an image upload

    This class should contain all the information needed to perform an
    image upload, except the filename.  The run method will be
    executed asynchronously in an executor.
    """

    @abc.abstractmethod
    def run(self, filename):
        """Run the import.

        :param filename str: Path to the local file to upload

        :return: The external id of the image in the cloud
        """
        pass


class BaseImageCopyJob(BaseImageJob):
    """Abstract class to encapsulate an image copy

    This class should contain all the information needed to perform an
    image copy.  The run method will be executed asynchronously in
    an executor.
    """

    @abc.abstractmethod
    def run(self, external_id):
        """Run the import.

        :param external_id str: The external id of the image to copy

        :return: The external id of the image in the cloud
        """
        pass


class BaseProvider(zkobject.PolymorphicZKObjectMixin,
                   zkobject.ShardedZKObject):

    """Base class for provider."""
    schema = BaseProviderSchema().getProviderSchema()
    internal_schema = BaseProviderSchema().getProviderSchema(internal=True)

    def __init__(self, *args):
        super().__init__()
        self._set(nodes=[])
        if args:
            (driver, zk_client, connection, tenant_name,
             canonical_name, config, system_id) = args
            config = config.copy()
            config.pop('_source_context')
            config.pop('_start_mark')
            attrs = dict(
                driver=driver,
                connection=connection,
                connection_name=connection.connection_name,
                tenant_name=tenant_name,
                canonical_name=canonical_name,
                config=config,
                system_id=system_id,
                zk_client=zk_client,
            )
            # Some of the values above are needed when parsing
            # (especially images)
            self._set(**attrs)
            parsed_config = self.parseConfig(config, connection)
            parsed_config.pop('connection')
            parsed_config.update(attrs)
            self._set(**parsed_config)

    def __repr__(self):
        return (f"<{self.__class__.__name__} "
                f"canonical_name={self.canonical_name}>")

    @classmethod
    def fromZK(cls, context, path, connections, system_id, zk_client):
        """Deserialize a Provider (subclass) from ZK.

        To deserialize a Provider from ZK, pass the connection
        registry as the "connections" argument.

        The Provider subclass will automatically be deserialized and
        the connection/driver attributes updated from the connection
        registry.

        """
        raw_data, zstat = cls._loadData(context, path)
        extra = {
            'connections': connections,
            'system_id': system_id,
            'zk_client': zk_client,
        }
        obj = cls._fromRaw(context, raw_data, zstat, extra)
        connection = connections.connections[obj.connection_name]
        obj._set(
            connection=connection,
            driver=connection.driver,
            system_id=system_id,
            zk_client=zk_client,
        )
        return obj

    def getProviderSchema(self, internal=False):
        if internal:
            return self.internal_schema
        else:
            return self.schema

    def parseConfig(self, config, connection):
        schema = self.getProviderSchema(internal=True)
        ret = schema(config)
        images = self.parseImages(config, connection)
        flavors = self.parseFlavors(config, connection)
        labels = self.parseLabels(config, connection)
        for label in labels.values():
            image = images.get(label.image)
            if image is None:
                raise Exception(
                    f'Image "{label.image}" is not attached to provider')
            flavor = flavors.get(label.flavor)
            if flavor is None:
                raise Exception(
                    f'Flavor "{label.flavor}" is not attached to provider')
            label.inheritFrom(image, flavor)
        ret.update(dict(
            images=images,
            flavors=flavors,
            labels=labels,
        ))
        self.validateConfig(ret)
        return ret

    def deserialize(self, raw, context, extra):
        self._set(
            system_id=extra['system_id'],
            zk_client=extra['zk_client'],
        )
        data = super().deserialize(raw, context)
        connections = extra['connections']
        connection = connections.connections[data['connection_name']]
        data['connection'] = connection
        data['driver'] = connection.driver
        self._set(
            canonical_name=data['canonical_name'],
        )
        data.update(self.parseConfig(data['config'], connection))
        return data

    def serialize(self, context):
        data = dict(
            tenant_name=self.tenant_name,
            canonical_name=self.canonical_name,
            config=self.config,
            connection_name=self.connection.connection_name,
        )
        return json.dumps(data, sort_keys=True).encode("utf8")

    @property
    def tenant_scoped_name(self):
        return f'{self.tenant_name}-{self.name}'

    def parseImages(self, config, connection):
        images = {}
        for image_config in config.get('images', []):
            i = self.parseImage(image_config, config, connection)
            images[i.name] = i
        return images

    def parseFlavors(self, config, connection):
        flavors = {}
        for flavor_config in config.get('flavors', []):
            f = self.parseFlavor(flavor_config, config, connection)
            flavors[f.name] = f
        return flavors

    def parseLabels(self, config, connection):
        labels = {}
        for label_config in config.get('labels', []):
            l = self.parseLabel(label_config, config, connection)
            labels[l.name] = l
        return labels

    @abc.abstractmethod
    def parseLabel(self, label_config, provider_config):
        """Instantiate a ProviderLabel subclass

        :returns: a ProviderLabel subclass
        :rtype: ProviderLabel
        """
        pass

    @abc.abstractmethod
    def parseFlavor(self, flavor_config, provider_config):
        """Instantiate a ProviderFlavor subclass

        :returns: a ProviderFlavor subclass
        :rtype: ProviderFlavor
        """
        pass

    @abc.abstractmethod
    def parseImage(self, image_config, provider_config):
        """Instantiate a ProviderImage subclass

        :returns: a ProviderImage subclass
        :rtype: ProviderImage
        """
        pass

    @abc.abstractmethod
    def getEndpoint(self):
        """Get an endpoint for this provider"""
        pass

    def validateConfig(self, config):
        """Validate the full and final config for this provider

        This is called after all schema validation and configuration
        inheritance has been performed.  This allows us to validate
        multiple settings from different areas (eg, that a label is
        valid with a certain flavor config).

        """
        pass

    def getPath(self):
        path = (f'/zuul/tenant/{self.tenant_name}'
                f'/provider/{self.canonical_name}/config')
        return path

    def hasLabel(self, label):
        return label in self.labels

    def getNodeTags(self, system_id, label, node_uuid,
                    request=None):
        """Return the tags that should be stored with the node

        :param str system_id: The Zuul system uuid
        :param ProviderLabel label: The node label
        :param str node_uuid: The node uuid
        :param NodesetRequest request: The node request or None
        """
        tags = dict()
        attrs = request.getSafeAttributes().toDict() if request else {}
        for k, v in label.tags.items():
            try:
                tags[k] = v.format(**attrs)
            except Exception:
                self.log.exception("Error formatting metadata %s", k)

        fixed = {
            'zuul_system_id': system_id,
            'zuul_node_uuid': node_uuid,
        }
        tags.update(fixed)
        return tags

    def getCreateStateMachine(self, node,
                              image_external_id,
                              log):
        """Return a state machine suitable for creating an instance

        This method should return a new state machine object
        initialized to create the described node.

        :param ProviderNode node: The node object.
        :param ProviderLabel label: A config object representing the
            provider-label for the node.
        :param str image_external_id: If provided, the external id of
            a previously uploaded image; if None, then the adapter should
            look up a cloud image based on the label.
        :param log Logger: A logger instance for emitting annotated
            logs related to the request.

        :returns: A :py:class:`StateMachine` object.

        """
        raise NotImplementedError()

    def getDeleteStateMachine(self, node, log):
        """Return a state machine suitable for deleting an instance

        This method should return a new state machine object
        initialized to delete the described instance.

        :param node ProviderNode: The node that should be deleted.
        :param log Logger: A logger instance for emitting annotated
            logs related to the request.
        """
        raise NotImplementedError()

    def getEndpointLimits(self):
        """Return the endpoint quota limits for this provider

        The default implementation returns a simple QuotaInformation
        with no limits.  Override this to provide accurate
        information.

        :returns: A :py:class:`QuotaInformation` object.

        """
        return model.QuotaInformation(default=math.inf)

    def getQuotaForLabel(self, label):
        """Return information about the quota used for a label

        The default implementation returns a simple QuotaInformation
        for one instance; override this to return more detailed
        information including cores and RAM.

        :param ProviderLabel label: A config object describing
            a label for an instance.

        :returns: A :py:class:`QuotaInformation` object.
        """
        return model.QuotaInformation(instances=1)

    def refreshQuotaForLabel(self, label, update):
        """Query the endpoint for quota used for a label and update
        the quota cache

        :param ProviderLabel label: A config object describing
            a label for an instance.
        :param bool update: Whether an update should be performed even
            if there are values present.
        """
        raise NotImplementedError()

    def getAZs(self):
        """Return a list of availability zones for this provider

        One of these will be selected at random and supplied to the
        create state machine.  If a request handler is building a node
        set from an existing ready node, then the AZ from that node
        will be used instead of the results of this method.

        :returns: A list of availability zone names.
        """
        return [None]

    def labelReady(self, label):
        """Indicate whether a label is ready in the provided cloud

        This is used by the launcher to determine whether it should
        consider a label to be in-service for a provider.  If this
        returns False, the label will be ignored for this provider.

        This does not need to consider whether a diskimage is ready;
        the launcher handles that itself.  Instead, this can be used
        to determine whether a cloud-image is available.

        :param ProviderLabel label: A config object describing a label
            for an instance.

        :returns: A bool indicating whether the label is ready.
        """
        return True

    # The following methods must be implemented only if image
    # management is supported:

    def getSnapshotStateMachine(self, node, log):
        """Return a state machine suitable for snapshotting an instance

        This method should return a new state machine object
        initialized to snapshot the described instance.

        :param node ProviderNode: The node that should be snapshotted.
        :param log Logger: A logger instance for emitting annotated
            logs related to the request.
        """
        return None

    def downloadUrl(self, url, path):
        """Attempt to download the given URL to the destination path

        If this provider is able to download URLs of the given form,
        it should attempt to do so and save the result.  If it can not
        handle the given URL, return None.

        This is an optional method that may be implemented in order to
        allow for image storage in cloud-specific storage systems.

        :param url str: The URL of the file to download
        :param path str: The local destination path

        :return: None if the provider can not handle the URL, or the
        path if it sucessfully downloaded it.

        """
        return None

    def getImageImportJob(self, url, provider_image, image_name,
                          image_format, metadata, md5, sha256):
        """Get an image import job if able

        If the provider can support a direct image import from the
        supplied URL, then return a BaseImageUploadJob that will do so.

        :param url str: The URL of the image
        :param provider_image ProviderImageConfig:
            The provider's config for this image
        :param image_name str: The name of the image
        :param image_format str: The format of the image (e.g., "qcow")
        :param metadata dict: A dictionary of metadata that must be
            stored on the image in the cloud.
        :param md5 str: The md5 hash of the image file
        :param sha256 str: The sha256 hash of the image file

        :return: A BaseImageUploadJob that will import the image

        """
        # Most drivers probably won't implement this.
        return None

    def getImageCopyJob(self, source_provider, provider_image, image_name,
                        image_format, metadata, md5, sha256):
        """Get an image copy job if able

        If the provider can support copying an existing image from the
        supplied provider, then return a BaseImageUploadJob that will do so.

        The external_id will be passed as an argument to the run
        method of the resulting job.

        :param source_provider Provider: The provider of the source image
        :param provider_image ProviderImageConfig:
            The provider's config for this image
        :param image_name str: The name of the image
        :param image_format str: The format of the image (e.g., "qcow")
        :param metadata dict: A dictionary of metadata that must be
            stored on the image in the cloud.
        :param md5 str: The md5 hash of the image file
        :param sha256 str: The sha256 hash of the image file

        :return: A BaseImageUploadJob that will import the image

        """
        # Most drivers probably won't implement this.
        return None

    def getImageUploadJob(self, provider_image, image_name,
                          image_format, metadata, md5, sha256):
        """Get an image upload job

        Return a BaseImageUploadJob that will upload the local filename
        to the provider as an image.

        The path to the local file to be uploaded will be passed as an
        argument to the run method of the resulting job.

        :param provider_image ProviderImageConfig:
            The provider's config for this image
        :param image_name str: The name of the image
        :param image_format str: The format of the image (e.g., "qcow")
        :param metadata dict: A dictionary of metadata that must be
            stored on the image in the cloud.
        :param md5 str: The md5 hash of the image file
        :param sha256 str: The sha256 hash of the image file

        :return: A BaseImageUploadJob that will upload the image

        """
        # Required if the driver handles images at all.
        raise NotImplementedError()

    def deleteImage(self, external_id):
        """Delete an image from the cloud

        :param external_id str: The external id of the image to delete
        """
        raise NotImplementedError()

    # The following methods are optional
    def getConsoleLog(self, label, node):
        """Return the console log from the specified server

        :param label ConfigLabel: The label config for the node
        :param ProviderNode node: The node of the server
        """
        raise NotImplementedError()

    def notifyNodescanFailure(self, label, node):
        """Notify the adapter of a nodescan failure

        :param label ConfigLabel: The label config for the node
        :param ProviderNode node: The node of the server
        """
        pass

    def postConfig(self, update=False):
        """Perform any provider-global actions after reconfiguration

        This will be called any time a tenant layout is updated, once
        for each provider.  It may be called multiple times for the
        same update, and it may be called even when nothing about the
        provider changed.

        """
        if update:
            for label in self.labels.values():
                self.refreshQuotaForLabel(label, update)

    def canReuseNode(self, node):
        """The provider can veto the reuse of a node

        If the provider decides that a node should not be reused,
        return False here.  This will only be called if the launcher
        has decided that it should be reused (so label configuration
        will already have been checked).

        :param ProviderNode node: The node
        """
        if node.state == node.State.FAILED:
            return False
        return True


class EndpointCacheMixin:
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.endpoints = {}
        self.endpoints_lock = threading.Lock()

    def getEndpointById(self, endpoint_id, create_args):
        with self.endpoints_lock:
            try:
                return self.endpoints[endpoint_id]
            except KeyError:
                pass
            endpoint = self._endpoint_class(*create_args)
            self.endpoints[endpoint_id] = endpoint
        return endpoint

    def getEndpoints(self):
        with self.endpoints_lock:
            return list(self.endpoints.values())

    def stopEndpoints(self):
        with self.endpoints_lock:
            for endpoint in self.endpoints.values():
                endpoint.stop()
