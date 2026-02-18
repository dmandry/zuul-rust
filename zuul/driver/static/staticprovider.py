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
import uuid

import zuul.provider.schema as provider_schema
from zuul.lib.voluputil import (
    AsList,
    Nullable,
    Optional,
    Required,
    assemble,
)

import voluptuous as vs

from zuul.driver.static.staticendpoint import (
    StaticCreateStateMachine,
    StaticDeleteStateMachine,
)
from zuul.model import QuotaInformation
from zuul.provider import (
    BaseProvider,
    BaseProviderFlavor,
    BaseProviderImage,
    BaseProviderLabel,
    BaseProviderSchema,
)

NODE_NAMESPACE = uuid.UUID('913b8645-cc07-4e1c-88e6-7ba14fdd286f')


class StaticProviderImage(BaseProviderImage):
    def __init__(self, image_config, provider_config):
        super().__init__(image_config, provider_config)
        # Implement provider defaults
        if self.connection_type is None:
            self.connection_type = 'ssh'
        if self.connection_port is None:
            self.connection_port = 22


class StaticProviderFlavor(BaseProviderFlavor):
    pass


class StaticProviderLabel(BaseProviderLabel):
    static_label_schema = vs.Schema({})

    inheritable_schema = assemble(
        BaseProviderLabel.inheritable_schema,
        provider_schema.ssh_label,
        static_label_schema,
    )
    schema = assemble(
        BaseProviderLabel.schema,
        provider_schema.ssh_label,
        static_label_schema,
    )

    image_flavor_inheritable_schema = vs.Schema({})

    def __init__(self, label_config, provider_config):
        super().__init__(label_config, provider_config)
        # Static nodes are always reused
        self.reuse = True


class StaticNodeConfig:
    schema = vs.Schema({
        Required(
            'name',
            doc="""\
            The hostname or ip address of the static node.
            """,
        ): str,
        # Normally we get the connection port and user from the
        # image, but in order to allow registering the same host
        # multiple times with different values, we allow the
        # setting here.  If it's null, we will get it from the
        # image.
        Optional(
            'connection-port',
            doc="""\
            The port that Zuul should use when connecting to the node.
            For most nodes this is not necessary. This defaults to 22 when
            ``connection-type`` is 'ssh' and 5986 when it is 'winrm'.
            """,
        ): Nullable(int),
        Optional(
            'username',
            doc="""\
            The username Zuul should use when connecting to the node.
            """,
        ): Nullable(str),
        Required(
            'label',
            doc="""\
            The label to associate with this node.  This is the
            primary label for the node and will be used to supply any
            necessary information about the node's characteristics.
            """,
        ): str,
        Optional(
            'aliases',
            doc="""\
            A list of label aliases to associate with this node.
            These labels may be used to request this node, but none of
            their attributes will be used to define characteristics.
            """,
            default=[]): AsList(str),
        Optional(
            'host-key',
            doc="""\
            The ssh host key of the node.
            """,
        ): Nullable(str),
    })

    def __init__(self, node_config):
        self.__dict__.update(node_config)

    def inheritFrom(self, image, flavor):
        for attr in ['username', 'connection_port']:
            if getattr(self, attr, None) is None:
                setattr(self, attr,
                        getattr(flavor, attr, None) or
                        getattr(image, attr, None))


class StaticProviderSchema(BaseProviderSchema):
    def getLabelSchema(self):
        return StaticProviderLabel.schema

    def getInheritableLabelSchema(self):
        return StaticProviderLabel.inheritable_schema

    def getProviderSchema(self):
        schema = super().getProviderSchema()

        resource_limits = {}
        resource_limits[Optional(
            'instances',
            default=vs.UNDEFINED,
            doc="""The number of instances.""",
        )] = int

        static_provider_schema = vs.Schema({
            Optional(
                'resource-limits',
                doc="""\
                Resource limits for this provider.
                """,
                default=dict()): resource_limits,
            Required(
                'nodes',
                doc="""A list of nodes to be used by this provider.""",
            ): [StaticNodeConfig.schema],
        })

        return assemble(
            schema,
            static_provider_schema,
            doc="""\
            The attributes available for configuring a static provider
            are below.
            """,
        )


class StaticProvider(BaseProvider, subclass_id='static'):
    log = logging.getLogger("zuul.StaticProvider")
    schema = StaticProviderSchema().getProviderSchema()

    @property
    def endpoint(self):
        ep = getattr(self, '_endpoint', None)
        if ep:
            return ep
        self._set(_endpoint=self.getEndpoint())
        return self._endpoint

    def parseImage(self, image_config, provider_config, connection):
        return StaticProviderImage(image_config, provider_config)

    def parseFlavor(self, flavor_config, provider_config, connection):
        return StaticProviderFlavor(flavor_config, provider_config)

    def parseLabel(self, label_config, provider_config, connection):
        return StaticProviderLabel(label_config, provider_config)

    def parseNodeConfig(self, node_config, provider_config, connection):
        return StaticNodeConfig(node_config)

    def getEndpoint(self):
        return self.driver.getEndpoint(self)

    def parseConfig(self, config, connection):
        ret = super().parseConfig(config, connection)
        nodes = {}
        for node in config['nodes']:
            label = ret['labels'].get(node['label'])
            if label is None:
                raise Exception(
                    f'Label "{node["label"]}" is not attached to provider')
            # These were validated in the super
            image = ret['images'][label.image]
            flavor = ret['flavors'][label.flavor]
            node = self.parseNodeConfig(node, config, connection)
            node.inheritFrom(image, flavor)
            key = ' '.join([
                self.canonical_name,
                node.name,
                str(node.connection_port),
                str(node.username),
            ])
            node_id = uuid.uuid5(NODE_NAMESPACE, key).hex
            node.node_id = node_id
            nodes[node_id] = node
        ret['nodes'] = nodes
        return ret

    def getCreateStateMachine(self, node, image_external_id, log):
        label = self.labels[node.label]
        return StaticCreateStateMachine(self, node, label, log)

    def getDeleteStateMachine(self, node, log):
        return StaticDeleteStateMachine(self, node, log)

    def getEndpointLimits(self):
        return QuotaInformation(default=math.inf)

    def getQuotaForLabel(self, label):
        return QuotaInformation(instances=1)

    def refreshQuotaForLabel(self, label, update):
        pass

    def canReuseNode(self, node):
        # In case a node was unable to be deregistered in an earlier
        # reconfiguration, check it again now.
        node_uuid = node.main_node_id or node.uuid
        if node_uuid not in self.nodes:
            return False
        return True
