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

from zuul.lib.voluputil import (
    AsList,
    Nullable,
    Optional,
    Required,
    assemble,
)

import voluptuous as vs

from zuul.driver.kubernetes.kubernetesendpoint import (
    KubernetesCreateStateMachine,
    KubernetesDeleteStateMachine,
)
from zuul.model import QuotaInformation
from zuul.provider import (
    BaseProvider,
    BaseProviderFlavor,
    BaseProviderImage,
    BaseProviderLabel,
    BaseProviderSchema,
)


class KubernetesProviderImage(BaseProviderImage):
    def __init__(self, image_config, provider_config):
        super().__init__(image_config, provider_config)
        # Implement provider defaults
        if self.connection_type is None:
            self.connection_type = 'kubectl'


class KubernetesProviderFlavor(BaseProviderFlavor):
    pass


class KubernetesProviderLabel(BaseProviderLabel):
    kubernetes_pull_secrets = vs.Schema({
        Required('name'): str,
        Optional('namespace', default='default'): str,
    })

    kubernetes_label_schema = vs.Schema({
        Required('kind'): vs.Any('pod', 'namespace'),
        Required('spec'): dict,
    })

    kubernetes_label_inheritable_schema = vs.Schema({
        Optional('kind'): Nullable(vs.Any('pod', 'namespace')),
    })

    kubernetes_label_common_schema = vs.Schema({
        Optional('annotations'): Nullable(dict),
        Optional('image-pull-secrets', default=[]): AsList(
            kubernetes_pull_secrets),
    })

    inheritable_schema = assemble(
        BaseProviderLabel.inheritable_schema,
        kubernetes_label_inheritable_schema,
        kubernetes_label_common_schema,
    )

    schema = assemble(
        BaseProviderLabel.schema,
        kubernetes_label_schema,
        kubernetes_label_common_schema,
    )

    image_flavor_inheritable_schema = vs.Schema({})

    def __init__(self, label_config, provider_config):
        super().__init__(label_config, provider_config)
        self.host_key_checking = False


class KubernetesProviderSchema(BaseProviderSchema):
    def getLabelSchema(self):
        return KubernetesProviderLabel.schema

    def getImageSchema(self):
        return KubernetesProviderImage.schema

    def getFlavorSchema(self):
        return KubernetesProviderFlavor.schema

    def getInheritableLabelSchema(self):
        return KubernetesProviderLabel.inheritable_schema

    def getInheritableImageSchema(self):
        return KubernetesProviderImage.inheritable_schema

    def getInheritableZuulImageSchema(self):
        return KubernetesProviderImage.inheritable_zuul_schema

    def getInheritableCloudImageSchema(self):
        return KubernetesProviderImage.inheritable_cloud_schema

    def getInheritableFlavorSchema(self):
        return KubernetesProviderFlavor.inheritable_schema

    def getProviderSchema(self):
        schema = super().getProviderSchema()

        resource_limits = {
            'pods': int,
            'namespaces': int,
        }

        kubernetes_provider_schema = vs.Schema({
            Optional('resource-limits', default=dict()): resource_limits,
        })

        return assemble(
            schema,
            kubernetes_provider_schema,
        )


class KubernetesProvider(BaseProvider, subclass_id='kubernetes'):
    log = logging.getLogger("zuul.KubernetesProvider")
    schema = KubernetesProviderSchema().getProviderSchema()

    @property
    def endpoint(self):
        ep = getattr(self, '_endpoint', None)
        if ep:
            return ep
        self._set(_endpoint=self.getEndpoint())
        return self._endpoint

    def parseImage(self, image_config, provider_config, connection):
        return KubernetesProviderImage(image_config, provider_config)

    def parseFlavor(self, flavor_config, provider_config, connection):
        return KubernetesProviderFlavor(flavor_config, provider_config)

    def parseLabel(self, label_config, provider_config, connection):
        return KubernetesProviderLabel(label_config, provider_config)

    def getEndpoint(self):
        return self.driver.getEndpoint(self)

    def getCreateStateMachine(self, node, image_external_id, log):
        # TODO: decide on a method of producing a hostname
        # that is max 15 chars.
        hostname = f"np{node.uuid[:13]}"
        label = self.labels[node.label]
        flavor = self.flavors[label.flavor]
        image = self.images[label.image]
        return KubernetesCreateStateMachine(
            self.endpoint,
            node,
            hostname,
            label,
            flavor,
            image,
            log)

    def getDeleteStateMachine(self, node, log):
        return KubernetesDeleteStateMachine(self.endpoint, node, log)

    def listInstances(self):
        return self.endpoint.listInstances()

    def getEndpointLimits(self):
        return QuotaInformation(default=math.inf)

    def getQuotaForLabel(self, label):
        return self.endpoint.getQuotaForLabel(label)

    def refreshQuotaForLabel(self, label, update):
        pass

    def getNodeTags(self, system_id, label, node_uuid,
                    provider=None, request=None):
        tags = super().getNodeTags(system_id, label, node_uuid,
                                   provider, request)
        # So that we can disambiguate requests for namespaces and pods
        # (both of which create namespaces).
        tags['zuul_kubernetes_kind'] = label.kind
        return tags
