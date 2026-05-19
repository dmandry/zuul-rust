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
        Required(
            'name',
            doc="""\
            Identifier for this secret.  The referenced secret must
            already exist under this name so that Nodepool may copy
            it.  It will be copied into the new namespace with the
            same name, therefore, if multiple entries are provided,
            they must have distinct names.
            """,
        ): str,
        Optional(
            'namespace',
            default='default',
            doc="""\
            The namespace of the existing secret to copy.
            """,
        ): str,
    })

    kubernetes_label_schema = vs.Schema({
        Required(
            'kind',
            doc="""\
            The Kubernetes driver supports two types of labels:
            """,
        ): vs.Any(
            Constant(
                'pod',
                doc="""\
                Pod labels provide a dedicated namespace with a single
                pod and a service account that can exec and get the
                logs of the pod.
                """,
            ),
            Constant(
                'namespace',
                doc="""\
                Namespace labels provide an empty namespace configured
                with a service account that can create pods, services,
                configmaps, etc.
                """,
            ),
        ),
        Required(
            'spec',
            doc="""\

            Zuul will supply the contents of this value verbatim to
            Kubernetes as the ``spec`` attribute of the Kubernetes
            ``Pod`` definition.

            This attribute allows for the creation of arbitrary
            complex pod definitions but the user is responsible for
            ensuring that they are suitable.  The first container in
            the pod is expected to be a long-running container that
            hosts a shell environment for running commands.  The
            following minimal definition is recommended as a starting
            point:

            .. code-block:: yaml

               labels:
                 - name: custom-pod
                   kind: pod
                   spec:
                     containers:
                       - name: custom-pod
                         image: ubuntu:jammy
                         imagePullPolicy: IfNotPresent
                         command: ["/bin/sh", "-c"]
                         args: ["sleep infinity"]
            """,
        ): dict,
    })

    kubernetes_label_inheritable_schema = vs.Schema({
        Optional(
            'kind',
            doc="""\
            The Kubernetes driver supports two types of labels:
            """,
        ): Nullable(vs.Any(
            Constant(
                'pod',
                doc="""\
                Pod labels provide a dedicated namespace with a single
                pod and a service account that can exec and get the
                logs of the pod.
                """,
            ),
            Constant(
                'namespace',
                doc="""\
                Namespace labels provide an empty namespace configured
                with a service account that can create pods, services,
                configmaps, etc.
                """,
            ),
        )),
    })

    kubernetes_label_common_schema = vs.Schema({
        Optional(
            'annotations',
            doc="""\
            A dictionary of additional values to be added to the pod
            metadata.  The value of this field is added to the
            `metadata.annotations` field in Kubernetes.  This field
            contains arbitrary key/value pairs that can be accessed by
            tools and libraries.  """,
        ): Nullable(dict),
        Optional(
            'image-pull-secrets',
            default=[],
            doc="""\

            The imagePullSecrets needed to pull container images from
            a private registry.  Because Zuul creates pods in a new
            namespace, and image pull secrets must exist in the
            namespace of the pods that use them, the referenced
            secrets will be copied into the temporary namespace that
            Zuul creates before creating the pod.  The new secrets
            will have the same name as the old secrets.

            Each entry is a dictionary with the following keys:
            """,
        ): AsList(kubernetes_pull_secrets),
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

    internal_schema = assemble(
        schema,
        provider_schema.internal_base_label,
        extra=vs.ALLOW_EXTRA,
    )

    image_flavor_inheritable_schema = vs.Schema({})

    def __init__(self, label_config, provider_config):
        super().__init__(label_config, provider_config)
        self.host_key_checking = False


class KubernetesProviderSchema(BaseProviderSchema):
    def getLabelSchema(self, internal=False):
        if internal:
            return KubernetesProviderLabel.internal_schema
        else:
            return KubernetesProviderLabel.schema

    def getImageSchema(self, internal=False):
        if internal:
            return KubernetesProviderImage.internal_schema
        else:
            return KubernetesProviderImage.schema

    def getFlavorSchema(self, internal=False):
        if internal:
            return KubernetesProviderFlavor.internal_schema
        else:
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

    def getProviderSchema(self, internal=False):
        schema = super().getProviderSchema(internal)

        resource_limits = {
            Optional(
                'pods',
                default=vs.UNDEFINED,
                doc="""The number of pods.""",
            ): Nullable(int),
            Optional(
                'namespaces',
                default=vs.UNDEFINED,
                doc="""The number of pods.""",
            ): Nullable(int),
        }

        kubernetes_provider_schema = vs.Schema({
            Optional(
                'resource-limits',
                doc="""\
                Resource limits for this provider.  Configure these
                values to cause Zuul to attempt to limit the resource
                usage.  This can be used to limit Zuul's usage to a
                level below the cloud quota.""",
                default=dict(),
            ): resource_limits,
        })

        return assemble(
            schema,
            kubernetes_provider_schema,
            doc="""\
            The attributes available for configuring a Kubernetes
            provider are below.
            """,
        )


class KubernetesProvider(BaseProvider, subclass_id='kubernetes'):
    log = logging.getLogger("zuul.KubernetesProvider")
    schema = KubernetesProviderSchema().getProviderSchema()
    internal_schema = KubernetesProviderSchema().getProviderSchema(
        internal=True)

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
                    request=None):
        tags = super().getNodeTags(system_id, label, node_uuid,
                                   request)
        # So that we can disambiguate requests for namespaces and pods
        # (both of which create namespaces).
        tags['zuul_kubernetes_kind'] = label.kind
        return tags
