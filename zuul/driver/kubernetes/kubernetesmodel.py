# Copyright 2025 Acme Gating, LLC
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

from zuul import model
from zuul.provider import statemachine


class KubernetesProviderNode(model.ProviderNode, subclass_id="kubernetes"):
    def __init__(self):
        super().__init__()
        self._set(
            kubernetes_namespace_id=None,
        )

    def getDriverData(self):
        return dict(
            kubernetes_namespace_id=self.kubernetes_namespace_id,
        )


class KubernetesInstance(statemachine.Instance):
    def __init__(self, kubernetes_type, kubernetes_id, metadata, quota):
        super().__init__()
        self.kubernetes_type = kubernetes_type
        self.kubernetes_id = kubernetes_id
        self.metadata = metadata
        self.quota = quota
        self.interface_ip = kubernetes_id

    def getQuotaInformation(self):
        return self.quota

    @property
    def external_id(self):
        return f'{self.kubernetes_type}={self.kubernetes_id}'


class KubernetesResource(statemachine.Resource):
    TYPE_NAMESPACE = 'namespace'
    TYPE_PROJECT = 'project'

    def __init__(self, metadata, type, id):
        super().__init__(metadata, type)
        self.id = id

    @property
    def unique_id(self):
        return '-'.join([self.type, self.id])
