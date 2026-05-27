# Copyright (C) 2018 Red Hat
# Copyright 2023, 2025 Acme Gating, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy


class FakeCoreClient:
    def __init__(self):
        self.namespaces = {}
        self._pod_requests = []

        ns_body = {
            'apiVersion': 'v1',
            'kind': 'Namespace',
            'metadata': {
                'name': 'default',
            }
        }
        self.create_namespace(ns_body)

        class FakeApi:
            class configuration:
                host = "http://localhost:8080"
                verify_ssl = False
                tls_server_name = "127.0.0.1"
        self.api_client = FakeApi()

    def list_namespace(self):
        class FakeNamespaces:
            items = list(self.namespaces.values())
        return FakeNamespaces

    def create_namespace(self, ns_body):
        class FakeNamespace:
            class metadata:
                name = ns_body['metadata']['name']
                labels = ns_body['metadata'].get('labels')
            _secrets = {}
            _pods = {}
        self.namespaces[FakeNamespace.metadata.name] = FakeNamespace
        return FakeNamespace

    def delete_namespace(self, name, body):
        if name not in self.namespaces:
            raise RuntimeError("Unknown namespace %s" % name)
        del self.namespaces[name]

    def create_namespaced_service_account(self, ns, sa_body):
        return

    def read_namespaced_service_account(self, user, ns):
        class FakeSA:
            class secret:
                name = "fake"
        FakeSA.secrets = [FakeSA.secret]
        return FakeSA

    def create_namespaced_secret(self, ns, secret_body):
        namespace = self.namespaces[ns]
        name = secret_body['metadata']['name']

        class FakeSecret:
            class metadata:
                name = secret_body['metadata']['name']
            data = secret_body.get('data')
        if secret_body['type'] == 'kubernetes.io/service-account-token':
            FakeSecret.data = {
                'ca.crt': 'ZmFrZS1jYQ==', 'token': 'ZmFrZS10b2tlbg=='
            }
        namespace._secrets[name] = FakeSecret

    def read_namespaced_secret(self, name, ns):
        namespace = self.namespaces[ns]
        return namespace._secrets[name]

    def create_namespaced_pod(self, ns, pod_body):
        namespace = self.namespaces[ns]
        name = pod_body['metadata']['name']

        class FakePod:
            class status:
                phase = "Running"

            class metadata:
                name = pod_body['metadata']['name']
                labels = pod_body['metadata'].get('labels')
            spec = pod_body.get('spec')
        namespace._pods[name] = FakePod

    def read_namespaced_pod(self, name, ns):
        namespace = self.namespaces[ns]
        return namespace._pods[name]

    def list_namespaced_pod(self, ns):
        namespace = self.namespaces[ns]

        class PodList:
            items = namespace._pods.values()
        return PodList


class FakeRbacClient:
    def create_namespaced_role(self, ns, role_body):
        return

    def create_namespaced_role_binding(self, ns, role_binding_body):
        return


class FakeDynamicClient:
    def __init__(self, core_client, openshift):
        class FakeResources:
            def get(self, api_version, kind):
                if kind == 'ProjectRequest':
                    if not openshift:
                        raise Exception("Not an OpenShift server")

                    class FakeProjects:
                        def create(self, body):
                            new_body = copy.deepcopy(body)
                            new_body['apiVersion'] = 'v1'
                            new_body['kind'] = 'Namespace'
                            core_client.create_namespace(new_body)
                    return FakeProjects()
                if kind == 'Project':
                    if not openshift:
                        raise Exception("Not an OpenShift server")

                    class FakeProject:
                        def get(self):

                            class FakeProjects:
                                items = list(core_client.namespaces.values())
                            return FakeProjects

                        def delete(self, name):
                            core_client.delete_namespace(name, None)
                    return FakeProject()

        self.resources = FakeResources()
