# Copyright 2018, 2021 Red Hat
# Copyright 2023, 2025 Acme Gating, LLC
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

import base64
import logging

import kubernetes

from zuul.driver.kubernetes.kubernetesmodel import (
    KubernetesInstance,
    KubernetesResource,
)
from zuul.model import QuotaInformation
from zuul.provider import (
    BaseProviderEndpoint,
    statemachine
)


def _getConfig(config_file, context, log):
    config = kubernetes.client.Configuration()
    try:
        kubernetes.config.load_kube_config(
            config_file=config_file, context=context,
            client_configuration=config)
    except kubernetes.config.config_exception.ConfigException as e:
        if 'Invalid kube-config file. No configuration found.' in str(e):
            log.debug("Kubernetes config file not found, attempting "
                      "to load in-cluster configs")
            kubernetes.config.load_incluster_config(
                client_configuration=config)
        else:
            raise
    return config


def _getClient(config_file, context, log):
    conf = _getConfig(config_file, context, log)
    api_client = kubernetes.client.api_client.ApiClient(conf)
    core_client = kubernetes.client.CoreV1Api(api_client)
    rbac_client = kubernetes.client.RbacAuthorizationV1Api(api_client)
    dynamic_client = kubernetes.dynamic.DynamicClient(api_client)
    return (core_client, rbac_client, dynamic_client)


class KubernetesDeleteStateMachine(statemachine.StateMachine):
    NAMESPACE_DELETING = 'deleting namespace'

    def __init__(self, endpoint, node, log):
        self.log = log
        self.node = node
        self.endpoint = endpoint
        self.use_openshift_projects =\
            endpoint.connection.use_openshift_projects
        super().__init__(node.delete_state)

    def advance(self):
        if self.state == self.START:
            if self.node.kubernetes_namespace_id:
                if self.use_openshift_projects:
                    self.endpoint._deleteProject(
                        self.node.kubernetes_namespace_id)
                else:
                    self.endpoint._deleteNamespace(
                        self.node.kubernetes_namespace_id)
                self.state = self.NAMESPACE_DELETING
            else:
                self.state = self.COMPLETE

        # We don't currently wait
        if self.state == self.NAMESPACE_DELETING:
            self.state = self.COMPLETE

        if self.state == self.COMPLETE:
            self.complete = True


class KubernetesCreateStateMachine(statemachine.StateMachine):
    NAMESPACE_CREATING = 'creating namespace'
    SA_CREATING = 'creating service account'
    RBAC_CREATING = 'creating rbac'
    POD_CREATING = 'creating pod'

    def __init__(self, endpoint, node, hostname, label, flavor, image,
                 log):
        self.log = log
        self.endpoint = endpoint
        self.node = node
        self.hostname = hostname
        super().__init__(node.create_state)
        self.label = label
        self.use_openshift_projects =\
            endpoint.connection.use_openshift_projects
        if label.kind == 'pod':
            self.create_pod = True
            self.restricted_access = True
            self.containers = [c['name'] for c in label.spec['containers']]
        elif label.kind == 'namespace':
            self.create_pod = False
            self.restricted_access = False
            self.containers = None
        self.quota = self.endpoint.getQuotaForLabel(label)
        self.user = 'zuul-worker'
        self.namespace = self.hostname
        self.node.kubernetes_connection['user'] = self.user
        self.node.kubernetes_connection['host'] =\
            self.endpoint.core_client.api_client.configuration.host
        self.node.kubernetes_connection['skiptls'] =\
            not self.endpoint.core_client.api_client.configuration.verify_ssl
        self.node.kubernetes_connection['namespace'] = self.namespace
        # Pod and namespace names are the same
        self.node.kubernetes_connection['pod'] = self.namespace
        self.node.kubernetes_connection['containers'] = self.containers

    def advance(self):
        if self.state == self.START:
            if self.use_openshift_projects:
                self.endpoint._createProject(self.hostname, self.node.tags)
            else:
                self.endpoint._createNamespace(self.hostname, self.node.tags)
            self.endpoint._createImagePullSecrets(self.namespace, self.label)
            self.endpoint._createServiceAccount(self.namespace,
                                                self.user)
            self.node.kubernetes_namespace_id = self.namespace
            self.state = self.SA_CREATING

        if self.state == self.SA_CREATING:
            token = self.endpoint._getToken(self.namespace, self.user)
            if token is None:
                return
            (token, ca_crt) = token
            self.node.kubernetes_connection['token'] = token
            if not self.node.kubernetes_connection['skiptls']:
                self.node.kubernetes_connection['ca_crt'] = ca_crt
            self.endpoint._createRole(self.namespace, self.user,
                                      self.restricted_access)
            self.state = self.RBAC_CREATING

        if self.state == self.RBAC_CREATING:
            self.endpoint._createPod(self.namespace, self.hostname,
                                     self.label, self.node.tags)
            self.state = self.POD_CREATING

        if self.state == self.POD_CREATING:
            if not self.endpoint._getPod(self.namespace, self.hostname):
                return
            self._updateNode()
            self.state = self.COMPLETE

        if self.state == self.COMPLETE:
            self.complete = True

    def _updateNode(self):
        instance = KubernetesInstance(self.label.kind, self.hostname,
                                      self.node.tags, self.quota)
        self.node.updateFromInstance(instance)


class KubernetesProviderEndpoint(BaseProviderEndpoint):
    def __init__(self, zk_client, driver, connection, system_id):
        name = connection.connection_name
        super().__init__(zk_client, driver, connection, name, system_id)
        self.log = logging.getLogger(f"zuul.kubernetes.{self.name}")

    def startEndpoint(self):
        self.log.debug("Starting kubernetes endpoint")
        self.core_client, self.rbac_client, self.dynamic_client =\
            self._getClient()
        self._running = True

    def stopEndpoint(self):
        self.log.debug("Stopping kubernetes endpoint")
        self._running = False

    def postConfig(self, provider):
        pass

    def refreshQuotaLimits(self, update):
        if self.quota_cache.hasLimits() and not update:
            return False
        limits = QuotaInformation()
        self.quota_cache.setLimits(limits)
        return True

    def listResources(self, providers):
        if self.connection.use_openshift_projects:
            projects = self.dynamic_client.resources.get(
                api_version='v1', kind='Project')
            namespaces = projects.get().items
            namespace_type = KubernetesResource.TYPE_PROJECT
        else:
            namespaces = self.core_client.list_namespace().items
            namespace_type = KubernetesResource.TYPE_NAMESPACE

        for namespace in namespaces:
            yield KubernetesResource(namespace.metadata.labels or {},
                                     namespace_type,
                                     namespace.metadata.name)

    def deleteResource(self, resource):
        self.log.info(f"Deleting leaked {resource.type}: {resource.id}")
        if resource.type == KubernetesResource.TYPE_NAMESPACE:
            self._deleteNamespace(resource.id)
        elif resource.type == KubernetesResource.TYPE_PROJECT:
            self._deleteProject(resource.id)

    def listInstances(self):
        if self.connection.use_openshift_projects:
            projects = self.dynamic_client.resources.get(
                api_version='v1', kind='Project')
            namespaces = projects.get().items
        else:
            namespaces = self.core_client.list_namespace().items

        for namespace in namespaces:
            quota = QuotaInformation(namespaces=1)
            yield KubernetesInstance('namespace',
                                     namespace.metadata.name,
                                     namespace.metadata.labels or {},
                                     quota)
            for pod in self.core_client.list_namespaced_pod(
                    namespace.metadata.name).items:
                quota = QuotaInformation(pods=1)
                yield KubernetesInstance('pod',
                                         pod.metadata.name,
                                         pod.metadata.labels or {},
                                         quota)

    # Local implementation
    def getQuotaForLabel(self, label):
        resources = {'namespaces': 1}
        if label.kind == 'pod':
            resources['pods'] = 1
        return QuotaInformation(**resources)

    def _getClient(self):
        return _getClient(self.connection.config_file,
                          self.connection.context,
                          self.log)

    def _createNamespace(self, namespace, labels):
        self.log.debug("Creating namespace %s", namespace)

        # Create the namespace
        ns_body = {
            'apiVersion': 'v1',
            'kind': 'Namespace',
            'metadata': {
                'name': namespace,
                'labels': labels,
            }
        }
        self.core_client.create_namespace(ns_body)

    def _createProject(self, project, labels):
        self.log.debug("Creating project %s", project)

        # Create the project
        proj_body = {
            'apiVersion': 'project.openshift.io/v1',
            'kind': 'ProjectRequest',
            'metadata': {
                'name': project,
                'labels': labels,
            }
        }
        projects = self.dynamic_client.resources.get(
            api_version='project.openshift.io/v1', kind='ProjectRequest')
        projects.create(body=proj_body)

    def _createImagePullSecrets(self, namespace, label):
        # Copy any image pull secrets required
        for conf_secret in label.image_pull_secrets:
            old_secret = self.core_client.read_namespaced_secret(
                conf_secret['name'], conf_secret['namespace'])
            new_secret = {
                'apiVersion': 'v1',
                'kind': 'Secret',
                'type': 'kubernetes.io/dockerconfigjson',
                'metadata': {
                    'name': conf_secret['name'],
                },
                'data': old_secret.data,
            }
            self.core_client.create_namespaced_secret(namespace, new_secret)

    def _createServiceAccount(self, namespace, user):
        sa_body = {
            'apiVersion': 'v1',
            'kind': 'ServiceAccount',
            'metadata': {'name': user}
        }
        self.core_client.create_namespaced_service_account(namespace, sa_body)

        secret_body = {
            'apiVersion': 'v1',
            'kind': 'Secret',
            'type': 'kubernetes.io/service-account-token',
            'metadata': {
                'name': user,
                'annotations': {
                    'kubernetes.io/service-account.name': user
                }
            }
        }
        self.core_client.create_namespaced_secret(namespace, secret_body)

    def _getToken(self, namespace, user):
        secret = self.core_client.read_namespaced_secret(user, namespace)
        ca_crt = None
        token = None
        if secret.data:
            token = secret.data.get('token')
            ca_crt = secret.data.get('ca.crt')
            if token and ca_crt:
                token = base64.b64decode(
                    token.encode('utf-8')).decode('utf-8')
                return (token, ca_crt)

    def _createRole(self, namespace, user, restricted_access):
        # Create service account role
        all_verbs = ["create", "delete", "get", "list", "patch",
                     "update", "watch"]
        if restricted_access:
            role_name = "zuul-restricted"
            role_body = {
                'kind': 'Role',
                'apiVersion': 'rbac.authorization.k8s.io/v1',
                'metadata': {
                    'name': role_name,
                },
                'rules': [{
                    'apiGroups': [""],
                    'resources': ["pods"],
                    'verbs': ["get", "list"],
                }, {
                    'apiGroups': [""],
                    'resources': ["pods/exec"],
                    'verbs': all_verbs
                }, {
                    'apiGroups': [""],
                    'resources': ["pods/log"],
                    'verbs': all_verbs
                }, {
                    'apiGroups': [""],
                    'resources': ["pods/portforward"],
                    'verbs': all_verbs
                }]
            }
        else:
            role_name = "zuul"
            role_body = {
                'kind': 'Role',
                'apiVersion': 'rbac.authorization.k8s.io/v1',
                'metadata': {
                    'name': role_name,
                },
                'rules': [{
                    'apiGroups': ["apps"],
                    'resources': ["deployments", "replicasets"],
                    'verbs': all_verbs,
                }, {
                    'apiGroups': ["batch"],
                    'resources': ["cronjobs", "jobs"],
                    'verbs': all_verbs,
                }, {
                    'apiGroups': [""],
                    'resources': ["pods", "pods/exec", "pods/log",
                                  "pods/portforward", "services",
                                  "endpoints", "configmaps", "secrets"],
                    'verbs': all_verbs,
                }]
            }
        self.rbac_client.create_namespaced_role(namespace, role_body)

        # Give service account admin access
        role_binding_body = {
            'apiVersion': 'rbac.authorization.k8s.io/v1',
            'kind': 'RoleBinding',
            'metadata': {'name': 'zuul-role'},
            'roleRef': {
                'apiGroup': 'rbac.authorization.k8s.io',
                'kind': 'Role',
                'name': role_name,
            },
            'subjects': [{
                'kind': 'ServiceAccount',
                'name': user,
                'namespace': namespace,
            }],
            'userNames': ['system:serviceaccount:%s:zuul-worker' % namespace]
        }
        self.rbac_client.create_namespaced_role_binding(
            namespace, role_binding_body)

    def _createPod(self, namespace, name, label, tags):
        kubernetes_labels = tags
        kubernetes_annotations = {}
        if label.annotations:
            kubernetes_annotations.update(label.annotations)

        pod_body = {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {
                'name': name,
                'labels': kubernetes_labels,
                'annotations': kubernetes_annotations,
            },
            'spec': label.spec,
            'restartPolicy': 'Never',
        }

        self.core_client.create_namespaced_pod(namespace, pod_body)

    def _getPod(self, namespace, name):
        pod = self.core_client.read_namespaced_pod(name, namespace)
        if pod.status.phase == "Running":
            return True
        return False

    def _deleteNamespace(self, namespace):
        self.log.debug("Deleting namespace %s", namespace)

        delete_body = {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "propagationPolicy": "Background"
        }
        self.core_client.delete_namespace(namespace, body=delete_body)

    def _deleteProject(self, project):
        self.log.debug("Deleting project %s", project)
        projects = self.dynamic_client.resources.get(
            api_version='v1', kind='Project')
        projects.delete(name=project)
