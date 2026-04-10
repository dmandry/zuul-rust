# Copyright 2024-2025 Acme Gating, LLC
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

import contextlib
import os
import time
from unittest import mock
import yaml

import kubernetes

import zuul.executor
from zuul.driver.kubernetes.kubernetesendpoint import (
    _getClient,
)

from tests.fake_kubernetes import (
    FakeCoreClient,
    FakeRbacClient,
    FakeDynamicClient,
)
from tests.base import (
    BaseTestCase,
    FIXTURE_DIR,
    ZuulTestCase,
    iterate_timeout,
    okay_tracebacks,
    simple_layout,
)
from tests.unit.test_cloud_driver import BaseCloudDriverTest


class TestKubernetesConfig(BaseTestCase):

    def test_kubernetes_in_cluster_config(self):
        # Test that we instantiate api client objects from a fake
        # in-cluster config.
        token_file = os.path.join(FIXTURE_DIR, 'k8s/token')
        ca_file = os.path.join(FIXTURE_DIR, 'k8s/ca.crt')

        os.environ['KUBERNETES_SERVICE_HOST'] = '198.51.100.1'
        os.environ['KUBERNETES_SERVICE_PORT'] = '443'

        self.patch(kubernetes.config.incluster_config,
                   'SERVICE_TOKEN_FILENAME',
                   token_file)
        self.patch(kubernetes.config.incluster_config,
                   'SERVICE_CERT_FILENAME',
                   ca_file)

        # The dynamic client really tries to call the cluster api so
        # we need to mock it out.
        class AssertDynamicClient:
            def __init__(this, api_client):
                self.assertIsInstance(
                    api_client, kubernetes.client.api_client.ApiClient)

        self.patch(kubernetes.dynamic, 'DynamicClient', AssertDynamicClient)

        _getClient(None, None, self.log)

    def test_kubernetes_config_file(self):
        # Test that we instantiate api client objects from a fake
        # config file.
        config_file = os.path.join(FIXTURE_DIR, 'k8s/config')

        class AssertDynamicClient:
            def __init__(this, api_client):
                self.assertIsInstance(
                    api_client, kubernetes.client.api_client.ApiClient)

        self.patch(kubernetes.dynamic, 'DynamicClient', AssertDynamicClient)

        _getClient(config_file, 'test', self.log)


class BaseKubernetesDriverTest(ZuulTestCase):
    cloud_test_connection_type = 'kubectl'
    cloud_test_provider_name = 'kube-main'
    cloud_test_min_instances = 1
    is_openshift = False

    def setup_config(self, config_file):
        config = super().setup_config(config_file)
        kubeconfig = os.path.join(FIXTURE_DIR, 'k8s/config')
        config.set('connection kube', 'kubeconfig_file', kubeconfig)
        config.set('connection openshift', 'kubeconfig_file', kubeconfig)
        return config

    def setUp(self):
        self.initTestConfig()
        self.fake_core_client = FakeCoreClient()
        self.fake_rbac_client = FakeRbacClient()
        self.fake_dynamic_client = FakeDynamicClient(self.fake_core_client,
                                                     self.is_openshift)

        def coreClientFactory(api_client):
            return self.fake_core_client

        def rbacClientFactory(api_client):
            return self.fake_rbac_client

        def dynamicClientFactory(api_client):
            return self.fake_dynamic_client

        self.patch(kubernetes.client, 'CoreV1Api', coreClientFactory)
        self.patch(kubernetes.client, 'RbacAuthorizationV1Api',
                   rbacClientFactory)
        self.patch(kubernetes.dynamic, 'DynamicClient', dynamicClientFactory)

        super().setUp()

    @contextlib.contextmanager
    def _block_futures(self):
        with (mock.patch(
                'zuul.driver.kubernetes.kubernetesendpoint.'
                'KubernetesProviderEndpoint._completeApi', return_value=None)):
            yield


class TestKubernetesDriver(BaseKubernetesDriverTest, BaseCloudDriverTest):
    def _assertProviderNodeAttributes(self, pnode):
        # Don't call the superclass here since it assumes IP connectivity.
        self.assertEqual(pnode.connection_type,
                         self.cloud_test_connection_type)
        if checks := self.test_config.driver.kubernetes.get('node_checks'):
            checks(self, pnode)

    @simple_layout('layouts/kubernetes/nodepool.yaml', enable_nodepool=True)
    def test_kubernetes_node_lifecycle(self):
        self._test_node_lifecycle('debian-normal')

    @simple_layout('layouts/kubernetes/more.yaml', enable_nodepool=True)
    def test_kubernetes_node_lifecycle_more(self):
        # Test with more options than the normal test
        secret = {
            'apiVersion': 'v1',
            'kind': 'Secret',
            'type': 'kubernetes.io/dockerconfigjson',
            'metadata': {
                'name': 'testsecret',
            },
            'data': 'something',
        }
        self.fake_core_client.create_namespaced_secret('default', secret)
        self._test_node_lifecycle('debian-normal')

    @simple_layout('layouts/kubernetes/resource-limits.yaml',
                   enable_nodepool=True)
    def test_kubernetes_resource_limits(self):
        self._test_quota('debian-normal')

    @simple_layout('layouts/kubernetes/nodepool.yaml', enable_nodepool=True)
    def test_kubernetes_resource_cleanup(self):
        self.waitUntilSettled()
        self.launcher.cleanup_worker.INTERVAL = 1

        system_id = self.launcher.system.system_id
        tags = {
            'zuul_system_id': system_id,
            'zuul_node_uuid': '0000000042',
        }
        ns_body = {
            'apiVersion': 'v1',
            'kind': 'Namespace',
            'metadata': {
                'name': 'test',
                'labels': tags,
            }
        }
        self.fake_core_client.create_namespace(ns_body)
        self.assertEqual(2, len(self.fake_core_client.list_namespace().items))

        self.log.debug("Start cleanup worker")
        self.launcher.cleanup_worker.start()

        for _ in iterate_timeout(30, 'instance deletion'):
            if len(self.fake_core_client.list_namespace().items) == 1:
                break
            time.sleep(1)

    @simple_layout('layouts/kubernetes/nodepool.yaml', enable_nodepool=True)
    @okay_tracebacks("Unable to start kubectl port forward")
    def test_kubernetes_inventory(self):
        # Test the unique aspects of k8s inventory files
        self.patch(zuul.executor.server.KubeFwd,
                   'kubectl_command',
                   os.path.join(FIXTURE_DIR, 'fake_kubectl.sh'))
        self.executor_server.hold_jobs_in_build = True

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        build = self.getBuildByName('check-job')
        inv_path = os.path.join(build.jobdir.root, 'ansible', 'inventory.yaml')
        with open(inv_path, 'r') as f:
            inventory = yaml.safe_load(f)
        label = inventory['all']['hosts']['controller']['nodepool']['label']
        self.assertEqual('debian-normal', label)
        host = inventory['all']['hosts']['controller']['ansible_host']
        self.assertTrue(host.startswith('np'))

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1)
        self.assertNotIn('NODE_FAILURE', A.messages[0])
        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,1'),
        ], ordered=False)


class TestKubernetesDriverOpenShift(
        BaseKubernetesDriverTest, BaseCloudDriverTest):
    is_openshift = True

    def _assertProviderNodeAttributes(self, pnode):
        # Don't call the superclass here since it assumes IP connectivity.
        self.assertEqual(pnode.connection_type,
                         self.cloud_test_connection_type)
        if checks := self.test_config.driver.kubernetes.get('node_checks'):
            checks(self, pnode)

    @simple_layout('layouts/kubernetes/openshift.yaml', enable_nodepool=True)
    def test_kubernetes_node_lifecycle_openshift(self):
        self._test_node_lifecycle('debian-normal')

    @simple_layout('layouts/kubernetes/openshift.yaml', enable_nodepool=True)
    def test_kubernetes_resource_cleanup_openshift(self):
        self.waitUntilSettled()
        self.launcher.cleanup_worker.INTERVAL = 1

        system_id = self.launcher.system.system_id
        tags = {
            'zuul_system_id': system_id,
            'zuul_node_uuid': '0000000042',
        }
        proj_body = {
            'apiVersion': 'project.openshift.io/v1',
            'kind': 'ProjectRequest',
            'metadata': {
                'name': 'test',
                'labels': tags,
            }
        }
        projects = self.fake_dynamic_client.resources.get(
            api_version='project.openshift.io/v1', kind='ProjectRequest')
        projects.create(body=proj_body)

        def list_projects():
            projects = self.fake_dynamic_client.resources.get(
                api_version='v1', kind='Project')
            return projects.get().items

        self.assertEqual(2, len(list_projects()))

        self.log.debug("Start cleanup worker")
        self.launcher.cleanup_worker.start()

        for _ in iterate_timeout(30, 'instance deletion'):
            if len(list_projects()) == 1:
                break
            time.sleep(1)

    @simple_layout('layouts/kubernetes/openshift.yaml', enable_nodepool=True)
    @okay_tracebacks("Unable to start kubectl port forward")
    def test_kubernetes_inventory_openshift(self):
        # Test the unique aspects of k8s inventory files
        self.patch(zuul.executor.server.KubeFwd,
                   'kubectl_command',
                   os.path.join(FIXTURE_DIR, 'fake_kubectl.sh'))
        self.executor_server.hold_jobs_in_build = True

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        build = self.getBuildByName('check-job')
        inv_path = os.path.join(build.jobdir.root, 'ansible', 'inventory.yaml')
        with open(inv_path, 'r') as f:
            inventory = yaml.safe_load(f)
        label = inventory['all']['hosts']['controller']['nodepool']['label']
        self.assertEqual('debian-normal', label)
        host = inventory['all']['hosts']['controller']['ansible_host']
        self.assertTrue(host.startswith('np'))

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 1)
        self.assertNotIn('NODE_FAILURE', A.messages[0])
        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,1'),
        ], ordered=False)
