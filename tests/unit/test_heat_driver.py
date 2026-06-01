# Copyright 2026 Red Hat, Inc.
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

import fixtures

from zuul import model

from tests.base import (
    FIXTURE_DIR,
    ZuulTestCase,
    driver_config,
    iterate_timeout,
    okay_tracebacks,
    simple_layout,
)
from tests.fake_openstack import (
    FakeOpenstackCloud,
    FakeHeatProviderEndpoint,
    FakeStack,
)
from tests.unit.test_cloud_driver import BaseCloudDriverTest
from zuul.driver.heat import HeatDriver
from zuul.driver.heat.heatmodel import HeatProviderNode


class BaseHeatDriverTest(ZuulTestCase):
    cloud_test_provider_name = 'heat-main'
    cloud_test_min_instances = 1
    config_file = 'zuul-connections-nodepool.conf'

    def setUp(self):
        self.initTestConfig()
        clouds_yaml = os.path.join(FIXTURE_DIR, 'clouds.yaml')
        self.useFixture(
            fixtures.EnvironmentVariable('OS_CLIENT_CONFIG_FILE', clouds_yaml))
        self.fake_cloud = {}
        self._makeFakeCloud(None)
        self.patch(HeatDriver, '_endpoint_class', FakeHeatProviderEndpoint)
        self.patch(FakeHeatProviderEndpoint, '_fake_cloud', self.fake_cloud)
        super().setUp()

    def _makeFakeCloud(self, region):
        fake_cloud = FakeOpenstackCloud(region)
        fake_cloud.max_instances = \
            self.test_config.driver.heat.get('max_instances', 100)
        fake_cloud.max_cores = \
            self.test_config.driver.heat.get('max_cores', 100)
        fake_cloud.max_ram = \
            self.test_config.driver.heat.get('max_ram', 1000000)
        fake_cloud.max_volumes = \
            self.test_config.driver.heat.get('max_volumes', 100)
        fake_cloud.max_volume_gb = \
            self.test_config.driver.heat.get('max_volume_gb', 100)
        self.fake_cloud[region] = fake_cloud

    @contextlib.contextmanager
    def _block_futures(self):
        with mock.patch(
                'zuul.driver.heat.heatendpoint.'
                'HeatProviderEndpoint._completeApi', return_value=None):
            yield


class TestHeatDriverIntegration(BaseHeatDriverTest, BaseCloudDriverTest):
    _PERMISSIVE_POLICY_KW = {
        'flavor_name': 'normal',
        'flavor_spec': '      - name: normal',
        'provider_policy': (
            '    template-policy:\n'
            '      allowed-resource-types:\n'
            "        - 'OS::Nova::.*'\n"
            "        - '^OS::Neutron::(?!RBAC).*'\n"
            '      allowed-intrinsic-functions:\n'
            "        - 'get_param'\n"
            "        - 'get_resource'\n"
            "        - 'get_attr'"
        ),
    }

    def _getTemplateConfig(self):
        tpl_path = os.path.join(
            FIXTURE_DIR, 'layouts', 'heat', 'test-stack.yaml')
        with open(tpl_path) as f:
            template_content = f.read()
        return {'heat_template_content': template_content,
                'heat_template_path': 'zuul.heat.d/test-stack.yaml',
                'heat_parameters': {'my_param': 'my_value'}}

    def _assertProviderNodeAttributes(self, pnode):
        super()._assertProviderNodeAttributes(pnode)
        self.assertIsNotNone(pnode.heat_stack_id)

    @simple_layout(
        'layouts/heat/nodepool.yaml', enable_nodepool=True,
        replace=lambda _: TestHeatDriverIntegration._PERMISSIVE_POLICY_KW)
    def test_heat_node_lifecycle(self):
        self._test_node_lifecycle(
            'heat-stack',
            template_config=[self._getTemplateConfig()])

    @simple_layout(
        'layouts/heat/nodepool.yaml', enable_nodepool=True,
        replace=lambda _: TestHeatDriverIntegration._PERMISSIVE_POLICY_KW)
    @driver_config('heat', max_instances=2)
    def test_heat_quota(self):
        self._test_quota(
            'heat-stack',
            template_config=[self._getTemplateConfig()])

    @simple_layout(
        'layouts/heat/nodepool.yaml', enable_nodepool=True,
        replace=lambda _: TestHeatDriverIntegration._PERMISSIVE_POLICY_KW)
    def test_state_machines(self):
        self._test_heat_state_machines(
            'heat-stack', 'heat-main',
            HeatProviderNode, ['create_future', 'delete_future'],
            self._getTemplateConfig())

    @simple_layout(
        'layouts/heat/nodepool.yaml', enable_nodepool=True,
        replace=lambda _: {
            'flavor_name': 'normal',
            'flavor_spec': '      - name: normal',
            'provider_policy': (
                '    template-policy:\n'
                '      allowed-resource-types:\n'
                "        - 'OS::Nova::.*'\n"
                "        - 'OS::Neutron::"
                "(Net|Subnet|Router|RouterInterface)'"
            ),
        })
    @okay_tracebacks('TemplatePolicyViolation')
    def test_heat_template_policy_rejection(self):
        """Test that disallowed resource types are rejected.

        The template-policy layout allows OS::Nova::* and a subset of
        OS::Neutron types via regex, but excludes OS::Neutron::FloatingIP.
        The test-stack.yaml template uses FloatingIP, so the node request
        must be rejected with a FAILED state.
        """
        self.waitForLauncherLayoutSync()
        request = self.requestNodes(
            ['heat-stack'],
            template_config=[self._getTemplateConfig()])
        self.assertEqual(request.state, model.NodesetRequest.State.FAILED)

    @simple_layout(
        'layouts/heat/nodepool.yaml', enable_nodepool=True,
        replace=lambda _: {
            'flavor_name': 'normal',
            'flavor_spec': '      - name: normal',
            'provider_policy': (
                '    template-policy:\n'
                '      allowed-intrinsic-functions:\n'
                "        - 'get_param'"
            ),
        })
    @okay_tracebacks('TemplatePolicyViolation')
    def test_heat_intrinsic_function_rejection(self):
        """Test that disallowed intrinsic functions are rejected.

        The intrinsic-functions-policy layout only allows
        'get_param'.  The test-stack.yaml template also uses
        'get_resource' and 'get_attr', so the node request must
        be rejected with a FAILED state.
        """
        self.waitForLauncherLayoutSync()
        request = self.requestNodes(
            ['heat-stack'],
            template_config=[self._getTemplateConfig()])
        self.assertEqual(request.state, model.NodesetRequest.State.FAILED)

    @simple_layout(
        'layouts/heat/nodepool.yaml', enable_nodepool=True,
        replace=lambda _: {
            'flavor_name': 'limited',
            'flavor_spec': (
                '      - name: limited\n'
                '        max-instances: 1\n'
                '        max-cores: 4'
            ),
            'provider_policy': '',
        })
    @okay_tracebacks('FlavorLimitExceeded')
    def test_heat_flavor_limits_rejection(self):
        """Test that a template exceeding flavor limits is rejected.

        The flavor-limits layout defines a flavor with max-instances=1
        and max-cores=4.  The test-stack.yaml template has 2
        OS::Nova::Server resources (each with 'Fake Flavor' = 4 vcpus),
        so it exceeds both limits.  The node request must be rejected
        with a FAILED state.
        """
        self.waitForLauncherLayoutSync()
        request = self.requestNodes(
            ['heat-stack'],
            template_config=[self._getTemplateConfig()])
        self.assertEqual(request.state, model.NodesetRequest.State.FAILED)

    @simple_layout(
        'layouts/heat/nodepool.yaml', enable_nodepool=True,
        replace=lambda _: TestHeatDriverIntegration._PERMISSIVE_POLICY_KW)
    def test_heat_resource_cleanup(self):
        self.waitUntilSettled()
        self.launcher.cleanup_worker.INTERVAL = 1

        system_id = self.launcher.system.system_id
        cloud = self.fake_cloud[None]
        if not hasattr(cloud, 'stacks'):
            cloud.stacks = []

        leaked_stack = FakeStack(
            id='leaked-stack-0001',
            name='zuul-leaked-test',
            status='CREATE_COMPLETE',
            stack_status_reason=None,
            tags=[
                f'zuul_system_id={system_id}',
                'zuul_node_uuid=0000000042',
            ],
            outputs=[
                {'output_key': 'bastion_ip',
                 'output_value': '198.51.100.99'},
            ],
        )
        cloud.stacks.append(leaked_stack)
        self.assertEqual(1, len(cloud.stacks))

        self.log.debug("Start cleanup worker")
        self.launcher.cleanup_worker.start()

        for _ in iterate_timeout(30, 'leaked stack deletion'):
            if not cloud.stacks:
                break
            time.sleep(1)

    @simple_layout(
        'layouts/heat/nodepool-multi-provider.yaml', enable_nodepool=True)
    @okay_tracebacks('TemplatePolicyViolation')
    def test_heat_multi_provider_policy_filtering(self):
        """Test that provider selection filters by template policy.

        Two providers serve the same label 'heat-stack':
        - heat-strict: allows only OS::Nova::Server resources
        - heat-permissive: allows OS::Nova::* and OS::Neutron::* (except RBAC)

        The test template uses Neutron resource types, so heat-strict must be
        excluded via policy violation and the request must land on
        heat-permissive.
        """
        self.waitForLauncherLayoutSync()
        ctx = self.createZKContext(None)
        request = self.requestNodes(
            ['heat-stack'],
            template_config=[self._getTemplateConfig()])
        self.assertEqual(request.state, model.NodesetRequest.State.FULFILLED)
        self.assertEqual(len(request.nodes), 1)

        node = model.ProviderNode.fromZK(
            ctx, path=model.ProviderNode._getPath(request.nodes[0]))
        self.assertIn('heat-permissive', node.provider)
        self.assertNotIn('heat-strict', node.provider)

        request.delete(ctx)
        self.waitUntilSettled()

    def _test_heat_state_machines(self, label_name, provider_name,
                                  node_class, future_names,
                                  template_config):
        self.waitUntilSettled()
        self.launcher._running = False
        self.launcher.wake_event.set()
        self.launcher.launcher_thread.join()

        layout = self.scheds.first.sched.abide.tenants.get(
            'tenant-one').layout
        provider = layout.providers[provider_name]
        provider.getEndpoint().start()

        with self.createZKContext(None) as ctx:
            node = node_class.new(
                ctx, label=label_name,
                uuid='1234',
                tags={'zuul_node_uuid': '1234'},
                template_config=template_config)
            self._drive_state_machine(
                ctx, node, future_names, provider, True)
            self._drive_state_machine(
                ctx, node, future_names, provider, False)


class TestHeatDriverViaNodes(BaseHeatDriverTest, BaseCloudDriverTest):
    """Integration test verifying zuul_via_nodes output populates
    node_properties['via_nodes'] through the full lifecycle."""

    via_nodes_value = {
        'worker': {'host': '192.168.1.10', 'user': 'zuul', 'port': 22},
    }

    def _makeFakeCloud(self, region):
        super()._makeFakeCloud(region)
        via_nodes = self.via_nodes_value

        def _stack_outputs_with_via():
            return [
                {'output_key': 'bastion_ip',
                 'output_value': '198.51.100.1'},
                {'output_key': 'zuul_via_nodes',
                 'output_value': via_nodes},
            ]
        self.fake_cloud[region]._stack_outputs = _stack_outputs_with_via

    def _getTemplateConfig(self):
        tpl_path = os.path.join(
            FIXTURE_DIR, 'layouts', 'heat', 'test-stack.yaml')
        with open(tpl_path) as f:
            template_content = f.read()
        return {'heat_template_content': template_content,
                'heat_template_path': 'zuul.d/templates/test-stack.yaml'}

    def _assertProviderNodeAttributes(self, pnode):
        super()._assertProviderNodeAttributes(pnode)
        self.assertIsNotNone(pnode.heat_stack_id)
        self.assertEqual(
            pnode.node_properties.get('via_nodes'),
            self.via_nodes_value)

    @simple_layout(
        'layouts/heat/nodepool.yaml', enable_nodepool=True,
        replace=lambda _: TestHeatDriverIntegration._PERMISSIVE_POLICY_KW)
    def test_heat_via_nodes_lifecycle(self):
        self._test_node_lifecycle(
            'heat-stack',
            template_config=[self._getTemplateConfig()])
