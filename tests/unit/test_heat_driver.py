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
from unittest.mock import Mock

import fixtures
import openstack
import yaml

from zuul import model

from tests.base import (
    BaseTestCase,
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
from zuul.driver.heat.heatparser import (
    HeatTemplateParser,
    ResourcePolicyViolation,
)


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

    @simple_layout('layouts/heat/nodepool.yaml', enable_nodepool=True)
    def test_heat_node_lifecycle(self):
        self._test_node_lifecycle(
            'heat-stack',
            template_config=[self._getTemplateConfig()])

    @simple_layout('layouts/heat/nodepool.yaml', enable_nodepool=True)
    @driver_config('heat', max_instances=2)
    def test_heat_quota(self):
        self._test_quota(
            'heat-stack',
            template_config=[self._getTemplateConfig()])

    @simple_layout('layouts/heat/nodepool.yaml', enable_nodepool=True)
    def test_state_machines(self):
        self._test_heat_state_machines(
            'heat-stack', 'heat-main',
            HeatProviderNode, ['create_future', 'delete_future'],
            self._getTemplateConfig())

    @simple_layout('layouts/heat/resource-policy-nodepool.yaml',
                   enable_nodepool=True)
    @okay_tracebacks('ResourcePolicyViolation')
    def test_heat_resource_policy_rejection(self):
        """Test that a template with a disallowed resource type is rejected.

        The resource-policy layout allows OS::Nova::* and a subset of
        OS::Neutron types via regex, but excludes OS::Neutron::FloatingIP.
        The test-stack.yaml template uses FloatingIP, so the node request
        must be rejected with a FAILED state.
        """
        self.waitForLauncherLayoutSync()
        request = self.requestNodes(
            ['heat-stack'],
            template_config=[self._getTemplateConfig()])
        self.assertEqual(request.state, model.NodesetRequest.State.FAILED)

    @simple_layout('layouts/heat/flavor-limits-nodepool.yaml',
                   enable_nodepool=True)
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

    @simple_layout('layouts/heat/nodepool.yaml', enable_nodepool=True)
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


class TestHeatTemplateParser(BaseTestCase):
    """Test Heat template parser functionality."""

    @staticmethod
    def _build_template(servers=None, volumes=None, networks=0,
                        floating_ips=0, routers=0, resource_groups=0,
                        extra_resources=None, parameters=None):
        tpl = {'heat_template_version': 'rocky'}

        if parameters:
            params = {}
            for name, attrs in parameters.items():
                p = {'type': attrs['type']}
                if 'default' in attrs:
                    p['default'] = attrs['default']
                params[name] = p
            tpl['parameters'] = params

        resources = {}

        for i, flavor in enumerate(servers or []):
            rname = f'server{i}' if len(servers) > 1 else 'server'
            resources[rname] = {
                'type': 'OS::Nova::Server',
                'properties': {'flavor': flavor},
            }

        for i, size in enumerate(volumes or []):
            rname = f'vol{i}' if len(volumes) > 1 else 'vol'
            resources[rname] = {
                'type': 'OS::Cinder::Volume',
                'properties': {'size': size},
            }

        for i in range(networks):
            rname = f'net{i}' if networks > 1 else 'net'
            resources[rname] = {'type': 'OS::Neutron::Net'}

        for i in range(floating_ips):
            rname = f'fip{i}' if floating_ips > 1 else 'fip'
            resources[rname] = {'type': 'OS::Neutron::FloatingIP'}

        for i in range(routers):
            rname = f'router{i}' if routers > 1 else 'router'
            resources[rname] = {'type': 'OS::Neutron::Router'}

        for i in range(resource_groups):
            rname = f'rg{i}' if resource_groups > 1 else 'rg'
            resources[rname] = {'type': 'OS::Heat::ResourceGroup'}

        if extra_resources:
            for rname, rtype in extra_resources.items():
                resources[rname] = {'type': rtype}

        if resources:
            tpl['resources'] = resources

        return yaml.safe_dump(tpl, default_flow_style=False, sort_keys=False)

    def setUp(self):
        super().setUp()
        self.mock_endpoint = Mock()

    def test_parse_flavor_via_get_param(self):
        """Test get_param resolution priority for flavors."""
        flavors = {
            'm1.small': Mock(vcpus=1, ram=2048),
            'm1.large': Mock(vcpus=4, ram=8192),
        }
        self.mock_endpoint._findFlavor.side_effect = flavors.get
        parser = HeatTemplateParser(self.mock_endpoint, {})

        cases = [
            # (param_default, runtime_params, expected_flavor)
            (None, {'instance_flavor': 'm1.large'}, 'm1.large'),
            ('m1.small', {'instance_flavor': 'm1.large'}, 'm1.large'),
            ('m1.small', None, 'm1.small'),
        ]
        for default, runtime, expected in cases:
            self.mock_endpoint._findFlavor.reset_mock()
            param_def = {'type': 'string'}
            if default:
                param_def['default'] = default
            template = self._build_template(
                parameters={'instance_flavor': param_def},
                servers=[{'get_param': 'instance_flavor'}])

            quota = parser.parse(template, runtime_parameters=runtime)

            expected_flavor = flavors[expected]
            self.assertEqual(quota.quota['cores'], expected_flavor.vcpus)
            self.assertEqual(quota.quota['ram'], expected_flavor.ram)
            self.mock_endpoint._findFlavor.assert_called_once_with(expected)

    def test_parse_volume_size_via_get_param(self):
        """Test volume size resolution via get_param."""
        template = self._build_template(
            parameters={'volume_size': {'type': 'number'}},
            volumes=[{'get_param': 'volume_size'}])
        parser = HeatTemplateParser(self.mock_endpoint, {})
        runtime_params = {'volume_size': 100}

        quota = parser.parse(template, runtime_parameters=runtime_params)

        self.assertEqual(quota.quota['volumes'], 1)
        self.assertEqual(quota.quota['volume-gb'], 100)

    def test_parse_fails_missing_required_properties(self):
        """Test that resources missing required properties raise ValueError."""
        parser = HeatTemplateParser(self.mock_endpoint, {})
        cases = [
            ("missing required 'flavor' property", """\
heat_template_version: 2015-04-30
resources:
  server:
    type: OS::Nova::Server
    properties:
      image: test-image
"""),
            ("missing required 'size' property", """\
heat_template_version: 2015-04-30
resources:
  volume:
    type: OS::Cinder::Volume
    properties:
      description: test volume
"""),
        ]
        for expected_msg, template in cases:
            with self.assertRaises(ValueError) as cm:
                parser.parse(template)
            self.assertIn(expected_msg, str(cm.exception))

    def test_parse_fails_unresolvable_references(self):
        """Test that unresolvable get_param references raise ValueError."""
        parser = HeatTemplateParser(self.mock_endpoint, {})
        cases = [
            (self._build_template(
                parameters={'instance_flavor': {'type': 'string'}},
                servers=[{'get_param': 'instance_flavor'}]),
             'Unable to resolve flavor reference'),
            (self._build_template(
                parameters={'volume_size': {'type': 'number'}},
                volumes=[{'get_param': 'volume_size'}]),
             'Unable to resolve volume size reference'),
        ]
        for template, expected_msg in cases:
            with self.assertRaises(ValueError) as cm:
                parser.parse(template, runtime_parameters=None)
            self.assertIn(expected_msg, str(cm.exception))

    def test_parse_direct_flavor_reference(self):
        """Test direct flavor string resolution and lookup failure."""
        parser = HeatTemplateParser(self.mock_endpoint, {})

        # Successful lookup
        self.mock_endpoint._findFlavor.return_value = Mock(vcpus=2, ram=4096)
        template = self._build_template(servers=['m1.medium'])
        quota = parser.parse(template)
        self.assertEqual(quota.quota['cores'], 2)
        self.assertEqual(quota.quota['ram'], 4096)

        # Flavor not found in cloud
        self.mock_endpoint._findFlavor.side_effect = (
            openstack.exceptions.NotFoundException(
                "Flavor m1.nonexistent not found"))
        template = self._build_template(servers=['m1.nonexistent'])
        with self.assertRaises(ValueError) as cm:
            parser.parse(template)
        self.assertIn('m1.nonexistent', str(cm.exception))

    def test_parse_multiple_servers_with_runtime_params(self):
        """Test parsing multiple servers with runtime parameters."""
        template = self._build_template(
            parameters={'bastion_flavor': {'type': 'string'},
                        'worker_flavor': {'type': 'string'}},
            servers=[{'get_param': 'bastion_flavor'},
                     {'get_param': 'worker_flavor'}])

        def find_flavor(name):
            if name == 'm1.small':
                return Mock(vcpus=1, ram=2048)
            elif name == 'm1.large':
                return Mock(vcpus=4, ram=8192)

        self.mock_endpoint._findFlavor.side_effect = find_flavor

        parser = HeatTemplateParser(self.mock_endpoint, {})
        runtime_params = {
            'bastion_flavor': 'm1.small',
            'worker_flavor': 'm1.large',
        }

        quota = parser.parse(template, runtime_parameters=runtime_params)

        self.assertEqual(quota.quota['instances'], 2)
        self.assertEqual(quota.quota['cores'], 5)  # 1 + 4
        self.assertEqual(quota.quota['ram'], 10240)  # 2048 + 8192

    def test_parse_rejects_unsupported_resource_types(self):
        """Test that unsupported resource types are rejected."""
        self.mock_endpoint._findFlavor.return_value = Mock(vcpus=1, ram=2048)
        parser_no_policy = HeatTemplateParser(
            self.mock_endpoint, resource_policy=None)

        for res_type in HeatTemplateParser.UNSUPPORTED_TYPES:
            template = self._build_template(
                servers=['m1.small'], networks=1,
                extra_resources={'r': res_type})
            with self.assertRaises(ResourcePolicyViolation) as cm:
                parser_no_policy.parse(template, validate_policy=False)
            self.assertIn(res_type, str(cm.exception))


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

    @simple_layout('layouts/heat/nodepool.yaml', enable_nodepool=True)
    def test_heat_via_nodes_lifecycle(self):
        self._test_node_lifecycle(
            'heat-stack',
            template_config=[self._getTemplateConfig()])
