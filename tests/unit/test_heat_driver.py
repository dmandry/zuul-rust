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

from unittest.mock import Mock

import openstack
import voluptuous as vs
import yaml

from tests.base import BaseTestCase
from zuul.lib.connections import ConnectionRegistry
from zuul.driver.heat import HeatDriver
from zuul.driver.heat import heatprovider
from zuul.driver.heat import heatmodel
from zuul.driver.heat.heatconnection import HeatConnection
from zuul.driver.heat.heatmodel import (
    HeatProviderNode,
    HeatInstance,
    HeatResource,
)
from zuul.driver.heat.heatparser import (
    FlavorLimitExceeded,
    HeatTemplateParser,
    ResourcePolicyViolation,
)
from zuul.driver.heat.heatendpoint import quota_from_limits
from zuul.driver.heat.heatstatemachine import (
    HeatCreateStateMachine,
    HeatDeleteStateMachine,
)
from zuul.exceptions import ConfigurationError
from zuul.model import QuotaInformation


class FakeResource(dict):
    """Dict subclass with attribute access, mimicking SDK Resource objects."""

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(key)


def _build_template(servers=None, volumes=None, networks=0,
                    floating_ips=0, routers=0, resource_groups=0,
                    extra_resources=None, parameters=None):
    """Build a HOT YAML template string from declarative resource descriptions.

    :param servers: list of flavor specs -- strings for direct flavors
        (e.g. 'm1.small') or dicts for intrinsic functions
        (e.g. {'get_param': 'bastion_flavor'}).
    :param volumes: list of size specs -- ints for direct sizes (in GB)
        or dicts for intrinsic functions.
    :param networks: number of OS::Neutron::Net resources.
    :param floating_ips: number of OS::Neutron::FloatingIP resources.
    :param routers: number of OS::Neutron::Router resources.
    :param resource_groups: number of OS::Heat::ResourceGroup resources.
    :param extra_resources: dict of {name: resource_type_string} for
        resource types not covered by the above args.
    :param parameters: dict mapping parameter name to attributes dict
        with 'type' (required) and 'default' (optional).
    :returns: valid HOT YAML template string.
    """
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


class TestHeatDriver(BaseTestCase):
    """Test Heat driver registration and basic functionality."""

    def test_driver_registration(self):
        """Test that the Heat driver is registered properly."""
        registry = ConnectionRegistry()
        self.addCleanup(registry.stop)
        self.assertIn('heat', registry.drivers)
        driver = registry.drivers['heat']
        self.assertEqual(driver.name, 'heat')

    def test_driver_node_class(self):
        """Test that the Heat driver returns the correct node class."""
        registry = ConnectionRegistry()
        self.addCleanup(registry.stop)
        driver = registry.drivers['heat']
        node_class = driver.getProviderNodeClass()
        self.assertEqual(node_class.__name__, 'HeatProviderNode')


class TestHeatModel(BaseTestCase):
    """Test Heat model classes."""

    def test_heat_provider_node_driver_data(self):
        """Test HeatProviderNode getDriverData serialization."""
        node = HeatProviderNode()
        node._set(
            heat_stack_id='test-stack-id-123',
            heat_stack_name='zuul-test-stack',
            heat_stack_status='CREATE_COMPLETE',
            heat_template_path='.zuul/templates/test.yaml',
            heat_stack_outputs={'bastion_ip': '192.0.2.100'},
            heat_physical_resource_ids=['server-uuid-1', 'vol-uuid-2'],
        )

        data = node.getDriverData()
        self.assertEqual(data['heat_stack_id'], 'test-stack-id-123')
        self.assertEqual(data['heat_stack_name'], 'zuul-test-stack')
        self.assertEqual(data['heat_stack_status'], 'CREATE_COMPLETE')
        self.assertEqual(data['heat_template_path'],
                         '.zuul/templates/test.yaml')
        self.assertEqual(data['heat_stack_outputs'],
                         {'bastion_ip': '192.0.2.100'})
        self.assertEqual(data['heat_physical_resource_ids'],
                         ['server-uuid-1', 'vol-uuid-2'])

    def test_heat_instance_creation(self):
        """Test HeatInstance creation."""
        quota = QuotaInformation(instances=3, cores=12, ram=24576)
        instance = HeatInstance(
            cloud='mycloud',
            region='RegionOne',
            stack_id='test-stack-123',
            bastion_ip='192.0.2.50',
            outputs={'internal_ips': ['10.0.0.10', '10.0.0.11']},
            quota=quota
        )

        self.assertEqual(instance.heat_stack_id, 'test-stack-123')
        self.assertEqual(instance.cloud, 'mycloud')
        self.assertEqual(instance.region, 'RegionOne')
        self.assertEqual(instance.public_ipv4, '192.0.2.50')
        self.assertEqual(instance.interface_ip, '192.0.2.50')
        self.assertEqual(instance.connection_port, 22)
        self.assertEqual(instance.connection_type, 'ssh')
        self.assertEqual(instance.heat_stack_outputs['internal_ips'],
                         ['10.0.0.10', '10.0.0.11'])
        self.assertEqual(instance.getQuotaInformation(), quota)
        self.assertEqual(instance.external_id, 'stack=test-stack-123')

    def test_heat_resource_stack(self):
        """Test HeatResource for a stack."""
        metadata = {'zuul_system_id': 'test-system',
                    'zuul_node_uuid': 'node-123'}
        resource = HeatResource(metadata, HeatResource.TYPE_STACK,
                                'stack-456')

        self.assertEqual(resource.type, HeatResource.TYPE_STACK)
        self.assertEqual(resource.id, 'stack-456')
        self.assertEqual(resource.unique_id, 'heat-stack-stack-456')
        self.assertEqual(resource.metadata['zuul_system_id'], 'test-system')


class TestHeatConnection(BaseTestCase):
    """Test Heat connection implementation."""

    def setUp(self):
        super().setUp()
        self.driver = HeatDriver()

    def test_connection_creation_with_cloud(self):
        """Test creating a connection with required cloud parameter."""
        config = {
            'cloud': 'mycloud'
        }
        conn = self.driver.getConnection('heat-conn', config)
        self.assertIsInstance(conn, HeatConnection)
        self.assertEqual(conn.connection_name, 'heat-conn')
        self.assertEqual(conn.cloud_name, 'mycloud')
        self.assertIsNone(conn.region_name)
        self.assertEqual(conn.rate, 2)  # default rate

    def test_connection_creation_with_region(self):
        """Test creating a connection with optional region parameter."""
        config = {
            'cloud': 'mycloud',
            'region': 'RegionOne'
        }
        conn = self.driver.getConnection('heat-conn', config)
        self.assertEqual(conn.cloud_name, 'mycloud')
        self.assertEqual(conn.region_name, 'RegionOne')

    def test_connection_creation_with_rate(self):
        """Test creating a connection with custom rate limit."""
        config = {
            'cloud': 'mycloud',
            'rate': 5
        }
        conn = self.driver.getConnection('heat-conn', config)
        self.assertEqual(conn.rate, 5)

    def test_connection_creation_with_config_file(self):
        """Test creating a connection with custom clouds.yaml path."""
        config = {
            'cloud': 'mycloud',
            'client_config_file': '/custom/path/clouds.yaml'
        }
        conn = self.driver.getConnection('heat-conn', config)
        self.assertEqual(conn.config_files, '/custom/path/clouds.yaml')

    def test_connection_missing_cloud_parameter(self):
        """Test that connection creation fails without cloud parameter."""
        config = {
            'region': 'RegionOne'
        }
        with self.assertRaises(ConfigurationError) as cm:
            self.driver.getConnection('heat-conn', config)
        self.assertIn('cloud', str(cm.exception))
        self.assertIn('required', str(cm.exception))

    def test_connection_full_config(self):
        """Test creating a connection with all parameters."""
        config = {
            'cloud': 'production-cloud',
            'region': 'RegionTwo',
            'rate': 10,
            'client_config_file': '/etc/zuul/clouds.yaml'
        }
        conn = self.driver.getConnection('heat-prod', config)
        self.assertEqual(conn.connection_name, 'heat-prod')
        self.assertEqual(conn.cloud_name, 'production-cloud')
        self.assertEqual(conn.region_name, 'RegionTwo')
        self.assertEqual(conn.rate, 10)
        self.assertEqual(conn.config_files, '/etc/zuul/clouds.yaml')


class TestHeatProviderSchema(BaseTestCase):
    """Test Heat provider schema validation."""

    def setUp(self):
        super().setUp()
        self.driver = HeatDriver()

    def test_provider_schema_basic(self):
        """Test basic provider configuration validation."""
        schema = self.driver.getProviderSchema()
        config = {
            'name': 'test-provider',
            'section': 'test-section',
            'connection': 'test-connection',
            'labels': [],
            'images': [],
            'flavors': [],
        }
        validated = schema(config)
        self.assertIsInstance(validated, dict)
        self.assertEqual(validated['name'], 'test-provider')

    def test_provider_schema_resource_limits(self):
        """Test resource-limits schema validation."""
        schema = self.driver.getProviderSchema()
        config = {
            'name': 'test-provider',
            'section': 'test-section',
            'connection': 'test-connection',
            'labels': [],
            'images': [],
            'flavors': [],
            'resource-limits': {
                'stacks': 10,
                'instances': 50,
                'cores': 200,
                'ram': 409600,
                'volumes': 20,
                'volume-gb': 1000,
                'networks': 5,
                'routers': 3,
                'routers-with-external-gw': 2,
                'floating-ips': 5,
            }
        }
        validated = schema(config)
        self.assertEqual(validated['resource_limits']['stacks'], 10)
        self.assertEqual(validated['resource_limits']['cores'], 200)
        self.assertEqual(validated['resource_limits']['floating_ips'], 5)

    def test_label_schema(self):
        """Test Heat label schema validation."""
        schema_class = self.driver.getProviderSchemaClass()
        schema_instance = schema_class()
        label_schema = schema_instance.getLabelSchema(internal=True)

        label_config = {
            'name': 'test-label',
            'project_canonical_name': 'example.com/project',
            'config_hash': 'abc123',
            'image': 'test-image',
            'flavor': 'test-flavor',
            'region': 'RegionOne',
        }
        validated = label_schema(label_config)
        self.assertEqual(validated['name'], 'test-label')
        self.assertEqual(validated['region'], 'RegionOne')

    def test_flavor_schema(self):
        """Test Heat flavor schema with resource limits."""
        schema_class = self.driver.getProviderSchemaClass()
        schema_instance = schema_class()
        flavor_schema = schema_instance.getFlavorSchema(internal=True)

        flavor_config = {
            'name': 'large-stack',
            'project_canonical_name': 'example.com/project',
            'config_hash': 'def456',
            'max-instances': 10,
            'max-cores': 40,
            'max-ram': 81920,
            'max-volumes': 5,
            'max-volume-gb': 500,
            'max-networks': 3,
            'max-routers': 2,
            'max-routers-with-external-gw': 1,
            'max-floating-ips': 2,
        }
        validated = flavor_schema(flavor_config)
        self.assertEqual(validated['name'], 'large-stack')
        self.assertEqual(validated['max_instances'], 10)
        self.assertEqual(validated['max_cores'], 40)
        self.assertEqual(validated['max_floating_ips'], 2)

    def test_flavor_schema_partial_limits(self):
        """Omitted optional flavor limits must not fail validation."""
        schema_class = self.driver.getProviderSchemaClass()
        schema_instance = schema_class()
        flavor_schema = schema_instance.getFlavorSchema(internal=True)

        flavor_config = {
            'name': 'tutorial-heat-flavor',
            'project_canonical_name': 'example.com/project',
            'config_hash': 'def456',
            'max-ram': 32768,
            'max-cores': 64,
            'max-volumes': 10,
        }
        validated = flavor_schema(flavor_config)
        self.assertEqual(validated['name'], 'tutorial-heat-flavor')
        self.assertEqual(validated['max_ram'], 32768)
        self.assertEqual(validated['max_cores'], 64)
        self.assertEqual(validated['max_volumes'], 10)
        self.assertNotIn('max_instances', validated)
        self.assertNotIn('max_volume_gb', validated)
        self.assertNotIn('max_networks', validated)
        self.assertNotIn('max_routers', validated)
        self.assertNotIn('max_routers_with_external_gw', validated)
        self.assertNotIn('max_floating_ips', validated)

    def test_provider_schema_with_resource_limits(self):
        """Test provider schema accepts resource-limits."""
        schema_class = self.driver.getProviderSchemaClass()
        schema_instance = schema_class()
        provider_schema = schema_instance.getProviderSchema(internal=True)

        provider_config = {
            'name': 'heat-provider',
            'section': 'heat',
            'connection': 'heat-cloud',
            'labels': [{
                'name': 'heat-lab',
                'project_canonical_name': 'example.com/project',
                'config_hash': None,
            }],
            'images': [{
                'name': 'heat-placeholder',
                'type': 'cloud',
                'project_canonical_name': 'example.com/project',
                'config_hash': 'img123',
                'branch': 'master',
            }],
            'flavors': [{
                'name': 'lab-flavor',
                'project_canonical_name': 'example.com/project',
                'config_hash': 'flav123',
            }],
            'resource-limits': {
                'stacks': 10,
                'instances': 50,
                'cores': 200,
            },
            'resource-policy': {
                'allowed-resource-types': [
                    '^(?!OS::Heat::ResourceGroup$).*',
                ],
            },
        }
        validated = provider_schema(provider_config)
        self.assertEqual(validated['name'], 'heat-provider')
        self.assertEqual(validated['resource_limits']['stacks'], 10)
        self.assertEqual(validated['resource_limits']['instances'], 50)
        self.assertIn(
            '^(?!OS::Heat::ResourceGroup$).*',
            validated['resource_policy']['allowed_resource_types'])

    def test_image_schema(self):
        """Test Heat image schema validation."""
        schema_class = self.driver.getProviderSchemaClass()
        schema_instance = schema_class()
        image_schema = schema_instance.getImageSchema(internal=True)

        # Test cloud image without image-id
        cloud_image = {
            'name': 'ubuntu-22.04',
            'project_canonical_name': 'example.com/project',
            'config_hash': 'ghi789',
            'branch': 'main',
            'type': 'cloud',
        }
        validated = image_schema(cloud_image)
        self.assertEqual(validated['name'], 'ubuntu-22.04')
        self.assertEqual(validated['type'], 'cloud')

        # Test cloud image with image-id
        cloud_image_with_id = {
            'name': 'bastion-image',
            'project_canonical_name': 'example.com/project',
            'config_hash': 'abc123',
            'branch': 'main',
            'type': 'cloud',
            'image-id': 'glance-uuid-456',
        }
        validated = image_schema(cloud_image_with_id)
        self.assertEqual(validated['name'], 'bastion-image')
        self.assertEqual(validated['type'], 'cloud')
        self.assertEqual(validated['image_id'], 'glance-uuid-456')

        # Test that zuul-type images are rejected
        # Heat doesn't implement upload/import methods, only cloud images work
        zuul_image = {
            'name': 'test-image',
            'project_canonical_name': 'example.com/project',
            'config_hash': 'jkl012',
            'branch': 'main',
            'type': 'zuul',
        }
        with self.assertRaises(vs.Invalid):
            image_schema(zuul_image)


class TestHeatTemplateParser(BaseTestCase):
    """Test Heat template parser functionality."""

    def setUp(self):
        super().setUp()
        self.mock_endpoint = Mock()

    def test_parse_with_runtime_parameters_flavor(self):
        """Test parsing template with flavor from runtime parameters."""
        template = _build_template(
            parameters={'instance_flavor': {'type': 'string'}},
            servers=[{'get_param': 'instance_flavor'}])
        mock_flavor = Mock()
        mock_flavor.vcpus = 4
        mock_flavor.ram = 8192
        self.mock_endpoint._findFlavor.return_value = mock_flavor

        parser = HeatTemplateParser(self.mock_endpoint, {})
        runtime_params = {'instance_flavor': 'm1.large'}

        quota = parser.parse(template, runtime_parameters=runtime_params)

        self.assertEqual(quota.quota['instances'], 1)
        self.assertEqual(quota.quota['cores'], 4)
        self.assertEqual(quota.quota['ram'], 8192)
        self.mock_endpoint._findFlavor.assert_called_once_with('m1.large')

    def test_parse_with_runtime_parameters_overrides_default(self):
        """Test that runtime parameters override template defaults."""
        template = _build_template(
            parameters={'instance_flavor': {'type': 'string',
                                            'default': 'm1.small'}},
            servers=[{'get_param': 'instance_flavor'}])
        small_flavor = Mock(vcpus=1, ram=2048)
        large_flavor = Mock(vcpus=4, ram=8192)

        def find_flavor(name):
            if name == 'm1.small':
                return small_flavor
            elif name == 'm1.large':
                return large_flavor

        self.mock_endpoint._findFlavor.side_effect = find_flavor

        parser = HeatTemplateParser(self.mock_endpoint, {})
        runtime_params = {'instance_flavor': 'm1.large'}

        quota = parser.parse(template, runtime_parameters=runtime_params)

        self.assertEqual(quota.quota['cores'], 4)
        self.assertEqual(quota.quota['ram'], 8192)
        self.mock_endpoint._findFlavor.assert_called_once_with('m1.large')

    def test_parse_without_runtime_parameters_uses_default(self):
        """Test that template defaults are used when no runtime params."""
        template = _build_template(
            parameters={'instance_flavor': {'type': 'string',
                                            'default': 'm1.small'}},
            servers=[{'get_param': 'instance_flavor'}])
        mock_flavor = Mock(vcpus=1, ram=2048)
        self.mock_endpoint._findFlavor.return_value = mock_flavor

        parser = HeatTemplateParser(self.mock_endpoint, {})

        quota = parser.parse(template, runtime_parameters=None)

        self.assertEqual(quota.quota['cores'], 1)
        self.assertEqual(quota.quota['ram'], 2048)
        self.mock_endpoint._findFlavor.assert_called_once_with('m1.small')

    def test_parse_with_runtime_parameters_volume_size(self):
        """Test parsing volume size from runtime parameters."""
        template = _build_template(
            parameters={'volume_size': {'type': 'number'}},
            volumes=[{'get_param': 'volume_size'}])
        parser = HeatTemplateParser(self.mock_endpoint, {})
        runtime_params = {'volume_size': 100}

        quota = parser.parse(template, runtime_parameters=runtime_params)

        self.assertEqual(quota.quota['volumes'], 1)
        self.assertEqual(quota.quota['volume-gb'], 100)

    def test_parse_fails_without_flavor_parameter(self):
        """Test that parsing fails when flavor parameter has no value."""
        template = _build_template(
            parameters={'instance_flavor': {'type': 'string'}},
            servers=[{'get_param': 'instance_flavor'}])
        parser = HeatTemplateParser(self.mock_endpoint, {})

        with self.assertRaises(ValueError) as cm:
            parser.parse(template, runtime_parameters=None)

        self.assertIn('Unable to resolve flavor reference', str(cm.exception))

    def test_parse_fails_missing_volume_size(self):
        """Test that parsing fails when volume has no size property."""
        template = """\
heat_template_version: 2015-04-30
resources:
  volume:
    type: OS::Cinder::Volume
    properties:
      description: test volume
"""
        parser = HeatTemplateParser(self.mock_endpoint, {})

        with self.assertRaises(ValueError) as cm:
            parser.parse(template)

        self.assertIn("missing required 'size' property", str(cm.exception))

    def test_parse_fails_unresolvable_volume_size(self):
        """Test that parsing fails when volume size cannot be resolved."""
        template = _build_template(
            parameters={'volume_size': {'type': 'number'}},
            volumes=[{'get_param': 'volume_size'}])
        parser = HeatTemplateParser(self.mock_endpoint, {})

        with self.assertRaises(ValueError) as cm:
            parser.parse(template, runtime_parameters=None)

        self.assertIn('Unable to resolve volume size reference',
                      str(cm.exception))

    def test_parse_fails_missing_flavor_property(self):
        """Test that parsing fails when server has no flavor property."""
        template = """\
heat_template_version: 2015-04-30
resources:
  server:
    type: OS::Nova::Server
    properties:
      image: test-image
"""
        parser = HeatTemplateParser(self.mock_endpoint, {})

        with self.assertRaises(ValueError) as cm:
            parser.parse(template)

        self.assertIn("missing required 'flavor' property", str(cm.exception))

    def test_parse_direct_flavor_reference(self):
        """Test parsing with direct flavor reference (no parameters)."""
        template = _build_template(servers=['m1.medium'])
        mock_flavor = Mock(vcpus=2, ram=4096)
        self.mock_endpoint._findFlavor.return_value = mock_flavor

        parser = HeatTemplateParser(self.mock_endpoint, {})

        quota = parser.parse(template)

        self.assertEqual(quota.quota['cores'], 2)
        self.assertEqual(quota.quota['ram'], 4096)
        self.mock_endpoint._findFlavor.assert_called_once_with('m1.medium')

    def test_get_flavor_resources_raises_on_lookup_failure(self):
        """Test _getFlavorResources raises ValueError

        On flavor lookup failure _getFlavorResources raises ValueError
        """
        template = _build_template(servers=['m1.nonexistent'])
        self.mock_endpoint._findFlavor.side_effect = (
            openstack.exceptions.NotFoundException(
                "Flavor m1.nonexistent not found"))

        parser = HeatTemplateParser(self.mock_endpoint, {})

        with self.assertRaises(ValueError) as cm:
            parser.parse(template)

        self.assertIn('m1.nonexistent', str(cm.exception))
        self.mock_endpoint._findFlavor.assert_called_once_with(
            'm1.nonexistent')

    def test_parse_multiple_servers_with_runtime_params(self):
        """Test parsing multiple servers with runtime parameters."""
        template = _build_template(
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

    def test_parse_rejects_resource_group(self):
        """Test that OS::Heat::ResourceGroup raises ResourcePolicyViolation."""
        template = _build_template(resource_groups=1)
        parser = HeatTemplateParser(self.mock_endpoint, {})

        with self.assertRaises(ResourcePolicyViolation) as cm:
            parser.parse(template)

        self.assertIn('OS::Heat::ResourceGroup', str(cm.exception))
        self.assertIn('not supported', str(cm.exception))
        self.assertIn('cannot expand resource groups', str(cm.exception))

    def test_parse_rejects_remote_stack(self):
        """Test that OS::Heat::Stack raises ResourcePolicyViolation."""
        template = _build_template(
            extra_resources={'nested': 'OS::Heat::Stack'})
        parser = HeatTemplateParser(self.mock_endpoint, {})

        with self.assertRaises(ResourcePolicyViolation) as cm:
            parser.parse(template)

        self.assertIn('OS::Heat::Stack', str(cm.exception))
        self.assertIn('not supported', str(cm.exception))
        self.assertIn('nested/remote stack', str(cm.exception))

    def test_parse_rejects_resource_group_with_other_resources(self):
        """Test ResourceGroup is rejected even alongside valid resources."""
        template = _build_template(
            servers=['m1.small'], networks=1, resource_groups=1)
        mock_flavor = Mock(vcpus=1, ram=2048)
        self.mock_endpoint._findFlavor.return_value = mock_flavor
        parser = HeatTemplateParser(self.mock_endpoint, {})

        with self.assertRaises(ResourcePolicyViolation) as cm:
            parser.parse(template)

        self.assertIn('OS::Heat::ResourceGroup', str(cm.exception))

    def test_parse_rejects_resource_group_regardless_of_policy(self):
        """Test ResourceGroup is blocked even without resource-policy config.

        The unsupported type check is unconditional -- it applies whether
        or not the operator has configured allowed-resource-types.
        """
        template = _build_template(resource_groups=1)
        parser = HeatTemplateParser(self.mock_endpoint, resource_policy=None)

        with self.assertRaises(ResourcePolicyViolation) as cm:
            parser.parse(template, validate_policy=False)

        self.assertIn('OS::Heat::ResourceGroup', str(cm.exception))


class TestHeatEndpoint(BaseTestCase):
    """Test Heat endpoint functionality."""

    def setUp(self):
        super().setUp()
        self.driver = HeatDriver()

    def _create_mock_zk_client(self):
        """Create a mock ZooKeeper client for testing."""
        zk_client = Mock()
        # Mock the underlying client with proper method stubs
        zk_client.client = Mock()
        zk_client.client.get_children = Mock(return_value=[])
        return zk_client

    def _create_endpoint(self, connection_name='test-cloud', region=None):
        connection = Mock()
        connection.connection_name = connection_name
        connection.rate = 2
        connection.cloud_name = 'test'
        connection.config_files = None
        zk_client = self._create_mock_zk_client()
        return self.driver._getEndpoint(
            zk_client, connection, region, 'test-system')

    def test_endpoint_caching(self):
        """Test that endpoints are cached by region."""
        endpoint1 = self._create_endpoint(region='RegionOne')
        endpoint2 = self._create_endpoint(region='RegionOne')

        # Should be the same cached instance
        self.assertIs(endpoint1, endpoint2)

    def test_endpoint_different_regions(self):
        """Test that different regions get different endpoints."""
        endpoint1 = self._create_endpoint(region='RegionOne')
        endpoint2 = self._create_endpoint(region='RegionTwo')

        # Should be different instances
        self.assertIsNot(endpoint1, endpoint2)

    def test_endpoint_name_construction(self):
        """Test endpoint name is constructed correctly."""
        endpoint = self._create_endpoint(
            connection_name='my-cloud', region='RegionOne')

        # Name should be connection-region
        self.assertEqual(endpoint.name, 'my-cloud-RegionOne')

    def test_endpoint_name_no_region(self):
        """Test endpoint name when region is None."""
        endpoint = self._create_endpoint(connection_name='my-cloud')

        # Name should handle None region
        self.assertEqual(endpoint.name, 'my-cloud-default')

    def test_get_stack_transient_error_retry(self):
        """Test _getStack retries on transient HTTP 5xx errors."""
        endpoint = self._create_endpoint()

        # Mock HTTP 503 error (transient)
        http_error = openstack.exceptions.HttpException(
            message="Service Unavailable",
            http_status=503)

        mock_client = Mock()
        mock_client.orchestration.get_stack = Mock(side_effect=http_error)
        endpoint._client = mock_client

        # Should return None (triggers retry), not raise
        result = endpoint._getStack('test-stack-id')
        self.assertIsNone(result)

    def test_get_stack_client_error_raises(self):
        """Test _getStack raises on client errors (4xx)."""
        endpoint = self._create_endpoint()

        # Mock HTTP 400 error (client error)
        http_error = openstack.exceptions.HttpException(
            message="Bad Request",
            http_status=400)

        mock_client = Mock()
        mock_client.orchestration.get_stack = Mock(side_effect=http_error)
        endpoint._client = mock_client

        # Should raise (not retry)
        with self.assertRaises(openstack.exceptions.HttpException):
            endpoint._getStack('test-stack-id')

    def test_get_stack_not_found(self):
        """Test _getStack returns None when stack not found."""
        endpoint = self._create_endpoint()

        # Mock ResourceNotFound
        mock_client = Mock()
        mock_client.orchestration.get_stack = Mock(
            side_effect=openstack.exceptions.ResourceNotFound)
        endpoint._client = mock_client

        # Should return None (expected case)
        result = endpoint._getStack('test-stack-id')
        self.assertIsNone(result)

    def test_refresh_quota_limits_updates_cache(self):
        """Test refreshQuotaLimits fetches and caches quota limits."""
        endpoint = self._create_endpoint()

        # Mock the quota cache to avoid ZooKeeper operations
        mock_quota_cache = Mock()
        endpoint.quota_cache = mock_quota_cache

        # Mock the OpenStack client
        mock_client = Mock()
        # Mock compute limits using the same structure as OpenStack driver
        mock_compute_limits = Mock()
        mock_compute_limits.max_total_instances = 10
        mock_compute_limits.max_total_cores = 20
        mock_compute_limits.max_total_ram_size = 51200
        mock_client.get_compute_limits.return_value = mock_compute_limits
        mock_client.current_project_id = 'test-project'

        # Mock volume limits using the same structure as OpenStack driver
        mock_volume_limits = {
            'absolute': {
                'maxTotalVolumes': 10,
                'maxTotalVolumeGigabytes': 1000
            }
        }
        mock_client.get_volume_limits.return_value = mock_volume_limits

        mock_network_quota = {
            'network': 10,
            'router': 5,
            'floatingip': 10,
            'port': 50
        }
        mock_client.network.get_quota.return_value = mock_network_quota

        endpoint._client = mock_client

        # Test 1: No cached limits, should fetch and cache (returns True)
        mock_quota_cache.hasLimits.return_value = False
        result = endpoint.refreshQuotaLimits(update=False)
        self.assertTrue(result)
        mock_client.get_compute_limits.assert_called_once()
        mock_quota_cache.setLimits.assert_called_once()

        # Test 2: Has cached limits, update=False, should skip (returns False)
        mock_client.get_compute_limits.reset_mock()
        mock_quota_cache.setLimits.reset_mock()
        mock_quota_cache.hasLimits.return_value = True
        result = endpoint.refreshQuotaLimits(update=False)
        self.assertFalse(result)
        mock_client.get_compute_limits.assert_not_called()
        mock_quota_cache.setLimits.assert_not_called()

        # Test 3: Has cached limits, update=True, should refresh (returns True)
        result = endpoint.refreshQuotaLimits(update=True)
        self.assertTrue(result)
        mock_client.get_compute_limits.assert_called_once()
        mock_quota_cache.setLimits.assert_called_once()

    def test_quota_from_limits_all_keys(self):
        """Test quota_from_limits produces all expected quota keys."""
        mock_compute = Mock()
        mock_compute.max_total_instances = 10
        mock_compute.max_total_cores = 20
        mock_compute.max_total_ram_size = 51200

        volume = {
            'absolute': {
                'maxTotalVolumes': 10,
                'maxTotalVolumeGigabytes': 1000,
            }
        }

        network = {'network': 5, 'router': 3, 'floatingip': 8, 'port': 50}

        quota = quota_from_limits(mock_compute, volume, network)
        self.assertEqual(quota.quota['instances'], 10)
        self.assertEqual(quota.quota['cores'], 20)
        self.assertEqual(quota.quota['ram'], 51200)
        self.assertEqual(quota.quota['volumes'], 10)
        self.assertEqual(quota.quota['volume-gb'], 1000)
        self.assertEqual(quota.quota['networks'], 5)
        self.assertEqual(quota.quota['routers'], 3)
        self.assertEqual(quota.quota['floating-ips'], 8)
        self.assertEqual(quota.quota['ports'], 50)

    def test_quota_from_limits_unlimited(self):
        """Test quota_from_limits converts -1 to infinity."""
        import math

        mock_compute = Mock()
        mock_compute.max_total_instances = -1
        mock_compute.max_total_cores = -1
        mock_compute.max_total_ram_size = -1

        network = {'floatingip': -1}

        quota = quota_from_limits(mock_compute, None, network)
        self.assertEqual(quota.quota['instances'], math.inf)
        self.assertEqual(quota.quota['cores'], math.inf)
        self.assertEqual(quota.quota['ram'], math.inf)
        self.assertEqual(quota.quota['floating-ips'], math.inf)

    def test_find_flavor_success(self):
        """Test _findFlavor returns flavor when found."""
        endpoint = self._create_endpoint()

        # Mock successful flavor lookup
        mock_flavor = Mock()
        mock_flavor.vcpus = 4
        mock_flavor.ram = 8192
        endpoint._client = Mock()
        endpoint._client.compute.find_flavor = Mock(return_value=mock_flavor)

        # Call _findFlavor
        result = endpoint._findFlavor('m1.large')

        # Should return the flavor
        self.assertEqual(result, mock_flavor)
        endpoint._client.compute.find_flavor.assert_called_once_with(
            'm1.large')

    def test_find_flavor_not_found(self):
        """Test _findFlavor raises NotFoundException when flavor not found."""
        endpoint = self._create_endpoint()

        # Mock flavor not found (returns None)
        endpoint._client = Mock()
        endpoint._client.compute.find_flavor = Mock(return_value=None)

        # Should raise NotFoundException
        with self.assertRaises(openstack.exceptions.NotFoundException) as cm:
            endpoint._findFlavor('nonexistent-flavor')

        # Check error message contains flavor name
        self.assertIn('nonexistent-flavor', str(cm.exception))
        self.assertIn('not found', str(cm.exception).lower())

    def _create_endpoint_for_listing(self, managed_resource_ids=None):
        """Create an endpoint ready for listInstances() testing.

        Returns (endpoint, mock_client) with all resource list methods
        mocked empty and no stacks by default.
        """
        endpoint = self._create_endpoint()
        mock_client = Mock()
        endpoint._client = mock_client
        mock_client.orchestration.stacks.return_value = []
        endpoint._listServers = Mock(return_value=[])
        endpoint._listVolumes = Mock(return_value=[])
        endpoint._listNetworks = Mock(return_value=[])
        endpoint._listRouters = Mock(return_value=[])
        endpoint._listFloatingIps = Mock(return_value=[])
        endpoint.managed_resource_ids = managed_resource_ids or {}
        return endpoint, mock_client

    def test_list_instances_stacks(self):
        """Tests listInstances yields stacks with stacks=1 quota."""
        endpoint, mock_client = self._create_endpoint_for_listing()

        mock_stack1 = Mock()
        mock_stack1.id = 'stack-1'
        mock_stack1.status = 'CREATE_COMPLETE'

        mock_stack2 = Mock()
        mock_stack2.id = 'stack-2'
        mock_stack2.status = 'UPDATE_COMPLETE'

        # DELETE_COMPLETE should be skipped
        mock_stack3 = Mock()
        mock_stack3.id = 'stack-3'
        mock_stack3.status = 'DELETE_COMPLETE'

        # DELETE_FAILED still consumes resources
        mock_stack4 = Mock()
        mock_stack4.id = 'stack-4'
        mock_stack4.status = 'DELETE_FAILED'

        # DELETE_IN_PROGRESS still consumes resources
        mock_stack5 = Mock()
        mock_stack5.id = 'stack-5'
        mock_stack5.status = 'DELETE_IN_PROGRESS'

        mock_client.orchestration.stacks.return_value = [
            mock_stack1, mock_stack2, mock_stack3, mock_stack4, mock_stack5
        ]

        mock_details1 = {
            'id': 'stack-1',
            'stack_status': 'CREATE_COMPLETE',
            'outputs': [
                {'output_key': 'bastion_ip', 'output_value': '10.0.0.1'},
                {'output_key': 'other_output', 'output_value': 'value1'}
            ],
            'tags': [
                'zuul_system_id=test-system',
                'zuul_node_uuid=node-1',
                'custom_tag=value'
            ]
        }

        mock_details2 = {
            'id': 'stack-2',
            'stack_status': 'UPDATE_COMPLETE',
            'outputs': [
                {'output_key': 'bastion_ip', 'output_value': '10.0.0.2'}
            ],
            'tags': ['zuul_system_id=test-system', 'zuul_node_uuid=node-2']
        }

        mock_details4 = {
            'id': 'stack-4',
            'stack_status': 'DELETE_FAILED',
            'outputs': [
                {'output_key': 'bastion_ip', 'output_value': '10.0.0.4'}
            ],
            'tags': ['zuul_system_id=test-system', 'zuul_node_uuid=node-4']
        }

        mock_details5 = {
            'id': 'stack-5',
            'stack_status': 'DELETE_IN_PROGRESS',
            'outputs': [
                {'output_key': 'bastion_ip', 'output_value': '10.0.0.5'}
            ],
            'tags': ['zuul_system_id=test-system', 'zuul_node_uuid=node-5']
        }

        def get_stack_side_effect(stack_id):
            if stack_id == 'stack-1':
                return mock_details1
            elif stack_id == 'stack-2':
                return mock_details2
            elif stack_id == 'stack-4':
                return mock_details4
            elif stack_id == 'stack-5':
                return mock_details5
            raise Exception(f"Unexpected stack_id: {stack_id}")

        mock_client.orchestration.get_stack.side_effect = (
            get_stack_side_effect)

        instances = list(endpoint.listInstances())

        # Filter to only stack instances (HeatInstance type)
        stack_instances = [
            i for i in instances if hasattr(i, 'heat_stack_id')]
        self.assertEqual(len(stack_instances), 4)

        # Check first instance
        self.assertEqual(stack_instances[0].heat_stack_id, 'stack-1')
        self.assertEqual(stack_instances[0].public_ipv4, '10.0.0.1')
        self.assertEqual(stack_instances[0].metadata['zuul_system_id'],
                         'test-system')
        self.assertEqual(stack_instances[0].metadata['zuul_node_uuid'],
                         'node-1')
        self.assertEqual(stack_instances[0].metadata['custom_tag'], 'value')

        # Check quota is stacks=1 for each
        for si in stack_instances:
            resources = si.getQuotaInformation().getResources()
            self.assertEqual(resources.get('stacks'), 1)

        # Check second instance
        self.assertEqual(stack_instances[1].heat_stack_id, 'stack-2')
        self.assertEqual(stack_instances[1].public_ipv4, '10.0.0.2')

        # DELETE_FAILED and DELETE_IN_PROGRESS are included
        self.assertEqual(stack_instances[2].heat_stack_id, 'stack-4')
        self.assertEqual(stack_instances[3].heat_stack_id, 'stack-5')

        mock_client.orchestration.stacks.assert_called_once_with(
            tags='zuul_system_id=test-system'
        )

    def test_list_instances_missing_bastion_ip(self):
        """Tests listInstances tolerates missing bastion_ip for stacks."""
        endpoint, mock_client = self._create_endpoint_for_listing()

        mock_stack = Mock()
        mock_stack.id = 'stack-1'
        mock_stack.status = 'CREATE_COMPLETE'

        mock_client.orchestration.stacks.return_value = [mock_stack]

        mock_details = {
            'id': 'stack-1',
            'stack_status': 'CREATE_COMPLETE',
            'outputs': [
                {'output_key': 'other_output', 'output_value': 'value1'}
            ],
            'tags': ['zuul_system_id=test-system']
        }

        mock_client.orchestration.get_stack.return_value = mock_details

        instances = list(endpoint.listInstances())
        stack_instances = [
            i for i in instances if hasattr(i, 'heat_stack_id')]
        self.assertEqual(len(stack_instances), 1)
        self.assertIsNone(stack_instances[0].public_ipv4)
        self.assertEqual(stack_instances[0].heat_stack_id, 'stack-1')

    def test_list_instances_nova_servers(self):
        """Tests listInstances yields Nova servers with flavor-based quota."""
        endpoint, mock_client = self._create_endpoint_for_listing()

        # Two servers with different flavors
        server1 = FakeResource(
            id='server-aaa',
            status='ACTIVE',
            flavor=Mock(vcpus=4, ram=8192),
            **{'os-extended-volumes:volumes_attached': [
                {'id': 'vol-111'}, {'id': 'vol-222'}
            ]})

        server2 = FakeResource(
            id='server-bbb',
            status='ACTIVE',
            flavor=Mock(vcpus=2, ram=4096),
            **{'os-extended-volumes:volumes_attached': []})

        endpoint._listServers = Mock(return_value=[server1, server2])

        instances = list(endpoint.listInstances())

        # Find server instances
        server_instances = [
            i for i in instances if getattr(i, 'resource_type', None)
            == 'server']
        self.assertEqual(len(server_instances), 2)

        # First server: 4 cores, 8192 RAM, 2 attached volumes
        q1 = server_instances[0].getQuotaInformation().getResources()
        self.assertEqual(q1.get('instances'), 1)
        self.assertEqual(q1.get('cores'), 4)
        self.assertEqual(q1.get('ram'), 8192)
        self.assertEqual(q1.get('volumes'), 2)

        # Second server: 2 cores, 4096 RAM, no volumes
        q2 = server_instances[1].getQuotaInformation().getResources()
        self.assertEqual(q2.get('instances'), 1)
        self.assertEqual(q2.get('cores'), 2)
        self.assertEqual(q2.get('ram'), 4096)
        self.assertNotIn('volumes', q2)

    def test_list_instances_nova_server_deleted_skipped(self):
        """Tests that deleted Nova servers are skipped."""
        endpoint, mock_client = self._create_endpoint_for_listing()

        server = FakeResource(
            id='server-del',
            status='deleted',
            flavor=Mock(vcpus=1, ram=512),
            **{'os-extended-volumes:volumes_attached': []})

        endpoint._listServers = Mock(return_value=[server])

        instances = list(endpoint.listInstances())
        server_instances = [
            i for i in instances if getattr(i, 'resource_type', None)
            == 'server']
        self.assertEqual(len(server_instances), 0)

    def test_list_instances_cinder_volumes_standalone(self):
        """Tests listInstances yields standalone volumes with quota."""
        endpoint, mock_client = self._create_endpoint_for_listing()

        # A server with one attached volume
        server = FakeResource(
            id='server-111',
            status='ACTIVE',
            flavor=Mock(vcpus=1, ram=1024),
            **{'os-extended-volumes:volumes_attached': [
                {'id': 'vol-attached'}]})
        endpoint._listServers = Mock(return_value=[server])

        # Two volumes: one attached (should be skipped), one standalone
        vol_attached = FakeResource(id='vol-attached', size=50)
        vol_standalone = FakeResource(id='vol-standalone', size=100)

        endpoint._listVolumes = Mock(
            return_value=[vol_attached, vol_standalone])

        instances = list(endpoint.listInstances())

        vol_instances = [
            i for i in instances if getattr(i, 'resource_type', None)
            == 'volume']
        # Only standalone volume is yielded
        self.assertEqual(len(vol_instances), 1)
        self.assertEqual(vol_instances[0].resource_id, 'vol-standalone')

        q = vol_instances[0].getQuotaInformation().getResources()
        self.assertEqual(q.get('volumes'), 1)
        self.assertEqual(q.get('volume-gb'), 100)

    def test_list_instances_neutron_networks(self):
        """Tests listInstances yields project-owned networks."""
        endpoint, mock_client = self._create_endpoint_for_listing()

        net1 = FakeResource(id='net-aaa')
        net2 = FakeResource(id='net-bbb')

        endpoint._listNetworks = Mock(return_value=[net1, net2])

        instances = list(endpoint.listInstances())

        net_instances = [
            i for i in instances if getattr(i, 'resource_type', None)
            == 'network']
        self.assertEqual(len(net_instances), 2)

        for ni in net_instances:
            q = ni.getQuotaInformation().getResources()
            self.assertEqual(q.get('networks'), 1)

    def test_list_instances_neutron_routers(self):
        """Tests listInstances yields routers with gateway detection."""
        endpoint, mock_client = self._create_endpoint_for_listing()

        # Router with external gateway
        router_ext = FakeResource(
            id='router-ext',
            external_gateway_info={'network_id': 'ext-net'})

        # Router without external gateway
        router_int = FakeResource(
            id='router-int',
            external_gateway_info=None)

        endpoint._listRouters = Mock(return_value=[router_ext, router_int])

        instances = list(endpoint.listInstances())

        router_instances = [
            i for i in instances if getattr(i, 'resource_type', None)
            == 'router']
        self.assertEqual(len(router_instances), 2)

        # Router with external gateway has both keys
        q_ext = router_instances[0].getQuotaInformation().getResources()
        self.assertEqual(q_ext.get('routers'), 1)
        self.assertEqual(q_ext.get('routers-with-external-gw'), 1)

        # Router without external gateway
        q_int = router_instances[1].getQuotaInformation().getResources()
        self.assertEqual(q_int.get('routers'), 1)
        self.assertNotIn('routers-with-external-gw', q_int)

    def test_list_instances_neutron_floating_ips(self):
        """Tests listInstances yields floating IPs."""
        endpoint, mock_client = self._create_endpoint_for_listing()

        fip1 = FakeResource(id='fip-aaa')
        fip2 = FakeResource(id='fip-bbb')

        endpoint._listFloatingIps = Mock(return_value=[fip1, fip2])

        instances = list(endpoint.listInstances())

        fip_instances = [
            i for i in instances if getattr(i, 'resource_type', None)
            == 'floatingip']
        self.assertEqual(len(fip_instances), 2)

        for fi in fip_instances:
            q = fi.getQuotaInformation().getResources()
            self.assertEqual(q.get('floating-ips'), 1)

    def test_list_instances_mixed_managed_unmanaged(self):
        """Tests mixed managed and unmanaged resources in same listing."""
        endpoint, mock_client = self._create_endpoint_for_listing(
            managed_resource_ids={'server-zuul': 'node-aaa'})

        # Managed server (inside a Heat stack)
        server_zuul = FakeResource(
            id='server-zuul',
            status='ACTIVE',
            flavor=Mock(vcpus=4, ram=8192),
            **{'os-extended-volumes:volumes_attached': []})

        # Unmanaged server (manually created or from another system)
        server_other = FakeResource(
            id='server-other',
            status='ACTIVE',
            flavor=Mock(vcpus=2, ram=4096),
            **{'os-extended-volumes:volumes_attached': []})

        endpoint._listServers = Mock(
            return_value=[server_zuul, server_other])

        instances = list(endpoint.listInstances())
        server_instances = [
            i for i in instances if getattr(i, 'resource_type', None)
            == 'server']

        self.assertEqual(len(server_instances), 2)

        # Managed server has metadata (cleanup worker will skip it)
        managed = [s for s in server_instances
                   if s.resource_id == 'server-zuul'][0]
        self.assertEqual(managed.metadata['zuul_system_id'], 'test-system')
        self.assertEqual(managed.metadata['zuul_node_uuid'], 'node-aaa')

        # Unmanaged server has empty metadata (counted as unmanaged usage)
        unmanaged = [s for s in server_instances
                     if s.resource_id == 'server-other'][0]
        self.assertEqual(unmanaged.metadata, {})

    def _create_endpoint_with_flavors(self, *flavors):
        """Create an endpoint with a mock client and pre-loaded flavor cache.

        Each positional arg is a dict of flavor attributes
        (id, name, vcpus, ram).  Returns (endpoint, mock_client).
        """
        endpoint = self._create_endpoint()
        mock_client = Mock()
        endpoint._client = mock_client
        mock_client.compute.flavors.return_value = [
            FakeResource(**f) for f in flavors
        ]
        return endpoint, mock_client

    def test_flavor_cache_single_api_call_for_multiple_servers(self):
        """Tests that multiple flavor ID lookups only hit the API once."""
        endpoint, mock_client = self._create_endpoint_with_flavors(
            dict(id='flv-a', name='m1.small', vcpus=1, ram=2048),
            dict(id='flv-b', name='m1.large', vcpus=4, ram=8192),
        )

        servers = [
            FakeResource(id='s1', status='ACTIVE', flavor={'id': 'flv-a'}),
            FakeResource(id='s2', status='ACTIVE', flavor={'id': 'flv-b'}),
            FakeResource(id='s3', status='ACTIVE', flavor={'id': 'flv-a'}),
        ]

        results = [endpoint._getFlavorFromServer(s) for s in servers]

        self.assertEqual(results[0]['vcpus'], 1)
        self.assertEqual(results[1]['vcpus'], 4)
        self.assertEqual(results[2]['vcpus'], 1)
        mock_client.compute.flavors.assert_called_once_with(details=True)

    def test_flavor_inline_skips_cache(self):
        """Tests _getFlavorFromServer uses inline data without API call."""
        endpoint, mock_client = self._create_endpoint_with_flavors()

        inline_flavor = Mock(vcpus=4, ram=8192)
        server = FakeResource(
            id='server-1', status='ACTIVE', flavor=inline_flavor)

        result = endpoint._getFlavorFromServer(server)
        self.assertIs(result, inline_flavor)
        mock_client.compute.flavors.assert_not_called()

    def test_flavor_id_not_in_cache_raises(self):
        """Tests _findFlavorById raises when flavor ID is unknown."""
        endpoint, _ = self._create_endpoint_with_flavors(
            dict(id='flv-aaa', name='m1.small', vcpus=1, ram=2048),
        )

        with self.assertRaises(Exception) as cm:
            endpoint._findFlavorById('nonexistent-id')
        self.assertIn('nonexistent-id', str(cm.exception))

    def test_list_resources(self):
        """Tests listResources method."""
        endpoint = self._create_endpoint()

        mock_client = Mock()
        endpoint._client = mock_client

        # Create mock stacks
        mock_stack1 = Mock()
        mock_stack1.id = 'stack-1'
        mock_stack1.status = 'CREATE_COMPLETE'

        mock_stack2 = Mock()
        mock_stack2.id = 'stack-2'
        mock_stack2.status = 'CREATE_FAILED'

        # Deleted stack (should be skipped)
        mock_stack3 = Mock()
        mock_stack3.id = 'stack-3'
        mock_stack3.status = 'DELETE_COMPLETE'

        # Deleting stack (should be skipped)
        mock_stack4 = Mock()
        mock_stack4.id = 'stack-4'
        mock_stack4.status = 'DELETE_IN_PROGRESS'

        mock_client.orchestration.stacks.return_value = [
            mock_stack1, mock_stack2, mock_stack3, mock_stack4
        ]

        # Mock stack details
        mock_details1 = {
            'id': 'stack-1',
            'tags': [
                'zuul_system_id=test-system',
                'zuul_node_uuid=node-1',
                'custom_tag=leaked'
            ]
        }

        mock_details2 = {
            'id': 'stack-2',
            'tags': ['zuul_system_id=test-system']
        }

        def get_stack_side_effect(stack_id):
            if stack_id == 'stack-1':
                return mock_details1
            elif stack_id == 'stack-2':
                return mock_details2
            raise Exception(f"Unexpected stack_id: {stack_id}")

        mock_client.orchestration.get_stack.side_effect = (
            get_stack_side_effect)

        # Call listResources
        resources = list(endpoint.listResources([]))

        # Verify results - only 2 resources (skipped DELETE_COMPLETE and
        # DELETE_IN_PROGRESS)
        self.assertEqual(len(resources), 2)

        # Check first resource
        self.assertEqual(resources[0].type, heatmodel.HeatResource.TYPE_STACK)
        self.assertEqual(resources[0].id, 'stack-1')
        self.assertEqual(resources[0].metadata['zuul_system_id'],
                         'test-system')
        self.assertEqual(resources[0].metadata['custom_tag'], 'leaked')

        # Check second resource
        self.assertEqual(resources[1].type, heatmodel.HeatResource.TYPE_STACK)
        self.assertEqual(resources[1].id, 'stack-2')

        # Verify API was called with correct tag filter
        mock_client.orchestration.stacks.assert_called_once_with(
            tags='zuul_system_id=test-system'
        )

    def test_delete_resource(self):
        """Tests deleteResource method."""
        endpoint = self._create_endpoint()

        mock_client = Mock()
        endpoint._client = mock_client

        # Create a mock resource
        resource = heatmodel.HeatResource(
            metadata={'zuul_system_id': 'test-system'},
            type=heatmodel.HeatResource.TYPE_STACK,
            id='stack-to-delete'
        )

        # Call deleteResource
        endpoint.deleteResource(resource)

        # Verify _deleteStack was called
        mock_client.orchestration.delete_stack.assert_called_once_with(
            'stack-to-delete'
        )

    def test_list_stack_resources(self):
        """Test _listStackResources returns tracked physical resource IDs."""
        endpoint = self._create_endpoint()

        mock_client = Mock()
        endpoint._client = mock_client

        mock_server = Mock()
        mock_server.resource_type = 'OS::Nova::Server'
        mock_server.physical_resource_id = 'server-uuid-1'

        mock_volume = Mock()
        mock_volume.resource_type = 'OS::Cinder::Volume'
        mock_volume.physical_resource_id = 'volume-uuid-2'

        mock_net = Mock()
        mock_net.resource_type = 'OS::Neutron::Net'
        mock_net.physical_resource_id = 'net-uuid-3'

        mock_fip = Mock()
        mock_fip.resource_type = 'OS::Neutron::FloatingIP'
        mock_fip.physical_resource_id = 'fip-uuid-4'

        # Untracked types should be filtered out
        mock_subnet = Mock()
        mock_subnet.resource_type = 'OS::Neutron::Subnet'
        mock_subnet.physical_resource_id = 'subnet-uuid-5'

        mock_port = Mock()
        mock_port.resource_type = 'OS::Neutron::Port'
        mock_port.physical_resource_id = 'port-uuid-6'

        mock_value = Mock()
        mock_value.resource_type = 'OS::Heat::Value'
        mock_value.physical_resource_id = 'value-uuid-7'

        mock_client.orchestration.resources.return_value = [
            mock_server, mock_volume, mock_net, mock_fip,
            mock_subnet, mock_port, mock_value,
        ]

        result = endpoint._listStackResources('stack-123')

        self.assertEqual(result, [
            'server-uuid-1', 'volume-uuid-2',
            'net-uuid-3', 'fip-uuid-4',
        ])
        mock_client.orchestration.resources.assert_called_once_with(
            'stack-123', nested_depth=1)

    def test_create_stack_joins_tags(self):
        """Tests _createStack joins tags list into comma-separated string."""
        endpoint = self._create_endpoint()

        mock_client = Mock()
        endpoint._client = mock_client

        tags = [
            'zuul_system_id=test-system',
            'zuul_node_uuid=node-1',
            'custom=value',
        ]

        endpoint._createStack(
            'test-stack', 'template: {}', {'key': 'val'}, tags)

        mock_client.orchestration.create_stack.assert_called_once_with(
            name='test-stack',
            template='template: {}',
            parameters={'key': 'val'},
            tags='zuul_system_id=test-system,'
                 'zuul_node_uuid=node-1,'
                 'custom=value',
        )

    def test_create_stack_no_tags(self):
        """Tests _createStack handles empty tags."""
        endpoint = self._create_endpoint()

        mock_client = Mock()
        endpoint._client = mock_client

        endpoint._createStack(
            'test-stack', 'template: {}', {}, None)

        mock_client.orchestration.create_stack.assert_called_once_with(
            name='test-stack',
            template='template: {}',
            parameters={},
            tags=None,
        )

    def test_get_external_network(self):
        """Test _getExternalNetwork returns first external network."""
        endpoint = self._create_endpoint()

        mock_client = Mock()
        mock_client.get_external_ipv4_floating_networks.return_value = [
            {'name': 'external-net', 'id': 'ext-net-id-1'},
            {'name': 'other-ext-net', 'id': 'ext-net-id-2'},
        ]
        endpoint._client = mock_client

        result = endpoint._getExternalNetwork()
        self.assertEqual(result, 'external-net')

    def test_get_external_network_none_found(self):
        """Test _getExternalNetwork raises when no external network."""
        endpoint = self._create_endpoint()

        mock_client = Mock()
        mock_client.get_external_ipv4_floating_networks.return_value = []
        endpoint._client = mock_client

        self.assertRaises(Exception, endpoint._getExternalNetwork)


class TestHeatStateMachines(BaseTestCase):
    """Test Heat state machine functionality."""

    def setUp(self):
        super().setUp()
        self.HeatCreateStateMachine = HeatCreateStateMachine
        self.HeatDeleteStateMachine = HeatDeleteStateMachine

    def _make_create_sm(self, stack_name='test-stack',
                        template='template: {}',
                        parameters=None, tags=None,
                        timeout=600, **node_attrs):
        """Create a HeatCreateStateMachine with mock endpoint and node.

        Returns (endpoint, node, log, sm). The node gets sensible
        defaults for node_properties and template_config (empty dicts)
        which can be overridden via **node_attrs.
        """
        endpoint = Mock()
        node = Mock()
        node.create_state = {}
        node.node_properties = {}
        node.template_config = {}
        for k, v in node_attrs.items():
            setattr(node, k, v)
        log = Mock()
        sm = self.HeatCreateStateMachine(
            endpoint, node, stack_name, template,
            parameters or {}, tags or {}, timeout, log)
        return endpoint, node, log, sm

    def _make_delete_sm(self, stack_id='stack-123'):
        """Create a HeatDeleteStateMachine with mock endpoint and node.

        Returns (endpoint, node, log, sm).
        """
        endpoint = Mock()
        node = Mock()
        node.delete_state = {}
        node.heat_stack_id = stack_id
        log = Mock()
        sm = self.HeatDeleteStateMachine(endpoint, node, log)
        return endpoint, node, log, sm

    def _setup_complete_stack(self, endpoint, outputs=None,
                              resource_ids=None,
                              stack_id='stack-123'):
        """Configure endpoint mocks for a CREATE_COMPLETE stack.

        Sets up _getStack, _getInstance, and _listStackResources on
        the endpoint mock. Returns (stack, instance).
        """
        if outputs is None:
            outputs = [
                {'output_key': 'bastion_ip',
                 'output_value': '192.0.2.1'}
            ]
        stack = Mock()
        stack.id = stack_id
        stack.status = 'CREATE_COMPLETE'
        stack.outputs = outputs
        endpoint._getStack = Mock(return_value=stack)
        instance = Mock()
        endpoint._getInstance = Mock(return_value=instance)
        endpoint._listStackResources = Mock(
            return_value=resource_ids or [])
        return stack, instance

    def test_create_state_machine_init(self):
        """Test create state machine initialization."""
        endpoint, node, log, sm = self._make_create_sm(
            tags={'tag1': 'value1'})

        self.assertEqual(sm.state, sm.START)
        self.assertEqual(sm.stack_name, 'test-stack')
        self.assertFalse(sm.complete)

    def test_delete_state_machine_init(self):
        """Test delete state machine initialization."""
        endpoint, node, log, sm = self._make_delete_sm()

        self.assertEqual(sm.state, sm.START)
        self.assertEqual(sm.stack_id, 'stack-123')
        self.assertFalse(sm.complete)

    def test_delete_state_machine_no_stack_id(self):
        """Test delete state machine with no stack ID."""
        endpoint, node, log, sm = self._make_delete_sm(stack_id=None)
        node.uuid = 'node-456'

        # Should skip straight to COMPLETE if no stack ID
        self.assertEqual(sm.state, sm.COMPLETE)

    def test_create_state_machine_to_dict(self):
        """Test create state machine serialization."""
        endpoint, node, log, sm = self._make_create_sm()
        sm.stack_id = 'stack-789'

        state_dict = sm.toDict()
        self.assertIn('state', state_dict)
        self.assertIn('start_time', state_dict)
        self.assertIn('heat_stack_id', state_dict)
        self.assertEqual(state_dict['heat_stack_id'], 'stack-789')

    def test_create_advance_start_to_submit(self):
        """Test create state machine advances from START to SUBMIT."""
        endpoint, node, log, sm = self._make_create_sm()
        endpoint._submitApi = Mock(return_value='future-123')
        endpoint._completeApi = Mock(return_value=None)

        self.assertEqual(sm.state, sm.START)
        sm.advance()
        self.assertEqual(sm.state, sm.STACK_CREATING_SUBMIT)

    def test_create_advance_submit_api_call(self):
        """Test create state machine submits API call."""
        endpoint, node, log, sm = self._make_create_sm(
            parameters={'param1': 'value1'}, tags=['tag1=val1'])
        endpoint._submitApi = Mock(return_value='future-123')
        endpoint._completeApi = Mock(return_value=None)
        sm.state = sm.STACK_CREATING_SUBMIT

        sm.advance()

        endpoint._submitApi.assert_called_once_with(
            endpoint._createStack,
            'test-stack',
            'template: {}',
            {'param1': 'value1'},
            ['tag1=val1']
        )
        endpoint._completeApi.assert_called_once_with('future-123')
        # Still in SUBMIT state because API not complete
        self.assertEqual(sm.state, sm.STACK_CREATING_SUBMIT)

    def test_create_advance_submit_api_complete(self):
        """Test create state machine handles completed API call."""
        endpoint, node, log, sm = self._make_create_sm()
        stack = Mock()
        stack.id = 'stack-123'
        stack.name = 'test-stack'
        stack.status = 'CREATE_IN_PROGRESS'
        endpoint._submitApi = Mock(return_value='future-123')
        endpoint._completeApi = Mock(return_value=stack)
        endpoint._getStack = Mock(return_value=stack)
        sm.state = sm.STACK_CREATING_SUBMIT

        sm.advance()

        self.assertEqual(sm.stack_id, 'stack-123')
        self.assertEqual(node.heat_stack_id, 'stack-123')
        self.assertEqual(node.heat_stack_name, 'test-stack')
        self.assertEqual(sm.state, sm.STACK_CREATING)

    def test_create_advance_submit_quota_exception(self):
        """Test create state machine handles quota exceptions."""
        endpoint, node, log, sm = self._make_create_sm()
        endpoint._submitApi = Mock(return_value='future-123')
        endpoint._completeApi = Mock(
            side_effect=openstack.exceptions.SDKException(
                'Quota exceeded for instances'))
        sm.state = sm.STACK_CREATING_SUBMIT

        from zuul import exceptions
        with self.assertRaises(exceptions.QuotaException):
            sm.advance()

    def test_create_advance_submit_general_exception(self):
        """Test create state machine handles general exceptions."""
        endpoint, node, log, sm = self._make_create_sm()
        endpoint._submitApi = Mock(return_value='future-123')
        endpoint._completeApi = Mock(
            side_effect=Exception('Network error'))
        sm.state = sm.STACK_CREATING_SUBMIT

        with self.assertRaises(Exception) as cm:
            sm.advance()
        self.assertIn('Network error', str(cm.exception))

    def test_create_advance_creating_in_progress(self):
        """Test create state machine waits for CREATE_IN_PROGRESS."""
        endpoint, node, log, sm = self._make_create_sm()
        stack = Mock()
        stack.id = 'stack-123'
        stack.status = 'CREATE_IN_PROGRESS'
        endpoint._getStack = Mock(return_value=stack)
        sm.state = sm.STACK_CREATING
        sm.stack_id = 'stack-123'

        sm.advance()

        endpoint._getStack.assert_called_once_with('stack-123')
        # Still in CREATING state
        self.assertEqual(sm.state, sm.STACK_CREATING)
        self.assertFalse(sm.complete)

    def test_create_advance_creating_complete(self):
        """Test create state machine handles CREATE_COMPLETE."""
        endpoint, node, log, sm = self._make_create_sm(
            quota=QuotaInformation(stacks=1))
        stack, instance = self._setup_complete_stack(
            endpoint,
            resource_ids=['server-uuid-1', 'vol-uuid-2'])
        sm.state = sm.STACK_CREATING
        sm.stack_id = 'stack-123'

        sm.advance()

        self.assertEqual(sm.state, sm.COMPLETE)
        self.assertTrue(sm.complete)
        node.updateFromInstance.assert_called_once_with(instance)
        endpoint._getInstance.assert_called_once_with(
            stack, node.quota,
            {'bastion_ip': '192.0.2.1'})
        endpoint._listStackResources.assert_called_once_with('stack-123')
        self.assertEqual(node.heat_physical_resource_ids,
                         ['server-uuid-1', 'vol-uuid-2'])

    def test_create_advance_creating_failed(self):
        """Test create state machine handles CREATE_FAILED."""
        endpoint, node, log, sm = self._make_create_sm()
        stack = Mock()
        stack.id = 'stack-123'
        stack.status = 'CREATE_FAILED'
        stack.stack_status_reason = 'Insufficient resources'
        endpoint._getStack = Mock(return_value=stack)
        sm.state = sm.STACK_CREATING
        sm.stack_id = 'stack-123'

        with self.assertRaises(Exception) as cm:
            sm.advance()
        self.assertIn('Insufficient resources', str(cm.exception))

    def test_create_advance_creating_unexpected_status(self):
        """Test create state machine handles unexpected status."""
        endpoint, node, log, sm = self._make_create_sm()
        stack = Mock()
        stack.id = 'stack-123'
        stack.status = 'UNKNOWN_STATUS'
        endpoint._getStack = Mock(return_value=stack)
        sm.state = sm.STACK_CREATING
        sm.stack_id = 'stack-123'

        with self.assertRaises(Exception) as cm:
            sm.advance()
        self.assertIn('Unexpected stack status', str(cm.exception))
        self.assertIn('UNKNOWN_STATUS', str(cm.exception))

    def test_create_advance_creating_stack_disappeared(self):
        """Test create state machine handles disappeared stack."""
        endpoint, node, log, sm = self._make_create_sm()
        endpoint._getStack = Mock(return_value=None)
        sm.state = sm.STACK_CREATING
        sm.stack_id = 'stack-123'

        with self.assertRaises(Exception) as cm:
            sm.advance()
        self.assertIn('disappeared', str(cm.exception))

    def test_create_advance_creating_timeout(self):
        """Test create state machine timeout during STACK_CREATING."""
        endpoint, node, log, sm = self._make_create_sm(timeout=10)
        stack = Mock()
        stack.id = 'stack-123'
        stack.status = 'CREATE_IN_PROGRESS'
        endpoint._getStack = Mock(return_value=stack)
        sm.state = sm.STACK_CREATING
        sm.stack_id = 'stack-123'

        # Set start_time to 15 seconds ago to trigger timeout
        import time
        sm.start_time = time.time() - 15

        with self.assertRaises(Exception) as cm:
            sm.advance()
        self.assertIn('timed out', str(cm.exception).lower())
        # timeout value should be in message
        self.assertIn('10', str(cm.exception))

    def test_create_advance_complete_extracts_outputs(self):
        """Test create state machine extracts outputs on COMPLETE."""
        endpoint, node, log, sm = self._make_create_sm(
            quota={'cores': 2})
        stack, instance = self._setup_complete_stack(
            endpoint, outputs=[
                {'output_key': 'bastion_ip', 'output_value': '192.168.1.1'},
                {'output_key': 'server_id', 'output_value': 'srv-456'},
            ])
        sm.state = sm.COMPLETE
        sm.stack_id = 'stack-123'

        sm.advance()

        self.assertEqual(node.heat_stack_outputs, {
            'bastion_ip': '192.168.1.1',
            'server_id': 'srv-456'
        })
        self.assertEqual(node.node_properties['heat_stack_outputs'], {
            'bastion_ip': '192.168.1.1',
            'server_id': 'srv-456'
        })
        self.assertEqual(node.heat_stack_status, 'CREATE_COMPLETE')
        self.assertTrue(sm.complete)
        node.updateFromInstance.assert_called_once_with(instance)
        endpoint._getInstance.assert_called_once_with(
            stack, {'cores': 2}, {
                'bastion_ip': '192.168.1.1',
                'server_id': 'srv-456'
            })

    def test_create_advance_complete_strips_template_content(self):
        """Test that heat_template_content is removed from node_properties
        after successful stack creation."""
        endpoint, node, log, sm = self._make_create_sm(
            quota=QuotaInformation(stacks=1),
            template_config={
                'heat_template_content': _build_template(),
                'heat_template_path': 'ci/heat/stack.yaml',
            })
        stack, instance = self._setup_complete_stack(
            endpoint, outputs=[
                {'output_key': 'bastion_ip', 'output_value': '192.168.1.1'},
            ])
        sm.state = sm.COMPLETE
        sm.stack_id = 'stack-123'

        sm.advance()

        self.assertTrue(sm.complete)
        self.assertNotIn('heat_template_content', node.template_config)
        self.assertIn('heat_template_path', node.template_config)
        self.assertEqual(node.node_properties['heat_stack_outputs'],
                         {'bastion_ip': '192.168.1.1'})

    def test_update_node_via_nodes_populated_from_output(self):
        """Test that zuul_via_nodes output is copied to node_properties."""
        via_nodes_value = {
            'worker': {'host': '192.168.1.10', 'user': 'zuul', 'port': 22},
        }
        endpoint, node, log, sm = self._make_create_sm()
        self._setup_complete_stack(endpoint, outputs=[
            {'output_key': 'bastion_ip', 'output_value': '10.0.0.5'},
            {'output_key': 'zuul_via_nodes', 'output_value': via_nodes_value},
        ])
        sm.state = sm.COMPLETE
        sm.stack_id = 'stack-123'

        sm.advance()

        self.assertTrue(sm.complete)
        self.assertIn('via_nodes', node.node_properties)
        self.assertEqual(node.node_properties['via_nodes'], via_nodes_value)

    def test_update_node_via_nodes_absent_when_no_output(self):
        """Test that via_nodes is not set when zuul_via_nodes absent."""
        endpoint, node, log, sm = self._make_create_sm()
        self._setup_complete_stack(endpoint, outputs=[
            {'output_key': 'bastion_ip', 'output_value': '10.0.0.5'},
        ])
        sm.state = sm.COMPLETE
        sm.stack_id = 'stack-123'

        sm.advance()

        self.assertTrue(sm.complete)
        self.assertNotIn('via_nodes', node.node_properties)

    def test_delete_advance_start_to_submit(self):
        """Test delete state machine advances from START to SUBMIT."""
        endpoint, node, log, sm = self._make_delete_sm()

        self.assertEqual(sm.state, sm.START)
        sm.advance()
        self.assertEqual(sm.state, sm.STACK_DELETING_SUBMIT)

    def test_delete_advance_submit_api_call(self):
        """Test delete state machine submits API call."""
        endpoint, node, log, sm = self._make_delete_sm()
        endpoint._submitApi = Mock(return_value='future-123')
        endpoint._completeApi = Mock(return_value=None)
        sm.state = sm.STACK_DELETING_SUBMIT

        sm.advance()

        endpoint._submitApi.assert_called_once_with(
            endpoint._deleteStack,
            'stack-123'
        )
        endpoint._completeApi.assert_called_once_with('future-123')
        # Still in SUBMIT state because API not complete
        self.assertEqual(sm.state, sm.STACK_DELETING_SUBMIT)

    def test_delete_advance_submit_api_complete(self):
        """Test delete state machine handles completed API call."""
        endpoint, node, log, sm = self._make_delete_sm()
        endpoint._submitApi = Mock(return_value='future-123')
        endpoint._completeApi = Mock(return_value=True)
        stack = Mock()
        stack.id = 'stack-123'
        stack.status = 'DELETE_IN_PROGRESS'
        endpoint._getStack = Mock(return_value=stack)
        sm.state = sm.STACK_DELETING_SUBMIT

        sm.advance()

        self.assertEqual(sm.state, sm.STACK_DELETING)

    def test_delete_advance_submit_already_deleted(self):
        """Test delete state machine handles already deleted stack."""
        endpoint, node, log, sm = self._make_delete_sm()
        endpoint._submitApi = Mock(return_value='future-123')
        endpoint._completeApi = Mock(
            side_effect=openstack.exceptions.NotFoundException())
        sm.state = sm.STACK_DELETING_SUBMIT

        sm.advance()

        self.assertEqual(sm.state, sm.COMPLETE)

    def test_delete_advance_submit_exception(self):
        """Test delete state machine handles deletion exceptions."""
        endpoint, node, log, sm = self._make_delete_sm()
        endpoint._submitApi = Mock(return_value='future-123')
        endpoint._completeApi = Mock(
            side_effect=Exception('Network error'))
        sm.state = sm.STACK_DELETING_SUBMIT

        with self.assertRaises(Exception) as cm:
            sm.advance()
        self.assertIn('Network error', str(cm.exception))

    def test_delete_advance_deleting_in_progress(self):
        """Test delete state machine waits for DELETE_IN_PROGRESS."""
        endpoint, node, log, sm = self._make_delete_sm()
        stack = Mock()
        stack.id = 'stack-123'
        stack.status = 'DELETE_IN_PROGRESS'
        endpoint._getStack = Mock(return_value=stack)
        sm.state = sm.STACK_DELETING
        sm.stack_id = 'stack-123'

        sm.advance()

        endpoint._getStack.assert_called_once_with('stack-123')
        # Still in DELETING state
        self.assertEqual(sm.state, sm.STACK_DELETING)
        self.assertFalse(sm.complete)

    def test_delete_advance_deleting_stack_disappeared(self):
        """Test delete state machine handles disappeared stack."""
        endpoint, node, log, sm = self._make_delete_sm()
        endpoint._getStack = Mock(return_value=None)
        sm.state = sm.STACK_DELETING
        sm.stack_id = 'stack-123'

        sm.advance()

        self.assertEqual(sm.state, sm.COMPLETE)

    def test_delete_advance_deleting_complete(self):
        """Test delete state machine handles DELETE_COMPLETE."""
        endpoint, node, log, sm = self._make_delete_sm()
        stack = Mock()
        stack.id = 'stack-123'
        stack.status = 'DELETE_COMPLETE'
        endpoint._getStack = Mock(return_value=stack)
        sm.state = sm.STACK_DELETING
        sm.stack_id = 'stack-123'

        sm.advance()

        self.assertEqual(sm.state, sm.COMPLETE)

    def test_delete_advance_deleting_failed(self):
        """Test delete state machine handles DELETE_FAILED."""
        endpoint, node, log, sm = self._make_delete_sm()
        stack = Mock()
        stack.id = 'stack-123'
        stack.status = 'DELETE_FAILED'
        stack.stack_status_reason = 'Resource is locked'
        endpoint._getStack = Mock(return_value=stack)
        sm.state = sm.STACK_DELETING
        sm.stack_id = 'stack-123'

        with self.assertRaises(Exception) as cm:
            sm.advance()
        self.assertIn('Resource is locked', str(cm.exception))

    def test_delete_advance_deleting_unexpected_status_retry(self):
        """Test delete state machine retries on unexpected status."""
        endpoint, node, log, sm = self._make_delete_sm()
        stack = Mock()
        stack.id = 'stack-123'
        stack.status = 'UPDATE_COMPLETE'
        endpoint._getStack = Mock(return_value=stack)
        sm.state = sm.STACK_DELETING
        sm.stack_id = 'stack-123'

        sm.advance()

        # Should reset to SUBMIT state for retry
        self.assertEqual(sm.state, sm.STACK_DELETING_SUBMIT)
        self.assertIsNone(sm.delete_future)

    def test_delete_advance_complete(self):
        """Test delete state machine marks complete."""
        endpoint, node, log, sm = self._make_delete_sm()
        sm.state = sm.COMPLETE

        sm.advance()

        self.assertTrue(sm.complete)


class TestHeatProviderIntegration(BaseTestCase):
    """Test Heat provider integration and wiring."""

    # A realistic template that declares all zuul_* parameters.
    # The Heat driver only passes parameters that are defined in the
    # template, so tests that assert on specific zuul_* parameters must
    # use a template that declares them.
    FULL_ZUUL_PARAMS_TEMPLATE = _build_template(parameters={
        'zuul_key_name': {'type': 'string', 'default': ''},
        'zuul_username': {'type': 'string', 'default': ''},
        'zuul_image': {'type': 'string', 'default': ''},
        'zuul_external_network': {'type': 'string', 'default': ''},
        'zuul_ephemeral_public_key': {'type': 'string', 'default': ''},
        'zuul_ephemeral_private_key': {'type': 'string', 'default': ''},
    })

    def setUp(self):
        super().setUp()
        self.driver = HeatDriver()
        self.HeatCreateStateMachine = HeatCreateStateMachine

    def _make_provider(self, key_name=None, external_network=None,
                       username=None, image_id=None, endpoint=None,
                       config=None, flavor=None):
        """Create a configured HeatProvider with label and image mocks."""
        label = Mock()
        label.key_name = key_name
        label.external_network = external_network
        label.image = 'test-image'

        image = Mock()
        image.username = username
        image.image_id = image_id

        if endpoint is None:
            endpoint = Mock()

        provider_kwargs = dict(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=endpoint,
            labels={'test-label': label},
            images={'test-image': image},
        )

        if config is not None:
            provider_kwargs['config'] = config
        if flavor is not None:
            label.flavor = 'test-flavor'
            provider_kwargs['flavors'] = {'test-flavor': flavor}

        provider = heatprovider.HeatProvider()
        provider._set(**provider_kwargs)
        return provider

    def _make_node(self, template_content=None, heat_parameters=None,
                   template_path=None, tags=None):
        """Create a configured node Mock for state machine tests."""
        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = tags or {}
        template_config = {}
        if template_content is not None:
            template_config['heat_template_content'] = template_content
        if template_path is not None:
            template_config['heat_template_path'] = template_path
        if heat_parameters is not None:
            template_config['heat_parameters'] = heat_parameters
        node.template_config = template_config
        node.node_properties = {}
        node.create_state = {}
        return node

    def _get_create_sm(self, provider_kw=None, node_kw=None,
                       image_external_id=None):
        """Create a state machine using the standard template.

        Convenience wrapper around _make_provider + _make_node +
        getCreateStateMachine.  Returns the state machine.

        :param provider_kw: kwargs passed to _make_provider
        :param node_kw: kwargs passed to _make_node (template_content
            defaults to FULL_ZUUL_PARAMS_TEMPLATE, template_path to
            'ci/heat/stack.yaml')
        :param image_external_id: passed as image_external_id arg
        :returns: HeatCreateStateMachine instance
        """
        provider_kw = provider_kw or {}
        node_kw = node_kw or {}
        node_kw.setdefault('template_content',
                           self.FULL_ZUUL_PARAMS_TEMPLATE)
        node_kw.setdefault('template_path', 'ci/heat/stack.yaml')

        provider = self._make_provider(**provider_kw)
        node = self._make_node(**node_kw)
        return provider.getCreateStateMachine(
            node, image_external_id, Mock())

    def test_get_quota_for_label(self):
        """Test getQuotaForLabel returns basic stack quota."""
        provider = heatprovider.HeatProvider()
        quota = provider.getQuotaForLabel('test-label')

        # Should return quota with 1 stack
        # QuotaInformation stores values in quota dict
        self.assertEqual(quota.quota.get('stacks'), 1)

    def test_get_endpoint_limits_with_cached_limits(self):
        """Test getEndpointLimits returns limits from quota cache."""
        mock_endpoint = Mock()
        mock_limits = Mock()
        mock_limits.quota = {
            'instances': 10,
            'cores': 40,
            'ram': 81920,
            'volumes': 20,
            'floating-ips': 5,
        }
        mock_endpoint.quota_cache.getLimits.return_value = mock_limits

        provider = self._make_provider(endpoint=mock_endpoint)
        quota = provider.getEndpointLimits()

        self.assertEqual(quota.quota.get('instances'), 10)
        self.assertEqual(quota.quota.get('cores'), 40)
        self.assertEqual(quota.quota.get('ram'), 81920)
        self.assertEqual(quota.quota.get('volumes'), 20)
        self.assertEqual(quota.quota.get('floating-ips'), 5)
        mock_endpoint.quota_cache.getLimits.assert_called_once()

    def test_get_endpoint_limits_without_cached_limits(self):
        """Test getEndpointLimits returns infinity when no cache."""
        mock_endpoint = Mock()
        mock_endpoint.quota_cache.getLimits.return_value = None

        provider = self._make_provider(endpoint=mock_endpoint)
        quota = provider.getEndpointLimits()

        self.assertEqual(quota.quota, {})
        self.assertEqual(quota.default, float('inf'))

    def test_get_quota_for_label_with_node_properties(self):
        """Test getQuotaForLabel with template in node_properties."""
        provider = heatprovider.HeatProvider()
        mock_endpoint = Mock()
        mock_flavor = Mock()
        mock_flavor.vcpus = 2
        mock_flavor.ram = 4096
        mock_endpoint._findFlavor = Mock(return_value=mock_flavor)
        provider._set(
            _endpoint=mock_endpoint,
            config={'resource_policy': {}},
        )

        template = _build_template(
            servers=['m1.small', 'm1.small'],
            networks=1, floating_ips=1)
        template_config = {'heat_template_content': template}
        quota = provider.getQuotaForLabel('test-label',
                                          template_config=template_config)

        self.assertEqual(quota.quota.get('stacks'), 1)
        self.assertEqual(quota.quota.get('instances'), 2)
        self.assertEqual(quota.quota.get('cores'), 4)
        self.assertEqual(quota.quota.get('ram'), 8192)
        self.assertEqual(quota.quota.get('networks'), 1)
        self.assertEqual(quota.quota.get('floating-ips'), 1)

    def test_get_quota_for_label_no_node_properties(self):
        """Test getQuotaForLabel with empty template_config falls back."""
        provider = heatprovider.HeatProvider()
        quota = provider.getQuotaForLabel('test-label',
                                          template_config={})
        self.assertEqual(quota.quota.get('stacks'), 1)
        self.assertIsNone(quota.quota.get('instances'))

    def test_get_quota_for_label_with_runtime_parameters(self):
        """Test getQuotaForLabel with runtime heat_parameters."""
        provider = heatprovider.HeatProvider()
        mock_endpoint = Mock()

        def find_flavor(name):
            if name == 'm1.small':
                flavor = Mock()
                flavor.vcpus = 1
                flavor.ram = 2048
                return flavor
            elif name == 'm1.xlarge':
                flavor = Mock()
                flavor.vcpus = 8
                flavor.ram = 16384
                return flavor

        mock_endpoint._findFlavor = Mock(side_effect=find_flavor)
        provider._set(
            _endpoint=mock_endpoint,
            config={'resource_policy': {}},
        )

        template = _build_template(
            parameters={'instance_flavor': {'type': 'string',
                                            'default': 'm1.small'}},
            servers=[{'get_param': 'instance_flavor'}])
        template_config = {
            'heat_template_content': template,
            'heat_parameters': {'instance_flavor': 'm1.xlarge'},
        }
        quota = provider.getQuotaForLabel('test-label',
                                          template_config=template_config)

        self.assertEqual(quota.quota.get('stacks'), 1)
        self.assertEqual(quota.quota.get('instances'), 1)
        self.assertEqual(quota.quota.get('cores'), 8)
        self.assertEqual(quota.quota.get('ram'), 16384)
        mock_endpoint._findFlavor.assert_called_once_with('m1.xlarge')

    def test_refresh_quota_for_label(self):
        """Test refreshQuotaForLabel."""
        provider = heatprovider.HeatProvider()
        quota = provider.refreshQuotaForLabel('test-label', update=True)

        # Should return same basic quota as getQuotaForLabel
        self.assertEqual(quota.quota.get('stacks'), 1)

    def test_tags_converted_to_list_format(self):
        """Tests dict-to-list conversion of tags for Heat API."""
        provider = self._make_provider()
        node = self._make_node(
            template_content='template: {}',
            tags={'custom_tag1': 'value1', 'custom_tag2': 'value2'},
        )

        sm = provider.getCreateStateMachine(
            node, None, Mock())

        self.assertIsInstance(sm.tags, list)
        self.assertEqual(len(sm.tags), 4)

        tags_set = set(sm.tags)
        self.assertIn('custom_tag1=value1', tags_set)
        self.assertIn('custom_tag2=value2', tags_set)
        self.assertIn(
            'zuul_node_uuid=test-node-uuid-123',
            tags_set)
        self.assertIn(
            'zuul_system_id=test-system', tags_set)

    def test_create_state_machine_no_template(self):
        """Test getCreateStateMachine raises without template."""
        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=Mock(),
        )

        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.tags = {}
        node.template_config = {
            'heat_template_path': 'ci/heat/stack.yaml',
        }
        node.node_properties = {}
        node.create_state = {}

        self.assertRaises(
            Exception,
            provider.getCreateStateMachine,
            node, None, Mock())

    def test_generate_ephemeral_keypair(self):
        """Test ephemeral Ed25519 keypair generation."""
        priv, pub = heatprovider.HeatProvider._generateEphemeralKeypair()
        self.assertTrue(pub.startswith('ssh-ed25519 '))
        self.assertIn('BEGIN OPENSSH PRIVATE KEY', priv)
        self.assertIn('END OPENSSH PRIVATE KEY', priv)

        # Each call produces a unique keypair
        priv2, pub2 = heatprovider.HeatProvider._generateEphemeralKeypair()
        self.assertNotEqual(pub, pub2)
        self.assertNotEqual(priv, priv2)

    def test_create_state_machine_from_properties(self):
        """Test getCreateStateMachine reads template from
        node_properties and populates parameters."""
        sm = self._get_create_sm(provider_kw=dict(
            key_name='my-keypair', external_network='public',
            username='zuul'))
        self.assertEqual(sm.template_content, self.FULL_ZUUL_PARAMS_TEMPLATE)
        self.assertEqual(sm.parameters['zuul_key_name'], 'my-keypair')
        self.assertEqual(sm.parameters['zuul_username'], 'zuul')
        self.assertEqual(
            sm.parameters['zuul_external_network'], 'public')
        self.assertIn('zuul_ephemeral_public_key', sm.parameters)
        self.assertIn('zuul_ephemeral_private_key', sm.parameters)
        self.assertTrue(
            sm.parameters['zuul_ephemeral_public_key'].startswith(
                'ssh-ed25519 '))
        self.assertIn(
            'BEGIN OPENSSH PRIVATE KEY',
            sm.parameters['zuul_ephemeral_private_key'])

    def test_create_state_machine_parameters_empty_when_unset(self):
        """Test only ephemeral keys are set when key_name/username/image
        are unset."""
        sm = self._get_create_sm()
        self.assertNotIn('zuul_key_name', sm.parameters)
        self.assertNotIn('zuul_username', sm.parameters)
        self.assertNotIn('zuul_image', sm.parameters)
        self.assertIn('zuul_ephemeral_public_key', sm.parameters)
        self.assertIn('zuul_ephemeral_private_key', sm.parameters)

    def test_create_state_machine_zuul_image_from_image_id(self):
        """Test zuul_image parameter is set from image.image_id."""
        sm = self._get_create_sm(
            provider_kw=dict(image_id='abc123-glance-uuid'))
        self.assertEqual(sm.parameters['zuul_image'], 'abc123-glance-uuid')

    def test_create_state_machine_zuul_image_from_external_id(self):
        """Test zuul_image uses image_external_id for Zuul-managed images."""
        sm = self._get_create_sm(
            image_external_id='zuul-managed-image-456')
        self.assertEqual(
            sm.parameters['zuul_image'], 'zuul-managed-image-456')

    def test_create_state_machine_external_id_overrides_image_id(self):
        """Test image_external_id takes precedence over image.image_id."""
        sm = self._get_create_sm(
            provider_kw=dict(image_id='cloud-image-id'),
            image_external_id='zuul-managed-image-789')
        self.assertEqual(
            sm.parameters['zuul_image'], 'zuul-managed-image-789')

    def test_create_state_machine_user_heat_parameters(self):
        """Test user-defined heat_parameters from template-config."""
        sm = self._get_create_sm(
            provider_kw=dict(
                key_name='my-keypair', external_network='public',
                username='zuul'),
            node_kw=dict(heat_parameters={
                'external_network': 'public',
                'dns_nameserver': '8.8.8.8',
            }))
        self.assertEqual(sm.parameters['external_network'], 'public')
        self.assertEqual(sm.parameters['dns_nameserver'], '8.8.8.8')
        self.assertEqual(sm.parameters['zuul_key_name'], 'my-keypair')
        self.assertEqual(sm.parameters['zuul_username'], 'zuul')
        self.assertIn('zuul_ephemeral_public_key', sm.parameters)
        self.assertIn('zuul_ephemeral_private_key', sm.parameters)

    def test_create_state_machine_zuul_params_override_user_params(self):
        """Test Zuul-internal parameters take precedence over user ones."""
        sm = self._get_create_sm(
            provider_kw=dict(
                key_name='zuul-keypair', external_network='public',
                username='zuul', image_id='real-image-id'),
            node_kw=dict(heat_parameters={
                'zuul_key_name': 'user-attempt-to-override',
                'zuul_image': 'user-attempt-to-override',
                'custom_param': 'custom_value',
            }))
        self.assertEqual(sm.parameters['zuul_key_name'], 'zuul-keypair')
        self.assertEqual(sm.parameters['zuul_image'], 'real-image-id')
        self.assertEqual(sm.parameters['custom_param'], 'custom_value')

    def test_external_network_auto_discovered(self):
        """Test zuul_external_network is auto-discovered from Neutron."""
        endpoint = Mock()
        endpoint._getExternalNetwork.return_value = 'discovered-ext-net'

        provider = self._make_provider(endpoint=endpoint)
        node = self._make_node(
            template_content=self.FULL_ZUUL_PARAMS_TEMPLATE,
            template_path='ci/heat/stack.yaml')

        sm = provider.getCreateStateMachine(
            node, None, Mock())
        self.assertEqual(
            sm.parameters['zuul_external_network'], 'discovered-ext-net')
        endpoint._getExternalNetwork.assert_called_once()

    def test_zuul_params_not_passed_when_not_in_template(self):
        """Test zuul_* params are not sent to Heat when template lacks them.

        Heat rejects stack creation with 400 if parameters are supplied
        that are not declared in the template.  The driver must only pass
        zuul_* parameters that the template actually defines.
        """
        endpoint = Mock()
        provider = self._make_provider(
            key_name='my-keypair', external_network='public',
            username='zuul', image_id='my-image-id',
            endpoint=endpoint)

        node = self._make_node(
            template_content=_build_template(),
            template_path='ci/heat/stack.yaml')

        sm = provider.getCreateStateMachine(node, None, Mock())

        self.assertNotIn('zuul_key_name', sm.parameters)
        self.assertNotIn('zuul_username', sm.parameters)
        self.assertNotIn('zuul_image', sm.parameters)
        self.assertNotIn('zuul_external_network', sm.parameters)
        self.assertNotIn('zuul_ephemeral_public_key', sm.parameters)
        self.assertNotIn('zuul_ephemeral_private_key', sm.parameters)
        endpoint._getExternalNetwork.assert_not_called()

    def _assert_resource_policy(self, allowed_types, template,
                                should_pass=True, expected_violation=None):
        """Assert resource policy validation passes or rejects a template."""
        provider = self._make_provider(
            external_network='public',
            config={
                'resource_policy': {
                    'allowed_resource_types': allowed_types,
                },
            })

        node = self._make_node(template_content=template)

        if should_pass:
            sm = provider.getCreateStateMachine(node, None, Mock())
            self.assertIsNotNone(sm)
        else:
            with self.assertRaises(ResourcePolicyViolation) as cm:
                provider.getCreateStateMachine(node, None, Mock())
            if expected_violation:
                self.assertIn(expected_violation, str(cm.exception))

    def _assert_flavor_limits(self, flavor_attrs, template,
                              resolved_vcpus=2, resolved_ram=4096,
                              expected_errors=None):
        """Assert flavor limit validation passes or rejects a template.

        If flavor_attrs is None, no flavor is configured (skips validation).
        If expected_errors is None, expect success; otherwise expect
        FlavorLimitExceeded containing each error fragment.
        """
        if flavor_attrs is None:
            flavor = None
            endpoint = None
        else:
            flavor = Mock()
            for attr, value in flavor_attrs.items():
                setattr(flavor, attr, value)
            endpoint = Mock()
            endpoint._findFlavor.return_value = Mock(
                vcpus=resolved_vcpus, ram=resolved_ram)

        provider = self._make_provider(
            external_network='public', endpoint=endpoint,
            config={}, flavor=flavor)

        node = self._make_node(template_content=template)

        if expected_errors is None:
            sm = provider.getCreateStateMachine(node, None, Mock())
            self.assertIsNotNone(sm)
        else:
            with self.assertRaises(FlavorLimitExceeded) as cm:
                provider.getCreateStateMachine(node, None, Mock())
            error_msg = str(cm.exception)
            if isinstance(expected_errors, str):
                expected_errors = [expected_errors]
            for fragment in expected_errors:
                self.assertIn(fragment, error_msg)

    def test_create_state_machine_resource_policy_allowed_pass(self):
        """Test getCreateStateMachine passes with compliant template."""
        self._assert_resource_policy(
            allowed_types=['OS::Nova::Server', 'OS::Neutron::Net'],
            template=_build_template(servers=['m1.small'], networks=1))

    def test_create_state_machine_resource_policy_allowed_reject(self):
        """Test getCreateStateMachine rejects template not in allow list."""
        self._assert_resource_policy(
            allowed_types=['OS::Nova::Server', 'OS::Neutron::Net'],
            template=_build_template(
                servers=['m1.small'], resource_groups=1),
            should_pass=False,
            expected_violation='OS::Heat::ResourceGroup')

    def test_create_state_machine_resource_policy_negative_lookahead(self):
        """Test getCreateStateMachine rejects via negative lookahead."""
        self._assert_resource_policy(
            allowed_types=[
                '^(?!OS::Heat::ResourceGroup$)'
                '(?!OS::Neutron::FloatingIP$).*',
            ],
            template=_build_template(
                servers=['m1.small'], floating_ips=1),
            should_pass=False,
            expected_violation='OS::Neutron::FloatingIP')

    def test_create_sm_resource_policy_negative_lookahead_pass(self):
        """Test getCreateStateMachine passes with negative lookahead."""
        self._assert_resource_policy(
            allowed_types=['^(?!OS::Heat::ResourceGroup$).*'],
            template=_build_template(servers=['m1.small'], networks=1))

    def test_create_state_machine_empty_resource_policy_skips(self):
        """Test getCreateStateMachine skips validation with empty policy."""
        provider = self._make_provider(
            external_network='public',
            config={'resource_policy': {}})

        node = self._make_node(
            template_content=_build_template(resource_groups=1))

        sm = provider.getCreateStateMachine(node, None, Mock())
        self.assertIsNotNone(sm)

    def test_create_state_machine_resource_policy_regex_prefix_match(self):
        """Test resource policy with regex prefix pattern in allow list."""
        policy = ['^(?!OS::Keystone).*']
        self._assert_resource_policy(
            allowed_types=policy,
            template=_build_template(
                servers=['m1.small'],
                extra_resources={'user': 'OS::Keystone::User'}),
            should_pass=False,
            expected_violation='OS::Keystone::User')
        self._assert_resource_policy(
            allowed_types=policy,
            template=_build_template(servers=['m1.small'], networks=1))

    def test_create_state_machine_resource_policy_regex_suffix_match(self):
        """Test resource policy with regex suffix pattern in allow list."""
        policy = ['^(?!.*::FloatingIP$).*']
        self._assert_resource_policy(
            allowed_types=policy,
            template=_build_template(
                servers=['m1.small'], floating_ips=1),
            should_pass=False,
            expected_violation='OS::Neutron::FloatingIP')
        self._assert_resource_policy(
            allowed_types=policy,
            template=_build_template(servers=['m1.small'], networks=1))

    def test_create_state_machine_resource_policy_regex_complex_pattern(self):
        """Test resource policy with complex regex pattern."""
        self._assert_resource_policy(
            allowed_types=['^OS::(Nova|Neutron|Cinder)::.*'],
            template=_build_template(
                servers=['m1.small'], networks=1, volumes=[10]))

    def test_provider_schema_resource_policy_invalid_regex(self):
        """Test that invalid regex patterns are caught at config load time."""
        import voluptuous as vs
        schema = self.driver.getProviderSchema()
        config = {
            'name': 'test-provider',
            'section': 'test-section',
            'connection': 'test-connection',
            'labels': [],
            'images': [],
            'flavors': [],
            'resource-policy': {
                'allowed-resource-types': [
                    'OS::Nova::Server',
                    '[invalid regex(',  # Invalid regex
                ]
            }
        }
        with self.assertRaises(vs.Invalid) as cm:
            schema(config)
        self.assertIn('Invalid regex pattern', str(cm.exception))
        self.assertIn('[invalid regex(', str(cm.exception))

    def test_create_state_machine_flavor_limits_pass(self):
        """Test getCreateStateMachine passes when within flavor limits."""
        self._assert_flavor_limits(
            flavor_attrs=dict(
                max_instances=5, max_cores=20, max_ram=40960,
                max_networks=3, max_floating_ips=2),
            template=_build_template(
                servers=['m1.small', 'm1.small'], networks=1))

    def test_create_state_machine_flavor_limits_exceeded_instances(self):
        """Test getCreateStateMachine rejects when instances exceed limit."""
        self._assert_flavor_limits(
            flavor_attrs=dict(
                max_instances=2, max_cores=100, max_ram=102400),
            template=_build_template(
                servers=['m1.small', 'm1.small', 'm1.small']),
            expected_errors='instances: 3 > 2')

    def test_create_state_machine_flavor_limits_exceeded_cores(self):
        """Test getCreateStateMachine rejects when cores exceed limit."""
        self._assert_flavor_limits(
            flavor_attrs=dict(
                max_instances=10, max_cores=4, max_ram=102400),
            resolved_vcpus=4, resolved_ram=8192,
            template=_build_template(
                servers=['m1.xlarge', 'm1.xlarge']),
            expected_errors='cores: 8 > 4')

    def test_create_state_machine_flavor_limits_exceeded_floating_ips(self):
        """Test getCreateStateMachine rejects when floating IPs exceed."""
        self._assert_flavor_limits(
            flavor_attrs=dict(
                max_instances=10, max_cores=100, max_ram=102400,
                max_floating_ips=1),
            resolved_vcpus=1, resolved_ram=1024,
            template=_build_template(
                servers=['m1.small'], floating_ips=2),
            expected_errors='floating-ips: 2 > 1')

    def test_create_state_machine_flavor_limits_multiple_violations(self):
        """Test FlavorLimitExceeded reports all violations together."""
        self._assert_flavor_limits(
            flavor_attrs=dict(
                max_instances=1, max_cores=2, max_ram=2048),
            resolved_vcpus=4, resolved_ram=8192,
            template=_build_template(
                servers=['m1.xlarge', 'm1.xlarge']),
            expected_errors=[
                'instances: 2 > 1',
                'cores: 8 > 2',
                'ram: 16384 > 2048',
            ])

    def test_create_state_machine_flavor_limits_undefined_skipped(self):
        """Test undefined flavor limits are not enforced."""
        self._assert_flavor_limits(
            flavor_attrs=dict(
                max_instances=vs.UNDEFINED, max_cores=vs.UNDEFINED,
                max_ram=vs.UNDEFINED, max_volumes=vs.UNDEFINED,
                max_volume_gb=vs.UNDEFINED, max_networks=vs.UNDEFINED,
                max_routers=vs.UNDEFINED,
                max_routers_with_external_gw=vs.UNDEFINED,
                max_floating_ips=vs.UNDEFINED),
            resolved_vcpus=4, resolved_ram=8192,
            template=_build_template(
                servers=['m1.xlarge'] * 4))

    def test_create_state_machine_flavor_limits_no_flavor_skips(self):
        """Test validation is skipped when no flavor is configured."""
        self._assert_flavor_limits(
            flavor_attrs=None,
            template=_build_template(servers=['m1.xlarge']))

    def test_create_state_machine_flavor_limits_volumes(self):
        """Test flavor limit enforcement for volumes."""
        self._assert_flavor_limits(
            flavor_attrs=dict(
                max_instances=10, max_cores=100, max_ram=102400,
                max_volumes=2, max_volume_gb=50),
            resolved_vcpus=1, resolved_ram=1024,
            template=_build_template(
                servers=['m1.small'], volumes=[20, 20, 20]),
            expected_errors=[
                'volumes: 3 > 2',
                'volume-gb: 60 > 50',
            ])


class TestCleanupWorkerManagedSet(BaseTestCase):
    """Test that the cleanup worker populates managed_resource_ids

    The cleanup worker iterates all nodes in the cache. For Heat nodes
    on the endpoint being cleaned, it collects heat_physical_resource_ids
    and stores the union on the endpoint as managed_resource_ids. This
    set is used by listInstances() to classify project-wide cloud
    resources as managed vs unmanaged for quota tracking.
    """

    def _make_cleanup_worker(self):
        """Create a CleanupWorker with mocked launcher dependencies."""
        from zuul.launcher.server import CleanupWorker

        launcher = Mock()
        launcher.system.system_id = 'test-system'
        launcher.api.nodes_cache.getItems.return_value = []
        launcher.api.getProviderNode.return_value = None
        launcher._getProviders.return_value = []
        worker = CleanupWorker(launcher)
        return worker

    def _make_endpoint(self, name='test-cloud-RegionOne'):
        """Create a mock endpoint for cleanup tests."""
        endpoint = Mock()
        endpoint.name = name
        endpoint.canonical_name = name
        endpoint.listRequiredInfrastructureResources.return_value = []
        endpoint.listInstances.return_value = []
        endpoint.listResources.return_value = []
        endpoint.quota_cache.setUnmanagedUsage = Mock()
        return endpoint

    def _make_heat_node(self, uuid, endpoint_name, resource_ids=None):
        """Create a mock Heat node with physical resource IDs."""
        node = Mock()
        node.uuid = uuid
        node.endpoint_name = endpoint_name
        node.heat_physical_resource_ids = resource_ids or []
        node.getRequiredResources.return_value = []
        return node

    def _make_non_heat_node(self, uuid, endpoint_name):
        """Create a mock non-Heat node without heat_physical_resource_ids."""
        node = Mock(spec=['uuid', 'endpoint_name', 'getRequiredResources'])
        node.uuid = uuid
        node.endpoint_name = endpoint_name
        node.getRequiredResources.return_value = []
        return node

    def test_managed_set_populated_from_heat_nodes(self):
        """Test managed_resource_ids is populated from Heat nodes.

        managed_resource_ids is a dict mapping resource UUID -> node UUID
        so that listInstances() can reconstruct Zuul metadata for managed
        resources.
        """
        worker = self._make_cleanup_worker()
        endpoint = self._make_endpoint()

        node1 = self._make_heat_node(
            'node-1', endpoint.name,
            resource_ids=['server-aaa', 'vol-bbb'])
        node2 = self._make_heat_node(
            'node-2', endpoint.name,
            resource_ids=['server-ccc', 'net-ddd'])

        worker.launcher.api.nodes_cache.getItems.return_value = [
            node1, node2]

        worker.cleanupLeakedResources(endpoint)

        self.assertEqual(
            endpoint.managed_resource_ids,
            {
                'server-aaa': 'node-1',
                'vol-bbb': 'node-1',
                'server-ccc': 'node-2',
                'net-ddd': 'node-2',
            })

    def test_managed_set_empty_when_no_heat_nodes(self):
        """Test managed_resource_ids is empty when no Heat nodes exist."""
        worker = self._make_cleanup_worker()
        endpoint = self._make_endpoint()

        worker.launcher.api.nodes_cache.getItems.return_value = []

        worker.cleanupLeakedResources(endpoint)

        self.assertEqual(endpoint.managed_resource_ids, {})

    def test_managed_set_ignores_nodes_on_other_endpoints(self):
        """Test that nodes on other endpoints don't contribute."""
        worker = self._make_cleanup_worker()
        endpoint = self._make_endpoint(name='cloud-a-RegionOne')

        node_other = self._make_heat_node(
            'node-other', 'cloud-b-RegionTwo',
            resource_ids=['server-zzz'])
        node_mine = self._make_heat_node(
            'node-mine', 'cloud-a-RegionOne',
            resource_ids=['server-aaa'])

        worker.launcher.api.nodes_cache.getItems.return_value = [
            node_other, node_mine]

        worker.cleanupLeakedResources(endpoint)

        self.assertEqual(
            endpoint.managed_resource_ids, {'server-aaa': 'node-mine'})

    def test_managed_set_handles_non_heat_nodes(self):
        """Test that non-Heat nodes (without the attribute) are handled."""
        worker = self._make_cleanup_worker()
        endpoint = self._make_endpoint()

        heat_node = self._make_heat_node(
            'heat-node', endpoint.name,
            resource_ids=['server-111'])
        non_heat_node = self._make_non_heat_node(
            'openstack-node', endpoint.name)

        worker.launcher.api.nodes_cache.getItems.return_value = [
            heat_node, non_heat_node]

        worker.cleanupLeakedResources(endpoint)

        self.assertEqual(
            endpoint.managed_resource_ids, {'server-111': 'heat-node'})
