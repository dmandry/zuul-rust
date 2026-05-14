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
from zuul.driver.heat.heatstatemachine import (
    HeatCreateStateMachine,
    HeatDeleteStateMachine,
)
from zuul.exceptions import ConfigurationError
from zuul.model import QuotaInformation


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

    def test_heat_provider_node_init(self):
        """Test HeatProviderNode initialization."""
        node = HeatProviderNode()
        self.assertIsNone(node.heat_stack_id)
        self.assertIsNone(node.heat_stack_name)
        self.assertIsNone(node.heat_stack_status)
        self.assertIsNone(node.heat_template_path)
        self.assertIsNone(node.heat_stack_outputs)

    def test_heat_provider_node_driver_data(self):
        """Test HeatProviderNode getDriverData serialization."""
        node = HeatProviderNode()
        node._set(
            heat_stack_id='test-stack-id-123',
            heat_stack_name='zuul-test-stack',
            heat_stack_status='CREATE_COMPLETE',
            heat_template_path='.zuul/templates/test.yaml',
            heat_stack_outputs={'bastion_ip': '192.0.2.100'}
        )

        data = node.getDriverData()
        self.assertEqual(data['heat_stack_id'], 'test-stack-id-123')
        self.assertEqual(data['heat_stack_name'], 'zuul-test-stack')
        self.assertEqual(data['heat_stack_status'], 'CREATE_COMPLETE')
        self.assertEqual(data['heat_template_path'],
                         '.zuul/templates/test.yaml')
        self.assertEqual(data['heat_stack_outputs'],
                         {'bastion_ip': '192.0.2.100'})

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

    def test_heat_resource_image(self):
        """Test HeatResource for a Glance image."""
        metadata = {'zuul_node_uuid': 'node-999',
                    'zuul_system_id': 'test-system'}
        resource = HeatResource(
            metadata, HeatResource.TYPE_IMAGE, 'image-xyz123')

        self.assertEqual(resource.type, HeatResource.TYPE_IMAGE)
        self.assertEqual(resource.id, 'image-xyz123')
        self.assertEqual(resource.unique_id, 'heat-image-image-xyz123')
        self.assertEqual(resource.metadata['zuul_node_uuid'], 'node-999')

    def test_heat_resource_with_stack_id(self):
        """Test HeatResource with optional stack_id tracking."""
        metadata = {'zuul_node_uuid': 'node-555'}
        resource = HeatResource(
            metadata, HeatResource.TYPE_IMAGE,
            'image-from-stack', stack_id='parent-stack-789')

        self.assertEqual(resource.stack_id, 'parent-stack-789')
        self.assertEqual(resource.unique_id, 'heat-image-image-from-stack')


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

    def test_connection_driver_name(self):
        """Test that connection has correct driver name."""
        config = {
            'cloud': 'mycloud'
        }
        conn = self.driver.getConnection('heat-conn', config)
        self.assertEqual(conn.driver_name, 'heat')

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

    def test_get_schema_classes(self):
        """Test schema class retrieval methods."""
        self.assertIsNotNone(self.driver.getProviderSchema())
        self.assertIsNotNone(self.driver.getProviderSchemaClass())
        self.assertIsNotNone(self.driver.getProviderClass())

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

    def test_provider_schema_resource_policy_whitelist(self):
        """Test resource-policy with allowed types."""
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
                    'OS::Neutron::Net',
                    'OS::Neutron::Router',
                ]
            }
        }
        validated = schema(config)
        self.assertEqual(
            len(validated['resource_policy']['allowed_resource_types']), 3)

    def test_provider_schema_resource_policy_blacklist(self):
        """Test resource-policy with disallowed types."""
        schema = self.driver.getProviderSchema()
        config = {
            'name': 'test-provider',
            'section': 'test-section',
            'connection': 'test-connection',
            'labels': [],
            'images': [],
            'flavors': [],
            'resource-policy': {
                'disallowed-resource-types': [
                    'OS::Heat::ResourceGroup',
                    'OS::Neutron::FloatingIP',
                ]
            }
        }
        validated = schema(config)
        self.assertEqual(
            len(validated['resource_policy']['disallowed_resource_types']),
            2)

    def test_provider_schema_resource_policy_mutual_exclusion(self):
        """Test that allowed and disallowed types cannot both be specified."""
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
                'allowed-resource-types': ['OS::Nova::Server'],
                'disallowed-resource-types': ['OS::Heat::ResourceGroup'],
            }
        }

        # Should raise an error because both are specified
        with self.assertRaises(vs.Invalid) as cm:
            schema(config)

        self.assertIn('Cannot specify both', str(cm.exception))
        self.assertIn('allowed-resource-types', str(cm.exception))
        self.assertIn('disallowed-resource-types', str(cm.exception))

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
            'az': 'nova',
        }
        validated = label_schema(label_config)
        self.assertEqual(validated['name'], 'test-label')
        self.assertEqual(validated['region'], 'RegionOne')
        self.assertEqual(validated['az'], 'nova')

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
                'disallowed-resource-types': ['OS::Heat::ResourceGroup'],
            },
        }
        validated = provider_schema(provider_config)
        self.assertEqual(validated['name'], 'heat-provider')
        self.assertEqual(validated['resource_limits']['stacks'], 10)
        self.assertEqual(validated['resource_limits']['instances'], 50)
        self.assertIn(
            'OS::Heat::ResourceGroup',
            validated['resource_policy']['disallowed_resource_types'])

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

        # Test zuul image
        zuul_image = {
            'name': 'test-image',
            'project_canonical_name': 'example.com/project',
            'config_hash': 'jkl012',
            'branch': 'main',
            'type': 'zuul',
        }
        validated = image_schema(zuul_image)
        self.assertEqual(validated['name'], 'test-image')
        self.assertEqual(validated['type'], 'zuul')


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

    def test_endpoint_caching(self):
        """Test that endpoints are cached by region."""
        connection = Mock()
        connection.connection_name = 'test-cloud'
        connection.rate = 2
        zk_client = self._create_mock_zk_client()

        # Get endpoints for the same region twice
        endpoint1 = self.driver._getEndpoint(
            zk_client, connection, 'RegionOne', 'test-system')
        endpoint2 = self.driver._getEndpoint(
            zk_client, connection, 'RegionOne', 'test-system')

        # Should be the same cached instance
        self.assertIs(endpoint1, endpoint2)

    def test_endpoint_different_regions(self):
        """Test that different regions get different endpoints."""
        connection = Mock()
        connection.connection_name = 'test-cloud'
        connection.rate = 2
        zk_client = self._create_mock_zk_client()

        # Get endpoints for different regions
        endpoint1 = self.driver._getEndpoint(
            zk_client, connection, 'RegionOne', 'test-system')
        endpoint2 = self.driver._getEndpoint(
            zk_client, connection, 'RegionTwo', 'test-system')

        # Should be different instances
        self.assertIsNot(endpoint1, endpoint2)

    def test_endpoint_name_construction(self):
        """Test endpoint name is constructed correctly."""
        connection = Mock()
        connection.connection_name = 'my-cloud'
        connection.rate = 2
        zk_client = self._create_mock_zk_client()

        endpoint = self.driver._getEndpoint(
            zk_client, connection, 'RegionOne', 'test-system')

        # Name should be connection-region
        self.assertEqual(endpoint.name, 'my-cloud-RegionOne')

    def test_endpoint_name_no_region(self):
        """Test endpoint name when region is None."""
        connection = Mock()
        connection.connection_name = 'my-cloud'
        connection.rate = 2
        zk_client = self._create_mock_zk_client()

        endpoint = self.driver._getEndpoint(
            zk_client, connection, None, 'test-system')

        # Name should handle None region
        self.assertEqual(endpoint.name, 'my-cloud-default')

    def test_get_stack_transient_error_retry(self):
        """Test _getStack retries on transient HTTP 5xx errors."""
        connection = Mock()
        connection.connection_name = 'test-cloud'
        connection.rate = 2
        connection.cloud_name = 'test'
        connection.config_files = None
        zk_client = self._create_mock_zk_client()

        endpoint = self.driver._getEndpoint(
            zk_client, connection, None, 'test-system')

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
        connection = Mock()
        connection.connection_name = 'test-cloud'
        connection.rate = 2
        connection.cloud_name = 'test'
        connection.config_files = None
        zk_client = self._create_mock_zk_client()

        endpoint = self.driver._getEndpoint(
            zk_client, connection, None, 'test-system')

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
        connection = Mock()
        connection.connection_name = 'test-cloud'
        connection.rate = 2
        connection.cloud_name = 'test'
        connection.config_files = None
        zk_client = self._create_mock_zk_client()

        endpoint = self.driver._getEndpoint(
            zk_client, connection, None, 'test-system')

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
        connection = Mock()
        connection.connection_name = 'test-cloud'
        connection.rate = 2
        connection.cloud_name = 'test'
        connection.config_files = None
        zk_client = self._create_mock_zk_client()

        endpoint = self.driver._getEndpoint(
            zk_client, connection, None, 'test-system')

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

    def test_get_instance_missing_bastion_ip(self):
        """Test that missing bastion IP returns instance with None IP."""
        connection = Mock()
        connection.connection_name = 'test-cloud'
        connection.rate = 2
        connection.cloud_name = 'test'
        connection.config_files = None
        zk_client = self._create_mock_zk_client()

        endpoint = self.driver._getEndpoint(
            zk_client, connection, None, 'test-system')

        stack = Mock()
        stack.id = 'stack-123'

        quota = QuotaInformation(stacks=1)

        outputs = {}

        instance = endpoint._getInstance(stack, quota, outputs)
        self.assertIsNone(instance.public_ipv4)
        self.assertIsNone(instance.interface_ip)
        self.assertEqual(instance.heat_stack_id, 'stack-123')

    def test_find_flavor_success(self):
        """Test _findFlavor returns flavor when found."""
        connection = Mock()
        connection.connection_name = 'test-cloud'
        connection.rate = 2
        connection.cloud_name = 'test'
        connection.config_files = None
        zk_client = self._create_mock_zk_client()

        endpoint = self.driver._getEndpoint(
            zk_client, connection, None, 'test-system')

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
        connection = Mock()
        connection.connection_name = 'test-cloud'
        connection.rate = 2
        connection.cloud_name = 'test'
        connection.config_files = None
        zk_client = self._create_mock_zk_client()

        endpoint = self.driver._getEndpoint(
            zk_client, connection, None, 'test-system')

        # Mock flavor not found (returns None)
        endpoint._client = Mock()
        endpoint._client.compute.find_flavor = Mock(return_value=None)

        # Should raise NotFoundException
        with self.assertRaises(openstack.exceptions.NotFoundException) as cm:
            endpoint._findFlavor('nonexistent-flavor')

        # Check error message contains flavor name
        self.assertIn('nonexistent-flavor', str(cm.exception))
        self.assertIn('not found', str(cm.exception).lower())

    def test_list_instances(self):
        """Tests listInstances method."""
        connection = Mock()
        connection.connection_name = 'test-cloud'
        connection.rate = 2
        connection.cloud_name = 'test'
        connection.config_files = None
        zk_client = self._create_mock_zk_client()

        endpoint = self.driver._getEndpoint(
            zk_client, connection, None, 'test-system')

        # Mock the client and stack listing
        mock_client = Mock()
        endpoint._client = mock_client

        # Create mock stacks returned by API
        mock_stack1 = Mock()
        mock_stack1.id = 'stack-1'
        mock_stack1.status = 'CREATE_COMPLETE'

        mock_stack2 = Mock()
        mock_stack2.id = 'stack-2'
        mock_stack2.status = 'UPDATE_COMPLETE'

        # Mock deleted stack (should be skipped for quota)
        mock_stack3 = Mock()
        mock_stack3.id = 'stack-3'
        mock_stack3.status = 'DELETE_COMPLETE'

        # Mock failed deletion stack (should NOT be skipped - still uses
        # resources)
        mock_stack4 = Mock()
        mock_stack4.id = 'stack-4'
        mock_stack4.status = 'DELETE_FAILED'

        # Mock deleting stack (should NOT be skipped - still uses resources)
        mock_stack5 = Mock()
        mock_stack5.id = 'stack-5'
        mock_stack5.status = 'DELETE_IN_PROGRESS'

        mock_client.orchestration.stacks.return_value = [
            mock_stack1, mock_stack2, mock_stack3, mock_stack4, mock_stack5
        ]

        # Mock stack details
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

        # Call listInstances
        instances = list(endpoint.listInstances())

        # Verify results - 4 instances (only DELETE_COMPLETE is skipped)
        self.assertEqual(len(instances), 4)

        # Check first instance
        self.assertEqual(instances[0].heat_stack_id, 'stack-1')
        self.assertEqual(instances[0].public_ipv4, '10.0.0.1')
        self.assertEqual(instances[0].metadata['zuul_system_id'],
                         'test-system')
        self.assertEqual(instances[0].metadata['zuul_node_uuid'], 'node-1')
        self.assertEqual(instances[0].metadata['custom_tag'], 'value')

        # Check second instance
        self.assertEqual(instances[1].heat_stack_id, 'stack-2')
        self.assertEqual(instances[1].public_ipv4, '10.0.0.2')
        self.assertEqual(instances[1].metadata['zuul_node_uuid'], 'node-2')

        # Check that DELETE_FAILED stack is included (still uses resources)
        self.assertEqual(instances[2].heat_stack_id, 'stack-4')
        self.assertEqual(instances[2].public_ipv4, '10.0.0.4')

        # Check that DELETE_IN_PROGRESS stack is included (still uses
        # resources)
        self.assertEqual(instances[3].heat_stack_id, 'stack-5')
        self.assertEqual(instances[3].public_ipv4, '10.0.0.5')

        # Verify API was called with correct tag filter
        mock_client.orchestration.stacks.assert_called_once_with(
            tags='zuul_system_id=test-system'
        )

    def test_list_instances_missing_bastion_ip(self):
        """Tests listInstances tolerates missing bastion_ip."""
        connection = Mock()
        connection.connection_name = 'test-cloud'
        connection.rate = 2
        connection.cloud_name = 'test'
        connection.config_files = None
        zk_client = self._create_mock_zk_client()

        endpoint = self.driver._getEndpoint(
            zk_client, connection, None, 'test-system')

        mock_client = Mock()
        endpoint._client = mock_client

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
        self.assertEqual(len(instances), 1)
        self.assertIsNone(instances[0].public_ipv4)
        self.assertEqual(instances[0].heat_stack_id, 'stack-1')

    def test_list_resources(self):
        """Tests listResources method."""
        connection = Mock()
        connection.connection_name = 'test-cloud'
        connection.rate = 2
        connection.cloud_name = 'test'
        connection.config_files = None
        zk_client = self._create_mock_zk_client()

        endpoint = self.driver._getEndpoint(
            zk_client, connection, None, 'test-system')

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
        connection = Mock()
        connection.connection_name = 'test-cloud'
        connection.rate = 2
        connection.cloud_name = 'test'
        connection.config_files = None
        zk_client = self._create_mock_zk_client()

        endpoint = self.driver._getEndpoint(
            zk_client, connection, None, 'test-system')

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

    def test_create_stack_joins_tags(self):
        """Tests _createStack joins tags list into comma-separated string."""
        connection = Mock()
        connection.connection_name = 'test-cloud'
        connection.rate = 2
        connection.cloud_name = 'test'
        connection.config_files = None
        zk_client = self._create_mock_zk_client()

        endpoint = self.driver._getEndpoint(
            zk_client, connection, None, 'test-system')

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
        connection = Mock()
        connection.connection_name = 'test-cloud'
        connection.rate = 2
        connection.cloud_name = 'test'
        connection.config_files = None
        zk_client = self._create_mock_zk_client()

        endpoint = self.driver._getEndpoint(
            zk_client, connection, None, 'test-system')

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
        connection = Mock()
        connection.connection_name = 'test-cloud'
        connection.rate = 2
        connection.cloud_name = 'test'
        connection.config_files = None
        zk_client = self._create_mock_zk_client()

        endpoint = self.driver._getEndpoint(
            zk_client, connection, None, 'test-system')

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
        connection = Mock()
        connection.connection_name = 'test-cloud'
        connection.rate = 2
        connection.cloud_name = 'test'
        connection.config_files = None
        zk_client = self._create_mock_zk_client()

        endpoint = self.driver._getEndpoint(
            zk_client, connection, None, 'test-system')

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

    def test_create_state_machine_init(self):
        """Test create state machine initialization."""
        endpoint = Mock()
        node = Mock()
        node.create_state = {}
        log = Mock()

        sm = self.HeatCreateStateMachine(
            endpoint, node, 'test-stack', 'template: {}',
            {}, {'tag1': 'value1'}, log)

        self.assertEqual(sm.state, sm.START)
        self.assertEqual(sm.stack_name, 'test-stack')
        self.assertFalse(sm.complete)

    def test_delete_state_machine_init(self):
        """Test delete state machine initialization."""
        endpoint = Mock()
        node = Mock()
        node.delete_state = {}
        node.heat_stack_id = 'stack-123'
        log = Mock()

        sm = self.HeatDeleteStateMachine(endpoint, node, log)

        self.assertEqual(sm.state, sm.START)
        self.assertEqual(sm.stack_id, 'stack-123')
        self.assertFalse(sm.complete)

    def test_delete_state_machine_no_stack_id(self):
        """Test delete state machine with no stack ID."""
        endpoint = Mock()
        node = Mock()
        node.delete_state = {}
        node.heat_stack_id = None
        node.uuid = 'node-456'
        log = Mock()

        sm = self.HeatDeleteStateMachine(endpoint, node, log)

        # Should skip straight to COMPLETE if no stack ID
        self.assertEqual(sm.state, sm.COMPLETE)

    def test_create_state_machine_to_dict(self):
        """Test create state machine serialization."""
        endpoint = Mock()
        node = Mock()
        node.create_state = {}
        log = Mock()

        sm = self.HeatCreateStateMachine(
            endpoint, node, 'test-stack', 'template: {}',
            {}, {}, log)
        sm.stack_id = 'stack-789'

        state_dict = sm.toDict()
        self.assertIn('state', state_dict)
        self.assertIn('start_time', state_dict)
        self.assertIn('heat_stack_id', state_dict)
        self.assertEqual(state_dict['heat_stack_id'], 'stack-789')

    def test_create_advance_start_to_submit(self):
        """Test create state machine advances from START to SUBMIT."""
        endpoint = Mock()
        endpoint._submitApi = Mock(return_value='future-123')
        endpoint._completeApi = Mock(return_value=None)
        node = Mock()
        node.create_state = {}
        log = Mock()

        sm = self.HeatCreateStateMachine(
            endpoint, node, 'test-stack', 'template: {}',
            {}, {}, log)

        self.assertEqual(sm.state, sm.START)
        sm.advance()
        self.assertEqual(sm.state, sm.STACK_CREATING_SUBMIT)

    def test_create_advance_submit_api_call(self):
        """Test create state machine submits API call."""
        endpoint = Mock()
        endpoint._submitApi = Mock(return_value='future-123')
        endpoint._completeApi = Mock(return_value=None)
        node = Mock()
        node.create_state = {}
        log = Mock()

        sm = self.HeatCreateStateMachine(
            endpoint, node, 'test-stack', 'template: {}',
            {'param1': 'value1'}, ['tag1=val1'], log)
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
        endpoint = Mock()
        stack = Mock()
        stack.id = 'stack-123'
        stack.name = 'test-stack'
        stack.status = 'CREATE_IN_PROGRESS'
        endpoint._submitApi = Mock(return_value='future-123')
        endpoint._completeApi = Mock(return_value=stack)
        endpoint._getStack = Mock(return_value=stack)
        node = Mock()
        node.create_state = {}
        log = Mock()

        sm = self.HeatCreateStateMachine(
            endpoint, node, 'test-stack', 'template: {}',
            {}, {}, log)
        sm.state = sm.STACK_CREATING_SUBMIT

        sm.advance()

        self.assertEqual(sm.stack_id, 'stack-123')
        self.assertEqual(node.heat_stack_id, 'stack-123')
        self.assertEqual(node.heat_stack_name, 'test-stack')
        self.assertEqual(sm.state, sm.STACK_CREATING)

    def test_create_advance_submit_quota_exception(self):
        """Test create state machine handles quota exceptions."""
        endpoint = Mock()
        endpoint._submitApi = Mock(return_value='future-123')
        endpoint._completeApi = Mock(
            side_effect=openstack.exceptions.SDKException(
                'Quota exceeded for instances'))
        node = Mock()
        node.create_state = {}
        log = Mock()

        sm = self.HeatCreateStateMachine(
            endpoint, node, 'test-stack', 'template: {}',
            {}, {}, log)
        sm.state = sm.STACK_CREATING_SUBMIT

        from zuul import exceptions
        with self.assertRaises(exceptions.QuotaException):
            sm.advance()

    def test_create_advance_submit_general_exception(self):
        """Test create state machine handles general exceptions."""
        endpoint = Mock()
        endpoint._submitApi = Mock(return_value='future-123')
        endpoint._completeApi = Mock(
            side_effect=Exception('Network error'))
        node = Mock()
        node.create_state = {}
        log = Mock()

        sm = self.HeatCreateStateMachine(
            endpoint, node, 'test-stack', 'template: {}',
            {}, {}, log)
        sm.state = sm.STACK_CREATING_SUBMIT

        with self.assertRaises(Exception) as cm:
            sm.advance()
        self.assertIn('Network error', str(cm.exception))

    def test_create_advance_creating_in_progress(self):
        """Test create state machine waits for CREATE_IN_PROGRESS."""
        endpoint = Mock()
        stack = Mock()
        stack.id = 'stack-123'
        stack.status = 'CREATE_IN_PROGRESS'
        endpoint._getStack = Mock(return_value=stack)
        node = Mock()
        node.create_state = {}
        log = Mock()

        sm = self.HeatCreateStateMachine(
            endpoint, node, 'test-stack', 'template: {}',
            {}, {}, log)
        sm.state = sm.STACK_CREATING
        sm.stack_id = 'stack-123'

        sm.advance()

        endpoint._getStack.assert_called_once_with('stack-123')
        # Still in CREATING state
        self.assertEqual(sm.state, sm.STACK_CREATING)
        self.assertFalse(sm.complete)

    def test_create_advance_creating_complete(self):
        """Test create state machine handles CREATE_COMPLETE."""
        endpoint = Mock()
        stack = Mock()
        stack.id = 'stack-123'
        stack.status = 'CREATE_COMPLETE'
        stack.outputs = [
            {'output_key': 'bastion_ip', 'output_value': '192.0.2.1'}
        ]
        endpoint._getStack = Mock(return_value=stack)
        instance = Mock()
        endpoint._getInstance = Mock(return_value=instance)
        node = Mock()
        node.create_state = {}
        node.node_properties = {}
        node.quota = QuotaInformation(stacks=1)
        log = Mock()

        sm = self.HeatCreateStateMachine(
            endpoint, node, 'test-stack', 'template: {}',
            {}, {}, log)
        sm.state = sm.STACK_CREATING
        sm.stack_id = 'stack-123'

        sm.advance()

        self.assertEqual(sm.state, sm.COMPLETE)
        self.assertTrue(sm.complete)
        node.updateFromInstance.assert_called_once_with(instance)
        endpoint._getInstance.assert_called_once_with(
            stack, node.quota,
            {'bastion_ip': '192.0.2.1'})

    def test_create_advance_creating_failed(self):
        """Test create state machine handles CREATE_FAILED."""
        endpoint = Mock()
        stack = Mock()
        stack.id = 'stack-123'
        stack.status = 'CREATE_FAILED'
        stack.stack_status_reason = 'Insufficient resources'
        endpoint._getStack = Mock(return_value=stack)
        node = Mock()
        node.create_state = {}
        log = Mock()

        sm = self.HeatCreateStateMachine(
            endpoint, node, 'test-stack', 'template: {}',
            {}, {}, log)
        sm.state = sm.STACK_CREATING
        sm.stack_id = 'stack-123'

        with self.assertRaises(Exception) as cm:
            sm.advance()
        self.assertIn('Insufficient resources', str(cm.exception))

    def test_create_advance_creating_unexpected_status(self):
        """Test create state machine handles unexpected status."""
        endpoint = Mock()
        stack = Mock()
        stack.id = 'stack-123'
        stack.status = 'UNKNOWN_STATUS'
        endpoint._getStack = Mock(return_value=stack)
        node = Mock()
        node.create_state = {}
        log = Mock()

        sm = self.HeatCreateStateMachine(
            endpoint, node, 'test-stack', 'template: {}',
            {}, {}, log)
        sm.state = sm.STACK_CREATING
        sm.stack_id = 'stack-123'

        with self.assertRaises(Exception) as cm:
            sm.advance()
        self.assertIn('Unexpected stack status', str(cm.exception))
        self.assertIn('UNKNOWN_STATUS', str(cm.exception))

    def test_create_advance_creating_stack_disappeared(self):
        """Test create state machine handles disappeared stack."""
        endpoint = Mock()
        endpoint._getStack = Mock(return_value=None)
        node = Mock()
        node.create_state = {}
        log = Mock()

        sm = self.HeatCreateStateMachine(
            endpoint, node, 'test-stack', 'template: {}',
            {}, {}, log)
        sm.state = sm.STACK_CREATING
        sm.stack_id = 'stack-123'

        with self.assertRaises(Exception) as cm:
            sm.advance()
        self.assertIn('disappeared', str(cm.exception))

    def test_create_advance_complete_extracts_outputs(self):
        """Test create state machine extracts outputs on COMPLETE."""
        endpoint = Mock()
        stack = Mock()
        stack.id = 'stack-123'
        stack.status = 'CREATE_COMPLETE'
        stack.outputs = [
            {'output_key': 'bastion_ip', 'output_value': '192.168.1.1'},
            {'output_key': 'server_id', 'output_value': 'srv-456'}
        ]
        endpoint._getStack = Mock(return_value=stack)
        instance = Mock()
        endpoint._getInstance = Mock(return_value=instance)
        node = Mock()
        node.create_state = {}
        node.quota = {'cores': 2}
        node.node_properties = {}
        log = Mock()

        sm = self.HeatCreateStateMachine(
            endpoint, node, 'test-stack', 'template: {}',
            {}, {}, log)
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
        endpoint = Mock()
        stack = Mock()
        stack.id = 'stack-123'
        stack.status = 'CREATE_COMPLETE'
        stack.outputs = [
            {'output_key': 'bastion_ip', 'output_value': '192.168.1.1'},
        ]
        endpoint._getStack = Mock(return_value=stack)
        instance = Mock()
        endpoint._getInstance = Mock(return_value=instance)
        node = Mock()
        node.create_state = {}
        node.quota = QuotaInformation(stacks=1)
        node.node_properties = {
            'heat_template_content': 'heat_template_version: 2015-04-30',
            'heat_template_path': 'ci/heat/stack.yaml',
        }
        log = Mock()

        sm = self.HeatCreateStateMachine(
            endpoint, node, 'test-stack', 'template: {}',
            {}, {}, log)
        sm.state = sm.COMPLETE
        sm.stack_id = 'stack-123'

        sm.advance()

        self.assertTrue(sm.complete)
        self.assertNotIn('heat_template_content', node.node_properties)
        self.assertIn('heat_template_path', node.node_properties)
        self.assertEqual(node.node_properties['heat_stack_outputs'],
                         {'bastion_ip': '192.168.1.1'})

    def test_delete_advance_start_to_submit(self):
        """Test delete state machine advances from START to SUBMIT."""
        endpoint = Mock()
        node = Mock()
        node.delete_state = {}
        node.heat_stack_id = 'stack-123'
        log = Mock()

        sm = self.HeatDeleteStateMachine(endpoint, node, log)

        self.assertEqual(sm.state, sm.START)
        sm.advance()
        self.assertEqual(sm.state, sm.STACK_DELETING_SUBMIT)

    def test_delete_advance_submit_api_call(self):
        """Test delete state machine submits API call."""
        endpoint = Mock()
        endpoint._submitApi = Mock(return_value='future-123')
        endpoint._completeApi = Mock(return_value=None)
        node = Mock()
        node.delete_state = {}
        node.heat_stack_id = 'stack-123'
        log = Mock()

        sm = self.HeatDeleteStateMachine(endpoint, node, log)
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
        endpoint = Mock()
        endpoint._submitApi = Mock(return_value='future-123')
        endpoint._completeApi = Mock(return_value=True)
        stack = Mock()
        stack.id = 'stack-123'
        stack.status = 'DELETE_IN_PROGRESS'
        endpoint._getStack = Mock(return_value=stack)
        node = Mock()
        node.delete_state = {}
        node.heat_stack_id = 'stack-123'
        log = Mock()

        sm = self.HeatDeleteStateMachine(endpoint, node, log)
        sm.state = sm.STACK_DELETING_SUBMIT

        sm.advance()

        self.assertEqual(sm.state, sm.STACK_DELETING)

    def test_delete_advance_submit_already_deleted(self):
        """Test delete state machine handles already deleted stack."""
        endpoint = Mock()
        endpoint._submitApi = Mock(return_value='future-123')
        endpoint._completeApi = Mock(
            side_effect=openstack.exceptions.NotFoundException())
        node = Mock()
        node.delete_state = {}
        node.heat_stack_id = 'stack-123'
        log = Mock()

        sm = self.HeatDeleteStateMachine(endpoint, node, log)
        sm.state = sm.STACK_DELETING_SUBMIT

        sm.advance()

        self.assertEqual(sm.state, sm.COMPLETE)

    def test_delete_advance_submit_exception(self):
        """Test delete state machine handles deletion exceptions."""
        endpoint = Mock()
        endpoint._submitApi = Mock(return_value='future-123')
        endpoint._completeApi = Mock(
            side_effect=Exception('Network error'))
        node = Mock()
        node.delete_state = {}
        node.heat_stack_id = 'stack-123'
        log = Mock()

        sm = self.HeatDeleteStateMachine(endpoint, node, log)
        sm.state = sm.STACK_DELETING_SUBMIT

        with self.assertRaises(Exception) as cm:
            sm.advance()
        self.assertIn('Network error', str(cm.exception))

    def test_delete_advance_deleting_in_progress(self):
        """Test delete state machine waits for DELETE_IN_PROGRESS."""
        endpoint = Mock()
        stack = Mock()
        stack.id = 'stack-123'
        stack.status = 'DELETE_IN_PROGRESS'
        endpoint._getStack = Mock(return_value=stack)
        node = Mock()
        node.delete_state = {}
        node.heat_stack_id = 'stack-123'
        log = Mock()

        sm = self.HeatDeleteStateMachine(endpoint, node, log)
        sm.state = sm.STACK_DELETING
        sm.stack_id = 'stack-123'

        sm.advance()

        endpoint._getStack.assert_called_once_with('stack-123')
        # Still in DELETING state
        self.assertEqual(sm.state, sm.STACK_DELETING)
        self.assertFalse(sm.complete)

    def test_delete_advance_deleting_stack_disappeared(self):
        """Test delete state machine handles disappeared stack."""
        endpoint = Mock()
        endpoint._getStack = Mock(return_value=None)
        node = Mock()
        node.delete_state = {}
        node.heat_stack_id = 'stack-123'
        log = Mock()

        sm = self.HeatDeleteStateMachine(endpoint, node, log)
        sm.state = sm.STACK_DELETING
        sm.stack_id = 'stack-123'

        sm.advance()

        self.assertEqual(sm.state, sm.COMPLETE)

    def test_delete_advance_deleting_complete(self):
        """Test delete state machine handles DELETE_COMPLETE."""
        endpoint = Mock()
        stack = Mock()
        stack.id = 'stack-123'
        stack.status = 'DELETE_COMPLETE'
        endpoint._getStack = Mock(return_value=stack)
        node = Mock()
        node.delete_state = {}
        node.heat_stack_id = 'stack-123'
        log = Mock()

        sm = self.HeatDeleteStateMachine(endpoint, node, log)
        sm.state = sm.STACK_DELETING
        sm.stack_id = 'stack-123'

        sm.advance()

        self.assertEqual(sm.state, sm.COMPLETE)

    def test_delete_advance_deleting_failed(self):
        """Test delete state machine handles DELETE_FAILED."""
        endpoint = Mock()
        stack = Mock()
        stack.id = 'stack-123'
        stack.status = 'DELETE_FAILED'
        stack.stack_status_reason = 'Resource is locked'
        endpoint._getStack = Mock(return_value=stack)
        node = Mock()
        node.delete_state = {}
        node.heat_stack_id = 'stack-123'
        log = Mock()

        sm = self.HeatDeleteStateMachine(endpoint, node, log)
        sm.state = sm.STACK_DELETING
        sm.stack_id = 'stack-123'

        with self.assertRaises(Exception) as cm:
            sm.advance()
        self.assertIn('Resource is locked', str(cm.exception))

    def test_delete_advance_deleting_unexpected_status_retry(self):
        """Test delete state machine retries on unexpected status."""
        endpoint = Mock()
        stack = Mock()
        stack.id = 'stack-123'
        stack.status = 'UPDATE_COMPLETE'
        endpoint._getStack = Mock(return_value=stack)
        node = Mock()
        node.delete_state = {}
        node.heat_stack_id = 'stack-123'
        log = Mock()

        sm = self.HeatDeleteStateMachine(endpoint, node, log)
        sm.state = sm.STACK_DELETING
        sm.stack_id = 'stack-123'

        sm.advance()

        # Should reset to SUBMIT state for retry
        self.assertEqual(sm.state, sm.STACK_DELETING_SUBMIT)
        self.assertIsNone(sm.delete_future)

    def test_delete_advance_complete(self):
        """Test delete state machine marks complete."""
        endpoint = Mock()
        node = Mock()
        node.delete_state = {}
        node.heat_stack_id = 'stack-123'
        log = Mock()

        sm = self.HeatDeleteStateMachine(endpoint, node, log)
        sm.state = sm.COMPLETE

        sm.advance()

        self.assertTrue(sm.complete)


class TestHeatProviderIntegration(BaseTestCase):
    """Test Heat provider integration and wiring."""

    # A realistic template that declares all zuul_* parameters.
    # The Heat driver only passes parameters that are defined in the
    # template, so tests that assert on specific zuul_* parameters must
    # use a template that declares them.
    FULL_ZUUL_PARAMS_TEMPLATE = """\
heat_template_version: 2015-04-30
parameters:
  zuul_key_name:
    type: string
    default: ''
  zuul_username:
    type: string
    default: ''
  zuul_image:
    type: string
    default: ''
  zuul_external_network:
    type: string
    default: ''
  zuul_ephemeral_public_key:
    type: string
    default: ''
  zuul_ephemeral_private_key:
    type: string
    default: ''
"""

    def setUp(self):
        super().setUp()
        self.driver = HeatDriver()
        self.HeatCreateStateMachine = HeatCreateStateMachine

    def test_provider_has_parse_methods(self):
        """Test that provider has required parse methods."""
        provider = heatprovider.HeatProvider()

        # Verify methods exist (actual behavior tested via schema tests)
        self.assertTrue(hasattr(provider, 'parseImage'))
        self.assertTrue(hasattr(provider, 'parseFlavor'))
        self.assertTrue(hasattr(provider, 'parseLabel'))
        self.assertTrue(callable(provider.parseImage))
        self.assertTrue(callable(provider.parseFlavor))
        self.assertTrue(callable(provider.parseLabel))

    def test_get_quota_for_label(self):
        """Test getQuotaForLabel returns basic stack quota."""
        provider = heatprovider.HeatProvider()
        quota = provider.getQuotaForLabel('test-label')

        # Should return quota with 1 stack
        # QuotaInformation stores values in quota dict
        self.assertEqual(quota.quota.get('stacks'), 1)

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

        template = """\
heat_template_version: 2015-04-30
resources:
  bastion:
    type: OS::Nova::Server
    properties:
      flavor: m1.small
  internal:
    type: OS::Nova::Server
    properties:
      flavor: m1.small
  net:
    type: OS::Neutron::Net
  fip:
    type: OS::Neutron::FloatingIP
"""
        node_properties = {'heat_template_content': template}
        quota = provider.getQuotaForLabel('test-label',
                                          node_properties=node_properties)

        self.assertEqual(quota.quota.get('stacks'), 1)
        self.assertEqual(quota.quota.get('instances'), 2)
        self.assertEqual(quota.quota.get('cores'), 4)
        self.assertEqual(quota.quota.get('ram'), 8192)
        self.assertEqual(quota.quota.get('networks'), 1)
        self.assertEqual(quota.quota.get('floating-ips'), 1)

    def test_get_quota_for_label_no_node_properties(self):
        """Test getQuotaForLabel with empty node_properties falls back."""
        provider = heatprovider.HeatProvider()
        quota = provider.getQuotaForLabel('test-label',
                                          node_properties={})
        self.assertEqual(quota.quota.get('stacks'), 1)
        self.assertIsNone(quota.quota.get('instances'))

    def test_refresh_quota_for_label(self):
        """Test refreshQuotaForLabel."""
        provider = heatprovider.HeatProvider()
        quota = provider.refreshQuotaForLabel('test-label', update=True)

        # Should return same basic quota as getQuotaForLabel
        self.assertEqual(quota.quota.get('stacks'), 1)

    def test_tags_converted_to_list_format(self):
        """Tests dict-to-list conversion of tags for Heat API."""
        label = Mock()
        label.key_name = None
        label.external_network = None
        label.image = 'test-image'
        image = Mock()
        image.username = None

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=Mock(),
            labels={'test-label': label},
            images={'test-image': image},
        )

        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {
            'custom_tag1': 'value1',
            'custom_tag2': 'value2',
        }
        node.node_properties = {
            'heat_template_content': 'template: {}',
        }
        node.create_state = {}

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
        node.node_properties = {
            'heat_template_path': 'ci/heat/stack.yaml',
        }
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
        label = Mock()
        label.key_name = 'my-keypair'
        label.external_network = 'public'
        label.image = 'test-image'
        image = Mock()
        image.username = 'zuul'

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=Mock(),
            labels={'test-label': label},
            images={'test-image': image},
        )

        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_path': 'ci/heat/stack.yaml',
            'heat_template_content': self.FULL_ZUUL_PARAMS_TEMPLATE,
        }
        node.create_state = {}

        sm = provider.getCreateStateMachine(
            node, None, Mock())
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
        label = Mock()
        label.key_name = None
        label.external_network = None
        label.image = 'test-image'
        image = Mock()
        image.username = None
        image.image_id = None

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=Mock(),
            labels={'test-label': label},
            images={'test-image': image},
        )

        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_path': 'ci/heat/stack.yaml',
            'heat_template_content': self.FULL_ZUUL_PARAMS_TEMPLATE,
        }
        node.create_state = {}

        sm = provider.getCreateStateMachine(
            node, None, Mock())
        self.assertNotIn('zuul_key_name', sm.parameters)
        self.assertNotIn('zuul_username', sm.parameters)
        self.assertNotIn('zuul_image', sm.parameters)
        self.assertIn('zuul_ephemeral_public_key', sm.parameters)
        self.assertIn('zuul_ephemeral_private_key', sm.parameters)

    def test_create_state_machine_zuul_image_from_image_id(self):
        """Test zuul_image parameter is set from image.image_id."""
        label = Mock()
        label.key_name = None
        label.external_network = None
        label.image = 'test-image'
        image = Mock()
        image.username = None
        image.image_id = 'abc123-glance-uuid'

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=Mock(),
            labels={'test-label': label},
            images={'test-image': image},
        )

        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_path': 'ci/heat/stack.yaml',
            'heat_template_content': self.FULL_ZUUL_PARAMS_TEMPLATE,
        }
        node.create_state = {}

        sm = provider.getCreateStateMachine(
            node, None, Mock())
        self.assertEqual(sm.parameters['zuul_image'], 'abc123-glance-uuid')

    def test_create_state_machine_zuul_image_from_external_id(self):
        """Test zuul_image uses image_external_id for Zuul-managed images."""
        label = Mock()
        label.key_name = None
        label.external_network = None
        label.image = 'test-image'
        image = Mock()
        image.username = None
        image.image_id = None

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=Mock(),
            labels={'test-label': label},
            images={'test-image': image},
        )

        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_path': 'ci/heat/stack.yaml',
            'heat_template_content': self.FULL_ZUUL_PARAMS_TEMPLATE,
        }
        node.create_state = {}

        sm = provider.getCreateStateMachine(
            node, 'zuul-managed-image-456', Mock())
        self.assertEqual(
            sm.parameters['zuul_image'], 'zuul-managed-image-456')

    def test_create_state_machine_external_id_overrides_image_id(self):
        """Test image_external_id takes precedence over image.image_id."""
        label = Mock()
        label.key_name = None
        label.external_network = None
        label.image = 'test-image'
        image = Mock()
        image.username = None
        image.image_id = 'cloud-image-id'

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=Mock(),
            labels={'test-label': label},
            images={'test-image': image},
        )

        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_path': 'ci/heat/stack.yaml',
            'heat_template_content': self.FULL_ZUUL_PARAMS_TEMPLATE,
        }
        node.create_state = {}

        sm = provider.getCreateStateMachine(
            node, 'zuul-managed-image-789', Mock())
        self.assertEqual(
            sm.parameters['zuul_image'], 'zuul-managed-image-789')

    def test_create_state_machine_all_parameters(self):
        """Test all zuul parameters are set together."""
        label = Mock()
        label.key_name = 'my-keypair'
        label.external_network = 'public'
        label.image = 'test-image'
        image = Mock()
        image.username = 'zuul'
        image.image_id = 'bastion-image-id'

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=Mock(),
            labels={'test-label': label},
            images={'test-image': image},
        )

        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_path': 'ci/heat/stack.yaml',
            'heat_template_content': self.FULL_ZUUL_PARAMS_TEMPLATE,
        }
        node.create_state = {}

        sm = provider.getCreateStateMachine(
            node, None, Mock())
        self.assertEqual(sm.parameters['zuul_key_name'], 'my-keypair')
        self.assertEqual(sm.parameters['zuul_username'], 'zuul')
        self.assertEqual(sm.parameters['zuul_image'], 'bastion-image-id')
        self.assertEqual(
            sm.parameters['zuul_external_network'], 'public')
        self.assertIn('zuul_ephemeral_public_key', sm.parameters)
        self.assertIn('zuul_ephemeral_private_key', sm.parameters)

    def test_create_state_machine_user_heat_parameters(self):
        """Test user-defined heat_parameters from node-properties."""
        label = Mock()
        label.key_name = 'my-keypair'
        label.external_network = 'public'
        label.image = 'test-image'
        image = Mock()
        image.username = 'zuul'
        image.image_id = None

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=Mock(),
            labels={'test-label': label},
            images={'test-image': image},
        )

        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_path': 'ci/heat/stack.yaml',
            'heat_template_content': self.FULL_ZUUL_PARAMS_TEMPLATE,
            'heat_parameters': {
                'external_network': 'public',
                'dns_nameserver': '8.8.8.8',
            },
        }
        node.create_state = {}

        sm = provider.getCreateStateMachine(
            node, None, Mock())
        self.assertEqual(sm.parameters['external_network'], 'public')
        self.assertEqual(sm.parameters['dns_nameserver'], '8.8.8.8')
        self.assertEqual(sm.parameters['zuul_key_name'], 'my-keypair')
        self.assertEqual(sm.parameters['zuul_username'], 'zuul')
        self.assertIn('zuul_ephemeral_public_key', sm.parameters)
        self.assertIn('zuul_ephemeral_private_key', sm.parameters)

    def test_create_state_machine_zuul_params_override_user_params(self):
        """Test Zuul-internal parameters take precedence over user ones."""
        label = Mock()
        label.key_name = 'zuul-keypair'
        label.external_network = 'public'
        label.image = 'test-image'
        image = Mock()
        image.username = 'zuul'
        image.image_id = 'real-image-id'

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=Mock(),
            labels={'test-label': label},
            images={'test-image': image},
        )

        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_path': 'ci/heat/stack.yaml',
            'heat_template_content': self.FULL_ZUUL_PARAMS_TEMPLATE,
            'heat_parameters': {
                'zuul_key_name': 'user-attempt-to-override',
                'zuul_image': 'user-attempt-to-override',
                'custom_param': 'custom_value',
            },
        }
        node.create_state = {}

        sm = provider.getCreateStateMachine(
            node, None, Mock())
        self.assertEqual(sm.parameters['zuul_key_name'], 'zuul-keypair')
        self.assertEqual(sm.parameters['zuul_image'], 'real-image-id')
        self.assertEqual(sm.parameters['custom_param'], 'custom_value')

    def test_create_state_machine_no_user_heat_parameters(self):
        """Test behavior is unchanged when heat_parameters is absent."""
        label = Mock()
        label.key_name = 'my-keypair'
        label.external_network = 'public'
        label.image = 'test-image'
        image = Mock()
        image.username = 'zuul'
        image.image_id = None

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=Mock(),
            labels={'test-label': label},
            images={'test-image': image},
        )

        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_path': 'ci/heat/stack.yaml',
            'heat_template_content': self.FULL_ZUUL_PARAMS_TEMPLATE,
        }
        node.create_state = {}

        sm = provider.getCreateStateMachine(
            node, None, Mock())
        self.assertEqual(sm.parameters['zuul_key_name'], 'my-keypair')
        self.assertEqual(sm.parameters['zuul_username'], 'zuul')
        self.assertIn('zuul_ephemeral_public_key', sm.parameters)
        self.assertIn('zuul_ephemeral_private_key', sm.parameters)

    def test_external_network_from_label(self):
        """Test zuul_external_network is set from label config."""
        label = Mock()
        label.key_name = None
        label.external_network = 'ext-net'
        label.image = 'test-image'
        image = Mock()
        image.username = None
        image.image_id = None

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=Mock(),
            labels={'test-label': label},
            images={'test-image': image},
        )

        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_path': 'ci/heat/stack.yaml',
            'heat_template_content': self.FULL_ZUUL_PARAMS_TEMPLATE,
        }
        node.create_state = {}

        sm = provider.getCreateStateMachine(
            node, None, Mock())
        self.assertEqual(
            sm.parameters['zuul_external_network'], 'ext-net')
        provider._endpoint._getExternalNetwork.assert_not_called()

    def test_external_network_auto_discovered(self):
        """Test zuul_external_network is auto-discovered from Neutron."""
        label = Mock()
        label.key_name = None
        label.external_network = None
        label.image = 'test-image'
        image = Mock()
        image.username = None
        image.image_id = None

        endpoint = Mock()
        endpoint._getExternalNetwork.return_value = 'discovered-ext-net'

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=endpoint,
            labels={'test-label': label},
            images={'test-image': image},
        )

        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_path': 'ci/heat/stack.yaml',
            'heat_template_content': self.FULL_ZUUL_PARAMS_TEMPLATE,
        }
        node.create_state = {}

        sm = provider.getCreateStateMachine(
            node, None, Mock())
        self.assertEqual(
            sm.parameters['zuul_external_network'], 'discovered-ext-net')
        endpoint._getExternalNetwork.assert_called_once()

    def test_external_network_overrides_user_param(self):
        """Test zuul_external_network overrides user heat_parameters."""
        label = Mock()
        label.key_name = None
        label.external_network = 'operator-ext-net'
        label.image = 'test-image'
        image = Mock()
        image.username = None
        image.image_id = None

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=Mock(),
            labels={'test-label': label},
            images={'test-image': image},
        )

        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_path': 'ci/heat/stack.yaml',
            'heat_template_content': self.FULL_ZUUL_PARAMS_TEMPLATE,
            'heat_parameters': {
                'zuul_external_network': 'user-attempt-override',
            },
        }
        node.create_state = {}

        sm = provider.getCreateStateMachine(
            node, None, Mock())
        self.assertEqual(
            sm.parameters['zuul_external_network'], 'operator-ext-net')

    def test_zuul_params_not_passed_when_not_in_template(self):
        """Test zuul_* params are not sent to Heat when template lacks them.

        Heat rejects stack creation with 400 if parameters are supplied
        that are not declared in the template.  The driver must only pass
        zuul_* parameters that the template actually defines.
        """
        label = Mock()
        label.key_name = 'my-keypair'
        label.external_network = 'public'
        label.image = 'test-image'
        image = Mock()
        image.username = 'zuul'
        image.image_id = 'my-image-id'

        endpoint = Mock()

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=endpoint,
            labels={'test-label': label},
            images={'test-image': image},
        )

        # Minimal template with no parameters section at all
        template = 'heat_template_version: 2015-04-30\n'
        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_path': 'ci/heat/stack.yaml',
            'heat_template_content': template,
        }
        node.create_state = {}

        sm = provider.getCreateStateMachine(node, None, Mock())

        # No zuul_* params should be passed since template declares none
        self.assertNotIn('zuul_key_name', sm.parameters)
        self.assertNotIn('zuul_username', sm.parameters)
        self.assertNotIn('zuul_image', sm.parameters)
        self.assertNotIn('zuul_external_network', sm.parameters)
        self.assertNotIn('zuul_ephemeral_public_key', sm.parameters)
        self.assertNotIn('zuul_ephemeral_private_key', sm.parameters)
        # _getExternalNetwork must not be called when template has no param
        endpoint._getExternalNetwork.assert_not_called()

    def test_create_state_machine_resource_policy_allowed_pass(self):
        """Test getCreateStateMachine passes with compliant template."""
        label = Mock()
        label.key_name = None
        label.external_network = 'public'
        label.image = 'test-image'
        image = Mock()
        image.username = None
        image.image_id = None

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=Mock(),
            labels={'test-label': label},
            images={'test-image': image},
            config={
                'resource_policy': {
                    'allowed_resource_types': [
                        'OS::Nova::Server',
                        'OS::Neutron::Net',
                    ],
                },
            },
        )

        template = """\
heat_template_version: 2015-04-30
resources:
  bastion:
    type: OS::Nova::Server
    properties:
      flavor: m1.small
  net:
    type: OS::Neutron::Net
"""
        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_content': template,
        }
        node.create_state = {}

        sm = provider.getCreateStateMachine(node, None, Mock())
        self.assertIsNotNone(sm)

    def test_create_state_machine_resource_policy_allowed_reject(self):
        """Test getCreateStateMachine rejects template with disallowed type."""
        from zuul.driver.heat.heatparser import ResourcePolicyViolation

        label = Mock()
        label.key_name = None
        label.external_network = 'public'
        label.image = 'test-image'
        image = Mock()
        image.username = None
        image.image_id = None

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=Mock(),
            labels={'test-label': label},
            images={'test-image': image},
            config={
                'resource_policy': {
                    'allowed_resource_types': [
                        'OS::Nova::Server',
                        'OS::Neutron::Net',
                    ],
                },
            },
        )

        template = """\
heat_template_version: 2015-04-30
resources:
  bastion:
    type: OS::Nova::Server
    properties:
      flavor: m1.small
  rg:
    type: OS::Heat::ResourceGroup
"""
        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_content': template,
        }
        node.create_state = {}

        with self.assertRaises(ResourcePolicyViolation) as cm:
            provider.getCreateStateMachine(node, None, Mock())
        self.assertIn('OS::Heat::ResourceGroup', str(cm.exception))

    def test_create_state_machine_resource_policy_disallowed_reject(self):
        """Test getCreateStateMachine rejects blacklisted resource type."""
        from zuul.driver.heat.heatparser import ResourcePolicyViolation

        label = Mock()
        label.key_name = None
        label.external_network = 'public'
        label.image = 'test-image'
        image = Mock()
        image.username = None
        image.image_id = None

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=Mock(),
            labels={'test-label': label},
            images={'test-image': image},
            config={
                'resource_policy': {
                    'disallowed_resource_types': [
                        'OS::Heat::ResourceGroup',
                        'OS::Neutron::FloatingIP',
                    ],
                },
            },
        )

        template = """\
heat_template_version: 2015-04-30
resources:
  bastion:
    type: OS::Nova::Server
    properties:
      flavor: m1.small
  fip:
    type: OS::Neutron::FloatingIP
"""
        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_content': template,
        }
        node.create_state = {}

        with self.assertRaises(ResourcePolicyViolation) as cm:
            provider.getCreateStateMachine(node, None, Mock())
        self.assertIn('OS::Neutron::FloatingIP', str(cm.exception))

    def test_create_state_machine_resource_policy_disallowed_pass(self):
        """Test getCreateStateMachine passes when no blacklisted types."""
        label = Mock()
        label.key_name = None
        label.external_network = 'public'
        label.image = 'test-image'
        image = Mock()
        image.username = None
        image.image_id = None

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=Mock(),
            labels={'test-label': label},
            images={'test-image': image},
            config={
                'resource_policy': {
                    'disallowed_resource_types': [
                        'OS::Heat::ResourceGroup',
                    ],
                },
            },
        )

        template = """\
heat_template_version: 2015-04-30
resources:
  bastion:
    type: OS::Nova::Server
    properties:
      flavor: m1.small
  net:
    type: OS::Neutron::Net
"""
        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_content': template,
        }
        node.create_state = {}

        sm = provider.getCreateStateMachine(node, None, Mock())
        self.assertIsNotNone(sm)

    def test_create_state_machine_empty_resource_policy_skips(self):
        """Test getCreateStateMachine skips validation with empty policy."""
        label = Mock()
        label.key_name = None
        label.external_network = 'public'
        label.image = 'test-image'
        image = Mock()
        image.username = None
        image.image_id = None

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=Mock(),
            labels={'test-label': label},
            images={'test-image': image},
            config={
                'resource_policy': {},
            },
        )

        template = """\
heat_template_version: 2015-04-30
resources:
  rg:
    type: OS::Heat::ResourceGroup
"""
        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_content': template,
        }
        node.create_state = {}

        sm = provider.getCreateStateMachine(node, None, Mock())
        self.assertIsNotNone(sm)

    def test_create_state_machine_flavor_limits_pass(self):
        """Test getCreateStateMachine passes when within flavor limits."""
        label = Mock()
        label.key_name = None
        label.external_network = 'public'
        label.image = 'test-image'
        label.flavor = 'test-flavor'
        image = Mock()
        image.username = None
        image.image_id = None

        flavor = Mock()
        flavor.max_instances = 5
        flavor.max_cores = 20
        flavor.max_ram = 40960
        flavor.max_networks = 3
        flavor.max_floating_ips = 2

        endpoint = Mock()
        endpoint._findFlavor.return_value = Mock(vcpus=2, ram=4096)

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=endpoint,
            labels={'test-label': label},
            images={'test-image': image},
            flavors={'test-flavor': flavor},
            config={},
        )

        template = """\
heat_template_version: 2015-04-30
resources:
  bastion:
    type: OS::Nova::Server
    properties:
      flavor: m1.small
  worker1:
    type: OS::Nova::Server
    properties:
      flavor: m1.small
  net:
    type: OS::Neutron::Net
"""
        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_content': template,
        }
        node.create_state = {}

        sm = provider.getCreateStateMachine(node, None, Mock())
        self.assertIsNotNone(sm)

    def test_create_state_machine_flavor_limits_exceeded_instances(self):
        """Test getCreateStateMachine rejects when instances exceed limit."""
        from zuul.driver.heat.heatparser import FlavorLimitExceeded

        label = Mock()
        label.key_name = None
        label.external_network = 'public'
        label.image = 'test-image'
        label.flavor = 'test-flavor'
        image = Mock()
        image.username = None
        image.image_id = None

        flavor = Mock()
        flavor.max_instances = 2
        flavor.max_cores = 100
        flavor.max_ram = 102400

        endpoint = Mock()
        endpoint._findFlavor.return_value = Mock(vcpus=2, ram=4096)

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=endpoint,
            labels={'test-label': label},
            images={'test-image': image},
            flavors={'test-flavor': flavor},
            config={},
        )

        template = """\
heat_template_version: 2015-04-30
resources:
  bastion:
    type: OS::Nova::Server
    properties:
      flavor: m1.small
  worker1:
    type: OS::Nova::Server
    properties:
      flavor: m1.small
  worker2:
    type: OS::Nova::Server
    properties:
      flavor: m1.small
"""
        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_content': template,
        }
        node.create_state = {}

        with self.assertRaises(FlavorLimitExceeded) as cm:
            provider.getCreateStateMachine(node, None, Mock())
        self.assertIn('instances: 3 > 2', str(cm.exception))

    def test_create_state_machine_flavor_limits_exceeded_cores(self):
        """Test getCreateStateMachine rejects when cores exceed limit."""
        from zuul.driver.heat.heatparser import FlavorLimitExceeded

        label = Mock()
        label.key_name = None
        label.external_network = 'public'
        label.image = 'test-image'
        label.flavor = 'test-flavor'
        image = Mock()
        image.username = None
        image.image_id = None

        flavor = Mock()
        flavor.max_instances = 10
        flavor.max_cores = 4
        flavor.max_ram = 102400

        endpoint = Mock()
        endpoint._findFlavor.return_value = Mock(vcpus=4, ram=8192)

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=endpoint,
            labels={'test-label': label},
            images={'test-image': image},
            flavors={'test-flavor': flavor},
            config={},
        )

        template = """\
heat_template_version: 2015-04-30
resources:
  bastion:
    type: OS::Nova::Server
    properties:
      flavor: m1.xlarge
  worker1:
    type: OS::Nova::Server
    properties:
      flavor: m1.xlarge
"""
        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_content': template,
        }
        node.create_state = {}

        with self.assertRaises(FlavorLimitExceeded) as cm:
            provider.getCreateStateMachine(node, None, Mock())
        self.assertIn('cores: 8 > 4', str(cm.exception))

    def test_create_state_machine_flavor_limits_exceeded_floating_ips(self):
        """Test getCreateStateMachine rejects when floating IPs exceed."""
        from zuul.driver.heat.heatparser import FlavorLimitExceeded

        label = Mock()
        label.key_name = None
        label.external_network = 'public'
        label.image = 'test-image'
        label.flavor = 'test-flavor'
        image = Mock()
        image.username = None
        image.image_id = None

        flavor = Mock()
        flavor.max_instances = 10
        flavor.max_cores = 100
        flavor.max_ram = 102400
        flavor.max_floating_ips = 1

        endpoint = Mock()
        endpoint._findFlavor.return_value = Mock(vcpus=1, ram=1024)

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=endpoint,
            labels={'test-label': label},
            images={'test-image': image},
            flavors={'test-flavor': flavor},
            config={},
        )

        template = """\
heat_template_version: 2015-04-30
resources:
  bastion:
    type: OS::Nova::Server
    properties:
      flavor: m1.small
  fip1:
    type: OS::Neutron::FloatingIP
  fip2:
    type: OS::Neutron::FloatingIP
"""
        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_content': template,
        }
        node.create_state = {}

        with self.assertRaises(FlavorLimitExceeded) as cm:
            provider.getCreateStateMachine(node, None, Mock())
        self.assertIn('floating-ips: 2 > 1', str(cm.exception))

    def test_create_state_machine_flavor_limits_multiple_violations(self):
        """Test FlavorLimitExceeded reports all violations together."""
        from zuul.driver.heat.heatparser import FlavorLimitExceeded

        label = Mock()
        label.key_name = None
        label.external_network = 'public'
        label.image = 'test-image'
        label.flavor = 'test-flavor'
        image = Mock()
        image.username = None
        image.image_id = None

        flavor = Mock()
        flavor.max_instances = 1
        flavor.max_cores = 2
        flavor.max_ram = 2048

        endpoint = Mock()
        endpoint._findFlavor.return_value = Mock(vcpus=4, ram=8192)

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=endpoint,
            labels={'test-label': label},
            images={'test-image': image},
            flavors={'test-flavor': flavor},
            config={},
        )

        template = """\
heat_template_version: 2015-04-30
resources:
  bastion:
    type: OS::Nova::Server
    properties:
      flavor: m1.xlarge
  worker:
    type: OS::Nova::Server
    properties:
      flavor: m1.xlarge
"""
        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_content': template,
        }
        node.create_state = {}

        with self.assertRaises(FlavorLimitExceeded) as cm:
            provider.getCreateStateMachine(node, None, Mock())
        error_msg = str(cm.exception)
        self.assertIn('instances: 2 > 1', error_msg)
        self.assertIn('cores: 8 > 2', error_msg)
        self.assertIn('ram: 16384 > 2048', error_msg)

    def test_create_state_machine_flavor_limits_undefined_skipped(self):
        """Test undefined flavor limits are not enforced."""
        import voluptuous as vs

        label = Mock()
        label.key_name = None
        label.external_network = 'public'
        label.image = 'test-image'
        label.flavor = 'test-flavor'
        image = Mock()
        image.username = None
        image.image_id = None

        flavor = Mock()
        flavor.max_instances = vs.UNDEFINED
        flavor.max_cores = vs.UNDEFINED
        flavor.max_ram = vs.UNDEFINED
        flavor.max_volumes = vs.UNDEFINED
        flavor.max_volume_gb = vs.UNDEFINED
        flavor.max_networks = vs.UNDEFINED
        flavor.max_routers = vs.UNDEFINED
        flavor.max_routers_with_external_gw = vs.UNDEFINED
        flavor.max_floating_ips = vs.UNDEFINED

        endpoint = Mock()
        endpoint._findFlavor.return_value = Mock(vcpus=4, ram=8192)

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=endpoint,
            labels={'test-label': label},
            images={'test-image': image},
            flavors={'test-flavor': flavor},
            config={},
        )

        template = """\
heat_template_version: 2015-04-30
resources:
  bastion:
    type: OS::Nova::Server
    properties:
      flavor: m1.xlarge
  worker1:
    type: OS::Nova::Server
    properties:
      flavor: m1.xlarge
  worker2:
    type: OS::Nova::Server
    properties:
      flavor: m1.xlarge
  worker3:
    type: OS::Nova::Server
    properties:
      flavor: m1.xlarge
"""
        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_content': template,
        }
        node.create_state = {}

        sm = provider.getCreateStateMachine(node, None, Mock())
        self.assertIsNotNone(sm)

    def test_create_state_machine_flavor_limits_no_flavor_skips(self):
        """Test validation is skipped when no flavor is configured."""
        label = Mock()
        label.key_name = None
        label.external_network = 'public'
        label.image = 'test-image'
        label.flavor = None
        image = Mock()
        image.username = None
        image.image_id = None

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=Mock(),
            labels={'test-label': label},
            images={'test-image': image},
            flavors={},
            config={},
        )

        template = """\
heat_template_version: 2015-04-30
resources:
  bastion:
    type: OS::Nova::Server
    properties:
      flavor: m1.xlarge
"""
        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_content': template,
        }
        node.create_state = {}

        sm = provider.getCreateStateMachine(node, None, Mock())
        self.assertIsNotNone(sm)

    def test_create_state_machine_flavor_limits_volumes(self):
        """Test flavor limit enforcement for volumes."""
        from zuul.driver.heat.heatparser import FlavorLimitExceeded

        label = Mock()
        label.key_name = None
        label.external_network = 'public'
        label.image = 'test-image'
        label.flavor = 'test-flavor'
        image = Mock()
        image.username = None
        image.image_id = None

        flavor = Mock()
        flavor.max_instances = 10
        flavor.max_cores = 100
        flavor.max_ram = 102400
        flavor.max_volumes = 2
        flavor.max_volume_gb = 50

        endpoint = Mock()
        endpoint._findFlavor.return_value = Mock(vcpus=1, ram=1024)

        provider = heatprovider.HeatProvider()
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=endpoint,
            labels={'test-label': label},
            images={'test-image': image},
            flavors={'test-flavor': flavor},
            config={},
        )

        template = """\
heat_template_version: 2015-04-30
resources:
  bastion:
    type: OS::Nova::Server
    properties:
      flavor: m1.small
  vol1:
    type: OS::Cinder::Volume
    properties:
      size: 20
  vol2:
    type: OS::Cinder::Volume
    properties:
      size: 20
  vol3:
    type: OS::Cinder::Volume
    properties:
      size: 20
"""
        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.label = 'test-label'
        node.tags = {}
        node.node_properties = {
            'heat_template_content': template,
        }
        node.create_state = {}

        with self.assertRaises(FlavorLimitExceeded) as cm:
            provider.getCreateStateMachine(node, None, Mock())
        error_msg = str(cm.exception)
        self.assertIn('volumes: 3 > 2', error_msg)
        self.assertIn('volume-gb: 60 > 50', error_msg)
