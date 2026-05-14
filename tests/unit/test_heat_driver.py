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
        self.assertIn('heat', registry.drivers)
        driver = registry.drivers['heat']
        self.assertEqual(driver.name, 'heat')

    def test_driver_node_class(self):
        """Test that the Heat driver returns the correct node class."""
        registry = ConnectionRegistry()
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
        self.assertIsNone(node.heat_outputs)

    def test_heat_provider_node_driver_data(self):
        """Test HeatProviderNode getDriverData serialization."""
        node = HeatProviderNode()
        node._set(
            heat_stack_id='test-stack-id-123',
            heat_stack_name='zuul-test-stack',
            heat_stack_status='CREATE_COMPLETE',
            heat_template_path='.zuul/templates/test.yaml',
            heat_outputs={'bastion_ip': '192.0.2.100'}
        )

        data = node.getDriverData()
        self.assertEqual(data['heat_stack_id'], 'test-stack-id-123')
        self.assertEqual(data['heat_stack_name'], 'zuul-test-stack')
        self.assertEqual(data['heat_stack_status'], 'CREATE_COMPLETE')
        self.assertEqual(data['heat_template_path'],
                         '.zuul/templates/test.yaml')
        self.assertEqual(data['heat_outputs'],
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
        self.assertEqual(instance.heat_outputs['internal_ips'],
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

    def test_image_schema(self):
        """Test Heat image schema validation."""
        schema_class = self.driver.getProviderSchemaClass()
        schema_instance = schema_class()
        image_schema = schema_instance.getImageSchema(internal=True)

        # Test cloud image
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

    def test_get_instance_missing_bastion_ip(self):
        """Test that missing bastion IP raises ValueError."""
        connection = Mock()
        connection.connection_name = 'test-cloud'
        connection.rate = 2
        connection.cloud_name = 'test'
        connection.config_files = None
        zk_client = self._create_mock_zk_client()

        endpoint = self.driver._getEndpoint(
            zk_client, connection, None, 'test-system')

        # Mock stack object
        stack = Mock()
        stack.id = 'stack-123'

        # Create quota
        quota = QuotaInformation(stacks=1)

        # Empty outputs - missing bastion_ip
        outputs = {}

        # Should raise ValueError with helpful message
        with self.assertRaises(ValueError) as cm:
            endpoint._getInstance(stack, quota, outputs)

        # Check error message mentions the required output and provides context
        self.assertIn('bastion_ip', str(cm.exception))
        self.assertIn('floating IP address', str(cm.exception))
        self.assertIn('stack-123', str(cm.exception))

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
        endpoint._getInstance = Mock(return_value='instance-obj')
        node = Mock()
        node.create_state = {}
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
        endpoint._getInstance = Mock(return_value='instance-obj')
        node = Mock()
        node.create_state = {}
        node.quota = {'cores': 2}
        log = Mock()

        sm = self.HeatCreateStateMachine(
            endpoint, node, 'test-stack', 'template: {}',
            {}, {}, log)
        sm.state = sm.COMPLETE
        sm.stack_id = 'stack-123'

        result = sm.advance()

        self.assertEqual(node.heat_outputs, {
            'bastion_ip': '192.168.1.1',
            'server_id': 'srv-456'
        })
        self.assertEqual(node.heat_stack_status, 'CREATE_COMPLETE')
        self.assertTrue(sm.complete)
        self.assertEqual(result, 'instance-obj')
        endpoint._getInstance.assert_called_once_with(
            stack, {'cores': 2}, {
                'bastion_ip': '192.168.1.1',
                'server_id': 'srv-456'
            })

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

    def test_refresh_quota_for_label(self):
        """Test refreshQuotaForLabel."""
        provider = heatprovider.HeatProvider()
        quota = provider.refreshQuotaForLabel('test-label', update=True)

        # Should return same basic quota as getQuotaForLabel
        self.assertEqual(quota.quota.get('stacks'), 1)

    def test_tags_converted_to_list_format(self):
        """Tests dict-to-list conversion of tags for Heat API."""
        provider = heatprovider.HeatProvider()
        # getCreateStateMachine passes self.endpoint; avoid lazy getEndpoint()
        # (needs driver/connection). A mock endpoint is enough for this test.
        provider._set(
            canonical_name='example.com/project',
            system_id='test-system',
            _endpoint=Mock(),
        )

        # Create a mock node with tags
        node = Mock()
        node.uuid = 'test-node-uuid-123'
        node.tags = {'custom_tag1': 'value1', 'custom_tag2': 'value2'}
        node.create_state = {}

        # Get the state machine
        sm = provider.getCreateStateMachine(
            node, 'template: {}', Mock())

        # Verify tags are converted to list of "key=value" strings
        self.assertIsInstance(sm.tags, list)
        # 2 custom + zuul_node_uuid + zuul_system_id
        self.assertEqual(len(sm.tags), 4)

        # Check that all required tags are present in correct format
        tags_set = set(sm.tags)
        self.assertIn('custom_tag1=value1', tags_set)
        self.assertIn('custom_tag2=value2', tags_set)
        self.assertIn('zuul_node_uuid=test-node-uuid-123', tags_set)
        self.assertIn('zuul_system_id=test-system', tags_set)
