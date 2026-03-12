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
import tempfile
import time
from unittest import mock

from zuul.driver.azure.azuremodel import AzureProviderNode
import zuul.driver.azure.azureendpoint

from tests import fake_azure
from tests.base import (
    ResponsesFixture,
    ZuulTestCase,
    driver_config,
    iterate_timeout,
    return_data,
    simple_layout,
)
from tests.unit.test_launcher import ImageMocksFixture
from tests.unit.test_cloud_driver import BaseCloudDriverTest


def make_image(name, tags):
    return {
        'name': name,
        'id': ('/subscriptions/c35cf7df-ed75-4c85-be00-535409a85120/'
               'resourceGroups/nodepool/providers/Microsoft.Compute/'
               f'images/{name}'),
        'type': 'Microsoft.Compute/images',
        'location': 'eastus',
        'tags': tags,
        'properties': {
            'storageProfile': {
                'osDisk': {
                    'osType': 'Linux',
                    'osState': 'Generalized',
                    'diskSizeGB': 1,
                    'blobUri': 'https://example.net/nodepoolstorage/img.vhd',
                    'caching': 'ReadWrite',
                    'storageAccountType': 'Standard_LRS'
                },
                'dataDisks': [],
                'zoneResilient': False
            },
            'provisioningState': 'Succeeded',
            'hyperVGeneration': 'V1'
        }
    }


class BaseAzureDriverTest(ZuulTestCase):
    cloud_test_image_format = 'vhd'
    cloud_test_provider_name = 'azure-main'
    config_file = 'zuul-connections-nodepool.conf'
    debian_return_data = {
        'zuul': {
            'artifacts': [
                {
                    'name': 'vhd image',
                    'url': 'http://example.com/image.vhd',
                    'metadata': {
                        'type': 'zuul_image',
                        'image_name': 'debian-local',
                        'format': 'vhd',
                        'sha256': ImageMocksFixture.vhd_sha256,
                        'md5sum': ImageMocksFixture.vhd_md5sum,
                    }
                },
            ]
        }
    }

    default_quotas = {
        'lowPriorityCores': 3,
        'cores': 4,
        'virtualMachines': 25000,
    }

    def setUp(self):
        self.initTestConfig()
        self.patch(zuul.driver.azure.azureendpoint, 'CACHE_TTL', 1)
        responses_fixture = self.useFixture(ResponsesFixture())
        self.useFixture(ImageMocksFixture(responses_fixture))

        quotas = self.default_quotas.copy()
        quotas.update(self.test_config.driver.azure.get('quotas', {}))

        self.fake_azure = fake_azure.FakeAzureFixture(
            responses_fixture, quotas)
        self.useFixture(self.fake_azure)

        super().setUp()

    @contextlib.contextmanager
    def _block_futures(self):
        with (mock.patch(
                'zuul.driver.azure.azureendpoint.'
                'AzureProviderEndpoint._completeApi', return_value=None)):
            yield


class TestAzureDriver(BaseAzureDriverTest, BaseCloudDriverTest):
    def _assertProviderNodeAttributes(self, pnode):
        super()._assertProviderNodeAttributes(pnode)
        self.assertEqual('Azure', pnode.cloud)
        self.assertEqual('centralus', pnode.region)
        self.assertEqual('1', pnode.az)
        if checks := self.test_config.driver.azure.get('node_checks'):
            checks(self, pnode)

    @simple_layout('layouts/azure/nodepool.yaml', enable_nodepool=True)
    def test_azure_node_lifecycle(self):
        self._test_node_lifecycle('debian-normal')

    def check_more_attrs(self, pnode):
        pass

    @simple_layout('layouts/azure/more.yaml', enable_nodepool=True)
    @driver_config('azure', node_checks=check_more_attrs)
    def test_azure_node_lifecycle_more(self):
        # Test with more options than the normal test
        # windows-generate
        # ephemeral disk
        self._test_node_lifecycle('debian-normal')
        vm_request = (
            self.fake_azure.crud['Microsoft.Compute/virtualMachines'].
            requests[0])
        osDisk = vm_request['properties']['storageProfile']['osDisk']
        self.assertIn('caching', osDisk)
        self.assertEqual(osDisk['caching'], 'ReadOnly')
        self.assertIn('diffDiskSettings', osDisk)
        self.assertIn('option', osDisk['diffDiskSettings'])
        self.assertEqual(osDisk['diffDiskSettings']['option'], 'Local')

        self.assertEqual(
            vm_request['properties']['osProfile']['adminUsername'],
            'foobar')
        self.assertEqual(
            len(vm_request['properties']['osProfile']['adminPassword']),
            64)
        self.assertEqual(
            len(vm_request['identity']['userAssignedIdentities']),
            1)
        uai = vm_request['identity']['userAssignedIdentities']
        self.assertIn('/subscriptions/582c3efd-3e57-4073-82c5-d122636fb45c'
                      '/resourceGroups/id-group'
                      '/providers/Microsoft.ManagedIdentity'
                      '/userAssignedIdentities/id-name', uai)
        self.assertEqual(
            len(vm_request['identity']['userAssignedIdentities']),
            1)
        self.assertEqual(
            '/subscriptions/582c3efd-3e57-4073-82c5-d122636fb45c'
            '/resourceGroups/not-nodepool'
            '/providers/Microsoft.Network'
            '/virtualNetworks/not-nodepool'
            '/subnets/default',
            vm_request['properties']['networkProfile']
            ['networkInterfaceConfigurations'][0]['properties']
            ['ipConfigurations'][0]['properties']['subnet']['id'])

    @simple_layout('layouts/azure/nodepool.yaml', enable_nodepool=True)
    @driver_config('azure', quotas={'cores': 1})
    def test_azure_quota(self):
        self._test_quota('debian-normal')

    @simple_layout('layouts/azure/nodepool-spot.yaml', enable_nodepool=True)
    @driver_config('azure', quotas={'lowPriorityCores': 1})
    def test_azure_quota_spot(self):
        self._test_quota('debian-normal')

    @simple_layout('layouts/azure/resource-limits.yaml',
                   enable_nodepool=True)
    def test_azure_resource_limits(self):
        self._test_quota('debian-normal')

    @simple_layout('layouts/azure/nodepool-image.yaml',
                   enable_nodepool=True)
    @return_data(
        'build-debian-local-image',
        'refs/heads/master',
        BaseAzureDriverTest.debian_return_data,
    )
    def test_azure_diskimage(self):
        self._test_diskimage()

    @simple_layout('layouts/azure/nodepool.yaml', enable_nodepool=True)
    def test_state_machines(self):
        label_name = "debian-normal"
        provider_name = "azure-main"
        node_class = AzureProviderNode
        future_names = ['delete_future', 'create_future']
        self._test_state_machines(label_name, provider_name,
                                  node_class, future_names)

    # Azure-driver specific tests

    @simple_layout('layouts/azure/nodepool.yaml',
                   enable_nodepool=True)
    def test_azure_resource_cleanup(self):
        self.waitUntilSettled()
        self.launcher.cleanup_worker.INTERVAL = 1
        provider = self.launcher._getProvider(
            'tenant-one', 'azure-main')
        endpoint = provider.getEndpoint()
        client = endpoint._client

        system_id = self.launcher.system.system_id
        tags = {
            'zuul_system_id': system_id,
            'zuul_node_uuid': '0000000042',
        }

        subnet_id = ('/subscriptions/c35cf7df-ed75-4c85-be00-535409a85120/'
                     'resourceGroups/nodepool/providers/Microsoft.Network/'
                     'virtualNetworks/NodePool/subnets/default')
        pip_spec = {
            'name': 'npf2d7ff7d081c4-v4-ip',
            'tags': tags,
            'properties': {
                'deleteOption': 'Delete',
                'publicIPAllocationMethod': 'Static',
                'publicIPAddressVersion': 'IPv4',
            },
            'sku': {
                'name': 'Standard',
            },
        }
        vm_spec = {
            'location': 'centralus',
            'properties': {
                'hardwareProfile': {'vmSize': 'Standard_B1ls'},
                'networkProfile': {
                    'networkApiVersion': '2020-11-01',
                    'networkInterfaceConfigurations': [
                        {'name': 'npf2d7ff7d081c4-nic',
                         'tags': tags,
                         'properties': {
                             'deleteOption': 'Delete',
                             'ipConfigurations': [
                                 {'name': 'npf2d7ff7d081c4-v4-config',
                                  'properties': {
                                      'privateIPAddressVersion': 'IPv4',
                                      'subnet': {
                                          'id': subnet_id,
                                      },
                                      'publicIPAddressConfiguration': pip_spec,
                                  }}]}}]},
                'osProfile': {
                    'computerName': 'npf2d7ff7d081c4',
                },
                'storageProfile': {
                    'imageReference': {'offer': 'UbuntuServer',
                                       'publisher': 'Canonical',
                                       'sku': '18.04-LTS',
                                       'version': 'latest'},
                    'osDisk': {'createOption': 'FromImage',
                               'deleteOption': 'Delete'}}},
            'tags': tags,
        }

        client.resource_group_virtual_machines.create(
            'otherrg', 'npf2d7ff7d081c4', vm_spec)

        image_spec = {
            'location': 'centralus',
            'tags': tags,
            'properties': {
                'hyperVGeneration': 'V2',
                'storageProfile': {
                    'osDisk': {
                        'osType': 'Linux',
                        'managedDisk': {
                            'id': 'testdisk',
                        },
                        'osState': 'Generalized'
                    },
                    'zoneResilient': True
                }
            }
        }
        image = client.resource_group_images.create(
            'otherrg', 'testimage', image_spec)
        image = client.wait_for_async_operation(image)

        self.assertEqual(1, len(client.virtual_machines.list()))
        self.assertEqual(1, len(client.network_interfaces.list()))
        self.assertEqual(1, len(client.public_ip_addresses.list()))
        self.assertEqual(1, len(client.disks.list()))
        self.assertEqual(1, len(client.images.list()))

        self.log.debug("Start cleanup worker")
        self.launcher.cleanup_worker.start()

        for _ in iterate_timeout(30, 'vm deletion'):
            if not client.virtual_machines.list():
                break
            time.sleep(1)
        for _ in iterate_timeout(30, 'nic deletion'):
            if not client.network_interfaces.list():
                break
            time.sleep(1)
        for _ in iterate_timeout(30, 'pip deletion'):
            if not client.public_ip_addresses.list():
                break
            time.sleep(1)
        for _ in iterate_timeout(30, 'disk deletion'):
            if not client.disks.list():
                break
            time.sleep(1)
        for _ in iterate_timeout(30, 'image deletion'):
            if not client.images.list():
                break
            time.sleep(1)

    @simple_layout('layouts/azure/nodepool-images.yaml', enable_nodepool=True)
    def test_azure_image_id(self):
        self._test_node_lifecycle('debian-normal-id')
        self.assertEqual(
            self.fake_azure.crud['Microsoft.Compute/virtualMachines'].
            requests[0]['properties']['storageProfile']
            ['imageReference']['id'],
            "/subscriptions/c35cf7df-ed75-4c85-be00-535409a85120"
            "/resourceGroups/nodepool/providers/Microsoft.Compute"
            "/images/test-image-1234")

    @simple_layout('layouts/azure/nodepool-images.yaml', enable_nodepool=True)
    def test_azure_image_filter(self):
        self.fake_azure.crud['Microsoft.Compute/images'].items.append(
            make_image('test1', {'foo': 'bar'}))
        self._test_node_lifecycle('debian-normal-filter')
        self.assertEqual(
            self.fake_azure.crud['Microsoft.Compute/virtualMachines'].
            requests[0]['properties']['storageProfile']
            ['imageReference']['id'],
            "/subscriptions/c35cf7df-ed75-4c85-be00-535409a85120"
            "/resourceGroups/nodepool/providers/Microsoft.Compute"
            "/images/test1")

    @simple_layout('layouts/azure/nodepool-images.yaml', enable_nodepool=True)
    def test_azure_image_community_gallery(self):
        self._test_node_lifecycle('debian-normal-community')
        self.assertEqual(
            self.fake_azure.crud['Microsoft.Compute/virtualMachines'].
            requests[0]['properties']['storageProfile']
            ['imageReference']['communityGalleryImageId'],
            "/CommunityGalleries/community-gallery"
            "/Images/community-image"
            "/Versions/latest")

    @simple_layout('layouts/azure/nodepool-images.yaml', enable_nodepool=True)
    def test_azure_image_shared_gallery(self):
        self._test_node_lifecycle('debian-normal-shared')
        self.assertEqual(
            self.fake_azure.crud['Microsoft.Compute/virtualMachines'].
            requests[0]['properties']['storageProfile']
            ['imageReference']['sharedGalleryImageId'],
            "/SharedGalleries/shared-gallery"
            "/Images/shared-image"
            "/Versions/latest")


class TestAzureDriverOidc(BaseAzureDriverTest, BaseCloudDriverTest):
    config_file = 'zuul-connections-azure-oidc.conf'

    def setUp(self):
        self.token_file = tempfile.NamedTemporaryFile('w', delete=False)
        with self.token_file as f:
            f.write("testtoken")
        super().setUp()

    def setup_config(self, config_file):
        config = super().setup_config(config_file)
        config.set('connection azure', 'federated_token_file',
                   self.token_file.name)
        return config

    @simple_layout('layouts/azure/nodepool.yaml', enable_nodepool=True)
    def test_azure_node_lifecycle(self):
        self._test_node_lifecycle('debian-normal')
