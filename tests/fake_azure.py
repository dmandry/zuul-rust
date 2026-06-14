# Copyright 2021-2025 Acme Gating, LLC
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
import json
import time
import os
import re
import urllib
import uuid

import fixtures

import responses
import requests


class CRUDManager:
    name = ''

    def __init__(self, cloud):
        self.cloud = cloud
        self.items = []
        self.requests = []

    def list(self, request):
        resp = {'value': self.items}
        return (200, {}, json.dumps(resp))

    def get(self, request):
        url = urllib.parse.urlparse(request.path_url)
        for item in self.items:
            if item['id'] == url.path:
                return (200, {}, json.dumps(item))
        return (404, {}, json.dumps({
            'error': {
                'message': 'Not Found',
                'code': 'NotFound',
            }}))


class ResourceGroupsCRUD(CRUDManager):
    name = "resourcegroups"

    def put(self, request):
        data = json.loads(request.body)
        url = urllib.parse.urlparse(request.path_url)
        name = url.path.split('/')[-1]
        data['id'] = url.path
        data['name'] = name
        data['type'] = "Microsoft.Resources/resourceGroups"
        data['provisioningState'] = 'Succeeded'

        self.items.append(data)
        return (200, {}, json.dumps(data))


class PublicIPAddressesCRUD(CRUDManager):
    name = "Microsoft.Network/publicIPAddresses"

    def put(self, request):
        return self._put(json.loads(request.body),
                         request.path_url)

    def _put(self, data, url):
        url = urllib.parse.urlparse(url)
        name = url.path.split('/')[-1]
        data['id'] = url.path
        data['name'] = name
        data['type'] = self.name
        data['properties'] = {
            "provisioningState": "Updating",
            "resourceGuid": str(uuid.uuid4()),
            "publicIPAddressVersion": "IPv4",
            "publicIPAllocationMethod": "Static",
            "idleTimeoutInMinutes": 4,
            "ipTags": [],
        }
        self.items.append(data)
        ret = json.dumps(data)
        # Finish provisioning after return
        data['properties']['ipAddress'] = "198.51.100.1"
        data['properties']['provisioningState'] = "Succeeded"
        return (200, {}, ret)

    def delete(self, request):
        url = urllib.parse.urlparse(request.path_url)
        name = url.path.split('/')[-1]
        for item in self.items:
            if item['name'] == name:
                self.items.remove(item)
                return (200, {}, '')
        return (404, {}, json.dumps({
            'error': {
                'message': 'Not Found',
                'code': 'NotFound',
            }}))


class NetworkInterfacesCRUD(CRUDManager):
    name = "Microsoft.Network/networkInterfaces"

    def put(self, request):
        return self._put(json.loads(request.body),
                         request.path_url)

    def _put(self, data, url):
        url = urllib.parse.urlparse(url)
        name = url.path.split('/')[-1]
        data['id'] = url.path
        data['name'] = name
        data['type'] = self.name
        ipconfig = data['properties']['ipConfigurations'][0]
        data['properties'] = {
            "provisioningState": "Succeeded",
            "resourceGuid": str(uuid.uuid4()),
            "ipConfigurations": [
                {
                    "name": ipconfig['name'],
                    "id": os.path.join(data['id'], ipconfig['name']),
                    "type": ("Microsoft.Network/networkInterfaces/"
                             "ipConfigurations"),
                    "properties": {
                        "provisioningState": "Succeeded",
                        "privateIPAddress": "10.0.0.4",
                        "privateIPAllocationMethod": "Dynamic",
                        # publicIPAddress conditionally added below
                        "subnet": ipconfig['properties']['subnet'],
                        "primary": True,
                        "privateIPAddressVersion": "IPv4",
                    },
                }
            ],
            "enableAcceleratedNetworking": False,
            "enableIPForwarding": False,
            "hostedWorkloads": [],
            "tapConfigurations": [],
            "nicType": "Standard"
        }
        if 'publicIPAddress' in ipconfig['properties']:
            data['properties']['ipConfigurations'][0]['properties'][
                'publicIPAddress'] = ipconfig['properties']['publicIPAddress']
        self.items.append(data)
        return (200, {}, json.dumps(data))

    def delete(self, request):
        url = urllib.parse.urlparse(request.path_url)
        name = url.path.split('/')[-1]
        for item in self.items:
            if item['name'] == name:
                self.items.remove(item)
                return (200, {}, '')
        return (404, {}, json.dumps({
            'error': {
                'message': 'Not Found',
                'code': 'NotFound',
            }}))


class VirtualMachinesCRUD(CRUDManager):
    name = "Microsoft.Compute/virtualMachines"

    def put(self, request):
        data = json.loads(request.body)
        origdata = copy.deepcopy(data)
        self.requests.append(origdata)
        url = urllib.parse.urlparse(request.path_url)
        name = url.path.split('/')[-1]
        data['id'] = url.path
        data['name'] = name
        data['type'] = self.name
        data['properties'] = {
            "vmId": str(uuid.uuid4()),
            "hardwareProfile": data['properties']['hardwareProfile'],
            "storageProfile": {
                "imageReference": (data['properties']['storageProfile']
                                   ['imageReference']),
                "osDisk": {
                    "osType": "Linux",
                    "createOption": "FromImage",
                    "caching": "ReadWrite",
                    "managedDisk": {
                        "storageAccountType": "Premium_LRS"
                    },
                    "diskSizeGB": 30
                },
                "dataDisks": []
            },
            "osProfile": data['properties']['osProfile'],
            "networkProfile": data['properties']['networkProfile'],
            "provisioningState": "Creating"
        }
        if 'priority' in origdata['properties']:
            data['properties']['priority'] = origdata['properties']['priority']

        data['zones'] = ["1"]
        self.items.append(data)

        # Add a disk
        disk_data = data.copy()
        disk_data['name'] = 'bionic-azure-' + str(uuid.uuid4())
        disk_data['type'] = "Microsoft.Compute/disks"
        disk_data['id'] = '/'.join(url.path.split('/')[:6] +
                                   [disk_data['type'], disk_data['name']])
        disk_data['properties'] = {"provisioningState": "Succeeded"}
        self.cloud.crud["Microsoft.Compute/disks"].items.append(disk_data)

        resource_group = self.cloud._extract_resource_group(url.path)

        # Add a PIP
        data_nic = (data['properties']['networkProfile']
                    ['networkInterfaceConfigurations'][0])
        data_ip = data_nic['properties']['ipConfigurations'][0]
        pip_url = None
        if 'publicIPAddressConfiguration' in data_ip['properties']:
            data_pip = data_ip['properties']['publicIPAddressConfiguration']
            pip_data = {
                'location': data['location'],
                'tags': data_pip['tags'],
                'sku': {
                    'name': data_pip['sku'],
                },
                'properties': {
                    'publicIPAddressVersion': (data_ip['properties']
                                               ['privateIPAddressVersion']),
                    'publicIPAllocationMethod': (data_pip['properties']
                                                 ['publicIPAllocationMethod']),
                },
            }
            pip_url = (f'/subscriptions/{self.cloud.subscription_id}'
                       f'/resourceGroups/{resource_group}/providers'
                       f'/Microsoft.Network/publicIPAddresses/{name}-v4-abcd')
            self.cloud.crud["Microsoft.Network/publicIPAddresses"]._put(
                pip_data, pip_url)

        # Add a NIC
        nic_data = {
            'location': data['location'],
            'tags': data_nic['tags'],
            'properties': {
                'ipConfigurations': [{
                    'name': data_ip['name'],
                    'properties': {
                        'privateIPAddressVersion': (
                            data_ip['properties']
                            ['privateIPAddressVersion']),
                        'subnet': data_ip['properties']['subnet'],
                    }
                }],
            }
        }
        data['properties']['networkProfile']['networkInterfaces'] =\
            [nic_data.copy()]
        if pip_url:
            (nic_data['properties']['ipConfigurations'][0]['properties']
             ['publicIPAddress']) = {
                'id': pip_url,
                'type': 'Microsoft.Network/publicIPAddresses',
                'properties': {
                    'publicIPAddressVersion': (data_ip['properties']
                                               ['privateIPAddressVersion']),
                },
            }
        nic_url = (f'/subscriptions/{self.cloud.subscription_id}'
                   f'/resourceGroups/{resource_group}/providers'
                   f'/Microsoft.Network/networkInterfaces/{name}-nic-abcd')
        data['properties']['networkProfile']['networkInterfaces'][0]['id'] =\
            nic_url
        self.cloud.crud["Microsoft.Network/networkInterfaces"]._put(
            nic_data, nic_url)

        ret = json.dumps(data)
        # Finish provisioning after return
        data['properties']['provisioningState'] = "Succeeded"
        return (200, {}, ret)

    def delete(self, request):
        url = urllib.parse.urlparse(request.path_url)
        name = url.path.split('/')[-1]
        for item in self.items:
            if item['name'] == name:
                self.items.remove(item)
                return (200, {}, '')
        return (404, {}, json.dumps({
            'error': {
                'message': 'Not Found',
                'code': 'NotFound',
            }}))


class DisksCRUD(CRUDManager):
    name = "Microsoft.Compute/disks"

    def put(self, request):
        data = json.loads(request.body)
        url = urllib.parse.urlparse(request.path_url)
        name = url.path.split('/')[-1]
        data['id'] = url.path
        data['name'] = name
        data['type'] = self.name
        data['properties'] = {
            "provisioningState": "Succeeded",
        }
        self.items.append(data)
        async_url = 'https://management.azure.com/async' + request.path_url
        headers = {'Azure-AsyncOperation': async_url,
                   'Retry-After': '0'}
        return (200, headers, json.dumps(data))

    def post(self, request):
        data = json.loads(request.body)
        url = urllib.parse.urlparse(request.path_url)
        name = url.path.split('/')[-2]
        action = url.path.split('/')[-1]
        if action == 'beginGetAccess':
            async_url = 'https://management.azure.com/async' + request.path_url
            async_url = async_url.replace('/beginGetAccess', '')
            for item in self.items:
                if item['name'] == name:
                    item['accessSAS'] = (
                        'https://management.azure.com/sas/' + name)
        if action == 'endGetAccess':
            async_url = 'https://management.azure.com/async' + request.path_url
            async_url = async_url.replace('/endGetAccess', '')
        headers = {'Azure-AsyncOperation': async_url,
                   'Retry-After': '0'}
        return (200, headers, json.dumps(data))

    def delete(self, request):
        url = urllib.parse.urlparse(request.path_url)
        name = url.path.split('/')[-1]
        async_url = 'https://management.azure.com/async' + request.path_url
        for item in self.items:
            if item['name'] == name:
                self.items.remove(item)
                headers = {'Azure-AsyncOperation': async_url,
                           'Retry-After': '0'}
                return (200, headers, '')
        return (404, {}, json.dumps({
            'error': {
                'message': 'Not Found',
                'code': 'NotFound',
            }}))


class ImagesCRUD(CRUDManager):
    name = "Microsoft.Compute/images"

    def put(self, request):
        data = json.loads(request.body)
        url = urllib.parse.urlparse(request.path_url)
        name = url.path.split('/')[-1]
        data['id'] = url.path
        data['name'] = name
        data['type'] = self.name
        data['properties'] = {
            "provisioningState": "Succeeded",
        }
        self.items.append(data)
        async_url = 'https://management.azure.com/async' + request.path_url
        headers = {'Azure-AsyncOperation': async_url,
                   'Retry-After': '0'}
        return (200, headers, json.dumps(data))

    def delete(self, request):
        url = urllib.parse.urlparse(request.path_url)
        name = url.path.split('/')[-1]
        async_url = 'https://management.azure.com/async' + request.path_url
        for item in self.items:
            if item['name'] == name:
                self.items.remove(item)
                headers = {'Azure-AsyncOperation': async_url,
                           'Retry-After': '0'}
                return (200, headers, '')
        return (404, {}, json.dumps({
            'error': {
                'message': 'Not Found',
                'code': 'NotFound',
            }}))


class FakeAzureFixture(fixtures.Fixture):
    tenant_id = 'b6e5bbf0-ebc0-48a7-9112-ad8829d7edc1'
    subscription_id = '582c3efd-3e57-4073-82c5-d122636fb45c'

    def __init__(self, responses_fixture, quotas):
        super().__init__()
        self.responses = responses_fixture.requests_mock
        self._quotas = quotas

    def _setUp(self):
        self.crud = {}

        self.responses.add(
            responses.POST,
            f'https://login.microsoftonline.com/{self.tenant_id}/oauth2/token',
            json={
                'access_token': 'secret_token',
                'expires_on': time.time() + 600,
            })

        self.responses.add_callback(
            responses.GET,
            ('https://management.azure.com/subscriptions/'
             f'{self.subscription_id}/providers/Microsoft.Compute/skus/'
             '?api-version=2019-04-01'),
            callback=self._get_compute_skus,
            content_type='application/json')

        self.responses.add_callback(
            responses.GET,
            ('https://management.azure.com/subscriptions/'
             f'{self.subscription_id}/providers/Microsoft.Compute/locations/'
             'centralus/usages?api-version=2020-12-01'),
            callback=self._get_compute_usages,
            content_type='application/json')

        async_re = re.compile('https://management.azure.com/async/(.*)')
        self.responses.add_callback(
            responses.GET, async_re,
            callback=self._get_async,
            content_type='application/json')

        sas_re = re.compile('https://management.azure.com/sas/(.*)')
        self.responses.add_callback(
            responses.PUT, sas_re,
            callback=self._put_sas,
            content_type='application/json')

        self._setup_crud(ResourceGroupsCRUD, '2020-06-01',
                         resource_grouped=False)

        self._setup_crud(VirtualMachinesCRUD, '2025-11-01')
        self._setup_crud(NetworkInterfacesCRUD, '2020-11-01')
        self._setup_crud(PublicIPAddressesCRUD, '2020-11-01')
        self._setup_crud(DisksCRUD, '2020-06-30')
        self._setup_crud(ImagesCRUD, '2020-12-01')

    def _setup_crud(self, manager, api_version, resource_grouped=True):
        self.crud[manager.name] = manager(self)

        if resource_grouped:
            rg = 'resourceGroups/(.*?)/providers/'
        else:
            rg = ''

        list_re = re.compile(
            'https://management.azure.com/subscriptions/'
            + f'{self.subscription_id}/'
            + rg + f'{manager.name}/?\\?api-version={api_version}')
        crud_re = re.compile(
            'https://management.azure.com/subscriptions/'
            + f'{self.subscription_id}/'
            + rg + f'{manager.name}/(.+?)\\?api-version={api_version}')
        self.responses.add_callback(
            responses.GET, list_re, callback=self.crud[manager.name].list,
            content_type='application/json')
        self.responses.add_callback(
            responses.GET, crud_re, callback=self.crud[manager.name].get,
            content_type='application/json')
        self.responses.add_callback(
            responses.PUT, crud_re, callback=self.crud[manager.name].put,
            content_type='application/json')
        if hasattr(self.crud[manager.name], 'post'):
            self.responses.add_callback(
                responses.POST, crud_re, callback=self.crud[manager.name].post,
                content_type='application/json')
        if hasattr(self.crud[manager.name], 'delete'):
            self.responses.add_callback(
                responses.DELETE, crud_re,
                callback=self.crud[manager.name].delete,
                content_type='application/json')
        if resource_grouped:
            # Also add list all
            list_re = re.compile(
                'https://management.azure.com/subscriptions/'
                + f'{self.subscription_id}/'
                + f'providers/{manager.name}/?\\?api-version={api_version}')
            self.responses.add_callback(
                responses.GET, list_re, callback=self.crud[manager.name].list,
                content_type='application/json')

    def _extract_resource_group(self, path):
        url = re.compile('/subscriptions/(.*?)/resourceGroups/(.*?)/')
        m = url.match(path)
        return m.group(2)

    def _get_compute_skus(self, request):
        data = {
            'value': [
                {'capabilities': [
                    {'name': 'MaxResourceVolumeMB', 'value': '4096'},
                    {'name': 'OSVhdSizeMB', 'value': '1047552'},
                    {'name': 'vCPUs', 'value': '1'},
                    {'name': 'HyperVGenerations', 'value': 'V1,V2'},
                    {'name': 'MemoryGB', 'value': '0.5'},
                    {'name': 'MaxDataDiskCount', 'value': '2'},
                    {'name': 'LowPriorityCapable', 'value': 'False'},
                    {'name': 'PremiumIO', 'value': 'True'},
                    {'name': 'VMDeploymentTypes', 'value': 'IaaS'},
                    {'name': 'CombinedTempDiskAndCachedIOPS', 'value': '200'},
                    {'name': 'CombinedTempDiskAndCachedReadBytesPerSecond',
                     'value': '10485760'},
                    {'name': 'CombinedTempDiskAndCachedWriteBytesPerSecond',
                     'value': '10485760'},
                    {'name': 'UncachedDiskIOPS', 'value': '160'},
                    {'name': 'UncachedDiskBytesPerSecond',
                     'value': '10485760'},
                    {'name': 'EphemeralOSDiskSupported', 'value': 'True'},
                    {'name': 'EncryptionAtHostSupported', 'value': 'True'},
                    {'name': 'AcceleratedNetworkingEnabled', 'value': 'False'},
                    {'name': 'RdmaEnabled', 'value': 'False'},
                    {'name': 'MaxNetworkInterfaces', 'value': '2'}],
                 'family': 'standardBSFamily',
                 'locationInfo': [
                     {'location': 'centralus',
                      'zoneDetails': [
                          {'Name': ['3', '2', '1'],
                           'capabilities': [
                               {'name': 'UltraSSDAvailable',
                                'value': 'True'}]}],
                      'zones': ['3', '1', '2']}],
                 'locations': ['centralus'],
                 'name': 'Standard_B1ls',
                 'resourceType': 'virtualMachines',
                 'restrictions': [],
                 'size': 'B1ls',
                 'tier': 'Standard'}
            ]
        }
        return (200, {}, json.dumps(data))

    def _get_compute_usages(self, request):
        mgr = self.crud["Microsoft.Compute/virtualMachines"]
        data = {
            'value': [
                {
                    "limit": self._quotas['lowPriorityCores'],
                    "unit": "Count",
                    "currentValue": len(mgr.items),
                    "name": {
                        "value": "lowPriorityCores",
                        "localizedValue": "Total Regional Low-priority vCPUs",
                    }
                }, {
                    "limit": self._quotas['cores'],
                    "unit": "Count",
                    "currentValue": len(mgr.items),
                    "name": {
                        "value": "cores",
                        "localizedValue": "Total Regional vCPUs"
                    }
                }, {
                    "limit": self._quotas['virtualMachines'],
                    "unit": "Count",
                    "currentValue": len(mgr.items),
                    "name": {
                        "value": "virtualMachines",
                        "localizedValue": "Virtual Machines"
                    }
                }
            ]}
        return (200, {}, json.dumps(data))

    def _get_async(self, request):
        path = request.path_url[len('/async'):]
        ret = requests.get('https://management.azure.com' + path)
        data = {
            'status': 'Succeeded',
            'properties': {
                'output': ret.json(),
            }
        }
        return (200, {}, json.dumps(data))

    def _put_sas(self, request):
        return (201, {}, '')
