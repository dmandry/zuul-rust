# Copyright 2021-2025 Acme Gating, LLC
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

from enum import StrEnum

from zuul import model
from zuul.provider import statemachine


class AzureProviderNode(model.ProviderNode, subclass_id="azure"):
    def __init__(self):
        super().__init__()
        self._set(
            azure_vm_id=None,
        )

    def getDriverData(self):
        return dict(
            azure_vm_id=self.azure_vm_id,
        )


class AzureInstance(statemachine.Instance):
    def __init__(self, region, vm, quota, nic=None, public_ipv4=None,
                 public_ipv6=None):
        super().__init__()
        self.azure_vm_id = vm and vm['id'] or None
        self.metadata = vm.get('tags', {})
        self.private_ipv4 = None
        self.private_ipv6 = None
        self.public_ipv4 = None
        self.public_ipv6 = None
        self.quota = quota

        if nic:
            for ip_config_data in nic['properties']['ipConfigurations']:
                ip_config_prop = ip_config_data['properties']
                if ip_config_prop['privateIPAddressVersion'] == 'IPv4':
                    self.private_ipv4 = ip_config_prop['privateIPAddress']
                if ip_config_prop['privateIPAddressVersion'] == 'IPv6':
                    self.private_ipv6 = ip_config_prop['privateIPAddress']

        if public_ipv4:
            self.public_ipv4 = public_ipv4['properties'].get('ipAddress')
        if public_ipv6:
            self.public_ipv6 = public_ipv6['properties'].get('ipAddress')

        self.interface_ip = (self.public_ipv4 or self.public_ipv6 or
                             self.private_ipv4 or self.private_ipv6)
        self.cloud = 'Azure'
        self.region = vm['location']
        if len(vm.get('zones', [])) > 0:
            self.az = vm['zones'][0]
        else:
            self.az = ''

    def getQuotaInformation(self):
        return self.quota

    @property
    def external_id(self):
        return f'vm={self.azure_vm_id}'


class AzureResource(statemachine.Resource):
    class ResourceType(StrEnum):
        INSTANCE = 'instance'
        NIC = 'nic'
        PIP = 'pip'
        DISK = 'disk'
        IMAGE = 'image'

    def __init__(self, metadata, type, id):
        super().__init__(metadata, type)
        self.id = id

    @property
    def unique_id(self):
        return '-'.join([self.type, self.id])
