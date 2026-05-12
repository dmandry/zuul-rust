# Copyright 2024 BMW Group
# Copyright 2024 Acme Gating, LLC
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

from zuul import model
from zuul.provider import statemachine


class OpenstackProviderNode(model.ProviderNode, subclass_id="openstack"):
    def __init__(self):
        super().__init__()
        self._set(
            openstack_server_id=None,
            openstack_floating_ip_id=None,
        )

    def getDriverData(self):
        return dict(
            openstack_server_id=self.openstack_server_id,
            openstack_floating_ip_id=self.openstack_floating_ip_id,
        )

    def getExternalId(self):
        return self.openstack_server_id


class OpenstackInstance(statemachine.Instance):
    def __init__(self, cloud, region, server, quota):
        super().__init__()
        self.openstack_server_id = server['id']
        self.metadata = server.get('metadata', {})
        self.private_ipv4 = server.get('private_v4')
        self.private_ipv6 = None
        self.public_ipv4 = server.get('public_v4')
        self.public_ipv6 = server.get('public_v6')
        self.host_id = server['hostId']
        self.cloud = cloud
        self.region = region
        self.az = server.get('OS-EXT-AZ:availability_zone')
        self.interface_ip = server.get('interface_ip')
        self.quota = quota

    def getQuotaInformation(self):
        return self.quota

    @property
    def external_id(self):
        return f'server={self.openstack_server_id}'


class OpenstackResource(statemachine.Resource):
    TYPE_HOST = 'host'
    TYPE_INSTANCE = 'instance'
    TYPE_AMI = 'ami'
    TYPE_SNAPSHOT = 'snapshot'
    TYPE_VOLUME = 'volume'
    TYPE_OBJECT = 'object'

    def __init__(self, metadata, type, id):
        super().__init__(metadata, type)
        self.id = id

    @property
    def unique_id(self):
        return '-'.join([self.type, self.id])
