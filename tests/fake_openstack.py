# Copyright (C) 2011-2013 OpenStack Foundation
# Copyright 2022, 2024 Acme Gating, LLC
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

import logging
import uuid

from zuul.driver.openstack.openstackendpoint import OpenstackProviderEndpoint


class FakeOpenstackObject:
    def __init__(self, **kw):
        self.__dict__.update(kw)
        self.__kw = list(kw.keys())

    def _get_dict(self):
        data = {}
        for k in self.__kw:
            data[k] = getattr(self, k)
        return data

    def __contains__(self, key):
        return key in self.__dict__

    def __getitem__(self, key, default=None):
        return getattr(self, key, default)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def get(self, key, default=None):
        return getattr(self, key, default)

    def set(self, key, value):
        setattr(self, key, value)


class FakeOpenstackFlavor(FakeOpenstackObject):
    pass


class FakeOpenstackServer(FakeOpenstackObject):
    pass


class FakeOpenstackLocation(FakeOpenstackObject):
    pass


class FakeOpenstackImage(FakeOpenstackObject):
    pass


class FakeOpenstackMember(FakeOpenstackObject):
    def __repr__(self):
        return (f'<FakeOpenstackMember m={self.member_id} '
                f'i={self.image_id} {self.status}')


class FakeOpenstackNetwork(FakeOpenstackObject):
    pass


class FakeOpenstackFloatingIp(FakeOpenstackObject):
    def _fake_toDict(self):
        return {
            'version': 4,
            'addr': self.floating_ip_address,
            'OS-EXT-IPS:type': 'floating',
        }


class FakeOpenstackPort(FakeOpenstackObject):
    pass


class FakeOpenstackAuth(FakeOpenstackObject):
    pass


class FakeOpenstackCloud:
    log = logging.getLogger("zuul.FakeOpenstackCloud")

    def __init__(self,
                 region,
                 project,
                 needs_floating_ip=False,
                 auto_attach_floating_ip=True,
                 ):
        self._fake_region = region
        # A project is not really a characteristic of a server-side
        # cloud, but in tests we treat different projects as different
        # clouds for simplicity.
        self._fake_project = project
        self._fake_needs_floating_ip = needs_floating_ip
        self._fake_auto_attach_floating_ip = auto_attach_floating_ip
        self.servers = []
        self.volumes = []
        self.images = []
        self.members = []
        self.floating_ips = []
        self.flavors = [
            FakeOpenstackFlavor(
                id='425e3203150e43d6b22792f86752533d',
                name='Fake Flavor',
                ram=8192,
                vcpus=4,
            )
        ]
        self.networks = [
            FakeOpenstackNetwork(
                id=uuid.uuid4().hex,
                name='fake-network',
            )
        ]
        self.ports = []

    def _getConnection(self):
        return FakeOpenstackConnection(self)


class FakeOpenstackResponse:
    def __init__(self, data):
        self._data = data
        self.links = []

    def json(self):
        return self._data


class FakeOpenstackSession:
    def __init__(self, cloud):
        self.cloud = cloud

    def get(self, uri, headers, params):
        if uri == '/servers/detail':
            server_list = []
            for server in self.cloud.servers:
                data = server._get_dict()
                data['status'] = 'ACTIVE'
                data['hostId'] = data.pop('host_id')
                data['OS-EXT-AZ:availability_zone'] = data.pop('location').zone
                data['os-extended-volumes:volumes_attached'] =\
                    data.pop('volumes')
                server_list.append(data)
            return FakeOpenstackResponse({'servers': server_list})


class FakeOpenstackConfig:
    def __init__(self, region):
        self.config = {}
        self.config['image_format'] = 'qcow2'
        self.config['region_name'] = region


class FakeOpenstackImageProxy:
    def __init__(self, cloud):
        self.cloud = cloud

    def add_member(self, image, **attrs):
        member = FakeOpenstackMember(
            member_id=uuid.uuid4().hex,
            image_id=image,
            status='pending',
        )
        self.cloud.members.append(member)
        return member

    def update_member(self, member_id, image_id, **attrs):
        for m in self.cloud.members:
            if (m.member_id == member_id and
                m.image_id == image_id):
                m.__dict__.update(attrs)
                break
        else:
            raise Exception("Unable to find member")


class FakeOpenstackConnection:
    log = logging.getLogger("zuul.FakeOpenstackConnection")

    def __init__(self, cloud):
        self.cloud = cloud
        self.compute = FakeOpenstackSession(cloud)
        self.config = FakeOpenstackConfig(cloud._fake_region or 'region1')
        self.auth = FakeOpenstackAuth(
            project_id=cloud._fake_project,
        )
        self.image = FakeOpenstackImageProxy(cloud)

    def _needs_floating_ip(self, server, nat_destination):
        return self.cloud._fake_needs_floating_ip

    def _use_neutron_floating(self):
        return self.cloud._fake_needs_floating_ip

    def _has_floating_ips(self):
        return self.cloud._fake_needs_floating_ip

    def delete_unattached_floating_ips(self):
        for x in self.cloud.floating_ips:
            if not getattr(x, 'port', None):
                self.cloud.floating_ips.remove(x)

    def list_flavors(self, get_extra=False):
        return self.cloud.flavors

    def list_volumes(self):
        return self.cloud.volumes

    def list_ports(self, filters=None):
        if filters and filters.get('status'):
            return [p for p in self.cloud.ports
                    if p.status == filters['status']]
        return self.cloud.ports

    def delete_port(self, port_id):
        for x in self.cloud.ports:
            if x.id == port_id:
                self.cloud.ports.remove(x)
                return

    def get_network(self, name_or_id, filters=None):
        for x in self.cloud.networks:
            if x.id == name_or_id or x.name == name_or_id:
                return x

    def list_servers(self):
        return self.cloud.servers

    def create_server(self, wait=None, name=None, image=None,
                      flavor=None, config_drive=None, key_name=None,
                      nics=None, meta=None, availability_zone=None):
        location = FakeOpenstackLocation(zone=availability_zone)
        if self.cloud._fake_needs_floating_ip:
            addresses = dict(
                public=[],
                private=[dict(version=4, addr='198.51.100.1')]
            )
            interface_ip = '198.51.100.1'
        else:
            addresses = dict(
                public=[dict(version=4, addr='198.51.100.1'),
                        dict(version=6, addr='2001:db8::1')],
                private=[dict(version=4, addr='198.51.100.1')]
            )
            interface_ip = '198.51.100.1'

        args = dict(
            id=uuid.uuid4().hex,
            name=name,
            host_id='fake_host_id',
            location=location,
            volumes=[],
            status='BUILDING',
            addresses=addresses,
            interface_ip=interface_ip,
            flavor=flavor,
            metadata=meta,
        )
        server = FakeOpenstackServer(**args)
        self.cloud.servers.append(server)
        return server

    def delete_server(self, name_or_id):
        for x in self.cloud.servers:
            if x.id == name_or_id:
                self.cloud.servers.remove(x)
                return

    def create_image(self, wait=None, name=None, filename=None,
                     is_public=None, md5=None, sha256=None,
                     timeout=None, **meta):
        image = FakeOpenstackImage(
            id=uuid.uuid4().hex,
            name=name,
            filename=filename,
            is_public=is_public,
            md5=md5,
            sha256=sha256,
            status='ACTIVE',
            properties=meta,
        )
        self.cloud.images.append(image)
        return image

    def delete_image(self, name_or_id):
        for x in self.cloud.images:
            if x.id == name_or_id:
                self.cloud.images.remove(x)
                return

    def list_images(self):
        return self.cloud.images

    def create_floating_ip(self, server, wait=None):
        args = dict(
            id=uuid.uuid4().hex,
            floating_ip_address='fake',
            status='ACTIVE',
        )
        if self.cloud._fake_auto_attach_floating_ip:
            args['port_id'] = 'fake'
        fip = FakeOpenstackFloatingIp(**args)
        self.cloud.floating_ips.append(fip)
        if self.cloud._fake_auto_attach_floating_ip:
            server['addresses']['public'].append(fip._fake_toDict())
        return fip

    def list_floating_ips(self):
        return self.cloud.floating_ips

    def get_floating_ip(self, id, filters=None):
        for x in self.cloud.floating_ips:
            if x.id == id:
                return x
            for k, v in filters.items():
                if x[k] == v:
                    return x

    def delete_floating_ip(self, floating_ip_id, retry=1):
        for x in self.cloud.floating_ips:
            if x.id == floating_ip_id:
                self.cloud.floating_ips.remove(x)
                return

    def _attach_ip_to_server(self, server, floating_ip, skip_attach=False):
        if floating_ip.get('port_id'):
            raise Exception("Attaching already attached fip")
        floating_ip['port_id'] = 'fake'
        server['addresses']['public'].append(floating_ip._fake_toDict())

    def get_compute_limits(self):
        return FakeOpenstackObject(
            max_total_cores=self.cloud.max_cores,
            max_total_instances=self.cloud.max_instances,
            max_total_ram_size=self.cloud.max_ram,
            total_cores_used=4 * len(self.cloud.servers),
            total_instances_used=len(self.cloud.servers),
            total_ram_used=8192 * len(self.cloud.servers),
        )

    def get_volume_limits(self):
        return FakeOpenstackObject(
            absolute=dict(
                maxTotalVolumes=self.cloud.max_volumes,
                maxTotalVolumeGigabytes=self.cloud.max_volume_gb,
            ))


class FakeOpenstackProviderEndpoint(OpenstackProviderEndpoint):
    def _getRegionConfig(self):
        return FakeOpenstackConfig('region1')

    def _getClient(self):
        name = f'{self.connection.connection_name}-{self.region}'
        return self._fake_cloud[name]._getConnection()

    def _expandServer(self, server):
        return server
