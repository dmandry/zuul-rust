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
from zuul.driver.heat.heatendpoint import HeatProviderEndpoint


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


class FakeOpenstackCloud:
    log = logging.getLogger("zuul.FakeOpenstackCloud")

    def __init__(self,
                 region,
                 needs_floating_ip=False,
                 auto_attach_floating_ip=True,
                 ):
        self._fake_region = region
        self._fake_needs_floating_ip = needs_floating_ip
        self._fake_auto_attach_floating_ip = auto_attach_floating_ip
        self.servers = []
        self.volumes = []
        self.images = []
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

    def _stack_outputs(self):
        """Default stack outputs for Heat fakes.

        Override or monkey-patch to customise per-test.
        """
        return [
            {'output_key': 'bastion_ip',
             'output_value': '198.51.100.1'},
        ]


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


class FakeOpenstackConnection:
    log = logging.getLogger("zuul.FakeOpenstackConnection")

    def __init__(self, cloud):
        self.cloud = cloud
        self.compute = FakeOpenstackSession(cloud)
        self.config = FakeOpenstackConfig(cloud._fake_region or 'region1')

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
        )
        self.cloud.images.append(image)
        return image

    def delete_image(self, name_or_id):
        for x in self.cloud.servers:
            if x.id == name_or_id:
                self.cloud.servers.remove(x)
                return

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
        return self._fake_cloud[self.region]._getConnection()

    def _expandServer(self, server):
        return server


# ---------------------------------------------------------------------------
# Heat driver fakes
#
# The Heat endpoint uses proxy-style SDK calls
# (e.g. self._client.orchestration.create_stack()) rather than the
# shade-style calls used by the OpenStack driver
# (e.g. self.create_server()).  The classes below provide the proxy
# objects and a connection that satisfies the Heat endpoint's API
# surface without touching the shade-style fakes above.
# ---------------------------------------------------------------------------

class FakeStack(FakeOpenstackObject):
    pass


class FakeStackResource(FakeOpenstackObject):
    pass


class FakeOrchestrationProxy:
    """Fake openstack.orchestration proxy for Heat stack lifecycle."""

    def __init__(self, cloud):
        self.cloud = cloud
        if not hasattr(cloud, 'stacks'):
            cloud.stacks = []

    def create_stack(self, name, template, parameters, tags=None):
        tag_list = tags.split(',') if tags else []
        stack_id = uuid.uuid4().hex
        stack = FakeStack(
            id=stack_id,
            name=name,
            status='CREATE_IN_PROGRESS',
            stack_status_reason=None,
            tags=tag_list,
            outputs=[],
        )
        self.cloud.stacks.append(stack)
        return stack

    def get_stack(self, stack_id):
        for stack in self.cloud.stacks:
            if stack.id == stack_id:
                if stack.status == 'CREATE_IN_PROGRESS':
                    stack.status = 'CREATE_COMPLETE'
                    stack.outputs = self.cloud._stack_outputs()
                return stack
        return None

    def delete_stack(self, stack_id):
        self.cloud.stacks = [
            s for s in self.cloud.stacks if s.id != stack_id
        ]
        return True

    def stacks(self, tags=None):
        filter_tags = tags.split(',') if tags else []
        result = []
        for stack in self.cloud.stacks:
            stack_tags = getattr(stack, 'tags', []) or []
            if filter_tags and not all(t in stack_tags for t in filter_tags):
                continue
            result.append(stack)
        return result

    def resources(self, stack_id, nested_depth=None):
        return []


class FakeComputeProxy:
    """Fake openstack.compute proxy."""

    def __init__(self, cloud):
        self.cloud = cloud

    def servers(self, details=False):
        return list(self.cloud.servers)

    def find_flavor(self, name):
        for f in self.cloud.flavors:
            if f.name == name or f.id == name:
                return f
        return None

    def flavors(self, details=False):
        return list(self.cloud.flavors)


class FakeNetworkProxy:
    """Fake openstack.network proxy."""

    def __init__(self, cloud):
        self.cloud = cloud

    def networks(self, project_id=None, is_router_external=None,
                 is_shared=None):
        return list(self.cloud.networks)

    def routers(self, project_id=None):
        return list(getattr(self.cloud, 'routers', []))

    def ips(self, project_id=None):
        return list(self.cloud.floating_ips)

    def get_quota(self, project_id):
        return {
            'network': -1,
            'router': -1,
            'floatingip': -1,
            'port': -1,
        }


class FakeBlockStorageProxy:
    """Fake openstack.block_storage proxy."""

    def __init__(self, cloud):
        self.cloud = cloud

    def volumes(self, details=False):
        return list(self.cloud.volumes)


class FakeHeatConnection:
    """Connection object for the Heat endpoint with proxy-style API.

    Provides .orchestration, .compute, .network, .block_storage proxies
    and connection-level helpers required by HeatProviderEndpoint.
    """
    log = logging.getLogger("zuul.FakeHeatConnection")

    def __init__(self, cloud):
        self.cloud = cloud
        self.orchestration = FakeOrchestrationProxy(cloud)
        self.compute = FakeComputeProxy(cloud)
        self.network = FakeNetworkProxy(cloud)
        self.block_storage = FakeBlockStorageProxy(cloud)
        self.config = FakeOpenstackConfig(cloud._fake_region or 'region1')
        self.current_project_id = 'fake_project_id'

    def get_compute_limits(self):
        return FakeOpenstackObject(
            max_total_cores=getattr(self.cloud, 'max_cores', 100),
            max_total_instances=getattr(self.cloud, 'max_instances', 100),
            max_total_ram_size=getattr(self.cloud, 'max_ram', 1000000),
            total_cores_used=0,
            total_instances_used=0,
            total_ram_used=0,
        )

    def get_volume_limits(self):
        return FakeOpenstackObject(
            absolute=dict(
                maxTotalVolumes=getattr(self.cloud, 'max_volumes', 100),
                maxTotalVolumeGigabytes=getattr(
                    self.cloud, 'max_volume_gb', 1000),
            ))

    def get_external_ipv4_floating_networks(self):
        return [{'name': 'fake-external-network'}]

    def close(self):
        pass


class FakeHeatProviderEndpoint(HeatProviderEndpoint):
    """Fake Heat endpoint that uses in-memory fakes instead of real APIs."""

    def _getRegionConfig(self):
        return FakeOpenstackConfig('region1')

    def _getClient(self):
        cloud = self._fake_cloud[self.region]
        return FakeHeatConnection(cloud)
