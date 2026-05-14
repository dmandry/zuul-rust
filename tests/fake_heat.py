# Copyright 2026 Red Hat, Inc.
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

from zuul.driver.heat.heatendpoint import HeatProviderEndpoint

from tests.fake_openstack import (
    FakeOpenstackCloud,
    FakeOpenstackConfig,
    FakeOpenstackObject,
)


class FakeHeatCloud(FakeOpenstackCloud):
    """FakeOpenstackCloud subclass with Heat-specific state."""

    def __init__(self, region, **kw):
        super().__init__(region, **kw)
        self.stacks = []

    def _stack_outputs(self):
        """Default stack outputs for Heat fakes.

        Override or monkey-patch to customise per-test.
        """
        return [
            {'output_key': 'bastion_ip',
             'output_value': '198.51.100.1'},
        ]


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
