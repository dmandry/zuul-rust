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

from zuul import model
from zuul.provider import statemachine


class HeatProviderNode(model.ProviderNode, subclass_id="heat"):
    """Provider node for Heat stacks.

    Each node represents a Heat stack that provides a bastion/jump host
    as the Zuul node. The stack may contain multiple internal instances
    accessible through the bastion.
    """

    def __init__(self):
        super().__init__()
        self._set(
            heat_stack_id=None,
            heat_stack_name=None,
            heat_stack_status=None,
            heat_template_path=None,
            heat_stack_outputs=None,
            heat_physical_resource_ids=[],
        )

    def getDriverData(self):
        """Persisted to ZooKeeper; used to resume state machines on restart."""
        return dict(
            heat_stack_id=self.heat_stack_id,
            heat_stack_name=self.heat_stack_name,
            heat_stack_status=self.heat_stack_status,
            heat_template_path=self.heat_template_path,
            heat_stack_outputs=self.heat_stack_outputs,
            heat_physical_resource_ids=self.heat_physical_resource_ids,
        )

    def getManagedResources(self):
        return self.heat_physical_resource_ids


class HeatInstance(statemachine.Instance):
    """Represents a Heat stack as a Zuul node instance.

    One Heat stack = one Zuul node. The stack's ``bastion_ip`` output
    provides the routable IP for external SSH access.
    """

    def __init__(self, cloud, region, stack_id, bastion_ip, outputs, quota):
        super().__init__()
        self.heat_stack_id = stack_id
        self.cloud = cloud
        self.region = region
        self.public_ipv4 = bastion_ip
        self.private_ipv4 = None
        self.public_ipv6 = None
        self.private_ipv6 = None
        self.interface_ip = bastion_ip
        self.connection_port = 22
        self.connection_type = 'ssh'
        self.heat_stack_outputs = outputs or {}
        self.quota = quota
        self.metadata = {}

    def getQuotaInformation(self):
        """Return quota consumed by this stack.

        This includes the total resources consumed by all instances,
        volumes, and networks within the Heat stack.

        :returns: QuotaInformation object.
        """
        return self.quota

    @property
    def external_id(self):
        """Return the external identifier for this instance.

        :returns: Identifier string.
        """
        return f'stack={self.heat_stack_id}'


class HeatQuotaInstance(statemachine.Instance):
    """Represents an individual cloud resource for quota tracking.

    Yielded by listInstances() so the cleanup worker can classify
    resources as managed vs unmanaged.
    """

    def __init__(self, resource_id, resource_type, metadata, quota):
        super().__init__()
        self.resource_id = resource_id
        self.resource_type = resource_type
        self.metadata = metadata
        self.quota = quota

    def getQuotaInformation(self):
        """Return quota for this individual resource.

        :returns: QuotaInformation object.
        """
        return self.quota

    @property
    def external_id(self):
        """Return the external identifier for this resource.

        :returns: Identifier string.
        """
        return f'{self.resource_type}={self.resource_id}'


class HeatResource(statemachine.Resource):
    """Represents an OpenStack resource for leak detection and cleanup."""

    TYPE_STACK = 'heat-stack'
    # TODO(hjensas): Implement TYPE_IMAGE for Glance image cleanup.

    def __init__(self, metadata, type, id, stack_id=None):
        super().__init__(metadata, type)
        self.id = id
        self.stack_id = stack_id

    @property
    def unique_id(self):
        """Return a unique identifier for this resource.

        :returns: Identifier string.
        """
        return f'{self.type}-{self.id}'
