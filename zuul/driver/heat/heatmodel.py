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

    Each node represents a single Heat stack that provides a bastion/jump
    host as the Zuul node. The stack may contain multiple internal instances
    that are accessible through the bastion.
    """

    def __init__(self):
        super().__init__()
        self._set(
            heat_stack_id=None,
            heat_stack_name=None,
            heat_stack_status=None,
            heat_template_path=None,
            heat_stack_outputs=None,
        )

    def getDriverData(self):
        """Return data needed to reconstruct state machines after restart.

        This data is persisted to ZooKeeper and used to resume operations
        if the launcher restarts mid-operation. The data includes:

        - heat_stack_id: OpenStack Heat stack UUID
        - heat_stack_name: Human-readable stack name
        - heat_stack_status: Last known status (CREATE_COMPLETE, etc.)
        - heat_template_path: Path reference from node-properties
        - heat_stack_outputs: Dictionary of stack outputs from Heat

        State machines (create/delete) use this data to resume their
        work from the appropriate state without losing context.
        """
        return dict(
            heat_stack_id=self.heat_stack_id,
            heat_stack_name=self.heat_stack_name,
            heat_stack_status=self.heat_stack_status,
            heat_template_path=self.heat_template_path,
            heat_stack_outputs=self.heat_stack_outputs,
        )


class HeatInstance(statemachine.Instance):
    """Represents a Heat stack instance (the bastion node).

    This is returned by the create state machine upon successful
    stack creation. It contains the information needed to populate
    the Zuul node.

    Architecture: One Heat stack = One Zuul node. The stack may contain
    multiple VMs, but only the bastion (jump host) is exposed to Zuul.
    The bastion must have a floating IP for external access. Internal
    instances are accessed by job playbooks via SSH ProxyJump through
    the bastion.

    The Heat template must provide a 'bastion_ip' output with the
    floating IP address of the bastion host.
    """

    def __init__(self, cloud, region, stack_id, bastion_ip, outputs, quota):
        super().__init__()
        self.heat_stack_id = stack_id
        self.cloud = cloud
        self.region = region
        # The bastion is the publicly accessible entry point
        self.public_ipv4 = bastion_ip
        self.private_ipv4 = None
        self.public_ipv6 = None
        self.private_ipv6 = None
        self.interface_ip = bastion_ip
        self.connection_port = 22
        self.connection_type = 'ssh'
        # Stack outputs may contain additional info (internal IPs, etc.)
        self.heat_stack_outputs = outputs or {}
        self.quota = quota
        self.metadata = {}

    def getQuotaInformation(self):
        """Return quota consumed by this stack.

        This includes the total resources consumed by all instances,
        volumes, and networks within the Heat stack. The quota is
        calculated during stack creation by parsing the Heat template
        and querying Nova for flavor details (vCPUs, RAM).

        :returns: QuotaInformation object with cores, ram, volumes, etc.
        """
        return self.quota

    @property
    def external_id(self):
        """Return the external identifier for this instance.

        Format: 'stack=<stack_id>'
        Example: 'stack=abc123-def456-789'

        This ID is used for logging and tracking the stack in
        external systems (OpenStack Heat).
        """
        return f'stack={self.heat_stack_id}'


class HeatResource(statemachine.Resource):
    """Represents an OpenStack resource managed by the Heat driver.

    This class is used for resource tracking and cleanup. Resources
    can be in two states:

    1. Active: Heat stacks currently in use by running jobs
    2. Leaked: Resources whose corresponding nodes/jobs no longer exist

    The launcher's cleanup system uses listResources() to enumerate
    all resources with zuul_system_id tags, then determines which are
    leaked by checking if their zuul_node_uuid still exists in ZooKeeper.

    Currently tracks Heat orchestration stacks. Additional resource types
    (images, volumes, networks) can be added in the future as needed.
    """

    TYPE_STACK = 'heat-stack'
    # TODO(hjensas): Implement TYPE_IMAGE for Glance image cleanup.
    # This would require:
    # 1. Add image listing to listResources() in heatendpoint.py
    # 2. Add image deletion handling to deleteResource()
    # 3. Document tagging requirements for test-created images

    def __init__(self, metadata, type, id, stack_id=None):
        """Initialize a Heat resource.

        :param metadata: Dict with zuul_system_id, zuul_node_uuid, etc.
        :param type: Resource type (e.g., 'heat-stack')
        :param id: OpenStack resource ID
        :param stack_id: Optional Heat stack ID that created this resource
        """
        super().__init__(metadata, type)
        self.id = id
        self.stack_id = stack_id

    @property
    def unique_id(self):
        """Return a unique identifier for this resource.

        Format: '<type>-<id>'
        Example: 'heat-stack-abc123-def456'

        This ID is used to track resources across cleanup operations
        and ensure idempotent deletion.
        """
        return f'{self.type}-{self.id}'
