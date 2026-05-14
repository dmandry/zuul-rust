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

"""Heat State Machines for Zuul."""

import time

import openstack.exceptions

from zuul import exceptions
from zuul.provider import statemachine


STACK_CREATE_IN_PROGRESS = 'CREATE_IN_PROGRESS'
STACK_CREATE_COMPLETE = 'CREATE_COMPLETE'
STACK_CREATE_FAILED = 'CREATE_FAILED'
STACK_DELETE_IN_PROGRESS = 'DELETE_IN_PROGRESS'
STACK_DELETE_COMPLETE = 'DELETE_COMPLETE'
STACK_DELETE_FAILED = 'DELETE_FAILED'


class HeatCreateStateMachine(statemachine.StateMachine):
    """State machine for creating a Heat stack."""

    STACK_CREATING_SUBMIT = 'submit creating stack'
    STACK_CREATING = 'creating stack'
    COMPLETE = 'complete'

    def __init__(self, endpoint, node, stack_name, template_content,
                 parameters, tags, timeout, log):
        self.log = log
        self.endpoint = endpoint
        self.node = node
        self.stack_name = stack_name
        self.template_content = template_content
        self.parameters = parameters or {}
        self.tags = tags
        self.timeout = timeout
        self.create_future = None
        super().__init__(node.create_state)

        self.stack_id = node.create_state.get("heat_stack_id")

        # Recovery: we may have crashed after creating the stack
        # but before updating the node state.
        if self.state == self.STACK_CREATING_SUBMIT and self.stack_id:
            stack = self.endpoint._getStack(self.stack_id)
            if stack:
                self.node.heat_stack_id = self.stack_id
                self.node.heat_stack_name = stack.name
                self.state = self.STACK_CREATING
                self.log.info("Recovered stack %s in %s state",
                              self.stack_id, stack.status)

    def advance(self):
        if self.complete:
            return

        if self.state == self.START:
            self.state = self.STACK_CREATING_SUBMIT

        if self.state == self.STACK_CREATING_SUBMIT:
            if not self.create_future:
                self.log.debug("Submitting Heat stack creation: %s",
                               self.stack_name)
                self.create_future = self.endpoint._submitApi(
                    self.endpoint._createStack,
                    self.stack_name,
                    self.template_content,
                    self.parameters,
                    self.tags,
                )

            try:
                stack = self.endpoint._completeApi(self.create_future)
                if stack is None:
                    return

                self.stack_id = stack.id
                self.node.heat_stack_id = stack.id
                self.node.heat_stack_name = stack.name
                self.log.debug("Heat stack %s created with ID %s",
                               stack.name, stack.id)
                self.state = self.STACK_CREATING

            except openstack.exceptions.ConflictException as e:
                self.log.error("Stack creation failed (quota/conflict): %s", e)
                raise exceptions.QuotaException(
                    f"Quota exceeded or conflict creating stack: {e}") from e
            except openstack.exceptions.SDKException as e:
                error_msg = str(e).lower()
                if any(q in error_msg for q in [
                    'quota', 'exceeded', 'limit', 'insufficient'
                ]):
                    self.log.error("Stack creation failed (quota): %s", e)
                    raise exceptions.QuotaException(
                        f"Quota exceeded creating stack: {e}") from e
                self.log.error("Stack creation failed: %s", e)
                raise

        if self.state == self.STACK_CREATING:
            elapsed = time.time() - self.start_time
            if elapsed > self.timeout:
                self.log.error(
                    "Stack %s creation timed out after %d seconds "
                    "(timeout: %d seconds)",
                    self.stack_id, int(elapsed), self.timeout)
                raise exceptions.LaunchStatusException(
                    f"Heat stack creation timed out after {int(elapsed)} "
                    f"seconds (timeout: {self.timeout}s)")

            stack = self.endpoint._getStack(self.stack_id)
            if not stack:
                raise exceptions.LaunchStatusException(
                    f"Stack {self.stack_id} disappeared during creation")

            self.log.debug("Stack %s status: %s (elapsed: %ds)",
                           self.stack_id, stack.status, int(elapsed))

            if stack.status == STACK_CREATE_IN_PROGRESS:
                return

            elif stack.status == STACK_CREATE_COMPLETE:
                self.log.debug("Stack %s creation complete", self.stack_id)
                self.state = self.COMPLETE

            elif stack.status == STACK_CREATE_FAILED:
                reason = stack.stack_status_reason or 'Unknown error'
                self.log.error("Stack %s creation failed: %s",
                               self.stack_id, reason)
                raise exceptions.LaunchStatusException(
                    f"Heat stack creation failed: {reason}")

            else:
                self.log.error("Stack %s in unexpected status: %s",
                               self.stack_id, stack.status)
                raise exceptions.LaunchStatusException(
                    f"Unexpected stack status: {stack.status}")

        if self.state == self.COMPLETE:
            stack = self.endpoint._getStack(self.stack_id)
            if not stack:
                raise exceptions.LaunchStatusException(
                    f"Stack {self.stack_id} not found")

            outputs = {o['output_key']: o['output_value']
                       for o in (stack.outputs or [])}

            self.log.debug("Stack %s outputs: %s", self.stack_id, outputs)

            self.node.heat_stack_outputs = outputs
            self.node.heat_stack_status = stack.status

            self._updateNode(stack, outputs)
            self.complete = True

    def _updateNode(self, stack, outputs):
        if not outputs.get('bastion_ip'):
            raise exceptions.LaunchStatusException(
                f"Stack {stack.id} missing required 'bastion_ip' output. "
                f"Heat template must provide this output with the floating "
                f"IP address for SSH access.")
        instance = self.endpoint._getInstance(
            stack, self.node.quota, outputs)
        self.node.updateFromInstance(instance)

        # Collect physical resource IDs from the stack so the cleanup
        # worker can classify project-wide cloud resources as managed
        # vs unmanaged for accurate quota tracking.
        resource_ids = self.endpoint._listStackResources(stack.id)
        self.node.heat_physical_resource_ids = resource_ids
        self.log.debug(
            "Stack %s has %d tracked physical resources",
            stack.id, len(resource_ids))

        # Expose stack outputs in node_properties so they are available
        # as Ansible variables via nodepool.node_properties.heat_stack_outputs
        self.node.node_properties['heat_stack_outputs'] = outputs

        # Large and no longer needed after stack creation.
        self.node.template_config.pop('heat_template_content', None)

    def toDict(self):
        """Serialize state to a dict for ZooKeeper persistence."""
        data = super().toDict()
        if self.stack_id:
            data['heat_stack_id'] = self.stack_id
        return data


class HeatDeleteStateMachine(statemachine.StateMachine):
    """State machine for deleting a Heat stack."""

    STACK_DELETING_SUBMIT = 'submit deleting stack'
    STACK_DELETING = 'deleting stack'
    COMPLETE = 'complete'

    def __init__(self, endpoint, node, log):
        self.log = log
        self.endpoint = endpoint
        self.node = node
        self.delete_future = None
        super().__init__(node.delete_state)

        self.stack_id = node.heat_stack_id
        if not self.stack_id:
            self.log.warning("Node %s has no stack ID, nothing to delete",
                             node.uuid)
            self.state = self.COMPLETE

    def advance(self):
        if self.complete:
            return

        if self.state == self.START:
            self.state = self.STACK_DELETING_SUBMIT

        if self.state == self.STACK_DELETING_SUBMIT:
            if not self.delete_future:
                self.log.debug("Submitting Heat stack deletion: %s",
                               self.stack_id)
                self.delete_future = self.endpoint._submitApi(
                    self.endpoint._deleteStack,
                    self.stack_id,
                )

            try:
                result = self.endpoint._completeApi(self.delete_future)
                if result is None:
                    return

                self.state = self.STACK_DELETING

            except openstack.exceptions.NotFoundException:
                self.log.debug("Stack %s already deleted", self.stack_id)
                self.state = self.COMPLETE
                return
            except openstack.exceptions.SDKException as e:
                self.log.error("Stack deletion failed: %s", e)
                raise

        if self.state == self.STACK_DELETING:
            stack = self.endpoint._getStack(self.stack_id)
            if not stack:
                self.log.debug("Stack %s deleted successfully",
                               self.stack_id)
                self.state = self.COMPLETE
                return

            self.log.debug("Stack %s status: %s", self.stack_id, stack.status)

            if stack.status == STACK_DELETE_IN_PROGRESS:
                return

            elif stack.status == STACK_DELETE_COMPLETE:
                self.log.debug("Stack %s deletion complete", self.stack_id)
                self.state = self.COMPLETE

            elif stack.status == STACK_DELETE_FAILED:
                reason = stack.stack_status_reason or 'Unknown error'
                self.log.error("Stack %s deletion failed: %s",
                               self.stack_id, reason)
                raise exceptions.LaunchStatusException(
                    f"Heat stack deletion failed: {reason}")

            else:
                self.log.warning("Stack %s in status %s, retrying delete",
                                 self.stack_id, stack.status)
                self.state = self.STACK_DELETING_SUBMIT
                self.delete_future = None

        if self.state == self.COMPLETE:
            self.complete = True
