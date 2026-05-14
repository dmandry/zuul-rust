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

"""Heat State Machines for Zuul.

This module provides state machine implementations for Heat stack lifecycle
operations (create and delete). State machines manage the asynchronous
workflows for interacting with the OpenStack Heat API.
"""

import openstack.exceptions

from zuul import exceptions
from zuul.provider import statemachine


# Heat Stack Status Constants
# These match the status values returned by OpenStack Heat API
STACK_CREATE_IN_PROGRESS = 'CREATE_IN_PROGRESS'
STACK_CREATE_COMPLETE = 'CREATE_COMPLETE'
STACK_CREATE_FAILED = 'CREATE_FAILED'
STACK_DELETE_IN_PROGRESS = 'DELETE_IN_PROGRESS'
STACK_DELETE_COMPLETE = 'DELETE_COMPLETE'
STACK_DELETE_FAILED = 'DELETE_FAILED'


class HeatCreateStateMachine(statemachine.StateMachine):
    """State machine for creating a Heat stack.

    This state machine manages the workflow of creating an OpenStack Heat
    stack, waiting for it to complete, and extracting the necessary
    information (bastion IP, outputs) for Zuul to use the stack as a node.

    The stack creation follows these states:
    1. START: Initial state
    2. STACK_CREATING_SUBMIT: Submit the stack creation API call
    3. STACK_CREATING: Wait for stack to reach CREATE_COMPLETE status
    4. COMPLETE: Stack is ready, extract bastion IP from outputs

    If the stack creation fails, the state machine will raise an exception
    and the stack will be cleaned up by the delete state machine.
    """

    STACK_CREATING_SUBMIT = 'submit creating stack'
    STACK_CREATING = 'creating stack'
    COMPLETE = 'complete'

    def __init__(self, endpoint, node, stack_name, template_content,
                 parameters, tags, log):
        """Initialize the Heat create state machine.

        :param endpoint: HeatProviderEndpoint instance
        :param node: HeatProviderNode being created
        :param stack_name: Name for the Heat stack
        :param template_content: Heat template YAML content (string)
        :param parameters: Dict of template parameters
        :param tags: List of tag strings in "key=value" format for the stack
        :param log: Logger instance
        """
        self.log = log
        self.endpoint = endpoint
        self.node = node
        self.stack_name = stack_name
        self.template_content = template_content
        self.parameters = parameters or {}
        self.tags = tags
        self.create_future = None
        super().__init__(node.create_state)

        # Track the stack ID once created
        self.stack_id = node.create_state.get("heat_stack_id")

        # If we already have a stack ID, check if it exists
        if self.state == self.STACK_CREATING_SUBMIT and self.stack_id:
            # Recovery: We may have crashed after creating the stack
            # but before updating the node state
            stack = self.endpoint._getStack(self.stack_id)
            if stack:
                self.node.heat_stack_id = self.stack_id
                self.node.heat_stack_name = stack.name
                self.state = self.STACK_CREATING
                self.log.info("Recovered stack %s in %s state",
                              self.stack_id, stack.status)

    def advance(self):
        """Advance the state machine to the next state.

        This method is called repeatedly by the launcher until the state
        machine reaches COMPLETE or raises an exception.
        """
        if self.complete:
            return

        if self.state == self.START:
            self.state = self.STACK_CREATING_SUBMIT

        if self.state == self.STACK_CREATING_SUBMIT:
            if not self.create_future:
                self.log.info("Submitting Heat stack creation: %s",
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
                    # API call still in progress
                    return

                # Stack creation submitted successfully
                self.stack_id = stack.id
                self.node.heat_stack_id = stack.id
                self.node.heat_stack_name = stack.name
                self.log.info("Heat stack %s created with ID %s",
                              stack.name, stack.id)
                self.state = self.STACK_CREATING

            except openstack.exceptions.ConflictException as e:
                # HTTP 409 Conflict typically means quota exceeded
                self.log.error("Stack creation failed (quota/conflict): %s", e)
                raise exceptions.QuotaException(
                    f"Quota exceeded or conflict creating stack: {e}")
            except openstack.exceptions.SDKException as e:
                # Fallback: check error message for quota-related keywords
                error_msg = str(e).lower()
                if any(q in error_msg for q in [
                    'quota', 'exceeded', 'limit', 'insufficient'
                ]):
                    self.log.error("Stack creation failed (quota): %s", e)
                    raise exceptions.QuotaException(
                        f"Quota exceeded creating stack: {e}")
                # Other SDK errors re-raised as-is
                self.log.error("Stack creation failed: %s", e)
                raise

        if self.state == self.STACK_CREATING:
            # Poll the stack status
            stack = self.endpoint._getStack(self.stack_id)
            if not stack:
                raise exceptions.LaunchStatusException(
                    f"Stack {self.stack_id} disappeared during creation")

            self.log.debug("Stack %s status: %s", self.stack_id, stack.status)

            if stack.status == STACK_CREATE_IN_PROGRESS:
                # Still creating, check again later
                return

            elif stack.status == STACK_CREATE_COMPLETE:
                # Stack creation succeeded
                self.log.info("Stack %s creation complete", self.stack_id)
                self.state = self.COMPLETE

            elif stack.status == STACK_CREATE_FAILED:
                # Stack creation failed
                reason = stack.stack_status_reason or 'Unknown error'
                self.log.error("Stack %s creation failed: %s",
                               self.stack_id, reason)
                raise exceptions.LaunchStatusException(
                    f"Heat stack creation failed: {reason}")

            else:
                # Unexpected status
                self.log.error("Stack %s in unexpected status: %s",
                               self.stack_id, stack.status)
                raise exceptions.LaunchStatusException(
                    f"Unexpected stack status: {stack.status}")

        if self.state == self.COMPLETE:
            # Get the final stack details and outputs
            stack = self.endpoint._getStack(self.stack_id)
            if not stack:
                raise exceptions.LaunchStatusException(
                    f"Stack {self.stack_id} not found")

            # Extract outputs from the stack
            outputs = {}
            for output in (stack.outputs or []):
                outputs[output['output_key']] = output['output_value']

            self.log.info("Stack %s outputs: %s", self.stack_id, outputs)

            # Store stack outputs in the node
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

        # Expose stack outputs in node_properties so they are available
        # as Ansible variables via nodepool.node_properties.heat_stack_outputs
        self.node.node_properties['heat_stack_outputs'] = outputs

        # Remove template content from node_properties now that the stack
        # has been created; it can be large and is not needed by the executor.
        self.node.node_properties.pop('heat_template_content', None)

    def toDict(self):
        """Serialize the state machine state to a dictionary.

        This is used to persist the state machine state to ZooKeeper so
        it can be recovered if the launcher crashes.

        :returns: Dict with state machine state
        """
        data = super().toDict()
        if self.stack_id:
            data['heat_stack_id'] = self.stack_id
        return data


class HeatDeleteStateMachine(statemachine.StateMachine):
    """State machine for deleting a Heat stack.

    This state machine manages the workflow of deleting an OpenStack Heat
    stack and waiting for the deletion to complete. It's simpler than the
    create state machine since there's less that can go wrong.

    The stack deletion follows these states:
    1. START: Initial state
    2. STACK_DELETING_SUBMIT: Submit the stack deletion API call
    3. STACK_DELETING: Wait for stack to be deleted
    4. COMPLETE: Stack is deleted

    If the stack deletion fails, the state machine will raise an exception
    and the stack will be marked as leaked for manual cleanup.
    """

    STACK_DELETING_SUBMIT = 'submit deleting stack'
    STACK_DELETING = 'deleting stack'
    COMPLETE = 'complete'

    def __init__(self, endpoint, node, log):
        """Initialize the Heat delete state machine.

        :param endpoint: HeatProviderEndpoint instance
        :param node: HeatProviderNode being deleted
        :param log: Logger instance
        """
        self.log = log
        self.endpoint = endpoint
        self.node = node
        self.delete_future = None
        super().__init__(node.delete_state)

        self.stack_id = node.heat_stack_id
        if not self.stack_id:
            # No stack to delete (may have never been created)
            self.log.warning("Node %s has no stack ID, nothing to delete",
                             node.uuid)
            self.state = self.COMPLETE

    def advance(self):
        """Advance the state machine to the next state.

        This method is called repeatedly by the launcher until the state
        machine reaches COMPLETE or raises an exception.
        """
        if self.complete:
            return

        if self.state == self.START:
            self.state = self.STACK_DELETING_SUBMIT

        if self.state == self.STACK_DELETING_SUBMIT:
            if not self.delete_future:
                self.log.info("Submitting Heat stack deletion: %s",
                              self.stack_id)
                self.delete_future = self.endpoint._submitApi(
                    self.endpoint._deleteStack,
                    self.stack_id,
                )

            try:
                result = self.endpoint._completeApi(self.delete_future)
                if result is None:
                    # API call still in progress
                    return

                self.log.info("Heat stack %s deletion submitted",
                              self.stack_id)
                self.state = self.STACK_DELETING

            except openstack.exceptions.NotFoundException:
                # Stack already deleted - this is success
                self.log.info("Stack %s already deleted", self.stack_id)
                self.state = self.COMPLETE
                return
            except openstack.exceptions.SDKException as e:
                # Other OpenStack errors are failures
                self.log.error("Stack deletion failed: %s", e)
                raise

        if self.state == self.STACK_DELETING:
            # Poll the stack status
            stack = self.endpoint._getStack(self.stack_id)
            if not stack:
                # Stack is gone (deleted)
                self.log.info("Stack %s deleted successfully", self.stack_id)
                self.state = self.COMPLETE
                return

            self.log.debug("Stack %s status: %s", self.stack_id, stack.status)

            if stack.status == STACK_DELETE_IN_PROGRESS:
                # Still deleting, check again later
                return

            elif stack.status == STACK_DELETE_COMPLETE:
                # Stack deletion succeeded (though it should be gone)
                self.log.info("Stack %s deletion complete", self.stack_id)
                self.state = self.COMPLETE

            elif stack.status == STACK_DELETE_FAILED:
                # Stack deletion failed
                reason = stack.stack_status_reason or 'Unknown error'
                self.log.error("Stack %s deletion failed: %s",
                               self.stack_id, reason)
                raise exceptions.LaunchStatusException(
                    f"Heat stack deletion failed: {reason}")

            else:
                # Stack still exists but not in delete state - try again
                self.log.warning("Stack %s in status %s, retrying delete",
                                 self.stack_id, stack.status)
                self.state = self.STACK_DELETING_SUBMIT
                self.delete_future = None

        if self.state == self.COMPLETE:
            self.complete = True
