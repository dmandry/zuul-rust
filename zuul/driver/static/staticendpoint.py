# Copyright 2025 Acme Gating, LLC
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

import logging
import time

from zuul.driver.static.staticmodel import StaticInstance
from zuul.model import QuotaInformation
from zuul.provider import (
    BaseProviderEndpoint,
    statemachine
)

DELETE_RECYCLE_TIME = 300


class StaticDeleteStateMachine(statemachine.StateMachine):
    def __init__(self, provider, node, log):
        self.log = log
        self.node = node
        self.reuse = provider.canReuseNode(node)
        super().__init__(node.delete_state)

    def advance(self):
        if self.state == self.START:
            if self.reuse:
                if time.time() - self.start_time > DELETE_RECYCLE_TIME:
                    # If we are going to reuse the node, we are
                    # probably here due to an error.  Wait some time
                    # before we try re-scanning ssh keys.
                    self.fail()
            else:
                # Otherwise, we can immediately complete the state machine.
                self.fail()

        if self.state == self.COMPLETE:
            self.complete = True

    def fail(self):
        # We always want the same outcome: to transition state, so we
        # do that in this method so that it happens whether or not we
        # succeed, or encounter a timeout or other exception.
        if self.reuse:
            self.node.state = self.node.State.BUILDING
            self.node.state_time = time.time()
        self.state = self.COMPLETE
        self.complete = True


class StaticCreateStateMachine(statemachine.StateMachine):
    def __init__(self, provider, node, label, log):
        self.log = log
        self.provider = provider
        self.node = node
        super().__init__(node.create_state)
        node_config = provider.nodes[node.uuid]
        quota = self.provider.getQuotaForLabel(label)
        instance = StaticInstance(node_config, quota)
        self.node.updateFromInstance(instance)
        self.state = self.COMPLETE
        self.complete = True


class StaticProviderEndpoint(BaseProviderEndpoint):
    def __init__(self, zk_client, driver, connection, system_id):
        name = connection.connection_name
        super().__init__(zk_client, driver, connection, name, system_id)
        self.log = logging.getLogger(f"zuul.static.{self.name}")

    def startEndpoint(self):
        self._running = True
        self.log.debug("Starting static endpoint")

    def stopEndpoint(self):
        self.log.debug("Stopping static endpoint")
        self._running = False

    def postConfig(self, provider):
        pass

    def refreshQuotaLimits(self, update):
        if self.quota_cache.hasLimits() and not update:
            return False
        limits = QuotaInformation()
        self.quota_cache.setLimits(limits)
        return True

    def listInstances(self):
        return []

    def listResources(self, providers):
        return []
