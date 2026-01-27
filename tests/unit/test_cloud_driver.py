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

import concurrent.futures
import time

import fixtures
import testtools
from kazoo.exceptions import NoNodeError

from zuul import model
from zuul.launcher.client import LauncherClient

from tests.base import (
    ZuulTestCase,
    iterate_timeout,
)


class BaseCloudDriverTest(ZuulTestCase):
    cloud_test_connection_type = 'ssh'
    cloud_test_image_format = ''
    cloud_test_provider_name = ''
    # The number of instances to expect when nothing is running
    cloud_test_min_instances = 0

    def setUp(self):
        self.useFixture(fixtures.MonkeyPatch(
            'zuul.launcher.server.NodescanRequest.FAKE', True))
        super().setUp()

    def _getProvider(self):
        # Use the launcher provider so that we're using the same ttl
        # method caches.
        for provider in self.launcher.tenant_providers['tenant-one']:
            if provider.name == self.cloud_test_provider_name:
                return provider

    def _assertProviderNodeAttributes(self, pnode):
        self.assertEqual(pnode.connection_type,
                         self.cloud_test_connection_type)
        self.assertIsNotNone(pnode.interface_ip)

    def _test_node_lifecycle(self, label):
        # Call this in a test to run a node lifecycle
        for _ in iterate_timeout(
                30, "scheduler and launcher to have the same layout"):
            if (self.scheds.first.sched.local_layout_state.get("tenant-one") ==
                self.launcher.local_layout_state.get("tenant-one")):
                break
        provider = self._getProvider()
        nodeset = model.NodeSet()
        nodeset.addNode(model.Node("node", label))

        ctx = self.createZKContext(None)
        request = self.requestNodes([n.label for n in nodeset.getNodes()])

        client = LauncherClient(self.zk_client, None)
        request = client.getRequest(request.uuid)

        self.assertEqual(request.state, model.NodesetRequest.State.FULFILLED)
        self.assertEqual(len(request.nodes), 1)

        client.acceptNodeset(request, nodeset)
        self.waitUntilSettled()

        with testtools.ExpectedException(NoNodeError):
            # Request should be gone
            request.refresh(ctx)

        for node in nodeset.getNodes():
            pnode = node._provider_node
            self.assertIsNotNone(pnode)
            self.assertTrue(pnode.hasLock())
            self._assertProviderNodeAttributes(pnode)

        for _ in iterate_timeout(10, "instances to appear"):
            if (len(list(provider.endpoint.listInstances())) >
                self.cloud_test_min_instances):
                break
        client.useNodeset(nodeset)
        self.waitUntilSettled()

        for node in nodeset.getNodes():
            pnode = node._provider_node
            self.assertTrue(pnode.hasLock())
            self.assertEqual(pnode.state, pnode.State.IN_USE)

        client.returnNodeset(nodeset)
        self.waitUntilSettled()

        for node in nodeset.getNodes():
            pnode = node._provider_node
            self.assertFalse(pnode.hasLock())
            self.assertEqual(pnode.state, pnode.State.USED)

            for _ in iterate_timeout(60, "node to be deleted"):
                try:
                    pnode.refresh(ctx)
                except NoNodeError:
                    break

        # Iterate here because the aws driver (at least) performs
        # delayed async deletes.
        for _ in iterate_timeout(60, "instances to be deleted"):
            if (len(list(provider.endpoint.listInstances())) ==
                self.cloud_test_min_instances):
                break

    def _test_quota(self, label):
        for _ in iterate_timeout(
                30, "scheduler and launcher to have the same layout"):
            if (self.scheds.first.sched.local_layout_state.get("tenant-one") ==
                self.launcher.local_layout_state.get("tenant-one")):
                break
        provider = self._getProvider()
        nodeset1 = model.NodeSet()
        nodeset1.addNode(model.Node("node", label))

        nodeset2 = model.NodeSet()
        nodeset2.addNode(model.Node("node", label))

        ctx = self.createZKContext(None)
        request1 = self.requestNodes([n.label for n in nodeset1.getNodes()])
        request2 = self.requestNodes(
            [n.label for n in nodeset2.getNodes()],
            timeout=None)

        client = LauncherClient(self.zk_client, None)
        request1 = client.getRequest(request1.uuid)

        self.assertEqual(request1.state, model.NodesetRequest.State.FULFILLED)
        self.assertEqual(len(request1.nodes), 1)

        client.acceptNodeset(request1, nodeset1)
        client.useNodeset(nodeset1)

        # We should still be waiting on request2.

        # TODO: This is potentially racy (but only producing
        # false-negatives) and also slow. We should find a way to
        # determine if the launcher had paused a provider.
        with testtools.ExpectedException(Exception):
            self.waitForNodeRequest(request2, 10)
        request2 = client.getRequest(request2.uuid)
        self.assertEqual(request2.state, model.NodesetRequest.State.REQUESTED)

        client.returnNodeset(nodeset1)
        self.waitUntilSettled()

        for node in nodeset1.getNodes():
            pnode = node._provider_node
            for _ in iterate_timeout(60, "node to be deleted"):
                try:
                    pnode.refresh(ctx)
                except NoNodeError:
                    break

        self.waitForNodeRequest(request2, 10)
        request2 = client.getRequest(request2.uuid)
        self.assertEqual(request2.state, model.NodesetRequest.State.FULFILLED)
        self.assertEqual(len(request2.nodes), 1)

        client.acceptNodeset(request2, nodeset2)
        client.useNodeset(nodeset2)
        client.returnNodeset(nodeset2)
        self.waitUntilSettled()

        # Iterate here because the aws driver (at least) performs
        # delayed async deletes.
        for _ in iterate_timeout(60, "instances to be deleted"):
            if (len(list(provider.endpoint.listInstances())) ==
                self.cloud_test_min_instances):
                break

    def _test_diskimage(self, expected_uploads=1):
        self.addImageBuildEvent(
            'tenant-one',
            'review.example.com/org/common-config',
            'master',
            ['debian-local'],
        )
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='build-debian-local-image', result='SUCCESS'),
        ], ordered=False)

        name = 'review.example.com%2Forg%2Fcommon-config/debian-local'
        artifacts = self.launcher.image_build_registry.\
            getArtifactsForImage(name)
        self.assertEqual(1, len(artifacts))
        self.assertEqual(self.cloud_test_image_format, artifacts[0].format)
        self.assertTrue(artifacts[0].validated)
        uploads = self.launcher.image_upload_registry.getUploadsForImage(
            name)
        self.assertEqual(expected_uploads, len(uploads))
        for x in range(expected_uploads):
            self.assertEqual(artifacts[0].uuid, uploads[x].artifact_uuid)
            self.assertIsNotNone(uploads[x].external_id)
            self.assertTrue(uploads[x].validated)

    def _drive_state_machine(self, ctx, node, future_names, provider, create):
        executed_future_names = set()
        next_future_names = set()
        for _ in iterate_timeout(60, "create state machine to complete"):
            with node.activeContext(ctx):
                # Re-create the SM from the state in ZK
                if create:
                    sm = provider.getCreateStateMachine(
                        node, None, self.log)
                    node.create_state_machine = sm
                else:
                    sm = provider.getDeleteStateMachine(
                        node, self.log)
                    node.delete_state_machine = sm

                with self._block_futures():
                    sm.advance()
                # If there are pending futures we will try to re-create
                # the SM once from the state and then advance it once
                # more so the futures can complete.
                pending_futures = {
                    name: future
                    for name in future_names
                    if (future := getattr(sm, name, None))
                }
                this_futures = {
                    name: future for (name, future)
                    in pending_futures.items()
                    if name in next_future_names
                }
                if this_futures:
                    concurrent.futures.wait(this_futures.values())
                    executed_future_names.update(this_futures.keys())
                    sm.advance()
                next_future_names = (set(pending_futures.keys()) -
                                     executed_future_names)
            if sm.complete:
                break
            # Avoid busy-looping as we have to wait for the TTL
            # cache to expire.
            time.sleep(0.5)

    def _test_state_machines(self, label_name, provider_name,
                             node_class, future_names):
        # Stop the launcher main loop, so we can drive the state machine
        # on our own.
        self.waitUntilSettled()
        self.launcher._running = False
        self.launcher.wake_event.set()
        self.launcher.launcher_thread.join()

        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout
        provider = layout.providers[provider_name]
        # Start the endpoint since we're going to use the scheduler's endpoint.
        provider.getEndpoint().start()

        with self.createZKContext(None) as ctx:
            node = node_class.new(ctx, label=label_name,
                                  uuid='1234',
                                  tags={'zuul_node_uuid': '1234'})
            self._drive_state_machine(ctx, node, future_names, provider, True)
            self._drive_state_machine(ctx, node, future_names, provider, False)
