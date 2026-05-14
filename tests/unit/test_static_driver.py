# Copyright 2024-2025 Acme Gating, LLC
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

from collections import Counter
import time
from unittest import mock

import testtools
from kazoo.exceptions import NoNodeError

from zuul import model
from zuul.launcher.client import LauncherClient
import zuul.launcher.server
import zuul.driver.static.staticendpoint
from tests.base import (
    ZuulTestCase,
    okay_tracebacks,
    iterate_timeout,
    simple_layout,
)
from tests.fake_nodescan import NodescanFixture


class TestStaticDriver(ZuulTestCase):
    config_file = 'zuul-connections-nodepool.conf'

    def _assertProviderNodeAttributes(self, pnode):
        self.assertEqual(pnode.connection_type, 'ssh')
        self.assertEqual(pnode.username, 'vinz')
        self.assertIsNotNone(pnode.interface_ip)
        self.assertIsNone(pnode.cloud)
        self.assertIsNone(pnode.region)
        self.assertTrue(pnode.reuse)
        if checks := self.test_config.driver.static.get('node_checks'):
            checks(self, pnode)

    def setUp(self):
        self.useFixture(NodescanFixture())
        super().setUp()

    def _getProvider(self):
        # Use the launcher provider so that we're using the same ttl
        # method caches.
        for provider in self.launcher.tenant_providers['tenant-one']:
            if provider.name == 'static-main':
                return provider

    def _waitForRegistration(self):
        for _ in iterate_timeout(60, "nodes to be ready"):
            nodes = self.launcher.api.nodes_cache.getItems()
            if (len(nodes) and
                all(n.state in (n.State.READY, n.State.SLOT_HOST)
                    for n in nodes)):
                break

    def _test_static_node_lifecycle(self, label):
        # Similar to the cloud test, but we don't deal with "instances"
        self._waitForRegistration()
        for _ in iterate_timeout(
                30, "scheduler and launcher to have the same layout"):
            if (self.scheds.first.sched.local_layout_state.get("tenant-one") ==
                self.launcher.local_layout_state.get("tenant-one")):
                break
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
            self.assertTrue(pnode.state in (pnode.State.USED,
                                            pnode.State.READY))

            for _ in iterate_timeout(60, "node to be marked ready"):
                pnode.refresh(ctx)
                if pnode.state == pnode.State.READY:
                    break

        # Make sure it can be reused
        request = self.requestNodes([label])
        self.assertEqual(request.state, model.NodesetRequest.State.FULFILLED)
        self.assertEqual(len(request.nodes), 1)

    @simple_layout('layouts/static/nodepool.yaml', enable_nodepool=True)
    def test_static_node_lifecycle(self):
        self._test_static_node_lifecycle('debian-normal')

    @simple_layout('layouts/static/nodepool.yaml', enable_nodepool=True)
    @okay_tracebacks('_checkNodescanRequest')
    def test_static_failure(self):
        # Test failure of a single static node
        ctx = self.createZKContext(None)

        def my_advance(*args, **kw):
            raise Exception("Test exception")

        with mock.patch.object(
            zuul.launcher.server.NodescanRequest, 'advance', my_advance
        ):
            request = self.requestNodes(["debian-normal"], timeout=None)

            for _ in iterate_timeout(60, "nodes to be marked failed"):
                nodes = self.launcher.api.nodes_cache.getItems()
                states = Counter(n.state for n in nodes)
                if states['failed'] == 1:
                    break

        # Node scanning happens normally now
        with mock.patch.object(
            zuul.driver.static.staticendpoint, 'DELETE_RECYCLE_TIME', 0
        ):
            for _ in iterate_timeout(60, "nodes to be marked ready"):
                request2 = self.launcher.api.getNodesetRequest(request.uuid)
                nodes = self.launcher.api.nodes_cache.getItems()
                states = Counter(n.state for n in nodes)
                if (states['ready'] == 1 and
                    not request2.is_locked):
                    break

        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertEqual(1, len(nodes))
        main = nodes[0]
        # Get a copy so we're not modifying the launcher's
        main = model.ProviderNode.fromZK(ctx, path=main.getPath())
        self.assertEqual(main.State.READY, main.state)
        self.assertEqual(request2.uuid, main.request_id)

        request.delete(ctx)
        self.waitUntilSettled()

        for _ in iterate_timeout(60, "nodes to be unassigned"):
            nodes = self.launcher.api.nodes_cache.getItems()
            if not any(x.request_id for x in nodes):
                break

    @simple_layout('layouts/static/subnodes.yaml', enable_nodepool=True)
    def test_static_subnodes(self):
        self._waitForRegistration()
        ctx = self.createZKContext(None)
        request = self.requestNodes(['debian-normal'])
        self.assertEqual(request.State.FULFILLED, request.state)

        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertEqual(3, len(nodes))
        # Get a list with the main node first and the subnodes last
        nodes.sort(key=lambda x: len(x.subnodes))
        nodes.reverse()
        main = nodes[0]
        sub1 = nodes[1]
        sub2 = nodes[2]
        # Get a copy so we're not modifying the launcher's
        main = model.ProviderNode.fromZK(ctx, path=main.getPath())
        sub1 = model.ProviderNode.fromZK(ctx, path=sub1.getPath())
        sub2 = model.ProviderNode.fromZK(ctx, path=sub2.getPath())
        self.assertIsNone(main.main_node_id)
        self.assertEqual(main.uuid, sub1.main_node_id)
        self.assertEqual(main.uuid, sub2.main_node_id)
        self.assertEqual(set([sub1.uuid, sub2.uuid]), set(main.subnodes))
        self.assertEqual([], sub1.subnodes)
        self.assertEqual([], sub2.subnodes)
        self.assertEqual(main.State.SLOT_HOST, main.state)
        self.assertEqual(sub1.State.READY, sub1.state)
        self.assertEqual(sub2.State.READY, sub2.state)

        with sub1.locked(ctx):
            sub1.refresh(ctx)
            with sub1.activeContext(ctx):
                sub1.assign(ctx, request_id="dne", tenant_name="test")
                # Modify the request so that the max zxid of the
                # request cache is later than the node assignment
                # (since the "dne" request does not exist).
                with request.locked(ctx), request.activeContext(ctx):
                    request.priority += 1
                sub1.setState(sub1.State.USED)

        for _ in iterate_timeout(60, "sub1 to be marked ready"):
            sub1.refresh(ctx)
            if sub1.state == sub1.State.READY:
                break

        # Main and sub2 should still exist
        main.refresh(ctx)
        sub2.refresh(ctx)

        with sub2.locked(ctx):
            sub2.refresh(ctx)
            with sub2.activeContext(ctx):
                sub2.unassign(ctx)
                sub2.assign(ctx, request_id="dne", tenant_name="test")
                # Modify the request so that the max zxid of the
                # request cache is later than the node assignment
                # (since the "dne" request does not exist).
                with request.locked(ctx), request.activeContext(ctx):
                    request.priority += 1
                sub2.setState(sub2.State.USED)

        for _ in iterate_timeout(60, "sub2 to be marked ready"):
            sub2.refresh(ctx)
            if sub2.state == sub2.State.READY:
                break

    @simple_layout('layouts/static/subnodes.yaml', enable_nodepool=True)
    @okay_tracebacks('_checkNodescanRequest')
    def test_static_subnodes_failure(self):
        # Test a node failure
        ctx = self.createZKContext(None)

        def my_advance(*args, **kw):
            raise Exception("Test exception")

        with mock.patch.object(
            zuul.launcher.server.NodescanRequest, 'advance', my_advance
        ):
            request = self.requestNodes(["debian-normal"], timeout=None)

            for _ in iterate_timeout(60, "nodes to be marked failed"):
                nodes = self.launcher.api.nodes_cache.getItems()
                states = Counter(n.state for n in nodes)
                if states['failed'] == 3:
                    break

        # Node scanning happens normally now
        with mock.patch.object(
            zuul.driver.static.staticendpoint, 'DELETE_RECYCLE_TIME', 0
        ):
            for _ in iterate_timeout(60, "nodes to be marked ready"):
                request2 = self.launcher.api.getNodesetRequest(request.uuid)
                nodes = self.launcher.api.nodes_cache.getItems()
                states = Counter(n.state for n in nodes)
                if (states['ready'] == 2 and states['slot-host'] == 1 and
                    not request2.is_locked):
                    break

        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertEqual(3, len(nodes))
        # Get a list with the main node first and the subnode last
        nodes.sort(key=lambda x: len(x.subnodes))
        nodes.reverse()
        main = nodes[0]
        sub1 = nodes[1]
        sub2 = nodes[2]
        # Get a copy so we're not modifying the launcher's
        main = model.ProviderNode.fromZK(ctx, path=main.getPath())
        sub1 = model.ProviderNode.fromZK(ctx, path=sub1.getPath())
        sub2 = model.ProviderNode.fromZK(ctx, path=sub2.getPath())
        self.assertIsNone(main.main_node_id)
        self.assertEqual(main.uuid, sub1.main_node_id)
        self.assertEqual(main.uuid, sub2.main_node_id)
        self.assertEqual(2, len(main.subnodes))
        self.assertIn(sub1.uuid, main.subnodes)
        self.assertIn(sub2.uuid, main.subnodes)
        self.assertEqual([], sub1.subnodes)
        self.assertEqual([], sub2.subnodes)
        self.assertEqual(main.State.SLOT_HOST, main.state)
        self.assertEqual(sub1.State.READY, sub1.state)
        self.assertEqual(sub2.State.READY, sub2.state)
        if (sub1.request_id == request.uuid):
            self.assertIsNone(sub2.request_id)
        elif (sub2.request_id == request.uuid):
            self.assertIsNone(sub1.request_id)
        else:
            self.assertTrue(False, "one of the subnodes must be assigned")

        request.delete(ctx)
        self.waitUntilSettled()

        for _ in iterate_timeout(60, "nodes to be unassigned"):
            nodes = self.launcher.api.nodes_cache.getItems()
            if not any(x.request_id for x in nodes):
                break

    @simple_layout('layouts/static/subnodes.yaml', enable_nodepool=True)
    @okay_tracebacks('_checkNodescanRequest')
    def test_static_subnodes_reuse_failure(self):
        # Test that if we detect a failure in a subnode after it is
        # used once that we mark it and the main node as failed for
        # later deletion.
        ctx = self.createZKContext(None)
        request = self.requestNodes(['debian-normal'])
        self.assertEqual(request.State.FULFILLED, request.state)

        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertEqual(3, len(nodes))
        # Get a list with the main node first and the subnode last
        nodes.sort(key=lambda x: len(x.subnodes))
        nodes.reverse()
        main = nodes[0]
        self.assertIsNone(main.main_node_id)
        subnodes = {x.uuid: x for x in nodes[1:]}
        sub1 = subnodes[main.subnodes[0]]
        sub2 = subnodes[main.subnodes[1]]
        # Get a copy so we're not modifying the launcher's
        main = model.ProviderNode.fromZK(ctx, path=main.getPath())
        sub1 = model.ProviderNode.fromZK(ctx, path=sub1.getPath())
        sub2 = model.ProviderNode.fromZK(ctx, path=sub2.getPath())
        self.assertEqual(main.uuid, sub1.main_node_id)
        self.assertEqual(main.uuid, sub2.main_node_id)
        self.assertEqual(set([sub1.uuid, sub2.uuid]), set(main.subnodes))
        self.assertEqual([], sub1.subnodes)
        self.assertEqual([], sub2.subnodes)
        self.assertEqual(main.State.SLOT_HOST, main.state)
        self.assertEqual(sub1.State.READY, sub1.state)
        self.assertEqual(sub2.State.READY, sub2.state)
        if (sub1.request_id == request.uuid):
            self.assertIsNone(sub2.request_id)
            sub_used = sub1
        elif (sub2.request_id == request.uuid):
            self.assertIsNone(sub1.request_id)
            sub_used = sub2
        else:
            self.assertTrue(False, "one of the subnodes must be assigned")

        def my_advance(*args, **kw):
            raise Exception("Test exception")

        with mock.patch.object(
            zuul.launcher.server.NodescanRequest, 'advance', my_advance
        ):
            with sub_used.locked(ctx):
                sub_used.refresh(ctx)
                with sub_used.activeContext(ctx):
                    sub_used.unassign(ctx)
                    sub_used.assign(ctx, request_id="dne", tenant_name="test")
                    # Modify the request so that the max zxid of the
                    # request cache is later than the node assignment
                    # (since the "dne" request does not exist).
                    with request.locked(ctx), request.activeContext(ctx):
                        request.priority += 1
                    sub_used.setState(sub_used.State.USED)

            for _ in iterate_timeout(10, "nodes to be marked failed"):
                nodes = self.launcher.api.nodes_cache.getItems()
                states = Counter(n.state for n in nodes)
                if states['failed'] == 3:
                    break

            # Delete the request
            request.delete(ctx)

        with mock.patch.object(
            zuul.driver.static.staticendpoint, 'DELETE_RECYCLE_TIME', 0
        ):
            for _ in iterate_timeout(10, "nodes to be marked ready"):
                nodes = self.launcher.api.nodes_cache.getItems()
                states = Counter(n.state for n in nodes)
                if (states['ready'] == 2 and states['slot-host'] == 1):
                    break

        # Make sure they are unassigned
        sub1.refresh(ctx)
        sub2.refresh(ctx)
        self.assertIsNone(sub1.request_id)
        self.assertIsNone(sub2.request_id)

    @simple_layout('layouts/static/nodepool.yaml', enable_nodepool=True)
    def test_static_deregistration(self):
        self._waitForRegistration()

        self.commitConfigUpdate(
            "org/common-config", 'layouts/static/nodepool-dereg.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        for _ in iterate_timeout(60, "nodes to be deregistered"):
            nodes = self.launcher.api.nodes_cache.getItems()
            if not len(nodes):
                break

    @simple_layout('layouts/static/nodepool.yaml', enable_nodepool=True)
    def test_static_deregistration_in_use(self):
        ctx = self.createZKContext(None)
        self._waitForRegistration()

        nodes = self.launcher.api.nodes_cache.getItems()
        main = nodes[0]
        # Get a copy so we don't share the lock with the launcher
        main = model.ProviderNode.fromZK(ctx, path=main.getPath())

        with main.locked(ctx):
            main.refresh(ctx)
            with main.activeContext(ctx):
                main.assign(ctx, request_id="dne", tenant_name="test")
                # Modify the request cache's max zxid to be later than
                # the node assignment (since the "dne" request does
                # not exist, and we have no requests in this test).
                self.launcher.api.requests_cache._max_zxid =\
                    main.min_request_zxid
                main.setState(main.State.USED)

            self.commitConfigUpdate(
                "org/common-config", 'layouts/static/nodepool-dereg.yaml')
            self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
            self.waitUntilSettled()

        for _ in iterate_timeout(60, "nodes to be deregistered"):
            nodes = self.launcher.api.nodes_cache.getItems()
            if not len(nodes):
                break

    @simple_layout('layouts/static/subnodes.yaml', enable_nodepool=True)
    def test_static_subnodes_deregistration(self):
        self._waitForRegistration()

        self.commitConfigUpdate(
            "org/common-config", 'layouts/static/nodepool-dereg.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()

        for _ in iterate_timeout(60, "nodes to be deregistered"):
            nodes = self.launcher.api.nodes_cache.getItems()
            if not len(nodes):
                break

    @simple_layout('layouts/static/subnodes.yaml', enable_nodepool=True)
    def test_static_subnodes_deregistration_in_use(self):
        ctx = self.createZKContext(None)
        self._waitForRegistration()

        nodes = self.launcher.api.nodes_cache.getItems()
        # Get a list with the main node first and the subnodes last
        nodes.sort(key=lambda x: len(x.subnodes))
        nodes.reverse()
        sub1 = nodes[1]
        # Get a copy so we don't share the lock with the launcher
        sub1 = model.ProviderNode.fromZK(ctx, path=sub1.getPath())
        with sub1.locked(ctx):
            sub1.refresh(ctx)
            with sub1.activeContext(ctx):
                sub1.assign(ctx, request_id="dne", tenant_name="test")
                # Modify the request cache's max zxid to be later than
                # the node assignment (since the "dne" request does
                # not exist, and we have no requests in this test).
                self.launcher.api.requests_cache._max_zxid =\
                    sub1.min_request_zxid
                sub1.setState(sub1.State.USED)

            self.commitConfigUpdate(
                "org/common-config", 'layouts/static/nodepool-dereg.yaml')
            self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
            self.waitUntilSettled()

        for _ in iterate_timeout(60, "nodes to be deregistered"):
            nodes = self.launcher.api.nodes_cache.getItems()
            if not len(nodes):
                break

    @simple_layout('layouts/static/nodepool.yaml', enable_nodepool=True)
    def test_static_node_aliases(self):
        self._test_static_node_lifecycle('special-hardware')

    @simple_layout('layouts/static/nodepool-error-provider.yaml',
                   enable_nodepool=True)
    def test_static_config_error_provider(self):
        # Test a syntax error in parseConfig
        layout = self.scheds.first.sched.abide.tenants.get('tenant-one').layout
        self.assertEqual(1, len(layout.loading_errors))
        self.assertIn('Label "does-not-exist" is not attached to provider',
                      layout.loading_errors[0].error)

    @simple_layout('layouts/static/nodepool.yaml', enable_nodepool=True)
    def test_static_node_reuse_deactivate(self):
        # Deactivate a reusable node and make sure it's deleted.
        self._waitForRegistration()
        ctx = self.createZKContext(None)
        request = self.requestNodes(['debian-normal'])
        self.assertEqual(request.State.FULFILLED, request.state)

        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertEqual(1, len(nodes))
        node = nodes[0]
        # Get a copy so we're not modifying the launcher's
        node = model.ProviderNode.fromZK(ctx, path=node.getPath())

        client = LauncherClient(self.zk_client, None)
        client.setNextNodeState(node, node.State.HOLD,
                                cache=self.launcher.api.nodes_cache)

        with mock.patch.object(
            zuul.driver.static.staticendpoint, 'DELETE_RECYCLE_TIME', 0
        ):

            with node.locked(ctx):
                with node.activeContext(ctx):
                    request.delete(ctx)
                    node.setState(node.State.USED)

            for _ in iterate_timeout(10, "node to be deactivated"):
                node.refresh(ctx)
                if (node.state == node.State.HOLD and
                    node.lifecycle.getZKVersion() is None):
                    break

            request = self.requestNodes(['debian-normal'], timeout=None)
            # No nodes available for now
            time.sleep(5)
            request.refresh(ctx)
            self.assertEqual(request.State.REQUESTED, request.state)

            # Re-activate the node and wait for it to be assigned
            client.setNextNodeState(node, node.State.USED,
                                    cache=self.launcher.api.nodes_cache)

            for _ in iterate_timeout(10, "request to be fulfilled"):
                request.refresh(ctx)
                if request.state == request.State.FULFILLED:
                    break

    @simple_layout('layouts/static/subnodes.yaml', enable_nodepool=True)
    def test_static_subnodes_deactivate_subnode(self):
        self._waitForRegistration()
        ctx = self.createZKContext(None)
        request = self.requestNodes(['debian-normal'])
        self.assertEqual(request.State.FULFILLED, request.state)

        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertEqual(3, len(nodes))
        # Get a list with the main node first and the subnodes last
        nodes.sort(key=lambda x: len(x.subnodes))
        nodes.reverse()
        main = nodes[0]
        sub1 = nodes[1]
        sub2 = nodes[2]
        # Get a copy so we're not modifying the launcher's
        main = model.ProviderNode.fromZK(ctx, path=main.getPath())
        sub1 = model.ProviderNode.fromZK(ctx, path=sub1.getPath())
        sub2 = model.ProviderNode.fromZK(ctx, path=sub2.getPath())

        node = sub2
        other = sub1

        client = LauncherClient(self.zk_client, None)
        client.setNextNodeState(node, node.State.HOLD,
                                cache=self.launcher.api.nodes_cache)

        with node.locked(ctx):
            with node.activeContext(ctx):
                request.delete(ctx)
                node.setState(node.State.USED)

        for _ in iterate_timeout(10, "node to be deactivated"):
            node.refresh(ctx)
            if (node.state == node.State.HOLD and
                node.lifecycle.getZKVersion() is None):
                break

        # Main and other subnode should still exist
        main.refresh(ctx)
        other.refresh(ctx)
        self.assertEqual(node.State.SLOT_HOST, main.state)
        self.assertEqual(node.State.READY, other.state)

        with mock.patch.object(
            zuul.driver.static.staticendpoint, 'DELETE_RECYCLE_TIME', 0
        ):
            # Re-activate the node
            client.setNextNodeState(node, node.State.USED,
                                    cache=self.launcher.api.nodes_cache)

            for _ in iterate_timeout(10, "node to be reactivated"):
                node.refresh(ctx)
                if node.state == node.State.READY:
                    break

    @simple_layout('layouts/static/subnodes.yaml', enable_nodepool=True)
    def test_static_subnodes_deactivate_main(self):
        self._waitForRegistration()
        ctx = self.createZKContext(None)
        request = self.requestNodes(['debian-normal'])
        self.assertEqual(request.State.FULFILLED, request.state)

        nodes = self.launcher.api.nodes_cache.getItems()
        self.assertEqual(3, len(nodes))
        # Get a list with the main node first and the subnodes last
        nodes.sort(key=lambda x: len(x.subnodes))
        nodes.reverse()
        main = nodes[0]
        sub1 = nodes[1]
        sub2 = nodes[2]
        # Get a copy so we're not modifying the launcher's
        main = model.ProviderNode.fromZK(ctx, path=main.getPath())
        sub1 = model.ProviderNode.fromZK(ctx, path=sub1.getPath())
        sub2 = model.ProviderNode.fromZK(ctx, path=sub2.getPath())

        node = sub2
        other = sub1

        client = LauncherClient(self.zk_client, None)
        client.setNextNodeState(main, main.State.HOLD,
                                cache=self.launcher.api.nodes_cache)

        for _ in iterate_timeout(10, "unused node to be deactivated"):
            other.refresh(ctx)
            if other.state == other.State.HOLD:
                break

        # Main and other subnode should still exist
        main.refresh(ctx)
        node.refresh(ctx)
        other.refresh(ctx)
        self.assertEqual(node.State.SLOT_HOST, main.state)
        self.assertEqual(node.State.READY, node.state)
        self.assertEqual(node.State.HOLD, other.state)

        with node.locked(ctx):
            with node.activeContext(ctx):
                request.delete(ctx)
                node.setState(node.State.USED)

        for _ in iterate_timeout(10, "used node to be deactivated"):
            node.refresh(ctx)
            if node.state == node.State.HOLD:
                break

        # Main and other subnode should still exist
        main.refresh(ctx)
        node.refresh(ctx)
        other.refresh(ctx)
        self.assertEqual(node.State.SLOT_HOST, main.state)
        self.assertEqual(node.State.HOLD, node.state)
        self.assertEqual(node.State.HOLD, other.state)

        with mock.patch.object(
            zuul.driver.static.staticendpoint, 'DELETE_RECYCLE_TIME', 0
        ):
            # Re-activate the node
            client.setNextNodeState(main, main.State.USED,
                                    cache=self.launcher.api.nodes_cache)

            for _ in iterate_timeout(10, "main to be reactivated"):
                main.refresh(ctx)
                if main.state == main.State.SLOT_HOST:
                    break

            for _ in iterate_timeout(10, "node to be reactivated"):
                node.refresh(ctx)
                if node.state == node.State.READY:
                    break

            for _ in iterate_timeout(10, "other to be reactivated"):
                other.refresh(ctx)
                if other.state == other.State.READY:
                    break
