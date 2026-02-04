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

import math
import time
import uuid

from zuul import model
import zuul.driver.aws.awsendpoint
import zuul.launcher.server

from tests.base import (
    driver_config,
    simple_layout,
)
from tests.unit.test_launcher import LauncherBaseTestCase


class DummyProviderNode(model.ProviderNode, subclass_id="dummy"):
    pass


class FakeItemCache:
    def __init__(self):
        self._items = {}

    def getItem(self, item_id):
        return self._items.get(item_id)

    def getItems(self):
        return self._items.values()

    def getQuota(self, provider, include_requested=False):
        return model.QuotaInformation()

    def waitForSync(self, timeout):
        return True


class FakeLauncherApi:
    def __init__(self):
        self.nodes_cache = FakeItemCache()
        self.requests_cache = FakeItemCache()

    def stop(self):
        pass

    def getMatchingRequests(self):
        return []

    def getProviderNode(self, node_id):
        return self.nodes_cache.getItem(node_id)

    def getProviderNodes(self):
        return self.nodes_cache.getItems()

    def getMatchingProviderNodes(self):
        return []


class TestSelectProviders(LauncherBaseTestCase):

    def _makeRequest(self, labels):
        request = model.NodesetRequest()
        request._set(
            uuid=uuid.uuid4().hex,
            tenant_name='tenant-one',
            pipeline_name="check",
            buildset_uuid=uuid.uuid4().hex,
            build_uuid=uuid.uuid4().hex,
            job_uuid=uuid.uuid4().hex,
            job_name="foobar",
            labels=labels,
            priority=100,
            request_time=time.time(),
            zuul_event_id=uuid.uuid4().hex,
            span_info=None,
        )
        return request

    def _makeNode(self, label, request_id=None, state=None):
        node = DummyProviderNode()
        node._set(
            uuid=uuid.uuid4().hex,
            executor_zone=None,
            state=state,
        )
        node.assignment._set(
            request_id=request_id,
        )
        return node

    def _assertProviders(self, expected, label_providers):
        self.assertEqual(len(expected), len(label_providers))
        for i, expected_label in enumerate(expected):
            self.log.debug(
                "Label %s: %s from %s",
                i,
                label_providers[i][0].name,
                label_providers[i][1].name,
            )
        for i, expected_label in enumerate(expected):
            self.assertEqual(expected_label[0], label_providers[i][0].name)
            self.assertEqual(expected_label[1], label_providers[i][1].name)

    def setUp(self):
        super().setUp()
        test = self
        self.ordered_providers = None

        def getEndpointLimits(self):
            quotas = test.test_config.driver.test_launcher['provider_quotas']
            return model.QuotaInformation(
                default=math.inf,
                **quotas[self.name],
            )
        self.patch(zuul.driver.aws.awsprovider.AwsProvider,
                   'getEndpointLimits',
                   getEndpointLimits)

        def shuffleProviders(self, providers):
            if test.ordered_providers:
                orig_providers = {p.name: p for p in providers}
                new_providers = [
                    orig_providers[pname] for pname in test.ordered_providers]
                return new_providers
            return providers[:]
        self.patch(zuul.launcher.server.Launcher,
                   '_shuffleProviders',
                   shuffleProviders)

        self.waitUntilSettled()
        self.launcher.api.stop()
        self.launcher.api = FakeLauncherApi()

    @simple_layout('layouts/launcher-select-providers.yaml',
                   enable_nodepool=True)
    @driver_config('test_launcher', provider_quotas={
        'east': {'instances': 2},
        'west': {'instances': 1},
        'unused': {'instances': 0},
    })
    def test_select_providers(self):
        # 1 node: use first available provider
        self.ordered_providers = ['east', 'west', 'unused']
        request = self._makeRequest(['debian-normal'])
        ready_nodes = zuul.launcher.server.LabelNodeMapping()
        request_ready_nodes = self.launcher._filterUnassignedNodes(
            ready_nodes, request)
        messages = []
        label_providers = self.launcher._selectProviders(
            request, request_ready_nodes, messages)
        self.launcher._emitMessages(self.log, messages)
        self._assertProviders([
            ('debian-normal', 'east'),
        ], label_providers)

        # 2 node: use "east" because it can handle it, even though
        # it's second.
        self.ordered_providers = ['unused', 'east', 'west']
        request = self._makeRequest(['debian-normal', 'debian-normal'])
        ready_nodes = zuul.launcher.server.LabelNodeMapping()
        request_ready_nodes = self.launcher._filterUnassignedNodes(
            ready_nodes, request)
        messages = []
        label_providers = self.launcher._selectProviders(
            request, request_ready_nodes, messages)
        self.launcher._emitMessages(self.log, messages)
        self._assertProviders([
            ('debian-normal', 'east'),
            ('debian-normal', 'east'),
        ], label_providers)

        # TODO: implement the TODO regarding checking for a multi-node
        # request with a single label that exceeds capacity and then
        # perform the above test with ['east', 'west', 'unused'].
        # Since we don't check that now, the only thing we can verify
        # is whether the provider can launch any nodes of a label.

        # 2 node split: 1 from east, 1 from west
        self.ordered_providers = ['east', 'west', 'unused']
        request = self._makeRequest(['debian-small', 'debian-large'])
        ready_nodes = zuul.launcher.server.LabelNodeMapping()
        request_ready_nodes = self.launcher._filterUnassignedNodes(
            ready_nodes, request)
        messages = []
        label_providers = self.launcher._selectProviders(
            request, request_ready_nodes, messages)
        self.launcher._emitMessages(self.log, messages)
        self._assertProviders([
            ('debian-small', 'east'),
            ('debian-large', 'west'),
        ], label_providers)

        # 2 node existing fail: both nodes failover
        self.ordered_providers = ['east', 'west', 'unused']
        east = 'review.example.com%2Forg%2Fcommon-config/east'
        request = self._makeRequest(['debian-normal', 'debian-normal'])
        node1 = self._makeNode('debian-normal', request.uuid)
        request.addProviderNode(node1)
        node1._set(state=node1.State.FAILED,
                   provider=east)
        self.launcher.api.nodes_cache._items[node1.uuid] = node1
        node2 = self._makeNode('debian-normal', request.uuid)
        request.addProviderNode(node2)
        node2._set(state=node1.State.BUILDING,
                   provider=east)
        self.launcher.api.nodes_cache._items[node2.uuid] = node2
        request.updateProviderNode(0, node1, add_failed_provider=east)
        request.updateProviderNode(0, node1, add_failed_provider=east)
        ready_nodes = zuul.launcher.server.LabelNodeMapping()
        request_ready_nodes = self.launcher._filterUnassignedNodes(
            ready_nodes, request)
        messages = []
        label_providers = self.launcher._selectProviders(
            request, request_ready_nodes, messages)
        self.launcher._emitMessages(self.log, messages)
        self._assertProviders([
            ('debian-normal', 'west'),
            ('debian-normal', 'west'),
        ], label_providers)

        # 1 node existing tempfail: change provider
        self.ordered_providers = ['west', 'east', 'unused']
        east = 'review.example.com%2Forg%2Fcommon-config/east'
        request = self._makeRequest(['debian-normal'])
        node1 = self._makeNode('debian-normal', request.uuid)
        request.addProviderNode(node1)
        node1._set(state=node1.State.TEMPFAILED,
                   provider=east)
        self.launcher.api.nodes_cache._items[node1.uuid] = node1
        ready_nodes = zuul.launcher.server.LabelNodeMapping()
        request_ready_nodes = self.launcher._filterUnassignedNodes(
            ready_nodes, request)
        messages = []
        label_providers = self.launcher._selectProviders(
            request, request_ready_nodes, messages)
        self.launcher._emitMessages(self.log, messages)
        self._assertProviders([
            ('debian-normal', 'west'),
        ], label_providers)

        # 2 node existing tempfail: keep same provider
        self.ordered_providers = ['west', 'east', 'unused']
        east = 'review.example.com%2Forg%2Fcommon-config/east'
        request = self._makeRequest(['debian-normal', 'debian-normal'])
        node1 = self._makeNode('debian-normal', request.uuid)
        request.addProviderNode(node1)
        node1._set(state=node1.State.TEMPFAILED,
                   provider=east)
        self.launcher.api.nodes_cache._items[node1.uuid] = node1
        node2 = self._makeNode('debian-normal', request.uuid)
        request.addProviderNode(node2)
        node2._set(state=node1.State.BUILDING,
                   provider=east)
        self.launcher.api.nodes_cache._items[node2.uuid] = node2
        ready_nodes = zuul.launcher.server.LabelNodeMapping()
        request_ready_nodes = self.launcher._filterUnassignedNodes(
            ready_nodes, request)
        messages = []
        label_providers = self.launcher._selectProviders(
            request, request_ready_nodes, messages)
        self.launcher._emitMessages(self.log, messages)
        self._assertProviders([
            ('debian-normal', 'east'),
            ('debian-normal', 'east'),
        ], label_providers)

        # 1 node with ready node in west: use west
        self.ordered_providers = ['east', 'west', 'unused']
        west = 'review.example.com%2Forg%2Fcommon-config/west'
        request = self._makeRequest(['debian-normal'])
        node1 = self._makeNode('debian-normal')
        request.addProviderNode(node1)
        provider = self.launcher._getProvider('tenant-one', 'west')
        label = provider.labels['debian-normal']
        node1._set(
            state=node1.State.READY,
            label='debian-normal',
            connection_name='aws',
            endpoint_name='aws/aws-us-west-1',
            label_config_hash=label.config_hash,
            provider=west,
        )
        self.launcher.api.nodes_cache._items[node1.uuid] = node1
        ready_nodes = self.launcher._getUnassignedNodes()
        request_ready_nodes = self.launcher._filterUnassignedNodes(
            ready_nodes, request)
        self.assertEqual(1, len(request_ready_nodes.getNodesForLabel(
            'debian-normal')))
        messages = []
        label_providers = self.launcher._selectProviders(
            request, request_ready_nodes, messages)
        self.launcher._emitMessages(self.log, messages)
        self._assertProviders([
            ('debian-normal', 'west'),
        ], label_providers)
