# Copyright (C) 2023 Acme Gating, LLC
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

import time
from unittest.mock import MagicMock
from unittest.mock import patch

import testtools

from zuul import exceptions
from zuul.model import ProviderNode
from zuul.launcher.server import NodescanWorker, NodescanRequest
from zuul.web import ProviderNodeConverter
from zuul.zk import zkobject

from tests.base import (
    BaseTestCase,
    iterate_timeout,
    okay_tracebacks,
)
from tests.fake_nodescan import (
    FakeSocket,
    FakePoll,
    FakeTransport,
    NodescanFixture,
)


class DummyProviderNode(ProviderNode, subclass_id="dummy-nodescan"):
    pass


class TestNodescanWorker(BaseTestCase):

    def createZKContext(self, lock=None):
        return zkobject.ZKContext(self.zk_client, lock,
                                  None, self.log)

    def test_nodescan(self):
        # Test the nodescan worker
        self.useFixture(NodescanFixture())
        worker = NodescanWorker()
        node = DummyProviderNode()
        node._set(
            interface_ip='198.51.100.1',
            connection_port=22,
            connection_type='ssh',
            host_key_checking=True,
            boot_timeout=300,
        )
        worker.start()
        request = NodescanRequest(node, self.log)
        worker.addRequest(request)
        for _ in iterate_timeout(30, 'waiting for nodescan'):
            if request.complete:
                break
        result = request.result()
        self.assertEqual(result, ['fake key fake base64'])
        worker.stop()
        worker.join()

    def test_nodescan_connection_timeout(self):
        # Test a timeout during socket connection
        self.useFixture(NodescanFixture(poll_fail=True))
        worker = NodescanWorker()
        node = DummyProviderNode()
        node._set(
            interface_ip='198.51.100.1',
            connection_port=22,
            connection_type='ssh',
            host_key_checking=True,
            boot_timeout=1,
        )
        worker.start()
        request = NodescanRequest(node, self.log)
        worker.addRequest(request)
        for _ in iterate_timeout(30, 'waiting for nodescan'):
            if request.complete:
                break
        with testtools.ExpectedException(
                exceptions.ConnectionTimeoutException):
            request.result()
        worker.stop()
        worker.join()

    def test_nodescan_ssh_timeout(self):
        # Test a timeout during ssh connection
        self.useFixture(NodescanFixture(transport_fail=True))
        worker = NodescanWorker()
        node = DummyProviderNode()
        node._set(
            interface_ip='198.51.100.1',
            connection_port=22,
            connection_type='ssh',
            host_key_checking=True,
            boot_timeout=1,
        )
        worker.start()
        request = NodescanRequest(node, self.log)
        worker.addRequest(request)
        for _ in iterate_timeout(30, 'waiting for nodescan'):
            if request.complete:
                break
        with testtools.ExpectedException(
                exceptions.ConnectionTimeoutException):
            request.result()
        worker.stop()
        worker.join()

    @okay_tracebacks('Fake ssh error')
    def test_nodescan_ssh_error(self):
        # Test an ssh error
        self.useFixture(NodescanFixture(transport_active=False))
        worker = NodescanWorker()
        node = DummyProviderNode()
        node._set(
            interface_ip='198.51.100.1',
            connection_port=22,
            connection_type='ssh',
            host_key_checking=True,
            boot_timeout=1,
        )
        worker.start()
        request = NodescanRequest(node, self.log)
        worker.addRequest(request)
        for _ in iterate_timeout(30, 'waiting for nodescan'):
            if request.complete:
                break
        with testtools.ExpectedException(
                exceptions.ConnectionTimeoutException):
            request.result()
        worker.stop()
        worker.join()

    @patch('paramiko.transport.Transport')
    @patch('zuul.launcher.server.NodescanRequest._socket_class')
    @patch('zuul.launcher.server.NodescanWorker._poll_class')
    def test_nodescan_queue(self, mock_epoll, mock_socket, mock_transport):
        # Test the max_requests queing function
        fake_socket1 = FakeSocket()
        fake_socket2 = FakeSocket()
        fake_socket2.fd = 2
        # We get two sockets for each host
        sockets = [fake_socket1, fake_socket1, fake_socket2, fake_socket2]

        def getsocket(*args, **kw):
            return sockets.pop(0)

        mock_socket.side_effect = getsocket
        mock_epoll.return_value = FakePoll()
        mock_transport.return_value = FakeTransport()
        worker = NodescanWorker()
        worker.MAX_REQUESTS = 1
        node1 = DummyProviderNode()
        node1._set(
            interface_ip='198.51.100.1',
            connection_port=22,
            connection_type='ssh',
            host_key_checking=True,
            boot_timeout=300,
        )
        node2 = DummyProviderNode()
        node2._set(
            interface_ip='198.51.100.2',
            connection_port=22,
            connection_type='ssh',
            host_key_checking=True,
            boot_timeout=300,
        )

        request1 = NodescanRequest(node1, self.log)
        request2 = NodescanRequest(node2, self.log)
        worker.addRequest(request1)
        worker.addRequest(request2)
        worker.start()
        for _ in iterate_timeout(5, 'waiting for nodescan'):
            if request1.complete and request2.complete:
                break
        result1 = request1.result()
        result2 = request2.result()
        self.assertEqual(result1, ['fake key fake base64'])
        self.assertEqual(result2, ['fake key fake base64'])
        worker.stop()
        worker.join()

    def test_nodescan_ssh_private_key_skipped_when_path_missing(self):
        self.useFixture(NodescanFixture())
        worker = NodescanWorker()
        node = DummyProviderNode()
        node._set(
            interface_ip='198.51.100.1',
            connection_port=22,
            connection_type='ssh',
            host_key_checking=True,
            boot_timeout=300,
            ssh_private_key='/no/such/zuul-test-ssh-key-file',
            username='zuul',
        )
        worker.start()
        request = NodescanRequest(node, self.log)
        worker.addRequest(request)
        for _ in iterate_timeout(30, 'waiting for nodescan'):
            if request.complete:
                break
        self.assertEqual(FakeTransport.auth_publickey_calls, 0)
        self.assertEqual(request.result(), ['fake key fake base64'])
        worker.stop()
        worker.join()

    @patch('zuul.launcher.server.Path.exists', return_value=True)
    @patch('paramiko.PKey.from_path')
    def test_nodescan_ssh_private_key_verified_after_transport(
            self, mock_from_path, mock_exists):
        self.useFixture(NodescanFixture())
        mock_from_path.return_value = object()
        worker = NodescanWorker()
        node = DummyProviderNode()
        node._set(
            interface_ip='198.51.100.1',
            connection_port=22,
            connection_type='ssh',
            host_key_checking=True,
            boot_timeout=300,
            ssh_private_key='/fake/zuul-launcher-key',
            username='zuul',
        )
        worker.start()
        request = NodescanRequest(node, self.log)
        worker.addRequest(request)
        for _ in iterate_timeout(30, 'waiting for nodescan'):
            if request.complete:
                break
        self.assertGreaterEqual(FakeTransport.auth_publickey_calls, 1)
        mock_from_path.assert_called_with('/fake/zuul-launcher-key')
        self.assertEqual(request.result(), ['fake key fake base64'])
        worker.stop()
        worker.join()

    @okay_tracebacks(
        'fake auth failure for nodescan tests',
        'Timeout connecting to',
    )
    @patch('zuul.launcher.server.Path.exists', return_value=True)
    @patch('paramiko.PKey.from_path')
    def test_nodescan_ssh_private_key_openstack_reboot_once_on_timeout(
            self, mock_from_path, mock_exists):
        self.useFixture(NodescanFixture(auth_publickey_fail=True))
        mock_from_path.return_value = object()

        def fake_is_private_key_timeout(self, reboot_attempts):
            return reboot_attempts == 0

        patcher = patch.object(
            NodescanRequest, '_isPrivateKeyTimeout',
            fake_is_private_key_timeout)
        self.addCleanup(patcher.stop)
        patcher.start()

        reboot = MagicMock()
        csm = MagicMock()
        csm.rebootServer = reboot

        worker = NodescanWorker()
        node = DummyProviderNode()
        node._set(
            interface_ip='198.51.100.1',
            connection_port=22,
            connection_type='ssh',
            host_key_checking=True,
            boot_timeout=8,
            ssh_private_key='/fake/zuul-launcher-key',
            username='zuul',
            driver_name='openstack',
            create_state_machine=csm,
        )
        worker.start()
        request = NodescanRequest(node, self.log)
        worker.addRequest(request)
        for _ in iterate_timeout(60, 'waiting for nodescan failure'):
            if request.complete:
                break
        with testtools.ExpectedException(
                exceptions.ConnectionTimeoutException):
            request.result()
        reboot.assert_called_once()
        worker.stop()
        worker.join()

    def test_provider_node_converter_includes_ssh_private_key(self):
        now = time.time()
        node = DummyProviderNode()
        node._set(
            label='test-label',
            state=node.State.READY,
            state_time=now,
            request_time=now,
            connection_type='ssh',
            interface_ip='10.0.0.2',
            username='cloud-user',
            ssh_private_key='/var/lib/zuul/executor_rsa',
            provider='openstack/main',
            region='region-one',
        )
        d = ProviderNodeConverter.toDict(node)
        self.assertEqual(
            d['ssh_private_key'], '/var/lib/zuul/executor_rsa')
