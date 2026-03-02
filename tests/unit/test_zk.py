# Copyright 2019 Red Hat, Inc.
# Copyright 2022, 2024 Acme Gating, LLC
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

import binascii
from collections import defaultdict
import json
import math
import queue
import random
import string
import threading
import time
import uuid
import zlib
from unittest import mock

import fixtures
import testtools

from zuul import model
from zuul.lib import yamlutil as yaml
from zuul.model import (
    BuildRequest,
    HoldRequest,
    MergeRequest,
    QuotaInformation,
)
from zuul.zk import ZooKeeperClient
from zuul.zk.blob_store import BlobStore
from zuul.zk.branch_cache import BranchCache, BranchFlag, BranchInfo
from zuul.zk.change_cache import (
    AbstractChangeCache,
    ChangeKey,
    ConcurrentUpdateError,
)
from zuul.zk.config_cache import SystemConfigCache, UnparsedConfigCache
from zuul.zk.election import SessionAwareElection, RendezvousElection
from zuul.zk.exceptions import LockException
from zuul.zk.executor import ExecutorApi
from zuul.zk.job_request_queue import JobRequestEvent
from zuul.zk.merger import MergerApi
from zuul.zk.launcher import LauncherApi, LockableZKObjectCache
from zuul.zk.layout import LayoutStateStore, LayoutState
from zuul.zk.locks import locked
from zuul.zk.nodepool import ZooKeeperNodepool
from zuul.zk.sharding import (
    RawShardIO,
    BufferedShardReader,
    BufferedShardWriter,
    NODE_BYTE_SIZE_LIMIT,
)
from zuul.zk.components import (
    BaseComponent,
    ComponentRegistry,
    ExecutorComponent,
    LauncherComponent,
    SchedulerComponent,
    COMPONENT_REGISTRY
)
from tests.base import (
    BaseTestCase,
    HoldableExecutorApi,
    HoldableMergerApi,
    iterate_timeout,
    model_version,
    ZOOKEEPER_SESSION_TIMEOUT,
)
from zuul.zk.zkobject import (
    LockableZKObject,
    PolymorphicZKObjectMixin,
    ShardedZKObject,
    ZKContext,
    ZKObject,
)
from zuul.zk.locks import tenant_write_lock

import kazoo.recipe.lock
from kazoo.exceptions import ZookeeperError, OperationTimeoutError, NoNodeError


class ZooKeeperBaseTestCase(BaseTestCase):

    def setUp(self):
        super().setUp()

        self.setupZK()

        self.zk_client = ZooKeeperClient(
            self.zk_chroot_fixture.zk_hosts,
            tls_cert=self.zk_chroot_fixture.zookeeper_cert,
            tls_key=self.zk_chroot_fixture.zookeeper_key,
            tls_ca=self.zk_chroot_fixture.zookeeper_ca,
            timeout=ZOOKEEPER_SESSION_TIMEOUT,
        )
        self.addCleanup(self.zk_client.disconnect)
        self.zk_client.connect()
        self.setupModelPin()
        self.component_registry = ComponentRegistry(self.zk_client)
        # We don't have any other component to initialize the global
        # registry in these tests, so we do it ourselves.
        COMPONENT_REGISTRY.create(self.zk_client)

    def getRequest(self, api, request_uuid, state=None):
        for _ in iterate_timeout(30, "cache to update"):
            req = api.getRequest(request_uuid)
            if req:
                if state is None:
                    return req
                if req.state == state:
                    return req


class TestZookeeperClient(ZooKeeperBaseTestCase):

    def test_ltime(self):
        ltime = self.zk_client.getCurrentLtime()
        self.assertGreaterEqual(ltime, 0)
        self.assertIsInstance(ltime, int)
        self.assertGreater(self.zk_client.getCurrentLtime(), ltime)


class TestNodepool(ZooKeeperBaseTestCase):

    def setUp(self):
        super().setUp()
        self.zk_nodepool = ZooKeeperNodepool(self.zk_client)

    def _createRequest(self):
        req = HoldRequest()
        req.count = 1
        req.reason = 'some reason'
        req.expiration = 1
        return req

    def test_hold_requests_api(self):
        # Test no requests returns empty list
        self.assertEqual([], self.zk_nodepool.getHoldRequests())

        # Test get on non-existent request is None
        self.assertIsNone(self.zk_nodepool.getHoldRequest('anything'))

        # Test creating a new request
        req1 = self._createRequest()
        self.zk_nodepool.storeHoldRequest(req1)
        self.assertIsNotNone(req1.id)
        self.assertEqual(1, len(self.zk_nodepool.getHoldRequests()))

        # Test getting the request
        req2 = self.zk_nodepool.getHoldRequest(req1.id)
        self.assertEqual(req1.toDict(), req2.toDict())

        # Test updating the request
        req2.reason = 'a new reason'
        self.zk_nodepool.storeHoldRequest(req2)
        req2 = self.zk_nodepool.getHoldRequest(req2.id)
        self.assertNotEqual(req1.reason, req2.reason)

        # Test lock operations
        self.zk_nodepool.lockHoldRequest(req2, blocking=False)
        with testtools.ExpectedException(
            LockException, "Timeout trying to acquire lock .*"
        ):
            self.zk_nodepool.lockHoldRequest(req2, blocking=True, timeout=2)
        self.zk_nodepool.unlockHoldRequest(req2)
        self.assertIsNone(req2.lock)

        # Test deleting the request
        self.zk_nodepool.deleteHoldRequest(req1, None)
        self.assertEqual([], self.zk_nodepool.getHoldRequests())


class TestSharding(ZooKeeperBaseTestCase):

    def test_reader(self):
        shard_io = RawShardIO(self.zk_client.client, "/test/shards")
        with testtools.ExpectedException(NoNodeError):
            len(shard_io._shards)

        with BufferedShardReader(
            self.zk_client.client, "/test/shards"
        ) as shard_reader:
            with testtools.ExpectedException(NoNodeError):
                shard_reader.read()
            shard_io.write(b"foobar")
            self.assertEqual(len(shard_io._shards), 1)
            self.assertEqual(shard_io.read(), b"foobar")

    def test_writer(self):
        shard_io = RawShardIO(self.zk_client.client, "/test/shards")
        with testtools.ExpectedException(NoNodeError):
            len(shard_io._shards)

        with BufferedShardWriter(
            self.zk_client.client, "/test/shards"
        ) as shard_writer:
            shard_writer.write(b"foobar")

        self.assertEqual(len(shard_io._shards), 1)
        self.assertEqual(shard_io.read(), b"foobar")

    def test_truncate(self):
        shard_io = RawShardIO(self.zk_client.client, "/test/shards")
        shard_io.write(b"foobar")
        self.assertEqual(len(shard_io._shards), 1)

        with BufferedShardWriter(
            self.zk_client.client, "/test/shards"
        ) as shard_writer:
            shard_writer.truncate(0)

        with testtools.ExpectedException(NoNodeError):
            len(shard_io._shards)

    def test_shard_bytes_limit(self):
        with BufferedShardWriter(
            self.zk_client.client, "/test/shards"
        ) as shard_writer:
            shard_writer.write(b"x" * (NODE_BYTE_SIZE_LIMIT + 1))
            shard_writer.flush()
            self.assertEqual(len(shard_writer.raw._shards), 2)

    def test_json(self):
        data = {"key": "value"}
        with BufferedShardWriter(
            self.zk_client.client, "/test/shards"
        ) as shard_io:
            shard_io.write(json.dumps(data).encode("utf8"))

        with BufferedShardReader(
            self.zk_client.client, "/test/shards"
        ) as shard_io:
            self.assertDictEqual(json.load(shard_io), data)

    def test_no_makepath(self):
        with testtools.ExpectedException(NoNodeError):
            with BufferedShardWriter(
                self.zk_client.client, "/test/shards",
                makepath=False
            ) as shard_writer:
                shard_writer.write(b"foobar")

    def _test_write_old_read_new(self, shard_count):
        # Write shards in the old format where each shard is
        # compressed individually
        data = b'{"key": "value"}'
        data_shards = []
        shard_len = math.ceil(len(data) / shard_count)
        for start in range(0, len(data), shard_len):
            data_shards.append(data[start:start + shard_len])
        # Make sure we split them correctly
        self.assertEqual(data, b''.join(data_shards))
        self.assertEqual(shard_count, len(data_shards))
        for shard in data_shards:
            shard = zlib.compress(shard)
            self.log.debug(f"{binascii.hexlify(shard)=}")
            self.zk_client.client.create("/test/shards/", shard,
                                         sequence=True, makepath=True)

        # Read shards, expecting the new format
        with BufferedShardReader(
            self.zk_client.client, "/test/shards"
        ) as shard_io:
            read_data = shard_io.read()
            decompressed_data = zlib.decompress(read_data)
            self.log.debug(f"{binascii.hexlify(read_data)=}")
            self.log.debug(f"{decompressed_data=}")
            self.assertEqual(decompressed_data, data)

    def test_write_old_read_new_1(self):
        self._test_write_old_read_new(1)

    def test_write_old_read_new_2(self):
        self._test_write_old_read_new(2)

    def test_write_old_read_new_3(self):
        self._test_write_old_read_new(3)

    def _test_write_new_read_new(self, shard_count):
        # Write shards in the new format
        data = b'{"key": "value"}'
        compressed_data = zlib.compress(data)

        data_shards = []
        shard_len = math.ceil(len(compressed_data) / shard_count)
        for start in range(0, len(compressed_data), shard_len):
            data_shards.append(compressed_data[start:start + shard_len])
        # Make sure we split them correctly
        self.assertEqual(compressed_data, b''.join(data_shards))
        self.assertEqual(shard_count, len(data_shards))
        for shard in data_shards:
            self.log.debug(f"{binascii.hexlify(shard)=}")
            self.zk_client.client.create("/test/shards/", shard,
                                         sequence=True, makepath=True)

        # Read shards, expecting the new format
        with BufferedShardReader(
            self.zk_client.client, "/test/shards"
        ) as shard_io:
            read_data = shard_io.read()
            decompressed_data = zlib.decompress(read_data)
            self.log.debug(f"{binascii.hexlify(read_data)=}")
            self.log.debug(f"{decompressed_data=}")
            self.assertEqual(decompressed_data, data)

    def test_write_new_read_new_1(self):
        self._test_write_new_read_new(1)

    def test_write_new_read_new_2(self):
        self._test_write_new_read_new(2)

    def test_write_new_read_new_3(self):
        self._test_write_new_read_new(3)

    def _test_write_old_read_old(self, shard_count):
        # Test that the writer can write in the old format
        data = b'{"key": "value is longer"}'
        compressed_data = zlib.compress(data)

        shard_len = math.ceil(len(compressed_data) / shard_count)
        # We subtract 1024 from the size limit when writing the old
        # format
        size_limit = shard_len + 1024
        with mock.patch("zuul.zk.sharding.NODE_BYTE_SIZE_LIMIT", size_limit):
            with BufferedShardWriter(
                    self.zk_client.client, "/test/shards"
            ) as shard_writer:
                shard_writer.write(compressed_data)

        read_shards = []
        for shard in sorted(
                self.zk_client.client.get_children("/test/shards")):
            self.log.debug(f"{shard=}")
            read_data, _ = self.zk_client.client.get(f"/test/shards/{shard}")
            self.log.debug(f"{binascii.hexlify(read_data)=}")
            self.log.debug(f"{len(read_data)=}")
            read_shards.append(zlib.decompress(read_data))
        self.assertEqual(b"".join(read_shards), data)
        self.assertEqual(shard_count, len(read_shards))

    @model_version(30)
    def test_write_old_read_old_1(self):
        self._test_write_old_read_old(1)

    @model_version(30)
    def test_write_old_read_old_2(self):
        self._test_write_old_read_old(2)

    @model_version(30)
    def test_write_old_read_old_3(self):
        self._test_write_old_read_old(3)


class TestUnparsedConfigCache(ZooKeeperBaseTestCase):

    def setUp(self):
        super().setUp()
        self.config_cache = UnparsedConfigCache(self.zk_client)

    def test_files_cache(self):
        master_files = self.config_cache.getFilesCache("project", "master")

        with self.config_cache.readLock("project"):
            self.assertEqual(len(master_files), 0)

        with self.config_cache.writeLock("project"):
            master_files["/path/to/file"] = "content"

        with self.config_cache.readLock("project"):
            self.assertEqual(master_files["/path/to/file"], "content")
            self.assertEqual(len(master_files), 1)

        with self.config_cache.writeLock("project"):
            master_files.clear()
            self.assertEqual(len(master_files), 0)

    def test_valid_for(self):
        tpc = model.TenantProjectConfig("project")
        tpc.extra_config_files = {"foo.yaml", "bar.yaml"}
        tpc.extra_config_dirs = {"foo.d/", "bar.d/"}

        master_files = self.config_cache.getFilesCache("project", "master")
        self.assertFalse(master_files.isValidFor(tpc, min_ltime=-1))

        master_files.setValidFor(tpc.extra_config_files, tpc.extra_config_dirs,
                                 ltime=1)
        self.assertTrue(master_files.isValidFor(tpc, min_ltime=-1))

        tpc.extra_config_files = set()
        tpc.extra_config_dirs = set()
        self.assertTrue(master_files.isValidFor(tpc, min_ltime=-1))
        self.assertFalse(master_files.isValidFor(tpc, min_ltime=2))

        tpc.extra_config_files = {"bar.yaml"}
        tpc.extra_config_dirs = {"bar.d/"}
        # Valid for subset
        self.assertTrue(master_files.isValidFor(tpc, min_ltime=-1))

        tpc.extra_config_files = {"foo.yaml", "bar.yaml"}
        tpc.extra_config_dirs = {"foo.d/", "bar.d/", "other.d/"}
        # Invalid for additional dirs
        self.assertFalse(master_files.isValidFor(tpc, min_ltime=-1))
        self.assertFalse(master_files.isValidFor(tpc, min_ltime=2))

        tpc.extra_config_files = {"foo.yaml", "bar.yaml", "other.yaml"}
        tpc.extra_config_dirs = {"foo.d/", "bar.d/"}
        # Invalid for additional files
        self.assertFalse(master_files.isValidFor(tpc, min_ltime=-1))
        self.assertFalse(master_files.isValidFor(tpc, min_ltime=2))

    def test_cache_ltime(self):
        cache = self.config_cache.getFilesCache("project", "master")
        self.assertEqual(cache.ltime, -1)
        cache.setValidFor(set(), set(), ltime=1)
        self.assertEqual(cache.ltime, 1)

    def test_branch_cleanup(self):
        master_files = self.config_cache.getFilesCache("project", "master")
        release_files = self.config_cache.getFilesCache("project", "release")

        master_files["/path/to/file"] = "content"
        release_files["/path/to/file"] = "content"

        self.config_cache.clearCache("project", "master")
        self.assertEqual(len(master_files), 0)
        self.assertEqual(len(release_files), 1)

    def test_project_cleanup(self):
        master_files = self.config_cache.getFilesCache("project", "master")
        stable_files = self.config_cache.getFilesCache("project", "stable")
        other_files = self.config_cache.getFilesCache("other", "master")

        self.assertEqual(len(master_files), 0)
        self.assertEqual(len(stable_files), 0)
        master_files["/path/to/file"] = "content"
        stable_files["/path/to/file"] = "content"
        other_files["/path/to/file"] = "content"
        self.assertEqual(len(master_files), 1)
        self.assertEqual(len(stable_files), 1)
        self.assertEqual(len(other_files), 1)

        self.config_cache.clearCache("project")
        self.assertEqual(len(master_files), 0)
        self.assertEqual(len(stable_files), 0)
        self.assertEqual(len(other_files), 1)


class TestComponentRegistry(ZooKeeperBaseTestCase):
    def setUp(self):
        super().setUp()
        self.second_zk_client = ZooKeeperClient(
            self.zk_chroot_fixture.zk_hosts,
            tls_cert=self.zk_chroot_fixture.zookeeper_cert,
            tls_key=self.zk_chroot_fixture.zookeeper_key,
            tls_ca=self.zk_chroot_fixture.zookeeper_ca,
            timeout=ZOOKEEPER_SESSION_TIMEOUT,
        )
        self.addCleanup(self.second_zk_client.disconnect)
        self.second_zk_client.connect()
        self.second_component_registry = ComponentRegistry(
            self.second_zk_client)

    def assertComponentAttr(self, component_name, attr_name,
                            attr_value, timeout=25):
        for _ in iterate_timeout(
            timeout,
            f"{component_name} in cache has {attr_name} set to {attr_value}",
        ):
            components = list(self.second_component_registry.all(
                component_name))
            if (
                len(components) > 0 and
                getattr(components[0], attr_name) == attr_value
            ):
                break

    def assertComponentState(self, component_name, state, timeout=25):
        return self.assertComponentAttr(
            component_name, "state", state, timeout
        )

    def assertComponentStopped(self, component_name, timeout=25):
        for _ in iterate_timeout(
            timeout, f"{component_name} in cache is stopped"
        ):
            components = list(self.second_component_registry.all(
                component_name))
            if len(components) == 0:
                break

    def test_component_registry(self):
        self.component_info = ExecutorComponent(self.zk_client, 'test')
        self.component_info.register()
        self.assertComponentState("executor", BaseComponent.STOPPED)

        self.zk_client.client.stop()
        self.assertComponentStopped("executor")

        self.zk_client.client.start()
        self.assertComponentState("executor", BaseComponent.STOPPED)

        self.component_info.state = self.component_info.RUNNING
        self.assertComponentState("executor", BaseComponent.RUNNING)

        self.log.debug("DISCONNECT")
        self.second_zk_client.client.stop()
        self.second_zk_client.client.start()
        self.log.debug("RECONNECT")
        self.component_info.state = self.component_info.PAUSED
        self.assertComponentState("executor", BaseComponent.PAUSED)

        # Make sure the registry didn't create any read/write
        # component objects that re-registered themselves.
        components = list(self.second_component_registry.all('executor'))
        self.assertEqual(len(components), 1)

        self.component_info.state = self.component_info.RUNNING
        self.assertComponentState("executor", BaseComponent.RUNNING)


class TestExecutorApi(ZooKeeperBaseTestCase):
    def test_build_request(self):
        # Test the lifecycle of a build request
        request_queue = queue.Queue()
        event_queue = queue.Queue()

        # A callback closure for the request queue
        def rq_put():
            request_queue.put(None)

        # and the event queue
        def eq_put(br, e):
            event_queue.put((br, e))

        # Simulate the client side
        client = ExecutorApi(self.zk_client)
        self.addCleanup(client.stop)
        # Simulate the server side
        server = ExecutorApi(self.zk_client,
                             build_request_callback=rq_put,
                             build_event_callback=eq_put)
        self.addCleanup(server.stop)

        # Scheduler submits request
        request = BuildRequest(
            "A", None, None, "job", "job_uuid", "tenant", "pipeline", '1')
        client.submit(request, {'job': 'test'})
        request_queue.get(timeout=30)

        # Executor receives request
        reqs = list(server.next())
        self.assertEqual(len(reqs), 1)
        a = reqs[0]
        self.assertEqual(a.uuid, 'A')
        params = client.getParams(a)
        self.assertEqual(params, {'job': 'test'})
        client.clearParams(a)
        params = client.getParams(a)
        self.assertIsNone(params)

        # Executor locks request
        self.assertTrue(server.lock(a, blocking=False))
        a.state = BuildRequest.RUNNING
        server.update(a)
        self.getRequest(client, a.uuid, BuildRequest.RUNNING)

        # Executor should see no pending requests
        reqs = list(server.next())
        self.assertEqual(len(reqs), 0)

        # Executor pauses build
        a.state = BuildRequest.PAUSED
        server.update(a)
        self.getRequest(client, a.uuid, BuildRequest.PAUSED)

        # Scheduler resumes build
        self.assertTrue(event_queue.empty())
        sched_a = self.getRequest(client, a.uuid)
        client.requestResume(sched_a)
        (build_request, event) = event_queue.get(timeout=30)
        self.assertEqual(build_request, a)
        self.assertEqual(event, JobRequestEvent.RESUMED)

        # Executor resumes build
        a.state = BuildRequest.RUNNING
        server.update(a)
        server.fulfillResume(a)
        self.getRequest(client, a.uuid, BuildRequest.RUNNING)

        # Scheduler cancels build
        self.assertTrue(event_queue.empty())
        sched_a = self.getRequest(client, a.uuid)
        client.requestCancel(sched_a)
        (build_request, event) = event_queue.get(timeout=30)
        self.assertEqual(build_request, a)
        self.assertEqual(event, JobRequestEvent.CANCELED)

        # Executor aborts build
        a.state = BuildRequest.COMPLETED
        server.update(a)
        server.fulfillCancel(a)
        server.unlock(a)
        self.getRequest(client, a.uuid, BuildRequest.COMPLETED)

        # Scheduler removes build request on completion
        client.remove(sched_a)

        self.assertEqual(set(self.getZKPaths('/zuul/executor')),
                         set(['/zuul/executor/unzoned',
                              '/zuul/executor/unzoned/locks',
                              '/zuul/executor/unzoned/params',
                              '/zuul/executor/unzoned/requests',
                              '/zuul/executor/unzoned/result-data',
                              '/zuul/executor/unzoned/results',
                              '/zuul/executor/unzoned/waiters']))

    def test_build_request_remove(self):
        # Test the scheduler forcibly removing a request (perhaps the
        # tenant is being deleted, so there will be no result queue).
        request_queue = queue.Queue()
        event_queue = queue.Queue()

        def rq_put():
            request_queue.put(None)

        def eq_put(br, e):
            event_queue.put((br, e))

        # Simulate the client side
        client = ExecutorApi(self.zk_client)
        self.addCleanup(client.stop)
        # Simulate the server side
        server = ExecutorApi(self.zk_client,
                             build_request_callback=rq_put,
                             build_event_callback=eq_put)
        self.addCleanup(server.stop)

        # Scheduler submits request
        request = BuildRequest(
            "A", None, None, "job", "job_uuid", "tenant", "pipeline", '1')
        client.submit(request, {})
        request_queue.get(timeout=30)

        # Executor receives request
        reqs = list(server.next())
        self.assertEqual(len(reqs), 1)
        a = reqs[0]
        self.assertEqual(a.uuid, 'A')

        # Executor locks request
        self.assertTrue(server.lock(a, blocking=False))
        a.state = BuildRequest.RUNNING
        server.update(a)
        self.getRequest(client, a.uuid, MergeRequest.RUNNING)

        # Executor should see no pending requests
        reqs = list(server.next())
        self.assertEqual(len(reqs), 0)
        self.assertTrue(event_queue.empty())

        # Scheduler rudely removes build request
        sched_a = self.getRequest(client, a.uuid)
        client.remove(sched_a)

        # Make sure it shows up as deleted
        (build_request, event) = event_queue.get(timeout=30)
        self.assertEqual(build_request, a)
        self.assertEqual(event, JobRequestEvent.DELETED)

        # Executor should not write anything else since the request
        # was deleted.

    def test_build_request_hold(self):
        # Test that we can hold a build request in "queue"
        request_queue = queue.Queue()
        event_queue = queue.Queue()

        def rq_put():
            request_queue.put(None)

        def eq_put(br, e):
            event_queue.put((br, e))

        # Simulate the client side
        client = HoldableExecutorApi(self.zk_client)
        self.addCleanup(client.stop)
        client.hold_in_queue = True
        # Simulate the server side
        server = ExecutorApi(self.zk_client,
                             build_request_callback=rq_put,
                             build_event_callback=eq_put)
        self.addCleanup(server.stop)

        # Scheduler submits request
        request = BuildRequest(
            "A", None, None, "job", "job_uuid", "tenant", "pipeline", '1')
        client.submit(request, {})
        request_queue.get(timeout=30)

        # Executor receives nothing
        reqs = list(server.next())
        self.assertEqual(len(reqs), 0)

        # Test releases hold
        a = self.getRequest(client, request.uuid)
        a.state = BuildRequest.REQUESTED
        client.update(a)

        # Executor receives request
        request_queue.get(timeout=30)
        reqs = list(server.next())
        self.assertEqual(len(reqs), 1)
        a = reqs[0]
        self.assertEqual(a.uuid, 'A')

        # The rest is redundant.

    def test_nonexistent_lock(self):
        request_queue = queue.Queue()
        event_queue = queue.Queue()

        def rq_put():
            request_queue.put(None)

        def eq_put(br, e):
            event_queue.put((br, e))

        # Simulate the client side
        client = ExecutorApi(self.zk_client)
        self.addCleanup(client.stop)

        # Scheduler submits request
        request = BuildRequest(
            "A", None, None, "job", "job_uuid", "tenant", "pipeline", '1')
        client.submit(request, {})
        sched_a = self.getRequest(client, request.uuid)

        # Simulate the server side
        server = ExecutorApi(self.zk_client,
                             build_request_callback=rq_put,
                             build_event_callback=eq_put)
        self.addCleanup(server.stop)

        exec_a = self.getRequest(client, request.uuid)
        client.remove(sched_a)

        # Try to lock a request that was just removed
        self.assertFalse(server.lock(exec_a))

    def test_efficient_removal(self):
        # Test that we don't try to lock a removed request
        request_queue = queue.Queue()
        event_queue = queue.Queue()

        def rq_put():
            request_queue.put(None)

        def eq_put(br, e):
            event_queue.put((br, e))

        # Simulate the client side
        client = ExecutorApi(self.zk_client)
        self.addCleanup(client.stop)

        # Scheduler submits two requests
        request_a = BuildRequest(
            "A", None, None, "job", "job_uuid", "tenant", "pipeline", '1')
        client.submit(request_a, {})

        request_b = BuildRequest(
            "B", None, None, "job", "job_uuid", "tenant", "pipeline", '2')
        client.submit(request_b, {})
        sched_b = self.getRequest(client, request_b.uuid)

        request_c = BuildRequest(
            "C", None, None, "job", "job_uuid", "tenant", "pipeline", '3')
        client.submit(request_c, {})
        sched_c = self.getRequest(client, request_c.uuid)

        # Simulate the server side
        server = ExecutorApi(self.zk_client,
                             build_request_callback=rq_put,
                             build_event_callback=eq_put)
        self.addCleanup(server.stop)

        count = 0
        for exec_request in server.next():
            count += 1
            if count == 1:
                # Someone starts the second request and client removes
                # the third request all while we're processing the first.
                sched_b.state = sched_b.RUNNING
                client.update(sched_b)
                client.remove(sched_c)
                for _ in iterate_timeout(30, "cache to be up-to-date"):
                    if (len(server.zone_queues[None].cache._cached_objects
                            ) == 2):
                        break
        # Make sure we only got the first request
        self.assertEqual(count, 1)

    def test_lost_build_requests(self):
        # Test that lostBuildRequests() returns unlocked running build
        # requests
        executor_api = ExecutorApi(self.zk_client)
        self.addCleanup(executor_api.stop)

        br = BuildRequest(
            "A", "zone", None, "job", "job_uuid", "tenant", "pipeline", '1')
        executor_api.submit(br, {})

        br = BuildRequest(
            "B", None, None, "job", "job_uuid", "tenant", "pipeline", '1')
        executor_api.submit(br, {})
        b_uuid = br.uuid

        br = BuildRequest(
            "C", "zone", None, "job", "job_uuid", "tenant", "pipeline", '1')
        executor_api.submit(br, {})
        c_uuid = br.uuid

        br = BuildRequest(
            "D", "zone", None, "job", "job_uuid", "tenant", "pipeline", '1')
        executor_api.submit(br, {})
        d_uuid = br.uuid

        br = BuildRequest(
            "E", "zone", None, "job", "job_uuid", "tenant", "pipeline", '1')
        executor_api.submit(br, {})
        e_uuid = br.uuid

        executor_api.waitForSync()
        b = self.getRequest(executor_api, b_uuid)
        c = self.getRequest(executor_api, c_uuid)
        d = self.getRequest(executor_api, d_uuid)
        e = self.getRequest(executor_api, e_uuid)

        # Make sure the get() method used the correct zone keys
        self.assertEqual(set(executor_api.zone_queues.keys()), {"zone", None})

        b.state = BuildRequest.RUNNING
        executor_api.update(b)

        c.state = BuildRequest.RUNNING
        executor_api.lock(c)
        executor_api.update(c)

        d.state = BuildRequest.COMPLETED
        executor_api.update(d)

        e.state = BuildRequest.PAUSED
        executor_api.update(e)

        # The lost_builds method should only return builds which are running or
        # paused, but not locked by any executor, in this case build b and e.
        lost_build_requests = list(executor_api.lostRequests())

        self.assertEqual(2, len(lost_build_requests))
        self.assertEqual(b.path, lost_build_requests[0].path)

    def test_lost_build_request_params(self):
        # Test cleaning up orphaned request parameters
        executor_api = ExecutorApi(self.zk_client)
        self.addCleanup(executor_api.stop)

        br = BuildRequest(
            "A", "zone", None, "job", "job_uuid", "tenant", "pipeline", '1')
        executor_api.submit(br, {})

        params_root = executor_api.zone_queues['zone'].PARAM_ROOT
        self.assertEqual(len(executor_api._getAllRequestIds()), 1)
        self.assertEqual(len(
            self.zk_client.client.get_children(params_root)), 1)

        # Delete the request but not the params
        self.zk_client.client.delete(br.path)
        self.assertEqual(len(executor_api._getAllRequestIds()), 0)
        self.assertEqual(len(
            self.zk_client.client.get_children(params_root)), 1)

        # Clean up leaked params
        executor_api.cleanup(0)
        self.assertEqual(len(
            self.zk_client.client.get_children(params_root)), 0)

    def test_existing_build_request(self):
        # Test that an executor sees an existing build request when
        # coming online

        # Test the lifecycle of a build request
        request_queue = queue.Queue()
        event_queue = queue.Queue()

        # A callback closure for the request queue
        def rq_put():
            request_queue.put(None)

        # and the event queue
        def eq_put(br, e):
            event_queue.put((br, e))

        # Simulate the client side
        client = ExecutorApi(self.zk_client)
        self.addCleanup(client.stop)
        client.submit(
            BuildRequest(
                "A", None, None, "job", "job_uuid",
                "tenant", "pipeline", '1'), {})

        # Simulate the server side
        server = ExecutorApi(self.zk_client,
                             build_request_callback=rq_put,
                             build_event_callback=eq_put)
        self.addCleanup(server.stop)

        # Scheduler submits request
        request_queue.get(timeout=30)

        # Executor receives request
        reqs = list(server.next())
        self.assertEqual(len(reqs), 1)
        a = reqs[0]
        self.assertEqual(a.uuid, 'A')

    def test_unlock_request(self):
        # Test that locking and unlocking works
        request_queue = queue.Queue()
        event_queue = queue.Queue()

        # A callback closure for the request queue
        def rq_put():
            request_queue.put(None)

        # and the event queue
        def eq_put(br, e):
            event_queue.put((br, e))

        # Simulate the client side
        client = ExecutorApi(self.zk_client)
        self.addCleanup(client.stop)
        # Simulate the server side
        server = ExecutorApi(self.zk_client,
                             build_request_callback=rq_put,
                             build_event_callback=eq_put)
        self.addCleanup(server.stop)

        # Scheduler submits request
        request = BuildRequest(
            "A", None, None, "job", "job_uuid", "tenant", "pipeline", '1')
        client.submit(request, {'job': 'test'})
        request_queue.get(timeout=30)

        # Executor receives request
        reqs = list(server.next())
        self.assertEqual(len(reqs), 1)
        a = reqs[0]

        # Get a client copy of the request for later.  Normally the
        # client does not lock requests, but we will use this to
        # simulate a second executor attempting to lock a request
        # while our first executor operates.  This ensures the lock
        # contender counting works correctly.
        client_a = self.getRequest(client, a.uuid)

        # Executor locks request
        self.assertTrue(server.lock(a, blocking=False))

        # Someone else attempts to lock it
        t = threading.Thread(target=client.lock, args=(client_a, True))
        t.start()

        # Wait for is_locked to be updated and both lock contenders to
        # show in the cache:
        for _ in iterate_timeout(30, "lock to propagate"):
            r1 = self.getRequest(server, a.uuid)
            r2 = self.getRequest(client, a.uuid)
            if (r1.is_locked and
                r2.is_locked and
                r1.lock_contenders == 2 and
                r2.lock_contenders == 2):
                break

        # Should see no pending requests
        reqs = list(server.next())
        self.assertEqual(len(reqs), 0)
        reqs = list(client.next())
        self.assertEqual(len(reqs), 0)

        # Unlock request
        server.unlock(a)

        # Wait for client to get lock
        t.join()

        # Wait for lock_contenders to be updated:
        for _ in iterate_timeout(30, "lock to propagate"):
            r1 = self.getRequest(server, a.uuid)
            r2 = self.getRequest(client, a.uuid)
            if r1.lock_contenders == r2.lock_contenders == 1:
                break

        # Release client lock
        client.unlock(client_a)

        # Wait for is_locked to be updated:
        for _ in iterate_timeout(30, "lock to propagate"):
            r1 = self.getRequest(server, a.uuid)
            r2 = self.getRequest(client, a.uuid)
            if not r1.is_locked and not r2.is_locked:
                break

        # Should see pending requests
        reqs = list(server.next())
        self.assertEqual(len(reqs), 1)
        reqs = list(client.next())
        self.assertEqual(len(reqs), 1)
        client.remove(a)


class TestMergerApi(ZooKeeperBaseTestCase):
    def _assertEmptyRoots(self, client):
        self.assertEqual(self.getZKPaths(client.REQUEST_ROOT), [])
        self.assertEqual(self.getZKPaths(client.PARAM_ROOT), [])
        self.assertEqual(self.getZKPaths(client.RESULT_ROOT), [])
        self.assertEqual(self.getZKPaths(client.RESULT_DATA_ROOT), [])
        self.assertEqual(self.getZKPaths(client.WAITER_ROOT), [])
        self.assertEqual(self.getZKPaths(client.LOCK_ROOT), [])

    def test_merge_request(self):
        # Test the lifecycle of a merge request
        request_queue = queue.Queue()

        # A callback closure for the request queue
        def rq_put():
            request_queue.put(None)

        # Simulate the client side
        client = MergerApi(self.zk_client)
        # Simulate the server side
        server = MergerApi(self.zk_client,
                           merge_request_callback=rq_put)

        # Scheduler submits request
        payload = {'merge': 'test'}
        request = MergeRequest(
            uuid='A',
            job_type=MergeRequest.MERGE,
            build_set_uuid='AA',
            tenant_name='tenant',
            pipeline_name='check',
            event_id='1',
        )
        client.submit(request, payload)
        request_queue.get(timeout=30)

        # Merger receives request
        reqs = list(server.next())
        self.assertEqual(len(reqs), 1)
        a = reqs[0]
        self.assertEqual(a.uuid, 'A')
        params = client.getParams(a)
        self.assertEqual(params, payload)
        client.clearParams(a)
        params = client.getParams(a)
        self.assertIsNone(params)

        # Merger locks request
        self.assertTrue(server.lock(a, blocking=False))
        a.state = MergeRequest.RUNNING
        server.update(a)
        self.getRequest(client, a.uuid, MergeRequest.RUNNING)

        # Merger should see no pending requests
        reqs = list(server.next())
        self.assertEqual(len(reqs), 0)

        # Merger removes and unlocks merge request on completion
        server.remove(a)
        server.unlock(a)

        self._assertEmptyRoots(client)

    def test_merge_request_hold(self):
        # Test that we can hold a merge request in "queue"
        request_queue = queue.Queue()

        def rq_put():
            request_queue.put(None)

        # Simulate the client side
        client = HoldableMergerApi(self.zk_client)
        client.hold_in_queue = True
        # Simulate the server side
        server = MergerApi(self.zk_client,
                           merge_request_callback=rq_put)

        # Scheduler submits request
        payload = {'merge': 'test'}
        client.submit(MergeRequest(
            uuid='A',
            job_type=MergeRequest.MERGE,
            build_set_uuid='AA',
            tenant_name='tenant',
            pipeline_name='check',
            event_id='1',
        ), payload)
        request_queue.get(timeout=30)

        # Merger receives nothing
        reqs = list(server.next())
        self.assertEqual(len(reqs), 0)

        # Test releases hold
        # We have to get a new merge_request object to update it.
        a = self.getRequest(client, 'A')
        self.assertEqual(a.uuid, 'A')
        a.state = MergeRequest.REQUESTED
        client.update(a)

        # Merger receives request
        request_queue.get(timeout=30)
        reqs = list(server.next())
        self.assertEqual(len(reqs), 1)
        a = reqs[0]
        self.assertEqual(a.uuid, 'A')

        server.remove(a)
        # The rest is redundant.
        self._assertEmptyRoots(client)

    def test_merge_request_result(self):
        # Test the lifecycle of a merge request
        request_queue = queue.Queue()

        # A callback closure for the request queue
        def rq_put():
            request_queue.put(None)

        # Simulate the client side
        client = MergerApi(self.zk_client)
        # Simulate the server side
        server = MergerApi(self.zk_client,
                           merge_request_callback=rq_put)

        # Scheduler submits request
        payload = {'merge': 'test'}
        future = client.submit(MergeRequest(
            uuid='A',
            job_type=MergeRequest.MERGE,
            build_set_uuid='AA',
            tenant_name='tenant',
            pipeline_name='check',
            event_id='1',
        ), payload, needs_result=True)
        request_queue.get(timeout=30)

        # Merger receives request
        reqs = list(server.next())
        self.assertEqual(len(reqs), 1)
        a = reqs[0]
        self.assertEqual(a.uuid, 'A')

        # Merger locks request
        self.assertTrue(server.lock(a, blocking=False))
        a.state = MergeRequest.RUNNING
        server.update(a)
        self.getRequest(client, a.uuid, MergeRequest.RUNNING)

        # Merger reports result
        result_data = {'result': 'ok'}
        server.reportResult(a, result_data)

        self.assertEqual(set(self.getZKPaths(client.RESULT_ROOT)),
                         set(['/zuul/merger/results/A']))
        self.assertEqual(set(self.getZKPaths(client.RESULT_DATA_ROOT)),
                         set(['/zuul/merger/result-data/A',
                              '/zuul/merger/result-data/A/0000000000']))
        self.assertEqual(self.getZKPaths(client.WAITER_ROOT),
                         ['/zuul/merger/waiters/A'])

        # Merger removes and unlocks merge request on completion
        server.remove(a)
        server.unlock(a)

        # Scheduler awaits result
        self.assertTrue(future.wait())
        self.assertEqual(future.data, result_data)

        self._assertEmptyRoots(client)

    def test_lost_merge_request_params(self):
        # Test cleaning up orphaned request parameters
        merger_api = MergerApi(self.zk_client)

        # Scheduler submits request
        payload = {'merge': 'test'}
        merger_api.submit(MergeRequest(
            uuid='A',
            job_type=MergeRequest.MERGE,
            build_set_uuid='AA',
            tenant_name='tenant',
            pipeline_name='check',
            event_id='1',
        ), payload)
        path_a = '/'.join([merger_api.REQUEST_ROOT, 'A'])

        params_root = merger_api.PARAM_ROOT
        self.assertEqual(len(merger_api._getAllRequestIds()), 1)
        self.assertEqual(len(
            self.zk_client.client.get_children(params_root)), 1)

        # Delete the request but not the params
        self.zk_client.client.delete(path_a)
        self.assertEqual(len(merger_api._getAllRequestIds()), 0)
        self.assertEqual(len(
            self.zk_client.client.get_children(params_root)), 1)

        # Clean up leaked params
        merger_api.cleanup(0)
        self.assertEqual(len(
            self.zk_client.client.get_children(params_root)), 0)

        self._assertEmptyRoots(merger_api)

    def test_lost_merge_request_result(self):
        # Test that we can clean up orphaned merge results
        request_queue = queue.Queue()

        # A callback closure for the request queue
        def rq_put():
            request_queue.put(None)

        # Simulate the client side
        client = MergerApi(self.zk_client)
        # Simulate the server side
        server = MergerApi(self.zk_client,
                           merge_request_callback=rq_put)

        # Scheduler submits request
        payload = {'merge': 'test'}
        future = client.submit(MergeRequest(
            uuid='A',
            job_type=MergeRequest.MERGE,
            build_set_uuid='AA',
            tenant_name='tenant',
            pipeline_name='check',
            event_id='1',
        ), payload, needs_result=True)

        request_queue.get(timeout=30)

        # Merger receives request
        reqs = list(server.next())
        self.assertEqual(len(reqs), 1)
        a = reqs[0]
        self.assertEqual(a.uuid, 'A')

        # Merger locks request
        self.assertTrue(server.lock(a, blocking=False))
        a.state = MergeRequest.RUNNING
        server.update(a)
        a = self.getRequest(client, a.uuid, MergeRequest.RUNNING)

        # Merger reports result
        result_data = {'result': 'ok'}
        server.reportResult(a, result_data)

        # Merger removes and unlocks merge request on completion
        server.remove(a)
        server.unlock(a)

        self.assertEqual(set(self.getZKPaths(client.RESULT_ROOT)),
                         set(['/zuul/merger/results/A']))
        self.assertEqual(set(self.getZKPaths(client.RESULT_DATA_ROOT)),
                         set(['/zuul/merger/result-data/A',
                              '/zuul/merger/result-data/A/0000000000']))
        self.assertEqual(self.getZKPaths(client.WAITER_ROOT),
                         ['/zuul/merger/waiters/A'])

        # Scheduler "disconnects"
        self.zk_client.client.delete(future._waiter_path)

        # Find orphaned results
        client.cleanup(age=0)

        self._assertEmptyRoots(client)

    def test_nonexistent_lock(self):
        request_queue = queue.Queue()

        def rq_put():
            request_queue.put(None)

        # Simulate the client side
        client = MergerApi(self.zk_client)

        # Scheduler submits request
        payload = {'merge': 'test'}
        client.submit(MergeRequest(
            uuid='A',
            job_type=MergeRequest.MERGE,
            build_set_uuid='AA',
            tenant_name='tenant',
            pipeline_name='check',
            event_id='1',
        ), payload)
        client_a = self.getRequest(client, 'A')

        # Simulate the server side
        server = MergerApi(self.zk_client,
                           merge_request_callback=rq_put)
        request_queue.get(timeout=30)
        server_a = list(server.next())[0]

        client.remove(client_a)

        # Try to lock a request that was just removed
        self.assertFalse(server.lock(server_a))
        self._assertEmptyRoots(client)

    def test_efficient_removal(self):
        # Test that we don't try to lock a removed request
        request_queue = queue.Queue()
        event_queue = queue.Queue()

        def rq_put():
            request_queue.put(None)

        def eq_put(br, e):
            event_queue.put((br, e))

        # Simulate the client side
        client = MergerApi(self.zk_client)

        # Scheduler submits three requests
        payload = {'merge': 'test'}
        client.submit(MergeRequest(
            uuid='A',
            job_type=MergeRequest.MERGE,
            build_set_uuid='AA',
            tenant_name='tenant',
            pipeline_name='check',
            event_id='1',
        ), payload)

        client.submit(MergeRequest(
            uuid='B',
            job_type=MergeRequest.MERGE,
            build_set_uuid='BB',
            tenant_name='tenant',
            pipeline_name='check',
            event_id='2',
        ), payload)
        client_b = self.getRequest(client, 'B')

        client.submit(MergeRequest(
            uuid='C',
            job_type=MergeRequest.MERGE,
            build_set_uuid='CC',
            tenant_name='tenant',
            pipeline_name='check',
            event_id='2',
        ), payload)
        client_c = self.getRequest(client, 'C')

        # Simulate the server side
        server = MergerApi(self.zk_client,
                           merge_request_callback=rq_put)
        request_queue.get(timeout=30)

        count = 0
        for merge_request in server.next():
            count += 1
            if count == 1:
                # Someone starts the second request and client removes
                # the third request all while we're processing the first.
                client_b.state = client_b.RUNNING
                client.update(client_b)
                client.remove(client_c)
        # Make sure we only got the first request
        self.assertEqual(count, 1)

    def test_leaked_lock(self):
        client = MergerApi(self.zk_client)

        # Manually create a lock with no underlying request
        self.zk_client.client.create(f"{client.LOCK_ROOT}/A", b'')

        client.cleanup(0)
        self._assertEmptyRoots(client)

    def test_lost_merge_requests(self):
        # Test that lostMergeRequests() returns unlocked running merge
        # requests
        merger_api = MergerApi(self.zk_client)

        payload = {'merge': 'test'}
        merger_api.submit(MergeRequest(
            uuid='A',
            job_type=MergeRequest.MERGE,
            build_set_uuid='AA',
            tenant_name='tenant',
            pipeline_name='check',
            event_id='1',
        ), payload)
        merger_api.submit(MergeRequest(
            uuid='B',
            job_type=MergeRequest.MERGE,
            build_set_uuid='BB',
            tenant_name='tenant',
            pipeline_name='check',
            event_id='1',
        ), payload)
        merger_api.submit(MergeRequest(
            uuid='C',
            job_type=MergeRequest.MERGE,
            build_set_uuid='CC',
            tenant_name='tenant',
            pipeline_name='check',
            event_id='1',
        ), payload)
        merger_api.submit(MergeRequest(
            uuid='D',
            job_type=MergeRequest.MERGE,
            build_set_uuid='DD',
            tenant_name='tenant',
            pipeline_name='check',
            event_id='1',
        ), payload)

        b = self.getRequest(merger_api, 'B')
        c = self.getRequest(merger_api, 'C')
        d = self.getRequest(merger_api, 'D')

        b.state = MergeRequest.RUNNING
        merger_api.update(b)

        merger_api.lock(c)
        c.state = MergeRequest.RUNNING
        merger_api.update(c)

        d.state = MergeRequest.COMPLETED
        merger_api.update(d)

        # The lost_merges method should only return merges which are running
        # but not locked by any merger, in this case merge b
        lost_merge_requests = list(merger_api.lostRequests())

        self.assertEqual(1, len(lost_merge_requests))
        self.assertEqual(b.path, lost_merge_requests[0].path)

        # This test does not clean them up, so we can't assert empty roots

    def test_existing_merge_request(self):
        # Test that a merger sees an existing merge request when
        # coming online

        # Test the lifecycle of a merge request
        request_queue = queue.Queue()

        # A callback closure for the request queue
        def rq_put():
            request_queue.put(None)

        # Simulate the client side
        client = MergerApi(self.zk_client)
        payload = {'merge': 'test'}
        client.submit(MergeRequest(
            uuid='A',
            job_type=MergeRequest.MERGE,
            build_set_uuid='AA',
            tenant_name='tenant',
            pipeline_name='check',
            event_id='1',
        ), payload)

        # Simulate the server side
        server = MergerApi(self.zk_client,
                           merge_request_callback=rq_put)

        # Scheduler submits request
        request_queue.get(timeout=30)

        # Merger receives request
        reqs = list(server.next())
        self.assertEqual(len(reqs), 1)
        a = reqs[0]
        self.assertEqual(a.uuid, 'A')

        client.remove(a)
        self._assertEmptyRoots(client)


class TestLocks(ZooKeeperBaseTestCase):

    def test_locking_ctx(self):
        lock = self.zk_client.client.Lock("/lock")
        with locked(lock) as ctx_lock:
            self.assertIs(lock, ctx_lock)
            self.assertTrue(lock.is_acquired)
        self.assertFalse(lock.is_acquired)

    def test_already_locked_ctx(self):
        lock = self.zk_client.client.Lock("/lock")
        other_lock = self.zk_client.client.Lock("/lock")
        other_lock.acquire()
        with testtools.ExpectedException(
            LockException, "Failed to acquire lock .*"
        ):
            with locked(lock, blocking=False):
                pass
        self.assertFalse(lock.is_acquired)

    def test_unlock_exception(self):
        lock = self.zk_client.client.Lock("/lock")
        with testtools.ExpectedException(RuntimeError):
            with locked(lock):
                self.assertTrue(lock.is_acquired)
                raise RuntimeError
        self.assertFalse(lock.is_acquired)


class TestLayoutStore(ZooKeeperBaseTestCase):

    def test_layout_state(self):
        store = LayoutStateStore(self.zk_client, lambda: None)
        layout_uuid = uuid.uuid4().hex
        branch_cache_min_ltimes = {
            "gerrit": 123,
            "github": 456,
        }
        state = LayoutState("tenant", "hostname", 0, layout_uuid,
                            branch_cache_min_ltimes, -1)
        store["tenant"] = state
        self.assertEqual(state, store["tenant"])
        self.assertNotEqual(state.ltime, -1)
        self.assertNotEqual(store["tenant"].ltime, -1)
        self.assertEqual(store["tenant"].branch_cache_min_ltimes,
                         branch_cache_min_ltimes)

    def test_ordering(self):
        layout_uuid = uuid.uuid4().hex
        state_one = LayoutState("tenant", "hostname", 1, layout_uuid,
                                {}, -1, ltime=1)
        state_two = LayoutState("tenant", "hostname", 2, layout_uuid,
                                {}, -1, ltime=2)

        self.assertGreater(state_two, state_one)

    def test_cleanup(self):
        store = LayoutStateStore(self.zk_client, lambda: None)
        min_ltimes = defaultdict(lambda x: -1)
        min_ltimes['foo'] = 1
        state_one = LayoutState("tenant", "hostname", 1, uuid.uuid4().hex,
                                {}, -1, ltime=1)
        state_two = LayoutState("tenant", "hostname", 2, uuid.uuid4().hex,
                                {}, -1, ltime=2)
        store.setMinLtimes(state_one, min_ltimes)
        store.setMinLtimes(state_two, min_ltimes)
        store['tenant'] = state_one
        # Run with the default delay of 5 minutes; nothing should be deleted.
        store.cleanup()
        self.assertEqual(store.get('tenant'), state_one)
        self.assertIsNotNone(
            self.zk_client.client.exists(
                f'/zuul/layout-data/{state_one.uuid}'))
        self.assertIsNotNone(
            self.zk_client.client.exists(
                f'/zuul/layout-data/{state_two.uuid}'))
        # Run again with immediate deletion
        store.cleanup(delay=0)
        self.assertEqual(store.get('tenant'), state_one)
        self.assertIsNotNone(
            self.zk_client.client.exists(
                f'/zuul/layout-data/{state_one.uuid}'))
        self.assertIsNone(
            self.zk_client.client.exists(
                f'/zuul/layout-data/{state_two.uuid}'))


class TestSystemConfigCache(ZooKeeperBaseTestCase):

    def setUp(self):
        super().setUp()
        self.config_cache = SystemConfigCache(self.zk_client, lambda: None)

    def test_set_get(self):
        uac = model.UnparsedAbideConfig()
        uac.tenants = {"foo": "bar"}
        uac.authz_rules = ["bar", "foo"]
        attrs = model.SystemAttributes.fromDict({
            "use_relative_priority": True,
            "max_hold_expiration": 7200,
            "default_hold_expiration": 3600,
            "default_ansible_version": "X",
            "web_root": "/web/root",
            "websocket_url": "/web/socket",
            "oidc_signing_key_rotation_interval": 3600,
            "oidc_supported_signing_algorithms": ["RS256", "ES256"],
            "oidc_default_signing_algorithm": "RS256",
        })
        self.config_cache.set(uac, attrs)

        uac_cached, cached_attrs = self.config_cache.get()
        self.assertEqual(uac.uuid, uac_cached.uuid)
        self.assertEqual(uac.tenants, uac_cached.tenants)
        self.assertEqual(uac.authz_rules, uac_cached.authz_rules)
        self.assertEqual(attrs, cached_attrs)

    def test_cache_empty(self):
        with testtools.ExpectedException(RuntimeError):
            self.config_cache.get()

    def test_ltime(self):
        uac = model.UnparsedAbideConfig()
        attrs = model.SystemAttributes()

        self.assertEqual(self.config_cache.ltime, -1)

        self.config_cache.set(uac, attrs)
        self.assertGreater(self.config_cache.ltime, -1)
        self.assertEqual(uac.ltime, self.config_cache.ltime)

        old_ltime = self.config_cache.ltime
        self.config_cache.set(uac, attrs)
        self.assertGreater(self.config_cache.ltime, old_ltime)
        self.assertEqual(uac.ltime, self.config_cache.ltime)

        cache_uac, _ = self.config_cache.get()
        self.assertEqual(uac.ltime, cache_uac.ltime)

    def test_valid(self):
        uac = model.UnparsedAbideConfig()
        attrs = model.SystemAttributes()

        self.assertFalse(self.config_cache.is_valid)

        self.config_cache.set(uac, attrs)
        self.assertTrue(self.config_cache.is_valid)


class DummyChange:

    def __init__(self, project, data=None):
        self.uid = uuid.uuid4().hex
        self.project = project
        self.cache_stat = None
        if data is not None:
            self.deserialize(data)

    @property
    def cache_version(self):
        return -1 if self.cache_stat is None else self.cache_stat.version

    def serialize(self):
        d = self.__dict__.copy()
        d.pop('cache_stat')
        return d

    def deserialize(self, data):
        self.__dict__.update(data)

    def getRelatedChanges(self, sched, relevant):
        return


class DummyChangeCache(AbstractChangeCache):
    CHANGE_TYPE_MAP = {
        "DummyChange": DummyChange,
    }


class DummySource:
    def getProject(self, project_name):
        return project_name

    def getChange(self, change_key):
        return DummyChange('project')


class DummyConnections:
    def getSource(self, name):
        return DummySource()


class DummyScheduler:
    def __init__(self):
        self.connections = DummyConnections()


class DummyConnection:
    def __init__(self):
        self.connection_name = "DummyConnection"
        self.source = DummySource()
        self.sched = DummyScheduler()


class TestChangeCache(ZooKeeperBaseTestCase):

    def setUp(self):
        super().setUp()
        self.cache = DummyChangeCache(self.zk_client, DummyConnection())
        self.addCleanup(self.cache.stop)

    def test_insert(self):
        change_foo = DummyChange("project", {"foo": "bar"})
        change_bar = DummyChange("project", {"bar": "foo"})
        key_foo = ChangeKey('conn', 'project', 'change', 'foo', '1')
        key_bar = ChangeKey('conn', 'project', 'change', 'bar', '1')
        self.cache.set(key_foo, change_foo)
        self.cache.set(key_bar, change_bar)

        self.assertEqual(self.cache.get(key_foo), change_foo)
        self.assertEqual(self.cache.get(key_bar), change_bar)

        compressed_size, uncompressed_size = self.cache.estimateDataSize()
        self.assertTrue(compressed_size != uncompressed_size != 0)

    def test_update(self):
        change = DummyChange("project", {"foo": "bar"})
        key = ChangeKey('conn', 'project', 'change', 'foo', '1')
        self.cache.set(key, change)

        change.number = 123
        self.cache.set(key, change, change.cache_version)

        # The change instance must stay the same
        updated_change = self.cache.get(key)
        self.assertIs(change, updated_change)
        self.assertEqual(change.number, 123)

    def test_delete(self):
        change = DummyChange("project", {"foo": "bar"})
        key = ChangeKey('conn', 'project', 'change', 'foo', '1')
        self.cache.set(key, change)
        self.cache.delete(key)
        self.assertIsNone(self.cache.get(key))

        # Deleting an non-existent key should not raise an exception
        invalid_key = ChangeKey('conn', 'project', 'change', 'invalid', '1')
        self.cache.delete(invalid_key)

    def test_concurrent_delete(self):
        change = DummyChange("project", {"foo": "bar"})
        key = ChangeKey('conn', 'project', 'change', 'foo', '1')
        self.cache.set(key, change)
        old_version = change.cache_version
        # Simulate someone updating the cache after we decided to
        # delete the entry
        self.cache.set(key, change, old_version)
        self.assertNotEqual(old_version, change.cache_version)
        self.cache.delete(key, old_version)
        # The change should still be in the cache
        self.assertIsNotNone(self.cache.get(key))

    def test_prune(self):
        change1 = DummyChange("project", {"foo": "bar"})
        change2 = DummyChange("project", {"foo": "baz"})
        key1 = ChangeKey('conn', 'project', 'change', 'foo', '1')
        key2 = ChangeKey('conn', 'project', 'change', 'foo', '2')
        self.cache.set(key1, change1)
        self.cache.set(key2, change2)
        self.cache.prune([key1], max_age=0)
        self.assertIsNotNone(self.cache.get(key1))
        self.assertIsNone(self.cache.get(key2))

    def test_concurrent_update(self):
        change = DummyChange("project", {"foo": "bar"})
        key = ChangeKey('conn', 'project', 'change', 'foo', '1')
        self.cache.set(key, change)

        # Attempt to update with the old change stat
        with testtools.ExpectedException(ConcurrentUpdateError):
            self.cache.set(key, change, change.cache_version - 1)

    def test_change_update_retry(self):
        change = DummyChange("project", {"foobar": 0})
        key = ChangeKey('conn', 'project', 'change', 'foo', '1')
        self.cache.set(key, change)

        # Update the change so we have a new cache stat.
        change.foobar = 1
        self.cache.set(key, change, change.cache_version)
        self.assertEqual(self.cache.get(key).foobar, 1)

        def updater(c):
            c.foobar += 1

        # Change the cache stat so the change is considered outdated and we
        # need to retry because of a concurrent update error.
        change.cache_stat = model.CacheStat(change.cache_stat.key,
                                            uuid.uuid4().hex,
                                            change.cache_version - 1,
                                            change.cache_stat.mzxid - 1,
                                            0, 0, 0)
        updated_change = self.cache.updateChangeWithRetry(
            key, change, updater)
        self.assertEqual(updated_change.foobar, 2)

    def test_cache_sync(self):
        other_cache = DummyChangeCache(self.zk_client, DummyConnection())
        key = ChangeKey('conn', 'project', 'change', 'foo', '1')
        change = DummyChange("project", {"foo": "bar"})
        self.cache.set(key, change)
        self.assertIsNotNone(other_cache.get(key))

        change_other = other_cache.get(key)
        change_other.number = 123
        other_cache.set(key, change_other, change_other.cache_version)

        for _ in iterate_timeout(10, "update to propagate"):
            if getattr(change, "number", None) == 123:
                break

        other_cache.delete(key)
        self.assertIsNone(self.cache.get(key))

    def test_cache_sync_on_start(self):
        key = ChangeKey('conn', 'project', 'change', 'foo', '1')
        change = DummyChange("project", {"foo": "bar"})
        self.cache.set(key, change)
        change.number = 123
        self.cache.set(key, change, change.cache_version)

        other_cache = DummyChangeCache(self.zk_client, DummyConnection())
        other_cache.cleanup()
        other_cache.cleanup()
        self.assertIsNotNone(other_cache.get(key))

    def test_cleanup(self):
        change = DummyChange("project", {"foo": "bar"})
        key = ChangeKey('conn', 'project', 'change', 'foo', '1')
        self.cache.set(key, change)

        self.cache.cleanup()
        self.assertEqual(len(self.cache._data_cleanup_candidates), 0)
        self.assertEqual(
            len(self.zk_client.client.get_children(self.cache.data_root)), 1)

        change.number = 123
        self.cache.set(key, change, change.cache_version)

        self.cache.cleanup()
        self.assertEqual(len(self.cache._data_cleanup_candidates), 1)
        self.assertEqual(
            len(self.zk_client.client.get_children(self.cache.data_root)), 2)

        self.cache.cleanup()
        self.assertEqual(len(self.cache._data_cleanup_candidates), 0)
        self.assertEqual(
            len(self.zk_client.client.get_children(self.cache.data_root)), 1)

    def test_watch_cleanup(self):
        change = DummyChange("project", {"foo": "bar"})
        key = ChangeKey('conn', 'project', 'change', 'foo', '1')
        self.cache.set(key, change)

        for _ in iterate_timeout(10, "watch to be registered"):
            if change.cache_stat.key._hash in self.cache._watched_keys:
                break

        self.cache.delete(key)
        self.assertIsNone(self.cache.get(key))

        for _ in iterate_timeout(10, "watch to be removed"):
            if change.cache_stat.key._hash not in self.cache._watched_keys:
                break
        # No need for extra waiting here


class DummyZKObjectMixin:
    _retry_interval = 0.1

    def getPath(self):
        return f'/zuul/pipeline/{self.name}'

    def serialize(self, context):
        d = {'name': self.name,
             'foo': self.foo}
        return json.dumps(d).encode('utf-8')


class DummyZKObject(DummyZKObjectMixin, ZKObject):
    pass


class DummyNoMakepathZKObject(DummyZKObjectMixin, ZKObject):
    makepath = False


class DummyShardedZKObject(DummyZKObjectMixin, ShardedZKObject):
    pass


class TestZKObject(ZooKeeperBaseTestCase):
    def _test_zk_object(self, zkobject_class):
        stop_event = threading.Event()
        self.zk_client.client.create('/zuul/pipeline', makepath=True)
        # Create a new object
        tenant_name = 'fake_tenant'
        with tenant_write_lock(self.zk_client, tenant_name) as lock:
            context = ZKContext(self.zk_client, lock, stop_event, self.log)
            pipeline1 = zkobject_class.new(context,
                                           name=tenant_name,
                                           foo='bar')
            self.assertEqual(pipeline1.foo, 'bar')

        compressed_size, uncompressed_size = pipeline1.estimateDataSize()
        self.assertTrue(compressed_size != uncompressed_size != 0)

        # Load an object from ZK (that we don't already have)
        with tenant_write_lock(self.zk_client, tenant_name) as lock:
            context = ZKContext(self.zk_client, lock, stop_event, self.log)
            pipeline2 = zkobject_class.fromZK(context,
                                              '/zuul/pipeline/fake_tenant')
            self.assertEqual(pipeline2.foo, 'bar')

        compressed_size, uncompressed_size = pipeline2.estimateDataSize()
        self.assertTrue(compressed_size != uncompressed_size != 0)

        # Test that nested ZKObject sizes are summed up correctly
        p1_compressed, p1_uncompressed = pipeline1.estimateDataSize()
        p2_compressed, p2_uncompressed = pipeline2.estimateDataSize()
        pipeline2._set(other=pipeline1)
        compressed_size, uncompressed_size = pipeline2.estimateDataSize()
        self.assertEqual(compressed_size, p1_compressed + p2_compressed)
        self.assertEqual(uncompressed_size, p1_uncompressed + p2_uncompressed)

        def get_ltime(obj):
            zstat = self.zk_client.client.exists(obj.getPath())
            return zstat.last_modified_transaction_id

        # Update an object
        with (tenant_write_lock(self.zk_client, tenant_name) as lock,
              ZKContext(
                  self.zk_client, lock, stop_event, self.log) as context):
            ltime1 = get_ltime(pipeline1)
            pipeline1.updateAttributes(context, foo='qux')
            self.assertEqual(pipeline1.foo, 'qux')
            ltime2 = get_ltime(pipeline1)
            self.assertNotEqual(ltime1, ltime2)

            # This should not produce an unnecessary write
            pipeline1.updateAttributes(context, foo='qux')
            ltime3 = get_ltime(pipeline1)
            self.assertEqual(ltime2, ltime3)

        # Update an object using an active context
        with (tenant_write_lock(self.zk_client, tenant_name) as lock,
              ZKContext(
                  self.zk_client, lock, stop_event, self.log) as context):
            ltime1 = get_ltime(pipeline1)
            with pipeline1.activeContext(context):
                pipeline1.foo = 'baz'
            self.assertEqual(pipeline1.foo, 'baz')
            ltime2 = get_ltime(pipeline1)
            self.assertNotEqual(ltime1, ltime2)

            # This should not produce an unnecessary write
            with pipeline1.activeContext(context):
                pipeline1.foo = 'baz'
            ltime3 = get_ltime(pipeline1)
            self.assertEqual(ltime2, ltime3)

        # Update of object w/o active context should not work
        with testtools.ExpectedException(Exception):
            pipeline1.foo = 'nope'
        self.assertEqual(pipeline1.foo, 'baz')

        # Refresh an existing object
        with (tenant_write_lock(self.zk_client, tenant_name) as lock,
              ZKContext(
                  self.zk_client, lock, stop_event, self.log) as context):
            pipeline2.refresh(context)
            self.assertEqual(pipeline2.foo, 'baz')

        # Delete an object
        with (tenant_write_lock(self.zk_client, tenant_name) as lock,
             ZKContext(self.zk_client, lock, stop_event, self.log) as context):
            self.assertIsNotNone(self.zk_client.client.exists(
                '/zuul/pipeline/fake_tenant'))
            pipeline2.delete(context)
            self.assertIsNone(self.zk_client.client.exists(
                '/zuul/pipeline/fake_tenant'))

    def _test_zk_object_exception(self, zkobject_class):
        # Exercise the exception handling in the _save method
        stop_event = threading.Event()
        self.zk_client.client.create('/zuul/pipeline', makepath=True)
        # Create a new object
        tenant_name = 'fake_tenant'

        class ZKFailsOnUpdate:
            def delete(self, *args, **kw):
                raise ZookeeperError()

            def set(self, *args, **kw):
                raise ZookeeperError()

        class FailsOnce:
            def __init__(self, real_client):
                self.count = 0
                self._real_client = real_client

            def create(self, *args, **kw):
                return self._real_client.create(*args, **kw)

            def delete(self, *args, **kw):
                self.count += 1
                if self.count < 2:
                    raise OperationTimeoutError()
                return self._real_client.delete(*args, **kw)

            def set(self, *args, **kw):
                self.count += 1
                if self.count < 2:
                    raise OperationTimeoutError()
                return self._real_client.set(*args, **kw)

            def exists(self, *args, **kw):
                return self._real_client.exists(*args, **kw)

            def ensure_path(self, *args, **kw):
                return self._real_client.ensure_path(*args, **kw)

        # Fail an update
        with (tenant_write_lock(self.zk_client, tenant_name) as lock,
              ZKContext(
                  self.zk_client, lock, stop_event, self.log) as context):
            pipeline1 = zkobject_class.new(context,
                                           name=tenant_name,
                                           foo='one')
            self.assertEqual(pipeline1.foo, 'one')

            # Simulate a fatal ZK exception
            context.client = ZKFailsOnUpdate()
            with testtools.ExpectedException(ZookeeperError):
                pipeline1.updateAttributes(context, foo='two')

            # We should still have the old attribute
            self.assertEqual(pipeline1.foo, 'one')

            # Any other error is retryable
            context.client = FailsOnce(self.zk_client.client)
            pipeline1.updateAttributes(context, foo='three')

            # This time it should be updated
            self.assertEqual(pipeline1.foo, 'three')

            # Repeat test using an active context
            context.client = ZKFailsOnUpdate()
            with testtools.ExpectedException(ZookeeperError):
                with pipeline1.activeContext(context):
                    pipeline1.foo = 'four'
            self.assertEqual(pipeline1.foo, 'three')

            context.client = FailsOnce(self.zk_client.client)
            with pipeline1.activeContext(context):
                pipeline1.foo = 'five'
            self.assertEqual(pipeline1.foo, 'five')

    def _test_zk_object_too_large(self, zkobject_class):
        # This produces a consistent string that compresses to > 1MiB
        rnd = random.Random()
        rnd.seed(42)
        size = 1024 * 1200
        foo = ''.join(rnd.choice(string.printable) for x in range(size))

        stop_event = threading.Event()
        self.zk_client.client.create('/zuul/pipeline', makepath=True)
        # Create a new object
        tenant_name = 'fake_tenant'
        with tenant_write_lock(self.zk_client, tenant_name) as lock:
            context = ZKContext(self.zk_client, lock, stop_event, self.log)
            with testtools.ExpectedException(Exception,
                                             'ZK data size too large'):
                pipeline1 = zkobject_class.new(context,
                                               name=tenant_name,
                                               foo=foo)

            pipeline1 = zkobject_class.new(context,
                                           name=tenant_name,
                                           foo='foo')

            with testtools.ExpectedException(Exception,
                                             'ZK data size too large'):
                pipeline1.updateAttributes(context, foo=foo)

            # Refresh an existing object
            pipeline1.refresh(context)
            self.assertEqual(pipeline1.foo, 'foo')

    def test_zk_object(self):
        self._test_zk_object(DummyZKObject)

    def test_zk_object_no_makepath(self):
        with testtools.ExpectedException(NoNodeError):
            with ZKContext(self.zk_client, None, None, self.log) as context:
                DummyNoMakepathZKObject.new(context, name="tenant", foo="bar")

    def test_sharded_zk_object(self):
        self._test_zk_object(DummyShardedZKObject)

    def test_zk_object_exception(self):
        self._test_zk_object_exception(DummyZKObject)

    def test_sharded_zk_object_exception(self):
        self._test_zk_object_exception(DummyShardedZKObject)

    def test_zk_object_too_large(self):
        # This only makes sense for a regular zkobject
        self._test_zk_object_too_large(DummyZKObject)


class TestBranchCache(ZooKeeperBaseTestCase):
    def test_branch_cache_protected_then_all(self):
        conn = DummyConnection()
        cache = BranchCache(self.zk_client, conn, self.component_registry)

        protected_flags = BranchFlag.PROTECTED
        all_flags = BranchFlag.PRESENT
        test_data = {
            'project1': {
                'all': [
                    BranchInfo('protected1', present=True),
                    BranchInfo('protected2', present=True),
                    BranchInfo('unprotected1', present=True),
                    BranchInfo('unprotected2', present=True),
                ],
                'protected': [
                    BranchInfo('protected1', protected=True),
                    BranchInfo('protected2', protected=True),
                ],
            },
        }

        # Test a protected-only query followed by all
        cache.setProjectBranches('project1', protected_flags,
                                 test_data['project1']['protected'])
        self.assertEqual(
            sorted([bi.name for bi in
                    cache.getProjectBranches('project1', protected_flags)
                    if bi.protected is True]),
            [bi.name for bi in test_data['project1']['protected']]
        )
        self.assertRaises(
            LookupError,
            lambda: cache.getProjectBranches('project1', all_flags),
        )

        cache.setProjectBranches('project1', all_flags,
                                 test_data['project1']['all'])
        self.assertEqual(
            sorted([bi.name for bi in
                    cache.getProjectBranches('project1', protected_flags)
                    if bi.protected is True]),
            [bi.name for bi in test_data['project1']['protected']]
        )
        self.assertEqual(
            sorted([bi.name for bi in
                    cache.getProjectBranches('project1', all_flags)]),
            [bi.name for bi in test_data['project1']['all']]
        )

        # There's a lot of exception catching in the branch cache,
        # so exercise a serialize/deserialize cycle.
        ctx = ZKContext(self.zk_client, None, None, self.log)
        data = cache.cache.serialize(ctx)
        cache.cache.deserialize(data, ctx)

    def test_branch_cache_all_then_protected(self):
        conn = DummyConnection()
        cache = BranchCache(self.zk_client, conn, self.component_registry)

        protected_flags = BranchFlag.PROTECTED
        all_flags = BranchFlag.PRESENT
        test_data = {
            'project1': {
                'all': [
                    BranchInfo('protected1', present=True),
                    BranchInfo('protected2', present=True),
                    BranchInfo('unprotected1', present=True),
                    BranchInfo('unprotected2', present=True),
                ],
                'protected': [
                    BranchInfo('protected1', protected=True),
                    BranchInfo('protected2', protected=True),
                ],
            },
        }

        self.assertRaises(
            LookupError,
            lambda: cache.getProjectBranches('project1', protected_flags)
        )
        self.assertRaises(
            LookupError,
            lambda: cache.getProjectBranches('project1', all_flags)
        )

        # Test the other order; all followed by protected-only
        cache.setProjectBranches('project1', all_flags,
                                 test_data['project1']['all'])
        self.assertRaises(
            LookupError,
            lambda: cache.getProjectBranches('project1', protected_flags)
        )
        self.assertEqual(
            sorted([bi.name for bi in
                    cache.getProjectBranches('project1', all_flags)]),
            [bi.name for bi in test_data['project1']['all']]
        )

        cache.setProjectBranches('project1', protected_flags,
                                 test_data['project1']['protected'])
        self.assertEqual(
            sorted([bi.name for bi in
                    cache.getProjectBranches('project1', protected_flags)
                    if bi.protected is True]),
            [bi.name for bi in test_data['project1']['protected']]
        )
        self.assertEqual(
            sorted([bi.name for bi in
                    cache.getProjectBranches('project1', all_flags)]),
            [bi.name for bi in test_data['project1']['all']]
        )

        # There's a lot of exception catching in the branch cache,
        # so exercise a serialize/deserialize cycle.
        ctx = ZKContext(self.zk_client, None, None, self.log)
        data = cache.cache.serialize(ctx)
        cache.cache.deserialize(data, ctx)

    def test_branch_cache_change_protected(self):
        conn = DummyConnection()
        cache = BranchCache(self.zk_client, conn, self.component_registry)
        protected_flags = BranchFlag.PROTECTED
        all_flags = BranchFlag.PRESENT

        data1 = {
            'project1': {
                'all': [
                    BranchInfo('newbranch', present=True),
                    BranchInfo('protected', present=True),
                ],
                'protected': [
                    BranchInfo('protected', protected=True),
                ],
            },
        }
        data2 = {
            'project1': {
                'all': [
                    BranchInfo('newbranch', present=True),
                    BranchInfo('protected', present=True),
                ],
                'protected': [
                    BranchInfo('newbranch', present=True, protected=True),
                    BranchInfo('protected', protected=True),
                ],
            },
        }

        # Create a new unprotected branch
        cache.setProjectBranches('project1', all_flags,
                                 data1['project1']['all'])
        cache.setProjectBranches('project1', protected_flags,
                                 data1['project1']['protected'])
        self.assertEqual(
            sorted([bi.name for bi in
                    cache.getProjectBranches('project1', protected_flags)
                    if bi.protected is True]),
            [bi.name for bi in data1['project1']['protected']]
        )
        self.assertEqual(
            sorted([bi.name for bi in
                    cache.getProjectBranches('project1', all_flags)]),
            [bi.name for bi in data1['project1']['all']]
        )

        # Change it to protected
        cache.setProtected('project1', 'newbranch', True)
        self.assertEqual(
            sorted([bi.name for bi in
                    cache.getProjectBranches('project1', protected_flags)
                    if bi.protected is True]),
            [bi.name for bi in data2['project1']['protected']]
        )
        self.assertEqual(
            sorted([bi.name for bi in
                    cache.getProjectBranches('project1', all_flags)]),
            [bi.name for bi in data2['project1']['all']]
        )

        # Change it back
        cache.setProtected('project1', 'newbranch', False)
        self.assertEqual(
            sorted([bi.name for bi in
                    cache.getProjectBranches('project1', protected_flags)
                    if bi.protected is True]),
            [bi.name for bi in data1['project1']['protected']]
        )
        self.assertEqual(
            sorted([bi.name for bi in
                    cache.getProjectBranches('project1', all_flags)]),
            [bi.name for bi in data1['project1']['all']]
        )

    def test_branch_cache_lookup_error(self):
        # Test that a missing branch cache entry results in a LookupError
        conn = DummyConnection()
        cache = BranchCache(self.zk_client, conn, self.component_registry)
        self.assertRaises(
            LookupError,
            lambda: cache.getProjectBranches('project1', True)
        )
        self.assertIsNone(
            cache.getProjectBranches('project1', True, default=None)
        )


class TestConfigurationErrorList(ZooKeeperBaseTestCase):
    def test_config_error_list(self):
        stop_event = threading.Event()
        self.zk_client.client.create('/zuul/pipeline', makepath=True)

        source_context = model.SourceContext(
            'cname', 'project', 'connection', 'branch', 'test')

        m1 = yaml.Mark('name', 0, 0, 0, '', 0)
        m2 = yaml.Mark('name', 1, 0, 0, '', 0)
        start_mark = model.ZuulMark(m1, m2, 'hello')

        # Create a new object
        with (tenant_write_lock(self.zk_client, 'test') as lock,
              ZKContext(
                  self.zk_client, lock, stop_event, self.log) as context):
            pipeline = DummyZKObject.new(context, name="test", foo="bar")
            e1 = model.ConfigurationError(
                source_context, start_mark, "Test error1")
            e2 = model.ConfigurationError(
                source_context, start_mark, "Test error2")
            with pipeline.activeContext(context):
                path = '/zuul/pipeline/test/errors'
                el1 = model.ConfigurationErrorList.new(
                    context, errors=[e1, e2], _path=path)

            el2 = model.ConfigurationErrorList.fromZK(
                context, path, _path=path)
            self.assertEqual(el1.errors, el2.errors)
            self.assertFalse(el1 is el2)
            self.assertEqual(el1.errors[0], el2.errors[0])
            self.assertEqual(el1.errors[0], e1)
            self.assertNotEqual(e1, e2)
            self.assertEqual([e1, e2], [e1, e2])


class TestBlobStore(ZooKeeperBaseTestCase):
    def _assertEmptyBlobStore(self, bs, path):
        with testtools.ExpectedException(KeyError):
            bs.get(path)

        # Make sure all keys have been cleaned up
        keys = self.zk_client.client.get_children(bs.lock_root)
        self.assertEqual([], keys)

    def test_blob_store(self):
        stop_event = threading.Event()
        self.zk_client.client.create('/zuul/pipeline', makepath=True)
        # Create a new object
        tenant_name = 'fake_tenant'

        start_ltime = self.zk_client.getCurrentLtime()
        with (tenant_write_lock(self.zk_client, tenant_name) as lock,
              ZKContext(
                  self.zk_client, lock, stop_event, self.log) as context):
            bs = BlobStore(context)
            with testtools.ExpectedException(KeyError):
                bs.get('nope')

            key = bs.put(b'something')

            self.assertEqual(bs.get(key), b'something')
            self.assertEqual([x for x in bs], [key])
            self.assertEqual(len(bs), 1)

            self.assertTrue(key in bs)
            self.assertFalse('nope' in bs)
            self.assertTrue(bs._checkKey(key))
            self.assertFalse(bs._checkKey('nope'))

            cur_ltime = self.zk_client.getCurrentLtime()
            self.assertEqual(bs.getKeysLastUsedBefore(cur_ltime), {key})
            self.assertEqual(bs.getKeysLastUsedBefore(start_ltime), set())
            # Test deletion
            bs.delete(key, cur_ltime)
            self._assertEmptyBlobStore(bs, key)

            # Put the blob back and test cleanup
            key = bs.put(b'something')
            live_blobs = set()
            cur_ltime = self.zk_client.getCurrentLtime()
            bs.cleanup(cur_ltime, live_blobs)
            self._assertEmptyBlobStore(bs, key)

            # Test leaked lock dir cleanup
            self.zk_client.client.create(bs._getLockPath(key))
            bs.lock_grace_period = 0
            cur_ltime = self.zk_client.getCurrentLtime()
            bs.cleanup(cur_ltime, live_blobs)
            self._assertEmptyBlobStore(bs, key)

    def test_blob_store_lock_cleanup_race(self):
        # The blob store lock cleanup can have a race condition as follows:
        # [1] Put a blob
        # [1] Delete the blob but fail to delete the lock path for the blob
        # [2] Start to put the blob it a second time
        # [2] Ensure the lock path exists
        # [1] Delete the leaked lock path
        # [2] Fail to create the lock because the lock path does not exist
        # To address this, we will retry the lock if it fails with
        # NoNodeError.  This test verifies that behavior.
        stop_event = threading.Event()
        self.zk_client.client.create('/zuul/pipeline', makepath=True)
        # Create a new object
        tenant_name = 'fake_tenant'

        created_event = threading.Event()
        deleted_event = threading.Event()
        orig_ensure_path = kazoo.recipe.lock.Lock._ensure_path

        def _create_blob(bs):
            bs.put(b'something')

        def _ensure_path(*args, **kw):
            orig_ensure_path(*args, **kw)
            created_event.set()
            deleted_event.wait()

        with (tenant_write_lock(self.zk_client, tenant_name) as lock,
              ZKContext(
                  self.zk_client, lock, stop_event, self.log) as context):
            bs = BlobStore(context)

            # Get the key
            key = bs.put(b'something')
            cur_ltime = self.zk_client.getCurrentLtime()
            bs.delete(key, cur_ltime)

            # Recreate the lock dir
            self.zk_client.client.create(bs._getLockPath(key))
            # Block the lock method so we can delete from under it
            self.useFixture(fixtures.MonkeyPatch(
                'kazoo.recipe.lock.Lock._ensure_path',
                _ensure_path))
            # Start recreating the blob
            thread = threading.Thread(target=_create_blob, args=(bs,))
            thread.start()
            created_event.wait()
            # Run the cleanup
            live_blobs = set()
            bs.lock_grace_period = 0
            cur_ltime = self.zk_client.getCurrentLtime()
            bs.cleanup(cur_ltime, live_blobs)
            self._assertEmptyBlobStore(bs, key)
            # Finish recreating the blob
            deleted_event.set()
            thread.join()
            # Ensure the blob exists
            self.assertEqual(bs.get(key), b'something')


class TestPipelineInit(ZooKeeperBaseTestCase):
    # Test the initialize-on-refresh code paths of various pipeline objects

    def test_pipeline_state_new_object(self):
        # Test the initialize-on-refresh code path with no existing object
        tenant = model.Tenant('tenant')
        pipeline = model.Pipeline('gate')
        layout = model.Layout(tenant)
        tenant.layout = layout
        manager = mock.Mock()
        manager.pipeline = pipeline
        manager.tenant = tenant
        manager.state = model.PipelineState.create(
            manager, None)
        context = ZKContext(self.zk_client, None, None, self.log)
        manager.state.refresh(context)
        self.assertTrue(self.zk_client.client.exists(manager.state.getPath()))
        self.assertEqual(manager.state.layout_uuid, layout.uuid)

    def test_pipeline_state_existing_object(self):
        # Test the initialize-on-refresh code path with a pre-existing object
        tenant = model.Tenant('tenant')
        pipeline = model.Pipeline('gate')
        layout = model.Layout(tenant)
        tenant.layout = layout
        manager = mock.Mock()
        manager.pipeline = pipeline
        manager.tenant = tenant
        manager.state = model.PipelineState.create(
            manager, None)
        manager.change_list = model.PipelineChangeList.create(
            manager)
        context = ZKContext(self.zk_client, None, None, self.log)
        # We refresh the change list here purely for the side effect
        # of creating the pipeline state object with no data (the list
        # is a subpath of the state object).
        manager.change_list.refresh(context)
        manager.state.refresh(context)
        self.assertTrue(
            self.zk_client.client.exists(manager.change_list.getPath()))
        self.assertTrue(self.zk_client.client.exists(manager.state.getPath()))
        self.assertEqual(manager.state.layout_uuid, layout.uuid)

    def test_pipeline_change_list_new_object(self):
        # Test the initialize-on-refresh code path with no existing object
        tenant = model.Tenant('tenant')
        pipeline = model.Pipeline('gate')
        layout = model.Layout(tenant)
        tenant.layout = layout
        manager = mock.Mock()
        manager.pipeline = pipeline
        manager.tenant = tenant
        manager.state = model.PipelineState.create(
            manager, None)
        manager.change_list = model.PipelineChangeList.create(
            manager)
        context = ZKContext(self.zk_client, None, None, self.log)
        manager.change_list.refresh(context)
        self.assertTrue(
            self.zk_client.client.exists(manager.change_list.getPath()))
        manager.state.refresh(context)
        self.assertEqual(manager.state.layout_uuid, layout.uuid)

    def test_pipeline_change_list_new_object_without_lock(self):
        # Test the initialize-on-refresh code path if we don't have
        # the lock.  This should fail.
        tenant = model.Tenant('tenant')
        pipeline = model.Pipeline('gate')
        layout = model.Layout(tenant)
        tenant.layout = layout
        manager = mock.Mock()
        manager.pipeline = pipeline
        manager.tenant = tenant
        manager.state = model.PipelineState.create(
            manager, None)
        manager.change_list = model.PipelineChangeList.create(
            manager)
        context = ZKContext(self.zk_client, None, None, self.log)
        with testtools.ExpectedException(NoNodeError):
            manager.change_list.refresh(context, allow_init=False)
        self.assertIsNone(
            self.zk_client.client.exists(manager.change_list.getPath()))
        manager.state.refresh(context)
        self.assertEqual(manager.state.layout_uuid, layout.uuid)


class TestPolymorphicZKObjectMixin(ZooKeeperBaseTestCase):

    def test_create(self):
        class Parent(PolymorphicZKObjectMixin, ZKObject):
            def __init__(self):
                super().__init__()
                self._set(
                    _path=None,
                    uid=uuid.uuid4().hex,
                    state="test"
                )

            def serialize(self, context):
                return json.dumps({
                    "uid": self.uid,
                    "state": self.state,
                }).encode("utf8")

            def getPath(self):
                return f"/test/{self.uid}"

        class ChildA(Parent, subclass_id="child-A"):
            pass

        class ChildB(Parent, subclass_id="chid-B"):
            pass

        context = ZKContext(self.zk_client, None, None, self.log)
        child_a = ChildA.new(context)

        child_a_from_zk = Parent.fromZK(context, child_a.getPath())
        self.assertIsInstance(child_a_from_zk, ChildA)

        child_b = ChildB.new(context)
        child_b_from_zk = Parent.fromZK(context, child_b.getPath())
        self.assertIsInstance(child_b_from_zk, ChildB)
        child_b_from_zk.updateAttributes(context, state="update-1")

        child_b.refresh(context)
        self.assertEqual(child_b.state, "update-1")

    def test_missing_base(self):
        with testtools.ExpectedException(TypeError):
            class Child(PolymorphicZKObjectMixin, subclass_id="child"):
                pass

    def test_wrong_nesting(self):
        class Parent(PolymorphicZKObjectMixin):
            pass

        with testtools.ExpectedException(TypeError):
            class NewParent(Parent):
                pass

    def test_duplicate_subclass_id(self):
        class Parent(PolymorphicZKObjectMixin):
            pass

        class ChildA(Parent, subclass_id="child-a"):
            pass

        with testtools.ExpectedException(ValueError):
            class ChildAWrong(Parent, subclass_id="child-a"):
                pass

    def test_independent_hierarchies(self):
        class ParentA(PolymorphicZKObjectMixin):
            pass

        class ChildA(ParentA, subclass_id="child-A"):
            pass

        class ParentB(PolymorphicZKObjectMixin):
            pass

        class ChildB(ParentB, subclass_id="child-B"):
            pass

        self.assertIn(ChildA._subclass_id, ParentA._subclasses)
        self.assertIs(ParentA._subclasses[ChildA._subclass_id], ChildA)
        self.assertNotIn(ChildB._subclass_id, ParentA._subclasses)

        self.assertIn(ChildB._subclass_id, ParentB._subclasses)
        self.assertIs(ParentB._subclasses[ChildB._subclass_id], ChildB)
        self.assertNotIn(ChildA._subclass_id, ParentB._subclasses)


class TestPolymorphicZKObjectMixinSharded(ZooKeeperBaseTestCase):

    def test_create(self):
        class Parent(PolymorphicZKObjectMixin, ShardedZKObject):
            def __init__(self):
                super().__init__()
                self._set(
                    _path=None,
                    uid=uuid.uuid4().hex,
                    state="test"
                )

            def serialize(self, context):
                return json.dumps({
                    "uid": self.uid,
                    "state": self.state,
                }).encode("utf8")

            def getPath(self):
                return f"/test/{self.uid}"

        class ChildA(Parent, subclass_id="child-A"):
            pass

        class ChildB(Parent, subclass_id="chid-B"):
            pass

        context = ZKContext(self.zk_client, None, None, self.log)
        child_a = ChildA.new(context)

        child_a_from_zk = Parent.fromZK(context, child_a.getPath())
        self.assertIsInstance(child_a_from_zk, ChildA)

        child_b = ChildB.new(context)
        child_b_from_zk = Parent.fromZK(context, child_b.getPath())
        self.assertIsInstance(child_b_from_zk, ChildB)
        child_b_from_zk.updateAttributes(context, state="update-1")

        child_b.refresh(context)
        self.assertEqual(child_b.state, "update-1")

    def test_missing_base(self):
        with testtools.ExpectedException(TypeError):
            class Child(PolymorphicZKObjectMixin, subclass_id="child"):
                pass

    def test_wrong_nesting(self):
        class Parent(PolymorphicZKObjectMixin):
            pass

        with testtools.ExpectedException(TypeError):
            class NewParent(Parent):
                pass

    def test_duplicate_subclass_id(self):
        class Parent(PolymorphicZKObjectMixin):
            pass

        class ChildA(Parent, subclass_id="child-a"):
            pass

        with testtools.ExpectedException(ValueError):
            class ChildAWrong(Parent, subclass_id="child-a"):
                pass

    def test_independent_hierarchies(self):
        class ParentA(PolymorphicZKObjectMixin):
            pass

        class ChildA(ParentA, subclass_id="child-A"):
            pass

        class ParentB(PolymorphicZKObjectMixin):
            pass

        class ChildB(ParentB, subclass_id="child-B"):
            pass

        self.assertIn(ChildA._subclass_id, ParentA._subclasses)
        self.assertIs(ParentA._subclasses[ChildA._subclass_id], ChildA)
        self.assertNotIn(ChildB._subclass_id, ParentA._subclasses)

        self.assertIn(ChildB._subclass_id, ParentB._subclasses)
        self.assertIs(ParentB._subclasses[ChildB._subclass_id], ChildB)
        self.assertNotIn(ChildA._subclass_id, ParentB._subclasses)


class DummyAProviderNode(model.ProviderNode, subclass_id="dummy-A-node"):
    pass


class DummyBProviderNode(model.ProviderNode, subclass_id="dummy-B-node"):
    pass


class TestLauncherApi(ZooKeeperBaseTestCase):

    def setUp(self):
        super().setUp()
        self.component_info = LauncherComponent(self.zk_client, "test")
        self.component_info.state = self.component_info.RUNNING
        self.component_info.register()
        self.api = LauncherApi(
            self.zk_client, self.component_registry, self.component_info,
            lambda: None)
        self.addCleanup(self.api.stop)

    def test_launcher(self):
        labels = ["foo", "bar"]
        context = ZKContext(self.zk_client, None, None, self.log)
        request = model.NodesetRequest.new(
            context,
            tenant_name="tenant",
            pipeline_name="check",
            buildset_uuid=uuid.uuid4().hex,
            job_uuid=uuid.uuid4().hex,
            job_name="foobar",
            labels=labels,
            priority=100,
            request_time=time.time(),
            zuul_event_id=None,
            span_info=None,
        )

        # Wait for request to show up in the cache
        for _ in iterate_timeout(10, "request to show up"):
            request_list = self.api.getNodesetRequests()
            if len(request_list):
                break

        self.assertEqual(len(request_list), 1)
        for req in request_list:
            request = self.api.getNodesetRequest(req.uuid)
            self.assertIs(request, req)
            self.assertIsNotNone(request)
            self.assertEqual(labels, request.labels)
            self.assertIsNotNone(request._zstat)
            self.assertIsNotNone(request.getPath())

        self.assertIsNotNone(request.acquireLock(context))
        self.assertTrue(request.hasLock())
        for _ in iterate_timeout(10, "request to be locked"):
            if request.is_locked:
                break

        # Create provider nodes for the requested labels
        for i, label in enumerate(request.labels):
            # Alternate between the two dummy provider nodes classes
            node_class = DummyAProviderNode if i % 2 else DummyBProviderNode
            node = node_class.new(
                context, uuid=uuid.uuid4().hex,
                request_time=request.request_time, label=label)
            node.assign(context, request_id=request.uuid, tenant_name='test')

        # Wait for the nodes to show up in the cache
        for _ in iterate_timeout(10, "nodes to show up"):
            provider_nodes = self.api.getProviderNodes()
            if len(provider_nodes) == 2:
                break

        # Accept and update the nodeset request
        with request.activeContext(context):
            for n in provider_nodes:
                request.addProviderNode(n)
            request.state = model.NodesetRequest.State.ACCEPTED

        # "Fulfill" requested provider nodes
        for node in self.api.getMatchingProviderNodes():
            self.assertIsNotNone(node.acquireLock(context))
            self.assertTrue(node.hasLock())
            for _ in iterate_timeout(10, "node to be locked"):
                if node.is_locked:
                    break
            node.updateAttributes(
                context,
                state=model.ProviderNode.State.READY
            )
            node.releaseLock(context)
            self.assertFalse(node.hasLock())
            for _ in iterate_timeout(10, "node to be unlocked"):
                if not node.is_locked:
                    break

        # Wait for nodes to show up be ready and unlocked
        for _ in iterate_timeout(10, "nodes to be ready"):
            requested_nodes = [self.api.getProviderNode(ni)
                               for ni in request.nodes]
            if len(requested_nodes) != 2:
                continue
            if all(n.state == model.ProviderNode.State.READY
                   and not n.is_locked for n in requested_nodes):
                break

        # Mark the request as fulfilled + unlock
        request.updateAttributes(
            context,
            state=model.NodesetRequest.State.FULFILLED,
        )
        request.releaseLock(context)
        self.assertFalse(request.hasLock())
        for _ in iterate_timeout(10, "request to be unlocked"):
            if not request.is_locked:
                break

        # Should be a no-op
        self.api.cleanupNodes()

        # Remove request and wait for it to be removed from the cache
        request.delete(context)
        for _ in iterate_timeout(10, "request to be removed"):
            request_list = self.api.getNodesetRequests()
            if not len(request_list):
                break

        not_request = self.api.getNodesetRequest(request.uuid)
        self.assertIsNone(not_request)

        # Make sure we still have the provider nodes
        provider_nodes = self.api.getProviderNodes()
        self.assertEqual(len(provider_nodes), 2)

        # Mark nodes as used
        for node in provider_nodes:
            self.assertIsNotNone(node.acquireLock(context))
            for _ in iterate_timeout(10, "wait for lock to show up"):
                if node.is_locked:
                    break
            node.updateAttributes(
                context,
                state=model.ProviderNode.State.USED,
            )
            node.releaseLock(context)

        # Cleanup used nodes and wait for them to be removed from the cache
        for _ in iterate_timeout(10, "nodes to be removed"):
            self.api.cleanupNodes()
            provider_nodes = self.api.getProviderNodes()
            if not provider_nodes:
                break

    def test_nodeset_request_revision(self):
        labels = ["foo", "bar"]
        context = ZKContext(self.zk_client, None, None, self.log)
        request = model.NodesetRequest.new(
            context,
            tenant_name="tenant",
            pipeline_name="check",
            buildset_uuid=uuid.uuid4().hex,
            job_uuid=uuid.uuid4().hex,
            job_name="foobar",
            labels=labels,
            priority=100,
            request_time=time.time(),
            zuul_event_id=None,
            span_info=None,
            _relative_priority=100,
        )
        self.assertEqual(100, request.relative_priority)

        # Wait for request to show up in the cache
        for _ in iterate_timeout(10, "request to show up"):
            request_list = self.api.getNodesetRequests()
            if len(request_list):
                break

        self.assertEqual(len(request_list), 1)
        for req in request_list:
            cached_request = self.api.getNodesetRequest(req.uuid)
            self.assertEqual(100, request.relative_priority)

        model.NodesetRequestRevision.new(
            context, request=request, relative_priority=50)
        for _ in iterate_timeout(10, "revision to be applied"):
            if cached_request.relative_priority == 50:
                break

        # Relative priority should still be the initial value
        self.assertEqual(100, request.relative_priority)

        # Refresh request inc. revision
        request.refresh(context)
        self.assertEqual(50, request.relative_priority)

        # Update revision
        request.revise(context, relative_priority=10)
        self.assertEqual(10, request.relative_priority)

    def test_node_quota_cache(self):
        context = ZKContext(self.zk_client, None, None, self.log)

        class Dummy:
            pass

        provider = Dummy()
        provider.canonical_name = 'provider'

        quota = QuotaInformation(instances=1)
        node_class = DummyAProviderNode

        node1 = node_class.new(
            context, uuid='1', quota=quota, label='foo',
            provider='provider', state=node_class.State.REQUESTED)
        for _ in iterate_timeout(10, "cache to sync"):
            used = self.api.nodes_cache.getQuota(provider)
            requested = self.api.nodes_cache.getQuota(
                provider, include_requested=True)
            if (used.quota.get('instances', 0) == 0 and
                requested.quota.get('instances', 0) == 1):
                break

        node1.updateAttributes(context, state=node_class.State.BUILDING)
        for _ in iterate_timeout(10, "cache to sync"):
            used = self.api.nodes_cache.getQuota(provider)
            requested = self.api.nodes_cache.getQuota(
                provider, include_requested=True)
            if (used.quota.get('instances', 0) == 1 and
                requested.quota.get('instances', 0) == 1):
                break

        node2 = node_class.new(
            context, uuid='2', quota=quota, label='foo',
            provider='provider', state=node_class.State.BUILDING)
        for _ in iterate_timeout(10, "cache to sync"):
            used = self.api.nodes_cache.getQuota(provider)
            if used.quota.get('instances') == 2:
                break

        node1.delete(context)
        for _ in iterate_timeout(10, "cache to sync"):
            used = self.api.nodes_cache.getQuota(provider)
            if used.quota.get('instances') == 1:
                break

        node2.delete(context)
        for _ in iterate_timeout(10, "cache to sync"):
            used = self.api.nodes_cache.getQuota(provider)
            if used.quota.get('instances') == 0:
                break

    def test_node_order(self):
        context = ZKContext(self.zk_client, None, None, self.log)

        node_class = DummyAProviderNode
        quota = QuotaInformation(instances=1)
        node_class.new(
            context, uuid='1', quota=quota, label='foo',
            provider='provider', state=node_class.State.REQUESTED,
            request_time=20)
        node_class.new(
            context, uuid='2', quota=quota, label='foo',
            provider='provider', state=node_class.State.REQUESTED,
            request_time=10)
        for _ in iterate_timeout(10, "cache to sync"):
            items = self.api.nodes_cache.getItems()
            if len(items) == 2:
                break
        # All natural sorting favors 1,2; we expect 2,1
        items = sorted(items)
        self.assertEqual('2', items[0].uuid)
        self.assertEqual('1', items[1].uuid)

    def test_node_snapshot(self):
        context = ZKContext(self.zk_client, None, None, self.log)
        node = DummyAProviderNode.new(
            context, uuid=uuid.uuid4().hex,
            label='foo')
        node.assign(context, request_id=uuid.uuid4().hex, tenant_name='test')
        with node.locked(context):
            # The executor copy
            node.createSnapshot(context)

            # Wait for the nodes to show up in the cache
            for _ in iterate_timeout(10, "nodes to show up"):
                provider_nodes = self.api.getProviderNodes()
                if len(provider_nodes) == 1:
                    break
            snapshot2 = provider_nodes[0].snapshot
            with snapshot2.locked(context):
                snapshot2.updateAttributes(
                    context,
                    complete=True,
                    external_id="image_id",
                )

            node.snapshot.refresh(context)
            self.assertTrue(node.snapshot.complete)
            self.assertEqual("image_id", node.snapshot.external_id)
            node.delete(context)

        self.assertFalse(self.zk_client.client.exists(node.getPath()))
        self.assertFalse(self.zk_client.client.exists(node.getLockPath()))

    def test_node_assignment(self):
        context = ZKContext(self.zk_client, None, None, self.log)
        node = DummyAProviderNode.new(
            context,
            uuid=uuid.uuid4().hex,
            label='foo')
        node.assign(context, request_id='1234', tenant_name='tenant-one')

        # Wait for the nodes to show up in the cache
        for _ in iterate_timeout(10, "node to show up"):
            provider_nodes = self.api.getProviderNodes()
            if len(provider_nodes) == 1:
                if (provider_nodes[0].request_id == '1234' and
                    provider_nodes[0].tenant_name == 'tenant-one'):
                    break

        node.unassign(context)
        for _ in iterate_timeout(10, "node to update"):
            provider_nodes = self.api.getProviderNodes()
            if len(provider_nodes) == 1:
                if (provider_nodes[0].request_id is None and
                    provider_nodes[0].tenant_name is None):
                    break

    def test_node_assignment_upgrade(self):
        context = ZKContext(self.zk_client, None, None, self.log)

        class DummyUpgradeProviderNode(
                model.ProviderNode, subclass_id="dummy-upgrade-node"):
            def serialize(self, context):
                data = super().serialize(context)
                data = json.loads(data)
                data['min_request_version'] = None
                data['request_id'] = '1234'
                data['tenant_name'] = 'tenant-one'
                return json.dumps(data, sort_keys=True).encode("utf-8")

        node = DummyUpgradeProviderNode.new(
            context,
            uuid=uuid.uuid4().hex,
            label='foo')

        # Wait for the nodes to show up in the cache
        for _ in iterate_timeout(10, "node to show up"):
            provider_nodes = self.api.getProviderNodes()
            if len(provider_nodes) == 1:
                if (provider_nodes[0].request_id == '1234' and
                    provider_nodes[0].tenant_name == 'tenant-one'):
                    break

        node.unassign(context)
        for _ in iterate_timeout(10, "node to update"):
            provider_nodes = self.api.getProviderNodes()
            if len(provider_nodes) == 1:
                if (provider_nodes[0].request_id is None and
                    provider_nodes[0].tenant_name is None):
                    break

        node.delete(context)


class DummyLockable(LockableZKObject):
    ROOT = "/test/dummy"
    DUMMY_PATH = "dummy"
    LOCKS_PATH = "locks"

    def __init__(self):
        super().__init__()
        self._set(
            uuid=uuid.uuid4().hex,
            is_locked=False,
            lock_holder=None,
        )

    def serialize(self, context):
        return json.dumps({
            "uuid": self.uuid,
        }).encode("utf8")

    def getPath(self):
        return f"{self.ROOT}/{self.DUMMY_PATH}/{self.uuid}"

    def getLockPath(self):
        return f"{self.ROOT}/{self.LOCKS_PATH}/{self.uuid}"


class TestLockableZKObjectCache(ZooKeeperBaseTestCase):

    def test_is_locked_contenders(self):
        cache = LockableZKObjectCache(
            self.zk_client,
            None,
            root=DummyLockable.ROOT,
            items_path=DummyLockable.DUMMY_PATH,
            locks_path=DummyLockable.LOCKS_PATH,
            zkobject_class=DummyLockable)

        ctx = ZKContext(self.zk_client, None, None, self.log)
        dummy = DummyLockable.new(ctx)

        for _ in iterate_timeout(10, "cache to sync"):
            if cache.getItems():
                break

        # Acquire lock for all items
        for item in cache.getItems():
            item.acquireLock(ctx)

        for _ in iterate_timeout(10, "cache to sync"):
            if all(d.is_locked for d in cache.getItems()):
                break

        # Create a dummy lock contender
        contender_path = f"{dummy.getLockPath()}/deadbeef__lock__0000000001"
        self.zk_client.client.create(contender_path)

        # Make sure items are still considered locked
        for _ in iterate_timeout(10, "cache to sync"):
            if all(d.is_locked for d in cache.getItems()):
                break

        # Discard the pending lock and make sure items are still
        # considered locked
        self.zk_client.client.delete(contender_path)
        for _ in iterate_timeout(10, "cache to sync"):
            if all(d.is_locked for d in cache.getItems()):
                break

        # Release the lock and confirm items are no longer considered locked
        for item in cache.getItems():
            item.releaseLock(ctx)

        for _ in iterate_timeout(10, "cache to sync"):
            if not any(d.is_locked for d in cache.getItems()):
                break


class TestRendezvousElection(ZooKeeperBaseTestCase):
    def test_rendezvous_election(self):
        # The strings foo and bar are not entirely arbitrary; they
        # rendezvous hash with the path to produce a list of
        # components in the order expected by the test.
        c1 = SchedulerComponent(self.zk_client, "foo")
        c1.register()
        c2 = SchedulerComponent(self.zk_client, "bar")
        c2.register()

        e1 = RendezvousElection(
            self.zk_client.client,
            '/test/election/lock',
            'scheduler',
            c1,
        )
        e2 = RendezvousElection(
            self.zk_client.client,
            '/test/election/lock',
            'scheduler',
            c2,
        )

        self.assertEqual(0, len(e1._getScores()))
        self.assertEqual(None, e1._getWinner())
        self.assertFalse(e1.is_still_valid())

        self.assertEqual(0, len(e2._getScores()))
        self.assertEqual(None, e2._getWinner())
        self.assertFalse(e2.is_still_valid())

        # c2 will be the winner since it's the only one running
        c2.state = c2.RUNNING
        time.sleep(1)
        self.assertEqual(1, len(e1._getScores()))
        self.assertEqual(c2.hostname, e1._getWinner().hostname)
        self.assertFalse(e1.is_still_valid())

        self.assertEqual(1, len(e2._getScores()))
        self.assertEqual(c2.hostname, e2._getWinner().hostname)
        self.assertTrue(e2.is_still_valid())

        # Once both are enabled, c1 is the winner
        c1.state = c1.RUNNING
        time.sleep(1)
        self.assertEqual(2, len(e1._getScores()))
        self.assertEqual(c1.hostname, e1._getWinner().hostname)
        self.assertTrue(e1.is_still_valid())

        self.assertEqual(2, len(e2._getScores()))
        self.assertEqual(c1.hostname, e2._getWinner().hostname)
        self.assertFalse(e2.is_still_valid())

        event1 = threading.Event()
        event2 = threading.Event()

        def run1():
            event1.set()
            while e1.is_still_valid():
                time.sleep(0.1)

        def run2():
            event2.set()
            while e2.is_still_valid():
                time.sleep(0.1)

        t1 = threading.Thread(
            target=e1.run,
            args=(run1,),
        )
        t1.start()
        t2 = threading.Thread(
            target=e2.run,
            args=(run2,),
        )
        t2.start()

        # Wait for the thread to start
        event1.wait()
        # Second component should still be waiting
        self.assertFalse(event2.is_set())

        # Stop the thread
        e1.cancel()
        # Wait for the thread to stop
        t1.join()

        # The second election still should not have started yet since
        # the first component is still running.
        self.assertFalse(event2.is_set())

        self.log.debug("Stop c1")
        # Let the second election win
        c1.state = c1.STOPPED
        event2.wait()

        self.log.debug("Stop c2")
        # Stop the second election by stopping the second component
        e2.running = False
        c2.state = c2.STOPPED
        t2.join()

    def test_rendezvous_election_upgrade(self):
        # Test that we can upgrade from a "regular" election to a
        # rendezvous election.

        # The numbers (e1, e2) mirror the test above, but "1" is the
        # regular election and "2" is the rendezvous.
        c2 = SchedulerComponent(self.zk_client, "bar")
        c2.register()

        e1 = SessionAwareElection(
            self.zk_client.client,
            '/test/election/lock',
        )
        e2 = RendezvousElection(
            self.zk_client.client,
            '/test/election/lock',
            'scheduler',
            c2,
        )

        self.assertEqual(0, len(e2._getScores()))
        self.assertEqual(None, e2._getWinner())
        self.assertFalse(e2.is_still_valid())

        # c2 will be the winner since it's the only one running
        c2.state = c2.RUNNING
        time.sleep(1)
        self.assertEqual(1, len(e2._getScores()))
        self.assertEqual(c2.hostname, e2._getWinner().hostname)
        self.assertTrue(e2.is_still_valid())

        event1 = threading.Event()
        event2 = threading.Event()
        e1._test_stop = False

        def run1():
            event1.set()
            while not e1._test_stop:
                time.sleep(0.1)

        def run2():
            event2.set()
            while e2.is_still_valid():
                time.sleep(0.1)

        t1 = threading.Thread(
            target=e1.run,
            args=(run1,),
        )
        t1.start()
        # Wait for the thread to start
        event1.wait()

        t2 = threading.Thread(
            target=e2.run,
            args=(run2,),
        )
        t2.start()

        time.sleep(1)
        # Second component should still be waiting
        self.assertFalse(event2.is_set())

        # Stop the thread
        self.log.debug("Stop c1")
        e1._test_stop = True
        # Wait for the thread to stop
        t1.join()

        # Wait for the second election to win
        event2.wait()

        self.log.debug("Stop c2")
        # Stop the second election by stopping the second component
        e2.running = False
        c2.state = c2.STOPPED
        t2.join()


class TestQuotaCache(ZooKeeperBaseTestCase):
    def test_quota_cache(self):
        from zuul.zk.quotacache import QuotaCache
        c1 = QuotaCache(self.zk_client, 'aws-us-west-2')
        c2 = QuotaCache(self.zk_client, 'aws-us-west-2')

        limits = model.QuotaInformation(default=math.inf, foo=42)
        c1.setLimits(limits)
        for _ in iterate_timeout(10, "cache to sync"):
            if c2._cached_objects:
                break
        self.assertEqual(limits, c2.getLimits())

        res = model.QuotaInformation(bar=82)
        c1.setResource('instance-small', res)
        for _ in iterate_timeout(10, "cache to sync"):
            if len(c2._cached_objects) == 2:
                break
        self.assertEqual(res, c2.getResource('instance-small'))

        res = model.QuotaInformation(instances=20)
        c1.setUnmanagedUsage(res)
        for _ in iterate_timeout(10, "cache to sync"):
            if len(c2._cached_objects) == 3:
                break
        self.assertEqual(res, c2.getUnmanagedUsage())

        limits2 = model.QuotaInformation(default=math.inf, foo=22)
        c1.setLimits(limits2)
        for _ in iterate_timeout(10, "cache to sync"):
            if limits2 == c2.getLimits():
                break
