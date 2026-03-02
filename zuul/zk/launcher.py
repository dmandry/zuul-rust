# Copyright 2024 BMW Group
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

import collections
import json
import logging
import threading

import mmh3
from kazoo.exceptions import NoNodeError

from zuul.model import (
    NodesetRequest,
    ProviderNode,
    ProviderNodeAssignment,
    ProviderNodeSnapshot,
    QuotaInformation,
)
from zuul.zk.cache import ZuulTreeCache
from zuul.zk.zkobject import ZKContext


def _dictToBytes(data):
    return json.dumps(data).encode("utf-8")


def _bytesToDict(raw_data):
    return json.loads(raw_data.decode("utf-8"))


def launcher_score(name, item):
    return mmh3.hash(f"{name}-{item.uuid}", signed=False)


class LockableZKObjectCache(ZuulTreeCache):

    def __init__(self, zk_client, updated_event, root, items_path,
                 locks_path, zkobject_class):
        self.updated_event = updated_event
        self.items_path = items_path
        self.locks_path = locks_path
        self.zkobject_class = zkobject_class
        super().__init__(zk_client, root)

    def _parsePath(self, path):
        if not path.startswith(self.root):
            return None
        path = path[len(self.root) + 1:]
        parts = path.split('/')
        # We are interested in requests with a parts that look like:
        # ([<self.items_path>, <self.locks_path>], <uuid>, ...)
        if len(parts) < 2:
            return None
        return parts

    def parsePath(self, path):
        key = None
        fetch = False
        parts = self._parsePath(path)
        if parts is None:
            return (key, fetch)
        if len(parts) < 2:
            return (key, fetch)

        object_type, item_uuid, *_ = parts
        if object_type == self.items_path:
            key = (item_uuid,)
            fetch = True
        elif object_type == self.locks_path:
            key = None
            if len(parts) == 3:
                fetch = True

        return (key, fetch)

    def preCacheHook(self, event, exists, data=None, stat=None):
        parts = self._parsePath(event.path)
        if parts is None:
            return

        # Expecting (<self.locks_path>, <uuid>, <lock>,)
        if len(parts) != 3:
            return

        object_type, request_uuid, contender, *_ = parts
        if object_type != self.locks_path:
            return

        key = (request_uuid,)
        request = self._cached_objects.get(key)

        if not request:
            return

        if exists:
            request._lock_contenders[contender] = data
        else:
            request._lock_contenders.pop(contender, None)

        is_locked = bool(request._lock_contenders)
        lock_holder = None
        if is_locked:
            # This is a very simplified contender lock algorithm that
            # only works for exclusive locks.
            sorted_names = sorted(request._lock_contenders.keys(),
                                  key=lambda x: x[-10:])
            if sorted_names:
                lock_holder = request._lock_contenders[sorted_names[0]]
                if lock_holder:
                    lock_holder = lock_holder.decode('utf8')
                else:
                    lock_holder = None
        update_args = {}
        if request.is_locked != is_locked:
            update_args['is_locked'] = is_locked
        if request.lock_holder != lock_holder:
            update_args['lock_holder'] = lock_holder
        if update_args:
            request._set(**update_args)
            if self.updated_event:
                self.updated_event()

    def postCacheHook(self, event, data, stat, key, obj):
        if self.updated_event:
            self.updated_event()

    def objectFromRaw(self, key, data, zstat):
        return self.zkobject_class._fromRaw(
            self._zk_context, data, zstat, None)

    def updateFromRaw(self, obj, key, data, zstat):
        obj._updateFromRaw(self._zk_context, data, zstat, None)

    def getItem(self, item_id):
        self.ensureReady()
        return self._cached_objects.get((item_id,))

    def getItems(self):
        # get a copy of the values view to avoid runtime errors in the event
        # the _cached_nodes dict gets updated while iterating
        self.ensureReady()
        return list(self._cached_objects.values())

    def getKeys(self):
        self.ensureReady()
        return set(self._cached_objects.keys())


class RequestCache(LockableZKObjectCache):

    def preCacheHook(self, event, exists, data=None, stat=None):
        parts = self._parsePath(event.path)
        if parts is None:
            return

        # Expecting (<self.locks_path>, <uuid>, <lock>,)
        # or (<self.items_path>, <uuid>, revision,)
        if len(parts) != 3:
            return

        object_type, request_uuid, *_ = parts
        key = (request_uuid,)
        request = self._cached_objects.get(key)

        if not request:
            return

        if object_type == self.locks_path:
            request._set(is_locked=exists)
        elif data is not None:
            request._revision._updateFromRaw(
                self._zk_context, data, stat, None)
            return self.STOP_OBJECT_UPDATE


class NodeCache(LockableZKObjectCache):
    def __init__(self, *args, **kw):
        # The states we're interested in
        self._quota_states_used = ProviderNode.ALLOCATED_STATES
        self._quota_states_requested = (
            ProviderNode.ALLOCATED_STATES + (ProviderNode.State.REQUESTED,)
        )
        # Key -> quota, for each cached object
        self._cached_quota_used = {}
        self._cached_quota_requested = {}
        # Provider canonical name -> quota, for each provider
        self._provider_quota_used = collections.defaultdict(
            lambda: QuotaInformation())
        self._provider_quota_requested = collections.defaultdict(
            lambda: QuotaInformation())
        super().__init__(*args, **kw)

    def _handleQuota(self, quota_states, quota_cache, provider_cache,
                     event, data, stat, key, obj):
        # Have we previously added quota for this object?
        old_quota = quota_cache.get(key)
        if key in self._cached_objects:
            new_quota = obj.quota
        else:
            new_quota = None
        # Now that we've established whether we should count the quota
        # based on object presence, take node state into account.
        if obj is None or obj.state not in quota_states:
            new_quota = None
        if new_quota != old_quota:
            # Add the new value first so if another thread races these
            # two operations, it sees us go over quota and not under.
            if new_quota is not None:
                provider_cache[obj.provider].add(new_quota)
            if old_quota is not None:
                provider_cache[obj.provider].subtract(old_quota)
            if new_quota is None:
                quota_cache.pop(key, None)
            else:
                quota_cache[key] = new_quota

    def preCacheHook(self, event, exists, data=None, stat=None):
        parts = self._parsePath(event.path)
        if parts is None:
            return

        # (<self.locks_path>, <uuid>, <lock>,)
        # (<self.items_path>, <uuid>,)
        # (<self.items_path>, <uuid>, snapshot,)
        # (<self.items_path>, <uuid>, snapshot-lock,)
        # (<self.items_path>, <uuid>, assignment,)
        if len(parts) >= 3:
            # Ignore anything related to snapshots
            if (parts[0] == ProviderNode.NODES_PATH and
                parts[2] in (ProviderNodeSnapshot.SNAPSHOT_PATH,
                             ProviderNodeSnapshot.SNAPSHOT_LOCK_PATH)):
                return self.STOP_OBJECT_UPDATE
            if (parts[0] == ProviderNode.NODES_PATH and
                parts[2] == ProviderNodeAssignment.ASSIGNMENT_PATH):
                key = (parts[1],)
                node = self._cached_objects.get(key)
                if not node:
                    return
                if exists:
                    node.assignment._updateFromRaw(
                        self._zk_context, data, stat, None)
                else:
                    node.assignment._clear()
                return self.STOP_OBJECT_UPDATE
        return super().preCacheHook(event, exists, data, stat)

    def postCacheHook(self, event, data, stat, key, obj):
        self._handleQuota(
            self._quota_states_used,
            self._cached_quota_used,
            self._provider_quota_used,
            event, data, stat, key, obj)
        self._handleQuota(
            self._quota_states_requested,
            self._cached_quota_requested,
            self._provider_quota_requested,
            event, data, stat, key, obj)
        super().postCacheHook(event, data, stat, key, obj)

    def getQuota(self, provider, include_requested=False):
        if include_requested:
            return self._provider_quota_requested[provider.canonical_name]
        else:
            return self._provider_quota_used[provider.canonical_name]


class LauncherApi:
    log = logging.getLogger("zuul.LauncherApi")

    def __init__(self, zk_client, component_registry, component_info,
                 event_callback, connection_filter=None):
        self.zk_client = zk_client
        self.component_registry = component_registry
        self.component_info = component_info
        self.event_callback = event_callback
        self.requests_cache = RequestCache(
            self.zk_client,
            self.event_callback,
            root=NodesetRequest.ROOT,
            items_path=NodesetRequest.REQUESTS_PATH,
            locks_path=NodesetRequest.LOCKS_PATH,
            zkobject_class=NodesetRequest)
        self.nodes_cache = NodeCache(
            self.zk_client,
            self.event_callback,
            root=ProviderNode.ROOT,
            items_path=ProviderNode.NODES_PATH,
            locks_path=ProviderNode.LOCKS_PATH,
            zkobject_class=ProviderNode)
        self.connection_filter = connection_filter
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()
        self.requests_cache.stop()
        self.nodes_cache.stop()

    def getMatchingRequests(self):
        candidate_launchers = {
            c.hostname: c for c in self.component_registry.all("launcher")}
        candidate_names = set(candidate_launchers.keys())

        for request in sorted(self.requests_cache.getItems()):
            if request.hasLock():
                # We are holding a lock, so short-circuit here.
                yield request
            if request.is_locked:
                # Request is locked by someone else
                continue
            if request.state == NodesetRequest.State.TEST_HOLD:
                continue

            score_launchers = (
                set(request._lscores.keys()) if request._lscores else set())
            missing_scores = candidate_names - score_launchers
            if missing_scores or request._lscores is None:
                # (Re-)compute launcher scores
                request._set(_lscores={launcher_score(n, request): n
                                       for n in candidate_names})

            launcher_scores = sorted(request._lscores.items())
            # self.log.debug("Launcher scores: %s", launcher_scores)
            for score, launcher_name in launcher_scores:
                launcher = candidate_launchers.get(launcher_name)
                if not launcher:
                    continue
                if launcher.state != launcher.RUNNING:
                    continue
                if launcher.hostname == self.component_info.hostname:
                    yield request
                break

    def getNodesetRequest(self, request_id):
        return self.requests_cache.getItem(request_id)

    def getNodesetRequests(self):
        return sorted(self.requests_cache.getItems())

    def getMatchingProviderNodes(self):
        all_launchers = {
            c.hostname: c for c in self.component_registry.all("launcher")}

        for node in sorted(self.nodes_cache.getItems()):
            if node.hasLock():
                # We are holding a lock, so short-circuit here.
                yield node
            if node.is_locked:
                # Node is locked by someone else
                if node.state != node.State.SNAPSHOT:
                    # But in snapshot state, we may have work to do
                    continue

            candidate_launchers = {
                n: c for n, c in all_launchers.items()
                if not c.connection_filter
                or node.connection_name in c.connection_filter}
            candidate_names = set(candidate_launchers)
            if node._lscores is None:
                missing_scores = candidate_names
            else:
                score_launchers = set(node._lscores.keys())
                missing_scores = candidate_names - score_launchers

            if missing_scores or node._lscores is None:
                # (Re-)compute launcher scores
                node._set(_lscores={launcher_score(n, node): n
                                    for n in candidate_names})

            launcher_scores = sorted(node._lscores.items())

            for score, launcher_name in launcher_scores:
                launcher = candidate_launchers.get(launcher_name)
                if not launcher:
                    # Launcher is no longer online
                    continue
                if launcher.state != launcher.RUNNING:
                    continue
                if launcher.hostname == self.component_info.hostname:
                    yield node
                break

    def getProviderNode(self, node_id):
        return self.nodes_cache.getItem(node_id)

    def getProviderNodes(self):
        return self.nodes_cache.getItems()

    def createZKContext(self, lock=None, log=None):
        return ZKContext(
            self.zk_client, lock, self.stop_event, log or self.log)

    def cleanupNodes(self):
        # TODO: This method currently just performs some basic cleanup and
        # might need to be extended in the future.
        for node in self.getProviderNodes():
            if node.state != ProviderNode.State.USED:
                continue
                # FIXME: check if the node request still exists
            if node.is_locked:
                continue
            if self.getNodesetRequest(node.request_id):
                continue
            with self.createZKContext(None) as outer_ctx:
                if lock := node.acquireLock(outer_ctx):
                    with self.createZKContext(lock) as ctx:
                        try:
                            node.delete(ctx)
                        except NoNodeError:
                            # Node is already deleted
                            pass
                        finally:
                            node.releaseLock(ctx)
