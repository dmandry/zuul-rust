# Copyright 2021 BMW Group
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
from contextlib import contextmanager
from urllib.parse import quote_plus

from kazoo.exceptions import NoNodeError
from kazoo.protocol.states import KazooState
from kazoo.recipe.lock import Lock, ReadLock, WriteLock

from zuul.zk.exceptions import LockException

LOCK_ROOT = "/zuul/locks"
TENANT_LOCK_ROOT = f"{LOCK_ROOT}/tenant"
CONNECTION_LOCK_ROOT = f"{LOCK_ROOT}/connection"


class SessionAwareMixin:
    def __init__(self, client, path, identifier=None, extra_lock_patterns=(),
                 ensure_path=True):
        self._zuul_ephemeral = None
        self._zuul_session_expired = False
        self._zuul_watching_session = False
        self._zuul_seen_contenders = set()
        self._zuul_seen_contender_names = set()
        self._zuul_contender_watch = None
        self._zuul_ensure_path = ensure_path
        super().__init__(client, path, identifier, extra_lock_patterns)

    def _ensure_path(self):
        # Override
        if self._zuul_ensure_path:
            return super()._ensure_path()

    def acquire(self, blocking=True, timeout=None, ephemeral=True):
        ret = super().acquire(blocking, timeout, ephemeral)
        self._zuul_session_expired = False
        if ret and ephemeral:
            self._zuul_ephemeral = ephemeral
            self.client.add_listener(self._zuul_session_watcher)
            self._zuul_watching_session = True
        return ret

    def release(self):
        if self._zuul_watching_session:
            self.client.remove_listener(self._zuul_session_watcher)
            self._zuul_watching_session = False
        return super().release()

    def _zuul_session_watcher(self, state):
        if state == KazooState.LOST:
            self._zuul_session_expired = True

            # Return true to de-register
            return True

    def is_still_valid(self):
        if not self._zuul_ephemeral:
            return True
        return not self._zuul_session_expired

    def watch_for_contenders(self):
        if not self.is_acquired:
            raise Exception("Unable to set contender watch without lock")
        self._zuul_contender_watch = self.client.ChildrenWatch(
            self.path,
            self._zuul_event_watch, send_event=True)

    def _zuul_event_watch(self, children, event=None):
        if not self.is_acquired:
            # Stop watching
            return False
        if children:
            for child in children:
                if child in self._zuul_seen_contenders:
                    continue
                self._zuul_seen_contenders.add(child)
                try:
                    data, stat = self.client.get(self.path + "/" + child)
                    if data is not None:
                        data = data.decode("utf-8")
                        self._zuul_seen_contender_names.add(data)
                except NoNodeError:
                    pass
        return True

    def contender_present(self, name):
        if self._zuul_contender_watch is None:
            raise Exception("Watch not started")
        return name in self._zuul_seen_contender_names

    # This is a kazoo recipe bugfix included here for convenience, but
    # otherwise is not required for the main purpose of this class.
    # https://github.com/python-zk/kazoo/issues/732
    def _best_effort_cleanup(self):
        self._retry(
            self._inner_best_effort_cleanup,
        )

    def _inner_best_effort_cleanup(self):
        node = self.node or self._find_node()
        if node:
            try:
                self._delete_node(node)
            except NoNodeError:
                pass


class SessionAwareLock(SessionAwareMixin, Lock):
    pass


class SessionAwareWriteLock(SessionAwareMixin, WriteLock):
    pass


class SessionAwareReadLock(SessionAwareMixin, ReadLock):
    pass


class FakeLock:
    """A lock that implements the protocol but does not write to ZK.
    Use this when intentionally violating a lock protocol."""

    def __init__(self, *args, **kw):
        pass

    def is_still_valid(self):
        return True

    def acquire(self, blocking, timeout):
        return True

    def release(self):
        pass


@contextmanager
def locked(lock, blocking=True, timeout=None):
    try:
        if not lock.acquire(blocking=blocking, timeout=timeout):
            raise LockException(f"Failed to acquire lock {lock}")
    except NoNodeError:
        # If we encounter a NoNodeError when locking, try one more
        # time in case we're racing the cleanup of a lock directory.
        lock.assured_path = False
        if not lock.acquire(blocking=blocking, timeout=timeout):
            raise LockException(f"Failed to acquire lock {lock}")
    try:
        yield lock
    finally:
        try:
            lock.release()
        except Exception:
            log = logging.getLogger("zuul.zk.locks")
            log.exception("Failed to release lock %s", lock)


@contextmanager
def tenant_read_lock(client, tenant_name, log=None, blocking=True):
    safe_tenant = quote_plus(tenant_name)
    if blocking and log:
        log.debug("Wait for %s read tenant lock", tenant_name)
    with locked(
        SessionAwareReadLock(
            client.client,
            f"{TENANT_LOCK_ROOT}/{safe_tenant}"),
        blocking=blocking
    ) as lock:
        try:
            if log:
                log.debug("Aquired %s read tenant lock", tenant_name)
            yield lock
        finally:
            if log:
                log.debug("Released %s read tenant lock", tenant_name)


@contextmanager
def tenant_write_lock(client, tenant_name, log=None, blocking=True,
                      identifier=None):
    safe_tenant = quote_plus(tenant_name)
    if blocking and log:
        log.debug("Wait for %s write tenant lock (id: %s)",
                  tenant_name, identifier)
    with locked(
        SessionAwareWriteLock(
            client.client,
            f"{TENANT_LOCK_ROOT}/{safe_tenant}",
            identifier=identifier),
        blocking=blocking,
    ) as lock:
        try:
            if log:
                log.debug("Aquired %s write tenant lock (id: %s)",
                          tenant_name, identifier)
            yield lock
        finally:
            if log:
                log.debug("Released %s write tenant lock (id: %s)",
                          tenant_name, identifier)


@contextmanager
def pipeline_lock(client, tenant_name, pipeline_name, blocking=True):
    safe_tenant = quote_plus(tenant_name)
    safe_pipeline = quote_plus(pipeline_name)
    with locked(
        SessionAwareLock(
            client.client,
            f"/zuul/locks/pipeline/{safe_tenant}/{safe_pipeline}"),
        blocking=blocking
    ) as lock:
        yield lock


@contextmanager
def management_queue_lock(client, tenant_name, blocking=True):
    safe_tenant = quote_plus(tenant_name)
    with locked(
        SessionAwareLock(
            client.client,
            f"/zuul/locks/events/management/{safe_tenant}"),
        blocking=blocking
    ) as lock:
        yield lock


@contextmanager
def trigger_queue_lock(client, tenant_name, blocking=True):
    safe_tenant = quote_plus(tenant_name)
    with locked(
        SessionAwareLock(
            client.client,
            f"/zuul/locks/events/trigger/{safe_tenant}"),
        blocking=blocking
    ) as lock:
        yield lock
