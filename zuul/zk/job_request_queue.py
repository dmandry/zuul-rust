# Copyright 2021 BMW Group
# Copyright 2021 Acme Gating, LLC
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

import json
import logging
import time
from contextlib import suppress
from enum import Enum
import zlib

from kazoo.exceptions import LockTimeout, NoNodeError, NodeExistsError
from kazoo.protocol.states import EventType, ZnodeStat
from kazoo.client import TransactionRequest

from zuul.lib.jsonutil import json_dumps
from zuul.lib.logutil import get_annotated_logger
from zuul.model import JobRequest
from zuul.zk import sharding
from zuul.zk.event_queues import JobResultFuture
from zuul.zk.exceptions import JobRequestNotFound
from zuul.zk.locks import SessionAwareLock
from zuul.zk.cache import ZuulTreeCache


class JobRequestEvent(Enum):
    CREATED = 0
    UPDATED = 1
    RESUMED = 2
    CANCELED = 3
    DELETED = 4


class RequestUpdater:
    """This class cooperates with the event queues so that we can update a
    request and submit an event in a single transaction."""

    _log = logging.getLogger("zuul.JobRequestQueue")

    def __init__(self, request):
        self.request = request
        self.log = get_annotated_logger(
            self._log, event=request.event_id, build=request.uuid
        )

    def preRun(self):
        """A pre-flight check.  Return whether we should attempt the
        transaction."""
        self.log.debug("Updating request %s", self.request)

        if self.request._zstat is None:
            self.log.debug(
                "Cannot update request %s: Missing version information.",
                self.request.uuid,
            )
            return False
        return True

    def run(self, client):
        """Actually perform the transaction.  The 'client' argument may be a
        transaction or a plain client."""
        if isinstance(client, TransactionRequest):
            setter = client.set_data
        else:
            setter = client.set
        return setter(
            self.request.path,
            JobRequestQueue._dictToBytes(self.request.toDict()),
            version=self.request._zstat.version,
        )

    def postRun(self, result):
        """Process the results of the transaction."""
        try:
            if isinstance(result, Exception):
                raise result
            elif isinstance(result, ZnodeStat):
                self.request._zstat = result
            else:
                raise Exception("Unknown result from ZooKeeper for %s: %s",
                                self.request, result)
        except NoNodeError:
            raise JobRequestNotFound(
                f"Could not update {self.request.path}"
            )


class JobRequestCache(ZuulTreeCache):
    def __init__(self, client, root, request_class,
                 request_callback=None,
                 event_callback=None):
        self.request_class = request_class
        self.request_callback = request_callback
        self.event_callback = event_callback
        super().__init__(client, root)

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
            return (key, fetch, None)
        if len(parts) != 2:
            return (key, fetch, None)
        if parts[0] == 'requests':
            key = (parts[0], parts[1])
            fetch = True
        return (key, fetch, None)

    def preCacheHook(self, event, exists, data=None, stat=None):
        parts = self._parsePath(event.path)
        if parts is None:
            return

        if parts[0] == 'requests' and self.event_callback:
            if event.type == EventType.CREATED:
                if len(parts) == 3:
                    job_event = None
                    if parts[2] == 'cancel':
                        job_event = JobRequestEvent.CANCELED
                    elif parts[2] == 'resume':
                        job_event = JobRequestEvent.RESUMED
                    if job_event:
                        request = self.getRequest(parts[1])
                        if not request:
                            return
                        self.event_callback(request, job_event)
            elif event.type == EventType.DELETED:
                if len(parts) == 2:
                    request = self.getRequest(parts[1])
                    if not request:
                        return
                    self.event_callback(request, JobRequestEvent.DELETED)
        elif parts[0] == 'locks':
            request = self.getRequest(parts[1])
            if not request:
                return
            if len(parts) < 3:
                return
            if exists:
                request.lock_contenders.add(parts[2])
            else:
                request.lock_contenders.discard(parts[2])
            request.is_locked = bool(request.lock_contenders)

    def objectFromRaw(self, key, data, stat):
        if key[0] == 'requests':
            content = self._bytesToDict(data)
            request = self.request_class.fromDict(content)
            request._zstat = stat
            request.path = "/".join([self.root, 'requests', request.uuid])
            request._old_state = request.state
            return request

    def updateFromRaw(self, request, key, data, stat):
        content = self._bytesToDict(data)
        with request.thread_lock:
            # TODO: move thread locking into the TreeCache so we don't
            # duplicate this check.
            if stat.mzxid >= request._zstat.mzxid:
                request.updateFromDict(content)
                request._zstat = stat

    def postCacheHook(self, event, data, stat, key, obj):
        if key[0] == 'requests' and self.request_callback:
            if event.type in (EventType.CREATED, EventType.NONE):
                self.request_callback()

            # This is a test-specific condition: For test cases which
            # are using hold_*_jobs_in_queue the state change on the
            # request from HOLD to REQUESTED is done outside of the
            # server.  Thus, we must also set the wake event (the
            # callback) so the servercan pick up those jobs after they
            # are released. To not cause a thundering herd problem in
            # production for each cache update, the callback is only
            # called under this very specific condition that can only
            # occur in the tests.
            elif (
                self.request_callback
                and obj
                and obj._old_state == self.request_class.HOLD
                and obj.state == self.request_class.REQUESTED
            ):
                obj._old_state = obj.state
                self.request_callback()

    def getRequest(self, request_uuid):
        key = ('requests', request_uuid)
        return self._cached_objects.get(key)

    def getRequests(self):
        return self._cached_objects.values()


class JobRequestQueue:
    log = logging.getLogger("zuul.JobRequestQueue")
    request_class = JobRequest

    def __init__(self, client, root, use_cache=True,
                 request_callback=None, event_callback=None):
        self.zk_client = client
        self.kazoo_client = client.client

        self.REQUEST_ROOT = f"{root}/requests"
        self.LOCK_ROOT = f"{root}/locks"
        self.PARAM_ROOT = f"{root}/params"
        self.RESULT_ROOT = f"{root}/results"
        self.RESULT_DATA_ROOT = f"{root}/result-data"
        self.WAITER_ROOT = f"{root}/waiters"

        self.kazoo_client.ensure_path(self.REQUEST_ROOT)
        self.kazoo_client.ensure_path(self.PARAM_ROOT)
        self.kazoo_client.ensure_path(self.RESULT_ROOT)
        self.kazoo_client.ensure_path(self.RESULT_DATA_ROOT)
        self.kazoo_client.ensure_path(self.WAITER_ROOT)
        self.kazoo_client.ensure_path(self.LOCK_ROOT)

        if use_cache:
            self.cache = JobRequestCache(
                client, root, self.request_class,
                request_callback, event_callback)
        else:
            self.cache = None

    # So far only used by tests
    def waitForSync(self):
        self.cache.waitForSync()

    @property
    def initial_state(self):
        # This supports holding requests in tests
        return self.request_class.REQUESTED

    def inState(self, *states):
        if not states:
            # If no states are provided, build a tuple containing all available
            # ones to always match. We need a tuple to be compliant to the
            # type of *states above.
            states = self.request_class.ALL_STATES

        requests = [
            req for req in list(self.cache.getRequests())
            if req.state in states
        ]

        # Sort the list of requests by precedence and their creation time
        # in ZooKeeper in ascending order to prevent older requests from
        # starving.
        return sorted(requests)

    def next(self):
        for request in self.inState(self.request_class.REQUESTED):
            request = self.cache.getRequest(request.uuid)
            if (request and
                request.state == self.request_class.REQUESTED and
                not request.is_locked):
                yield request

    def submit(self, request, params, needs_result=False):
        log = get_annotated_logger(self.log, event=request.event_id)

        path = "/".join([self.REQUEST_ROOT, request.uuid])
        request.path = path

        if not isinstance(request, self.request_class):
            raise RuntimeError("Request of wrong class")
        if request.state != self.request_class.UNSUBMITTED:
            raise RuntimeError("Request state must be unsubmitted")
        request.state = self.initial_state

        result = None

        # If a result is needed, create the result_path with the same
        # UUID and store it on the request, so the server can store
        # the result there.
        if needs_result:
            result_path = "/".join(
                [self.RESULT_ROOT, request.uuid]
            )
            waiter_path = "/".join(
                [self.WAITER_ROOT, request.uuid]
            )
            self.kazoo_client.create(waiter_path, ephemeral=True)
            result = JobResultFuture(self.zk_client, request.path,
                                     result_path, waiter_path)
            request.result_path = result_path

        log.debug("Submitting job request to ZooKeeper %s", request)

        params_path = self._getParamsPath(request.uuid)
        with sharding.BufferedShardWriter(
            self.kazoo_client, params_path
        ) as stream:
            stream.write(zlib.compress(self._dictToBytes(params)))

        self.kazoo_client.create(path, self._dictToBytes(request.toDict()))

        return result

    def getRequestUpdater(self, request):
        return RequestUpdater(request)

    def update(self, request):
        updater = self.getRequestUpdater(request)
        if not updater.preRun():
            return

        try:
            result = updater.run(self.kazoo_client)
        except Exception as e:
            result = e

        updater.postRun(result)

    def reportResult(self, request, result):
        # Write the result data first since it may be multiple nodes.
        result_data_path = "/".join(
            [self.RESULT_DATA_ROOT, request.uuid]
        )
        with sharding.BufferedShardWriter(
                self.kazoo_client, result_data_path) as stream:
            stream.write(zlib.compress(self._dictToBytes(result)))

        # Then write the result node to signify it's ready.
        data = {'result_data_path': result_data_path}
        self.kazoo_client.create(request.result_path,
                                 self._dictToBytes(data))

    def getRequest(self, request_uuid):
        if self.cache:
            return self.cache.getRequest(request_uuid)
        else:
            path = "/".join([self.REQUEST_ROOT, request_uuid])
            return self._get(path)

    def _get(self, path):
        try:
            data, zstat = self.kazoo_client.get(path)
        except NoNodeError:
            return None

        if not data:
            return None

        content = self._bytesToDict(data)
        request = self.request_class.fromDict(content)
        request.path = path
        request._zstat = zstat
        return request

    def refresh(self, request):
        """Refreshs a request object with the current data from ZooKeeper. """
        try:
            data, zstat = self.kazoo_client.get(request.path)
        except NoNodeError:
            raise JobRequestNotFound(
                f"Could not refresh {request}, ZooKeeper node is missing")

        if not data:
            raise JobRequestNotFound(
                f"Could not refresh {request}, ZooKeeper node is empty")

        content = self._bytesToDict(data)

        with request.thread_lock:
            if zstat.mzxid >= request._zstat.mzxid:
                request.updateFromDict(content)
                request._zstat = zstat

    def remove(self, request):
        log = get_annotated_logger(self.log, request.event_id)
        log.debug("Removing request %s", request)
        try:
            self.kazoo_client.delete(request.path, recursive=True)
        except NoNodeError:
            # Nothing to do if the node is already deleted
            pass
        try:
            self.clearParams(request)
        except NoNodeError:
            pass
        self._deleteLock(request.uuid)

    # We use child nodes here so that we don't need to lock the
    # request node.
    def requestResume(self, request):
        try:
            self.kazoo_client.create(f"{request.path}/resume")
        except (NoNodeError, NodeExistsError):
            # znode is either gone or already exists
            pass

    def requestCancel(self, request):
        try:
            self.kazoo_client.create(f"{request.path}/cancel")
        except (NoNodeError, NodeExistsError):
            # znode is either gone or already exists
            pass

    def fulfillResume(self, request):
        self.kazoo_client.delete(f"{request.path}/resume")

    def fulfillCancel(self, request):
        self.kazoo_client.delete(f"{request.path}/cancel")

    def lock(self, request, blocking=True, timeout=None):
        path = "/".join([self.LOCK_ROOT, request.uuid])
        have_lock = False
        lock = None
        try:
            lock = SessionAwareLock(self.kazoo_client, path)
            have_lock = lock.acquire(blocking, timeout)
        except NoNodeError:
            # Request disappeared
            have_lock = False
        except LockTimeout:
            have_lock = False
            self.log.error(
                "Timeout trying to acquire lock: %s", request.uuid
            )

        # If we aren't blocking, it's possible we didn't get the lock
        # because someone else has it.
        if not have_lock:
            return False

        if not self.kazoo_client.exists(request.path):
            self._releaseLock(request, lock)
            return False

        # Update the request to ensure that we operate on the newest data.
        try:
            self.refresh(request)
        except JobRequestNotFound:
            self._releaseLock(request, lock)
            return False

        request.lock = lock
        return True

    def _releaseLock(self, request, lock):
        """Releases a lock.

        This is used directly after acquiring the lock in case something went
        wrong.
        """
        lock.release()
        self.log.error("Request not found for locking: %s", request.uuid)

        # We may have just re-created the lock parent node just after the
        # scheduler deleted it; therefore we should (re-) delete it.
        self._deleteLock(request.uuid)

    def _deleteLock(self, uuid):
        # Recursively delete the children and the lock parent node.
        path = "/".join([self.LOCK_ROOT, uuid])
        try:
            children = self.kazoo_client.get_children(path)
        except NoNodeError:
            # The lock is apparently already gone.
            return
        tr = self.kazoo_client.transaction()
        for child in children:
            tr.delete("/".join([path, child]))
        tr.delete(path)
        # We don't care about the results
        tr.commit()

    def unlock(self, request):
        if request.lock is None:
            self.log.warning(
                "Request %s does not hold a lock", request
            )
        else:
            request.lock.release()
            request.lock = None

    def isLocked(self, request):
        path = "/".join([self.LOCK_ROOT, request.uuid])
        if not self.kazoo_client.exists(path):
            return False
        lock = SessionAwareLock(self.kazoo_client, path)
        is_locked = len(lock.contenders()) > 0
        return is_locked

    def lostRequests(self, *states):
        # Get a list of requests which are running but not locked by
        # any client.
        if not states:
            states = (self.request_class.RUNNING,)
        for req in self.inState(*states):
            try:
                if req.is_locked:
                    continue
                if self.isLocked(req):
                    continue
                # It may have completed in the interim, so double
                # check that.
                if req.state not in states:
                    continue
                # We may be racing a cache update, so manually
                # refresh and check again.
                self.refresh(req)
                if req.state not in states:
                    continue
            except (NoNodeError, JobRequestNotFound):
                # Request was removed in the meantime
                continue
            yield req

    def _getAllRequestIds(self):
        # Get a list of all request ids without using the cache.
        return self.kazoo_client.get_children(self.REQUEST_ROOT)

    def _findLostParams(self, age):
        # Get data nodes which are older than the specified age (we
        # don't want to delete nodes which are just being written
        # slowly).
        # Convert to MS
        now = int(time.time() * 1000)
        age = age * 1000
        data_nodes = dict()
        for data_id in self.kazoo_client.get_children(self.PARAM_ROOT):
            data_path = self._getParamsPath(data_id)
            data_zstat = self.kazoo_client.exists(data_path)
            if not data_zstat:
                # Node was deleted in the meantime
                continue
            if now - data_zstat.mtime > age:
                data_nodes[data_id] = data_path

        # If there are no candidate data nodes, we don't need to
        # filter them by known requests.
        if not data_nodes:
            return data_nodes.values()

        # Remove current request uuids
        for request_id in self._getAllRequestIds():
            if request_id in data_nodes:
                del data_nodes[request_id]

        # Return the paths
        return data_nodes.values()

    def _findLostResults(self):
        # Get a list of results which don't have a connection waiting for
        # them. As the results and waiters are not part of our cache, we have
        # to look them up directly from ZK.
        waiters1 = set(self.kazoo_client.get_children(self.WAITER_ROOT))
        results = set(self.kazoo_client.get_children(self.RESULT_ROOT))
        result_data = set(self.kazoo_client.get_children(
            self.RESULT_DATA_ROOT))
        waiters2 = set(self.kazoo_client.get_children(self.WAITER_ROOT))

        waiters = waiters1.union(waiters2)
        lost_results = results - waiters
        lost_data = result_data - waiters
        return lost_results, lost_data

    def cleanup(self, age=300):
        # Delete build request params which are not associated with
        # any current build requests.  Note, this does not clean up
        # lost requests themselves; the client takes care of that.
        try:
            for path in self._findLostParams(age):
                try:
                    self.log.error("Removing request params: %s", path)
                    self.kazoo_client.delete(path, recursive=True)
                except Exception:
                    self.log.exception(
                        "Unable to delete request params %s", path)
        except Exception:
            self.log.exception(
                "Error cleaning up request queue %s", self)
        try:
            lost_results, lost_data = self._findLostResults()
            for result_id in lost_results:
                try:
                    path = '/'.join([self.RESULT_ROOT, result_id])
                    self.log.error("Removing request result: %s", path)
                    self.kazoo_client.delete(path, recursive=True)
                except Exception:
                    self.log.exception(
                        "Unable to delete request params %s", result_id)
            for result_id in lost_data:
                try:
                    path = '/'.join([self.RESULT_DATA_ROOT, result_id])
                    self.log.error(
                        "Removing request result data: %s", path)
                    self.kazoo_client.delete(path, recursive=True)
                except Exception:
                    self.log.exception(
                        "Unable to delete request params %s", result_id)
        except Exception:
            self.log.exception(
                "Error cleaning up result queue %s", self)
        try:
            for lock_id in self.kazoo_client.get_children(self.LOCK_ROOT):
                try:
                    lock_path = "/".join([self.LOCK_ROOT, lock_id])
                    request_path = "/".join([self.REQUEST_ROOT, lock_id])
                    if not self.kazoo_client.exists(request_path):
                        self.log.error("Removing stale lock: %s", lock_path)
                        self.kazoo_client.delete(lock_path, recursive=True)
                except Exception:
                    self.log.exception(
                        "Unable to delete lock %s", lock_path)
        except Exception:
            self.log.exception("Error cleaning up locks %s", self)

    @staticmethod
    def _bytesToDict(data):
        return json.loads(data.decode("utf-8"))

    @staticmethod
    def _dictToBytes(data):
        # The custom json_dumps() will also serialize MappingProxyType objects
        return json_dumps(data, sort_keys=True).encode("utf-8")

    def _getParamsPath(self, uuid):
        return '/'.join([self.PARAM_ROOT, uuid])

    def clearParams(self, request):
        """Erase the parameters from ZK to save space"""
        self.kazoo_client.delete(self._getParamsPath(request.uuid),
                                 recursive=True)

    def getParams(self, request):
        """Return the parameters for a request, if they exist.

        Once a request is accepted by an executor, the params
        may be erased from ZK; this will return None in that case.

        """
        with sharding.BufferedShardReader(
            self.kazoo_client, self._getParamsPath(request.uuid)
        ) as stream:
            try:
                data = zlib.decompress(stream.read())
            except NoNodeError:
                return None
            return self._bytesToDict(data)

    def deleteResult(self, path):
        with suppress(NoNodeError):
            self.kazoo_client.delete(path, recursive=True)
