# Copyright 2024 BMW Group
# Copyright 2024-2026 Acme Gating, LLC
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

from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import collections
from contextlib import nullcontext
import copy
import errno
import fcntl
import hashlib
import logging
import math
import os
import random
import select
import socket
import subprocess
import threading
import time
import uuid
from urllib.parse import urlparse

import cachetools
import mmh3
import paramiko
import requests
from opentelemetry import trace
from kazoo.exceptions import NodeExistsError

from zuul import exceptions, model
from zuul.launcher.uploadplanner import UploadPlanner
import zuul.lib.repl
from zuul.lib import commandsocket, tracing
from zuul.lib.collections import DefaultKeyDict
from zuul.lib.config import get_default
from zuul.lib.monitoring import MonitoringServer
from zuul.zk.image_registry import (
    ImageBuildRegistry,
    ImageUploadRegistry,
)
from zuul.lib.logutil import get_annotated_logger
from zuul.lib.statsd import get_statsd, normalize_statsd_name
from zuul.version import get_version_string
from zuul.zk import ZooKeeperClient
from zuul.zk.components import COMPONENT_REGISTRY, LauncherComponent
from zuul.zk.election import SessionAwareElection
from zuul.zk.exceptions import LockException
from zuul.zk.event_queues import (
    PipelineResultEventQueue,
    TenantTriggerEventQueue,
)
from zuul.zk.launcher import LauncherApi
from zuul.zk.layout import (
    LayoutProvidersStore,
    LayoutStateStore,
)
from zuul.zk.locks import tenant_read_lock
from zuul.zk.system import ZuulSystem
from zuul.zk.zkobject import ZKContext
from zuul.launcher.subnodes import (
    SubnodeProviderNode,
    SubnodeStateMachine,
)

COMMANDS = (
    commandsocket.StopCommand,
    commandsocket.ReplCommand,
    commandsocket.NoReplCommand,
)

# What gets written to disk in a single write() call; should be a
# multiple of 4k block size.
DOWNLOAD_BLOCK_SIZE = 1024 * 64
# The byte range size for an individual GET operation.  Should be a
# multiple of the above.  Current value is approx 100MiB.
DOWNLOAD_CHUNK_SIZE = DOWNLOAD_BLOCK_SIZE * 1525


def _normalize_statsd_name(name):
    # In the launcher we also want to relace the '/', but since we have
    # existing metrics using the below helper method, we can't change
    # it directly in there at the moment.
    name = normalize_statsd_name(name)
    name = name.replace('/', '_')
    return name


def scores_for_label(label_cname, candidate_names):
    return {
        mmh3.hash(f"{n}-{label_cname}", signed=False): n
        for n in candidate_names
    }


def endpoint_score(name, endpoint):
    return mmh3.hash(f"{name}-{endpoint.canonical_name}", signed=False)


class NodesetRequestError(Exception):
    """Errors that should lead to the request being declined."""
    pass


class ProviderNodeError(Exception):
    """Errors that should lead to the provider node being failed."""
    pass


class LaunchTimeoutError(ProviderNodeError):
    """The node exceeded the provider's launch-timeout"""
    pass


class LauncherStatsElection(SessionAwareElection):
    """Election for emitting launcher stats."""

    def __init__(self, client):
        self.election_root = "/zuul/launcher/stats-election"
        super().__init__(client.client, self.election_root)


class LabelNodeMapping:
    def __init__(self):
        self.label_to_nodes = collections.defaultdict(lambda: {})

    def __repr__(self):
        return repr(self.label_to_nodes)

    def addNode(self, node):
        for label in node.all_labels:
            self.label_to_nodes[label][node.uuid] = node

    def removeNode(self, node):
        for label in node.all_labels:
            self.label_to_nodes[label].pop(node.uuid, None)

    def getNodesForLabel(self, label):
        return self.label_to_nodes[label].values()


class DeleteJob:
    log = logging.getLogger("zuul.Launcher")

    def __init__(self, launcher, image_build_artifact, upload):
        self.log = get_annotated_logger(self.log, upload=upload.uuid)
        self.launcher = launcher
        self.image_build_artifact = image_build_artifact
        self.upload = upload

    def run(self):
        try:
            self._run()
        except Exception:
            self.log.exception("Error in delete job")
        try:
            self.launcher.upload_uuids_in_delete_queue.discard(
                self.upload.uuid)
        except Exception:
            self.log.exception("Error in upload job")

    def _run(self):
        try:
            with self.launcher.createZKContext(None, self.log) as ctx:
                try:
                    with self.upload.locked(ctx, blocking=False):
                        self.upload.refresh(ctx)
                        self.log.info("Deleting image upload %s", self.upload)
                        with self.upload.activeContext(ctx):
                            self.upload.state = self.upload.State.DELETING
                        provider_cname = self.upload.providers[0]
                        provider = self.launcher.\
                            _getProviderByCanonicalName(provider_cname)
                        if self.upload.external_id:
                            provider.deleteImage(self.upload.external_id)
                        self.upload.delete(ctx)
                        self.launcher.upload_deleted_event.set()
                        self.launcher.wake_event.set()
                except LockException:
                    return
        except Exception:
            self.log.exception("Unable to delete upload %s", self.upload)


class UploadJob:
    log = logging.getLogger("zuul.Launcher")

    def __init__(self, launcher, image_build_artifact, uploads, all_uploads):
        self.log = get_annotated_logger(
            self.log, iba=image_build_artifact.uuid)
        self.launcher = launcher
        self.image_build_artifact = image_build_artifact
        # All pending uploads for this artifact.  The uploads may have
        # different configurations.
        self.uploads = uploads
        # All uploads for this artifact, even completed ones
        self.all_uploads = all_uploads

    def run(self):
        try:
            self._run()
        except Exception:
            self.log.exception("Error in upload job")
        try:
            for upload in self.uploads:
                self.launcher.upload_uuids_in_queue.discard(upload.uuid)
        except Exception:
            self.log.exception("Error in upload job")

    def _acquireUploadLocks(self, ctx, uploads):
        try:
            with self.image_build_artifact.locked(ctx, blocking=False):
                for upload in self.uploads:
                    if upload.acquireLock(ctx, blocking=False):
                        upload.refresh(ctx)
                        if upload.external_id:
                            # Someone else finished it
                            upload.releaseLock(ctx)
                        elif upload.state != upload.State.PENDING:
                            # Someone else failed it (or otherwise)
                            upload.releaseLock(ctx)
                        else:
                            uploads.append(upload)
                            self.log.debug(
                                "Acquired upload lock for %s",
                                upload)
                            with upload.activeContext(ctx):
                                upload.state = upload.State.UPLOADING
        except LockException:
            # We may have raced another launcher; set the
            # event to try again.
            self.launcher.image_updated_event.set()

    def _handleDownload(self):
        url = self.image_build_artifact.url
        url_components = urlparse(url)
        ext = url_components.path.split('.')[-1]
        path = os.path.join(self.launcher.temp_dir,
                            self.image_build_artifact.uuid)
        path = f'{path}.{ext}'
        self.log.info("Downloading artifact %s from %s into %s",
                      self.image_build_artifact, url, path)
        try:
            if ret := self.launcher.downloadArtifact(url, path):
                return ret
        except Exception as e:
            self.log.info("Unable to download %s: %s", url, e)

        # We can't download the file using requests, see if any of the
        # tenant's connections can download it using internal methods.
        providers = self.launcher.tenant_providers[
            self.image_build_artifact.build_tenant_name]
        for provider in providers:
            try:
                if ret := provider.downloadUrl(url, path):
                    return ret
            except Exception as e:
                self.log.info("Unable to download %s via %s: %s",
                              url, provider, e)

    def _handleCompression(self, path):
        if path.endswith('.zst'):
            orig_path = path
            try:
                subprocess.run(["zstd", "-dq", path],
                               cwd=self.launcher.temp_dir, check=True,
                               capture_output=True)
                path = path[:-len('.zst')]
                self.log.debug("Decompressed image to %s", path)
            finally:
                # Regardless of whether the decompression succeeded,
                # delete the original.  If it did succeed, the
                # original may already be gone.
                if os.path.exists(orig_path):
                    os.unlink(orig_path)
                    self.log.info("Deleted %s", orig_path)
        return path

    def _validateChecksum(self, path):
        artifact = self.image_build_artifact
        if not artifact.sha256:
            self.log.debug("Image build artifact %s has no sha256 checksum, "
                           "skipping validation", artifact)
            return
        h = hashlib.sha256()
        with open(path, 'rb') as f:
            while data := f.read(4096):
                h.update(data)
        digest = h.hexdigest()
        if digest != artifact.sha256:
            raise Exception(
                f"Downloaded file {path} sha256 "
                f"digest {digest} does not match "
                f"artifact {artifact} digest {artifact.sha256}")

    def _run(self):
        path = None
        uploads = []
        futures = {}
        with self.launcher.createZKContext(None, self.log) as ctx:
            try:
                self.log.debug("Starting upload job for %s",
                               self.image_build_artifact)
                # Get a list of uploads we hold locks for
                self._acquireUploadLocks(ctx, uploads)
                if not uploads:
                    return

                pending_uploads = uploads[:]
                planner = UploadPlanner(
                    self.launcher, self.image_build_artifact,
                    pending_uploads, self.all_uploads)
                planner.plan()

                for upload, job in planner.import_jobs:
                    log = get_annotated_logger(self.log, upload=upload.uuid)
                    log.debug("Scheduling import for %s", upload)
                    future = self.launcher.endpoint_import_executor.submit(
                        EndpointUploadJob(
                            self.launcher, upload, job, futures, log).run
                    )
                    futures[future] = upload

                # Copy jobs for completed uploads can run now:
                for upload, job, external_id in planner.copy_jobs:
                    log = get_annotated_logger(self.log, upload=upload.uuid)
                    log.debug("Scheduling copy for %s", upload)
                    future = self.launcher.endpoint_import_executor.submit(
                        EndpointUploadJob(
                            self.launcher, upload, job, futures, log,
                            external_id).run
                    )
                    futures[future] = upload

                # Any uploads remaining need to be handled by
                # downloading the artifact and uploading.
                if planner.upload_jobs:
                    path = self._handleDownload()
                    if not path:
                        raise Exception("Unable to download artifact %s" % (
                            self.image_build_artifact,))
                    path = self._handleCompression(path)
                    self._validateChecksum(path)
                    for upload, job in planner.upload_jobs:
                        log = get_annotated_logger(
                            self.log, upload=upload.uuid)
                        log.debug("Scheduling upload for %s", upload)
                        future = self.launcher.endpoint_upload_executor.submit(
                            EndpointUploadJob(
                                self.launcher, upload, job, futures, log, path
                            ).run
                        )
                        futures[future] = upload

            finally:
                while futures:
                    done, _ = concurrent.futures.wait(
                        set(futures.keys()),
                        return_when=concurrent.futures.FIRST_COMPLETED
                    )

                    for future in done:
                        upload = futures.pop(future)
                        log = get_annotated_logger(
                            self.log, upload=upload.uuid)
                        try:
                            log.debug("Upload %s finished", upload)
                            future.result()
                        except Exception:
                            log.exception("Unable to upload image %s", upload)
                        try:
                            self.finalizeUpload(upload, ctx, log)
                            # We are sending the image validate event
                            # here while still holding the upload lock.
                            # This shouldn't be an issue as we release
                            # the lock immediately afterwards. The
                            # pipeline will only need to acquire the
                            # lock when reporting the buildset.
                            if (not upload.validated
                                    and upload.state == upload.State.READY):
                                self.launcher.addImageValidateEvent(upload)
                        except Exception:
                            log.exception("Unable to update state for %s",
                                          upload)
                        finally:
                            try:
                                upload.releaseLock(ctx)
                                log.debug("Released upload lock for %s",
                                          upload)

                            except Exception:
                                log.exception(
                                    "Unable to release lock for %s", upload)
                if path:
                    try:
                        os.unlink(path)
                        self.log.info("Deleted %s", path)
                    except Exception:
                        self.log.exception("Unable to delete %s", path)
                self.log.debug("Finished upload job for %s with %s uploads",
                               self.image_build_artifact, len(uploads))

    def finalizeUpload(self, upload, ctx, log):
        with upload.activeContext(ctx):
            if upload.external_id:
                log.debug("Marking upload %s ready", upload)
                upload.state = upload.State.READY
            else:
                # Make 3 total attempts
                if upload.attempt < 2:
                    new_upload = upload.copy(ctx)
                    log.debug("Replacing upload %s with %s",
                              upload, new_upload)
                    log.debug("Marking upload %s deleting", upload)
                    upload.state = upload.State.DELETING
                    self.launcher.upload_deleted_event.set()
                else:
                    log.debug("Marking upload %s failed", upload)
                    upload.state = upload.State.FAILED


class EndpointUploadJob:

    def __init__(self, launcher, upload, job, futures, log, *args):
        self.log = log
        self.launcher = launcher
        self.upload = upload
        self.job = job
        self.futures = futures
        self.args = args
        safe_iname = _normalize_statsd_name(upload.canonical_name)
        safe_ename = _normalize_statsd_name(upload.endpoint_name)
        self.stats_key = f"zuul.image.{safe_iname}.upload.{safe_ename}"

    def run(self):
        try:
            # zuul.image.<image>.upload.<endpoint>.duration timer
            with self.launcher.statsd_timer(f"{self.stats_key}.duration"):
                self._run()
        except Exception:
            self.log.exception("Error in endpoint upload job")

    def _run(self):
        # The upload has a list of providers with identical
        # configurations.  Pick one of them as a representative.
        self.log.info("Starting upload %s", self.upload)
        external_id = self.job.run(*self.args)
        self.log.info("Finished upload %s", self.upload)
        with self.launcher.createZKContext(self.upload._lock, self.log) as ctx:
            self.upload.updateAttributes(
                ctx,
                external_id=external_id)

        for (dep_upload, dep_job) in self.job.dependents:
            log = get_annotated_logger(self.log, upload=dep_upload.uuid)
            # The only dependent jobs right now are copy jobs
            log.debug("Scheduling copy for %s", dep_upload)
            future = self.launcher.endpoint_import_executor.submit(
                EndpointUploadJob(self.launcher, dep_upload, dep_job,
                                  self.futures, log, external_id).run,
            )
            self.futures[future] = dep_upload


class NodescanRequest:
    """A state machine for a nodescan request.

    When complete, use the result() method to obtain the keys or raise
    an exception if an errer was encountered during processing.

    """

    START = 'start'
    CONNECTING_INIT = 'connecting'
    NEGOTIATING_INIT = 'negotiating'
    CONNECTING_KEY = 'connecting key'
    NEGOTIATING_KEY = 'negotiating key'
    COMPLETE = 'complete'
    # This is for unit tests to override
    _socket_class = socket.socket

    def __init__(self, node, log):
        self.state = self.START
        self.node = node
        self.host_key_checking = node.host_key_checking
        self.timeout = node.boot_timeout
        self.log = log
        self.complete = False
        self.keys = []
        if (node.connection_type == 'ssh' or
            node.connection_type == 'network_cli'):
            self.gather_hostkeys = True
        else:
            self.gather_hostkeys = False
        self.ip = node.interface_ip
        self.port = node.connection_port
        if self.port:
            addrinfo = socket.getaddrinfo(self.ip, self.port)[0]
            self.family = addrinfo[0]
            self.sockaddr = addrinfo[4]
        self.sock = None
        self.transport = None
        self.event = None
        self.key_types = None
        self.key_index = None
        self.key_type = None
        self.start_time = time.monotonic()
        self.worker = None
        self.exception = None
        self.connect_start_time = None
        # Stats
        self.init_connection_attempts = 0
        self.key_connection_failures = 0
        self.key_negotiation_failures = 0

    def setWorker(self, worker):
        """Store a reference to the worker thread so we register and unregister
        the socket file descriptor from the polling object"""
        self.worker = worker

    def fail(self, exception):
        """Declare this request a failure and store the related exception"""
        self.exception = exception
        self.cleanup()
        self.complete = True
        self.state = self.COMPLETE

    def cleanup(self):
        """Try to close everything and unregister from the worker"""
        self._close()
        if self.exception:
            status = 'failed'
        else:
            status = 'complete'
        dt = int(time.monotonic() - self.start_time)
        self.log.debug("Nodescan request %s with %s keys, "
                       "%s initial connection attempts, "
                       "%s key connection failures, "
                       "%s key negotiation failures in %s seconds",
                       status, len(self.keys),
                       self.init_connection_attempts,
                       self.key_connection_failures,
                       self.key_negotiation_failures,
                       dt)
        self.worker = None

    def result(self):
        """Return the resulting keys, or raise an exception"""
        if self.exception:
            raise self.exception
        return self.keys

    def _close(self):
        if self.transport:
            try:
                self.transport.close()
            except Exception:
                pass
            self.transport = None
            self.event = None
        if self.sock:
            self.worker.unRegisterDescriptor(self.sock)
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    def _checkTimeout(self):
        now = time.monotonic()
        if now - self.start_time > self.timeout:
            raise exceptions.ConnectionTimeoutException(
                f"Timeout connecting to {self.ip} on port {self.port}")

    def _checkTransport(self):
        # This stanza is from
        # https://github.com/paramiko/paramiko/blob/main/paramiko/transport.py
        if not self.transport.active:
            e = self.transport.get_exception()
            if e is not None:
                raise e
            raise paramiko.exceptions.SSHException("Negotiation failed.")

    def _connect(self):
        if self.sock:
            self.worker.unRegisterDescriptor(self.sock)
        self.sock = self._socket_class(self.family, socket.SOCK_STREAM)
        # Set nonblocking so we can poll for connection completion
        self.sock.setblocking(False)
        try:
            self.sock.connect(self.sockaddr)
        except BlockingIOError:
            pass
        self.connect_start_time = time.monotonic()
        self.worker.registerDescriptor(self.sock)

    def _start(self):
        # Use our Event subclass that will wake the worker when the
        # event is set.
        self.event = NodescanEvent(self.worker)
        # Return the socket to blocking mode as we hand it off to paramiko.
        self.sock.setblocking(True)
        self.transport = paramiko.transport.Transport(self.sock)
        if self.key_type is not None:
            opts = self.transport.get_security_options()
            opts.key_types = [self.key_type]
        # This starts a thread.
        self.transport.start_client(
            event=self.event, timeout=self.timeout)

    def _nextKey(self):
        self._close()
        self.key_index += 1
        if self.key_index >= len(self.key_types):
            self.state = self.COMPLETE
            return True
        self.key_type = self.key_types[self.key_index]
        self._connect()
        self.state = self.CONNECTING_KEY

    def advance(self, socket_ready):
        if self.state == self.START:
            if self.worker is None:
                raise Exception("Request not registered with worker")
            if not self.host_key_checking:
                self.state = self.COMPLETE
            else:
                self.init_connection_attempts += 1
                self._connect()
                self.state = self.CONNECTING_INIT

        if self.state == self.CONNECTING_INIT:
            if not socket_ready:
                # Check the overall timeout
                self._checkTimeout()
                # If we're still here, then don't let any individual
                # connection attempt last more than 10 seconds:
                if time.monotonic() - self.connect_start_time >= 10:
                    self._close()
                    self.state = self.START
                return
            eno = self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if eno:
                if eno not in [errno.ECONNREFUSED, errno.EHOSTUNREACH]:
                    self.log.exception(
                        f"Error {eno} connecting to {self.ip} "
                        f"on port {self.port}")
                # Try again.  Don't immediately start to reconnect
                # since econnrefused can happen very quickly, so we
                # could end up busy-waiting.
                self._close()
                self.state = self.START
                self._checkTimeout()
                return
            if self.gather_hostkeys:
                self._start()
                self.state = self.NEGOTIATING_INIT
            else:
                self.state = self.COMPLETE

        if self.state == self.NEGOTIATING_INIT:
            if not self.event.is_set():
                self._checkTimeout()
                return
            # This will raise an exception on ssh errors
            try:
                self._checkTransport()
            except Exception:
                self.log.exception(
                    f"SSH error connecting to {self.ip} on port {self.port}")
                # Try again; go to start to avoid busy-waiting.
                self._close()
                self.key_negotiation_failures += 1
                self.state = self.START
                self._checkTimeout()
                return
            # This is our first successful connection.  Now that
            # we've done it, start again specifying the first key
            # type.
            opts = self.transport.get_security_options()
            self.key_types = opts.key_types
            self.key_index = -1
            self._nextKey()

        if self.state == self.CONNECTING_KEY:
            if not socket_ready:
                self._checkTimeout()
                # If we're still here, then don't let any individual
                # connection attempt last more than 10 seconds:
                if time.monotonic() - self.connect_start_time >= 10:
                    # Restart the connection attempt for this key (not
                    # the whole series).
                    self.key_connection_failures += 1
                    self._close()
                    self._connect()
                    self.state = self.CONNECTING_KEY
                return
            eno = self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if eno:
                self.log.error(
                    f"Error {eno} connecting to {self.ip} on port {self.port}")
                self.key_connection_failures += 1
                self._close()
                self._connect()
                self.state = self.CONNECTING_KEY
                return
            self._start()
            self.state = self.NEGOTIATING_KEY

        if self.state == self.NEGOTIATING_KEY:
            if not self.event.is_set():
                self._checkTimeout()
                return
            # This will raise an exception on ssh errors
            try:
                self._checkTransport()
            except Exception as e:
                msg = str(e)
                if 'no acceptable host key' not in msg:
                    # We expect some host keys to not be valid
                    # when scanning only log if the error isn't
                    # due to mismatched host key types.
                    self.log.exception(
                        f"SSH error connecting to {self.ip} "
                        f"on port {self.port}")
                    self.key_negotiation_failures += 1
                self._nextKey()

        # Check if we're still in the same state
        if self.state == self.NEGOTIATING_KEY:
            key = self.transport.get_remote_server_key()
            if key:
                self.keys.append("%s %s" % (key.get_name(), key.get_base64()))
                self.log.debug('Added ssh host key: %s', key.get_name())
            self._nextKey()

        if self.state == self.COMPLETE:
            self._close()
            self.complete = True


class NodescanEvent(threading.Event):
    """A subclass of event that will wake the NodescanWorker poll"""
    def __init__(self, worker, *args, **kw):
        super().__init__(*args, **kw)
        self._zuul_worker = worker

    def set(self):
        super().set()
        try:
            os.write(self._zuul_worker.wake_write, b'\n')
        except Exception:
            pass


class NodescanWorker:
    """Handles requests for nodescans.

    This class has a single thread that drives nodescan requests
    submitted by the launcher.

    """
    # This process is highly scalable, except for paramiko which
    # spawns a thread for each ssh connection.  To avoid thread
    # overload, we set a max value for concurrent requests.
    # Simultaneous requests higher than this value will be queued.
    MAX_REQUESTS = 100
    TIMEOUT = 1
    # For unit tests to override
    _poll_class = select.epoll

    def __init__(self):
        # Remember to close all pipes to prevent leaks in tests.
        self.wake_read, self.wake_write = os.pipe()
        fcntl.fcntl(self.wake_read, fcntl.F_SETFL, os.O_NONBLOCK)
        self._running = False
        self._active_requests = []
        self._pending_requests = []
        self.poll = self._poll_class()
        self.poll.register(self.wake_read, select.EPOLLIN)

    def start(self):
        self._running = True
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def stop(self):
        self._running = False
        os.write(self.wake_write, b'\n')

    def join(self):
        self.thread.join()

        for fd in (self.wake_read, self.wake_write):
            try:
                os.close(fd)
            except OSError as e:
                # If the nodescan worker is joined multiple times the
                # file descriptors have already been closed.
                if e.errno != errno.EBADF:
                    raise

    def addRequest(self, request):
        """Submit a nodescan request"""
        request.setWorker(self)
        if len(self._active_requests) >= self.MAX_REQUESTS:
            self._pending_requests.append(request)
        else:
            self._active_requests.append(request)
            # If the poll is sleeping, wake it up for immediate action
            os.write(self.wake_write, b'\n')

    def removeRequest(self, request):
        """Remove the request and cleanup"""
        if request is None:
            return
        request.cleanup()
        try:
            self._active_requests.remove(request)
        except ValueError:
            pass
        try:
            self._pending_requests.remove(request)
        except ValueError:
            pass

    def length(self):
        return len(self._active_requests) + len(self._pending_requests)

    def registerDescriptor(self, fd):
        """Register the fd with the poll object"""
        # Oneshot means that once it triggers, it will automatically
        # be removed.  That's great for us since we only use this for
        # detecting when the initial connection is complete and have
        # no further use.
        self.poll.register(
            fd, select.EPOLLOUT | select.EPOLLERR |
            select.EPOLLHUP | select.EPOLLONESHOT)

    def unRegisterDescriptor(self, fd):
        """Unregister the fd with the poll object"""
        try:
            self.poll.unregister(fd)
        except Exception:
            pass

    def run(self):
        # To avoid busy waiting, we will process any request with a
        # ready socket, but only process the remaining requests
        # periodically.  This value is the last time we processed
        # those unready requests.
        last_unready_check = 0
        while self._running:
            # Set the poll timeout to 1 second so that we check all
            # requests for timeouts every second.  This could be
            # increased to a few seconds without significant impact.
            timeout = self.TIMEOUT
            while (self._pending_requests and
                   len(self._active_requests) < self.MAX_REQUESTS):
                # If we have room for more requests, add them and set
                # the timeout to 0 so that we immediately start
                # advancing them.
                request = self._pending_requests.pop(0)
                self._active_requests.append(request)
                timeout = 0
                last_unready_check = 0
            ready = self.poll.poll(timeout=timeout)
            ready = [x[0] for x in ready]
            if self.wake_read in ready:
                # Empty the wake pipe
                while True:
                    try:
                        os.read(self.wake_read, 1024)
                    except BlockingIOError:
                        break
            process_unready = (time.monotonic() - last_unready_check >
                               self.TIMEOUT)
            for request in self._active_requests:
                try:
                    socket_ready = (request.sock and
                                    request.sock.fileno() in ready)
                    event_ready = request.event and request.event.is_set()
                    if process_unready or socket_ready or event_ready:
                        request.advance(socket_ready)
                except Exception as e:
                    request.fail(e)
                if request.complete:
                    self.removeRequest(request)
            if process_unready:
                last_unready_check = time.monotonic()


class CleanupWorker:
    # Delay 60 seconds between iterations
    INTERVAL = 60
    log = logging.getLogger("zuul.Launcher")

    def __init__(self, launcher):
        self.launcher = launcher
        self.wake_event = threading.Event()
        # These are indexed by endpoint cname
        self.possibly_leaked_nodes = {}
        self.possibly_leaked_uploads = {}
        self.possibly_leaked_resources = {}
        self._running = False
        self.thread = None

    def start(self):
        self.log.debug("Starting cleanup worker thread")
        self._running = True
        self.thread = threading.Thread(target=self.run,
                                       name="CleanupWorker")
        self.thread.start()

    def stop(self):
        self.log.debug("Stopping cleanup worker")
        self._running = False
        self.wake_event.set()

    def join(self):
        self.log.debug("Joining cleanup thread")
        if self.thread:
            self.thread.join()
        self.log.debug("Joined cleanup thread")

    def run(self):
        while self._running:
            # Wait before performing the first cleanup
            self.wake_event.wait(self.INTERVAL)
            self.wake_event.clear()
            try:
                self._run()
            except Exception:
                self.log.exception("Error in cleanup worker:")

    def getMatchingEndpoints(self):
        all_launchers = {
            c.hostname: c for c in COMPONENT_REGISTRY.registry.all("launcher")}

        for endpoint in self.launcher.endpoints.values():
            candidate_launchers = {
                n: c for n, c in all_launchers.items()
                if not c.connection_filter
                or endpoint.connection.connection_name in c.connection_filter}
            candidate_names = set(candidate_launchers)
            launcher_scores = {endpoint_score(n, endpoint): n
                               for n in candidate_names}
            sorted_scores = sorted(launcher_scores.items())
            for score, launcher_name in sorted_scores:
                launcher = candidate_launchers.get(launcher_name)
                if not launcher:
                    # Launcher is no longer online
                    continue
                if launcher.state != launcher.RUNNING:
                    continue
                if launcher.hostname == self.launcher.component_info.hostname:
                    yield endpoint
                break

    def _run(self):
        for endpoint in self.getMatchingEndpoints():
            try:
                self.cleanupLeakedResources(endpoint)
            except Exception:
                self.log.exception("Error in cleanup worker:")

    def cleanupLeakedResources(self, endpoint):
        newly_leaked_nodes = {}
        newly_leaked_uploads = {}
        newly_leaked_resources = {}
        possibly_leaked_nodes = self.possibly_leaked_nodes.get(
            endpoint.canonical_name, {})
        possibly_leaked_uploads = self.possibly_leaked_uploads.get(
            endpoint.canonical_name, {})
        possibly_leaked_resources = self.possibly_leaked_resources.get(
            endpoint.canonical_name, {})

        # Get a list of all providers that share this endpoint.  This
        # is because some providers may store resources like image
        # uploads in multiple per-provider locations.
        providers = [
            p for p in self.launcher._getProviders()
            if p.getEndpoint().canonical_name == endpoint.canonical_name
        ]
        now = int(time.time())

        # The endpoint may have infrastructure resources that may be
        # used by nodes.  Start with a list of the ones that the
        # endpoint itself thinks are still in use.
        required_resource_ids = set(
            r.unique_id for r in endpoint.listRequiredInfrastructureResources()
        )

        # Get information about all nodes and their required resources
        node_ids = []
        for node in self.launcher.api.nodes_cache.getItems():
            node_ids.append(node.uuid)
            # If this node is associated with a provider on this
            # endpoint, consider its resources as required.
            if node.endpoint_name == endpoint.name:
                required_resource_ids.update(set(
                    node.getRequiredResources()))

        # Update quota; we get different classes back from
        # listInstances and listResources, so we can't use the same
        # call for both.  Ideally the endpoint will cache the internal
        # API call.
        quota_used = model.QuotaInformation()
        system_id = self.launcher.system.system_id
        for instance in endpoint.listInstances():
            meta = instance.metadata
            if (meta.get('zuul_system_id') == system_id and
                meta.get('zuul_node_uuid') in node_ids):
                continue
            quota_used.add(instance.getQuotaInformation())
        endpoint.quota_cache.setUnmanagedUsage(quota_used)

        for resource in endpoint.listResources(providers):
            if (resource.metadata.get('zuul_system_id') !=
                self.launcher.system.system_id):
                continue
            node_id = resource.metadata.get('zuul_node_uuid')
            upload_id = resource.metadata.get('zuul_upload_uuid')
            expiration = resource.metadata.get('zuul_expiration')
            if node_id and self.launcher.api.getProviderNode(node_id) is None:
                newly_leaked_nodes[node_id] = resource
                if node_id in possibly_leaked_nodes:
                    # We've seen this twice now, so it's not a race
                    # condition.
                    try:
                        endpoint.deleteResource(resource)
                        # if self._statsd:
                        #     key = ('nodepool.provider.%s.leaked.%s'
                        #            % (self.provider.name,
                        #               resource.plural_metric_name))
                        #     self._statsd.incr(key, 1)
                    except Exception:
                        self.log.exception("Unable to delete leaked "
                                           f"resource for node {node_id}")
            if (upload_id and
                self.launcher.image_upload_registry.getItem(
                    upload_id) is None):
                newly_leaked_uploads[upload_id] = resource
                if upload_id in possibly_leaked_uploads:
                    # We've seen this twice now, so it's not a race
                    # condition.
                    try:
                        endpoint.deleteResource(resource)
                        # if self._statsd:
                        #     key = ('nodepool.provider.%s.leaked.%s'
                        #            % (self.provider.name,
                        #               resource.plural_metric_name))
                        #     self._statsd.incr(key, 1)
                    except Exception:
                        self.log.exception(
                            "Unable to delete leaked "
                            f"resource for upload {upload_id}")
            if expiration:
                expiration = int(expiration)
                if now > expiration:
                    # We can delete this immediately, we don't need to
                    # wait for another cycle.
                    try:
                        endpoint.deleteResource(resource)
                        # if self._statsd:
                        #     key = ('nodepool.provider.%s.leaked.%s'
                        #            % (self.provider.name,
                        #               resource.plural_metric_name))
                        #     self._statsd.incr(key, 1)
                    except Exception:
                        self.log.exception(
                            "Unable to delete leaked resource")
            if ((not (node_id or upload_id or expiration)) and
                resource.unique_id not in required_resource_ids):
                # This may be an infrastructure resource.
                newly_leaked_resources[resource.unique_id] = resource
                if resource.unique_id in possibly_leaked_resources:
                    try:
                        endpoint.deleteResource(resource)
                        # if self._statsd:
                        #     key = ('nodepool.provider.%s.leaked.%s'
                        #            % (self.provider.name,
                        #               resource.plural_metric_name))
                        #     self._statsd.incr(key, 1)
                    except Exception:
                        self.log.exception(
                            "Unable to delete leaked resource")

        self.possibly_leaked_nodes[endpoint.canonical_name] =\
            newly_leaked_nodes
        self.possibly_leaked_uploads[endpoint.canonical_name] =\
            newly_leaked_uploads
        self.possibly_leaked_resources[endpoint.canonical_name] =\
            newly_leaked_resources


class Launcher:
    log = logging.getLogger("zuul.Launcher")
    tracer = trace.get_tracer("zuul")
    # Max. time to wait for a cache to sync
    CACHE_SYNC_TIMEOUT = 10
    # Max. time the main event loop is allowed to sleep
    MAX_SLEEP = 1
    # Min. interval to wake up the main event loop
    MIN_INTERVAL = 60
    DELETE_TIMEOUT = 600
    MAX_QUOTA_AGE = 5 * 60  # How long to keep the quota information cached
    _stats_interval = 30

    def __init__(self, config, connections, _hostname=None):
        # _hostname is only used by the unit tests
        self._running = True
        # The cleanup method requires some extra AWS mocks that are
        # not enabled in most tests, so we allow the test suite to
        # disable it.
        # TODO: switch basic launcher tests to openstack and remove.
        self._start_cleanup = True
        self.config = config
        self.connections = connections
        filter_string = get_default(
            self.config, "launcher", "connection_filter")
        if filter_string:
            self.connection_filter = [
                f.strip() for f in filter_string.split(",")]
        else:
            self.connection_filter = None
        self.repl = None
        # All tenants and all providers
        self.tenant_providers = {}
        # Only endpoints corresponding to connections handled by this
        # launcher
        self.endpoints = {}

        self.statsd = get_statsd(config)
        if self.statsd:
            self.statsd_timer = self.statsd.timer
        else:
            self.statsd_timer = nullcontext

        # Raw provider quota limits
        self._provider_limits_cache = cachetools.TTLCache(
            maxsize=8192, ttl=self.MAX_QUOTA_AGE)
        # Provider quota - unmanaged usage
        self._provider_quota_cache = cachetools.TTLCache(
            maxsize=8192, ttl=self.MAX_QUOTA_AGE)

        self.tracing = tracing.Tracing(self.config)
        self.zk_client = ZooKeeperClient.fromConfig(self.config)
        self.zk_client.connect()

        # TODO: remove this warning after we make 3.9 mandatory
        if not self.zk_client.cap_watched_event_zxid:
            self.log.warning(
                "A future version of Zuul will require "
                "ZooKeeper 3.9.0 or later")

        self.system = ZuulSystem(self.zk_client)
        self.trigger_events = TenantTriggerEventQueue.createRegistry(
            self.zk_client, self.connections
        )
        self.result_events = PipelineResultEventQueue.createRegistry(
            self.zk_client
        )

        COMPONENT_REGISTRY.create(self.zk_client)
        self.hostname = _hostname or socket.getfqdn()
        self.component_info = LauncherComponent(
            self.zk_client, self.hostname, version=get_version_string(),
            connection_filter=self.connection_filter)
        self.component_info.register()
        self.wake_event = threading.Event()
        self.stop_event = threading.Event()
        self.join_event = threading.Event()
        # A lock when changing the providers, mostly for the stats thread
        self.providers_update_lock = threading.Lock()

        COMPONENT_REGISTRY.registry.registerCallback(self.wake_event.set)

        self.monitoring_server = MonitoringServer(config, 'launcher',
                                                  self.component_info)
        self.monitoring_server.start()

        self.api = LauncherApi(
            self.zk_client, COMPONENT_REGISTRY.registry, self.component_info,
            self.wake_event.set, self.connection_filter)

        temp_root = get_default(self.config, 'launcher', 'temp_dir',
                                '/tmp', expand_user=True)
        self.temp_dir = os.path.join(temp_root, 'zuul-launcher')
        os.makedirs(self.temp_dir, exist_ok=True)

        self.command_map = {
            commandsocket.StopCommand.name: self.stop,
            commandsocket.ReplCommand.name: self.startRepl,
            commandsocket.NoReplCommand.name: self.stopRepl,
        }
        command_socket = get_default(
            self.config, "launcher", "command_socket",
            "/var/lib/zuul/launcher.socket")
        self.command_socket = commandsocket.CommandSocket(command_socket)
        self._command_running = False

        self.layout_updated_event = threading.Event()
        self.layout_updated_event.set()

        self.image_updated_event = threading.Event()
        self.upload_deleted_event = threading.Event()

        self.tenant_layout_state = LayoutStateStore(
            self.zk_client, self._layoutUpdatedCallback)
        self.layout_providers_store = LayoutProvidersStore(
            self.zk_client, self.connections, self.system.system_id)
        self.local_layout_state = {}

        self.image_build_registry = ImageBuildRegistry(
            self.zk_client,
            self._imageUpdatedCallback
        )
        self.image_upload_registry = ImageUploadRegistry(
            self.zk_client,
            self._imageUpdatedCallback
        )

        self.nodescan_worker = NodescanWorker()
        self._run_lock = threading.Lock()
        self.launcher_thread = threading.Thread(
            target=self.run,
            name="Launcher",
        )
        # Avoid submitting duplicate upload jobs
        self.upload_uuids_in_queue = set()
        self.upload_uuids_in_delete_queue = set()
        # Simultaneous image artifact processes (download+upload)
        self.upload_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="UploadWorker",
        )
        # Simultaneous uploads
        self.endpoint_upload_executor = ThreadPoolExecutor(
            # TODO: make configurable
            max_workers=10,
            thread_name_prefix="UploadWorker",
        )
        # Simultaneous imports/copies -- we can have more threads here
        # since we aren't doing much work (but we may still poll, so
        # don't go crazy)
        self.endpoint_import_executor = ThreadPoolExecutor(
            max_workers=20,
            thread_name_prefix="ImportWorker",
        )
        self.cleanup_worker = CleanupWorker(self)
        self.stats_election = LauncherStatsElection(self.zk_client)
        self.stats_thread = threading.Thread(target=self.runStatsElection)
        self.stats_thread.daemon = True

    def _layoutUpdatedCallback(self):
        self.layout_updated_event.set()
        self.wake_event.set()

    def _imageUpdatedCallback(self):
        self.image_updated_event.set()
        self.wake_event.set()

    def run(self):
        self.component_info.state = self.component_info.RUNNING
        self.log.debug("Launcher running")
        while self._running:
            loop_start = time.monotonic()
            try:
                with self._run_lock:
                    self._run()
            except Exception:
                self.log.exception("Error in main thread:")
            loop_duration = time.monotonic() - loop_start
            time.sleep(max(0, self.MAX_SLEEP - loop_duration))
            self.wake_event.wait(timeout=self.MIN_INTERVAL)
            self.wake_event.clear()

    def _run(self):
        if self.layout_updated_event.is_set():
            self.layout_updated_event.clear()
            with self.providers_update_lock:
                try:
                    updated = self.updateTenantProviders()
                except Exception:
                    self.layout_updated_event.set()
                    raise
            if updated:
                self.checkOldImages()
                self.checkMissingUploads()
        if self.image_updated_event.is_set():
            self.checkOldImages()
            self.checkMissingUploads()
        if self.upload_deleted_event.is_set():
            self.checkOldImages()
        self._processRequests()
        self._processNodes()
        self._processMinReady()

    def _processRequests(self):
        unassigned_nodes = self._getUnassignedNodes()
        for request in self.api.getMatchingRequests():
            log = get_annotated_logger(self.log, request, request=request.uuid)
            try:
                if not request.hasLock():
                    if ((request.state in request.FINAL_STATES) and
                        (request.event_state == request.EventState.COMPLETE)):
                        # Nothing to do here
                        continue
                    log.debug("Got request %s", request)
                    with self.createZKContext(None, log) as ctx:
                        if not request.acquireLock(ctx, blocking=False):
                            log.debug("Failed to lock matching request %s",
                                      request)
                            continue
                        request.refresh(ctx)

                    request_span = tracing.restoreSpan(request.span_info)
                    with trace.use_span(request_span):
                        request._set(_span=self.tracer.start_span(
                            "NodesetRequestProcessing"))

                if not self._cachesReadyForRequest(request):
                    self.log.debug("Caches are not up-to-date for %s", request)
                    continue
                self._processNodesetRequest(request, unassigned_nodes, log)
            except Exception:
                log.exception("Failed to process request %s", request)

    def _processNodesetRequest(self, request, unassigned_nodes, log):
        try:
            if request.state == model.NodesetRequest.State.REQUESTED:
                self._acceptRequest(request, log, unassigned_nodes)
            elif request.state == model.NodesetRequest.State.ACCEPTED:
                self._checkRequest(request, log, unassigned_nodes)
        except NodesetRequestError as err:
            state = model.NodesetRequest.State.FAILED
            log.error("Marking request %s as %s: %s",
                      request, state, str(err))
            with self.createZKContext(request._lock, log) as ctx:
                request.updateAttributes(ctx, state=state)
        except Exception:
            log.exception("Error processing request %s", request)
        if request.state in request.FINAL_STATES:
            # Note that we still hold the lock for a few more
            # instructions, but the schedulers or executors should
            # not care.
            event = model.NodesProvisionedEvent(
                request.uuid, request.buildset_uuid)
            self.result_events[request.tenant_name][
                request.pipeline_name].put(event)
            request._span.set_attributes(request.getSpanAttributes())
            request._span.end()
            with self.createZKContext(None, log) as ctx:
                request.updateAttributes(
                    ctx, event_state=request.EventState.COMPLETE)
                request.releaseLock(ctx)

    def _cachesReadyForRequest(self, request):
        # Make sure we have all associated provider nodes in the cache
        return all(
            self.api.getProviderNode(n)
            for n in request.nodes
        )

    def _filterUnassignedNodes(self, unassigned_nodes, request):
        request_unassigned_nodes = LabelNodeMapping()
        if request.image_upload_uuid:
            # This is a request for an image validation job.
            # Don't use any unassigned nodes for that.
            return request_unassigned_nodes
        providers = {p.canonical_name: p
                     for p in self.tenant_providers.get(request.tenant_name)}
        if not providers:
            return request_unassigned_nodes
        label_names = set(request.labels)
        for label_name in label_names:
            unassigned_for_label = unassigned_nodes.getNodesForLabel(
                label_name)
            if not unassigned_for_label:
                continue
            # TODO: sort by age? use old nodes first? random to reduce
            # chance of thundering herd?
            for node in unassigned_for_label:
                if node.request_id:
                    continue
                if node.state not in node.ASSIGNABLE_STATES:
                    continue
                if node.hasExpired():
                    continue
                # Check to see if this node is valid in this tenant
                if node.provider in providers:
                    request_unassigned_nodes.addNode(node)
        return request_unassigned_nodes

    def _assignUnassignedNode(self, ctx, unassigned_nodes,
                              request_unassigned_nodes, request,
                              label, provider, log):
        unassigned_for_label = request_unassigned_nodes.getNodesForLabel(
            label.name)
        # TODO: sort by age? use old nodes first? random to reduce
        # chance of thundering herd?
        for node in unassigned_for_label:
            if node.provider != provider.canonical_name:
                continue
            # We can assign a building node without
            # acquiring the lock.
            lock_node = not (node.state == node.State.BUILDING)
            if lock_node and not node.acquireLock(ctx, blocking=False):
                log.debug("Failed to lock matching unassigned node %s",
                          node)
                continue
            try:
                node.refresh(ctx)
                # Double check if it's still available
                if (node.request_id or
                    node.state not in node.ASSIGNABLE_STATES):
                    request_unassigned_nodes.getNode(node)
                    unassigned_nodes.getNode(node)
                    continue
                nearest_lock = (
                    lock_node and node._lock or request._lock
                )
                with self.createZKContext(
                        nearest_lock, self.log) as ctx:
                    try:
                        node.assign(
                            ctx,
                            request_id=request.uuid,
                            tenant_name=request.tenant_name,
                            min_request_version=request.getZKVersion() + 1,
                        )
                    except NodeExistsError:
                        continue
                request_unassigned_nodes.removeNode(node)
                unassigned_nodes.removeNode(node)
                log.debug("Assigned unassigned node %s", node.uuid)
                return node
            except Exception:
                log.exception(
                    "Faild to assign unassigned node %s", node)
                continue
            finally:
                if lock_node:
                    node.releaseLock(ctx)
        return None

    def _acceptRequest(self, request, log, unassigned_nodes):
        messages = []
        request_unassigned_nodes = self._filterUnassignedNodes(
            unassigned_nodes, request)
        # Create provider nodes for the requested labels
        label_providers = self._selectProviders(
            request, request_unassigned_nodes, messages)
        with (self.createZKContext(request._lock, log) as ctx,
              request.activeContext(ctx)):
            for i, (label, provider) in enumerate(label_providers):
                # If we crashed and are resuming, we may already have
                # a node for this slot.
                if i < len(request.provider_node_data):
                    continue
                node = self._assignUnassignedNode(
                    ctx, unassigned_nodes, request_unassigned_nodes,
                    request, label, provider, log)
                if node is None:
                    # We need to request a new node
                    if provider.driver.name == 'static':
                        return
                    # If we have not assigned any nodes to this
                    # request we don't have to proceed if there is no
                    # quota.
                    if not bool(request.provider_node_data):
                        try:
                            has_quota = self.doesProviderHaveQuotaForLabel(
                                provider, label, messages)
                        except Exception:
                            self.log.exception(
                                "Error checking quota for label %s "
                                "in provider %s", label, provider)
                            raise NodesetRequestError(
                                "Unable to determine quota")
                        if not has_quota:
                            # TODO: We may want to consider all the
                            # labels at once so that if we can
                            # fulfill any label immediately, we
                            # accept the request; currently this will
                            # happen only if we can fulfill the first
                            # label immediately.
                            return
                    self._emitMessages(log, messages)
                    node = self._requestNode(
                        label, request, provider, log, ctx)
                    log.debug("Requested node %s", node.uuid)
                request.addProviderNode(node)
            if request.provider_node_data:
                self._emitMessages(log, messages)
                log.debug("Accepting request %s", request)
                request.state = model.NodesetRequest.State.ACCEPTED

    def _shuffleProviders(self, providers):
        # This is to allow unit test control of this process.
        providers = providers[:]
        random.shuffle(providers)
        return providers

    def _selectProviders(self, request, request_unassigned_nodes, messages):
        providers = self.tenant_providers.get(request.tenant_name)
        if not providers:
            raise NodesetRequestError(
                f"No provider for tenant {request.tenant_name}")

        # Start with a randomized list of providers so that different
        # requests may have different behavior.
        providers = self._shuffleProviders(providers)

        # Sort that list by quota
        providers.sort(key=lambda p: self.getQuotaPercentage(p, messages))

        # Then if there are unassigned nodes, sort so that we can use the
        # most unassigned nodes.
        if request_unassigned_nodes:
            # provider -> total unassigned nodes
            usable_provider_unassigned_nodes = collections.Counter()
            for label_name in set(request.labels):
                for node in request_unassigned_nodes.getNodesForLabel(
                        label_name):
                    usable_provider_unassigned_nodes[node.provider] += 1
                    messages.append(
                        f"Found usable unassigned node {node} "
                        f"in {node.provider}")
            providers.sort(
                key=lambda p: usable_provider_unassigned_nodes[
                    p.canonical_name],
                reverse=True
            )

        # A list of providers that could work for all labels in the request
        ideal_providers_for_all_labels = set(providers)
        # The same, but omit providers with failures
        providers_for_all_labels = set(providers)
        # A list of providers for each label in the request, indexed
        # by label index number
        ideal_providers_for_label = {}
        # The same, but omit providers with failures
        providers_for_label = {}
        # Which providers are used by existing nodes in this request
        existing_provider_names = collections.Counter()

        for i, label_name in enumerate(request.labels):
            provider_failure_names = collections.Counter()
            pnd = request.getProviderNodeData(i)
            if pnd:
                provider_failure_names.update(pnd['failed_providers'])
                if pnd['uuid']:
                    existing_node = self.api.getProviderNode(pnd['uuid'])
                    # Special case for the current node just failed
                    if existing_node.state == existing_node.State.FAILED:
                        provider_failure_names[existing_node.provider] += 1
                    if existing_node.state not in existing_node.FAILED_STATES:
                        existing_provider_names[existing_node.provider] += 1
            candidate_providers = [
                p for p in providers
                if p.hasLabel(label_name)
                and (provider_failure_names[p.canonical_name] <
                     p.launch_attempts)
            ]
            ideal_providers_for_label[i] = []
            providers_for_label[i] = []
            for provider in providers:
                if not (label := provider.labels.get(label_name)):
                    continue
                image = provider.images[label.image]
                if (request.image_upload_uuid
                        and image.name in request.image_names):
                    image_cname = image.canonical_name
                    uploads = self.image_upload_registry.getUploadsForImage(
                        image_cname)
                    # Check that the given provider can actually supply
                    # the requested image.
                    valid_uploads = (
                        u for u in uploads
                        if provider.canonical_name in u.providers
                        and u.uuid == request.image_upload_uuid
                    )
                    if not any(valid_uploads):
                        continue
                # Check if the provider could possibly handle the
                # request based on quota but not current zuul usage.
                # TODO: consider the impact of a multi-node request
                # for the same label where that single request is
                # larger than the capacity.
                try:
                    if not self.doesProviderHaveQuotaForLabel(
                            provider, label, messages, include_usage=False):
                        continue
                except Exception:
                    self.log.exception(
                        "Error checking quota for label %s "
                        "in provider %s", label, provider)
                    continue
                ideal_providers_for_label[i].append(provider)
                if (provider_failure_names[provider.canonical_name]
                        >= provider.launch_attempts):
                    continue
                providers_for_label[i].append(provider)
            ideal_providers_for_all_labels &= set(ideal_providers_for_label[i])
            providers_for_all_labels &= set(providers_for_label[i])

        # If there is at least one provider that can provide all the
        # labels for this request, then we will ensure that we use the
        # same provider for all.
        if require_same_provider := bool(ideal_providers_for_all_labels):
            messages.append("Requiring same provider for all labels")

        # Turn the reduced set union of providers that work for all
        # labels back into an ordered list.
        providers_for_all_labels = [
            p for p in providers if p in providers_for_all_labels]

        main_provider_name = None
        main_provider = None
        most_common = existing_provider_names.most_common()
        if most_common:
            main_provider_name = most_common[0][0]
            messages.append(
                f"Using most common provider {main_provider_name} "
                f"from {most_common}")
        elif request.preferred_provider:
            main_provider_name = request.preferred_provider
            messages.append(
                f"Using requested provider {main_provider_name}")
        if main_provider_name:
            for main_provider in providers:
                if main_provider.canonical_name == main_provider_name:
                    break
            else:
                raise Exception(
                    f"Unable to find provider {main_provider_name}")
        elif providers_for_all_labels:
            main_provider = providers_for_all_labels[0]
            messages.append(
                "Using first provider "
                f"for all labels {main_provider.canonical_name} "
                f"from {providers_for_all_labels}")

        label_providers = []
        for i, label_name in enumerate(request.labels):
            if require_same_provider:
                candidate_providers = providers_for_all_labels
            else:
                candidate_providers = providers_for_label[i]
            if not candidate_providers:
                raise NodesetRequestError(
                    f"No provider found for label {label_name}")
            if main_provider in candidate_providers:
                provider = main_provider
                messages.append(
                    f"Selected request main provider {provider} "
                    f"for label {label_name} index {i} from "
                    f"candidate providers: {candidate_providers}")
            else:
                provider = candidate_providers[0]
                messages.append(
                    f"Selected provider {provider} "
                    f"for label {label_name} index {i} from "
                    f"candidate providers: {candidate_providers}")
            label = provider.labels[label_name]
            label_providers.append((label, provider))

        return label_providers

    def _registerStaticNodes(self, provider):
        # Register static nodes; we do this here rather than the
        # driver so that we can reuse _requestNode.
        with self.createZKContext(None, self.log) as ctx:
            for static_node in provider.nodes.values():
                node = self.api.getProviderNode(static_node.node_id)
                if node:
                    continue
                label = provider.labels[static_node.label]
                try:
                    self._requestNode(label, None, provider, self.log, ctx,
                                      static_node)
                except NodeExistsError:
                    pass

    def _deRegisterStaticNodes(self, provider):
        with self.createZKContext(None, self.log) as ctx:
            for node in self.api.getProviderNodes():
                if provider.canonical_name != node.provider:
                    continue
                if node.uuid in provider.nodes:
                    continue
                self.log.info("Deregistering %s", node)
                self._bestEffortLockAndSetNodeState(
                    ctx, node, node.State.DEREGISTER, self.log)

    def _requestNode(self, label, request, provider, log, ctx,
                     static_node=None):
        image = provider.images[label.image]
        label_quota = provider.getQuotaForLabel(label)
        endpoint_name = provider.getEndpoint().name
        if static_node:
            node_uuid = static_node.node_id
            connection_port = static_node.connection_port
            state = model.ProviderNode.State.BUILDING
            label_aliases = static_node.aliases
        else:
            node_uuid = uuid.uuid4().hex
            connection_port = image.connection_port
            state = model.ProviderNode.State.REQUESTED
            label_aliases = []
        args = dict(
            uuid=node_uuid,
            state=state,
            label=label.name,
            label_config_hash=label.config_hash,
            label_aliases=label_aliases,
            max_ready_age=label.max_ready_age,
            max_age=label.max_age,
            min_retention_time=label.min_retention_time,
            host_key_checking=label.host_key_checking,
            boot_timeout=label.boot_timeout,
            snapshot_timeout=label.snapshot_timeout,
            snapshot_expiration=label.snapshot_expiration,
            executor_zone=label.executor_zone,
            connection_name=provider.connection_name,
            # Set any node attributes we already know here
            connection_port=connection_port,
            connection_type=image.connection_type,
            username=image.username,
            python_path=image.python_path,
            shell_type=image.shell_type,
            quota=label_quota,
            provider=provider.canonical_name,
            endpoint_name=endpoint_name,
        )

        # Save a copy of the args here since nothing below should be
        # set on subnodes.
        subnode_args = args.copy()
        request_args = {}
        assign_args = {}
        if request is not None:
            with trace.use_span(request._span):
                node_span = self.tracer.start_span("ProviderNode")
            request_args['tags'] = provider.getNodeTags(
                self.system.system_id, label, node_uuid, provider, request)
            request_args['span_info'] = tracing.getSpanInfo(node_span)
            request_args['zuul_event_id'] = request.zuul_event_id
            request_args['request_time'] = request.request_time
            request_args['image_upload_uuid'] = request.image_upload_uuid
            assign_args['request_id'] = request.uuid
            assign_args['min_request_version'] = request.getZKVersion() + 1
            assign_args['tenant_name'] = request.tenant_name
        else:
            args['tags'] = provider.getNodeTags(
                self.system.system_id, label, node_uuid)
        if not (label.slots and label.slots > 1):
            args.update(request_args)
        node_class = provider.driver.getProviderNodeClass()
        node = node_class.new(ctx, **args)
        log.debug("Requested node %s", node)
        subnodes = []
        retnode = node

        if label.slots and label.slots > 1:
            for slot in range(max(label.slots, 0)):
                if slot == 0:
                    merged_subnode_args = {}
                    merged_subnode_args.update(subnode_args)
                    merged_subnode_args.update(request_args)
                else:
                    merged_subnode_args = subnode_args
                subnode = self._createSubNode(
                    ctx, node, merged_subnode_args, slot)
                log.debug("Requested subnode %s for node %s", subnode, node)
                subnodes.append(subnode.uuid)
                if slot == 0:
                    retnode = subnode
                    if request is not None:
                        # Assign the sub node
                        subnode.assign(ctx, **assign_args)
            node.updateAttributes(
                ctx,
                subnodes=subnodes,
            )
        elif request is not None:
            # Assign the main (only) node
            node.assign(ctx, **assign_args)
        return retnode

    def _createSubNode(self, context, node, args, slot):
        # Get a new uuid for the subnode
        args['uuid'] = uuid.uuid4().hex
        # Subnodes use no quota
        args['quota'] = model.QuotaInformation()
        args['state'] = node.state
        subnode = SubnodeProviderNode.new(
            context,
            state_time=node.state_time,
            slot=slot,
            main_node_id=node.uuid,
            # A fake create state machine since we follow the main
            # node.
            create_state_machine=SubnodeStateMachine(),
            **args,
        )
        return subnode

    def _checkRequest(self, request, log, unassigned_nodes):
        messages = []
        requested_nodes = [self.api.getProviderNode(node_id) for
                           node_id in request.nodes]
        request_unassigned_nodes = self._filterUnassignedNodes(
            unassigned_nodes, request)
        if any(n.state in n.FAILED_STATES for n in requested_nodes):
            # If any nodes failed, see if we need to change providers
            # for any or all of them.
            label_providers = self._selectProviders(
                request, request_unassigned_nodes, messages)
            for i, node in enumerate(requested_nodes):
                label, provider = label_providers[i]
                if (node.state in node.FAILED_STATES or
                    node.state == node.State.HOLD or
                    provider.canonical_name != node.provider):
                    # We're either retrying this node or we're
                    # changing providers.
                    self._emitMessages(log, messages)
                    log.info(
                        "Retrying node for label %s index %s with provider %s",
                        label, i, provider)
                    with self.createZKContext(request._lock, log) as ctx:
                        if node.state == node.State.FAILED:
                            add_failed_provider = node.provider
                        else:
                            add_failed_provider = None

                        node = self._assignUnassignedNode(
                            ctx, unassigned_nodes, request_unassigned_nodes,
                            request, label, provider, log)
                        if node is None:
                            if provider.driver.name == 'static':
                                return
                            node = self._requestNode(
                                label, request, provider, log, ctx)
                        requested_nodes[i] = node
                        # if this was a tempfail, retry again as
                        # normal; otherwise, add to the failed
                        # providers list in the request
                        with request.activeContext(ctx):
                            request.updateProviderNode(
                                i, node,
                                add_failed_provider=add_failed_provider,
                            )

        if not all(n.state in n.FINAL_STATES for n in requested_nodes):
            return
        log.debug("Request %s nodes ready: %s", request, requested_nodes)
        failed = not all(n.state in (model.ProviderNode.State.READY,
                                     model.ProviderNode.State.SLOT_HOST)
                         for n in requested_nodes)
        # TODO:
        # * gracefully handle node failures (retry with another connection)
        # * deallocate ready nodes from the request if finally failed

        state = (model.NodesetRequest.State.FAILED if failed
                 else model.NodesetRequest.State.FULFILLED)
        log.debug("Marking request %s as %s", request, state)
        with self.createZKContext(request._lock, log) as ctx:
            request.updateAttributes(ctx, state=state)

    def _processNodes(self):
        for node in self.api.getMatchingProviderNodes():
            log = get_annotated_logger(self.log, node, request=node.request_id)
            try:
                self._processNodeSnapshot(node, log)
            except Exception:
                log.exception("Error processing node %s snapshot", node)

            try:
                if not node.hasLock():
                    if not self._isNodeActionable(node):
                        continue

                    with self.createZKContext(None, log) as ctx:
                        if not node.acquireLock(ctx, blocking=False):
                            log.debug("Failed to lock matching node %s", node)
                            continue
                        node.refresh(ctx)
                self._processProviderNode(node, log)
            except Exception:
                log.exception("Failed to process node %s", node)

    def _processProviderNode(self, node, log):
        request = self.api.getNodesetRequest(node.request_id)
        if ((request or node.request_id is None)
                and node.state in node.CREATE_STATES):
            try:
                self._checkNodeCreate(node, log)
            except exceptions.QuotaException as e:
                state = node.State.TEMPFAILED
                log.info("Marking node %s as %s due to %s", node, state, e)
                with self.createZKContext(node._lock, self.log) as ctx:
                    with node.activeContext(ctx):
                        node.setState(state)
                        self._setSubnodeStates(node, log)
                    self.wake_event.set()
            except Exception as e:
                state = node.State.FAILED
                if isinstance(e, LaunchTimeoutError):
                    log.error("Marking node %s as %s", node, state)
                else:
                    log.exception("Marking node %s as %s", node, state)
                with self.createZKContext(node._lock, self.log) as ctx:
                    with node.activeContext(ctx):
                        node.setState(state)
                        self._setSubnodeStates(node, log)
                    self.wake_event.set()

            # Export span for node creation phase
            if (node.state in node.FINAL_STATES and
                    node.create_state_machine):
                with trace.use_span(tracing.restoreSpan(node.span_info)):
                    with self.tracer.start_as_current_span(
                        "ProviderNodeCreate",
                        start_time=node.create_state_machine.start_time,
                        attributes=node.getSpanAttributes()
                    ):
                        # Nothing to do here; ctx manager is just
                        # a shorthand for span start/end. The
                        # start time is dated back to the start
                        # of the create state machine.
                        pass

        # Another launcher may have assigned our node to a request we
        # haven't seen yet; make sure the request cache is up to date.
        if (node.request_id and
            node.min_request_zxid and
            request is None):
            if self.zk_client.cap_watched_event_zxid:
                if self.api.requests_cache.max_zxid < node.min_request_zxid:
                    # Wait for the cache to update and try again
                    self.wake_event.set()
                    return
            else:
                # TODO: remove this when 3.9 is mandatory
                if node.state in node.ASSIGNABLE_STATES:
                    self.log.debug("Waiting for request cache to sync "
                                   "for request %s and node %s", request, node)
                    self.api.requests_cache.waitForSync()
                    request = self.api.getNodesetRequest(node.request_id)

        # Mark outdated nodes w/o a request for cleanup when ...
        if not request and (
                # ... it expired
                node.hasExpired() or
                # ... we are sure that our providers are up-to-date
                # and we can't find a provider for this node.
                (not self.layout_updated_event.is_set()
                 and not self._hasProvider(node))):
            state = node.State.OUTDATED
            log.debug("Marking node %s as %s", node, state)
            with self.createZKContext(node._lock, self.log) as ctx:
                with node.activeContext(ctx):
                    node.setState(state)

        # Deallocate node w/o a request for re-use
        elif (node.request_id and not request and
              node.state in node.ASSIGNABLE_STATES):
            log.debug("Deallocating node %s from missing request %s",
                      node, node.request_id)
            with self.createZKContext(node._lock, self.log) as ctx:
                node.unassign(ctx)

        # Recycle a node if the label allows reuse
        elif (node.request_id and not request
                and node.state == node.State.USED):
            if provider := self._getProviderForNode(node):
                if label := provider.labels.get(node.label):
                    if self._canReuseNode(provider, node, label):
                        self._checkNodeReuse(node, log)
                        if node.state == node.State.USED:
                            # If we haven't changed state, keep the
                            # lock and check back later.
                            return

        # Clean up the node if ...
        if (
            # ... it is associated with a request that no
            # longer exists
            (node.request_id is not None and not request)
            # ... it is failed/outdated
            or node.state in (
                node.State.FAILED,
                node.State.TEMPFAILED,
                node.State.SLOT_HOST,
                node.State.OUTDATED,
                node.State.DEREGISTER,
                node.State.USED,
            )
        ):
            # Safety check to make sure we don't delete main nodes if
            # subnodes are in use.
            ok = True
            for subnode_id in node.subnodes:
                if subnode := self.api.getProviderNode(subnode_id):
                    if subnode.state not in (subnode.State.REQUESTED,
                                             subnode.State.BUILDING):
                        log.debug("Not deleting node %s due to subnode %s",
                                  node, subnode)
                        ok = False
                        break
            if ok:
                try:
                    self._cleanupNode(node, log)
                except Exception:
                    log.exception("Error in node cleanup")
                    self.wake_event.set()

        if node.state == model.ProviderNode.State.READY:
            with self.createZKContext(None, self.log) as ctx:
                node.releaseLock(ctx)

    def _isNodeActionable(self, node):
        if node.is_locked:
            return False

        # If we are a subnode and our main node is in requested or
        # building, then we should not act.
        if isinstance(node, SubnodeProviderNode):
            if main_node := self.api.getProviderNode(node.main_node_id):
                if main_node.state in (
                        node.State.REQUESTED, node.State.BUILDING):
                    return False

        # If we have subnodes, then they are "live" and we should not
        # do anything with the node until they are deleted.  We went
        # into the pending-delete state because we're ready to delete
        # but just waiting on the subnodes.
        if node.state == node.State.SLOT_HOST:
            for subnode_id in node.subnodes:
                if self.api.getProviderNode(subnode_id):
                    return False
            # A slot host with no nodes should be deleted
            return True

        if node.state in node.LAUNCHER_STATES:
            return True

        if node.state == node.State.HOLD:
            if node.hasHoldExpired():
                return True
            return False

        if node.request_id:
            request_exists = bool(self.api.getNodesetRequest(node.request_id))
            return not request_exists
        elif node.hasExpired():
            return True
        elif not self._hasProvider(node):
            if self.layout_updated_event.is_set():
                # If our providers are not up-to-date we can't be sure
                # there is no provider for this node.
                return False
            # We no longer have a provider that uses the given node
            return True

        return False

    def _emitMessages(self, log, messages):
        for m in messages:
            log.debug(m)
        messages.clear()

    def _checkNodeCreate(self, node, log):
        if isinstance(node, SubnodeProviderNode):
            return
        # Log messages that we will only emit if they are interesting
        messages = []
        with self.createZKContext(node._lock, self.log) as ctx:
            with node.activeContext(ctx):
                provider = self._getProviderForNode(node)
                if not node.create_state_machine:
                    log.debug("Building node %s", node)
                    image_external_id = self.getImageExternalId(
                        node, provider)
                    log.debug("Node %s external id %s",
                              node, image_external_id)
                    node.create_state_machine =\
                        provider.getCreateStateMachine(
                            node, image_external_id, log)

                old_state = node.create_state_machine.state
                if old_state == node.create_state_machine.START:
                    if not self.doesProviderHaveQuotaForNode(
                            provider, node, messages):
                        # Consider this provider paused, don't attempt
                        # to create the node until we think we have
                        # quota.
                        self.wake_event.set()
                        return
                    # Start the clock for launch-timeout from this point.
                    self._emitMessages(log, messages)
                    node.create_state_machine.start_time = time.time()
                instance = node.create_state_machine.advance()
                new_state = node.create_state_machine.state
                if old_state != new_state:
                    log.debug("Node %s advanced from %s to %s",
                              node, old_state, new_state)
                if (new_state != node.create_state_machine.START and
                    node.state == node.State.REQUESTED):
                    # Set state to building so that we start counting
                    # quota.
                    node.setState(node.State.BUILDING)
                    self._setSubnodeStates(node, log)

                if not node.create_state_machine.complete:
                    if provider.launch_timeout:
                        now = time.time()
                        if (now - node.create_state_machine.start_time >
                            provider.launch_timeout):
                            log.error("Timeout creating node %s", node)
                            raise LaunchTimeoutError("Timeout creating node")
                    self.wake_event.set()
                    return
                elif node.nodescan_request is None:
                    # We just finished the create state machine,
                    # update with new info.
                    self._updateNodeFromInstance(node, instance)

                # Note this method has the side effect of updating
                # node info from the instance.
                if self._checkNodescanRequest(node, log):
                    if node.subnodes:
                        state = node.State.SLOT_HOST
                    else:
                        state = node.State.READY
                    self._setSubnodeStates(node, log, state)
                    node.setState(state)
                    node.nodescan_request = None
                    log.debug("Marking node %s as %s", node, node.state)
                    self.wake_event.set()
                else:
                    self.wake_event.set()
                    return
            node.releaseLock(ctx)

    def _setSubnodeStates(self, node, log, state=None):
        if not node.subnodes:
            return
        state = state or node.state
        new_state = state
        for subnode_id in node.subnodes:
            if subnode := self.api.getProviderNode(subnode_id):
                with subnode.activeContext(node._active_context):
                    if state == node.State.SLOT_HOST:
                        # This is the initial creation, so we copy
                        # data and set the state to READY.
                        subnode.updateFromMainNode(node)
                        new_state = node.State.READY
                    subnode.setState(new_state)
                log.debug("Marking subnode %s as %s",
                          subnode, subnode.state)

    def _setReadySubnodeStates(self, context, node, log):
        # Set the state of any READY subnodes to the main node state.
        # This happens when we detect a global node error on one
        # subnode after the initial creation.
        if not node.subnodes:
            return
        state = node.state
        for subnode_id in node.subnodes:
            if subnode := self.api.getProviderNode(subnode_id):
                if subnode.state != subnode.State.READY:
                    continue
                if subnode.request_id:
                    continue
                self._bestEffortLockAndSetNodeState(
                    context, subnode, state, log)

    def _bestEffortLockAndSetNodeState(self, context, node, state, log):
        # This is for subnodes to attempt to interact with the main
        # node or each other; we try to lock the node and update the
        # state, but we ignore lock errors.
        if node.hasLock():
            # This node is assigned to this launcher and we have the lock.
            lock_mgr = nullcontext()
        else:
            lock_mgr = node.locked(context, blocking=False)
        try:
            with lock_mgr:
                with self.createZKContext(node._lock, log) as ctx:
                    with node.activeContext(ctx):
                        node.setState(state)
                        log.debug("Marking node %s as %s",
                                  node, node.state)
        except LockException:
            pass

    def _checkNodescanRequest(self, node, log):
        if node.nodescan_request is None:
            node.nodescan_request = NodescanRequest(node, log)
            self.nodescan_worker.addRequest(node.nodescan_request)
            log.debug(
                "Submitted nodescan request for %s queue length %s",
                node.interface_ip,
                self.nodescan_worker.length())
        if not node.nodescan_request.complete:
            return False
        try:
            keys = node.nodescan_request.result()
        except Exception as e:
            if isinstance(e, exceptions.ConnectionTimeoutException):
                log.warning("Error scanning keys: %s", str(e))
            else:
                log.exception("Exception scanning keys:")
            raise exceptions.LaunchKeyscanException(
                "Can't scan key for %s" % (node,))
        if keys:
            node.host_keys = keys
        return True

    def _canReuseNode(self, provider, node, label):
        if not label.reuse:
            return False
        if node.main_node_id:
            main_node = self.api.getProviderNode(node.main_node_id)
        else:
            main_node = node
        if not provider.canReuseNode(main_node):
            return False
        return True

    def _checkNodeReuse(self, node, log):
        # Handle re-processing a node for reuse
        if node.main_node_id:
            main_node = self.api.getProviderNode(node.main_node_id)
        else:
            main_node = None
        with self.createZKContext(node._lock, log) as ctx:
            with node.activeContext(ctx):
                try:
                    if main_node and main_node.state == main_node.State.FAILED:
                        raise Exception("Main node is failed")
                    done = self._checkNodescanRequest(node, log)
                except Exception:
                    state = node.State.FAILED
                    log.exception("Marking node %s as %s", node, state)
                    node.nodescan_request = None
                    node.setState(state)
                    # If we are a subnode, then declare the main node
                    # and any READY subnodes as failed as well.
                    if main_node:
                        self._bestEffortLockAndSetNodeState(
                            ctx, main_node, state, log)
                        self._setReadySubnodeStates(ctx, main_node, log)
                    return

                if not done:
                    self.wake_event.set()
                    return

                state = node.State.READY
                log.debug("Marking node %s as %s for reuse",
                          node, state)
                node.unassign(ctx)
                node.setState(state)

    def _processNodeSnapshot(self, node, log):
        if node.state != node.State.SNAPSHOT:
            return
        snapshot = node.snapshot
        if snapshot.complete:
            return
        # This is similar to processNode
        if not snapshot.hasLock():
            with self.createZKContext(None, log) as ctx:
                if not snapshot.acquireLock(ctx, blocking=False):
                    log.debug("Failed to lock node %s for snapshot", node)
                    return
                snapshot.refresh(ctx)

        try:
            self._checkNodeSnapshot(node, log)
        except Exception:
            log.exception("Error processing node %s snapshot", node)
            with self.createZKContext(snapshot._lock, self.log) as ctx:
                with snapshot.activeContext(ctx):
                    snapshot.complete = True
                    self.wake_event.set()

        if snapshot.complete:
            with self.createZKContext(None, self.log) as ctx:
                snapshot.releaseLock(ctx)

    def _checkNodeSnapshot(self, node, log):
        snapshot = node.snapshot
        with self.createZKContext(snapshot._lock, self.log) as ctx:
            with snapshot.activeContext(ctx):
                provider = self._getProviderForNode(node)
                if not snapshot.state_machine:
                    tags = copy.deepcopy(node.tags)
                    tags.pop('zuul_node_uuid', None)
                    exp = int(time.time() + node.snapshot_expiration)
                    tags['zuul_expiration'] = str(exp)
                    snapshot.tags = tags
                    log.debug("Snapshotting node %s", node)
                    snapshot.state_machine = provider.getSnapshotStateMachine(
                        node, log)

                old_state = snapshot.state_machine.state
                if old_state == snapshot.state_machine.START:
                    snapshot.state_machine.start_time = time.time()
                external_id = snapshot.state_machine.advance()
                new_state = snapshot.state_machine.state
                if old_state != new_state:
                    log.debug("Snapshot for %s advanced from %s to %s",
                              node, old_state, new_state)

                if not snapshot.state_machine.complete:
                    if node.snapshot_timeout:
                        now = time.time()
                        if (now - snapshot.state_machine.start_time >
                            node.snapshot_timeout):
                            log.error("Timeout snapshotting node %s", node)
                            raise Exception("Timeout snapshotting node")
                    self.wake_event.set()
                    return
                # Snapshot is complete
                snapshot.complete = True
                snapshot.external_id = external_id
                snapshot.releaseLock(ctx)

    def _cleanupNode(self, node, log):
        with self.createZKContext(node._lock, self.log) as ctx:
            with node.activeContext(ctx):
                self.nodescan_worker.removeRequest(node.nodescan_request)
                node.nodescan_request = None

                if not node.delete_state_machine:
                    log.debug("Cleaning up node %s", node)
                    provider = self._getProviderForNode(
                        node, ignore_label=True)
                    # The static driver may choose to reuse the node
                    if (provider.driver.name != 'static' and
                        isinstance(node, SubnodeProviderNode)):
                        node.delete_state_machine = SubnodeStateMachine()
                    else:
                        node.delete_state_machine =\
                            provider.getDeleteStateMachine(node, log)

                old_state = node.delete_state_machine.state

                now = time.time()
                if (now - node.delete_state_machine.start_time >
                    self.DELETE_TIMEOUT):
                    log.error("Timeout deleting node %s", node)
                    node.delete_state_machine.fail()

                if not node.delete_state_machine.complete:
                    try:
                        node.delete_state_machine.advance()
                        new_state = node.delete_state_machine.state
                        if old_state != new_state:
                            log.debug("Node %s advanced from %s to %s",
                                      node, old_state, new_state)
                    except Exception:
                        log.exception("Error in delete state machine:")
                        node.delete_state_machine.fail()

                if not node.delete_state_machine.complete:
                    self.wake_event.set()
                    return

                if node.state == node.State.BUILDING:
                    # Static node going back into the pool
                    node.delete_state_machine = None
                    return

            request = self.api.getNodesetRequest(node.request_id)
            request_up_to_date = False
            if request is not None:
                request_version = request.getZKVersion()
                if node.min_request_version is None:
                    request_up_to_date = True
                elif (request_version is not None and
                      request_version >= node.min_request_version):
                    request_up_to_date = True
            if (request is None or
                (request_up_to_date and
                 node.uuid not in request.nodes)):
                # We can clean up the node if:
                # The request is gone
                # The request exists and our copy is sufficiently new and:
                #   The request no longer references this node
                log.debug("Removing provider node %s", node)
                node.delete(ctx)
                node.releaseLock(ctx)
                # Export provider node spans
                node_span = tracing.restoreSpan(node.span_info)
                with trace.use_span(node_span, end_on_exit=True):
                    node_span.set_attributes(node.getSpanAttributes())
                    if node.delete_state_machine:
                        with self.tracer.start_as_current_span(
                            "ProviderNodeDelete",
                            start_time=node.delete_state_machine.start_time,
                            attributes=node.getSpanAttributes()
                        ):
                            # Nothing to do here; ctx manager is just
                            # a shorthand for span start/end. The
                            # start time is dated back to the start
                            # of the create state machine.
                            pass

    def _processMinReady(self):
        if not self.api.nodes_cache.waitForSync(
                timeout=self.CACHE_SYNC_TIMEOUT):
            self.log.warning("Timeout waiting %ss for node cache to sync",
                             self.CACHE_SYNC_TIMEOUT)
            return

        for label, provider in self._getMissingMinReadySlots():
            with self.createZKContext(None, self.log) as ctx:
                node = self._requestNode(label, None, provider, self.log, ctx)
                self.log.debug("Created min-ready node %s via provider %s",
                               node, provider)

    def _getMissingMinReadySlots(self):
        candidate_launchers = {
            c.hostname: c for c in COMPONENT_REGISTRY.registry.all("launcher")}
        candidate_names = set(candidate_launchers.keys())
        label_scores = DefaultKeyDict(
            lambda lcn: scores_for_label(lcn, candidate_names))

        # Collect min-ready labels that we need to process
        tenant_labels = collections.defaultdict(
            lambda: collections.defaultdict(list))
        for tenant_name, tenant_providers in self.tenant_providers.items():
            for tenant_provider in tenant_providers:
                for label in tenant_provider.labels.values():
                    if not label.min_ready:
                        continue
                    # Check if this launcher is responsible for
                    # spawning min-ready nodes for this label.
                    if not self._hasHighestMinReadyScore(
                            label.canonical_name,
                            label_scores,
                            candidate_launchers):
                        continue
                    # We collect all label variants to determin if
                    # min-ready is satisfied based on the config hashes
                    tenant_labels[tenant_name][label.name].append(label)

        unassigned_hashes = self._getUnassignedNodeLabelHashes()
        for tenant_name, min_ready_labels in tenant_labels.items():
            for label_name, labels in min_ready_labels.items():
                valid_label_hashes = set(lbl.config_hash for lbl in labels)
                tenant_min_ready = sum(
                    1 for h in unassigned_hashes[label_name]
                    if h in valid_label_hashes
                )
                label_providers = [
                    p for p in self.tenant_providers[tenant_name]
                    if p.hasLabel(label_name)
                ]
                for _ in range(tenant_min_ready, labels[0].min_ready):
                    provider = random.choice(label_providers)
                    label = provider.labels[label_name]
                    yield label, provider
                    unassigned_hashes[label.name].append(label.config_hash)

    def _hasHighestMinReadyScore(
            self, label_cname, label_scores, candidate_launchers):
        scores = sorted(label_scores[label_cname].items())
        for score, launcher_name in scores:
            launcher = candidate_launchers.get(launcher_name)
            if not launcher:
                continue
            if launcher.state != launcher.RUNNING:
                continue
            if (launcher.hostname
                    == self.component_info.hostname):
                return True
            return False
        return False

    def _getUnassignedNodeLabelHashes(self):
        unassigned_nodes = collections.defaultdict(list)
        for node in self.api.getProviderNodes():
            if node.request_id is not None:
                continue
            unassigned_nodes[node.label].append(node.label_config_hash)
        return unassigned_nodes

    def _getUnassignedNodes(self):
        # Get unassigned ready nodes and also building nodes.  We can
        # have unassigned building nodes if a request is canceled, or
        # in the case of subnodes.
        unassigned_nodes = LabelNodeMapping()
        nodes = []
        for node in self.api.getProviderNodes():
            if node.request_id is not None:
                continue
            if node.subnodes:
                continue
            if node.state not in node.ASSIGNABLE_STATES:
                continue
            nodes.append(node)

        # Sort by state first, then time.  We will add these to the
        # LabelNodeMapping in this order, so it will be sorted as
        # well.
        def _sort_key(n):
            return n.ASSIGNABLE_STATES[n.state], n
        nodes.sort(key=_sort_key)

        for node in nodes:
            unassigned_nodes.addNode(node)

        return unassigned_nodes

    def _getProviderForNode(self, node, ignore_label=False):
        # Common case when a node is assigned to a provider
        if node.provider:
            return self._getProviderByCanonicalName(node.provider)

        # Fallback for min-ready nodes w/o a assigned provider
        for provider in self._getProviders():
            if provider.connection_name != node.connection_name:
                continue
            if ignore_label:
                return provider
            if not (label := provider.labels.get(node.label)):
                continue
            if label.config_hash == node.label_config_hash:
                return provider
        raise ProviderNodeError(f"Unable to find provider for node {node}")

    def _updateNodeFromInstance(self, node, instance):
        if instance is None:
            return

        # TODO:
        # if (pool.use_internal_ip and
        #     (instance.private_ipv4 or instance.private_ipv6)):
        #     server_ip = instance.private_ipv4 or instance.private_ipv6
        # else:
        server_ip = instance.interface_ip

        node.interface_ip = server_ip
        node.public_ipv4 = instance.public_ipv4
        node.private_ipv4 = instance.private_ipv4
        node.public_ipv6 = instance.public_ipv6
        node.private_ipv6 = instance.private_ipv6
        node.host_id = instance.host_id
        node.cloud = instance.cloud
        node.region = instance.region
        node.az = instance.az
        node.driver_data = instance.driver_data

        # If we did not know the resource information before
        # launching, update it now.
        node.quota = instance.getQuotaInformation()

        # Optionally, if the node has updated values that we set from
        # the image attributes earlier, set those.
        for attr in ('username', 'python_path', 'shell_type',
                     'connection_port', 'connection_type',
                     'host_keys'):
            if hasattr(instance, attr):
                setattr(node, attr, getattr(instance, attr))

        # As a special case for metastatic, if we got node_attributes
        # from the backing driver, use them as default values and let
        # the values from the pool override.
        instance_node_attrs = getattr(instance, 'node_attributes', None)
        if instance_node_attrs is not None:
            attrs = instance_node_attrs.copy()
            if node.attributes:
                attrs.update(node.attributes)
            node.attributes = attrs

    def _getProvider(self, tenant_name, provider_name):
        for provider in self.tenant_providers[tenant_name]:
            if provider.name == provider_name:
                return provider
        raise ProviderNodeError(
            f"Unable to find {provider_name} in tenant {tenant_name}")

    def _getProviders(self):
        # The same provider can behave differently in different
        # tenants because, for example, different image definitions
        # may appear in the different tenants.  This method will
        # return all of the providers.
        for providers in self.tenant_providers.values():
            for provider in providers:
                yield provider

    def _getUniqueProviders(self):
        # Sometimes (eg, when generating stats) we may not care about
        # the differences between the same provider in different
        # tenants.  This returns unique providers.
        seen = set()
        for providers in self.tenant_providers.values():
            for provider in providers:
                if provider.canonical_name not in seen:
                    yield provider
                    seen.add(provider.canonical_name)

    def _hasProvider(self, node):
        try:
            self._getProviderForNode(node)
        except ProviderNodeError:
            return False
        return True

    def _getProviderByCanonicalName(self, provider_cname):
        # Note this scans all tenants, and identical providers in
        # different tenants may have different python objects, so only
        # use this method if the call site doesn't care which
        # tenant-provider object is returned.
        for tenant_providers in self.tenant_providers.values():
            for provider in tenant_providers:
                if provider.canonical_name == provider_cname:
                    return provider
        raise Exception(f"Unable to find {provider_cname}")

    def _cleanTempdir(self):
        try:
            for f in os.listdir(self.temp_dir):
                path = os.path.join(self.temp_dir, f)
                if os.path.isfile(path):
                    try:
                        os.unlink(path)
                    except Exception:
                        self.log.exception("Error deleting %s", path)
                    self.log.info("Deleted %s", path)
        except Exception:
            self.log.exception("Error cleanning temp dir")

    def start(self):
        self.log.debug("Cleaning up temp dir")
        self._cleanTempdir()
        self.log.debug("Starting command processor")
        self._command_running = True
        self.command_socket.start()
        self.command_thread = threading.Thread(
            target=self.runCommand, name="command")
        self.command_thread.daemon = True
        self.command_thread.start()

        self.log.debug("Starting nodescan worker")
        self.nodescan_worker.start()

        self.log.debug("Starting launcher thread")
        self.launcher_thread.start()

        if self._start_cleanup:
            self.log.debug("Starting cleanup thread")
            self.cleanup_worker.start()

        self.stats_thread.start()

    def stop(self):
        self.log.debug("Stopping launcher")
        self._running = False
        self.stop_event.set()
        self.wake_event.set()
        self.component_info.state = self.component_info.STOPPED
        self.stopRepl()
        self._command_running = False
        self.command_socket.stop()
        self.log.debug("Stopping stats thread")
        self.stats_election.cancel()
        self.stats_thread.join()
        self.cleanup_worker.stop()
        self.cleanup_worker.join()
        self.upload_executor.shutdown()
        self.endpoint_import_executor.shutdown()
        self.endpoint_upload_executor.shutdown()
        self.nodescan_worker.stop()
        self.connections.stop()
        self.monitoring_server.stop()
        # Endpoints are stopped by drivers
        self.log.debug("Stopped launcher")

    def join(self):
        self.log.debug("Joining launcher")
        self.launcher_thread.join()
        # Don't set the join event until after the main thread is
        # joined because doing so will terminate the ZKContext.
        self.join_event.set()
        self.api.stop()
        self.zk_client.disconnect()
        self.tracing.stop()
        self.nodescan_worker.join()
        self.monitoring_server.join()
        self.log.debug("Joined launcher")

    def runCommand(self):
        while self._command_running:
            try:
                command, args = self.command_socket.get()
                if command != '_stop':
                    self.command_map[command](*args)
            except Exception:
                self.log.exception("Exception while processing command")

    def startRepl(self):
        if self.repl:
            return
        self.repl = zuul.lib.repl.REPLServer(self)
        self.repl.start()

    def stopRepl(self):
        if not self.repl:
            return
        self.repl.stop()
        self.repl = None

    def createZKContext(self, lock, log):
        return ZKContext(self.zk_client, lock, self.join_event, log,
                         default_lock_identifier=self.component_info.hostname)

    def _chooseLauncherForEndpoint(self, endpoint):
        all_launchers = {
            c.hostname: c for c in COMPONENT_REGISTRY.registry.all("launcher")}
        candidate_launchers = {
            n: c for n, c in all_launchers.items()
            if not c.connection_filter
            or endpoint.connection.connection_name in c.connection_filter}
        candidate_names = set(candidate_launchers)
        lscores = {mmh3.hash(n, signed=False): n
                   for n in candidate_names}
        launcher_scores = sorted(lscores.items())
        return launcher_scores[0][1]

    def updateTenantProviders(self):
        # We need to handle new and deleted tenants, so we need to
        # process all tenants currently known and the new ones.
        tenant_names = set(self.tenant_providers.keys())
        tenant_names.update(self.tenant_layout_state)

        endpoints = {}
        updated = False
        for tenant_name in tenant_names:
            # Reload the tenant if the layout changed.
            if self._updateTenantProviders(tenant_name):
                updated = True

        seen_endpoint_names = set()
        seen_provider_names = set()
        if updated:
            for providers in self.tenant_providers.values():
                for provider in providers:
                    if (self.connection_filter and
                        provider.connection_name not in
                        self.connection_filter):
                        continue
                    endpoint = provider.getEndpoint()
                    endpoints[endpoint.canonical_name] = endpoint

                    designated_launcher = self._chooseLauncherForEndpoint(
                        endpoint)
                    force = bool(designated_launcher ==
                                 self.component_info.hostname)
                    endpoint.start()
                    try:
                        endpoint.postConfig(provider)
                        if endpoint.canonical_name not in seen_endpoint_names:
                            quota_updated = endpoint.refreshQuotaLimits(force)
                            seen_endpoint_names.add(provider.canonical_name)
                            force = quota_updated or force
                        if provider.canonical_name not in seen_provider_names:
                            provider.postConfig(force)
                            seen_provider_names.add(provider.canonical_name)
                    except Exception:
                        self.log.exception("Error in postconfig for %s:",
                                           provider)
            self.endpoints = endpoints
            for providers in self.tenant_providers.values():
                for provider in providers:
                    if (self.connection_filter and
                        provider.connection_name not in
                        self.connection_filter):
                        continue
                    if provider.driver.name != 'static':
                        continue
                    self._registerStaticNodes(provider)
                    self._deRegisterStaticNodes(provider)
        return updated

    def _updateTenantProviders(self, tenant_name):
        # Reload the tenant if the layout changed.
        updated = False
        if (self.local_layout_state.get(tenant_name)
                == self.tenant_layout_state.get(tenant_name)):
            return updated
        self.log.debug("Updating tenant %s", tenant_name)
        with tenant_read_lock(self.zk_client, tenant_name, self.log) as tlock:
            layout_state = self.tenant_layout_state.get(tenant_name)

            if layout_state:
                with self.createZKContext(tlock, self.log) as context:
                    providers = list(self.layout_providers_store.get(
                        context, self.zk_client, tenant_name))
                    self.tenant_providers[tenant_name] = providers
                    for provider in providers:
                        self.log.debug("Loaded provider %s", provider.name)
                self.local_layout_state[tenant_name] = layout_state
                updated = True
            else:
                self.tenant_providers.pop(tenant_name, None)
                self.local_layout_state.pop(tenant_name, None)
        return updated

    def addImageValidateEvent(self, image_upload):
        iba = self.image_build_registry.getItem(image_upload.artifact_uuid)
        project_hostname, project_name = \
            iba.project_canonical_name.split('/', 1)
        tenant_name = iba.build_tenant_name
        driver = self.connections.drivers['zuul']
        event = driver.getImageValidateEvent(
            [iba.name], project_hostname, project_name, iba.project_branch,
            image_upload.uuid)
        self.log.info("Submitting image validate event for %s %s",
                      tenant_name, iba.name)
        self.trigger_events[tenant_name].put(event.trigger_name, event)

    def addImageDeleteEvent(self, iba):
        project_hostname, project_name = \
            iba.project_canonical_name.split('/', 1)
        tenant_name = iba.build_tenant_name
        driver = self.connections.drivers['zuul']
        event = driver.getImageDeleteEvent(
            [iba.name], project_hostname, project_name, iba.project_branch,
            iba.uuid)
        self.log.info("Submitting image delete event for %s %s",
                      tenant_name, iba.name)
        self.trigger_events[tenant_name].put(event.trigger_name, event)

    def _waitForStableImageRegistry(self):
        # Wait for any non-ready IBAs to become ready.  This resolves
        # a race condition where the scheduler might have created the
        # first of more than one IBA in a buildset, and we were
        # immediately triggered.  Wait until the scheduler is finished
        # so that if a buildset creates more than one IBA we have all
        # the info.
        now = time.time()
        while True:
            done = True
            for iba in self.image_build_registry.getAllArtifacts():
                if iba.state is not None:
                    continue
                if now - iba.timestamp > 30:
                    # We'll only wait 30 seconds for this so we don't
                    # get stuck.
                    continue
                done = False
                break
            if done:
                return
            time.sleep(1)

    def checkMissingImage(self, tenant_name, image, images_by_project_branch):
        # If there is already a successful build for
        # this image, skip.
        seen_formats = set()
        for iba in self.image_build_registry.getArtifactsForImage(
                image.canonical_name):
            seen_formats.add(iba.format)

        if image.format in seen_formats:
            # We have at least one build with the required
            # formats
            return

        self.log.debug("Missing format %s for image %s",
                       image.format, image.canonical_name)
        # Collect images with the same project-branch so we can build
        # them in one buildset.
        key = (image.project_canonical_name, image.branch)
        images = images_by_project_branch.setdefault(key, set())
        images.add(image.name)

    def checkOldImages(self):
        self.log.debug("Checking for old images")
        self.upload_deleted_event.clear()
        # Get the list of ibas for which we could consider uploads
        # first (to make sure that we don't race the creation of
        # uploads and setting the iba to ready).
        active_ibas = [iba for iba in self.image_build_registry.getItems()
                       if iba.state in
                       (iba.State.DELETING, iba.State.READY)]

        # Make sure that the upload registry is up to date after the
        # IBA registry so that we know about any uploads referencing
        # our active ibas.
        self.image_upload_registry.waitForSync()
        keep_uploads = set()
        known_uploads = set(self.image_upload_registry.getItems())
        known_providers = set()
        for tenant_name, providers in self.tenant_providers.items():
            for provider in providers:
                known_providers.add(provider.canonical_name)
                for image in provider.images.values():
                    if image.type == 'zuul':
                        self.checkOldImage(tenant_name, provider, image,
                                           keep_uploads)

        uploads_by_artifact = collections.defaultdict(list)
        latest_upload_timestamp = 0
        for upload in known_uploads:
            if upload.timestamp > latest_upload_timestamp:
                latest_upload_timestamp = upload.timestamp
            uploads_by_artifact[upload.artifact_uuid].append(upload)
            if upload.endpoint_name not in self.endpoints:
                continue
            iba = self.image_build_registry.getItem(upload.artifact_uuid)
            if not iba:
                self.log.warning("Unable to find artifact %s for upload %s",
                                 upload.artifact_uuid, upload.uuid)
                continue
            if set(upload.providers).isdisjoint(known_providers):
                # An orphaned upload means that either
                # a) the provider was removed without following the
                #    provider removal procedure (clear out images before
                #    deleting the provider)
                # or
                # b) that we could not load the provider e.g. due to a
                #    config error.
                # In both cases we want to keep the uploads in case the
                # provider shows up again.
                self.log.warning(
                    "Keeping orphaned upload %s with unknown providers %s",
                    upload, upload.providers)
                continue
            if (iba.state == iba.State.DELETING or
                upload.state == upload.State.DELETING or
                upload not in keep_uploads):
                if upload.uuid not in self.upload_uuids_in_delete_queue:
                    self.upload_uuids_in_delete_queue.add(upload.uuid)
                    self.upload_executor.submit(
                        DeleteJob(self, iba, upload).run)
        for iba in active_ibas:
            if (iba.timestamp > latest_upload_timestamp or
                not latest_upload_timestamp):
                # Ignore artifacts that are newer than the newest upload
                continue
            if len(uploads_by_artifact[iba.uuid]) == 0:
                self.log.info("Deleting image build artifact "
                              "with no uploads: %s", iba)
                with self.createZKContext(None, self.log) as ctx:
                    try:
                        with iba.locked(ctx, blocking=False):
                            iba.delete(ctx)
                    except LockException:
                        pass

    def checkOldImage(self, tenant_name, provider, image,
                      keep_uploads):
        image_cname = image.canonical_name
        uploads = self.image_upload_registry.getUploadsForImage(image_cname)
        valid_uploads = [
            upload for upload in uploads
            if (upload.isPermittedForProvider(image, provider) and
                upload.state == upload.State.READY and
                upload.validated and
                upload.external_id)
        ]
        # Keep the 2 most recent validated uploads (uploads are
        # already sorted by timestamp)
        newest_valid_uploads = valid_uploads[-2:]
        keep_uploads.update(set(newest_valid_uploads))
        # And also keep any uploads (regardless of validation) newer
        # than that (since they may have validation jobs running).
        if newest_valid_uploads:
            oldest_good_timestamp = newest_valid_uploads[0].timestamp
        else:
            oldest_good_timestamp = 0
        new_uploads = [
            upload for upload in uploads
            if (upload.isPermittedForProvider(image, provider) and
                upload.timestamp > oldest_good_timestamp)
        ]
        keep_uploads.update(set(new_uploads))

    def checkMissingUploads(self):
        self.log.debug("Checking for missing uploads")
        all_uploads_by_artifact_id = collections.defaultdict(list)
        pending_uploads_by_artifact_id = collections.defaultdict(list)
        self.image_updated_event.clear()
        for upload in self.image_upload_registry.getItems():
            if upload.endpoint_name not in self.endpoints:
                continue
            iba = self.image_build_registry.getItem(upload.artifact_uuid)
            if not iba:
                self.log.warning("Unable to find artifact %s for upload %s",
                                 upload.artifact_uuid, upload.uuid)
                continue
            if iba.state == iba.State.DELETING:
                continue
            if upload.state == upload.State.UPLOADING:
                # If there is an unlocked upload in uploading state,
                # it has probably crashed.  Reset it.
                if not upload.is_locked:
                    with self.createZKContext(None, self.log) as ctx:
                        try:
                            with upload.locked(ctx, blocking=False):
                                # Double check the state after lock.
                                upload.refresh(ctx)
                                if upload.state == upload.State.UPLOADING:
                                    with upload.activeContext(ctx):
                                        upload.state = upload.State.PENDING
                        except LockException:
                            # Upload locked again (lock / cache update race)
                            pass
            upload_list = all_uploads_by_artifact_id[upload.artifact_uuid]
            upload_list.append(upload)
            if upload.uuid in self.upload_uuids_in_queue:
                continue
            if upload.state != upload.State.PENDING:
                continue
            upload_list = pending_uploads_by_artifact_id[upload.artifact_uuid]
            upload_list.append(upload)

        for artifact_uuid, uploads in pending_uploads_by_artifact_id.items():
            iba = self.image_build_registry.getItem(artifact_uuid)
            for upload in uploads:
                self.upload_uuids_in_queue.add(upload.uuid)
            all_uploads = all_uploads_by_artifact_id[artifact_uuid]
            self.upload_executor.submit(UploadJob(self, iba, uploads,
                                                  all_uploads).run)

    def _downloadArtifactChunk(self, url, start, end, path):
        headers = {'Range': f'bytes={start}-{end}'}
        with open(path, 'r+b') as f:
            f.seek(start)
            with requests.get(url, stream=True, headers=headers) as resp:
                resp.raise_for_status()
                for chunk in resp.iter_content(chunk_size=DOWNLOAD_BLOCK_SIZE):
                    f.write(chunk)

    def downloadArtifact(self, url, path):
        futures = []
        with requests.head(url) as resp:
            if resp.status_code == 403:
                self.log.debug(
                    "Received 403 for %s, retrying with range header", url)
                # This may be a pre-signed url that can't handle a
                # HEAD request, retry with GET:
                with requests.get(url, stream=True,
                                  headers={"Range": "bytes=0-0"}) as resp:
                    resp.raise_for_status()
                    size = int(resp.headers['content-range'].split('/')[-1])
            else:
                resp.raise_for_status()
                size = int(resp.headers['content-length'])
        self.log.debug("Creating %s with size %s", path, size)
        with open(path, 'wb') as f:
            f.truncate(size)
        try:
            with ThreadPoolExecutor(max_workers=5) as executor:
                for start in range(0, size, DOWNLOAD_CHUNK_SIZE):
                    end = start + DOWNLOAD_CHUNK_SIZE - 1
                    futures.append(executor.submit(self._downloadArtifactChunk,
                                                   url,
                                                   start, end, path))
                for future in concurrent.futures.as_completed(futures):
                    future.result()
        except Exception:
            # If we encountered a problem, delete the file so it isn't
            # orphaned.
            if os.path.exists(path):
                os.unlink(path)
                self.log.info("Deleted %s", path)
            raise
        self.log.debug("Downloaded %s bytes to %s", size, path)
        return path

    def getImageExternalId(self, node, provider):
        label = provider.labels[node.label]
        image = provider.images[label.image]
        if image.type != 'zuul':
            return None
        image_cname = image.canonical_name
        uploads = self.image_upload_registry.getUploadsForImage(image_cname)
        if node.image_upload_uuid:
            valid_uploads = [
                u for u in uploads if u.uuid == node.image_upload_uuid
            ]
        else:
            valid_uploads = [
                upload for upload in uploads
                if (upload.state == upload.State.READY and
                    upload.validated and
                    upload.external_id and
                    upload.isPermittedForProvider(image, provider))

            ]
        if not valid_uploads:
            raise Exception("No image found")
        # Uploads are already sorted by timestamp
        image_upload = valid_uploads[-1]
        return image_upload.external_id

    def getEndpointLimits(self, provider):
        val = self._provider_limits_cache.get(provider.canonical_name)
        if val:
            return val

        quota = provider.getEndpointLimits()
        self.log.debug("Provider endpoint quota limits for %s: %s",
                       provider.name, quota)

        self._provider_limits_cache[provider.canonical_name] = quota
        return quota

    def getProviderQuota(self, provider):
        val = self._provider_quota_cache.get(provider.canonical_name)
        if val:
            return val

        # This is initialized with the full endpoint quota and later
        # becomes the quota available for Zuul.
        quota = self.getEndpointLimits(provider).copy()

        unmanaged = provider.getEndpoint().quota_cache.getUnmanagedUsage()
        self.log.debug("Provider endpoint unmanaged quota used for %s: %s",
                       provider.name, unmanaged)
        # Subtract the unmanaged quota usage from nodepool_max
        # to get the quota available for us.
        quota.subtract(unmanaged)

        self._provider_quota_cache[provider.canonical_name] = quota
        return quota

    def getProviderQuotaAvailable(self, provider, messages,
                                  include_requested=False):
        # This is initialized with the full provider endpoint quota,
        # which is cached and updated every 5 minutes.
        quota = self.getProviderQuota(provider).copy()

        # Subtract the quota used by other providers on the same
        # endpoint.
        other = model.QuotaInformation()
        for other_provider in self._getUniqueProviders():
            if (other_provider.endpoint != provider.endpoint
                or other_provider.canonical_name == provider.canonical_name):
                continue
            other.add(self.api.nodes_cache.getQuota(
                other_provider, include_requested=include_requested))

        quota.subtract(other)
        messages.append(
            f"Provider endpoint other quota used for {provider.name}: {other}")

        # Restrict quota limits based on our provider limits
        provider_limits = model.QuotaInformation(
            default=math.inf, **provider.resource_limits
        )
        quota.min(provider_limits)
        return quota

    def getQuotaPercentage(self, provider, messages):
        try:
            total = self.getProviderQuotaAvailable(
                provider, messages, include_requested=True)
        except Exception:
            # This will emit an annotated log message, but no traceback
            messages.append("Unable to get provider quota")
            self.log.exception("Unable to get provider quota")
            # If there is an error getting quota information, assume
            # the provider is having problems and report it as full.
            return 1.0
        # This is continuously updated in the background
        used = self.api.nodes_cache.getQuota(provider, include_requested=True)
        pct = 0.0
        messages.append(
            f"Provider {provider} quota available before Zuul: {total}")
        messages.append(
            f"Provider {provider} quota claimed by Zuul: {used}")
        for resource in total.quota.keys():
            used_r = used.quota.get(resource, used.default)
            total_r = total.quota[resource]
            if not total_r:
                pct = max(1.0, pct)
            else:
                pct = max(used_r / total_r, pct)
        messages.append(
            f"Provider {provider} usage factor: {pct}")
        if pct < 1.0:
            # If we are below 100% usage, lose precision so that we only
            # consider 10% gradiations.  This may help us avoid
            # thundering herds by allowing some randomization among
            # launchers that are within 10% of each other.

            # Over 100% use the exact value so that we always assign
            # to the launcher with the smallest backlog.
            pct = round(pct, 1)
        return pct

    def doesProviderHaveQuotaForLabel(self, provider, label, messages,
                                      include_usage=True):
        if include_usage:
            # When include_usage is True, we include requested nodes here
            # because this is called to decide whether to add a new request
            # to the provider, so we should include other nodes we've
            # already allocated to providers in the decision.
            total = self.getProviderQuotaAvailable(
                provider, messages, include_requested=True)
            messages.append(
                f"Provider quota available before {provider}: {total}")
            used = self.api.nodes_cache.getQuota(
                provider, include_requested=True)
            total.subtract(used)
            messages.append(
                f"Provider quota available including {provider}: {total}")
        else:
            total = self.getEndpointLimits(provider).copy()
            # Restrict quota limits based on our provider limits
            provider_limits = model.QuotaInformation(
                default=math.inf, **provider.resource_limits
            )
            total.min(provider_limits)
            messages.append(f"Provider {provider} limits: {total}")

        label_quota = provider.getQuotaForLabel(label)
        total.subtract(label_quota)
        messages.append(
            f"Label {label} required quota: {label_quota}")
        return total.nonNegative()

    def doesProviderHaveQuotaForNode(self, provider, node, messages):
        # We do not include requested nodes here because this is
        # called to decide whether to issue the create API call for a
        # node already allocated to the provider.  We only want to
        # "pause" the provider if it really is at quota.
        total = self.getProviderQuotaAvailable(
            provider, messages, include_requested=False)
        messages.append(f"Provider quota before {provider}: {total}")
        used = self.api.nodes_cache.getQuota(provider, include_requested=False)
        total.subtract(used)
        messages.append(f"Provider quota including {provider}: {total}")
        total.subtract(node.quota)
        messages.append(f"Node {node} required quota: {node.quota}")
        return total.nonNegative()

    def runStatsElection(self):
        while self._running:
            try:
                self.log.debug("Running stats election")
                self.stats_election.run(self.runStats)
            except Exception:
                self.log.exception("Exception running election stats:")
                time.sleep(1)

    def runStats(self):
        self.log.debug("Won stats election")
        while not self.stop_event.wait(self._stats_interval):
            if not self.stats_election.is_still_valid():
                self.log.debug("Stats election no longer valid")
                return
            try:
                with self.providers_update_lock:
                    self._runStats()
            except Exception:
                self.log.exception("Error in periodic stats:")

    def _runStats(self):
        if not self.statsd:
            return

        node_defaults = {}
        for state in model.ProviderNode.State:
            node_defaults[state] = 0

        nodes = node_defaults.copy()
        provider_nodes = collections.defaultdict(
            lambda: node_defaults.copy())
        provider_label_nodes = collections.defaultdict(
            lambda: collections.defaultdict(
                lambda: node_defaults.copy()))

        for node in self.api.nodes_cache.getItems():
            nodes[node.state] += 1
            provider_nodes[node.provider][node.state] += 1
            provider_label_nodes[node.provider][node.label][
                node.state] += 1

        requests = {}
        for state in model.NodesetRequest.State:
            requests[state] = 0
        for request in self.api.requests_cache.getItems():
            if request.state != request.State.TEST_HOLD:
                requests[request.state] += 1

        providers = {}
        for tenant_name, tenant_providers in self.tenant_providers.items():
            for tenant_provider in tenant_providers:
                providers[tenant_provider.canonical_name] = tenant_provider
        for provider in providers.values():
            safe_pname = _normalize_statsd_name(provider.canonical_name)
            quota = self.getEndpointLimits(provider).copy()
            # Restrict quota limits based on our provider limits
            provider_limits = model.QuotaInformation(
                default=math.inf, **provider.resource_limits
            )
            quota.min(provider_limits)
            limits = quota.getResources()
            # zuul.provider.<provider>.limit.<resource> gauge
            for res, value in limits.items():
                safe_res = _normalize_statsd_name(res)
                self.statsd.gauge(
                    f'zuul.provider.{safe_pname}.limit.{safe_res}',
                    value)
            usage = self.api.nodes_cache.getQuota(provider).getResources()
            for res, value in usage.items():
                safe_res = _normalize_statsd_name(res)
                self.statsd.gauge(
                    f'zuul.provider.{safe_pname}.usage.{safe_res}',
                    value)
            # zuul.provider.<provider>.nodes.state.<state> gauge
            for state, value in provider_nodes[
                    provider.canonical_name].items():
                self.statsd.gauge(
                    f'zuul.provider.{safe_pname}.nodes.state.{state.value}',
                    value)
            # zuul.provider.<provider>.label.<label>.nodes.state.<state> gauge
            for label, label_nodes in provider_label_nodes[
                    provider.canonical_name].items():
                safe_label = _normalize_statsd_name(label)
                for state, value in label_nodes.items():
                    self.statsd.gauge(
                        f'zuul.provider.{safe_pname}.label.{safe_label}.'
                        f'nodes.state.{state.value}',
                        value)
        # zuul.nodes.state.<state> gauge
        for state, value in nodes.items():
            self.statsd.gauge(
                f'zuul.nodes.state.{state.value}',
                value)
        # zuul.nodeset_requests.state.<state> gauge
        for state, value in requests.items():
            self.statsd.gauge(
                f'zuul.nodeset_requests.state.{state.value}',
                value)

        uploads_by_image = collections.defaultdict(list)
        for upload in self.image_upload_registry.getItems():
            uploads_by_image[
                _normalize_statsd_name(upload.canonical_name)].append(upload)

        upload_states = collections.Counter()
        # zuul.image.<image>.upload.<endpoint>.state.<state> gauge
        for image_cname, uploads in uploads_by_image.items():
            upload_states_by_endpoint = collections.defaultdict(
                collections.Counter)

            for upload in uploads:
                upload_states_by_endpoint[upload.endpoint_name][
                    upload.state] += 1

            for endpoint_name, state_counter in (
                    upload_states_by_endpoint.items()):
                safe_ename = _normalize_statsd_name(endpoint_name)
                upload_states.update(state_counter)
                for state in model.ImageUpload.STATES:
                    self.statsd.gauge(
                        f'zuul.image.{image_cname}.upload.{safe_ename}'
                        f'.state.{state}', state_counter[state])

        # zuul.uploads.state.<state> gauge
        for state, count in upload_states.items():
            self.statsd.gauge(f'zuul.uploads.state.{state}', count)
