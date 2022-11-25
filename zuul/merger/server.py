# Copyright 2014 OpenStack Foundation
# Copyright 2021-2022, 2024 Acme Gating, LLC
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
import socket
import sys
import threading
import time
import yaml
from abc import ABCMeta
from configparser import ConfigParser
from contextlib import nullcontext

from kazoo.exceptions import NoNodeError
from kazoo.retry import KazooRetry

from zuul.lib import commandsocket
from zuul.lib.config import get_default
from zuul.lib.logutil import get_annotated_logger
from zuul.lib import tracing
from zuul.merger import merger
from zuul.model import (
    FilesChangesCompletedEvent, MergeCompletedEvent, MergeRequest
)
from zuul.lib.monitoring import MonitoringServer
from zuul.version import get_version_string
from zuul.zk import ZooKeeperClient
from zuul.zk.components import MergerComponent, COMPONENT_REGISTRY
from zuul.zk.event_queues import PipelineResultEventQueue
from zuul.zk.merger import MergerApi

COMMANDS = [
    commandsocket.StopCommand,
    commandsocket.PauseCommand,
    commandsocket.UnPauseCommand,
    commandsocket.GracefulCommand
]


class BaseRepoLocks(metaclass=ABCMeta):

    def getRepoLock(self, connection_name, project_name):
        return nullcontext()


class RepoLocks(BaseRepoLocks):

    def __init__(self):
        self.locks = {}

    def getRepoLock(self, connection_name, project_name):
        key = '%s:%s' % (connection_name, project_name)
        self.locks.setdefault(key, threading.Lock())
        return self.locks[key]


class BaseMergeServer(metaclass=ABCMeta):
    log = logging.getLogger("zuul.BaseMergeServer")

    _repo_locks_class = BaseRepoLocks
    _merger_api_class = MergerApi

    def __init__(
        self,
        config: ConfigParser,
        component: str,
        connections,
    ):
        self.connections = connections
        self._merger_running = False
        self._merger_paused = False
        self.merge_email = get_default(config, 'merger', 'git_user_email',
                                       'zuul.merger.default@example.com')
        self.merge_name = get_default(config, 'merger', 'git_user_name',
                                      'Zuul Merger Default')
        self.merge_speed_limit = get_default(
            config, 'merger', 'git_http_low_speed_limit', '1000')
        self.merge_speed_time = get_default(
            config, 'merger', 'git_http_low_speed_time', '30')
        self.git_timeout = get_default(config, 'merger', 'git_timeout', 300)
        self.sync_timeout = get_default(config, 'merger', 'sync_timeout', 30)

        self.merge_root = get_default(config, component, 'git_dir',
                                      '/var/lib/zuul/{}-git'.format(component))

        self.projects = get_default(config, 'merger', 'projects', None)
        if self.projects:
            self.projects = yaml.safe_load(self.projects)

        self.config = config

        self.tracing = tracing.Tracing(self.config)
        self.zk_client = ZooKeeperClient.fromConfig(self.config)
        self.zk_client.connect()

        self.result_events = PipelineResultEventQueue.createRegistry(
            self.zk_client
        )

        self.merger_thread = threading.Thread(
            target=self.runMerger,
            name="Merger",
        )

        self.merger_loop_wake_event = threading.Event()

        self.merger_api = self._merger_api_class(
            self.zk_client,
            merge_request_callback=self.merger_loop_wake_event.set,
        )

        # This merger and its git repos are used to maintain
        # up-to-date copies of all the repos that are used by jobs, as
        # well as to support the merger:cat functon to supply
        # configuration information to Zuul when it starts.
        self.merger = self._getMerger(self.merge_root, None,
                                      execution_context=False)

        # Repo locking is needed on the executor
        self.repo_locks = self._repo_locks_class()

    def _getMerger(self, root, cache_root, logger=None,
                   execution_context=True, scheme=None,
                   cache_scheme=None):
        return merger.Merger(
            root,
            self.connections,
            self.zk_client,
            self.merge_email,
            self.merge_name,
            self.merge_speed_limit,
            self.merge_speed_time,
            cache_root,
            logger,
            execution_context=execution_context,
            git_timeout=self.git_timeout,
            scheme=scheme,
            cache_scheme=cache_scheme,
            sync_timeout=self.sync_timeout,
            projects=self.projects,
        )

    def _repoLock(self, connection_name, project_name):
        # The merger does not need locking so return a null lock.
        return nullcontext()

    def _update(self, connection_name, project_name, zuul_event_id=None):
        # The executor overrides _update so it can do the update
        # asynchronously.
        self.merger.updateRepo(connection_name, project_name,
                               zuul_event_id=zuul_event_id)

    def start(self):
        self.log.debug('Starting merger')
        self._merger_running = True
        self.merger_thread.start()

    def stop(self):
        self.log.debug('Stopping base merge server')
        self._merger_running = False
        self.merger_loop_wake_event.set()
        self.tracing.stop()
        self.log.debug('Stopped base merge server')

    def join(self):
        self.log.debug('Joining base merge server')
        self.merger_loop_wake_event.set()
        self.merger_thread.join()
        self.zk_client.disconnect()
        self.log.debug('Joined base merge server')

    def pause(self):
        self.log.debug('Pausing merger')
        self._merger_paused = True

    def unpause(self):
        self.log.debug('Resuming merger')
        self._merger_paused = False
        self.merger_loop_wake_event.set()

    def isComponentRunning(self):
        return self._merger_running

    def runMerger(self):
        while self._merger_running:
            self.merger_loop_wake_event.wait()
            self.merger_loop_wake_event.clear()
            if self._merger_paused:
                continue
            try:
                for merge_request in self.merger_api.next():
                    if not self._merger_running:
                        break

                    self._lockAndRunMergeJob(merge_request)
            except Exception:
                self.log.exception("Error in merge thread:")
                time.sleep(5)
                self.merger_loop_wake_event.set()

    def _lockAndRunMergeJob(self, merge_request):
        # Lock and update the merge request
        if not self.merger_api.lock(merge_request, blocking=False):
            return
        with tracing.startSpanInContext(
                merge_request.span_context, "MergerJob",
                attributes={"merger": self.hostname}):
            self._runMergeJob(merge_request)

    def _runMergeJob(self, merge_request):
        log = get_annotated_logger(
            self.log, merge_request.event_id
        )

        # Ensure that the request is still in state requested. This method is
        # called based on cached data and there might be a mismatch between the
        # cached state and the real state of the request. The lock might
        # have been successful because the request is already completed and
        # thus unlocked.
        if merge_request.state != MergeRequest.REQUESTED:
            self._retry(merge_request.lock, self.merger_api.unlock,
                        merge_request)

        try:
            merge_request.state = MergeRequest.RUNNING
            params = self.merger_api.getParams(merge_request)
        except Exception:
            log.exception("Exception while preparing to start merge job")
            # If we failed at this point, we have not written anything
            # to ZK yet; the only thing we need to do is to ensure
            # that we release the lock, and another merger will be
            # able to grab the request.
            self._retry(merge_request.lock, self.merger_api.unlock,
                        merge_request)
            return

        result = None
        try:
            self.merger_api.clearParams(merge_request)
            # Directly update the merge request in ZooKeeper, so we
            # don't loop over and try to lock it again and again.
            self.merger_api.update(merge_request)
            self.log.debug("Next executed merge job: %s", merge_request)
            try:
                result = self.executeMergeJob(merge_request, params)
            except Exception:
                log.exception("Error running merge job:")
        finally:
            self.completeMergeJob(merge_request, result)

    def executeMergeJob(self, merge_request, params):
        result = None
        start = time.monotonic()
        if merge_request.job_type == MergeRequest.MERGE:
            result = self.merge(merge_request, params)
        elif merge_request.job_type == MergeRequest.CAT:
            result = self.cat(merge_request, params)
        elif merge_request.job_type == MergeRequest.REF_STATE:
            result = self.refstate(merge_request, params)
        elif merge_request.job_type == MergeRequest.FILES_CHANGES:
            result = self.fileschanges(merge_request, params)
        end = time.monotonic()
        result['elapsed_time'] = end - start
        return result

    def cat(self, merge_request, args):
        self.log.debug("Got cat job: %s", merge_request.uuid)

        connection_name = args['connection']
        project_name = args['project']

        lock = self.repo_locks.getRepoLock(connection_name, project_name)
        try:
            self._update(connection_name, project_name)
            with lock:
                files = self.merger.getFiles(connection_name, project_name,
                                             args['branch'], args['files'],
                                             args.get('dirs'))
        except Exception:
            result = dict(update=False)
        else:
            result = dict(updated=True, files=files)

        return result

    def merge(self, merge_request, args):
        self.log.debug("Got merge job: %s", merge_request.uuid)
        zuul_event_id = merge_request.event_id

        errors = []
        result = dict(merged=False,
                      commit=None,
                      files=None,
                      repo_state=None,
                      zuul_event_id=zuul_event_id,
                      errors=errors)
        for item in args['items']:
            try:
                self._update(item['connection'], item['project'])
            except Exception:
                msg = (f"Unable to update {item['connection']}/"
                       f"{item['project']}")
                errors.append(msg)
                return result

        ret = self.merger.mergeChanges(
            args['items'], args.get('files'),
            args.get('dirs', []),
            args.get('repo_state'),
            branches=args.get('branches'),
            repo_locks=self.repo_locks,
            zuul_event_id=zuul_event_id,
            errors=errors)

        if ret is not None:
            result['merged'] = True
            (result['commit'], result['files'], result['repo_state'],
             recent, orig_commit, ops) = ret
        return result

    def refstate(self, merge_request, args):
        self.log.debug("Got refstate job: %s", merge_request.uuid)
        zuul_event_id = merge_request.event_id
        success, repo_state, item_in_branches = \
            self.merger.getRepoState(
                args['items'], self.repo_locks, branches=args.get('branches'))
        result = dict(updated=success,
                      repo_state=repo_state,
                      item_in_branches=item_in_branches)
        result['zuul_event_id'] = zuul_event_id
        return result

    def fileschanges(self, merge_request, args):
        self.log.debug("Got fileschanges job: %s", merge_request.uuid)
        zuul_event_id = merge_request.event_id
        changes = args.get('changes')
        results = []
        for change in changes:
            connection_name = change['connection']
            project_name = change['project']

            lock = self.repo_locks.getRepoLock(connection_name, project_name)
            try:
                self._update(connection_name, project_name,
                             zuul_event_id=zuul_event_id)
                with lock:
                    files = self.merger.getFilesChanges(
                        connection_name, project_name,
                        change['branch'], change['tosha'],
                        zuul_event_id=zuul_event_id)
                results.append(files)
            except Exception:
                return dict(updated=False)

        return dict(updated=True, files=results)

    def completeMergeJob(self, merge_request, result):
        log = get_annotated_logger(self.log, merge_request.event_id)

        # Always provide a result event, even if we have no
        # information; otherwise items can get stuck in the queue.
        if result is None:
            result = {}

        payload = json.dumps(result, sort_keys=True)
        self.log.debug("Completed %s job %s: payload size: %s",
                       merge_request.job_type, merge_request.uuid,
                       sys.getsizeof(payload))
        merged = result.get("merged", False)
        updated = result.get("updated", False)
        commit = result.get("commit")
        repo_state = result.get("repo_state", {})
        item_in_branches = result.get("item_in_branches", [])
        files = result.get("files", {})
        errors = result.get("errors", [])
        elapsed_time = result.get("elapsed_time")

        log.info(
            "Merge %s complete, merged: %s, updated: %s, commit: %s, "
            "branches: %s",
            merge_request,
            merged,
            updated,
            commit,
            item_in_branches,
        )

        lock_valid = merge_request.lock.is_still_valid()
        if not lock_valid:
            return

        # Provide a result either via a result future or a result event
        if merge_request.result_path:
            log.debug(
                "Providing synchronous result via future for %s",
                merge_request,
            )
            self._retry(merge_request.lock,
                        self.merger_api.reportResult, merge_request, result)

        elif merge_request.build_set_uuid:
            log.debug(
                "Providing asynchronous result via result event for %s",
                merge_request,
            )
            if merge_request.job_type == MergeRequest.FILES_CHANGES:
                event = FilesChangesCompletedEvent(
                    merge_request.uuid,
                    merge_request.build_set_uuid,
                    files,
                    elapsed_time,
                    merge_request.span_info,
                )
            else:
                event = MergeCompletedEvent(
                    merge_request.uuid,
                    merge_request.build_set_uuid,
                    merged,
                    updated,
                    commit,
                    files,
                    repo_state,
                    item_in_branches,
                    errors,
                    elapsed_time,
                    merge_request.span_info,
                    merge_request.event_id,
                )

            def put_complete_event(log, merge_request, event):
                try:
                    self.result_events[merge_request.tenant_name][
                        merge_request.pipeline_name].put(event)
                except NoNodeError:
                    log.warning("Pipeline was removed: %s",
                                merge_request.pipeline_name)

            self._retry(merge_request.lock,
                        put_complete_event, log, merge_request, event)

        # Set the merge request to completed, unlock and delete it. Although
        # the state update is mainly for consistency reasons, it might come in
        # handy in case the deletion or unlocking failes. Thus, we know that
        # the merge request was already processed and we have a result in the
        # result queue.
        merge_request.state = MergeRequest.COMPLETED
        self._retry(merge_request.lock,
                    self.merger_api.update, merge_request)
        self._retry(merge_request.lock,
                    self.merger_api.unlock, merge_request)
        # TODO (felix): If we want to optimize ZK requests, we could only call
        # the remove() here.
        self.merger_api.remove(merge_request)

    def _retry(self, lock, func, *args, **kw):
        """Retry a method to deal with ZK connection issues

        This is a helper method to retry retryable ZK operations as
        long as we are still running.

        This method exits when one of the following occurs:

        * The callable function completes.
        * This server stops.
        * The lock (if supplied) is invalidated due to connection loss.
        * A non-retryable ZK exception is raised.

        Pass None as the lock parameter if the lock issue is not
        relevant.

        """
        def interrupt():
            if ((not self.isComponentRunning()) or
                (lock and not lock.is_still_valid())):
                return True
            return False

        kazoo_retry = KazooRetry(max_tries=-1, interrupt=interrupt,
                                 delay=5, backoff=0, ignore_expire=False)
        try:
            return kazoo_retry(func, *args, **kw)
        except InterruptedError:
            pass


class MergeServer(BaseMergeServer):
    log = logging.getLogger("zuul.MergeServer")

    def __init__(
        self,
        config: ConfigParser,
        connections,
    ):
        super().__init__(config, 'merger', connections)
        self.hostname = socket.getfqdn()
        self.component_info = MergerComponent(
            self.zk_client, self.hostname, version=get_version_string())
        self.component_info.register()
        COMPONENT_REGISTRY.create(self.zk_client)

        self.monitoring_server = MonitoringServer(self.config, 'merger',
                                                  self.component_info)
        self.monitoring_server.start()

        self.command_map = {
            commandsocket.StopCommand.name: self.stop,
            # Stop for the mergers is always graceful. We add this alias
            # to make it clearer to users that they can gracefully stop.
            commandsocket.GracefulCommand.name: self.stop,
            commandsocket.PauseCommand.name: self.pause,
            commandsocket.UnPauseCommand.name: self.unpause,
        }
        command_socket = get_default(
            self.config, 'merger', 'command_socket',
            '/var/lib/zuul/merger.socket')
        self.command_socket = commandsocket.CommandSocket(command_socket)

        self._command_running = False

    def start(self):
        super().start()
        self._command_running = True
        self.log.debug("Starting command processor")
        self.command_socket.start()
        self.command_thread = threading.Thread(
            target=self.runCommand, name='command')
        self.command_thread.daemon = True
        self.command_thread.start()
        self.component_info.state = self.component_info.RUNNING

    def stop(self):
        self.log.debug("Stopping merger")
        self.component_info.state = self.component_info.STOPPED
        super().stop()
        self._command_running = False
        self.command_socket.stop()
        self.monitoring_server.stop()
        self.log.debug("Stopped merger")

    def join(self):
        self.log.debug("Joining merger")
        super().join()
        self.monitoring_server.join()
        self.log.debug("Joined merger")

    def pause(self):
        self.log.debug('Pausing')
        self.component_info.state = self.component_info.PAUSED
        super().pause()

    def unpause(self):
        self.log.debug('Resuming')
        super().unpause()
        self.component_info.state = self.component_info.RUNNING

    def runCommand(self):
        while self._command_running:
            try:
                command, args = self.command_socket.get()
                if command != '_stop':
                    self.command_map[command](*args)
            except Exception:
                self.log.exception("Exception while processing command")
