# Copyright 2012-2015 Hewlett-Packard Development Company, L.P.
# Copyright 2013 OpenStack Foundation
# Copyright 2013 Antoine "hashar" Musso
# Copyright 2013 Wikimedia Foundation Inc.
# Copyright 2021-2025 Acme Gating, LLC
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
import traceback
import urllib.parse
import uuid
from contextlib import suppress
from contextlib import nullcontext
from collections import defaultdict, OrderedDict

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from kazoo.exceptions import NotEmptyError
from opentelemetry import trace

from zuul import configloader, exceptions
from zuul.launcher.client import LauncherClient
from zuul.lib import commandsocket
from zuul.lib.ansible import AnsibleManager
from zuul.lib.config import get_default
from zuul.lib.keystorage import KeyStorage
from zuul.lib.logutil import get_annotated_logger
from zuul.lib.monitoring import MonitoringServer
from zuul.lib.queue import NamedQueue
from zuul.lib.times import Times
from zuul.lib.statsd import get_statsd, normalize_statsd_name
from zuul.lib import tracing
import zuul.lib.queue
import zuul.lib.repl
from zuul import nodepool
from zuul.executor.client import ExecutorClient
from zuul.merger.client import MergeClient
from zuul.model import (
    Abide,
    Build,
    BuildCompletedEvent,
    BuildEvent,
    BuildPausedEvent,
    BuildStartedEvent,
    BuildStatusEvent,
    Change,
    ChangeManagementEvent,
    DequeueEvent,
    EnqueueEvent,
    FilesChangesCompletedEvent,
    HoldRequest,
    ImageBuildArtifact,
    ImageUpload,
    MergeCompletedEvent,
    NodeRequest,
    NodesProvisionedEvent,
    PipelinePostConfigEvent,
    PipelineSemaphoreReleaseEvent,
    PromoteEvent,
    QueryCache,
    ReconfigureEvent,
    SemaphoreReleaseEvent,
    SupercedeEvent,
    SystemAttributes,
    TenantReconfigureEvent,
    UnparsedAbideConfig,
    STATE_FAILED,
    SEVERITY_ERROR,
)
from zuul.version import get_version_string
from zuul.zk import ZooKeeperClient
from zuul.zk.blob_store import BlobStore
from zuul.zk.cleanup import (
    SemaphoreCleanupLock,
    BuildRequestCleanupLock,
    ConnectionCleanupLock,
    GeneralCleanupLock,
    MergeRequestCleanupLock,
    NodeRequestCleanupLock,
)
from zuul.zk.components import (
    BaseComponent, COMPONENT_REGISTRY, SchedulerComponent
)
from zuul.zk.config_cache import SystemConfigCache, UnparsedConfigCache
from zuul.zk.event_queues import (
    EventWatcher,
    TenantManagementEventQueue,
    TenantTriggerEventQueue,
    PipelineManagementEventQueue,
    PipelineResultEventQueue,
    PipelineTriggerEventQueue,
    PIPELINE_ROOT,
    PIPELINE_NAME_ROOT,
    TENANT_ROOT,
)
from zuul.zk.exceptions import LockException
from zuul.zk.layout import (
    LayoutProvidersStore,
    LayoutState,
    LayoutStateStore,
)
from zuul.zk.locks import (
    locked,
    tenant_read_lock,
    tenant_write_lock,
    pipeline_lock,
    management_queue_lock,
    trigger_queue_lock,
)
from zuul.zk.system import ZuulSystem
from zuul.zk.zkobject import ZKContext
from zuul.zk.election import SessionAwareElection

RECONFIG_LOCK_ID = "RECONFIG"


class PendingReconfiguration(Exception):
    pass


class FullReconfigureCommand(commandsocket.Command):
    name = 'full-reconfigure'
    help = 'Perform a reconfiguration of all tenants'


class SmartReconfigureCommand(commandsocket.Command):
    name = 'smart-reconfigure'
    help = 'Perform a reconfiguration of updated tenants'


class TenantArgument(commandsocket.Argument):
    name = 'tenant'
    help = 'The name of the tenant'


class TenantReconfigureCommand(commandsocket.Command):
    name = 'tenant-reconfigure'
    help = 'Perform a reconfiguration of a specific tenant'
    args = [TenantArgument]


class PipelineArgument(commandsocket.Argument):
    name = 'pipeline'
    help = 'The name of the pipeline'


class ZKProfileCommand(commandsocket.Command):
    name = 'zkprofile'
    help = 'Toggle ZK profiling for a pipeline'
    args = [TenantArgument, PipelineArgument]


COMMANDS = [
    FullReconfigureCommand,
    SmartReconfigureCommand,
    TenantReconfigureCommand,
    ZKProfileCommand,
    commandsocket.StopCommand,
    commandsocket.ReplCommand,
    commandsocket.NoReplCommand,
]


class SchedulerStatsElection(SessionAwareElection):
    """Election for emitting scheduler stats."""

    def __init__(self, client):
        self.election_root = "/zuul/scheduler/stats-election"
        super().__init__(client.client, self.election_root)


class DummyFrozenJob:
    """Some internal methods expect a FrozenJob for cleanup;
    use this when we don't actually have one"""
    pass


class Scheduler(threading.Thread):
    """The engine of Zuul.

    The Scheduler is responsible for receiving events and dispatching
    them to appropriate components (including pipeline managers,
    mergers and executors).

    It runs a single threaded main loop which processes events
    received one at a time and takes action as appropriate.  Other
    parts of Zuul may run in their own thread, but synchronization is
    performed within the scheduler to reduce or eliminate the need for
    locking in most circumstances.

    The main daemon will have one instance of the Scheduler class
    running which will persist for the life of the process.  The
    Scheduler instance is supplied to other Zuul components so that
    they can submit events or otherwise communicate with other
    components.

    """

    log = logging.getLogger("zuul.Scheduler")
    tracer = trace.get_tracer("zuul")

    _stats_interval = 30
    _semaphore_cleanup_interval = IntervalTrigger(minutes=60, jitter=60)
    _general_cleanup_interval = IntervalTrigger(minutes=60, jitter=60)
    _build_request_cleanup_interval = IntervalTrigger(seconds=60, jitter=5)
    _merge_request_cleanup_interval = IntervalTrigger(seconds=60, jitter=5)
    _connection_cleanup_interval = IntervalTrigger(minutes=5, jitter=10)
    _merger_client_class = MergeClient
    _executor_client_class = ExecutorClient
    _launcher_client_class = LauncherClient

    def __init__(self, config, connections, app, wait_for_init,
                 disable_pipelines=False, testonly=False):
        threading.Thread.__init__(self)
        self.daemon = True
        self.disable_pipelines = disable_pipelines
        self._profile_pipelines = set()
        self.wait_for_init = wait_for_init
        self.hostname = socket.getfqdn()
        self.tracing = tracing.Tracing(config)
        self.primed_event = threading.Event()
        # Wake up the main run loop
        self.wake_event = threading.Event()
        # Wake up the update loop
        self.layout_update_event = threading.Event()
        # Only used by tests in order to quiesce the layout update loop
        self.layout_update_lock = threading.Lock()
        # Hold this lock when updating the local unparsed abide
        self.unparsed_abide_lock = threading.Lock()
        # Hold this lock when updating the local layout for a tenant
        self.layout_lock = defaultdict(threading.Lock)
        # Only used by tests in order to quiesce the main run loop
        self.run_handler_lock = threading.Lock()
        self.command_map = {
            FullReconfigureCommand.name: self.fullReconfigureCommandHandler,
            SmartReconfigureCommand.name: self.smartReconfigureCommandHandler,
            TenantReconfigureCommand.name:
                self.tenantReconfigureCommandHandler,
            ZKProfileCommand.name: self.zkProfileCommandHandler,
            commandsocket.StopCommand.name: self.stop,
            commandsocket.ReplCommand.name: self.startRepl,
            commandsocket.NoReplCommand.name: self.stopRepl,
        }
        self._stopped = False

        self._zuul_app = app
        self.connections = connections
        self.sql = self.connections.getSqlReporter(None)
        self.statsd = get_statsd(config)
        if self.statsd:
            self.statsd_timer = self.statsd.timer
        else:
            self.statsd_timer = nullcontext
        self.times = Times(self.sql, self.statsd)
        self.repl = None
        self.stats_thread = threading.Thread(target=self.runStatsElection)
        self.stats_thread.daemon = True
        self.stop_event = threading.Event()
        self.apsched = BackgroundScheduler()
        # TODO(jeblair): fix this
        # Despite triggers being part of the pipeline, there is one trigger set
        # per scheduler. The pipeline handles the trigger filters but since
        # the events are handled by the scheduler itself it needs to handle
        # the loading of the triggers.
        # self.triggers['connection_name'] = triggerObject
        self.triggers = dict()
        self.config = config

        self.zk_client = ZooKeeperClient.fromConfig(self.config)
        self.zk_client.connect()
        self.query_cache = QueryCache(self.zk_client)
        self.system = ZuulSystem(self.zk_client)

        self.zuul_version = get_version_string()

        self.component_info = SchedulerComponent(
            self.zk_client, self.hostname, version=self.zuul_version)
        self.component_info.register()
        self.component_registry = COMPONENT_REGISTRY.create(self.zk_client)
        self.system_config_cache = SystemConfigCache(self.zk_client,
                                                     self.wake_event.set)
        self.unparsed_config_cache = UnparsedConfigCache(self.zk_client)

        self.monitoring_server = MonitoringServer(self.config, 'scheduler',
                                                  self.component_info)
        self.monitoring_server.start()

        # TODO (swestphahl): Remove after we've refactored reconfigurations
        # to be performed on the tenant level.
        self.reconfigure_event_queue = NamedQueue("ReconfigureEventQueue")
        self.event_watcher = EventWatcher(
            self.zk_client, self.wake_event.set
        )
        self.management_events = TenantManagementEventQueue.createRegistry(
            self.zk_client)
        self.pipeline_management_events = (
            PipelineManagementEventQueue.createRegistry(
                self.zk_client
            )
        )
        self.trigger_events = TenantTriggerEventQueue.createRegistry(
            self.zk_client, self.connections
        )
        self.pipeline_trigger_events = (
            PipelineTriggerEventQueue.createRegistry(
                self.zk_client, self.connections
            )
        )
        self.pipeline_result_events = PipelineResultEventQueue.createRegistry(
            self.zk_client
        )

        self.general_cleanup_lock = GeneralCleanupLock(self.zk_client)
        self.semaphore_cleanup_lock = SemaphoreCleanupLock(self.zk_client)
        self.build_request_cleanup_lock = BuildRequestCleanupLock(
            self.zk_client)
        self.merge_request_cleanup_lock = MergeRequestCleanupLock(
            self.zk_client)
        self.connection_cleanup_lock = ConnectionCleanupLock(self.zk_client)
        self.node_request_cleanup_lock = NodeRequestCleanupLock(self.zk_client)

        self.keystore = KeyStorage(
            self.zk_client,
            password=self._get_key_store_password())

        self.abide = Abide()
        self.unparsed_abide = UnparsedAbideConfig()
        self.tenant_layout_state = LayoutStateStore(
            self.zk_client,
            self.layout_update_event.set)
        self.layout_providers_store = LayoutProvidersStore(
            self.zk_client, self.connections, self.system.system_id)
        self.local_layout_state = {}

        command_socket = get_default(
            self.config, 'scheduler', 'command_socket',
            '/var/lib/zuul/scheduler.socket')
        self.command_socket = commandsocket.CommandSocket(command_socket)

        self.last_reconfigured = None

        self.globals = SystemAttributes.fromConfig(self.config)
        self.ansible_manager = AnsibleManager(
            default_version=self.globals.default_ansible_version)

        self.stats_election = SchedulerStatsElection(self.zk_client)

        if not testonly:
            self.executor = self._executor_client_class(self.config, self)
            self.merger = self._merger_client_class(self.config, self)
            self.launcher = self._launcher_client_class(
                self.zk_client, self.stop_event,
                component_info=self.component_info)
            self.nodepool = nodepool.Nodepool(
                self.zk_client, self.system.system_id, self.statsd,
                scheduler=True)

    def start(self):
        super(Scheduler, self).start()

        self._command_running = True
        self.log.debug("Starting command processor")
        self.command_socket.start()
        self.command_thread = threading.Thread(target=self.runCommand,
                                               name='command')
        self.command_thread.daemon = True
        self.command_thread.start()

        self.stats_thread.start()
        self.apsched.start()
        self.times.start()
        # Start a thread to perform initial cleanup, then schedule
        # later cleanup tasks.
        self.start_cleanup_thread = threading.Thread(
            target=self.startCleanup, name='cleanup start')
        self.start_cleanup_thread.daemon = True
        self.start_cleanup_thread.start()
        self.component_info.state = self.component_info.INITIALIZING

        # Start a thread to perform background tenant layout updates
        self.layout_update_thread = threading.Thread(
            target=self.runTenantLayoutUpdates, name='layout updates')
        self.layout_update_thread.daemon = True
        self.layout_update_thread.start()

    def stop(self):
        self.log.debug("Stopping scheduler")
        self.keystore.stop()
        self._stopped = True
        self.wake_event.set()
        # Main thread, connections and layout update may be waiting
        # on the primed event
        self.primed_event.set()
        self.start_cleanup_thread.join()
        self.log.debug("Stopping apscheduler")
        self.apsched.shutdown()
        self.log.debug("Waiting for main thread")
        self.join()
        self.component_info.state = self.component_info.STOPPED
        # Don't set the stop event until after the main thread is
        # joined because doing so will terminate the ZKContext.
        self.stop_event.set()
        self.times.stop()
        self.log.debug("Stopping nodepool")
        self.nodepool.stop()
        self.log.debug("Stopping connections")
        # Layout update can reconfigure connections, so make sure
        # layout update is stopped first.
        self.log.debug("Waiting for layout update thread")
        self.layout_update_event.set()
        self.layout_update_thread.join()
        self.stopConnections()
        self.log.debug("Stopping stats thread")
        self.stats_election.cancel()
        self.stats_thread.join()
        self.stopRepl()
        self._command_running = False
        self.log.debug("Stopping command socket")
        self.command_socket.stop()
        # If we stop from the command socket we cannot join the command thread.
        if threading.current_thread() is not self.command_thread:
            self.command_thread.join()
        self.log.debug("Stopping timedb thread")
        self.times.join()
        self.log.debug("Stopping monitoring server")
        self.monitoring_server.stop()
        self.monitoring_server.join()
        self.log.debug("Disconnecting from ZooKeeper")
        self.zk_client.disconnect()
        self.log.debug("Stopping tracing")
        self.tracing.stop()
        self.executor.stop()
        if self.statsd:
            self.statsd.close()

    def runCommand(self):
        while self._command_running:
            try:
                command, args = self.command_socket.get()
                if command != '_stop':
                    self.command_map[command](*args)
            except Exception:
                self.log.exception("Exception while processing command")

    def stopConnections(self):
        self.connections.stop()

    def runStatsElection(self):
        while not self._stopped:
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
                self._runStats()
            except Exception:
                self.log.exception("Error in periodic stats:")

    def _runStats(self):
        if not self.statsd:
            return

        executor_stats_default = {
            "online": 0,
            "accepting": 0,
            "queued": 0,
            "running": 0
        }
        # Calculate the executor and merger stats
        executors_online = 0
        executors_accepting = 0
        executors_unzoned_online = 0
        executors_unzoned_accepting = 0
        # zone -> accepting|online
        zoned_executor_stats = {}
        zoned_executor_stats.setdefault(None, executor_stats_default.copy())
        mergers_online = 0

        for executor_component in self.component_registry.all("executor"):
            if executor_component.allow_unzoned:
                if executor_component.state == BaseComponent.RUNNING:
                    executors_unzoned_online += 1
                if executor_component.accepting_work:
                    executors_unzoned_accepting += 1
            if executor_component.zone:
                zone_stats = zoned_executor_stats.setdefault(
                    executor_component.zone,
                    executor_stats_default.copy())
                if executor_component.state == BaseComponent.RUNNING:
                    zone_stats["online"] += 1
                if executor_component.accepting_work:
                    zone_stats["accepting"] += 1
            # An executor with merger capabilities does also count as merger
            if executor_component.process_merge_jobs:
                mergers_online += 1

            # TODO(corvus): Remove for 5.0:
            executors_online += 1
            if executor_component.accepting_work:
                executors_accepting += 1

        # Get all builds so we can filter by state and zone
        for build_request in self.executor.executor_api.inState():
            zone_stats = zoned_executor_stats.setdefault(
                build_request.zone,
                executor_stats_default.copy())
            if build_request.state == build_request.REQUESTED:
                zone_stats['queued'] += 1
            if build_request.state in (
                    build_request.RUNNING, build_request.PAUSED):
                zone_stats['running'] += 1

        # Publish the executor stats
        self.statsd.gauge('zuul.executors.unzoned.accepting',
                          executors_unzoned_accepting)
        self.statsd.gauge('zuul.executors.unzoned.online',
                          executors_unzoned_online)
        self.statsd.gauge('zuul.executors.unzoned.jobs_running',
                          zoned_executor_stats[None]['running'])
        self.statsd.gauge('zuul.executors.unzoned.jobs_queued',
                          zoned_executor_stats[None]['queued'])
        # TODO(corvus): Remove for 5.0:
        self.statsd.gauge('zuul.executors.jobs_running',
                          zoned_executor_stats[None]['running'])
        self.statsd.gauge('zuul.executors.jobs_queued',
                          zoned_executor_stats[None]['queued'])

        for zone, stats in zoned_executor_stats.items():
            if zone is None:
                continue
            self.statsd.gauge(
                f'zuul.executors.zone.{normalize_statsd_name(zone)}.accepting',
                stats["accepting"],
            )
            self.statsd.gauge(
                f'zuul.executors.zone.{normalize_statsd_name(zone)}.online',
                stats["online"],
            )
            self.statsd.gauge(
                'zuul.executors.zone.'
                f'{normalize_statsd_name(zone)}.jobs_running',
                stats['running'])
            self.statsd.gauge(
                'zuul.executors.zone.'
                f'{normalize_statsd_name(zone)}.jobs_queued',
                stats['queued'])

        # TODO(corvus): Remove for 5.0:
        self.statsd.gauge('zuul.executors.accepting', executors_accepting)
        self.statsd.gauge('zuul.executors.online', executors_online)

        # Publish the merger stats
        for merger_component in self.component_registry.all("merger"):
            if merger_component.state == BaseComponent.RUNNING:
                mergers_online += 1

        merge_queue = 0
        merge_running = 0
        for merge_request in self.merger.merger_api.inState():
            if merge_request.state == merge_request.REQUESTED:
                merge_queue += 1
            if merge_request.state == merge_request.RUNNING:
                merge_running += 1

        self.statsd.gauge('zuul.mergers.online', mergers_online)
        self.statsd.gauge('zuul.mergers.jobs_running', merge_running)
        self.statsd.gauge('zuul.mergers.jobs_queued', merge_queue)
        self.statsd.gauge('zuul.scheduler.eventqueues.management',
                          self.reconfigure_event_queue.qsize())
        queue_base = 'zuul.scheduler.eventqueues.connection'
        conn_base = 'zuul.connection'
        for connection in self.connections.connections.values():
            queue = connection.getEventQueue()
            if queue is not None:
                self.statsd.gauge(f'{queue_base}.{connection.connection_name}',
                                  len(queue))
            if hasattr(connection, 'estimateCacheDataSize'):
                compressed_size, uncompressed_size =\
                    connection.estimateCacheDataSize()
                self.statsd.gauge(f'{conn_base}.{connection.connection_name}.'
                                  'cache.data_size_compressed',
                                  compressed_size)
                self.statsd.gauge(f'{conn_base}.{connection.connection_name}.'
                                  'cache.data_size_uncompressed',
                                  uncompressed_size)

        for tenant in self.abide.tenants.values():
            self.statsd.gauge(f"zuul.tenant.{tenant.name}.management_events",
                              len(self.management_events[tenant.name]))
            self.statsd.gauge(f"zuul.tenant.{tenant.name}.trigger_events",
                              len(self.trigger_events[tenant.name]))
            trigger_event_queues = self.pipeline_trigger_events[tenant.name]
            result_event_queues = self.pipeline_result_events[tenant.name]
            management_event_queues = (
                self.pipeline_management_events[tenant.name]
            )
            for manager in tenant.layout.pipeline_managers.values():
                pipeline = manager.pipeline
                base = f"zuul.tenant.{tenant.name}.pipeline.{pipeline.name}"
                self.statsd.gauge(f"{base}.trigger_events",
                                  len(trigger_event_queues[pipeline.name]))
                self.statsd.gauge(f"{base}.result_events",
                                  len(result_event_queues[pipeline.name]))
                self.statsd.gauge(f"{base}.management_events",
                                  len(management_event_queues[pipeline.name]))
                compressed_size, uncompressed_size =\
                    manager.state.estimateDataSize()
                self.statsd.gauge(f'{base}.data_size_compressed',
                                  compressed_size)
                self.statsd.gauge(f'{base}.data_size_uncompressed',
                                  uncompressed_size)

        self.nodepool.emitStatsTotals(self.abide)

    def startCleanup(self):
        # Run the first cleanup immediately after the first
        # reconfiguration.
        while not self.stop_event.wait(0):
            if self._stopped:
                return
            if not self.last_reconfigured:
                time.sleep(0.1)
                continue

            try:
                self._runSemaphoreCleanup()
            except Exception:
                self.log.exception("Error in semaphore cleanup:")
            if self._stopped:
                return
            try:
                self._runBuildRequestCleanup()
            except Exception:
                self.log.exception("Error in build request cleanup:")
            if self._stopped:
                return
            try:
                self._runNodeRequestCleanup()
            except Exception:
                self.log.exception("Error in node request cleanup:")
            if self._stopped:
                return

            self.apsched.add_job(self._runSemaphoreCleanup,
                                 trigger=self._semaphore_cleanup_interval)
            self.apsched.add_job(self._runBuildRequestCleanup,
                                 trigger=self._build_request_cleanup_interval)
            self.apsched.add_job(self._runMergeRequestCleanup,
                                 trigger=self._merge_request_cleanup_interval)
            self.apsched.add_job(self._runConnectionCleanup,
                                 trigger=self._connection_cleanup_interval)
            self.apsched.add_job(self._runGeneralCleanup,
                                 trigger=self._general_cleanup_interval)
            return

    def _runSemaphoreCleanup(self):
        if self.semaphore_cleanup_lock.acquire(blocking=False):
            try:
                self.log.debug("Starting semaphore cleanup")
                for tenant in self.abide.tenants.values():
                    try:
                        tenant.semaphore_handler.cleanupLeaks()
                    except Exception:
                        self.log.exception("Error in semaphore cleanup:")
            finally:
                self.semaphore_cleanup_lock.release()

    def _runNodeRequestCleanup(self):
        if self.node_request_cleanup_lock.acquire(blocking=False):
            try:
                self.log.debug("Starting node request cleanup")
                try:
                    self._cleanupNodeRequests()
                except Exception:
                    self.log.exception("Error in node request cleanup:")
            finally:
                self.node_request_cleanup_lock.release()

    def _cleanupNodeRequests(self):
        # Get all the requests in ZK that belong to us
        zk_requests = set()
        for req_id in self.nodepool.zk_nodepool.getNodeRequests():
            req = self.nodepool.zk_nodepool.getNodeRequest(req_id, cached=True)
            if req.requestor == self.system.system_id:
                zk_requests.add(req_id)
        for req_id in self.launcher.getRequestIds():
            zk_requests.add(req_id)
        # Get all the current node requests in the queues
        outstanding_requests = set()
        for tenant in self.abide.tenants.values():
            for manager in tenant.layout.pipeline_managers.values():
                with pipeline_lock(
                    self.zk_client, tenant.name, manager.pipeline.name,
                ) as lock:
                    with self.createZKContext(lock, self.log) as ctx:
                        with manager.currentContext(ctx):
                            manager.change_list.refresh(ctx)
                            manager.state.refresh(ctx, read_only=True)
                            # In case we're in the middle of a reconfig,
                            # include the old queue items.
                            for item in manager.state.getAllItems(
                                    include_old=True):
                                nrs = item.current_build_set.getNodeRequests()
                                for _, req_id in nrs:
                                    outstanding_requests.add(req_id)
        leaked_requests = zk_requests - outstanding_requests
        for req_id in leaked_requests:
            try:
                self.log.warning("Deleting leaked node request: %s", req_id)
                if self.nodepool.isNodeRequestID(req_id):
                    self.nodepool.zk_nodepool.deleteNodeRequest(req_id)
                else:
                    req = self.launcher.getRequest(req_id)
                    if req:
                        self.launcher.deleteRequest(req)
            except Exception:
                self.log.exception("Error deleting leaked node request: %s",
                                   req_id)

    def _runGeneralCleanup(self):
        self.log.debug("Starting general cleanup")
        if self.general_cleanup_lock.acquire(blocking=False):
            try:
                self._runOidcSigningKeyRotation()
                self._runConfigCacheCleanup()
                self._runExecutorApiCleanup()
                self._runMergerApiCleanup()
                self._runLayoutDataCleanup()
                self._runBlobStoreCleanup()
                self._runLeakedPipelineCleanup()
                self.maintainConnectionCache()
            except Exception:
                self.log.exception("Error in general cleanup:")
            finally:
                self.general_cleanup_lock.release()
        # This has its own locking
        self._runNodeRequestCleanup()
        self.log.debug("Finished general cleanup")

    def _runOidcSigningKeyRotation(self):
        try:
            self.log.debug("Running OIDC signing keys rotation")

            # Get the max_ttl over all the tenants
            max_ttl = max(t.max_oidc_ttl for t in self.abide.tenants.values())
            for algorithm in self.globals.oidc_supported_signing_algorithms:
                try:
                    self.keystore.rotateOidcSigningKeys(
                        algorithm,
                        self.globals.oidc_signing_key_rotation_interval,
                        max_ttl
                    )
                except exceptions.AlgorithmNotSupportedException:
                    self.log.warning(
                        "OIDC signing algorithm '%s' is not supported!",
                        algorithm)

            self.log.debug("Finished OIDC signing keys rotation")
        except Exception:
            self.log.exception("Error in OIDC signing keys rotation:")

    def _runConfigCacheCleanup(self):
        # TODO: The only way the config_object_cache can get smaller
        # is if the cleanup is run by a newly (re-)started scheduler.
        # The unparsed_config_cache cleanup is based on that,
        # therefore that is the only way for it to get smaller as
        # well.  This could potentially be improved.  If this is
        # changed, update test_config_cache_cleanup.
        try:
            self.log.debug("Starting config cache cleanup")
            cached_projects = set(
                self.unparsed_config_cache.listCachedProjects())
            active_projects = set(
                self.abide.config_object_cache.keys())
            unused_projects = cached_projects - active_projects
            for project_cname in unused_projects:
                self.unparsed_config_cache.clearCache(project_cname)
            self.log.debug("Finished config cache cleanup")
        except Exception:
            self.log.exception("Error in config cache cleanup:")

    def _runExecutorApiCleanup(self):
        try:
            self.executor.executor_api.cleanup()
        except Exception:
            self.log.exception("Error in executor API cleanup:")

    def _runMergerApiCleanup(self):
        try:
            self.merger.merger_api.cleanup()
        except Exception:
            self.log.exception("Error in merger API cleanup:")

    def _runLayoutDataCleanup(self):
        try:
            self.tenant_layout_state.cleanup()
        except Exception:
            self.log.exception("Error in layout data cleanup:")

    def _runLeakedPipelineCleanup(self):
        for tenant in self.abide.tenants.values():
            try:
                with tenant_read_lock(self.zk_client, tenant.name,
                                      self.log, blocking=False):
                    if not self.isTenantLayoutUpToDate(tenant.name):
                        self.log.debug(
                            "Skipping leaked pipeline cleanup for tenant %s",
                            tenant.name)
                        continue
                    valid_managers = tenant.layout.pipeline_managers.values()
                    valid_state_paths = set(
                        m.state.getPath() for m in valid_managers)
                    valid_event_root_paths = set(
                        PIPELINE_NAME_ROOT.format(
                            tenant=m.tenant.name, pipeline=m.pipeline.name)
                        for m in valid_managers)

                    safe_tenant = urllib.parse.quote_plus(tenant.name)
                    state_root = f"/zuul/tenant/{safe_tenant}/pipeline"
                    event_root = PIPELINE_ROOT.format(tenant=tenant.name)

                    all_state_paths = set(
                        f"{state_root}/{p}" for p in
                        self.zk_client.client.get_children(state_root))
                    all_event_root_paths = set(
                        f"{event_root}/{p}" for p in
                        self.zk_client.client.get_children(event_root))

                    leaked_state_paths = all_state_paths - valid_state_paths
                    leaked_event_root_paths = (
                        all_event_root_paths - valid_event_root_paths)

                    for leaked_path in (
                            leaked_state_paths | leaked_event_root_paths):
                        self.log.info("Removing leaked pipeline path %s",
                                      leaked_path)
                        try:
                            self.zk_client.client.delete(leaked_path,
                                                         recursive=True)
                        except Exception:
                            self.log.exception(
                                "Error removing leaked pipeline path %s in "
                                "tenant %s", leaked_path, tenant.name)
            except LockException:
                # We'll cleanup this tenant on the next iteration
                pass

    def _runBlobStoreCleanup(self):
        self.log.debug("Starting blob store cleanup")
        try:
            live_blobs = set()
            # get the start ltime so that we can filter out any
            # blobs used since this point
            start_ltime = self.zk_client.getCurrentLtime()
            # lock and refresh the pipeline
            for tenant_name in self.unparsed_abide.tenants:
                tenant = self.abide.tenants.get(tenant_name)
                if not tenant:
                    self.log.info("Skipping blob store cleanup due to "
                                  "unconfigured tenant: %s",
                                  tenant_name)
                    return
                start_wait = time.time()
                while not self.isTenantLayoutUpToDate(tenant.name):
                    # If we don't have an up-to-date config from all
                    # the tenants, it may be dangerous to clean the
                    # blob store.  Wait a reasonable amount of time
                    # for each tenant to update in the unlikely event
                    # any given tenant is not up to date.
                    time.sleep(10)
                    if (time.time() - start_wait) > 300:
                        self.log.info("Skipping blob store cleanup due to "
                                      "out of date tenant config: %s",
                                      tenant.name)
                        return
                for manager in tenant.layout.pipeline_managers.values():
                    with (pipeline_lock(
                            self.zk_client, tenant.name,
                            manager.pipeline.name) as lock,
                          self.createZKContext(lock, self.log) as ctx):
                        manager.state.refresh(ctx, read_only=True)
                        # add any blobstore references
                        for item in manager.state.getAllItems(
                                include_old=True):
                            live_blobs.update(item.getBlobKeys())
            with self.createZKContext(None, self.log) as ctx:
                blobstore = BlobStore(ctx)
                blobstore.cleanup(start_ltime, live_blobs)
            self.log.debug("Finished blob store cleanup")
        except Exception:
            self.log.exception("Error in blob store cleanup:")

    def _runBuildRequestCleanup(self):
        # If someone else is running the cleanup, skip it.
        if self.build_request_cleanup_lock.acquire(blocking=False):
            try:
                self.log.debug("Starting build request cleanup")
                self.executor.cleanupLostBuildRequests()
            finally:
                self.log.debug("Finished build request cleanup")
                self.build_request_cleanup_lock.release()

    def _runMergeRequestCleanup(self):
        # If someone else is running the cleanup, skip it.
        if self.merge_request_cleanup_lock.acquire(blocking=False):
            try:
                self.log.debug("Starting merge request cleanup")
                self.merger.cleanupLostMergeRequests()
            finally:
                self.log.debug("Finished merge request cleanup")
                self.merge_request_cleanup_lock.release()

    def _runConnectionCleanup(self):
        if self.connection_cleanup_lock.acquire(blocking=False):
            try:
                for connection in self.connections.connections.values():
                    self.log.debug("Cleaning up connection cache for: %s",
                                   connection)
                    connection.cleanupCache()
            finally:
                self.connection_cleanup_lock.release()

    def addTriggerEvent(self, driver_name, event):
        event.arrived_at_scheduler_timestamp = time.time()
        for tenant in self.abide.tenants.values():
            trusted, project = tenant.getProject(event.canonical_project_name)

            if project is None:
                continue
            self.trigger_events[tenant.name].put(driver_name, event)

    def addChangeManagementEvent(self, event):
        tenant_name = event.tenant_name
        pipeline_name = event.pipeline_name

        tenant = self.abide.tenants.get(tenant_name)
        if tenant is None:
            raise ValueError(f'Unknown tenant {event.tenant_name}')
        manager = tenant.layout.pipeline_managers.get(pipeline_name)
        if manager is None:
            raise ValueError(f'Unknown pipeline {event.pipeline_name}')

        self.pipeline_management_events[tenant_name][pipeline_name].put(
            event, needs_result=False
        )

    def reportBuildEnds(self, builds, tenant, final):
        self.sql.reportBuildEnds(builds, tenant=tenant, final=final)
        for build in builds:
            self._reportBuildStats(build)

    def reportBuildEnd(self, build, tenant, final):
        self.sql.reportBuildEnd(build, tenant=tenant, final=final)
        self._reportBuildStats(build)

    def _reportBuildStats(self, build):
        # Note, as soon as the result is set, other threads may act
        # upon this, even though the event hasn't been fully
        # processed. This could result in race conditions when e.g. skipping
        # child jobs via zuul_return. Therefore we must delay setting the
        # result to the main event loop.
        try:
            if self.statsd and build.pipeline:
                item = build.build_set.item
                manager = item.manager
                tenant = manager.tenant
                job = build.job
                change = item.getChangeForJob(job)
                jobname = job.name.replace('.', '_').replace('/', '_')
                hostname = (change.project.
                            canonical_hostname.replace('.', '_'))
                projectname = (change.project.name.
                               replace('.', '_').replace('/', '_'))
                branchname = (getattr(change, 'branch', '').
                              replace('.', '_').replace('/', '_'))
                basekey = 'zuul.tenant.%s' % tenant.name
                pipekey = '%s.pipeline.%s' % (basekey, manager.pipeline.name)
                # zuul.tenant.<tenant>.pipeline.<pipeline>.all_jobs
                key = '%s.all_jobs' % pipekey
                self.statsd.incr(key)
                jobkey = '%s.project.%s.%s.%s.job.%s' % (
                    pipekey, hostname, projectname, branchname, jobname)
                # zuul.tenant.<tenant>.pipeline.<pipeline>.project.
                #   <host>.<project>.<branch>.job.<job>.<result>
                key = '%s.%s' % (
                    jobkey,
                    'RETRY' if build.result is None else build.result
                )
                results_with_runtime = ['SUCCESS', 'FAILURE',
                                        'POST_FAILURE', 'TIMED_OUT']
                if build.result in results_with_runtime and build.start_time:
                    dt = (build.end_time - build.start_time) * 1000
                    self.statsd.timing(key, dt)
                self.statsd.incr(key)
                # zuul.tenant.<tenant>.pipeline.<pipeline>.project.
                #  <host>.<project>.<branch>.job.<job>.wait_time
                if build.start_time:
                    key = '%s.wait_time' % jobkey
                    dt = (build.start_time - build.execute_time) * 1000
                    self.statsd.timing(key, dt)
        except Exception:
            self.log.exception("Exception reporting runtime stats")

    def reconfigureTenant(self, tenant, project, trigger_event):
        log = get_annotated_logger(self.log, trigger_event)
        log.debug("Submitting tenant reconfiguration event for "
                  "%s due to event %s in project %s, ltime %s",
                  tenant.name, trigger_event, project,
                  trigger_event.zuul_event_ltime)
        branch = trigger_event and trigger_event.branch
        event = TenantReconfigureEvent(
            tenant.name, project.canonical_name, branch,
        )
        event.zuul_event_id = trigger_event.zuul_event_id
        event.branch_cache_ltimes[trigger_event.connection_name] = (
            trigger_event.branch_cache_ltime)
        event.trigger_event_ltime = trigger_event.zuul_event_ltime
        self.management_events[tenant.name].put(event, needs_result=False)

    def fullReconfigureCommandHandler(self):
        self._zuul_app.fullReconfigure()

    def smartReconfigureCommandHandler(self):
        self._zuul_app.smartReconfigure()

    def tenantReconfigureCommandHandler(self, tenant_name):
        self._zuul_app.tenantReconfigure([tenant_name])

    def zkProfileCommandHandler(self, tenant_name, pipeline_name):
        key = set([(tenant_name, pipeline_name)])
        self._profile_pipelines = self._profile_pipelines ^ key
        self.log.debug("Now profiling %s", self._profile_pipelines)

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

    def _reportInitialStats(self, tenant):
        if not self.statsd:
            return
        try:
            for manager in tenant.layout.pipeline_managers.values():
                # stats.gauges.zuul.tenant.<tenant>.pipeline.
                #    <pipeline>.current_changes
                key = 'zuul.tenant.%s.pipeline.%s' % (
                    tenant.name, manager.pipeline.name)
                self.statsd.gauge(key + '.current_changes', 0)
        except Exception:
            self.log.exception("Exception reporting initial "
                               "pipeline stats:")

    def prime(self, config):
        self.log.info("Priming scheduler config")
        start = time.monotonic()

        if self.system_config_cache.is_valid:
            self.log.info("Using system config from Zookeeper")
            self.updateSystemConfig()
        else:
            self.log.info("Creating initial system config")
            # This verifies the voluptuous schema
            self.primeSystemConfig()

        loader = configloader.ConfigLoader(
            self.connections, self.system, self.zk_client, self.globals,
            self.unparsed_config_cache, self.statsd,
            self, self.merger, self.keystore)
        new_tenants = (set(self.unparsed_abide.tenants)
                       - self.abide.tenants.keys())

        for tenant_name in new_tenants:
            with self.layout_lock[tenant_name]:
                stats_key = f'zuul.tenant.{tenant_name}'
                layout_state = self.tenant_layout_state.get(tenant_name)
                # In case we don't have a cached layout state we need to
                # acquire the write lock since we load a new tenant.
                if layout_state is None:
                    # There is no need to use the reconfig lock ID here as
                    # we are starting from an empty layout state and there
                    # should be no concurrent read locks.
                    lock_ctx = tenant_write_lock(self.zk_client,
                                                 tenant_name, self.log)
                    timer_ctx = self.statsd_timer(
                        f'{stats_key}.reconfiguration_time')
                else:
                    lock_ctx = tenant_read_lock(self.zk_client,
                                                tenant_name, self.log)
                    timer_ctx = nullcontext()

                with lock_ctx as tlock, timer_ctx:
                    # Refresh the layout state now that we are holding the lock
                    # and we can be sure it won't be changed concurrently.
                    layout_state = self.tenant_layout_state.get(tenant_name)
                    layout_uuid = layout_state and layout_state.uuid

                    if layout_state:
                        # Get the ltimes from the previous reconfiguration.
                        min_ltimes = self.tenant_layout_state.getMinLtimes(
                            layout_state)
                        branch_cache_min_ltimes = (
                            layout_state.branch_cache_min_ltimes)
                    else:
                        # Consider all caches valid if we don't have a
                        # layout state.
                        min_ltimes = defaultdict(
                            lambda: defaultdict(lambda: -1))
                        branch_cache_min_ltimes = defaultdict(lambda: -1)

                    tenant = loader.loadTenant(
                        self.abide, tenant_name, self.ansible_manager,
                        self.unparsed_abide, min_ltimes=min_ltimes,
                        layout_uuid=layout_uuid,
                        branch_cache_min_ltimes=branch_cache_min_ltimes)

                    if layout_state is None:
                        # Reconfigure only tenants w/o an existing layout state
                        with self.createZKContext(tlock, self.log) as ctx:
                            self._reconfigureTenant(
                                ctx, min_ltimes, -1, tenant)
                        self._reportInitialStats(tenant)
                    else:
                        self.local_layout_state[tenant_name] = layout_state
                    self.connections.reconfigureDrivers(tenant)

        # TODO(corvus): This isn't quite accurate; we don't really
        # know when the last reconfiguration took place.  But we
        # need to set some value here in order for the cleanup
        # start thread to know that it can proceed.  We should
        # store the last reconfiguration times in ZK and use them
        # here.
        self.last_reconfigured = int(time.time())

        duration = round(time.monotonic() - start, 3)
        self.log.info("Config priming complete (duration: %s seconds)",
                      duration)
        self.primed_event.set()
        self.wake_event.set()
        self.component_info.state = self.component_info.RUNNING

    def reconfigure(self, config, smart=False, tenants=None):
        zuul_event_id = uuid.uuid4().hex
        log = get_annotated_logger(self.log, zuul_event_id)
        log.debug("Submitting reconfiguration event")

        event = ReconfigureEvent(smart=smart, tenants=tenants)
        event.zuul_event_id = zuul_event_id
        event.zuul_event_ltime = self.zk_client.getCurrentLtime()
        event.ack_ref = threading.Event()
        self.reconfigure_event_queue.put(event)
        self.wake_event.set()

        log.debug("Waiting for reconfiguration")
        event.ack_ref.wait()
        log.debug("Reconfiguration complete")
        self.last_reconfigured = int(time.time())
        # TODOv3(jeblair): reconfigure time should be per-tenant

    def autohold(self, tenant_name, project_name, job_name, ref_filter,
                 reason, count, node_hold_expiration):
        key = (tenant_name, project_name, job_name, ref_filter)
        self.log.debug("Autohold requested for %s", key)

        request = HoldRequest()
        request.tenant = tenant_name
        request.project = project_name
        request.job = job_name
        request.ref_filter = ref_filter
        request.reason = reason
        request.max_count = count

        # Set node_hold_expiration to default if no value is supplied
        if node_hold_expiration is None:
            node_hold_expiration = self.globals.default_hold_expiration

        # Reset node_hold_expiration to max if it exceeds the max
        elif self.globals.max_hold_expiration and (
                node_hold_expiration == 0 or
                node_hold_expiration > self.globals.max_hold_expiration):
            node_hold_expiration = self.globals.max_hold_expiration

        request.node_expiration = node_hold_expiration

        # No need to lock it since we are creating a new one.
        self.nodepool.zk_nodepool.storeHoldRequest(request)

    def autohold_list(self):
        '''
        Return current hold requests as a list of dicts.
        '''
        data = []
        for request_id in self.nodepool.zk_nodepool.getHoldRequests():
            request = self.nodepool.zk_nodepool.getHoldRequest(request_id)
            if not request:
                continue
            data.append(request.toDict())
        return data

    def autohold_info(self, hold_request_id):
        '''
        Get autohold request details.

        :param str hold_request_id: The unique ID of the request to delete.
        '''
        try:
            hold_request = self.nodepool.zk_nodepool.getHoldRequest(
                hold_request_id)
        except Exception:
            self.log.exception(
                "Error retrieving autohold ID %s:", hold_request_id)
            return {}

        if hold_request is None:
            return {}
        return hold_request.toDict()

    def autohold_delete(self, hold_request_id):
        '''
        Delete an autohold request.

        :param str hold_request_id: The unique ID of the request to delete.
        '''
        hold_request = None
        try:
            hold_request = self.nodepool.zk_nodepool.getHoldRequest(
                hold_request_id)
        except Exception:
            self.log.exception(
                "Error retrieving autohold ID %s:", hold_request_id)

        if not hold_request:
            self.log.info("Ignored request to remove invalid autohold ID %s",
                          hold_request_id)
            return

        self.log.debug("Removing autohold %s", hold_request)
        try:
            self.nodepool.zk_nodepool.deleteHoldRequest(
                hold_request, self.launcher)
        except Exception:
            self.log.exception(
                "Error removing autohold request %s:", hold_request)

    def promote(self, tenant_name, pipeline_name, change_ids):
        event = PromoteEvent(tenant_name, pipeline_name, change_ids)
        event.zuul_event_id = uuid.uuid4().hex
        result = self.management_events[tenant_name].put(event)
        log = get_annotated_logger(self.log, event.zuul_event_id)
        log.debug("Waiting for promotion")
        result.wait()
        log.debug("Promotion complete")

    def dequeue(self, tenant_name, pipeline_name, project_name, change, ref):
        # We need to do some pre-processing here to get the correct
        # form of the project hostname and name based on un-known
        # inputs.
        tenant = self.abide.tenants.get(tenant_name)
        if tenant is None:
            raise ValueError(f'Unknown tenant {tenant_name}')
        (trusted, project) = tenant.getProject(project_name)
        if project is None:
            raise ValueError(f'Unknown project {project_name}')

        event = DequeueEvent(tenant_name, pipeline_name,
                             project.canonical_hostname, project.name,
                             change, ref)
        event.zuul_event_id = uuid.uuid4().hex
        log = get_annotated_logger(self.log, event.zuul_event_id)
        result = self.management_events[tenant_name].put(event)
        log.debug("Waiting for dequeue")
        result.wait()
        log.debug("Dequeue complete")

    def enqueue(self, tenant_name, pipeline_name, project_name,
                change, ref, oldrev, newrev):
        # We need to do some pre-processing here to get the correct
        # form of the project hostname and name based on un-known
        # inputs.
        tenant = self.abide.tenants.get(tenant_name)
        if tenant is None:
            raise ValueError(f'Unknown tenant {tenant_name}')
        (trusted, project) = tenant.getProject(project_name)
        if project is None:
            raise ValueError(f'Unknown project {project_name}')

        event = EnqueueEvent(tenant_name, pipeline_name,
                             project.canonical_hostname, project.name,
                             change, ref, oldrev, newrev)
        event.zuul_event_id = uuid.uuid4().hex
        log = get_annotated_logger(self.log, event.zuul_event_id)
        result = self.management_events[tenant_name].put(event)
        log.debug("Waiting for enqueue")
        result.wait()
        log.debug("Enqueue complete")

    def _get_key_store_password(self):
        try:
            return self.config["keystore"]["password"]
        except KeyError:
            raise RuntimeError("No key store password configured!")

    def runTenantLayoutUpdates(self):
        log = logging.getLogger("zuul.Scheduler.LayoutUpdate")
        # Only run this after config priming is complete
        self.primed_event.wait()
        while not self._stopped:
            self.layout_update_event.wait()
            self.layout_update_event.clear()
            if self._stopped:
                break
            with self.layout_update_lock:
                # We need to handle new and deleted tenants, so we need to
                # process all tenants currently known and the new ones.
                tenant_names = set(self.abide.tenants)
                tenant_names.update(self.unparsed_abide.tenants.keys())

                for tenant_name in tenant_names:
                    if self._stopped:
                        break
                    try:
                        if (self.unparsed_abide.ltime
                                < self.system_config_cache.ltime):
                            self.updateSystemConfig()

                        with tenant_read_lock(self.zk_client, tenant_name,
                                              log, blocking=False):
                            remote_state = self.tenant_layout_state.get(
                                tenant_name)
                            local_state = self.local_layout_state.get(
                                tenant_name)

                            if not local_state and not remote_state:
                                # The tenant may still be in the
                                # process of initial configuration
                                self.layout_update_event.set()
                                continue

                            if (local_state is None or
                                    remote_state is None or
                                    remote_state > local_state):
                                log.debug(
                                    "Local layout of tenant %s not up to date",
                                    tenant_name)
                                self.updateTenantLayout(log, tenant_name)
                                # Wake up the main thread to process any
                                # events for this tenant.
                                self.wake_event.set()
                    except LockException:
                        log.debug(
                            "Skipping layout update of locked tenant %s",
                            tenant_name)
                        self.layout_update_event.set()
                    except Exception:
                        log.exception("Error updating layout of tenant %s",
                                      tenant_name)
                        self.layout_update_event.set()
                # In case something is locked, don't busy-loop.
                time.sleep(0.1)

    def updateTenantLayout(self, log, tenant_name):
        log.debug("Updating layout of tenant %s", tenant_name)
        loader = configloader.ConfigLoader(
            self.connections, self.system, self.zk_client, self.globals,
            self.unparsed_config_cache, self.statsd, self,
            self.merger, self.keystore)
        # Since we are using the ZK 'locked' context manager (in order
        # to have a non-blocking lock) with a threading lock, we need
        # to pass -1 as the timeout value here as the default value
        # for ZK locks is None.
        with locked(self.layout_lock[tenant_name], blocking=False, timeout=-1):
            start = time.monotonic()
            log.debug("Updating local layout of tenant %s ", tenant_name)
            layout_state = self.tenant_layout_state.get(tenant_name)
            layout_uuid = layout_state and layout_state.uuid
            if layout_state:
                branch_cache_min_ltimes = layout_state.branch_cache_min_ltimes
            else:
                # We don't need the cache ltimes as the tenant was deleted
                branch_cache_min_ltimes = None

            # Get the ltimes from the previous reconfiguration.
            if layout_state:
                min_ltimes = self.tenant_layout_state.getMinLtimes(
                    layout_state)
            else:
                min_ltimes = None

            loader.loadTPCs(self.abide, self.unparsed_abide,
                            [tenant_name])
            tenant = loader.loadTenant(
                self.abide, tenant_name, self.ansible_manager,
                self.unparsed_abide, min_ltimes=min_ltimes,
                layout_uuid=layout_uuid,
                branch_cache_min_ltimes=branch_cache_min_ltimes)
            if tenant is not None:
                self.local_layout_state[tenant_name] = layout_state
                self.connections.reconfigureDrivers(tenant)
            else:
                with suppress(KeyError):
                    del self.local_layout_state[tenant_name]

        duration = round(time.monotonic() - start, 3)
        self.log.info("Local layout update complete for %s (duration: %s "
                      "seconds)", tenant_name, duration)

    def isTenantLayoutUpToDate(self, tenant_name):
        remote_state = self.tenant_layout_state.get(tenant_name)
        if remote_state is None:
            # The tenant may still be in the
            # process of initial configuration
            self.wake_event.set()
            return False
        local_state = self.local_layout_state.get(tenant_name)
        if local_state is None or remote_state > local_state:
            self.log.debug("Local layout of tenant %s not up to date",
                           tenant_name)
            self.layout_update_event.set()
            return False
        if not self.abide.hasTPCRegistry(tenant_name):
            return False
        return True

    def _checkTenantSourceConf(self, config):
        tenant_config = None
        script = False
        if self.config.has_option(
            'scheduler', 'tenant_config'):
            tenant_config = self.config.get(
                'scheduler', 'tenant_config')
        if self.config.has_option(
            'scheduler', 'tenant_config_script'):
            if tenant_config:
                raise Exception(
                    "tenant_config and tenant_config_script options "
                    "are exclusive.")
            tenant_config = self.config.get(
                'scheduler', 'tenant_config_script')
            script = True
        if not tenant_config:
            raise Exception(
                "tenant_config or tenant_config_script option "
                "is missing from the configuration.")
        return tenant_config, script

    def validateTenants(self, config, tenants_to_validate):
        self.config = config
        self.log.info("Config validation beginning")
        start = time.monotonic()

        # Use a temporary config cache for the validation
        validate_root = f"/zuul/validate/{uuid.uuid4().hex}"
        self.unparsed_config_cache = UnparsedConfigCache(self.zk_client,
                                                         validate_root)

        loader = configloader.ConfigLoader(
            self.connections, self.system, self.zk_client, self.globals,
            self.unparsed_config_cache, self.statsd,
            self, self.merger, self.keystore)
        tenant_config, script = self._checkTenantSourceConf(self.config)
        unparsed_abide = loader.readConfig(
            tenant_config,
            from_script=script,
            tenants_to_validate=tenants_to_validate)
        available_tenants = list(unparsed_abide.tenants)
        tenants_to_load = tenants_to_validate or available_tenants

        try:
            abide = Abide()
            loader.loadAuthzRules(abide, unparsed_abide)
            loader.loadSemaphores(abide, unparsed_abide)
            loader.loadTPCs(abide, unparsed_abide)
            for tenant_name in tenants_to_load:
                loader.loadTenant(abide, tenant_name, self.ansible_manager,
                                  unparsed_abide, min_ltimes=None,
                                  ignore_cat_exception=False)
        finally:
            self.zk_client.client.delete(validate_root, recursive=True)

        loading_errors = []
        for tenant in abide.tenants.values():
            for error in tenant.layout.loading_errors:
                if error.severity == SEVERITY_ERROR:
                    loading_errors.append(error.error)
        if loading_errors:
            summary = '\n\n\n'.join(loading_errors)
            raise exceptions.ConfigurationSyntaxError(
                f"Configuration errors: {summary}")

        duration = round(time.monotonic() - start, 3)
        self.log.info("Config validation complete (duration: %s seconds)",
                      duration)

    def _doReconfigureEvent(self, event):
        # This is called in the scheduler loop after another thread submits
        # a request
        reconfigured_tenants = []

        self.log.info("Reconfiguration beginning (smart=%s, tenants=%s)",
                      event.smart, event.tenants)
        start = time.monotonic()

        # Update runtime related system attributes from config
        self.config = self._zuul_app.config
        self.globals = SystemAttributes.fromConfig(self.config)
        self.ansible_manager = AnsibleManager(
            default_version=self.globals.default_ansible_version)

        with self.unparsed_abide_lock:
            loader = configloader.ConfigLoader(
                self.connections, self.system, self.zk_client, self.globals,
                self.unparsed_config_cache, self.statsd,
                self, self.merger, self.keystore)
            tenant_config, script = self._checkTenantSourceConf(self.config)
            old_unparsed_abide = self.unparsed_abide
            self.unparsed_abide = loader.readConfig(
                tenant_config, from_script=script)
            # Cache system config in Zookeeper
            self.system_config_cache.set(self.unparsed_abide, self.globals)

            # We need to handle new and deleted tenants, so we need to process
            # all tenants currently known and the new ones.
            tenant_names = {t for t in self.abide.tenants}
            tenant_names.update(self.unparsed_abide.tenants.keys())

            # Remove TPCs of deleted tenants
            deleted_tenants = tenant_names.difference(
                self.unparsed_abide.tenants.keys())
            for tenant_name in deleted_tenants:
                self.abide.clearTPCRegistry(tenant_name)

            loader.loadAuthzRules(self.abide, self.unparsed_abide)
            loader.loadSemaphores(self.abide, self.unparsed_abide)
            loader.loadTPCs(self.abide, self.unparsed_abide)
        # Note end of unparsed abide lock here.

        if event.smart:
            # Consider caches always valid
            min_ltimes = defaultdict(
                lambda: defaultdict(lambda: -1))
            # Consider all project branch caches valid.
            branch_cache_min_ltimes = defaultdict(lambda: -1)
        else:
            # Consider caches valid if the cache ltime >= event ltime
            min_ltimes = defaultdict(
                lambda: defaultdict(lambda: event.zuul_event_ltime))
            # Invalidate the branch cache
            for connection in self.connections.connections.values():
                if hasattr(connection, 'clearBranchCache'):
                    if event.tenants:
                        # Only clear the projects used by this
                        # tenant (zuul-web won't be able to load
                        # any tenants that we don't immediately
                        # reconfigure after clearing)
                        for tenant_name in event.tenants:
                            projects = [
                                tpc.project.name for tpc in
                                self.abide.getAllTPCs(tenant_name)
                            ]
                            connection.clearBranchCache(projects)
                    else:
                        # Clear all projects since we're reloading
                        # all tenants.
                        connection.clearBranchCache()
            ltime = self.zk_client.getCurrentLtime()
            # Consider the branch cache valid only after we
            # cleared it
            branch_cache_min_ltimes = defaultdict(lambda: ltime)

        for tenant_name in tenant_names:
            reconfig_time = None
            with self.layout_lock[tenant_name]:
                if event.smart:
                    old_tenant = old_unparsed_abide.tenants.get(tenant_name)
                    new_tenant = self.unparsed_abide.tenants.get(tenant_name)
                    if old_tenant == new_tenant:
                        continue
                if event.tenants and tenant_name not in event.tenants:
                    continue

                old_tenant = self.abide.tenants.get(tenant_name)

                stats_key = f'zuul.tenant.{tenant_name}'
                with (tenant_write_lock(
                        self.zk_client, tenant_name, self.log,
                        identifier=RECONFIG_LOCK_ID) as lock,
                      self.statsd_timer(f'{stats_key}.reconfiguration_time')):
                    reconfig_time = time.time()
                    tenant = loader.loadTenant(
                        self.abide, tenant_name, self.ansible_manager,
                        self.unparsed_abide, min_ltimes=min_ltimes,
                        branch_cache_min_ltimes=branch_cache_min_ltimes)
                    reconfigured_tenants.append(tenant_name)
                    with self.createZKContext(lock, self.log) as ctx:
                        if tenant is not None:
                            self._reconfigureTenant(ctx, min_ltimes,
                                                    event.zuul_event_ltime,
                                                    tenant, old_tenant)
                        else:
                            self._reconfigureDeleteTenant(ctx, old_tenant)
                            with suppress(KeyError):
                                del self.tenant_layout_state[tenant_name]
                            with suppress(KeyError):
                                del self.local_layout_state[tenant_name]
            if reconfig_time:
                desc = "Tenant reconfiguration due to external request"
                self.sql.reportSystemEvent(
                    tenant_name,
                    event_id=event.zuul_event_id,
                    event_time=reconfig_time,
                    event_type='tenant reconfiguration',
                    description=desc,
                )

        duration = round(time.monotonic() - start, 3)
        self.log.info("Reconfiguration complete (smart: %s, tenants: %s, "
                      "duration: %s seconds)", event.smart, event.tenants,
                      duration)
        if event.smart:
            self.log.info("Reconfigured tenants: %s", reconfigured_tenants)

    def _doTenantReconfigureEvent(self, event):
        log = get_annotated_logger(self.log, event.zuul_event_id)
        # This is called in the scheduler loop after another thread submits
        # a request
        if self.unparsed_abide.ltime < self.system_config_cache.ltime:
            self.updateSystemConfig()

        log.info("Tenant reconfiguration beginning for %s due to "
                 "projects %s", event.tenant_name, event.project_branches)
        start = time.monotonic()
        # Consider all caches valid (min. ltime -1) except for the
        # changed project-branches.
        min_ltimes = defaultdict(lambda: defaultdict(lambda: -1))
        for project_name, branch_name in event.project_branches:
            if branch_name is None:
                min_ltimes[project_name] = defaultdict(
                    lambda: event.zuul_event_ltime)
            else:
                min_ltimes[project_name][
                    branch_name
                ] = event.zuul_event_ltime

        # Consider all branch caches valid except for the ones where
        # the events provides a minimum ltime.
        branch_cache_min_ltimes = defaultdict(lambda: -1)
        for connection_name, ltime in event.branch_cache_ltimes.items():
            branch_cache_min_ltimes[connection_name] = ltime

        loader = configloader.ConfigLoader(
            self.connections, self.system, self.zk_client, self.globals,
            self.unparsed_config_cache, self.statsd,
            self, self.merger, self.keystore)
        loader.loadTPCs(self.abide, self.unparsed_abide,
                        [event.tenant_name])
        stats_key = f'zuul.tenant.{event.tenant_name}'

        reconfig_time = None
        with self.layout_lock[event.tenant_name]:
            old_tenant = self.abide.tenants.get(event.tenant_name)
            with (tenant_write_lock(
                    self.zk_client, event.tenant_name, self.log,
                    identifier=RECONFIG_LOCK_ID) as lock,
                  self.statsd_timer(f'{stats_key}.reconfiguration_time')):
                log.debug("Loading tenant %s", event.tenant_name)
                loader.loadTenant(
                    self.abide, event.tenant_name, self.ansible_manager,
                    self.unparsed_abide, min_ltimes=min_ltimes,
                    branch_cache_min_ltimes=branch_cache_min_ltimes)
                tenant = self.abide.tenants[event.tenant_name]
                reconfig_time = time.time()
                with self.createZKContext(lock, self.log) as ctx:
                    self._reconfigureTenant(ctx, min_ltimes,
                                            event.trigger_event_ltime,
                                            tenant, old_tenant)
        duration = round(time.monotonic() - start, 3)
        log.info("Tenant reconfiguration complete for %s (duration: %s "
                 "seconds)", event.tenant_name, duration)
        if reconfig_time:
            branches = [f'{b[0]}@{b[1]}' for b in event.project_branches]
            branches = ', '.join(branches)
            desc = f"Tenant reconfiguration due to {branches}"
            self.sql.reportSystemEvent(
                event.tenant_name,
                event_id=event.zuul_event_id,
                event_time=reconfig_time,
                event_type='tenant reconfiguration',
                description=desc,
            )

    def _reenqueueGetProject(self, tenant, item, change):
        project = change.project
        # Attempt to get the same project as the one passed in.  If
        # the project is now found on a different connection or if it
        # is no longer available (due to a connection being removed),
        # return None.
        (trusted, new_project) = tenant.getProject(project.canonical_name)
        if new_project:
            if project.connection_name != new_project.connection_name:
                return None
            return new_project

        if item.live:
            return None

        # If this is a non-live item we may be looking at a "foreign"
        # project, i.e. one which is not defined in the config but
        # accessible by one of the configured connections. In this
        # case look up the source by canonical hostname (under the
        # assumption that hasn't changed) and get the project from
        # that source.
        source = self.connections.getSourceByCanonicalHostname(
            project.canonical_hostname)
        if not source:
            return None
        return source.getProject(project.name)

    def _reenqueuePipeline(self, tenant, manager, context):
        pipeline = manager.pipeline
        self.log.debug("Re-enqueueing changes for pipeline %s",
                       pipeline.name)
        # TODO(jeblair): This supports an undocument and
        # unanticipated hack to create a static window.  If we
        # really want to support this, maybe we should have a
        # 'static' type?  But it may be in use in the wild, so we
        # should allow this at least until there's an upgrade
        # path.
        if (pipeline.window and
            pipeline.window_increase_type == 'exponential' and
            pipeline.window_decrease_type == 'exponential' and
            pipeline.window_increase_factor == 1 and
            pipeline.window_decrease_factor == 1):
            static_window = True
        else:
            static_window = False

        items_to_remove = []
        builds_to_cancel = []
        requests_to_cancel = []
        for shared_queue in list(manager.state.old_queues):
            last_head = None
            for item in shared_queue.queue:
                # If the old item ahead made it in, re-enqueue
                # this one behind it.
                new_projects = [self._reenqueueGetProject(
                    tenant, item, change) for change in item.changes]
                if item.item_ahead in items_to_remove:
                    old_item_ahead = None
                    item_ahead_valid = False
                else:
                    old_item_ahead = item.item_ahead
                    item_ahead_valid = True
                with item.activeContext(context):
                    item.item_ahead = None
                    item.items_behind = []
                    reenqueued = False
                    if all(new_projects):
                        for change_index, change in enumerate(item.changes):
                            change.project = new_projects[change_index]
                        item.queue = None
                        if not old_item_ahead or not last_head:
                            last_head = item
                        try:
                            reenqueued = manager.reEnqueueItem(
                                item, last_head, old_item_ahead,
                                item_ahead_valid=item_ahead_valid)
                        except Exception:
                            log = get_annotated_logger(self.log, item.event)
                            log.exception(
                                "Exception while re-enqueing item %s", item)
                if not reenqueued:
                    items_to_remove.append(item)

            # Attempt to keep window sizes from shrinking where possible
            project, branch = shared_queue.project_branches[0]
            new_queue = manager.state.getQueue(project, branch)
            if new_queue and shared_queue.window and (not static_window):
                new_queue.updateAttributes(
                    context, window=max(shared_queue.window,
                                        new_queue.window_floor))
            manager.state.removeOldQueue(context, shared_queue)
        for item in items_to_remove:
            log = get_annotated_logger(self.log, item.event)
            log.info("Removing item %s during reconfiguration", item)
            for build in item.current_build_set.getBuilds():
                builds_to_cancel.append(build)
            for request_job, request in \
                item.current_build_set.getNodeRequests():
                requests_to_cancel.append(
                    (
                        item.current_build_set,
                        request,
                        request_job,
                    )
                )
            try:
                self.sql.reportBuildsetEnd(
                    item.current_build_set, 'dequeue',
                    final=False, result='DEQUEUED')
            except Exception:
                log.exception(
                    "Error reporting buildset completion to DB:")

        for build in builds_to_cancel:
            self.log.info(
                "Canceling build %s during reconfiguration" % (build,))
            self.cancelJob(build.build_set, build.job, build=build)
        for build_set, request, request_job in requests_to_cancel:
            self.log.info(
                "Canceling node request %s during reconfiguration",
                request)
            self.cancelJob(build_set, request_job)

    def _reconfigureTenant(self, context, min_ltimes,
                           last_reconfigure_event_ltime, tenant,
                           old_tenant=None):
        # This is called from _doReconfigureEvent while holding the
        # layout lock
        if old_tenant:
            for name, old_manager in \
                    old_tenant.layout.pipeline_managers.items():
                new_manager = tenant.layout.pipeline_managers.get(name)
                if not new_manager:
                    with old_manager.currentContext(context):
                        try:
                            self._reconfigureDeletePipeline(old_manager)
                        except Exception:
                            self.log.exception(
                                "Failed to cleanup deleted pipeline %s:",
                                old_manager.pipeline)

        self.management_events[tenant.name].initialize()
        self.trigger_events[tenant.name].initialize()
        self.connections.reconfigureDrivers(tenant)

        # TODOv3(jeblair): remove postconfig calls?
        for manager in tenant.layout.pipeline_managers.values():
            pipeline = manager.pipeline
            self.pipeline_management_events[tenant.name][
                pipeline.name].initialize()
            self.pipeline_trigger_events[tenant.name][
                pipeline.name].initialize()
            self.pipeline_result_events[tenant.name
                ][pipeline.name].initialize()
            for trigger in pipeline.triggers:
                trigger.postConfig(pipeline)
            for reporter in pipeline.actions:
                reporter.postConfig()
            # Emit an event to trigger a pipeline run after the
            # reconfiguration.
            event = PipelinePostConfigEvent()
            self.pipeline_management_events[tenant.name][pipeline.name].put(
                event, needs_result=False)

        # Assemble a new list of min. ltimes of the project branch caches.
        branch_cache_min_ltimes = {
            s.connection.connection_name:
                s.getProjectBranchCacheLtime()
            for s in self.connections.getSources()
        }

        # Make sure last_reconfigure_event_ltime never goes backward
        old_layout_state = self.tenant_layout_state.get(tenant.name)
        if old_layout_state:
            if (old_layout_state.last_reconfigure_event_ltime >
                last_reconfigure_event_ltime):
                self.log.debug("Setting layout state last reconfigure ltime "
                               "to previous ltime %s which is newer than %s",
                               old_layout_state.last_reconfigure_event_ltime,
                               last_reconfigure_event_ltime)
                last_reconfigure_event_ltime =\
                    old_layout_state.last_reconfigure_event_ltime
        if last_reconfigure_event_ltime < 0:
            last_reconfigure_event_ltime = self.zk_client.getCurrentLtime()
            self.log.debug("Setting layout state last reconfigure ltime "
                           "to current ltime %s", last_reconfigure_event_ltime)
        else:
            self.log.debug("Setting layout state last reconfigure ltime "
                           "to %s", last_reconfigure_event_ltime)
        layout_state = LayoutState(
            tenant_name=tenant.name,
            hostname=self.hostname,
            last_reconfigured=int(time.time()),
            last_reconfigure_event_ltime=last_reconfigure_event_ltime,
            uuid=tenant.layout.uuid,
            branch_cache_min_ltimes=branch_cache_min_ltimes,
        )
        # Write the new provider configs
        self.layout_providers_store.set(context, tenant.name,
                                        tenant.layout.providers.values())
        # Save the min_ltimes which are sharded before we atomically
        # update the layout state.
        self.tenant_layout_state.setMinLtimes(layout_state, min_ltimes)
        # We need to update the local layout state before the remote state,
        # to avoid race conditions in the layout changed callback.
        self.local_layout_state[tenant.name] = layout_state
        self.tenant_layout_state[tenant.name] = layout_state

    def _reconfigureDeleteTenant(self, context, tenant):
        # Called when a tenant is deleted during reconfiguration
        self.log.info("Removing tenant %s during reconfiguration" %
                      (tenant,))
        for manager in tenant.layout.pipeline_managers.values():
            with manager.currentContext(context):
                try:
                    self._reconfigureDeletePipeline(manager)
                except Exception:
                    self.log.exception(
                        "Failed to cleanup deleted pipeline %s:",
                        manager.pipeline)

        # Delete the tenant root path for this tenant in ZooKeeper to remove
        # all tenant specific event queues
        try:
            self.zk_client.client.delete(f"{TENANT_ROOT}/{tenant.name}",
                                         recursive=True)
        except NotEmptyError:
            # In case a build result has been submitted during the
            # reconfiguration, this cleanup will fail. We handle this in a
            # periodic cleanup job.
            pass

    def _reconfigureDeletePipeline(self, manager):
        self.log.info("Removing pipeline %s during reconfiguration",
                      manager.pipeline)

        ctx = manager.current_context
        manager.state.refresh(ctx)

        builds_to_cancel = []
        requests_to_cancel = []
        for item in manager.state.getAllItems():
            with item.activeContext(manager.current_context):
                item.item_ahead = None
                item.items_behind = []
            self.log.info(
                "Removing item %s during reconfiguration" % (item,))
            for build in item.current_build_set.getBuilds():
                builds_to_cancel.append(build)
            for request_job, request in \
                item.current_build_set.getNodeRequests():
                requests_to_cancel.append(
                    (
                        item.current_build_set,
                        request,
                        request_job,
                    )
                )
            try:
                self.sql.reportBuildsetEnd(
                    item.current_build_set, 'dequeue',
                    final=False, result='DEQUEUED')
            except Exception:
                self.log.exception(
                    "Error reporting buildset completion to DB:")

        for build in builds_to_cancel:
            self.log.info(
                "Canceling build %s during reconfiguration", build)
            try:
                self.cancelJob(build.build_set, build.job,
                               build=build, force=True)
            except Exception:
                self.log.exception(
                    "Error canceling build %s during reconfiguration", build)
        for build_set, request, request_job in requests_to_cancel:
            self.log.info(
                "Canceling node request %s during reconfiguration", request)
            try:
                self.cancelJob(build_set, request_job, force=True)
            except Exception:
                self.log.exception(
                    "Error canceling node request %s during reconfiguration",
                    request)

        # Delete the pipeline event root path in ZooKeeper to remove
        # all pipeline specific event queues.
        try:
            self.zk_client.client.delete(
                PIPELINE_NAME_ROOT.format(
                    tenant=manager.tenant.name,
                    pipeline=manager.pipeline.name),
                recursive=True)
        except Exception:
            # In case a pipeline event has been submitted during
            # reconfiguration this cleanup will fail.
            self.log.exception(
                "Error removing event queues for deleted pipeline %s in "
                "tenant %s", manager.pipeline.name, manager.tenant.name)

        # Delete the pipeline root path in ZooKeeper to remove all pipeline
        # state.
        try:
            self.zk_client.client.delete(manager.state.getPath(),
                                         recursive=True)
        except Exception:
            self.log.exception(
                "Error removing state for deleted pipeline %s in tenant %s",
                manager.pipeline.name, manager.tenant.name)

    def _doPromoteEvent(self, event):
        tenant = self.abide.tenants.get(event.tenant_name)
        manager = tenant.layout.pipeline_managers[event.pipeline_name]
        change_ids = [c.split(',') for c in event.change_ids]
        items_to_enqueue = []
        change_queue = None

        # A list of (queue, items); the queue should be promoted
        # within the pipeline, and the items should be promoted within
        # the queue.
        promote_operations = OrderedDict()
        for number, patchset in change_ids:
            found = False
            for shared_queue in manager.state.queues:
                for item in shared_queue.queue:
                    if not item.live:
                        continue
                    for item_change in item.changes:
                        # We cannot use the getChangeKey method to look
                        # up the change directly from the project source
                        # as we are missing the project information.
                        # Convert the change number and patchset from
                        # our enqueued items to string to compare them
                        # with the event data.
                        if (str(item_change.number) == number and
                            str(item_change.patchset) == patchset):
                            promote_operations.setdefault(
                                shared_queue, []).append(item)
                            found = True
                            break
                if found:
                    break
            if not found:
                raise Exception("Unable to find %s,%s" % (number, patchset))

        # Reverse the promote operations so that we promote the first
        # change queue last (which will put it at the top).
        for change_queue, items_to_enqueue in\
            reversed(promote_operations.items()):
            # We can leave changes in the pipeline as long as the head
            # remains identical; as soon as we make a change, we have
            # to re-enqueue everything behind it in order to ensure
            # dependencies are correct.
            head_same = True
            for item in change_queue.queue[:]:
                if item.live and item == items_to_enqueue[0] and head_same:
                    # We can skip this one and leave it in place
                    items_to_enqueue.pop(0)
                    continue
                elif not item.live:
                    # Ignore this; if the actual item behind it is
                    # dequeued, it will get cleaned up eventually.
                    continue
                if item not in items_to_enqueue:
                    items_to_enqueue.append(item)
                head_same = False
                manager.cancelJobs(item)
                manager.dequeueItem(item)

            for item in items_to_enqueue:
                for item_change in item.changes:
                    manager.addChange(
                        item_change, item.event,
                        enqueue_time=item.enqueue_time,
                        quiet=True,
                        ignore_requirements=True)
            # Regardless, move this shared change queue to the head.
            manager.state.promoteQueue(change_queue)

    def _doDequeueEvent(self, event):
        tenant = self.abide.tenants.get(event.tenant_name)
        if tenant is None:
            raise ValueError('Unknown tenant %s' % event.tenant_name)
        manager = tenant.layout.pipeline_managers.get(event.pipeline_name)
        if manager is None:
            raise ValueError('Unknown pipeline %s' % event.pipeline_name)
        manager = tenant.layout.pipeline_managers.get(event.pipeline_name)
        canonical_name = event.project_hostname + '/' + event.project_name
        (trusted, project) = tenant.getProject(canonical_name)
        if project is None:
            raise ValueError('Unknown project %s' % event.project_name)
        change_key = project.source.getChangeKey(event)
        change = project.source.getChange(change_key, event=event)
        if change.project.name != project.name:
            if event.change:
                item = 'Change %s' % event.change
            else:
                item = 'Ref %s' % event.ref
            raise Exception('%s does not belong to project "%s"'
                            % (item, project.name))
        for shared_queue in manager.state.queues:
            for item in shared_queue.queue:
                if not item.live:
                    continue
                for item_change in item.changes:
                    if item_change.project != change.project:
                        continue
                    if (isinstance(item_change, Change) and
                        item_change.number == change.number and
                        item_change.patchset == change.patchset) or\
                        (item_change.ref == change.ref):
                        manager.removeItem(item)
                        return
        raise Exception("Unable to find shared change queue for %s:%s" %
                        (event.project_name,
                         event.change or event.ref))

    def _doEnqueueEvent(self, event):
        tenant = self.abide.tenants.get(event.tenant_name)
        if tenant is None:
            raise ValueError(f'Unknown tenant {event.tenant_name}')
        manager = tenant.layout.pipeline_managers.get(event.pipeline_name)
        if manager is None:
            raise ValueError(f'Unknown pipeline {event.pipeline_name}')
        canonical_name = event.project_hostname + '/' + event.project_name
        (trusted, project) = tenant.getProject(canonical_name)
        if project is None:
            raise ValueError(f'Unknown project {event.project_name}')
        try:
            change_key = project.source.getChangeKey(event)
            change = project.source.getChange(change_key,
                                              event=event, refresh=True)
        except Exception as exc:
            raise ValueError('Unknown change') from exc

        if change.project.name != project.name:
            raise Exception(
                f'Change {change} does not belong to project "{project.name}"')
        self.log.debug("Event %s for change %s was directly assigned "
                       "to pipeline %s", event, change, self)
        manager.addChange(change, event, ignore_requirements=True)

    def _doSupercedeEvent(self, event):
        tenant = self.abide.tenants[event.tenant_name]
        manager = tenant.layout.pipeline_managers[event.pipeline_name]
        canonical_name = f"{event.project_hostname}/{event.project_name}"
        trusted, project = tenant.getProject(canonical_name)
        if project is None:
            return
        change_key = project.source.getChangeKey(event)
        change = project.source.getChange(change_key, event=event)
        for shared_queue in manager.state.queues:
            for item in shared_queue.queue:
                if not item.live:
                    continue
                for item_change in item.changes:
                    if item_change.project != change.project:
                        continue
                    if ((isinstance(item_change, Change)
                         and item_change.number == change.number
                         and item_change.patchset == change.patchset
                        ) or (item_change.ref == change.ref)):
                        log = get_annotated_logger(self.log, item.event)
                        log.info("Item %s is superceded, dequeuing", item)
                        manager.removeItem(item)
                        return

    def _doSemaphoreReleaseEvent(self, event, tenant, notified):
        semaphore = tenant.layout.getSemaphore(
            self.abide, event.semaphore_name)
        if semaphore.global_scope:
            tenants = [t for t in self.abide.tenants.values()
                       if event.semaphore_name in t.global_semaphores]
        else:
            tenants = [tenant]
        for tenant in tenants:
            if tenant.name in notified:
                continue
            for pipeline_name in tenant.layout.pipeline_managers.keys():
                event = PipelineSemaphoreReleaseEvent()
                self.pipeline_management_events[
                    tenant.name][pipeline_name].put(
                        event, needs_result=False)
            notified.add(tenant.name)

    def _areAllBuildsComplete(self):
        self.log.debug("Checking if all builds are complete")
        waiting = False
        for tenant in self.abide.tenants.values():
            for manager in tenant.layout.pipeline_managers.values():
                for item in manager.state.getAllItems():
                    for build in item.current_build_set.getBuilds():
                        if build.result is None:
                            self.log.debug("%s waiting on %s" %
                                           (manager, build))
                            waiting = True
        if not waiting:
            self.log.debug("All builds are complete")
            return True
        return False

    def run(self):
        if self.statsd:
            self.log.debug("Statsd enabled")
        else:
            self.log.debug("Statsd not configured")
        if self.wait_for_init:
            self.log.debug("Waiting for tenant initialization")
            self.primed_event.wait()
        while True:
            self.log.debug("Run handler sleeping")
            self.wake_event.wait()
            self.wake_event.clear()
            if self._stopped:
                self.log.debug("Run handler stopping")
                return
            self.log.debug("Run handler awake")
            self.run_handler_lock.acquire()
            with self.statsd_timer("zuul.scheduler.run_handler"):
                try:
                    self._run()
                except Exception:
                    self.log.exception("Exception in run handler:")
                    # There may still be more events to process
                    self.wake_event.set()
                finally:
                    self.run_handler_lock.release()

    def _run(self):
        if not self._stopped:
            self.process_reconfigure_queue()

        if self.unparsed_abide.ltime < self.system_config_cache.ltime:
            self.updateSystemConfig()

        for tenant_name in self.unparsed_abide.tenants:
            if self._stopped:
                break

            tenant = self.abide.tenants.get(tenant_name)
            if not tenant:
                continue

            tenant_state = self.event_watcher.tenant_state[tenant_name]

            # This will also forward events for the pipelines
            # (e.g. enqueue or dequeue events) to the matching
            # pipeline event queues that are processed afterwards.
            self.process_tenant_management_queue(tenant)

            if self._stopped:
                break

            try:
                with tenant_read_lock(
                        self.zk_client, tenant_name, self.log, blocking=False
                ) as tlock:
                    if not self.isTenantLayoutUpToDate(tenant_name):
                        continue
                    tlock.watch_for_contenders()

                    # Get tenant again, as it might have been updated
                    # by a tenant reconfig or layout change.
                    tenant = self.abide.tenants[tenant_name]
                    if (not self._stopped and
                        not tenant_state.trigger_queue_paused):
                        # This will forward trigger events to pipeline
                        # event queues that are processed below.
                        self.process_tenant_trigger_queue(tenant)
                    elif tenant_state.trigger_queue_paused:
                        self.log.info("Trigger queue paused for tenant %s",
                                      tenant.name)

                    self.process_pipelines(tenant, tlock)
            except PendingReconfiguration:
                self.log.debug("Stopping tenant %s pipeline processing due to "
                               "pending reconfig", tenant_name)
                self.wake_event.set()
            except LockException:
                self.log.debug("Skipping locked tenant %s",
                               tenant.name)
                remote_state = self.tenant_layout_state.get(
                    tenant_name)
                local_state = self.local_layout_state.get(
                    tenant_name)
                if (remote_state is None or
                    local_state is None or
                    remote_state > local_state):
                    # Let's keep looping until we've updated to the
                    # latest tenant layout.
                    self.wake_event.set()
            except Exception:
                self.log.exception("Exception processing tenant %s:",
                                   tenant_name)
                # There may still be more events to process
                self.wake_event.set()

    def primeSystemConfig(self):
        # Strictly speaking, we don't need this lock since it's
        # guaranteed to be single-threaded, but it's used here for
        # consistency and future-proofing.
        with self.unparsed_abide_lock:
            loader = configloader.ConfigLoader(
                self.connections, self.system, self.zk_client, self.globals,
                self.unparsed_config_cache, self.statsd,
                self, self.merger, self.keystore)
            tenant_config, script = self._checkTenantSourceConf(self.config)
            self.unparsed_abide = loader.readConfig(
                tenant_config, from_script=script)
            self.system_config_cache.set(self.unparsed_abide, self.globals)

            loader.loadAuthzRules(self.abide, self.unparsed_abide)
            loader.loadSemaphores(self.abide, self.unparsed_abide)
            loader.loadTPCs(self.abide, self.unparsed_abide)

    def updateSystemConfig(self):
        with self.unparsed_abide_lock:
            if self.unparsed_abide.ltime >= self.system_config_cache.ltime:
                # We have updated our local unparsed abide since the
                # caller last checked.
                return
            self.log.debug("Updating system config")
            self.unparsed_abide, self.globals = self.system_config_cache.get()
            self.ansible_manager = AnsibleManager(
                default_version=self.globals.default_ansible_version)
            loader = configloader.ConfigLoader(
                self.connections, self.system, self.zk_client, self.globals,
                self.unparsed_config_cache, self.statsd,
                self, self.merger, self.keystore)

            tenant_names = set(self.abide.tenants)
            deleted_tenants = tenant_names.difference(
                self.unparsed_abide.tenants.keys())

            # Remove TPCs of deleted tenants
            for tenant_name in deleted_tenants:
                self.abide.clearTPCRegistry(tenant_name)

            loader.loadAuthzRules(self.abide, self.unparsed_abide)
            loader.loadSemaphores(self.abide, self.unparsed_abide)
            loader.loadTPCs(self.abide, self.unparsed_abide)

    def process_pipelines(self, tenant, tenant_lock):
        for manager in tenant.layout.pipeline_managers.values():
            pipeline = manager.pipeline
            if self._stopped:
                return
            self.abortIfPendingReconfig(tenant_lock)
            stats_key = f'zuul.tenant.{tenant.name}.pipeline.{pipeline.name}'
            try:
                with (pipeline_lock(
                        self.zk_client, tenant.name, pipeline.name,
                        blocking=False) as lock,
                      self.createZKContext(lock, self.log) as ctx):
                    self.log.debug("Processing pipeline %s in tenant %s",
                                   pipeline.name, tenant.name)
                    with manager.currentContext(ctx):
                        if ((tenant.name, pipeline.name) in
                            self._profile_pipelines):
                            ctx.profile = True
                        with self.statsd_timer(f'{stats_key}.handling'):
                            refreshed = self._process_pipeline(
                                tenant, tenant_lock, manager)
                    # Update pipeline summary for zuul-web
                    if refreshed:
                        manager.summary.update(ctx, self.globals)
                        if self.statsd:
                            self._contextStats(ctx, stats_key)
            except PendingReconfiguration:
                # Don't do anything else here and let the next level up
                # handle it.
                raise
            except LockException:
                self.log.debug("Skipping locked pipeline %s in tenant %s",
                               pipeline.name, tenant.name)
                try:
                    # In case this pipeline is locked for some reason
                    # other than processing events, we need to return
                    # to it to process them.
                    if self._pipelineHasEvents(tenant, manager):
                        self.wake_event.set()
                except Exception:
                    self.log.exception(
                        "Exception checking events for pipeline "
                        "%s in tenant %s",
                        pipeline.name, tenant.name)
            except Exception:
                self.log.exception(
                    "Exception processing pipeline %s in tenant %s",
                    pipeline.name, tenant.name)

    def _contextStats(self, ctx, stats_key):
        self.statsd.timing(f'{stats_key}.read_time',
                           ctx.cumulative_read_time * 1000)
        self.statsd.timing(f'{stats_key}.write_time',
                           ctx.cumulative_write_time * 1000)
        self.statsd.gauge(f'{stats_key}.read_objects',
                          ctx.cumulative_read_objects)
        self.statsd.gauge(f'{stats_key}.write_objects',
                          ctx.cumulative_write_objects)
        self.statsd.gauge(f'{stats_key}.read_znodes',
                          ctx.cumulative_read_znodes)
        self.statsd.gauge(f'{stats_key}.write_znodes',
                          ctx.cumulative_write_znodes)
        self.statsd.gauge(f'{stats_key}.read_bytes',
                          ctx.cumulative_read_bytes)
        self.statsd.gauge(f'{stats_key}.write_bytes',
                          ctx.cumulative_write_bytes)

    def _pipelineHasEvents(self, tenant, manager):
        return any((
            self.pipeline_trigger_events[
                tenant.name][manager.pipeline.name].hasEvents(),
            self.pipeline_result_events[
                tenant.name][manager.pipeline.name].hasEvents(),
            self.pipeline_management_events[
                tenant.name][manager.pipeline.name].hasEvents(),
            manager.state.isDirty(self.zk_client.client),
        ))

    def abortIfPendingReconfig(self, tenant_lock):
        if tenant_lock.contender_present(RECONFIG_LOCK_ID):
            raise PendingReconfiguration()

    def _process_pipeline(self, tenant, tenant_lock, manager):
        # Return whether or not we refreshed the pipeline.

        pipeline = manager.pipeline
        # We only need to process the pipeline if there are
        # outstanding events.
        if not self._pipelineHasEvents(tenant, manager):
            self.log.debug("No events to process for pipeline %s in tenant %s",
                           pipeline.name, tenant.name)
            return False

        stats_key = f'zuul.tenant.{tenant.name}.pipeline.{pipeline.name}'
        ctx = manager.current_context
        with self.statsd_timer(f'{stats_key}.refresh'):
            manager.change_list.refresh(ctx)
            manager.summary.refresh(ctx)
            manager.state.refresh(ctx)

        manager.state.setDirty(self.zk_client.client)
        if manager.state.old_queues:
            self._reenqueuePipeline(tenant, manager, ctx)

        tenant_state = self.event_watcher.tenant_state[tenant.name]

        with self.statsd_timer(f'{stats_key}.event_process'):
            self.process_pipeline_management_queue(
                tenant, tenant_lock, manager)
            # Give result events priority -- they let us stop builds,
            # whereas trigger events cause us to execute builds.
            if not tenant_state.result_queue_paused:
                self.process_pipeline_result_queue(
                    tenant, tenant_lock, manager)
            else:
                self.log.info("Result queue paused for tenant %s",
                              tenant.name)
            if not tenant_state.trigger_queue_paused:
                self.process_pipeline_trigger_queue(
                    tenant, tenant_lock, manager)
            else:
                self.log.info("Trigger queue paused for tenant %s",
                              tenant.name)
        self.abortIfPendingReconfig(tenant_lock)
        try:
            with self.statsd_timer(f'{stats_key}.process'):
                while not self._stopped and manager.processQueue(
                        tenant_lock):
                    self.abortIfPendingReconfig(tenant_lock)
            manager.state.cleanup(ctx)
        except PendingReconfiguration:
            # Don't do anything else here and let the next level up
            # handle it.
            raise
        except Exception:
            self.log.exception("Exception in pipeline processing:")
            manager.state.updateAttributes(
                ctx, state=pipeline.STATE_ERROR)
            # Continue processing other pipelines+tenants
        else:
            manager.state.updateAttributes(
                ctx, state=pipeline.STATE_NORMAL)
            manager.state.clearDirty(self.zk_client.client)
        return True

    def _gatherConnectionCacheKeys(self):
        relevant = set()
        with self.createZKContext(None, self.log) as ctx:
            for tenant in self.abide.tenants.values():
                for manager in tenant.layout.pipeline_managers.values():
                    self.log.debug("Gather relevant cache items for: %s %s",
                                   tenant.name, manager.pipeline.name)
                    # This will raise an exception and abort the process if
                    # unable to refresh the change list.
                    manager.change_list.refresh(ctx, allow_init=False)
                    change_keys = manager.change_list.getChangeKeys()
                    relevant_changes = manager.resolveChangeKeys(
                        change_keys)
                    for change in relevant_changes:
                        relevant.add(change.cache_stat.key)
        return relevant

    def maintainConnectionCache(self):
        self.log.debug("Starting connection cache maintenance")
        relevant = self._gatherConnectionCacheKeys()

        # We'll only remove changes older than `max_age` from the cache, as
        # it may take a while for an event that was processed by a connection
        # (which updated/populated the cache) to end up in a pipeline.
        for connection in self.connections.connections.values():
            connection.maintainCache(relevant, max_age=7200)  # 2h
            self.log.debug("Finished connection cache maintenance for: %s",
                           connection)
        self.log.debug("Finished connection cache maintenance")

    def process_tenant_trigger_queue(self, tenant):
        try:
            with trigger_queue_lock(
                self.zk_client, tenant.name, blocking=False
            ):
                self.log.debug("Processing tenant trigger events in %s",
                               tenant.name)
                # Update the pipeline changes
                ctx = self.createZKContext(None, self.log)
                for manager in tenant.layout.pipeline_managers.values():
                    # This will raise an exception if it is unable to
                    # refresh the change list.  We will proceed anyway
                    # and use our data from the last time we did
                    # refresh in order to avoid stalling trigger
                    # processing.  In this case we may not forward
                    # some events which are related to changes in the
                    # pipeline but don't match the pipeline trigger
                    # criteria.
                    try:
                        manager.change_list.refresh(ctx, allow_init=False)
                    except json.JSONDecodeError:
                        self.log.warning(
                            "Unable to refresh pipeline change list for %s",
                            manager.pipeline.name)
                    except Exception:
                        self.log.exception(
                            "Unable to refresh pipeline change list for %s",
                            manager.pipeline.name)

                # Get the ltime of the last reconfiguration event
                self.trigger_events[tenant.name].refreshMetadata()
                tenant_state = self.event_watcher.tenant_state[tenant.name]
                for event in self.trigger_events[tenant.name]:
                    log = get_annotated_logger(self.log, event.zuul_event_id)
                    try:
                        if not tenant_state.trigger_queue_discarding:
                            log.debug("Forwarding trigger event %s", event)
                            trigger_span = tracing.restoreSpanContext(
                                event.span_context)
                            with self.tracer.start_as_current_span(
                                    "TenantTriggerEventProcessing",
                                    links=[
                                        trace.Link(
                                            trigger_span.get_span_context())
                                    ]):
                                self._forward_trigger_event(event, tenant)
                        else:
                            log.debug("Discarding trigger event %s", event)
                    except Exception:
                        log.exception("Unable to forward event %s "
                                      "to tenant %s", event, tenant.name)
                    finally:
                        self.trigger_events[tenant.name].ack(event)
                self.trigger_events[tenant.name].cleanup()
        except LockException:
            self.log.debug("Skipping locked trigger event queue in tenant %s",
                           tenant.name)

    def _forward_trigger_event(self, event, tenant):
        log = get_annotated_logger(self.log, event.zuul_event_id)
        trusted, project = tenant.getProject(event.canonical_project_name)

        if project is None:
            return

        try:
            change_key = project.source.getChangeKey(event)
            change = project.source.getChange(change_key, event=event)
        except exceptions.ChangeNotFound as e:
            log.debug("Unable to get change %s from source %s",
                      e.change, project.source)
            return

        reconfigure_tenant = False
        if (event.branch_updated and
            hasattr(change, 'files') and
            change.updatesConfig(tenant)):
            reconfigure_tenant = True
            if change.files is None:
                log.debug("Reconfiguring tenant after branch updated "
                          "without file list, assuming config update")
        elif (event.branch_deleted and
              self.abide.hasConfigObjectCache(project.canonical_name,
                                              event.branch)):
            reconfigure_tenant = True

        # The branch_created attribute is also true when a tag is
        # created. Since we load config only from branches only trigger
        # a tenant reconfiguration if the branch is set as well.
        if event.branch_created and event.branch:
            reconfigure_tenant = True
        # If the driver knows the branch but we don't have a config, we
        # also need to reconfigure. This happens if a GitHub branch
        # was just configured as protected without a push in between.
        elif (event.branch in project.source.getProjectBranches(
                project, tenant, min_ltime=event.branch_cache_ltime)
              and not self.abide.hasConfigObjectCache(
                project.canonical_name, event.branch)):
            reconfigure_tenant = True

        # If the branch is unprotected and unprotected branches
        # are excluded from the tenant for that project skip reconfig.
        if (reconfigure_tenant and not
            event.branch_protected and
            tenant.getExcludeUnprotectedBranches(project)):

            reconfigure_tenant = False

        # If all config classes are excluded for this project we don't need
        # to trigger a reconfiguration.
        tpc = tenant.project_configs.get(project.canonical_name)
        if tpc and not tpc.load_classes:
            reconfigure_tenant = False

        # If we are listing included branches and this branch
        # is not included, skip reconfig.
        if (reconfigure_tenant and
            not tpc.includesBranch(event.branch)):
            reconfigure_tenant = False

        # But if the event is that branch protection status or the
        # default branch has changed, do reconfigure.
        if (event.isBranchProtectionChanged() or
            event.isDefaultBranchChanged()):
            reconfigure_tenant = True

        if reconfigure_tenant:
            # The change that just landed updates the config
            # or a branch was just created or deleted.  Clear
            # out cached data for this project and perform a
            # reconfiguration.
            self.reconfigureTenant(tenant, change.project, event)
            # This will become the new required minimum event ltime
            # for every trigger event processed after the
            # reconfiguration, so make sure we update it after having
            # submitted the reconfiguration event.
            self.trigger_events[tenant.name].last_reconfigure_event_ltime =\
                event.zuul_event_ltime

        event.min_reconfigure_ltime = self.trigger_events[
            tenant.name].last_reconfigure_event_ltime

        span = trace.get_current_span()
        span.set_attribute("reconfigure_tenant", reconfigure_tenant)
        event.span_context = tracing.getSpanContext(span)

        for manager in tenant.layout.pipeline_managers.values():
            # For most kinds of dependencies, it's sufficient to check
            # if this change is already in the pipeline, because the
            # only way to update a dependency cycle is to update one
            # of the changes in it.  However, dependencies-by-topic
            # can have changes added to the cycle without updating any
            # of the existing changes in the cycle.  That means in
            # order to detect whether a new change is added to an
            # existing cycle in the pipeline, we need to know all of
            # the dependencies of the new change, and check if *they*
            # are in the pipeline.  Therefore, go ahead and update our
            # dependencies here so they are available for comparison
            # against the pipeline contents.  This front-loads some
            # work that otherwise would happen in the pipeline
            # manager, but the result of the work goes into the change
            # cache, so it's not wasted; it's just less parallelized.
            if isinstance(change, Change):
                manager.updateCommitDependencies(change, event)
            if (
                manager.eventMatches(event, change)
                or manager.isChangeRelevantToPipeline(change)
            ):
                self.pipeline_trigger_events[tenant.name][
                    manager.pipeline.name
                ].put(event.driver_name, event)

    def process_pipeline_trigger_queue(self, tenant, tenant_lock, manager):
        for event in self.pipeline_trigger_events[tenant.name][
                manager.pipeline.name]:
            log = get_annotated_logger(self.log, event.zuul_event_id)
            if not isinstance(event, SupercedeEvent):
                local_state = self.local_layout_state[tenant.name]
                last_ltime = local_state.last_reconfigure_event_ltime
                # The event tells us the ltime of the most recent
                # reconfiguration event up to that point.  If our local
                # layout state wasn't generated by an event after that
                # time, then we are too out of date to process this event.
                # Abort now and wait for an update.
                if (event.min_reconfigure_ltime > -1 and
                    event.min_reconfigure_ltime > last_ltime):
                    log.debug("Trigger event minimum reconfigure ltime of %s "
                              "newer than current reconfigure ltime of %s, "
                              "aborting early",
                              event.min_reconfigure_ltime, last_ltime)
                    return
            log.debug("Processing trigger event %s", event)
            try:
                if not self.disable_pipelines:
                    if isinstance(event, SupercedeEvent):
                        self._doSupercedeEvent(event)
                    else:
                        self._process_trigger_event(tenant, manager, event)
            finally:
                self.pipeline_trigger_events[tenant.name][
                    manager.pipeline.name
                ].ack(event)
            if self._stopped:
                return
            self.abortIfPendingReconfig(tenant_lock)
        self.pipeline_trigger_events[
            tenant.name][manager.pipeline.name].cleanup()

    def _process_trigger_event(self, tenant, manager, event):
        log = get_annotated_logger(
            self.log, event.zuul_event_id
        )
        trusted, project = tenant.getProject(event.canonical_project_name)
        if project is None:
            return
        try:
            change_key = project.source.getChangeKey(event)
            change = project.source.getChange(change_key, event=event)
        except exceptions.ChangeNotFound as e:
            log.debug("Unable to get change %s from source %s",
                      e.change, project.source)
            return

        if event.isPatchsetCreated():
            manager.removeOldVersionsOfChange(change, event)
        elif event.isChangeAbandoned():
            manager.removeAbandonedChange(change, event)

        # Let the pipeline update any dependencies that may need
        # refreshing if this change has updated.
        if event.isPatchsetCreated() or event.isMessageChanged():
            manager.refreshDeps(change, event)

        if match_info := manager.eventMatches(event, change):
            manager.addChange(change, event, debug=match_info.debug)

    def process_tenant_management_queue(self, tenant):
        try:
            with management_queue_lock(
                self.zk_client, tenant.name, blocking=False
            ):
                if not self.isTenantLayoutUpToDate(tenant.name):
                    self.log.debug(
                        "Skipping management event queue for tenant %s",
                        tenant.name)
                    return
                self.log.debug("Processing tenant management events in %s",
                               tenant.name)
                self._process_tenant_management_queue(tenant)
        except LockException:
            self.log.debug("Skipping locked management event queue"
                           " in tenant %s", tenant.name)

    def _process_tenant_management_queue(self, tenant):
        # Set of tenant names that were notified of
        # a semaphore release.
        semaphore_notified = set()
        for event in self.management_events[tenant.name]:
            event_forwarded = False
            try:
                if isinstance(event, TenantReconfigureEvent):
                    self.log.debug("Processing tenant reconfiguration "
                                   "event for tenant %s", tenant.name)
                    self._doTenantReconfigureEvent(event)
                elif isinstance(event, (PromoteEvent, ChangeManagementEvent)):
                    event_forwarded = self._forward_management_event(event)
                elif isinstance(event, SemaphoreReleaseEvent):
                    self._doSemaphoreReleaseEvent(
                        event, tenant, semaphore_notified)
                else:
                    self.log.error("Unable to handle event %s for tenant %s",
                                   event, tenant.name)
            finally:
                if event_forwarded:
                    self.management_events[tenant.name].ackWithoutResult(
                        event)
                else:
                    self.management_events[tenant.name].ack(event)
        self.management_events[tenant.name].cleanup()

    def _forward_management_event(self, event):
        event_forwarded = False
        try:
            tenant = self.abide.tenants.get(event.tenant_name)
            if tenant is None:
                raise ValueError(f'Unknown tenant {event.tenant_name}')
            manager = tenant.layout.pipeline_managers.get(event.pipeline_name)
            if manager is None:
                raise ValueError(f'Unknown pipeline {event.pipeline_name}')
            self.pipeline_management_events[tenant.name][
                manager.pipeline.name
            ].put(event)
            event_forwarded = True
        except Exception:
            event.exception(
                "".join(
                    traceback.format_exception(*sys.exc_info())
                )
            )
        return event_forwarded

    def process_reconfigure_queue(self):
        while not self.reconfigure_event_queue.empty() and not self._stopped:
            self.log.debug("Fetching reconfiguration event")
            event = self.reconfigure_event_queue.get()
            try:
                if isinstance(event, ReconfigureEvent):
                    self._doReconfigureEvent(event)
                else:
                    self.log.error("Unable to handle event %s", event)
            finally:
                if event.ack_ref:
                    event.ack_ref.set()
                self.reconfigure_event_queue.task_done()

    def process_pipeline_management_queue(self, tenant, tenant_lock, manager):
        for event in self.pipeline_management_events[tenant.name][
            manager.pipeline.name
        ]:
            log = get_annotated_logger(self.log, event.zuul_event_id)
            log.debug("Processing management event %s", event)
            try:
                if not self.disable_pipelines:
                    self._process_management_event(event)
            finally:
                self.pipeline_management_events[tenant.name][
                    manager.pipeline.name
                ].ack(event)
            if self._stopped:
                return
            self.abortIfPendingReconfig(tenant_lock)
        self.pipeline_management_events[tenant.name][
            manager.pipeline.name].cleanup()

    def _process_management_event(self, event):
        try:
            if isinstance(event, PromoteEvent):
                self._doPromoteEvent(event)
            elif isinstance(event, DequeueEvent):
                self._doDequeueEvent(event)
            elif isinstance(event, EnqueueEvent):
                self._doEnqueueEvent(event)
            elif isinstance(event, PipelinePostConfigEvent):
                # We don't need to do anything; the event just
                # triggers a pipeline run.
                pass
            elif isinstance(event, PipelineSemaphoreReleaseEvent):
                # Same as above.
                pass
            else:
                self.log.error("Unable to handle event %s" % event)
        except Exception:
            self.log.exception("Exception in management event:")
            event.exception(
                "".join(traceback.format_exception(*sys.exc_info()))
            )

    def process_pipeline_result_queue(self, tenant, tenant_lock, manager):
        for event in self.pipeline_result_events[tenant.name][
                manager.pipeline.name]:
            log = get_annotated_logger(
                self.log,
                event=getattr(event, "zuul_event_id", None),
                build=getattr(event, "build_uuid", None),
            )
            log.debug("Processing result event %s", event)
            try:
                if not self.disable_pipelines:
                    self._process_result_event(event, manager)
            finally:
                self.pipeline_result_events[tenant.name][
                    manager.pipeline.name
                ].ack(event)
            if self._stopped:
                return
            self.abortIfPendingReconfig(tenant_lock)
        self.pipeline_result_events[
            tenant.name][manager.pipeline.name].cleanup()

    def _process_result_event(self, event, manager):
        if isinstance(event, BuildStartedEvent):
            self._doBuildStartedEvent(event, manager)
        elif isinstance(event, BuildStatusEvent):
            self._doBuildStatusEvent(event, manager)
        elif isinstance(event, BuildPausedEvent):
            self._doBuildPausedEvent(event, manager)
        elif isinstance(event, BuildCompletedEvent):
            self._doBuildCompletedEvent(event, manager)
        elif isinstance(event, MergeCompletedEvent):
            self._doMergeCompletedEvent(event, manager)
        elif isinstance(event, FilesChangesCompletedEvent):
            self._doFilesChangesCompletedEvent(event, manager)
        elif isinstance(event, NodesProvisionedEvent):
            self._doNodesProvisionedEvent(event, manager)
        elif isinstance(event, SemaphoreReleaseEvent):
            # MODEL_API <= 32
            # Kept for backward compatibility; semaphore release events
            # are now processed in the management event queue.
            self._doSemaphoreReleaseEvent(event, manager.tenant, set())
        else:
            self.log.error("Unable to handle event %s", event)

    def _getBuildSetFromPipeline(self, event, manager):
        log = get_annotated_logger(
            self.log,
            event=getattr(event, "zuul_event_id", None),
            build=getattr(event, "build_uuid", None),
        )
        if not manager:
            log.warning(
                "Build set %s is not associated with a pipeline",
                event.build_set_uuid,
            )
            return

        for item in manager.state.getAllItems():
            # If the provided buildset UUID doesn't match any current one,
            # we assume that it's not current anymore.
            if item.current_build_set.uuid == event.build_set_uuid:
                return item.current_build_set

        log.warning("Build set %s is not current", event.build_set_uuid)

    def _getBuildFromPipeline(self, event, manager):
        log = get_annotated_logger(
            self.log, event.zuul_event_id, build=event.build_uuid)
        build_set = self._getBuildSetFromPipeline(event, manager)
        if not build_set:
            return

        job = build_set.item.getJob(event.job_uuid)
        build = build_set.getBuild(job)
        # Verify that the build uuid matches the one of the result
        if not build:
            log.debug(
                "Build %s could not be found in the current buildset",
                event.build_uuid)
            return

        # Verify that the build UUIDs match since we looked up the build by
        # its job name. In case of a retried build, we might already have a new
        # build in the buildset.
        # TODO (felix): Not sure if that reasoning is correct, but I think it
        # shouldn't harm to have such an additional safeguard.
        if not build.uuid == event.build_uuid:
            log.debug(
                "Build UUID %s doesn't match the current build's UUID %s",
                event.build_uuid, build.uuid)
            return

        return build

    def _doBuildStartedEvent(self, event, manager):
        build = self._getBuildFromPipeline(event, manager)
        if not build:
            return

        with build.activeContext(manager.current_context):
            build.start_time = event.data["start_time"]

            log = get_annotated_logger(
                self.log, build.zuul_event_id, build=build.uuid)
            try:
                item = build.build_set.item
                job = build.job
                change = item.getChangeForJob(job)
                estimate = self.times.getEstimatedTime(
                    manager.tenant.name,
                    change.project.name,
                    getattr(change, 'branch', None),
                    job.name)
                if not estimate:
                    estimate = 0.0
                build.estimated_time = estimate
            except Exception:
                log.exception("Exception estimating build time:")
        manager.onBuildStarted(build)

    def _doBuildStatusEvent(self, event, manager):
        build = self._getBuildFromPipeline(event, manager)
        if not build:
            return

        args = {}
        if 'url' in event.data:
            args['url'] = event.data['url']
        if 'pre_fail' in event.data:
            args['pre_fail'] = event.data['pre_fail']
        build.updateAttributes(manager.current_context,
                               **args)

    def _doBuildPausedEvent(self, event, manager):
        build = self._getBuildFromPipeline(event, manager)
        if not build:
            return

        # Setting paused is deferred to event processing stage to avoid a race
        # with child job skipping.
        with build.activeContext(manager.current_context):
            build.paused = True
            build.addEvent(
                BuildEvent(
                    event_time=time.time(),
                    event_type=BuildEvent.TYPE_PAUSED))
            build.setResultData(
                event.data.get("data", {}),
                event.data.get("secret_data", {}))

        manager.onBuildPaused(build)

    def _doBuildCompletedEvent(self, event, manager):
        log = get_annotated_logger(
            self.log, event.zuul_event_id, build=event.build_uuid)
        build = self._getBuildFromPipeline(event, manager)
        if not build:
            self.log.error(
                "Unable to find build %s. Creating a fake build to clean up "
                "build resources.",
                event.build_uuid)
            # Create a fake build with a minimal set of attributes that
            # allows reporting the build via SQL and cleaning up build
            # resources.
            build = Build()
            job = DummyFrozenJob()
            job.uuid = event.job_uuid
            job.provides = []
            build._set(
                job=job,
                uuid=event.build_uuid,
                zuul_event_id=event.zuul_event_id,

                # Set the build_request_ref on the fake build to make the
                # cleanup work.
                build_request_ref=event.build_request_ref,

                # TODO (felix): Do we have to fully evaluate the build
                # result (see the if/else block with different results
                # further down) or is it sufficient to just use the result
                # as-is from the executor since the build is anyways
                # outdated. In case the build result is None it won't be
                # changed to RETRY.
                result=event.data.get("result"),
            )

            self._cleanupCompletedBuild(build)
            try:
                self.sql.reportBuildEnd(
                    build, tenant=manager.tenant.name,
                    final=True)
            except exceptions.MissingBuildsetError:
                # If we have not reported start for this build, we
                # don't need to report end.  If we haven't reported
                # start, we won't have a build record in the DB, so we
                # would normally create one and attach it to a
                # buildset, but we don't know the buildset, and we
                # don't have enough information to construct a
                # buildset record from scratch.  That's okay in this
                # situation.
                pass
            except Exception:
                log.exception("Error reporting build completion to DB:")

            # Make sure we don't forward this outdated build result with
            # an incomplete (fake) build object to the pipeline manager.
            return

        event_result = event.data

        result = event_result.get("result")
        result_data = event_result.get("data", {})
        secret_result_data = event_result.get("secret_data", {})
        warnings = event_result.get("warnings", [])
        unreachable = event_result.get("unreachable", False)

        log.info("Build complete, result %s, warnings %s", result, warnings)

        with build.activeContext(manager.current_context):
            build.error_detail = event_result.get("error_detail")
            build.unreachable = unreachable

            if result is None:
                build.retry = True
            if result == "ABORTED":
                # Always retry if the executor just went away
                build.retry = True
            # TODO: Remove merger_failure in v6.0
            if result in ["MERGE_CONFLICT", "MERGER_FAILURE"]:
                # The build result MERGE_CONFLICT is a bit misleading here
                # because when we got here we know that there are no merge
                # conflicts. Instead this is most likely caused by some
                # infrastructure failure. This can be anything like connection
                # issue, drive corruption, full disk, corrupted git cache, etc.
                # This may or may not be a recoverable failure so we should
                # retry here respecting the max retries. But to be able to
                # distinguish from RETRY_LIMIT which normally indicates pre
                # playbook failures we keep the build result after the max
                # attempts.
                if build.build_set.getTries(build.job) < build.job.attempts:
                    build.retry = True

            if build.retry:
                result = "RETRY"

            # If the build was canceled, we did actively cancel the job so
            # don't overwrite the result and don't retry.
            if build.canceled:
                result = build.result or "CANCELED"
                build.retry = False

            build.end_time = event_result["end_time"]
            build.setResultData(result_data, secret_result_data)
            build.build_set.updateAttributes(
                manager.current_context,
                warning_messages=build.build_set.warning_messages + warnings)
            build.held = event_result.get("held")

            build.result = result

            attributes = {
                "uuid": build.uuid,
                "job": build.job.name,
                "buildset_uuid": build.build_set.item.current_build_set.uuid,
                "zuul_event_id": build.build_set.item.event.zuul_event_id,
            }
            tracing.endSavedSpan(build.span_info, attributes=attributes)

        self._cleanupCompletedBuild(build)
        try:
            self.reportBuildEnd(
                build, tenant=manager.tenant.name, final=(not build.retry))
        except Exception:
            log.exception("Error reporting build completion to DB:")

        manager.onBuildCompleted(build)

    def _cleanupCompletedBuild(self, build):
        # TODO (felix): Returning the nodes doesn't work in case the buildset
        # is not current anymore. Does it harm to not do anything in here in
        # this case?
        if build.build_set:
            # In case the build didn't show up on any executor, the node
            # request does still exist, so we have to make sure it is
            # removed from ZK.
            request_id = build.build_set.getJobNodeRequestID(build.job)
            if request_id:
                if self.nodepool.isNodeRequestID(request_id):
                    self.nodepool.deleteNodeRequest(
                        request_id, event_id=build.zuul_event_id)
                else:
                    req = self.launcher.getRequest(request_id)
                    if req:
                        self.launcher.deleteRequest(req)

            # The build is completed and the nodes were already returned
            # by the executor. For consistency, also remove the node
            # request from the build set.
            build.build_set.removeJobNodeRequestID(build.job)

        # The test suite expects the build to be removed from the
        # internal dict after it's added to the report queue.
        self.executor.removeBuild(build)

    def _doMergeCompletedEvent(self, event, manager):
        build_set = self._getBuildSetFromPipeline(event, manager)
        if not build_set:
            return

        tracing.endSavedSpan(
            event.span_info,
            attributes={
                "uuid": event.request_uuid,
                "buildset_uuid": build_set.uuid,
                "zuul_event_id": build_set.item.event.zuul_event_id,
            }
        )
        manager.onMergeCompleted(event, build_set)

    def _doFilesChangesCompletedEvent(self, event, manager):
        build_set = self._getBuildSetFromPipeline(event, manager)
        if not build_set:
            return

        tracing.endSavedSpan(
            event.span_info,
            attributes={
                "uuid": event.request_uuid,
                "buildset_uuid": build_set.uuid,
                "zuul_event_id": build_set.item.event.zuul_event_id,
            }
        )
        manager.onFilesChangesCompleted(event, build_set)

    def _doNodesProvisionedEvent(self, event, manager):
        if self.nodepool.isNodeRequestID(event.request_id):
            request = self.nodepool.zk_nodepool.getNodeRequest(
                event.request_id)
        else:
            request = self.launcher.getRequest(event.request_id)

        if not request:
            self.log.warning("Unable to find request %s while processing"
                             "nodes provisioned event %s", request, event)
            return

        log = get_annotated_logger(self.log, request)
        # Look up the buildset to access the local node request object
        build_set = self._getBuildSetFromPipeline(event, manager)
        if not build_set:
            log.warning("Build set not found while processing"
                        "nodes provisioned event %s", event)
            return

        job = build_set.item.getJob(request.job_uuid)
        if job is None:
            log.warning("Item %s does not contain job %s "
                        "for node request %s",
                        build_set.item, request.job_uuid, request)
            return

        if isinstance(request, NodeRequest):
            nodeset_info = self.nodepool.getNodesetInfo(request, job.nodeset)
            # If the request failed, we must directly delete it as the nodes
            # will never be accepted.
            if request.state == STATE_FAILED:
                self.nodepool.deleteNodeRequest(request.id,
                                                event_id=event.request_id)
        else:
            nodeset_info = self.launcher.getNodesetInfo(request)
            if request.state == request.State.FAILED:
                self.launcher.deleteRequest(request)

            # End the NodesetRequest span
            tracing.endSavedSpan(request.span_info,
                                 attributes=request.getSpanAttributes())

        job = build_set.item.getJob(request.job_uuid)
        if build_set.getJobNodeSetInfo(job) is None:
            manager.onNodesProvisioned(
                request, nodeset_info, build_set)
        else:
            self.log.warning("Duplicate nodes provisioned event: %s",
                             event)

    def cancelJob(self, buildset, job, build=None, final=False,
                  force=False):
        """Cancel a running build

        Set final to true to create a fake build result even if the
        job has not run.  TODO: explain this better.

        Set force to true to forcibly release build resources without
        waiting for a result from the executor.  Use this when
        removing pipelines.

        """
        item = buildset.item
        log = get_annotated_logger(self.log, item.event)
        try:
            # Cancel node request if needed
            req_id = buildset.getJobNodeRequestID(job)
            if req_id:
                if self.nodepool.isNodeRequestID(req_id):
                    if req := self.nodepool.zk_nodepool.getNodeRequest(req_id):
                        self.nodepool.cancelRequest(req)
                else:
                    if req := self.launcher.getRequest(req_id):
                        self.launcher.deleteRequest(req)

            # Make sure we always remove the node request from the buildset
            # (empty request don't have an ID).
            buildset.removeJobNodeRequestID(job)

            # Cancel build if needed
            build = build or buildset.getBuild(job)
            if build:
                try:
                    self.executor.cancel(build)
                except Exception:
                    log.exception(
                        "Exception while canceling build %s for %s",
                        build, item)

                # In the unlikely case that a build is removed and
                # later added back, make sure we clear out the nodeset
                # so it gets requested again.
                req_info = buildset.getJobNodeSetInfo(job)
                if req_info:
                    buildset.removeJobNodeSetInfo(job)

                if build.result is None:
                    build.updateAttributes(
                        buildset.item.manager.current_context,
                        result='CANCELED')

                if force:
                    # Directly delete the build rather than putting a
                    # CANCELED event in the result event queue since
                    # the result event queue won't be processed
                    # anymore once the pipeline is removed.
                    self.executor.removeBuild(build)
                    try:
                        self.reportBuildEnd(
                            build, build.build_set.item.manager.tenant.name,
                            final=False)
                    except Exception:
                        self.log.exception(
                            "Error reporting build completion to DB:")

            else:
                if final:
                    # If final is set make sure that the job is not resurrected
                    # later by re-requesting nodes.
                    fakebuild = Build.new(
                        buildset.item.manager.current_context,
                        job=job, build_set=item.current_build_set,
                        result='CANCELED')
                    buildset.addBuild(fakebuild)
        finally:
            # Release the semaphore in any case
            manager = buildset.item.manager
            tenant = manager.tenant
            if COMPONENT_REGISTRY.model_api >= 33:
                event_queue = self.management_events[tenant.name]
            else:
                event_queue = self.pipeline_result_events[
                    tenant.name][manager.pipeline.name]
            tenant.semaphore_handler.release(event_queue, item, job)

    # Image related methods
    def createImageBuildArtifact(self, tenant_name, image, build,
                                 metadata, artifact_url,
                                 validated):
        # Image builds are not tenant scoped; the tenant_name
        # parameter is present to support triggering validation builds
        # in the same tenant.
        log = get_annotated_logger(self.log, build.zuul_event_id,
                                   build=build.uuid)
        art_uuid = uuid.uuid4().hex
        image_format = metadata['format']
        image_md5sum = metadata.get('md5sum')
        image_sha256 = metadata.get('sha256')
        log.info(
            "Storing image build artifact metadata: "
            "uuid: %s name: %s format: %s build: %s url: %s",
            art_uuid, image.canonical_name, image_format, build.uuid,
            artifact_url)
        with self.createZKContext(None, self.log) as ctx:
            # This starts with an unknown state, then
            # createImageUploads will set it to ready.
            return ImageBuildArtifact.new(
                ctx,
                uuid=art_uuid,
                name=image.name,
                canonical_name=image.canonical_name,
                project_canonical_name=image.project_canonical_name,
                project_branch=image.branch,
                build_tenant_name=tenant_name,
                build_uuid=build.uuid,
                format=image_format,
                md5sum=image_md5sum,
                sha256=image_sha256,
                url=artifact_url,
                timestamp=build.end_time,
                validated=validated,
            )

    def createImageUploads(self, iba):
        # iba is an ImageBuildArtifact
        with self.createZKContext(None, self.log) as outer_ctx:
            with iba.locked(outer_ctx) as lock:
                with self.createZKContext(lock, self.log) as ctx:
                    # Providers may share image upload configurations, but
                    # these shared configurations may appear on distinct
                    # endpoints.  Identify the unique configurations for
                    # each endpoint and create one upload for each.
                    # (endpoint, hash) -> [providers]
                    uploads = defaultdict(list)
                    for tenant in self.abide.tenants.values():
                        for provider in tenant.layout.providers.values():
                            for image in provider.images.values():
                                if (image.canonical_name == iba.canonical_name
                                    and image.format == iba.format):
                                    # This image is needed, add this endpoint
                                    endpoint = provider.getEndpoint()
                                    key = (endpoint.canonical_name,
                                           image.zuul_config_hash)
                                    uploads[key].append(
                                        provider.canonical_name)
                    for (endpoint_name, config_hash), providers in \
                            uploads.items():
                        upload_uuid = uuid.uuid4().hex
                        self.log.info(
                            "Storing image upload: "
                            "uuid: %s name: %s artifact uuid: %s "
                            "endpoint: %s config hash: %s",
                            upload_uuid, iba.canonical_name, iba.uuid,
                            endpoint_name, config_hash)
                        ImageUpload.new(
                            ctx,
                            uuid=upload_uuid,
                            canonical_name=iba.canonical_name,
                            artifact_uuid=iba.uuid,
                            endpoint_name=endpoint_name,
                            providers=providers,
                            config_hash=config_hash,
                            timestamp=time.time(),
                            validated=iba.validated,
                            _state=ImageUpload.State.PENDING,
                            state_time=time.time(),
                        )
                    # Only mark the iba as ready once all the upload
                    # records for it have been created.
                    with iba.activeContext(ctx):
                        iba.state = iba.State.READY

    def validateImageUpload(self, image_upload_uuid, build, validated):
        upload = ImageUpload()
        upload._set(uuid=image_upload_uuid)
        with self.createZKContext(None, self.log) as outer_ctx:
            with upload.locked(outer_ctx, blocking=True) as lock:
                with self.createZKContext(lock, self.log) as ctx:
                    upload.refresh(ctx)
                    upload.updateAttributes(
                        ctx,
                        validated=validated,
                        timestamp=time.time(),
                        build_uuid=build.uuid,
                    )

    def createZKContext(self, lock, log):
        return ZKContext(self.zk_client, lock, self.stop_event, log)
