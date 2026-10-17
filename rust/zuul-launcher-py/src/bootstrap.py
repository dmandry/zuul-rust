# Copyright 2026 Acme Gating, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Embedded by the PyO3 harness. Builds the same Python object graph the
# unit tests poke (caches, component registry, events). The main loop's
# _run is the Rust launcher; this file does not call server.Launcher._run.

import logging
import os
import socket
import threading
import time
from contextlib import nullcontext

import cachetools

from zuul.lib import commandsocket, tracing
from zuul.lib.concurrency import MonitoringThreadPoolExecutor
from zuul.lib.config import get_default
from zuul.lib.monitoring import MonitoringServer
from zuul.lib.statsd import get_statsd
from zuul.version import get_version_string
from zuul.zk import ZooKeeperClient
from zuul.zk.components import COMPONENT_REGISTRY, LauncherComponent
from zuul.zk.event_queues import (
    PipelineResultEventQueue,
    TenantTriggerEventQueue,
)
from zuul.zk.image_registry import ImageBuildRegistry, ImageUploadRegistry
from zuul.zk.launcher import LauncherApi
from zuul.zk.layout import LayoutProvidersStore, LayoutStateStore
from zuul.zk.system import ZuulSystem
from zuul.launcher.server import CleanupWorker, NodescanWorker


def bootstrap(launcher, config, connections, hostname):
    launcher._running = True
    launcher._start_cleanup = True
    launcher.config = config
    launcher.connections = connections
    filter_string = get_default(config, "launcher", "connection_filter")
    if filter_string:
        launcher.connection_filter = [f.strip() for f in filter_string.split(",")]
    else:
        launcher.connection_filter = None
    launcher.repl = None
    launcher.tenant_providers = {}
    launcher.endpoints = {}
    launcher.hostname = hostname or socket.getfqdn()
    launcher.log = logging.getLogger("zuul.Launcher")

    statsd_extra_keys = {"hostname": launcher.hostname}
    launcher.statsd = get_statsd(config, statsd_extra_keys)
    if launcher.statsd:
        launcher.statsd_timer = launcher.statsd.timer
    else:
        launcher.statsd_timer = nullcontext

    launcher._provider_limits_cache = cachetools.TTLCache(maxsize=8192, ttl=300)
    launcher._provider_quota_cache = cachetools.TTLCache(maxsize=8192, ttl=300)

    launcher.tracing = tracing.Tracing(config)
    launcher.zk_client = ZooKeeperClient.fromConfig(config)
    launcher.zk_client.connect()

    launcher._zk_hosts = get_default(config, "zookeeper", "hosts")
    launcher._zk_cert = get_default(config, "zookeeper", "tls_cert")
    launcher._zk_key = get_default(config, "zookeeper", "tls_key")
    launcher._zk_ca = get_default(config, "zookeeper", "tls_ca")
    timeout = float(get_default(config, "zookeeper", "session_timeout", 120.0))
    launcher._zk_timeout_ms = int(timeout * 1000)

    launcher.system = ZuulSystem(launcher.zk_client)
    launcher.trigger_events = TenantTriggerEventQueue.createRegistry(
        launcher.zk_client, launcher.connections
    )
    launcher.result_events = PipelineResultEventQueue.createRegistry(
        launcher.zk_client
    )

    COMPONENT_REGISTRY.create(launcher.zk_client)
    launcher.component_info = LauncherComponent(
        launcher.zk_client,
        launcher.hostname,
        version=get_version_string(),
        connection_filter=launcher.connection_filter,
    )
    launcher.component_info.register()
    launcher.wake_event = threading.Event()
    launcher.stop_event = threading.Event()
    launcher.join_event = threading.Event()
    launcher.providers_update_lock = threading.Lock()
    COMPONENT_REGISTRY.registry.registerCallback(launcher.wake_event.set)

    launcher.monitoring_server = MonitoringServer(
        config, "launcher", launcher.component_info
    )
    launcher.monitoring_server.start()

    launcher.api = LauncherApi(
        launcher.zk_client,
        COMPONENT_REGISTRY.registry,
        launcher.component_info,
        launcher.wake_event.set,
        launcher.connection_filter,
    )

    temp_root = get_default(config, "launcher", "temp_dir", "/tmp", expand_user=True)
    launcher.temp_dir = os.path.join(temp_root, "zuul-launcher")
    os.makedirs(launcher.temp_dir, exist_ok=True)

    def _noop_repl(*_args):
        launcher.log.debug("REPL is not implemented in the rust launcher")

    launcher.command_map = {
        commandsocket.StopCommand.name: launcher.stop,
        commandsocket.ReplCommand.name: _noop_repl,
        commandsocket.NoReplCommand.name: _noop_repl,
    }
    command_socket = get_default(
        config, "launcher", "command_socket", "/var/lib/zuul/launcher.socket"
    )
    launcher.command_socket = commandsocket.CommandSocket(command_socket)
    launcher._command_running = False

    launcher.layout_updated_event = threading.Event()
    launcher.layout_updated_event.set()
    launcher.image_updated_event = threading.Event()
    launcher.upload_deleted_event = threading.Event()

    def _layout_updated():
        launcher.layout_updated_event.set()
        launcher.wake_event.set()

    def _image_updated():
        launcher.image_updated_event.set()
        launcher.wake_event.set()

    launcher.tenant_layout_state = LayoutStateStore(
        launcher.zk_client, _layout_updated
    )
    launcher.layout_providers_store = LayoutProvidersStore(
        launcher.zk_client, launcher.connections, launcher.system.system_id
    )
    launcher.local_layout_state = {}
    launcher.image_build_registry = ImageBuildRegistry(
        launcher.zk_client, _image_updated
    )
    launcher.image_upload_registry = ImageUploadRegistry(
        launcher.zk_client, _image_updated
    )

    launcher.nodescan_worker = NodescanWorker()
    launcher._run_lock = threading.Lock()
    launcher.launcher_thread = threading.Thread(target=launcher.run, name="Launcher")
    launcher.upload_uuids_in_queue = set()
    launcher.upload_uuids_in_delete_queue = set()
    launcher.upload_executor = MonitoringThreadPoolExecutor(
        statsd=launcher.statsd,
        metric="zuul.launcher.server.{hostname}.upload_workers",
        max_workers=1,
        thread_name_prefix="UploadWorker",
    )
    launcher.endpoint_upload_executor = MonitoringThreadPoolExecutor(
        statsd=launcher.statsd,
        metric="zuul.launcher.server.{hostname}.endpoint_upload_workers",
        max_workers=10,
        thread_name_prefix="EndpointUploadWorker",
    )
    launcher.endpoint_import_executor = MonitoringThreadPoolExecutor(
        statsd=launcher.statsd,
        metric="zuul.launcher.server.{hostname}.endpoint_import_workers",
        max_workers=20,
        thread_name_prefix="EndpointImportWorker",
    )
    launcher.cleanup_worker = CleanupWorker(launcher)

    def _stats_wait():
        # The stats election loop is not ported. This thread only keeps
        # stop/join from hanging.
        launcher.stop_event.wait()

    launcher.stats_thread = threading.Thread(
        target=_stats_wait, name="LauncherStats", daemon=True
    )


def _clean_tempdir(launcher):
    try:
        for name in os.listdir(launcher.temp_dir):
            path = os.path.join(launcher.temp_dir, name)
            if os.path.isfile(path):
                try:
                    os.unlink(path)
                except Exception:
                    launcher.log.exception("Error deleting %s", path)
                launcher.log.info("Deleted leaked file %s", path)
    except Exception:
        launcher.log.exception("Error cleaning temp dir")


def start(launcher):
    launcher.log.debug("Cleaning up temp dir")
    _clean_tempdir(launcher)
    launcher.log.debug("Starting command processor")
    launcher._command_running = True
    launcher.command_socket.start()
    launcher.command_thread = threading.Thread(
        target=_run_command, args=(launcher,), name="command", daemon=True
    )
    launcher.command_thread.start()
    launcher.log.debug("Starting nodescan worker")
    launcher.nodescan_worker.start()
    launcher.log.debug("Starting launcher thread")
    launcher.launcher_thread.start()
    if launcher._start_cleanup:
        launcher.log.debug("Starting cleanup thread")
        launcher.cleanup_worker.start()
    launcher.stats_thread.start()


def _run_command(launcher):
    while launcher._command_running:
        try:
            command, args = launcher.command_socket.get()
            if command != "_stop":
                launcher.command_map[command](*args)
        except Exception:
            launcher.log.exception("Exception while processing command")


def run(launcher):
    """Python thread loop. _run itself is the Rust launcher."""
    launcher.log.debug("Launcher running")
    try:
        import zuul.launcher.server as server

        max_sleep = server.Launcher.MAX_SLEEP
    except Exception:
        max_sleep = 1.0
    min_interval = 60
    while launcher._running:
        launcher.log.debug("Run handler awake")
        loop_start = time.monotonic()
        try:
            with launcher.statsd_timer("zuul.launcher.run_handler"):
                with launcher._run_lock:
                    launcher._run()
        except Exception:
            launcher.log.exception("Error in main thread:")
        loop_duration = time.monotonic() - loop_start
        time.sleep(max(0, max_sleep - loop_duration))
        launcher.log.debug("Run handler sleeping")
        launcher.wake_event.wait(timeout=min_interval)
        launcher.wake_event.clear()


def stop(launcher):
    # Tests create extra launchers and also stop the primary one from
    # shutdown(). A second stop must not touch the command socket again.
    if getattr(launcher, "_stop_called", False):
        return
    launcher._stop_called = True
    launcher.log.debug("Stopping launcher")
    launcher._running = False
    launcher.stop_event.set()
    launcher.wake_event.set()
    launcher.component_info.state = launcher.component_info.STOPPED
    launcher.repl = None
    launcher._command_running = False
    launcher.command_socket.stop()
    if getattr(launcher, "command_thread", None) is not None:
        launcher.command_thread.join()
    launcher.log.debug("Stopping stats thread")
    launcher.stats_thread.join()
    launcher.cleanup_worker.stop()
    launcher.cleanup_worker.join()
    launcher.upload_executor.shutdown()
    launcher.endpoint_import_executor.shutdown()
    launcher.endpoint_upload_executor.shutdown()
    launcher.nodescan_worker.stop()
    launcher.connections.stop()
    launcher.monitoring_server.stop()
    launcher.log.debug("Stopped launcher")


def join(launcher):
    if getattr(launcher, "_join_called", False):
        return
    launcher.log.debug("Joining launcher")
    launcher.launcher_thread.join()
    launcher.join_event.set()
    launcher.api.stop()
    launcher.zk_client.disconnect()
    launcher.tracing.stop()
    launcher.nodescan_worker.join()
    launcher.monitoring_server.join()
    launcher._close_rust_session()
    launcher._join_called = True
    launcher.log.debug("Joined launcher")
