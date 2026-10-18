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


def _record_original_sms():
    """Capture advance/_updateNode before unittest.mock patches them."""
    if _ORIG_SM:
        return
    from zuul.driver.aws import awsendpoint
    from zuul.driver.azure import azureendpoint
    from zuul.driver.openstack import openstackendpoint
    from zuul.driver.kubernetes import kubernetesendpoint

    _ORIG_SM.update({
        ("aws", "create"): awsendpoint.AwsCreateStateMachine.advance,
        ("aws", "delete"): awsendpoint.AwsDeleteStateMachine.advance,
        ("aws", "snapshot"): awsendpoint.AwsSnapshotStateMachine.advance,
        ("aws", "update_node"): awsendpoint.AwsCreateStateMachine._updateNode,
        ("azure", "create"): azureendpoint.AzureCreateStateMachine.advance,
        ("azure", "delete"): azureendpoint.AzureDeleteStateMachine.advance,
        ("azure", "update_node"): azureendpoint.AzureCreateStateMachine._updateNode,
        ("openstack", "create"): openstackendpoint.OpenstackCreateStateMachine.advance,
        ("openstack", "delete"): openstackendpoint.OpenstackDeleteStateMachine.advance,
        ("openstack", "update_node"):
            openstackendpoint.OpenstackCreateStateMachine._updateNode,
        ("kubernetes", "create"):
            kubernetesendpoint.KubernetesCreateStateMachine.advance,
        ("kubernetes", "delete"):
            kubernetesendpoint.KubernetesDeleteStateMachine.advance,
        ("kubernetes", "update_node"):
            kubernetesendpoint.KubernetesCreateStateMachine._updateNode,
    })


# Filled by _record_original_sms; must run before test patches.
_ORIG_SM = {}
try:
    _record_original_sms()
except Exception:
    pass


def bootstrap(launcher, config, connections, hostname):
    _record_original_sms()
    launcher._running = True
    # Default matches server.Launcher; tests.base createLauncher sets
    # False so unit tests do not run leaked-resource cleanup (moto has
    # no describe_import_snapshot_tasks).
    if not hasattr(launcher, "_start_cleanup"):
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
    # server.Launcher.tracer is a class attribute. _requestNode calls
    # self.tracer.start_span, and this object is not an instance of
    # that class. __getattr__ also forwards it; set it here so accept
    # does not depend on that fallback.
    from opentelemetry import trace

    launcher.tracer = trace.get_tracer("zuul")

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
    launcher._layout_generation = 0
    launcher.image_build_registry = ImageBuildRegistry(
        launcher.zk_client, _image_updated
    )
    launcher.image_upload_registry = ImageUploadRegistry(
        launcher.zk_client, _image_updated
    )

    # Keep Python NodescanWorker: unit tests patch
    # zuul.launcher.server.NodescanRequest.advance; the cloud "nodescan"
    # adapter drives that class through this worker. Pure Rust nodescan
    # would miss those patches.
    launcher.nodescan_worker = NodescanWorker()
    launcher._run_lock = threading.Lock()
    launcher.launcher_thread = threading.Thread(target=launcher.run, name="Launcher")
    launcher.upload_uuids_in_queue = set()
    launcher.upload_uuids_in_delete_queue = set()
    # Upload / cleanup / stats threads run in Rust (start_rust_workers).
    # Keep empty placeholders so getattr paths and bound DeleteJob/UploadJob
    # code that touch executor attributes do not AttributeError; they are
    # never started. Tests that call cleanup_worker._run() / .start() still
    # use the Python CleanupWorker object below.
    launcher.upload_executor = None
    launcher.endpoint_upload_executor = None
    launcher.endpoint_import_executor = None
    launcher.cleanup_worker = CleanupWorker(launcher)
    # Upload, snapshot, and cleanup call back into Launcher methods the
    # PyO3 class does not define (_getProviders, _processNodeSnapshot).
    _bind_missing_methods(launcher)
    _install_node_cache_hook(launcher)

    # Not started: Rust StatsWorker owns the stats election. Keep a
    # cancel()-able stand-in so stop() is safe if something still
    # references it.
    from zuul.launcher.server import LauncherStatsElection

    launcher.stats_election = LauncherStatsElection(launcher.zk_client)
    launcher.stats_thread = None


def _bind_missing_methods(launcher):
    """Attach Launcher methods the Rust class does not implement.

    Upload, snapshot, quota, and stats code in server.py calls back into
    the launcher. Binding the unbound functions keeps those paths on the
    14.3.0 implementation.
    """
    if getattr(launcher, "_methods_bound", False):
        return
    from zuul.launcher.server import Launcher

    cls = type(launcher)
    for name, func in vars(Launcher).items():
        if not callable(func) or name.startswith("__"):
            continue
        if hasattr(cls, name):
            continue
        setattr(launcher, name, func.__get__(launcher, cls))
    launcher._methods_bound = True


def call_launcher(launcher, name, *args):
    from zuul.launcher.server import Launcher

    _bind_missing_methods(launcher)
    return getattr(Launcher, name)(launcher, *args)


def call_launcher_args(launcher, name, args):
    return call_launcher(launcher, name, *args)


def _attach_state_machines(launcher, node):
    """Put a state-machine stand-in on the cached node.

    test_launcher_failover reads create_state from ZooKeeper and
    INSTANCE_CREATING from node.create_state_machine. Rust drives the
    real advance path; this only exposes class constants and toDict().
    """
    if node is None:
        return
    machines = launcher.__dict__.setdefault("_cloud_sms", {})
    uuid = getattr(node, "uuid", None)
    if not uuid:
        return
    for kind, attr, state_attr in (
        ("create", "create_state_machine", "create_state"),
        ("delete", "delete_state_machine", "delete_state"),
    ):
        machine = machines.get((kind, uuid))
        if machine is None:
            state = getattr(node, state_attr, None) or {}
            if not state:
                continue
            cls = _sm_class_for_node(node, kind)
            if cls is None:
                continue
            machine = _StateMachineRef(cls, state)
            machines[(kind, uuid)] = machine
        else:
            state = getattr(node, state_attr, None) or {}
            if state and hasattr(machine, "sync_from_state"):
                machine.sync_from_state(state)
        node._set(**{attr: machine})


def _install_node_cache_hook(launcher):
    cache = launcher.api.nodes_cache
    if getattr(cache, "_rust_sm_hook", False):
        return
    orig_update = cache.updateFromRaw
    orig_object = cache.objectFromRaw

    def updateFromRaw(obj, key, data, stat):
        # Attach after the ZK payload is applied so create_state is
        # visible. Attaching first left create_state_machine unset and
        # broke test_launcher_failover.
        result = orig_update(obj, key, data, stat)
        _attach_state_machines(launcher, obj)
        return result

    def objectFromRaw(key, data, stat):
        obj = orig_object(key, data, stat)
        _attach_state_machines(launcher, obj)
        return obj

    cache.updateFromRaw = updateFromRaw
    cache.objectFromRaw = objectFromRaw
    cache._rust_sm_hook = True


def emit_stats(launcher, payload):
    """Send Rust launch counters to the same statsd client the tests record."""
    import json

    if not getattr(launcher, "statsd", None):
        return
    for item in json.loads(payload):
        kind = item.get("kind")
        name = item.get("name")
        value = item.get("value", 0)
        if kind == "c":
            launcher.statsd.incr(name, int(value))
        elif kind == "ms":
            launcher.statsd.timing(name, value)
        elif kind == "g":
            launcher.statsd.gauge(name, value)


def maintain_uploads(launcher):
    """No-op: Rust process_once already runs maintain_images_and_uploads."""
    return


def maintenance_flags(launcher):
    """Layout / image / delete events for the Rust maintain path."""
    import json

    layout = bool(getattr(launcher, "_layout_just_updated", False))
    launcher._layout_just_updated = False
    image = launcher.image_updated_event.is_set()
    if image:
        launcher.image_updated_event.clear()
    deleted = launcher.upload_deleted_event.is_set()
    if deleted:
        launcher.upload_deleted_event.clear()
    return json.dumps({
        "layout_updated": layout,
        "image_updated": image,
        "upload_deleted": deleted,
    })


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
    # NodescanWorker stays Python so NodescanRequest.advance patches apply.
    launcher.log.debug("Starting nodescan worker")
    launcher.nodescan_worker.start()
    launcher.log.debug("Starting launcher thread")
    launcher.launcher_thread.start()
    # Cleanup, stats election, and the 1/10/20 upload pools run in Rust.
    # Do not start Python cleanup_worker / stats_thread (avoids double
    # stats election). Honor _start_cleanup like server.py start().
    launcher.log.debug("Starting rust workers")
    launcher.start_rust_workers(start_cleanup=bool(
        getattr(launcher, "_start_cleanup", True)
    ))
    # Match MonitoringThreadPoolExecutor.__init__ emitting gauge 0.
    _emit_upload_workers_gauge(launcher, 0)


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
                    # stop() sets _running and then takes this lock before
                    # it shuts endpoint executors down.
                    if not launcher._running:
                        break
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
    launcher.log.debug("Stopping rust workers")
    launcher.stop_rust_workers()
    # Python cleanup object may have been started by a test; stop it.
    if getattr(launcher, "cleanup_worker", None) is not None:
        launcher.cleanup_worker.stop()
        launcher.cleanup_worker.join()
    if getattr(launcher, "stats_election", None) is not None:
        launcher.stats_election.cancel()
    if getattr(launcher, "stats_thread", None) is not None:
        launcher.stats_thread.join()
    # The run loop polls instead of waiting on a ChildrenWatch, so it
    # is often inside a state-machine step when stop() begins.
    with launcher._run_lock:
        launcher.nodescan_worker.stop()
        launcher.connections.stop()
        launcher.monitoring_server.stop()
    launcher.log.debug("Stopped launcher")


def publish_layout(launcher, payload):
    """Project the Rust layout snapshot onto the Python launcher object.

    Providers are loaded again through LayoutProvidersStore so tests see
    real provider objects (images, labels, endpoints). The Rust loop
    already read the same ZooKeeper paths under the tenant read lock.
    """
    import json

    from zuul.zk.layout import LayoutState
    from zuul.zk.zkobject import ZKContext

    data = json.loads(payload)
    generation = data.get("generation", 0)
    if generation == getattr(launcher, "_layout_generation", 0):
        _set_running(launcher, data)
        return
    tenants = data.get("tenants") or {}
    states = {}
    for name, item in tenants.items():
        states[name] = LayoutState(
            item["tenant_name"],
            item["hostname"],
            item["last_reconfigured"],
            item.get("uuid"),
            item.get("branch_cache_min_ltimes"),
            item.get("last_reconfigure_event_ltime", -1),
            item.get("ltime", -1),
        )
    launcher.local_layout_state = states
    providers = {}
    ctx = ZKContext(
        launcher.zk_client, None, launcher.join_event, launcher.log
    )
    for name in tenants:
        try:
            providers[name] = list(
                launcher.layout_providers_store.get(
                    ctx, launcher.zk_client, name
                )
            )
        except Exception:
            launcher.log.exception("Unable to mirror providers for %s", name)
            providers[name] = launcher.tenant_providers.get(name, [])
    launcher.tenant_providers = providers
    launcher._layout_generation = generation
    _post_config(launcher)
    launcher._layout_just_updated = True
    _set_running(launcher, data)


def _set_running(launcher, data):
    if data.get("state") != "running":
        return
    if launcher.component_info.state != launcher.component_info.RUNNING:
        launcher.component_info.state = launcher.component_info.RUNNING


def get_provider(launcher, tenant_name, provider_name):
    from zuul.launcher.server import ProviderNodeError

    try:
        providers = launcher.tenant_providers[tenant_name]
    except KeyError:
        raise ProviderNodeError(
            f"Unable to find {provider_name} in tenant {tenant_name}"
        )
    for provider in providers:
        if provider.name == provider_name:
            return provider
    raise ProviderNodeError(
        f"Unable to find {provider_name} in tenant {tenant_name}"
    )


def cloud_call(launcher, op, payload):
    """Bridge used by the Rust driver state machines.

    Moto, azul, OpenStack/K8s fakes, and unittest patches of endpoint
    methods (and StateMachine.advance) live in-process. AWS / Azure /
    OpenStack / Kubernetes create and delete run in Rust and call
    fine-grained ep.* ops here; sm.patched_advance covers tests that
    replace advance with a Mock. Legacy step/fail remain for leftover
    callers.
    """
    import json

    data = json.loads(payload) if payload else {}
    try:
        if op == "step":
            result = _cloud_step(launcher, data)
        elif op == "nodescan":
            result = _nodescan_step(launcher, data)
        elif op == "endpoint_limits":
            result = _endpoint_limits(launcher, data)
        elif op == "label_quota":
            result = _label_quota(launcher, data)
        elif op == "node_tags":
            result = _node_tags(launcher, data)
        elif op == "image_attrs":
            result = _image_attrs(launcher, data)
        elif op == "provider_quota":
            result = _provider_quota(launcher, data)
        elif op == "quota_for_node":
            # Unmanaged usage is still recorded by the Python CleanupWorker;
            # Rust asks for the boolean until that worker is fully ported.
            result = _quota_for_node(launcher, data)
        elif op == "quota_for_label":
            result = _quota_for_label(launcher, data)
        elif op == "accept_one":
            result = _accept_one(launcher, data)
        elif op == "fail":
            result = _cloud_fail(launcher, data)
        elif op == "node_visible":
            result = _node_visible(launcher, data)
        elif op == "node.cache_insert":
            result = _node_cache_insert(launcher, data)
        elif op == "node.cache_assign":
            result = _node_cache_assign(launcher, data)
        elif op == "class_attr":
            result = _class_attr(data)
        elif op == "matching_requests":
            result = _matching_requests(launcher)
        elif op == "matching_nodes":
            result = _matching_nodes(launcher)
        elif op == "request_cache_ready":
            result = _request_cache_ready(launcher, data)
        elif op == "ep.refresh":
            result = _ep_refresh(launcher, data)
        elif op == "ep.refresh_delete":
            result = _ep_refresh_delete(launcher, data)
        elif op == "ep.submit_create_instance":
            result = _ep_submit_create_instance(launcher, data)
        elif op == "ep.complete_create_instance":
            result = _ep_complete_create_instance(launcher, data)
        elif op == "ep.submit_allocate_host":
            result = _ep_submit_allocate_host(launcher, data)
        elif op == "ep.complete_allocate_host":
            result = _ep_complete_allocate_host(launcher, data)
        elif op == "ep.delete_instance":
            result = _ep_delete_instance(launcher, data)
        elif op == "ep.release_host":
            result = _ep_release_host(launcher, data)
        elif op == "ep.quota_for_label":
            result = _ep_quota_for_label(launcher, data)
        elif op == "ep.create_snapshot":
            result = _ep_create_snapshot(launcher, data)
        elif op == "ep.refresh_snapshot":
            result = _ep_refresh(launcher, data)
        elif op == "ep.azure_list_instance_id":
            result = _ep_azure_list_instance_id(launcher, data)
        elif op == "ep.azure_get_image_reference":
            result = _ep_azure_get_image_reference(launcher, data)
        elif op == "ep.azure_get_subnet_id":
            result = _ep_azure_get_subnet_id(launcher, data)
        elif op == "ep.azure_submit_create_vm":
            result = _ep_azure_submit_create_vm(launcher, data)
        elif op == "ep.azure_complete_create_vm":
            result = _ep_azure_complete_create_vm(launcher, data)
        elif op == "ep.azure_delete_vm":
            result = _ep_azure_delete_vm(launcher, data)
        elif op == "ep.azure_update_node":
            result = _ep_azure_update_node(launcher, data)
        elif op == "ep.os_find_flavor":
            result = _ep_os_find_flavor(launcher, data)
        elif op == "ep.os_list_instance_id":
            result = _ep_os_list_instance_id(launcher, data)
        elif op == "ep.os_submit_create_server":
            result = _ep_os_submit_create_server(launcher, data)
        elif op == "ep.os_complete_create_server":
            result = _ep_os_complete_create_server(launcher, data)
        elif op == "ep.os_refresh_server":
            result = _ep_os_refresh_server(launcher, data)
        elif op == "ep.os_get_server":
            result = _ep_os_get_server(launcher, data)
        elif op == "ep.os_needs_floating_ip":
            result = _ep_os_needs_floating_ip(launcher, data)
        elif op == "ep.os_create_floating_ip":
            result = _ep_os_create_floating_ip(launcher, data)
        elif op == "ep.os_refresh_floating_ip":
            result = _ep_os_refresh_floating_ip(launcher, data)
        elif op == "ep.os_attach_ip":
            result = _ep_os_attach_ip(launcher, data)
        elif op == "ep.os_server_floating_ip":
            result = _ep_os_server_floating_ip(launcher, data)
        elif op == "ep.os_get_instance":
            result = _ep_os_get_instance(launcher, data)
        elif op == "ep.os_has_floating_ips":
            result = _ep_os_has_floating_ips(launcher, data)
        elif op == "ep.os_get_floating_ips":
            result = _ep_os_get_floating_ips(launcher, data)
        elif op == "ep.os_delete_floating_ip":
            result = _ep_os_delete_floating_ip(launcher, data)
        elif op == "ep.os_refresh_floating_ip_delete":
            result = _ep_os_refresh_floating_ip_delete(launcher, data)
        elif op == "ep.os_submit_delete_server":
            result = _ep_os_submit_delete_server(launcher, data)
        elif op == "ep.os_complete_delete_server":
            result = _ep_os_complete_delete_server(launcher, data)
        elif op == "ep.os_refresh_server_delete":
            result = _ep_os_refresh_server_delete(launcher, data)
        elif op == "ep.os_handle_server_fault":
            result = _ep_os_handle_server_fault(launcher, data)
        elif op == "ep.k8s_setup":
            result = _ep_k8s_setup(launcher, data)
        elif op == "ep.k8s_use_openshift":
            result = _ep_k8s_use_openshift(launcher, data)
        elif op == "ep.k8s_create_namespace":
            result = _ep_k8s_create_namespace(launcher, data)
        elif op == "ep.k8s_create_project":
            result = _ep_k8s_create_project(launcher, data)
        elif op == "ep.k8s_get_subject_access_review":
            result = _ep_k8s_get_subject_access_review(launcher, data)
        elif op == "ep.k8s_create_image_pull_secrets":
            result = _ep_k8s_create_image_pull_secrets(launcher, data)
        elif op == "ep.k8s_create_service_account":
            result = _ep_k8s_create_service_account(launcher, data)
        elif op == "ep.k8s_get_token":
            result = _ep_k8s_get_token(launcher, data)
        elif op == "ep.k8s_create_role":
            result = _ep_k8s_create_role(launcher, data)
        elif op == "ep.k8s_create_pod":
            result = _ep_k8s_create_pod(launcher, data)
        elif op == "ep.k8s_get_pod":
            result = _ep_k8s_get_pod(launcher, data)
        elif op == "ep.k8s_delete_namespace":
            result = _ep_k8s_delete_namespace(launcher, data)
        elif op == "ep.k8s_delete_project":
            result = _ep_k8s_delete_project(launcher, data)
        elif op == "ep.k8s_update_node":
            result = _ep_k8s_update_node(launcher, data)
        elif op == "sm.patched_advance":
            result = _sm_patched_advance(launcher, data)
        # Upload / delete / snapshot / cleanup SDK-boundary adapters.
        # upload.job_run   — AwsImageUploadJob / Import / Copy .run (+ azure/os)
        # upload.delete    — provider.deleteImage + ImageUpload ZK delete
        # snapshot.advance — lock/prepare/commit for Rust AwsSnapshot
        # cleanup.tick     — Python CleanupWorker._run (leaked resources)
        elif op == "upload.job_run":
            result = _upload_job_run(launcher, data)
        elif op == "upload.artifact_job":
            result = _upload_artifact_job(launcher, data)
        elif op == "upload.queue_add":
            for uuid in data.get("upload_uuids") or []:
                launcher.upload_uuids_in_queue.add(uuid)
            result = {"ok": True}
        elif op == "upload.delete":
            result = _upload_delete(launcher, data)
        elif op == "upload.reset_crashed":
            result = _upload_reset_crashed(launcher, data)
        elif op == "upload.delete_artifact":
            result = _upload_delete_artifact(launcher, data)
        elif op == "snapshot.advance":
            result = _snapshot_advance(launcher, data)
        elif op == "snapshot.sm_advance":
            result = _snapshot_sm_advance(launcher, data)
        elif op == "cleanup.tick":
            result = _cleanup_tick(launcher, data)
        else:
            result = {"error_kind": "other", "message": "unknown cloud op %s" % op}
    except Exception as exc:
        result = _cloud_error(exc)
        # Quota and capacity are retried without a traceback. Other
        # failures are logged with the original stack so the unit
        # tests' okay_tracebacks filters still match. An endpoint
        # executor already shut down by stop() is not a node error.
        if (
            isinstance(exc, RuntimeError)
            and "cannot schedule new futures after shutdown" in str(exc)
        ):
            launcher.log.debug("cloud op %s ended after executor shutdown", op)
        elif op in (
            "upload.job_run",
            "upload.artifact_job",
            "cleanup.tick",
            "snapshot.advance",
            "snapshot.sm_advance",
        ):
            # These adapters log their own exception() with the original
            # stack (Error in endpoint upload job / cleanup worker / …)
            # so okay_tracebacks can match. Avoid a second wrapper frame
            # whose msg is "cloud op %s failed".
            launcher.log.debug(
                "cloud op %s %s: %s",
                op,
                result.get("error_kind"),
                result.get("message"),
            )
        elif result.get("error_kind") == "other":
            launcher.log.exception("cloud op %s failed", op)
        else:
            launcher.log.info(
                "cloud op %s %s: %s",
                op,
                result.get("error_kind"),
                result.get("message"),
            )
    return json.dumps(result, default=str)


def _cloud_error(exc):
    from zuul import exceptions

    message = "%s: %s" % (type(exc).__name__, exc)
    if isinstance(exc, getattr(exceptions, "QuotaException", ())):
        return {"error_kind": "quota", "message": message}
    if isinstance(exc, getattr(exceptions, "CapacityException", ())):
        return {"error_kind": "capacity", "message": message}
    return {"error_kind": "other", "message": message}


def _find_provider(launcher, canonical):
    for providers in launcher.tenant_providers.values():
        for provider in providers:
            if provider.canonical_name == canonical or provider.name == canonical:
                return provider
    raise Exception("Unable to find provider %s" % canonical)


def _choose_launcher_for_endpoint(launcher, endpoint):
    # Launcher._chooseLauncherForEndpoint. Include ourselves if the
    # component registry watch has not delivered our own znode yet.
    import mmh3

    from zuul.zk.components import COMPONENT_REGISTRY

    all_launchers = {
        c.hostname: c for c in COMPONENT_REGISTRY.registry.all("launcher")
    }
    hostname = launcher.component_info.hostname
    if hostname not in all_launchers:
        all_launchers[hostname] = launcher.component_info
    candidates = {
        name: component
        for name, component in all_launchers.items()
        if not component.connection_filter
        or endpoint.connection.connection_name in component.connection_filter
    }
    scores = {mmh3.hash(name, signed=False): name for name in candidates}
    return scores[sorted(scores)[0]]


def _post_config(launcher):
    # The provider half of Launcher.updateTenantProviders: start each
    # endpoint, refresh quota, and fill the per-label quota cache.
    # provider.postConfig only refreshes labels when update is true,
    # which is the designated-launcher / quota-updated flag.
    seen_endpoint_names = set()
    seen_provider_names = set()
    endpoints = {}
    for providers in launcher.tenant_providers.values():
        for provider in providers:
            if (
                launcher.connection_filter
                and provider.connection_name not in launcher.connection_filter
            ):
                continue
            endpoint = provider.getEndpoint()
            endpoints[endpoint.canonical_name] = endpoint
            designated = _choose_launcher_for_endpoint(launcher, endpoint)
            force = bool(designated == launcher.component_info.hostname)
            endpoint.start()
            try:
                endpoint.postConfig(provider)
                if endpoint.canonical_name not in seen_endpoint_names:
                    quota_updated = endpoint.refreshQuotaLimits(force)
                    # 14.3.0 records the provider name in this set.
                    seen_endpoint_names.add(provider.canonical_name)
                    force = quota_updated or force
                if provider.canonical_name not in seen_provider_names:
                    provider.postConfig(force)
                    seen_provider_names.add(provider.canonical_name)
            except Exception:
                launcher.log.exception("Error in postconfig for %s:", provider)
    launcher.endpoints = endpoints


def _ensure_endpoint(launcher, provider):
    # Endpoint startup and quota refresh happen in _post_config. This
    # only covers a provider that was not part of that pass.
    endpoint = provider.getEndpoint()
    started = launcher.__dict__.setdefault("_endpoints_started", set())
    if provider.canonical_name in started:
        return endpoint
    endpoint.start()
    started.add(provider.canonical_name)
    return endpoint


def _export_node(node):
    keys = (
        "state",
        "state_time",
        "interface_ip",
        "public_ipv4",
        "private_ipv4",
        "public_ipv6",
        "private_ipv6",
        "host_id",
        "cloud",
        "region",
        "az",
        "username",
        "python_path",
        "shell_type",
        "connection_port",
        "connection_type",
        "host_keys",
        "host_key_checking",
        "attributes",
        "aws_instance_id",
        "aws_dedicated_host_id",
        "aws_launch_template_name",
        "azure_vm_id",
        "openstack_server_id",
        "openstack_floating_ip_id",
        "kubernetes_namespace_id",
        "kubernetes_connection",
        "node_properties",
        "resources",
    )
    out = {}
    for key in keys:
        if hasattr(node, key):
            out[key] = _jsonable(getattr(node, key))
    quota = getattr(node, "quota", None)
    if quota is not None and hasattr(quota, "getResources"):
        out["quota"] = _jsonable(quota.getResources())
    return out


class _mutable:
    """Allow ZKObject attribute writes without saving the object."""

    def __init__(self, node):
        self.node = node

    def __enter__(self):
        self.node._set(_active_context=True)
        return self.node

    def __exit__(self, *_exc):
        self.node._set(_active_context=None)
        return False


def _jsonable(value):
    import math

    if isinstance(value, float) and not math.isfinite(value):
        # serde_json rejects bare Infinity/NaN; null means unlimited.
        return None
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    return str(value)


def _quota_payload(quota):
    import math

    default = getattr(quota, "default", math.inf)
    if default == math.inf:
        default = None
    resources = quota.getResources() if hasattr(quota, "getResources") else {}
    return {"default": default, "quota": _jsonable(resources)}


def _cloud_step(launcher, data):
    from zuul.zk.zkobject import ZKContext
    from zuul.model import ProviderNode

    kind = data.get("kind") or "create"
    node_uuid = data["node_uuid"]
    key = (kind, node_uuid)
    machines = launcher.__dict__.setdefault("_cloud_sms", {})
    if key not in machines:
        provider = _find_provider(launcher, data["provider"])
        _ensure_endpoint(launcher, provider)
        ctx = ZKContext(
            launcher.zk_client, None, launcher.join_event, launcher.log
        )
        node = ProviderNode.fromZK(ctx, path=ProviderNode._getPath(node_uuid))
        # The state machine assigns node attributes. ZKObject rejects
        # that unless an active context is set. Rust writes the node
        # back from the exported fields, so this does not save here.
        with _mutable(node):
            if kind == "delete":
                machines[key] = provider.getDeleteStateMachine(node, launcher.log)
            else:
                image_external_id = data.get("image_external_id")
                if not image_external_id:
                    image_external_id = call_launcher(
                        launcher, "getImageExternalId", node, provider
                    )
                machines[key] = provider.getCreateStateMachine(
                    node, image_external_id, launcher.log
                )
    machine = machines[key]
    with _mutable(machine.node):
        external = machine.advance()
    node = getattr(machine, "node", None)
    exported = _export_node(node) if node is not None else {}
    # Persist the machine state so a restarted launcher resumes it.
    if hasattr(machine, "toDict"):
        state_key = "delete_state" if kind == "delete" else "create_state"
        exported[state_key] = _jsonable(machine.toDict())
    payload = {
        "state": getattr(machine, "state", None),
        "complete": bool(getattr(machine, "complete", False)),
        "interface_ip": getattr(node, "interface_ip", None) if node is not None else None,
        "node": exported,
    }
    if external:
        payload["external_id"] = _jsonable(external)
    cached = launcher.api.getProviderNode(node_uuid)
    _attach_state_machines(launcher, cached)
    if payload["complete"]:
        machines.pop(key, None)
    return payload


def _cloud_fail(launcher, data):
    kind = data.get("kind") or "delete"
    key = (kind, data["node_uuid"])
    machines = launcher.__dict__.setdefault("_cloud_sms", {})
    machine = machines.get(key)
    if machine is None:
        return {"state": "complete", "complete": True, "node": {}}
    if not hasattr(machine, "node") or not hasattr(machine, "fail"):
        return {"state": "complete", "complete": True, "node": {}}
    with _mutable(machine.node):
        machine.fail()
    payload = {
        "state": getattr(machine, "state", None),
        "complete": bool(getattr(machine, "complete", False)),
        "node": _export_node(machine.node),
    }
    if payload["complete"]:
        machines.pop(key, None)
    return payload


def _sm_class(driver, kind):
    if driver == "aws":
        from zuul.driver.aws import awsendpoint

        return {
            "create": awsendpoint.AwsCreateStateMachine,
            "delete": awsendpoint.AwsDeleteStateMachine,
            "snapshot": awsendpoint.AwsSnapshotStateMachine,
            "update_node": awsendpoint.AwsCreateStateMachine,
        }.get(kind)
    if driver == "azure":
        from zuul.driver.azure import azureendpoint

        return {
            "create": azureendpoint.AzureCreateStateMachine,
            "delete": azureendpoint.AzureDeleteStateMachine,
            "update_node": azureendpoint.AzureCreateStateMachine,
        }.get(kind)
    if driver == "openstack":
        from zuul.driver.openstack import openstackendpoint

        return {
            "create": openstackendpoint.OpenstackCreateStateMachine,
            "delete": openstackendpoint.OpenstackDeleteStateMachine,
            "update_node": openstackendpoint.OpenstackCreateStateMachine,
        }.get(kind)
    if driver == "kubernetes":
        from zuul.driver.kubernetes import kubernetesendpoint

        return {
            "create": kubernetesendpoint.KubernetesCreateStateMachine,
            "delete": kubernetesendpoint.KubernetesDeleteStateMachine,
            "update_node": kubernetesendpoint.KubernetesCreateStateMachine,
        }.get(kind)
    return None


def _sm_class_for_node(node, kind):
    name = type(node).__name__
    if "Aws" in name or hasattr(node, "aws_instance_id"):
        return _sm_class("aws", kind)
    if "Azure" in name or hasattr(node, "azure_vm_id"):
        return _sm_class("azure", kind)
    if "Openstack" in name or hasattr(node, "openstack_server_id"):
        return _sm_class("openstack", kind)
    if "Kubernetes" in name or hasattr(node, "kubernetes_namespace_id"):
        return _sm_class("kubernetes", kind)
    return None


class _StateMachineRef:
    """Stand-in with class constants and toDict for cache attachment."""

    def __init__(self, cls, state_dict):
        self._cls = cls
        for name in dir(cls):
            if name.isupper():
                setattr(self, name, getattr(cls, name))
        self.sync_from_state(state_dict or {})

    def sync_from_state(self, state_dict):
        self.state = state_dict.get("state", getattr(self._cls, "START", "start"))
        self.start_time = state_dict.get("start_time", 0)
        self.complete = bool(state_dict.get("complete", False))
        self.attempts = state_dict.get("attempts", 0)
        self.image_external_id = state_dict.get("image_external_id")
        self.snapshot_id = state_dict.get("snapshot_id")

    def toDict(self):
        data = {
            "state": self.state,
            "start_time": self.start_time,
        }
        if self.attempts:
            data["attempts"] = self.attempts
        if self.image_external_id is not None:
            data["image_external_id"] = self.image_external_id
        if self.snapshot_id is not None:
            data["snapshot_id"] = self.snapshot_id
        return data


def _future_store(launcher):
    return launcher.__dict__.setdefault("_ep_futures", {})


def _store_future(launcher, future):
    store = _future_store(launcher)
    seq = launcher.__dict__.setdefault("_ep_future_seq", 0) + 1
    launcher.__dict__["_ep_future_seq"] = seq
    fid = "f%d" % seq
    store[fid] = future
    return fid


def _provider_endpoint(launcher, data):
    provider = _find_provider(launcher, data["provider"])
    endpoint = _ensure_endpoint(launcher, provider)
    return provider, endpoint


def _load_node(launcher, node_uuid):
    from zuul.model import ProviderNode
    from zuul.zk.zkobject import ZKContext

    ctx = ZKContext(
        launcher.zk_client, None, launcher.join_event, launcher.log
    )
    return ProviderNode.fromZK(ctx, path=ProviderNode._getPath(node_uuid))


def _ep_refresh(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    force = bool(data.get("force"))
    # Azure _refresh accepts force=; AWS ignores unknown kwargs via its
    # own signature (obj only). Call with force only when requested.
    if force:
        obj = endpoint._refresh(data.get("obj") or {}, force=True)
    else:
        obj = endpoint._refresh(data.get("obj") or {})
    return {"obj": _jsonable(obj)}


def _ep_refresh_delete(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    obj = endpoint._refreshDelete(data.get("obj"))
    return {"obj": _jsonable(obj)}


def _ep_submit_create_instance(launcher, data):
    provider, endpoint = _provider_endpoint(launcher, data)
    label = provider.labels[data["label"]]
    flavor = provider.flavors[label.flavor]
    image = provider.images[label.image]
    launch_template_name = data.get("launch_template_name")
    if not launch_template_name:
        names = endpoint.provider_label_template_names.get(
            provider.canonical_name, {}
        )
        launch_template_name = names.get(label.name)
    image_external_id = data.get("image_external_id")
    if not image_external_id and data.get("node_uuid"):
        node = _load_node(launcher, data["node_uuid"])
        image_external_id = call_launcher(
            launcher, "getImageExternalId", node, provider
        )
    tags = data.get("tags") or {}
    hostname = data.get("hostname") or ""
    future = endpoint._submitCreateInstance(
        provider,
        label,
        flavor,
        image,
        image_external_id,
        tags,
        hostname,
        data.get("dedicated_host_id"),
        launch_template_name,
        launcher.log,
    )
    return {
        "future_id": _store_future(launcher, future),
        "launch_template_name": launch_template_name,
        "image_external_id": image_external_id,
    }


def _ep_complete_create_instance(launcher, data):
    provider, endpoint = _provider_endpoint(launcher, data)
    future = _future_store(launcher).get(data.get("future_id"))
    if future is None:
        return {"instance": None}
    instance = endpoint._completeCreateInstance(future)
    if instance is not None:
        _future_store(launcher).pop(data.get("future_id"), None)
    return {"instance": _jsonable(instance)}


def _ep_submit_allocate_host(launcher, data):
    provider, endpoint = _provider_endpoint(launcher, data)
    label = provider.labels[data["label"]]
    flavor = provider.flavors[label.flavor]
    future = endpoint._submitAllocateHost(
        label,
        flavor,
        data.get("tags") or {},
        data.get("hostname") or "",
        launcher.log,
    )
    return {"future_id": _store_future(launcher, future)}


def _ep_complete_allocate_host(launcher, data):
    provider, endpoint = _provider_endpoint(launcher, data)
    future = _future_store(launcher).get(data.get("future_id"))
    if future is None:
        return {"host": None}
    host = endpoint._completeAllocateHost(future)
    if host is not None:
        _future_store(launcher).pop(data.get("future_id"), None)
    return {"host": _jsonable(host)}


def _ep_delete_instance(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    instance = endpoint._deleteInstance(data["instance_id"], launcher.log)
    return {"instance": _jsonable(instance)}


def _ep_release_host(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    host = endpoint._releaseHost(data["host_id"], launcher.log)
    return {"host": _jsonable(host)}


def _ep_quota_for_label(launcher, data):
    provider, endpoint = _provider_endpoint(launcher, data)
    label = provider.labels[data["label"]]
    module = type(endpoint).__module__
    if "kubernetes" in module:
        quota = endpoint.getQuotaForLabel(label)
    else:
        flavor = provider.flavors[label.flavor]
        if "aws" in module and data.get("instance_type") is not None:
            quota = endpoint.getQuotaForLabel(
                label, flavor, data.get("instance_type")
            )
        else:
            quota = endpoint.getQuotaForLabel(label, flavor)
    return _quota_payload(quota)


def _ep_create_snapshot(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    snapshot = endpoint._createSnapshot(
        data.get("volume_id"), data.get("tags") or {}
    )
    return {"snapshot": _jsonable(snapshot)}


def _ep_azure_list_instance_id(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    node_uuid = data.get("node_uuid")
    for instance in endpoint.listInstances():
        if instance.metadata.get("zuul_node_uuid") == node_uuid:
            return {"azure_vm_id": instance.azure_vm_id}
    return {"azure_vm_id": None}


def _ep_azure_get_image_reference(launcher, data):
    provider, endpoint = _provider_endpoint(launcher, data)
    label = provider.labels[data["label"]]
    image = provider.images[label.image]
    if data.get("image_external_id"):
        return {"image_reference": {"id": data["image_external_id"]}}
    return {
        "image_reference": _jsonable(
            endpoint._getImageReferenceForImage(image)
        )
    }


def _ep_azure_get_subnet_id(launcher, data):
    provider, endpoint = _provider_endpoint(launcher, data)
    label = provider.labels[data["label"]]
    subnet_id = endpoint._getSubnetIdForLabel(
        provider.resource_group, label
    )
    return {"subnet_id": subnet_id}


def _ep_azure_submit_create_vm(launcher, data):
    provider, endpoint = _provider_endpoint(launcher, data)
    label = provider.labels[data["label"]]
    flavor = provider.flavors[label.flavor]
    image = provider.images[label.image]
    image_reference = data.get("image_reference")
    if not image_reference and data.get("image_external_id"):
        image_reference = {"id": data["image_external_id"]}
    if not image_reference:
        image_reference = endpoint._getImageReferenceForImage(image)
    subnet_id = data.get("subnet_id")
    if not subnet_id:
        subnet_id = endpoint._getSubnetIdForLabel(
            provider.resource_group, label
        )
    future = endpoint._submitApi(
        endpoint._createVirtualMachine,
        provider.resource_group,
        label,
        flavor,
        image,
        image_reference,
        subnet_id,
        data.get("tags") or {},
        data.get("hostname") or "",
        launcher.log,
    )
    return {"future_id": _store_future(launcher, future)}


def _ep_azure_complete_create_vm(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    future = _future_store(launcher).get(data.get("future_id"))
    if future is None:
        return {"vm": None}
    vm = endpoint._completeApi(future)
    if vm is not None:
        _future_store(launcher).pop(data.get("future_id"), None)
    return {"vm": _jsonable(vm)}


def _ep_azure_delete_vm(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    vm = endpoint._deleteVirtualMachine(data.get("vm_id"))
    return {"vm": _jsonable(vm)}


def _ep_azure_update_node(launcher, data):
    from zuul.driver.azure.azuremodel import AzureInstance

    provider, endpoint = _provider_endpoint(launcher, data)
    node = _load_node(launcher, data["node_uuid"])
    label = provider.labels[data["label"]]
    flavor = provider.flavors[label.flavor]
    quota = endpoint.getQuotaForLabel(label, flavor)
    with _mutable(node):
        if data.get("azure_vm_id"):
            node.azure_vm_id = data.get("azure_vm_id")
        node.quota = quota
        instance = AzureInstance(
            endpoint.region,
            data.get("vm"),
            quota,
            data.get("nic"),
            data.get("public_ipv4"),
            data.get("public_ipv6"),
        )
        node.updateFromInstance(instance)
    return {"node": _export_node(node)}


def _ep_os_find_flavor(launcher, data):
    from zuul.driver.openstack.openstackendpoint import quota_from_flavor

    provider, endpoint = _provider_endpoint(launcher, data)
    label = provider.labels[data["label"]]
    flavor = provider.flavors[label.flavor]
    os_flavor = endpoint._findFlavor(flavor_name=flavor.flavor_name)
    quota = quota_from_flavor(os_flavor, label=label)
    return {
        "flavor": _jsonable(os_flavor),
        "quota": _quota_payload(quota),
        "auto_floating_ip": bool(getattr(label, "auto_floating_ip", False)),
    }


def _ep_os_list_instance_id(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    node_uuid = data.get("node_uuid")
    for instance in endpoint.listInstances():
        if instance.metadata.get("zuul_node_uuid") == node_uuid:
            return {"openstack_server_id": instance.openstack_server_id}
    return {"openstack_server_id": None}


def _ep_os_submit_create_server(launcher, data):
    provider, endpoint = _provider_endpoint(launcher, data)
    label = provider.labels[data["label"]]
    flavor = provider.flavors[label.flavor]
    image = provider.images[label.image]
    image_external_id = data.get("image_external_id")
    if not image_external_id and data.get("node_uuid"):
        node = _load_node(launcher, data["node_uuid"])
        image_external_id = call_launcher(
            launcher, "getImageExternalId", node, provider
        )
    if image_external_id:
        image_external = image_external_id
    elif getattr(image, "image_id", None):
        image_external = dict(id=image.image_id)
    else:
        image_external = image.external_name
    os_flavor = endpoint._findFlavor(flavor_name=flavor.flavor_name)
    future = endpoint._submitApi(
        endpoint._createServer,
        data.get("hostname") or "",
        image=image_external,
        flavor=os_flavor,
        key_name=label.key_name,
        az=label.az,
        config_drive=image.config_drive,
        networks=label.networks,
        security_groups=label.security_groups,
        boot_from_volume=label.boot_from_volume,
        volume_size=label.volume_size,
        instance_properties=data.get("tags") or {},
        userdata=label.userdata,
    )
    return {"future_id": _store_future(launcher, future)}


def _ep_os_complete_create_server(launcher, data):
    from zuul.driver.openstack.openstackendpoint import is_message_about_quota
    import openstack.cloud.exc

    _provider, endpoint = _provider_endpoint(launcher, data)
    future = _future_store(launcher).get(data.get("future_id"))
    if future is None:
        return {"server": None}
    try:
        try:
            server = endpoint._completeApi(future)
            if server is not None:
                _future_store(launcher).pop(data.get("future_id"), None)
            return {"server": _jsonable(server)}
        except openstack.cloud.exc.OpenStackCloudCreateException as exc:
            payload = {
                "server": None,
                "resource_id": getattr(exc, "resource_id", None),
                "message": "%s: %s" % (type(exc).__name__, exc),
                "quota_fault": False,
            }
            if payload["resource_id"]:
                payload["quota_fault"] = bool(
                    _os_server_fault_is_quota(
                        endpoint, payload["resource_id"], launcher.log
                    )
                )
            return payload
    except Exception as exc:
        if is_message_about_quota(str(exc)):
            return {
                "error_kind": "quota",
                "message": "%s: %s" % (type(exc).__name__, exc),
            }
        raise


def _os_server_fault_is_quota(endpoint, server_id, log):
    from zuul.driver.openstack.openstackendpoint import is_message_about_quota

    if not server_id:
        return False
    try:
        server = endpoint._getServerByIdNow(server_id)
        if not server:
            return False
        fault = server.get("fault", {}).get("message")
        if fault:
            log.error("Detailed node error: %s", fault)
            if is_message_about_quota(fault):
                return True
    except Exception:
        log.exception("Failed to retrieve node error information:")
    return False


def _ep_os_refresh_server(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    server = endpoint._refreshServer(data.get("server") or {})
    return {"server": _jsonable(server)}


def _ep_os_get_server(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    server = endpoint._getServer(data.get("server_id"))
    return {"server": _jsonable(server)}


def _ep_os_needs_floating_ip(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    return {
        "needs": bool(endpoint._needsFloatingIp(data.get("server") or {}))
    }


def _ep_os_create_floating_ip(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    fip = endpoint._createFloatingIp(data.get("server") or {})
    return {"floating_ip": _jsonable(fip)}


def _ep_os_refresh_floating_ip(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    fip = endpoint._refreshFloatingIp(data.get("floating_ip") or {})
    return {"floating_ip": _jsonable(fip)}


def _ep_os_attach_ip(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    endpoint._attachIpToServer(
        data.get("server") or {}, data.get("floating_ip") or {}
    )
    return {}


def _ep_os_server_floating_ip(launcher, data):
    import openstack.cloud.meta

    server = data.get("server") or {}
    ip = openstack.cloud.meta.get_server_ip(
        server, ext_tag="floating", public=True
    )
    return {"ip": ip}


def _ep_os_get_instance(launcher, data):
    provider, endpoint = _provider_endpoint(launcher, data)
    node = _load_node(launcher, data["node_uuid"])
    label = provider.labels[data["label"]]
    flavor = provider.flavors[label.flavor]
    from zuul.driver.openstack.openstackendpoint import quota_from_flavor

    os_flavor = endpoint._findFlavor(flavor_name=flavor.flavor_name)
    quota = quota_from_flavor(os_flavor, label=label)
    with _mutable(node):
        if data.get("openstack_server_id"):
            node.openstack_server_id = data.get("openstack_server_id")
        if data.get("openstack_floating_ip_id"):
            node.openstack_floating_ip_id = data.get(
                "openstack_floating_ip_id"
            )
        node.quota = quota
        instance = endpoint._getInstance(data.get("server") or {}, quota)
        node.updateFromInstance(instance)
    return {"node": _export_node(node)}


def _ep_os_has_floating_ips(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    return {"has": bool(endpoint._hasFloatingIps())}


def _ep_os_get_floating_ips(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    fips = endpoint._getFloatingIps(data.get("server") or {})
    return {"floating_ips": _jsonable(fips)}


def _ep_os_delete_floating_ip(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    endpoint._deleteFloatingIp(data.get("floating_ip") or {})
    return {}


def _ep_os_refresh_floating_ip_delete(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    fip = endpoint._refreshFloatingIpDelete(data.get("floating_ip"))
    return {"floating_ip": _jsonable(fip)}


def _ep_os_submit_delete_server(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    future = endpoint._submitApi(
        endpoint._deleteServer, data.get("server_id")
    )
    return {"future_id": _store_future(launcher, future)}


def _ep_os_complete_delete_server(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    future = _future_store(launcher).get(data.get("future_id"))
    if future is None:
        return {"done": False}
    done = endpoint._completeApi(future)
    if done:
        _future_store(launcher).pop(data.get("future_id"), None)
    return {"done": bool(done)}


def _ep_os_refresh_server_delete(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    server = endpoint._refreshServerDelete(data.get("server"))
    return {"server": _jsonable(server)}


def _ep_os_handle_server_fault(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    return {
        "quota": _os_server_fault_is_quota(
            endpoint, data.get("server_id"), launcher.log
        )
    }


def _ep_k8s_setup(launcher, data):
    provider, endpoint = _provider_endpoint(launcher, data)
    label = provider.labels[data["label"]]
    if label.kind == "pod":
        containers = [c["name"] for c in label.spec["containers"]]
    else:
        containers = None
    cfg = endpoint.core_client.api_client.configuration
    return {
        "use_openshift_projects": bool(
            endpoint.connection.use_openshift_projects
        ),
        "kind": label.kind,
        "host": cfg.host,
        "skiptls": not cfg.verify_ssl,
        "containers": containers,
        "quota": _quota_payload(endpoint.getQuotaForLabel(label)),
    }


def _ep_k8s_use_openshift(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    return {
        "use_openshift_projects": bool(
            endpoint.connection.use_openshift_projects
        )
    }


def _ep_k8s_create_namespace(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    endpoint._createNamespace(data.get("hostname"), data.get("tags") or {})
    return {}


def _ep_k8s_create_project(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    endpoint._createProject(data.get("hostname"), data.get("tags") or {})
    return {}


def _ep_k8s_get_subject_access_review(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    review = endpoint._getSubjectAccessReview(data.get("namespace"))
    if review is None:
        return {"allowed": None}
    return {"allowed": bool(review.status.allowed)}


def _ep_k8s_create_image_pull_secrets(launcher, data):
    provider, endpoint = _provider_endpoint(launcher, data)
    label = provider.labels[data["label"]]
    endpoint._createImagePullSecrets(data.get("namespace"), label)
    return {}


def _ep_k8s_create_service_account(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    endpoint._createServiceAccount(data.get("namespace"), data.get("user"))
    return {}


def _ep_k8s_get_token(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    token = endpoint._getToken(data.get("namespace"), data.get("user"))
    if token is None:
        return {"token": None, "ca_crt": None}
    token_value, ca_crt = token
    return {"token": token_value, "ca_crt": ca_crt}


def _ep_k8s_create_role(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    endpoint._createRole(
        data.get("namespace"),
        data.get("user"),
        bool(data.get("restricted_access")),
    )
    return {}


def _ep_k8s_create_pod(launcher, data):
    provider, endpoint = _provider_endpoint(launcher, data)
    label = provider.labels[data["label"]]
    endpoint._createPod(
        data.get("namespace"),
        data.get("hostname"),
        label,
        data.get("tags") or {},
    )
    return {}


def _ep_k8s_get_pod(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    ready = endpoint._getPod(data.get("namespace"), data.get("hostname"))
    return {"ready": bool(ready)}


def _ep_k8s_delete_namespace(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    endpoint._deleteNamespace(data.get("namespace_id"))
    return {}


def _ep_k8s_delete_project(launcher, data):
    _provider, endpoint = _provider_endpoint(launcher, data)
    endpoint._deleteProject(data.get("namespace_id"))
    return {}


def _ep_k8s_update_node(launcher, data):
    from zuul.driver.kubernetes.kubernetesmodel import KubernetesInstance

    provider, endpoint = _provider_endpoint(launcher, data)
    node = _load_node(launcher, data["node_uuid"])
    label = provider.labels[data["label"]]
    quota = endpoint.getQuotaForLabel(label)
    with _mutable(node):
        if data.get("kubernetes_namespace_id"):
            node.kubernetes_namespace_id = data.get(
                "kubernetes_namespace_id"
            )
        if data.get("kubernetes_connection"):
            node.kubernetes_connection = data.get("kubernetes_connection")
        node.quota = quota
        instance = KubernetesInstance(
            data.get("kind") or label.kind,
            data.get("hostname"),
            data.get("tags") or {},
            quota,
        )
        node.updateFromInstance(instance)
    return {"node": _export_node(node)}


def _callable_is_patched(current, original):
    import unittest.mock as mock

    if isinstance(current, mock.Mock):
        return True
    cur = getattr(current, "__func__", current)
    orig = getattr(original, "__func__", original)
    return cur is not orig


def _sm_patched_advance(launcher, data):
    """Run a Mock/replaced StateMachine.advance (or _updateNode).

    Returns {"patched": false} when the class still has the original
    callable so Rust continues its own state machine.
    """
    from types import SimpleNamespace

    _record_original_sms()
    driver = data.get("driver") or "aws"
    kind = data.get("kind") or "create"
    key = (driver, kind)
    cls = _sm_class(driver, kind if kind != "update_node" else "create")
    if cls is None:
        return {"patched": False}
    if kind == "update_node":
        # Always run _updateNode (original or unittest replacement) so
        # updateFromInstance and test patches of _updateNode apply.
        current = getattr(cls, "_updateNode")
        provider, endpoint = _provider_endpoint(launcher, data)
        node = _load_node(launcher, data["node_uuid"])
        label = provider.labels[data["label"]]
        flavor = provider.flavors[label.flavor]
        instance = data.get("instance") or {}
        host = data.get("host")
        with _mutable(node):
            if data.get("aws_instance_id"):
                node.aws_instance_id = data.get("aws_instance_id")
                # Seed instance_id before _updateNode so the exported
                # node_properties keep it when ebs_volume_id is added.
                if not getattr(node, "node_properties", None):
                    node.node_properties = {}
                node.node_properties.setdefault(
                    "instance_id", data["aws_instance_id"]
                )
            if data.get("aws_dedicated_host_id"):
                node.aws_dedicated_host_id = data.get("aws_dedicated_host_id")
            sm = SimpleNamespace(
                endpoint=endpoint,
                node=node,
                label=label,
                flavor=flavor,
                instance=instance,
                host=host,
                provider=provider,
                log=launcher.log,
            )
            current(sm)
        machines = launcher.__dict__.setdefault("_cloud_sms", {})
        machines[("create", data["node_uuid"])] = _StateMachineRef(
            cls, {"state": "complete"}
        )
        cached = launcher.api.getProviderNode(data["node_uuid"])
        _attach_state_machines(launcher, cached)
        return {"patched": True, "node": _export_node(node)}

    current = getattr(cls, "advance")
    original = _ORIG_SM.get(key)
    if original is None or not _callable_is_patched(current, original):
        return {"patched": False}

    ns = SimpleNamespace(
        state=data.get("state") or cls.START,
        complete=bool(data.get("complete")),
        start_time=data.get("start_time") or 0,
        snapshot_id=data.get("snapshot_id"),
        log=launcher.log,
    )
    # Copy class constants tests may read.
    for name in dir(cls):
        if name.isupper():
            setattr(ns, name, getattr(cls, name))
    external = current(ns)
    payload = {
        "patched": True,
        "state": getattr(ns, "state", None),
        "complete": bool(getattr(ns, "complete", False)),
    }
    if external is not None:
        payload["external_id"] = _jsonable(external)
    if data.get("node_uuid"):
        # Persist create/delete_state for failover-style assertions.
        exported = {
            "create_state"
            if kind == "create"
            else "delete_state"
            if kind == "delete"
            else "snapshot_state": {
                "state": payload["state"],
                "start_time": ns.start_time,
            }
        }
        payload["node"] = exported
        machines = launcher.__dict__.setdefault("_cloud_sms", {})
        machines[(kind, data["node_uuid"])] = _StateMachineRef(
            cls, exported.get("create_state") or exported.get("delete_state") or {}
        )
    return payload


def _cleanup_tick(launcher, data):
    """One CleanupWorker._run pass (list/delete leaked cloud resources)."""
    _ = data
    _bind_missing_methods(launcher)
    worker = getattr(launcher, "cleanup_worker", None)
    if worker is None:
        return {"ran": False}
    worker._run()
    return {"ran": True}


def _emit_upload_workers_gauge(launcher, value):
    """Match MonitoringThreadPoolExecutor gauge for the 1-worker upload pool."""
    if not getattr(launcher, "statsd", None):
        return
    try:
        launcher.statsd.gauge(
            "zuul.launcher.server.{hostname}.upload_workers",
            int(value),
        )
    except Exception:
        launcher.log.exception("Error emitting upload_workers gauge")


def _upload_artifact_job(launcher, data):
    """One UploadJob per artifact on the 1-worker upload pool.

    Honors MonkeyPatch of zuul.launcher.server.UploadJob.run. Planning is
    Rust-supplied (`planned`); lock/UPLOADING/finalize/release match
    server.py UploadJob._run so ZK versions stay coherent. Only
    getImage*Job.run crosses the SDK/patch boundary.
    """
    import types

    from zuul.lib.logutil import get_annotated_logger
    from zuul.launcher.server import UploadJob

    _bind_missing_methods(launcher)
    artifact_uuid = data.get("artifact_uuid")
    upload_uuids = list(data.get("upload_uuids") or [])
    planned = list(data.get("planned") or [])
    iba = launcher.image_build_registry.getItem(artifact_uuid)
    if iba is None:
        return {"ok": False, "error": "missing artifact"}
    uploads = []
    for uuid in upload_uuids:
        upload = launcher.image_upload_registry.getItem(uuid)
        if upload is not None:
            uploads.append(upload)
    # Skip if Python queue already has these (duplicate enqueue guard).
    if uploads and all(
        u.uuid in launcher.upload_uuids_in_queue for u in uploads
    ):
        return {"ok": True, "skipped": True, "planned": 0}
    all_uploads = [
        u
        for u in launcher.image_upload_registry.getItems()
        if u.artifact_uuid == artifact_uuid
    ]
    job = UploadJob(launcher, iba, uploads, all_uploads)

    def _rust_run(self):
        path = None
        locked = []
        with launcher.createZKContext(None, self.log) as ctx:
            try:
                self.log.debug(
                    "Starting upload job for %s", self.image_build_artifact
                )
                # Acquire all locks + set UPLOADING once (server.py).
                self._acquireUploadLocks(ctx, locked)
                if not locked:
                    return
                locked_by_uuid = {u.uuid: u for u in locked}
                # Top-level planned jobs (imports, ready copies, uploads).
                # Dependents run after a successful source, still under
                # the locks acquired above.
                for spec in planned:
                    upload_uuid = spec.get("upload_uuid")
                    upload = locked_by_uuid.get(upload_uuid)
                    if upload is None:
                        continue
                    kind = spec.get("kind") or "upload"
                    if kind == "copy-dependent":
                        # Only run when a source schedules us.
                        continue
                    log = get_annotated_logger(self.log, upload=upload.uuid)
                    path_ref = {"path": path}
                    external_id = _endpoint_upload_execute(
                        launcher,
                        upload,
                        iba,
                        kind,
                        spec,
                        path_ref,
                        log,
                    )
                    if path_ref.get("path"):
                        path = path_ref["path"]
                    try:
                        self.finalizeUpload(upload, ctx, log)
                        if (
                            not upload.validated
                            and upload.state == upload.State.READY
                        ):
                            launcher.addImageValidateEvent(upload)
                    except Exception:
                        log.exception(
                            "Unable to update state for %s", upload
                        )
                    try:
                        upload.releaseLock(ctx)
                        log.debug("Released upload lock for %s", upload)
                        locked.remove(upload)
                        locked_by_uuid.pop(upload.uuid, None)
                    except Exception:
                        log.exception(
                            "Unable to release lock for %s", upload
                        )
                    # Dependent copies after a successful source.
                    if external_id:
                        for dep_uuid in spec.get("dependents") or []:
                            dep = locked_by_uuid.get(dep_uuid)
                            if dep is None:
                                continue
                            dep_log = get_annotated_logger(
                                self.log, upload=dep.uuid
                            )
                            dep_log.debug("Scheduling copy for %s", dep)
                            # Source is the upload that just finished;
                            # its providers[0] is the copy source provider
                            # (matches UploadPlanner + getImageCopyJob).
                            src_provider = None
                            if upload.providers:
                                src_provider = upload.providers[0]
                            elif spec.get("source_provider"):
                                src_provider = spec.get("source_provider")
                            _endpoint_upload_execute(
                                launcher,
                                dep,
                                iba,
                                "copy",
                                {
                                    "source_external_id": external_id,
                                    "source_provider": src_provider,
                                },
                                {"path": None},
                                dep_log,
                            )
                            try:
                                self.finalizeUpload(dep, ctx, dep_log)
                                if (
                                    not dep.validated
                                    and dep.state == dep.State.READY
                                ):
                                    launcher.addImageValidateEvent(dep)
                            except Exception:
                                dep_log.exception(
                                    "Unable to update state for %s", dep
                                )
                            try:
                                dep.releaseLock(ctx)
                                dep_log.debug(
                                    "Released upload lock for %s", dep
                                )
                                locked.remove(dep)
                                locked_by_uuid.pop(dep.uuid, None)
                            except Exception:
                                dep_log.exception(
                                    "Unable to release lock for %s", dep
                                )
                # Leftover locks (skipped dependents / unplanned): finalize
                # like UploadJob's unused-lock loop.
                for upload in locked[:]:
                    log = get_annotated_logger(self.log, upload=upload.uuid)
                    try:
                        self.finalizeUpload(upload, ctx, log)
                    except Exception:
                        log.exception(
                            "Unable to update state for %s", upload
                        )
                    try:
                        upload.releaseLock(ctx)
                        log.debug(
                            "Released unused upload lock for %s", upload
                        )
                        locked.remove(upload)
                    except Exception:
                        log.exception(
                            "Unable to release unused lock for %s", upload
                        )
            finally:
                if path:
                    try:
                        os.unlink(path)
                        self.log.info("UploadJob deleted %s", path)
                    except Exception:
                        self.log.exception("Unable to delete %s", path)
                self.log.debug(
                    "Finished upload job for %s with %s uploads",
                    self.image_build_artifact,
                    len(locked),
                )
        launcher.wake_event.set()

    job._run = types.MethodType(_rust_run, job)
    for upload in uploads:
        launcher.upload_uuids_in_queue.add(upload.uuid)
    _emit_upload_workers_gauge(launcher, 1)
    try:
        job.run()
    finally:
        _emit_upload_workers_gauge(launcher, 0)
    return {"ok": True, "planned": len(planned)}


def _endpoint_upload_execute(
    launcher, upload, iba, kind, spec, path_ref, log
):
    """SDK-boundary EndpointUploadJob._run: getImage*Job.run + external_id.

    Caller holds the upload lock and has already set UPLOADING. Matches
    server.py EndpointUploadJob (createZKContext(upload._lock) for the
    attribute write; exceptions logged as 'Error in endpoint upload job').
    """
    external_id = None
    timer = nullcontext()
    if hasattr(launcher, "statsd_timer"):
        timer = launcher.statsd_timer(
            "zuul.image.%s.upload.%s.duration"
            % (
                _statsd_name(upload.canonical_name),
                _statsd_name(upload.endpoint_name),
            )
        )
    try:
        with timer:
            log.info("Starting upload %s", upload)
            provider_cname = upload.providers[0]
            provider = launcher._getProviderByCanonicalName(provider_cname)
            provider_image = None
            for image in provider.images.values():
                if image.canonical_name == upload.canonical_name:
                    provider_image = image
                    break
            if provider_image is None:
                raise Exception(
                    "Unable to find image %s" % upload.canonical_name
                )
            metadata = provider.getUploadTags(
                launcher.system.system_id,
                provider_image,
                upload.uuid,
            )
            image_name = "%s-%s" % (provider_image.name, upload.uuid)
            md5 = iba.md5sum
            sha256 = iba.sha256
            image_format = iba.format
            if kind == "import":
                job = provider.getImageImportJob(
                    iba.url,
                    provider_image,
                    image_name,
                    image_format,
                    metadata,
                    md5,
                    sha256,
                )
                if job is None:
                    raise Exception("no import job for %s" % upload.uuid)
                external_id = job.run()
            elif kind == "copy":
                source_external_id = spec.get("source_external_id")
                if not source_external_id:
                    raise Exception("copy requires source_external_id")
                source_provider = provider
                if spec.get("source_provider"):
                    source_provider = launcher._getProviderByCanonicalName(
                        spec["source_provider"]
                    )
                job = provider.getImageCopyJob(
                    source_provider,
                    provider_image,
                    image_name,
                    image_format,
                    metadata,
                    md5,
                    sha256,
                )
                if job is None:
                    raise Exception("no copy job for %s" % upload.uuid)
                external_id = job.run(source_external_id)
            else:
                path = path_ref.get("path")
                if not path:
                    path = _upload_download(launcher, iba)
                    if not path:
                        raise Exception(
                            "Unable to download artifact %s" % iba
                        )
                    # Match UploadJob: decompress + checksum before upload.
                    path = _upload_decompress(launcher, path, log)
                    _upload_validate_checksum(iba, path, log)
                    path_ref["path"] = path
                job = provider.getImageUploadJob(
                    provider_image,
                    image_name,
                    image_format,
                    metadata,
                    md5,
                    sha256,
                )
                if job is None:
                    raise Exception("no upload job for %s" % upload.uuid)
                external_id = job.run(path)
            log.info("Finished upload %s", upload)
            if external_id:
                with launcher.createZKContext(upload._lock, log) as ctx:
                    upload.updateAttributes(ctx, external_id=external_id)
    except Exception:
        log.exception("Error in endpoint upload job")
        return None
    return external_id


def _statsd_name(name):
    from zuul.launcher.server import _normalize_statsd_name

    return _normalize_statsd_name(name)


def _upload_decompress(launcher, path, log):
    """Match UploadJob._handleCompression for .zst artifacts."""
    import subprocess

    inflated_path = path
    if path.endswith(".zst"):
        orig_path = path
        inflated_path = path[: -len(".zst")]
        try:
            subprocess.run(
                ["zstd", "-dq", orig_path],
                cwd=launcher.temp_dir,
                check=True,
                capture_output=True,
            )
            log.debug("Decompressed image to %s", inflated_path)
        except Exception:
            if os.path.exists(inflated_path):
                os.unlink(inflated_path)
                log.info("Deleted decompressed image %s", inflated_path)
            raise
        finally:
            if os.path.exists(orig_path):
                os.unlink(orig_path)
                log.info("Deleted zst source image %s", orig_path)
    return inflated_path


def _upload_validate_checksum(iba, path, log):
    """Match UploadJob._validateChecksum."""
    import hashlib

    if not iba.sha256:
        log.debug(
            "Image build artifact %s has no sha256 checksum, "
            "skipping validation",
            iba,
        )
        return
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while data := f.read(4096):
            h.update(data)
    digest = h.hexdigest()
    if digest != iba.sha256:
        raise Exception(
            "Downloaded file %s sha256 digest %s does not match "
            "artifact %s digest %s" % (path, digest, iba, iba.sha256)
        )


def _upload_job_run(launcher, data):
    """Legacy single-upload entry (upload.job_run) for isolated SDK runs.

    Prefer upload.artifact_job which owns locking. This path still locks
    one upload for callers that invoke job_run directly.
    """
    from zuul.lib.logutil import get_annotated_logger
    from zuul.zk.exceptions import LockException

    _bind_missing_methods(launcher)
    kind = data.get("kind") or "upload"
    upload_uuid = data["upload_uuid"]
    upload = launcher.image_upload_registry.getItem(upload_uuid)
    if upload is None:
        raise Exception("upload %s not found" % upload_uuid)
    iba = launcher.image_build_registry.getItem(upload.artifact_uuid)
    if iba is None:
        raise Exception("artifact %s not found" % upload.artifact_uuid)
    log = get_annotated_logger(launcher.log, upload=upload.uuid)
    path_ref = {"path": None}
    try:
        with launcher.createZKContext(None, launcher.log) as ctx:
            try:
                with upload.locked(ctx, blocking=False):
                    upload.refresh(ctx)
                    if upload.external_id:
                        return {
                            "external_id": upload.external_id,
                            "state": upload.state,
                            "skipped": True,
                        }
                    if upload.state not in (
                        upload.State.PENDING,
                        upload.State.UPLOADING,
                    ):
                        return {
                            "external_id": upload.external_id,
                            "state": upload.state,
                            "skipped": True,
                        }
                    with upload.activeContext(ctx):
                        upload.state = upload.State.UPLOADING
                    external_id = _endpoint_upload_execute(
                        launcher, upload, iba, kind, data, path_ref, log
                    )
                    try:
                        with upload.activeContext(ctx):
                            if upload.external_id:
                                upload.state = upload.State.READY
                            elif upload.attempt < 2:
                                upload.copy(ctx)
                                upload.state = upload.State.DELETING
                                launcher.upload_deleted_event.set()
                            else:
                                upload.state = upload.State.FAILED
                        if (
                            not upload.validated
                            and upload.state == upload.State.READY
                        ):
                            launcher.addImageValidateEvent(upload)
                    except Exception:
                        log.exception(
                            "Unable to update state for %s", upload
                        )
            except LockException:
                launcher.image_updated_event.set()
                return {"locked": False, "external_id": None}
    finally:
        launcher.upload_uuids_in_queue.discard(upload_uuid)
        path = path_ref.get("path")
        if path and os.path.exists(path):
            try:
                os.unlink(path)
            except Exception:
                launcher.log.exception("Unable to delete %s", path)
    launcher.wake_event.set()
    return {
        "external_id": getattr(upload, "external_id", None),
        "state": getattr(upload, "state", None),
        "locked": True,
    }


def _upload_download(launcher, iba):
    from urllib.parse import urlparse

    _bind_missing_methods(launcher)
    url = iba.url
    ext = urlparse(url).path.split(".")[-1]
    path = os.path.join(launcher.temp_dir, "%s.%s" % (iba.uuid, ext))
    launcher.log.info(
        "Downloading artifact %s from %s into %s", iba, url, path
    )
    downloaded = None
    try:
        ret = launcher.downloadArtifact(url, path)
        if ret:
            downloaded = ret
    except Exception as e:
        launcher.log.info("Unable to download %s: %s", url, e)
    if not downloaded:
        providers = launcher.tenant_providers.get(iba.build_tenant_name, [])
        for provider in providers:
            try:
                ret = provider.downloadUrl(url, path)
                if ret:
                    downloaded = ret
                    break
            except Exception as e2:
                launcher.log.info(
                    "Unable to download %s via %s: %s", url, provider, e2
                )
    if not downloaded:
        raise Exception("Unable to download artifact %s" % iba)
    path = downloaded
    # Decompress .zst like UploadJob._handleCompression.
    if path.endswith(".zst"):
        import subprocess

        orig = path
        inflated = path[: -len(".zst")]
        try:
            subprocess.run(
                ["zstd", "-dq", orig],
                cwd=launcher.temp_dir,
                check=True,
                capture_output=True,
            )
        except Exception:
            if os.path.exists(inflated):
                os.unlink(inflated)
            raise
        finally:
            if os.path.exists(orig):
                os.unlink(orig)
        path = inflated
    return path


def _upload_reset_crashed(launcher, data):
    """Reset unlocked UPLOADING uploads to PENDING (checkMissingUploads).

    Acquires the ImageUpload lock, refreshes, and double-checks state so we
    never race an in-flight UploadJob the way a raw ZK set would.
    """
    from zuul.zk.exceptions import LockException

    _bind_missing_methods(launcher)
    upload_uuid = data["upload_uuid"]
    upload = launcher.image_upload_registry.getItem(upload_uuid)
    if upload is None:
        return {"ok": False, "missing": True}
    if upload.is_locked:
        return {"ok": True, "skipped": True, "reason": "locked"}
    if upload.state != upload.State.UPLOADING:
        return {"ok": True, "skipped": True, "reason": "not_uploading"}
    with launcher.createZKContext(None, launcher.log) as ctx:
        try:
            with upload.locked(ctx, blocking=False):
                upload.refresh(ctx)
                if upload.state == upload.State.UPLOADING:
                    with upload.activeContext(ctx):
                        upload.state = upload.State.PENDING
                    return {"ok": True, "reset": True}
        except LockException:
            # Locked again (lock / cache update race)
            return {"ok": True, "skipped": True, "reason": "lock_race"}
    return {"ok": True, "skipped": True}


def _upload_delete_artifact(launcher, data):
    """Delete an ImageBuildArtifact with no uploads (checkOldImages)."""
    from zuul.zk.exceptions import LockException

    _bind_missing_methods(launcher)
    artifact_uuid = data["artifact_uuid"]
    iba = launcher.image_build_registry.getItem(artifact_uuid)
    if iba is None:
        return {"deleted": False, "missing": True}
    with launcher.createZKContext(None, launcher.log) as ctx:
        try:
            with iba.locked(ctx, blocking=False):
                launcher.log.info(
                    "Deleting image build artifact with no uploads: %s", iba
                )
                iba.delete(ctx)
                return {"deleted": True}
        except LockException:
            return {"deleted": False, "locked": True}


def _upload_delete(launcher, data):
    """Delete cloud image and ImageUpload ZK node (DeleteJob._run)."""
    from zuul.zk.exceptions import LockException

    _bind_missing_methods(launcher)
    upload_uuid = data["upload_uuid"]
    upload = launcher.image_upload_registry.getItem(upload_uuid)
    if upload is None:
        launcher.upload_uuids_in_delete_queue.discard(upload_uuid)
        return {"deleted": False, "missing": True}
    iba = launcher.image_build_registry.getItem(upload.artifact_uuid)
    try:
        with launcher.createZKContext(None, launcher.log) as ctx:
            try:
                with upload.locked(ctx, blocking=False):
                    upload.refresh(ctx)
                    launcher.log.info("Deleting image upload %s", upload)
                    with upload.activeContext(ctx):
                        upload.state = upload.State.DELETING
                    provider_cname = upload.providers[0]
                    provider = launcher._getProviderByCanonicalName(
                        provider_cname
                    )
                    if upload.external_id:
                        provider.deleteImage(upload.external_id)
                    upload.delete(ctx)
                    launcher.upload_deleted_event.set()
                    launcher.wake_event.set()
            except LockException:
                return {"deleted": False, "locked": False}
    finally:
        launcher.upload_uuids_in_delete_queue.discard(upload_uuid)
    return {"deleted": True}


def _snapshot_sm_advance(launcher, data):
    """Run server.py _processNodeSnapshot (lock + _checkNodeSnapshot).

    Uses the bound 14.3.0 method so MonkeyPatch of
    AwsSnapshotStateMachine.advance applies and okay_tracebacks of
    `_checkNodeSnapshot` still match timeout/error stacks.
    """
    from zuul.lib.logutil import get_annotated_logger

    _bind_missing_methods(launcher)
    node_uuid = data["node_uuid"]
    node = launcher.api.getProviderNode(node_uuid)
    if node is None:
        return {"skip": True}
    log = get_annotated_logger(
        launcher.log, node, request=getattr(node, "request_id", None)
    )
    launcher._processNodeSnapshot(node, log)
    snapshot = node.snapshot
    return {
        "skip": False,
        "complete": bool(snapshot.complete),
        "external_id": _jsonable(getattr(snapshot, "external_id", None)),
    }


def _snapshot_advance(launcher, data):
    """ZK lock/prepare/commit for Rust AwsSnapshot (ep.create_snapshot path).

    Phases:
      begin  — acquire snapshot lock, set tags, return volume/provider context
      commit — write snapshot_state / complete / external_id; unlock if done
      fail   — mark complete and unlock
    """
    import copy
    import time as _time

    _bind_missing_methods(launcher)
    phase = data.get("phase") or "begin"
    node_uuid = data["node_uuid"]
    node = launcher.api.getProviderNode(node_uuid)
    if node is None or node.state != node.State.SNAPSHOT:
        return {"skip": True}
    snapshot = node.snapshot
    if snapshot.complete and phase == "begin":
        return {"skip": True, "complete": True}

    if phase == "begin":
        if not snapshot.hasLock():
            with launcher.createZKContext(None, launcher.log) as ctx:
                if not snapshot.acquireLock(ctx, blocking=False):
                    return {"skip": True, "locked": False}
                snapshot.refresh(ctx)
        provider = launcher._getProviderForNode(node)
        with launcher.createZKContext(snapshot._lock, launcher.log) as ctx:
            with snapshot.activeContext(ctx):
                if not snapshot.tags:
                    tags = copy.deepcopy(node.tags)
                    tags.pop("zuul_node_uuid", None)
                    exp = int(_time.time() + (node.snapshot_expiration or 0))
                    tags["zuul_expiration"] = str(exp)
                    snapshot.tags = tags
        volume_id = None
        region = None
        account_id = None
        endpoint = provider.getEndpoint()
        if hasattr(node, "aws_instance_id") and node.aws_instance_id:
            instance = endpoint._refresh(dict(InstanceId=node.aws_instance_id))
            for attachment in instance.get("BlockDeviceMappings") or []:
                if attachment.get("DeviceName") == instance.get("RootDeviceName"):
                    volume_id = attachment.get("Ebs", {}).get("VolumeId")
                    break
            region = getattr(endpoint, "region", None)
            if hasattr(endpoint, "_getAccountId"):
                account_id = endpoint._getAccountId()
        state = dict(snapshot.snapshot_state or {})
        return {
            "skip": False,
            "locked": True,
            "provider": provider.canonical_name,
            "volume_id": volume_id,
            "tags": snapshot.tags or {},
            "snapshot_state": state,
            "region": region,
            "account_id": account_id,
            "snapshot_timeout": node.snapshot_timeout,
            "start_time": state.get("start_time") or 0,
        }

    if phase == "fail":
        with launcher.createZKContext(snapshot._lock, launcher.log) as ctx:
            with snapshot.activeContext(ctx):
                snapshot.complete = True
            snapshot.releaseLock(ctx)
        launcher.wake_event.set()
        return {"complete": True}

    # commit
    state = data.get("state") or {}
    complete = bool(data.get("complete"))
    external_id = data.get("external_id")
    with launcher.createZKContext(snapshot._lock, launcher.log) as ctx:
        with snapshot.activeContext(ctx):
            snapshot.snapshot_state = state
            if complete:
                snapshot.complete = True
                snapshot.external_id = external_id
        if complete:
            snapshot.releaseLock(ctx)
    if not complete:
        launcher.wake_event.set()
    return {"complete": complete, "external_id": external_id}


def _nodescan_step(launcher, data):
    from zuul.launcher.server import NodescanRequest

    node_uuid = data["uuid"]
    scans = launcher.__dict__.setdefault("_nodescan_requests", {})
    if node_uuid not in scans:
        node = _ScanNode(data)
        request = NodescanRequest(node, launcher.log)
        launcher.nodescan_worker.addRequest(request)
        scans[node_uuid] = request
    request = scans[node_uuid]
    if not request.complete:
        return {"complete": False, "keys": [], "error": ""}
    error = ""
    keys = []
    if request.exception is not None:
        error = str(request.exception)
    else:
        keys = list(request.keys)
    scans.pop(node_uuid, None)
    launcher.nodescan_worker.removeRequest(request)
    return {"complete": True, "keys": keys, "error": error}


class _ScanNode:
    def __init__(self, data):
        self.uuid = data.get("uuid")
        self.interface_ip = data.get("interface_ip")
        self.connection_port = data.get("connection_port") or 22
        self.host_key_checking = bool(data.get("host_key_checking"))
        self.connection_type = data.get("connection_type") or "ssh"
        self.boot_timeout = data.get("boot_timeout") or 120


def _image_attrs(launcher, data):
    provider = _find_provider(launcher, data["provider"])
    label = provider.labels[data["label"]]
    image = provider.images[label.image]
    return {
        "connection_port": image.connection_port,
        "connection_type": image.connection_type,
        "username": image.username,
        "python_path": image.python_path,
        "shell_type": image.shell_type,
    }


def _provider_quota(launcher, data):
    provider = _find_provider(launcher, data["provider"])
    return _quota_payload(call_launcher(launcher, "getProviderQuota", provider))


def _quota_for_node(launcher, data):
    # doesProviderHaveQuotaForNode subtracts unmanaged usage recorded
    # by the cleanup worker. The node is loaded from ZooKeeper so the
    # quota is the one just written, not a stale cache copy.
    from zuul.model import ProviderNode
    from zuul.zk.zkobject import ZKContext

    provider = _find_provider(launcher, data["provider"])
    ctx = ZKContext(launcher.zk_client, None, launcher.join_event, launcher.log)
    node = ProviderNode.fromZK(ctx, path=ProviderNode._getPath(data["uuid"]))
    messages = []
    ok = call_launcher(
        launcher, "doesProviderHaveQuotaForNode", provider, node, messages
    )
    return {"ok": bool(ok)}


def _quota_for_label(launcher, data):
    provider = _find_provider(launcher, data["provider"])
    label = provider.labels[data["label"]]
    messages = []
    ok = call_launcher(
        launcher,
        "doesProviderHaveQuotaForLabel",
        data.get("tenant") or provider.tenant_name,
        provider,
        label,
        messages,
        bool(data.get("include_usage", True)),
    )
    return {"ok": bool(ok)}


def _quota_percentage(launcher, data):
    provider = _find_provider(launcher, data["provider"])
    messages = []
    pct = call_launcher(launcher, "getQuotaPercentage", provider, messages)
    return {"pct": float(pct)}


def _node_visible(launcher, data):
    """Whether LauncherApi.getProviderNode can see this uuid.

    test_subnodes_crash_during_create patches that method to return
    None. The Rust node map still has the record, so the presence
    check has to go through the patched Python method.
    """
    node = launcher.api.getProviderNode(data["uuid"])
    return {"present": node is not None}


def _node_cache_insert(launcher, data):
    """Insert a Rust-written node into the Python nodes_cache.

    Python _requestNode/_createSubNode call nodes_cache.insert so the
    object appears before the ZK watch fires. Rust writes ZK directly;
    this adapter mirrors that insert so tests that pause the cache
    event queue (test_subnodes_cache_insert) still see the nodes.
    """
    from zuul import model
    from zuul.launcher.subnodes import SubnodeProviderNode

    uuid = data["uuid"]
    ctx = launcher.createZKContext(None, launcher.log)
    path = model.ProviderNode._getPath(uuid)
    try:
        if data.get("subnode"):
            node = SubnodeProviderNode.fromZK(ctx, path=path)
        else:
            node = model.ProviderNode.fromZK(ctx, path=path)
    except Exception:
        launcher.log.exception("cache_insert: unable to load node %s", uuid)
        return {"ok": False}
    launcher.api.nodes_cache.insert(node)
    return {"ok": True}


def _node_cache_assign(launcher, data):
    """Apply a Rust write_assignment onto the live Python cache object.

    insert() uses setdefault and will not replace an existing object, so
    a later assignment must refresh or assign on the cached instance.
    """
    from kazoo.exceptions import NodeExistsError

    uuid = data["uuid"]
    node = launcher.api.nodes_cache.getItem(uuid)
    if node is None:
        return _node_cache_insert(launcher, {**data, "subnode": True})
    ctx = launcher.createZKContext(None, launcher.log)
    try:
        # Prefer refreshing the assignment child if it already exists.
        node.assignment.refresh(ctx)
        if node.request_id == data.get("request_id"):
            return {"ok": True}
    except Exception:
        pass
    try:
        node.assign(
            ctx,
            request_id=data["request_id"],
            tenant_name=data.get("tenant_name"),
            min_request_version=data.get("min_request_version"),
        )
    except NodeExistsError:
        try:
            node.assignment.refresh(ctx)
        except Exception:
            launcher.log.exception(
                "cache_assign: unable to refresh assignment for %s", uuid
            )
            return {"ok": False}
    except Exception:
        launcher.log.exception("cache_assign: unable to assign %s", uuid)
        return {"ok": False}
    return {"ok": True}


def _class_attr(data):
    """A server.Launcher class constant, including test monkeypatches."""
    from zuul.launcher.server import Launcher

    return {"value": float(getattr(Launcher, data["name"]))}


def _matching_requests(launcher):
    # getMatchingRequests applies launcher_score, which unit tests patch.
    # The Rust scorer cannot see that patch.
    return {"ids": [request.uuid for request in launcher.api.getMatchingRequests()]}


def _matching_nodes(launcher):
    return {
        "ids": [node.uuid for node in launcher.api.getMatchingProviderNodes()]
    }


def _request_cache_ready(launcher, data):
    """The max_zxid / waitForSync gate in _processProviderNode.

    test_multi_launcher_assignment patches RequestCache.max_zxid and
    waits until that property is read. The Rust cache does not go
    through that object, so the check has to happen here.
    """
    node = launcher.api.getProviderNode(data["uuid"])
    if node is None or not node.request_id:
        return {"wait": False, "request_visible": False}
    request = launcher.api.getNodesetRequest(node.request_id)
    if request is not None or not node.min_request_zxid:
        return {"wait": False, "request_visible": request is not None}
    if launcher.zk_client.cap_watched_event_zxid:
        if launcher.api.requests_cache.max_zxid < node.min_request_zxid:
            return {"wait": True, "request_visible": False}
        return {"wait": False, "request_visible": False}
    if node.state in node.ASSIGNABLE_STATES:
        launcher.log.debug(
            "Waiting for request cache to sync for request %s and node %s",
            request,
            node,
        )
        launcher.api.requests_cache.waitForSync()
        request = launcher.api.getNodesetRequest(node.request_id)
    return {"wait": False, "request_visible": request is not None}


def _accept_one(launcher, data):
    """Acquire the request lock and call launcher._acceptRequest.

    Rust process_requests drives acceptance. This adapter only covers
    ZooKeeper lock acquisition on the Python request object (so the
    Python node/request caches agree with the holder) and routes through
    `_acceptRequest` so unittest patches of that method still run. The
    PyO3 `_acceptRequest` implementation calls Rust accept_request_by_uuid;
    it does not run server.Launcher._acceptRequest.
    """
    from kazoo.exceptions import NoNodeError
    from opentelemetry import trace
    from zuul.lib import tracing as zuul_tracing
    from zuul.zk.zkobject import ZKContext

    request = launcher.api.getNodesetRequest(data["uuid"])
    if request is None:
        return {"ok": False, "missing": True}
    log = launcher.log
    try:
        if not request.hasLock():
            if (
                request.state in request.FINAL_STATES
                and request.event_state == request.EventState.COMPLETE
            ):
                return {"ok": True, "skipped": True}
            ctx = ZKContext(launcher.zk_client, None, launcher.stop_event, log)
            if not request.acquireLock(ctx, blocking=False):
                return {"ok": False, "locked": False}
            try:
                request.refresh(ctx)
            except NoNodeError:
                return {"ok": False, "missing": True}
            request_span = zuul_tracing.restoreSpan(
                getattr(request, "span_info", None)
            )
            with trace.use_span(request_span):
                request._set(
                    _span=trace.get_tracer("zuul").start_span(
                        "NodesetRequestProcessing"
                    )
                )
        # Seed Rust provider_quota cache with fresh endpoint limits minus
        # unmanaged. Bypass _provider_quota_cache: a concurrent refresh
        # between the test's cache.clear() and CleanupWorker._run() can
        # leave a stale empty-unmanaged entry for 300s.
        import json as _json

        payloads = {}
        if getattr(launcher, "_provider_quota_cache", None) is not None:
            launcher._provider_quota_cache.clear()
        for providers in launcher.tenant_providers.values():
            for provider in providers:
                try:
                    q = call_launcher(launcher, "getProviderQuota", provider)
                    payload = _quota_payload(q)
                    # Index under every name Rust might use for lookup.
                    payloads[provider.canonical_name] = payload
                    payloads[provider.name] = payload
                    ep = provider.getEndpoint()
                    payloads[ep.canonical_name] = payload
                except Exception:
                    launcher.log.exception(
                        "Unable to seed quota for %s", provider.canonical_name
                    )
        if payloads:
            launcher.seed_quota_cache(_json.dumps(payloads))
        # unassigned/pending are unused by the Rust-backed method; pass
        # placeholders so patched wrappers that forward *args still work.
        unassigned = call_launcher(launcher, "_getUnassignedNodes")
        pending = set()
        launcher._acceptRequest(request, log, unassigned, pending)
        return {"ok": True}
    except NoNodeError:
        return {"ok": False, "missing": True}
    except Exception as exc:
        # Missing-label / no-provider failures are completed inside Rust
        # accept_request_by_uuid. Other accept errors are info-level so
        # checkLogs does not treat them as unexpected tracebacks.
        launcher.log.info(
            "Error accepting request %s: %s: %s",
            data.get("uuid"),
            type(exc).__name__,
            exc,
        )
        return {"ok": False, "error": True}


def _node_tags(launcher, data):
    provider = _find_provider(launcher, data["provider"])
    label = provider.labels[data["label"]]
    request = None
    if data.get("request"):
        request = launcher.api.getNodesetRequest(data["request"])
    return provider.getNodeTags(
        launcher.system.system_id, label, data["uuid"], request
    )


def _endpoint_limits(launcher, data):
    provider = _find_provider(launcher, data["provider"])
    _ensure_endpoint(launcher, provider)
    return _quota_payload(provider.getEndpointLimits())


def _label_quota(launcher, data):
    provider = _find_provider(launcher, data["provider"])
    label = provider.labels[data["label"]]
    return _quota_payload(provider.getQuotaForLabel(label))


def emit_events(launcher, payload):
    import json

    from zuul.model import NodesProvisionedEvent

    for item in json.loads(payload):
        event = NodesProvisionedEvent(item["uuid"], item.get("buildset"))
        launcher.result_events[item["tenant"]][item["pipeline"]].put(event)
        # accept_one holds the request lock through ACCEPTED until
        # finish_request (FINAL + event). Release here to match
        # server.py _processNodesetRequest after FINAL_STATES.
        request = launcher.api.getNodesetRequest(item["uuid"])
        if request is not None and request.hasLock():
            try:
                if hasattr(request, "_span") and request._span is not None:
                    try:
                        request._span.set_attributes(request.getSpanAttributes())
                        request._span.end()
                    except Exception:
                        pass
                with launcher.createZKContext(None, launcher.log) as ctx:
                    request.releaseLock(ctx)
            except Exception:
                launcher.log.exception(
                    "Unable to release request lock for %s", item["uuid"]
                )


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
