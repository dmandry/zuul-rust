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
    # Upload, snapshot, and cleanup call back into Launcher methods the
    # PyO3 class does not define (_getProviders, _processNodeSnapshot).
    _bind_missing_methods(launcher)
    _install_node_cache_hook(launcher)

    # server.Launcher.__init__ creates this before the stats thread.
    # runStatsElection logs a traceback every second if it is missing,
    # and checkLogs fails the test on that traceback.
    from zuul.launcher.server import LauncherStatsElection

    launcher.stats_election = LauncherStatsElection(launcher.zk_client)

    def _stats_loop():
        from zuul.launcher.server import Launcher

        _bind_missing_methods(launcher)
        launcher.runStats = lambda: Launcher.runStats(launcher)
        try:
            Launcher.runStatsElection(launcher)
        except Exception:
            launcher.log.exception("Exception running election stats:")

    launcher.stats_thread = threading.Thread(
        target=_stats_loop, name="LauncherStats", daemon=True
    )


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
    """Put the live Python state machine on the cached node.

    test_launcher_failover reads create_state from ZooKeeper and
    INSTANCE_CREATING from node.create_state_machine. The machine is
    not serialized. Attaching it before the cache applies a new
    create_state keeps that attribute from being None once create_state
    is truthy.
    """
    if node is None:
        return
    machines = launcher.__dict__.get("_cloud_sms") or {}
    uuid = getattr(node, "uuid", None)
    if not uuid:
        return
    for kind, attr in (
        ("create", "create_state_machine"),
        ("delete", "delete_state_machine"),
    ):
        machine = machines.get((kind, uuid))
        if machine is not None:
            node._set(**{attr: machine})


def _install_node_cache_hook(launcher):
    cache = launcher.api.nodes_cache
    if getattr(cache, "_rust_sm_hook", False):
        return
    orig_update = cache.updateFromRaw
    orig_object = cache.objectFromRaw

    def updateFromRaw(obj, key, data, stat):
        _attach_state_machines(launcher, obj)
        return orig_update(obj, key, data, stat)

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
    """Image upload/delete and snapshot work from Launcher._run.

    The three upload executors (1, 10, and 20 workers) and the cleanup
    thread are the same objects server.py starts.
    """
    from zuul.launcher.server import Launcher

    _bind_missing_methods(launcher)
    changed = getattr(launcher, "_layout_just_updated", False)
    launcher._layout_just_updated = False
    try:
        if changed or launcher.image_updated_event.is_set():
            Launcher.checkOldImages(launcher)
            Launcher.checkMissingUploads(launcher)
        elif launcher.upload_deleted_event.is_set():
            Launcher.checkOldImages(launcher)
        for node in launcher.api.getMatchingProviderNodes():
            if node.state != node.State.SNAPSHOT:
                continue
            try:
                Launcher._processNodeSnapshot(launcher, node, launcher.log)
            except Exception:
                launcher.log.exception("Error processing node %s snapshot", node)
    except Exception:
        launcher.log.exception("Error in image maintenance:")


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
    launcher.log.debug("Stopping stats thread")
    # Kazoo Election.run blocks until this launcher wins or cancel()
    # is called. stop_event only wakes the winner's runStats loop.
    launcher.stats_election.cancel()
    launcher.stats_thread.join()
    launcher.cleanup_worker.stop()
    launcher.cleanup_worker.join()
    # The run loop polls instead of waiting on a ChildrenWatch, so it
    # is often inside a state-machine step when stop() begins. Finish
    # that step before the endpoint executors are shut down.
    with launcher._run_lock:
        launcher.upload_executor.shutdown()
        launcher.endpoint_import_executor.shutdown()
        launcher.endpoint_upload_executor.shutdown()
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

    Moto, the responses fixture, the OpenStack and Kubernetes fakes, and
    the tests' patches of AwsCreateStateMachine / endpoint methods are all
    in-process. This shim drives the real Python provider state machine
    (the 14.3.0 boto3 / azul / SDK call sequence) and copies the node
    fields back. It is test-crate only; the shared launcher decides when
    to create, delete, pack slots, scan, and fulfill.
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
            result = _quota_for_node(launcher, data)
        elif op == "quota_for_label":
            result = _quota_for_label(launcher, data)
        elif op == "quota_percentage":
            result = _quota_percentage(launcher, data)
        elif op == "accept_requested":
            result = _accept_requested(launcher, data)
        elif op == "fail":
            result = _cloud_fail(launcher, data)
        elif op == "node_visible":
            result = _node_visible(launcher, data)
        elif op == "class_attr":
            result = _class_attr(data)
        elif op == "matching_requests":
            result = _matching_requests(launcher)
        elif op == "matching_nodes":
            result = _matching_nodes(launcher)
        elif op == "request_cache_ready":
            result = _request_cache_ready(launcher, data)
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


def _accept_requested(launcher, _data):
    """Launcher._processRequests for REQUESTED and ACCEPTED requests.

    Acceptance goes through launcher._acceptRequest so a test patch of
    that method runs. Accepted requests go through _checkRequest, which
    keeps both nodes of a request on one provider after a retry.
    """
    from kazoo.exceptions import NoNodeError
    from opentelemetry import trace
    from zuul.launcher.server import NodesetRequestError
    from zuul.lib import tracing as zuul_tracing
    from zuul.model import NodesProvisionedEvent
    from zuul.zk.zkobject import ZKContext

    log = launcher.log
    pending = set()
    unassigned = call_launcher(launcher, "_getUnassignedNodes")
    for request in list(launcher.api.getMatchingRequests()):
        try:
            if not request.hasLock():
                if (
                    request.state in request.FINAL_STATES
                    and request.event_state == request.EventState.COMPLETE
                ):
                    continue
                ctx = ZKContext(
                    launcher.zk_client, None, launcher.stop_event, log
                )
                if not request.acquireLock(ctx, blocking=False):
                    continue
                request.refresh(ctx)
                request_span = zuul_tracing.restoreSpan(
                    getattr(request, "span_info", None)
                )
                with trace.use_span(request_span):
                    request._set(
                        _span=trace.get_tracer("zuul").start_span(
                            "NodesetRequestProcessing"
                        )
                    )
            if not call_launcher(launcher, "_cachesReadyForRequest", request):
                continue
            try:
                if request.state == request.State.REQUESTED:
                    launcher._acceptRequest(request, log, unassigned, pending)
                elif request.state == request.State.ACCEPTED:
                    launcher._checkRequest(request, log, unassigned)
            except NodesetRequestError as err:
                launcher.log.error(
                    "Marking request %s as %s: %s",
                    request,
                    request.State.FAILED,
                    err,
                )
                ctx = ZKContext(
                    launcher.zk_client,
                    request._lock,
                    launcher.stop_event,
                    log,
                )
                request.updateAttributes(ctx, state=request.State.FAILED)
            except Exception:
                launcher.log.exception("Error processing request %s", request)
            if request.state in request.FINAL_STATES:
                if getattr(request, "_span", None) is not None:
                    request._span.set_attributes(request.getSpanAttributes())
                    request._span.end()
                event = NodesProvisionedEvent(
                    request.uuid, request.buildset_uuid
                )
                launcher.result_events[request.tenant_name][
                    request.pipeline_name
                ].put(event)
                ctx = ZKContext(
                    launcher.zk_client, None, launcher.stop_event, log
                )
                request.updateAttributes(
                    ctx, event_state=request.EventState.COMPLETE
                )
                request.releaseLock(ctx)
        except NoNodeError:
            # The test deleted the request between the cache read and
            # refresh. Python sleeps long enough to miss this; our poll
            # hits it. A missing request is not a launcher error.
            continue
        finally:
            if request.hasLock():
                try:
                    ctx = ZKContext(
                        launcher.zk_client, None, launcher.stop_event, log
                    )
                    request.releaseLock(ctx)
                except NoNodeError:
                    pass
    return {"ok": True}


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
