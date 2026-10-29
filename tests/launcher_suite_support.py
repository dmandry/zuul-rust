# Copyright 2026 Acme Gating, LLC
# SPDX-License-Identifier: Apache-2.0
"""Fixture host for the Rust port of tests/unit/test_launcher.py.

Bridge mode runs the real Python launcher, scheduler client, ZooKeeper
model, and test fixtures. Native mode keeps those fixtures and replaces
the launcher thread with an observer: it mirrors layout state so
waitUntilSettled can proceed, and does not provision. The Rust test
process runs the native launcher against the same ZooKeeper chroot and
a moto server (the in-process moto mock is not visible to that process).
"""

import json
import math
import os
import shutil
import threading
import time
import traceback

# Same guard as zuul_launcher_py. Native and bridge fixtures import this
# module; the Python stestr run does not. A VM suspend jumps
# CLOCK_REALTIME by minutes while CLOCK_MONOTONIC stays put, and the
# next iterate_timeout / waitUntilSettled sample then raises.
if not getattr(time, "_zuul_clock_guard", False):
    _real_time = time.time
    _real_mono = time.monotonic
    _clock = {"wall": _real_time(), "mono": _real_mono(), "offset": 0.0}

    def _guarded_time():
        wall = _real_time()
        mono = _real_mono()
        dwall = wall - _clock["wall"]
        dmono = mono - _clock["mono"]
        if dwall > dmono + 2.0:
            _clock["offset"] -= dwall - dmono
        _clock["wall"] = wall
        _clock["mono"] = mono
        return wall + _clock["offset"]

    time.time = _guarded_time
    time._zuul_clock_guard = True

_CHECKS = []


def _pollable_nodescan_fd():
    """An fd epoll_ctl will accept.

    NodescanFixture's FakeSocket.fileno is 1. That is pollable when stdout
    is a pipe, and EPERM when it is a regular file. A private pipe is
    pollable either way, and it does not add a thread for shutdown() to see.
    """
    fd = getattr(_pollable_nodescan_fd, "fd", None)
    if fd is None:
        read_fd, write_fd = os.pipe()
        _pollable_nodescan_fd.fd = read_fd
        _pollable_nodescan_fd.write_fd = write_fd
        fd = read_fd
    return fd


def _pin_executable():
    """Subprocesses must be the venv interpreter.

    PyO3's embedded interpreter reports a sys.executable that is not the
    3.11 venv, so multiprocessing and Ansible start the wrong Python and
    cannot import extension modules. The fixture controller process is
    already that venv interpreter; pointing at it is a no-op there.
    """
    import sys

    root = "/home/ubuntu/zuul-nox-311"
    venv = os.path.join(root, "bin", "python")
    if not os.path.exists(venv):
        return
    sys.executable = venv
    # AnsibleManager looks up venvs under sys.exec_prefix. The embedded
    # interpreter's prefix is unset ("<exec_prefix>"), so version 11 is
    # reported as not installed.
    sys.prefix = root
    sys.exec_prefix = root
    try:
        import multiprocessing

        multiprocessing.set_executable(venv)
    except Exception:
        pass


def __suite_ser(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return "nan"
        if value == math.inf:
            return "inf"
        if value == -math.inf:
            return "-inf"
        if value.is_integer() and abs(value) < 1e15:
            return str(int(value))
        return repr(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, (bytes, bytearray)):
        return "bytes:" + bytes(value).hex()
    if isinstance(value, (set, frozenset)):
        parts = sorted(__suite_ser(item) for item in value)
        return "set:[" + ",".join(parts) + "]"
    if isinstance(value, tuple):
        return "tuple:[" + ",".join(__suite_ser(item) for item in value) + "]"
    if isinstance(value, list):
        return "list:[" + ",".join(__suite_ser(item) for item in value) + "]"
    if isinstance(value, dict):
        parts = []
        for key in sorted(value.keys(), key=__suite_ser):
            parts.append(__suite_ser(key) + ":" + __suite_ser(value[key]))
        return "dict:{" + ",".join(parts) + "}"
    to_dict = getattr(value, "toDict", None)
    if callable(to_dict):
        try:
            return "model:" + __suite_ser(to_dict())
        except Exception:
            pass
    return "repr:" + repr(value)


def __suite_check(op, left_src, left, right_src, right):
    _CHECKS.append(
        {
            "op": op,
            "left_src": left_src,
            "left": left,
            "right_src": right_src,
            "right": right,
        }
    )


def __suite_cmp(op, src_a, left, src_b, right):
    __suite_check(op, src_a, __suite_ser(left), src_b, __suite_ser(right))


def __suite_truth(src, value, expect):
    ok = bool(value) if expect else not bool(value)
    __suite_check(
        "eq",
        src,
        "true" if ok else "false",
        "True" if expect else "False",
        "true",
    )


def __suite_in(src_a, left, src_b, right, expect_in):
    found = left in right
    ok = found if expect_in else not found
    __suite_check(
        "eq",
        f"{src_a} in {src_b}",
        "true" if ok else "false",
        "True",
        "true",
    )


def __suite_history(test, expected, ordered=True):
    def project(build, keys):
        return {key: getattr(build, key) for key in keys}

    if not isinstance(expected, (list, tuple)):
        __suite_check("eq", "history", __suite_ser(expected), "list", "null")
        return
    wanted = list(expected)
    history = list(test.history)
    __suite_check(
        "eq",
        "len(self.history)",
        __suite_ser(len(history)),
        "len(expected)",
        __suite_ser(len(wanted)),
    )
    if ordered:
        actual = []
        for index, item in enumerate(wanted):
            if index < len(history) and isinstance(item, dict):
                actual.append(project(history[index], list(item.keys())))
            else:
                actual.append({"missing": index})
        __suite_check("eq", "history", __suite_ser(actual), "expected", __suite_ser(wanted))
        return
    matched = []
    used = set()
    for item in wanted:
        found = None
        if isinstance(item, dict):
            for index, build in enumerate(history):
                if index in used:
                    continue
                if all(getattr(build, key) == val for key, val in item.items()):
                    found = index
                    break
        if found is None:
            matched.append({"missing": item})
        else:
            used.add(found)
            matched.append({key: getattr(history[found], key) for key in item})
    __suite_check("eq", "history", __suite_ser(matched), "expected", __suite_ser(wanted))
    __suite_check(
        "eq",
        "unseen history",
        __suite_ser(len(history) - len(used)),
        "0",
        "0",
    )


def __suite_reported(test, *args, **kwargs):
    try:
        type(test).assertReportedStat(test, *args, **kwargs)
    except Exception as exc:
        __suite_check("eq", "assertReportedStat", __suite_ser(str(exc)), "ok", "ok")
        raise
    __suite_check("eq", "assertReportedStat", "ok", "ok", "ok")


def __suite_opaque(source):
    # An assertion kind the rewriter did not specialize. Running it here
    # keeps the original fixture check, and the Rust side still requires
    # the recorded result.
    raise AssertionError("unported assertion: " + source)


def _helpers():
    return {
        "__suite_ser": __suite_ser,
        "__suite_check": __suite_check,
        "__suite_cmp": __suite_cmp,
        "__suite_truth": __suite_truth,
        "__suite_in": __suite_in,
        "__suite_history": __suite_history,
        "__suite_reported": __suite_reported,
        "__suite_opaque": __suite_opaque,
    }


def _dump_config(case, path, moto_url):
    import configparser

    parser = configparser.RawConfigParser()
    for section in case.config.sections():
        parser.add_section(section)
        for key, value in case.config.items(section):
            parser.set(section, key, value)
    if moto_url and parser.has_section("connection aws"):
        parser.set("connection aws", "endpoint_url", moto_url)
    with open(path, "w", encoding="utf-8") as handle:
        parser.write(handle)


def _method_quotas(class_name, method):
    """driver_config quotas the Python fixture feeds to getEndpointLimits."""
    import tests.unit.test_launcher as mod

    func = getattr(getattr(mod, class_name), method)
    cfg = getattr(func, "__driver_config__", None) or {}
    raw = (cfg.get("test_launcher") or {}).get("quotas") or {}
    out = {}
    for key, value in raw.items():
        out[str(key)] = float(value)
    return out


def _pass_dir():
    root = os.environ.get("ZUUL_TEST_ROOT", "/tmp/zuul-test")
    path = os.path.join(root, "native-pass")
    os.makedirs(path, exist_ok=True)
    return path


class _PassLink:
    """Blocking unix socket to one native launcher thread.

    The reader is named Thread- so shutdown's thread check ignores it.
    poke and a done message with wake set the launcher's wake_event,
    which is what server.Launcher.run waits on.
    """

    def __init__(self, path, launcher):
        import socket as socket_mod

        self.launcher = launcher
        self.sock = socket_mod.socket(socket_mod.AF_UNIX, socket_mod.SOCK_STREAM)
        self.sock.connect(path)
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        self.pending = {}
        self.seq = 0
        thread = threading.Thread(
            target=self._read, name="Thread-native-pass", daemon=True
        )
        thread.start()

    def request(self, op, reply, timeout=30):
        with self.lock:
            self.seq += 1
            seq = self.seq
            self.pending[(reply, seq)] = False
            payload = (json.dumps({"op": op, "seq": seq}) + "\n").encode()
            self.sock.sendall(payload)
            deadline = time.monotonic() + timeout
            while not self.pending.get((reply, seq)):
                remain = deadline - time.monotonic()
                if remain <= 0:
                    raise TimeoutError(op)
                self.cv.wait(remain)
            self.pending.pop((reply, seq), None)

    def send_stop(self):
        try:
            with self.lock:
                self.sock.sendall(b'{"op":"stop"}\n')
        except OSError:
            pass

    def _read(self):
        buf = b""
        while True:
            try:
                chunk = self.sock.recv(4096)
            except OSError:
                return
            if not chunk:
                return
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if not line.strip():
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue
                op = msg.get("op")
                if op == "poke" or (op == "done" and msg.get("wake")):
                    self.launcher.wake_event.set()
                if op in ("done", "cleanup_done", "stats_done"):
                    with self.cv:
                        self.pending[(op, msg.get("seq"))] = True
                        self.cv.notify_all()


def _ensure_link(launcher):
    link = getattr(launcher, "_pass_link", None)
    if link is not None:
        return link
    directory = _pass_dir()
    ready = os.path.join(directory, launcher.hostname + ".ready")
    if not os.path.exists(ready):
        return None
    # The listener is bound before the ready file is written, so a
    # missing socket means the native thread has already exited.
    path = os.path.join(directory, launcher.hostname + ".sock")
    if not os.path.exists(path):
        return None
    try:
        link = _PassLink(path, launcher)
    except OSError:
        return None
    launcher._pass_link = link
    return link


def _watch_launcher_stop(launcher):
    """Tell the native thread this launcher was stopped.

    The Rust component is a different ephemeral znode from the one
    Launcher.stop writes. Without this, the stopped launcher stays
    RUNNING and can win launcher_score while its thread is gone.
    """
    orig = launcher.stop
    host = launcher.hostname

    def stop():
        link = getattr(launcher, "_pass_link", None)
        if link is not None:
            link.send_stop()
        try:
            path = os.path.join(_pass_dir(), host + ".stop")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("1")
        except OSError:
            pass
        return orig()

    launcher.stop = stop


def _wrap_cleanup_run(launcher):
    """Run the native cleanup pass when a test calls cleanup_worker._run.

    Unmanaged instances are recorded there. The observer's own pass
    only fills the Python quota cache, which the Rust process does
    not read.
    """
    worker = getattr(launcher, "cleanup_worker", None)
    if worker is None or getattr(worker._run, "_suite_cleanup", False):
        return
    orig = worker._run

    def _run():
        orig()
        link = _ensure_link(launcher)
        if link is None:
            return
        try:
            link.request("cleanup", "cleanup_done")
        except TimeoutError:
            launcher.log.warning("native cleanup timed out")
        except Exception as exc:
            launcher._pass_link = None
            launcher.log.warning("native cleanup socket closed: %s", exc)

    _run._suite_cleanup = True
    worker._run = _run


def _wrap_upload_checks(launcher):
    """Let a direct checkMissingUploads see znodes created by the test.

    The test and the observer use different ZooKeeper sessions. The
    upload registry watch is asynchronous, and the test calls
    checkMissingUploads immediately after ImageUpload.new.
    """
    if getattr(launcher.checkMissingUploads, "_suite_sync", False):
        return
    orig = launcher.checkMissingUploads

    def check_missing_uploads(*args, **kwargs):
        # ImageUpload.new runs on the test's ZooKeeper session. The
        # observer's watch can still be in flight when the test calls
        # this, and one waitForSync only drains events already queued.
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                launcher.image_upload_registry.waitForSync(timeout=0.5)
                launcher.image_build_registry.waitForSync(timeout=0.5)
            except Exception:
                launcher.log.debug(
                    "upload registry sync before check", exc_info=True
                )
            upload = launcher.image_upload_registry.getItem("upload-uuid")
            artifact = launcher.image_build_registry.getItem("iba-uuid")
            artifact_ready = (
                artifact is not None
                and artifact.state == artifact.State.READY
            )
            if upload is not None and artifact_ready:
                break
            time.sleep(0.05)
        return orig(*args, **kwargs)

    check_missing_uploads._suite_sync = True
    launcher.checkMissingUploads = check_missing_uploads


def _drive_native_pass(launcher):
    """One native launcher pass while this thread holds _run_lock.

    server.Launcher.run holds _run_lock around _run. The observer is
    _run, so a token issued here is a pass the test can pause by
    taking that lock. Before the Rust thread writes the ready file,
    return immediately so setUp does not wait on a launcher that has
    not started.
    """
    try:
        link = _ensure_link(launcher)
        if link is None:
            return
        try:
            link.request("pass", "done")
        except TimeoutError:
            launcher.log.warning("native pass timed out")
    except Exception as exc:
        # Launcher.run logs any exception here as "Error in main thread",
        # and checkLogs fails the test. The pass socket closes when the
        # native thread is handed off; the next pass reconnects.
        launcher._pass_link = None
        launcher.log.warning("native pass socket closed: %s", exc)


def _silence_python_stats(test, launcher):
    """Stats assertions read FakeStatsd. Only the Rust process may send.

    The observer is a real Launcher, so its timer, executors, and
    _runStats would otherwise publish the same names. Clearing the
    queue drops packets from setUp, before the native thread exists.
    A replacement launcher must not clear packets already received.
    """
    from contextlib import nullcontext

    launcher.statsd = None
    launcher.statsd_timer = nullcontext
    for name in (
        "upload_executor",
        "endpoint_upload_executor",
        "endpoint_import_executor",
    ):
        pool = getattr(launcher, name, None)
        if pool is not None:
            # MonitoringThreadPoolExecutor stores the client as
            # _zuul_statsd (zuul/lib/concurrency.py). Nulling .statsd
            # leaves _zuul_emit sending upload_workers from this process.
            pool._zuul_statsd = None

    def _run_stats():
        link = _ensure_link(launcher)
        if link is None:
            return
        try:
            link.request("stats", "stats_done")
        except TimeoutError:
            launcher.log.warning("native stats timed out")
        except Exception as exc:
            launcher._pass_link = None
            launcher.log.warning("native stats socket closed: %s", exc)

    launcher._runStats = _run_stats
    marker = os.path.join(_pass_dir(), "supervisor")
    if not os.path.exists(marker):
        stats = getattr(test, "statsd", None)
        if stats is not None and hasattr(stats, "clear"):
            stats.clear()


class _MotoProxy:
    """Forward to moto, and answer ListServiceQuotas.

    moto 5 does not implement service quotas. The Python fixture patches
    getEndpointLimits. The native client hits the endpoint, so the proxy
    returns that patch: driver_config quotas as QuotaCode entries, or an
    empty page when the test left the limit at infinity.
    """

    def __init__(self, upstream, quotas=None):
        self.upstream = upstream
        self.quotas = dict(quotas or {})
        self.httpd = None

    def start(self):
        import socket
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

        upstream = self.upstream
        quotas = dict(self.quotas)

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, fmt, *args):
                return

            def _read(self):
                length = int(self.headers.get("Content-Length", "0") or 0)
                return self.rfile.read(length) if length else b""

            def _empty_quotas(self):
                items = [
                    {"QuotaCode": key, "Value": value}
                    for key, value in quotas.items()
                ]
                data = json.dumps({"Quotas": items}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/x-amz-json-1.1")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def _forward(self, method, body):
                import http.client
                from urllib.parse import urlsplit

                parts = urlsplit(upstream)
                conn = http.client.HTTPConnection(parts.hostname, parts.port, timeout=60)
                headers = {
                    key: value
                    for key, value in self.headers.items()
                    if key.lower() not in ("host", "content-length")
                }
                path = self.path or "/"
                conn.request(method, path, body=body, headers=headers)
                resp = conn.getresponse()
                payload = resp.read()
                self.send_response(resp.status)
                for key, value in resp.getheaders():
                    if key.lower() in ("transfer-encoding", "connection"):
                        continue
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                conn.close()

            def _dispatch(self, method):
                body = self._read()
                target = self.headers.get("X-Amz-Target", "")
                if "ListServiceQuotas" in target:
                    self._empty_quotas()
                    return
                self._forward(method, body)

            def do_GET(self):
                self._dispatch("GET")

            def do_POST(self):
                self._dispatch("POST")

            def do_PUT(self):
                self._dispatch("PUT")

            def do_DELETE(self):
                self._dispatch("DELETE")

            def do_HEAD(self):
                self._dispatch("HEAD")

        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        self.httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        return f"http://127.0.0.1:{port}"

    def stop(self):
        if self.httpd is not None:
            self.httpd.shutdown()
            self.httpd = None


def _start_moto(quotas=None):
    import socket
    import subprocess
    import sys

    from moto.server import ThreadedMotoServer

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
    server.start()
    upstream = f"http://127.0.0.1:{port}"
    proxy = _MotoProxy(upstream, quotas)
    url = proxy.start()
    server._suite_proxy = proxy
    script = (
        "import boto3\n"
        f"url={url!r}\n"
        "kw=dict(region_name='us-east-1', endpoint_url=url,\n"
        "        aws_access_key_id='fake', aws_secret_access_key='fake')\n"
        "ec2=boto3.client('ec2', **kw)\n"
        "try:\n"
        "    ec2.create_key_pair(KeyName='zuul')\n"
        "except Exception as exc:\n"
        "    print('key', exc)\n"
        "s3=boto3.client('s3', **kw)\n"
        "try:\n"
        "    s3.create_bucket(Bucket='zuul')\n"
        "except Exception as exc:\n"
        "    print('bucket', exc)\n"
    )
    subprocess.run([sys.executable, "-c", script], check=False)
    return server, url


_BOOT = None
_HOOK_LAUNCHERS = {}
_LAUNCH_SEQ = 0
_ORIG_CALLABLES = {}


def _bootstrap():
    global _BOOT
    if _BOOT is not None:
        return _BOOT
    import importlib.util

    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "rust",
        "zuul-launcher-py",
        "src",
        "bootstrap.py",
    )
    spec = importlib.util.spec_from_file_location("zuul_launcher_bootstrap", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    _BOOT = mod
    return mod


def _save_hook_originals():
    if _ORIG_CALLABLES:
        return
    import zuul.launcher.server as server
    import zuul.zk.launcher as zk_launcher
    from zuul.driver.aws import awsendpoint, awsprovider

    _ORIG_CALLABLES.update(
        {
            "getEndpointLimits": awsprovider.AwsProvider.getEndpointLimits,
            "getQuotaForLabel": awsprovider.AwsProvider.getQuotaForLabel,
            "doesProviderHaveQuotaForLabel": server.Launcher.doesProviderHaveQuotaForLabel,
            "launcher_score": zk_launcher.launcher_score,
            "refresh": awsendpoint.AwsProviderEndpoint._refresh,
            "refresh_delete": awsendpoint.AwsProviderEndpoint._refreshDelete,
            "create_instance": awsendpoint.AwsProviderEndpoint._createInstance,
            "create_advance": awsendpoint.AwsCreateStateMachine.advance,
            "delete_advance": awsendpoint.AwsDeleteStateMachine.advance,
            "snapshot_advance": awsendpoint.AwsSnapshotStateMachine.advance,
            "upload_run": awsendpoint.AwsImageUploadJob.run,
            "import_run": awsendpoint.AwsImageImportJob.run,
            "upload_job": server.UploadJob.run,
        }
    )
    boot = _bootstrap()
    boot._record_original_sms()


def _patched(current, original):
    boot = _bootstrap()
    return boot._callable_is_patched(current, original)


def _launcher_for(args):
    name = (args or {}).get("hostname")
    if name and name in _HOOK_LAUNCHERS:
        return _HOOK_LAUNCHERS[name]
    if _HOOK_LAUNCHERS:
        return next(iter(_HOOK_LAUNCHERS.values()))
    return None


def _load_cloud(result):
    """cloud_call returns json.dumps(payload). Hooks need the object."""
    if isinstance(result, str):
        try:
            return json.loads(result)
        except Exception:
            return result
    return result


def _hook_reply(hooked, result=None, error=None):
    if error is not None:
        kind = "other"
        text = str(error)
        name = type(error).__name__
        if name == "QuotaException" or "Quota" in name:
            kind = "quota"
        if "Capacity" in name:
            kind = "capacity"
        return {"hooked": True, "error_kind": kind, "message": text}
    return {"hooked": bool(hooked), "result": result if result is not None else {}}


def _handle_hook(msg, callback):
    op = msg.get("op") or ""
    args = msg.get("args") or {}
    launcher = _launcher_for(args)
    boot = _bootstrap()
    try:
        if op == "score_patched":
            import zuul.zk.launcher as zk_launcher

            patched = _patched(zk_launcher.launcher_score, _ORIG_CALLABLES["launcher_score"])
            return _hook_reply(True, {"patched": patched})
        if op == "accept_patched":
            fn = getattr(launcher, "_acceptRequest", None) if launcher else None
            wrapped = bool(getattr(fn, "_suite_accept_wrapper", False))
            return _hook_reply(True, {"patched": not wrapped})
        if op == "snapshot_patched":
            from zuul.driver.aws import awsendpoint

            patched = _patched(
                awsendpoint.AwsSnapshotStateMachine.advance,
                _ORIG_CALLABLES["snapshot_advance"],
            )
            return _hook_reply(True, {"patched": patched})
        if op.startswith("upload_patched_"):
            from zuul.driver.aws import awsendpoint
            import zuul.launcher.server as server

            kind = op[len("upload_patched_") :]
            if kind == "import":
                patched = _patched(
                    awsendpoint.AwsImageImportJob.run, _ORIG_CALLABLES["import_run"]
                )
            else:
                patched = _patched(
                    awsendpoint.AwsImageUploadJob.run, _ORIG_CALLABLES["upload_run"]
                ) or _patched(server.UploadJob.run, _ORIG_CALLABLES["upload_job"])
            return _hook_reply(True, {"patched": patched})
        if launcher is None:
            return _hook_reply(False)
        if op == "class_attr":
            return _hook_reply(True, boot._class_attr(args))
        if op == "node_visible":
            return _hook_reply(True, boot._node_visible(launcher, args))
        if op == "nodescan.mode":
            return _hook_reply(True, boot._nodescan_mode(launcher, args))
        if op == "nodescan":
            return _hook_reply(True, boot._nodescan_step(launcher, args))
        if op == "nodescan.io":
            return _hook_reply(True, boot._nodescan_io(launcher, args))
        if op == "request_cache_ready":
            return _hook_reply(True, boot._request_cache_ready(launcher, args))
        if op == "endpoint_limits":
            from zuul.driver.aws import awsprovider

            if not _patched(
                awsprovider.AwsProvider.getEndpointLimits,
                _ORIG_CALLABLES["getEndpointLimits"],
            ):
                return _hook_reply(False)
            return _hook_reply(True, boot._endpoint_limits(launcher, args))
        if op == "label_quota":
            from zuul.driver.aws import awsprovider

            if not _patched(
                awsprovider.AwsProvider.getQuotaForLabel,
                _ORIG_CALLABLES["getQuotaForLabel"],
            ):
                return _hook_reply(False)
            return _hook_reply(True, boot._label_quota(launcher, args))
        if op == "provider_quota_for_label":
            import zuul.launcher.server as server

            if not _patched(
                server.Launcher.doesProviderHaveQuotaForLabel,
                _ORIG_CALLABLES["doesProviderHaveQuotaForLabel"],
            ):
                return _hook_reply(False)
            provider = boot._find_provider(launcher, args["provider"])
            label = provider.labels[args["label"]]
            messages = []
            ok = server.Launcher.doesProviderHaveQuotaForLabel(
                launcher,
                args.get("tenant") or provider.tenant_name,
                provider,
                label,
                messages,
                bool(args.get("include_usage", True)),
            )
            return _hook_reply(True, {"ok": bool(ok)})
        if op == "owned_requests":
            return _hook_reply(True, boot._owned_requests(launcher, args))
        if op == "matching_requests":
            return _hook_reply(True, boot._matching_requests(launcher))
        if op == "matching_nodes":
            return _hook_reply(True, boot._matching_nodes(launcher))
        if op == "sm.patched_advance":
            result = boot._sm_patched_advance(launcher, args)
            if not result.get("patched"):
                return _hook_reply(False)
            return _hook_reply(True, result)
        if op in ("ep.refresh", "ep.refresh_delete", "ep.submit_create_instance", "ep.complete_create_instance"):
            from zuul.driver.aws import awsendpoint

            relevant = False
            if op == "ep.refresh":
                relevant = _patched(
                    awsendpoint.AwsProviderEndpoint._refresh, _ORIG_CALLABLES["refresh"]
                )
            elif op == "ep.refresh_delete":
                relevant = _patched(
                    awsendpoint.AwsProviderEndpoint._refreshDelete,
                    _ORIG_CALLABLES["refresh_delete"],
                )
            else:
                relevant = _patched(
                    awsendpoint.AwsProviderEndpoint._createInstance,
                    _ORIG_CALLABLES["create_instance"],
                )
            if not relevant:
                return _hook_reply(False)
            result = _load_cloud(boot.cloud_call(launcher, op, json.dumps(args)))
            if isinstance(result, dict) and result.get("error_kind"):
                return {
                    "hooked": True,
                    "error_kind": result.get("error_kind"),
                    "message": result.get("message") or "hook error",
                }
            return _hook_reply(True, result)
        if op == "upload.check_missing":
            launcher.checkMissingUploads()
            return _hook_reply(True, {"ok": True})
        if op in ("node.cache_insert", "node.cache_assign") or op == "upload.sdk_run" or op in (
            "upload.delete",
            "upload.delete_artifact",
            "upload.run_is_patched",
            "upload.artifact_job",
            "upload.fetch",
            "upload.log",
            "upload.notify",
            "upload.validate_event",
            "upload.reset_crashed",
            "stats.timing",
        ):
            result = _load_cloud(boot.cloud_call(launcher, op, json.dumps(args)))
            if isinstance(result, dict) and result.get("error_kind"):
                return {
                    "hooked": True,
                    "error_kind": result.get("error_kind"),
                    "message": result.get("message") or "upload error",
                }
            return _hook_reply(True, result)
        if op.startswith("snapshot."):
            result = _load_cloud(boot.cloud_call(launcher, op, json.dumps(args)))
            if isinstance(result, dict) and result.get("error_kind"):
                return {
                    "hooked": True,
                    "error_kind": result.get("error_kind"),
                    "message": result.get("message") or "snapshot error",
                }
            return _hook_reply(True, result)
        if op == "accept_one":
            return _hook_accept(launcher, args, callback)
        return _hook_reply(False)
    except Exception as exc:
        return _hook_reply(False, error=exc)


def _hook_accept(launcher, args, callback):
    from zuul.zk.zkobject import ZKContext
    from kazoo.exceptions import NoNodeError

    request = launcher.api.getNodesetRequest(args.get("uuid"))
    if request is None:
        return _hook_reply(True, {"ok": False, "missing": True})
    log = launcher.log
    launcher._hook_callback = callback
    try:
        if not request.hasLock():
            ctx = ZKContext(launcher.zk_client, None, launcher.stop_event, log)
            if not request.acquireLock(ctx, blocking=False):
                return _hook_reply(True, {"ok": False, "locked": False})
            try:
                request.refresh(ctx)
            except NoNodeError:
                return _hook_reply(True, {"ok": False, "missing": True})
        launcher._acceptRequest(request, log, None, None)
        return _hook_reply(True, {"ok": True})
    finally:
        launcher._hook_callback = None


def _serve_hook(conn):
    try:
        reader = conn.makefile("r", encoding="utf-8")
        writer = conn.makefile("w", encoding="utf-8")

        def callback(op, args):
            writer.write(json.dumps({"callback": op, "args": args}) + "\n")
            writer.flush()
            line = reader.readline()
            if not line:
                return {"ok": False}
            msg = json.loads(line)
            return msg.get("callback_result") or {}

        while True:
            line = reader.readline()
            if not line:
                break
            msg = json.loads(line)
            reply = _handle_hook(msg, callback)
            writer.write(json.dumps(reply) + "\n")
            writer.flush()
    except Exception:
        traceback.print_exc()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _start_hook_server(path):
    import socket as socket_mod

    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    sock = socket_mod.socket(socket_mod.AF_UNIX, socket_mod.SOCK_STREAM)
    sock.bind(path)
    sock.listen(64)

    def accept_loop():
        while True:
            try:
                conn, _addr = sock.accept()
            except OSError:
                return
            threading.Thread(target=_serve_hook, args=(conn,), daemon=True).start()

    import threading

    threading.Thread(target=accept_loop, daemon=True).start()
    return sock


def _note_launcher(launcher):
    boot = _bootstrap()
    boot._install_node_cache_hook(launcher)
    import zuul.launcher.server as server

    launcher._orig_nodescan_advance = server.NodescanRequest.advance
    launcher._orig_nodescan_socket = server.NodescanRequest._socket_class
    launcher._orig_upload_run = server.UploadJob.run
    try:
        from zuul.driver.aws import awsendpoint

        launcher._orig_snap_advance = awsendpoint.AwsSnapshotStateMachine.advance
    except Exception:
        pass
    if not getattr(launcher._acceptRequest, "_suite_accept_wrapper", False):
        orig = launcher._acceptRequest

        def wrapper(request, log, unassigned_nodes=None, pending_labels=None):
            cb = getattr(launcher, "_hook_callback", None)
            if cb is None:
                return orig(request, log, unassigned_nodes, pending_labels)
            return cb("rust_accept", {"uuid": getattr(request, "uuid", None)})

        wrapper._suite_accept_wrapper = True
        launcher._acceptRequest = wrapper
    _HOOK_LAUNCHERS[launcher.hostname] = launcher
    _wrap_cleanup_run(launcher)
    _wrap_upload_checks(launcher)


def _install_observer(launchers_path):
    import tests.base as base

    def observer_run(self):
        # tests/base.py waitUntilSettled compares layout uuids. The
        # observer copies the ZK layout and does not provision, so the
        # native launcher is the only component that takes work.
        # run() already holds _run_lock. The token is issued outside
        # _test_lock so waitUntilSettled can keep polling.
        # TestLauncher._run holds _test_lock across the whole pass.
        # waitUntilSettled takes that lock before it will call the
        # system settled, so it cannot observe a pass that has not
        # finished writing its result event.
        with self._test_lock:
            try:
                if self.layout_updated_event.is_set() or not self.tenant_providers:
                    self.layout_updated_event.clear()
                    try:
                        self.updateTenantProviders()
                    except Exception:
                        self.layout_updated_event.set()
                        with open("/tmp/observer-provider.err", "a", encoding="utf-8") as handle:
                            handle.write(traceback.format_exc() + "\n")
                store = self.tenant_layout_state
                for tenant_name in list(store):
                    state = store.get(tenant_name)
                    if state is not None:
                        self.local_layout_state[tenant_name] = state
                # updateTenantProviders assigns endpoints only when the
                # layout reload succeeds all the way through endpoint
                # postConfig. A first pass that raises in
                # _chooseLauncherForEndpoint (no RUNNING component yet)
                # keeps the providers and leaves endpoints empty, and
                # the next call no longer sees a layout change.
                if not self.endpoints and self.tenant_providers:
                    self.local_layout_state.clear()
                    try:
                        self.updateTenantProviders()
                    except Exception:
                        self.layout_updated_event.set()
                        with open(
                            "/tmp/observer-provider.err", "a", encoding="utf-8"
                        ) as handle:
                            handle.write(traceback.format_exc() + "\n")
                boot = _bootstrap()
                for node in list(self.api.nodes_cache.getItems()):
                    boot._attach_state_machines(self, node)
            except Exception:
                self.log.exception("observer layout mirror failed")
            _drive_native_pass(self)

    def create_launcher(self, instance_id=0, connection_filter=None):
        launcher = _install_observer.original(
            self, instance_id, connection_filter
        )
        # Launcher.__init__ registers a stopped component. The native
        # process registers the same hostname and is the one that runs.
        try:
            path = getattr(launcher.component_info, "path", None)
            if path:
                launcher.zk_client.client.delete(path)
        except Exception:
            launcher.log.debug("observer component cleanup", exc_info=True)
        # The supervisor starts one thread per distinct jsonl line.
        # A replacement launcher has the same hostname, so the record
        # needs a unique seq or the new thread is never spawned.
        global _LAUNCH_SEQ
        _LAUNCH_SEQ += 1
        record = {
            "hostname": launcher.hostname,
            "connection_filter": (
                list(launcher.connection_filter)
                if launcher.connection_filter
                else None
            ),
            "seq": _LAUNCH_SEQ,
        }
        # The previous Rust thread for this hostname exits by consuming
        # {hostname}.stop and closing its ZooKeeper session (ephemeral
        # node locks). That has to finish before the new record is
        # written, or the new thread sees the stop file and exits.
        stop_file = os.path.join(_pass_dir(), launcher.hostname + ".stop")
        ready = os.path.join(_pass_dir(), launcher.hostname + ".ready")
        if os.path.exists(stop_file):
            deadline = time.time() + 30
            while os.path.exists(stop_file) and time.time() < deadline:
                time.sleep(0.01)
            try:
                os.remove(ready)
            except FileNotFoundError:
                pass
        with open(launchers_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")
            handle.flush()
        # A launcher created after the supervisor is up has to finish
        # its first pass before requestNodes. setUp runs before that
        # supervisor exists, so it must not wait here.
        marker = os.path.join(_pass_dir(), "supervisor")
        if os.path.exists(marker):
            deadline = time.time() + 20
            while time.time() < deadline and not os.path.exists(ready):
                time.sleep(0.02)
        _note_launcher(launcher)
        _silence_python_stats(self, launcher)
        _watch_launcher_stop(launcher)
        launcher.wake_event.set()
        return launcher

    _install_observer.original = base.ZuulTestCase.createLauncher
    base.TestLauncher._run = observer_run
    base.ZuulTestCase.createLauncher = create_launcher


class Session:
    def __init__(self):
        self.case = None
        self.moto = None
        self.ready = False

    def begin(self, class_name, method, mode):
        _pin_executable()
        # Bridge mode must exercise zuul.launcher.server.Launcher. Pop the
        # selector before the test module chooses its base class.
        os.environ.pop("ZUUL_LAUNCHER_IMPL", None)
        import tests.unit.test_launcher as mod

        import tests.fake_nodescan as fake_nodescan

        def _fileno(self):
            # The worker waits for EPOLLOUT. A pipe's write end is
            # writable immediately, which is what fileno 1 is when
            # stdout is a pipe.
            _pollable_nodescan_fd()
            return _pollable_nodescan_fd.write_fd

        fake_nodescan.FakeSocket.fileno = _fileno
        if mode == "bridge":
            import tests.base as base
            import zuul.launcher.server as server

            if base.TestLauncher.__mro__[1] is not server.Launcher:
                raise RuntimeError(
                    "bridge mode is not using the Python launcher: %s"
                    % (base.TestLauncher.__mro__,)
                )

        os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        cls = getattr(mod, class_name)
        case = cls(method)
        launchers_path = ""
        config_path = ""
        moto_url = None
        before_env = dict(os.environ)
        try:
            if mode == "native":
                root = os.environ.get("ZUUL_TEST_ROOT", "/tmp/zuul-test")
                os.makedirs(root, exist_ok=True)
                launchers_path = os.path.join(
                    root, f"launchers-{os.getpid()}.jsonl"
                )
                config_path = os.path.join(root, f"zuul-{os.getpid()}.conf")
                try:
                    os.remove(launchers_path)
                except FileNotFoundError:
                    pass
                shutil.rmtree(_pass_dir(), ignore_errors=True)
                os.makedirs(_pass_dir(), exist_ok=True)
                _HOOK_LAUNCHERS.clear()
                global _LAUNCH_SEQ
                _LAUNCH_SEQ = 0
                _save_hook_originals()
                hook_sock = os.path.join(_pass_dir(), "hooks.sock")
                _start_hook_server(hook_sock)
                os.environ["ZUUL_HOOK_SOCK"] = hook_sock
                self.moto, moto_url = _start_moto(
                    _method_quotas(class_name, method)
                )
                _install_observer(launchers_path)
            case.setUp()
            self.case = case
            self.ready = True
            if mode == "native":
                _dump_config(case, config_path, moto_url)
        except Exception:
            self.case = case
            errors = self._drain_cleanups()
            self.case = None
            return {
                "ok": False,
                "error": traceback.format_exc(),
                "cleanup": errors,
            }
        delta = {
            key: value
            for key, value in os.environ.items()
            if before_env.get(key) != value and key not in ("PATH", "PYTHONPATH")
        }
        return {
            "ok": True,
            "config_path": config_path,
            "launchers_path": launchers_path,
            "moto_url": moto_url,
            "env": delta,
        }

    def execute(self, source, patches, prelude=""):
        global _CHECKS
        _CHECKS = []
        if self.case is None:
            return {"ok": False, "checks": [], "error": "begin failed"}
        import tests.unit.test_launcher as mod

        namespace = dict(mod.__dict__)
        namespace.update(_helpers())
        error = None
        launcher = getattr(self.case, "launcher", None)
        wake = getattr(launcher, "wake_event", None)
        if wake is not None:
            wake.set()
        try:
            if prelude:
                exec(prelude, namespace, namespace)
            exec(source, namespace, namespace)
            fn = namespace["__case"]
            for expr in reversed(list(patches or [])):
                fn = eval(expr, namespace, namespace)(fn)  # noqa: S307
            fn(self.case)
        except Exception:
            error = traceback.format_exc()
            try:
                hist = list(getattr(self.case, "history", []) or [])
                builds = list(getattr(self.case, "builds", []) or [])
                error += "\nSUITE history: " + repr([str(item) for item in hist])
                error += "\nSUITE builds: " + repr([
                    (str(item), getattr(item, "result", None), getattr(item, "waiting", None))
                    for item in builds
                ])
                stream = getattr(self.case, "_log_stream", None)
                if stream is not None:
                    text = stream.getvalue()
                    error += "\nSUITE log tail:\n" + text[-2000:]
            except Exception as exc:
                error += "\nSUITE history failed: " + repr(exc)
        if os.environ.get("ZUUL_SUITE_DUMP_LOG") and self.case is not None:
            stream = getattr(self.case, "_log_stream", None)
            if stream is not None:
                with open("/tmp/suite-case.log", "w", encoding="utf-8") as handle:
                    handle.write(stream.getvalue())
        return {"ok": error is None, "checks": list(_CHECKS), "error": error}

    def _drain_cleanups(self):
        case = self.case
        if case is None:
            return []
        errors = []
        while getattr(case, "_cleanups", None):
            function, args, kwargs = case._cleanups.pop()
            try:
                function(*args, **kwargs)
            except Exception:
                errors.append(traceback.format_exc())
        return errors

    def end(self):
        errors = self._drain_cleanups()
        self.case = None
        self.ready = False
        if self.moto is not None:
            proxy = getattr(self.moto, "_suite_proxy", None)
            if proxy is not None:
                try:
                    proxy.stop()
                except Exception:
                    errors.append(traceback.format_exc())
            try:
                self.moto.stop()
            except Exception:
                errors.append(traceback.format_exc())
            self.moto = None
        return {"ok": not errors, "error": "\n".join(errors)}


def serve(json_out):
    session = Session()

    def reply(payload):
        json_out.write(json.dumps(payload) + "\n")
        json_out.flush()

    for line in sys_stdin():
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError as exc:
            reply({"ok": False, "error": str(exc)})
            continue
        cmd = msg.get("cmd")
        try:
            if cmd == "begin":
                reply(session.begin(msg["class"], msg["method"], msg.get("mode", "bridge")))
            elif cmd == "exec":
                reply(
                    session.execute(
                        msg.get("source", ""),
                        msg.get("patches") or [],
                        msg.get("prelude") or "",
                    )
                )
            elif cmd == "end":
                reply(session.end())
                break
            else:
                reply({"ok": False, "error": f"unknown cmd {cmd}"})
        except Exception:
            reply({"ok": False, "error": traceback.format_exc()})


def sys_stdin():
    import sys

    return sys.stdin
