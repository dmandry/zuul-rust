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
import traceback

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


class _MotoProxy:
    """Forward to moto, and answer ListServiceQuotas.

    moto 5 does not implement service quotas. The Python fixture patches
    that call away. The native client hits the endpoint, so the proxy
    returns an empty quota page (limits stay unlimited, matching the
    fixture's math.inf patch) and caches instead of retrying a 500.
    """

    def __init__(self, upstream):
        self.upstream = upstream
        self.httpd = None

    def start(self):
        import socket
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

        upstream = self.upstream

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, fmt, *args):
                return

            def _read(self):
                length = int(self.headers.get("Content-Length", "0") or 0)
                return self.rfile.read(length) if length else b""

            def _empty_quotas(self):
                data = b'{"Quotas":[]}'
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


def _start_moto():
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
    proxy = _MotoProxy(upstream)
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


def _install_observer(launchers_path):
    import tests.base as base

    def observer_run(self):
        # tests/base.py waitUntilSettled compares layout uuids. The
        # observer copies the ZK layout and does not provision, so the
        # native launcher is the only component that takes work.
        with self._test_lock:
            try:
                store = self.tenant_layout_state
                for tenant_name in list(store):
                    state = store.get(tenant_name)
                    if state is not None:
                        self.local_layout_state[tenant_name] = state
            except Exception:
                self.log.exception("observer layout mirror failed")

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
        record = {
            "hostname": launcher.hostname,
            "connection_filter": (
                list(launcher.connection_filter)
                if launcher.connection_filter
                else None
            ),
        }
        with open(launchers_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")
            handle.flush()
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
                self.moto, moto_url = _start_moto()
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
