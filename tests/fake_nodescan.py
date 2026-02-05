# Copyright (C) 2023 Acme Gating, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import fixtures


class FakeSocket:
    def __init__(self, *args):
        self.blocking = True
        self.fd = 1

    def setblocking(self, b):
        self.blocking = b

    def getsockopt(self, level, optname):
        return None

    def connect(self, addr):
        if not self.blocking:
            raise BlockingIOError()
        raise Exception("blocking connect attempted")

    def fileno(self):
        return self.fd


class FakePoll:
    _INIT_FAIL = False

    def __init__(self):
        self.fds = []
        self._fail = self._INIT_FAIL

    def register(self, fd, bitmap):
        if isinstance(fd, FakeSocket):
            # This isn't perfect, but it's enough to avoid collisions
            # in tests.
            fd.fd = len(self.fds) + 1
            fd = fd.fd
        self.fds.append(fd)

    def unregister(self, fd):
        if fd in self.fds:
            self.fds.remove(fd)

    def poll(self, timeout=None):
        if self._fail:
            return []
        fds = self.fds[:]
        return [(f, 0) for f in fds]


class Dummy:
    pass


class FakeKey:
    def get_name(self):
        return 'fake key'

    def get_base64(self):
        return 'fake base64'


class FakeTransport:
    _INIT_ACTIVE = True
    _INIT_FAIL = False

    def __init__(self, *args, **kw):
        self.active = self._INIT_ACTIVE
        self._fail = self._INIT_FAIL

    def start_client(self, event=None, timeout=None):
        if not self._fail:
            event.set()

    def get_security_options(self):
        ret = Dummy()
        ret.key_types = ['rsa']
        return ret

    def get_remote_server_key(self):
        return FakeKey()

    def get_exception(self):
        return Exception("Fake ssh error")


class NodescanFixture(fixtures.Fixture):
    def __init__(self, transport_active=True, transport_fail=False,
                 poll_fail=False):
        super().__init__()
        self._transport_active = transport_active
        self._transport_fail = transport_fail
        self._poll_fail = poll_fail

    def _setUp(self):
        self.useFixture(fixtures.MonkeyPatch(
            'zuul.launcher.server.NodescanRequest._socket_class',
            FakeSocket))
        self.useFixture(fixtures.MonkeyPatch(
            'zuul.launcher.server.NodescanWorker._poll_class',
            FakePoll))
        self.useFixture(fixtures.MonkeyPatch(
            'paramiko.transport.Transport',
            FakeTransport))
        self.useFixture(fixtures.MonkeyPatch(
            'tests.fake_nodescan.FakeTransport._INIT_ACTIVE',
            self._transport_active))
        self.useFixture(fixtures.MonkeyPatch(
            'tests.fake_nodescan.FakeTransport._INIT_FAIL',
            self._transport_fail))
        self.useFixture(fixtures.MonkeyPatch(
            'tests.fake_nodescan.FakePoll._INIT_FAIL',
            self._poll_fail))
