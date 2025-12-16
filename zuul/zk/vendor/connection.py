# This file is derived from the Kazoo project
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

from kazoo.exceptions import (
    AuthFailedError,
    EXCEPTIONS,
    NoNodeError,
)
from kazoo.loggingsupport import BLATHER
from kazoo.protocol.connection import (
    ConnectionHandler,
    CREATED_EVENT,
    DELETED_EVENT,
    CHANGED_EVENT,
    CHILD_EVENT,
    CLOSE_RESPONSE,
    WATCH_XID,
    PING_XID,
    AUTH_XID,
)
from kazoo.protocol.serialization import (
    Close,
    Exists,
    GetChildren,
    GetChildren2,
    Transaction,
    Watch,
)
from kazoo.protocol.states import (
    Callback,
    KeeperState,
    EVENT_TYPE_MAP,
)

from zuul.zk.vendor.states import (
    AddWatchMode,
    WatcherType,
    WatchedEvent,
)
from zuul.zk.vendor.serialization import (
    AddWatch,
    RemoveWatches,
)


class ZuulConnectionHandler(ConnectionHandler):

    def _find_persistent_recursive_watchers(self, path):
        parts = path.split("/")
        watchers = []
        for count in range(len(parts)):
            candidate = "/".join(parts[: count + 1])
            if not candidate:
                continue
            watchers.extend(
                self.client._persistent_recursive_watchers.get(candidate, [])
            )
        return watchers

    def _read_watch_event(self, buffer, offset, zxid):
        client = self.client
        watch, offset = Watch.deserialize(buffer, offset)
        path = watch.path

        self.logger.debug("Received EVENT: %s", watch)

        watchers = []

        if watch.type in (CREATED_EVENT, CHANGED_EVENT):
            watchers.extend(client._data_watchers.pop(path, []))
            watchers.extend(client._persistent_watchers.get(path, []))
            watchers.extend(self._find_persistent_recursive_watchers(path))
        elif watch.type == DELETED_EVENT:
            watchers.extend(client._data_watchers.pop(path, []))
            watchers.extend(client._child_watchers.pop(path, []))
            watchers.extend(client._persistent_watchers.get(path, []))
            watchers.extend(self._find_persistent_recursive_watchers(path))
        elif watch.type == CHILD_EVENT:
            watchers.extend(client._child_watchers.pop(path, []))
        else:
            self.logger.warning("Received unknown event %r", watch.type)
            return

        # Strip the chroot if needed
        path = client.unchroot(path)
        ev = WatchedEvent(EVENT_TYPE_MAP[watch.type], client._state, path,
                          zxid)

        # Last check to ignore watches if we've been stopped
        if client._stopped.is_set():
            return

        # Dump the watchers to the watch thread
        for watch in watchers:
            client.handler.dispatch_callback(Callback("watch", watch, (ev,)))

    def _read_response(self, header, buffer, offset):
        client = self.client
        request, async_object, xid = client._pending.popleft()
        if header.zxid and header.zxid > 0:
            client.last_zxid = header.zxid
        if header.xid != xid:
            exc = RuntimeError(
                "xids do not match, expected %r " "received %r",
                xid,
                header.xid,
            )
            async_object.set_exception(exc)
            raise exc

        # Determine if its an exists request and a no node error
        exists_error = (
            header.err == NoNodeError.code and request.type == Exists.type
        )

        # Set the exception if its not an exists error
        if header.err and not exists_error:
            callback_exception = EXCEPTIONS[header.err]()
            self.logger.debug(
                "Received error(xid=%s) %r", xid, callback_exception
            )
            if async_object:
                async_object.set_exception(callback_exception)
        elif request and async_object:
            if exists_error:
                # It's a NoNodeError, which is fine for an exists
                # request
                async_object.set(None)
            else:
                try:
                    response = request.deserialize(buffer, offset)
                except Exception as exc:
                    self.logger.exception(
                        "Exception raised during deserialization "
                        "of request: %s",
                        request,
                    )
                    async_object.set_exception(exc)
                    return
                self.logger.debug(
                    "Received response(xid=%s): %r", xid, response
                )

                # We special case a Transaction as we have to unchroot things
                if request.type == Transaction.type:
                    response = Transaction.unchroot(client, response)

                async_object.set(response)

            # Determine if watchers should be registered or unregistered
            if not client._stopped.is_set():
                watcher = getattr(request, "watcher", None)
                if watcher:
                    if isinstance(request, AddWatch):
                        if request.mode == AddWatchMode.PERSISTENT:
                            client._persistent_watchers[request.path].add(
                                watcher
                            )
                        elif request.mode == AddWatchMode.PERSISTENT_RECURSIVE:
                            client._persistent_recursive_watchers[
                                request.path
                            ].add(watcher)
                    elif isinstance(request, (GetChildren, GetChildren2)):
                        client._child_watchers[request.path].add(watcher)
                    else:
                        client._data_watchers[request.path].add(watcher)
                if isinstance(request, RemoveWatches):
                    if request.watcher_type == WatcherType.CHILDREN:
                        client._child_watchers.pop(request.path, None)
                    elif request.watcher_type == WatcherType.DATA:
                        client._data_watchers.pop(request.path, None)
                    elif request.watcher_type == WatcherType.ANY:
                        client._child_watchers.pop(request.path, None)
                        client._data_watchers.pop(request.path, None)
                        client._persistent_watchers.pop(request.path, None)
                        client._persistent_recursive_watchers.pop(
                            request.path, None
                        )

        if isinstance(request, Close):
            self.logger.log(BLATHER, "Read close response")
            return CLOSE_RESPONSE

    def _read_socket(self, read_timeout):
        """Called when there's something to read on the socket"""
        client = self.client

        header, buffer, offset = self._read_header(read_timeout)
        if header.xid == PING_XID:
            self.logger.log(BLATHER, "Received Ping")
            self.ping_outstanding.clear()
        elif header.xid == AUTH_XID:
            self.logger.log(BLATHER, "Received AUTH")

            request, async_object, xid = client._pending.popleft()
            if header.err:
                async_object.set_exception(AuthFailedError())
                client._session_callback(KeeperState.AUTH_FAILED)
            else:
                async_object.set(True)
        elif header.xid == WATCH_XID:
            self._read_watch_event(buffer, offset, header.zxid)
        else:
            self.logger.log(BLATHER, "Reading for header %r", header)

            return self._read_response(header, buffer, offset)
