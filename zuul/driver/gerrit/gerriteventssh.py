# Copyright 2011 OpenStack, LLC.
# Copyright 2012 Hewlett-Packard Development Company, L.P.
# Copyright 2013 Acme Gating, LLC
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
import paramiko
import pprint
import select
import threading
import time

from zuul.zk.event_queues import EventReceiverElection


class GerritSSHEventListener(threading.Thread):
    log = logging.getLogger("zuul.GerritConnection.ssh")
    poll_timeout = 500

    def __init__(self, gerrit_connection, connection_config):
        threading.Thread.__init__(self)
        self.username = connection_config.get('user')
        self.keyfile = connection_config.get('sshkey', None)
        server = connection_config.get('server')
        self.hostname = connection_config.get('ssh_server', server)
        self.port = int(connection_config.get('port', 29418))
        self.gerrit_connection = gerrit_connection
        self._stop_event = threading.Event()
        self.watcher_election = EventReceiverElection(
            gerrit_connection.sched.zk_client,
            gerrit_connection.connection_name,
            "watcher")
        self.keepalive = int(connection_config.get('keepalive', 60))
        self._stopped = False
        self.daemon = True

    def _read(self, fd):
        while True:
            l = fd.readline()
            data = json.loads(l)
            self.log.debug("Received data from Gerrit event stream: \n%s" %
                           pprint.pformat(data))
            self.gerrit_connection.addEvent(data)
            # Continue until all the lines received are consumed
            if fd._pos == fd._realpos:
                break

    def _listen(self, stdout, stderr):
        poll = select.poll()
        poll.register(stdout.channel)
        while not self._stopped and self.watcher_election.is_still_valid():
            ret = poll.poll(self.poll_timeout)
            if not self.watcher_election.is_still_valid():
                return
            for (fd, event) in ret:
                if fd == stdout.channel.fileno():
                    if event == select.POLLIN:
                        self._read(stdout)
                    else:
                        raise Exception("event on ssh connection")

    def _run(self):
        try:
            client = paramiko.SSHClient()
            client.load_system_host_keys()
            client.set_missing_host_key_policy(paramiko.WarningPolicy())
            # SSH banner, handshake, and auth timeouts default to 15
            # seconds, so we only set the socket timeout here.
            client.connect(self.hostname,
                           username=self.username,
                           port=self.port,
                           key_filename=self.keyfile,
                           timeout=self.gerrit_connection.ssh_timeout)
            transport = client.get_transport()
            transport.set_keepalive(self.keepalive)

            # We don't set a timeout for this since it is never
            # expected to complete.  We rely on ssh and tcp keepalives
            # to detected a down connection.
            stdin, stdout, stderr = client.exec_command("gerrit stream-events")

            self._listen(stdout, stderr)

            if not stdout.channel.exit_status_ready():
                # The stream-event is still running but we are done polling
                # on stdout most likely due to being asked to stop.
                # Try to stop the stream-events command sending Ctrl-C
                stdin.write("\x03")
                time.sleep(.2)
                if not stdout.channel.exit_status_ready():
                    # we're still not ready to exit, lets force the channel
                    # closed now.
                    stdout.channel.close()
            ret = stdout.channel.recv_exit_status()
            self.log.debug("SSH exit status: %s" % ret)

            if ret and ret not in [-1, 130]:
                raise Exception("Gerrit error executing stream-events")
        finally:
            # If we don't close on exceptions to connect we can leak the
            # connection and DoS Gerrit.
            client.close()

    def run(self):
        while not self._stopped:
            try:
                self.watcher_election.run(self._run)
            except Exception:
                self.log.exception("Exception on ssh event stream with %s:",
                                   self.gerrit_connection.connection_name)
                self._stop_event.wait(5)

    def stop(self):
        self.log.debug("Stopping watcher")
        self._stopped = True
        self._stop_event.set()
        self.watcher_election.cancel()
