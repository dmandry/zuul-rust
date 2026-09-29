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

import datetime
import logging
import math
import threading
import time

from zuul.zk.event_queues import EventReceiverElection


class GerritChecksPoller(threading.Thread):
    # Poll gerrit without stream-events
    log = logging.getLogger("zuul.GerritConnection.poller")
    poll_interval = 30

    def __init__(self, connection):
        threading.Thread.__init__(self)
        self.connection = connection
        self.last_merged_poll = 0
        self.poller_election = EventReceiverElection(
            connection.sched.zk_client, connection.connection_name, "poller")
        self._stopped = False
        self._stop_event = threading.Event()
        self.daemon = True

    def _makePendingCheckEvent(self, change, uuid, check):
        return {'type': 'pending-check',
                'uuid': uuid,
                'change': {
                    'project': change['patch_set']['repository'],
                    'number': change['patch_set']['change_number'],
                },
                'patchSet': {
                    'number': change['patch_set']['patch_set_id'],
                }}

    def _makeChangeMergedEvent(self, change):
        """Make a simulated change-merged event

        Mostly for the benefit of scheduler reconfiguration.
        """
        rev = change['revisions'][change['current_revision']]
        return {'type': 'change-merged',
                'change': {
                    'project': change['project'],
                    'number': change['_number'],
                },
                'patchSet': {
                    'number': rev['_number'],
                }}

    def _poll_checkers(self):
        for checker in self.connection.watched_checkers:
            changes = self.connection.get(
                'plugins/checks/checks.pending/?'
                'query=checker:%s+(state:NOT_STARTED)' % checker)
            for change in changes:
                for uuid, check in change['pending_checks'].items():
                    event = self._makePendingCheckEvent(
                        change, uuid, check)
                    self.connection.addEvent(event)

    def _poll_merged_changes(self):
        now = datetime.datetime.utcnow()
        age = self.last_merged_poll
        if age:
            # Allow an extra 4 seconds for request time
            age = int(math.ceil((now - age).total_seconds())) + 4
        changes = self.connection.simpleQueryHTTP(
            "status:merged -age:%ss" % (age,))
        self.last_merged_poll = now
        for change in changes:
            event = self._makeChangeMergedEvent(change)
            self.connection.addEvent(event)

    def _poll(self):
        next_start = self._last_start + self.poll_interval
        self._stop_event.wait(max(next_start - time.time(), 0))
        if self._stopped or not self.poller_election.is_still_valid():
            return
        self._last_start = time.time()
        self._poll_checkers()
        if self.connection.event_source == self.connection.EVENT_SOURCE_NONE:
            self._poll_merged_changes()

    def _run(self):
        self._last_start = time.time()
        while not self._stopped and self.poller_election.is_still_valid():
            # during tests, a sub-class _poll method is used to send
            # notifications
            self._poll()

    def run(self):
        while not self._stopped:
            try:
                self.poller_election.run(self._run)
            except Exception:
                self.log.exception("Exception on Gerrit poll with %s:",
                                   self.connection.connection_name)
                time.sleep(1)

    def stop(self):
        self.log.debug("Stopping watcher")
        self._stopped = True
        self._stop_event.set()
        self.poller_election.cancel()
