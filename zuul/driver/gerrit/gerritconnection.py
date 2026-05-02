# Copyright 2011 OpenStack, LLC.
# Copyright 2012 Hewlett-Packard Development Company, L.P.
# Copyright 2023-2025 Acme Gating, LLC
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

import collections
import copy
import datetime
import enum
import hmac
import itertools
import json
import logging
import paramiko
import pprint
import re
import re2
import requests
import shlex
import threading
import time
import urllib
import urllib.parse
import weakref

import cherrypy

import cachetools
from typing import Dict, List
from uuid import uuid4

from opentelemetry import trace

from zuul import version as zuul_version
from zuul.connection import (
    BaseConnection,
    BaseThreadPoolEventConnector,
    ZKBranchCacheMixin,
    ZKChangeCacheMixin,
)
from zuul.driver.gerrit.auth import FormAuth
from zuul.driver.gerrit.gcloudauth import GCloudAuth
from zuul.driver.gerrit.gerritmodel import GerritChange, GerritTriggerEvent
from zuul.driver.gerrit.gerriteventssh import GerritSSHEventListener
from zuul.driver.gerrit.gerriteventchecks import GerritChecksPoller
from zuul.driver.gerrit.gerriteventkafka import GerritKafkaEventListener
from zuul.driver.gerrit.gerriteventawskinesis import (
    GerritAWSKinesisEventListener,
)
from zuul.driver.gerrit.gerriteventgcloudpubsub import (
    GerritGcloudPubsubEventListener,
)
from zuul.driver.git.gitwatcher import GitWatcher
from zuul.web.handler import BaseWebController
from zuul.lib import tracing
from zuul.lib.logutil import get_annotated_logger
from zuul.model import Ref, Tag, Branch, Project, FalseWithReason
from zuul.zk.branch_cache import BranchCache, BranchFlag, BranchInfo
from zuul.zk.change_cache import (
    AbstractChangeCache,
    ChangeKey,
    ConcurrentUpdateError,
)
from zuul.zk.event_queues import ConnectionEventQueue

# HTTP timeout in seconds
TIMEOUT = 30
# SSH connection timeout
SSH_TIMEOUT = TIMEOUT


class HTTPConflictException(Exception):
    message = "Received response 409"


class HTTPBadRequestException(Exception):
    pass


class HTTPNotFoundException(Exception):
    pass


class GerritEventProcessingException(Exception):
    pass


class GerritChangeCache(AbstractChangeCache):
    log = logging.getLogger("zuul.driver.GerritChangeCache")

    CHANGE_TYPE_MAP = {
        "Ref": Ref,
        "Tag": Tag,
        "Branch": Branch,
        "GerritChange": GerritChange,
    }


class ChangeNetworkFuture:
    """A set of changes related by dependencies"""
    def __init__(self, min_ltime):
        self.changes = set()
        self.event = threading.Event()
        self.query_results = {}
        # We will not accept cached data older than this time
        self.min_ltime = min_ltime

    def add(self, change_key):
        self.changes.add(change_key)

    def setComplete(self):
        self.event.set()

    def wait(self):
        self.event.wait()

    def hasNumber(self, number):
        for change_key in self.changes:
            if number == change_key.stable_id:
                return True
        return False

    def addQueryResult(self, number, data):
        existing = self.query_results.get(number)
        existing_time = existing and existing.zuul_query_ltime or 0
        if existing_time < data.zuul_query_ltime:
            self.query_results[number] = data

    def getQueryResult(self, number):
        data = self.query_results.get(number)
        return data

    def mergeQueryResults(self, future):
        for (number, data) in future.query_results.items():
            if number not in self.query_results:
                self.query_results[number] = data
            else:
                own_data = self.query_results[number]
                if own_data.zuul_query_ltime < data.zuul_query_ltime:
                    self.query_results[number] = data


class ChangeNetworkConflict(Exception):
    """Raised when two threads are found to be walking the same change graph.

    The associated ChangeNetworkFuture is the winner of the conflict
    and the caller should wait for it.

    """

    def __init__(self, future):
        self.future = future


class ChangeNetworkManager:
    """Prevent collisions between multiple threads querying changes"""

    def __init__(self):
        self.lock = threading.Lock()
        self.futures = []
        self.query_locks = weakref.WeakValueDictionary()

    def getQueryLock(self, number):
        # TODO: verify that WeakValueDictionary.setdefault is atomic;
        # if so, we can remove this internal locking call.
        with self.lock:
            return self.query_locks.setdefault(number, threading.Lock())

    def getQueryResult(self, number):
        with self.lock:
            for f in self.futures:
                if f.hasNumber(number):
                    data = f.getQueryResult(number)
                    if data:
                        return data

    def updateQueryResult(self, number, data):
        with self.lock:
            for f in self.futures:
                if f.hasNumber(number):
                    f.addQueryResult(number, data)

    def permissionToProceed(self, future):
        with self.lock:
            for f in self.futures:
                if f is future:
                    continue
                if future.changes.intersection(f.changes):
                    # The first thread to request permission for a
                    # change always succeeds, second or later threads
                    # connected to the same network always lose.
                    if future in self.futures:
                        self.futures.remove(future)
                    # Let the winner benefit from any queries the
                    # loser has performed.
                    f.mergeQueryResults(future)
                    raise ChangeNetworkConflict(f)
            # No conflicts
            if future not in self.futures:
                self.futures.append(future)
        return True

    def setComplete(self, future):
        if future is None:
            return
        with self.lock:
            try:
                self.futures.remove(future)
            except ValueError:
                pass
            future.setComplete()


class GerritChangeData(object):
    """Compatability layer for SSH/HTTP

    This class holds the raw data returned from a change query over
    SSH or HTTP.  Most of the work of parsing the data and storing it
    on the change is in the gerritmodel.GerritChange class, however
    this does perform a small amount of parsing of dependencies since
    they are handled outside of that class.  This provides an API to
    that data independent of the source.

    """

    SSH = 1
    HTTP = 2

    def __init__(self, fmt, data, related=None, files=None,
                 commentable_files=None, zuul_query_ltime=None):
        self.format = fmt
        self.data = data
        self.files = files
        self.commentable_files = commentable_files
        self.zuul_query_ltime = zuul_query_ltime
        self.submit_type = 'MERGE_IF_NECESSARY'

        if fmt == self.SSH:
            self.parseSSH(data)
        else:
            self.parseHTTP(data)
            if related:
                self.parseRelatedHTTP(data, related)

    def parseSSH(self, data):
        self.needed_by = []
        self.depends_on = None
        self.message = data['commitMessage']
        self.current_patchset = str(data['currentPatchSet']['number'])
        self.number = str(data['number'])
        self.id = data['id']

        if 'dependsOn' in data:
            parts = data['dependsOn'][0]['ref'].split('/')
            self.depends_on = (parts[3], parts[4])

        for needed in data.get('neededBy', []):
            parts = needed['ref'].split('/')
            self.needed_by.append((parts[3], parts[4]))

    def parseHTTP(self, data):
        rev = data['revisions'][data['current_revision']]
        self.message = rev['commit']['message']
        self.current_patchset = str(rev['_number'])
        self.number = str(data['_number'])
        self.id = data['change_id']

    def parseRelatedHTTP(self, data, related):
        self.needed_by = []
        self.depends_on = None
        current_rev = data['revisions'][data['current_revision']]
        self.submit_type = data.get('submit_type', self.submit_type)
        for change in related['changes']:
            # For cherry-pick only, allow the dependency to float to
            # the latest revision since Gerrit will not prohibit
            # merges with dependencies on older versions.  We also
            # want Zuul to test the latest patchset if possible, since
            # that is what will end up being merged.
            if self.submit_type == 'CHERRY_PICK':
                ps = change['_current_revision_number']
            else:
                ps = change['_revision_number']
            for parent in current_rev['commit']['parents']:
                if change['commit']['commit'] == parent['commit']:
                    self.depends_on = (change['_change_number'], ps)
                    break
            else:
                self.needed_by.append((change['_change_number'], ps))


class QueryHistory:
    class Query(enum.Enum):
        SEEN = 1  # Not a real query, just that we've seen the change
        CHANGE = 2  # The main change query

    def __init__(self):
        self.queries = collections.defaultdict(lambda: dict())

    def getByKey(self, query, change_key):
        if not isinstance(change_key, ChangeKey):
            raise Exception("Must supply a ChangeKey")
        key = (change_key.stable_id, change_key.revision)
        return self.queries[query].get(key)

    def add(self, query, change):
        if not isinstance(change, GerritChange):
            raise Exception("Must supply a GerritChange")
        key = (change.number, change.patchset)
        self.queries[query][key] = change


class PeekQueue:
    def __init__(self, handler, replication_delay):
        self.queue = collections.deque()
        self.handler = handler
        self.change_merged_cache = cachetools.LRUCache(128)
        # If we see events whose timestamp is this long since an event
        # we're waiting for, give up the wait.  This is an extra 10
        # seconds beyond the replication delay.
        self.timeout = replication_delay + 10

    def append(self, event):
        self.queue.append(event)

    def run(self, cutoff=None):
        # If cutoff is supplied, we can treat that as the "current"
        # time and give up on waiting for any events whose timeout is
        # before that time.
        if not self.queue:
            return

        # Attempt to match ref-updated events with change-merged
        # events.
        ref_updates = {}
        new_event_list = collections.deque()
        latest_time = 0
        for event in self.queue:
            data = event["payload"]
            kind = data.get('type')
            refupdate = data.get('refUpdate', {})
            ref = refupdate.get('refName')
            latest_time = max(event["timestamp"], latest_time)
            inserted = False
            if (kind == 'ref-updated' and
                ((not ref.startswith('refs/')) or
                 ref.startswith('refs/heads/'))):
                # This is a ref-updated event for a branch, we
                # want to find its change-merged event.
                newrev = refupdate.get('newRev')
                if newrev in self.change_merged_cache:
                    # As of writing, this should not happen, but if it
                    # does it means gerrit has reversed the order of
                    # events!
                    del self.change_merged_cache[newrev]
                else:
                    ref_updates[newrev] = event
            elif kind == 'change-merged':
                newrev = data.get('newRev')
                if newrev in ref_updates:
                    # This is a change-merged event that matches a
                    # ref-updated event we're interested in.
                    other_event = ref_updates.pop(newrev)
                    idx = new_event_list.index(other_event)
                    # Put our event immediately before the ref-updated event
                    new_event_list.insert(idx, event)
                    # Give both events the same earlier ltime so that
                    # we don't have ltime going backwards.
                    event.zuul_event_ltime = other_event.zuul_event_ltime
                    inserted = True
                else:
                    self.change_merged_cache[newrev] = True
            if not inserted:
                new_event_list.append(event)

        while new_event_list:
            event = new_event_list.popleft()
            data = event["payload"]
            kind = data.get('type')
            ok = False
            if kind == 'ref-updated':
                refupdate = data.get('refUpdate')
                newrev = refupdate.get('newRev')
                if newrev in ref_updates:
                    # We're waiting on data for this one
                    if (cutoff and
                        (cutoff - event["timestamp"]) >= self.timeout):
                        # It's been more than 10 seconds plus the
                        # replication delay since we saw the
                        # ref-updated event, and we're at the end of
                        # the list of events in zk, so it's probably
                        # not going to show up.  Release it.
                        ok = True
                    elif latest_time - event["timestamp"] >= self.timeout:
                        # We've seen other events emmitted later 10
                        # seconds since the event, so it may be
                        # missing at this point; release it.
                        ok = True
                    # Otherwise, we're still waiting
                else:
                    # Not a branch ref-update
                    ok = True
            else:
                # Not a ref-update at all
                ok = True
            if not ok:
                # if we're still waiting for an event, don't send
                # any more so that we preserve the order.
                return

            self.queue.remove(event)
            self.handler(event)

    def getDelay(self):
        """Return a delay time that is later than the expiration time
        of the next event.

        In other words: how long to sleep before polling the peek
        queue again, assuming no new events.

        """
        if not self.queue:
            return None
        event = self.queue[0]
        now = time.time()
        return max((event["timestamp"] + self.timeout) - now, 0.0)


class GerritEventConnector(BaseThreadPoolEventConnector):
    """Move events from Gerrit to the scheduler."""

    IGNORED_EVENTS = (
        'cache-eviction',  # evict-cache plugin
        'fetch-ref-replicated',
        'fetch-ref-replication-scheduled',
        'ref-replicated',
        'ref-replication-scheduled',
        'ref-replication-done'
    )

    log = logging.getLogger("zuul.GerritEventConnector")

    def __init__(self, connection):
        super().__init__(connection)
        self._peek_queue = PeekQueue(self._peekQueueHandler,
                                     self.connection.replication_delay)

    def _getEventProcessor(self, event):
        return GerritEventProcessor(self, event).run

    def _calculateDelay(self, connection_event):
        timestamp = connection_event["timestamp"]
        now = time.time()
        delay = max((timestamp + self.connection.replication_delay) - now, 0.0)
        # Gerrit can produce inconsistent data immediately after an
        # event, So ensure that we do not deliver the event to Zuul
        # until at least a certain amount of time has passed.  Note
        # that if we receive several events in succession, we will
        # only need to delay for the first event.  In essence, Zuul
        # should always be a constant number of seconds behind Gerrit.

        self.log.debug("Handling event received %ss ago, delaying %ss",
                       now - timestamp, delay)
        return delay

    def _dispatchEvents(self):
        # This is the first half of the event dispatcher.  It reads
        # events from the webhook event queue and passes them to a
        # concurrent executor for pre-processing.

        # This overrides the superclass in order to add the peek queue.
        try:
            peek_ack_refs = [r.ack_ref for r in self._peek_queue.queue]
            event_id_offset = max(
                self.event_queue.eventIdFromAckRef(r)
                for r in list(self._events_in_progress) +
                peek_ack_refs)
        except ValueError:
            event_id_offset = None

        for event in self.event_queue.iter(event_id_offset):
            if self._shouldStop():
                break

            delay = self._calculateDelay(event)
            if delay:
                return delay

            self._peek_queue.append(event)
            self._peek_queue.run()
        # Once there are no more ZK events, run the queue a final time
        # and allow it to timeout based on the current time.
        self._peek_queue.run(cutoff=time.time())
        return self._peek_queue.getDelay()

    def _peekQueueHandler(self, event):
        # Called when the peek queue has decided an event should be processed
        processor = self._getEventProcessor(event)
        future = self._thread_pool.submit(processor)

        # Events are acknowledged in the event forwarder
        # loop after pre-processing. This way we can
        # ensure that no events are lost.
        self._events_in_progress.add(event.ack_ref)
        self._event_forward_queue.append(future)


class GerritEventProcessor:
    tracer = trace.get_tracer("zuul")

    def __init__(self, connector, connection_event):
        self.connector = connector
        self.connection = connector.connection
        self.zk_client = self.connection.sched.zk_client
        self.connection_event = connection_event
        self.event_span = tracing.restoreSpanContext(
            self.connection_event.get("span_context"))
        logger = logging.getLogger("zuul.GerritEventProcessor")
        self.zuul_event_id = connection_event["zuul_event_id"]
        self.log = get_annotated_logger(logger, self.zuul_event_id)
        self.events = []

    def run(self):
        if self.connector._stopped:
            return

        try:
            attributes = {"rel": "GerritEvent"}
            link = trace.Link(self.event_span.get_span_context(),
                              attributes=attributes)
            with self.tracer.start_as_current_span(
                    "GerritEventProcessing", links=[link]):
                self.events = self._handleEvent(self.connection_event)
        except GerritEventProcessingException as e:
            self.log.warning("Skipping event due to %s", e)
        except Exception:
            self.log.exception("Skipping event due to:")
        event_type = self.connection_event["payload"].get('type')
        return (self.events, self.connection_event, self.zuul_event_id,
                event_type)

    def _handleEvent(self, connection_event):
        timestamp = connection_event["timestamp"]
        data = connection_event["payload"]
        event = GerritTriggerEvent.fromGerritEventDict(
            data, timestamp, self.connection, self.zuul_event_id)
        min_change_ltime = self.zk_client.getCurrentLtime()

        # In order to perform connection hygene actions like those
        # below, the preFilter method must pass relevant events
        # through to get to this point.
        if event.default_branch_changed:
            self.log.debug('Updating default branch for %s to %s',
                           event.project_name, event.branch)
            self.connection._branch_cache.setProjectDefaultBranch(
                event.project_name, event.branch)
        if event._accountfield_unknown:
            self.log.warning("Received unrecognized event type '%s' "
                             "from Gerrit. Can not get account information." %
                             (event.type,))
        if event._branch_ref_update:
            self.connection.clearConnectionCacheOnBranchEvent(event)

        change = self._getChange(event, min_change_ltime)
        if (change and change.patchset and
            event.change_number and event.patch_number is None):
            event.patch_number = str(change.patchset)

        return [event]

    def _getChange(self, event, min_change_ltime):
        # Grab the change if we are managing the project or if it exists in the
        # cache as it may be a dependency
        change = None
        if event.change_number:
            refresh = True
            change_key = self.connection.source.getChangeKey(event)
            change = self.connection._change_cache.get(change_key)
            if change is None:
                refresh = False
                for tenant in self.connection.sched.abide.tenants.values():
                    # TODO(fungi): it would be better to have some simple means
                    # of inferring the hostname from the connection, or at
                    # least split this into separate method arguments, rather
                    # than assembling and passing in a baked string.
                    if (None, None) != tenant.getProject('/'.join((
                            self.connection.canonical_hostname,
                            event.project_name))):
                        refresh = True
                        break
            else:
                # We have a cache entry for this change Get the
                # query ltime for the cache entry; if it's after the
                # event ltime, we don't need to refresh.
                if (change.zuul_query_ltime and
                    change.zuul_query_ltime > min_change_ltime):
                    refresh = False

            if refresh:
                # Call _getChange for the side effect of updating the
                # cache.  Note that this modifies Change objects outside
                # the main thread.
                # NOTE(jhesketh): Ideally we'd just remove the change from the
                # cache to denote that it needs updating. However the change
                # object is already used by Items and hence BuildSets etc. and
                # we need to update those objects by reference so that they
                # have the correct/new information and also avoid hitting
                # gerrit multiple times.
                change = self.connection._getChange(
                    change_key, refresh=True, event=event,
                    allow_key_update=True, change=change,
                    update_if_older_than=min_change_ltime)
        return change


class GerritWebController(BaseWebController):
    log = logging.getLogger("zuul.GerritWebController")
    tracer = trace.get_tracer("zuul")

    # Reject bodies larger than 1 MiB.  Real Gerrit webhook payloads are
    # well under 100 KiB; this is purely a guard against abuse.
    MAX_BODY_BYTES = 1024 * 1024

    def __init__(self, zuul_web, connection):
        self.connection = connection
        self.zuul_web = zuul_web
        self.token = self.connection.connection_config.get('webhook_token')
        self.event_queue = ConnectionEventQueue(
            self.zuul_web.zk_client,
            self.connection.connection_name,
            None,
        )

    @staticmethod
    def _shouldDrop(data):
        # Match the noise-suppression done by GerritConnection.addEvent
        # for the SSH/Kafka/etc. paths.
        if data.get('type') in GerritEventConnector.IGNORED_EVENTS:
            return True
        if data.get('type') == 'ref-updated':
            refname = data.get('refUpdate', {}).get('refName', '')
            if (refname.startswith('refs/changes/') and
                    refname.endswith('/meta')):
                return True
        return False

    def _validateToken(self, headers, token_param):
        if not self.token:
            return
        provided = token_param or headers.get('x-gerrit-token')
        if not provided:
            raise cherrypy.HTTPError(
                401,
                'Token required: provide ?token=... or X-Gerrit-Token header.')
        if not hmac.compare_digest(self.token, provided):
            raise cherrypy.HTTPError(401, 'Invalid token.')

    @cherrypy.expose
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    def payload(self, token=None):
        headers = {k.lower(): v for k, v in cherrypy.request.headers.items()}
        self._validateToken(headers, token)

        content_length = headers.get('content-length')
        if content_length is not None:
            try:
                if int(content_length) > self.MAX_BODY_BYTES:
                    raise cherrypy.HTTPError(413, 'Request body too large.')
            except ValueError:
                raise cherrypy.HTTPError(400, 'Invalid Content-Length.')

        body = cherrypy.request.body.read(self.MAX_BODY_BYTES + 1)
        if len(body) > self.MAX_BODY_BYTES:
            raise cherrypy.HTTPError(413, 'Request body too large.')

        with self.tracer.start_as_current_span("GerritEvent") as span:
            try:
                json_body = json.loads(body.decode('utf-8'))
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                raise cherrypy.HTTPError(400, 'Invalid JSON: %s' % e)
            if self._shouldDrop(json_body):
                return {'ok': True}
            event = {
                "timestamp": time.time(),
                "zuul_event_id": uuid4().hex,
                "span_context": tracing.getSpanContext(span),
                "payload": json_body,
            }
            self.event_queue.put(event)
        return {'ok': True}


class GerritConnection(ZKChangeCacheMixin, ZKBranchCacheMixin, BaseConnection):
    driver_name = 'gerrit'
    log = logging.getLogger("zuul.GerritConnection")
    tracer = trace.get_tracer("zuul")
    iolog = logging.getLogger("zuul.GerritConnection.io")
    depends_on_re = re.compile(r"^Depends-On: (I[0-9a-f]{40})\s*$",
                               re.MULTILINE | re.IGNORECASE)
    refname_bad_sequences = re2.compile(
        r"[ \\*\[?:^~\x00-\x1F\x7F]|"  # Forbidden characters
        r"@{|\.\.|\.$|^@$|/$|^/|//+")  # everything else we can check with re2
    is_merged_replication_timeout = 300
    replication_retry_interval = 5
    _poller_class = GerritChecksPoller
    _ref_watcher_class = GitWatcher
    ref_watcher_poll_interval = 60
    submit_retry_backoff = 10

    EVENT_SOURCE_NONE = 'none'
    EVENT_SOURCE_STREAM_EVENTS = 'stream-events'
    EVENT_SOURCE_KAFKA = 'kafka'
    EVENT_SOURCE_KINESIS = 'kinesis'
    EVENT_SOURCE_GCLOUD_PUBSUB = 'gcloudpubsub'
    EVENT_SOURCE_WEBHOOK = 'webhook'

    def __init__(self, driver, connection_name, connection_config):
        super(GerritConnection, self).__init__(driver, connection_name,
                                               connection_config)
        if 'server' not in self.connection_config:
            raise Exception('server is required for gerrit connections in '
                            '%s' % self.connection_name)
        if 'user' not in self.connection_config:
            raise Exception('user is required for gerrit connections in '
                            '%s' % self.connection_name)

        self.user = self.connection_config.get('user')
        self.server = self.connection_config.get('server')
        self.ssh_server = self.connection_config.get('ssh_server', self.server)
        self.canonical_hostname = self.connection_config.get(
            'canonical_hostname', self.server)
        self.port = int(self.connection_config.get('port', 29418))
        self.keyfile = self.connection_config.get('sshkey', None)
        self.keepalive = int(self.connection_config.get('keepalive', 60))
        self.max_dependencies = self.connection_config.get(
            'max_dependencies', None)
        if self.max_dependencies is not None:
            self.max_dependencies = int(self.max_dependencies)
        self.event_source = self.EVENT_SOURCE_NONE
        # TODO(corvus): Document this when the checks api is stable;
        # it's not useful without it.
        enable_stream_events = self.connection_config.get(
            'stream_events', True)
        if enable_stream_events in [
                'true', 'True', '1', 1, 'TRUE', True]:
            self.event_source = self.EVENT_SOURCE_STREAM_EVENTS
        if self.connection_config.get('kafka_bootstrap_servers', None):
            self.event_source = self.EVENT_SOURCE_KAFKA
        elif self.connection_config.get('aws_kinesis_region', None):
            self.event_source = self.EVENT_SOURCE_KINESIS
        elif self.connection_config.get('gcloud_pubsub_project', None):
            self.event_source = self.EVENT_SOURCE_GCLOUD_PUBSUB
        elif self.connection_config.get('event_source', None) == 'webhook':
            self.event_source = self.EVENT_SOURCE_WEBHOOK
        self.replication_delay = int(self.connection_config.get(
            'replication_delay', 0))

        # Thread for whatever event source we use
        self.event_thread = None
        # Next two are only used by checks plugin
        self.poller_thread = None
        self.ref_watcher_thread = None
        self.client = None
        self.watched_checkers = []
        self.project_checker_map = {}
        self.watched_event_filters = []
        self.watched_event_filters_by_tenant = {}
        self.watched_event_filters_lock = threading.Lock()
        self.version = (0, 0, 0)
        self.submit_whole_topic = None
        self.ssh_timeout = SSH_TIMEOUT

        self.baseurl = self.connection_config.get(
            'baseurl', 'https://%s' % self.server).rstrip('/')
        default_gitweb_url_template = '{baseurl}/gitweb?' \
                                      'p={project.name}.git;' \
                                      'a=commitdiff;h={sha}'
        url_template = self.connection_config.get('gitweb_url_template',
                                                  default_gitweb_url_template)
        self.gitweb_url_template = url_template

        self.change_network_manager = ChangeNetworkManager()
        self.projects = {}
        self.gerrit_event_connector = None
        self.source = driver.getSource(self)

        self.session = None
        self.password = self.connection_config.get('password', None)
        self.git_over_ssh = self.connection_config.get('git_over_ssh', False)
        self.auth_type = self.connection_config.get('auth_type', None)
        self.anonymous_git = False
        if self.password or self.auth_type == 'gcloud_service':
            self.verify_ssl = self.connection_config.get('verify_ssl', True)
            if self.verify_ssl not in ['true', 'True', '1', 1, 'TRUE']:
                self.verify_ssl = False
            self.user_agent = 'Zuul/%s %s' % (
                zuul_version.release_string,
                requests.utils.default_user_agent())
            self.session = requests.Session()
            if self.auth_type == 'digest':
                authclass = requests.auth.HTTPDigestAuth
            elif self.auth_type == 'form':
                authclass = FormAuth
            elif self.auth_type == 'gcloud_service':
                authclass = GCloudAuth
                # The executors in google cloud may not have access
                # to the gerrit account credentials, so just use
                # anonymous http access for git
                self.anonymous_git = True
            else:
                authclass = requests.auth.HTTPBasicAuth
            self.auth = authclass(self.user, self.password)

    def setWatchedCheckers(self, checkers_to_watch):
        # TODO: This is not safe for multiple tenants
        # TODO: This does not remove checkers when tenants are deleted
        self.log.debug("Setting watched checkers to %s", checkers_to_watch)
        self.watched_checkers = set()
        self.project_checker_map = {}
        schemes_to_watch = set()
        uuids_to_watch = set()
        for x in checkers_to_watch:
            if 'scheme' in x:
                schemes_to_watch.add(x['scheme'])
            if 'uuid' in x:
                uuids_to_watch.add(x['uuid'])
        if schemes_to_watch:
            # get a list of all configured checkers
            try:
                configured_checkers = self.get('plugins/checks/checkers/')
            except Exception:
                self.log.exception("Unable to get checkers")
                configured_checkers = []

            # filter it through scheme matches in checkers_to_watch
            for checker in configured_checkers:
                if checker['status'] != 'ENABLED':
                    continue
                checker_scheme, checker_id = checker['uuid'].split(':')
                repo = checker['repository']
                repo = self.canonical_hostname + '/' + repo
                # map scheme matches to project names
                if checker_scheme in schemes_to_watch:
                    repo_checkers = self.project_checker_map.setdefault(
                        repo, set())
                    repo_checkers.add(checker['uuid'])
                    self.watched_checkers.add(checker['uuid'])
        # add uuids from checkers_to_watch
        for x in uuids_to_watch:
            self.watched_checkers.add(x)

    def setWatchedEventFilters(self, tenant_name, filters):
        # TODO: This does not remove filters when tenants are deleted
        self.log.debug("Setting watched event filters for %s to %s",
                       tenant_name, filters)
        with self.watched_event_filters_lock:
            self.watched_event_filters_by_tenant[tenant_name] = filters
            new_filters = set()
            # Build a set of unique filters across all tenants
            for tenant_filters in \
                self.watched_event_filters_by_tenant.values():
                new_filters.update(tenant_filters)
            self.log.debug("Setting watched event filters to %s",
                           new_filters)
            self.watched_event_filters = new_filters

    def toDict(self):
        d = super().toDict()
        d.update({
            "baseurl": self.baseurl,
            "canonical_hostname": self.canonical_hostname,
            "server": self.server,
            "ssh_server": self.ssh_server,
            "port": self.port,
        })
        return d

    def url(self, path):
        return self.baseurl + '/a/' + path

    def get(self, path):
        url = self.url(path)
        self.log.debug('GET: %s' % (url,))
        r = self.session.get(
            url,
            verify=self.verify_ssl,
            auth=self.auth, timeout=TIMEOUT,
            headers={'User-Agent': self.user_agent})
        self.iolog.debug('Received: %s %s' % (r.status_code, r.text,))
        if r.status_code == 409:
            raise HTTPConflictException()
        elif r.status_code == 404:
            raise HTTPNotFoundException()
        elif r.status_code != 200:
            raise Exception("Received response %s" % (r.status_code,))
        ret = None
        if r.text and len(r.text) > 4:
            try:
                ret = json.loads(r.text[4:])
            except Exception:
                self.log.exception(
                    "Unable to parse result %s from post to %s" %
                    (r.text, url))
                raise
        return ret

    def post(self, path, data):
        url = self.url(path)
        self.log.debug('POST: %s' % (url,))
        self.log.debug('data: %s' % (data,))
        r = self.session.post(
            url, data=json.dumps(data).encode('utf8'),
            verify=self.verify_ssl,
            auth=self.auth, timeout=TIMEOUT,
            headers={'Content-Type': 'application/json;charset=UTF-8',
                     'User-Agent': self.user_agent})
        self.iolog.debug('Received: %s %s' % (r.status_code, r.text,))
        if r.status_code == 409:
            raise HTTPConflictException()
        if r.status_code == 400:
            raise HTTPBadRequestException('Received response 400: %s' % r.text)
        elif r.status_code != 200:
            raise Exception("Received response %s: %s" % (
                r.status_code, r.text))
        ret = None
        if r.text and len(r.text) > 4:
            try:
                ret = json.loads(r.text[4:])
            except Exception:
                self.log.exception(
                    "Unable to parse result %s from post to %s" %
                    (r.text, url))
                raise
        return ret

    def getProject(self, name: str) -> Project:
        return self.projects.get(name)

    def addProject(self, project: Project) -> None:
        self.projects[project.name] = project

    def getChange(self, change_key, refresh=False, event=None):
        if change_key.connection_name != self.connection_name:
            return None
        if change_key.change_type == 'GerritChange':
            return self._getChange(change_key, refresh=refresh, event=event)
        elif change_key.change_type == 'Tag':
            return self._getTag(change_key, refresh=refresh, event=event)
        elif change_key.change_type == 'Branch':
            return self._getBranch(change_key, refresh=refresh, event=event)
        elif change_key.change_type == 'Ref':
            return self._getRef(change_key, refresh=refresh, event=event)

    def _checkMaxDependencies(self, change, history):
        if change and history:
            history.add(history.Query.SEEN, change)
            if (self.max_dependencies is not None and
                len(history.queries[history.Query.SEEN]) >
                self.max_dependencies):
                raise GerritEventProcessingException(
                    f"Change {change} has too many dependencies")

    def _getChange(self, change_key, refresh=False, history=None,
                   network_future=None, event=None,
                   allow_key_update=False, change=None,
                   update_if_older_than=None):
        # Ensure number and patchset are str
        if change is None:
            # We may be called from the event handler which has
            # already gotten a change from the cache but thinks it may
            # need to be updated.
            change = self._change_cache.get(change_key)
        self._checkMaxDependencies(change, history)
        if change and not refresh:
            return change
        if not change:
            change = GerritChange(None)
            change.number = change_key.stable_id
            change.patchset = change_key.revision
            self._checkMaxDependencies(change, history)
        network_start = network_future is None
        while True:
            if network_future is None:
                network_future = ChangeNetworkFuture(update_if_older_than)
            network_future.add(change_key)
            try:
                self.change_network_manager.permissionToProceed(network_future)
                return self._updateChange(change_key, change, event, history,
                                          network_future, allow_key_update)
            except ChangeNetworkConflict as e:
                if network_start:
                    # This is the top of the stack.  We wait for the
                    # future and try again.
                    e.future.wait()
                    self.change_network_manager.setComplete(network_future)
                    # Start a new future but copy over the query cache
                    # results from the winner.  In case we decide that
                    # the winning network did not update our change(s)
                    # recently enough, we may still be able to benefit
                    # from some of the previously run queries.
                    network_future = ChangeNetworkFuture(update_if_older_than)
                    network_future.query_results = e.future.query_results
                    change = self._change_cache.get(change_key) or change
                    continue
                else:
                    raise
            finally:
                if network_start:
                    self.change_network_manager.setComplete(network_future)

    def _getTag(self, change_key, refresh=False, event=None):
        tag = change_key.stable_id
        change = self._change_cache.get(change_key)
        if change:
            if refresh:
                self._change_cache.updateChangeWithRetry(
                    change_key, change, lambda c: None)
            return change
        project = self.source.getProject(change_key.project_name)
        change = Tag(project)
        change.tag = tag
        change.ref = f'refs/tags/{tag}'
        change.oldrev = change_key.oldrev
        change.newrev = change_key.newrev
        change.url = self._getWebUrl(project, sha=change.newrev)
        try:
            self._change_cache.set(change_key, change)
        except ConcurrentUpdateError:
            change = self._change_cache.get(change_key)
        return change

    def _getBranch(self, change_key, refresh=False, event=None):
        branch = change_key.stable_id
        change = self._change_cache.get(change_key)
        if change:
            if refresh:
                self._change_cache.updateChangeWithRetry(
                    change_key, change, lambda c: None)
            return change
        project = self.source.getProject(change_key.project_name)
        change = Branch(project)
        change.branch = branch
        change.ref = f'refs/heads/{branch}'
        change.oldrev = change_key.oldrev
        change.newrev = change_key.newrev
        change.url = self._getWebUrl(project, sha=change.newrev)
        try:
            self._change_cache.set(change_key, change)
        except ConcurrentUpdateError:
            change = self._change_cache.get(change_key)
        return change

    def _getRef(self, change_key, refresh=False, event=None):
        change = self._change_cache.get(change_key)
        if change:
            if refresh:
                self._change_cache.updateChangeWithRetry(
                    change_key, change, lambda c: None)
            return change
        project = self.source.getProject(change_key.project_name)
        change = Ref(project)
        change.ref = change_key.stable_id
        change.oldrev = change_key.oldrev
        change.newrev = change_key.newrev
        change.url = self._getWebUrl(project, sha=change.newrev)
        try:
            self._change_cache.set(change_key, change)
        except ConcurrentUpdateError:
            change = self._change_cache.get(change_key)
        return change

    def _getDependsOnFromCommit(self, message, change, event):
        log = get_annotated_logger(self.log, event)
        records = []
        seen = set()
        for match in self.depends_on_re.findall(message):
            if match in seen:
                log.debug("Ignoring duplicate Depends-On: %s", match)
                continue
            seen.add(match)
            query = "change:%s" % (match,)
            log.debug("Updating %s: Running query %s to find needed changes",
                      change, query)
            records.extend(self.simpleQuery(query, event=event))
        return [(x.number, x.current_patchset) for x in records]

    def _getNeededByFromCommit(self, change_id, change, event):
        log = get_annotated_logger(self.log, event)
        records = []
        seen = set()
        query = 'message:{%s}' % change_id
        log.debug("Updating %s: Running query %s to find changes needed-by",
                  change, query)
        results = self.simpleQuery(query, event=event)
        for result in results:
            for match in self.depends_on_re.findall(
                    result.message):
                if match != change_id:
                    continue
                # Note: This is not a ChangeCache ChangeKey
                key = (result.number, result.current_patchset)
                if key in seen:
                    continue
                log.debug("Updating %s: Found change %s,%s "
                          "needs %s from commit",
                          change, key[0], key[1], change_id)
                seen.add(key)
                records.append(result)
        return [(x.number, x.current_patchset) for x in records]

    def _updateChange(self, key, change, event, history, network_future,
                      allow_key_update=False):
        log = get_annotated_logger(self.log, event)

        # In case this change is already in the history we have a
        # cyclic dependency and don't need to update ourselves again
        # as this gets done in a previous frame of the call stack.
        # NOTE(jeblair): The only case where this can still be hit is
        # when we get an event for a change with no associated
        # patchset; for instance, when the gerrit topic is changed.
        # In that case, we will update change 1234,None, which will be
        # inserted into the cache as its own entry, but then we will
        # resolve the patchset before adding it to the history list,
        # then if there are dependencies, we can walk down and then
        # back up to the version of this change with a patchset which
        # will match the history list but will have bypassed the
        # change cache because the previous object had a patchset of
        # None.  All paths hit the change cache first.  To be able to
        # drop history, we need to resolve the patchset on events with
        # no patchsets before adding the entry to the change cache.
        if history and change.number and change.patchset:
            history_change = history.getByKey(history.Query.CHANGE, key)
            if history_change:
                log.debug("Change %s is in history", change)
                return history_change

        if (network_future.min_ltime and
            change.zuul_query_ltime and
            change.zuul_query_ltime > network_future.min_ltime):
            # The change was updated in another thread while we were
            # processing this change network.
            log.debug("Change %s is up to date", change)
            return change

        log.info("Updating %s", change)
        data = self.queryChange(change.number, event=event,
                                min_ltime=network_future.min_ltime)
        # Do a local update without updating the cache so that we can
        # reference this change when we recurse for dependencies.
        change.update(data, {}, self)

        # Get the dependencies for this change, and recursively update
        # dependent changes (recursively calling this method).
        if change.open:
            extra = self._updateChangeDependencies(
                log, key, change, data, event, history, network_future)
        else:
            extra = {}

        # Actually update this change in the change cache.
        def _update_change(c):
            return c.update(data, extra, self)

        change = self._change_cache.updateChangeWithRetry(
            key, change, _update_change, allow_key_update=allow_key_update)

        return change

    def _updateChangeDependencies(self, log, key, change, data, event,
                                  history, network_future):
        if history is None:
            history = QueryHistory()
        history.add(history.Query.CHANGE, change)

        needs_changes = set()
        git_needs_changes = []
        if data.depends_on is not None:
            dep_num, dep_ps = data.depends_on
            log.debug("Updating %s: Getting git-dependent change %s,%s",
                      change, dep_num, dep_ps)
            dep_key = ChangeKey(self.connection_name, None,
                                'GerritChange', str(dep_num), str(dep_ps))
            dep = self._getChange(dep_key, history=history,
                                  network_future=network_future,
                                  event=event)
            # This is a git commit dependency. So we only ignore it if it is
            # already merged. So even if it is "ABANDONED", we should not
            # ignore it.

            if (not dep.is_merged) and dep not in needs_changes:
                git_needs_changes.append(dep_key.reference)
                needs_changes.add(dep_key.reference)

        compat_needs_changes = []
        for (dep_num, dep_ps) in self._getDependsOnFromCommit(
                data.message, change, event):
            log.debug("Updating %s: Getting commit-dependent "
                      "change %s,%s", change, dep_num, dep_ps)
            dep_key = ChangeKey(self.connection_name, None,
                                'GerritChange', str(dep_num), str(dep_ps))
            dep = self._getChange(dep_key, history=history,
                                  network_future=network_future,
                                  event=event)
            if dep.open and dep not in needs_changes:
                compat_needs_changes.append(dep_key.reference)
                needs_changes.add(dep_key.reference)

        needed_by_changes = set()
        git_needed_by_changes = []
        for (dep_num, dep_ps) in data.needed_by:
            try:
                log.debug("Updating %s: Getting git-needed change %s,%s",
                          change, dep_num, dep_ps)
                dep_key = ChangeKey(self.connection_name, None,
                                    'GerritChange', str(dep_num), str(dep_ps))
                dep = self._getChange(dep_key, history=history,
                                      network_future=network_future,
                                      event=event)
                if (dep.open and dep.is_current_patchset and
                    dep not in needed_by_changes):
                    git_needed_by_changes.append(dep_key.reference)
                    needed_by_changes.add(dep_key.reference)
            except ChangeNetworkConflict:
                raise
            # We ignore GerritEventProcessingExceptions (e.g. when exceeding
            # max_dependencies) here because they are not important on the
            # "needed_by" side of the dependency chain. Those are already
            # checked for on the "needs" side.
            except Exception:
                log.exception("Failed to get git-needed change %s,%s",
                              dep_num, dep_ps)

        compat_needed_by_changes = []
        for (dep_num, dep_ps) in self._getNeededByFromCommit(
                data.id, change, event):
            try:
                log.debug("Updating %s: Getting commit-needed change %s,%s",
                          change, dep_num, dep_ps)
                # Because a commit needed-by may be a cross-repo
                # dependency, cause that change to refresh so that it will
                # reference the latest patchset of its Depends-On (this
                # change). In case the dep is already in history we already
                # refreshed this change so refresh is not needed in this case.
                dep_key = ChangeKey(self.connection_name, None,
                                    'GerritChange', str(dep_num), str(dep_ps))
                refresh = not history.getByKey(history.Query.CHANGE, dep_key)
                dep = self._getChange(
                    dep_key, refresh=refresh, history=history,
                    network_future=network_future,
                    event=event)
                if (dep.open and dep.is_current_patchset
                    and dep not in needed_by_changes):
                    compat_needed_by_changes.append(dep_key.reference)
                    needed_by_changes.add(dep_key.reference)
            except ChangeNetworkConflict:
                raise
            # We ignore GerritEventProcessingExceptions (e.g. when exceeding
            # max_dependencies) here because they are not important on the
            # "needed_by" side of the dependency chain. Those are already
            # checked for on the "needs" side.
            except Exception:
                log.exception("Failed to get commit-needed change %s,%s",
                              dep_num, dep_ps)

        return dict(
            git_needs_changes=git_needs_changes,
            compat_needs_changes=compat_needs_changes,
            git_needed_by_changes=git_needed_by_changes,
            compat_needed_by_changes=compat_needed_by_changes,
        )

    def isMerged(self, change, head=None):
        self.log.debug("Checking if change %s is merged" % change)
        if not change.number:
            self.log.debug("Change has no number; considering it merged")
            # Good question.  It's probably ref-updated, which, ah,
            # means it's merged.
            return True

        data = self.queryChange(change.number)
        # Note: in the case of a cherry-pick there will be a new
        # patchset (i.e., change.patchset might be 3 while
        # data.current_patchset would be 4).  We could add a new
        # change to the cache here, but that would be more expensive.
        # Also, the ssh-query code path with cherry-pick does not have
        # a way of determining the current patchset for dependencies,
        # so in that case, it is useful for any changes still pointing
        # at this old version to think it is merged.
        key = ChangeKey(self.connection_name, None,
                        'GerritChange', str(change.number),
                        str(change.patchset))

        def _update_change(c):
            c.update(data, {}, self)

        self._change_cache.updateChangeWithRetry(key, change, _update_change)

        if change.is_merged:
            self.log.debug("Change %s is merged" % (change,))
        else:
            self.log.debug("Change %s is not merged" % (change,))
        if not head:
            return change.is_merged
        if not change.is_merged:
            return False

        ref = 'refs/heads/' + change.branch
        self.log.debug("Waiting for %s to appear in git repo" % (change))
        if not hasattr(change, '_ref_sha'):
            self.log.error("Unable to confirm change %s in git repo: "
                           "the change has not been reported; "
                           "this pipeline may be misconfigured "
                           "(check for multiple Gerrit connections)." %
                           (change,))
            return False

        if sha := self._waitForRefSha(change.project, ref, change._ref_sha):
            self.log.debug("Change %s is in the git repo" %
                           (change))
            return sha
        self.log.debug("Change %s did not appear in the git repo" %
                       (change))
        return False

    def _waitForRefSha(self, project, ref, old_sha=''):
        # Wait for the ref to show up in the repo
        start = time.time()
        while time.time() - start < self.is_merged_replication_timeout:
            sha = self.getRefSha(project, ref)
            if old_sha != sha:
                return sha
            time.sleep(self.replication_retry_interval)
        return False

    def getRefSha(self, project: Project, ref: str) -> str:
        refs = {}  # type: Dict[str, str]
        try:
            refs = self.getInfoRefs(project)
        except Exception:
            self.log.exception("Exception looking for ref %s" %
                               ref)
        sha = refs.get(ref, '')
        return sha

    def canMerge(self, change, allow_needs, event=None):
        log = get_annotated_logger(self.log, event)
        if not change.number:
            log.debug("Change has no number; considering it merged")
            # Good question.  It's probably ref-updated, which, ah,
            # means it's merged.
            return True
        if change.wip:
            self.log.debug("Unable to merge due to WIP")
            return FalseWithReason("work in progress flag")
        if not (change.open or change.is_merged):
            self.log.debug("Unable to merge due to change is closed")
            return FalseWithReason("change is closed")
        missing_labels = change.missing_labels - set(allow_needs)
        if missing_labels:
            self.log.debug("Unable to merge due to "
                           "missing labels: %s", missing_labels)
            return FalseWithReason(f"missing labels: {missing_labels}")
        for sr in change.submit_requirements:
            if sr.get('status') == 'UNSATISFIED':
                # Otherwise, we don't care and should skip.

                # We're going to look at each unsatisfied submit
                # requirement, and if one of the involved labels is an
                # "allow_needs" label, we will assume that Zuul may be
                # able to take an action which can cause the
                # requirement to be satisfied, and we will ignore it.
                # Otherwise, it is likely a requirement that Zuul can
                # not alter in which case the requirement should stand
                # and block merging.
                result = sr.get("submittability_expression_result", {})
                expression = result.get("expression", '')
                expr_contains_allow = False
                for allow in allow_needs:
                    if f'label:{allow}' in expression:
                        expr_contains_allow = True
                        break
                if not expr_contains_allow:
                    self.log.debug("Unable to merge due to "
                                   "submit requirement: %s", sr)
                    return FalseWithReason(f"submit requirement: {sr}")
        return True

    def getProjectOpenChanges(self, project: Project) -> List[GerritChange]:
        # This is a best-effort function in case Gerrit is unable to return
        # a particular change.  It happens.
        query = "project:{%s} status:open" % (project.name,)
        self.log.debug("Running query %s to get project open changes" %
                       (query,))
        data = self.simpleQuery(query)
        changes = []  # type: List[GerritChange]
        for record in data:
            try:
                change_key = ChangeKey(self.connection_name, None,
                                       'GerritChange',
                                       str(record.number),
                                       str(record.current_patchset))
                changes.append(self._getChange(change_key))
            except Exception:
                self.log.exception("Unable to query change %s",
                                   record.number)
        return changes

    @staticmethod
    def _checkRefFormat(refname: str) -> bool:
        # These are the requirements for valid ref names as per
        # man git-check-ref-format
        parts = refname.split('/')
        return \
            (GerritConnection.refname_bad_sequences.search(refname) is None and
             len(parts) > 1 and
             not any(part.startswith('.') or part.endswith('.lock')
                     for part in parts))

    def _getProjectBranchesRequiredFlags(
            self, exclude_unprotected, exclude_locked):
        return BranchFlag.PRESENT

    def _filterProjectBranches(
            self, branch_infos, exclude_unprotected, exclude_locked):
        return branch_infos

    def _fetchProjectBranches(self, project, required_flags):
        refs = self.getInfoRefs(project)
        heads = [str(k[len('refs/heads/'):]) for k in refs
                 if k.startswith('refs/heads/') and
                 GerritConnection._checkRefFormat(k)]
        branch_infos = [BranchInfo(h, present=True) for h in heads]
        self.log.debug("Fetched %s branches for %s", len(heads), project)
        return BranchFlag.PRESENT, branch_infos

    def _fetchProjectDefaultBranch(self, project):
        if not self.session:
            return 'master'
        head = None
        for attempt in range(1, 4):
            try:
                head = self.get(
                    'projects/%s/HEAD' % (
                        urllib.parse.quote(project.name, safe=''),
                    ))
                if head.startswith('refs/heads/'):
                    head = head[len('refs/heads/'):]
                return head
            except HTTPNotFoundException:
                self.log.exception("Unable to get HEAD for %s",
                                   project)
                return head
            except Exception as e:
                if attempt >= 3:
                    self.log.exception("Unable to get HEAD for %s",
                                       project)
                    return head
                self.log.warning("Unable to get HEAD for %s, will retry: %s",
                                 project, e)
                time.sleep(1)

    def isBranchProtected(self, project_name, branch_name,
                          zuul_event_id=None):
        # TODO: This could potentially be expanded to do something
        # with user-specific branches.
        return True

    def addEvent(self, data):
        # NOTE(mnaser): Certain plugins fire events which end up causing
        #               an unrecognized event log *and* a traceback if they
        #               do not contain full project information, we skip them
        #               here to keep logs clean.
        if data.get('type') in GerritEventConnector.IGNORED_EVENTS:
            return
        # Due to notedb, an high percentage of all events Zuul
        # processes are ref-updated of the /meta ref, and that is
        # unlikely to be used in Zuul.  Skip those here so that we
        # reduce traffic on the event queue.
        if data.get('type') == 'ref-updated':
            refname = data.get('refUpdate', {}).get('refName', '')
            if (refname.startswith('refs/changes/') and
                refname.endswith('/meta')):
                return

        # Partially realize a GerritTriggerEvent with enough
        # information to determine branches/refs/etc.
        event = GerritTriggerEvent.fromGerritEventDict(
            data, None, self, None)

        # If the event might trigger Zuul reconfiguration actions, we
        # keep it; otherwise check to see if it matches our
        # pre-filters:
        if not (
                event._branch_ref_update or
                event.default_branch_changed or
                event.change_number):
            for event_filter in self.watched_event_filters:
                r = event_filter.preFilter(event)
                if r:
                    break
            else:
                self.log.debug("Event did not match pre-filters %s", event)
                return

        event_uuid = uuid4().hex
        attributes = {
            "zuul_event_id": event_uuid,
        }
        # Gerrit events don't have an event id that could be used to globally
        # identify this event in the system so we have to generate one.
        with self.tracer.start_span(
                "GerritEvent", attributes=attributes) as span:
            event = {
                "timestamp": time.time(),
                "zuul_event_id": event_uuid,
                "span_context": tracing.getSpanContext(span),
                "payload": data,
            }
            self.event_queue.put(event)

    def review(self, item, change, message, submit, labels, checks_api,
               notify, file_comments, phase1, phase2, zuul_event_id=None):
        if self.session:
            meth = self.review_http
        else:
            meth = self.review_ssh
        return meth(item, change, message, submit, labels, checks_api, notify,
                    file_comments, phase1, phase2,
                    zuul_event_id=zuul_event_id)

    def review_ssh(self, item, change, message, submit, labels, checks_api,
                   notify, file_comments, phase1, phase2, zuul_event_id=None):
        log = get_annotated_logger(self.log, zuul_event_id)
        if checks_api:
            log.error("Zuul is configured to report to the checks API, "
                      "but no HTTP password is present for the connection "
                      "in the configuration file.")
        project = change.project.name
        cmd = 'gerrit review --project %s' % project
        if notify:
            cmd += ' --notify %s' % shlex.quote(notify)
        if phase1:
            if message:
                cmd += ' --message %s' % shlex.quote(message)
            for key, val in labels.items():
                if val is True:
                    cmd += ' --%s' % key
                else:
                    cmd += ' --label %s=%s' % (key, val)
            if self.version >= (2, 13, 0):
                cmd += ' --tag autogenerated:zuul:%s' % (
                    item.manager.pipeline.name)
        if phase2 and submit:
            cmd += ' --submit'
        changeid = '%s,%s' % (change.number, change.patchset)
        cmd += ' %s' % changeid
        out, err = self._ssh(cmd, zuul_event_id=zuul_event_id)
        return err

    def report_checks(self, log, item, change, changeid, checkinfo):
        checkinfo = checkinfo.copy()
        uuid = checkinfo.pop('uuid', None)
        scheme = checkinfo.pop('scheme', None)
        if uuid is None:
            uuids = self.project_checker_map.get(
                change.project.canonical_name, set())
            for u in uuids:
                if u.split(':')[0] == scheme:
                    uuid = u
                    break
        if uuid is None:
            log.error("Unable to find matching checker for %s %s",
                      item, checkinfo)
            return

        def fmt(t):
            return str(datetime.datetime.fromtimestamp(t))

        if item.enqueue_time:
            checkinfo['started'] = fmt(item.enqueue_time)
            if item.report_time:
                checkinfo['finished'] = fmt(item.report_time)
            url = item.formatItemUrl()
            if url:
                checkinfo['url'] = url
        if checkinfo:
            for x in range(1, 4):
                try:
                    self.post('changes/%s/revisions/%s/checks/%s' %
                              (changeid, change.commit, uuid),
                              checkinfo)
                    break
                except HTTPConflictException:
                    log.exception("Conflict submitting check data to gerrit.")
                    break
                except HTTPBadRequestException:
                    log.exception(
                        "Bad request submitting check data to gerrit.")
                    break
                except Exception as e:
                    log.exception("Error submitting check data to gerrit on "
                                  "attempt %s: %s", x, e)
                    time.sleep(x * self.submit_retry_backoff)

    def review_http(self, item, change, message, submit, labels,
                    checks_api, notify, file_comments, phase1, phase2,
                    zuul_event_id=None):
        changeid = "%s~%s~%s" % (
            urllib.parse.quote(str(change.project), safe=''),
            urllib.parse.quote(str(change.branch), safe=''),
            change.id)
        log = get_annotated_logger(self.log, zuul_event_id)
        data = dict(strict_labels=False)
        if notify:
            data['notify'] = notify
        if phase1:
            data['message'] = message
            if change.is_current_patchset:
                if labels:
                    data['labels'] = labels
                if file_comments:
                    if (2, 15, 0) <= self.version < (3, 13, 0):
                        file_comments = copy.deepcopy(file_comments)
                        url = item.formatItemUrl()
                        for comments in itertools.chain(
                                file_comments.values()):
                            for comment in comments:
                                comment['robot_id'] = 'zuul'
                                comment['robot_run_id'] = \
                                    item.current_build_set.uuid
                                if url:
                                    comment['url'] = url
                        data['robot_comments'] = file_comments
                    else:
                        data['comments'] = file_comments
            if self.version >= (2, 13, 0):
                data['tag'] = 'autogenerated:zuul:%s' % (
                    item.manager.pipeline.name)
            if checks_api:
                self.report_checks(log, item, change, changeid, checks_api)
            if (message or data.get('labels') or data.get('comments')
                or data.get('robot_comments')):
                for x in range(1, 4):
                    try:
                        self.post('changes/%s/revisions/%s/review' %
                                  (changeid, change.commit),
                                  data)
                        break
                    except HTTPConflictException:
                        log.exception("Conflict submitting data to gerrit.")
                        break
                    except HTTPBadRequestException:
                        log.exception(
                            "Bad request submitting check data to gerrit.")
                        break
                    except Exception as e:
                        log.exception(
                            "Error submitting data to gerrit "
                            "on attempt %s: %s",
                            x, e)
                        time.sleep(x * self.submit_retry_backoff)
        if phase2 and change.is_current_patchset and submit:
            for x in range(1, 4):
                try:
                    self.post('changes/%s/submit' % (changeid,), {})
                    break
                except HTTPConflictException:
                    log.info("Conflict submitting data to gerrit, "
                             "change may already be merged")
                    break
                except HTTPBadRequestException:
                    log.exception(
                        "Bad request submitting check data to gerrit.")
                    break
                except Exception as e:
                    log.exception(
                        "Error submitting data to gerrit on attempt %s: %s",
                        x, e)
                    time.sleep(x * self.submit_retry_backoff)

    def queryChangeSSH(self, number, event=None):
        args = '--all-approvals --comments --commit-message'
        args += ' --current-patch-set --dependencies --files'
        args += ' --patch-sets --submit-records'
        cmd = 'gerrit query --format json %s change:%s' % (args, number)
        out, err = self._ssh(cmd)
        if not out:
            return False
        lines = out.split('\n')
        if not lines:
            return False
        data = json.loads(lines[0])
        if not data:
            return False
        iolog = get_annotated_logger(self.iolog, event)
        iolog.debug("Received data from Gerrit query: \n%s",
                    pprint.pformat(data))
        return data

    def queryChangeHTTP(self, number, event=None):
        query = ('changes/%s?o=DETAILED_ACCOUNTS&o=CURRENT_REVISION&'
                 'o=CURRENT_COMMIT&o=CURRENT_FILES&o=LABELS&'
                 'o=DETAILED_LABELS&o=ALL_REVISIONS' % (number,))
        if self.version >= (3, 5, 0):
            query += '&o=SUBMIT_REQUIREMENTS'
        data = self.get(query)
        related = self.get('changes/%s/revisions/%s/related' % (
            number, data['current_revision']))

        files_query = 'changes/%s/revisions/%s/files' % (
            number, data['current_revision'])

        commentable_files_query = None
        # We query with parent=1 to get the list of files compared to
        # the merge target, unless this is the initial commit to the
        # repository (there are no parents).
        if data['revisions'][data['current_revision']]['commit']['parents']:
            files_query += '?parent=1'
            if len(data['revisions'][data['current_revision']][
                    'commit']['parents']) > 1:
                # If there is more than one parent, we also query the
                # list of files on this specific commit so we know
                # upon which we can leave comments.
                commentable_files_query = 'changes/%s/revisions/%s/files' % (
                    number, data['current_revision'])

        if commentable_files_query:
            commentable_files = self.get(commentable_files_query)
        else:
            commentable_files = None

        files = self.get(files_query)
        return data, related, files, commentable_files

    def queryChange(self, number, event=None, min_ltime=None):
        lock = self.change_network_manager.getQueryLock(number)
        with lock:
            if min_ltime:
                data = self.change_network_manager.getQueryResult(number)
                if data and data.zuul_query_ltime > min_ltime:
                    return data

            for attempt in range(1, 4):
                # Get a query ltime -- any events before this point should be
                # included in our change data.
                zuul_query_ltime = self.sched.zk_client.getCurrentLtime()
                try:
                    if self.session:
                        data, related, files, commentable_files =\
                            self.queryChangeHTTP(
                                number, event=event)
                        ret = GerritChangeData(
                            GerritChangeData.HTTP,
                            data, related, files, commentable_files,
                            zuul_query_ltime=zuul_query_ltime)
                    else:
                        data = self.queryChangeSSH(number, event=event)
                        ret = GerritChangeData(
                            GerritChangeData.SSH, data,
                            zuul_query_ltime=zuul_query_ltime)
                    self.change_network_manager.updateQueryResult(number, ret)
                    return ret
                except HTTPNotFoundException as e:
                    # do not retry on 404 results, instead skip further event
                    # processing since we can't load a corresponding change
                    raise GerritEventProcessingException(
                        f"Did not find change for number {number}") from e
                except RecursionError:
                    # do not retry when maximum recursion depth was exceeded
                    raise
                except Exception as e:
                    if attempt >= 3:
                        raise
                    # The internet is a flaky place try again.
                    self.log.warning(
                        "Error querying change %s, will retry: %s",
                        number, e)
                    time.sleep(1)

    def simpleQuerySSH(self, query, event=None):
        def _query_chunk(query, event):
            args = '--commit-message --current-patch-set'

            cmd = 'gerrit query --format json %s %s' % (
                args, query)
            out, err = self._ssh(cmd)
            if not out:
                return False
            lines = out.split('\n')
            if not lines:
                return False

            # filter out blank lines
            data = [json.loads(line) for line in lines
                    if line.startswith('{')]

            # check last entry for more changes
            more_changes = None
            if 'moreChanges' in data[-1]:
                more_changes = data[-1]['moreChanges']

            # we have to remove the statistics line
            del data[-1]

            if not data:
                return False, more_changes
            iolog = get_annotated_logger(self.iolog, event)
            iolog.debug("Received data from Gerrit query: \n%s",
                        pprint.pformat(data))
            return data, more_changes

        # gerrit returns 500 results by default, so implement paging
        # for large projects like nova
        alldata = []
        chunk, more_changes = _query_chunk(query, event)
        while chunk:
            alldata.extend(chunk)
            if more_changes is None:
                # continue sortKey based (before Gerrit 2.9)
                resume = "resume_sortkey:'%s'" % chunk[-1]["sortKey"]
            elif more_changes:
                # continue moreChanges based (since Gerrit 2.9)
                resume = "-S %d" % len(alldata)
            else:
                # no more changes
                break

            chunk, more_changes = _query_chunk(
                "%s %s" % (query, resume), event)
        return alldata

    def simpleQueryHTTP(self, query, event=None):
        iolog = get_annotated_logger(self.iolog, event)
        changes = []
        sortkey = ''
        done = False
        offset = 0
        query = urllib.parse.quote(query, safe='')
        while not done:
            # We don't actually want to limit to 500, but that's the
            # server-side default, and if we don't specify this, we
            # won't get a _more_changes flag.
            q = ('changes/?n=500%s&o=CURRENT_REVISION&o=CURRENT_COMMIT&'
                 'q=%s' % (sortkey, query))
            iolog.debug('Query: %s', q)
            batch = self.get(q)
            iolog.debug("Received data from Gerrit query: \n%s",
                        pprint.pformat(batch))
            done = True
            if batch:
                changes += batch
                if '_more_changes' in batch[-1]:
                    done = False
                    if '_sortkey' in batch[-1]:
                        sortkey = '&N=%s' % (batch[-1]['_sortkey'],)
                    else:
                        offset += len(batch)
                        sortkey = '&start=%s' % (offset,)
        return changes

    def simpleQuery(self, query, event=None):
        if self.session:
            # None of the users of this method require dependency
            # data, so we only perform the change query and omit the
            # related changes query.
            alldata = self.simpleQueryHTTP(query, event=event)
            return [GerritChangeData(GerritChangeData.HTTP, data)
                    for data in alldata]
        else:
            alldata = self.simpleQuerySSH(query, event=event)
            return [GerritChangeData(GerritChangeData.SSH, data)
                    for data in alldata]

    def _uploadPack(self, project):
        if self.session and not self.git_over_ssh:
            url = ('%s/%s/info/refs?service=git-upload-pack' %
                   (self.baseurl, project.name))
            for attempt in range(1, 4):
                try:
                    r = self.session.get(
                        url,
                        verify=self.verify_ssl,
                        auth=self.auth, timeout=TIMEOUT,
                        headers={'User-Agent': self.user_agent})
                    self.iolog.debug('Received: %s %s',
                                     r.status_code, r.text)
                    if r.status_code == 409:
                        raise HTTPConflictException()
                    if r.status_code == 404:
                        raise HTTPNotFoundException()
                    elif r.status_code != 200:
                        raise Exception("Received response %s" % (
                            r.status_code,))
                except HTTPNotFoundException:
                    raise
                except Exception as e:
                    if attempt >= 3:
                        self.log.exception(
                            "Error getting refs for %s:",
                            project)
                        raise
                    self.log.warning(
                        "Error getting refs for %s, will retry: %s",
                        project, e)
                    time.sleep(1)
            out = r.text[r.text.find('\n') + 5:]
        else:
            cmd = "git-upload-pack %s" % project.name
            out, err = self._ssh(cmd, "0000")
        return out

    def _open(self):
        if self.client:
            # Paramiko needs explicit closes, its possible we will open even
            # with an unclosed client so explicitly close here.
            self.client.close()
        try:
            client = paramiko.SSHClient()
            client.load_system_host_keys()
            client.set_missing_host_key_policy(paramiko.WarningPolicy())
            # SSH banner, handshake, and auth timeouts default to 15
            # seconds, so we only set the socket timeout here.
            client.connect(self.ssh_server,
                           username=self.user,
                           port=self.port,
                           key_filename=self.keyfile,
                           timeout=self.ssh_timeout)
            transport = client.get_transport()
            transport.set_keepalive(self.keepalive)
            self.client = client
        except Exception:
            client.close()
            self.client = None
            raise

    def _ssh(self, command, stdin_data=None, zuul_event_id=None):
        log = get_annotated_logger(self.log, zuul_event_id)
        if not self.client:
            self._open()

        max_attempts = 2
        for x in range(max_attempts):
            try:
                log.debug("SSH command:\n%s", command)
                stdin, stdout, stderr = self.client.exec_command(
                    command, timeout=TIMEOUT)
                break
            except Exception:
                self._open()
                if x + 1 >= max_attempts:
                    raise

        if stdin_data:
            stdin.write(stdin_data)

        out = stdout.read().decode('utf-8')
        self.iolog.debug("SSH received stdout:\n%s" % out)

        ret = stdout.channel.recv_exit_status()
        log.debug("SSH exit status: %s", ret)

        err = stderr.read().decode('utf-8')
        if err.strip():
            log.debug("SSH received stderr:\n%s", err)

        if ret:
            log.debug("SSH received stdout:\n%s", out)
            raise Exception("Gerrit error executing %s" % command)
        return (out, err)

    def getInfoRefs(self, project: Project) -> Dict[str, str]:
        try:
            # Encode the UTF-8 data back to a byte array, as the size of
            # each record in the pack is in bytes, and so the slicing must
            # also be done on a byte-basis.
            data = self._uploadPack(project).encode("utf-8")
        except Exception:
            self.log.error("Cannot get references from %s" % project)
            raise  # keeps error information
        ret = {}
        read_advertisement = False
        i = 0
        while i < len(data):
            if len(data) - i < 4:
                raise Exception("Invalid length in info/refs")
            plen = int(data[i:i + 4], 16)
            i += 4
            # It's the length of the packet, including the 4 bytes of the
            # length itself, unless it's null, in which case the length is
            # not included.
            if plen > 0:
                plen -= 4
            if len(data) - i < plen:
                raise Exception("Invalid data in info/refs")
            # Once the pack data is sliced, we can safely decode it back
            # into a (UTF-8) string.
            line = data[i:i + plen].decode("utf-8")
            i += plen
            if not read_advertisement:
                read_advertisement = True
                continue
            if plen == 0:
                # The terminating null
                continue
            line = line.strip()
            revision, ref = line.split()
            ret[ref] = revision
        return ret

    def getGitUrl(self, project: Project) -> str:
        if self.anonymous_git:
            url = ('%s/%s' % (self.baseurl, project.name))
        elif self.session and not self.git_over_ssh:
            baseurl = list(urllib.parse.urlparse(self.baseurl))
            # Make sure we escape '/' symbols, otherwise git's url
            # parser will think the username is a hostname.
            baseurl[1] = '%s:%s@%s' % (
                urllib.parse.quote(self.user, safe=''),
                urllib.parse.quote(self.password, safe=''),
                baseurl[1])
            baseurl = urllib.parse.urlunparse(baseurl)
            url = ('%s/a/%s' % (baseurl, project.name))
        else:
            url = 'ssh://%s@%s:%s/%s' % (self.user, self.ssh_server, self.port,
                                         project.name)
        return url

    def _getWebUrl(self, project: Project, sha: str=None) -> str:
        return self.gitweb_url_template.format(
            baseurl=self.baseurl,
            project=project.getSafeAttributes(),
            sha=sha)

    def _getRemoteVersion(self):
        version = self.get('config/server/version')
        base = version.split('-')[0]
        parts = base.split('.')
        major = minor = micro = 0
        if len(parts) > 0:
            major = int(parts[0])
        if len(parts) > 1:
            minor = int(parts[1])
        if len(parts) > 2:
            micro = int(parts[2])
        self.version = (major, minor, micro)
        self.log.info("Remote version is: %s (parsed as %s)" %
                      (version, self.version))

    def _getRemoteInfo(self):
        info = self.get('config/server/info')
        change_info = info.get('change', {})
        self.submit_whole_topic = change_info.get('submit_whole_topic', False)
        self.log.info("Remote submitWholeTopic: %s", self.submit_whole_topic)

    def refWatcherCallback(self, data):
        event = {
            'type': 'ref-updated',
            'refUpdate': {
                'project': data['project'],
                'refName': data['ref'],
                'oldRev': data['oldrev'],
                'newRev': data['newrev'],
            }
        }
        self.addEvent(event)

    def onLoad(self, zk_client, component_registry):
        self.log.debug("Starting Gerrit Connection/Watchers")
        if self.session:
            try:
                self._getRemoteVersion()
            except Exception:
                self.log.exception("Unable to determine remote Gerrit version")
            try:
                self._getRemoteInfo()
            except Exception:
                self.log.exception("Unable to fetch remote Gerrit info")

        # Set the project branch cache to read only if no scheduler is
        # provided to prevent fetching the branches from the connection.
        self.read_only = not self.sched

        self.log.debug('Creating Zookeeper branch cache')
        self._branch_cache = BranchCache(zk_client, self,
                                         component_registry)

        self.log.info("Creating Zookeeper event queue")
        if self.sched:
            component_info = self.sched.component_info
        else:
            component_info = None
        self.event_queue = ConnectionEventQueue(
            zk_client, self.connection_name, component_info)

        # If the connection was not loaded by a scheduler, but by e.g.
        # zuul-web, we want to stop here.
        if not self.sched:
            return

        self.log.debug('Creating Zookeeper change cache')
        self._change_cache = GerritChangeCache(zk_client, self)

        self.startEventSourceThread()
        # TODO: This is only for the checks plugin and can be removed
        # when checks support is removed.  Until then, we always start
        # this thread, but if no checks are configured, it remains
        # idle.
        self.startPollerThread()
        self.startEventConnector()

    def onStop(self):
        self.log.debug("Stopping Gerrit Connection/Watchers")
        self.stopEventSourceThread()
        self.stopPollerThread()
        self.stopRefWatcherThread()
        self.stopEventConnector()
        if self._change_cache:
            self._change_cache.stop()

    def getEventQueue(self):
        return getattr(self, "event_queue", None)

    def getWebController(self, zuul_web):
        if self.event_source == self.EVENT_SOURCE_WEBHOOK:
            return GerritWebController(zuul_web, self)
        return None

    def stopEventSourceThread(self):
        if self.event_thread:
            self.event_thread.stop()
            self.event_thread.join()

    def startEventSourceThread(self):
        if self.event_source == self.EVENT_SOURCE_STREAM_EVENTS:
            self.startSSHListener()
        elif self.event_source == self.EVENT_SOURCE_KAFKA:
            self.startKafkaListener()
        elif self.event_source == self.EVENT_SOURCE_KINESIS:
            self.startAWSKinesisListener()
        elif self.event_source == self.EVENT_SOURCE_GCLOUD_PUBSUB:
            self.startGcloudPubsubListener()
        elif self.event_source == self.EVENT_SOURCE_WEBHOOK:
            self.log.info("Using HTTP webhook event source")
        else:
            self.log.warning("No gerrit event source configured")
            self.startRefWatcherThread()
        if self.event_thread:
            self.event_thread.start()

    def startSSHListener(self):
        self.log.info("Starting SSH event stream client")
        self.event_thread = GerritSSHEventListener(
            self, self.connection_config)

    def startKafkaListener(self):
        self.log.info("Starting Kafka consumer")
        self.event_thread = GerritKafkaEventListener(
            self, self.connection_config)

    def startAWSKinesisListener(self):
        self.log.info("Starting AWS Kinesis consumer")
        self.event_thread = GerritAWSKinesisEventListener(
            self, self.connection_config)

    def startGcloudPubsubListener(self):
        self.log.info("Starting gcloud pubsub consumer")
        self.event_thread = GerritGcloudPubsubEventListener(
            self, self.connection_config)

    def startPollerThread(self):
        if self.session is not None:
            self.poller_thread = self._poller_class(self)
            self.poller_thread.start()
        else:
            self.log.info(
                "%s: Gerrit Poller is disabled because no "
                "HTTP authentication is defined",
                self.connection_name)

    def stopPollerThread(self):
        if self.poller_thread:
            self.poller_thread.stop()
            self.poller_thread.join()

    def stopRefWatcherThread(self):
        if self.ref_watcher_thread:
            self.ref_watcher_thread.stop()
            self.ref_watcher_thread.join()

    def startRefWatcherThread(self):
        self.ref_watcher_thread = self._ref_watcher_class(
            self,
            self.baseurl,
            self.ref_watcher_poll_interval,
            self.refWatcherCallback,
            election_name="ref-watcher")
        self.ref_watcher_thread.start()

    def startEventConnector(self):
        self.gerrit_event_connector = GerritEventConnector(self)
        self.gerrit_event_connector.start()

    def stopEventConnector(self):
        if self.gerrit_event_connector:
            self.gerrit_event_connector.stop()
