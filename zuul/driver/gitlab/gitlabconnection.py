# Copyright 2019 Red Hat, Inc.
# Copyright 2022 Acme Gating, LLC
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

import logging
import threading
import json
import cherrypy
import voluptuous as v
import time
import uuid
import re
import requests
import urllib3

import dateutil.parser

from urllib.parse import quote_plus

from opentelemetry import trace

from zuul.connection import (
    BaseConnection, ZKChangeCacheMixin, ZKBranchCacheMixin
)
from zuul.web.handler import BaseWebController
from zuul.lib import tracing
from zuul.lib.http import ZuulHTTPAdapter
from zuul.lib.logutil import get_annotated_logger
from zuul.lib.config import any_to_bool
from zuul.exceptions import MergeFailure
from zuul.model import Branch, Ref, Tag, FalseWithReason
from zuul.driver.gitlab.gitlabmodel import GitlabTriggerEvent, MergeRequest
from zuul.zk.branch_cache import BranchCache, BranchFlag, BranchInfo
from zuul.zk.change_cache import (
    AbstractChangeCache,
    ConcurrentUpdateError,
)
from zuul.zk.event_queues import ConnectionEventQueue

# HTTP timeout in seconds
TIMEOUT = 30


class GitlabChangeCache(AbstractChangeCache):
    log = logging.getLogger("zuul.driver.GitlabChangeCache")

    CHANGE_TYPE_MAP = {
        "Ref": Ref,
        "Tag": Tag,
        "Branch": Branch,
        "MergeRequest": MergeRequest,
    }


class GitlabEventConnector(threading.Thread):
    """Move events from Gitlab into the scheduler"""

    log = logging.getLogger("zuul.GitlabEventConnector")
    tracer = trace.get_tracer("zuul")

    def __init__(self, connection):
        super(GitlabEventConnector, self).__init__()
        self.daemon = True
        self.connection = connection
        self.event_queue = connection.event_queue
        self._stopped = False
        self._process_event = threading.Event()
        self.event_handler_mapping = {
            'merge_request': self._event_merge_request,
            'note': self._event_note,
            'push': self._event_push,
            'tag_push': self._event_tag_push,
        }

    def stop(self):
        self._stopped = True
        self._process_event.set()
        self.event_queue.election.cancel()

    def _onNewEvent(self):
        self._process_event.set()
        # Stop the data watch in case the connector was stopped
        return not self._stopped

    def run(self):
        # Wait for the scheduler to prime its config so that we have
        # the full tenant list before we start moving events.
        self.connection.sched.primed_event.wait()
        if self._stopped:
            return
        self.event_queue.registerEventWatch(self._onNewEvent)
        while not self._stopped:
            try:
                self.event_queue.election.run(self._run)
            except Exception:
                self.log.exception("Exception handling Gitlab event:")

    def _run(self):
        while not self._stopped:
            for event in self.event_queue:
                event_span = tracing.restoreSpanContext(
                    event.get("span_context"))
                attributes = {"rel": "GitlabEvent"}
                link = trace.Link(event_span.get_span_context(),
                                  attributes=attributes)
                with self.tracer.start_as_current_span(
                        "GitlabEventProcessing", links=[link]):
                    try:
                        self._handleEvent(event)
                    finally:
                        self.event_queue.ack(event)
                if self._stopped:
                    return
            self._process_event.wait(10)
            self._process_event.clear()

    def _event_base(self, body):
        event = GitlabTriggerEvent()
        event.connection_name = self.connection.connection_name
        attrs = body.get('object_attributes')
        if attrs:
            event.updated_at = int(dateutil.parser.parse(
                attrs['updated_at']).timestamp())
            event.created_at = int(dateutil.parser.parse(
                attrs['created_at']).timestamp())
        event.project_name = body['project']['path_with_namespace']
        return event

    # https://docs.gitlab.com/ee/user/project/integrations/webhooks.html#merge-request-events
    def _event_merge_request(self, body):
        event = self._event_base(body)
        attrs = body['object_attributes']
        event.type = 'gl_merge_request'
        event.title = attrs['title']
        event.change_number = attrs['iid']
        event.ref = "refs/merge-requests/%s/head" % event.change_number
        event.branch = attrs['target_branch']
        event.patch_number = attrs['last_commit']['id']
        event.change_url = self.connection.getMRUrl(event.project_name,
                                                    event.change_number)
        if attrs['action'] == 'open':
            event.action = 'opened'
        elif attrs['action'] == 'close':
            event.action = 'closed'
        elif attrs['action'] == 'merge':
            event.action = 'merged'
        elif attrs['action'] == 'update' and attrs.get("oldrev"):
            # As stated in the merge-request-event doc 'oldrev' attribute
            # is set when there is code change.
            event.action = 'changed'
        elif attrs['action'] == 'update' and "description" in body["changes"]:
            event.merge_request_description_changed = True
            event.action = 'changed'
        elif attrs['action'] == 'update' and body["changes"].get("labels"):
            event.action = 'labeled'
            previous_labels = [
                label["title"] for
                label in body["changes"]["labels"]["previous"]]
            current_labels = [
                label["title"] for
                label in body["changes"]["labels"]["current"]]
            new_labels = set(current_labels) - set(previous_labels)
            event.labels = list(new_labels)
            removed_labels = set(previous_labels) - set(current_labels)
            event.unlabels = list(removed_labels)
        elif attrs['action'] in ('approved', 'unapproved'):
            event.action = attrs['action']
        else:
            # Do not handle other merge_request action for now.
            return None
        return event

    # https://docs.gitlab.com/ee/user/project/integrations/webhooks.html#comment-on-merge-request
    def _event_note(self, body):
        event = self._event_base(body)
        event.comment = body['object_attributes']['note']
        mr = body['merge_request']
        event.title = mr['title']
        event.change_number = mr['iid']
        # mr['last_commit']['id'] is the commit SHA
        event.patch_number = mr['last_commit']['id']
        event.ref = "refs/merge-requests/%s/head" % event.change_number
        event.branch = mr['target_branch']
        event.change_url = self.connection.getMRUrl(event.project_name,
                                                    event.change_number)
        event.action = 'comment'
        event.type = 'gl_merge_request'
        return event

    # https://docs.gitlab.com/ee/user/project/integrations/webhooks.html#push-events
    def _event_push(self, body):
        event = self._event_base(body)
        event.branch = body['ref'].replace('refs/heads/', '')
        event.ref = body['ref']
        event.newrev = body['after']
        event.oldrev = body['before']
        event.type = 'gl_push'
        event.branch_protected = body['ref_protected']
        event.commits = body.get('commits')

        self.connection.clearConnectionCacheOnBranchEvent(event)

        return event

    # https://gitlab.com/help/user/project/integrations/webhooks#tag-events
    def _event_tag_push(self, body):
        event = self._event_base(body)
        event.ref = body['ref']
        event.newrev = body['after']
        event.oldrev = None
        event.tag = body['ref'].replace('refs/tags/', '')
        event.type = 'gl_push'
        event.branch_protected = body['ref_protected']
        return event

    def _handleEvent(self, connection_event):
        if self._stopped:
            return

        zuul_event_id = str(uuid.uuid4())
        log = get_annotated_logger(self.log, zuul_event_id)
        timestamp = time.time()
        json_body = connection_event["payload"]
        log.debug("Received payload: %s", json_body)

        event_type = json_body['object_kind']
        log.debug("Received event: %s", event_type)

        if event_type not in self.event_handler_mapping:
            message = "Unhandled Gitlab event: %s" % event_type
            log.info(message)
            return

        if event_type in self.event_handler_mapping:
            log.info("Handling event: %s" % event_type)

        try:
            event = self.event_handler_mapping[event_type](json_body)
        except Exception:
            log.exception(
                'Exception when handling event: %s' % event_type)
            event = None

        if event:
            event.zuul_event_id = zuul_event_id
            event.timestamp = timestamp
            event.project_hostname = self.connection.canonical_hostname
            if event.change_number:
                change_key = self.connection.source.getChangeKey(event)
                self.connection._getChange(change_key, refresh=True,
                                           event=event)

            # If this event references a branch and we're excluding
            # unprotected branches, we might need to check whether the
            # branch is now protected.
            if hasattr(event, "branch") and event.branch:
                self.connection.checkBranchCache(
                    event.project_name, event,
                    protected=event.branch_protected)

            self.connection.logEvent(event)
            self.connection.sched.addTriggerEvent(
                self.connection.driver_name, event
            )


class GitlabAPIClientException(Exception):
    pass


class GitlabAPIClient():
    log = logging.getLogger("zuul.GitlabAPIClient")

    def __init__(self, baseurl, api_token, keepalive, disable_pool):
        self._session = None
        self._orig_baseurl = baseurl
        self.baseurl = '%s/api/v4' % baseurl
        self.api_token = api_token
        self.keepalive = keepalive
        self.disable_pool = disable_pool
        self.get_mr_wait_factor = 2
        self.headers = {'Authorization': 'Bearer %s' % (
            self.api_token)}

        if not self.disable_pool:
            self._session = self._makeSession()

    def _makeSession(self):
        session = requests.Session()
        retry = urllib3.util.Retry(total=8,
                                   backoff_factor=0.1)
        adapter = ZuulHTTPAdapter(keepalive=self.keepalive,
                                  max_retries=retry)
        session.mount(self._orig_baseurl, adapter)
        return session

    @property
    def session(self):
        if self.disable_pool:
            return self._makeSession()
        return self._session

    def _manage_error(self, data, code, url, verb, zuul_event_id=None):
        if code < 400:
            return
        else:
            raise GitlabAPIClientException(
                "[e: %s] Unable to %s on %s (code: %s) due to: %s" % (
                    zuul_event_id, verb, url, code, data
                ))

    def get(self, url, zuul_event_id=None):
        log = get_annotated_logger(self.log, zuul_event_id)
        log.debug("Getting resource %s ..." % url)
        ret = self.session.get(url, headers=self.headers,
                               timeout=TIMEOUT)
        log.debug("GET returned (code: %s): %s" % (
            ret.status_code, ret.text))
        return ret.json(), ret.status_code, ret.url, 'GET'

    def post(self, url, params=None, zuul_event_id=None):
        log = get_annotated_logger(self.log, zuul_event_id)
        log.info(
            "Posting on resource %s, params (%s) ..." % (url, params))
        ret = self.session.post(url, data=params, headers=self.headers,
                                timeout=TIMEOUT)
        log.debug("POST returned (code: %s): %s" % (
            ret.status_code, ret.text))
        return ret.json(), ret.status_code, ret.url, 'POST'

    def put(self, url, params=None, zuul_event_id=None):
        log = get_annotated_logger(self.log, zuul_event_id)
        log.info(
            "Put on resource %s, params (%s) ..." % (url, params))
        ret = self.session.put(url, data=params, headers=self.headers,
                               timeout=TIMEOUT)
        log.debug("PUT returned (code: %s): %s" % (
            ret.status_code, ret.text))
        return ret.json(), ret.status_code, ret.url, 'PUT'

    # https://docs.gitlab.com/ee/api/merge_requests.html#get-single-mr
    def get_mr(self, project_name, number, zuul_event_id=None):
        log = get_annotated_logger(self.log, zuul_event_id)
        attempts = 0

        def _get_mr():
            path = "/projects/%s/merge_requests/%s" % (
                quote_plus(project_name), number)
            resp = self.get(self.baseurl + path, zuul_event_id=zuul_event_id)
            self._manage_error(*resp, zuul_event_id=zuul_event_id)
            return resp[0]

        # The Gitlab API might not return a complete MR description as
        # some attributes are updated asynchronously. This loop ensures
        # we query the API until all async attributes are available or until
        # a defined delay is reached.
        while True:
            attempts += 1
            mr = _get_mr()
            # The diff_refs attribute is updated asynchronously
            if all(map(lambda k: mr.get(k, None), ['diff_refs'])):
                return mr
            if attempts > 4:
                log.warning(
                    "Fetched MR %s#%s with incomplete data" % (
                        project_name, number))
                return mr
            wait_delay = attempts * self.get_mr_wait_factor
            log.info(
                "Will retry to fetch %s#%s due to incomplete data "
                "(in %s seconds) ..." % (project_name, number, wait_delay))
            time.sleep(wait_delay)

    # https://docs.gitlab.com/ee/api/branches.html#list-repository-branches
    def get_project_branches(self, project_name, exclude_unprotected,
                             zuul_event_id=None):
        path = "/projects/{}/repository/branches?per_page=100&page={}"
        page = 1
        branches = []

        # Handle pagination
        while True:
            url = self.baseurl + path.format(quote_plus(project_name), page)
            resp = self.get(url, zuul_event_id=zuul_event_id)
            if resp[0]:
                self._manage_error(*resp, zuul_event_id=zuul_event_id)
                branches.extend(resp[0])
                page += 1
            else:
                break

        if exclude_unprotected:
            return [branch['name'] for branch
                    in branches if branch['protected']]
        else:
            return [branch['name'] for branch in branches]

    # https://docs.gitlab.com/ee/api/branches.html#get-single-repository-branch
    def get_project_branch(self, project_name, branch_name,
                           zuul_event_id=None):
        path = "/projects/{}/repository/branches/{}"
        path = path.format(quote_plus(project_name), quote_plus(branch_name))
        url = self.baseurl + path
        resp = self.get(url, zuul_event_id=zuul_event_id)
        try:
            self._manage_error(*resp, zuul_event_id=zuul_event_id)
        except GitlabAPIClientException:
            if resp[1] != 404:
                raise
            return {}
        return resp[0]

    # https://docs.gitlab.com/ee/api/notes.html#create-new-merge-request-note
    def comment_mr(self, project_name, number, msg, zuul_event_id=None):
        path = "/projects/%s/merge_requests/%s/notes" % (
            quote_plus(project_name), number)
        params = {'body': msg}
        resp = self.post(
            self.baseurl + path, params=params,
            zuul_event_id=zuul_event_id)
        self._manage_error(*resp, zuul_event_id=zuul_event_id)
        return resp[0]

    # https://docs.gitlab.com/ee/api/merge_request_approvals.html#approve-merge-request
    def approve_mr(self, project_name, number, patchset, approve=True,
                   zuul_event_id=None):
        """
        Returns the JSON-decoded body of the response, or None if the
        merge request was already previously approved or unapproved.
        """
        approve = 'approve' if approve else 'unapprove'
        path = "/projects/%s/merge_requests/%s/%s" % (
            quote_plus(project_name), number, approve)
        params = {'sha': patchset} if approve else {}
        resp = self.post(
            self.baseurl + path, params=params,
            zuul_event_id=zuul_event_id)
        res, code = resp[0], resp[1]
        try:
            self._manage_error(*resp, zuul_event_id=zuul_event_id)
        except GitlabAPIClientException:
            log = get_annotated_logger(self.log, zuul_event_id)

            # Attempting to approve a merge request more than once by
            # the same user will result in a 401 Unauthorized
            # response.
            if approve == 'approve' and code == 401:
                log.debug('Merge request %s/%s is already approved' %
                          (project_name, number))
                return None
            # Attempting to unapprove an already unapproved a merge request
            # will result in a 404 response.
            if approve == 'unapprove' and code == 404:
                log.debug('Merge request %s/%s is already unapproved' %
                          (project_name, number))
                return None

            log.error('Failed to %s the merge request %s/%s: %s' %
                      (approve, project_name, number, res))

            # 409 is returned when current HEAD of the merge request doesn't
            # match the 'sha' parameter.
            if approve == 'approve' and code == 409:
                return None

            # Unhandled
            raise
        return res

    # https://docs.gitlab.com/ee/api/merge_request_approvals.html#get-configuration-1
    def get_mr_approvals_status(self, project_name, number,
                                zuul_event_id=None):
        path = "/projects/%s/merge_requests/%s/approvals" % (
            quote_plus(project_name), number)
        resp = self.get(self.baseurl + path, zuul_event_id=zuul_event_id)
        self._manage_error(*resp, zuul_event_id=zuul_event_id)
        return resp[0]

    # https://docs.gitlab.com/ee/api/merge_requests.html#accept-mr
    def merge_mr(self, project_name, number,
                 method,
                 zuul_event_id=None):
        path = "/projects/%s/merge_requests/%s/merge" % (
            quote_plus(project_name), number)
        params = {}
        if method == "squash":
            params['squash'] = True
        resp = self.put(
            self.baseurl + path, params, zuul_event_id=zuul_event_id)
        try:
            self._manage_error(*resp, zuul_event_id=zuul_event_id)
            if resp[0]['state'] != 'merged':
                raise MergeFailure(
                    "Merge request merge failed: %s" % resp.get('merge_error')
                )
        except GitlabAPIClientException as e:
            raise MergeFailure('Merge request merge failed: %s' % e)
        return resp[0]

    # https://docs.gitlab.com/ee/api/merge_requests.html#update-mr
    def update_mr(self, project_name, number,
                  zuul_event_id=None,
                  **params):
        path = "/projects/%s/merge_requests/%s" % (
            quote_plus(project_name), number)
        resp = self.put(self.baseurl + path, params=params,
                        zuul_event_id=zuul_event_id)
        self._manage_error(*resp, zuul_event_id=zuul_event_id)
        return resp[0]


class GitlabConnection(ZKChangeCacheMixin, ZKBranchCacheMixin, BaseConnection):
    driver_name = 'gitlab'
    log = logging.getLogger("zuul.GitlabConnection")
    payload_path = 'payload'

    def __init__(self, driver, connection_name, connection_config):
        super(GitlabConnection, self).__init__(
            driver, connection_name, connection_config)
        self.projects = {}
        self.server = self.connection_config.get('server', 'gitlab.com')
        self.baseurl = self.connection_config.get(
            'baseurl', 'https://%s' % self.server).rstrip('/')
        self.cloneurl = self.connection_config.get(
            'cloneurl', self.baseurl).rstrip('/')
        self.canonical_hostname = self.connection_config.get(
            'canonical_hostname', self.server)
        self.webhook_token = self.connection_config.get(
            'webhook_token', '')
        self.api_token_name = self.connection_config.get(
            'api_token_name', '')
        self.api_token = self.connection_config.get(
            'api_token', '')
        self.keepalive = self.connection_config.get('keepalive', 60)
        self.disable_pool = any_to_bool(self.connection_config.get(
            'disable_connection_pool', False))

        self.gl_client = GitlabAPIClient(self.baseurl, self.api_token,
                                         self.keepalive, self.disable_pool)
        self.sched = None
        self.source = driver.getSource(self)

    def _start_event_connector(self):
        self.gitlab_event_connector = GitlabEventConnector(self)
        self.gitlab_event_connector.start()

    def _stop_event_connector(self):
        if self.gitlab_event_connector:
            self.gitlab_event_connector.stop()
            self.gitlab_event_connector.join()

    def onLoad(self, zk_client, component_registry):
        self.log.info('Starting Gitlab connection: %s', self.connection_name)

        # Set the project branch cache to read only if no scheduler is
        # provided to prevent fetching the branches from the connection.
        self.read_only = not self.sched

        self.log.debug('Creating Zookeeper branch cache')
        self._branch_cache = BranchCache(zk_client, self, component_registry)

        self.log.info('Creating Zookeeper event queue')
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
        self._change_cache = GitlabChangeCache(zk_client, self)

        self.log.info('Starting event connector')
        self._start_event_connector()

    def onStop(self):
        if hasattr(self, 'gitlab_event_connector'):
            self._stop_event_connector()
        if self._change_cache:
            self._change_cache.stop()

    def getWebController(self, zuul_web):
        return GitlabWebController(zuul_web, self)

    def getEventQueue(self):
        return getattr(self, "event_queue", None)

    def getProject(self, name):
        return self.projects.get(name)

    def addProject(self, project):
        self.projects[project.name] = project

    def _getProjectBranchesRequiredFlags(
            self, exclude_unprotected, exclude_locked):
        required_flags = BranchFlag.CLEAR
        if exclude_unprotected:
            required_flags |= BranchFlag.PROTECTED
        if not required_flags:
            required_flags = BranchFlag.PRESENT
        return required_flags

    def _filterProjectBranches(
            self, branch_infos, exclude_unprotected, exclude_locked):
        if exclude_unprotected:
            branch_infos = [b for b in branch_infos if b.protected is True]
        return branch_infos

    def _fetchProjectBranches(self, project, required_flags):
        valid_flags = BranchFlag.CLEAR
        branch_infos = {}
        if BranchFlag.PROTECTED in required_flags:
            valid_flags |= BranchFlag.PROTECTED
            for branch_name in self.gl_client.get_project_branches(
                    project.name, True):
                bi = branch_infos.setdefault(
                    branch_name, BranchInfo(branch_name))
                bi.protected = True
        if BranchFlag.PRESENT in required_flags:
            valid_flags |= BranchFlag.PRESENT
            for branch_name in self.gl_client.get_project_branches(
                    project.name, False):
                bi = branch_infos.setdefault(
                    branch_name, BranchInfo(branch_name))
                bi.present = True
        return valid_flags, list(branch_infos.values())

    def getProjectBranchSha(self, project_name, branch_name,
                            zuul_event_id=None):
        branch = self.gl_client.get_project_branch(project_name, branch_name,
                                                   zuul_event_id)
        return branch['commit']['id']

    def isBranchProtected(self, project_name, branch_name,
                          zuul_event_id=None):
        branch = self.gl_client.get_project_branch(project_name, branch_name,
                                                   zuul_event_id)
        return branch.get('protected')

    def getGitwebUrl(self, project, sha=None):
        url = '%s/%s' % (self.baseurl, project)
        if sha is not None:
            url += '/tree/%s' % sha
        return url

    def getMRUrl(self, project, number):
        return '%s/%s/merge_requests/%s' % (self.baseurl, project, number)

    def getGitUrl(self, project):
        cloneurl = '%s/%s.git' % (self.cloneurl, project.name)
        # https://gitlab.com/gitlab-org/gitlab/-/issues/212953
        # any login name can be used, but it's likely going to be reduce to
        # username/token-name
        if (cloneurl.startswith('http') and self.api_token_name != '' and
            not re.match("http?://.+:.+@.+", cloneurl)):
            cloneurl = '%s://%s:%s@%s/%s.git' % (
                self.cloneurl.split('://')[0],
                self.api_token_name,
                self.api_token,
                self.cloneurl.split('://')[1],
                project.name)
        return cloneurl

    def getChange(self, change_key, refresh=False, event=None):
        if change_key.connection_name != self.connection_name:
            return None
        if change_key.change_type == 'MergeRequest':
            self.log.info("Getting change for %s#%s" % (
                change_key.project_name, change_key.stable_id))
            change = self._getChange(change_key,
                                     refresh=refresh, event=event)
        else:
            self.log.info("Getting change for %s ref:%s" % (
                change_key.project_name, change_key.stable_id))
            change = self._getNonMRRef(change_key, event=event)
        return change

    def _getChange(self, change_key, refresh=False, event=None):
        log = get_annotated_logger(self.log, event)
        number = int(change_key.stable_id)
        change = self._change_cache.get(change_key)
        if change and not refresh:
            log.debug("Getting change from cache %s" % str(change_key))
            return change
        project = self.source.getProject(change_key.project_name)
        if not change:
            change = MergeRequest(project.name)
            change.project = project
            change.number = number
            # patch_number is the tips commit SHA of the MR
            change.patchset = change_key.revision
            change.url = self.getMRUrl(project.name, number)
            change.uris = [change.url.split('://', 1)[-1]]  # remove scheme

        log.debug("Getting change mr#%s from project %s" % (
            number, project.name))
        log.info("Updating change from Gitlab %s" % change)
        mr = self.getMR(change.project.name, change.number, event=event)

        def _update_change(c):
            self._updateChange(c, event, mr)

        change = self._change_cache.updateChangeWithRetry(change_key, change,
                                                          _update_change)
        return change

    def _getNonMRRef(self, change_key, refresh=False, event=None):
        change = self._change_cache.get(change_key)
        if change:
            if refresh:
                self._change_cache.updateChangeWithRetry(
                    change_key, change, lambda c: None)
            return change
        project = self.source.getProject(change_key.project_name)
        if change_key.change_type == 'Tag':
            change = Tag(project)
            tag = change_key.stable_id
            change.tag = tag
            change.ref = f'refs/tags/{tag}'
        elif change_key.change_type == 'Branch':
            branch = change_key.stable_id
            change = Branch(project)
            change.branch = branch
            change.ref = f'refs/heads/{branch}'
        else:
            change = Ref(project)
            change.ref = change_key.stable_id
        change.oldrev = change_key.oldrev
        change.newrev = change_key.newrev
        change.url = self.getGitwebUrl(project, sha=change.newrev)
        change.files = self.getPushedFileNames(event)
        try:
            self._change_cache.set(change_key, change)
        except ConcurrentUpdateError:
            change = self._change_cache.get(change_key)
        return change

    def _updateChange(self, change, event, mr):
        log = get_annotated_logger(self.log, event)
        change.mr = mr
        change.ref = "refs/merge-requests/%s/head" % change.number
        change.branch = change.mr['target_branch']
        change.is_current_patchset = (change.mr['sha'] == change.patchset)
        diff_refs = change.mr.get("diff_refs", {})
        # Diff_refs may not be available immediately after a MR is
        # created.  We can get the commit_id/patch_number from the
        # event if there is one (and there probably will be if we're
        # getting a MR right after creation).  This method is also
        # called with event=None later in the lifecycle of an MR, so
        # we prefer to use diff_refs if it's available.
        if diff_refs:
            change.base_sha = diff_refs.get('base_sha')
            change.commit_id = diff_refs.get('head_sha')
        else:
            change.base_sha = None
            if event and hasattr(event, 'patch_number'):
                change.commit_id = event.patch_number
            else:
                change.commit_id = None
        change.owner = change.mr['author'].get('username')
        # Files changes are not part of the Merge Request data, so we
        # always request the Zuul merger fetch them.  But don't
        # overwrite the returned value if it has completed.
        if not change.files:
            change.files = None
        change.title = change.mr['title']
        change.open = change.mr['state'] == 'opened'
        change.is_merged = change.mr['state'] == 'merged'
        change.merge_commit_sha = change.mr.get('merge_commit_sha')
        # Can be "can_be_merged"
        change.merge_status = change.mr['merge_status']
        # Blocking discussions - not properly documented parameter
        # It is set to False when project settings enforce "all discussions
        # must be resolved" and there are unresolved discussions.
        # Gently try to get it for the case it is not present in some
        # installations.
        change.blocking_discussions_resolved = \
            change.mr.get('blocking_discussions_resolved', True)
        change.approved = change.mr['approved']
        change.message = change.mr.get('description', "") or ""
        change.labels = change.mr['labels']
        change.updated_at = int(dateutil.parser.parse(
            change.mr['updated_at']).timestamp())
        log.info("Updated change from Gitlab %s" % change)
        return change

    def getPushedFileNames(self, event):
        if event is None:
            return None
        if not hasattr(event, 'total_commits_count'):
            return None
        if event.total_commits_count > 20:
            # Gitlab only includes files for the most recent 20
            # commits.  If we have more than that, set files to None
            # so that mergers will perform lookups if necessary, and
            # the scheduler will assume a reconfiguration is required.
            return None
        files = set()
        for c in event.commits:
            for f in c.get('added') + c.get('modified') + c.get('removed'):
                files.add(f)
        return list(files)

    def canMerge(self, change, allow_needs, event=None):
        log = get_annotated_logger(self.log, event)
        can_merge = True
        if not change.blocking_discussions_resolved:
            can_merge = FalseWithReason('blocking discussions not resolved')
        elif change.merge_status != "can_be_merged":
            can_merge = FalseWithReason('GitLab mergeability')

        log.info('Check MR %s#%s mergeability can_merge: %s'
                 ' (merge status: %s, blocking discussions resolved: %s)',
                 change.project.name, change.number, can_merge,
                 change.merge_status, change.blocking_discussions_resolved)

        return can_merge

    def getMR(self, project_name, number, event=None):
        log = get_annotated_logger(self.log, event)
        mr = self.gl_client.get_mr(project_name, number, zuul_event_id=event)
        log.info('Got MR %s#%s', project_name, number)
        mr_approval_status = self.gl_client.get_mr_approvals_status(
            project_name, number, zuul_event_id=event)
        log.info('Got MR approval status %s#%s', project_name, number)
        if 'approvals_left' in mr_approval_status:
            # 'approvals_left' is not present when 'Required Merge Request
            # Approvals' feature isn't available
            mr['approved'] = mr_approval_status['approvals_left'] == 0
        else:
            mr['approved'] = mr_approval_status['approved']
        return mr

    def commentMR(self, project_name, number, message, event=None):
        log = get_annotated_logger(self.log, event)
        self.gl_client.comment_mr(
            project_name, number, message, zuul_event_id=event)
        log.info("Commented on MR %s#%s", project_name, number)

    def approveMR(self, project_name, number, patchset, approve, event=None):
        log = get_annotated_logger(self.log, event)
        result = self.gl_client.approve_mr(
            project_name, number, patchset, approve, zuul_event_id=event)
        if result:
            log.info(
                "Set approval: %s on MR %s#%s (%s)", approve,
                project_name, number, patchset)

    def getChangesDependingOn(self, change, projects, tenant):
        """ Reverse lookup of MR depending on this one
        """
        # TODO(fbo): Further research to implement this lookup:
        # https://docs.gitlab.com/ee/api/search.html#scope-merge_requests
        # Will be done in a folloup commit
        return []

    def mergeMR(self, project_name, number, method='merge', event=None):
        log = get_annotated_logger(self.log, event)
        self.gl_client.merge_mr(
            project_name, number, method, zuul_event_id=event)
        log.info("Merged MR %s#%s", project_name, number)

    def updateMRLabels(self, project_name, mr_number, labels, unlabels,
                       zuul_event_id=None):
        log = get_annotated_logger(self.log, zuul_event_id)
        self.gl_client.update_mr(
            project_name, mr_number, zuul_event_id=zuul_event_id,
            add_labels=','.join(labels),
            remove_labels=','.join(unlabels))
        log.debug("Added labels %s to, and removed labels %s from %s#%s",
                  labels, unlabels, project_name, mr_number)

    # Override from connection cache mixin
    def clearConnectionCacheOnBranchEvent(self, event):
        """Update event and clear connection cache if needed.

        This checks whether the event created or deleted a branch so
        that Zuul may know to perform a reconfiguration on the
        project. Drivers must call this method when a branch event is
        received.

        :param event:
            The event, inherit from `zuul.model.TriggerEvent` class.
        """
        if event.oldrev == '0' * 40:
            event.branch_created = True
        elif event.newrev == '0' * 40:
            event.branch_deleted = True
        else:
            event.branch_updated = True

        # Gitlab includes the branch protection flag in all push
        # events, so we can let checkBranchCache update the cache; we
        # don't need to clear the branch list here.
        return event


class GitlabWebController(BaseWebController):

    log = logging.getLogger("zuul.GitlabWebController")
    tracer = trace.get_tracer("zuul")

    def __init__(self, zuul_web, connection):
        self.connection = connection
        self.zuul_web = zuul_web
        self.event_queue = ConnectionEventQueue(
            self.zuul_web.zk_client,
            self.connection.connection_name,
            None
        )

    def _validate_token(self, headers):
        try:
            event_token = headers['x-gitlab-token']
        except KeyError:
            raise cherrypy.HTTPError(401, 'x-gitlab-token header missing.')

        configured_token = self.connection.webhook_token
        if not configured_token == event_token:
            self.log.debug(
                "Missmatch (Incoming token: %s, Configured token: %s)" % (
                    event_token, configured_token))
            raise cherrypy.HTTPError(
                401,
                'Token does not match the server side configured token')

    @cherrypy.expose
    @cherrypy.tools.json_out(content_type='application/json; charset=utf-8')
    @tracer.start_as_current_span("GitlabEvent")
    def payload(self):
        headers = dict()
        for key, value in cherrypy.request.headers.items():
            headers[key.lower()] = value
        body = cherrypy.request.body.read()
        self.log.info("Event header: %s" % headers)
        self.log.info("Event body: %s" % body)
        self._validate_token(headers)
        json_payload = json.loads(body.decode('utf-8'))

        data = {
            'payload': json_payload,
            'span_context': tracing.getSpanContext(trace.get_current_span()),
        }
        self.event_queue.put(data)
        return data


def getSchema():
    return v.Any(str, v.Schema(dict))
