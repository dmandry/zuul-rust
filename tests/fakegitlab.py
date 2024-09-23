# Copyright 2016 Red Hat, Inc.
# Copyright 2021 Acme Gating, LLC
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

from collections import defaultdict, namedtuple
from contextlib import contextmanager
import datetime
import http.server
import json
import logging
import os
import re
import socketserver
import threading
import time
import urllib.parse

import zuul.driver.gitlab.gitlabconnection as gitlabconnection
from tests.util import random_sha1

import git
from git.util import IterableList
import requests

FakeGitlabBranch = namedtuple('Branch',
                              ('name', 'protected', 'upstream_root'))


class GitlabWebServer(object):

    def __init__(self, merge_requests):
        super(GitlabWebServer, self).__init__()
        self.merge_requests = merge_requests
        self.fake_repos = defaultdict(lambda: IterableList('name'))
        # A dictionary so we can mutate it
        self.options = dict(
            community_edition=False,
            delayed_complete_mr=0,
            uncomplete_mr=False)
        self.stats = {"get_mr": 0}

    def start(self):
        merge_requests = self.merge_requests
        fake_repos = self.fake_repos
        options = self.options
        stats = self.stats

        class Server(http.server.SimpleHTTPRequestHandler):
            log = logging.getLogger("zuul.test.GitlabWebServer")

            branches_re = re.compile(r'.+/projects/(?P<project>.+)/'
                                     r'repository/branches\\?.*$')
            branch_re = re.compile(r'.+/projects/(?P<project>.+)/'
                                   r'repository/branches/(?P<branch>.+)$')
            mr_re = re.compile(r'.+/projects/(?P<project>.+)/'
                               r'merge_requests/(?P<mr>\d+)$')
            mr_approvals_re = re.compile(
                r'.+/projects/(?P<project>.+)/'
                r'merge_requests/(?P<mr>\d+)/approvals$')

            mr_notes_re = re.compile(
                r'.+/projects/(?P<project>.+)/'
                r'merge_requests/(?P<mr>\d+)/notes$')
            mr_approve_re = re.compile(
                r'.+/projects/(?P<project>.+)/'
                r'merge_requests/(?P<mr>\d+)/approve$')
            mr_unapprove_re = re.compile(
                r'.+/projects/(?P<project>.+)/'
                r'merge_requests/(?P<mr>\d+)/unapprove$')

            mr_merge_re = re.compile(r'.+/projects/(?P<project>.+)/'
                                     r'merge_requests/(?P<mr>\d+)/merge$')
            mr_update_re = re.compile(r'.+/projects/(?P<project>.+)/'
                                      r'merge_requests/(?P<mr>\d+)$')

            def _get_mr(self, project, number):
                project = urllib.parse.unquote(project)
                mr = merge_requests.get(project, {}).get(number)
                if not mr:
                    # Find out what gitlab does in this case
                    raise NotImplementedError()
                return mr

            def do_GET(self):
                path = self.path
                self.log.debug("Got GET %s", path)

                m = self.mr_re.match(path)
                if m:
                    return self.get_mr(**m.groupdict())
                m = self.mr_approvals_re.match(path)
                if m:
                    return self.get_mr_approvals(**m.groupdict())
                m = self.branch_re.match(path)
                if m:
                    return self.get_branch(**m.groupdict())
                m = self.branches_re.match(path)
                if m:
                    return self.get_branches(path, **m.groupdict())
                self.send_response(500)
                self.end_headers()

            def do_POST(self):
                path = self.path
                self.log.debug("Got POST %s", path)

                data = self.rfile.read(int(self.headers['Content-Length']))
                if (self.headers['Content-Type'] ==
                    'application/x-www-form-urlencoded'):
                    data = urllib.parse.parse_qs(data.decode('utf-8'))

                self.log.debug("Got data %s", data)

                m = self.mr_notes_re.match(path)
                if m:
                    return self.post_mr_notes(data, **m.groupdict())
                m = self.mr_approve_re.match(path)
                if m:
                    return self.post_mr_approve(data, **m.groupdict())
                m = self.mr_unapprove_re.match(path)
                if m:
                    return self.post_mr_unapprove(data, **m.groupdict())
                self.send_response(500)
                self.end_headers()

            def do_PUT(self):
                path = self.path
                self.log.debug("Got PUT %s", path)

                data = self.rfile.read(int(self.headers['Content-Length']))
                if (self.headers['Content-Type'] ==
                    'application/x-www-form-urlencoded'):
                    data = urllib.parse.parse_qs(data.decode('utf-8'))

                self.log.debug("Got data %s", data)

                m = self.mr_merge_re.match(path)
                if m:
                    return self.put_mr_merge(data, **m.groupdict())
                m = self.mr_update_re.match(path)
                if m:
                    return self.put_mr_update(data, **m.groupdict())
                self.send_response(500)
                self.end_headers()

            def send_data(self, data, code=200):
                data = json.dumps(data).encode('utf-8')
                self.send_response(code)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', len(data))
                self.end_headers()
                self.wfile.write(data)

            def get_mr(self, project, mr):
                stats["get_mr"] += 1
                mr = self._get_mr(project, mr)
                data = {
                    'target_branch': mr.branch,
                    'title': mr.subject,
                    'state': mr.state,
                    'description': mr.description,
                    'author': {
                        'name': 'Administrator',
                        'username': 'admin'
                    },
                    'updated_at':
                    mr.updated_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                    'sha': mr.sha,
                    'labels': mr.labels,
                    'blocking_discussions_resolved':
                        mr.blocking_discussions_resolved,
                    'merged_at': mr.merged_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                    if mr.merged_at else mr.merged_at,
                    'merge_status': mr.merge_status,
                }
                if options['delayed_complete_mr'] and \
                        time.monotonic() < options['delayed_complete_mr']:
                    diff_refs = None
                elif options['uncomplete_mr']:
                    diff_refs = None
                else:
                    diff_refs = {
                        'base_sha': mr.base_sha,
                        'head_sha': mr.sha,
                        'start_sha': 'c380d3acebd181f13629a25d2e2acca46ffe1e00'
                    }
                data['diff_refs'] = diff_refs
                self.send_data(data)

            def get_mr_approvals(self, project, mr):
                mr = self._get_mr(project, mr)
                if not options['community_edition']:
                    self.send_data({
                        'approvals_left': 0 if mr.approved else 1,
                    })
                else:
                    self.send_data({
                        'approved': mr.approved,
                    })

            def get_branch(self, project, branch):
                project = urllib.parse.unquote(project)
                branch = urllib.parse.unquote(branch)
                owner, name = project.split('/')
                if branch in fake_repos[(owner, name)]:
                    protected = fake_repos[(owner, name)][branch].protected
                    upstream_root = fake_repos[(owner, name)][
                        branch].upstream_root
                    repo_path = os.path.join(upstream_root, owner, name)
                    repo = git.Repo(repo_path)
                    sha = repo.heads[branch].commit.hexsha
                    self.send_data({
                        'protected': protected,
                        'commit': {
                            'id': sha,
                        },
                    })
                else:
                    return self.send_data({}, code=404)

            def get_branches(self, url, project):
                project = urllib.parse.unquote(project).split('/')
                req = urllib.parse.urlparse(url)
                query = urllib.parse.parse_qs(req.query)
                per_page = int(query["per_page"][0])
                page = int(query["page"][0])

                repo = fake_repos[tuple(project)]
                first_entry = (page - 1) * per_page
                last_entry = min(len(repo), (page) * per_page)

                if first_entry >= len(repo):
                    branches = []
                else:
                    branches = [{'name': repo[i].name,
                                 'protected': repo[i].protected}
                                for i in range(first_entry, last_entry)]
                self.send_data(branches)

            def post_mr_notes(self, data, project, mr):
                mr = self._get_mr(project, mr)
                mr.addNote(data['body'][0])
                self.send_data({})

            def post_mr_approve(self, data, project, mr):
                assert 'sha' in data
                mr = self._get_mr(project, mr)
                if data['sha'][0] != mr.sha:
                    return self.send_data(
                        {'message': 'SHA does not match HEAD of source '
                         'branch: <new_sha>'}, code=409)
                if mr.approved:
                    return self.send_data(
                        {'message': '401 Unauthorized'}, code=401)
                mr.approved = True
                self.send_data({}, code=201)

            def post_mr_unapprove(self, data, project, mr):
                mr = self._get_mr(project, mr)
                if not mr.approved:
                    return self.send_data(
                        {'message': "404 Not Found"}, code=404)
                mr.approved = False
                self.send_data({}, code=201)

            def put_mr_merge(self, data, project, mr):
                mr = self._get_mr(project, mr)
                squash = None
                if data and isinstance(data, dict):
                    squash = data.get('squash')
                mr.mergeMergeRequest(squash)
                self.send_data({'state': 'merged'})

            def put_mr_update(self, data, project, mr):
                mr = self._get_mr(project, mr)
                labels = set(mr.labels)
                add_labels = data.get('add_labels', [''])[0].split(',')
                remove_labels = data.get('remove_labels', [''])[0].split(',')
                labels = labels - set(remove_labels)
                labels = labels | set(add_labels)
                mr.labels = list(labels)
                self.send_data({})

            def log_message(self, fmt, *args):
                self.log.debug(fmt, *args)

        self.httpd = socketserver.ThreadingTCPServer(('', 0), Server)
        self.port = self.httpd.socket.getsockname()[1]
        self.thread = threading.Thread(name='GitlabWebServer',
                                       target=self.httpd.serve_forever)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        self.httpd.shutdown()
        self.thread.join()
        self.httpd.server_close()


class FakeGitlabConnection(gitlabconnection.GitlabConnection):
    log = logging.getLogger("zuul.test.FakeGitlabConnection")

    def __init__(self, driver, connection_name, connection_config,
                 changes_db=None, upstream_root=None):
        self.merge_requests = changes_db
        self.upstream_root = upstream_root
        self.mr_number = 0

        self._test_web_server = GitlabWebServer(changes_db)
        self._test_web_server.start()
        self._test_baseurl = 'http://localhost:%s' % self._test_web_server.port
        connection_config['baseurl'] = self._test_baseurl

        super(FakeGitlabConnection, self).__init__(driver, connection_name,
                                                   connection_config)

    def onStop(self):
        super().onStop()
        self._test_web_server.stop()

    def addProject(self, project):
        super(FakeGitlabConnection, self).addProject(project)
        self.addProjectByName(project.name)

    def addProjectByName(self, project_name):
        owner, proj = project_name.split('/')
        repo = self._test_web_server.fake_repos[(owner, proj)]
        branch = FakeGitlabBranch('master', False, self.upstream_root)
        if 'master' not in repo:
            repo.append(branch)

    def protectBranch(self, owner, project, branch, protected=True):
        if branch in self._test_web_server.fake_repos[(owner, project)]:
            del self._test_web_server.fake_repos[(owner, project)][branch]
        fake_branch = FakeGitlabBranch(branch, protected, self.upstream_root)
        self._test_web_server.fake_repos[(owner, project)].append(fake_branch)

    def _test_isBranchProtected(self, owner, project, branch):
        fake_repos = self._test_web_server.fake_repos
        if branch in fake_repos[(owner, project)]:
            return fake_repos[(owner, project)][branch].protected
        return False

    def deleteBranch(self, owner, project, branch):
        if branch in self._test_web_server.fake_repos[(owner, project)]:
            del self._test_web_server.fake_repos[(owner, project)][branch]

    def getGitUrl(self, project):
        return 'file://' + os.path.join(self.upstream_root, project.name)

    def real_getGitUrl(self, project):
        return super(FakeGitlabConnection, self).getGitUrl(project)

    def openFakeMergeRequest(self, project,
                             branch, title, description='', files=[],
                             base_sha=None):
        self.mr_number += 1
        merge_request = FakeGitlabMergeRequest(
            self, self.mr_number, project, branch, title, self.upstream_root,
            files=files, description=description, base_sha=base_sha)
        self.merge_requests.setdefault(
            project, {})[str(self.mr_number)] = merge_request
        return merge_request

    def emitEvent(self, event, use_zuulweb=False, project=None):
        name, payload = event
        if use_zuulweb:
            payload = json.dumps(payload).encode('utf-8')
            headers = {'x-gitlab-token': self.webhook_token}
            return requests.post(
                'http://127.0.0.1:%s/api/connection/%s/payload'
                % (self.zuul_web_port, self.connection_name),
                data=payload, headers=headers)
        else:
            data = {'payload': payload}
            self.event_queue.put(data)
            return data

    def setZuulWebPort(self, port):
        self.zuul_web_port = port

    def getPushEvent(
            self, project, before=None, after=None,
            branch='refs/heads/master',
            added_files=None, removed_files=None,
            modified_files=None,
            ref_protected=None,
    ):
        if added_files is None:
            added_files = []
        if removed_files is None:
            removed_files = []
        if modified_files is None:
            modified_files = []

        if ref_protected is None:
            bname = branch.replace('refs/heads/', '')
            owner, pname = project.split('/', 1)
            ref_protected = self._test_isBranchProtected(owner, pname, bname)

        name = 'gl_push'
        if not after:
            repo_path = os.path.join(self.upstream_root, project)
            repo = git.Repo(repo_path)
            after = repo.head.commit.hexsha
        data = {
            'object_kind': 'push',
            'before': before or '1' * 40,
            'after': after,
            'ref': branch,
            'ref_protected': ref_protected,
            'project': {
                'path_with_namespace': project
            },
            'commits': [
                {
                    'added': added_files,
                    'removed': removed_files,
                    'modified': modified_files
                }
            ],
            'total_commits_count': 1,
        }
        return (name, data)

    def getGitTagEvent(self, project, tag, sha):
        name = 'gl_push'
        data = {
            'object_kind': 'tag_push',
            'before': '0' * 40,
            'after': sha,
            'ref': 'refs/tags/%s' % tag,
            'ref_protected': False,
            'project': {
                'path_with_namespace': project
            },
        }
        return (name, data)

    @contextmanager
    def enable_community_edition(self):
        self._test_web_server.options['community_edition'] = True
        yield
        self._test_web_server.options['community_edition'] = False

    @contextmanager
    def enable_delayed_complete_mr(self, complete_at):
        self._test_web_server.options['delayed_complete_mr'] = complete_at
        yield
        self._test_web_server.options['delayed_complete_mr'] = 0

    @contextmanager
    def enable_uncomplete_mr(self):
        self._test_web_server.options['uncomplete_mr'] = True
        orig = self.gl_client.get_mr_wait_factor
        self.gl_client.get_mr_wait_factor = 0.1
        yield
        self.gl_client.get_mr_wait_factor = orig
        self._test_web_server.options['uncomplete_mr'] = False


class GitlabChangeReference(git.Reference):
    _common_path_default = "refs/merge-requests"
    _points_to_commits_only = True


class FakeGitlabMergeRequest(object):
    log = logging.getLogger("zuul.test.FakeGitlabMergeRequest")

    def __init__(self, gitlab, number, project, branch,
                 subject, upstream_root, files=[], description='',
                 base_sha=None):
        self.source_hostname = gitlab.canonical_hostname
        self.gitlab_server = gitlab.server
        self.number = number
        self.project = project
        self.branch = branch
        self.subject = subject
        self.description = description
        self.upstream_root = upstream_root
        self.number_of_commits = 0
        self.created_at = datetime.datetime.now(datetime.timezone.utc)
        self.updated_at = self.created_at
        self.merged_at = None
        self.sha = None
        self.state = 'opened'
        self.is_merged = False
        self.merge_status = 'can_be_merged'
        self.squash_merge = None
        self.labels = []
        self.notes = []
        self.url = "https://%s/%s/merge_requests/%s" % (
            self.gitlab_server, self.project, self.number)
        self.base_sha = base_sha
        self.approved = False
        self.blocking_discussions_resolved = True
        self.mr_ref = self._createMRRef(base_sha=base_sha)
        self._addCommitInMR(files=files)

    def _getRepo(self):
        repo_path = os.path.join(self.upstream_root, self.project)
        return git.Repo(repo_path)

    def _createMRRef(self, base_sha=None):
        base_sha = base_sha or 'refs/tags/init'
        repo = self._getRepo()
        return GitlabChangeReference.create(
            repo, self.getMRReference(), base_sha)

    def getMRReference(self):
        return '%s/head' % self.number

    def addNote(self, body):
        self.notes.append(
            {
                "body": body,
                "created_at": datetime.datetime.now(datetime.timezone.utc),
            }
        )

    def addCommit(self, files=[], delete_files=None):
        self._addCommitInMR(files=files, delete_files=delete_files)
        self._updateTimeStamp()

    def closeMergeRequest(self):
        self.state = 'closed'
        self._updateTimeStamp()

    def mergeMergeRequest(self, squash=None):
        self.state = 'merged'
        self.is_merged = True
        self.squash_merge = squash
        self._updateTimeStamp()
        self.merged_at = self.updated_at

    def reopenMergeRequest(self):
        self.state = 'opened'
        self._updateTimeStamp()
        self.merged_at = None

    def _addCommitInMR(self, files=[], delete_files=None, reset=False):
        repo = self._getRepo()
        ref = repo.references[self.getMRReference()]
        if reset:
            self.number_of_commits = 0
            ref.set_object('refs/tags/init')
        self.number_of_commits += 1
        repo.head.reference = ref
        repo.git.clean('-x', '-f', '-d')

        if files:
            self.files = files
        elif not delete_files:
            fn = '%s-%s' % (self.branch.replace('/', '_'), self.number)
            self.files = {fn: "test %s %s\n" % (self.branch, self.number)}
        msg = self.subject + '-' + str(self.number_of_commits)
        for fn, content in self.files.items():
            fn = os.path.join(repo.working_dir, fn)
            with open(fn, 'w') as f:
                f.write(content)
            repo.index.add([fn])

        if delete_files:
            for fn in delete_files:
                if fn in self.files:
                    del self.files[fn]
                fn = os.path.join(repo.working_dir, fn)
                repo.index.remove([fn])

        self.sha = repo.index.commit(msg).hexsha

        repo.create_head(self.getMRReference(), self.sha, force=True)
        self.mr_ref.set_commit(self.sha)
        repo.head.reference = 'master'
        repo.git.clean('-x', '-f', '-d')
        repo.heads['master'].checkout()

    def _updateTimeStamp(self):
        self.updated_at = datetime.datetime.now(datetime.timezone.utc)

    def getMergeRequestEvent(self, action, code_change=False,
                             previous_labels=None,
                             reviewers_updated=False):
        name = 'gl_merge_request'
        data = {
            'object_kind': 'merge_request',
            'project': {
                'path_with_namespace': self.project
            },
            'object_attributes': {
                'title': self.subject,
                'created_at': self.created_at.strftime(
                    '%Y-%m-%d %H:%M:%S.%f%z'),
                'updated_at': self.updated_at.strftime(
                    '%Y-%m-%d %H:%M:%S UTC'),
                'iid': self.number,
                'target_branch': self.branch,
                'last_commit': {'id': self.sha},
                'action': action,
                'blocking_discussions_resolved':
                    self.blocking_discussions_resolved
            },
        }
        data['labels'] = [{'title': label} for label in self.labels]

        if action == "update" and code_change:
            data["object_attributes"]["oldrev"] = random_sha1()

        data['changes'] = {}

        if previous_labels is not None:
            data['changes']['labels'] = {
                'previous': [{'title': label} for label in previous_labels],
                'current': data['labels']
            }

        if reviewers_updated:
            data["changes"]["reviewers"] = {'current': [], 'previous': []}

        return (name, data)

    def getMergeRequestOpenedEvent(self):
        return self.getMergeRequestEvent(action='open')

    def getMergeRequestClosedEvent(self):
        return self.getMergeRequestEvent(action='close')

    def getMergeRequestUpdatedEvent(self):
        self.addCommit()
        return self.getMergeRequestEvent(action='update',
                                         code_change=True)

    def getMergeRequestReviewersUpdatedEvent(self):
        return self.getMergeRequestEvent(action='update',
                                         reviewers_updated=True)

    def getMergeRequestMergedEvent(self):
        self.mergeMergeRequest()
        return self.getMergeRequestEvent(action='merge')

    def getMergeRequestMergedPushEvent(self, gitlab, added_files=None,
                                       removed_files=None,
                                       modified_files=None):
        return gitlab.getPushEvent(
            project=self.project,
            branch='refs/heads/%s' % self.branch,
            before=random_sha1(),
            after=self.sha,
            added_files=added_files,
            removed_files=removed_files,
            modified_files=modified_files)

    def getMergeRequestApprovedEvent(self):
        self.approved = True
        return self.getMergeRequestEvent(action='approved')

    def getMergeRequestUnapprovedEvent(self):
        self.approved = False
        return self.getMergeRequestEvent(action='unapproved')

    def getMergeRequestLabeledEvent(self, add_labels=[], remove_labels=[]):
        previous_labels = self.labels
        labels = set(previous_labels)
        labels = labels - set(remove_labels)
        labels = labels | set(add_labels)
        self.labels = list(labels)
        return self.getMergeRequestEvent(action='update',
                                         previous_labels=previous_labels)

    def getMergeRequestCommentedEvent(self, note):
        self.addNote(note)
        note_date = self.notes[-1]['created_at'].strftime(
            '%Y-%m-%d %H:%M:%S UTC')
        name = 'gl_merge_request'
        data = {
            'object_kind': 'note',
            'project': {
                'path_with_namespace': self.project
            },
            'merge_request': {
                'title': self.subject,
                'iid': self.number,
                'target_branch': self.branch,
                'last_commit': {'id': self.sha}
            },
            'object_attributes': {
                'created_at': note_date,
                'updated_at': note_date,
                'note': self.notes[-1]['body'],
            },
        }
        return (name, data)
