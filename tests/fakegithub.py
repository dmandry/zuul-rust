# Copyright 2018 Red Hat, Inc.
# Copyright 2024 Acme Gating, LLC
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

from collections import defaultdict
import datetime
import functools
import json
import logging
import os
import re
import time
import urllib
import uuid
import string
import random

from tests.fake_graphql import getGrapheneSchema
import zuul.driver.github.githubconnection as githubconnection
from zuul.driver.github.githubconnection import utc, GithubClientManager
from tests.util import random_sha1

import git
import github3.exceptions
import requests
from requests.structures import CaseInsensitiveDict
import requests_mock

FAKE_BASE_URL = 'https://example.com/api/v3/'


class GithubChangeReference(git.Reference):
    _common_path_default = "refs/pull"
    _points_to_commits_only = True


class FakeGithubPullRequest:
    _graphene_type = 'PullRequest'

    def __init__(self, github, number, project, branch,
                 subject, upstream_root, files=None,
                 writers=[], body=None, body_text=None, draft=False,
                 mergeable=True, base_sha=None):
        """Creates a new PR with several commits.
        Sends an event about opened PR.

        If the `files` argument is provided it must be a dictionary of
        file names OR FakeFile instances -> content.
        """
        self.id = str(uuid.uuid4())
        self.source_hostname = github.canonical_hostname
        self.github_server = github.server
        self.number = number
        self.project = project
        self.branch = branch
        self.subject = subject
        self.body = body
        self.body_text = body_text
        self.draft = draft
        self.mergeable = mergeable
        self.upstream_root = upstream_root
        # Dictionary of FakeFile -> content
        self.files = {}
        self.comments = []
        self.labels = []
        self.statuses = {}
        self.reviews = []
        self.review_threads = []
        self.writers = []
        self.admins = []
        self.commits = []
        self.updated_at = None
        self.is_merged = False
        self.merge_message = None
        self.state = 'open'
        self.url = 'https://%s/%s/pull/%s' % (self.github_server,
                                              project, number)
        self.base_sha = base_sha
        self.pr_ref = self._createPRRef(base_sha=base_sha)
        self.addCommit(files=files)

    @property
    def head_sha(self):
        return self.commits[-1].sha

    def addCommit(self, files=None, delete_files=None):
        """Adds a commit on top of the actual PR head."""
        self._addCommitToRepo(files=files, delete_files=delete_files)
        self._updateTimeStamp()

    def forcePush(self, files=None):
        """Clears actual commits and add a commit on top of the base."""
        self._addCommitToRepo(files=files, reset=True)
        self._updateTimeStamp()

    def getPullRequestOpenedEvent(self):
        return self._getPullRequestEvent('opened')

    def getPullRequestSynchronizeEvent(self):
        return self._getPullRequestEvent('synchronize')

    def getPullRequestReopenedEvent(self):
        return self._getPullRequestEvent('reopened')

    def getPullRequestClosedEvent(self):
        return self._getPullRequestEvent('closed')

    def getPullRequestEditedEvent(self, old_body=None):
        return self._getPullRequestEvent('edited', old_body)

    def addComment(self, message):
        self.comments.append(message)
        self._updateTimeStamp()

    def getIssueCommentAddedEvent(self, text):
        name = 'issue_comment'
        data = {
            'action': 'created',
            'issue': {
                'number': self.number
            },
            'comment': {
                'body': text
            },
            'repository': {
                'full_name': self.project
            },
            'sender': {
                'login': 'ghuser'
            }
        }
        return (name, data)

    def getCommentAddedEvent(self, text):
        name, data = self.getIssueCommentAddedEvent(text)
        # A PR comment has an additional 'pull_request' key in the issue data
        data['issue']['pull_request'] = {
            'url': 'http://%s/api/v3/repos/%s/pull/%s' % (
                self.github_server, self.project, self.number)
        }
        return (name, data)

    def getReviewAddedEvent(self, review):
        name = 'pull_request_review'
        data = {
            'action': 'submitted',
            'pull_request': {
                'number': self.number,
                'title': self.subject,
                'updated_at': self.updated_at,
                'base': {
                    'ref': self.branch,
                    'repo': {
                        'full_name': self.project
                    }
                },
                'head': {
                    'sha': self.head_sha
                }
            },
            'review': {
                'state': review
            },
            'repository': {
                'full_name': self.project
            },
            'sender': {
                'login': 'ghuser'
            }
        }
        return (name, data)

    def addLabel(self, name):
        if name not in self.labels:
            self.labels.append(name)
            self._updateTimeStamp()
            return self._getLabelEvent(name)

    def removeLabel(self, name):
        if name in self.labels:
            self.labels.remove(name)
            self._updateTimeStamp()
            return self._getUnlabelEvent(name)

    def _getLabelEvent(self, label):
        name = 'pull_request'
        data = {
            'action': 'labeled',
            'pull_request': {
                'number': self.number,
                'updated_at': self.updated_at,
                'base': {
                    'ref': self.branch,
                    'repo': {
                        'full_name': self.project
                    }
                },
                'head': {
                    'sha': self.head_sha
                }
            },
            'label': {
                'name': label
            },
            'sender': {
                'login': 'ghuser'
            }
        }
        return (name, data)

    def _getUnlabelEvent(self, label):
        name = 'pull_request'
        data = {
            'action': 'unlabeled',
            'pull_request': {
                'number': self.number,
                'title': self.subject,
                'updated_at': self.updated_at,
                'base': {
                    'ref': self.branch,
                    'repo': {
                        'full_name': self.project
                    }
                },
                'head': {
                    'sha': self.head_sha,
                    'repo': {
                        'full_name': self.project
                    }
                }
            },
            'label': {
                'name': label
            },
            'sender': {
                'login': 'ghuser'
            }
        }
        return (name, data)

    def convertToDraft(self):
        self.draft = True
        return self._getDraftEvent('converted_to_draft')

    def readyForReview(self):
        self.draft = False
        return self._getDraftEvent('ready_for_review')

    def _getDraftEvent(self, action):
        name = 'pull_request'
        data = {
            'action': action,
            'pull_request': {
                'number': self.number,
                'updated_at': self.updated_at,
                'base': {
                    'ref': self.branch,
                    'repo': {
                        'full_name': self.project
                    }
                },
                'head': {
                    'sha': self.head_sha
                }
            },
            'sender': {
                'login': 'ghuser'
            }
        }
        return (name, data)

    def editBody(self, body):
        old_body = self.body
        self.body = body
        self._updateTimeStamp()
        return self.getPullRequestEditedEvent(old_body=old_body)

    def _getRepo(self):
        repo_path = os.path.join(self.upstream_root, self.project)
        return git.Repo(repo_path)

    def _createPRRef(self, base_sha=None):
        base_sha = base_sha or 'refs/tags/init'
        repo = self._getRepo()
        return GithubChangeReference.create(
            repo, self.getPRReference(), base_sha)

    def _addCommitToRepo(self, files=None, delete_files=None, reset=False):
        repo = self._getRepo()
        ref = repo.references[self.getPRReference()]
        if reset:
            self.commits.clear()
            ref.set_object('refs/tags/init')
        repo.head.reference = ref
        repo.head.reset(working_tree=True)
        repo.git.clean('-x', '-f', '-d')

        if files:
            # Normalize the dictionary of 'Union[str,FakeFile] -> content'
            # to 'FakeFile -> content'.
            normalized_files = {}
            for fn, content in files.items():
                if isinstance(fn, FakeFile):
                    normalized_files[fn] = content
                else:
                    normalized_files[FakeFile(fn)] = content
            self.files.update(normalized_files)
        elif not delete_files:
            fn = '%s-%s' % (self.branch.replace('/', '_'), self.number)
            content = f"test {self.branch} {self.number}\n"
            self.files.update({FakeFile(fn): content})

        msg = self.subject + '-' + str(len(self.commits) + 1)
        for fake_file, content in self.files.items():
            fn = os.path.join(repo.working_dir, fake_file.filename)
            with open(fn, 'w') as f:
                f.write(content)
            repo.index.add([fn])

        if delete_files:
            for fn in delete_files:
                if fn in self.files:
                    del self.files[fn]
                fn = os.path.join(repo.working_dir, fn)
                repo.index.remove([fn])

        sha = repo.index.commit(msg).hexsha
        repo.create_head(self.getPRReference(), sha, force=True)
        self.pr_ref.set_commit(sha)
        # Create an empty set of statuses for the given sha,
        # each sha on a PR may have a status set on it
        self.statuses[sha] = []
        repo.head.reference = 'master'
        repo.head.reset(working_tree=True)
        repo.git.clean('-x', '-f', '-d')
        repo.heads['master'].checkout()

        self.commits.append(FakeCommit(sha))

    def _updateTimeStamp(self):
        self.updated_at = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime())

    def getPRHeadSha(self):
        repo = self._getRepo()
        return repo.references[self.getPRReference()].commit.hexsha

    def addReview(self, user, state, granted_on=None):
        gh_time_format = '%Y-%m-%dT%H:%M:%SZ'
        # convert the timestamp to a str format that would be returned
        # from github as 'submitted_at' in the API response

        if granted_on:
            granted_on = datetime.datetime.utcfromtimestamp(granted_on)
            submitted_at = time.strftime(
                gh_time_format, granted_on.timetuple())
        else:
            # github timestamps only down to the second, so we need to make
            # sure reviews that tests add appear to be added over a period of
            # time in the past and not all at once.
            if not self.reviews:
                # the first review happens 10 mins ago
                offset = 600
            else:
                # subsequent reviews happen 1 minute closer to now
                offset = 600 - (len(self.reviews) * 60)

            granted_on = datetime.datetime.utcfromtimestamp(
                time.time() - offset)
            submitted_at = time.strftime(
                gh_time_format, granted_on.timetuple())

        review = FakeGithubReview({
            'state': state,
            'user': {
                'login': user,
                'email': user + "@example.com",
            },
            'submitted_at': submitted_at,
        })
        self.reviews.append(review)
        return review

    def addReviewThread(self, review):
        # Create and return a new list of reviews which constitute a
        # thread
        thread = FakeGithubReviewThread()
        thread.addReview(review)
        self.review_threads.append(thread)
        return thread

    def getPRReference(self):
        return '%s/head' % self.number

    def _getPullRequestEvent(self, action, old_body=None):
        name = 'pull_request'
        data = {
            'action': action,
            'number': self.number,
            'pull_request': {
                'number': self.number,
                'title': self.subject,
                'updated_at': self.updated_at,
                'base': {
                    'ref': self.branch,
                    'repo': {
                        'full_name': self.project
                    }
                },
                'head': {
                    'sha': self.head_sha,
                    'repo': {
                        'full_name': self.project
                    }
                },
                'body': self.body
            },
            'sender': {
                'login': 'ghuser'
            },
            'repository': {
                'full_name': self.project,
            },
            'installation': {
                'id': 123,
            },
            'changes': {},
            'labels': [{'name': l} for l in self.labels]
        }
        if old_body:
            data['changes']['body'] = {'from': old_body}
        return (name, data)

    def getCommitStatusEvent(self, context, state='success', user='zuul'):
        name = 'status'
        data = {
            'state': state,
            'sha': self.head_sha,
            'name': self.project,
            'description': 'Test results for %s: %s' % (self.head_sha, state),
            'target_url': 'http://zuul/%s' % self.head_sha,
            'branches': [],
            'context': context,
            'sender': {
                'login': user
            }
        }
        return (name, data)

    def getCheckRunRequestedEvent(self, cr_name, app="zuul"):
        name = "check_run"
        data = {
            "action": "rerequested",
            "check_run": {
                "head_sha": self.head_sha,
                "name": cr_name,
                "app": {
                    "slug": app,
                },
            },
            "repository": {
                "full_name": self.project,
            },
        }
        return (name, data)

    def getCheckRunAbortEvent(self, check_run):
        # A check run aborted event can only be created from a FakeCheckRun as
        # we need some information like external_id which is "calculated"
        # during the creation of the check run.
        name = "check_run"
        data = {
            "action": "requested_action",
            "requested_action": {
                "identifier": "abort",
            },
            "check_run": {
                "head_sha": self.head_sha,
                "name": check_run["name"],
                "app": {
                    "slug": check_run["app"]
                },
                "external_id": check_run["external_id"],
            },
            "repository": {
                "full_name": self.project,
            },
        }

        return (name, data)

    def setMerged(self, commit_message):
        self.is_merged = True
        self.merge_message = commit_message

        repo = self._getRepo()
        repo.heads[self.branch].commit = repo.commit(self.head_sha)


class FakeUser(object):
    def __init__(self, login):
        self.login = login
        self.name = login
        self.email = '%s@example.com' % login
        self.html_url = 'https://example.com/%s' % login


class FakeBranch(object):
    def __init__(self, fake_repo, github_data, branch='master',
                 protected=False):
        self.name = branch
        self._fake_repo = fake_repo
        upstream_root = github_data.fake_github_connection.upstream_root
        repo_path = os.path.join(upstream_root, fake_repo.name)
        repo = git.Repo(repo_path)
        self.sha = repo.heads[branch].commit.hexsha

    @property
    def protected(self):
        return self.name in self._fake_repo._branch_protection_rules

    def as_dict(self):
        return {
            'name': self.name,
            'protected': self.protected,
            'commit': {
                'sha': self.sha,
            }
        }


class FakeCreator:
    def __init__(self, login):
        self.login = login


class FakeStatus(object):
    def __init__(self, state, url, description, context, user):
        self.state = state
        self.context = context
        self.creator = FakeCreator(user)
        self._url = url
        self._description = description

    def as_dict(self):
        return {
            'state': self.state,
            'url': self._url,
            'description': self._description,
            'context': self.context,
            'creator': {
                'login': self.creator.login
            }
        }


class FakeApp:
    def __init__(self, name, slug):
        self.name = name
        self.slug = slug


class FakeCheckSuite:
    _graphene_type = 'CheckSuite'

    def __init__(self):
        self.id = str(uuid.uuid4())
        self.runs = []


class FakeCheckRun:
    _graphene_type = 'CheckRun'

    def __init__(self, name, details_url, output, status, conclusion,
                 completed_at, external_id, actions, app):
        if actions is None:
            actions = []

        self.id = str(uuid.uuid4())
        self.name = name
        self.details_url = details_url
        self.output = output
        self.conclusion = conclusion
        self.completed_at = completed_at
        self.external_id = external_id
        self.actions = actions
        self.app = FakeApp(name=app, slug=app)

        # Github automatically sets the status to "completed" if a conclusion
        # is provided.
        if conclusion is not None:
            self.status = "completed"
        else:
            self.status = status

    def as_dict(self):
        return {
            'id': self.id,
            "name": self.name,
            "status": self.status,
            "output": self.output,
            "details_url": self.details_url,
            "conclusion": self.conclusion,
            "completed_at": self.completed_at,
            "external_id": self.external_id,
            "actions": self.actions,
            "app": {
                "slug": self.app.slug,
                "name": self.app.name,
            },
        }

    def update(self, status, conclusion, completed_at, output, details_url,
               external_id, actions):
        self.status = status
        self.conclusion = conclusion
        self.completed_at = completed_at
        self.output = output
        self.details_url = details_url
        self.external_id = external_id
        self.actions = actions


class FakeGithubReview(object):

    def __init__(self, data):
        self.data = data

    def as_dict(self):
        return self.data


class FakeGithubReviewThread(object):

    def __init__(self):
        self.resolved = False
        self.reviews = []

    def addReview(self, review):
        self.reviews.append(review)


class FakeCombinedStatus(object):
    def __init__(self, sha, statuses):
        self.sha = sha
        self.statuses = statuses


class FakeCommit:
    _graphene_type = 'Commit'

    def __init__(self, sha):
        self._statuses = []
        self.sha = sha
        self._check_suites = defaultdict(FakeCheckSuite)
        self.id = str(uuid.uuid4())
        self.signature = None

    def set_status(self, state, url, description, context, user):
        status = FakeStatus(
            state, url, description, context, user)
        # always insert a status to the front of the list, to represent
        # the last status provided for a commit.
        self._statuses.insert(0, status)

    def set_check_run(self, name, details_url, output, status, conclusion,
                      completed_at, external_id, actions, app):
        check_run = FakeCheckRun(
            name,
            details_url,
            output,
            status,
            conclusion,
            completed_at,
            external_id,
            actions,
            app,
        )
        # Always insert a check_run to the front of the list to represent the
        # last check_run provided for a commit.
        check_suite = self._check_suites[app]
        check_suite.runs.insert(0, check_run)
        return check_run

    def get_url(self, path, params=None):
        if path == 'statuses':
            statuses = [s.as_dict() for s in self._statuses]
            return FakeResponse(statuses)
        if path == "check-runs":
            check_runs = [c.as_dict() for c in self.check_runs()]
            resp = {"total_count": len(check_runs), "check_runs": check_runs}
            return FakeResponse(resp)

    def statuses(self):
        return self._statuses

    def check_runs(self):
        check_runs = []
        for suite in self._check_suites.values():
            check_runs.extend(suite.runs)
        return check_runs

    def status(self):
        '''
        Returns the combined status wich only contains the latest statuses of
        the commit together with some other information that we don't need
        here.
        '''
        latest_statuses_by_context = {}
        for status in self._statuses:
            if status.context not in latest_statuses_by_context:
                latest_statuses_by_context[status.context] = status
        combined_statuses = latest_statuses_by_context.values()
        return FakeCombinedStatus(self.sha, combined_statuses)


class FakeRepository(object):
    def __init__(self, name, data):
        self.data = data
        self.name = name
        self._api = FAKE_BASE_URL
        self._branches = [FakeBranch(self, data)]
        self._commits = {}

        # Simple dictionary to store permission values per feature (e.g.
        # checks, Repository contents, Pull requests, Commit statuses, ...).
        # Could be used to just enable/deable a permission (True, False) or
        # provide more specific values like "read" or "read&write". The mocked
        # functionality in the FakeRepository class should then check for this
        # value and raise an appropriate exception like a production Github
        # would do in case the permission is not sufficient or missing at all.
        self._permissions = {}

        # List of branch protection rules
        self._branch_protection_rules = defaultdict(FakeBranchProtectionRule)
        self._repodata = {
            'allow_merge_commit': True,
            'allow_squash_merge': True,
            'allow_rebase_merge': True,
            'default_branch': 'master',
        }

        # fail the next commit requests with 404
        self.fail_not_found = 0

    def branches(self, protected=False):
        if protected:
            # simulate there is no protected branch
            return [b for b in self._branches if b.protected]
        return self._branches

    def _set_branch_protection(self, branch_name, protected=True,
                               contexts=None, require_review=False,
                               require_conversation_resolution=False,
                               require_commit_signatures=False,
                               locked=False):
        if not protected:
            if branch_name in self._branch_protection_rules:
                del self._branch_protection_rules[branch_name]
            return

        rule = self._branch_protection_rules[branch_name]
        rule.pattern = branch_name
        rule.required_contexts = contexts or []
        rule.require_reviews = require_review
        rule.require_conversation_resolution = require_conversation_resolution
        rule.require_commit_signatures = require_commit_signatures
        rule.matching_refs = [branch_name]
        rule.lock_branch = locked
        return rule

    def _set_permission(self, key, value):
        # NOTE (felix): Currently, this is only used to mock a repo with
        # missing checks API permissions. But we could also use it to test
        # arbitrary permission values like missing write, but only read
        # permissions for a specific functionality.
        self._permissions[key] = value

    def _build_url(self, *args, **kwargs):
        path_args = ['repos', self.name]
        path_args.extend(args)
        fakepath = '/'.join(path_args)
        return FAKE_BASE_URL + fakepath

    def _get(self, url, headers=None):
        client = FakeGithubClient(data=self.data)
        return client.session.get(url, headers)

    def _create_branch(self, branch):
        self._branches.append((FakeBranch(self, self.data, branch=branch)))

    def _delete_branch(self, branch_name):
        self._branches = [b for b in self._branches if b.name != branch_name]

    def create_status(self, sha, state, url, description, context,
                      user='zuul'):
        # Since we're bypassing github API, which would require a user, we
        # default the user as 'zuul' here.
        commit = self._commits.get(sha, None)
        if commit is None:
            commit = FakeCommit(sha)
            self._commits[sha] = commit
        commit.set_status(state, url, description, context, user)

    def create_check_run(self, head_sha, name, details_url=None, output=None,
                         status=None, conclusion=None, completed_at=None,
                         external_id=None, actions=None, app="zuul"):

        # Raise the appropriate github3 exception in case we don't have
        # permission to access the checks API
        if self._permissions.get("checks") is False:
            # To create a proper github3 exception, we need to mock a response
            # object
            raise github3.exceptions.ForbiddenError(
                FakeResponse("Resource not accessible by integration", 403)
            )

        commit = self._commits.get(head_sha, None)
        if commit is None:
            commit = FakeCommit(head_sha)
            self._commits[head_sha] = commit
        commit.set_check_run(
            name,
            details_url,
            output,
            status,
            conclusion,
            completed_at,
            external_id,
            actions,
            app,
        )

    def commit(self, sha):

        if self.fail_not_found > 0:
            self.fail_not_found -= 1
            resp = FakeResponse(404, 'Not found')
            raise github3.exceptions.NotFoundError(resp)

        commit = self._commits.get(sha, None)
        if commit is None:
            commit = FakeCommit(sha)
            self._commits[sha] = commit
        return commit

    def get_url(self, path, params=None):
        if '/' in path:
            entity, request = path.split('/', 1)
        else:
            entity = path
            request = None

        if entity == 'branches':
            return self.get_url_branches(request, params=params)
        if entity == 'collaborators':
            return self.get_url_collaborators(request)
        if entity == 'commits':
            return self.get_url_commits(request, params=params)
        if entity == '':
            return self.get_url_repo()
        else:
            return None

    def get_url_branches(self, path, params=None):
        if path is None:
            # request wants a branch list
            return self.get_url_branch_list(params)

        elements = path.split('/')

        entity = elements[-1]
        if entity == 'protection':
            branch = '/'.join(elements[0:-1])
            return self.get_url_protection(branch)
        else:
            # fall back to treat all elements as branch
            branch = '/'.join(elements)
            return self.get_url_branch(branch)

    def get_url_commits(self, path, params=None):
        if '/' in path:
            sha, request = path.split('/', 1)
        else:
            sha = path
            request = None
        commit = self._commits.get(sha)

        # Commits are created lazy so check if there is a PR with the correct
        # head sha.
        if commit is None:
            pull_requests = [pr for pr in self.data.pull_requests.values()
                             if pr.head_sha == sha]
            if pull_requests:
                commit = FakeCommit(sha)
                self._commits[sha] = commit

        if not commit:
            return FakeResponse({}, 404)

        return commit.get_url(request, params=params)

    def get_url_branch_list(self, params):
        if params.get('protected') == 1:
            exclude_unprotected = True
        else:
            exclude_unprotected = False
        branches = [x.as_dict() for x in self.branches(exclude_unprotected)]

        return FakeResponse(branches, 200)

    def get_url_branch(self, branch_name):
        for branch in self._branches:
            if branch.name == branch_name:
                return FakeResponse(branch.as_dict())
        return FakeResponse(None, 404)

    def get_url_collaborators(self, path):
        login, entity = path.split('/')

        if entity == 'permission':
            owner, proj = self.name.split('/')
            permission = None
            for pr in self.data.pull_requests.values():
                pr_owner, pr_project = pr.project.split('/')
                if (pr_owner == owner and proj == pr_project):
                    if login in pr.admins:
                        permission = 'admin'
                        break
                    elif login in pr.writers:
                        permission = 'write'
                        break
                    else:
                        permission = 'read'
            data = {
                'permission': permission,
            }
            return FakeResponse(data)
        else:
            return None

    def get_url_protection(self, branch):
        rule = self._branch_protection_rules.get(branch)

        if not rule:
            # Note that GitHub returns 404 if branch protection is off so do
            # the same here as well
            return FakeResponse({}, 404)
        data = {
            'required_status_checks': {
                'contexts': rule.required_contexts
            }
        }
        return FakeResponse(data)

    def get_url_repo(self):
        return FakeResponse(self._repodata)

    def pull_requests(self, state=None, sort=None, direction=None):
        # sort and direction are unused currently, but present to match
        # real world call signatures.
        pulls = []
        for pull in self.data.pull_requests.values():
            if pull.project != self.name:
                continue
            if state and pull.state != state:
                continue
            pulls.append(FakePull(pull))
        return pulls


class FakeIssue(object):
    def __init__(self, fake_pull_request):
        self._fake_pull_request = fake_pull_request

    def pull_request(self):
        return FakePull(self._fake_pull_request)

    @property
    def number(self):
        return self._fake_pull_request.number


@functools.total_ordering
class FakeFile(object):
    def __init__(self, filename, previous_filename=None):
        self.filename = filename
        if previous_filename is not None:
            self.previous_filename = previous_filename

    def __eq__(self, other):
        return self.filename == other.filename

    def __lt__(self, other):
        return self.filename < other.filename

    __hash__ = object.__hash__


class FakePull(object):
    def __init__(self, fake_pull_request):
        self._fake_pull_request = fake_pull_request

    def issue(self):
        return FakeIssue(self._fake_pull_request)

    def files(self):
        # Github lists max. 300 files of a PR in alphabetical order
        return sorted(self._fake_pull_request.files)[:300]

    def reviews(self):
        return self._fake_pull_request.reviews

    def create_review(self, body, commit_id, event):
        review = FakeGithubReview({
            'state': event,
            'user': {
                'login': 'fakezuul',
                'email': 'fakezuul@fake.test',
            },
            'submitted_at': time.gmtime(),
        })
        self._fake_pull_request.reviews.append(review)
        return review

    def as_dict(self):
        pr = self._fake_pull_request
        server = pr.github_server
        data = {
            'number': pr.number,
            'title': pr.subject,
            'url': 'https://%s/api/v3/%s/pulls/%s' % (
                server, pr.project, pr.number
            ),
            'html_url': 'https://%s/%s/pull/%s' % (
                server, pr.project, pr.number
            ),
            'updated_at': pr.updated_at,
            'base': {
                'repo': {
                    'full_name': pr.project
                },
                'ref': pr.branch,
                'sha': pr.base_sha,
            },
            'user': {
                'login': 'octocat'
            },
            'draft': pr.draft,
            'mergeable': pr.mergeable,
            'state': pr.state,
            'head': {
                'sha': pr.head_sha,
                'ref': pr.getPRReference(),
                'repo': {
                    'full_name': pr.project
                }
            },
            'merged': pr.is_merged,
            'body': pr.body,
            'body_text': pr.body_text,
            'changed_files': len(pr.files),
            'labels': [{'name': l} for l in pr.labels]
        }
        return data


class FakeIssueSearchResult(object):
    def __init__(self, issue):
        self.issue = issue


class FakeResponse(object):
    def __init__(self, data, status_code=200, status_message='OK'):
        self.status_code = status_code
        self.status_message = status_message
        self.data = data
        self.links = {}

    @property
    def content(self):
        # Building github3 exceptions requires a Response object with the
        # content attribute set.
        return self.data

    def json(self):
        return self.data

    def raise_for_status(self):
        if 400 <= self.status_code < 600:
            if isinstance(self.data, str):
                text = '{} {}'.format(self.status_code, self.data)
            else:
                text = '{} {}'.format(self.status_code, self.status_message)
            raise requests.HTTPError(text, response=self)


class FakeGithubSession(object):

    def __init__(self, client):
        self.client = client
        self.headers = CaseInsensitiveDict()
        self._base_url = None
        self.schema = getGrapheneSchema()

        # Imitate hooks dict. This will be unused and ignored in the tests.
        self.hooks = {
            'response': []
        }

    def build_url(self, *args):
        fakepath = '/'.join(args)
        return FAKE_BASE_URL + fakepath

    def get(self, url, headers=None, params=None, allow_redirects=True):
        request = url
        if request.startswith(FAKE_BASE_URL):
            request = request[len(FAKE_BASE_URL):]

        entity, request = request.split('/', 1)

        if entity == 'repos':
            return self.get_repo(request, params=params)
        else:
            # unknown entity to process
            return None

    def post(self, url, data=None, headers=None, params=None, json=None):

        # Handle graphql
        if json and json.get('query'):
            query = json.get('query')
            variables = json.get('variables')
            result = self.schema.execute(
                query, variables=variables, context=self.client)
            if result.errors:
                # Note that github really returns 200 and an errors field in
                # case of an error.
                return FakeResponse({'errors': result.errors}, 200)
            return FakeResponse({'data': result.data}, 200)

        # Handle creating comments
        match = re.match(r'.+/repos/(.+)/issues/(\d+)/comments$', url)
        if match:
            project, pr_number = match.groups()
            project = urllib.parse.unquote(project)
            self.client._data.reports.append((project, pr_number, 'comment'))
            pull_request = self.client._data.pull_requests[int(pr_number)]
            pull_request.addComment(json['body'])
            return FakeResponse(None, 200)

        # Handle access token creation
        if re.match(r'.*/app/installations/.*/access_tokens', url):
            expiry = (datetime.datetime.now(utc) + datetime.timedelta(
                minutes=60)).replace(microsecond=0).isoformat()
            install_id = url.split('/')[-2]
            data = {
                'token': 'token-%s' % install_id,
                'expires_at': expiry,
            }
            return FakeResponse(data, 201)

        # Handle check run creation
        match = re.match(r'.*/repos/(.*)/check-runs$', url)
        if match:
            if self.client._data.fail_check_run_creation:
                return FakeResponse('Internal server error', 500)

            org, reponame = match.groups()[0].split('/', 1)
            repo = self.client._data.repos.get((org, reponame))

            if repo._permissions.get("checks") is False:
                # To create a proper github3 exception, we need to mock a
                # response object
                return FakeResponse(
                    "Resource not accessible by integration", 403)

            head_sha = json.get('head_sha')
            commit = repo._commits.get(head_sha, None)
            if commit is None:
                commit = FakeCommit(head_sha)
                repo._commits[head_sha] = commit
            check_run = commit.set_check_run(
                json['name'],
                json['details_url'],
                json['output'],
                json.get('status'),
                json.get('conclusion'),
                json.get('completed_at'),
                json['external_id'],
                json['actions'],
                json.get('app', 'zuul'),
            )

            return FakeResponse(check_run.as_dict(), 201)

        return FakeResponse(None, 404)

    def put(self, url, data=None, headers=None, params=None, json=None):
        # Handle pull request merge
        match = re.match(r'.+/repos/(.+)/pulls/(\d+)/merge$', url)
        if match:
            project, pr_number = match.groups()
            project = urllib.parse.unquote(project)
            pr = self.client._data.pull_requests[int(pr_number)]
            conn = self.client._data.fake_github_connection

            # record that this got reported
            self.client._data.reports.append(
                (pr.project, pr.number, 'merge', json["merge_method"]))
            if conn.merge_failure:
                raise Exception('Unknown merge failure')
            if conn.merge_not_allowed_count > 0:
                conn.merge_not_allowed_count -= 1
                # GitHub returns 405 Method not allowed with more details in
                # the body of the response.
                data = {
                    'message': 'Merge not allowed because of fake reason',
                }
                return FakeResponse(data, 405, 'Method not allowed')
            pr.setMerged(json.get("commit_message", ""))
            return FakeResponse({"merged": True, "sha": "fakemergesha"}, 200)

        return FakeResponse(None, 404)

    def patch(self, url, data=None, headers=None, params=None, json=None):

        # Handle check run update
        match = re.match(r'.*/repos/(.*)/check-runs/(.*)$', url)
        if match:
            org, reponame = match.groups()[0].split('/', 1)
            check_run_id = match.groups()[1]
            repo = self.client._data.repos.get((org, reponame))

            # Find the specified check run
            check_runs = [
                check_run
                for commit in repo._commits.values()
                for check_run in commit.check_runs()
                if check_run.id == check_run_id
            ]
            check_run = check_runs[0]

            conclusion = json.get('conclusion')
            if conclusion:
                status = 'completed'
            else:
                status = json['status']
            completed_at = json.get('completed_at')
            if status == 'completed' and not completed_at:
                raise Exception(
                    "Provide completed_at for completed check runs")
            check_run.update(status,
                             conclusion,
                             completed_at,
                             json['output'],
                             json['details_url'],
                             json['external_id'],
                             json['actions'])
            return FakeResponse(check_run.as_dict(), 200)

    def get_repo(self, request, params=None):
        parts = request.split('/', 2)
        if len(parts) == 2:
            org, project = parts
            request = ''
        else:
            org, project, request = parts
        project_name = '{}/{}'.format(org, project)

        repo = self.client.repo_from_project(project_name)

        return repo.get_url(request, params=params)

    def mount(self, prefix, adapter):
        # Don't care in tests
        pass


class FakeBranchProtectionRule:
    _graphene_type = 'BranchProtectionRule'

    def __init__(self):
        self.id = str(uuid.uuid4())
        self.pattern = None
        self.required_contexts = []
        self.require_reviews = False
        self.require_conversation_resolution = False
        self.require_commit_signatures = False
        self.require_codeowners_review = False


class FakeGithubData:
    def __init__(self, pull_requests, fake_github_connection):
        self.pull_requests = pull_requests
        self.repos = {}
        self.reports = []
        self.fail_check_run_creation = False
        self.fake_github_connection = fake_github_connection

    def __repr__(self):
        return ("pull_requests:%s repos:%s reports:%s "
                "fail_check_run_creation:%s" % (
                    self.pull_requests, self.repos, self.reports,
                    self.fail_check_run_creation))


class FakeGithubClient(object):

    def __init__(self, session=None, data=None):
        self._data = data
        self._inst_id = None
        self.session = FakeGithubSession(self)

    def setData(self, data):
        self._data = data

    def setInstId(self, inst_id):
        self._inst_id = inst_id

    def user(self, login):
        return FakeUser(login)

    def repository(self, owner, proj):
        return self._data.repos.get((owner, proj), None)

    def login(self, token):
        pass

    def repo_from_project(self, project):
        # This is a convenience method for the tests.
        owner, proj = project.split('/')
        return self.repository(owner, proj)

    def addProject(self, project):
        owner, proj = project.name.split('/')
        repo = FakeRepository(
            project.name, self._data)
        self._data.repos[(owner, proj)] = repo
        github_config =\
            self._data.fake_github_connection.driver.test_config.driver.github
        if bprs := github_config.get('branch_protection_rules', {}
                                     ).get(project.name, {}):
            for branch, rule_configs in bprs.items():
                for rule_config in rule_configs:
                    repo._set_branch_protection(
                        branch, protected=True, **rule_config)

    def addProjectByName(self, project_name):
        owner, proj = project_name.split('/')
        self._data.repos[(owner, proj)] = FakeRepository(
            project_name, self._data)

    def pull_request(self, owner, project, number):
        fake_pr = self._data.pull_requests[int(number)]
        repo = self.repository(owner, project)
        # Ensure a commit for the head_sha exists so this can be resolved in
        # graphql queries.
        repo._commits.setdefault(
            fake_pr.head_sha,
            FakeCommit(fake_pr.head_sha)
        )
        return FakePull(fake_pr)

    def search_issues(self, query):
        def tokenize(s):
            # Tokenize with handling for quoted substrings.
            # Bit hacky and needs PDA, but our current inputs are
            # constrained enough that this should work.
            s = s[:-len(" type:pr is:open in:body")]
            OR_split = [x.strip() for x in s.split('OR')]
            tokens = [x.strip('"') for x in OR_split]
            return tokens

        def query_is_sha(s):
            return re.match(r'[a-z0-9]{40}', s)

        if query_is_sha(query):
            # Github returns all PR's that contain the sha in their history
            result = []
            for pr in self._data.pull_requests.values():
                # Quick check if head sha matches
                if pr.head_sha == query:
                    result.append(FakeIssueSearchResult(FakeIssue(pr)))
                    continue

                # If head sha doesn't match it still could be in the pr history
                repo = pr._getRepo()
                commits = repo.iter_commits(
                    '%s...%s' % (pr.branch, pr.head_sha))
                for commit in commits:
                    if commit.hexsha == query:
                        result.append(FakeIssueSearchResult(FakeIssue(pr)))
                        continue

            return result

        # Non-SHA queries are of the form:
        #
        #     '"Depends-On: <url>" OR "Depends-On: <url>"
        #      OR ... type:pr is:open in:body'
        #
        # For the tests is currently enough to simply check for the
        # existence of the Depends-On strings in the PR body.
        tokens = tokenize(query)
        terms = set(tokens)
        results = []
        for pr in self._data.pull_requests.values():
            if not pr.body:
                body = ""
            else:
                body = pr.body
            for term in terms:
                if term in body:
                    issue = FakeIssue(pr)
                    results.append(FakeIssueSearchResult(issue))
                    break

        return iter(results)


class FakeGithubEnterpriseClient(FakeGithubClient):

    version = '2.21.0'

    def __init__(self, url, session=None, verify=True):
        super().__init__(session=session)

    def meta(self):
        data = {
            'installed_version': self.version,
        }
        return data


class FakeGithubClientManager(GithubClientManager):
    github_class = FakeGithubClient
    github_enterprise_class = FakeGithubEnterpriseClient

    log = logging.getLogger("zuul.test.FakeGithubClientManager")

    def __init__(self, connection_config):
        super().__init__(connection_config)
        self.record_clients = False
        self.recorded_clients = []
        self.github_data = None

    def getGithubClient(self,
                        project_name=None,
                        zuul_event_id=None):
        client = super().getGithubClient(
            project_name=project_name,
            zuul_event_id=zuul_event_id)

        # Some tests expect the installation id as part of the
        if self.app_id:
            inst_id = self.installation_map.get(project_name)
            client.setInstId(inst_id)

        # The super method creates a fake github client with empty data so
        # add it here.
        client.setData(self.github_data)

        if self.record_clients:
            self.recorded_clients.append(client)
        return client

    def _prime_installation_map(self):
        # Only valid if installed as a github app
        if not self.app_id:
            return

        # github_data.repos is a hash like
        # { ('org', 'project1'): <dataobj>
        #   ('org', 'project2'): <dataobj>,
        #   ('org2', 'project1'): <dataobj>, ... }
        #
        # we don't care about the value. index by org, e.g.
        #
        #  {
        #    'org': ('project1', 'project2')
        #    'org2': ('project1', 'project2')
        #  }
        orgs = defaultdict(list)
        project_id = 1
        for org, project in self.github_data.repos:
            # Each entry is in the format for "repositories" response
            # of GET /installation/repositories
            orgs[org].append({
                'id': project_id,
                'name': project,
                'full_name': '%s/%s' % (org, project)
                # note, lots of other stuff that's not relevant
            })
            project_id += 1

        self.log.debug("GitHub installation mapped to: %s" % orgs)

        # Mock response to GET /app/installations
        app_json = []
        app_projects = []
        app_id = 1

        # Ensure that we ignore suspended apps
        app_json.append(
            {
                'id': app_id,
                'suspended_at': '2021-09-23T01:43:44Z',
                'suspended_by': {
                    'login': 'ianw',
                    'type': 'User',
                    'id': 12345
                }
            })
        app_projects.append([])
        app_id += 1

        for org, projects in orgs.items():
            # We respond as if each org is a different app instance
            #
            # Below we will be sent the app_id in a token to query
            # what projects this app exports.  Keep the projects in a
            # sequential list so we can just look up "projects for app
            # X" == app_projects[X]
            app_projects.append(projects)
            app_json.append(
                {
                    'id': app_id,
                    # Acutally none of this matters, and there's lots
                    # more in a real response.  Padded out just for
                    # example sake.
                    'account': {
                        'login': org,
                        'id': 1234,
                        'type': 'User',
                    },
                    'permissions': {
                        'checks': 'write',
                        'metadata': 'read',
                        'contents': 'read'
                    },
                    'events': ['push',
                               'pull_request'
                               ],
                    'suspended_at': None,
                    'suspended_by': None,
                }
            )
            app_id += 1

        # TODO(ianw) : we could exercise the pagination paths ...
        with requests_mock.Mocker() as m:
            m.get('%s/app/installations' % self.base_url, json=app_json)

            def repositories_callback(request, context):
                # FakeGithubSession gives us an auth token "token
                # token-X" where "X" corresponds to the app id we want
                # the projects for.  apps start at id "1", so the projects
                # to return for this call are app_projects[token-1]
                token = int(request.headers['Authorization'][12:])
                projects = app_projects[token - 1]
                return {
                    'total_count': len(projects),
                    'repositories': projects
                }
            m.get('%s/installation/repositories?per_page=100' % self.base_url,
                  json=repositories_callback)

            # everything mocked now, call real implementation
            super()._prime_installation_map()


class FakeGithubConnection(githubconnection.GithubConnection):
    log = logging.getLogger("zuul.test.FakeGithubConnection")
    client_manager_class = FakeGithubClientManager

    def __init__(self, driver, connection_name, connection_config,
                 changes_db=None, upstream_root=None,
                 git_url_with_auth=False):
        super(FakeGithubConnection, self).__init__(driver, connection_name,
                                                   connection_config)
        self.connection_name = connection_name
        self.pr_number = 0
        self.pull_requests = changes_db
        self.statuses = {}
        self.upstream_root = upstream_root
        self.merge_failure = False
        self.merge_not_allowed_count = 0

        self.github_data = FakeGithubData(changes_db, self)
        self._github_client_manager.github_data = self.github_data

        self.git_url_with_auth = git_url_with_auth

    def setZuulWebPort(self, port):
        self.zuul_web_port = port

    def openFakePullRequest(self, project, branch, subject, files=[],
                            body=None, body_text=None, draft=False,
                            mergeable=True, base_sha=None):
        self.pr_number += 1
        pull_request = FakeGithubPullRequest(
            self, self.pr_number, project, branch, subject, self.upstream_root,
            files=files, body=body, body_text=body_text, draft=draft,
            mergeable=mergeable, base_sha=base_sha)
        self.pull_requests[self.pr_number] = pull_request
        return pull_request

    def getPushEvent(self, project, ref, old_rev=None, new_rev=None,
                     added_files=None, removed_files=None,
                     modified_files=None):
        if added_files is None:
            added_files = []
        if removed_files is None:
            removed_files = []
        if modified_files is None:
            modified_files = []
        if not old_rev:
            old_rev = '0' * 40
        if not new_rev:
            new_rev = random_sha1()
        name = 'push'
        data = {
            'ref': ref,
            'before': old_rev,
            'after': new_rev,
            'repository': {
                'full_name': project
            },
            'commits': [
                {
                    'added': added_files,
                    'removed': removed_files,
                    'modified': modified_files
                }
            ]
        }
        return (name, data)

    def getBranchProtectionRuleEvent(self, project, action):
        name = 'branch_protection_rule'
        data = {
            'action': action,
            'rule': {},
            'repository': {
                'full_name': project,
            }
        }
        return (name, data)

    def getRepositoryEvent(self, repository, action, changes):
        name = 'repository'
        data = {
            'action': action,
            'changes': changes,
            'repository': repository,
        }
        return (name, data)

    def emitEvent(self, event, use_zuulweb=False):
        """Emulates sending the GitHub webhook event to the connection."""
        name, data = event
        payload = json.dumps(data).encode('utf8')
        secret = self.connection_config['webhook_token']
        signature = githubconnection._sign_request(payload, secret)
        headers = {'x-github-event': name,
                   'x-hub-signature': signature,
                   'x-github-delivery': str(uuid.uuid4())}

        if use_zuulweb:
            return requests.post(
                'http://127.0.0.1:%s/api/connection/%s/payload'
                % (self.zuul_web_port, self.connection_name),
                json=data, headers=headers)
        else:
            data = {'headers': headers, 'body': data}
            self.event_queue.put(data)
            return data

    def addProject(self, project):
        # use the original method here and additionally register it in the
        # fake github
        super(FakeGithubConnection, self).addProject(project)
        self.getGithubClient(project.name).addProject(project)

    def getGitUrl(self, project):
        if self.git_url_with_auth:
            auth_token = ''.join(
                random.choice(string.ascii_lowercase) for x in range(8))
            prefix = 'file://x-access-token:%s@' % auth_token
        else:
            prefix = ''
        if self.repo_cache:
            return prefix + os.path.join(self.repo_cache, str(project))
        return prefix + os.path.join(self.upstream_root, str(project))

    def real_getGitUrl(self, project):
        return super(FakeGithubConnection, self).getGitUrl(project)

    def setCommitStatus(self, project, sha, state, url='', description='',
                        context='default', user='zuul', zuul_event_id=None):
        # record that this got reported and call original method
        self.github_data.reports.append(
            (project, sha, 'status', (user, context, state)))
        super(FakeGithubConnection, self).setCommitStatus(
            project, sha, state,
            url=url, description=description, context=context)

    def labelPull(self, project, pr_number, label, zuul_event_id=None):
        # record that this got reported
        self.github_data.reports.append((project, pr_number, 'label', label))
        pull_request = self.pull_requests[int(pr_number)]
        pull_request.addLabel(label)

    def unlabelPull(self, project, pr_number, label, zuul_event_id=None):
        # record that this got reported
        self.github_data.reports.append((project, pr_number, 'unlabel', label))
        pull_request = self.pull_requests[pr_number]
        pull_request.removeLabel(label)
