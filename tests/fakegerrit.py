# Copyright 2012 Hewlett-Packard Development Company, L.P.
# Copyright 2016 Red Hat, Inc.
# Copyright 2021-2024 Acme Gating, LLC
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
import os
import random
import time
import json
import re
import urllib.parse
import socketserver
import datetime
import threading
import string
import copy
import http.server

from zuul.lib.logutil import get_annotated_logger
from zuul.driver.git import gitwatcher
from zuul.driver.gerrit import (
    gerritconnection,
    gerritreporter,
)
from tests.util import FIXTURE_DIR, random_sha1

import git


class GerritChangeReference(git.Reference):
    _common_path_default = "refs/changes"
    _points_to_commits_only = True


class FakeGerritChange(object):
    categories = {'Approved': ('Approved', -1, 1),
                  'Code-Review': ('Code-Review', -2, 2),
                  'Verified': ('Verified', -2, 2)}

    def __init__(self, gerrit, number, project, branch, subject,
                 status='NEW', upstream_root=None, files={},
                 parent=None, merge_parents=None, merge_files=None,
                 merge_parent_files=None,
                 topic=None, empty=False):
        self.cherry_pick = False
        self.source_hostname = gerrit.canonical_hostname
        self.gerrit_baseurl = gerrit.baseurl
        self.reported = 0
        self.notify = None
        self.queried = 0
        self.patchsets = []
        self.number = number
        self.project = project
        self.branch = branch
        self.subject = subject
        self.latest_patchset = 0
        self.depends_on_change = None
        self.depends_on_patchset = None
        self.needed_by_changes = []
        self.fail_merge = False
        self.messages = []
        self.comments = []
        self.checks = {}
        self.checks_history = []
        self.submit_requirements = []
        self.data = {
            'branch': branch,
            'comments': self.comments,
            'commitMessage': subject,
            'createdOn': time.time(),
            'id': 'I' + random_sha1(),
            'lastUpdated': time.time(),
            'number': str(number),
            'open': status == 'NEW',
            'owner': {'email': 'user@example.com',
                      'name': 'User Name',
                      'username': 'username'},
            'patchSets': self.patchsets,
            'project': project,
            'status': status,
            'subject': subject,
            'submitRecords': [],
            'hashtags': [],
            'url': '%s/%s' % (self.gerrit_baseurl.rstrip('/'), number)}

        if topic:
            self.data['topic'] = topic
        self.upstream_root = upstream_root
        self.merge_parent_files = merge_parent_files
        if merge_parents:
            self.addMergePatchset(parents=merge_parents,
                                  merge_files=merge_files)
        else:
            self.addPatchset(files=files, parent=parent, empty=empty)
        if merge_parents:
            self.data['parents'] = merge_parents
        elif parent:
            self.data['parents'] = [parent]
        self.data['submitRecords'] = self.getSubmitRecords()
        self.open = status == 'NEW'

    def __setstate__(self, state):
        self.__dict__.update(state)
        # Only needed for upgrade test when adding new attribute.
        if not hasattr(self, 'cherry_pick'):
            self.cherry_pick = False

    def addFakeChangeToRepo(self, msg, files, large, parent):
        path = os.path.join(self.upstream_root, self.project)
        repo = git.Repo(path)
        if parent is None:
            parent = 'refs/tags/init'
        ref = GerritChangeReference.create(
            repo, '%s/%s/%s' % (str(self.number).zfill(2)[-2:],
                                self.number,
                                self.latest_patchset),
            parent)
        repo.head.reference = ref
        repo.head.reset(working_tree=True)
        repo.git.clean('-x', '-f', '-d')

        path = os.path.join(self.upstream_root, self.project)
        if not large:
            for fn, content in files.items():
                fn = os.path.join(path, fn)
                if content is None:
                    os.unlink(fn)
                    repo.index.remove([fn])
                else:
                    d = os.path.dirname(fn)
                    if not os.path.exists(d):
                        os.makedirs(d)
                    with open(fn, 'w') as f:
                        f.write(content)
                    repo.index.add([fn])
        else:
            for fni in range(100):
                fn = os.path.join(path, str(fni))
                f = open(fn, 'w')
                for ci in range(4096):
                    f.write(random.choice(string.printable))
                f.close()
                repo.index.add([fn])

        r = repo.index.commit(msg)
        repo.head.reference = 'master'
        repo.head.reset(working_tree=True)
        repo.git.clean('-x', '-f', '-d')
        repo.heads['master'].checkout()
        return r

    def addFakeMergeCommitChangeToRepo(self, msg, parents):
        path = os.path.join(self.upstream_root, self.project)
        repo = git.Repo(path)
        ref = GerritChangeReference.create(
            repo, '%s/%s/%s' % (str(self.number).zfill(2)[-2:],
                                self.number,
                                self.latest_patchset),
            parents[0])
        repo.head.reference = ref
        repo.head.reset(working_tree=True)
        repo.git.clean('-x', '-f', '-d')

        repo.index.merge_tree(parents[1])
        parent_commits = [repo.commit(p) for p in parents]
        r = repo.index.commit(msg, parent_commits=parent_commits)

        repo.head.reference = 'master'
        repo.head.reset(working_tree=True)
        repo.git.clean('-x', '-f', '-d')
        repo.heads['master'].checkout()
        return r

    def addPatchset(self, files=None, large=False, parent=None, empty=False):
        self.latest_patchset += 1
        if empty:
            files = {}
        elif not files:
            fn = '%s-%s' % (self.branch.replace('/', '_'), self.number)
            data = ("test %s %s %s\n" %
                    (self.branch, self.number, self.latest_patchset))
            files = {fn: data}
        msg = self.subject + '-' + str(self.latest_patchset)
        c = self.addFakeChangeToRepo(msg, files, large, parent)
        ps_files = [{'file': '/COMMIT_MSG',
                     'type': 'ADDED'},
                    {'file': 'README',
                     'type': 'MODIFIED'}]
        for f in files:
            ps_files.append({'file': f, 'type': 'ADDED'})
        d = {'approvals': [],
             'createdOn': time.time(),
             'files': ps_files,
             'number': str(self.latest_patchset),
             'ref': 'refs/changes/%s/%s/%s' % (str(self.number).zfill(2)[-2:],
                                               self.number,
                                               self.latest_patchset),
             'revision': c.hexsha,
             'uploader': {'email': 'user@example.com',
                          'name': 'User name',
                          'username': 'user'}}
        self.data['currentPatchSet'] = d
        self.patchsets.append(d)
        self.data['submitRecords'] = self.getSubmitRecords()

    def addCherryPickPatchset(self):
        path = os.path.join(self.upstream_root, self.project)
        repo = git.Repo(path)
        self.latest_patchset += 1
        prev = self.patchsets[-1]
        d = {
            'approvals': prev['approvals'],
            'createdOn': time.time(),
            'files': prev['files'],
            'number': str(self.latest_patchset),
            'ref': 'refs/changes/%s/%s/%s' % (str(self.number).zfill(2)[-2:],
                                              self.number,
                                              self.latest_patchset),
            'revision': repo.heads[self.branch].commit.hexsha,
            'uploader': prev['uploader'],
        }
        self.data['currentPatchSet'] = d
        self.patchsets.append(d)
        self.data['submitRecords'] = self.getSubmitRecords()

    def addMergePatchset(self, parents, merge_files=None):
        self.latest_patchset += 1
        if not merge_files:
            merge_files = []
        msg = self.subject + '-' + str(self.latest_patchset)
        c = self.addFakeMergeCommitChangeToRepo(msg, parents)
        ps_files = [{'file': '/COMMIT_MSG',
                     'type': 'ADDED'},
                    {'file': '/MERGE_LIST',
                     'type': 'ADDED'}]
        for f in merge_files:
            ps_files.append({'file': f, 'type': 'ADDED'})
        d = {'approvals': [],
             'createdOn': time.time(),
             'files': ps_files,
             'number': str(self.latest_patchset),
             'ref': 'refs/changes/%s/%s/%s' % (str(self.number).zfill(2)[-2:],
                                               self.number,
                                               self.latest_patchset),
             'revision': c.hexsha,
             'uploader': {'email': 'user@example.com',
                          'name': 'User name',
                          'username': 'user'}}
        self.data['currentPatchSet'] = d
        self.patchsets.append(d)
        self.data['submitRecords'] = self.getSubmitRecords()

    def setCheck(self, checker, reset=False, **kw):
        if reset:
            self.checks[checker] = {'state': 'NOT_STARTED',
                                    'created': str(datetime.datetime.now())}
        chk = self.checks.setdefault(checker, {})
        chk['updated'] = str(datetime.datetime.now())
        for (key, default) in [
                ('state', None),
                ('repository', self.project),
                ('change_number', self.number),
                ('patch_set_id', self.latest_patchset),
                ('checker_uuid', checker),
                ('message', None),
                ('url', None),
                ('started', None),
                ('finished', None),
        ]:
            val = kw.get(key, chk.get(key, default))
            if val is not None:
                chk[key] = val
            elif key in chk:
                del chk[key]
        self.checks_history.append(copy.deepcopy(self.checks))

    def addComment(self, filename, line, message, name, email, username,
                   comment_range=None):
        comment = {
            'file': filename,
            'line': int(line),
            'reviewer': {
                'name': name,
                'email': email,
                'username': username,
            },
            'message': message,
        }
        if comment_range:
            comment['range'] = comment_range
        self.comments.append(comment)

    def getPatchsetCreatedEvent(self, patchset):
        event = {"type": "patchset-created",
                 "change": {"project": self.project,
                            "branch": self.branch,
                            "id": "I5459869c07352a31bfb1e7a8cac379cabfcb25af",
                            "number": str(self.number),
                            "subject": self.subject,
                            "owner": {"name": "User Name"},
                            "url": "https://hostname/3"},
                 "patchSet": self.patchsets[patchset - 1],
                 "uploader": {"name": "User Name"}}
        return event

    def getChangeRestoredEvent(self):
        event = {"type": "change-restored",
                 "change": {"project": self.project,
                            "branch": self.branch,
                            "id": "I5459869c07352a31bfb1e7a8cac379cabfcb25af",
                            "number": str(self.number),
                            "subject": self.subject,
                            "owner": {"name": "User Name"},
                            "url": "https://hostname/3"},
                 "restorer": {"name": "User Name"},
                 "patchSet": self.patchsets[-1],
                 "reason": ""}
        return event

    def getChangeAbandonedEvent(self):
        event = {"type": "change-abandoned",
                 "change": {"project": self.project,
                            "branch": self.branch,
                            "id": "I5459869c07352a31bfb1e7a8cac379cabfcb25af",
                            "number": str(self.number),
                            "subject": self.subject,
                            "owner": {"name": "User Name"},
                            "url": "https://hostname/3"},
                 "abandoner": {"name": "User Name"},
                 "patchSet": self.patchsets[-1],
                 "reason": ""}
        return event

    def getChangeCommentEvent(self, patchset, comment=None,
                              patchsetcomment=None):
        if comment is None and patchsetcomment is None:
            comment = "Patch Set %d:\n\nThis is a comment" % patchset
        elif comment:
            comment = "Patch Set %d:\n\n%s" % (patchset, comment)
        else:  # patchsetcomment is not None
            comment = "Patch Set %d:\n\n(1 comment)" % patchset

        commentevent = {"comment": comment}
        if patchsetcomment:
            commentevent.update(
                {'patchSetComments':
                    {"/PATCHSET_LEVEL": [{"message": patchsetcomment}]}
                }
            )

        event = {"type": "comment-added",
                 "change": {"project": self.project,
                            "branch": self.branch,
                            "id": "I5459869c07352a31bfb1e7a8cac379cabfcb25af",
                            "number": str(self.number),
                            "subject": self.subject,
                            "owner": {"name": "User Name"},
                            "url": "https://hostname/3"},
                 "patchSet": self.patchsets[patchset - 1],
                 "author": {"name": "User Name"},
                 "approvals": [{"type": "Code-Review",
                                "description": "Code-Review",
                                "value": "0"}]}
        event.update(commentevent)
        return event

    def getChangeMergedEvent(self):
        event = {"submitter": {"name": "Jenkins",
                               "username": "jenkins"},
                 "newRev": self.patchsets[-1]['revision'],
                 "patchSet": self.patchsets[-1],
                 "change": self.data,
                 "type": "change-merged",
                 "eventCreatedOn": 1487613810}
        return event

    def getRefUpdatedEvent(self):
        path = os.path.join(self.upstream_root, self.project)
        repo = git.Repo(path)
        oldrev = repo.heads[self.branch].commit.hexsha

        event = {
            "type": "ref-updated",
            "submitter": {
                "name": "User Name",
            },
            "refUpdate": {
                "oldRev": oldrev,
                "newRev": self.patchsets[-1]['revision'],
                "refName": self.branch,
                "project": self.project,
            }
        }
        return event

    def getHashtagsChangedEvent(self, added=None, removed=None):
        event = {
            'type': 'hashtags-changed',
            'change': {'branch': self.branch,
                       'commitMessage': self.data['commitMessage'],
                       'createdOn': 1689442009,
                       'id': 'I254acfc54f9942394ff924a806cd87c70cec2f4d',
                       'number': int(self.number),
                       'owner': self.data['owner'],
                       'project': self.project,
                       'status': self.data['status'],
                       'subject': self.subject,
                       'url': 'https://hostname/3'},
            'changeKey': {'id': 'I254acfc54f9942394ff924a806cd87c70cec2f4d'},
            'editor': {'email': 'user@example.com',
                       'name': 'User Name',
                       'username': 'user'},
            'eventCreatedOn': 1701711038,
            'project': self.project,
            'refName': self.branch,
        }
        if added:
            event['added'] = added
        if removed:
            event['removed'] = removed
        return event

    def addApproval(self, category, value, username='reviewer_john',
                    granted_on=None, message='', tag=None, old_value=None):
        if not granted_on:
            granted_on = time.time()
        approval = {
            'description': self.categories[category][0],
            'type': category,
            'value': str(value),
            'by': {
                'username': username,
                'email': username + '@example.com',
            },
            'grantedOn': int(granted_on),
            '__tag': tag,  # Not available in ssh api
        }
        if old_value is not None:
            approval['oldValue'] = str(old_value)
        for i, x in enumerate(self.patchsets[-1]['approvals'][:]):
            if x['by']['username'] == username and x['type'] == category:
                del self.patchsets[-1]['approvals'][i]
        self.patchsets[-1]['approvals'].append(approval)
        event = {'approvals': [approval],
                 'author': {'email': 'author@example.com',
                            'name': 'Patchset Author',
                            'username': 'author_phil'},
                 'change': {'branch': self.branch,
                            'id': 'Iaa69c46accf97d0598111724a38250ae76a22c87',
                            'number': str(self.number),
                            'owner': {'email': 'owner@example.com',
                                      'name': 'Change Owner',
                                      'username': 'owner_jane'},
                            'project': self.project,
                            'subject': self.subject,
                            'url': 'https://hostname/459'},
                 'comment': message,
                 'patchSet': self.patchsets[-1],
                 'type': 'comment-added'}
        if 'topic' in self.data:
            event['change']['topic'] = self.data['topic']

        self.data['submitRecords'] = self.getSubmitRecords()
        return json.loads(json.dumps(event))

    def setWorkInProgress(self, wip):
        # Gerrit only includes 'wip' in the data returned via ssh if
        # the value is true.
        if wip:
            self.data['wip'] = True
        elif 'wip' in self.data:
            del self.data['wip']

    def getSubmitRecords(self):
        status = {}
        for cat in self.categories:
            status[cat] = 0

        for a in self.patchsets[-1]['approvals']:
            cur = status[a['type']]
            cat_min, cat_max = self.categories[a['type']][1:]
            new = int(a['value'])
            if new == cat_min:
                cur = new
            elif abs(new) > abs(cur):
                cur = new
            status[a['type']] = cur

        labels = []
        ok = True
        for typ, cat in self.categories.items():
            cur = status[typ]
            cat_min, cat_max = cat[1:]
            if cur == cat_min:
                value = 'REJECT'
                ok = False
            elif cur == cat_max:
                value = 'OK'
            else:
                value = 'NEED'
                ok = False
            labels.append({'label': cat[0], 'status': value})
        if ok:
            return [{'status': 'OK'}]
        return [{'status': 'NOT_READY',
                 'labels': labels}]

    def getSubmitRequirements(self):
        return self.submit_requirements

    def setSubmitRequirements(self, reqs):
        self.submit_requirements = reqs

    def setDependsOn(self, other, patchset):
        self.depends_on_change = other
        self.depends_on_patchset = patchset
        d = {'id': other.data['id'],
             'number': other.data['number'],
             'ref': other.patchsets[patchset - 1]['ref']
             }
        self.data['dependsOn'] = [d]

        other.needed_by_changes.append((self, len(self.patchsets)))
        needed = other.data.get('neededBy', [])
        d = {'id': self.data['id'],
             'number': self.data['number'],
             'ref': self.patchsets[-1]['ref'],
             'revision': self.patchsets[-1]['revision']
             }
        needed.append(d)
        other.data['neededBy'] = needed

    def query(self):
        self.queried += 1
        d = self.data.get('dependsOn')
        if d:
            d = d[0]
            if (self.depends_on_change.patchsets[-1]['ref'] == d['ref']):
                d['isCurrentPatchSet'] = True
            else:
                d['isCurrentPatchSet'] = False
        return json.loads(json.dumps(self.data))

    def queryHTTP(self, internal=False):
        if not internal:
            self.queried += 1
        labels = {}
        for cat in self.categories:
            labels[cat] = {}
        for app in self.patchsets[-1]['approvals']:
            label = labels[app['type']]
            _, label_min, label_max = self.categories[app['type']]
            val = int(app['value'])
            label_all = label.setdefault('all', [])
            approval = {
                "value": val,
                "username": app['by']['username'],
                "email": app['by']['email'],
                "date": str(datetime.datetime.fromtimestamp(app['grantedOn'])),
            }
            if app.get('__tag') is not None:
                approval['tag'] = app['__tag']
            label_all.append(approval)
            if val == label_min:
                label['blocking'] = True
                if 'rejected' not in label:
                    label['rejected'] = app['by']
            if val == label_max:
                if 'approved' not in label:
                    label['approved'] = app['by']
        revisions = {}
        for i, rev in enumerate(self.patchsets):
            num = i + 1
            files = {}
            for f in rev['files']:
                if f['file'] == '/COMMIT_MSG':
                    continue
                files[f['file']] = {"status": f['type'][0]}  # ADDED -> A
            parent = '0000000000000000000000000000000000000000'
            if self.depends_on_change:
                parent = self.depends_on_change.patchsets[
                    self.depends_on_patchset - 1]['revision']
            parents = self.data.get('parents', [])
            if len(parents) <= 1:
                parents = [parent]
            revisions[rev['revision']] = {
                "kind": "REWORK",
                "_number": num,
                "created": rev['createdOn'],
                "uploader": rev['uploader'],
                "ref": rev['ref'],
                "commit": {
                    "subject": self.subject,
                    "message": self.data['commitMessage'],
                    "parents": [{
                        "commit": p,
                    } for p in parents]
                },
                "files": files
            }
        if self.cherry_pick:
            submit_type = 'CHERRY_PICK'
        else:
            submit_type = 'MERGE_IF_NECESSARY'
        data = {
            "id": self.project + '~' + self.branch + '~' + self.data['id'],
            "project": self.project,
            "branch": self.branch,
            "hashtags": [],
            "change_id": self.data['id'],
            "subject": self.subject,
            "status": self.data['status'],
            "created": self.data['createdOn'],
            "updated": self.data['lastUpdated'],
            "_number": self.number,
            "owner": self.data['owner'],
            "labels": labels,
            "current_revision": self.patchsets[-1]['revision'],
            "revisions": revisions,
            "requirements": [],
            "work_in_progresss": self.data.get('wip', False),
            "submit_type": submit_type,
        }
        if 'parents' in self.data:
            data['parents'] = self.data['parents']
        if 'topic' in self.data:
            data['topic'] = self.data['topic']
        data['submit_requirements'] = self.getSubmitRequirements()
        return json.loads(json.dumps(data))

    def queryRevisionHTTP(self, revision):
        for ps in self.patchsets:
            if ps['revision'] == revision:
                break
        else:
            return None
        changes = []
        if self.depends_on_change:
            changes.append({
                "commit": {
                    "commit": self.depends_on_change.patchsets[
                        self.depends_on_patchset - 1]['revision'],
                },
                "_change_number": self.depends_on_change.number,
                "_revision_number": self.depends_on_patchset,
                "_current_revision_number":
                self.depends_on_change.latest_patchset,
            })
        for (needed_by_change, needed_by_patchset) in self.needed_by_changes:
            changes.append({
                "commit": {
                    "commit": needed_by_change.patchsets[
                        needed_by_patchset - 1]['revision'],
                },
                "_change_number": needed_by_change.number,
                "_revision_number": needed_by_patchset,
                "_current_revision_number": needed_by_change.latest_patchset,
            })
        return {"changes": changes}

    def queryFilesHTTP(self, revision, parent):
        for rev in self.patchsets:
            if rev['revision'] == revision:
                break
        else:
            return None

        files = {}
        # We're querying the list of files different in this commit
        # compared to the parent (ie, the list of files that we're
        # merging into the branch, not the files in this commit).  If
        # this is a merge commit and the test has specified such a
        # files list, use that.
        if (parent and len(self.data.get('parents', []))
            and self.merge_parent_files):
            rev_files = self.merge_parent_files or []
        else:
            # Otherwise, the simple case of "files in this commit"
            rev_files = rev['files']
        for f in rev_files:
            files[f['file']] = {"status": f['type'][0]}  # ADDED -> A
        return files

    def setMerged(self):
        if (self.depends_on_change and
                self.depends_on_change.data['status'] != 'MERGED'):
            return
        if self.fail_merge:
            return
        self.data['status'] = 'MERGED'
        self.data['open'] = False
        self.open = False

        path = os.path.join(self.upstream_root, self.project)
        repo = git.Repo(path)

        repo.head.reference = self.branch
        repo.head.reset(working_tree=True)
        if self.cherry_pick:
            repo.git.cherry_pick(self.patchsets[-1]['ref'], allow_empty=True)
        else:
            repo.git.merge('-s', 'resolve', self.patchsets[-1]['ref'])
        repo.heads[self.branch].commit = repo.head.commit
        if self.cherry_pick:
            self.addCherryPickPatchset()

    def setReported(self):
        self.reported += 1

    def setAbandoned(self):
        self.data['status'] = 'ABANDONED'
        self.data['open'] = False
        self.open = False

    def unsetAbandoned(self):
        self.data['status'] = 'NEW'
        self.data['open'] = True
        self.open = True


class FakeGerritPoller(gerritconnection.GerritChecksPoller):
    """A Fake Gerrit poller for use in tests.

    This subclasses
    :py:class:`~zuul.connection.gerrit.GerritPoller`.
    """

    poll_interval = 1

    def _poll(self, *args, **kw):
        r = super(FakeGerritPoller, self)._poll(*args, **kw)
        # Set the event so tests can confirm that the poller has run
        # after they changed something.
        self.connection._poller_event.set()
        return r


class FakeGerritRefWatcher(gitwatcher.GitWatcher):
    """A Fake Gerrit ref watcher.

    This subclasses
    :py:class:`~zuul.connection.git.GitWatcher`.
    """

    def __init__(self, *args, **kw):
        super(FakeGerritRefWatcher, self).__init__(*args, **kw)
        self.baseurl = self.connection.upstream_root
        self.poll_delay = 1

    def _poll(self, *args, **kw):
        r = super(FakeGerritRefWatcher, self)._poll(*args, **kw)
        # Set the event so tests can confirm that the watcher has run
        # after they changed something.
        self.connection._ref_watcher_event.set()
        return r


class GerritWebServer(object):

    def __init__(self, fake_gerrit):
        super(GerritWebServer, self).__init__()
        self.fake_gerrit = fake_gerrit

    def start(self):
        fake_gerrit = self.fake_gerrit

        class Server(http.server.SimpleHTTPRequestHandler):
            log = logging.getLogger("zuul.test.FakeGerritConnection")
            review_re = re.compile('/a/changes/(.*?)/revisions/(.*?)/review')
            together_re = re.compile('/a/changes/(.*?)/submitted_together')
            submit_re = re.compile('/a/changes/(.*?)/submit')
            pending_checks_re = re.compile(
                r'/a/plugins/checks/checks\.pending/\?'
                r'query=checker:(.*?)\+\(state:(.*?)\)')
            update_checks_re = re.compile(
                r'/a/changes/(.*)/revisions/(.*?)/checks/(.*)')
            list_checkers_re = re.compile('/a/plugins/checks/checkers/')
            change_re = re.compile(r'/a/changes/(.*)\?o=.*')
            related_re = re.compile(r'/a/changes/(.*)/revisions/(.*)/related')
            files_parent_re = re.compile(
                r'/a/changes/(.*)/revisions/(.*)/files'
                r'\?parent=1')
            files_re = re.compile(r'/a/changes/(.*)/revisions/(.*)/files')
            change_search_re = re.compile(r'/a/changes/\?n=500.*&q=(.*)')
            version_re = re.compile(r'/a/config/server/version')
            info_re = re.compile(r'/a/config/server/info')
            head_re = re.compile(r'/a/projects/(.*)/HEAD')

            def do_POST(self):
                path = self.path
                self.log.debug("Got POST %s", path)
                fake_gerrit.api_calls.append(('POST', path))

                data = self.rfile.read(int(self.headers['Content-Length']))
                data = json.loads(data.decode('utf-8'))
                self.log.debug("Got data %s", data)

                m = self.review_re.match(path)
                if m:
                    return self.review(m.group(1), m.group(2), data)
                m = self.submit_re.match(path)
                if m:
                    return self.submit(m.group(1), data)
                m = self.update_checks_re.match(path)
                if m:
                    return self.update_checks(
                        m.group(1), m.group(2), m.group(3), data)
                self.send_response(500)
                self.end_headers()

            def do_GET(self):
                path = self.path
                self.log.debug("Got GET %s", path)
                fake_gerrit.api_calls.append(('GET', path))
                if fake_gerrit._fake_return_api_error:
                    self.send_response(500)
                    self.end_headers()
                    return

                m = self.change_re.match(path)
                if m:
                    return self.get_change(m.group(1))
                m = self.related_re.match(path)
                if m:
                    return self.get_related(m.group(1), m.group(2))
                m = self.files_parent_re.match(path)
                if m:
                    return self.get_files(m.group(1), m.group(2), 1)
                m = self.files_re.match(path)
                if m:
                    return self.get_files(m.group(1), m.group(2))
                m = self.together_re.match(path)
                if m:
                    return self.get_submitted_together(m.group(1))
                m = self.change_search_re.match(path)
                if m:
                    return self.get_changes(m.group(1))
                m = self.pending_checks_re.match(path)
                if m:
                    return self.get_pending_checks(m.group(1), m.group(2))
                m = self.list_checkers_re.match(path)
                if m:
                    return self.list_checkers()
                m = self.version_re.match(path)
                if m:
                    return self.version()
                m = self.info_re.match(path)
                if m:
                    return self.info()
                m = self.head_re.match(path)
                if m:
                    return self.head(m.group(1))
                self.send_response(500)
                self.end_headers()

            def _403(self, msg):
                self.send_response(403)
                self.end_headers()
                self.wfile.write(msg.encode('utf8'))

            def _400(self, msg):
                self.send_response(400)
                self.end_headers()
                self.wfile.write(msg.encode('utf8'))

            def _404(self):
                self.send_response(404)
                self.end_headers()

            def _409(self):
                self.send_response(409)
                self.end_headers()

            def _get_change(self, change_id):
                change_id = urllib.parse.unquote(change_id)
                project, branch, change = change_id.split('~')
                for c in fake_gerrit.changes.values():
                    if (c.data['id'] == change and
                        c.data['branch'] == branch and
                        c.data['project'] == project):
                        return c

            def review(self, change_id, revision, data):
                change = self._get_change(change_id)
                if not change:
                    return self._404()

                message = data['message']
                b_len = len(message.encode('utf-8'))
                if b_len > gerritreporter.GERRIT_HUMAN_MESSAGE_LIMIT:
                    self.send_response(400, message='Message length exceeded')
                    self.end_headers()
                    return
                labels = data.get('labels', {})
                comments = data.get('robot_comments', data.get('comments', {}))
                tag = data.get('tag', None)
                try:
                    fake_gerrit._test_handle_review(
                        int(change.data['number']), message, False, labels,
                        None, True, False, comments, tag=tag)
                except Exception as e:
                    return self._400(str(e))
                self.send_response(200)
                self.end_headers()

            def submit(self, change_id, data):
                change = self._get_change(change_id)
                if not change:
                    return self._404()

                if not fake_gerrit._fake_submit_permission:
                    return self._403('submit not permitted')

                candidate = self._get_change(change_id)
                sr = candidate.getSubmitRecords()
                if sr[0]['status'] != 'OK':
                    # One of the changes in this topic isn't
                    # ready to merge
                    return self._409()
                changes_to_merge = set(change.data['number'])
                if fake_gerrit._fake_submit_whole_topic:
                    results = fake_gerrit._test_get_submitted_together(change)
                    for record in results:
                        candidate = self._get_change(record['id'])
                        sr = candidate.getSubmitRecords()
                        if sr[0]['status'] != 'OK':
                            # One of the changes in this topic isn't
                            # ready to merge
                            return self._409()
                        changes_to_merge.add(candidate.data['number'])
                message = None
                labels = {}
                for change_number in changes_to_merge:
                    fake_gerrit._test_handle_review(
                        int(change_number), message, True, labels,
                        None, False, True)
                self.send_response(200)
                self.end_headers()

            def update_checks(self, change_id, revision, checker, data):
                self.log.debug("Update checks %s %s %s",
                               change_id, revision, checker)
                change = self._get_change(change_id)
                if not change:
                    return self._404()

                change.setCheck(checker, **data)
                self.send_response(200)
                # TODO: return the real data structure, but zuul
                # ignores this now.
                self.end_headers()

            def get_pending_checks(self, checker, state):
                self.log.debug("Get pending checks %s %s", checker, state)
                ret = []
                for c in fake_gerrit.changes.values():
                    if checker not in c.checks:
                        continue
                    patchset_pending_checks = {}
                    if c.checks[checker]['state'] == state:
                        patchset_pending_checks[checker] = {
                            'state': c.checks[checker]['state'],
                        }
                    if patchset_pending_checks:
                        ret.append({
                            'patch_set': {
                                'repository': c.project,
                                'change_number': c.number,
                                'patch_set_id': c.latest_patchset,
                            },
                            'pending_checks': patchset_pending_checks,
                        })
                self.send_data(ret)

            def list_checkers(self):
                self.log.debug("Get checkers")
                self.send_data(fake_gerrit.fake_checkers)

            def get_change(self, number):
                change = fake_gerrit.changes.get(int(number))
                if not change:
                    return self._404()

                self.send_data(change.queryHTTP())
                self.end_headers()

            def get_related(self, number, revision):
                change = fake_gerrit.changes.get(int(number))
                if not change:
                    return self._404()
                data = change.queryRevisionHTTP(revision)
                if data is None:
                    return self._404()
                self.send_data(data)
                self.end_headers()

            def get_files(self, number, revision, parent=None):
                change = fake_gerrit.changes.get(int(number))
                if not change:
                    return self._404()
                data = change.queryFilesHTTP(revision, parent)
                if data is None:
                    return self._404()
                self.send_data(data)
                self.end_headers()

            def get_submitted_together(self, number):
                change = fake_gerrit.changes.get(int(number))
                if not change:
                    return self._404()

                results = fake_gerrit._test_get_submitted_together(change)
                self.send_data(results)
                self.end_headers()

            def get_changes(self, query):
                self.log.debug("simpleQueryHTTP: %s", query)
                query = urllib.parse.unquote(query)
                fake_gerrit.queries.append(query)
                results = []
                if query.startswith('(') and 'OR' in query:
                    query = query[1:-1]
                    for q in query.split(' OR '):
                        for r in fake_gerrit._simpleQuery(q, http=True):
                            if r not in results:
                                results.append(r)
                else:
                    results = fake_gerrit._simpleQuery(query, http=True)
                self.send_data(results)
                self.end_headers()

            def version(self):
                self.send_data('3.0.0-some-stuff')
                self.end_headers()

            def info(self):
                # This is not complete; see documentation for
                # additional fields.
                data = {
                    "accounts": {
                        "visibility": "ALL",
                        "default_display_name": "FULL_NAME"
                    },
                    "change": {
                        "allow_blame": True,
                        "disable_private_changes": True,
                        "update_delay": 300,
                        "mergeability_computation_behavior":
                        "API_REF_UPDATED_AND_CHANGE_REINDEX",
                        "enable_robot_comments": True,
                        "conflicts_predicate_enabled": True
                    },
                    "gerrit": {
                        "all_projects": "All-Projects",
                        "all_users": "All-Users",
                        "doc_search": True,
                    },
                    "note_db_enabled": True,
                    "sshd": {},
                    "suggest": {"from": 0},
                    "user": {"anonymous_coward_name": "Name of user not set"},
                    "receive": {"enable_signed_push": False},
                    "submit_requirement_dashboard_columns": []
                }
                if fake_gerrit._fake_submit_whole_topic:
                    data['change']['submit_whole_topic'] = True
                self.send_data(data)
                self.end_headers()

            def head(self, project):
                project = urllib.parse.unquote(project)
                head = fake_gerrit._fake_project_default_branch.get(
                    project, 'master')
                self.send_data('refs/heads/' + head)
                self.end_headers()

            def send_data(self, data):
                data = json.dumps(data).encode('utf-8')
                data = b")]}'\n" + data
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', len(data))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, fmt, *args):
                self.log.debug(fmt, *args)

        self.httpd = socketserver.ThreadingTCPServer(('', 0), Server)
        self.port = self.httpd.socket.getsockname()[1]
        self.thread = threading.Thread(name='GerritWebServer',
                                       target=self.httpd.serve_forever)
        self.thread.daemon = True
        self.thread.start()

    def stop(self):
        self.httpd.shutdown()
        self.thread.join()
        self.httpd.server_close()


class FakeGerritConnection(gerritconnection.GerritConnection):
    """A Fake Gerrit connection for use in tests.

    This subclasses
    :py:class:`~zuul.connection.gerrit.GerritConnection` to add the
    ability for tests to add changes to the fake Gerrit it represents.
    """

    log = logging.getLogger("zuul.test.FakeGerritConnection")
    _poller_class = FakeGerritPoller
    _ref_watcher_class = FakeGerritRefWatcher

    def __init__(self, driver, connection_name, connection_config,
                 changes_db=None, upstream_root=None, poller_event=None,
                 ref_watcher_event=None, submit_whole_topic=None):

        if connection_config.get('password'):
            self.web_server = GerritWebServer(self)
            self.web_server.start()
            url = 'http://localhost:%s' % self.web_server.port
            connection_config['baseurl'] = url
        else:
            self.web_server = None

        super(FakeGerritConnection, self).__init__(driver, connection_name,
                                                   connection_config)

        self.fixture_dir = os.path.join(FIXTURE_DIR, 'gerrit')
        self.change_number = 0
        self.changes = changes_db
        self.queries = []
        self.api_calls = []
        self._fake_return_api_error = False
        self.upstream_root = upstream_root
        self.fake_checkers = []
        self._poller_event = poller_event
        self._ref_watcher_event = ref_watcher_event
        self._fake_submit_whole_topic = bool(submit_whole_topic)
        self._fake_submit_permission = True
        self._fake_project_default_branch = {}
        self.submit_retry_backoff = 0

    def onStop(self):
        super().onStop()
        if self.web_server:
            self.web_server.stop()

    def addFakeChecker(self, **kw):
        self.fake_checkers.append(kw)

    def addFakeChange(self, project, branch, subject, status='NEW',
                      files=None, parent=None, merge_parents=None,
                      merge_files=None, topic=None, empty=False):
        """Add a change to the fake Gerrit."""
        self.change_number += 1
        c = FakeGerritChange(self, self.change_number, project, branch,
                             subject, upstream_root=self.upstream_root,
                             status=status, files=files, parent=parent,
                             merge_parents=merge_parents,
                             merge_files=merge_files,
                             topic=topic, empty=empty)
        self.changes[self.change_number] = c
        return c

    def addFakeTag(self, project, branch, tag, message=None):
        path = os.path.join(self.upstream_root, project)
        repo = git.Repo(path)
        commit = repo.heads[branch].commit
        ref = 'refs/tags/' + tag

        t = git.Tag.create(repo, tag, commit, logmsg=message)
        newrev = t.object.hexsha

        event = {
            "type": "ref-updated",
            "submitter": {
                "name": "User Name",
            },
            "refUpdate": {
                "oldRev": 40 * '0',
                "newRev": newrev,
                "refName": ref,
                "project": project,
            }
        }
        return event

    def getFakeBranchCreatedEvent(self, project, branch):
        path = os.path.join(self.upstream_root, project)
        repo = git.Repo(path)
        oldrev = 40 * '0'

        event = {
            "type": "ref-updated",
            "submitter": {
                "name": "User Name",
            },
            "refUpdate": {
                "oldRev": oldrev,
                "newRev": repo.heads[branch].commit.hexsha,
                "refName": 'refs/heads/' + branch,
                "project": project,
            }
        }
        return event

    def getFakeBranchDeletedEvent(self, project, branch):
        oldrev = '4abd38457c2da2a72d4d030219ab180ecdb04bf0'
        newrev = 40 * '0'

        event = {
            "type": "ref-updated",
            "submitter": {
                "name": "User Name",
            },
            "refUpdate": {
                "oldRev": oldrev,
                "newRev": newrev,
                "refName": 'refs/heads/' + branch,
                "project": project,
            }
        }
        return event

    def getProjectHeadUpdatedEvent(self, project, old, new):
        event = {
            "projectName": project,
            "oldHead": f"refs/heads/{old}",
            "newHead": f"refs/heads/{new}",
            "type": "project-head-updated",
        }
        return event

    def review(self, item, change, message, submit, labels,
               checks_api, notify, file_comments, phase1, phase2,
               zuul_event_id=None):
        if self.web_server:
            return super(FakeGerritConnection, self).review(
                item, change, message, submit, labels, checks_api,
                notify, file_comments, phase1, phase2,
                zuul_event_id)
        self._test_handle_review(int(change.number), message, submit,
                                 labels, notify, phase1, phase2)

    def _test_get_submitted_together(self, change):
        topic = change.data.get('topic')
        if not self._fake_submit_whole_topic:
            topic = None
        if topic:
            results = self._simpleQuery(f'status:open topic:{topic}',
                                        http=True)
        else:
            results = [change.queryHTTP(internal=True)]
        for dep in change.data.get('dependsOn', []):
            dep_change = self.changes.get(int(dep['number']))
            r = dep_change.queryHTTP(internal=True)
            if r['status'] == 'MERGED':
                continue
            if not dep_change.open:
                continue
            if r not in results:
                results.append(r)
        if len(results) == 1:
            return []
        return results

    def _test_handle_review(self, change_number, message, submit, labels,
                            notify,
                            phase1, phase2, file_comments=None, tag=None):
        # Handle a review action from a test
        change = self.changes[change_number]

        change.notify = notify

        # Add the approval back onto the change (ie simulate what gerrit would
        # do).
        # Usually when zuul leaves a review it'll create a feedback loop where
        # zuul's review enters another gerrit event (which is then picked up by
        # zuul). However, we can't mimic this behaviour (by adding this
        # approval event into the queue) as it stops jobs from checking what
        # happens before this event is triggered. If a job needs to see what
        # happens they can add their own verified event into the queue.
        # Nevertheless, we can update change with the new review in gerrit.

        if phase1:
            for cat in labels:
                change.addApproval(cat, labels[cat], username=self.user,
                                   tag=tag)

            if message:
                change.messages.append(message)

            if file_comments:
                ok_files = set(x['file']
                               for x in change.patchsets[-1]['files'])
                ok_files.add('/COMMIT_MSG')
                for filename, commentlist in file_comments.items():
                    if filename not in ok_files:
                        raise Exception(
                            f"file {filename} not found in revision")
                    for comment in commentlist:
                        change.addComment(filename, comment['line'],
                                          comment['message'], 'Zuul',
                                          'zuul@example.com', self.user,
                                          comment.get('range'))
            if message:
                change.setReported()
        if submit and phase2:
            change.setMerged()

    def queryChangeSSH(self, number, event=None):
        self.log.debug("Query change SSH: %s", number)
        change = self.changes.get(int(number))
        if change:
            return change.query()
        return {}

    def _simpleQuery(self, query, http=False):
        if http:
            def queryMethod(change):
                return change.queryHTTP()
        else:
            def queryMethod(change):
                return change.query()
        # the query can be in parenthesis so strip them if needed
        if query.startswith('('):
            query = query[1:-1]
        if query.startswith('change:'):
            # Query a specific changeid
            changeid = query[len('change:'):]
            l = [queryMethod(change) for change in self.changes.values()
                 if (change.data['id'] == changeid or
                     change.data['number'] == changeid)]
        elif query.startswith('message:'):
            # Query the content of a commit message
            msg = query[len('message:'):].strip()
            # Remove quoting if it is there
            if msg.startswith('{') and msg.endswith('}'):
                msg = msg[1:-1]
            l = [queryMethod(change) for change in self.changes.values()
                 if msg in change.data['commitMessage']]
        else:
            cut_off_time = 0
            l = list(self.changes.values())
            parts = query.split(" ")
            for part in parts:
                if part.startswith("-age"):
                    _, _, age = part.partition(":")
                    cut_off_time = (
                        datetime.datetime.now().timestamp() - float(age[:-1])
                    )
                    l = [
                        change for change in l
                        if change.data["lastUpdated"] >= cut_off_time
                    ]
                if part.startswith('topic:'):
                    topic = part[len('topic:'):].strip().strip('"\'')
                    l = [
                        change for change in l
                        if 'topic' in change.data
                        and topic in change.data['topic']
                    ]
                if part.startswith('status:'):
                    status = part[len('status:'):].strip().strip('"\'').lower()
                    if status == 'open':
                        l = [
                            change for change in l if change.open
                        ]
                    else:
                        l = [
                            change for change in l
                            if change.data['status'].lower() == status
                        ]
            l = [queryMethod(change) for change in l]
        return l

    def simpleQuerySSH(self, query, event=None):
        log = get_annotated_logger(self.log, event)
        log.debug("simpleQuerySSH: %s", query)
        self.queries.append(query)
        results = []
        if query.startswith('(') and 'OR' in query:
            query = query[1:-1]
            for q in query.split(' OR '):
                for r in self._simpleQuery(q):
                    if r not in results:
                        results.append(r)
        else:
            results = self._simpleQuery(query)
        return results

    def startSSHListener(self, *args, **kw):
        pass

    def _uploadPack(self, project):
        ret = ('00a31270149696713ba7e06f1beb760f20d359c4abed HEAD\x00'
               'multi_ack thin-pack side-band side-band-64k ofs-delta '
               'shallow no-progress include-tag multi_ack_detailed no-done\n')
        path = os.path.join(self.upstream_root, project.name)
        repo = git.Repo(path)
        for ref in repo.refs:
            if ref.path.endswith('.lock'):
                # don't treat lockfiles as ref
                continue
            r = ref.object.hexsha + ' ' + ref.path + '\n'
            ret += '%04x%s' % (len(r) + 4, r)
        ret += '0000'
        return ret

    def getGitUrl(self, project):
        return 'file://' + os.path.join(self.upstream_root, project.name)
