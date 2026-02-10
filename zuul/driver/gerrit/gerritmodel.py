# Copyright 2017 Red Hat, Inc.
# Copyright 2023 Acme Gating, LLC
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

import copy
import time
import urllib.parse
import dateutil.parser
import json

from zuul.model import EventFilter, RefFilter
from zuul.model import Change, TriggerEvent, FalseWithReason
from zuul.driver.util import time_to_seconds, to_list, make_regex
from zuul.lib.dependson import find_dependency_headers
from zuul import exceptions
from zuul.zk.change_cache import ChangeKey


EMPTY_GIT_REF = '0' * 40  # git sha of all zeros, used during creates/deletes


class GerritChange(Change):
    def __init__(self, project):
        super(GerritChange, self).__init__(project)
        self.id = None
        self.status = None
        self.wip = None
        self.approvals = []
        self.missing_labels = set()
        self.submit_requirements = []
        self.commit = None
        self.hashtags = []
        self.zuul_query_ltime = None

    def get_dependency_messages(self):
        matches = find_dependency_headers(self.message)
        for hashtag in self.hashtags:
            matches.extend(find_dependency_headers(hashtag))
        return matches

    def update(self, data, extra, connection):
        if not (self.zuul_query_ltime and
                self.zuul_query_ltime > data.zuul_query_ltime):
            # If our query is older than the existing change data, it
            # means that another scheduler raced us and won.  Skip the
            # update to avoid having is_merged flap.
            self.zuul_query_ltime = data.zuul_query_ltime
            if data.format == data.SSH:
                self.updateFromSSH(data.data, connection)
            else:
                self.updateFromHTTP(data.data, data.files,
                                    data.commentable_files,
                                    connection)
            for k, v in extra.items():
                setattr(self, k, v)
        key = ChangeKey(connection.connection_name, None,
                        'GerritChange', str(self.number), str(self.patchset))
        return key

    def serialize(self):
        d = super().serialize()
        d.update({
            "id": self.id,
            "status": self.status,
            "wip": self.wip,
            "approvals": self.approvals,
            "missing_labels": list(self.missing_labels),
            "submit_requirements": self.submit_requirements,
            "commit": self.commit,
            "hashtags": self.hashtags,
            "zuul_query_ltime": self.zuul_query_ltime,
        })
        return d

    def deserialize(self, data):
        super().deserialize(data)
        self.id = data.get("id")
        self.status = data["status"]
        self.wip = data["wip"]
        self.approvals = data["approvals"]
        self.missing_labels = set(data["missing_labels"])
        self.submit_requirements = data.get("submit_requirements", [])
        self.commit = data.get("commit")
        self.hashtags = data.get("hashtags", [])
        self.zuul_query_ltime = data.get("zuul_query_ltime")

    def updateFromSSH(self, data, connection):
        if self.patchset is None:
            self.patchset = str(data['currentPatchSet']['number'])
        if 'project' not in data:
            raise exceptions.ChangeNotFound(self.number, self.patchset)
        self.project = connection.source.getProject(data['project'])
        self.commit_id = str(data['currentPatchSet']['revision'])
        self.branch = data['branch']
        self.url = data['url']
        urlparse = urllib.parse.urlparse(connection.baseurl)
        baseurl = "%s://%s%s" % (urlparse.scheme, urlparse.netloc,
                                 urlparse.path)
        baseurl = baseurl.rstrip('/')
        self.uris = [
            '%s/%s' % (baseurl, self.number),
            '%s/#/c/%s' % (baseurl, self.number),
            '%s/c/%s/+/%s' % (baseurl, self.project.name, self.number),
        ]

        max_ps = 0
        files = []
        for ps in data['patchSets']:
            if str(ps['number']) == self.patchset:
                self.ref = ps['ref']
                self.commit = ps['revision']
                # SSH queries gives us a list of dicts for the files. We
                # convert that to a list of filenames.
                for f in ps.get('files', []):
                    files.append(f['file'])
            if int(ps['number']) > int(max_ps):
                max_ps = str(ps['number'])
        if max_ps == self.patchset:
            self.is_current_patchset = True
        else:
            self.is_current_patchset = False
        if len(data.get('parents', [])) > 1:
            # This is a merge commit, and the SSH query only reports
            # files in this commit's content (not files changed by the
            # underlying merged changes).  Set files to None to
            # instruct Zuul to ask the mergers to get the full file
            # list.
            self.files = None
        self.files = files
        self.id = data['id']
        self.is_merged = data.get('status', '') == 'MERGED'
        self.approvals = data['currentPatchSet'].get('approvals', [])
        self.open = data['open']
        self.status = data['status']
        self.wip = data.get('wip', False)
        self.owner = data['owner'].get('username')
        self.message = data['commitMessage']
        self.topic = data.get('topic')
        self.hashtags = data.get('hashtags', [])

        self.missing_labels = set()
        for sr in data.get('submitRecords', []):
            if sr['status'] == 'NOT_READY':
                for label in sr['labels']:
                    if label['status'] in ['OK', 'MAY']:
                        continue
                    elif label['status'] in ['NEED', 'REJECT']:
                        self.missing_labels.add(label['label'])

    def updateFromHTTP(self, data, files, commentable_files, connection):
        urlparse = urllib.parse.urlparse(connection.baseurl)
        baseurl = "%s://%s%s" % (urlparse.scheme, urlparse.netloc,
                                 urlparse.path)
        baseurl = baseurl.rstrip('/')
        current_revision = data['revisions'][data['current_revision']]
        if self.patchset is None:
            self.patchset = str(current_revision['_number'])
        self.project = connection.source.getProject(data['project'])
        self.commit_id = str(data['current_revision'])
        self.id = data['change_id']
        self.branch = data['branch']
        url1 = '%s/%s' % (baseurl, self.number)
        url2 = '%s/#/c/%s' % (baseurl, self.number)
        url3 = '%s/c/%s/+/%s' % (baseurl, self.project.name, self.number)
        if connection.version >= (2, 15, 0):
            # This is what modern Gerrit emits, and if a user
            # navigates to it without access, it provides the best
            # error messages and UX for logging in.
            self.url = url3
        else:
            self.url = url1
        self.uris = [url1, url2, url3]

        for rev_commit, revision in data['revisions'].items():
            if str(revision['_number']) == self.patchset:
                self.ref = revision['ref']
                self.commit = rev_commit

        if str(current_revision['_number']) == self.patchset:
            self.is_current_patchset = True
        else:
            self.is_current_patchset = False

        # HTTP queries give us a dict of files in the form of
        # {filename: { attrs }}. We only want a list of filenames here.
        if files:
            self.files = list(files.keys())
        else:
            self.files = []

        if commentable_files is not None:
            self.commentable_files = list(commentable_files.keys())
        else:
            self.commentable_files = None

        self.is_merged = data['status'] == 'MERGED'
        self.approvals = []
        self.missing_labels = set()
        for label_name, label_data in data.get('labels', {}).items():
            for app in label_data.get('all', []):
                if app.get('value', 0) == 0:
                    continue
                by = {}
                for k in ('name', 'username', 'email'):
                    if k in app:
                        by[k] = app[k]
                self.approvals.append({
                    "type": label_name,
                    "description": label_name,
                    "value": app['value'],
                    "grantedOn":
                    dateutil.parser.parse(app['date']).timestamp(),
                    "by": by,
                })
            if label_data.get('optional', False):
                continue
            if label_data.get('blocking', False):
                self.missing_labels.add(label_name)
                continue
            if 'approved' in label_data:
                continue
            self.missing_labels.add(label_name)
        self.submit_requirements = data.get('submit_requirements', [])
        self.open = data['status'] == 'NEW'
        self.status = data['status']
        self.wip = data.get('work_in_progress', False)
        self.owner = data['owner'].get('username')
        self.message = current_revision['commit']['message']
        self.topic = data.get('topic')
        self.hashtags = data.get('hashtags', [])


class GerritTriggerEvent(TriggerEvent):
    """Incoming event from an external system."""
    def __init__(self):
        super(GerritTriggerEvent, self).__init__()
        self.approvals = []
        self.uuid = None
        self.scheme = None
        self.patchsetcomments = None
        self.added = None  # Used by hashtags-changed event
        self.removed = None  # Used by hashtags-changed event
        self.default_branch_changed = None

    @classmethod
    def fromGerritEventDict(cls, data, timestamp, connection,
                            zuul_event_id):
        event = cls()
        event.timestamp = timestamp
        event.connection_name = connection.connection_name
        event.zuul_event_id = zuul_event_id

        event.type = data.get('type')
        event.uuid = data.get('uuid')

        # This catches when a change is merged, as it could potentially
        # have merged layout info which will need to be read in.
        # Ideally this would be done with a refupdate event so as to catch
        # directly pushed things as well as full changes being merged.
        # But we do not yet get files changed data for pure refupdate events.
        # TODO(jlk): handle refupdated events instead of just changes
        if event.type == 'change-merged':
            event.branch_updated = True
        event.trigger_name = 'gerrit'
        change = data.get('change')
        event.project_hostname = connection.canonical_hostname
        if change:
            event.project_name = change.get('project')
            event.branch = change.get('branch')
            event.change_number = str(change.get('number'))
            event.change_url = change.get('url')
            patchset = data.get('patchSet')
            if patchset:
                event.patch_number = str(patchset.get('number'))
                event.ref = patchset.get('ref')
            event.approvals = data.get('approvals', [])
            event.comment = data.get('comment')
            patchsetcomments = data.get('patchSetComments', {}).get(
                "/PATCHSET_LEVEL")
            if patchsetcomments:
                event.patchsetcomments = []
                for patchsetcomment in patchsetcomments:
                    event.patchsetcomments.append(
                        patchsetcomment.get('message'))
            event.added = data.get('added')
            event.removed = data.get('removed')
        refupdate = data.get('refUpdate')
        if refupdate:
            event.project_name = refupdate.get('project')
            event.ref = refupdate.get('refName')
            event.oldrev = refupdate.get('oldRev')
            event.newrev = refupdate.get('newRev')
        if event.project_name is None:
            # ref-replica* events
            event.project_name = data.get('project')
        if event.type == 'project-created':
            event.project_name = data.get('projectName')
        if event.type == 'project-head-updated':
            event.project_name = data.get('projectName')
            event.ref = data.get('newHead')
            event.branch = event.ref[len('refs/heads/'):]
            event.default_branch_changed = True
        # Map the event types to a field name holding a Gerrit
        # account attribute. See Gerrit stream-event documentation
        # in cmd-stream-events.html
        accountfield_from_type = {
            'patchset-created': 'uploader',
            'draft-published': 'uploader',  # Gerrit 2.5/2.6
            'change-abandoned': 'abandoner',
            'change-restored': 'restorer',
            'change-merged': 'submitter',
            'merge-failed': 'submitter',  # Gerrit 2.5/2.6
            'comment-added': 'author',
            'ref-updated': 'submitter',
            'reviewer-added': 'reviewer',  # Gerrit 2.5/2.6
            'topic-changed': 'changer',
            'hashtags-changed': 'editor',
            'vote-deleted': 'deleter',
            'project-created': None,  # Gerrit 2.14
            'pending-check': None,  # Gerrit 3.0+
            'project-head-updated': None,
        }
        event.account = None
        event._accountfield_unknown = False
        if event.type in accountfield_from_type:
            field = accountfield_from_type[event.type]
            if field:
                event.account = data.get(accountfield_from_type[event.type])
        else:
            event._accountfield_unknown = True

        # This checks whether the event created or deleted a branch so
        # that Zuul may know to perform a reconfiguration on the
        # project.
        branch_refs = 'refs/heads/'
        event._branch_ref_update = False
        if (event.type == 'ref-updated' and
            ((not event.ref.startswith('refs/')) or
             event.ref.startswith(branch_refs))):

            if event.ref.startswith(branch_refs):
                event.branch = event.ref[len(branch_refs):]
            else:
                event.branch = event.ref

            event._branch_ref_update = True
        return event

    def toDict(self):
        d = super().toDict()
        d["approvals"] = self.approvals
        d["uuid"] = self.uuid
        d["scheme"] = self.scheme
        d["patchsetcomments"] = self.patchsetcomments
        d["added"] = self.added
        d["removed"] = self.removed
        d["default_branch_changed"] = self.default_branch_changed
        return d

    def updateFromDict(self, d):
        super().updateFromDict(d)
        self.approvals = d["approvals"]
        self.uuid = d["uuid"]
        self.scheme = d["scheme"]
        self.patchsetcomments = d["patchsetcomments"]
        self.added = d.get("added")
        self.removed = d.get("removed")
        self.default_branch_changed = d.get("default_branch_changed")

    def __repr__(self):
        ret = '<GerritTriggerEvent %s %s' % (self.type,
                                             self.canonical_project_name)

        if self.branch:
            ret += " %s" % self.branch
        if self.change_number:
            ret += " %s,%s" % (self.change_number, self.patch_number)
        if self.approvals:
            ret += ' ' + ', '.join(
                ['%s:%s' % (a['type'], a['value']) for a in self.approvals])
        if self.added:
            ret += f" added {self.added}"
        if self.removed:
            ret += f" removed {self.removed}"
        ret += '>'

        return ret

    def isPatchsetCreated(self):
        return self.type in ('patchset-created', 'pending-check')

    def isChangeAbandoned(self):
        return 'change-abandoned' == self.type

    def isDefaultBranchChanged(self):
        return bool(self.default_branch_changed)


class GerritEventFilter(EventFilter):
    def __init__(self, connection_name, trigger, types=[], branches=[],
                 refs=[], event_approvals={}, event_approval_changes={},
                 comments=[], emails=[], usernames=[], required_approvals=[],
                 reject_approvals=[], added=[], removed=[], uuid=None,
                 scheme=None, ignore_deletes=True, require=None, reject=None,
                 debug=None, parse_context=None):

        EventFilter.__init__(self, connection_name, trigger, debug)

        # TODO: Backwards compat, remove after 9.x:
        if required_approvals and require is None:
            require = {'approval': required_approvals}
        if reject_approvals and reject is None:
            reject = {'approval': reject_approvals}

        if require:
            self.require_filter = GerritRefFilter.requiresFromConfig(
                connection_name, require, parse_context)
        else:
            self.require_filter = None

        if reject:
            self.reject_filter = GerritRefFilter.rejectFromConfig(
                connection_name, reject, parse_context)
        else:
            self.reject_filter = None

        self._types = [x.pattern for x in types]
        self._branches = [x.pattern for x in branches]
        self._refs = [x.pattern for x in refs]
        self._comments = [x.pattern for x in comments]
        self._emails = [x.pattern for x in emails]
        self._usernames = [x.pattern for x in usernames]
        self._added = [x.pattern for x in added]
        self._removed = [x.pattern for x in removed]
        self.types = types
        self.branches = branches
        self.refs = refs
        self.comments = comments
        self.emails = emails
        self.usernames = usernames
        self.added = added
        self.removed = removed
        self.event_approvals = event_approvals
        self.event_approval_changes = event_approval_changes
        self.uuid = uuid
        self.scheme = scheme
        self.ignore_deletes = ignore_deletes

        self._hash = hash(json.dumps(self.toDict(), sort_keys=True))

    def __hash__(self):
        return self._hash

    def toDict(self):
        if self.require_filter:
            require_filter = self.require_filter.toDict()
        else:
            require_filter = None
        if self.reject_filter:
            reject_filter = self.reject_filter.toDict()
        else:
            reject_filter = None
        return dict(
            require_filter=require_filter,
            reject_filter=reject_filter,
            types=self._types,
            branches=self._branches,
            refs=self._refs,
            comments=self._comments,
            emails=self._emails,
            usernames=self._usernames,
            added=self._added,
            removed=self._removed,
            event_approvals=self.event_approvals,
            event_approval_changes=self.event_approval_changes,
            uuid=self.uuid,
            scheme=self.scheme,
            ignore_deletes=self.ignore_deletes,
        )

    def __eq__(self, other):
        return (
            isinstance(other, GerritEventFilter) and
            self.require_filter == other.require_filter and
            self.reject_filter == other.reject_filter and
            self.types == other.types and
            self.branches == other.branches and
            self.refs == other.refs and
            self.comments == other.comments and
            self.emails == other.emails and
            self.usernames == other.usernames and
            self.added == other.added and
            self.removed == other.removed and
            self.event_approvals == other.event_approvals and
            self.event_approval_changes == other.event_approval_changes and
            self.uuid == other.uuid and
            self.scheme == other.scheme and
            self.ignore_deletes == other.ignore_deletes
        )

    def __repr__(self):
        ret = '<GerritEventFilter'
        ret += ' connection: %s' % self.connection_name

        if self._types:
            ret += ' types: %s' % ', '.join(self._types)
        if self.uuid:
            ret += ' uuid: %s' % (self.uuid,)
        if self.scheme:
            ret += ' scheme: %s' % (self.scheme,)
        if self._branches:
            ret += ' branches: %s' % ', '.join(self._branches)
        if self._refs:
            ret += ' refs: %s' % ', '.join(self._refs)
        if self.ignore_deletes:
            ret += ' ignore_deletes: %s' % self.ignore_deletes
        if self.event_approvals:
            ret += ' event_approvals: %s' % ', '.join(
                ['%s:%s' % a for a in self.event_approvals.items()])
        if self.event_approval_changes:
            ret += ' event_approval_changes: %s' % ', '.join(
                ['%s:%s' % a for a in self.event_approval_changes.items()])
        if self._comments:
            ret += ' comments: %s' % ', '.join(self._comments)
        if self._emails:
            ret += ' emails: %s' % ', '.join(self._emails)
        if self._usernames:
            ret += ' usernames: %s' % ', '.join(self._usernames)
        if self._added:
            ret += ' added: %s' % ', '.join(self._added)
        if self._removed:
            ret += ' removed: %s' % ', '.join(self._removed)
        if self.require_filter:
            ret += ' require: %s' % repr(self.require_filter)
        if self.reject_filter:
            ret += ' reject: %s' % repr(self.reject_filter)
        ret += '>'

        return ret

    def _checkEventType(self, event_type):
        matches_type = False
        for etype in self.types:
            if etype.match(event_type):
                matches_type = True
        if self.types and not matches_type:
            return FalseWithReason("Types %s do not match %s" % (
                self.types, event_type))

    def _checkEventBranch(self, event_branch):
        # branches are ORed
        matches_branch = False
        for branch in self.branches:
            if branch.match(event_branch):
                matches_branch = True
        if self.branches and not matches_branch:
            return FalseWithReason("Branches %s do not match %s" % (
                self.branches, event_branch))

    def _checkEventRef(self, event_ref):
        # refs are ORed
        matches_ref = False
        if event_ref is not None:
            for ref in self.refs:
                if ref.match(event_ref):
                    matches_ref = True
        if self.refs and not matches_ref:
            return FalseWithReason(
                "Refs %s do not match %s" % (self.refs, event_ref))

    def preFilter(self, event):
        # This is specific to the gerrit driver.
        # Perform some quick checks to see if we can ignore this
        # event.  If we don't know that we can ignore it, it will be
        # enqueued and go through the normal matching process.
        r = self._checkEventType(event.type)
        if r is not None:
            return r

        r = self._checkEventBranch(event.branch)
        if r is not None:
            return r

        r = self._checkEventRef(event.ref)
        if r is not None:
            return r

        return True

    def matches(self, event, change):
        if not super().matches(event, change):
            return False

        r = self._checkEventType(event.type)
        if r is not None:
            return r

        if event.type == 'pending-check':
            if self.uuid and event.uuid != self.uuid:
                return False
            if self.scheme and event.uuid.split(':')[0] != self.scheme:
                return False

        r = self._checkEventBranch(event.branch)
        if r is not None:
            return r

        r = self._checkEventRef(event.ref)
        if r is not None:
            return r

        if self.ignore_deletes and event.newrev == EMPTY_GIT_REF:
            # If the updated ref has an empty git sha (all 0s),
            # then the ref is being deleted
            return FalseWithReason("Ref deletion events are ignored")

        # comments are ORed
        matches_comment_re = False
        for comment_re in self.comments:
            if (event.comment is not None and
                comment_re.search(event.comment)):
                matches_comment_re = True
            if event.patchsetcomments is not None:
                for comment in event.patchsetcomments:
                    if (comment is not None and
                        comment_re.search(comment)):
                        matches_comment_re = True
        if self.comments and not matches_comment_re:
            return FalseWithReason("Comments %s do not match %s" % (
                self.comments, event.patchsetcomments))

        # We better have an account provided by Gerrit to do
        # email filtering.
        if event.account is not None:
            account_email = event.account.get('email')
            # emails are ORed
            matches_email_re = False
            for email_re in self.emails:
                if (account_email is not None and
                        email_re.search(account_email)):
                    matches_email_re = True
            if self.emails and not matches_email_re:
                return FalseWithReason("Username %s does not match %s" % (
                    self.emails, account_email))

            # usernames are ORed
            account_username = event.account.get('username')
            matches_username_re = False
            for username_re in self.usernames:
                if (account_username is not None and
                    username_re.search(account_username)):
                    matches_username_re = True
            if self.usernames and not matches_username_re:
                return FalseWithReason("Username %s does not match %s" % (
                    self.usernames, account_username))

        # approvals are ANDed
        for category, value in self.event_approvals.items():
            matches_approval = False
            for eapp in event.approvals:
                if (eapp['description'] == category and
                        int(eapp['value']) == int(value)):
                    matches_approval = True
            if not matches_approval:
                return FalseWithReason("Approvals %s do not match %s" % (
                    self.event_approvals, event.approvals))

        for category, value in self.event_approval_changes.items():
            matches_approval = False
            for eapp in event.approvals:
                if (eapp['description'] == category and
                        int(eapp['value']) == int(value) and
                        'oldValue' in eapp and
                        int(eapp['value']) != int(eapp['oldValue'])):
                    matches_approval = True
            if not matches_approval:
                return FalseWithReason(
                    "Changed approvals %s do not match %s" % (
                        self.event_approval_changes, event.approvals))

        # hashtags are ORed
        if self.added:
            matches_token = False
            event_added = event.added or []
            for action_re in self.added:
                if matches_token:
                    break
                for token in event_added:
                    if action_re.search(token):
                        matches_token = True
                        break
            if not matches_token:
                return FalseWithReason("Added %s does not match %s" % (
                    self.added, event.added))
        if self.removed:
            matches_token = False
            event_removed = event.removed or []
            for action_re in self.removed:
                if matches_token:
                    break
                for token in event_removed:
                    if action_re.search(token):
                        matches_token = True
                        break
            if not matches_token:
                return FalseWithReason("Removed %s does not match %s" % (
                    self.removed, event.removed))

        if self.require_filter:
            require_filter_result = self.require_filter.matches(change)
            if not require_filter_result:
                return require_filter_result

        if self.reject_filter:
            reject_filter_result = self.reject_filter.matches(change)
            if not reject_filter_result:
                return reject_filter_result

        return True


class GerritRefFilter(RefFilter):
    def __init__(self, connection_name,
                 parse_context,
                 open=None, reject_open=None,
                 current_patchset=None, reject_current_patchset=None,
                 wip=None, reject_wip=None,
                 statuses=[], reject_statuses=[],
                 required_approvals=[], reject_approvals=[],
                 required_hashtags=[], reject_hashtags=[]):
        RefFilter.__init__(self, connection_name)

        self._required_approvals = required_approvals
        self.required_approvals = self._tidy_approvals(
            copy.deepcopy(required_approvals), parse_context)
        self._reject_approvals = reject_approvals
        self.reject_approvals = self._tidy_approvals(
            copy.deepcopy(reject_approvals), parse_context)
        self.statuses = statuses
        self.reject_statuses = reject_statuses
        self.required_hashtags = required_hashtags
        self.reject_hashtags = reject_hashtags

        if reject_open is not None:
            self.open = not reject_open
        else:
            self.open = open
        if reject_wip is not None:
            self.wip = not reject_wip
        else:
            self.wip = wip
        if reject_current_patchset is not None:
            self.current_patchset = not reject_current_patchset
        else:
            self.current_patchset = current_patchset

    def toDict(self):
        return dict(
            required_approvals=self._required_approvals,
            reject_approvals=self._reject_approvals,
            statuses=self.statuses,
            reject_statuses=self.reject_statuses,
            required_hashtags=self.required_hashtags,
            reject_hashtags=self.reject_hashtags,
            open=self.open,
            wip=self.wip,
            current_patchset=self.current_patchset,
        )

    def __eq__(self, other):
        return (
            isinstance(other, GerritRefFilter) and
            self.required_approvals == other.required_approvals and
            self.reject_approvals == other.reject_approvals and
            self.statuses == other.statuses and
            self.reject_statuses == other.reject_statuses and
            self.required_hashtags == other.required_hashtags and
            self.reject_hashtags == other.reject_hashtags and
            self.open == other.open and
            self.wip == other.wip and
            self.current_patchset == other.current_patchset
        )

    @classmethod
    def requiresFromConfig(cls, connection_name, config, parse_context):
        with parse_context.confAttr(config, 'hashtags') as attr:
            hashtags = [make_regex(x, parse_context) for x in to_list(attr)]
        return cls(
            connection_name=connection_name,
            parse_context=parse_context,
            open=config.get('open'),
            current_patchset=config.get('current-patchset'),
            wip=config.get('wip'),
            statuses=to_list(config.get('status')),
            required_approvals=to_list(config.get('approval')),
            required_hashtags=hashtags,
        )

    @classmethod
    def rejectFromConfig(cls, connection_name, config, parse_context):
        with parse_context.confAttr(config, 'hashtags') as attr:
            hashtags = [make_regex(x, parse_context) for x in to_list(attr)]
        return cls(
            connection_name=connection_name,
            parse_context=parse_context,
            reject_open=config.get('open'),
            reject_current_patchset=config.get('current-patchset'),
            reject_wip=config.get('wip'),
            reject_statuses=to_list(config.get('status')),
            reject_approvals=to_list(config.get('approval')),
            reject_hashtags=hashtags,
        )

    def __repr__(self):
        ret = '<GerritRefFilter'

        ret += ' connection_name: %s' % self.connection_name
        if self.open is not None:
            ret += ' open: %s' % self.open
        if self.wip is not None:
            ret += ' wip: %s' % self.wip
        if self.current_patchset is not None:
            ret += ' current-patchset: %s' % self.current_patchset
        if self.statuses:
            ret += ' statuses: %s' % ', '.join(self.statuses)
        if self.reject_statuses:
            ret += ' reject-statuses: %s' % ', '.join(self.reject_statuses)
        if self.required_approvals:
            ret += (' required-approvals: %s' %
                    str(self.required_approvals))
        if self.reject_approvals:
            ret += (' reject-approvals: %s' %
                    str(self.reject_approvals))
        if self.required_hashtags:
            ret += (' required-hashtags: %s' %
                    [x.pattern for x in self.required_hashtags])
        if self.reject_hashtags:
            ret += (' reject-hashtags: %s' %
                    [x.pattern for x in self.reject_hashtags])
        ret += '>'

        return ret

    def matches(self, change):
        if self.open is not None:
            # if a "change" has no number, it's not a change, but a push
            # and cannot possibly pass this test.
            if hasattr(change, 'number'):
                if self.open != change.open:
                    return FalseWithReason(
                        "Change does not match open requirement")
            else:
                return FalseWithReason("Ref is not a Change")

        if self.current_patchset is not None:
            # if a "change" has no number, it's not a change, but a push
            # and cannot possibly pass this test.
            if hasattr(change, 'number'):
                if self.current_patchset != change.is_current_patchset:
                    return FalseWithReason(
                        "Change does not match current patchset requirement")
            else:
                return FalseWithReason("Ref is not a Change")

        if self.wip is not None:
            # if a "change" has no number, it's not a change, but a push
            # and cannot possibly pass this test.
            if hasattr(change, 'number'):
                if self.wip != change.wip:
                    return FalseWithReason(
                        "Change does not match WIP requirement")
            else:
                return FalseWithReason("Ref is not a Change")

        if self.statuses:
            if change.status not in self.statuses:
                return FalseWithReason(
                    "Required statuses %s do not match %s" % (
                        self.statuses, change.status))
        if self.reject_statuses:
            if change.status in self.reject_statuses:
                return FalseWithReason(
                    "Reject statuses %s match %s" % (
                        self.reject_statuses, change.status))

        for hashtag_re in self.required_hashtags:
            matches_hashtag = False
            for token in change.hashtags:
                if hashtag_re.search(token):
                    matches_hashtag = True
                    break
            if not matches_hashtag:
                return FalseWithReason(
                    "Required hashtags %s do not match %s" % (
                        [x.pattern for x in self.required_hashtags],
                        change.hashtags))
        for hashtag_re in self.reject_hashtags:
            for token in change.hashtags:
                if hashtag_re.search(token):
                    return FalseWithReason(
                        "Reject hashtags %s match %s" % (
                            [x.pattern for x in self.reject_hashtags],
                            change.hashtags))

        # required approvals are ANDed (reject approvals are ORed)
        matches_approvals_result = self.matchesApprovals(change)
        if not matches_approvals_result:
            return matches_approvals_result

        return True

    def _tidy_approvals(self, approvals, parse_context):
        for a in approvals:
            if 'username' in a:
                with parse_context.confAttr(a, 'username') as v:
                    a['username'] = make_regex(v, parse_context)
            if 'email' in a:
                with parse_context.confAttr(a, 'email') as v:
                    a['email'] = make_regex(v, parse_context)
            if 'newer-than' in a:
                with parse_context.confAttr(a, 'newer-than') as v:
                    a['newer-than'] = time_to_seconds(v)
            if 'older-than' in a:
                with parse_context.confAttr(a, 'older-than') as v:
                    a['older-than'] = time_to_seconds(v)
        return approvals

    def _match_approval_required_approval(self, rapproval, approval):
        # Check if the required approval and approval match
        if 'description' not in approval:
            return False
        now = time.time()
        by = approval.get('by', {})
        for k, v in rapproval.items():
            if k == 'username':
                if (not v.search(by.get('username', ''))):
                    return False
            elif k == 'email':
                if (not v.search(by.get('email', ''))):
                    return False
            elif k == 'newer-than':
                t = now - v
                if (approval['grantedOn'] < t):
                    return False
            elif k == 'older-than':
                t = now - v
                if (approval['grantedOn'] >= t):
                    return False
            else:
                if not isinstance(v, list):
                    v = [v]
                if (approval['description'] != k or
                        int(approval['value']) not in v):
                    return False
        return True

    def matchesApprovals(self, change):
        if self.required_approvals or self.reject_approvals:
            if not hasattr(change, 'number'):
                # Not a change, no reviews
                return FalseWithReason("Ref is not a Change")
        if self.required_approvals and not change.approvals:
            # A change with no approvals can not match
            return FalseWithReason("Approvals %s does not match %s" % (
                self.required_approvals, change.approvals))

        # TODO(jhesketh): If we wanted to optimise this slightly we could
        # analyse both the REQUIRE and REJECT filters by looping over the
        # approvals on the change and keeping track of what we have checked
        # rather than needing to loop on the change approvals twice
        return (self.matchesRequiredApprovals(change) and
                self.matchesNoRejectApprovals(change))

    def matchesRequiredApprovals(self, change):
        # Check if any approvals match the requirements
        for rapproval in self.required_approvals:
            matches_rapproval = False
            for approval in change.approvals:
                if self._match_approval_required_approval(rapproval, approval):
                    # We have a matching approval so this requirement is
                    # fulfilled
                    matches_rapproval = True
                    break
            if not matches_rapproval:
                return FalseWithReason(
                    "Required approvals %s do not match %s" % (
                        self.required_approvals, change.approvals))
        return True

    def matchesNoRejectApprovals(self, change):
        # Check to make sure no approvals match a reject criteria
        for rapproval in self.reject_approvals:
            for approval in change.approvals:
                if self._match_approval_required_approval(rapproval, approval):
                    # A reject approval has been matched, so we reject
                    # immediately
                    return FalseWithReason("Reject approvals %s match %s" % (
                        self.reject_approvals, change.approvals))
        # To get here no rejects can have been matched so we should be good to
        # queue
        return True
