# Copyright 2015 Hewlett-Packard Development Company, L.P.
# Copyright 2017 IBM Corp.
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
import re
import re2
import time

from zuul.model import Change, TriggerEvent, EventFilter, RefFilter
from zuul.model import FalseWithReason
from zuul.driver.util import time_to_seconds, to_list

EMPTY_GIT_REF = '0' * 40  # git sha of all zeros, used during creates/deletes


class PullRequest(Change):
    def __init__(self, project):
        super(PullRequest, self).__init__(project)
        self.pr = None
        self.updated_at = None
        self.title = None
        self.body_text = None
        self.reviews = []
        self.files = []
        self.labels = []
        self.draft = None
        self.mergeable = True
        self.review_decision = None
        self.unresolved_conversations = None
        self.unsigned_commits = None
        self.required_contexts = set()
        self.contexts = set()
        self.branch_protected = False
        self.merge_commit_sha = None

    @property
    def status(self):
        return ["{}:{}:{}".format(*c) for c in self.contexts]

    @property
    def successful_contexts(self) -> set:
        if not self.contexts:
            return set()
        return set(
            s[1] for s in self.contexts if s[2] == 'success'
        )

    def isUpdateOf(self, other):
        if (self.project == other.project and
            hasattr(other, 'number') and self.number == other.number and
            hasattr(other, 'patchset') and self.patchset != other.patchset and
            hasattr(other, 'updated_at') and
            self.updated_at > other.updated_at):
            return True
        return False

    def serialize(self):
        d = super().serialize()
        d.update({
            "pr": self.pr,
            "updated_at": self.updated_at,
            "title": self.title,
            "body_text": self.body_text,
            "reviews": list(self.reviews),
            "labels": self.labels,
            "draft": self.draft,
            "mergeable": self.mergeable,
            "review_decision": self.review_decision,
            "unresolved_conversations": self.unresolved_conversations,
            "unsigned_commits": self.unsigned_commits,
            "required_contexts": list(self.required_contexts),
            "contexts": list(self.contexts),
            "branch_protected": self.branch_protected,
        })
        return d

    def deserialize(self, data):
        super().deserialize(data)
        self.pr = data.get("pr")
        self.updated_at = data.get("updated_at")
        self.title = data.get("title")
        self.body_text = data.get("body_text")
        self.reviews = data.get("reviews", [])
        self.labels = data.get("labels", [])
        self.draft = data.get("draft")
        self.mergeable = data.get("mergeable", True)
        self.review_decision = data.get("review_decision")
        self.unresolved_conversations = data.get("unresolved_conversations")
        self.unsigned_commits = data.get("unsigned_commits")
        self.required_contexts = set(data.get("required_contexts", []))
        self.contexts = set(tuple(c) for c in data.get("contexts", []))
        self.branch_protected = data.get("branch_protected", False)


class GithubTriggerEvent(TriggerEvent):
    def __init__(self):
        super(GithubTriggerEvent, self).__init__()
        self.title = None
        self.label = None
        self.unlabel = None
        self.action = None
        self.delivery = None
        self.check_run = None
        self.status = None
        self.commits = []
        self.body_edited = None
        self.branch_protection_changed = None
        self.default_branch_changed = None

    def toDict(self):
        d = super().toDict()
        d["title"] = self.title
        d["label"] = self.label
        d["unlabel"] = self.unlabel
        d["action"] = self.action
        d["delivery"] = self.delivery
        d["check_run"] = self.check_run
        d["status"] = self.status
        d["commits"] = self.commits
        d["body_edited"] = self.body_edited
        d["branch_protection_changed"] = self.branch_protection_changed
        d["default_branch_changed"] = self.default_branch_changed
        return d

    def updateFromDict(self, d):
        super().updateFromDict(d)
        self.title = d["title"]
        self.label = d["label"]
        self.unlabel = d["unlabel"]
        self.action = d["action"]
        self.delivery = d["delivery"]
        self.check_run = d["check_run"]
        self.status = d["status"]
        self.commits = d["commits"]
        self.body_edited = d["body_edited"]
        self.branch_protection_changed = d.get("branch_protection_changed")
        self.default_branch_changed = d.get("default_branch_changed")

    def isBranchProtectionChanged(self):
        return bool(self.branch_protection_changed)

    def isDefaultBranchChanged(self):
        return bool(self.default_branch_changed)

    def isPatchsetCreated(self):
        if self.type == 'pull_request':
            return self.action in ['opened', 'changed']
        return False

    def isMessageChanged(self):
        return bool(self.body_edited)

    def isChangeAbandoned(self):
        if self.type == 'pull_request':
            return 'closed' == self.action
        return False

    def _repr(self):
        r = [super(GithubTriggerEvent, self)._repr()]
        if self.action:
            r.append(self.action)
        r.append(self.canonical_project_name)
        if self.change_number:
            r.append('%s,%s' % (self.change_number, self.patch_number))
        if self.delivery:
            r.append('delivery: %s' % self.delivery)
        if self.check_run:
            r.append('check_run: %s' % self.check_run)
        return ' '.join(r)


class GithubEventFilter(EventFilter):
    def __init__(self, connection_name, trigger, types=[],
                 branches=[], refs=[], comments=[], actions=[],
                 labels=[], unlabels=[], states=[], statuses=[],
                 required_statuses=[], check_runs=[],
                 ignore_deletes=True,
                 require=None, reject=None, debug=None):

        EventFilter.__init__(self, connection_name, trigger, debug)

        # TODO: Backwards compat, remove after 9.x:
        if required_statuses and require is None:
            require = {'status': required_statuses}

        if require:
            self.require_filter = GithubRefFilter.requiresFromConfig(
                connection_name, require)
        else:
            self.require_filter = None

        if reject:
            self.reject_filter = GithubRefFilter.rejectFromConfig(
                connection_name, reject)
        else:
            self.reject_filter = None

        self._types = [x.pattern for x in types]
        self._branches = [x.pattern for x in branches]
        self._refs = [x.pattern for x in refs]
        self._comments = [x.pattern for x in comments]
        self.types = types
        self.branches = branches
        self.refs = refs
        self.comments = comments
        self.actions = actions
        self.labels = labels
        self.unlabels = unlabels
        self.states = states
        self.statuses = statuses
        self.check_runs = check_runs
        self.ignore_deletes = ignore_deletes

    def __repr__(self):
        ret = '<GithubEventFilter'
        ret += ' connection: %s' % self.connection_name
        if self._types:
            ret += ' types: %s' % ', '.join(self._types)
        if self._branches:
            ret += ' branches: %s' % ', '.join(self._branches)
        if self._refs:
            ret += ' refs: %s' % ', '.join(self._refs)
        if self.ignore_deletes:
            ret += ' ignore_deletes: %s' % self.ignore_deletes
        if self._comments:
            ret += ' comments: %s' % ', '.join(self._comments)
        if self.actions:
            ret += ' actions: %s' % ', '.join(self.actions)
        if self.check_runs:
            ret += ' check_runs: %s' % ','.join(self.check_runs)
        if self.labels:
            ret += ' labels: %s' % ', '.join(self.labels)
        if self.unlabels:
            ret += ' unlabels: %s' % ', '.join(self.unlabels)
        if self.states:
            ret += ' states: %s' % ', '.join(self.states)
        if self.statuses:
            ret += ' statuses: %s' % ', '.join(self.statuses)
        if self.require_filter:
            ret += ' require: %s' % repr(self.require_filter)
        if self.reject_filter:
            ret += ' reject: %s' % repr(self.reject_filter)
        ret += '>'

        return ret

    def matches(self, event, change):
        if not super().matches(event, change):
            return False

        # event types are ORed
        matches_type = False
        for etype in self.types:
            if etype.match(event.type):
                matches_type = True
        if self.types and not matches_type:
            return FalseWithReason("Types %s do not match %s" % (
                self.types, event.type))

        # branches are ORed
        matches_branch = False
        for branch in self.branches:
            if branch.match(event.branch):
                matches_branch = True
        if self.branches and not matches_branch:
            return FalseWithReason("Branches %s do not match %s" % (
                self.branches, event.branch))

        # refs are ORed
        matches_ref = False
        if event.ref is not None:
            for ref in self.refs:
                if ref.match(event.ref):
                    matches_ref = True
        if self.refs and not matches_ref:
            return FalseWithReason(
                "Refs %s do not match %s" % (self.refs, event.ref))
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
        if self.comments and not matches_comment_re:
            return FalseWithReason("Comments %s do not match %s" % (
                self.comments, event.comment))

        # actions are ORed
        matches_action = False
        for action in self.actions:
            if (event.action == action):
                matches_action = True
        if self.actions and not matches_action:
            return FalseWithReason("Actions %s do not match %s" % (
                self.actions, event.action))

        # check_runs are ORed
        if self.check_runs:
            check_run_found = False
            for check_run in self.check_runs:
                # TODO: construct as ZuulRegex in initializer when re2
                # migration is complete.
                if re2.fullmatch(check_run, event.check_run):
                    check_run_found = True
                    break
            if not check_run_found:
                return FalseWithReason("Check runs %s do not match %s" % (
                    self.check_runs, event.check_run))

        # labels are ORed
        if self.labels and event.label not in self.labels:
            return FalseWithReason("Labels %s do not match %s" % (
                self.labels, event.label))

        # unlabels are ORed
        if self.unlabels and event.unlabel not in self.unlabels:
            return FalseWithReason("Unlabels %s do not match %s" % (
                self.unlabels, event.unlabel))

        # states are ORed
        if self.states and event.state not in self.states:
            return FalseWithReason("States %s do not match %s" % (
                self.states, event.state))

        # statuses are ORed
        if self.statuses:
            status_found = False
            for status in self.statuses:
                # TODO: construct as ZuulRegex in initializer when re2
                # migration is complete.
                if re2.fullmatch(status, event.status):
                    status_found = True
                    break
            if not status_found:
                return FalseWithReason("Statuses %s do not match %s" % (
                    self.statuses, event.status))

        if self.require_filter:
            require_filter_result = self.require_filter.matches(change)
            if not require_filter_result:
                return require_filter_result

        if self.reject_filter:
            reject_filter_result = self.reject_filter.matches(change)
            if not reject_filter_result:
                return reject_filter_result

        return True


class GithubRefFilter(RefFilter):
    def __init__(self, connection_name, statuses=[],
                 reviews=[], reject_reviews=[], open=None,
                 merged=None, current_patchset=None, draft=None,
                 reject_open=None, reject_merged=None,
                 reject_current_patchset=None, reject_draft=None,
                 labels=[], reject_labels=[], reject_statuses=[]):
        RefFilter.__init__(self, connection_name)

        self._required_reviews = copy.deepcopy(reviews)
        self._reject_reviews = copy.deepcopy(reject_reviews)
        self.required_reviews = self._tidy_reviews(self._required_reviews)
        self.reject_reviews = self._tidy_reviews(self._reject_reviews)
        self.required_statuses = statuses
        self.reject_statuses = reject_statuses
        self.required_labels = labels
        self.reject_labels = reject_labels

        if reject_open is not None:
            self.open = not reject_open
        else:
            self.open = open
        if reject_merged is not None:
            self.merged = not reject_merged
        else:
            self.merged = merged
        if reject_current_patchset is not None:
            self.current_patchset = not reject_current_patchset
        else:
            self.current_patchset = current_patchset
        if reject_draft is not None:
            self.draft = not reject_draft
        else:
            self.draft = draft

    @classmethod
    def requiresFromConfig(cls, connection_name, config):
        return cls(
            connection_name=connection_name,
            statuses=to_list(config.get('status')),
            reviews=to_list(config.get('review')),
            labels=to_list(config.get('label')),
            open=config.get('open'),
            merged=config.get('merged'),
            current_patchset=config.get('current-patchset'),
            draft=config.get('draft'),
        )

    @classmethod
    def rejectFromConfig(cls, connection_name, config):
        return cls(
            connection_name=connection_name,
            reject_statuses=to_list(config.get('status')),
            reject_reviews=to_list(config.get('review')),
            reject_labels=to_list(config.get('label')),
            reject_open=config.get('open'),
            reject_merged=config.get('merged'),
            reject_current_patchset=config.get('current-patchset'),
            reject_draft=config.get('draft'),
        )

    def __repr__(self):
        ret = '<GithubRefFilter'

        ret += ' connection_name: %s' % self.connection_name
        if self.required_statuses:
            ret += ' status: %s' % str(self.required_statuses)
        if self.reject_statuses:
            ret += ' reject-status: %s' % str(self.reject_statuses)
        if self.required_reviews:
            ret += (' reviews: %s' %
                    str(self.required_reviews))
        if self.reject_reviews:
            ret += (' reject-reviews: %s' %
                    str(self.reject_reviews))
        if self.required_labels:
            ret += ' labels: %s' % str(self.required_labels)
        if self.reject_labels:
            ret += ' reject-labels: %s' % str(self.reject_labels)
        if self.open is not None:
            ret += ' open: %s' % self.open
        if self.merged is not None:
            ret += ' merged: %s' % self.merged
        if self.current_patchset is not None:
            ret += ' current-patchset: %s' % self.current_patchset
        if self.draft is not None:
            ret += ' draft: %s' % self.draft

        ret += '>'

        return ret

    def _tidy_reviews(self, reviews):
        for r in reviews:
            for k, v in r.items():
                if k == 'username':
                    r['username'] = re.compile(v)
                elif k == 'email':
                    r['email'] = re.compile(v)
                elif k == 'newer-than':
                    r[k] = time_to_seconds(v)
                elif k == 'older-than':
                    r[k] = time_to_seconds(v)
        return reviews

    def _match_review_required_review(self, rreview, review):
        # Check if the required review and review match
        now = time.time()
        by = review.get('by', {})
        for k, v in rreview.items():
            if k == 'username':
                if (not v.search(by.get('username', ''))):
                    return False
            elif k == 'email':
                if (not v.search(by.get('email', ''))):
                    return False
            elif k == 'newer-than':
                t = now - v
                if (review['grantedOn'] < t):
                    return False
            elif k == 'older-than':
                t = now - v
                if (review['grantedOn'] >= t):
                    return False
            elif k == 'type':
                if review['type'] != v:
                    return False
            elif k == 'permission':
                # If permission is read, we've matched. You must have read
                # to provide a review.
                if v != 'read':
                    # Admins have implicit write.
                    if v == 'write':
                        if review['permission'] not in ('write', 'admin'):
                            return False
                    elif v == 'admin':
                        if review['permission'] != 'admin':
                            return False
        return True

    def matchesReviews(self, change):
        if self.required_reviews or self.reject_reviews:
            if not hasattr(change, 'number'):
                # not a PR, no reviews
                return FalseWithReason("Change is not a PR")
            if self.required_reviews and not change.reviews:
                # No reviews means no matching of required bits
                # having reject reviews but no reviews on the change is okay
                return FalseWithReason("Reviews %s do not match %s" % (
                    self.required_reviews, change.reviews))

        return (self.matchesRequiredReviews(change) and
                self.matchesNoRejectReviews(change))

    def matchesRequiredReviews(self, change):
        for rreview in self.required_reviews:
            matches_review = False
            for review in change.reviews:
                if self._match_review_required_review(rreview, review):
                    # Consider matched if any review matches
                    matches_review = True
                    break
            if not matches_review:
                return FalseWithReason(
                    "Required reviews %s do not match %s" % (
                        self.required_reviews, change.reviews))
        return True

    def matchesNoRejectReviews(self, change):
        for rreview in self.reject_reviews:
            for review in change.reviews:
                if self._match_review_required_review(rreview, review):
                    # A review matched, we can reject right away
                    return FalseWithReason("Reject reviews %s match %s" % (
                        self.reject_reviews, change.reviews))
        return True

    def matchesStatuses(self, change):
        if self.required_statuses or self.reject_statuses:
            if not hasattr(change, 'number'):
                # not a PR, no status
                return FalseWithReason("Can not match statuses without PR")
            if self.required_statuses and not change.status:
                return FalseWithReason(
                    "Required statuses %s do not match %s" % (
                        self.required_statuses, change.status))
        required_statuses_results = self.matchesRequiredStatuses(change)
        if not required_statuses_results:
            return required_statuses_results
        return self.matchesNoRejectStatuses(change)

    def matchesRequiredStatuses(self, change):
        # statuses are ORed
        # A PR head can have multiple statuses on it. If the change
        # statuses and the filter statuses are a null intersection, there
        # are no matches and we return false
        if self.required_statuses:
            for required_status in self.required_statuses:
                for status in change.status:
                    # TODO: construct as ZuulRegex in initializer when
                    # re2 migration is complete.
                    if re2.fullmatch(required_status, status):
                        return True
            return FalseWithReason("Required statuses %s do not match %s" % (
                self.required_statuses, change.status))
        return True

    def matchesNoRejectStatuses(self, change):
        # statuses are ANDed
        # If any of the rejected statusses are present, we return false
        for rstatus in self.reject_statuses:
            for status in change.status:
                # TODO: construct as ZuulRegex in initializer when re2
                # migration is complete.
                if re2.fullmatch(rstatus, status):
                    return FalseWithReason("Reject statuses %s match %s" % (
                        self.reject_statuses, change.status))
        return True

    def matchesLabels(self, change):
        if self.required_labels or self.reject_labels:
            if not hasattr(change, 'number'):
                # not a PR, no label
                return FalseWithReason("Can not match labels without PR")
            if self.required_labels and not change.labels:
                # No labels means no matching of required bits
                # having reject labels but no labels on the change is okay
                return FalseWithReason(
                    "Required labels %s does not match %s" % (
                        self.required_labels, change.labels))
        return (self.matchesRequiredLabels(change) and
                self.matchesNoRejectLabels(change))

    def matchesRequiredLabels(self, change):
        for label in self.required_labels:
            if label not in change.labels:
                return FalseWithReason("Labels %s do not match %s" % (
                    self.required_labels, change.labels))
        return True

    def matchesNoRejectLabels(self, change):
        for label in self.reject_labels:
            if label in change.labels:
                return FalseWithReason("Reject labels %s match %s" % (
                    self.reject_labels, change.labels))
        return True

    def matches(self, change):
        statuses_result = self.matchesStatuses(change)
        if not statuses_result:
            return statuses_result

        reviews_result = self.matchesReviews(change)
        if not reviews_result:
            return reviews_result

        labels_result = self.matchesLabels(change)
        if not labels_result:
            return labels_result

        if self.open is not None:
            # if a "change" has no number, it's not a change, but a push
            # and cannot possibly pass this test.
            if hasattr(change, 'number'):
                if self.open != change.open:
                    return FalseWithReason(
                        "Change does not match open requirement")
            else:
                return FalseWithReason("Change is not a PR")

        if self.merged is not None:
            # if a "change" has no number, it's not a change, but a push
            # and cannot possibly pass this test.
            if hasattr(change, 'number'):
                if self.merged != change.is_merged:
                    return FalseWithReason(
                        "Change does not match merged requirement")
            else:
                return FalseWithReason("Change is not a PR")

        if self.current_patchset is not None:
            # if a "change" has no number, it's not a change, but a push
            # and cannot possibly pass this test.
            if hasattr(change, 'number'):
                if self.current_patchset != change.is_current_patchset:
                    return FalseWithReason(
                        "Change does not match current-patchset requirement")
            else:
                return FalseWithReason("Change is not a PR")

        if self.draft is not None:
            # if a "change" has no number, it's not a change, but a push
            # and cannot possibly pass this test.
            if hasattr(change, 'number'):
                if self.draft != change.draft:
                    return FalseWithReason(
                        "Change does not match draft requirement")
            else:
                return FalseWithReason("Change is not a PR")

        return True
