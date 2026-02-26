# Copyright 2019 BMW Group
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

from graphene import (
    Boolean,
    Field,
    Int,
    Interface,
    List,
    ObjectType,
    Schema,
    String,
)


class ID(String):
    """Github global object ids are strings"""
    pass


class Node(Interface):
    id = ID(required=True)

    @classmethod
    def resolve_type(cls, instance, info):
        kind = getattr(instance, '_graphene_type', None)
        if kind == 'BranchProtectionRule':
            return BranchProtectionRule
        elif kind == 'Commit':
            return Commit
        elif kind == 'CheckSuite':
            return CheckSuite
        elif kind == 'PullRequest':
            return PullRequest


class PageInfo(ObjectType):
    endCursor = String()
    hasPreviousPage = Boolean()
    hasNextPage = Boolean()

    def resolve_endCursor(parent, info):
        return str(parent['after'] + parent['first'])

    def resolve_hasPreviousPage(parent, info):
        return parent['after'] > 0

    def resolve_hasNextPage(parent, info):
        return parent['after'] + parent['first'] < parent['length']


class MatchingRef(ObjectType):
    name = String()

    def resolve_name(parent, info):
        return parent


class MatchingRefs(ObjectType):
    pageInfo = Field(PageInfo)
    nodes = List(MatchingRef)

    def resolve_nodes(parent, info):
        return parent['nodes']

    def resolve_pageInfo(parent, info):
        return parent


class BranchProtectionRule(ObjectType):

    class Meta:
        interfaces = (Node, )

    id = ID(required=True)
    pattern = String()
    requiredStatusCheckContexts = List(String)
    requiresApprovingReviews = Boolean()
    requiresCommitSignatures = Boolean()
    requiresConversationResolution = Boolean()
    requiresCodeOwnerReviews = Boolean()
    matchingRefs = Field(MatchingRefs, first=Int(), after=String(),
                         query=String())
    lockBranch = Boolean()

    def resolve_id(parent, info):
        return parent.id

    def resolve_pattern(parent, info):
        return parent.pattern

    def resolve_requiredStatusCheckContexts(parent, info):
        return parent.required_contexts

    def resolve_requiresApprovingReviews(parent, info):
        return parent.require_reviews

    def resolve_requiresCommitSignatures(parent, info):
        return parent.require_commit_signatures

    def resolve_requiresConversationResolution(parent, info):
        return parent.require_conversation_resolution

    def resolve_requiresCodeOwnerReviews(parent, info):
        return parent.require_codeowners_review

    def resolve_lockBranch(parent, info):
        return parent.lock_branch

    def resolve_matchingRefs(parent, info, first, after=None, query=None):
        if after is None:
            after = '0'
        after = int(after)
        values = parent.matching_refs
        if query:
            values = [v for v in values if v == query]
        return dict(
            length=len(values),
            nodes=values[after:after + first],
            first=first,
            after=after,
        )


class BranchProtectionRules(ObjectType):
    pageInfo = Field(PageInfo)
    nodes = List(BranchProtectionRule)

    def resolve_nodes(parent, info):
        return parent['nodes']

    def resolve_pageInfo(parent, info):
        return parent


class Actor(ObjectType):
    login = String()


class StatusContext(ObjectType):
    state = String()
    context = String()
    creator = Field(Actor)

    def resolve_state(parent, info):
        state = parent.state.upper()
        return state

    def resolve_context(parent, info):
        return parent.context

    def resolve_creator(parent, info):
        return parent.creator


class Status(ObjectType):
    contexts = List(StatusContext)

    def resolve_contexts(parent, info):
        return parent


class CheckRun(ObjectType):

    class Meta:
        interfaces = (Node, )

    id = ID(required=True)
    name = String()
    conclusion = String()

    def resolve_name(parent, info):
        return parent.name

    def resolve_conclusion(parent, info):
        if parent.conclusion:
            return parent.conclusion.upper()
        return None


class CheckRuns(ObjectType):
    nodes = List(CheckRun)
    pageInfo = Field(PageInfo)

    def resolve_nodes(parent, info):
        return parent['nodes']

    def resolve_pageInfo(parent, info):
        return parent


class App(ObjectType):
    slug = String()
    name = String()


class CheckSuite(ObjectType):

    class Meta:
        interfaces = (Node, )

    id = ID(required=True)
    app = Field(App)
    checkRuns = Field(CheckRuns, first=Int(), after=String())

    def resolve_app(parent, info):
        if not parent.runs:
            return None
        return parent.runs[0].app

    def resolve_checkRuns(parent, info, first, after=None):
        # Github will only return the single most recent check run for
        # a given name (this is true for REST or graphql), despite the
        # format of the result being a list.
        # Since the check runs are ordered from latest to oldest result we
        # need to traverse the list in reverse order.
        check_runs_by_name = {
            "{}:{}".format(cr.app.name, cr.name):
            cr for cr in reversed(parent.runs)
        }
        if after is None:
            after = '0'
        after = int(after)
        values = list(check_runs_by_name.values())
        return dict(
            length=len(values),
            nodes=values[after:after + first],
            first=first,
            after=after,
        )


class CheckSuites(ObjectType):

    nodes = List(CheckSuite)
    pageInfo = Field(PageInfo)

    def resolve_nodes(parent, info):
        return parent['nodes']

    def resolve_pageInfo(parent, info):
        return parent


class GitSignature(ObjectType):

    isValid = Boolean()

    def resolve_isValid(parent, info):
        # For testing we currently only check if the fake commit
        # has a signature and consider it valid.
        return bool(parent)


class Commit(ObjectType):

    class Meta:
        interfaces = (Node, )

    id = ID(required=True)
    status = Field(Status)
    checkSuites = Field(CheckSuites, first=Int(), after=String())
    signature = Field(GitSignature)

    def resolve_status(parent, info):
        seen = set()
        result = []
        for status in parent._statuses:
            if status.context not in seen:
                seen.add(status.context)
                result.append(status)
        # Github returns None if there are no results
        return result or None

    def resolve_checkSuites(parent, info, first, after=None):
        if after is None:
            after = '0'
        after = int(after)
        # Each value is a list of check runs for that suite
        values = list(parent._check_suites.values())
        return dict(
            length=len(values),
            nodes=values[after:after + first],
            first=first,
            after=after,
        )

    def resolve_signature(parent, info):
        return parent.signature


class PullRequestCommit(ObjectType):
    commit = Field(Commit)

    def resolve_commit(parent, info):
        return parent


class PullRequestCommits(ObjectType):
    nodes = List(PullRequestCommit)
    pageInfo = Field(PageInfo)

    def resolve_nodes(parent, info):
        return parent['nodes']

    def resolve_pageInfo(parent, info):
        return parent


class PullRequestReviewThread(ObjectType):
    isResolved = Boolean()

    def resolve_isResolved(parent, info):
        return parent.resolved


class PullRequestReviewThreads(ObjectType):
    nodes = List(PullRequestReviewThread)
    pageInfo = Field(PageInfo)

    def resolve_nodes(parent, info):
        return parent['nodes']

    def resolve_pageInfo(parent, info):
        return parent


class PullRequest(ObjectType):
    class Meta:
        interfaces = (Node, )

    id = ID(required=True)
    isDraft = Boolean()
    reviewDecision = String()
    mergeable = String()
    merged = Boolean()
    reviewThreads = Field(PullRequestReviewThreads,
                          first=Int(), after=String())
    commits = Field(PullRequestCommits,
                    first=Int(), after=String())

    def resolve_id(parent, info):
        return parent.id

    def resolve_isDraft(parent, info):
        return parent.draft

    def resolve_mergeable(parent, info):
        return "MERGEABLE" if parent.mergeable else "CONFLICTING"

    def resolve_merged(parent, info):
        return parent.is_merged

    def resolve_reviewThreads(parent, info, first, after=None):
        if after is None:
            after = '0'
        after = int(after)
        values = parent.review_threads
        return dict(
            length=len(values),
            nodes=values[after:after + first],
            first=first,
            after=after,
        )

    def resolve_commits(parent, info, first, after=None):
        if after is None:
            after = '0'
        after = int(after)
        values = parent.commits
        return dict(
            length=len(values),
            nodes=values[after:after + first],
            first=first,
            after=after,
        )

    def resolve_reviewDecision(parent, info):
        if hasattr(info.context, 'version') and info.context.version:
            if info.context.version < (2, 21, 0):
                raise Exception('Field unsupported')

        # Check branch protection rules if reviews are required
        org, project = parent.project.split('/')
        repo = info.context._data.repos[(org, project)]
        rule = repo._branch_protection_rules.get(parent.branch)
        if not rule or not rule.require_reviews:
            # Github returns None if there is no review required
            return None

        approvals = [r for r in parent.reviews
                     if r.data['state'] == 'APPROVED']
        if approvals:
            return 'APPROVED'

        return 'REVIEW_REQUIRED'


class Repository(ObjectType):
    name = String()
    branchProtectionRules = Field(BranchProtectionRules,
                                  first=Int(), after=String())
    pullRequest = Field(PullRequest, number=Int(required=True))
    object = Field(Commit, expression=String(required=True))

    def resolve_name(parent, info):
        org, name = parent.name.split('/')
        return name

    def resolve_branchProtectionRules(parent, info, first, after=None):
        if after is None:
            after = '0'
        after = int(after)
        values = list(parent._branch_protection_rules.values())
        return dict(
            length=len(values),
            nodes=values[after:after + first],
            first=first,
            after=after,
        )

    def resolve_pullRequest(parent, info, number):
        return parent.data.pull_requests.get(number)

    def resolve_object(parent, info, expression):
        return parent._commits.get(expression)


class FakeGithubQuery(ObjectType):
    repository = Field(Repository, owner=String(required=True),
                       name=String(required=True))
    node = Field(Node, id=ID(required=True))

    def resolve_repository(root, info, owner, name):
        return info.context._data.repos.get((owner, name))

    def resolve_node(root, info, id):
        for repo in info.context._data.repos.values():
            for rule in repo._branch_protection_rules.values():
                if rule.id == id:
                    return rule
            for commit in repo._commits.values():
                if commit.id == id:
                    return commit
                for suite in commit._check_suites.values():
                    if suite.id == id:
                        return suite
        for pr in info.context._data.pull_requests.values():
            if pr.id == id:
                return pr


def getGrapheneSchema():
    return Schema(query=FakeGithubQuery, types=[ID])
