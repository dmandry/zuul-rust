# Copyright 2020 BMW Group
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

import importlib.resources
import logging

from zuul.lib.logutil import get_annotated_logger


def nested_get(d, *keys, default=None):
    temp = d
    for key in keys[:-1]:
        temp = temp.get(key, {}) if temp is not None else None
    return temp.get(keys[-1], default) if temp is not None else default


class GraphQLClient:
    log = logging.getLogger('zuul.github.graphql')

    def __init__(self, url):
        self.url = url
        self.queries = {}
        self._load_queries()

    def _load_queries(self):
        self.log.debug('Loading prepared graphql queries')
        query_names = [
            'canmerge',
            'canmerge-page-rules',
            'canmerge-page-suites',
            'canmerge-page-runs',
            'canmerge-page-threads',
            'branch-protection',
            'branch-protection-inner',
            'merged',
        ]
        for query_name in query_names:
            f = importlib.resources.files('zuul').joinpath(
                'driver/github/graphql/%s.graphql' % query_name)
            self.queries[query_name] = f.read_bytes().decode()

    @staticmethod
    def _prepare_query(query, variables):
        data = {
            'query': query,
            'variables': variables,
        }
        return data

    def _run_query(self, log, github, query_name, **args):
        args['zuul_query'] = query_name  # used for logging
        query = self.queries[query_name]
        query = self._prepare_query(query, args)
        response = github.session.post(self.url, json=query)
        response = response.json()
        if 'data' not in response:
            log.error("Error running query %s: %s",
                      query_name, response)
        return response

    def fetch_canmerge(self, github, change, zuul_event_id=None):
        log = get_annotated_logger(self.log, zuul_event_id)
        owner, repo = change.project.name.split('/')
        result = {}
        cursor = None
        matching_rules = []

        while True:
            if cursor is None:
                # First page of branchProtectionRules
                data = self._run_query(log, github, 'canmerge',
                                       owner=owner,
                                       repo=repo,
                                       pull=change.number,
                                       head_sha=change.patchset,
                                       ref_name=change.branch,
                                       )

                repository = nested_get(data, 'data', 'repository')
            else:
                # Subsequent pages of branchProtectionRules
                data = self._run_query(log, github, 'canmerge-page-rules',
                                       owner=owner,
                                       repo=repo,
                                       cursor=cursor,
                                       ref_name=change.branch,
                                       )

            # Find corresponding rule to our branch
            rules = nested_get(
                data, 'data', 'repository', 'branchProtectionRules', 'nodes',
                default=[])

            # Filter branch protection rules for the one matching the change.
            # matchingRefs should either be empty or only contain our branch
            matching_rules.extend([
                rule for rule in rules
                for ref in nested_get(
                    rule, 'matchingRefs', 'nodes', default=[])
                if ref.get('name') == change.branch
            ])

            rules_pageinfo = nested_get(
                data, 'data', 'repository',
                'branchProtectionRules', 'pageInfo')
            if not rules_pageinfo['hasNextPage']:
                break
            cursor = rules_pageinfo['endCursor']

        if len(matching_rules) > 1:
            log.warn('More than one branch protection rule '
                     'matches change %s', change)
            return result
        elif len(matching_rules) == 1:
            matching_rule = matching_rules[0]
        else:
            matching_rule = None

        # If there is a matching rule, get required status checks
        if matching_rule:
            result['requiredStatusCheckContexts'] = matching_rule.get(
                'requiredStatusCheckContexts', [])
            result['requiresApprovingReviews'] = matching_rule.get(
                'requiresApprovingReviews')
            result['requiresCodeOwnerReviews'] = matching_rule.get(
                'requiresCodeOwnerReviews')
            result['requiresConversationResolution'] = matching_rule.get(
                'requiresConversationResolution')
            result['requiresCommitSignatures'] = matching_rule.get(
                'requiresCommitSignatures')
            result['protected'] = True
        else:
            result['requiredStatusCheckContexts'] = []
            result['protected'] = False

        # Check for draft
        pull_request = nested_get(repository, 'pullRequest')
        result['isDraft'] = nested_get(pull_request, 'isDraft', default=False)

        # Check if Github detected a merge conflict. Possible enum values
        # are CONFLICTING, MERGEABLE and UNKNOWN.
        result['mergeable'] = nested_get(pull_request, 'mergeable',
                                         default='MERGEABLE')

        # Get review decision. This is supported since GHE 2.21. Default to
        # None to signal if the field is not present.
        result['reviewDecision'] = nested_get(
            pull_request, 'reviewDecision', default=None)

        if result.get('requiresConversationResolution'):
            result['unresolvedConversations'] = False
            for thread in self._fetch_canmerge_threads(
                    github, log, pull_request):
                if not thread.get('isResolved'):
                    result['unresolvedConversations'] = True
                    break

        if result.get('requiresCommitSignatures'):
            result['unsignedCommits'] = False
            for pr_commit in self._fetch_canmerge_commits(
                    github, log, pull_request):
                signature = pr_commit['commit']['signature']
                if not (signature and signature.get('isValid')):
                    result['unsignedCommits'] = True
                    break

        # Add status checks
        result['status'] = {}
        commit = nested_get(repository, 'object')
        # Status can be explicit None so make sure we work with a dict
        # afterwards
        status = commit.get('status') or {}
        for context in status.get('contexts', []):
            result['status'][context['context']] = context

        # Add check runs
        result['checks'] = {}
        for suite in self._fetch_canmerge_suites(github, log, commit):
            for run in self._fetch_canmerge_runs(github, log, suite):
                result['checks'][run['name']] = {
                    **run,
                    "app": suite.get("app")
                }

        return result

    def _fetch_canmerge_suites(self, github, log, commit):
        commit_id = commit['id']
        while True:
            for suite in nested_get(
                    commit, 'checkSuites', 'nodes', default=[]):
                yield suite
            page_info = nested_get(commit, 'checkSuites', 'pageInfo')
            if not page_info['hasNextPage']:
                return
            data = self._run_query(
                log, github, 'canmerge-page-suites',
                commit_node_id=commit_id,
                cursor=page_info['endCursor'])
            commit = nested_get(data, 'data', 'node')

    def _fetch_canmerge_runs(self, github, log, suite):
        suite_id = suite['id']
        while True:
            for run in nested_get(suite, 'checkRuns', 'nodes', default=[]):
                yield run
            page_info = nested_get(suite, 'checkRuns', 'pageInfo')
            if not page_info['hasNextPage']:
                return
            data = self._run_query(
                log, github, 'canmerge-page-runs',
                suite_node_id=suite_id,
                cursor=page_info['endCursor'])
            suite = nested_get(data, 'data', 'node')

    def _fetch_canmerge_threads(self, github, log, pull_request):
        pull_id = pull_request['id']
        while True:
            for thread in nested_get(
                    pull_request, 'reviewThreads', 'nodes', default=[]):
                yield thread
            page_info = nested_get(pull_request, 'reviewThreads', 'pageInfo')
            if not page_info['hasNextPage']:
                return
            data = self._run_query(
                log, github, 'canmerge-page-threads',
                pull_node_id=pull_id,
                cursor=page_info['endCursor'])
            pull_request = nested_get(data, 'data', 'node')

    def _fetch_canmerge_commits(self, github, log, pull_request):
        pull_id = pull_request['id']
        while True:
            for pr_commit in nested_get(
                    pull_request, 'commits', 'nodes', default=[]):
                yield pr_commit
            page_info = nested_get(pull_request, 'commits', 'pageInfo')
            if not page_info['hasNextPage']:
                return
            data = self._run_query(
                log, github, 'canmerge-page-commits',
                pull_node_id=pull_id,
                cursor=page_info['endCursor'])
            pull_request = nested_get(data, 'data', 'node')

    def _fetch_branch_protection(self, log, github, project,
                                 zuul_event_id=None):
        owner, repo = project.name.split('/')
        branches = {}
        branch_subqueries = []

        cursor = None
        while True:
            data = self._run_query(
                log, github, 'branch-protection',
                owner=owner,
                repo=repo,
                cursor=cursor)['data']

            for rule in data['repository']['branchProtectionRules']['nodes']:
                for branch in rule['matchingRefs']['nodes']:
                    branches[branch['name']] = rule['lockBranch']
                refs_pageinfo = rule['matchingRefs']['pageInfo']
                if refs_pageinfo['hasNextPage']:
                    branch_subqueries.append(dict(
                        rule_node_id=rule['id'],
                        cursor=refs_pageinfo['endCursor']))

            rules_pageinfo = data['repository']['branchProtectionRules'
                                                ]['pageInfo']
            if not rules_pageinfo['hasNextPage']:
                break
            cursor = rules_pageinfo['endCursor']

        for subquery in branch_subqueries:
            cursor = subquery['cursor']
            while True:
                data = self._run_query(
                    log, github, 'branch-protection-inner',
                    rule_node_id=subquery['rule_node_id'],
                    cursor=cursor)['data']
                for branch in data['node']['matchingRefs']['nodes']:
                    branches[branch['name']] = rule['lockBranch']
                refs_pageinfo = data['node']['matchingRefs']['pageInfo']
                if not refs_pageinfo['hasNextPage']:
                    break
                cursor = refs_pageinfo['endCursor']
        return branches

    def fetch_branch_protection(self, github, project, zuul_event_id=None):
        """Return a dictionary of branches and whether they are locked"""
        log = get_annotated_logger(self.log, zuul_event_id)
        return self._fetch_branch_protection(log, github, project,
                                             zuul_event_id)

    def merged(self, github, change):
        owner, repo = change.project.name.split('/')

        variables = {
            'zuul_query': 'merged',  # used for logging
            'owner': owner,
            'repo': repo,
            'pull': change.number,
        }
        query = self.queries['merged']
        query = self._prepare_query(query, variables)
        response = github.session.post(self.url, json=query)
        response.raise_for_status()
        return nested_get(response.json(), 'data', 'repository', 'pullRequest',
                          'merged')
