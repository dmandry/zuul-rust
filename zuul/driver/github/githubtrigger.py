# Copyright 2015 Hewlett-Packard Development Company, L.P.
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

import logging
import voluptuous as vs
from zuul.trigger import BaseTrigger
from zuul.driver.github.githubmodel import GithubEventFilter
from zuul.driver.github import githubsource
from zuul.driver.util import scalar_or_list, to_list, make_regex, ZUUL_REGEX
from zuul.exceptions import DeprecationWarning


class GithubUnlabelDeprecation(DeprecationWarning):
    zuul_error_name = 'Github unlabel Deprecation'
    zuul_error_message = """The 'unlabel' trigger attribute
is deprecated.  Use 'label' instead."""


class GithubEventListDeprecation(DeprecationWarning):
    zuul_error_name = 'Github event list Deprecation'
    zuul_error_message = """Specifying the 'event' trigger attribute
as a list is deprecated.  Use a single item instead."""


class GithubRequireStatusDeprecation(DeprecationWarning):
    zuul_error_name = 'Github require-status Deprecation'
    zuul_error_message = """The 'require-status' trigger attribute
is deprecated.  Use 'require' instead."""


class GithubRequestedActionDeprecation(DeprecationWarning):
    zuul_error_name = 'Github requested action Deprecation'
    zuul_error_message = """The 'requested' value for the 'action'
trigger attribute is deprecated.  Use 'rerequested' instead."""


class GithubTriggerConfigurationWarning(DeprecationWarning):
    zuul_error_name = 'Github Trigger Configuration Warning'


class GithubTrigger(BaseTrigger):
    name = 'github'
    log = logging.getLogger("zuul.trigger.GithubTrigger")

    def getEventFilters(self, connection_name, trigger_config,
                        parse_context):
        efilters = []
        pcontext = parse_context
        new_schema = getNewSchema()

        for trigger in to_list(trigger_config):

            # Deprecated in 8.3.0 but warning added in 10.0
            if 'require-status' in trigger:
                with pcontext.confAttr(trigger, 'require-status'):
                    pcontext.accumulator.addError(
                        GithubRequireStatusDeprecation())
            # Deprecated with warning in 10.0
            if 'unlabel' in trigger:
                with pcontext.confAttr(trigger, 'unlabel'):
                    pcontext.accumulator.addError(
                        GithubUnlabelDeprecation())
            # Deprecated with warning in 10.0
            if isinstance(trigger.get('event', None), list):
                with pcontext.confAttr(trigger, 'event'):
                    pcontext.accumulator.addError(
                        GithubEventListDeprecation())

            try:
                new_schema(trigger)
            except vs.Invalid as e:
                pcontext.accumulator.addError(
                    GithubTriggerConfigurationWarning(
                        "The trigger configuration as supplied "
                        f"has an error: {e}"))

            with pcontext.confAttr(trigger, 'event') as attr:
                types = [make_regex(x, pcontext)
                         for x in to_list(attr)]
            with pcontext.confAttr(trigger, 'branch') as attr:
                branches = [make_regex(x, pcontext)
                            for x in to_list(attr)]
            with pcontext.confAttr(trigger, 'ref') as attr:
                refs = [make_regex(x, pcontext)
                        for x in to_list(attr)]
            with pcontext.confAttr(trigger, 'comment') as attr:
                comments = [make_regex(x, pcontext)
                            for x in to_list(attr)]

            # This is a compatibility layer to map the action 'requested' back
            # to the original action 'rerequested'.
            # TODO: Remove after zuul 5.0
            # Note the original backwards compat handling for this did
            # not allow 'requested' as a list, so we don't do that
            # here either.
            if trigger.get('action') == 'requested':
                trigger['action'] = 'rerequested'
                with pcontext.confAttr(trigger, 'action'):
                    pcontext.accumulator.addError(
                        GithubRequestedActionDeprecation())

            f = GithubEventFilter(
                connection_name=connection_name,
                trigger=self,
                types=types,
                actions=to_list(trigger.get('action')),
                branches=branches,
                refs=refs,
                comments=comments,
                check_runs=to_list(trigger.get('check')),
                permission=trigger.get('permission'),
                labels=to_list(trigger.get('label')),
                unlabels=to_list(trigger.get('unlabel')),
                states=to_list(trigger.get('state')),
                statuses=to_list(trigger.get('status')),
                required_statuses=to_list(trigger.get('require-status')),
                require=trigger.get('require'),
                reject=trigger.get('reject'),
                debug=trigger.get('debug'),
            )
            efilters.append(f)

        return efilters

    def onPullRequest(self, payload):
        pass


def getNewSchema():
    # For now, this is only used to raise deprecation errors if the
    # syntax does not match.  This was added in 10.0.  When we're
    # ready to enforce this, rename this method getSchema.
    base_schema = vs.Schema({
        vs.Required('event'): vs.Any('pull_request',
                                     'pull_request_review',
                                     'push',
                                     'check_run'),
        'require': githubsource.getRequireSchema(),
        'reject': githubsource.getRejectSchema(),
        'debug': bool,
    })

    # Pull request
    pull_request_base_schema = base_schema.extend({
        vs.Required('event'): 'pull_request',
        'branch': scalar_or_list(vs.Any(ZUUL_REGEX, str)),
        'permission': vs.Any('read', 'write', 'admin'),
    })

    pull_request_default_schema = pull_request_base_schema.extend({
        'action': scalar_or_list(vs.Any(
            'opened', 'changed', 'closed', 'reopened',
            'converted_to_draft', 'ready_for_review')),
    })

    pull_request_comment_schema = pull_request_base_schema.extend({
        vs.Required('action'): 'comment',
        'comment': scalar_or_list(vs.Any(ZUUL_REGEX, str)),
    })

    pull_request_labeled_schema = pull_request_base_schema.extend({
        vs.Required('action'): scalar_or_list(vs.Any('labeled', 'unlabeled')),
        'label': scalar_or_list(str),
    })

    pull_request_status_schema = pull_request_base_schema.extend({
        vs.Required('action'): 'status',
        'status': scalar_or_list(str),
    })

    # Pull request review
    pull_request_review_base_schema = base_schema.extend({
        vs.Required('event'): 'pull_request_review',
        'branch': scalar_or_list(vs.Any(ZUUL_REGEX, str)),
    })

    # The docs for this are at:
    # https://docs.github.com/en/webhooks/webhook-events-and-payloads?actionType=dismissed#pull_request_review
    # They specify "dismissed, approved, changes_requested"
    # But we have also experimentally seen "commented"
    # The graphql docs are at:
    # https://docs.github.com/en/enterprise-server@3.12/graphql/reference/enums#pullrequestreviewstate
    # And they additionally specify "commented" and "pending".
    # It's unclear whether "pending" can show up in the webhook (it
    # may only appear in a graphql query of a user's own unsaved
    # reviews) but since we've seen the extra "commented" value, let's
    # be generous and accept "pending" as well.
    pull_request_review_schema = pull_request_review_base_schema.extend({
        'action': scalar_or_list(vs.Any('submitted', 'dismissed')),
        'state': scalar_or_list(vs.Any(
            'approved', 'commented', 'changes_requested',
            'dismissed', 'pending')),
    })

    # Push
    push_schema = base_schema.extend({
        vs.Required('event'): 'push',
        'ref': scalar_or_list(vs.Any(ZUUL_REGEX, str)),
    })

    # Check run
    check_run_schema = base_schema.extend({
        vs.Required('event'): 'check_run',
        # The 'requested' action is deprecated, but we have a separate,
        # more specific deprecation for this.
        'action': scalar_or_list(vs.Any('rerequested', 'completed')),
        'check': scalar_or_list(str),
    })

    init_schema = base_schema.extend({}, extra=vs.ALLOW_EXTRA)

    def validate(data):
        # This method could be a simple vs.Any() across all the
        # possibilities, but sometimes the error messages are less
        # intuitive (e.g., indicating that the action is wrong rather
        # than that a user has added an attribute that is incompatible
        # with the action).  Instead, we assume the user got the event
        # and/or action right first, then apply the appropriate schema
        # for what they selected.

        event = data.get('event')
        # TODO: run this unconditionally after
        # GithubEventListDeprecation is complete
        if isinstance(event, str):
            # First, make sure the common items are correct.
            init_schema(data)
        action = data.get('action')
        if event == 'pull_request':
            if action == 'comment':
                pull_request_comment_schema(data)
            elif action in ('labeled', 'unlabeled',):
                pull_request_labeled_schema(data)
            elif action == 'status':
                pull_request_status_schema(data)
            else:
                pull_request_default_schema(data)
        elif event == 'pull_request_review':
            pull_request_review_schema(data)
        elif event == 'push':
            push_schema(data)
        elif event == 'check_run':
            check_run_schema(data)

    return validate


def getSchema():
    old_schema = vs.Schema({
        vs.Required('event'):
            scalar_or_list(vs.Any('pull_request',
                                  'pull_request_review',
                                  'push',
                                  'check_run')),
        'action': scalar_or_list(str),
        'branch': scalar_or_list(vs.Any(ZUUL_REGEX, str)),
        'ref': scalar_or_list(vs.Any(ZUUL_REGEX, str)),
        'comment': scalar_or_list(vs.Any(ZUUL_REGEX, str)),
        'label': scalar_or_list(str),
        'unlabel': scalar_or_list(str),
        'permission': vs.Any('read', 'write', 'admin'),
        'state': scalar_or_list(str),
        'require-status': scalar_or_list(str),
        'require': githubsource.getRequireSchema(),
        'reject': githubsource.getRejectSchema(),
        'status': scalar_or_list(str),
        'check': scalar_or_list(str),
        'debug': bool,
    })

    return old_schema
