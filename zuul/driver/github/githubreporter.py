# Copyright 2015 Puppet Labs
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

import json
import logging
import voluptuous as v
import time

from zuul import model
from zuul.lib.logutil import get_annotated_logger
from zuul.reporter import BaseReporter
from zuul.exceptions import MergeFailure
from zuul.driver.util import scalar_or_list
from zuul.driver.github.githubsource import GithubSource


class GithubReporter(BaseReporter):
    """Sends off reports to Github."""

    name = 'github'
    log = logging.getLogger("zuul.GithubReporter")

    # Merge modes supported by github
    merge_modes = {
        model.MERGER_MERGE: 'merge',
        model.MERGER_MERGE_RESOLVE: 'merge',
        model.MERGER_MERGE_RECURSIVE: 'merge',
        model.MERGER_MERGE_ORT: 'merge',
        model.MERGER_SQUASH_MERGE: 'squash',
        model.MERGER_REBASE: 'rebase',
    }

    def __init__(self, driver, connection, pipeline, config=None,
                 parse_context=None):
        super(GithubReporter, self).__init__(
            driver, connection, config, parse_context)
        self._commit_status = self.config.get('status', None)
        self._create_comment = self.config.get('comment', True)
        self._check = self.config.get('check', False)
        self._merge = self.config.get('merge', False)
        self._labels = self.config.get('label', [])
        if not isinstance(self._labels, list):
            self._labels = [self._labels]
        self._unlabels = self.config.get('unlabel', [])
        self._review = self.config.get('review')
        self._review_body = self.config.get('review-body')
        if not isinstance(self._unlabels, list):
            self._unlabels = [self._unlabels]

    def getContext(self, manager):
        return "{}/{}".format(manager.tenant.name,
                              manager.pipeline.name)

    def report(self, item, phase1=True, phase2=True):
        """Report on an event."""
        log = get_annotated_logger(self.log, item.event)

        ret = []
        for change in item.changes:
            err = self._reportChange(item, change, log, phase1, phase2)
            if err:
                ret.append(err)
        return ret

    def _reportChange(self, item, change, log, phase1=True, phase2=True):
        """Report on an event."""
        # If the source is not GithubSource we cannot report anything here.
        if not isinstance(change.project.source, GithubSource):
            return

        # For supporting several Github connections we also must filter by
        # the canonical hostname.
        if change.project.source.connection.canonical_hostname != \
                self.connection.canonical_hostname:
            return

        # order is important for github branch protection.
        # A status should be set before a merge attempt
        if phase1 and self._commit_status is not None:
            if (hasattr(change, 'patchset') and
                    change.patchset is not None):
                self.setCommitStatus(item, change)
            elif (hasattr(change, 'newrev') and
                    change.newrev is not None):
                self.setCommitStatus(item, change)
        # Comments, labels, and merges can only be performed on pull requests.
        # If the change is not a pull request (e.g. a push) skip them.
        if hasattr(change, 'number'):
            errors_received = False
            if phase1:
                if self._labels or self._unlabels:
                    self.setLabels(item, change)
                if self._review:
                    self.addReview(item, change)
                if self._check:
                    check_errors = self.updateCheck(item, change)
                    # TODO (felix): We could use this mechanism to
                    # also report back errors from label and review
                    # actions
                    if check_errors:
                        item.current_build_set.warning_messages.extend(
                            check_errors
                        )
                        errors_received = True
                if self._create_comment or errors_received:
                    self.addPullComment(item, change)
            if phase2 and self._merge:
                try:
                    self.mergePull(item, change)
                except Exception as e:
                    self.addPullComment(item, change, str(e))

    def _formatJobResult(self, job_fields):
        # We select different emojis to represents build results:
        # heavy_check_mark: SUCCESS
        # warning: SKIPPED/ABORTED
        # x: all types of FAILUREs
        # In addition, failure results are in bold text

        job_result = job_fields[2]
        # Also need to handle user defined success_message.
        # The job_fields[6]: the user defined seccess_message (if available)
        success_message = job_fields[6]

        emoji = 'x'
        bold_result = True

        if job_result in ('SUCCESS', success_message):
            emoji = 'heavy_check_mark'
            bold_result = False
        elif job_result in ('SKIPPED', 'ABORTED', 'CANCELED'):
            emoji = 'warning'
            bold_result = False

        if bold_result:
            return ':%s: [%s](%s) **%s**%s%s%s\n' % (
                (emoji,) + job_fields[:6])
        else:
            return ':%s: [%s](%s) %s%s%s%s\n' % (
                (emoji,) + job_fields[:6])

    def _formatItemReportJobs(self, item, short):
        # Return the list of jobs portion of the report
        ret = ''
        jobs_fields, skipped = self._getItemReportJobsFields(item)
        for job_fields in jobs_fields:
            ret += self._formatJobResult(job_fields)
        if skipped:
            jobtext = 'job' if skipped == 1 else 'jobs'
            ret += 'Skipped %i %s\n' % (skipped, jobtext)
        return ret

    def addPullComment(self, item, change, comment=None):
        log = get_annotated_logger(self.log, item.event)
        message = comment or self._formatItemReport(item)
        project = change.project.name
        pr_number = change.number
        log.debug('Reporting change %s, params %s, message: %s',
                  change, self.config, message)
        self.connection.commentPull(project, pr_number, message,
                                    zuul_event_id=item.event)

    def setCommitStatus(self, item, change):
        log = get_annotated_logger(self.log, item.event)

        project = change.project.name
        if hasattr(change, 'patchset'):
            sha = change.patchset
        elif hasattr(change, 'newrev'):
            sha = change.newrev
        state = self._commit_status

        url = item.formatItemUrl()

        description = '%s status: %s' % (item.manager.pipeline.name,
                                         self._commit_status)

        if len(description) >= 140:
            # This pipeline is named with a long name and thus this
            # desciption would overflow the GitHub limit of 1024 bytes.
            # Truncate the description. In practice, anything over 140
            # characters seems to trip the limit.
            description = 'status: %s' % self._commit_status

        log.debug(
            'Reporting change %s, params %s, '
            'context: %s, state: %s, description: %s, url: %s',
            change, self.config, self.getContext(item.manager),
            state, description, url)

        self.connection.setCommitStatus(
            project, sha, state, url, description,
            self.getContext(item.manager),
            zuul_event_id=item.event)

    def mergePull(self, item, change):
        log = get_annotated_logger(self.log, item.event)
        merge_mode = item.current_build_set.getMergeMode(change)

        if merge_mode not in self.merge_modes:
            mode = model.get_merge_mode_name(merge_mode)
            self.log.warning('Merge mode %s not supported by Github', mode)
            raise MergeFailure('Merge mode %s not supported by Github' % mode)

        project = change.project.name
        pr_number = change.number
        sha = change.patchset
        log.debug('Reporting change %s, params %s, merging via API',
                  change, self.config)
        message = self._formatMergeMessage(change, merge_mode)
        merge_mode = self.merge_modes[merge_mode]

        max_retries = 5
        for i in range(1, max_retries + 1):
            try:
                if change.stack_number is None:
                    merge_sha = self.connection.mergePullSync(
                        project, pr_number, message, sha=sha,
                        method=merge_mode, zuul_event_id=item.event)
                else:
                    merge_sha = self.connection.mergePullAsync(
                        project, pr_number, message, sha=sha,
                        method=merge_mode, zuul_event_id=item.event)
                self.connection.updateChangeAttributes(
                    change,
                    merge_commit_sha=merge_sha,
                    is_merged=True)
                return
            except MergeFailure as e:
                log.exception('Merge attempt of change %s  %s/%s failed.',
                              change, i, max_retries, exc_info=True)
                error_message = str(e)
                if i < max_retries:
                    time.sleep(5)
                    # Sometimes GitHub lies about the merge status and reports
                    # merge failed but in fact did the merge successfully. To
                    # account for that re-fetch the PR and check if it's still
                    # unmerged.
                    try:
                        if self.connection.isMerged(change, item.event):
                            # The change has been merged despite we got an
                            # error back from GitHub
                            log.warning(
                                'Merge attempt of change %s failed but change '
                                'is merged. Treating as success.', change)
                            self.connection.updateChangeAttributes(
                                change, is_merged=True)
                            return
                    except Exception:
                        log.exception('Error while checking for merged '
                                      'change %s', change)
        log.warning('Merge of change %s failed after %s attempts, giving up',
                    change, max_retries)
        raise MergeFailure(error_message)

    def addReview(self, item, change):
        log = get_annotated_logger(self.log, item.event)
        project = change.project.name
        pr_number = change.number
        sha = change.patchset
        log.debug('Reporting change %s, params %s, review:\n%s',
                  change, self.config, self._review)
        self.connection.reviewPull(
            project,
            pr_number,
            sha,
            self._review,
            self._review_body,
            zuul_event_id=item.event)
        for label in self._unlabels:
            self.connection.unlabelPull(project, pr_number, label,
                                        zuul_event_id=item.event)

    def updateCheck(self, item, change):
        log = get_annotated_logger(self.log, item.event)
        message = self._formatItemReport(item)
        project = change.project.name
        pr_number = change.number
        sha = change.patchset

        status = self._check
        # We declare a item as completed if it either has a result
        # (success|failure) or a dequeue reporter is called (cancelled in case
        # of Github checks API). For the latter one, the item might or might
        # not have a result, but we still must set a conclusion on the check
        # run. Thus, we cannot rely on the buildset's result only, but also
        # check the state the reporter is going to report.
        completed = (
            item.current_build_set.result is not None or status == "cancelled"
            or status == "skipped" or status == "neutral"
        )

        log.debug(
            "Updating check for change %s, params %s, context %s, message: %s",
            change, self.config, self.getContext(item.manager), message
        )

        details_url = item.formatItemUrl()

        # Check for inline comments that can be reported via checks API
        file_comments = self.getFileComments(item, change)

        # Github allows an external id to be added to a check run. We can use
        # this to identify the check run in any custom actions we define.
        # To uniquely identify the corresponding buildset in zuul, we need
        # tenant, pipeline and change. The buildset's uuid cannot be used
        # safely, as it might change e.g. during a gate reset. Fore more
        # information, please see Jim's comment on
        # https://review.opendev.org/#/c/666258/7
        external_id = json.dumps(
            {
                "tenant": item.manager.tenant.name,
                "pipeline": item.manager.pipeline.name,
                "change": change.number,
            }
        )

        state = item.dynamic_state[self.connection.connection_name]
        check_run_ids = state.setdefault('check_run_ids', {})
        check_run_id = check_run_ids.get(change.cache_key)
        check_run_id, errors = self.connection.updateCheck(
            project,
            pr_number,
            sha,
            status,
            completed,
            self.getContext(item.manager),
            details_url,
            message,
            file_comments,
            external_id,
            zuul_event_id=item.event,
            check_run_id=check_run_id,
        )

        if check_run_id:
            check_run_ids[change.cache_key] = check_run_id

        return errors

    def setLabels(self, item, change):
        log = get_annotated_logger(self.log, item.event)
        project = change.project.name
        pr_number = change.number
        if self._labels:
            log.debug('Reporting change %s, params %s, labels:\n%s',
                      change, self.config, self._labels)
        for label in self._labels:
            self.connection.labelPull(project, pr_number, label,
                                      zuul_event_id=item.event)
        if self._unlabels:
            log.debug('Reporting change %s, params %s, unlabels:\n%s',
                      change, self.config, self._unlabels)
        for label in self._unlabels:
            self.connection.unlabelPull(project, pr_number, label,
                                        zuul_event_id=item.event)

    def _formatMergeMessage(self, change, merge_mode):
        message = []
        # For squash merges we don't need to add the title to the body
        # as it will already be set as the commit subject.
        if merge_mode != model.MERGER_SQUASH_MERGE:
            if change.title:
                message.append(change.title)
        if change.body_text:
            message.append(change.body_text)
        merge_message = "\n\n".join(message)

        if change.reviews:
            review_users = []
            for r in change.reviews:
                name = r['by']['name']
                email = r['by']['email']
                username = r['by']['username']
                review_message = 'Reviewed-by:'
                if name:
                    review_message += ' {}'.format(name)
                elif username:
                    review_message += ' {}'.format(username)
                else:
                    review_message += ' Anonymous'
                if email:
                    review_message += ' <{}>'.format(email)
                review_users.append(review_message)
            merge_message += '\n\n'
            merge_message += '\n'.join(review_users)

        return merge_message

    def getSubmitAllowNeeds(self, manager):
        """Get a list of code review labels that are allowed to be
        "needed" in the submit records for a change, with respect
        to this queue.  In other words, the list of review labels
        this reporter itself is likely to set before submitting.
        """

        # check if we report a status or a check, if not we can return an
        # empty list
        status = self.config.get('status')
        check = self.config.get("check")
        if not any([status, check]):
            return []

        # we return a status so return the status we report to github
        return [self.getContext(manager)]


def getSchema():
    github_reporter = v.Schema({
        'status': v.Any('pending', 'success', 'failure'),
        'comment': bool,
        'merge': bool,
        'label': scalar_or_list(str),
        'unlabel': scalar_or_list(str),
        'review': v.Any('approve', 'request-changes', 'comment'),
        'review-body': str,
        'check': v.Any(
            "queued", "in_progress", "success", "failure", "cancelled",
            "skipped", "neutral"),
    })
    return github_reporter
