# Copyright 2014 Rackspace Australia
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

import abc
import logging


class BaseReporter(object, metaclass=abc.ABCMeta):
    """Base class for reporters.

    Defines the exact public methods that must be supplied.
    """

    log = logging.getLogger("zuul.reporter.BaseReporter")

    def __init__(self, driver, connection, config=None, parse_context=None):
        self.driver = driver
        self.connection = connection
        self.config = config or {}
        self._action = None

    def setAction(self, action):
        self._action = action

    @abc.abstractmethod
    def report(self, item, phase1=True, phase2=True):
        """Send the compiled report message

        Two-phase reporting may be enabled if one or the other of the
        `phase1` or `phase2` arguments is False.

        Phase1 should report everything except the actual merge action.
        Phase2 should report only the merge action.

        :arg phase1 bool: Whether to enable phase1 reporting
        :arg phase2 bool: Whether to enable phase2 reporting

        """

    def getSubmitAllowNeeds(self, manager):
        """Get a list of code review labels that are allowed to be
        "needed" in the submit records for a change, with respect
        to this queue.  In other words, the list of review labels
        this reporter itself is likely to set before submitting.

        :arg PipelineManager manager: The pipeline manager for the queue.
        """
        return []

    def postConfig(self):
        """Run tasks after configuration is reloaded"""

    def _configErrorMatchesChange(self, err, change):
        context = err.key.context
        if not context:
            return False
        if context.project_canonical_name != \
                change.project.canonical_name:
            return False
        if not hasattr(change, 'branch'):
            return False
        if context.branch != change.branch:
            return False
        return True

    def addConfigurationErrorComments(self, item, change, comments):
        """Add file comments for configuration errors.

        Updates the comments dictionary with additional file comments
        for any relevant configuration errors for the specified change.

        :arg QueueItem item: The queue item
        :arg Ref change: One of the item's changes to check
        :arg dict comments: a file comments dictionary

        """

        for err in item.getConfigErrors():
            context = err.key.context
            mark = err.key.mark
            if not self._configErrorMatchesChange(err, change):
                continue
            if not (mark and err.short_error):
                return False
            if context.path not in change.files:
                return False
            existing_comments = comments.setdefault(context.path, [])
            existing_comments.append(dict(line=mark.end_line,
                                          message=err.short_error,
                                          range=dict(
                                              start_line=mark.line + 1,
                                              start_character=mark.column,
                                              end_line=mark.end_line,
                                              end_character=mark.end_column)))

    def _getFileComments(self, item, change):
        """Get the file comments from the zuul_return value"""
        ret = {}
        for build in item.current_build_set.getBuilds():
            fc = build.result_data.get("zuul", {}).get("file_comments")
            if not fc:
                continue
            # Only consider comments for this change
            if change.cache_key not in build.job.all_refs:
                continue
            for fn, comments in fc.items():
                existing_comments = ret.setdefault(fn, [])
                existing_comments.extend(comments)
        self.addConfigurationErrorComments(item, change, ret)
        return ret

    def getFileComments(self, item, change):
        comments = self._getFileComments(item, change)
        self.filterComments(item, change, comments)
        return comments

    def filterComments(self, item, change, comments):
        """Filter comments for files in change

        Remove any comments for files which do not appear in the
        specified change.  Leave warning messages if this happens.

        :arg QueueItem item: The queue item
        :arg Change change: The change
        :arg dict comments: a file comments dictionary (modified in place)
        """

        for fn in list(comments.keys()):
            if change.commentable_files is not None:
                if fn not in change.commentable_files:
                    del comments[fn]
                    item.warning("Comments left for invalid file %s" % (fn,))
            elif fn not in change.files:
                del comments[fn]
                item.warning("Comments left for invalid file %s" % (fn,))

    def _getFormatter(self, action):
        format_methods = {
            'enqueue': self._formatItemReportEnqueue,
            'start': self._formatItemReportStart,
            'success': self._formatItemReportSuccess,
            'failure': self._formatItemReportFailure,
            'merge-conflict': self._formatItemReportMergeConflict,
            'merge-failure': self._formatItemReportMergeFailure,
            'config-error': self._formatItemReportConfigError,
            'no-jobs': self._formatItemReportNoJobs,
            'disabled': self._formatItemReportDisabled,
            'dequeue': self._formatItemReportDequeue,
        }
        return format_methods[action]

    def _formatItemReport(self, item, change=None,
                          with_jobs=True, action=None, short=False):
        """Format a report from the given items. Usually to provide results to
        a reporter taking free-form text."""
        action = action or self._action
        with_jobs = with_jobs and not item.didMergerFail()
        ret = self._getFormatter(action)(item, change, with_jobs, short)

        config_warnings = item.getConfigErrors(errors=False, warnings=True)
        if config_warnings:
            ret += '\nWarning:\n  ' + config_warnings[0].error + '\n'

        if item.current_build_set.warning_messages:
            warning = '\n  '.join(item.current_build_set.warning_messages)
            ret += '\nWarning:\n  ' + warning + '\n'

        if item.current_build_set.debug_messages:
            debug = '\n  '.join(item.current_build_set.debug_messages)
            ret += '\nDebug information:\n  ' + debug + '\n'

        if item.manager.pipeline.footer_message:
            ret += '\n' + item.manager.pipeline.footer_message

        return ret

    def _formatItemReportEnqueue(self, item, change, with_jobs, short):
        return item.manager.pipeline.enqueue_message.format(
            pipeline=item.manager.pipeline.getSafeAttributes(),
            item_url=item.formatItemUrl())

    def _formatItemReportStart(self, item, change, with_jobs, short):
        return item.manager.pipeline.start_message.format(
            pipeline=item.manager.pipeline.getSafeAttributes(),
            item_url=item.formatItemUrl())

    def _formatItemReportSuccess(self, item, change, with_jobs, short):
        msg = item.manager.pipeline.success_message
        if with_jobs:
            item_url = item.formatItemUrl()
            if item_url is not None:
                msg += '\n' + item_url
            msg += '\n\n' + self._formatItemReportJobs(item, short)
        return msg

    def _formatItemReportFailure(self, item, change, with_jobs, short):
        if len(item.changes) > 1:
            _this_change = 'These changes'
            _depends = 'depend'
            _is = 'are'
        else:
            _this_change = 'This change'
            _depends = 'depends'
            _is = 'is'

        if item.dequeued_needing_change:
            msg = (f'{_this_change} {_depends} on a change '
                   'that failed to merge.\n')
            if isinstance(item.dequeued_needing_change, str):
                msg += '\n' + item.dequeued_needing_change + '\n'
        elif item.dequeued_missing_requirements:
            msg = (f'{_this_change} {_is} unable to merge '
                   'due to a missing merge requirement.\n')
        elif len(item.changes) > 1:
            # No plural phrasing here; we are specifically talking
            # about this change as part of a cycle.
            msg = ('This change is part of a dependency cycle '
                   'that failed.\n\n')

            # Attempt to provide relevant config error information
            # since we will not add it below.
            changes_with_errors = self._getChangesWithErrors(item)
            if change in changes_with_errors:
                msg += str(item.getConfigErrors(
                    errors=True, warnings=False)[0].error)
            change_annotations = {c: ' (config error)'
                                  for c in changes_with_errors}
            if with_jobs:
                msg = '{}\n{}'.format(msg, self._formatItemReportJobs(
                    item, short))
            msg = "{}\n{}".format(
                msg, self._formatItemReportOtherChanges(item,
                                                        change_annotations))
        elif item.didMergerFail():
            msg = item.manager.pipeline.merge_conflict_message
        elif item.current_build_set.has_blocking_errors:
            msg = str(item.getConfigErrors(
                errors=True, warnings=False)[0].error)
        else:
            msg = item.manager.pipeline.failure_message
            if with_jobs:
                item_url = item.formatItemUrl()
                if item_url is not None:
                    msg += '\n' + item_url
                msg += '\n\n' + self._formatItemReportJobs(item, short)
        return msg

    def _getChangesWithErrors(self, item):
        # Attempt to determine whether this change is the source of a
        # configuration error.  This is best effort.
        if not item.current_build_set.has_blocking_errors:
            return []

        ret = []
        for change in item.changes:
            for err in item.getConfigErrors():
                if self._configErrorMatchesChange(err, change):
                    ret.append(change)
        return ret

    def _formatItemReportMergeConflict(self, item, change, with_jobs, short):
        return item.manager.pipeline.merge_conflict_message

    def _formatItemReportMergeFailure(self, item, change, with_jobs, short):
        return 'This change was not merged by the code review system.\n'

    def _formatItemReportConfigError(self, item, change, with_jobs, short):
        if item.getConfigErrors():
            msg = str(item.getConfigErrors()[0].error)
        else:
            msg = "Unknown configuration error"
        return msg

    def _formatItemReportNoJobs(self, item, change, with_jobs, short):
        return item.manager.pipeline.no_jobs_message.format(
            pipeline=item.manager.pipeline.getSafeAttributes(),
            item_url=item.formatItemUrl())

    def _formatItemReportDisabled(self, item, change, with_jobs=True,
                                  short=False):
        if item.current_build_set.result == 'SUCCESS':
            return self._formatItemReportSuccess(item, change,
                                                 with_jobs, short)
        elif item.current_build_set.result == 'FAILURE':
            return self._formatItemReportFailure(item, change,
                                                 with_jobs, short)
        else:
            return self._formatItemReport(item, change, with_jobs, short)

    def _formatItemReportDequeue(self, item, change, with_jobs, short):
        msg = item.manager.pipeline.dequeue_message
        if with_jobs:
            msg += '\n\n' + self._formatItemReportJobs(item, short)
        return msg

    def _formatItemReportOtherChanges(self, item, change_annotations):
        change_lines = []
        for change in item.changes:
            annotation = change_annotations.get(change, '')
            change_lines.append(f'  - {change.url}{annotation}')
        return "Related changes:\n{}\n".format("\n".join(change_lines))

    def _getItemReportJobsFields(self, item):
        # Extract the report elements from an item
        config = self.connection.sched.config
        jobs_fields = []
        skipped = 0
        for job in item.getJobs():
            build = item.current_build_set.getBuild(job)
            (result, url) = item.formatJobResult(job)
            # If child_jobs is being used to skip jobs, then the user
            # probably has an expectation that some jobs will be
            # skipped and doesn't need to see all of them.  Otherwise,
            # it may be a surprise and it may be better to include the
            # job in the report.
            if (build.error_detail and
                'Skipped due to child_jobs' in build.error_detail):
                skipped += 1
                continue
            if not job.voting:
                voting = ' (non-voting)'
            else:
                voting = ''

            if config and config.has_option(
                'zuul', 'report_times'):
                report_times = config.getboolean(
                    'zuul', 'report_times')
            else:
                report_times = True

            if report_times and build.end_time and build.start_time:
                dt = int(build.end_time - build.start_time)
                m, s = divmod(dt, 60)
                h, m = divmod(m, 60)
                if h:
                    elapsed = ' in %dh %02dm %02ds' % (h, m, s)
                elif m:
                    elapsed = ' in %dm %02ds' % (m, s)
                else:
                    elapsed = ' in %ds' % (s)
            else:
                elapsed = ''
            if build.error_detail:
                error = ' ' + build.error_detail
            else:
                error = ''
            name = job.name + ' '
            # TODO(mordred) The gerrit consumption interface depends on
            # something existing in the url field and don't have a great
            # behavior defined for url being none/missing. Put name into
            # the url field to match old behavior until we can deal with
            # the gerrit-side piece as well
            url = url or job.name
            # Pass user defined success_message deciding the build result
            # during result formatting
            success_message = job.success_message
            jobs_fields.append(
                (name, url, result, error, elapsed, voting, success_message))
        return jobs_fields, skipped

    def _formatItemReportJobs(self, item, short):
        # Return the list of jobs portion of the report
        # If short is true, omit successful jobs for brevity
        ret = ''
        jobs_fields, skipped = self._getItemReportJobsFields(item)
        for job_fields in jobs_fields:
            if short and job_fields[2] == 'SUCCESS':
                continue
            ret += '- %s%s : %s%s%s%s\n' % job_fields[:6]
        if skipped:
            jobtext = 'job' if skipped == 1 else 'jobs'
            ret += 'Skipped %i %s\n' % (skipped, jobtext)
        if short:
            ret += 'Successful jobs omitted due to length restrictions\n'
        return ret
