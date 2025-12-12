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
import collections
import contextlib
import itertools
import logging
import textwrap
import time
import urllib
from abc import ABCMeta, abstractmethod

from zuul import exceptions
from zuul import model
from zuul.lib.dependson import find_dependency_headers
from zuul.lib.logutil import get_annotated_logger
from zuul.lib.tarjan import strongly_connected_components
import zuul.lib.tracing as tracing
from zuul.model import (
    Change, PipelineState, PipelineChangeList,
    filter_severity, EnqueueEvent, FalseWithReason,
    QueryCacheEntry
)
from zuul.zk.change_cache import ChangeKey
from zuul.zk.exceptions import LockException
from zuul.zk.locks import pipeline_lock
from zuul.zk.components import COMPONENT_REGISTRY

from opentelemetry import trace


class DynamicChangeQueueContextManager(object):
    def __init__(self, change_queue, allow_delete=False):
        self.change_queue = change_queue
        self.allow_delete = allow_delete

    def __enter__(self):
        return self.change_queue

    def __exit__(self, etype, value, tb):
        if (self.allow_delete and
            self.change_queue and
            not self.change_queue.queue):
            self.change_queue.manager.state.removeQueue(
                self.change_queue)


class StaticChangeQueueContextManager(object):
    def __init__(self, change_queue):
        self.change_queue = change_queue

    def __enter__(self):
        return self.change_queue

    def __exit__(self, etype, value, tb):
        pass


EventMatchInfo = collections.namedtuple('EventMatchInfo', ['debug'])


class PipelineManager(metaclass=ABCMeta):
    """Abstract Base Class for enqueing and processing Changes in a Pipeline"""

    def __init__(self, sched, pipeline, tenant):
        self.log = logging.getLogger("zuul.Pipeline.%s.%s" %
                                     (tenant.name,
                                      pipeline.name,))
        self.sched = sched
        self.pipeline = pipeline
        self.tenant = tenant
        self.relative_priority_queues = {}
        # Cached dynamic layouts (layout uuid -> layout)
        self._layout_cache = {}
        # A small local cache to avoid hitting the ZK-based connection
        # change cache for multiple hits in the same pipeline run.
        self._change_cache = {}
        # Current ZK context when the pipeline is locked
        self.current_context = None
        # The pipeline summary used by zuul-web that is updated by the
        # schedulers after processing a pipeline.
        self.summary = model.PipelineSummary()
        self.summary._set(manager=self)
        self.state = None
        self.change_list = None

        if sched:
            self.sql = sched.sql

    def __str__(self):
        return "<%s %s>" % (self.__class__.__name__, self.pipeline.name)

    @contextlib.contextmanager
    def currentContext(self, ctx):
        try:
            self.current_context = ctx
            yield
        finally:
            self.current_context = None

    def _postConfig(self):
        layout = self.tenant.layout
        self.buildChangeQueues(layout)
        # Make sure we have state and change list objects.  We
        # don't actually ensure they exist in ZK here; these are
        # just local objects until they are serialized the first
        # time.  Since we don't hold the pipeline lock, we can't
        # reliably perform any read or write operations; we just
        # need to ensure we have in-memory objects to work with
        # and they will be initialized or loaded on the next
        # refresh.

        # These will be out of date until they are refreshed later.
        self.state = PipelineState.create(
            self, self.state)
        self.change_list = PipelineChangeList.create(
            self)

        # Now, try to acquire a non-blocking pipeline lock and refresh
        # them for the side effect of initializing them if necessary.
        # In the case of a new pipeline, no one else should have a
        # lock anyway, and this helps us avoid emitting a whole bunch
        # of errors elsewhere on startup when these objects don't
        # exist.  If the pipeline already exists and we can't acquire
        # the lock, that's fine, we're much less likely to encounter
        # read errors elsewhere in that case anyway.
        try:
            with (pipeline_lock(
                    self.sched.zk_client, self.tenant.name,
                    self.pipeline.name, blocking=False) as lock,
                    self.sched.createZKContext(lock, self.log) as ctx,
                    self.currentContext(ctx)):
                if not self.state.exists(ctx):
                    # We only do this if the pipeline doesn't exist in
                    # ZK because in that case, this process should be
                    # fast since it's empty.  If it does exist,
                    # refreshing it may be slow and since other actors
                    # won't encounter errors due to its absence, we
                    # would rather defer the work to later.
                    self.state.refresh(ctx)
                    self.change_list.refresh(ctx)
        except LockException:
            pass

    def buildChangeQueues(self, layout):
        self.log.debug("Building relative_priority queues")
        change_queues = self.relative_priority_queues
        tenant = self.tenant
        layout_project_configs = layout.project_configs

        for project_name, project_configs in layout_project_configs.items():
            (trusted, project) = tenant.getProject(project_name)
            queue_name = None
            project_in_pipeline = False
            for project_config in layout.getAllProjectConfigs(project_name):
                project_pipeline_config = project_config.pipelines.get(
                    self.pipeline.name)
                if not queue_name:
                    queue_name = project_config.queue_name
                if project_pipeline_config is None:
                    continue
                project_in_pipeline = True
            if not project_in_pipeline:
                continue

            if not queue_name:
                continue
            if queue_name in change_queues:
                change_queue = change_queues[queue_name]
            else:
                change_queue = []
                change_queues[queue_name] = change_queue
                self.log.debug("Created queue: %s" % queue_name)
            change_queue.append(project)
            self.log.debug("Added project %s to queue: %s" %
                           (project, queue_name))

    def getRelativePriorityQueue(self, project):
        for queue in self.relative_priority_queues.values():
            if project in queue:
                return queue
        return [project]

    def getSubmitAllowNeeds(self):
        # Get a list of code review labels that are allowed to be
        # "needed" in the submit records for a change, with respect
        # to this queue.  In other words, the list of review labels
        # this queue itself is likely to set before submitting.
        allow_needs = set()
        for action_reporter in self.pipeline.success_actions:
            allow_needs.update(action_reporter.getSubmitAllowNeeds(self))
        return allow_needs

    def eventMatches(self, event, change):
        # Return False if the event does not match, return a
        # EventMatchInfo (which will eval to True) if it does.  The
        # EventMatchInfo further indicates whether pipeline debugging
        # should be enabled.
        log = get_annotated_logger(self.log, event)
        if event.forced_pipeline:
            if event.forced_pipeline == self.pipeline.name:
                log.debug("Event %s for change %s was directly assigned "
                          "to pipeline %s" % (event, change, self))
                return EventMatchInfo(debug=False)
            else:
                return False
        for ef in self.pipeline.event_filters:
            match_result = ef.matches(event, change)
            if match_result:
                log.debug("Event %s for change %s matched %s "
                          "in pipeline %s" % (event, change, ef, self))
                return EventMatchInfo(debug=ef.debug)
            else:
                log.debug("Event %s for change %s does not match %s "
                          "in pipeline %s because %s" % (
                              event, change, ef, self, str(match_result)))
        return False

    def getNodePriority(self, item, change):
        queue_projects = set(self.getRelativePriorityQueue(
            change.project))
        items = []
        for i in self.state.getAllItems():
            if not i.live:
                continue
            item_projects = set([
                c.project for c in i.changes])
            if item_projects.intersection(queue_projects):
                items.append(i)
        index = items.index(item)
        # Quantize on a logarithmic scale so that we don't constantly
        # needlessly adjust thousands of node requests.
        # If we're in the top 10 changes, return the accurate number.
        if index < 10:
            return index
        # After 10, batch then in groups of 10 (so items 10-19 are all
        # at node priority 10, 20-29 at 20, etc).
        if index < 100:
            return index // 10 * 10
        # After 100, batch in groups of 100.
        return index // 100 * 100

    def resolveChangeReferences(self, change_references):
        return self.resolveChangeKeys(
            [ChangeKey.fromReference(r) for r in change_references])

    def resolveChangeKeys(self, change_keys):
        resolved_changes = []
        for key in change_keys:
            change = self._change_cache.get(key.reference)
            if change is None:
                source = self.sched.connections.getSource(key.connection_name)
                change = source.getChange(key)
                if change is None:
                    self.log.error("Unable to resolve change from key %s", key)

                if isinstance(change, model.Change):
                    update_commit_dependencies = (
                        change.commit_needs_changes is None)
                    update_topic_dependencies = (
                        change.topic_needs_changes is None
                        and self.useDependenciesByTopic(change.project))
                    if (update_commit_dependencies
                            or update_topic_dependencies):
                        self.updateCommitDependencies(change, event=None)
                self._change_cache[change.cache_key] = change
            resolved_changes.append(change)
        return resolved_changes

    def clearCache(self):
        self._change_cache.clear()

    def _maintainCache(self):
        active_layout_uuids = set()
        referenced_change_keys = set()
        for item in self.state.getAllItems():
            if item.layout_uuid:
                active_layout_uuids.add(item.layout_uuid)

            for change in item.changes:
                if isinstance(change, model.Change):
                    referenced_change_keys.update(change.getNeedsChanges(
                        self.useDependenciesByTopic(change.project)))
                    referenced_change_keys.update(
                        change.getNeededByChanges())

        # Clean up unused layouts in the cache
        unused_layouts = set(self._layout_cache.keys()) - active_layout_uuids
        if unused_layouts:
            self.log.debug("Removing unused layouts %s from cache",
                           unused_layouts)
            for uid in unused_layouts:
                with contextlib.suppress(KeyError):
                    del self._layout_cache[uid]

        # Clean up change cache
        unused_keys = set(self._change_cache.keys()) - referenced_change_keys
        for key in unused_keys:
            with contextlib.suppress(KeyError):
                del self._change_cache[key]

    def isChangeAlreadyInPipeline(self, change, event):
        # Checks live items in the pipeline
        for item in self.state.getAllItems():
            if not item.live:
                continue
            for c in item.changes:
                if change.equals(c):
                    if event and item.event:
                        if item.event.isSuperset(event):
                            return True
                    else:
                        return True
        return False

    def isChangeRelevantToPipeline(self, change):
        # Checks if any version of the change or its deps matches any
        # item in the pipeline.
        for change_key in self.change_list.getChangeKeys():
            if change.cache_stat.key.isSameChange(change_key):
                return True
        if isinstance(change, model.Change):
            for dep_change_ref in change.getNeedsChanges(
                    self.useDependenciesByTopic(change.project)):
                dep_change_key = ChangeKey.fromReference(dep_change_ref)
                for change_key in self.change_list.getChangeKeys():
                    if change_key.isSameChange(dep_change_key):
                        return True
        return False

    def isChangeAlreadyInQueue(self, change, change_queue, event, item=None):
        # Checks any item in the specified change queue
        # If item is supplied, only consider items ahead of the
        # supplied item (ie, is the change already in the queue ahead
        # of this item?).
        for queue_item in change_queue.queue:
            if queue_item is item:
                break
            for c in queue_item.changes:
                if change.equals(c):
                    if event and queue_item.event:
                        if queue_item.event.isSuperset(event):
                            return True
                    else:
                        return True
        return False

    def refreshDeps(self, change, event):
        if not isinstance(change, model.Change):
            return

        self.sched.query_cache.clearIfOlderThan(event)
        to_refresh = set()
        for item in self.state.getAllItems():
            for item_change in item.changes:
                if not isinstance(item_change, model.Change):
                    continue
                if item_change.equals(change):
                    to_refresh.add(item_change)
                for dep_change_ref in item_change.commit_needs_changes:
                    dep_change_key = ChangeKey.fromReference(dep_change_ref)
                    if dep_change_key.isSameChange(change.cache_stat.key):
                        to_refresh.add(item_change)

        for existing_change in to_refresh:
            self.updateCommitDependencies(existing_change, event)

    def reportEnqueue(self, item):
        if not self.state.disabled:
            log = get_annotated_logger(self.log, item.event)
            log.info("Reporting enqueue, action %s item %s" %
                     (self.pipeline.enqueue_actions, item))
            ret = self.sendReport(self.pipeline.enqueue_actions, item)
            if ret:
                log.error("Reporting item enqueued %s received: %s" %
                          (item, ret))
            else:
                item.updateAttributes(self.current_context,
                                      reported_enqueue=True)

    def reportStart(self, item):
        if not self.state.disabled:
            log = get_annotated_logger(self.log, item.event)
            log.info("Reporting start, action %s item %s" %
                     (self.pipeline.start_actions, item))
            ret = self.sendReport(self.pipeline.start_actions, item)
            if ret:
                log.error("Reporting item start %s received: %s" % (item, ret))
            else:
                item.updateAttributes(self.current_context,
                                      reported_start=True)

    def reportNormalBuildsetEnd(self, build_set, action, final, result=None):
        # Report a buildset end if there are jobs or errors
        if ((build_set.job_graph and len(build_set.job_graph.job_uuids) > 0) or
            build_set.has_blocking_errors or
            build_set.unable_to_merge):
            self.sql.reportBuildsetEnd(build_set, action,
                                       final, result)

    def reportDequeue(self, item, quiet=False):
        if not (self.state.disabled or quiet):
            log = get_annotated_logger(self.log, item.event)
            log.info(
                "Reporting dequeue, action %s item%s",
                self.pipeline.dequeue_actions,
                item,
            )
            ret = self.sendReport(self.pipeline.dequeue_actions, item)
            if ret:
                log.error(
                    "Reporting item dequeue %s received: %s", item, ret
                )
        # This might be called after canceljobs, which also sets a
        # non-final 'cancel' result.
        self.reportNormalBuildsetEnd(item.current_build_set, 'dequeue',
                                     final=False)

    def sendReport(self, action_reporters, item, phase1=True, phase2=True):
        """Sends the built message off to configured reporters.

        Takes the action_reporters and item and sends them to the
        pluggable reporters.

        """
        log = get_annotated_logger(self.log, item.event)
        report_errors = []
        if len(action_reporters) > 0:
            for reporter in action_reporters:
                try:
                    ret = reporter.report(item, phase1=phase1, phase2=phase2)
                    if ret:
                        for r in ret:
                            if r:
                                report_errors.append(r)
                except Exception as e:
                    item.setReportedResult('ERROR')
                    log.exception("Exception while reporting")
                    report_errors.append(str(e))
        return report_errors

    def areChangesReadyToBeEnqueued(self, changes, event,
                                    warnings=None, debug=False):
        return True

    def enqueueChangesAhead(self, change, event, quiet, ignore_requirements,
                            change_queue, history=None, dependency_graph=None,
                            warnings=None, debug=False):
        return True

    def enqueueChangesBehind(self, change, event, quiet, ignore_requirements,
                             change_queue, history=None,
                             dependency_graph=None):
        return True

    def getMissingNeededChanges(self, changes, change_queue, event,
                                dependency_graph=None, item=None):
        """Check that all needed changes are ahead in the queue.

        Return a list of any that are missing.  If it is not possible
        to correct the missing changes, "abort" will be true.

        If item is supplied, only consider items ahead of the supplied
        item when determining whether needed changes are already ahead
        in the queue.

        :returns: (abort, needed_changes)

        """
        return False, []

    def getFailingDependentItems(self, item):
        return []

    def getItemForChange(self, change, change_queue=None):
        if change_queue is not None:
            items = change_queue.queue
        else:
            items = self.state.getAllItems()

        for item in items:
            for c in item.changes:
                if change.equals(c):
                    return item
        return None

    def findOldVersionOfChangeAlreadyInQueue(self, change):
        # Return the item and the old version of the change
        for item in self.state.getAllItems():
            if not item.live:
                continue
            for item_change in item.changes:
                if change.isUpdateOf(item_change):
                    return item, item_change
        return None, None

    def removeOldVersionsOfChange(self, change, event):
        if not self.pipeline.dequeue_on_new_patchset:
            return
        old_item, old_change = self.findOldVersionOfChangeAlreadyInQueue(
            change)
        if old_item:
            log = get_annotated_logger(self.log, event)
            log.debug("Change %s is a new version, removing %s",
                      change, old_item)
            self.removeItem(old_item)
            if old_item.live:
                changes = old_item.changes[:]
                changes.remove(old_change)
                self.reEnqueueChanges(old_item, changes)

    def removeAbandonedChange(self, change, event):
        log = get_annotated_logger(self.log, event)
        log.debug("Change %s abandoned, removing", change)
        for queue in self.state.queues:
            # Below we need to remove dependent changes of abandoned
            # changes we remove here, but only if both are live.
            # Therefore, track the changes we remove independently for
            # each shared queue (since different queues may have
            # different live/non-live changes).
            changes_removed = set()
            for item in queue.queue:
                if not item.live:
                    continue
                self._removeAbandonedChangeExamineItem(
                    change, item, changes_removed)
            # Abbreviated version of dependency check; we're not going
            # to check everything (processOneItem will take care of
            # that), but we will remove any changes that depend on any
            # changes we just removed; we can do that without
            # recreating the full dependency graphs.
            if not changes_removed:
                continue
            for item in queue.queue:
                if not item.live:
                    continue
                self._removeAbandonedChangeDependentsExamineItem(
                    log, item, changes_removed)

    def _removeAbandonedChangeExamineItem(self, change, item, changes_removed):
        # The inner loop from the first part of removeAbandonedChange.
        for item_change in item.changes:
            if item_change.equals(change):
                if len(item.changes) > 1:
                    # We need to cancel all jobs here as setting
                    # dequeued needing change will skip all jobs.
                    self.cancelJobs(item)
                    msg = ("Dependency cycle change "
                           f"{change.url} abandoned.")
                    item.setDequeuedNeedingChange(msg)
                    try:
                        self.reportItem(item)
                    except exceptions.MergeFailure:
                        pass
                self.removeItem(item)
                changes_removed.update(item.changes)
                return

    def _removeAbandonedChangeDependentsExamineItem(
            self, log, item, changes_removed):
        # The inner loop from the second part of removeAbandonedChange.
        for item_change in item.changes:
            for needed_change in self.resolveChangeReferences(
                    item_change.getNeedsChanges(
                        self.useDependenciesByTopic(item_change.project))):
                for removed_change in changes_removed:
                    if needed_change.equals(removed_change):
                        log.debug("Change %s depends on abandoned change %s, "
                                  "removing", item_change, removed_change)
                        msg = f'Change {removed_change} is needed.'
                        item.setDequeuedNeedingChange(msg)
                        if item.live:
                            try:
                                self.reportItem(item)
                            except exceptions.MergeFailure:
                                pass
                            self.dequeueItem(item)
                            changes_removed.update(item.changes)
                            return

    def reEnqueueChanges(self, item, changes):
        for change in changes:
            orig_ref = None
            if item.event:
                orig_ref = item.event.ref
            event = EnqueueEvent(self.tenant.name,
                                 self.pipeline.name,
                                 change.project.canonical_hostname,
                                 change.project.name,
                                 change=change._id(),
                                 orig_ref=orig_ref)
            event.zuul_event_id = item.event.zuul_event_id
            self.sched.pipeline_management_events[
                self.tenant.name][self.pipeline.name].put(event)

    @abstractmethod
    def getChangeQueue(self, change, event, existing=None):
        pass

    def reEnqueueItem(self, item, last_head, old_item_ahead, item_ahead_valid):
        log = get_annotated_logger(self.log, item.event)
        with self.getChangeQueue(item.changes[0], item.event,
                                 last_head.queue) as change_queue:
            if change_queue:
                log.debug("Re-enqueing %s in queue %s",
                          item, change_queue)
                change_queue.enqueueItem(item)

                # If the old item ahead was re-enqued, this value will
                # be true, so we should attempt to move the item back
                # to where it was in case an item ahead is already
                # failing.
                if item_ahead_valid:
                    change_queue.moveItem(item, old_item_ahead)

                # Get an updated copy of the layout, but if we have a
                # job graph already, then keep it (our repo state and
                # jobs are frozen and will now only update if the item
                # ahead changes).  This resumes the buildset merge
                # state machine.  If we have an up-to-date layout, it
                # will go ahead and refresh the job graph if there
                # isn't one; or it will send a new merge job if
                # necessary, or it will do nothing if we're waiting on
                # a merge job.
                has_job_graph = bool(item.current_build_set.job_graph)
                if item.live:
                    # Only reset the layout for live items as we don't need to
                    # re-create the layout in independent pipelines.
                    item.updateAttributes(self.current_context,
                                          layout_uuid=None)

                # If the item is no longer active, but has a job graph we
                # will make sure to update it.
                if item.active or has_job_graph:
                    self.prepareItem(item)

                # Re-set build results in case any new jobs have been
                # added to the tree.
                for build in item.current_build_set.getBuilds():
                    if build.result:
                        item.setResult(build)
                # Similarly, reset the item state.
                if item.current_build_set.unable_to_merge:
                    item.setUnableToMerge()
                if item.current_build_set.has_blocking_errors:
                    item.setConfigErrors(item.getConfigErrors())
                if item.dequeued_needing_change:
                    item.setDequeuedNeedingChange(item.dequeued_needing_change)
                if item.dequeued_missing_requirements:
                    item.setDequeuedMissingRequirements()

                # It can happen that all in-flight builds have been removed
                # which would lead to paused parent jobs not being resumed.
                # To prevent that resume parent jobs if necessary.
                self._resumeBuilds(item.current_build_set)

                self.reportStats(item)
                return True
            else:
                log.error("Unable to find change queue for project %s",
                          item.change[0].project)
                return False

    def addChange(self, change, event, quiet=False, enqueue_time=None,
                  ignore_requirements=False, live=True,
                  change_queue=None, history=None, dependency_graph=None,
                  skip_presence_check=False, warnings=None,
                  debug=False):
        log = get_annotated_logger(self.log, event)
        log.debug("Considering adding change %s" % change)

        history = history if history is not None else []
        log.debug("History: %s", history)

        warnings = warnings if warnings is not None else []

        # Ensure the dependency graph is created when the first change is
        # processed to allow cycle detection with the Tarjan algorithm
        dependency_graph = dependency_graph or collections.OrderedDict()
        log.debug("Dependency graph: %s", dependency_graph)

        # If we are adding a live change, check if it's a live item
        # anywhere in the pipeline.  Otherwise, we will perform the
        # duplicate check below on the specific change_queue.
        if (live and
            self.isChangeAlreadyInPipeline(change, event) and
            not skip_presence_check):
            log.debug("Change %s is already in pipeline, ignoring" % change)
            return True

        if ((not self.pipeline.allow_other_connections) and
            (change.project.connection_name not in self.pipeline.connections)):
            log.debug("Change %s is not from a connection known to %s ",
                      change, self.pipeline)
            return False

        try:
            self.getDependencyGraph(change, dependency_graph, event,
                                    update_deps=True)
        except exceptions.DependencyLimitExceededError:
            return False

        with self.getChangeQueue(change, event, change_queue) as change_queue:
            if not change_queue:
                log.debug("Unable to find change queue for "
                          "change %s in project %s" %
                          (change, change.project))
                return False

            cycle = []
            if isinstance(change, model.Change):
                cycle = self.cycleForChange(change, dependency_graph, event)
                cycle = self.sortCycleByGitDepends(cycle)
            if not cycle:
                cycle = [change]

            if not ignore_requirements:
                for f in self.pipeline.ref_filters:
                    for cycle_change in cycle:
                        if (f.connection_name !=
                            cycle_change.project.connection_name):
                            log.debug("Filter %s skipped for change %s due "
                                      "to mismatched connections",
                                      f, cycle_change)
                            continue
                        match_result = f.matches(cycle_change)
                        if not match_result:
                            msg = (
                                f"Change {cycle_change} "
                                "does not match pipeline "
                                f"requirement {f} because {match_result}"
                            )
                            log.debug(msg)
                            if debug:
                                warnings.append(msg)
                                if not history:
                                    self._reportNonEnqueuedItem(
                                        change_queue, change, event, warnings)
                            return False

            if not self.areChangesReadyToBeEnqueued(
                    cycle, event,
                    warnings=warnings, debug=debug):
                log.debug("Cycle %s is not ready to be enqueued, ignoring" %
                          cycle)
                if warnings:
                    if not history:
                        self._reportNonEnqueuedItem(change_queue, change,
                                                    event, warnings)
                return False

            if len(cycle) > 1:
                for cycle_change in cycle:
                    if not self.canProcessCycle(cycle_change.project):
                        log.info("Not enqueueing change %s since the project "
                                 "does not allow circular dependencies",
                                 cycle_change)
                        warnings = ["Dependency cycle detected and project "
                                    f"{cycle_change.project.name} "
                                    "doesn't allow circular dependencies"]
                        self._reportNonEnqueuedItem(
                            change_queue, change, event, warnings)
                        return False

            if not (reason := self.checkPipelineWithinLimits(cycle, history)):
                log.info("Not enqueueing change %s since "
                         "pipeline not within limits: %s",
                         change, reason)
                warnings.append(
                    f"Unable to enqueue change: {reason}"
                )
                if not history:
                    # Only report if we are the originating change;
                    # otherwise we're being called from
                    # enqueueChangesAhead.
                    self._reportNonEnqueuedItem(
                        change_queue, change, event, warnings[-1:])
                return False

            warnings = []
            if not self.enqueueChangesAhead(
                    cycle, event, quiet,
                    ignore_requirements,
                    change_queue, history=history,
                    dependency_graph=dependency_graph,
                    warnings=warnings, debug=debug):
                log.debug("Failed to enqueue changes ahead of %s" % change)
                if warnings:
                    self._reportNonEnqueuedItem(change_queue, change,
                                                event, warnings)
                return False

            log.debug("History after enqueuing changes ahead: %s", history)

            if self.isChangeAlreadyInQueue(change, change_queue, event):
                if not skip_presence_check:
                    log.debug("Change %s is already in queue, ignoring",
                              change)
                    return True

            log.info("Adding %s to queue %s in %s" %
                     (cycle, change_queue, self.pipeline))
            if enqueue_time is None:
                enqueue_time = time.time()

            event_span = tracing.restoreSpanContext(event.span_context)
            link_attributes = {"rel": type(event).__name__}
            link = trace.Link(event_span.get_span_context(),
                              attributes=link_attributes)
            span_info = tracing.startSavedSpan(
                'QueueItem', start_time=enqueue_time, links=[link])

            item = change_queue.enqueueChanges(cycle, event,
                                               span_info=span_info,
                                               enqueue_time=enqueue_time,
                                               debug=debug)

            with item.activeContext(self.current_context):
                if enqueue_time:
                    item.enqueue_time = enqueue_time
                item.live = live
                self.reportStats(item, trigger_event=event)
                item.quiet = quiet

            if item.live:
                self.reportEnqueue(item)

            for c in cycle:
                # We ignore the supplied value for ignore_requirements
                # and instead always apply pipeline requirements to
                # changes behind.  If we perform a direct enqueue to
                # override requirements, that should only apply to a
                # change and its dependencies, not to its dependents.
                # If we or a user want to override requirements for a
                # change behind, we'll direct-enqueue the change
                # behind.
                self.enqueueChangesBehind(c, event, quiet,
                                          False,
                                          change_queue, history,
                                          dependency_graph)

            zuul_driver = self.sched.connections.drivers['zuul']
            tenant = self.tenant
            with trace.use_span(tracing.restoreSpan(item.span_info)):
                for c in item.changes:
                    zuul_driver.onChangeEnqueued(
                        tenant, c, self, event)
            self.dequeueSupercededItems(item)
            return True

    def _reportNonEnqueuedItem(self, change_queue, change, event, warnings):
        # Enqueue an item which otherwise can not be enqueued in order
        # to report a message to the user.
        actions = self.pipeline.failure_actions
        ci = change_queue.enqueueChanges([change], event)
        try:
            for w in warnings:
                ci.warning(w)
            ci.setReportedResult('FAILURE')

            # Only report the item if the project is in the current
            # pipeline. Otherwise the change could be spammed by
            # reports from unrelated pipelines.
            try:
                if self.tenant.layout.getProjectPipelineConfig(
                        ci, change):
                    self.sendReport(actions, ci)
            except model.TemplateNotFoundError:
                # This error is not unreasonable under the
                # circumstances; assume that a change to add a project
                # template is in a dequeued cycle.
                pass
        finally:
            # Ensure that the item is dequeued in any case. Otherwise we
            # might leak items caused by this temporary enqueue.
            self.dequeueItem(ci)
        # We don't use reportNormalBuildsetEnd here because we want to
        # report even with no jobs.
        self.sql.reportBuildsetEnd(ci.current_build_set,
                                   'failure', final=True)

    def cycleForChange(self, change, dependency_graph, event, debug=True):
        if debug:
            log = get_annotated_logger(self.log, event)
            log.debug("Running Tarjan's algorithm on current dependencies: %s",
                      dependency_graph)
        sccs = [s for s in strongly_connected_components(dependency_graph)
                if len(s) > 1]
        if debug:
            log.debug("Strongly connected components (cyles): %s", sccs)
        for scc in sccs:
            if change in scc:
                if debug:
                    log.debug("Dependency cycle detected for "
                              "change %s in project %s",
                              change, change.project)
                # Change can not be part of multiple cycles, so we can return
                return scc
        return []

    def sortCycleByGitDepends(self, cycle):
        new_cycle = []
        cycle = list(cycle)
        while cycle:
            self._sortCycleByGitDepends(cycle[0], cycle, new_cycle)
        return new_cycle

    def _sortCycleByGitDepends(self, change, cycle, new_cycle):
        cycle.remove(change)
        for needed_change in self.resolveChangeReferences(
                change.git_needs_changes):
            if needed_change not in cycle:
                continue
            self._sortCycleByGitDepends(needed_change, cycle, new_cycle)
        new_cycle.append(change)

    def getCycleDependencies(self, change, dependency_graph, event):
        cycle = self.cycleForChange(change, dependency_graph, event)
        return set(
            itertools.chain.from_iterable(
                dependency_graph[c] for c in cycle if c != change)
        ) - set(cycle)

    def getDependencyGraph(self, change, dependency_graph, event,
                           update_deps=False,
                           history=None, quiet=False, indent=''):
        log = get_annotated_logger(self.log, event)
        if not quiet:
            log.debug("%sChecking for changes needed by %s:", indent, change)
        if self.pipeline.ignore_dependencies:
            return
        if not isinstance(change, model.Change):
            return
        if history is None:
            history = set()
        self.sched.query_cache.clearIfOlderThan(event)
        history.add(change)
        if update_deps:
            self.updateCommitDependencies(change, event)
        for needed_change in self.resolveChangeReferences(
                change.getNeedsChanges(
                    self.useDependenciesByTopic(change.project))):
            if not quiet:
                log.debug("%sChange %s needs change %s:",
                          indent, change, needed_change)
            if needed_change.is_merged:
                if not quiet:
                    log.debug("%sNeeded change is merged", indent)
                continue

            if (self.tenant.max_dependencies is not None and
                (len(dependency_graph) >
                 self.tenant.max_dependencies)):
                log.info("%sDependency graph for change %s is too large",
                         indent, change)
                raise exceptions.DependencyLimitExceededError(
                    "Dependency graph is too large")

            node = dependency_graph.setdefault(change, [])
            if needed_change not in node:
                if not quiet:
                    log.debug("%sAdding change %s to dependency graph for "
                              "change %s", indent, needed_change, change)
                node.append(needed_change)
            if needed_change not in history:
                self.getDependencyGraph(needed_change,
                                        dependency_graph, event,
                                        update_deps,
                                        history, quiet, indent + '  ')

    def getQueueConfig(self, project):
        layout = self.tenant.layout
        queue_name = None
        for project_config in layout.getAllProjectConfigs(
            project.canonical_name
        ):
            if not queue_name:
                queue_name = project_config.queue_name

            project_pipeline_config = project_config.pipelines.get(
                self.pipeline.name)

            if project_pipeline_config is None:
                continue

        return layout.queues.get(queue_name)

    def canProcessCycle(self, project):
        queue_config = self.getQueueConfig(project)
        if queue_config is None:
            return False

        return queue_config.allow_circular_dependencies

    def useDependenciesByTopic(self, project):
        source = self.sched.connections.getSource(project.connection_name)
        if source.useDependenciesByTopic():
            return True

        queue_config = self.getQueueConfig(project)
        if queue_config is None:
            return False

        return queue_config.dependencies_by_topic

    def checkPipelineWithinLimits(self, cycle, history):
        pipeline_max = self.tenant.max_changes_per_pipeline
        if pipeline_max is None:
            return True
        additional = len(cycle) + len(history)
        if additional > pipeline_max:
            return FalseWithReason(
                f"{additional} changes to enqueue greater than "
                f"pipeline max of {pipeline_max}")

        count = additional
        for item in self.state.getAllItems():
            count += len(item.changes)
            if count > pipeline_max:
                return FalseWithReason(
                    f"{additional} additional changes would exceed "
                    f"pipeline max of {pipeline_max} under current "
                    "conditions")
        return True

    def getNonMergeableCycleChanges(self, item):

        """Return changes in the cycle that do not fulfill
        the pipeline's ready criteria."""
        return []

    def dequeueItem(self, item, quiet=False):
        log = get_annotated_logger(self.log, item.event)
        log.debug("Removing %s from queue", item)
        # In case a item is dequeued that doesn't have a result yet
        # (success/failed/...) we report it as dequeued.
        # Without this check, all items with a valid result would be reported
        # twice.
        if not item.current_build_set.result and item.live:
            item.setReportedResult('DEQUEUED')
            if not (item.reported_enqueue or item.reported_start):
                # If we haven't reported start, we don't know if Zuul
                # was supposed to act on the item at all, so keep it
                # quiet; but if we reported enqueue, then we're
                # probably in a pipeline that has an enqueue/dequeue
                # pair, so we should make sure we clean that up.
                quiet = True
            self.reportDequeue(item, quiet)
        item.queue.dequeueItem(item)

        span_attrs = {
            'zuul_event_id': item.event.zuul_event_id,
            'zuul_tenant': self.tenant.name,
            'zuul_pipeline': self.pipeline.name,
        }
        for change in item.changes:
            for k, v in change.getSafeAttributes().toDict().items():
                if v is not None:
                    span_attrs.setdefault(f'ref_{k}', []).append(v)
        tracing.endSavedSpan(item.current_build_set.span_info)
        tracing.endSavedSpan(item.span_info,
                             attributes=span_attrs)

    def removeItem(self, item):
        log = get_annotated_logger(self.log, item.event)
        # Remove an item from the queue, probably because it has been
        # superseded by another change.
        log.debug("Canceling builds behind item: %s "
                  "because it is being removed.", item)
        self.cancelJobs(item)
        self.dequeueItem(item)
        self.reportStats(item)

    def dequeueSupercededItems(self, item):
        for other_name in self.pipeline.supercedes:
            other_manager = self.tenant.layout.pipeline_managers.get(
                other_name)
            if not other_manager:
                continue

            for change in item.changes:
                change_id = (
                    change._id() if isinstance(change, Change)
                    else None
                )
                event = model.SupercedeEvent(
                    other_manager.tenant.name,
                    other_manager.pipeline.name,
                    change.project.canonical_hostname,
                    change.project.name,
                    change_id,
                    change.ref)
                self.sched.pipeline_trigger_events[
                    self.tenant.name][other_manager.pipeline.name
                        ].put_supercede(event)

    def updateCommitDependencies(self, change, event):
        log = get_annotated_logger(self.log, event)
        query_cache = self.sched.query_cache
        query_cache.clearIfOlderThan(event)

        must_update_commit_deps = (
            not hasattr(event, "zuul_event_ltime")
            or change.commit_needs_changes is None
            or change.cache_stat.mzxid <= event.zuul_event_ltime
        )

        if hasattr(event, "zuul_event_ltime"):
            if COMPONENT_REGISTRY.model_api < 32:
                topic_out_of_date =\
                    change.cache_stat.mzxid <= event.zuul_event_ltime
            else:
                topic_out_of_date =\
                    change.topic_query_ltime <= event.zuul_event_ltime
        else:
            # The value is unused and doesn't matter because of the
            # clause below, but True is the safer value.
            topic_out_of_date = True

        must_update_topic_deps = (
            self.useDependenciesByTopic(change.project) and (
                not hasattr(event, "zuul_event_ltime")
                or change.topic_needs_changes is None
                or topic_out_of_date
            )
        )

        update_attrs = {}
        if must_update_commit_deps:
            # Search for Depends-On headers and find appropriate changes
            log.debug("  Updating commit dependencies for %s", change)
            dependencies = []
            seen = set()
            for match in find_dependency_headers(change.message):
                log.debug("  Found Depends-On header: %s", match)
                if match in seen:
                    continue
                seen.add(match)
                try:
                    url = urllib.parse.urlparse(match)
                except ValueError:
                    continue
                source = self.sched.connections.getSourceByHostname(
                    url.hostname)
                if not source:
                    continue
                log.debug("  Found source: %s", source)
                dep = source.getChangeByURLWithRetry(match, event)
                if dep and (not dep.is_merged) and dep not in dependencies:
                    log.debug("  Adding dependency: %s", dep)
                    dependencies.append(dep)
            new_commit_needs_changes = [d.cache_key for d in dependencies]

            update_attrs = dict(commit_needs_changes=new_commit_needs_changes)

        # Ask the source for any tenant-specific changes (this allows
        # drivers to implement their own way of collecting deps):
        source = self.sched.connections.getSource(
            change.project.connection_name)
        if must_update_topic_deps:
            log.debug("  Updating topic dependencies for %s", change)
            new_topic_needs_changes_keys = []
            query_cache_key = (change.project.connection_name, change.topic)
            cache_entry = query_cache.topic_queries.get(query_cache_key)
            if cache_entry is None:
                changes_by_topic = source.getChangesByTopic(
                    change.topic, event)
                cache_entry = QueryCacheEntry(
                    self.sched.zk_client.getCurrentLtime(),
                    changes_by_topic)
                query_cache.topic_queries[query_cache_key] = cache_entry
            for dep in cache_entry.results:
                if dep and (not dep.is_merged) and dep is not change:
                    log.debug("  Adding dependency: %s", dep)
                    new_topic_needs_changes_keys.append(dep.cache_key)
            update_attrs['topic_needs_changes'] = new_topic_needs_changes_keys
            update_attrs['topic_query_ltime'] = cache_entry.ltime

        if update_attrs:
            source.setChangeAttributes(change, **update_attrs)

    def provisionNodes(self, item):
        log = item.annotateLogger(self.log)
        jobs = item.findJobsToRequest(self.tenant.semaphore_handler)
        if not jobs:
            return False
        build_set = item.current_build_set
        log.debug("Requesting nodes for %s", item)
        parent_span = tracing.restoreSpan(build_set.span_info)
        with trace.use_span(parent_span):
            for job in jobs:
                if self.sched.globals.use_relative_priority:
                    relative_priority = self.getNodePriority(
                        item, item.getChangeForJob(job))
                else:
                    relative_priority = 0
                if self._useNodepoolFallback(log, job):
                    self._makeNodepoolRequest(
                        log, build_set, job, relative_priority)
                else:
                    self._makeLauncherRequest(
                        log, build_set, job, relative_priority)
        return True

    def _useNodepoolFallback(self, log, job):
        if not self.tenant.use_nodepool:
            return False
        labels = {n.label for n in job.nodeset.getNodes()}
        for provider in self.tenant.layout.providers.values():
            labels -= set(provider.labels.keys())
            if not labels:
                return False
        log.debug("Falling back to Nodepool due to missing labels: %s", labels)
        return True

    def _makeNodepoolRequest(self, log, build_set, job, relative_priority):
        provider = self._getPausedParentProvider(build_set, job)
        priority = self._calculateNodeRequestPriority(build_set, job)
        tenant_name = self.tenant.name
        pipeline_name = self.pipeline.name
        item = build_set.item
        req = self.sched.nodepool.requestNodes(
            build_set.uuid, job, tenant_name, pipeline_name, provider,
            priority, relative_priority, event=item.event)
        log.debug("Adding node request %s for job %s to item %s",
                  req, job, item)
        build_set.setJobNodeRequestID(job, req.id)
        if req.fulfilled:
            nodeset_info = self.sched.nodepool.getNodesetInfo(
                req, job.nodeset)
            job = build_set.item.getJob(req.job_uuid)
            build_set.setJobNodeSetInfo(job, nodeset_info)
        else:
            job.setWaitingStatus(f'node request: {req.id}')

    def _makeLauncherRequest(self, log, build_set, job, relative_priority):
        provider = self._getPausedParentProvider(build_set, job)
        priority = self._calculateNodeRequestPriority(build_set, job)
        item = build_set.item
        req = self.sched.launcher.requestNodeset(
            item, job, priority, relative_priority, provider)
        log.debug("Adding nodeset request %s for job %s to item %s",
                  req, job, item)
        build_set.setJobNodeRequestID(job, req.uuid)
        if req.fulfilled and not req.labels:
            # Short circuit empty node request
            nodeset_info = model.NodesetInfo()
            build_set.setJobNodeSetInfo(job, nodeset_info)
        else:
            job.setWaitingStatus(f'nodeset request: {req.uuid}')

    def _getPausedParent(self, build_set, job):
        job_graph = build_set.job_graph
        if job_graph:
            for parent in job_graph.getParentJobsRecursively(job):
                build = build_set.getBuild(parent)
                if build.paused:
                    return build
        return None

    def _getPausedParentProvider(self, build_set, job):
        parent_build = self._getPausedParent(build_set, job)
        if parent_build:
            return build_set.getJobNodeProvider(parent_build.job)
        return None

    def _calculateNodeRequestPriority(self, build_set, job):
        precedence_adjustment = 0
        precedence = self.pipeline.precedence
        if self._getPausedParent(build_set, job):
            precedence_adjustment = -1
        initial_precedence = model.PRIORITY_MAP[precedence]
        return max(0, initial_precedence + precedence_adjustment)

    def _executeJobs(self, item, jobs):
        log = get_annotated_logger(self.log, item.event)
        log.debug("Executing jobs for %s", item)
        build_set = item.current_build_set

        resolved_changes = self.resolveChangeReferences(
            [c['ref'] for c in build_set.dependent_changes])
        dependent_changes = []
        for resolved_change, orig_dict in zip(resolved_changes,
                                              build_set.dependent_changes):
            change_dict = resolved_change.toDict()
            if 'bundle_id' in orig_dict:
                change_dict['bundle_id'] = orig_dict['bundle_id']
            dependent_changes.append(change_dict)

        for job in jobs:
            log.debug("Found job %s for %s", job, item)
            try:
                zone = build_set.getJobNodeExecutorZone(job)
                nodes = build_set.getJobNodeList(job)
                self.sched.executor.execute(
                    job, nodes, item, self.pipeline, zone,
                    dependent_changes,
                    build_set.merger_items)
                job.setWaitingStatus('executor')
            except Exception:
                log.exception("Exception while executing job %s "
                              "for %s:", job, item)
                try:
                    # If we hit an exception we don't have a build in the
                    # current item so a potentially aquired semaphore must be
                    # released as it won't be released on dequeue of the item.
                    tenant = self.tenant
                    pipeline = self.pipeline

                    if COMPONENT_REGISTRY.model_api >= 33:
                        event_queue = self.sched.management_events[tenant.name]
                    else:
                        event_queue = self.sched.pipeline_result_events[
                            tenant.name][pipeline.name]
                    tenant.semaphore_handler.release(event_queue, item, job)
                except Exception:
                    log.exception("Exception while releasing semaphore")

    def executeJobs(self, item):
        # TODO(jeblair): This should return a value indicating a job
        # was executed.  Appears to be a longstanding bug.
        if not item.current_build_set.job_graph:
            return False

        jobs = item.findJobsToRun(self.tenant.semaphore_handler)
        if jobs:
            self._executeJobs(item, jobs)

    def cancelJobs(self, item, prime=True):
        log = get_annotated_logger(self.log, item.event)
        log.debug("Cancel jobs for %s", item)
        canceled = False
        old_build_set = item.current_build_set
        jobs_to_cancel = item.getJobs()

        for job in jobs_to_cancel:
            self.sched.cancelJob(old_build_set, job, final=True)

        if (prime and item.current_build_set.ref):
            # Force a dequeued result here because we haven't actually
            # reported the item, but we are done with this buildset.
            self.reportNormalBuildsetEnd(
                item.current_build_set, 'dequeue', final=False,
                result='DEQUEUED')
            tracing.endSavedSpan(item.current_build_set.span_info)
            item.resetAllBuilds()

        for item_behind in item.items_behind:
            log.debug("Canceling jobs for %s, behind %s",
                      item_behind, item)
            if self.cancelJobs(item_behind, prime=prime):
                canceled = True
        return canceled

    def _findRelevantErrors(self, item, layout):
        # First collect all the config errors that are not related to the
        # current item.
        parent_error_keys = list(
            self.tenant.layout.loading_errors.error_keys)
        for item_ahead in item.items_ahead:
            parent_error_keys.extend(
                e.key for e in item.item_ahead.getConfigErrors())

        # Then find config errors which aren't in the parent.  But
        # include errors in this project-branch because the error
        # detection hash is imperfect and someone attempting to fix an
        # error may create a near duplicate error and it would go
        # undetected.  Or if there are two errors and the user only
        # fixes one, then they may not realize their work is
        # incomplete.
        relevant_errors = []
        severity_error = False
        for err in layout.loading_errors.errors:
            econtext = err.key.context
            matches_project_branch = False
            if econtext:
                for change in item.changes:
                    if (econtext.project_name == change.project.name and
                        econtext.branch == change.branch):
                        matches_project_branch = True
                        break
            if (err.key not in parent_error_keys or
                matches_project_branch):
                relevant_errors.append(err)
                if err.severity == model.SEVERITY_ERROR:
                    severity_error = True
        return relevant_errors, severity_error

    def _loadDynamicLayout(self, item):
        log = get_annotated_logger(self.log, item.event)
        # Load layout
        # Late import to break an import loop
        import zuul.configloader
        loader = zuul.configloader.ConfigLoader(
            self.sched.connections, self.sched.system, self.sched.zk_client,
            self.sched.globals, self.sched.unparsed_config_cache,
            self.sched.statsd, self.sched)

        log.debug("Loading dynamic layout")

        (trusted_updates, untrusted_updates) = item.includesConfigUpdates()
        build_set = item.current_build_set
        trusted_layout = None
        trusted_errors = False
        untrusted_layout = None
        untrusted_errors = False
        additional_project_branches = self._getProjectBranches(item)
        try:
            # First parse the config as it will land with the
            # full set of config and project repos.  This lets us
            # catch syntax errors in config repos even though we won't
            # actually run with that config.
            files = build_set.getFiles(self.current_context)
            if trusted_updates:
                log.debug("Loading dynamic layout (phase 1)")
                trusted_layout = loader.createDynamicLayout(
                    item,
                    files,
                    additional_project_branches,
                    self.sched.ansible_manager,
                    include_config_projects=True)
                trusted_errors = len(filter_severity(
                    trusted_layout.loading_errors.errors,
                    errors=True, warnings=False)) > 0

            # Then create the config a second time but without changes
            # to config repos so that we actually use this config.
            if untrusted_updates:
                log.debug("Loading dynamic layout (phase 2)")
                untrusted_layout = loader.createDynamicLayout(
                    item,
                    files,
                    additional_project_branches,
                    self.sched.ansible_manager,
                    include_config_projects=False)
                untrusted_errors = len(filter_severity(
                    untrusted_layout.loading_errors.errors,
                    errors=True, warnings=False)) > 0

            # Configuration state handling switchboard. Intentionally verbose
            # and repetetive to be exceptionally clear that we handle all
            # possible cases correctly. Note we never return trusted_layout
            # from a dynamic update.

            # No errors found at all use dynamic untrusted layout
            if (trusted_layout and not trusted_errors and
                    untrusted_layout and not untrusted_errors):
                log.debug("Loading dynamic layout complete")
                return untrusted_layout
            # No errors in untrusted only layout update
            elif (not trusted_layout and
                    untrusted_layout and not untrusted_errors):
                log.debug("Loading dynamic layout complete")
                return untrusted_layout
            # No errors in trusted only layout update
            elif (not untrusted_layout and
                    trusted_layout and not trusted_errors):
                # We're a change to a config repo (with no untrusted
                # config items ahead), so just use the current pipeline
                # layout.
                log.debug("Loading dynamic layout complete")
                return item.manager.tenant.layout
            # Untrusted layout only works with trusted updates
            elif (trusted_layout and not trusted_errors and
                    untrusted_layout and untrusted_errors):
                log.info("Configuration syntax error in dynamic layout")
                # The config is good if we include config-projects,
                # but is currently invalid if we omit them.  Instead
                # of returning the whole error message, just leave a
                # note that the config will work once the dependent
                # changes land.
                msg = "This change depends on a change "\
                      "to a config project.\n\n"
                msg += textwrap.fill(textwrap.dedent("""\
                The syntax of the configuration in this change has
                been verified to be correct once the config project
                change upon which it depends is merged, but it can not
                be used until that occurs."""))
                item.setConfigError(msg)
                return None
            # Untrusted layout is broken and trusted is broken or not set
            elif untrusted_layout and untrusted_errors:
                # Find a layout loading error that match
                # the current item.changes and only report
                # if one is found.
                relevant_errors, severity_error = self._findRelevantErrors(
                    item, untrusted_layout)
                if relevant_errors:
                    item.setConfigErrors(relevant_errors)
                if severity_error:
                    return None
                log.info(
                    "Configuration syntax error not related to "
                    "change context. Error won't be reported.")
                return untrusted_layout
            # Trusted layout is broken
            elif trusted_layout and trusted_errors:
                # Find a layout loading error that match
                # the current item.changes and only report
                # if one is found.
                relevant_errors, severity_error = self._findRelevantErrors(
                    item, trusted_layout)
                if relevant_errors:
                    item.setConfigErrors(relevant_errors)
                if severity_error:
                    return None
                log.info(
                    "Configuration syntax error not related to "
                    "change context. Error won't be reported.")
                # We're a change to a config repo with errors not relevant
                # to this repo. We use the pipeline layout.
                return item.manager.tenant.layout
            else:
                raise Exception("We have reached a configuration error that is"
                                "not accounted for.")

        except Exception:
            log.exception("Error in dynamic layout")
            item.setConfigError("Unknown configuration error")
            return None

    def _getProjectBranches(self, item):
        # Get the branches of the item and all items ahead
        project_branches = {}
        while item:
            for change in item.changes:
                if hasattr(change, 'branch'):
                    this_project_branches = project_branches.setdefault(
                        change.project.canonical_name, set())
                    this_project_branches.add(change.branch)
            item = item.item_ahead
        return project_branches

    def getFallbackLayout(self, item):
        parent_item = item.item_ahead
        if not parent_item:
            return self.tenant.layout

        return self.getLayout(parent_item)

    def getLayout(self, item):
        log = get_annotated_logger(self.log, item.event)
        layout = self._layout_cache.get(item.layout_uuid)
        if layout:
            log.debug("Using cached layout %s for item %s", layout.uuid, item)
            return layout

        if item.layout_uuid:
            log.debug("Re-calculating layout for item %s", item)

        layout = self._getLayout(item)
        if layout:
            item.updateAttributes(self.current_context,
                                  layout_uuid=layout.uuid)
            self._layout_cache[item.layout_uuid] = layout
        return layout

    def _getLayout(self, item):
        log = get_annotated_logger(self.log, item.event)
        if item.item_ahead:
            if (
                (item.item_ahead.live and
                 not item.item_ahead.current_build_set.job_graph) or
                (not item.item_ahead.live and not item.item_ahead.layout_uuid)
            ):
                # We're probably waiting on a merge job for the item ahead.
                return None

        # If the current item does not update the layout, use its parent.
        if not item.updatesConfig():
            return self.getFallbackLayout(item)
        # Else this item updates the config,
        # ask the merger for the result.
        build_set = item.current_build_set
        if build_set.merge_state != build_set.COMPLETE:
            return None
        if build_set.unable_to_merge:
            return self.getFallbackLayout(item)
        if not build_set.hasFiles():
            log.debug("No files on buildset for: %s", item)
            # This is probably post update on an always dynamic
            # branch; in this case the item says it updates the
            # config, but we don't get repo files on branches, so
            # there's nothing to check.
            return self.getFallbackLayout(item)

        log.debug("Preparing dynamic layout for: %s", item)
        start = time.time()
        layout = self._loadDynamicLayout(item)
        self.reportPipelineTiming('layout_generation_time', start)
        return layout

    def _branchesForRepoState(self, projects, tenant, items=None):
        items = items or []
        if all(tenant.getExcludeUnprotectedBranches(project)
               for project in projects):
            branches = set()

            # Add all protected branches of all involved projects
            for project in projects:
                branches.update(
                    tenant.getProjectBranches(project.canonical_name))

            # Additionally add all target branches of all involved items.
            branches.update(change.branch for item in items
                            for change in item.changes
                            if hasattr(change, 'branch'))

            # Make sure override-checkout targets are part of the repo state
            for item in items:
                if not item.current_build_set.job_graph:
                    continue

                for job in item.current_build_set.job_graph.getJobs():
                    if job.override_checkout:
                        branches.add(job.override_checkout)

                    for p in job.required_projects.values():
                        if p.override_checkout:
                            branches.add(p.override_checkout)

            branches = list(branches)
        else:
            branches = None
        return branches

    def scheduleMerge(self, item, files=None, dirs=None):
        log = item.annotateLogger(self.log)
        build_set = item.current_build_set
        log.debug("Scheduling merge for item %s (files: %s, dirs: %s)" %
                  (item, files, dirs))

        # If the involved projects exclude unprotected branches we should also
        # exclude them from the merge and repo state except the branch of the
        # change that is tested.
        tenant = self.tenant
        items = list(item.items_ahead) + [item]
        projects = {
            change.project for i in items for change in i.changes
            if tenant.getProject(change.project.canonical_name)[1]
        }
        branches = self._branchesForRepoState(projects=projects, tenant=tenant,
                                              items=items)

        # Using the first change as representative of whether this
        # pipeline is handling changes or refs
        if isinstance(item.changes[0], model.Change):
            self.sched.merger.mergeChanges(build_set.merger_items,
                                           build_set, files, dirs,
                                           precedence=self.pipeline.precedence,
                                           event=item.event,
                                           branches=branches)
        else:
            self.sched.merger.getRepoState(build_set.merger_items,
                                           build_set,
                                           precedence=self.pipeline.precedence,
                                           event=item.event,
                                           branches=branches)
        build_set.updateAttributes(self.current_context,
                                   merge_state=build_set.PENDING)
        return False

    def scheduleFilesChanges(self, item):
        log = item.annotateLogger(self.log)
        log.debug("Scheduling fileschanged for item %s", item)
        build_set = item.current_build_set
        self.sched.merger.getFilesChanges(item.changes, build_set=build_set,
                                          event=item.event)
        build_set.updateAttributes(self.current_context,
                                   files_state=build_set.PENDING)
        return False

    def scheduleGlobalRepoState(self, item):
        log = item.annotateLogger(self.log)

        tenant = self.tenant
        jobs = item.current_build_set.job_graph.getJobs()
        project_cnames = set()
        for job in jobs:
            log.debug('Processing job %s', job.name)
            project_cnames.update(job.affected_projects)
            log.debug('Needed projects: %s', project_cnames)

        # Filter projects for ones that are already in repo state
        connections = self.sched.connections.connections

        new_items = list()
        build_set = item.current_build_set

        # If we skipped the initial repo state (for branch/ref items),
        # we need to include the merger items for the final repo state.
        if not build_set.repo_state_keys:
            new_items.extend(build_set.merger_items)
        else:
            for merger_item in item.current_build_set.merger_items:
                canonical_hostname = connections[
                    merger_item['connection']].canonical_hostname
                canonical_project_name = (canonical_hostname + '/' +
                                          merger_item['project'])
                project_cnames.discard(canonical_project_name)

        if not project_cnames:
            item.current_build_set.updateAttributes(
                self.current_context,
                repo_state_state=item.current_build_set.COMPLETE)
            return True

        log.info('Scheduling global repo state for item %s', item)
        # At this point we know we're going to request a merge job;
        # set the waiting state on all the item's jobs so users know
        # what we're waiting on.
        for job in jobs:
            job.setWaitingStatus('repo state')

        projects = []
        for project_cname in project_cnames:
            projects.append(tenant.getProject(project_cname)[1])

        branches = self._branchesForRepoState(
            projects=projects, tenant=tenant, items=[item])

        for project in projects:
            new_item = dict()
            new_item['project'] = project.name
            new_item['connection'] = project.connection_name
            new_items.append(new_item)

        # Get state for not yet tracked projects
        self.sched.merger.getRepoState(items=new_items,
                                       build_set=item.current_build_set,
                                       event=item.event,
                                       branches=branches)
        item.current_build_set.updateAttributes(
            self.current_context,
            repo_state_request_time=time.time(),
            repo_state_state=item.current_build_set.PENDING)
        return True

    def prepareItem(self, item):
        build_set = item.current_build_set
        tenant = self.tenant
        # We always need to set the configuration of the item if it
        # isn't already set.
        if not build_set.ref:
            build_set.setConfiguration(self.current_context)

        # Next, if a change ahead has a broken config, then so does
        # this one.  Record that and don't do anything else.
        if (item.item_ahead and item.item_ahead.current_build_set and
            item.item_ahead.current_build_set.has_blocking_errors):
            msg = "This change depends on a change "\
                  "with an invalid configuration.\n"
            item.setConfigError(msg)
            # Find our layout since the reporter will need it to
            # determine if the project is in the pipeline.
            self.getLayout(item)
            return False

        # The next section starts between 0 and 2 remote merger
        # operations in parallel as needed.
        ready = True
        # If the project is in this tenant, fetch missing files so we
        # know if it updates the config.
        in_tenant = False
        for c in item.changes:
            if tenant.project_configs.get(c.project.canonical_name):
                in_tenant = True
            break
        if in_tenant:
            if build_set.files_state == build_set.NEW:
                ready = self.scheduleFilesChanges(item)
            if build_set.files_state == build_set.PENDING:
                ready = False
        # If this change alters config or is live, schedule merge and
        # build a layout.
        if build_set.merge_state == build_set.NEW:
            if item.live or item.updatesConfig():
                # Collect extra config files and dirs of required changes.
                extra_config_files = set()
                extra_config_dirs = set()
                for merger_item in item.current_build_set.merger_items:
                    source = self.sched.connections.getSource(
                        merger_item["connection"])
                    project = source.getProject(merger_item["project"])
                    tpc = tenant.project_configs.get(project.canonical_name)
                    if tpc:
                        extra_config_files.update(tpc.extra_config_files)
                        extra_config_dirs.update(tpc.extra_config_dirs)

                ready = self.scheduleMerge(
                    item,
                    files=(['zuul.yaml', '.zuul.yaml'] +
                           list(extra_config_files)),
                    dirs=(['zuul.d', '.zuul.d'] +
                          list(extra_config_dirs)))
        if build_set.merge_state == build_set.PENDING:
            ready = False

        # If a merger op is outstanding, we're not ready.
        if not ready:
            return False

        # If the change can not be merged or has config errors, don't
        # run jobs.
        if build_set.unable_to_merge or build_set.has_blocking_errors:
            # Find our layout since the reporter will need it to
            # determine if the project is in the pipeline.
            self.getLayout(item)
            return False

        # With the merges done, we have the info needed to get a
        # layout.  This may return the pipeline layout, a layout from
        # a change ahead, a newly generated layout for this change, or
        # None if there was an error that makes the layout unusable.
        # In the last case, it will have set the config_errors on this
        # item, which may be picked up by the next item.
        if not (item.layout_uuid or item.current_build_set.job_graph):
            layout = self.getLayout(item)
            if not layout:
                return False

        # We don't need to build a job graph for a non-live item, we
        # just need the layout.
        if not item.live:
            return False

        # At this point we have a layout for the item, and it's live,
        # so freeze the job graph.
        log = item.annotateLogger(self.log)
        if not item.current_build_set.job_graph:
            try:
                log.debug("Freezing job graph for %s" % (item,))
                start = time.time()
                item.freezeJobGraph(self.getLayout(item),
                                    self.current_context,
                                    skip_file_matcher=False,
                                    redact_secrets_and_keys=False)
                self.reportPipelineTiming('job_freeze_time', start)
            except Exception as e:
                # These errors do not warrant a traceback.
                if isinstance(e, model.JobConfigurationError):
                    log.info("Error freezing job graph for %s", item)
                else:
                    log.exception("Error freezing job graph for %s", item)
                item.setConfigError("Unable to freeze job graph: %s" %
                                    (str(e)))
                return False
            if (item.current_build_set.job_graph and
                len(item.current_build_set.job_graph.job_uuids) > 0):
                self.sql.reportBuildsetStart(build_set)

        # At this point we know all frozen jobs and their repos so update the
        # repo state with all missing repos.
        if build_set.repo_state_state == build_set.NEW:
            self.scheduleGlobalRepoState(item)
        if build_set.repo_state_state == build_set.PENDING:
            return False

        return True

    def _processOneItem(self, item, nnfi):
        log = item.annotateLogger(self.log)
        changed = False
        ready = False
        dequeued = False
        failing_reasons = []  # Reasons this item is failing

        item_ahead = item.item_ahead
        if item_ahead and (not item_ahead.live):
            item_ahead = None
        change_queue = item.queue

        meets_reqs = self.areChangesReadyToBeEnqueued(item.changes, item.event)
        dependency_graph = collections.OrderedDict()
        try:
            self.getDependencyGraph(item.changes[0], dependency_graph,
                                    item.event, quiet=True)
        except exceptions.DependencyLimitExceededError:
            self.removeItem(item)
            return True, nnfi

        # Verify that the cycle dependency graph is correct
        cycle = self.cycleForChange(
            item.changes[0], dependency_graph, item.event, debug=False)
        cycle = set(cycle or [item.changes[0]])
        item_cycle = set(item.changes)
        # We don't consider merged dependencies when building the
        # dependency graph, so we need to ignore differences resulting
        # from changes that have been merged in the meantime.  Put any
        # missing merged changes back in the cycle for comparison
        # purposes.
        merged_changes = set(c for c in (item_cycle - cycle) if c.is_merged)
        cycle |= merged_changes
        if cycle != item_cycle:
            log.info("Item cycle has changed: %s, now: %s, was: %s", item,
                     cycle, item_cycle)
            self.removeItem(item)
            if item.live:
                self.reEnqueueChanges(item, item.changes)
            return (True, nnfi)

        abort, needs_changes = self.getMissingNeededChanges(
            item.changes, change_queue, item.event,
            dependency_graph=dependency_graph,
            item=item)
        if not (meets_reqs and not needs_changes):
            # It's not okay to enqueue this change, we should remove it.
            log.info("Dequeuing %s because "
                     "it can no longer merge" % item)
            self.cancelJobs(item)
            if not meets_reqs:
                item.setDequeuedMissingRequirements()
            else:
                clist = ', '.join([c.url for c in needs_changes])
                if len(needs_changes) > 1:
                    msg = f'Changes {clist} are needed.'
                else:
                    msg = f'Change {clist} is needed.'
                item.setDequeuedNeedingChange(msg)
            if item.live:
                try:
                    self.reportItem(item)
                except exceptions.MergeFailure:
                    pass
            self.dequeueItem(item)
            return (True, nnfi)

        actionable = change_queue.isActionable(item)
        item.updateAttributes(self.current_context, active=actionable)

        dep_items = self.getFailingDependentItems(item)
        if dep_items:
            failing_reasons.append('a needed change is failing')
            self.cancelJobs(item, prime=False)
        else:
            item_ahead_merged = (item_ahead.areAllChangesMerged()
                                 if item_ahead else False)
            if (item_ahead != nnfi and not item_ahead_merged):
                # Our current base is different than what we expected,
                # and it's not because our current base merged.  Something
                # ahead must have failed.
                log.info("Resetting builds for changes %s because the "
                         "item ahead, %s, is not the nearest non-failing "
                         "item, %s" % (item.changes, item_ahead, nnfi))
                change_queue.moveItem(item, nnfi)
                changed = True
                self.cancelJobs(item)
            if actionable:
                ready = self.prepareItem(item)
                # Starting jobs reporting should only be done once if there are
                # jobs to run for this item.
                if (ready
                    and len(self.pipeline.start_actions) > 0
                    and len(item.current_build_set.job_graph.job_uuids) > 0
                    and not item.reported_start
                    and not item.quiet
                    ):
                    self.reportStart(item)
                if item.current_build_set.unable_to_merge:
                    failing_reasons.append("it has a merge conflict")
                if item.current_build_set.has_blocking_errors:
                    failing_reasons.append("it has an invalid configuration")
                if ready and self.provisionNodes(item):
                    changed = True
        if ready and self.executeJobs(item):
            changed = True

        if item.hasAnyJobFailed():
            failing_reasons.append("at least one job failed")
        if (not item.live) and (not item.items_behind) and (not dequeued):
            failing_reasons.append("is a non-live item with no items behind")
            self.dequeueItem(item)
            changed = dequeued = True

        can_report = not item_ahead and item.areAllJobsComplete() and item.live
        is_cycle = len(item.changes) > 1
        if can_report and is_cycle:
            # Before starting to merge the cycle items, make sure they
            # can still be merged, to reduce the chance of a partial merge.
            non_mergeable_cycle_changes = self.getNonMergeableCycleChanges(
                item)
            if non_mergeable_cycle_changes:
                clist = ', '.join([
                    c.url for c in non_mergeable_cycle_changes])
                if len(non_mergeable_cycle_changes) > 1:
                    msg = f'Changes {clist} can not be merged.'
                else:
                    msg = f'Change {clist} can not be merged.'
                item.setDequeuedNeedingChange(msg)
                failing_reasons.append("cycle can not be merged")
                log.debug(
                    "Dequeuing item %s because cycle can no longer merge",
                    item
                )
                try:
                    self.reportItem(item)
                except exceptions.MergeFailure:
                    pass
                for item_behind in item.items_behind:
                    log.info("Resetting builds for %s because the "
                             "item ahead, %s, can not be merged" %
                             (item_behind, item))
                    self.cancelJobs(item_behind)
                self.dequeueItem(item)
                return (True, nnfi)

        if can_report:
            succeeded = item.didAllJobsSucceed()
            try:
                self.reportItem(item)
            except exceptions.MergeFailure:
                failing_reasons.append("it did not merge")
                for item_behind in item.items_behind:
                    log.info("Resetting builds for %s because the "
                             "item ahead, %s, failed to merge" %
                             (item_behind, item))
                    self.cancelJobs(item_behind)
                # Only re-report items in the cycle when we encounter a merge
                # failure for a successful cycle.
                if is_cycle and succeeded:
                    self.sendReport(self.pipeline.failure_actions, item)
            self.dequeueItem(item)
            changed = dequeued = True
        elif not failing_reasons and item.live:
            nnfi = item
        if not dequeued:
            item.current_build_set.updateAttributes(
                self.current_context, failing_reasons=failing_reasons)
        if failing_reasons:
            log.debug("%s is a failing item because %s" %
                      (item, failing_reasons))
        if (item.live and not dequeued
                and self.sched.globals.use_relative_priority):
            for job, request_id in item.current_build_set.getNodeRequests():
                nodeset = item.current_build_set.getJobNodeSetInfo(job)
                if nodeset is not None:
                    # No need to revise a completed request
                    continue
                if self.sched.nodepool.isNodeRequestID(request_id):
                    self._reviseNodeRequest(request_id, item, job)
                else:
                    self._reviseNodesetRequest(request_id, item, job)
        return (changed, nnfi)

    def _reviseNodeRequest(self, request_id, item, job):
        node_request = self.sched.nodepool.zk_nodepool.getNodeRequest(
            request_id, cached=True)
        if not node_request:
            return
        if node_request.state != model.STATE_REQUESTED:
            # If the node request was locked and accepted by a
            # provider, we can no longer update the relative priority.
            return
        priority = self.getNodePriority(
            item, item.getChangeForJob(job))
        if node_request.relative_priority != priority:
            self.sched.nodepool.reviseRequest(node_request, priority)

    def _reviseNodesetRequest(self, request_id, item, job):
        request = self.sched.launcher.getRequest(request_id)
        if not request:
            return
        if request.state not in request.REVISE_STATES:
            # If the nodeset request was accepted by a launcher,
            # we can no longer update the relative priority.
            return
        relative_priority = self.getNodePriority(
            item, item.getChangeForJob(job))
        if request.relative_priority != relative_priority:
            self.sched.launcher.reviseRequest(request, relative_priority)

    def processQueue(self, tenant_lock):
        # Do whatever needs to be done for each change in the queue
        self.log.debug("Starting queue processor: %s" % self.pipeline.name)
        changed = False
        change_keys = set()
        for queue in self.state.queues[:]:
            queue_changed = False
            nnfi = None  # Nearest non-failing item
            for item in queue.queue[:]:
                self.sched.abortIfPendingReconfig(tenant_lock)
                item_changed, nnfi = self._processOneItem(
                    item, nnfi)
                if item_changed:
                    queue_changed = True
                self.reportStats(item)
                for change in item.changes:
                    change_keys.add(change.cache_stat.key)
            if queue_changed:
                changed = True
                status = ''
                for item in queue.queue:
                    status += item.formatStatus()
                if status:
                    self.log.debug("Queue %s status is now:\n %s" %
                                   (queue.name, status))

        self.change_list.setChangeKeys(self.current_context, change_keys)
        self._maintainCache()
        self.log.debug("Finished queue processor: %s (changed: %s)" %
                       (self.pipeline.name, changed))
        return changed

    def onBuildStarted(self, build):
        log = get_annotated_logger(self.log, build.zuul_event_id)
        log.debug("Build %s started", build)
        self.sql.reportBuildStart(build)
        self.reportPipelineTiming('job_wait_time',
                                  build.execute_time, build.start_time)
        if not build.build_set.item.first_job_start_time:
            # Only report this for the first job in a queue item so
            # that we don't include gate resets.
            build.build_set.item.updateAttributes(
                self.current_context,
                first_job_start_time=build.start_time)
            self.reportPipelineTiming('event_job_time',
                                      build.build_set.item.event.timestamp,
                                      build.start_time)
        return True

    def onBuildPaused(self, build):
        log = get_annotated_logger(self.log, build.zuul_event_id)
        item = build.build_set.item
        log.debug("Build %s of %s paused", build, item)
        item.setResult(build)

        # We need to resume builds because we could either have no children
        # or have children that are already skipped.
        self._resumeBuilds(build.build_set)
        return True

    def _resumeBuilds(self, build_set):
        """
        Resumes all paused builds of a buildset that may be resumed.
        """
        for build in build_set.getBuilds():
            if not build.paused:
                continue
            # check if all child jobs are finished
            child_builds = []
            item = build.build_set.item
            job_graph = item.current_build_set.job_graph
            child_builds += [
                item.current_build_set.getBuild(x)
                for x in job_graph.getDependentJobsRecursively(
                    build.job)]
            all_completed = True
            for child_build in child_builds:
                if not child_build or not child_build.result:
                    all_completed = False
                    break

            if all_completed:
                resume_time = time.time()
                self.sched.executor.resumeBuild(build)
                with build.activeContext(self.current_context):
                    build.paused = False
                    build.addEvent(
                        model.BuildEvent(
                            event_time=resume_time,
                            event_type=model.BuildEvent.TYPE_RESUMED))

    def _resetDependentBuilds(self, build_set, build):
        job_graph = build_set.job_graph

        for job in job_graph.getDependentJobsRecursively(build.job):
            self.sched.cancelJob(build_set, job)
            build = build_set.getBuild(job)
            if build:
                build_set.removeBuild(build)

        # Re-set build results in case we resetted builds that were skipped
        # not by this build/
        for build in build_set.getBuilds():
            if build.result:
                build_set.item.setResult(build)

    def _cancelRunningBuilds(self, build_set):
        item = build_set.item
        for job in item.getJobs():
            build = build_set.getBuild(job)
            if not build or not build.result:
                self.sched.cancelJob(build_set, job, final=True)

    def onBuildCompleted(self, build):
        log = get_annotated_logger(self.log, build.zuul_event_id)
        item = build.build_set.item

        log.debug("Build %s of %s completed", build, item)

        if COMPONENT_REGISTRY.model_api >= 33:
            event_queue = self.sched.management_events[self.tenant.name]
        else:
            event_queue = self.sched.pipeline_result_events[
                self.tenant.name][self.pipeline.name]
        self.tenant.semaphore_handler.release(
            event_queue, item, build.job)

        if item.getJob(build.job.uuid) is None:
            log.info("Build %s no longer in job graph for item %s",
                     build, item)
            return

        item.setResult(build)
        log.debug("Item %s status is now:\n %s", item, item.formatStatus())
        build_set = build.build_set

        if build.retry:
            if build.unreachable and build.job.incrementNodesetIndex():
                # If we are retrying a build because it encountered an
                # unreachable host, increment the nodeset.
                log.info("Next node request for retried build of job "
                         f"{build.job.name} of item {build_set.item} "
                         f"with nodeset alternative {build.job.nodeset_index}")

            if build_set.getJobNodeSetInfo(build.job):
                build_set.removeJobNodeSetInfo(build.job)

            # in case this was a paused build we need to retry all
            # child jobs
            self._resetDependentBuilds(build_set, build)

        self._resumeBuilds(build_set)

        if (build_set.fail_fast and
            build.failed and build.job.voting and not build.retry):
            # If fail-fast is set and the build is not successful
            # cancel all remaining jobs.
            log.debug("Build %s failed and fail-fast enabled, canceling "
                      "running builds", build)
            self._cancelRunningBuilds(build_set)

        return True

    def onFilesChangesCompleted(self, event, build_set):
        item = build_set.item
        for i, change in enumerate(item.changes):
            source = self.sched.connections.getSource(
                change.project.connection_name)
            if event.files is None:
                self.log.warning(
                    "Did not receive expected merge files content")
                files = None
            else:
                files = event.files[i]
            source.setChangeAttributes(change, files=files)
        build_set.updateAttributes(self.current_context,
                                   files_state=build_set.COMPLETE)
        if build_set.merge_state == build_set.COMPLETE:
            # We're the second of the files/merger pair, report the stat
            self.reportPipelineTiming('merge_request_time',
                                      build_set.configured_time)
        if event.elapsed_time:
            self.reportPipelineTiming('merger_files_changes_op_time',
                                      event.elapsed_time, elapsed=True)

    def onMergeCompleted(self, event, build_set):
        if build_set.merge_state == build_set.COMPLETE:
            self._onGlobalRepoStateCompleted(event, build_set)
            self.reportPipelineTiming('repo_state_time',
                                      build_set.repo_state_request_time)
            if event.elapsed_time:
                self.reportPipelineTiming('merger_repo_state_op_time',
                                          event.elapsed_time, elapsed=True)
        else:
            self._onMergeCompleted(event, build_set)
            if build_set.files_state == build_set.COMPLETE:
                # We're the second of the files/merger pair, report the stat
                self.reportPipelineTiming('merge_request_time',
                                          build_set.configured_time)
            if event.elapsed_time:
                self.reportPipelineTiming('merger_merge_op_time',
                                          event.elapsed_time, elapsed=True)

    def _onMergeCompleted(self, event, build_set):
        item = build_set.item
        log = get_annotated_logger(self.log, item.event)
        if isinstance(item.changes[0], model.Tag):
            # Since this is only used for Tag items, we know that
            # circular dependencies are not in play and there is only
            # one change in the list of changes.
            if len(item.changes) > 1:
                raise Exception("Tag item with more than one change")
            change = item.changes[0]
            source = self.sched.connections.getSource(
                change.project.connection_name)
            if not event.item_in_branches:
                log.warning("Tagged commit is not part of any included "
                            "branch. No jobs will run.")
            source.setChangeAttributes(
                change, containing_branches=event.item_in_branches)
        with build_set.activeContext(self.current_context):
            build_set.setMergeRepoState(event.repo_state)
            build_set.merge_state = build_set.COMPLETE
            if event.merged:
                build_set.setFiles(event.files)
        if not (event.merged or event.updated):
            log.info("Unable to merge %s" % item)
            item.setUnableToMerge(event.errors)

    def _onGlobalRepoStateCompleted(self, event, build_set):
        item = build_set.item
        if not event.updated:
            self.log.info("Unable to get global repo state for %s", item)
            item.setUnableToMerge(event.errors)
        else:
            self.log.info("Received global repo state for %s", item)
            with build_set.activeContext(self.current_context):
                build_set.setExtraRepoState(event.repo_state)
                build_set.repo_state_state = build_set.COMPLETE

    def _handleNodeRequestFallback(self, log, build_set, job, old_request):
        if not job.incrementNodesetIndex():
            # No alternatives to fall back upon
            return False

        log.info("Re-attempting node request for job "
                 f"{job.name} of item {build_set.item} "
                 f"with nodeset alternative {job.nodeset_index}")

        build_set.removeJobNodeRequestID(job)

        # Make a new request
        if self.sched.globals.use_relative_priority:
            relative_priority = self.getNodePriority(
                build_set.item,
                build_set.item.getChangeForJob(job))
        else:
            relative_priority = 0
        if self._useNodepoolFallback(log, job):
            self._makeNodepoolRequest(
                log, build_set, job, relative_priority)
        else:
            self._makeLauncherRequest(
                log, build_set, job, relative_priority)
        return True

    def onNodesProvisioned(self, request, nodeset_info, build_set):
        log = get_annotated_logger(self.log, request)

        self.reportPipelineTiming('node_request_time', request.created_time)
        job = build_set.item.getJob(request.job_uuid)
        # First see if we need to retry the request
        if not request.fulfilled:
            log.info("Node(set) request %s: failure for %s",
                     request, job.name)
            if self._handleNodeRequestFallback(log, build_set, job, request):
                return
        # No more fallbacks -- tell the buildset the request is complete
        build_set.setJobNodeSetInfo(job, nodeset_info)
        # Put a fake build through the cycle to clean it up.
        if not request.fulfilled:
            build_set.item.setNodeRequestFailure(
                job, f'Node(set) request {request.id} failed')
            self._resumeBuilds(build_set)
            pipeline = self.pipeline
            tenant = self.tenant
            if COMPONENT_REGISTRY.model_api >= 33:
                event_queue = self.sched.management_events[tenant.name]
            else:
                event_queue = self.sched.pipeline_result_events[
                    tenant.name][pipeline.name]
            tenant.semaphore_handler.release(
                event_queue, build_set.item, job)

            if build_set.fail_fast and job.voting:
                # If fail-fast is set and the node(set) request is not
                # successful cancel all remaining jobs.
                log.debug("Node(set) request %s failed and fail-fast enabled, "
                          "canceling running builds", request.id)
                self._cancelRunningBuilds(build_set)

        log.info("Completed node(set) request %s for job %s of item %s "
                 "with nodes %s",
                 request, job.name, build_set.item, request.nodes)

    def reportItem(self, item):
        log = get_annotated_logger(self.log, item.event)
        action = None

        phase1 = True
        phase2 = True
        is_cycle = len(item.changes) > 1
        succeeded = item.didAllJobsSucceed()
        already_reported = item.reported
        if (self.changes_merge
            and is_cycle
            and succeeded):
            phase2 = False
        if not already_reported:
            action, reported = self._reportItem(
                item, phase1=phase1, phase2=phase2)
            if phase2 is False:
                phase1 = False
                phase2 = True
                action, reported = self._reportItem(
                    item, phase1=phase1, phase2=phase2)
            item.updateAttributes(self.current_context,
                                  reported=reported)

        if not phase2:
            return
        if self.changes_merge:
            merged = item.reported
            if merged:
                for change in item.changes:
                    source = change.project.source
                    merged = source.isMerged(change, change.branch)
                    if not merged:
                        break
            if action:
                if action == 'success' and not merged:
                    log.debug("Overriding result for %s to merge failure",
                              item)
                    action = 'merge-failure'
                    item.setReportedResult('MERGE_FAILURE')
                self.reportNormalBuildsetEnd(item.current_build_set,
                                             action, final=True)
            change_queue = item.queue
            if not (succeeded and merged):
                if (not item.current_build_set.job_graph or
                    not item.current_build_set.job_graph.job_uuids):
                    error_reason = "did not have any jobs configured"
                elif not succeeded:
                    error_reason = "failed tests"
                else:
                    error_reason = "failed to merge"
                log.info("Changes for %s did not merge because it %s, "
                         "status: all-succeeded: %s, merged: %s",
                         item, error_reason, succeeded, merged)
                if not succeeded:
                    change_queue.decreaseWindowSize()
                    log.debug("%s window size decreased to %s",
                              change_queue, change_queue.window)
                raise exceptions.MergeFailure(
                    "Changes for %s failed to merge" % item)
            else:
                self.reportNormalBuildsetEnd(item.current_build_set,
                                             action, final=True)
                log.info("Reported %s status: all-succeeded: %s, "
                         "merged: %s", item, succeeded, merged)
                change_queue.increaseWindowSize()
                log.debug("%s window size increased to %s",
                          change_queue, change_queue.window)

                zuul_driver = self.sched.connections.drivers['zuul']
                tenant = self.tenant
                with trace.use_span(tracing.restoreSpan(item.span_info)):
                    for change in item.changes:
                        source = change.project.source
                        zuul_driver.onChangeMerged(tenant, change, source)
        elif action:
            self.reportNormalBuildsetEnd(item.current_build_set,
                                         action, final=True)

    def _reportItem(self, item, phase1, phase2):
        log = get_annotated_logger(self.log, item.event)
        log.debug("Reporting phase1: %s phase2: %s item: %s",
                  phase1, phase2, item)
        ret = True  # Means error as returned by trigger.report

        # In the case of failure, we may not have completed an initial
        # merge which would get the layout for this item, so in order
        # to determine whether this item's project is in this
        # pipeline, use the dynamic layout if available, otherwise,
        # fall back to the current static layout as a best
        # approximation.  However, if we ran jobs, then we obviously
        # were in the pipeline config.
        project_in_pipeline = bool(item.getJobs())

        if not project_in_pipeline:
            layout = None
            if item.layout_uuid:
                layout = self.getLayout(item)
            if not layout:
                layout = self.tenant.layout

            try:
                for change in item.changes:
                    try:
                        project_in_pipeline = bool(
                            layout.getProjectPipelineConfig(item, change))
                    except model.TemplateNotFoundError:
                        # If one of the changes is involved in a
                        # config error, then the item can not be in
                        # the pipeline.
                        project_in_pipeline = False
                    if project_in_pipeline:
                        break
            except Exception:
                log.exception("Invalid config for %s", item)
        if not project_in_pipeline:
            log.debug("Project not in pipeline %s for %s",
                      self.pipeline, item)
            project_in_pipeline = False
            action = 'no-jobs'
            actions = self.pipeline.no_jobs_actions
            item.setReportedResult('NO_JOBS')
        elif item.current_build_set.has_blocking_errors:
            log.debug("Invalid config for %s", item)
            action = 'config-error'
            actions = self.pipeline.config_error_actions
            item.setReportedResult('CONFIG_ERROR')
        elif item.didMergerFail():
            log.debug("Merge conflict")
            action = 'merge-conflict'
            actions = self.pipeline.merge_conflict_actions
            item.setReportedResult('MERGE_CONFLICT')
        elif item.wasDequeuedNeedingChange():
            log.debug("Dequeued needing change")
            action = 'failure'
            actions = self.pipeline.failure_actions
            item.setReportedResult('FAILURE')
        elif item.wasDequeuedMissingRequirements():
            log.debug("Dequeued missing merge requirements")
            action = 'failure'
            actions = self.pipeline.failure_actions
            item.setReportedResult('FAILURE')
        elif not item.getJobs():
            # We don't send empty reports with +1
            log.debug("No jobs for %s", item)
            action = 'no-jobs'
            actions = self.pipeline.no_jobs_actions
            item.setReportedResult('NO_JOBS')
        elif item.didAllJobsSucceed():
            log.debug("success %s", self.pipeline.success_actions)
            action = 'success'
            actions = self.pipeline.success_actions
            item.setReportedResult('SUCCESS')
            with self.state.activeContext(self.current_context):
                self.state.consecutive_failures = 0
        else:
            action = 'failure'
            actions = self.pipeline.failure_actions
            item.setReportedResult('FAILURE')
            with self.state.activeContext(self.current_context):
                self.state.consecutive_failures += 1
        if project_in_pipeline and self.state.disabled:
            actions = self.pipeline.disabled_actions
        # Check here if we should disable so that we only use the disabled
        # reporters /after/ the last disable_at failure is still reported as
        # normal.
        if (self.pipeline.disable_at and not self.state.disabled and
            self.state.consecutive_failures
                >= self.pipeline.disable_at):
            self.state.updateAttributes(
                self.current_context, disabled=True)
        if actions:
            log.info("Reporting item %s, actions: %s", item, actions)
            ret = self.sendReport(actions, item, phase1, phase2)
            if ret:
                log.error("Reporting item %s received: %s", item, ret)
        return action, (not ret)

    def reportStats(self, item, trigger_event=None):
        if not self.sched.statsd:
            return
        try:
            # Update the gauge on enqueue and dequeue, but timers only
            # when dequeing.
            if item.dequeue_time:
                dt = (item.dequeue_time - item.enqueue_time) * 1000
                item_changes = len(item.changes)
            else:
                dt = None
                item_changes = 0
            changes = sum(len(i.changes)
                          for i in self.state.getAllItems())
            # TODO(jeblair): add items keys like changes

            tenant = self.tenant
            basekey = 'zuul.tenant.%s' % tenant.name
            key = '%s.pipeline.%s' % (basekey, self.pipeline.name)
            # stats.timers.zuul.tenant.<tenant>.pipeline.<pipeline>.resident_time
            # stats_counts.zuul.tenant.<tenant>.pipeline.<pipeline>.total_changes
            # stats.gauges.zuul.tenant.<tenant>.pipeline.<pipeline>.current_changes
            # stats.gauges.zuul.tenant.<tenant>.pipeline.<pipeline>.window
            self.sched.statsd.gauge(key + '.current_changes', changes)
            self.sched.statsd.gauge(key + '.window', self.pipeline.window)
            if dt:
                self.sched.statsd.timing(key + '.resident_time', dt)
                self.sched.statsd.incr(key + '.total_changes', item_changes)
            if item.queue and item.queue.name:
                queuename = (item.queue.name.
                             replace('.', '_').replace('/', '.'))
                # stats.gauges.zuul.tenant.<tenant>.pipeline.<pipeline>.queue.<queue>.resident_time
                # stats.gauges.zuul.tenant.<tenant>.pipeline.<pipeline>.queue.<queue>.total_changes
                # stats.gauges.zuul.tenant.<tenant>.pipeline.<pipeline>.queue.<queue>.current_changes
                # stats.gauges.zuul.tenant.<tenant>.pipeline.<pipeline>.queue.<queue>.window
                queuekey = f'{key}.queue.{queuename}'

                # Handle per-branch queues
                layout = self.tenant.layout
                queue_config = layout.queues.get(item.queue.name)
                per_branch = (
                    queue_config and
                    queue_config.type == queue_config.Type.PER_BRANCH
                )
                if per_branch and item.queue.project_branches:
                    # Get the first project-branch of this queue,
                    # which is a tuple of project, branch, and get
                    # second item of that tuple, the branch name.  In
                    # a per-branch queue, we expect the branch name to
                    # be the same for every project.
                    branch = item.queue.project_branches[0][1]
                    if branch:
                        branch = branch.replace('.', '_').replace('/', '.')
                        queuekey = f'{queuekey}.branch.{branch}'

                queue_changes = sum(len(i.changes) for i in item.queue.queue)
                self.sched.statsd.gauge(queuekey + '.current_changes',
                                        queue_changes)
                self.sched.statsd.gauge(queuekey + '.window',
                                        item.queue.window)
                if dt:
                    self.sched.statsd.timing(queuekey + '.resident_time', dt)
                    self.sched.statsd.incr(queuekey + '.total_changes',
                                           item_changes)
            for change in item.changes:
                if hasattr(change, 'branch'):
                    hostname = (change.project.canonical_hostname.
                                replace('.', '_'))
                    projectname = (change.project.name.
                                   replace('.', '_').replace('/', '.'))
                    projectname = projectname.replace('.', '_').replace(
                        '/', '.')
                    branchname = change.branch.replace('.', '_').replace(
                        '/', '.')
                    # stats.timers.zuul.tenant.<tenant>.pipeline.<pipeline>.
                    #   project.<host>.<project>.<branch>.resident_time
                    # stats_counts.zuul.tenant.<tenant>.pipeline.<pipeline>.
                    #   project.<host>.<project>.<branch>.total_changes
                    key += '.project.%s.%s.%s' % (hostname, projectname,
                                                  branchname)
                    if dt:
                        self.sched.statsd.timing(key + '.resident_time', dt)
                        self.sched.statsd.incr(key + '.total_changes')
            if (
                trigger_event
                and hasattr(trigger_event, 'arrived_at_scheduler_timestamp')
            ):
                now = time.time()
                arrived = trigger_event.arrived_at_scheduler_timestamp
                processing = (now - arrived) * 1000
                elapsed = (now - trigger_event.timestamp) * 1000
                self.sched.statsd.timing(
                    basekey + '.event_enqueue_processing_time',
                    processing)
                self.sched.statsd.timing(
                    basekey + '.event_enqueue_time', elapsed)
                self.reportPipelineTiming('event_enqueue_time',
                                          trigger_event.timestamp)
        except Exception:
            self.log.exception("Exception reporting pipeline stats")

    def reportPipelineTiming(self, key, start, end=None, elapsed=False):
        if not self.sched.statsd:
            return
        if not start:
            return
        if end is None:
            end = time.time()
        pipeline = self.pipeline
        tenant = self.tenant
        stats_key = f'zuul.tenant.{tenant.name}.pipeline.{pipeline.name}'
        if elapsed:
            dt = start
        else:
            dt = (end - start) * 1000
        self.sched.statsd.timing(f'{stats_key}.{key}', dt)

    def formatStatusJSON(self, websocket_url=None):
        j_pipeline = dict(name=self.pipeline.name,
                          description=self.pipeline.description,
                          state=self.state.state,
                          manager=self.type)
        j_pipeline['triggers'] = [
            {'driver': t.driver.name} for t in self.pipeline.triggers
        ]
        j_queues = []
        j_pipeline['change_queues'] = j_queues
        for queue in self.state.queues:
            if not queue.queue:
                continue
            j_queue = dict(name=queue.name)
            j_queues.append(j_queue)
            j_queue['heads'] = []
            j_queue['window'] = queue.window

            if queue.project_branches and queue.project_branches[0][1]:
                j_queue['branch'] = queue.project_branches[0][1]
            else:
                j_queue['branch'] = None

            j_changes = []
            for e in queue.queue:
                if not e.item_ahead:
                    if j_changes:
                        j_queue['heads'].append(j_changes)
                    j_changes = []
                j_changes.append(e.formatJSON(websocket_url))
                if (len(j_changes) > 1 and
                        (j_changes[-2]['remaining_time'] is not None) and
                        (j_changes[-1]['remaining_time'] is not None)):
                    j_changes[-1]['remaining_time'] = max(
                        j_changes[-2]['remaining_time'],
                        j_changes[-1]['remaining_time'])
            if j_changes:
                j_queue['heads'].append(j_changes)
        return j_pipeline
