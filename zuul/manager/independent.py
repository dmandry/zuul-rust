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

from zuul import model
from zuul.lib.logutil import get_annotated_logger
from zuul.manager import PipelineManager, DynamicChangeQueueContextManager


class IndependentPipelineManager(PipelineManager):
    """PipelineManager that puts every Change into its own ChangeQueue."""

    changes_merge = False
    type = 'independent'

    def getChangeQueue(self, change, event, existing=None):
        log = get_annotated_logger(self.log, event)

        # We ignore any shared change queues on the pipeline and
        # instead create a new change queue for every change.
        if existing:
            return DynamicChangeQueueContextManager(existing)
        change_queue = model.ChangeQueue.new(
            self.current_context,
            manager=self,
            dynamic=True)
        change_queue.addProject(change.project, None)
        self.state.addQueue(change_queue)
        log.debug("Dynamically created queue %s", id(change_queue))
        return DynamicChangeQueueContextManager(
            change_queue, allow_delete=True)

    def getQueueWithLiveChange(self, change):
        for queue in self.state.queues:
            for queued_change in queue.queue[-1].changes:
                if queued_change.equals(change):
                    return queue
        return None

    def addChange(self, *args, **kw):
        change = args[0]
        event = args[1]
        live = kw.get('live', True)
        dequeue_existing_change = self.pipeline.dequeue_existing_change
        enqueue_duplicate_change = self.pipeline.enqueue_duplicate_change
        duplicate_change_behavior = self.pipeline.duplicate_change_behavior
        log = get_annotated_logger(self.log, event)

        if (live and enqueue_duplicate_change != "none" and
                self.isChangeAlreadyInPipeline(change, event)):
            kw['skip_presence_check'] = True
            queue = self.getQueueWithLiveChange(change)
            if queue:
                log.debug("Found existing queue %s for change %s", queue, change)
                live_items = [ item for item in queue.queue if item.live ]
                if (enqueue_duplicate_change == "latest" and
                        len(live_items) >= 2):
                    log.debug("Change %s already has a next item queued, ignoring",
                            change)
                    return True
                if dequeue_existing_change:
                    self.removeItem(queue.queue[-1])
                elif duplicate_change_behavior == "serial":
                    active_window = len(queue.queue) - len(live_items) + 1
                    queue.updateAttributes(
                        self.current_context,
                        window=active_window,
                        window_floor=active_window,
                        window_ceiling=active_window,
                        window_increase_type='none',
                        window_decrease_type='none',
                    )
                kw['change_queue'] = queue

        return super(IndependentPipelineManager, self).addChange(*args, **kw)

    def enqueueChangesAhead(self, changes, event, quiet, ignore_requirements,
                            change_queue, history=None, dependency_graph=None,
                            warnings=None, debug=False):
        log = get_annotated_logger(self.log, event)

        history = history if history is not None else []
        for change in changes:
            if hasattr(change, 'number'):
                history.append(change)
            else:
                # Don't enqueue dependencies ahead of a non-change ref.
                return True

        abort, needed_changes = self.getMissingNeededChanges(
            changes, change_queue, event,
            dependency_graph=dependency_graph,
            warnings=warnings, debug=debug)
        if abort:
            return False

        if not needed_changes:
            return True
        log.debug("  Changes %s must be merged ahead of %s" % (
            needed_changes, change))
        for needed_change in needed_changes:
            # This differs from the dependent pipeline by enqueuing
            # changes ahead as "not live", that is, not intended to
            # have jobs run.  Pipeline requirements are still in place
            # in order to avoid unreviewed code being executed in
            # pipelines that require review.
            if needed_change not in history:
                r = self.addChange(needed_change, event, quiet=True,
                                   ignore_requirements=ignore_requirements,
                                   live=False,
                                   change_queue=change_queue,
                                   history=history,
                                   dependency_graph=dependency_graph,
                                   warnings=warnings)
                if not r:
                    return False
        return True

    def getMissingNeededChanges(self, changes, change_queue, event,
                                dependency_graph=None, item=None,
                                warnings=None, debug=False):
        log = get_annotated_logger(self.log, event)

        if self.pipeline.ignore_dependencies:
            return False, []
        changes_needed = []
        abort = False
        for change in changes:
            log.debug("Checking for changes needed by %s:" % change)
            # Return true if okay to proceed enqueing this change,
            # false if the change should not be enqueued.
            if not isinstance(change, model.Change):
                log.debug("  %s does not support dependencies" % type(change))
                continue
            needed_changes = dependency_graph.get(change)
            if not needed_changes:
                log.debug("  No changes needed")
                continue
            for needed_change in needed_changes:
                log.debug("  Change %s needs change %s:" % (
                    change, needed_change))
                if needed_change.is_merged:
                    log.debug("  Needed change is merged")
                    continue
                if needed_change in changes:
                    log.debug("  Needed change is in cycle")
                    continue
                if self.isChangeAlreadyInQueue(
                        needed_change, change_queue, event):
                    log.debug("  Needed change is already ahead in the queue")
                    continue
                log.debug("  Change %s is needed" % needed_change)
                if needed_change not in changes_needed:
                    changes_needed.append(needed_change)
                    continue
                # This differs from the dependent pipeline check in not
                # verifying that the dependent change is mergable.
        return abort, changes_needed

    def dequeueItem(self, item, quiet=False):
        super(IndependentPipelineManager, self).dequeueItem(item, quiet)
        # An independent pipeline manager dynamically removes empty
        # queues
        if not item.queue.queue:
            self.state.removeQueue(item.queue)
