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

from abc import ABCMeta

from zuul import model
from zuul.lib.logutil import get_annotated_logger
from zuul.manager import PipelineManager, StaticChangeQueueContextManager
from zuul.manager import DynamicChangeQueueContextManager


class ChangeQueueManager:
    """Manages a single named queue (but may be per-branch)"""
    def __init__(self, pipeline_manager, name=None, per_branch=False):
        self.log = pipeline_manager.log
        self.pipeline_manager = pipeline_manager
        self.name = name
        self.per_branch = per_branch
        self.branch_matchers = {}
        self.created_for_branches = {}
        self.project_branches = set()

    def __repr__(self):
        if self.branch_matchers:
            kind = 'branch-assigned'
        elif self.per_branch:
            kind = 'per-branch'
        else:
            kind = 'all-branches'
        return f'<ChangeQueueManager for {self.name} type: {kind}>'

    def addBranchMatcher(self, project_name, matcher):
        matcher_set = self.branch_matchers.setdefault(project_name, set())
        matcher_set.add(matcher)

    def matchesBranch(self, change):
        for matcher in self.branch_matchers.get(
                change.project.canonical_name, []):
            if matcher is None:
                # This may be due to conditions such as:
                # * A trusted project configuring itself
                # * A project that disabled implied branches with pragma
                # * A project with only one branch
                # In these cases, just like the jobs attached to these
                # definitions, we will consider all branches assigned
                # to this queue.
                return True
            if matcher.matches(change):
                return True
        return False

    def getOrCreateQueue(self, project, branch):
        if not self.per_branch:
            branch = None
        change_queue = self.created_for_branches.get(branch)

        if not change_queue:
            name = self.name or project.name
            change_queue = self.pipeline_manager.constructChangeQueue(name)
            self.pipeline_manager.state.addQueue(change_queue)
            self.created_for_branches[branch] = change_queue

        if not change_queue.matches(project.canonical_name, branch):
            change_queue.addProject(project, branch)
            self.project_branches.add((project.canonical_name, branch))
            self.log.debug("Added project %s to queue: %s",
                           project, change_queue)

        return change_queue


class SharedQueuePipelineManager(PipelineManager, metaclass=ABCMeta):
    """Intermediate class that adds the shared-queue behavior.

    This is not a full pipeline manager; it just adds the shared-queue
    behavior to the base class and is used by the dependent and serial
    managers.
    """

    changes_merge = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # project_name -> manager
        self.default_queue_managers = {}
        # project_name -> [managers]
        self.branch_assigned_queue_managers = {}
        # queue_name -> manager
        self.named_queue_managers = {}
        self.change_queue_managers = []

    def _configureBranchAssignedQueueManager(self, layout, project,
                                             project_name, project_config):
        # Ensure that we have a ChangeQueueManager for this project stanza
        queue_name = project_config.queue_name
        queue = layout.queues.get(queue_name)
        if not queue:
            return
        if queue.type == queue.Type.BRANCH_ASSIGNED:
            manager = self.named_queue_managers.get(queue_name)
            if not manager:
                manager = ChangeQueueManager(
                    self,
                    name=queue_name,
                )
                self.named_queue_managers[queue_name] = manager
                self.change_queue_managers.append(manager)
                self.log.debug("Created branch-assigned queue: %s" % manager)
            manager.addBranchMatcher(
                project.canonical_name,
                project_config.getBranchMatcher(
                    self.tenant,
                    project.canonical_name,
                )
            )
            # Add this queue manager to the list of possible
            # branch-assigned queue managers for this project.
            manager_set = self.branch_assigned_queue_managers.setdefault(
                project_name, set())
            manager_set.add(manager)

    def buildChangeQueues(self, layout):
        self.log.debug("Building shared change queues")
        tenant = self.tenant
        layout_project_configs = layout.project_configs

        for project_name, project_configs in layout_project_configs.items():
            (trusted, project) = tenant.getProject(project_name)
            default_queue_name = None
            project_in_pipeline = False
            for project_config in layout.getAllProjectConfigs(project_name):
                project_pipeline_config = project_config.pipelines.get(
                    self.pipeline.name)
                if not default_queue_name:
                    queue = layout.queues.get(project_config.queue_name)
                    branch_assigned = (
                        queue and queue.type == queue.Type.BRANCH_ASSIGNED
                    )
                    if not branch_assigned:
                        default_queue_name = project_config.queue_name
                if project_pipeline_config is None:
                    continue
                project_in_pipeline = True
            if not project_in_pipeline:
                continue

            # Check if the default queue is global or per branch
            queue = layout.queues.get(default_queue_name)
            per_branch = bool(queue and queue.type == queue.Type.PER_BRANCH)

            if (default_queue_name and
                (default_queue_name in self.named_queue_managers)):
                change_queue_manager = self.named_queue_managers[
                    default_queue_name]
            else:
                change_queue_manager = ChangeQueueManager(
                    self, name=default_queue_name, per_branch=per_branch)
                if default_queue_name:
                    # If this is a named queue, keep track of it in
                    # case it is referenced again.  Otherwise, it will
                    # have a name automatically generated from its
                    # constituent projects.
                    self.named_queue_managers[default_queue_name] =\
                        change_queue_manager
                self.change_queue_managers.append(change_queue_manager)
                self.log.debug("Created queue: %s", change_queue_manager)
            self.default_queue_managers[project_name] = change_queue_manager
            self.log.debug("Added project %s to default queue manager: %s",
                           project, change_queue_manager)
            for project_config in layout.getAllProjectConfigs(project_name):
                self._configureBranchAssignedQueueManager(
                    layout, project, project_name, project_config)

    def getChangeQueue(self, change, event, existing=None):
        log = get_annotated_logger(self.log, event)

        # Ignore the existing queue, since we can always get the correct queue
        # from the pipeline. This avoids enqueuing changes in a wrong queue
        # e.g. during re-configuration.
        queue = self.state.getQueue(change.project.canonical_name,
                                    change.branch)
        if queue:
            return StaticChangeQueueContextManager(queue)
        else:
            # Change queues in the dependent pipeline manager are created
            # lazy so first check the managers for the project.

            # If this project-branch was assigned to a branch-assigned
            # queue, use that.
            for manager in self.branch_assigned_queue_managers.get(
                    change.project.canonical_name, []):
                if manager.matchesBranch(change):
                    return StaticChangeQueueContextManager(
                        manager.getOrCreateQueue(change.project, change.branch)
                    )
            # If the project was assigned to a traditional or
            # per-branch queue, use that.
            manager = self.default_queue_managers.get(
                change.project.canonical_name)
            if manager:
                return StaticChangeQueueContextManager(
                    manager.getOrCreateQueue(change.project, change.branch)
                )

            # No specific per-branch queue matched so look again with no branch
            queue = self.state.getQueue(change.project.canonical_name, None)
            if queue:
                return StaticChangeQueueContextManager(queue)

            # There is no existing queue for this change. Create a
            # dynamic one for this one change's use
            change_queue = model.ChangeQueue.new(
                self.current_context,
                manager=self,
                dynamic=True)
            change_queue.addProject(change.project, None)
            self.state.addQueue(change_queue)
            log.debug("Dynamically created queue %s", change_queue)
            return DynamicChangeQueueContextManager(
                change_queue, allow_delete=True)
