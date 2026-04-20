# Copyright 2014 Rackspace Australia
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
import collections
import concurrent.futures
import logging
import os
import threading

from zuul.lib.logutil import get_annotated_logger
from zuul import model


class ReadOnlyBranchCacheError(RuntimeError):
    pass


class BaseConnection(object, metaclass=abc.ABCMeta):
    """Base class for connections.

    A connection is a shared object that sources, triggers and reporters can
    use to speak with a remote API without needing to establish a new
    connection each time or without having to authenticate each time.

    Multiple instances of the same connection may exist with different
    credentials, for example, thus allowing for different pipelines to operate
    on different Gerrit installations or post back as a different user etc.

    Connections can implement their own public methods. Required connection
    methods are validated by the {trigger, source, reporter} they are loaded
    into. For example, a trigger will likely require some kind of query method
    while a reporter may need a review method."""

    log = logging.getLogger('zuul.BaseConnection')

    def __init__(self, driver, connection_name, connection_config):
        # connection_name is the name given to this connection in zuul.ini
        # connection_config is a dictionary of config_section from zuul.ini for
        # this connection.
        # __init__ shouldn't make the actual connection in case this connection
        # isn't used in the layout.
        self.driver = driver
        self.connection_name = connection_name
        self.connection_config = connection_config
        self.sched = None

    def logEvent(self, event):
        log = get_annotated_logger(self.log, event.zuul_event_id)
        log.debug('Scheduling event from {connection}: {event}'.format(
            connection=self.connection_name,
            event=event))
        try:
            if self.sched.statsd:
                self.sched.statsd.incr(
                    'zuul.event.{driver}.{event}'.format(
                        driver=self.driver.name, event=event.type))
                self.sched.statsd.incr(
                    'zuul.event.{driver}.{connection}.{event}'.format(
                        driver=self.driver.name,
                        connection=self.connection_name,
                        event=event.type))
        except Exception:
            self.log.exception("Exception reporting event stats")

    def onLoad(self, zk_client, component_registry):
        pass

    def onStop(self):
        pass

    def registerScheduler(self, sched) -> None:
        self.sched = sched

    def cleanupCache(self):
        """Clean up the connection cache.

        This allows a connection to perform periodic cleanup actions of
        the cache, e.g. garbage collection.
        """
        pass

    def maintainCache(self, relevant, max_age):
        """Remove stale changes from the cache.

        This lets the user supply a list of change cache keys that are
        still in use.  Anything in our cache that isn't in the supplied
        list and is older than the given max. age (in seconds) should
        be safe to remove from the cache."""
        pass

    def getWebController(self, zuul_web):
        """Return a cherrypy web controller to register with zuul-web.

        :param zuul.web.ZuulWeb zuul_web:
            Zuul Web instance.
        :returns: A `zuul.web.handler.BaseWebController` instance.
        """
        return None

    def getEventQueue(self):
        """Return the event queue for this connection.

        :returns: A `zuul.zk.event_queues.ConnectionEventQueue` instance
                  or None.
        """
        return None

    def validateWebConfig(self, config, connections):
        """Validate web config.

        If there is a fatal error, the method should raise an exception.

        :param config:
           The parsed config object.
        :param zuul.lib.connections.ConnectionRegistry connections:
           Registry of all configured connections.
        """
        return False

    def toDict(self):
        """Return public information about the connection
        """
        return {
            "name": self.connection_name,
            "driver": self.driver.name,
        }


class ZKBranchCacheMixin:
    # Expected to be defined by the connection and to be an instance
    # of BranchCache
    _branch_cache = None
    read_only = False

    @abc.abstractmethod
    def isBranchProtected(self, project_name, branch_name,
                          zuul_event_id):
        """Return if the branch is protected or None if the branch is unknown.

        :param str project_name:
            The name of the project.
        :param str branch_name:
            The name of the branch.
        :param zuul_event_id:
            Unique id associated to the handled event.
        """
        pass

    @abc.abstractmethod
    def _fetchProjectBranches(self, project, required_flags):
        """Perform a remote query to determine the project's branches.

        Connection subclasses should implement this method.

        :param model.Project project:
            The project.
        :param set(BranchFlag) required_flags:
            Which flags need to be valid in the result set.

        :returns: A list of BranchInfo objects
        """

    @abc.abstractmethod
    def _getProjectBranchesRequiredFlags(self, exclude_unprotected,
                                         exclude_locked):
        """Calculate the set of required branch flags needed to filter the
        branches as specified.

        Connection subclasses should implement this method.

        :param bool exclude_unprotected:
            Whether to exclude unprotected branches
        :param bool exclude_locked:
            Whether to exclude locked branches

        :returns: A set of BranchFlag objects
        """

    @abc.abstractmethod
    def _filterProjectBranches(self, branch_infos, exclude_unprotected,
                               exclude_locked):
        """Filter branches according to the specified criteria

        Connection subclasses should implement this method.

        :param list(BranchInfo) branch_infos:
            A list of BranchInfo objects previously supplied by
            _fetchProjectBranches
        :param bool exclude_unprotected:
            Whether to exclude unprotected branches
        :param bool exclude_locked:
            Whether to exclude locked branches

        :returns: A list of BranchInfo objects
        """

    def _fetchProjectMergeModes(self, project):
        """Perform a remote query to determine the project's supported merge
           modes.

        Connection subclasses should implement this method if they are
        able to determine which merge modes apply for a project.  The
        default implemantion returns that all merge modes are valid.

        :param model.Project project:
            The project.

        :returns: A list of merge modes as model IDs.

        """
        return model.ALL_MERGE_MODES

    def _fetchProjectDefaultBranch(self, project):
        """Perform a remote query to determine the project's default branch.

        Connection subclasses should implement this method if they are
        able to determine the upstream default branch for a project.  The
        default implemantion returns 'master' for now and will likely change
        to return something else if and when the git default changes.

        :param model.Project project:
            The project.

        :returns: The name of the default branch.

        """
        return 'master'

    def clearConnectionCacheOnBranchEvent(self, event):
        """Update event and clear connection cache if needed.

        This checks whether the event created or deleted a branch so
        that Zuul may know to perform a reconfiguration on the
        project. Drivers must call this method when a branch event is
        received.

        :param event:
            The event, inherit from `zuul.model.TriggerEvent` class.
        """
        if event.oldrev == '0' * 40:
            event.branch_created = True
        elif event.newrev == '0' * 40:
            event.branch_deleted = True
        else:
            event.branch_updated = True

        project = self.source.getProject(event.project_name)
        if event.branch:
            ltime = None
            if event.branch_deleted:
                # We currently cannot determine if a deleted branch was
                # protected so we need to assume it was. GitHub/GitLab don't
                # allow deletion of protected branches but we don't get a
                # notification about branch protection settings. Thus we don't
                # know if branch protection has been disabled before deletion
                # of the branch.
                # FIXME(tobiash): Find a way to handle that case
                ltime = self.updateProjectBranches(project, event)
            elif event.branch_created:
                # In GitHub, a new branch never can be protected
                # because that needs to be configured after it has
                # been created.  Other drivers could optimize this,
                # but for the moment, implement the lowest common
                # denominator and clear the cache so that we query.
                ltime = self.updateProjectBranches(project, event)

            # Our required ltime should either be the current ltime of
            # the branch cache, or, if we performed an update, then
            # the ltime of that update.
            event.branch_cache_ltime = ltime or self._branch_cache.ltime
        return event

    def updateProjectBranches(self, project, zuul_event_id=None):
        """Update the branch cache for the project.

        :param zuul.model.Project project:
            The project for which the branches are returned.
        :param zuul_event_id:
            Unique id associated to the handled event.

        :returns: The ltime of the branch cache update.
        """
        log = get_annotated_logger(self.log, event=zuul_event_id)

        # Figure out which queries we have a cache for
        required_flags = self._branch_cache.getProjectCompletedFlags(
            project.name)
        if required_flags:
            # Update them if we have them
            valid_flags, branch_infos = self._fetchProjectBranches(
                project, required_flags)
        else:
            valid_flags = None
            branch_infos = None
        merge_modes = self._fetchProjectMergeModes(project)
        default_branch = self._fetchProjectDefaultBranch(project)
        ltime = self._branch_cache.setAllProjectData(
            project.name, valid_flags, branch_infos,
            merge_modes, default_branch)
        log.info("Updated branches for %s", project.name)
        return ltime

    def getProjectBranches(self, project, tenant, min_ltime=-1):
        """Get the branch names for the given project.

        :param zuul.model.Project project:
            The project for which the branches are returned.
        :param zuul.model.Tenant tenant:
            The related tenant.
        :param int min_ltime:
            The minimum ltime to determine if we need to refresh the cache.

        :returns: The list of branch names.
        """
        exclude_unprotected = tenant.getExcludeUnprotectedBranches(project)
        exclude_locked = tenant.getExcludeLockedBranches(project)
        branches = None

        required_flags = self._getProjectBranchesRequiredFlags(
            exclude_unprotected, exclude_locked)
        if self._branch_cache:
            try:
                branches = self._branch_cache.getProjectBranches(
                    project.name, required_flags, min_ltime)
                if branches is not None:
                    branches = [b.name for b in self._filterProjectBranches(
                        branches, exclude_unprotected, exclude_locked)]
            except LookupError:
                if self.read_only:
                    # A scheduler hasn't attempted to fetch them yet
                    raise ReadOnlyBranchCacheError(
                        "Will not fetch project branches as read-only is set")
                else:
                    branches = None

        if branches is not None:
            return sorted(branches)
        elif self.read_only:
            # A scheduler has previously attempted a fetch, but got
            # the None due to an error; we can't retry since we're
            # read-only.
            raise RuntimeError(
                "Will not fetch project branches as read-only is set")

        # Above we calculated a set of flags needed to answer the
        # query.  If the fetch below fails, we will mark that set of
        # flags as failed in the ProjectInfo structure.  However, if
        # the fetch below succeeds, it can supply its own set of valid
        # flags that we will record as successful.  This lets the
        # driver indicate that the returned results include more data
        # than strictly necessary (ie, protected+locked and not just
        # protected).
        try:
            valid_flags, branch_infos = self._fetchProjectBranches(
                project, required_flags)
        except Exception:
            # We weren't able to get the branches.  We need to tell
            # future schedulers to try again but tell zuul-web that we
            # tried and failed.  Set the branches to None to indicate
            # that we have performed a fetch and retrieved no data.  Any
            # time we encounter None in the cache, we will try again.
            if self._branch_cache:
                self._branch_cache.setProjectBranches(
                    project.name, required_flags, None)
            raise
        self.log.info("Got branches for %s" % project.name)

        if self._branch_cache:
            self._branch_cache.setProjectBranches(
                project.name, valid_flags, branch_infos)

        branches = [b.name for b in self._filterProjectBranches(
                    branch_infos, exclude_unprotected, exclude_locked)]
        return sorted(branches)

    def getProjectMergeModes(self, project, tenant, min_ltime=-1):
        """Get the merge modes for the given project.

        :param zuul.model.Project project:
            The project for which the merge modes are returned.
        :param zuul.model.Tenant tenant:
            The related tenant.
        :param int min_ltime:
            The minimum ltime to determine if we need to refresh the cache.

        :returns: The list of merge modes by model id.
        """
        merge_modes = None

        if self._branch_cache:
            try:
                merge_modes = self._branch_cache.getProjectMergeModes(
                    project.name, min_ltime)
            except LookupError:
                if self.read_only:
                    # A scheduler hasn't attempted to fetch them yet
                    raise ReadOnlyBranchCacheError(
                        "Will not fetch merge modes as read-only is set")
                else:
                    merge_modes = None

        if merge_modes is not None:
            return merge_modes
        elif self.read_only:
            # A scheduler has previously attempted a fetch, but got
            # the None due to an error; we can't retry since we're
            # read-only.
            raise RuntimeError(
                "Will not fetch merge_modes as read-only is set")

        # We need to perform a query
        try:
            merge_modes = self._fetchProjectMergeModes(project)
        except Exception:
            # We weren't able to get the merge modes.  We need to tell
            # future schedulers to try again but tell zuul-web that we
            # tried and failed.  Set the merge modes to None to indicate
            # that we have performed a fetch and retrieved no data.  Any
            # time we encounter None in the cache, we will try again.
            if self._branch_cache:
                self._branch_cache.setProjectMergeModes(
                    project.name, None)
            raise
        self.log.info("Got merge modes for %s" % project.name)

        if self._branch_cache:
            self._branch_cache.setProjectMergeModes(
                project.name, merge_modes)

        return merge_modes

    def getProjectDefaultBranch(self, project, tenant, min_ltime=-1):
        """Get the default branch for the given project.

        :param zuul.model.Project project:
            The project for which the default branch is returned.
        :param zuul.model.Tenant tenant:
            The related tenant.
        :param int min_ltime:
            The minimum ltime to determine if we need to refresh the cache.

        :returns: The name of the default branch.
        """
        default_branch = None

        if self._branch_cache:
            try:
                default_branch = self._branch_cache.getProjectDefaultBranch(
                    project.name, min_ltime)
            except LookupError:
                if self.read_only:
                    # A scheduler hasn't attempted to fetch it yet
                    raise ReadOnlyBranchCacheError(
                        "Will not fetch default branch as read-only is set")
                else:
                    default_branch = None

        if default_branch is not None:
            return default_branch
        elif self.read_only:
            # A scheduler has previously attempted a fetch, but got
            # the None due to an error; we can't retry since we're
            # read-only.
            raise RuntimeError(
                "Will not fetch default branch as read-only is set")

        # We need to perform a query
        try:
            default_branch = self._fetchProjectDefaultBranch(project)
        except Exception:
            # We weren't able to get the default branch.  We need to tell
            # future schedulers to try again but tell zuul-web that we
            # tried and failed.  Set the default branch to None to indicate
            # that we have performed a fetch and retrieved no data.  Any
            # time we encounter None in the cache, we will try again.
            if self._branch_cache:
                self._branch_cache.setProjectDefaultBranch(
                    project.name, None)
            raise
        self.log.info("Got default branch for %s: %s", project.name,
                      default_branch)

        if self._branch_cache:
            self._branch_cache.setProjectDefaultBranch(
                project.name, default_branch)

        return default_branch

    def checkBranchCache(self, project_name, event, protected=None):
        """Update the cache for a project when a branch event is processed

        This method must be called when a branch event is processed: if the
        event references a branch and the unprotected branches are excluded,
        the branch protection status could have been changed.

        :params str project_name:
            The project name.
        :params event:
            The event, inherit from `zuul.model.TriggerEvent` class.
        :params protected:
            If defined the caller already knows if the branch is protected
            so the query can be skipped.
        """
        if protected is None:
            protected = self.isBranchProtected(project_name, event.branch,
                                               zuul_event_id=event)
        if protected is not None:
            required_flags = self._getProjectBranchesRequiredFlags(
                exclude_unprotected=True, exclude_locked=False)

            branches = self._branch_cache.getProjectBranches(
                project_name, required_flags, default=None)

            if not branches:
                branches = []

            branches = [b.name for b in branches]

            update = False
            if (event.branch in branches) and (not protected):
                update = True
            if (event.branch not in branches) and (protected):
                update = True
            if update:
                self.log.info("Project %s branch %s protected state "
                              "changed to %s",
                              project_name, event.branch, protected)
                self._branch_cache.setProtected(
                    project_name, event.branch, protected)
                event.branch_cache_ltime = self._branch_cache.ltime

            event.branch_protected = protected
        else:
            # This can happen if the branch was deleted in
            # GitHub/GitLab. In this case we assume that the branch
            # COULD have been protected before. However, if the event
            # says the branch was created we probably never loaded any
            # configuration from it. The cache update is handled by the
            # push event, so we don't touch the cache here again.
            event.branch_protected = not event.branch_created

    def clearBranchCache(self, projects=None):
        """Clear the branch cache

        In case the branch cache gets out of sync with the source,
        this method can be called to clear it and force querying the
        source the next time the cache is used.
        """
        self._branch_cache.clear(projects)


class ZKChangeCacheMixin:
    # Expected to be defined by the connection and to be an instance
    # that implements the AbstractChangeCache API.
    _change_cache = None

    def cleanupCache(self):
        self._change_cache.cleanup()

    def maintainCache(self, relevant, max_age):
        self._change_cache.prune(relevant, max_age)

    def updateChangeAttributes(self, change, **attrs):
        def _update_attrs(c):
            for name, value in attrs.items():
                setattr(c, name, value)
        self._change_cache.updateChangeWithRetry(change.cache_stat.key,
                                                 change, _update_attrs)

    def estimateCacheDataSize(self):
        return self._change_cache.estimateDataSize()


class BaseThreadPoolEventConnector:
    """Move events from the connection into the scheduler"""

    log = logging.getLogger("zuul.BaseThreadPoolEventConnector")

    def __init__(self, connection):
        self.connection = connection
        self.event_queue = connection.event_queue
        self._stopped = False
        self._events_in_progress = set()
        self._dispatcher_wake_event = threading.Event()
        self._event_dispatcher = threading.Thread(
            name=f'{self.connection.connection_name.title()}EventDispatcher',
            target=self.run_event_dispatcher,
            daemon=True)
        self._thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=min(32, (os.cpu_count() or 1) * 4))
        self._event_forward_queue = collections.deque()

    def stop(self):
        self._stopped = True
        self._dispatcher_wake_event.set()
        self.event_queue.election.cancel()
        self._event_dispatcher.join()

        self._thread_pool.shutdown()

    def start(self):
        self._event_dispatcher.start()

    def _shouldStop(self):
        return (self._stopped or
                not self.event_queue.election.is_still_valid())

    def _onNewEvent(self):
        self._dispatcher_wake_event.set()
        # Stop the data watch in case the connector was stopped
        return not self._stopped

    def run_event_dispatcher(self):
        # Wait for the scheduler to prime its config so that we have
        # the full tenant list before we start moving events.
        self.connection.sched.primed_event.wait()
        if self._stopped:
            return
        self.event_queue.registerEventWatch(self._onNewEvent)
        # Set the wake event so we get an initial run
        self._dispatcher_wake_event.set()
        while not self._stopped:
            try:
                self.event_queue.election.run(self._dispatchEventsMain)
            except Exception:
                self.log.exception("Exception handling %s %s event:",
                                   self.connection.connection_name,
                                   self.connection.driver_name)
            # In case we caught an exception with events in progress,
            # reset these in case we run the loop again.
            self._events_in_progress = set()
            self._event_forward_queue = collections.deque()

    def _dispatchEventsMain(self):
        while True:
            delay = None
            if self._dispatcher_wake_event.is_set():
                self._dispatcher_wake_event.clear()
                # We can start processing events as long as we're
                # running; if we are stopping, then we need to continue
                # this loop until previously processed events are
                # completed but not start processing any new events.
                if not self._shouldStop():
                    delay = self._dispatchEvents()

            # Now process the futures from this or any previous
            # iterations of the loop.
            if len(self._event_forward_queue):
                self._forwardEvents()

            # If there are no futures, we can sleep until there are
            # new events (or stop altogether); otherwise we need to
            # continue processing futures.
            if not len(self._event_forward_queue):
                if self._shouldStop():
                    return
                # Delay can be None and we will wait for the next event
                self._dispatcher_wake_event.wait(delay)
            else:
                # Sleep a small amount of time to give the futures
                # time to complete.
                self._dispatcher_wake_event.wait(0.1)
            if delay is not None:
                # If we were asked to delay, then we haven't processed
                # all the events yet so go again.
                self._dispatcher_wake_event.set()

    def _dispatchEvents(self):
        # This is the first half of the event dispatcher.  It reads
        # events from the webhook event queue and passes them to a
        # concurrent executor for pre-processing.
        try:
            event_id_offset = max(
                self.event_queue.eventIdFromAckRef(r)
                for r in self._events_in_progress)
        except ValueError:
            event_id_offset = None

        for event in self.event_queue.iter(event_id_offset):
            if self._shouldStop():
                break

            processor = self._getEventProcessor(event)
            future = self._thread_pool.submit(processor)

            # Events are acknowledged in the event forwarder loop after
            # pre-processing. This way we can ensure that no events are
            # lost.
            self._events_in_progress.add(event.ack_ref)
            self._event_forward_queue.append(future)

    @abc.abstractmethod
    def _getEventProcessor(self, event):
        """Returns a function that will be submitted to the TPE."""

    def _forwardEvents(self):
        # This is the second half of the event dispatcher.  It
        # collects pre-processed events from the concurrent executor
        # and forwards them to the scheduler queues.
        while True:
            try:
                if not len(self._event_forward_queue):
                    return
                # Peek at the next event and see if it's done or if we
                # need to wait for the next loop iteration.
                if not self._event_forward_queue[0].done():
                    return
                future = self._event_forward_queue.popleft()
                (events, connection_event, zuul_event_id,
                 event_type) = future.result()
                try:
                    if not events:
                        log = get_annotated_logger(self.log, zuul_event_id)
                        log.debug('Nothing to be forwarded, '
                                  'event type: %s', event_type)
                    for event in events:
                        self.connection.logEvent(event)
                        if isinstance(event, model.DequeueEvent):
                            self.connection.sched.addChangeManagementEvent(
                                event)
                        else:
                            self.connection.sched.addTriggerEvent(
                                self.connection.driver_name, event
                            )
                finally:
                    # Ack event in Zookeeper
                    self.event_queue.ack(connection_event)
                    self._events_in_progress.remove(connection_event.ack_ref)
            except Exception:
                self.log.exception("Exception moving %s %s event:",
                                   self.connection.connection_name,
                                   self.connection.driver_name)
