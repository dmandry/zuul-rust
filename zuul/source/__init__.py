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
import time

from zuul import model


class BaseSource(object, metaclass=abc.ABCMeta):
    """Base class for sources.

    A source class gives methods for fetching and updating changes. Each
    pipeline must have (only) one source. It is the canonical provider of the
    change to be tested.

    Defines the exact public methods that must be supplied."""

    def __init__(self, driver, connection, canonical_hostname, config=None):
        self.driver = driver
        self.connection = connection
        self.canonical_hostname = canonical_hostname
        self.config = config or {}

    @abc.abstractmethod
    def getRefSha(self, project, ref):
        """Return a sha for a given project ref."""

    @abc.abstractmethod
    def isMerged(self, change, head=None):
        """Determine if change is merged.

        If head is provided the change is checked if it is at head.

        Return False, True, or the merged commit sha if available.
        """

    @abc.abstractmethod
    def canMerge(self, change, allow_needs, event=None, allow_refresh=False):
        """Determine if change can merge.

        change: The change to check for mergeability
        allow_needs: The statuses/votes that are allowed to be missing on a
                     change, typically the votes the pipeline would set itself
                     before attempting to merge
        event: event information for log annotation
        allow_refresh: Allow refreshing of cached volatile data that cannot be
                       reliably kept up to date using events.
        """

    def postConfig(self):
        """Called after configuration has been processed."""

    @abc.abstractmethod
    def getChangeKey(self, event):
        """Get a ChangeKey from a ChangeManagementEvent or TriggerEvent"""

    @abc.abstractmethod
    def getChange(self, change_key, refresh=False, event=None):
        """Get the change represented by a change_key

        This method is called very frequently, and should generally
        return quickly.  The connection is expected to cache change
        objects and automatically update them as related events are
        received.

        The event is optional, and if present may be used to annotate
        log entries and supply additional information about the change
        if a refresh is necessary.

        If the change key does not correspond to this source, return
        None.

        """

    @abc.abstractmethod
    def getChangeByURL(self, url, event):
        """Get the change corresponding to the supplied URL.

        The URL may may not correspond to this source; if it doesn't,
        or there is no change at that URL, return None.

        """

    def getChangeByURLWithRetry(self, url, event):
        for x in range(3):
            # We retry this as we are unlikely to be able to report back
            # failures if our source is broken, but if we can get the
            # info on subsequent requests we can continue to do the
            # requested job work.
            try:
                return self.getChangeByURL(url, event)
            except Exception:
                # Note that if the change isn't found dep is None.
                # We do not raise in that case and do not need to handle it
                # here.
                retry = x != 2 and " Retrying" or ""
                self.log.exception("Failed to retrieve dependency %s.%s",
                                   url, retry)
                if retry:
                    time.sleep(1)
                else:
                    raise

    @abc.abstractmethod
    def getChangesDependingOn(self, change, projects, tenant):
        """Return changes which depend on changes at the supplied URIs.

        Search this source for changes which depend on the supplied
        change.  Generally the Change.uris attribute should be used to
        perform the search, as it contains a list of URLs without the
        scheme which represent a single change

        If the projects argument is None, search across all known
        projects.  If it is supplied, the search may optionally be
        restricted to only those projects.

        The tenant argument can be used by the source to limit the
        search scope.
        """

    def useDependenciesByTopic(self):
        """Return whether the source uses topic dependencies

        If a source treats changes in a topic as a dependency cycle,
        this will return True.

        This is only implemented by the Gerrit driver, however if
        other systems have a similar "topic" functionality, it could
        be added to other drivers.
        """
        return False

    def getChangesByTopic(self, topic, event=None):
        """Return changes in the same topic.

        This should return changes under the same topic, as well as
        changes under the same topic of any git-dependent changes,
        recursively.

        This is only implemented by the Gerrit driver, however if
        other systems have a similar "topic" functionality, it could
        be added to other drivers.
        """

        return []

    @abc.abstractmethod
    def getProjectOpenChanges(self, project):
        """Get the open changes for a project."""

    def getProjectDefaultMergeMode(self, project, valid_modes=None):
        """Return the default merge mode for this project.

        If users do not specify the merge mode for a project, this
        mode will be used.  It may be a driver-specific default,
        or the driver may use data from the remote system to provide
        a project-specific default.

        Since the driver's default mode might change to a mode not yet
        supported by the active project configuration, the driver
        should select the most appropriate default base on the list of
        valid modes.
        """
        return 'merge'

    @abc.abstractmethod
    def getGitUrl(self, project):
        """Get the git url for a project."""

    def getRetryTimeout(self, project):
        """Get the retry timeout for a project in seconds.

        This is used by the mergers to potentially increase the number
        of git fetch retries before giving up.  Return None to use the
        default.
        """
        return None

    @abc.abstractmethod
    def getProject(self, name):
        """Get a project."""

    @abc.abstractmethod
    def getProjectBranches(self, project, tenant, min_ltime=-1):
        """Get branches for a project

        This method is called very frequently, and should generally
        return quickly.  The connection is expected to cache branch
        lists for all projects queried, and further, to automatically
        clear or update that cache when it observes branch creation or
        deletion events.

        """

    def getProjectMergeModes(self, project, tenant, min_ltime=-1):
        """Get supported merge modes for a project

        This method is called very frequently, and should generally
        return quickly.  The connection is expected to cache merge
        modes for all projects queried.

        The default implementation indicates that all merge modes are
        supported.

        """

        return model.ALL_MERGE_MODES

    def getProjectDefaultBranch(self, project, tenant, min_ltime=-1):
        """Return the default branch for this project.

        If users do not specify the default branch for a project, this
        mode will be used.  It may be a driver-specific default, or
        the driver may use data from the remote system to provide a
        project-specific default.

        This method is called very frequently, and should generally
        return quickly.  The connection is expected to cache default
        branches for all projects queried.

        """

        return 'master'

    @abc.abstractmethod
    def getProjectBranchSha(self, project, branch_name):
        """Return the SHA of the specified branch"""

    @abc.abstractmethod
    def getProjectBranchCacheLtime(self):
        """Return the current ltime of the project branch cache."""

    @abc.abstractmethod
    def getRequireFilters(self, config, error_accumulator):
        """Return a list of ChangeFilters for the scheduler to match against.
        """

    @abc.abstractmethod
    def getRejectFilters(self, config, error_accumulator):
        """Return a list of ChangeFilters for the scheduler to match against.
        """

    def setChangeAttributes(self, change, **attrs):
        """"Set the provided attributes on the given change.

        This method must be used when modifying attributes of a change
        outside the driver context. The driver needs to make sure that
        the change is also reflected in the cache in Zookeeper.
        """
        # TODO (swestphahl): Remove this workaround after all drivers
        # have been converted to use a Zookeeper backed changed cache.
        for name, value in attrs.items():
            setattr(change, name, value)
        return change
