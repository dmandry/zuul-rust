# Copyright 2014 Rackspace Australia
# Copyright 2021 BMW Group
# Copyright 2021-2026 Acme Gating, LLC
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

from contextlib import contextmanager
import logging
import json
from urllib.parse import quote_plus, unquote_plus

from zuul.zk.launcher import LockableZKObjectCache
from zuul.zk.zkobject import ZKContext, ExpandableLockableZKObject
from zuul.zk.branch_cache_common import (
    RAISE_EXCEPTION,
    BranchFlag,
    BranchInfo,
)

from kazoo.exceptions import NoNodeError


# A helper method for the branch cache below.
def return_default(default, project_name):
    if default is RAISE_EXCEPTION:
        raise LookupError(
            f"No branches for project {project_name}")
    return default


class ProjectInfo(ExpandableLockableZKObject):
    """Store branch cache project information in ZK

    If a project is absent from the cache, it needs to be queried from
    the source.
    """

    def __init__(self):
        super().__init__()
        self._set(
            name=None,
            merge_modes=None,
            default_branch=None,
            branches={},
            # The set of flags we have performed queries for:
            completed_flags=BranchFlag.CLEAR,
            # If there was an error fetching the branches for a given set
            # of flags, the failure will be recorded here:
            failed_flags=BranchFlag.CLEAR,
            # Unused:
            is_locked=None,
            lock_holder=None,
        )

    def getPath(self):
        safe_project = quote_plus(self.name)
        return f"{self._root}/data/{safe_project}"

    def getLockPath(self):
        safe_project = quote_plus(self.name)
        return f"{self._root}/lock/{safe_project}"

    def acquireLock(self, *args, **kw):
        # Unlike other TreeCache systems, we want to create the lock
        # path if it doesn't exist.  We're not worried about
        # accidentally re-creating it.
        if kw.get('ensure_path') is None:
            kw['ensure_path'] = True
        return super().acquireLock(*args, **kw)

    def serialize(self, context):
        data = {
            'merge_modes': self.merge_modes,
            'default_branch': self.default_branch,
            'branches': {b.name: b.toDict() for b in self.branches.values()},
            'flags': [
                self.completed_flags,
                self.failed_flags,
            ],
        }
        return json.dumps(data, sort_keys=True).encode("utf-8")

    def deserialize(self, raw, context, extra=None):
        data = super().deserialize(raw, context)
        data['branches'] = {
            name: BranchInfo.fromDict(name, bdata)
            for name, bdata in data['branches'].items()
        }
        data['completed_flags'] = BranchFlag(data['flags'][0])
        data['failed_flags'] = BranchFlag(data['flags'][1])
        return data


class BranchTreeCache(LockableZKObjectCache):
    def __init__(self, zk_client, root):
        super().__init__(
            zk_client,
            None,
            root=root,
            items_path='data',
            locks_path='lock',
            zkobject_class=ProjectInfo,
        )

    def objectFromRaw(self, key, data, shard_index, zstat):
        obj = super().objectFromRaw(key, data, shard_index, zstat)
        if obj:
            obj._set(_root=self.root, name=unquote_plus(key[0]))
        return obj


class BranchCacheNew:
    BASE_PATH_FORMAT = '/zuul/cache/connection/{connection}/project'

    def __init__(self, zk_client, connection, component_registry):
        self.log = logging.getLogger(
            f"zuul.BranchCacheNew.{connection.connection_name}")

        self.connection = connection

        self.base_path = self.BASE_PATH_FORMAT.format(
            connection=self.connection.connection_name)

        self.cache = BranchTreeCache(zk_client, self.base_path)

        # TODO: standardize on a stop event for connections and add it
        # to the context.
        self.zk_client = zk_client
        self.zk_context = ZKContext(zk_client, None, None, self.log)

    def createZKContext(self, lock):
        return ZKContext(self.zk_client, lock, None, self.log)

    def _getProjectInfoForWrite(self, project_name):
        # Get a ProjectInfo object from the cache, or ZK
        # Create if necessary
        safe_project = quote_plus(project_name)
        project_info = self.cache.getItem(safe_project)
        if project_info is not None:
            return project_info
        project_info = ProjectInfo()
        project_info._set(
            _root=self.cache.root,
            name=project_name,
            create_on_save=True,
        )
        return project_info

    @contextmanager
    def _getActiveLockedProjectInfo(self, project_name):
        project_info = self._getProjectInfoForWrite(project_name)
        safe_project = quote_plus(project_name)
        with project_info.locked(self.zk_context) as lock:
            with self.createZKContext(lock) as zk_context:
                try:
                    project_info.refresh(zk_context)
                except NoNodeError:
                    pass
                with project_info.activeContext(zk_context):
                    yield project_info
                # This will insert or replace the item only if it is
                # not already in the cache.  This avoids race
                # conditions when we create a new item, we don't need
                # to wait for our own cache to update.
                self.cache.setItem(safe_project, project_info)

    def _getProjectInfoForRead(self, project_name, min_ltime):
        # Get a ProjectInfo object from the cache, or ZK
        if self.cache.max_zxid < min_ltime:
            self.cache.waitForSync()
        safe_project = quote_plus(project_name)
        project_info = self.cache.getItem(safe_project)
        return project_info

    def clear(self, projects=None):
        """Clear the cache"""
        if projects is None:
            items = self.cache.getItems()
        else:
            items = [
                self.cache.getItem(quote_plus(p))
                for p in projects
            ]
        for project_info in items:
            if project_info is None:
                continue
            with project_info.locked(self.zk_context) as lock:
                with self.createZKContext(lock) as zk_context:
                    project_info.delete(zk_context)

    def getProjectCompletedFlags(self, project_name):
        """Get the completed branch query flags for a project

        :param str project_name: The project name

        :returns: a BranchFlag of the completed query flags
        """
        project_info = self._getProjectInfoForRead(project_name, -1)
        if project_info is None:
            return BranchFlag.CLEAR
        return project_info.completed_flags

    def getProjectBranches(self, project_name, required_flags,
                           min_ltime=-1, default=RAISE_EXCEPTION):
        """Get the branch names for the given project.

        Checking the branch cache we need to distinguish three different
        cases:

            1. cache miss (not queried yet)
            2. cache hit (including empty list of branches)
            3. error when fetching branches

        If the cache doesn't contain any branches for the project and no
        default value is provided a LookupError is raised.

        If there was an error fetching the branches, the return value
        will be None.

        Otherwise the list of branches will be returned.

        :param str project_name:
            The project for which the branches are returned.
        :param bool required_flags:
            The branch flags we must have completed queries for in order
            for the cache to be considered valid.
        :param int min_ltime:
            The minimum cache ltime to consider the cache valid.
        :param any default:
            Optional default value to return if no cache entry exits.

        :returns: The list of branch names, or None if there was
            an error when fetching the branches.
        """
        project_info = self._getProjectInfoForRead(project_name, min_ltime)
        if project_info is None:
            return return_default(default, project_name)

        # Determine if we have enough info to answer the question
        # Check that required flags is a subset of completed flags
        if not (required_flags & project_info.completed_flags
                == required_flags):
            # We don't have the data, either because we haven't
            # queried it or the query failed.  Figure out which.
            # If there is any overlap, something failed.
            if (required_flags & project_info.failed_flags):
                return None
            return return_default(default, project_name)

        # We have the necessary info for this filtering.
        return list(project_info.branches.values())

    def setProjectBranches(self, project_name,
                           valid_flags, branch_infos):
        """Set the branch names for the given project.

        Use None as a sentinel value for the branches to indicate that
        there was a fetch error.

        :param str project_name:
            The project for the branches.
        :param BranchFlag valid_flags:
            The queries this list of branches is able to satisfy.
        :param list[str] branches:
            The list of branches or None to indicate a fetch error.
        """
        with self._getActiveLockedProjectInfo(project_name) as project_info:
            self._setProjectBranches(project_info,
                                     valid_flags, branch_infos)

    def _setProjectBranches(self, project_info,
                            valid_flags, branch_infos):

        if branch_infos is None:
            # We're storing an error, set the bits accordingly
            project_info.failed_flags |= valid_flags
            project_info.completed_flags &= ~valid_flags
            return

        # Set the bits indicating a good query.
        project_info.failed_flags &= ~valid_flags
        project_info.completed_flags |= valid_flags

        # Add or update branch info
        for branch_info in branch_infos:
            existing = project_info.branches.get(branch_info.name)
            if existing:
                existing.update(branch_info)
            else:
                project_info.branches[branch_info.name] = branch_info

        # Delete any existing branches which we would expect to be
        # in the results but aren't.  At the time of writing, this
        # isn't strictly necessary because we clear the branch
        # cache on branch deletion, but this may enable us to
        # change that in the future.
        valid_branches = set(bi.name for bi in branch_infos)
        for branch_name in list(project_info.branches.keys()):
            if branch_name in valid_branches:
                continue
            branch_info = project_info.branches[branch_name]
            # If the branch_info flags are a subset of the valid
            # flags, we can delete it.
            if (branch_info.valid_flags & valid_flags ==
                branch_info.valid_flags):
                del project_info.branches[branch_name]

    def setProtected(self, project_name, branch, protected):
        """Correct the protection state of a branch.

        This may be called if a branch has changed state without us
        receiving an explicit event.
        """
        with self._getActiveLockedProjectInfo(project_name) as project_info:
            branch_info = project_info.branches.get(branch)
            if branch_info is None:
                branch_info = BranchInfo(branch)
                project_info.branches[branch] = branch_info
            branch_info.protected = protected

    def getProjectMergeModes(self, project_name,
                             min_ltime=-1, default=RAISE_EXCEPTION):
        """Get the merge modes for the given project.

        Checking the branch cache we need to distinguish three different
        cases:

            1. cache miss (not queried yet)
            2. cache hit (including empty list of merge modes)
            3. error when fetching merge modes

        If the cache doesn't contain any merge modes for the project and no
        default value is provided a LookupError is raised.

        If there was an error fetching the merge modes, the return value
        will be None.

        Otherwise the list of merge modes will be returned.

        :param str project_name:
            The project for which the merge modes are returned.
        :param int min_ltime:
            The minimum cache ltime to consider the cache valid.
        :param any default:
            Optional default value to return if no cache entry exits.

        :returns: The list of merge modes by model id, or None if there was
            an error when fetching the merge modes.
        """
        project_info = self._getProjectInfoForRead(project_name, min_ltime)
        if project_info is None:
            return return_default(default, project_name)

        return project_info.merge_modes

    def setProjectMergeModes(self, project_name, merge_modes):
        """Set the supported merge modes for the given project.

        Use None as a sentinel value for the merge modes to indicate
        that there was a fetch error.

        :param str project_name:
            The project for the merge modes.
        :param list[int] merge_modes:
            The list of merge modes (by model ID) or None.

        """
        with self._getActiveLockedProjectInfo(project_name) as project_info:
            self._setProjectMergeModes(project_info, merge_modes)

    def _setProjectMergeModes(self, project_info, merge_modes):
        project_info.merge_modes = merge_modes

    def getProjectDefaultBranch(self, project_name,
                                min_ltime=-1, default=RAISE_EXCEPTION):
        """Get the default branch for the given project.

        Checking the branch cache we need to distinguish three different
        cases:

            1. cache miss (not queried yet)
            2. cache hit (including unknown default branch)
            3. error when fetching default branch

        If the cache doesn't contain a default branch for the project
        and no default value is provided a LookupError is raised.

        If there was an error fetching the default branch, the return
        value will be None.

        Otherwise the default branch will be returned.

        :param str project_name:
            The project for which the default branch is returned.
        :param int min_ltime:
            The minimum cache ltime to consider the cache valid.
        :param any default:
            Optional default value to return if no cache entry exits.

        :returns: The name of the default branch or None if there was
            an error when fetching it.

        """
        project_info = self._getProjectInfoForRead(project_name, min_ltime)
        if project_info is None:
            return return_default(default, project_name)

        return project_info.default_branch

    def setProjectDefaultBranch(self, project_name, default_branch):
        """Set the upstream default branch for the given project.

        Use None as a sentinel value for the default branch to indicate
        that there was a fetch error.

        :param str project_name:
            The project for the default branch.
        :param str default_branch:
            The default branch or None.

        """
        with self._getActiveLockedProjectInfo(project_name) as project_info:
            self._setProjectDefaultBranch(project_info, default_branch)

    def _setProjectDefaultBranch(self, project_info, default_branch):
        project_info.default_branch = default_branch

    def setAllProjectData(self, project_name, valid_flags,
                          branches, merge_modes, default_branch):
        """Set all the branch info for a project at once

        :param str project_name:
            The project for the branches.
        :param BranchFlag valid_flags:
            The queries this list of branches is able to satisfy.
        :param list[str] branches:
            The list of branches or None to indicate a fetch error.
        :param list[int] merge_modes:
            The list of merge modes (by model ID) or None.
        :param str default_branch:
            The default branch or None.
        """
        # Set all three pieces of info with one lock and one active
        # context so that we only write the cache out once.
        with self._getActiveLockedProjectInfo(project_name) as project_info:
            if valid_flags is not None:
                self._setProjectBranches(
                    project_info, valid_flags, branches)
            self._setProjectMergeModes(project_info, merge_modes)
            self._setProjectDefaultBranch(project_info, default_branch)
        return project_info.getZKmzxid()

    def _setProjectInfoDirect(
            self, project_name, completed_flags,
            failed_flags, branches, merge_modes, default_branch):
        # Set the project info object directly; this is used during upgrade.
        with self._getActiveLockedProjectInfo(project_name) as project_info:
            project_info.completed_flags = completed_flags
            project_info.failed_flags = failed_flags
            project_info.branches = branches
            project_info.merge_modes = merge_modes
            project_info.default_branch = default_branch
        return project_info.getZKmzxid()

    @property
    def ltime(self):
        # Note: this can be -1 if we just started and have no events
        return self.cache.max_zxid
