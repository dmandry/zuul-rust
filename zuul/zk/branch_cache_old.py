# Copyright 2014 Rackspace Australia
# Copyright 2021 BMW Group
# Copyright 2021, 2024 Acme Gating, LLC
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
import json

from zuul.zk.zkobject import ZKContext, ShardedZKObject
from zuul.zk.locks import (
    SessionAwareReadLock,
    SessionAwareWriteLock,
    locked as zk_locked
)
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


class ProjectInfo:
    """Store branch cache project information in ZK

    If a project is absent from the cache, it needs to be queried from
    the source.
    """
    def __init__(self, name, merge_modes=None, default_branch=None):
        self.name = name
        self.merge_modes = merge_modes
        self.default_branch = default_branch
        self.branches = {}
        # The set of flags we have performed queries for:
        self.completed_flags = BranchFlag.CLEAR
        # If there was an error fetching the branches for a given set
        # of flags, the failure will be recorded here:
        self.failed_flags = BranchFlag.CLEAR

    def toDict(self):
        return {
            'merge_modes': self.merge_modes,
            'default_branch': self.default_branch,
            'branches': {b.name: b.toDict() for b in self.branches.values()},
            'flags': [
                self.completed_flags,
                self.failed_flags,
            ],
        }

    @classmethod
    def fromDict(cls, name, data):
        o = cls(name)
        o.merge_modes = data['merge_modes']
        o.default_branch = data['default_branch']
        o.branches = {
            name: BranchInfo.fromDict(name, bdata)
            for name, bdata in data['branches'].items()
        }
        o.completed_flags = BranchFlag(data['flags'][0])
        o.failed_flags = BranchFlag(data['flags'][1])
        return o


class BranchCacheZKObject(ShardedZKObject):
    """Store the branch cache in ZK

    If a project is absent from the dict, it needs to be queried from
    the source.

    If there was an error fetching the branches, None will be stored
    as a sentinel value.
    """

    # We can always recreate data if necessary, so go ahead and
    # truncate when we update so we avoid corrupted data.
    truncate_on_create = True

    def getPath(self):
        return self._path

    def __init__(self):
        super().__init__()
        self._set(
            projects={},
        )

    def serialize(self, context):
        data = {
            "projects": {p.name: p.toDict() for p in self.projects.values()},
        }
        return json.dumps(data, sort_keys=True).encode("utf8")

    def deserialize(self, raw, context, extra=None):
        data = super().deserialize(raw, context, extra)
        projects = {}
        for project_name, project_data in data['projects'].items():
            projects[project_name] = ProjectInfo.fromDict(
                project_name, project_data)
        data['projects'] = projects
        return data


class BranchCacheOld:
    def __init__(self, zk_client, connection, component_registry):
        self.log = logging.getLogger(
            f"zuul.BranchCacheOld.{connection.connection_name}")

        self.connection = connection

        cname = self.connection.connection_name
        base_path = f'/zuul/cache/connection/{cname}/branches'
        lock_path = f'{base_path}/lock'
        data_path = f'{base_path}/data'

        self.rlock = SessionAwareReadLock(zk_client.client, lock_path)
        self.wlock = SessionAwareWriteLock(zk_client.client, lock_path)

        # TODO: standardize on a stop event for connections and add it
        # to the context.
        self.zk_context = ZKContext(zk_client, self.wlock, None, self.log)

        with (self.zk_context as ctx,
              zk_locked(self.wlock)):
            try:
                self.cache = BranchCacheZKObject.fromZK(
                    ctx, data_path, _path=data_path)
            except NoNodeError:
                self.cache = BranchCacheZKObject.new(
                    ctx, _path=data_path)

    def clear(self, projects=None):
        """Clear the cache"""
        with (zk_locked(self.wlock),
              self.zk_context as ctx,
              self.cache.activeContext(ctx)):
            if projects is None:
                self.cache.projects.clear()
            else:
                for p in projects:
                    self.cache.projects.pop(p, None)

    def getProjectCompletedFlags(self, project_name):
        """Get the completed branch query flags for a project

        :param str project_name: The project name

        :returns: a BranchFlag of the completed query flags
        """
        try:
            project_info = self.cache.projects[project_name]
        except KeyError:
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
        if self.ltime < min_ltime:
            with (zk_locked(self.rlock),
                  self.zk_context as ctx):
                self.cache.refresh(ctx)

        project_info = None
        try:
            project_info = self.cache.projects[project_name]
        except KeyError:
            return return_default(default, project_name)

        # We've definitely stored a failure, so return that.
        if project_info is None:
            return None

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
        with (zk_locked(self.wlock),
              self.zk_context as ctx,
              self.cache.activeContext(ctx)):
            self._setProjectBranches(project_name,
                                     valid_flags, branch_infos)

    def _setProjectBranches(self, project_name,
                            valid_flags, branch_infos):
        project_info = self.cache.projects.get(project_name)
        if project_info is None:
            project_info = ProjectInfo(project_name)
            self.cache.projects[project_name] = project_info

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
        with (zk_locked(self.wlock),
              self.zk_context as ctx,
              self.cache.activeContext(ctx)):

            project_info = self.cache.projects.get(project_name)
            if project_info is None:
                project_info = ProjectInfo(project_name)
                self.cache.projects[project_name] = project_info

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
        if self.ltime < min_ltime:
            with (zk_locked(self.rlock),
                  self.zk_context as ctx):
                self.cache.refresh(ctx)

        project_info = None
        try:
            project_info = self.cache.projects[project_name]
        except KeyError:
            return return_default(default, project_name)

        if project_info is None:
            return None

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
        with (zk_locked(self.wlock),
              self.zk_context as ctx,
              self.cache.activeContext(ctx)):
            self._setProjectMergeModes(project_name, merge_modes)

    def _setProjectMergeModes(self, project_name, merge_modes):
        project_info = self.cache.projects.get(project_name)
        if project_info is None:
            project_info = ProjectInfo(project_name)
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
        if self.ltime < min_ltime:
            with (zk_locked(self.rlock),
                  self.zk_context as ctx):
                self.cache.refresh(ctx)

        project_info = None
        try:
            project_info = self.cache.projects[project_name]
        except KeyError:
            return return_default(default, project_name)

        if project_info is None:
            return None

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
        with (zk_locked(self.wlock),
              self.zk_context as ctx,
              self.cache.activeContext(ctx)):
            self._setProjectDefaultBranch(project_name, default_branch)

    def _setProjectDefaultBranch(self, project_name, default_branch):
        """Set the upstream default branch for the given project.

        Use None as a sentinel value for the default branch to indicate
        that there was a fetch error.

        :param str project_name:
            The project for the default branch.
        :param str default_branch:
            The default branch or None.

        """
        project_info = self.cache.projects.get(project_name)
        if project_info is None:
            project_info = ProjectInfo(project_name)
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
        with (zk_locked(self.wlock),
              self.zk_context as ctx,
              self.cache.activeContext(ctx)):
            if valid_flags is not None:
                self._setProjectBranches(project_name, valid_flags, branches)
            self._setProjectMergeModes(project_name, merge_modes)
            self._setProjectDefaultBranch(project_name, default_branch)

    @property
    def ltime(self):
        return self.cache._zstat.last_modified_transaction_id
