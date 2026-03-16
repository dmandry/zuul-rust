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
import threading
from zuul.zk.components import COMPONENT_REGISTRY
from zuul.zk.locks import locked as zk_locked
from zuul.zk.branch_cache_old import BranchCacheOld
from zuul.zk.branch_cache_new import BranchCacheNew
from zuul.zk.branch_cache_common import (  # noqa
    BranchFlag,
    BranchInfo,
)


class BranchCache:
    def __init__(self, zk_client, connection, component_registry):
        self.log = logging.getLogger(
            f"zuul.BranchCache.{connection.connection_name}")

        self.zk_client = zk_client
        self.connection = connection
        self.component_registry = component_registry
        self._old_cache = None
        self._new_cache = None
        self._thread_lock = threading.Lock()

        self._makeOldCache()

    def _makeOldCache(self):
        self._old_cache = BranchCacheOld(
            self.zk_client, self.connection, self.component_registry)

    def _makeNewCache(self):
        self._new_cache = BranchCacheNew(
            self.zk_client, self.connection, self.component_registry)

    def _getOldOrNewCache(self):
        with self._thread_lock:
            # TODO: When we remove this backwards compat code, we
            # should add a migration to rmtree the old cache.
            if COMPONENT_REGISTRY.model_api < 37:
                return self._old_cache
            if self._new_cache:
                return self._new_cache
            return self._upgradeCache()

    def _upgradeCache(self):
        self.log.info("Waiting for branch cache upgrade lock")
        # Get the write lock (even though we don't write) because we
        # want exclusive access to the old cache for the upgrade (to
        # prevent anyone else from upgrading it at the same time).
        with (zk_locked(self._old_cache.wlock),
              self._old_cache.zk_context as ctx):
            self._old_cache.cache.refresh(ctx)

            base_path = BranchCacheNew.BASE_PATH_FORMAT.format(
                connection=self.connection.connection_name)

            # Check if it exists first since the next call will create it
            exists = self.zk_client.client.exists(base_path)
            new_cache = BranchCacheNew(
                self.zk_client, self.connection, self.component_registry)
            if exists:
                # Someone has already migrated this connection
                self.log.info("Cache already upgraded")
                self._new_cache = new_cache
                return self._new_cache

            self.log.info("Upgrading cache")
            # We just created it, migrate the old data
            for project_name, project_info in \
                    self._old_cache.cache.projects.items():
                self.log.info("Upgrading project %s (object loading errors "
                              "during upgrade are normal)", project_name)
                new_cache._setProjectInfoDirect(
                    project_name,
                    project_info.completed_flags,
                    project_info.failed_flags,
                    project_info.branches,
                    project_info.merge_modes,
                    project_info.default_branch,
                )

            # Note: this process creates new zxids, so the ltime of
            # the new branch cache will always be greater than the old
            # one, meaning that it will satisfy any min_ltime.
            self._new_cache = new_cache
            return self._new_cache

    def clear(self, *args, **kw):
        return self._getOldOrNewCache().clear(*args, **kw)

    def getProjectCompletedFlags(self, *args, **kw):
        return self._getOldOrNewCache().getProjectCompletedFlags(*args, **kw)

    def getProjectBranches(self, *args, **kw):
        return self._getOldOrNewCache().getProjectBranches(*args, **kw)

    def setProjectBranches(self, *args, **kw):
        return self._getOldOrNewCache().setProjectBranches(*args, **kw)

    def setProtected(self, *args, **kw):
        return self._getOldOrNewCache().setProtected(*args, **kw)

    def getProjectMergeModes(self, *args, **kw):
        return self._getOldOrNewCache().getProjectMergeModes(*args, **kw)

    def setProjectMergeModes(self, *args, **kw):
        return self._getOldOrNewCache().setProjectMergeModes(*args, **kw)

    def getProjectDefaultBranch(self, *args, **kw):
        return self._getOldOrNewCache().getProjectDefaultBranch(*args, **kw)

    def setProjectDefaultBranch(self, *args, **kw):
        return self._getOldOrNewCache().setProjectDefaultBranch(*args, **kw)

    def setAllProjectData(self, *args, **kw):
        return self._getOldOrNewCache().setAllProjectData(*args, **kw)

    @property
    def ltime(self):
        return self._getOldOrNewCache().ltime
