# Copyright 2021 BMW Group
# Copyright 2021 Acme Gating, LLC
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
from urllib.parse import quote_plus, unquote

from kazoo.exceptions import BadVersionError, NoNodeError

from zuul.lib.logutil import get_annotated_logger
from zuul.model import SemaphoreReleaseEvent
from zuul.zk import ZooKeeperSimpleBase
from zuul.zk.event_queues import PipelineResultEventQueue


def holdersFromData(data):
    if not data:
        return []
    return json.loads(data.decode("utf8"))


def holdersToData(holders):
    return json.dumps(holders, sort_keys=True).encode("utf8")


class SemaphoreHandler(ZooKeeperSimpleBase):
    log = logging.getLogger("zuul.zk.SemaphoreHandler")

    semaphore_root = "/zuul/semaphores"
    global_semaphore_root = "/zuul/global-semaphores"

    def __init__(self, client, statsd, tenant_name, layout, abide,
                 read_only=False):
        super().__init__(client)
        if read_only:
            statsd = None
        self.read_only = read_only
        self.abide = abide
        self.layout = layout
        self.statsd = statsd
        self.tenant_name = tenant_name
        self.tenant_root = f"{self.semaphore_root}/{tenant_name}"

    def _makePath(self, semaphore):
        semaphore_key = quote_plus(semaphore.name)
        if semaphore.global_scope:
            return f"{self.global_semaphore_root}/{semaphore_key}"
        else:
            return f"{self.tenant_root}/{semaphore_key}"

    def _emitStats(self, semaphore_path, num_holders):
        if self.statsd is None:
            return
        try:
            is_global = semaphore_path.startswith(self.global_semaphore_root)
            semaphore_quoted = semaphore_path.split('/')[-1]
            semaphore_name = unquote(semaphore_quoted)
            # statsd safe key:
            semaphore_key = semaphore_name.replace('.', '_').replace('/', '_')
            if is_global:
                key = (f'zuul.global_semaphore.{semaphore_key}')
            else:
                key = (f'zuul.tenant.{self.tenant_name}'
                       f'.semaphore.{semaphore_key}')
            self.statsd.gauge(f'{key}.holders', num_holders)
        except Exception:
            self.log.exception("Unable to send semaphore stats:")

    def getSemaphoreInfo(self, job_semaphore):
        semaphore = self.layout.getSemaphore(self.abide, job_semaphore.name)
        return {
            'name': job_semaphore.name,
            'path': self._makePath(semaphore),
            'resources_first': job_semaphore.resources_first,
            'max': 1 if semaphore is None else semaphore.max,
        }

    def getSemaphoreHandle(self, item, job):
        return {
            "buildset_path": item.current_build_set.getPath(),
            "job_name": job.name,
        }

    def acquire(self, item, job, request_resources):
        # This is the typical method for acquiring semaphores.  It
        # runs on the scheduler and acquires all semaphores for a job.
        if self.read_only:
            raise RuntimeError("Read-only semaphore handler")
        if not job.semaphores:
            return True

        log = get_annotated_logger(self.log, item.event)
        handle = self.getSemaphoreHandle(item, job)
        infos = [self.getSemaphoreInfo(job_semaphore)
                 for job_semaphore in job.semaphores]

        return self.acquireFromInfo(log, infos, handle, request_resources)

    def acquireFromInfo(self, log, infos, handle, request_resources=False):
        # This method is used by the executor to acquire a playbook
        # semaphore; it is similar to the acquire method but the
        # semaphore info is frozen (this operates without an abide).
        if self.read_only:
            raise RuntimeError("Read-only semaphore handler")
        if not infos:
            return True

        all_acquired = True
        for info in infos:
            if not self._acquire_one(log, info, handle, request_resources):
                all_acquired = False
                break
        if not all_acquired:
            # Since we know we have less than all the required
            # semaphores, set quiet=True so we don't log an inability
            # to release them.
            self.releaseFromInfo(log, None, infos, handle, quiet=True)
            return False
        return True

    def _acquire_one(self, log, info, handle, request_resources):
        if info['resources_first'] and request_resources:
            # We're currently in the resource request phase and want to get the
            # resources before locking. So we don't need to do anything here.
            return True
        else:
            # As a safety net we want to acuire the semaphore at least in the
            # run phase so don't filter this here as re-acuiring the semaphore
            # is not a problem here if it has been already acquired before in
            # the resources phase.
            pass

        self.kazoo_client.ensure_path(info['path'])
        semaphore_holders, zstat = self.getHolders(info['path'])

        if handle in semaphore_holders:
            return True

        # semaphore is there, check max
        while len(semaphore_holders) < info['max']:
            semaphore_holders.append(handle)

            try:
                self.kazoo_client.set(info['path'],
                                      holdersToData(semaphore_holders),
                                      version=zstat.version)
            except BadVersionError:
                log.debug(
                    "Retrying semaphore %s acquire due to concurrent update",
                    info['name'])
                semaphore_holders, zstat = self.getHolders(info['path'])
                continue

            log.info("Semaphore %s acquired: handle %s",
                     info['name'], handle)
            self._emitStats(info['path'], len(semaphore_holders))
            return True

        return False

    def release(self, event_queue, item, job, quiet=False):
        if self.read_only:
            raise RuntimeError("Read-only semaphore handler")
        if not job.semaphores:
            return

        log = get_annotated_logger(self.log, item.event)

        handle = self.getSemaphoreHandle(item, job)
        infos = [self.getSemaphoreInfo(job_semaphore)
                 for job_semaphore in job.semaphores]

        return self.releaseFromInfo(log, event_queue, infos, handle,
                                    quiet=False)

    def releaseFromInfo(self, log, event_queue, infos, handle, quiet=False):
        for info in infos:
            self._release_one(log, info, handle, quiet)
            if event_queue is not None:
                # If a scheduler has been provided (which it is except
                # in the case of a rollback from acquire in this
                # class), broadcast an event to trigger pipeline runs.
                event = SemaphoreReleaseEvent(info['name'])
                if isinstance(event_queue, PipelineResultEventQueue):
                    event_queue.put(event)
                else:
                    event_queue.put(event, needs_result=False)

    def _release_one(self, log, info, handle, quiet=False):
        while True:
            try:
                semaphore_holders, zstat = self.getHolders(info['path'])
                semaphore_holders.remove(handle)
            except (ValueError, NoNodeError):
                if not quiet:
                    log.error("Semaphore %s can not be released for %s "
                              "because the semaphore is not held",
                              info['path'], handle)
                break

            try:
                self.kazoo_client.set(info['path'],
                                      holdersToData(semaphore_holders),
                                      zstat.version)
            except BadVersionError:
                log.debug(
                    "Retrying semaphore %s release due to concurrent update",
                    info['path'])
                continue

            log.info("Semaphore %s released for %s",
                     info['path'], handle)
            self._emitStats(info['path'], len(semaphore_holders))
            break

    def getHolders(self, semaphore_path):
        data, zstat = self.kazoo_client.get(semaphore_path)
        return holdersFromData(data), zstat

    def getSemaphores(self):
        ret = []
        for root in (self.global_semaphore_root, self.tenant_root):
            try:
                ret.extend(self.kazoo_client.get_children(root))
            except NoNodeError:
                pass
        return ret

    def semaphoreHolders(self, semaphore_name):
        semaphore = self.layout.getSemaphore(self.abide, semaphore_name)
        semaphore_path = self._makePath(semaphore)
        try:
            holders, _ = self.getHolders(semaphore_path)
        except NoNodeError:
            holders = []
        return holders

    def cleanupLeaks(self):
        if self.read_only:
            raise RuntimeError("Read-only semaphore handler")

        for semaphore_name in self.getSemaphores():
            for holder in self.semaphoreHolders(semaphore_name):
                if (self.kazoo_client.exists(holder["buildset_path"])
                        is not None):
                    continue

                semaphore = self.layout.getSemaphore(
                    self.abide, semaphore_name)
                info = {
                    'name': semaphore.name,
                    'path': self._makePath(semaphore),
                }
                self.log.error("Releasing leaked semaphore %s held by %s",
                               info['path'], holder)
                self._release_one(self.log, info, holder, quiet=False)
