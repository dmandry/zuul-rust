# Copyright 2025-2026 Acme Gating, LLC
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
import os
import os.path

from zuul.executor.sensors import SensorInterface
from zuul.executor.sensors import cgrouputils


class ProcessSensor(SensorInterface):
    log = logging.getLogger("zuul.executor.sensor.process")

    def __init__(self, statsd, base_key, config=None):
        super().__init__(statsd, base_key)
        # The executor and ansible require a number of processes to function
        # minimally: the executor itself, ansible, ssh control persistence,
        # ssh and so on. Set a minimum of room for 10 processes before we
        # stop.
        self._min_headroom = 10
        self._safety_factor = 0.05
        self._uid = os.getuid()
        self._pid_max = self._getPidMax()
        # This appears to be systemd specific behavior with cgroups that
        # reflects the ulimit values. This way we don't need to have a
        # separate system for ulimit checking.
        self._user_slice_root = ('/sys/fs/cgroup/user.slice'
                                 f'/user-{self._uid}.slice')
        if cgrouputils.get_cgroup() is None:
            self.log.warning("cgroups v2 not available")

    def _getPidMax(self):
        # Default for x86_64
        default = 2 ** 22
        path = '/proc/sys/kernel/pid_max'
        if os.path.exists(path):
            with open(path) as f:
                s = f.read().strip()
                try:
                    i = int(s)
                except ValueError:
                    self.log.exception('Unable to determine pid_max')
                    i = default
                return i
        else:
            return default

    def isOk(self):
        # Processes running in the root cgroup won't have these values
        # but containers do.
        cgroup_avail, cgroup_limit = self._getMinAvailProcessCgroup()
        # Processes running under systemd will have these values.
        user_avail, user_limit = self._getMinAvailProcessUserSlice()

        avail = min(cgroup_avail, user_avail)
        limit = min(cgroup_limit, user_limit)
        min_headroom = limit * self._safety_factor
        if min_headroom < self._min_headroom:
            min_headroom = self._min_headroom
        # This shouldn't ever be negative but I'm not sure if you can reduce
        # cgroup limits below the current usage at runtime.
        headroom = max(avail, 0)
        usage = limit - avail

        if self.statsd:
            self.statsd.gauge(self.base_key + '.max_process',
                              limit)
            self.statsd.gauge(self.base_key + '.cur_process',
                              usage)

        if min_headroom >= headroom:
            return False, f'high process utilization: {usage} max: {limit}'
        return True, f'process utilization: {usage} max: {limit}'

    def _getMinAvailProcessCgroup(self):
        # If no max is found assume pid_max. If no current usage is found
        # assume 1 for the current process.
        min_avail = self._pid_max
        min_limit = self._pid_max

        for path in cgrouputils.walk_cgroups_tree():
            cur_path = f'{path}/pids.current'
            try:
                with open(cur_path) as f:
                    usage = f.read().strip()
                usage = int(usage)
            except ValueError:
                self.log.exception('Unable to convert cgroup '
                                   'process value')
                usage = 1
            except FileNotFoundError:
                usage = 1

            max_path = f'{path}/pids.max'
            try:
                with open(max_path) as f:
                    limit = f.read().strip()
                if limit == 'max':
                    limit = self._pid_max
                else:
                    limit = int(limit)
            except ValueError:
                self.log.exception('Unable to convert cgroup '
                                   'process value')
                limit = self._pid_max
            except FileNotFoundError:
                limit = self._pid_max

            level_avail = limit - usage
            min_avail = min(min_avail, level_avail)
            min_limit = min(min_limit, limit)

        return min_avail, min_limit

    def _getMinAvailProcessUserSlice(self):
        # If no max is found assume pid_max. If no current usage is found
        # assume 1 for the current process.
        limit = self._getUserCgroupValue('pids.max')
        if limit is None:
            limit = self._pid_max
        usage = self._getUserCgroupValue('pids.current')
        if usage is None:
            usage = 1
        return limit - usage, limit

    def _getUserCgroupValue(self, path):
        path = os.path.join(self._user_slice_root, path)
        try:
            with open(path) as f:
                s = f.read().strip()
                if s == 'max':
                    return self._pid_max
                else:
                    return int(s)
        except ValueError:
            self.log.exception('Unable to convert cgroup '
                               'process value')
            return None
        except FileNotFoundError:
            return None
