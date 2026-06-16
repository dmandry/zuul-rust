# Copyright 2018 BMW Car IT GmbH
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
import math
import psutil

from zuul.executor.sensors import SensorInterface
from zuul.executor.sensors import cgrouputils
from zuul.lib.config import get_default


def getAvailMemPct():
    avail_mem_pct = 100.0 - psutil.virtual_memory().percent
    return avail_mem_pct


class RAMSensor(SensorInterface):
    log = logging.getLogger("zuul.executor.sensor.ram")
    # This is to facilitate unit testing
    CGROUP_ROOT = '/sys/fs/cgroup'
    CGROUP_SELF = '/proc/self/cgroup'

    def __init__(self, statsd, base_key, config=None):
        super().__init__(statsd, base_key)
        self.min_avail_mem = float(get_default(config, 'executor',
                                               'min_avail_mem', '5.0'))
        if cgrouputils.get_cgroup() is None:
            self.log.warning("cgroups v2 not available")

    def _getAvailMemPctCgroup(self):
        min_pct = 100.0
        min_limit = math.inf

        for path in cgrouputils.walk_cgroups_tree():
            cur_path = f'{path}/memory.current'
            try:
                with open(cur_path) as f:
                    usage = f.read().strip()
                usage = int(usage)
            except FileNotFoundError:
                usage = 0

            max_path = f'{path}/memory.max'
            try:
                with open(max_path) as f:
                    limit = f.read().strip()
                if limit == 'max':
                    limit = math.inf
                else:
                    limit = int(limit)
            except FileNotFoundError:
                limit = math.inf

            level_pct = 100.0 - usage / limit * 100
            min_pct = min(min_pct, level_pct)
            min_limit = min(min_limit, limit)

        return min_pct, min_limit

    def isOk(self):
        avail_mem_pct = getAvailMemPct()
        avail_mem_pct_cgroup, cgroup_limit = self._getAvailMemPctCgroup()

        if self.statsd:
            self.statsd.gauge(self.base_key + '.pct_used_ram',
                              int((100.0 - avail_mem_pct) * 100))
            if math.isfinite(cgroup_limit):
                self.statsd.gauge(self.base_key + '.pct_used_ram_cgroup',
                                  int((100.0 - avail_mem_pct_cgroup) * 100))

        if avail_mem_pct < self.min_avail_mem:
            return False, "low memory {:3.1f}% < {}%".format(
                avail_mem_pct, self.min_avail_mem)

        if math.isinf(cgroup_limit):
            # we have no cgroup defined limit so we're done now
            return True, "memory {:3.1f}% >= {}%".format(
                avail_mem_pct, self.min_avail_mem)

        if avail_mem_pct_cgroup < self.min_avail_mem:
            return False, "low memory cgroup {:3.1f}% < {}%".format(
                avail_mem_pct_cgroup, self.min_avail_mem)

        return True, "memory cgroup {:3.1f}% >= {}%, {:3.1f}% >= {}%".format(
            avail_mem_pct, self.min_avail_mem,
            avail_mem_pct_cgroup, self.min_avail_mem)
