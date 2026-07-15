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

import multiprocessing


def cgroup_cpu_quota():
    """Return the number of CPUs allotted to this process's cgroup.

    :returns: An int with the number of CPUs available under the
        current cgroup cpu bandwidth quota (rounded down, minimum 1),
        or None if no quota is set (or the cgroup cpu controller
        isn't available).
    """
    # cgroup v2
    try:
        with open('/sys/fs/cgroup/cpu.max') as f:
            quota_str, period_str = f.read().split()
        if quota_str != 'max':
            period = int(period_str)
            if period > 0:
                return max(1, int(quota_str) // period)
    except (OSError, ValueError):
        pass

    # cgroup v1
    try:
        with open('/sys/fs/cgroup/cpu/cpu.cfs_quota_us') as f:
            quota = int(f.read().strip())
        with open('/sys/fs/cgroup/cpu/cpu.cfs_period_us') as f:
            period = int(f.read().strip())
        if quota > 0 and period > 0:
            return max(1, quota // period)
    except (OSError, ValueError):
        pass

    return None


def cpu_count():
    """Return the number of CPUs available to this process.

    Like multiprocessing.cpu_count(), but takes the cgroup cpu
    bandwidth quota into account if one is set (for example when
    running under a Kubernetes pod with resources.limits.cpu
    configured to less than the host's full core count).
    multiprocessing.cpu_count()/os.cpu_count() report the host's total
    core count regardless of cgroup or scheduler affinity
    restrictions, which can cause components that size worker pools
    or load thresholds from the CPU count to oversubscribe the
    quota actually available to them.

    :returns: An int with the number of CPUs to use for sizing
        purposes.
    """
    quota = cgroup_cpu_quota()
    if quota is not None:
        return quota
    return multiprocessing.cpu_count()
