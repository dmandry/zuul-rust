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

import unittest
from unittest import mock

from zuul.lib import cgroup


class TestCgroup(unittest.TestCase):

    def test_cgroup_v2_quota(self):
        m = mock.mock_open(read_data='900000 100000\n')
        with mock.patch('builtins.open', m):
            self.assertEqual(cgroup.cgroup_cpu_quota(), 9)

    def test_cgroup_v2_max(self):
        m = mock.mock_open(read_data='max 100000\n')
        with mock.patch('builtins.open', m):
            self.assertIsNone(cgroup.cgroup_cpu_quota())

    def test_cgroup_v2_zero_period(self):
        m = mock.mock_open(read_data='900000 0\n')
        with mock.patch('builtins.open', m):
            self.assertIsNone(cgroup.cgroup_cpu_quota())

    def test_cgroup_v1_quota(self):
        def fake_open(path, *args, **kwargs):
            if path == '/sys/fs/cgroup/cpu.max':
                raise FileNotFoundError(path)
            if path.endswith('cpu.cfs_quota_us'):
                return mock.mock_open(read_data='900000\n')()
            if path.endswith('cpu.cfs_period_us'):
                return mock.mock_open(read_data='100000\n')()
            raise FileNotFoundError(path)

        with mock.patch('builtins.open', side_effect=fake_open):
            self.assertEqual(cgroup.cgroup_cpu_quota(), 9)

    def test_cgroup_v1_unlimited(self):
        def fake_open(path, *args, **kwargs):
            if path == '/sys/fs/cgroup/cpu.max':
                raise FileNotFoundError(path)
            if path.endswith('cpu.cfs_quota_us'):
                return mock.mock_open(read_data='-1\n')()
            if path.endswith('cpu.cfs_period_us'):
                return mock.mock_open(read_data='100000\n')()
            raise FileNotFoundError(path)

        with mock.patch('builtins.open', side_effect=fake_open):
            self.assertIsNone(cgroup.cgroup_cpu_quota())

    def test_no_cgroup_files(self):
        with mock.patch('builtins.open', side_effect=FileNotFoundError):
            self.assertIsNone(cgroup.cgroup_cpu_quota())

    def test_cpu_count_falls_back(self):
        with mock.patch('builtins.open', side_effect=FileNotFoundError), \
             mock.patch('multiprocessing.cpu_count', return_value=16):
            self.assertEqual(cgroup.cpu_count(), 16)

    def test_cpu_count_uses_quota(self):
        m = mock.mock_open(read_data='900000 100000\n')
        with mock.patch('builtins.open', m), \
             mock.patch('multiprocessing.cpu_count', return_value=16):
            self.assertEqual(cgroup.cpu_count(), 9)
