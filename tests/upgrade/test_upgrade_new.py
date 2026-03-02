# Copyright 2024 Acme Gating, LLC
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

from tests.base import iterate_timeout
from tests.unit.test_launcher import LauncherBaseTestCase


class TestUpgradeNew(LauncherBaseTestCase):
    tenant_config_file = "config/single-tenant/main-launcher.yaml"
    scheduler_count = 1
    random_databases = False
    delete_databases = True
    use_tmpdir = False
    init_repos = False
    load_change_db = True
    always_attach_logs = True

    def assertFinalState(self):
        # In this test, we expect to shut down in a non-final state,
        # so skip these checks.
        pass

    def test_upgrade(self):
        A = self.fake_gerrit.changes[1]

        # Resume builds
        for _ in iterate_timeout(60, 'wait for build'):
            if self.history:
                break
        self.waitUntilSettled()

        # This was performed in the previous run
        # dict(name='project-merge', result='SUCCESS', changes='1,1'),
        self.assertHistory([
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
        ], ordered=False)
        self.assertEqual(len(self.builds), 0)

        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(A.reported, 2)
        # Make sure all 3 jobs are in the report.
        self.assertIn('project-merge', A.messages[-1])
        self.assertIn('project-test1', A.messages[-1])
        self.assertIn('project-test2', A.messages[-1])
