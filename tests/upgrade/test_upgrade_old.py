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

from tests.unit.test_launcher import LauncherBaseTestCase


class TestUpgradeOld(LauncherBaseTestCase):
    tenant_config_file = "config/single-tenant/main-launcher.yaml"
    scheduler_count = 1
    random_databases = False
    delete_databases = False
    use_tmpdir = False
    init_repos = True
    load_change_db = False
    always_attach_logs = True

    def assertFinalState(self):
        # In this test, we expect to shut down in a non-final state,
        # so skip these checks.
        pass

    def shutdown(self):
        # Shutdown the scheduler now before it gets any aborted events
        if self.validate_tenants is None:
            self.scheds.execute(lambda app: app.sched.stop())
            self.scheds.execute(lambda app: app.sched.join())
        else:
            self.scheds.execute(lambda app: app.sched.stopConnections())
        # Then release the executor jobs and stop the executors
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.scheds.execute(lambda app: app.sched.executor.stop())
        if self.merge_server:
            self.merge_server.stop()
            self.merge_server.join()

        self.executor_server.stop()
        self.executor_server.join()
        self.launcher.stop()
        self.launcher.join()
        self.statsd.stop()
        self.statsd.join()
        self.fake_nodepool.stop()
        self.zk_client.disconnect()
        self.printHistory()

    def test_upgrade(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        self.assertHistory([])
        self.assertEqual(len(self.builds), 1)
        self.executor_server.release()
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
        ], ordered=False)
        self.assertEqual(len(self.builds), 2)
        self.saveChangeDB()
