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

import time

from tests.base import ZuulTestCase, iterate_timeout


class TestTimerTwoTenants(ZuulTestCase):
    tenant_config_file = 'config/timer-two-tenant/main.yaml'

    def test_timer_two_tenants(self):
        # The pipeline triggers every second.  Wait until we have 4
        # jobs (2 from each tenant).
        for _ in iterate_timeout(60, 'jobs started'):
            if len(self.history) >= 4:
                break

        tenant_one_projects = set()
        tenant_two_projects = set()
        for h in self.history:
            if h.parameters['zuul']['tenant'] == 'tenant-one':
                tenant_one_projects.add((h.parameters['items'][0]['project']))
            if h.parameters['zuul']['tenant'] == 'tenant-two':
                tenant_two_projects.add((h.parameters['items'][0]['project']))

        # Verify that the right job ran in the right tenant
        self.assertEqual(tenant_one_projects, {'org/project1'})
        self.assertEqual(tenant_two_projects, {'org/project2'})

        # Stop running timer jobs so the assertions don't race.
        self.commitConfigUpdate('common-config', 'layouts/no-timer.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()


class TestTimerAlwaysDynamicBranches(ZuulTestCase):
    tenant_config_file = 'config/dynamic-only-project/dynamic.yaml'

    def test_exclude_always_dynamic_branches(self):
        # Test that no timer events are emitted for always dynamic branches.
        self.create_branch('org/project', 'stable')
        self.create_branch('org/project', 'feature/foo')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable'))
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'feature/foo'))
        self.executor_server.hold_jobs_in_build = True
        self.commitConfigUpdate('common-config', 'layouts/timer.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))

        # The pipeline triggers every second, so we should have seen
        # several by now.
        for _ in iterate_timeout(60, 'jobs started'):
            if len(self.builds) > 1:
                break

        timer = self.scheds.first.sched.connections.drivers['timer']
        timer_jobs = timer.apsched.get_jobs()
        self.assertEqual(len(timer_jobs), 2)

        # Ensure that the status json has the ref so we can render it in the
        # web ui.
        manager = self.scheds.first.sched.abide.tenants[
            'tenant-one'].layout.pipeline_managers['periodic']
        self.assertEqual(len(manager.state.queues), 2)
        for queue in manager.state.queues:
            item = queue.queue[0]
            self.assertIn(item.changes[0].branch, ['master', 'stable'])

        self.executor_server.hold_jobs_in_build = False

        # Stop queuing timer triggered jobs so that the assertions
        # below don't race against more jobs being queued.
        self.commitConfigUpdate('common-config', 'layouts/no-timer.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))

        # If APScheduler is in mid-event when we remove the job, we
        # can end up with one more event firing, so give it an extra
        # second to settle.
        time.sleep(3)
        self.waitUntilSettled()

        self.executor_server.release()
        self.waitUntilSettled()

        self.assertTrue(len(self.history) > 1)
        for job in self.history[:1]:
            self.assertEqual(job.result, 'SUCCESS')
            self.assertEqual(job.name, 'project-bitrot')
            self.assertIn(job.ref, ('refs/heads/stable', 'refs/heads/master'))


class TestTimerBranchFilter(ZuulTestCase):
    tenant_config_file = 'config/dynamic-only-project/dynamic.yaml'

    def test_timer_branch_filter(self):
        """Test that timer triggers can filter by branch patterns."""
        # Create additional branches
        self.create_branch('org/project', 'stable/train')
        self.create_branch('org/project', 'stable/ussuri')
        self.create_branch('org/project', 'feature/experimental')

        # Notify scheduler about new branches
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable/train'))
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable/ussuri'))
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'feature/experimental'))
        self.waitUntilSettled()

        # Apply timer configuration with branch filters
        self.executor_server.hold_jobs_in_build = True
        self.commitConfigUpdate('common-config',
                                'layouts/timer-branch-filter.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))

        # Wait for timer events to trigger (pipeline triggers every second)
        for _ in iterate_timeout(60, 'timer jobs started'):
            if len(self.builds) >= 3:
                break

        # Verify jobs are running
        self.assertTrue(len(self.builds) >= 3,
                        f"Expected at least 3 builds, got {len(self.builds)}")

        # Check which branches got timer events
        master_jobs = []
        stable_jobs = []
        feature_jobs = []

        for build in self.builds:
            if 'master' in build.parameters['zuul']['branch']:
                master_jobs.append(build)
            elif 'stable' in build.parameters['zuul']['branch']:
                stable_jobs.append(build)
            elif 'feature' in build.parameters['zuul']['branch']:
                feature_jobs.append(build)

        # Verify master branch timer triggered
        self.assertTrue(len(master_jobs) > 0,
                        "Timer should trigger for master branch")

        # Verify stable branches timer triggered
        self.assertTrue(len(stable_jobs) > 0,
                        "Timer should trigger for stable branches")

        # Verify feature branch did NOT trigger
        # (since our timer filters only match master and stable/*)
        self.assertEqual(len(feature_jobs), 0,
                         "Timer should NOT trigger for feature branches")

        # Verify the correct jobs ran
        for build in master_jobs:
            self.assertEqual(build.name, 'project-master-periodic')

        for build in stable_jobs:
            self.assertEqual(build.name, 'project-stable-periodic')

        # Clean up - stop timer first to prevent more jobs
        self.commitConfigUpdate('common-config', 'layouts/no-timer.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        # Give APScheduler time to stop
        time.sleep(3)
        self.waitUntilSettled()

        # Now release the held jobs
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()
