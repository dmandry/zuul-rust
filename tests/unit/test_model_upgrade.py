# Copyright 2022, 2024 Acme Gating, LLC
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
import os

from zuul.zk.components import (
    ComponentRegistry,
)
from tests.base import (
    ZuulTestCase,
    gerrit_config,
    simple_layout,
    iterate_timeout,
    model_version,
    FIXTURE_DIR,
)
from zuul import model
from zuul.zk.locks import management_queue_lock


class TestModelUpgrade(ZuulTestCase):
    tenant_config_file = "config/single-tenant/main-model-upgrade.yaml"
    scheduler_count = 1

    def getJobData(self, tenant, pipeline):
        item_path = f'/zuul/tenant/{tenant}/pipeline/{pipeline}/item'
        count = 0
        for item in self.zk_client.client.get_children(item_path):
            bs_path = f'{item_path}/{item}/buildset'
            for buildset in self.zk_client.client.get_children(bs_path):
                data = json.loads(self.getZKObject(
                    f'{bs_path}/{buildset}/job/check-job'))
                count += 1
                yield data
        if not count:
            raise Exception("No job data found")

    @model_version(0)
    @simple_layout('layouts/simple.yaml')
    def test_model_upgrade_0_1(self):
        component_registry = ComponentRegistry(self.zk_client)
        self.assertEqual(component_registry.model_api, 0)

        # Upgrade our component
        self.model_test_component_info.model_api = 1

        for _ in iterate_timeout(30, "model api to update"):
            if component_registry.model_api == 1:
                break

    @model_version(33)
    def test_model_upgrade_33_34(self):

        attrs = model.SystemAttributes.fromDict({
            "use_relative_priority": True,
            "max_hold_expiration": 7200,
            "default_hold_expiration": 3600,
            "default_ansible_version": "X",
            "web_root": "/web/root",
            "websocket_url": "/web/socket",
            "web_status_url": "ignored",
        })

        attr_dict = attrs.toDict()
        self.assertIn("web_status_url", attr_dict)
        self.assertEqual(attr_dict["web_status_url"], "")

        # Upgrade our component
        self.model_test_component_info.model_api = 34

        component_registry = ComponentRegistry(self.zk_client)
        for _ in iterate_timeout(30, "model api to update"):
            if component_registry.model_api == 34:
                break

        attr_dict = attrs.toDict()
        self.assertNotIn("web_status_url", attr_dict)

    @model_version(36)
    @simple_layout('layouts/simple.yaml')
    def test_model_upgrade_36_37(self):
        component_registry = ComponentRegistry(self.zk_client)
        self.assertEqual(component_registry.model_api, 36)
        self.waitUntilSettled()

        first = self.scheds.first

        tenant1 = first.sched.abide.tenants.get('tenant-one')
        connection1 = first.connections.connections['gerrit']
        source1 = connection1.source
        project1 = source1.getProject('org/project')
        branches1 = connection1.getProjectBranches(project1, tenant1)
        self.assertEqual(['master'], branches1)

        second = self.createScheduler()
        second.start()

        self.assertEqual(len(self.scheds), 2)
        for _ in iterate_timeout(10, "until priming is complete"):
            state_one = first.sched.local_layout_state.get("tenant-one")
            if state_one:
                break

        for _ in iterate_timeout(
                10, "all schedulers to have the same layout state"):
            if (second.sched.local_layout_state.get(
                    "tenant-one") == state_one):
                break

        tenant2 = second.sched.abide.tenants.get('tenant-one')
        connection2 = second.connections.connections['gerrit']
        source2 = connection2.source
        project2 = source2.getProject('org/project')

        old_ltime1 = connection1._branch_cache._old_cache.ltime
        old_ltime2 = connection2._branch_cache._old_cache.ltime
        new_ltime1 = connection1._branch_cache._new_cache
        new_ltime2 = connection2._branch_cache._new_cache
        self.assertEqual(old_ltime1, old_ltime2)
        self.assertEqual(None, new_ltime1)
        self.assertEqual(None, new_ltime2)
        # Remember this ltime for later
        stage1_ltime = old_ltime1

        # Upgrade our component
        self.model_test_component_info.model_api = 37

        for _ in iterate_timeout(30, "model api to update"):
            if component_registry.model_api == 37:
                break

        self.log.debug("Trigger upgrade on scheduler-0 and check")
        branches1 = connection1.getProjectBranches(project1, tenant1)
        self.assertEqual(['master'], branches1)

        old_ltime1 = connection1._branch_cache._old_cache.ltime
        old_ltime2 = connection2._branch_cache._old_cache.ltime
        new_ltime1 = connection1._branch_cache._new_cache.ltime
        new_ltime2 = connection2._branch_cache._new_cache
        self.assertEqual(stage1_ltime, old_ltime1)
        self.assertEqual(stage1_ltime, old_ltime2)
        self.assertNotEqual(None, new_ltime1)
        self.assertEqual(None, new_ltime2)
        self.assertTrue(new_ltime1 > stage1_ltime)
        stage2_ltime = new_ltime1

        self.log.debug("Trigger upgrade on scheduler-1 and check")
        event = model.TriggerEvent()
        event.zuul_event_ltime = self.zk_client.getCurrentLtime()
        first.sched.reconfigureTenant(tenant1, project1, event)
        state_two = first.sched.local_layout_state.get("tenant-one")
        for _ in iterate_timeout(
                10, "all schedulers to have the same layout state"):
            if (second.sched.local_layout_state.get(
                    "tenant-one") == state_two):
                break

        with first.sched.run_handler_lock:
            A = self.fake_gerrit.addFakeChange('org/project', "master", "A")
            self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
            self.waitUntilSettled(matcher=[second])

        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,1'),
        ], ordered=False)

        # The ltime checks will tell us the cache has upgraded without
        # triggering an upgrade (like getProjectBranches would), so
        # check them first.
        old_ltime1 = connection1._branch_cache._old_cache.ltime
        old_ltime2 = connection2._branch_cache._old_cache.ltime
        new_ltime1 = connection1._branch_cache._new_cache.ltime
        new_ltime2 = connection2._branch_cache._new_cache.ltime
        self.assertEqual(stage1_ltime, old_ltime1)
        self.assertEqual(stage1_ltime, old_ltime2)
        self.assertEqual(stage2_ltime, new_ltime1)
        self.assertEqual(stage2_ltime, new_ltime2)

        branches2 = connection2.getProjectBranches(project2, tenant2)
        self.assertEqual(['master'], branches2)

    @model_version(36)
    @simple_layout('layouts/simple.yaml')
    def test_model_upgrade_36_37_serial(self):
        # Test that the first component to start with no old-api
        # components running is the one that performs the upgrade.
        component_registry = ComponentRegistry(self.zk_client)
        self.assertEqual(component_registry.model_api, 36)
        self.waitUntilSettled()

        first = self.scheds.first

        tenant1 = first.sched.abide.tenants.get('tenant-one')
        connection1 = first.connections.connections['gerrit']
        source1 = connection1.source
        project1 = source1.getProject('org/project')
        branches1 = connection1.getProjectBranches(project1, tenant1)
        self.assertEqual(['master'], branches1)

        for _ in iterate_timeout(10, "until priming is complete"):
            state_one = first.sched.local_layout_state.get("tenant-one")
            if state_one:
                break

        old_ltime1 = connection1._branch_cache._old_cache.ltime
        new_ltime1 = connection1._branch_cache._new_cache
        self.assertEqual(None, new_ltime1)

        # Remember this ltime for later
        stage1_ltime = old_ltime1

        # Upgrade our component
        self.model_test_component_info.model_api = 37
        for _ in iterate_timeout(30, "model api to update"):
            if component_registry.model_api == 37:
                break

        self.log.debug("BranchCache start scheduler-1")
        second = self.createScheduler()
        second.start()

        for _ in iterate_timeout(
                10, "all schedulers to have the same layout state"):
            if (second.sched.local_layout_state.get(
                    "tenant-one") == state_one):
                break

        self.log.debug("BranchCache done start scheduler-1")
        tenant2 = second.sched.abide.tenants.get('tenant-one')
        connection2 = second.connections.connections['gerrit']
        source2 = connection2.source
        project2 = source2.getProject('org/project')

        old_ltime1 = connection1._branch_cache._old_cache.ltime
        old_ltime2 = connection2._branch_cache._old_cache.ltime
        new_ltime1 = connection1._branch_cache._new_cache
        new_ltime2 = connection2._branch_cache._new_cache.ltime
        self.assertEqual(stage1_ltime, old_ltime1)
        self.assertEqual(stage1_ltime, old_ltime2)
        self.assertEqual(None, new_ltime1)
        self.assertNotEqual(None, new_ltime2)
        self.assertTrue(new_ltime2 > stage1_ltime)
        stage2_ltime = new_ltime2

        self.log.debug("Trigger upgrade on scheduler-0 and check")
        event = model.TriggerEvent()
        event.zuul_event_ltime = self.zk_client.getCurrentLtime()
        second.sched.reconfigureTenant(tenant1, project1, event)
        state_two = second.sched.local_layout_state.get("tenant-one")
        for _ in iterate_timeout(
                10, "all schedulers to have the same layout state"):
            if (first.sched.local_layout_state.get(
                    "tenant-one") == state_two):
                break

        with second.sched.run_handler_lock:
            A = self.fake_gerrit.addFakeChange('org/project', "master", "A")
            self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
            self.waitUntilSettled(matcher=[first])

        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,1'),
        ], ordered=False)

        # The ltime checks will tell us the cache has upgraded without
        # triggering an upgrade (like getProjectBranches would), so
        # check them first.
        old_ltime1 = connection1._branch_cache._old_cache.ltime
        old_ltime2 = connection2._branch_cache._old_cache.ltime
        new_ltime1 = connection1._branch_cache._new_cache.ltime
        new_ltime2 = connection2._branch_cache._new_cache.ltime
        self.assertEqual(stage1_ltime, old_ltime1)
        self.assertEqual(stage1_ltime, old_ltime2)
        self.assertEqual(stage2_ltime, new_ltime1)
        self.assertEqual(stage2_ltime, new_ltime2)

        branches2 = connection2.getProjectBranches(project2, tenant2)
        self.assertEqual(['master'], branches2)


class TestModelUpgradeGerritCircularDependencies(ZuulTestCase):
    config_file = "zuul-gerrit-github.conf"
    tenant_config_file = "config/circular-dependencies/main.yaml"

    @model_version(31)
    @gerrit_config(submit_whole_topic=True)
    def test_model_31_32(self):
        self.executor_server.hold_jobs_in_build = True

        A = self.fake_gerrit.addFakeChange('org/project1', "master", "A",
                                           topic='test-topic')
        B = self.fake_gerrit.addFakeChange('org/project2', "master", "B",
                                           topic='test-topic')

        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        B.addApproval("Approved", 1)

        self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
        self.waitUntilSettled()

        first = self.scheds.first
        second = self.createScheduler()
        second.start()
        self.assertEqual(len(self.scheds), 2)
        for _ in iterate_timeout(10, "until priming is complete"):
            state_one = first.sched.local_layout_state.get("tenant-one")
            if state_one:
                break

        for _ in iterate_timeout(
                10, "all schedulers to have the same layout state"):
            if (second.sched.local_layout_state.get(
                    "tenant-one") == state_one):
                break

        self.model_test_component_info.model_api = 32
        with first.sched.layout_update_lock, first.sched.run_handler_lock:
            self.fake_gerrit.addEvent(A.addApproval("Approved", 1))
            self.waitUntilSettled(matcher=[second])

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()
        self.assertEqual(A.data["status"], "MERGED")
        self.assertEqual(B.data["status"], "MERGED")


class TestSemaphoreReleaseUpgrade(ZuulTestCase):
    tenant_config_file = 'config/global-semaphores/main.yaml'

    @model_version(32)
    def test_model_32(self):
        # This tests that a job finishing in one tenant will correctly
        # start a job in another tenant waiting on the semaphore.
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        self.assertHistory([])
        self.assertBuilds([
            dict(name='test-global-semaphore', changes='1,1'),
        ])

        # Block tenant management event queues so we know that the
        # semaphore release events are dispatched via the pipeline
        # trigger event queue.
        with (management_queue_lock(self.zk_client, "tenant-one"),
              management_queue_lock(self.zk_client, "tenant-two")):

            self.executor_server.hold_jobs_in_build = False
            self.executor_server.release()
            self.waitUntilSettled()

            self.assertHistory([
                dict(name='test-global-semaphore',
                     result='SUCCESS', changes='1,1'),
                dict(name='test-global-semaphore',
                     result='SUCCESS', changes='2,1'),
            ], ordered=False)


class TestOidcSecretSupport(ZuulTestCase):
    tenant_config_file = 'config/secrets/main.yaml'

    @model_version(34)
    def test_model_34(self):
        self._run_test()

    @model_version(35)
    def test_model_35(self):
        self._run_test()

    def _run_test(self):
        with open(os.path.join(FIXTURE_DIR,
                               'config/secrets/git/',
                               'org_project2/zuul-secret.yaml')) as f:
            config = f.read()
        file_dict = {'zuul.yaml': config}

        A = self.fake_gerrit.addFakeChange('org/project2', 'master', 'A',
                                           files=file_dict)
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(A.reported, 1, "A should report success")
        self.assertHistory([
            dict(name='project2-secret', result='SUCCESS', changes='1,1'),
        ])
