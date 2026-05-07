# Copyright 2021 BMW Group
# Copyright 2021-2023 Acme Gating, LLC
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

import threading
import time
from unittest import mock

import zuul.model

from tests.base import (
    ZuulTestCase,
    iterate_timeout,
    okay_tracebacks,
    simple_layout,
)
from zuul.zk.change_cache import ChangeKey
from zuul.zk.locks import SessionAwareWriteLock, TENANT_LOCK_ROOT
from zuul.scheduler import PendingReconfiguration


class TestScaleOutScheduler(ZuulTestCase):
    tenant_config_file = "config/single-tenant/main.yaml"
    # Those tests are testing specific interactions between multiple
    # schedulers. They create additional schedulers as necessary and
    # start or stop them individually to test specific interactions.
    # Using the scheduler_count in addition to create even more
    # schedulers doesn't make sense for those tests.
    scheduler_count = 1

    def test_multi_scheduler(self):
        # A smoke test that we can enqueue a change with one scheduler
        # and have another one finish the run.
        self.executor_server.hold_jobs_in_build = True

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        app = self.createScheduler()
        app.start()
        self.assertEqual(len(self.scheds), 2)

        # Hold the lock on the first scheduler so that only the second
        # will act.
        with self.scheds.first.sched.run_handler_lock:
            self.executor_server.hold_jobs_in_build = False
            self.executor_server.release()
            self.waitUntilSettled(matcher=[app])

        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
        ], ordered=False)

    def test_pipeline_cache_clear(self):
        # Test that the pipeline cache on a second scheduler isn't
        # holding old change objects.

        # Hold jobs in build
        sched1 = self.scheds.first
        self.executor_server.hold_jobs_in_build = True

        # We need a pair of changes in order to populate the pipeline
        # change cache (a single change doesn't activate the cache,
        # it's for dependencies).
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.addApproval('Code-Review', 2)
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        B.addApproval('Code-Review', 2)
        B.addApproval('Approved', 1)
        B.setDependsOn(A, 1)

        # Fail a job
        self.executor_server.failJob('project-test1', A)

        # Enqueue into gate with scheduler 1
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled()

        # Start scheduler 2
        sched2 = self.createScheduler()
        sched2.start()
        self.assertEqual(len(self.scheds), 2)

        # Pause scheduler 1
        with sched1.sched.run_handler_lock:
            # Release jobs
            self.executor_server.hold_jobs_in_build = False
            self.executor_server.release()
            # Wait for scheduler 2 to dequeue
            self.waitUntilSettled(matcher=[sched2])
        # Unpause scheduler 1
        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(B.data['status'], 'NEW')

        # Clear zk change cache
        self.fake_gerrit._change_cache.prune([], max_age=0)

        # At this point, scheduler 1 should have a bogus change entry
        # in the pipeline cache because scheduler 2 performed the
        # dequeue so scheduler 1 never cleaned up its cache.

        self.executor_server.fail_tests.clear()
        self.executor_server.hold_jobs_in_build = True
        # Pause scheduler 1
        with sched1.sched.run_handler_lock:
            # Enqueue into gate with scheduler 2
            self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
            self.waitUntilSettled(matcher=[sched2])

        # Pause scheduler 2
        with sched2.sched.run_handler_lock:
            # Make sure that scheduler 1 does some pipeline runs which
            # reconstitute state from ZK.  This gives it the
            # opportunity to use old cache data if we don't clear it.

            # Release job1
            self.executor_server.release()
            self.waitUntilSettled(matcher=[sched1])
            # Release job2
            self.executor_server.hold_jobs_in_build = False
            self.executor_server.release()
            # Wait for scheduler 1 to merge change
            self.waitUntilSettled(matcher=[sched1])
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(B.data['status'], 'MERGED')

    @simple_layout('layouts/multi-scheduler-status.yaml')
    def test_multi_scheduler_status(self):
        self.hold_merge_jobs_in_queue = True

        first = self.scheds.first
        second = self.createScheduler()
        second.start()
        self.assertEqual(len(self.scheds), 2)
        self.waitUntilSettled()

        self.log.debug("Force second scheduler to process check")
        with first.sched.run_handler_lock:
            event = zuul.model.PipelinePostConfigEvent()
            first.sched.pipeline_management_events[
                'tenant-one']['check'].put(event, needs_result=False)
            self.waitUntilSettled(matcher=[second])

        self.log.debug("Add change in first scheduler")
        with second.sched.run_handler_lock:
            A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
            self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
            self.waitUntilSettled(matcher=[first])

        self.log.debug("Finish change in second scheduler")
        with first.sched.run_handler_lock:
            self.hold_merge_jobs_in_queue = False
            self.merger_api.release()
            self.waitUntilSettled(matcher=[second])

        self.assertHistory([])

        tenant = first.sched.abide.tenants['tenant-one']
        manager = tenant.layout.pipeline_managers['check']
        summary = zuul.model.PipelineSummary()
        summary._set(manager=manager)
        with self.createZKContext() as context:
            summary.refresh(context)
        self.assertEqual(summary.status['change_queues'], [])

    def test_config_priming(self):
        # Wait until scheduler is primed
        self.waitUntilSettled()
        first_app = self.scheds.first
        initial_max_hold_exp = first_app.sched.globals.max_hold_expiration
        layout_state = first_app.sched.tenant_layout_state.get("tenant-one")
        self.assertIsNotNone(layout_state)

        # Second scheduler instance
        second_app = self.createScheduler()
        # Change a system attribute in order to check that the system config
        # from Zookeeper was used.
        second_app.sched.globals.max_hold_expiration += 1234
        second_app.config.set("scheduler", "max_hold_expiration", str(
            second_app.sched.globals.max_hold_expiration))

        second_app.start()
        self.waitUntilSettled()

        self.assertEqual(first_app.sched.local_layout_state.get("tenant-one"),
                         second_app.sched.local_layout_state.get("tenant-one"))

        # Make sure only the first schedulers issued cat jobs
        self.assertIsNotNone(
            first_app.sched.merger.merger_api.history.get("cat"))
        self.assertIsNone(
            second_app.sched.merger.merger_api.history.get("cat"))

        for _ in iterate_timeout(
                10, "Wait for all schedulers to have the same system config"):
            if (first_app.sched.unparsed_abide.ltime
                    == second_app.sched.unparsed_abide.ltime):
                break

        self.assertEqual(second_app.sched.globals.max_hold_expiration,
                         initial_max_hold_exp)

    def test_reconfigure(self):
        # Create a second scheduler instance
        app = self.createScheduler()
        app.start()
        self.assertEqual(len(self.scheds), 2)

        for _ in iterate_timeout(10, "Wait until priming is complete"):
            old = self.scheds.first.sched.tenant_layout_state.get("tenant-one")
            if old is not None:
                break

        for _ in iterate_timeout(
                10, "Wait for all schedulers to have the same layout state"):
            layout_states = [a.sched.local_layout_state.get("tenant-one")
                             for a in self.scheds.instances]
            if all(l == old for l in layout_states):
                break

        self.scheds.first.sched.reconfigure(self.scheds.first.config)
        self.waitUntilSettled()

        new = self.scheds.first.sched.tenant_layout_state["tenant-one"]
        self.assertNotEqual(old, new)

        for _ in iterate_timeout(10, "Wait for all schedulers to update"):
            layout_states = [a.sched.local_layout_state.get("tenant-one")
                             for a in self.scheds.instances]
            if all(l == new for l in layout_states):
                break

        layout_uuids = [a.sched.abide.tenants["tenant-one"].layout.uuid
                        for a in self.scheds.instances]
        self.assertTrue(all(l == new.uuid for l in layout_uuids))
        self.waitUntilSettled()

    def test_pending_reconfigure(self):
        # Create a second scheduler instance
        app = self.createScheduler()
        app.start()
        self.assertEqual(len(self.scheds), 2)

        for _ in iterate_timeout(10, "Wait until priming is complete"):
            old = self.scheds.first.sched.tenant_layout_state.get("tenant-one")
            if old is not None:
                break

        for _ in iterate_timeout(
                10, "Wait for all schedulers to have the same layout state"):
            layout_states = [a.sched.local_layout_state.get("tenant-one")
                             for a in self.scheds.instances]
            if all(l == old for l in layout_states):
                break
        self.waitUntilSettled()
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')

        first = self.scheds.first.sched
        second = app.sched
        origAbortIfPendingReconfig = second.abortIfPendingReconfig
        started_event = threading.Event()

        def abortIfPendingReconfig(tenant_lock):
            started_event.set()
            for _ in iterate_timeout(
                    30, "Wait for first scheduler to request lock"):
                if 'RECONFIG' in tenant_lock._zuul_seen_contender_names:
                    break
            try:
                origAbortIfPendingReconfig(tenant_lock)
            except PendingReconfiguration:
                raise
            raise Exception("Expectend PendingReconfiguration exception")

        # Prepare the second scheduler to pause inside the pending
        # reconfig check method so that we can release it when we
        # expect it to notice a pending reconfig.
        with mock.patch.object(
            second, "abortIfPendingReconfig", abortIfPendingReconfig
        ):
            # Pause the first scheduler while we submit an event that
            # we expect the second scheduler to act on.
            with first.run_handler_lock:
                self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
                # Wait for the second scheduler to get to the point
                # where it is ready to act on the event and is then
                # paused (as described above).
                started_event.wait()
            # At this point, the second scheduler is about to check
            # for a pending reconfig, and the first is idle.  Release
            # the first and schedule a reconfig.
            self.scheds.first.sched.reconfigure(self.scheds.first.config)

        # As soon as the pending reconfig lock shows up, the
        # second scheduler should be released, and everything
        # proceeds.
        self.waitUntilSettled()
        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
        ], ordered=False)

    def test_live_reconfiguration_del_pipeline(self):
        # Test pipeline deletion while changes are enqueued

        # Create a second scheduler instance
        app = self.createScheduler()
        app.start()
        self.assertEqual(len(self.scheds), 2)

        for _ in iterate_timeout(10, "Wait until priming is complete"):
            old = self.scheds.first.sched.tenant_layout_state.get("tenant-one")
            if old is not None:
                break

        for _ in iterate_timeout(
                10, "Wait for all schedulers to have the same layout state"):
            layout_states = [a.sched.local_layout_state.get("tenant-one")
                             for a in self.scheds.instances]
            if all(l == old for l in layout_states):
                break

        pipeline_zk_path = app.sched.abide.tenants[
            "tenant-one"].layout.pipeline_managers["check"].state.getPath()

        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')

        # Let the first scheduler enqueue the change into the pipeline that
        # will be removed later on.
        with app.sched.run_handler_lock:
            self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
            self.waitUntilSettled(matcher=[self.scheds.first])

        # Process item only on second scheduler so the first scheduler has
        # an outdated pipeline state.
        with self.scheds.first.sched.run_handler_lock:
            self.executor_server.release('.*-merge')
            self.waitUntilSettled(matcher=[app])
            self.assertEqual(len(self.builds), 2)

        self.commitConfigUpdate(
            'common-config',
            'layouts/live-reconfiguration-del-pipeline.yaml')
        # Trigger a reconfiguration on the first scheduler with the outdated
        # pipeline state of the pipeline that will be removed.
        self.scheds.execute(lambda a: a.sched.reconfigure(a.config),
                            matcher=[self.scheds.first])

        new = self.scheds.first.sched.tenant_layout_state.get("tenant-one")
        for _ in iterate_timeout(
                10, "Wait for all schedulers to have the same layout state"):
            layout_states = [a.sched.local_layout_state.get("tenant-one")
                             for a in self.scheds.instances]
            if all(l == new for l in layout_states):
                break

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertEqual(A.data['status'], 'NEW')
        self.assertEqual(A.reported, 0)

        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='ABORTED', changes='1,1'),
            dict(name='project-test2', result='ABORTED', changes='1,1'),
        ], ordered=False)

        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        self.assertEqual(len(tenant.layout.pipeline_managers), 0)
        stat = self.zk_client.client.exists(pipeline_zk_path)
        self.assertIsNone(stat)

    def test_change_cache(self):
        # Test re-using a change from the change cache.
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')

        B.setDependsOn(A, 1)

        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        # This has populated the change cache with our change.

        app = self.createScheduler()
        app.start()
        self.assertEqual(len(self.scheds), 2)

        # Hold the lock on the first scheduler so that only the second
        # will act.
        with self.scheds.first.sched.run_handler_lock:
            # Enqueue the change again.  The second scheduler will
            # load the change object from the cache.
            self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))

            self.waitUntilSettled(matcher=[app])

        # Each job should appear twice and contain both changes.
        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1 2,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1 2,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1 2,1'),
            dict(name='project-merge', result='SUCCESS', changes='1,1 2,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1 2,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1 2,1'),
        ], ordered=False)

    def test_change_cache_error(self):
        # Test that if a change is deleted from the change cache,
        # pipeline processing can continue
        self.executor_server.hold_jobs_in_build = True

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Delete the change cache
        for connection in self.scheds.first.connections.connections.values():
            if hasattr(connection, '_change_cache'):
                connection.maintainCache([], max_age=0)

        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )
        self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))

        # Release
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        self.assertHistory([
            dict(name='project-merge', result='SUCCESS', changes='1,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1'),
            dict(name='project-merge', result='SUCCESS', changes='1,1 2,1'),
            dict(name='project-test1', result='SUCCESS', changes='1,1 2,1'),
            dict(name='project-test2', result='SUCCESS', changes='1,1 2,1'),
        ], ordered=False)

    @simple_layout('layouts/cherry-pick.yaml')
    def test_change_cache_ismerged(self):
        # Test that we don't bump a depending change from the pipeline
        # due to a race with isMerged.
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        A.cherry_pick = True
        B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
        B.cherry_pick = True
        B.setDependsOn(A, 1)
        A.addApproval('Code-Review', 2)
        B.addApproval('Code-Review', 2)
        self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
        self.waitUntilSettled("Change A enqueued")
        self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
        self.waitUntilSettled("Change B enqueued")

        app = self.createScheduler()
        app.start()
        self.assertEqual(len(self.scheds), 2)
        first = self.scheds.first
        second = app
        # Wait until they're both ready
        self.waitUntilSettled("Second scheduler ready")

        # Here's the sequence we need to happen (scheduler id in []):
        # [2] tell gerrit to merge 1,1
        # [1] receive a comment-added event for 1,1
        # [1] perform a gerrit query for 1,1 and receive state=NEW
        # [1] stop processing right before updating the change cache
        # [2] perform the isMerged query for 1,1 and receive state=MERGED
        # [2] update 1,1 in the change cache with is_merged=True
        # [1] resume processing the change cache update
        # Then we make sure is_merged does not flip back to false

        # These controls whether scheduler 1 can write updates to the
        # change cache.
        update_permitted_event = threading.Event()
        update_permitted_event.set()
        waiting_to_update_event = threading.Event()

        # These allow us to stop scheduler 2 between the two queue
        # items.
        queue_item_finished = threading.Event()
        queue_item_resume = threading.Event()
        queue_item_resume.set()

        # Insert our control into the scheduler 1 change cache
        s1_cache = first.sched.connections.connections['gerrit']._change_cache
        orig_s1_set = s1_cache.updateChangeWithRetry

        def s1_set(*args, **kw):
            if not update_permitted_event.isSet():
                # We have performed the comment-added query and are
                # about to write the results.
                self.log.debug("Waiting to update change cache")
                # Tell scheduler 2 that we're waiting; we pause so
                # that it can proceed with its correct data.
                waiting_to_update_event.set()
                update_permitted_event.wait()
                # Now we're going to write old data.
                self.log.debug("Resuming update to change cache")
            return orig_s1_set(*args, **kw)
        s1_cache.updateChangeWithRetry = s1_set

        # Gain control of the review method.
        s2_conn = second.sched.connections.connections['gerrit']
        orig_s2_review = s2_conn.review

        def s2_review(*args, **kw):
            self.log.debug("Review start")
            # We don't normally automatically emit any post-merge
            # events in tests in order to keep them simple.  But we
            # need to in this case so that we can trigger a change
            # query that races the isMerged query.  Emit the
            # comment-added event which is the first thing that gerrit
            # emits as we merge a change.  Normally a change-merged
            # event would follow, but is not necessary for this test.
            self.fake_gerrit.addEvent(A.getChangeCommentEvent(1))
            # Wait until query is done
            self.log.debug("Wait for change-merged query to finish")
            waiting_to_update_event.wait()
            # We know the change-merged query has completed with state=NEW
            self.log.debug("Review resume")
            # Allow the report to happen, which will cause the state
            # to change to MERGED for subsequent queries.
            ret = orig_s2_review(*args, **kw)
            self.log.debug("Review done")
            return ret
        s2_conn.review = s2_review

        # Gain control of the pipeline manager so that we can pause
        # between processing queue items.
        s2_gate = second.sched.abide.tenants[
            'tenant-one'].layout.pipeline_managers['gate']
        orig_s2_processOneItem = s2_gate._processOneItem

        def s2_processOneItem(*args, **kw):
            ret = orig_s2_processOneItem(*args, **kw)
            self.log.debug("Finished queue item")
            queue_item_finished.set()
            self.log.debug("Wait for queue item")
            queue_item_resume.wait()
            return ret
        s2_gate._processOneItem = s2_processOneItem

        # Hold the lock on the first scheduler, so the queue
        # processing happens on the second.
        with first.sched.run_handler_lock:
            key = ChangeKey('gerrit', None, 'GerritChange', '1', '1')
            c1 = s1_cache.get(key)
            c2 = s1_cache.get(key)

            # Clear events for the start of the interesting part of
            # the test.
            queue_item_finished.clear()
            queue_item_resume.clear()
            update_permitted_event.clear()
            self.log.debug("Release held build")
            self.builds[0].release()
            self.log.debug("Wait for first queue item to finish")
            queue_item_finished.wait()
            self.log.debug("First queue item is finished")

            # Allow the delayed update to take effect (with the old,
            # pre-merge data) before we process the next queue item.
            old_uuid = c2.cache_stat.uuid

            update_permitted_event.set()
            # We wait for the caches to sync to make sure that we
            # don't trip the conflict detection in the change cache.
            # If we do, then the change cache will re-query the data.
            self.log.debug("Wait for caches to sync")
            for x in iterate_timeout(30, 'caches to sync'):
                if (c2.cache_stat.uuid != old_uuid and
                    c1.cache_stat.uuid == c2.cache_stat.uuid):
                    break

            # Allow the next item to process
            self.log.debug("Resume processing second queue item")
            queue_item_resume.set()
            self.waitUntilSettled("Merge change A", matcher=[second])

        # Release everything now
        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()

        self.waitUntilSettled("End")
        self.assertHistory([
            dict(name='check-job', result='SUCCESS', changes='1,1'),
            dict(name='check-job', result='SUCCESS', changes='1,1 2,1'),
        ])
        self.assertEqual(A.data['status'], 'MERGED')
        self.assertEqual(B.data['status'], 'MERGED')

    @okay_tracebacks('Unterminated string starting at')
    def test_pipeline_summary(self):
        # Test that we can deal with a truncated pipeline summary
        self.executor_server.hold_jobs_in_build = True
        tenant = self.scheds.first.sched.abide.tenants.get('tenant-one')
        manager = tenant.layout.pipeline_managers['check']
        context = self.createZKContext()

        def new_summary():
            summary = zuul.model.PipelineSummary()
            summary._set(manager=manager)
            with context:
                summary.refresh(context)
            return summary

        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()

        # Check we have a good summary
        summary1 = new_summary()
        self.assertNotEqual(summary1.status, {})
        self.assertTrue(context.client.exists(summary1.getPath()))

        # Make a syntax error in the status summary json
        summary = new_summary()
        summary._save(context, b'{"foo')

        # With the corrupt data, we should get an empty status but the
        # path should still exist.
        summary2 = new_summary()
        self.assertEqual(summary2.status, {})
        self.assertTrue(context.client.exists(summary2.getPath()))

        # Our earlier summary object should use its cached data
        with context:
            summary1.refresh(context)
        self.assertNotEqual(summary1.status, {})

        self.executor_server.hold_jobs_in_build = False
        self.executor_server.release()
        self.waitUntilSettled()

        # The scheduler should have written a new summary that our
        # second object can read now.
        with context:
            summary2.refresh(context)
        self.assertNotEqual(summary2.status, {})

    @simple_layout('layouts/semaphore.yaml')
    def test_semaphore(self):
        self.executor_server.hold_jobs_in_build = True
        A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
        self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
        self.waitUntilSettled()
        self.assertEqual(len(self.builds), 1)
        self.assertEqual(self.builds[0].name, 'test1')
        self.assertHistory([])

        tenant = self.scheds.first.sched.abide.tenants['tenant-one']
        semaphore = tenant.semaphore_handler.getSemaphores()[0]
        holders = tenant.semaphore_handler.semaphoreHolders(semaphore)
        self.assertEqual(len(holders), 1)

        # Start a second scheduler so that it runs through the initial
        # cleanup processes.
        app = self.createScheduler()
        # Hold the lock on the second scheduler so that if any events
        # happen, they are processed by the first scheduler (this lets
        # them be as out of sync as possible).
        with app.sched.run_handler_lock:
            app.start()
            self.assertEqual(len(self.scheds), 2)
            self.waitUntilSettled(matcher=[self.scheds.first])
            # Wait until initial cleanup is run
            app.sched.start_cleanup_thread.join()
            # We should not have released the semaphore
            holders = tenant.semaphore_handler.semaphoreHolders(semaphore)
            self.assertEqual(len(holders), 1)

        self.executor_server.release()
        self.waitUntilSettled()
        self.assertEqual(len(self.builds), 1)
        self.assertEqual(self.builds[0].name, 'test2')
        self.assertHistory([
            dict(name='test1', result='SUCCESS', changes='1,1'),
        ], ordered=False)
        holders = tenant.semaphore_handler.semaphoreHolders(semaphore)
        self.assertEqual(len(holders), 1)

        self.executor_server.release()
        self.waitUntilSettled()
        self.assertEqual(len(self.builds), 0)
        self.assertHistory([
            dict(name='test1', result='SUCCESS', changes='1,1'),
            dict(name='test2', result='SUCCESS', changes='1,1'),
        ], ordered=False)

        holders = tenant.semaphore_handler.semaphoreHolders(semaphore)
        self.assertEqual(len(holders), 0)

    @simple_layout('layouts/two-projects-integrated.yaml')
    def test_nodepool_relative_priority_check(self):
        "Test that nodes are requested at the relative priority"
        self.fake_nodepool.pause()

        # Start a second scheduler that uses the existing layout
        app = self.createScheduler()
        app.start()

        # Hold the lock on the first scheduler so that if any events
        # happen, they are processed by the second scheduler.
        with self.scheds.first.sched.run_handler_lock:
            A = self.fake_gerrit.addFakeChange('org/project', 'master', 'A')
            self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))
            self.waitUntilSettled(matcher=[app])

            B = self.fake_gerrit.addFakeChange('org/project', 'master', 'B')
            self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
            self.waitUntilSettled(matcher=[app])

            C = self.fake_gerrit.addFakeChange('org/project1', 'master', 'C')
            self.fake_gerrit.addEvent(C.getPatchsetCreatedEvent(1))
            self.waitUntilSettled(matcher=[app])

            D = self.fake_gerrit.addFakeChange('org/project2', 'master', 'D')
            self.fake_gerrit.addEvent(D.getPatchsetCreatedEvent(1))
            self.waitUntilSettled(matcher=[app])

            reqs = self.fake_nodepool.getNodeRequests()

            # The requests come back sorted by priority.

            # Change A, first change for project, high relative priority.
            self.assertEqual(reqs[0]['_oid'], '500-0000000000')
            self.assertEqual(reqs[0]['relative_priority'], 0)

            # Change C, first change for project1, high relative priority.
            self.assertEqual(reqs[1]['_oid'], '500-0000000002')
            self.assertEqual(reqs[1]['relative_priority'], 0)

            # Change B, second change for project, lower relative priority.
            self.assertEqual(reqs[2]['_oid'], '500-0000000001')
            self.assertEqual(reqs[2]['relative_priority'], 1)

            # Change D, first change for project2 shared with project1,
            # lower relative priority than project1.
            self.assertEqual(reqs[3]['_oid'], '500-0000000003')
            self.assertEqual(reqs[3]['relative_priority'], 1)

            # Fulfill only the first request
            self.fake_nodepool.fulfillRequest(reqs[0])
            for x in iterate_timeout(30, 'fulfill request'):
                reqs = list(self.scheds.first.sched.nodepool.getNodeRequests())
                if len(reqs) < 4:
                    break
            self.waitUntilSettled(matcher=[app])

            reqs = self.fake_nodepool.getNodeRequests()

            # Change B, now first change for project, equal priority.
            self.assertEqual(reqs[0]['_oid'], '500-0000000001')
            self.assertEqual(reqs[0]['relative_priority'], 0)

            # Change C, now first change for project1, equal priority.
            self.assertEqual(reqs[1]['_oid'], '500-0000000002')
            self.assertEqual(reqs[1]['relative_priority'], 0)

            # Change D, first change for project2 shared with project1,
            # still lower relative priority than project1.
            self.assertEqual(reqs[2]['_oid'], '500-0000000003')
            self.assertEqual(reqs[2]['relative_priority'], 1)

            self.fake_nodepool.unpause()
            self.waitUntilSettled(matcher=[app])
        self.waitUntilSettled()

    @simple_layout('layouts/two-projects-integrated.yaml')
    def test_nodepool_relative_priority_gate(self):
        "Test that nodes are requested at the relative priority"
        self.fake_nodepool.pause()

        # Start a second scheduler that uses the existing layout
        app = self.createScheduler()
        app.start()

        # Hold the lock on the first scheduler so that if any events
        # happen, they are processed by the second scheduler.
        with self.scheds.first.sched.run_handler_lock:
            A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
            A.addApproval('Code-Review', 2)
            self.fake_gerrit.addEvent(A.addApproval('Approved', 1))
            self.waitUntilSettled(matcher=[app])

            B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')
            B.addApproval('Code-Review', 2)
            self.fake_gerrit.addEvent(B.addApproval('Approved', 1))
            self.waitUntilSettled(matcher=[app])

            # project does not share a queue with project1 and project2.
            C = self.fake_gerrit.addFakeChange('org/project', 'master', 'C')
            C.addApproval('Code-Review', 2)
            self.fake_gerrit.addEvent(C.addApproval('Approved', 1))
            self.waitUntilSettled(matcher=[app])

            reqs = self.fake_nodepool.getNodeRequests()

            # The requests come back sorted by priority.

            # Change A, first change for shared queue, high relative
            # priority.
            self.assertEqual(reqs[0]['_oid'], '100-0000000000')
            self.assertEqual(reqs[0]['relative_priority'], 0)

            # Change C, first change for independent project, high
            # relative priority.
            self.assertEqual(reqs[1]['_oid'], '100-0000000002')
            self.assertEqual(reqs[1]['relative_priority'], 0)

            # Change B, second change for shared queue, lower relative
            # priority.
            self.assertEqual(reqs[2]['_oid'], '100-0000000001')
            self.assertEqual(reqs[2]['relative_priority'], 1)

            self.fake_nodepool.unpause()
            self.waitUntilSettled(matcher=[app])
        self.waitUntilSettled()

    @simple_layout('layouts/timer-jitter-slow.yaml')
    def test_timer_multi_scheduler(self):
        # Test that two schedulers create exactly the same timer jobs
        # including jitter.
        self.create_branch('org/project', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable'))
        self.waitUntilSettled()

        timer1 = self.scheds.first.sched.connections.drivers['timer']
        timer1_jobs = sorted(timer1.apsched.get_jobs(),
                             key=lambda x: x.trigger._zuul_jitter)

        sched2 = self.createScheduler()
        sched2.start()
        self.assertEqual(len(self.scheds), 2)

        timer1.stop()
        self.waitUntilSettled(matcher=[sched2])

        timer2 = sched2.connections.drivers['timer']

        for _ in iterate_timeout(10, "until jobs registered"):
            timer2_jobs = sorted(timer2.apsched.get_jobs(),
                                 key=lambda x: x.trigger._zuul_jitter)
            if timer2_jobs:
                break

        for x in range(len(timer1_jobs)):
            self.log.debug("Timer jitter: %s %s",
                           timer1_jobs[x].trigger._zuul_jitter,
                           timer2_jobs[x].trigger._zuul_jitter)
            self.assertEqual(timer1_jobs[x].trigger._zuul_jitter,
                             timer2_jobs[x].trigger._zuul_jitter)
            if x:
                # Assert that we're not applying the same jitter to
                # every job.
                self.assertNotEqual(timer1_jobs[x - 1].trigger._zuul_jitter,
                                    timer1_jobs[x].trigger._zuul_jitter)

        self.commitConfigUpdate('org/common-config', 'layouts/no-timer.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()
        # If APScheduler is in mid-event when we remove the job, we
        # can end up with one more event firing, so give it an extra
        # second to settle.
        time.sleep(1)
        self.waitUntilSettled()


class TestSOSCircularDependencies(ZuulTestCase):
    # Those tests are testing specific interactions between multiple
    # schedulers. They create additional schedulers as necessary and
    # start or stop them individually to test specific interactions.
    # Using the scheduler_count in addition to create even more
    # schedulers doesn't make sense for those tests.
    scheduler_count = 1

    @simple_layout('layouts/sos-circular.yaml')
    def test_sos_circular_deps(self):
        # This test sets the window to 1 so that we can test a code
        # path where we write the queue items to ZK as little as
        # possible on the first scheduler while doing most of the work
        # on the second.
        self.executor_server.hold_jobs_in_build = True
        Z = self.fake_gerrit.addFakeChange('org/project', "master", "Z")
        A = self.fake_gerrit.addFakeChange('org/project', "master", "A")
        B = self.fake_gerrit.addFakeChange('org/project', "master", "B")

        # Z, A <-> B (via commit-depends)
        A.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            A.subject, B.data["url"]
        )
        B.data["commitMessage"] = "{}\n\nDepends-On: {}\n".format(
            B.subject, A.data["url"]
        )
        Z.addApproval("Code-Review", 2)
        self.fake_gerrit.addEvent(Z.addApproval("Approved", 1))
        self.waitUntilSettled()
        A.addApproval("Code-Review", 2)
        B.addApproval("Code-Review", 2)
        A.addApproval("Approved", 1)
        self.fake_gerrit.addEvent(B.addApproval("Approved", 1))
        self.waitUntilSettled()

        # Start a second scheduler
        app = self.createScheduler()
        app.start()
        self.assertEqual(len(self.scheds), 2)
        self.waitUntilSettled()

        # Hold the lock on the first scheduler so that only the second
        # will act.
        with self.scheds.first.sched.run_handler_lock:
            # Release the first item so the second moves into the
            # active window.
            self.assertEqual(len(self.builds), 2)
            builds = self.builds[:]
            builds[0].release()
            builds[1].release()
            self.waitUntilSettled(matcher=[app])
            self.assertEqual(len(self.builds), 4)
            builds = self.builds[:]
            self.executor_server.failJob('job1', A)
            # Since it's one queue item for the two changes, all 4
            # builds need to complete.
            builds[0].release()
            builds[1].release()
            builds[2].release()
            builds[3].release()
            app.sched.wake_event.set()
            self.waitUntilSettled(matcher=[app])
            self.assertEqual(A.reported, 2)
            self.assertEqual(B.reported, 2)


class TestScaleOutSchedulerMultiTenant(ZuulTestCase):
    # Those tests are testing specific interactions between multiple
    # schedulers. They create additional schedulers as necessary and
    # start or stop them individually to test specific interactions.
    # Using the scheduler_count in addition to create even more
    # schedulers doesn't make sense for those tests.
    scheduler_count = 1
    tenant_config_file = "config/two-tenant/main.yaml"

    def test_background_layout_update(self):
        # This test performs a reconfiguration on one scheduler and
        # verifies that a second scheduler begins processing changes
        # for each tenant as it is updated.

        first = self.scheds.first
        # Create a second scheduler instance
        second = self.createScheduler()
        second.start()
        self.assertEqual(len(self.scheds), 2)
        tenant_one_lock = SessionAwareWriteLock(
            self.zk_client.client,
            f"{TENANT_LOCK_ROOT}/tenant-one")

        A = self.fake_gerrit.addFakeChange('org/project1', 'master', 'A')
        B = self.fake_gerrit.addFakeChange('org/project2', 'master', 'B')

        for _ in iterate_timeout(10, "until priming is complete"):
            state_one = first.sched.local_layout_state.get("tenant-one")
            state_two = first.sched.local_layout_state.get("tenant-two")
            if all([state_one, state_two]):
                break

        for _ in iterate_timeout(
                10, "all schedulers to have the same layout state"):
            if (second.sched.local_layout_state.get(
                    "tenant-one") == state_one and
                second.sched.local_layout_state.get(
                    "tenant-two") == state_two):
                break

        self.log.debug("Freeze scheduler-1")
        with second.sched.layout_update_lock:
            state_one = first.sched.local_layout_state.get("tenant-one")
            state_two = first.sched.local_layout_state.get("tenant-two")
            self.log.debug("Reconfigure scheduler-0")
            first.sched.reconfigure(first.config)
            for _ in iterate_timeout(
                    10, "tenants to be updated on scheduler-0"):
                if ((first.sched.local_layout_state["tenant-one"] !=
                     state_one) and
                    (first.sched.local_layout_state["tenant-two"] !=
                     state_two)):
                    break
            self.waitUntilSettled(matcher=[first])
            self.log.debug("Grab tenant-one write lock")
            tenant_one_lock.acquire(blocking=True)

        self.log.debug("Thaw scheduler-1")
        self.log.debug("Freeze scheduler-0")
        with first.sched.run_handler_lock:
            try:
                self.log.debug("Open change in tenant-one")
                self.fake_gerrit.addEvent(A.getPatchsetCreatedEvent(1))

                for _ in iterate_timeout(30, "trigger event appears"):
                    if second.sched.trigger_events['tenant-one'].hasEvents():
                        break

                for _ in iterate_timeout(
                        30, "tenant-two to be updated on scheduler-1"):
                    if (first.sched.local_layout_state["tenant-two"] ==
                        second.sched.local_layout_state.get("tenant-two")):
                        break
                # Tenant two should be up to date, but tenant one should
                # still be out of date on scheduler two.
                self.assertEqual(
                    first.sched.local_layout_state["tenant-two"],
                    second.sched.local_layout_state["tenant-two"])
                self.assertNotEqual(
                    first.sched.local_layout_state["tenant-one"],
                    second.sched.local_layout_state["tenant-one"])
                self.log.debug("Verify tenant-one change is unprocessed")
                # If we have updated tenant-two's configuration without
                # processing the tenant-one change, then we know we've
                # completed at least one run loop.
                self.assertHistory([])

                self.log.debug("Open change in tenant-two")
                self.fake_gerrit.addEvent(B.getPatchsetCreatedEvent(1))
                self.log.debug(
                    "Wait for scheduler-1 to process tenant-two change")

                for _ in iterate_timeout(30, "tenant-two build finish"):
                    if len(self.history):
                        break

                self.assertHistory([
                    dict(name='test', result='SUCCESS', changes='2,1'),
                ], ordered=False)

                # Tenant two should be up to date, but tenant one should
                # still be out of date on scheduler two.
                self.assertEqual(
                    first.sched.local_layout_state["tenant-two"],
                    second.sched.local_layout_state["tenant-two"])
                self.assertNotEqual(
                    first.sched.local_layout_state["tenant-one"],
                    second.sched.local_layout_state["tenant-one"])

                self.log.debug("Release tenant-one write lock")
            finally:
                # Release this in a finally clause so that the test
                # doesn't hang if we fail an assertion.
                tenant_one_lock.release()

            self.log.debug("Wait for both changes to be processed")
            self.waitUntilSettled(matcher=[second])
            self.assertHistory([
                dict(name='test', result='SUCCESS', changes='2,1'),
                dict(name='test', result='SUCCESS', changes='1,1'),
            ], ordered=False)

            # Both tenants should be up to date
            self.assertEqual(first.sched.local_layout_state["tenant-two"],
                             second.sched.local_layout_state["tenant-two"])
            self.assertEqual(first.sched.local_layout_state["tenant-one"],
                             second.sched.local_layout_state["tenant-one"])
        self.waitUntilSettled()

    def test_background_layout_update_add_tenant(self):
        # This test adds a new tenant and verifies that two schedulers
        # end up with layouts for the new tenant (one after an initial
        # reconfiguration, the other via the background update
        # thread).

        first = self.scheds.first
        # Create a second scheduler instance
        second = self.createScheduler()
        second.start()
        self.assertEqual(len(self.scheds), 2)

        for _ in iterate_timeout(10, "until priming is complete"):
            state_one = first.sched.local_layout_state.get("tenant-one")
            state_two = first.sched.local_layout_state.get("tenant-two")
            if all([state_one, state_two]):
                break

        for _ in iterate_timeout(
                10, "all schedulers to have the same layout state"):
            if (second.sched.local_layout_state.get(
                    "tenant-one") == state_one and
                second.sched.local_layout_state.get(
                    "tenant-two") == state_two):
                break

        self.log.debug("Freeze scheduler-1")
        with second.sched.layout_update_lock:
            state_one = first.sched.local_layout_state.get("tenant-one")
            state_two = first.sched.local_layout_state.get("tenant-two")
            self.log.debug("Reconfigure scheduler-0")

            self.newTenantConfig('config/two-tenant/three-tenant.yaml')
            first.smartReconfigure(command_socket=True)
            for _ in iterate_timeout(
                    10, "tenants to be updated on scheduler-0"):
                if 'tenant-three' in first.sched.local_layout_state:
                    break
            self.waitUntilSettled(matcher=[first])
        self.log.debug("Thaw scheduler-1")

        for _ in iterate_timeout(
                10, "tenants to be updated on scheduler-1"):
            if 'tenant-three' in second.sched.local_layout_state:
                break
        self.waitUntilSettled(matcher=[second])

    def test_background_layout_update_remove_tenant(self):
        # This test removes a tenant and verifies that the two schedulers
        # remove the tenant from their layout (one after an initial
        # reconfiguration, the other via the background update
        # thread).

        first = self.scheds.first
        # Create a second scheduler instance
        second = self.createScheduler()
        second.start()
        self.assertEqual(len(self.scheds), 2)

        for _ in iterate_timeout(10, "until priming is complete"):
            state_one = first.sched.local_layout_state.get("tenant-one")
            state_two = first.sched.local_layout_state.get("tenant-two")
            if all([state_one, state_two]):
                break

        for _ in iterate_timeout(
                10, "all schedulers to have the same layout state"):
            if (second.sched.local_layout_state.get(
                    "tenant-one") == state_one and
                second.sched.local_layout_state.get(
                    "tenant-two") == state_two):
                break
        self.assertIn('tenant-two', first.sched.abide.tenants)
        self.assertIn('tenant-two', second.sched.abide.tenants)

        self.log.debug("Freeze scheduler-1")
        with (second.sched.layout_update_lock,
              second.sched.run_handler_lock):
            self.log.debug("Reconfigure scheduler-0")
            self.newTenantConfig('config/two-tenant/one-tenant.yaml')
            first.smartReconfigure(command_socket=True)
            for _ in iterate_timeout(
                    10, "tenants to be removed on scheduler-0"):
                if 'tenant-two' not in first.sched.local_layout_state:
                    break
            self.waitUntilSettled(matcher=[first])
            self.assertNotIn('tenant-two', first.sched.abide.tenants)
        self.log.debug("Thaw scheduler-1")

        for _ in iterate_timeout(
                10, "tenants to be removed on scheduler-1"):
            if 'tenant-two' not in second.sched.local_layout_state:
                break
        self.waitUntilSettled(matcher=[second])
        self.assertNotIn('tenant-two', second.sched.abide.tenants)
