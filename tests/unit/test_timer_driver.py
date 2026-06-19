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
import datetime
import zoneinfo
from datetime import timezone

from zuul.driver.timer import TimerDriver
from zuul.driver.timer.crontrigger import ZuulCronTrigger
from zuul.driver.timer.rruletrigger import ZuulRRuleTrigger

from tests.base import BaseTestCase, ZuulTestCase, iterate_timeout


class TestTimerTrigger(BaseTestCase):

    def test_cron_trigger(self):
        cron_args = dict(
            minute='*',
            hour='*',
            day='*',
            month='*',
            day_of_week='*',
            second='0',
        )
        jitter = 25
        trigger = ZuulCronTrigger(jitter=jitter, **cron_args)
        tz = zoneinfo.ZoneInfo("Etc/UTC")

        # now is the scheduled time, we should run 25s in the future
        now = datetime.datetime(2026, 5, 19, 10, 0, 0, tzinfo=tz)
        expected = datetime.datetime(2026, 5, 19, 10, 0, 25, tzinfo=tz)

        nf = trigger.get_next_fire_time(None, now)
        self.assertEqual(expected, nf)

        # now is 10s after the scheduled time,
        # we should run 25s after scheduled
        now = datetime.datetime(2026, 5, 19, 10, 0, 10, tzinfo=tz)
        expected = datetime.datetime(2026, 5, 19, 10, 0, 25, tzinfo=tz)

        nf = trigger.get_next_fire_time(None, now)
        self.assertEqual(expected, nf)

        # now is 2s before the scheduled time,
        # we should run 25s after scheduled
        now = datetime.datetime(2026, 5, 19, 9, 59, 58, tzinfo=tz)
        expected = datetime.datetime(2026, 5, 19, 10, 0, 25, tzinfo=tz)

        nf = trigger.get_next_fire_time(None, now)
        self.assertEqual(expected, nf)

        # Test with a too-large jitter value
        jitter = 100
        trigger = ZuulCronTrigger(jitter=jitter, **cron_args)

        # Run 40s after scheduled since 100s backs up past the
        # previous scheduled time
        now = datetime.datetime(2026, 5, 19, 10, 0, 0, tzinfo=tz)
        expected = datetime.datetime(2026, 5, 19, 10, 0, 40, tzinfo=tz)

        nf = trigger.get_next_fire_time(None, now)
        self.assertEqual(expected, nf)

        # The next run; reset now to the next normally scheduled time
        # and test that we run a full 2 minutes later (rather than 1
        # minute later) because of our too-large jitter.
        now = datetime.datetime(2026, 5, 19, 10, 1, 0, tzinfo=tz)
        expected = datetime.datetime(2026, 5, 19, 10, 2, 40, tzinfo=tz)
        nf = trigger.get_next_fire_time(nf, now)


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


class TestIsRRule(BaseTestCase):

    def test_freq_prefix(self):
        self.assertTrue(TimerDriver._is_rrule("FREQ=DAILY"))

    def test_dtstart_prefix(self):
        self.assertTrue(TimerDriver._is_rrule(
            "DTSTART:20260101T000000Z\nRRULE:FREQ=DAILY"))

    def test_rrule_freq_in_string(self):
        self.assertTrue(TimerDriver._is_rrule("RRULE:FREQ=WEEKLY"))

    def test_case_insensitive_freq(self):
        self.assertTrue(TimerDriver._is_rrule("freq=daily"))

    def test_case_insensitive_dtstart(self):
        self.assertTrue(TimerDriver._is_rrule("dtstart:20260101T000000Z"))

    def test_case_insensitive_rrule_freq(self):
        self.assertTrue(TimerDriver._is_rrule("rrule:freq=weekly"))

    def test_cron_not_rrule(self):
        self.assertFalse(TimerDriver._is_rrule("* * * * *"))

    def test_cron_with_seconds_not_rrule(self):
        self.assertFalse(TimerDriver._is_rrule("* * * * * */1"))

    def test_cron_with_jitter_not_rrule(self):
        self.assertFalse(TimerDriver._is_rrule("* * * * * */1 1"))


class TestRRuleTrigger(BaseTestCase):

    def test_basic_next_fire_time(self):
        # DTSTART anchors the rule near 'now' so dateutil doesn't have to
        # iterate through decades of per-minute occurrences from the epoch.
        rrule = "DTSTART:20260519T100000Z\nRRULE:FREQ=MINUTELY"
        trigger = ZuulRRuleTrigger(rrule)
        now = datetime.datetime(2026, 5, 19, 10, 0, 30, tzinfo=timezone.utc)
        expected = datetime.datetime(2026, 5, 19, 10, 1, 0,
                                     tzinfo=timezone.utc)
        self.assertEqual(expected, trigger.get_next_fire_time(None, now))

    def test_jitter_added_to_next_time(self):
        rrule = "DTSTART:20260519T100000Z\nRRULE:FREQ=MINUTELY"
        trigger = ZuulRRuleTrigger(rrule, jitter=15)
        now = datetime.datetime(2026, 5, 19, 10, 0, 30, tzinfo=timezone.utc)
        expected = datetime.datetime(2026, 5, 19, 10, 1, 15,
                                     tzinfo=timezone.utc)
        self.assertEqual(expected, trigger.get_next_fire_time(None, now))

    def test_naive_now_treated_as_utc(self):
        # APScheduler may pass naive datetimes; the trigger coerces to UTC
        rrule = "DTSTART:20260519T100000Z\nRRULE:FREQ=MINUTELY"
        trigger = ZuulRRuleTrigger(rrule)
        now = datetime.datetime(2026, 5, 19, 10, 0, 30)  # no tzinfo
        expected = datetime.datetime(2026, 5, 19, 10, 1, 0,
                                     tzinfo=timezone.utc)
        self.assertEqual(expected, trigger.get_next_fire_time(None, now))

    def test_dtstart_in_rrule_string(self):
        # When DTSTART is embedded in the string, dateutil honours it
        rrule = ("DTSTART:20260101T000000Z\n"
                 "RRULE:FREQ=DAILY;BYHOUR=9;BYMINUTE=0;BYSECOND=0")
        trigger = ZuulRRuleTrigger(rrule)
        now = datetime.datetime(2026, 5, 19, 8, 0, 0, tzinfo=timezone.utc)
        expected = datetime.datetime(2026, 5, 19, 9, 0, 0,
                                     tzinfo=timezone.utc)
        self.assertEqual(expected, trigger.get_next_fire_time(None, now))

    def test_exhausted_rule_returns_none(self):
        # COUNT=1 with a DTSTART far in the past — no future occurrences
        rrule = "DTSTART:20200101T000000Z\nRRULE:FREQ=DAILY;COUNT=1"
        trigger = ZuulRRuleTrigger(rrule)
        now = datetime.datetime(2026, 5, 19, 0, 0, 0, tzinfo=timezone.utc)
        self.assertIsNone(trigger.get_next_fire_time(None, now))

    def test_rrule_colon_format(self):
        # Full iCalendar RRULE:FREQ= prefix is accepted
        trigger = ZuulRRuleTrigger("RRULE:FREQ=HOURLY")
        now = datetime.datetime(2026, 5, 19, 10, 30, 0, tzinfo=timezone.utc)
        expected = datetime.datetime(2026, 5, 19, 11, 0, 0,
                                     tzinfo=timezone.utc)
        self.assertEqual(expected, trigger.get_next_fire_time(None, now))


class TestTimerRRule(ZuulTestCase):
    tenant_config_file = 'config/single-tenant/main.yaml'

    def test_rrule_trigger_fires_jobs(self):
        self.create_branch('org/project', 'stable')
        self.fake_gerrit.addEvent(
            self.fake_gerrit.getFakeBranchCreatedEvent(
                'org/project', 'stable'))
        self.executor_server.hold_jobs_in_build = True
        self.commitConfigUpdate('common-config', 'layouts/timer-rrule.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))

        # Verify the driver created RRule-based APScheduler jobs
        timer = self.scheds.first.sched.connections.drivers['timer']
        timer_jobs = timer.apsched.get_jobs()
        self.assertTrue(len(timer_jobs) >= 1)
        for job in timer_jobs:
            self.assertIsInstance(job.trigger, ZuulRRuleTrigger)

        # The pipeline triggers every second, so we should see builds quickly
        for _ in iterate_timeout(60, 'jobs started'):
            if len(self.builds) > 1:
                break

        self.executor_server.hold_jobs_in_build = False

        # Stop queuing timer triggered jobs so the assertions don't race
        self.commitConfigUpdate('common-config', 'layouts/no-timer.yaml')
        self.scheds.execute(lambda app: app.sched.reconfigure(app.config))
        self.waitUntilSettled()
        time.sleep(3)
        self.waitUntilSettled()

        self.executor_server.release()
        self.waitUntilSettled()

        self.assertTrue(len(self.history) >= 2)
        for job in self.history:
            self.assertEqual(job.name, 'project-bitrot')
            self.assertEqual(job.result, 'SUCCESS')
