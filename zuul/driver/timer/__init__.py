# Copyright 2012 Hewlett-Packard Development Company, L.P.
# Copyright 2013 OpenStack Foundation
# Copyright 2016 Red Hat, Inc.
# Copyright 2023 Acme Gating, LLC
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
import logging
import threading
import time
import random
from collections import defaultdict
from uuid import uuid4

from apscheduler.schedulers.background import BackgroundScheduler
from opentelemetry import trace

from zuul.driver import Driver, TriggerInterface
from zuul.driver.timer import timertrigger
from zuul.driver.timer import timermodel
from zuul.driver.timer.timermodel import TimerTriggerEvent
from zuul.driver.timer.crontrigger import ZuulCronTrigger
from zuul.lib.logutil import get_annotated_logger
from zuul.zk.election import SessionAwareElection


class TimerDriver(Driver, TriggerInterface):
    name = 'timer'
    election_root = "/zuul/scheduler/timer-election"
    log = logging.getLogger("zuul.TimerDriver")
    tracer = trace.get_tracer("zuul")

    def __init__(self):
        self.log.debug("Starting apscheduler")
        self.apsched = BackgroundScheduler()
        self.apsched.start()
        self.tenant_jobs = {}
        self.election = None
        self.election_thread = None
        self.election_won = False
        # Mapping of locks: canonical project name -> lock
        # The lock are used to avoid concurrent update errors when a
        # lot of periodic pipelines are triggering simultanously.
        self.project_update_locks = defaultdict(threading.Lock)
        self.stop_event = threading.Event()
        self.stopped = False

    def registerScheduler(self, scheduler):
        self.sched = scheduler
        self.election = SessionAwareElection(
            self.sched.zk_client.client, self.election_root)
        self.election_thread = threading.Thread(name="TimerElection",
                                                target=self._runElection,
                                                daemon=True)
        self.election_thread.start()

    def _runElection(self):
        self.log.debug("Starting timer election loop")
        # If there are other contenders, wait until we are primed so
        # that we have all the tenant information.  If there are no
        # other schedulers running, we can start ASAP so we emit
        # events for the early tenants.
        try:
            if self.election.lock.contenders():
                self.log.debug("Waiting for scheduler to be primed")
                self.sched.primed_event.wait()
        except Exception:
            self.log.exception("Error in timer election startup:")
            # Just keep going
        while not self.stopped:
            try:
                self.log.info("Running timer election")
                self.election.run(self._electionInner)
            except Exception:
                self.log.exception("Error in timer election:")
        self.log.debug("Exiting timer election loop")

    def _electionInner(self):
        try:
            self.election_won = True
            self.log.info("Won timer election")
            self.stop_event.wait()
        finally:
            self.election_won = False
            self.stop_event.clear()
            self.log.debug("Timer election tenure ended")

    def reconfigure(self, tenant):
        if self.stopped:
            return
        if not self.apsched:
            # Handle possible reuse of the driver without connection objects.
            self.log.debug("Starting apscheduler on reconfigure")
            self.apsched = BackgroundScheduler()
            self.apsched.start()
        self._addJobs(tenant)

    def _removeJobs(self, tenant, new_jobs):
        # Compare existing jobs to new jobs and remove any that should
        # not be present.
        existing_jobs = self.tenant_jobs.get(tenant.name)
        if not existing_jobs:
            self.log.debug("No jobs to remove for %s, %s remain",
                           tenant, len(new_jobs))
            return
        to_remove = set(existing_jobs.keys()) - set(new_jobs.keys())
        for key in to_remove:
            job = existing_jobs[key]
            job.remove()
        self.log.debug("Removed %s jobs for %s, %s remain",
                       len(to_remove), tenant, len(new_jobs))

    def _addJobs(self, tenant):
        jobs = {}
        for manager in tenant.layout.pipeline_managers.values():
            pipeline = manager.pipeline
            for ef in pipeline.event_filters:
                if not isinstance(ef.trigger, timertrigger.TimerTrigger):
                    continue
                for timespec in ef.timespecs:
                    parts = timespec.split()
                    if len(parts) < 5 or len(parts) > 7:
                        self.log.error(
                            "Unable to parse time value '%s' "
                            "defined in pipeline %s" % (
                                timespec,
                                pipeline.name))
                        continue
                    try:
                        cron_args = dict(
                            minute=parts[0],
                            hour=parts[1],
                            day=parts[2],
                            month=parts[3],
                            day_of_week=parts[4],
                            second=None,
                        )
                        if len(parts) > 5:
                            cron_args['second'] = parts[5]
                        if len(parts) > 6:
                            jitter = int(parts[6])
                        else:
                            jitter = None
                        # Trigger any value errors by creating a
                        # throwaway object.
                        ZuulCronTrigger(jitter=jitter, **cron_args)
                    except ValueError:
                        self.log.error(
                            "Unable to create CronTrigger "
                            "for value '%s' defined in "
                            "pipeline %s",
                            timespec,
                            pipeline.name)
                        continue

                    self._addJobsInner(tenant, pipeline,
                                       cron_args, jitter, timespec,
                                       ef.dereference, ef.branches, jobs)
        self._removeJobs(tenant, jobs)
        self.tenant_jobs[tenant.name] = jobs

    def _addJobsInner(self, tenant, pipeline, cron_args, jitter,
                      timespec, dereference, branch_matchers, jobs):
        # jobs is a dict of args->job that we mutate
        existing_jobs = self.tenant_jobs.get(tenant.name, {})
        start_job_count = len(jobs)
        for project_name, pcs in tenant.layout.project_configs.items():
            # timer operates on branch heads and doesn't need
            # speculative layouts to decide if it should be
            # enqueued or not.  So it can be decided on cached
            # data if it needs to run or not.
            pcst = tenant.layout.getAllProjectConfigs(project_name)
            if not [True for pc in pcst if pipeline.name in pc.pipelines]:
                continue

            try:
                for branch in tenant.getProjectBranches(project_name):
                    # Apply branch filtering if branch matchers are defined
                    if branch_matchers:
                        branch_matched = False
                        for branch_matcher in branch_matchers:
                            if branch_matcher.match(branch):
                                branch_matched = True
                                break
                        if not branch_matched:
                            continue

                    args = (tenant.name, pipeline.name, project_name,
                            branch, dereference, timespec,)
                    existing_job = existing_jobs.get(args)
                    if jitter:
                        # Resolve jitter here so that it is the same
                        # on every scheduler for a given
                        # project-branch, assuming the same
                        # configuration.
                        prng_init = dict(
                            tenant=tenant.name,
                            project=project_name,
                            branch=branch,
                        )
                        prng_seed = json.dumps(prng_init, sort_keys=True)
                        prng = random.Random(prng_seed)
                        job_jitter = prng.uniform(0, jitter)
                    else:
                        job_jitter = None

                    if existing_job:
                        job = existing_job
                    else:
                        # The 'misfire_grace_time' argument is set to
                        # None to disable checking if the job missed
                        # its run time window.  This ensures we don't
                        # miss a trigger when the job is delayed due
                        # to e.g. high scheduler load. Those short
                        # delays are not a problem for our trigger
                        # use-case.
                        trigger = ZuulCronTrigger(
                            jitter=job_jitter, **cron_args)
                        job = self.apsched.add_job(
                            self._onTrigger, trigger=trigger,
                            args=args, misfire_grace_time=None)
                    jobs[job.args] = job
            except Exception:
                self.log.exception("Unable to create APScheduler job for "
                                   "%s %s %s",
                                   tenant, pipeline, project_name)
        end_job_count = len(jobs)
        self.log.debug("Added %s jobs for %s %s with %s existing tenant jobs",
                       (end_job_count - start_job_count),
                       tenant, pipeline,
                       len(existing_jobs))

    def _onTrigger(self, tenant_name, pipeline_name, project_name, branch,
                   dereference, timespec):
        try:
            if not self.election_won:
                return

            if not self.election.is_still_valid():
                self.log.debug("Noticed election is no longer valid")
                self.stop_event.set()
                return

            attributes = {
                "timespec": timespec,
            }
            with self.tracer.start_as_current_span(
                    "TimerEvent", attributes=attributes):
                self._dispatchEvent(tenant_name, pipeline_name, project_name,
                                    branch, dereference, timespec)
        except Exception:
            self.stop_event.set()
            self.log.exception("Error when dispatching timer event")

    def _dispatchEvent(self, tenant_name, pipeline_name, project_name,
                       branch, dereference, timespec):
        self.log.debug('Got trigger for tenant %s and pipeline %s '
                       'project %s branch %s with timespec %s',
                       tenant_name, pipeline_name, project_name,
                       branch, timespec)
        try:
            tenant = self.sched.abide.tenants[tenant_name]
            (trusted, project) = tenant.getProject(project_name)
            event = TimerTriggerEvent()
            event.type = 'timer'
            event.timespec = timespec
            event.forced_pipeline = pipeline_name
            event.project_hostname = project.canonical_hostname
            event.project_name = project.name
            event.ref = 'refs/heads/%s' % branch
            event.branch = branch
            event.zuul_event_id = str(uuid4().hex)
            event.timestamp = time.time()
            event.arrived_at_scheduler_timestamp = event.timestamp
            # Refresh the branch in order to update the item in the
            # change cache.
            change_key = project.source.getChangeKey(event)
            with self.project_update_locks[project.canonical_name]:
                if dereference:
                    event.newrev = project.source.getProjectBranchSha(
                        project, branch)
                else:
                    event.newrev = None
                project.source.getChange(change_key, refresh=True,
                                         event=event)
            log = get_annotated_logger(self.log, event)
            log.debug("Adding event")
            self.sched.pipeline_trigger_events[tenant.name][
                pipeline_name
            ].put(self.name, event)
        except Exception:
            self.log.exception("Error dispatching timer event for "
                               "tenant %s project %s branch %s",
                               tenant_name, project_name, branch)

    def stop(self):
        self.log.debug("Stopping timer driver")
        self.stopped = True
        self.stop_event.set()
        if self.apsched:
            self.log.debug("Stopping apscheduler")
            self.apsched.shutdown()
            self.apsched = None
            self.log.debug("Stopped apscheduler")
        if self.election:
            self.log.debug("Stopping election")
            self.election.cancel()
            self.log.debug("Stopped election")
        if self.election_thread:
            self.log.debug("Stopping election thread")
            self.election_thread.join()
            self.log.debug("Stopped election thread")
        self.log.debug("Stopped timer driver")

    def getTrigger(self, connection_name, config=None):
        return timertrigger.TimerTrigger(self, config)

    def getTriggerSchema(self):
        return timertrigger.getSchema()

    def getTriggerEventClass(self):
        return timermodel.TimerTriggerEvent
