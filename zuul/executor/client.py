# Copyright 2012 Hewlett-Packard Development Company, L.P.
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
import time

import zuul.executor.common
from zuul.lib.logutil import get_annotated_logger
from zuul.model import (
    Build,
    BuildCompletedEvent,
    BuildRequest,
    BuildStartedEvent,
    PRIORITY_MAP,
)
from zuul.zk.event_queues import PipelineResultEventQueue
from zuul.zk.executor import ExecutorApi
from zuul.zk.exceptions import JobRequestNotFound
import zuul.lib.tracing as tracing

from kazoo.exceptions import BadVersionError
from opentelemetry import trace


class ExecutorClient(object):
    log = logging.getLogger("zuul.ExecutorClient")
    _executor_api_class = ExecutorApi

    def __init__(self, config, sched):
        self.config = config
        self.sched = sched

        self.executor_api = self._executor_api_class(self.sched.zk_client)
        self.result_events = PipelineResultEventQueue.createRegistry(
            self.sched.zk_client
        )

    def stop(self):
        self.log.debug("Stopping")
        self.executor_api.stop()

    def getImageFormats(self, job, item):
        if not job.image_build_name:
            return None
        formats = set()
        for provider in item.manager.tenant.layout.providers.values():
            for image in provider.images.values():
                if image.name == job.image_build_name:
                    formats.add(image.format)
        return formats

    def execute(self, job, nodes, item, pipeline, executor_zone,
                dependent_changes=[], merger_items=[]):
        log = get_annotated_logger(self.log, item.event)
        tracer = trace.get_tracer("zuul")
        uuid = item.current_build_set.getBuildUUID(job)
        manager = item.manager
        log.info(
            "Execute job %s (uuid: %s) on nodes %s for %s "
            "with dependent changes %s",
            job, uuid, nodes, item, dependent_changes)

        params = zuul.executor.common.construct_build_params(
            uuid, self.sched.connections,
            job, item, pipeline, dependent_changes, merger_items,
            redact_secrets_and_keys=False)

        # Pass webroot to the executor for generating oidc token
        web_root = manager.tenant.web_root
        if web_root:
            params["zuul_root_url"] = web_root.split("/t/")[0].rstrip("/")
        # TODO: deprecate and remove this variable?
        params["zuul"]["_inheritance_path"] = list(job.inheritance_path)

        if job.image_build_name:
            params["zuul"]["image_build_name"] = job.image_build_name
        if image_formats := self.getImageFormats(job, item):
            params["zuul"]["image_formats"] = list(image_formats)

        semaphore_handler = manager.tenant.semaphore_handler
        params['semaphore_handle'] = semaphore_handler.getSemaphoreHandle(
            item, job)

        parent_span = tracing.restoreSpan(item.current_build_set.span_info)
        execute_time = time.time()
        with trace.use_span(parent_span):
            build_span = tracer.start_span("Build", start_time=execute_time)
        build_span_info = tracing.getSpanInfo(build_span)
        build = Build.new(
            manager.current_context,
            job=job,
            build_set=item.current_build_set,
            uuid=uuid,
            execute_time=execute_time,
            span_info=build_span_info,
            zuul_event_id=item.event.zuul_event_id,
        )

        log.debug("Adding build %s of job %s to item %s",
                  build, job, item)
        item.addBuild(build)

        if job.name == 'noop':
            data = {"start_time": time.time()}
            started_event = BuildStartedEvent(
                build.uuid, build.build_set.uuid, job.uuid,
                None, data, zuul_event_id=build.zuul_event_id)
            self.result_events[manager.tenant.name][manager.pipeline.name].put(
                started_event
            )

            result = {"result": "SUCCESS", "end_time": time.time()}
            completed_event = BuildCompletedEvent(
                build.uuid, build.build_set.uuid, job.uuid,
                None, result, zuul_event_id=build.zuul_event_id)
            self.result_events[manager.tenant.name][manager.pipeline.name].put(
                completed_event
            )

            return

        # Update zuul attempts after addBuild above to ensure build_set
        # is up to date.
        attempts = build.build_set.getTries(job)
        params["zuul"]['attempts'] = attempts
        params['zuul']['max_attempts'] = job.attempts
        # TODO (swestphahl): Remove deprecated 'max_attempts' parameter
        params['max_attempts'] = job.attempts

        # Store the Node(set)Request ID in the job arguments, so we can look
        # it up on the executor side to lock the nodes.
        if req_id := build.build_set.getJobNodeRequestID(job):
            if self.sched.nodepool.isNodeRequestID(req_id):
                params["noderequest_id"] = req_id
            else:
                # Note: We'll only create nodeset requests when all
                # components are updated to the model API version supporting
                # those requests, so we don't need to check for that here.
                params["nodesetrequest_id"] = req_id

        zone_known = False
        if executor_zone:
            # Check the component registry for executors subscribed to this
            # zone
            for comp in self.sched.component_registry.all(kind="executor"):
                if comp.zone == executor_zone:
                    zone_known = True
                    break

            if not zone_known:
                self.log.warning(
                    "Job requested '%s' zuul-executor zone, but no "
                    "zuul-executors found for this zone; ignoring zone "
                    "request", executor_zone)
                # Fall back to the default zone
                executor_zone = None

        with trace.use_span(build_span):
            request = BuildRequest(
                uuid=uuid,
                build_set_uuid=build.build_set.uuid,
                job_uuid=job.uuid,
                tenant_name=manager.tenant.name,
                pipeline_name=manager.pipeline.name,
                zone=executor_zone,
                event_id=item.event.zuul_event_id,
                precedence=PRIORITY_MAP[pipeline.precedence],
            )
        self.executor_api.submit(request, params)
        build.updateAttributes(manager.current_context,
                               build_request_ref=request.path)

    def cancel(self, build):
        log = get_annotated_logger(self.log, build.zuul_event_id,
                                   build=build.uuid)
        # Returns whether a running build was canceled
        log.info("Cancel build %s for job %s", build, build.job)

        build.updateAttributes(
            build.build_set.item.manager.current_context,
            canceled=True)

        if not build.build_request_ref:
            log.debug("Build has not been submitted to ZooKeeper")
            return False

        build_request = self.executor_api.get(build.build_request_ref)
        if build_request:
            log.debug("Canceling build request %s", build_request)
            # If we can acquire the build request lock here, the build wasn't
            # picked up by any executor server yet. With acquiring the lock
            # we prevent the executor server from picking up the build so we
            # can cancel it before it will run.
            if self.executor_api.lock(build_request, blocking=False):
                # Mark the build request as complete and forward the event to
                # the scheduler, so the executor server doesn't pick up the
                # request. The build will be deleted from the scheduler when it
                # picks up the BuildCompletedEvent.
                try:
                    if build_request.state == BuildRequest.COMPLETED:
                        log.debug("Build request already completed; skipping")
                        return False
                    log.debug(
                        "Canceling build %s directly because it is not locked"
                        "by any executor", build_request,
                    )
                    build_request.state = BuildRequest.COMPLETED
                    self.executor_api.update(build_request)

                    result = {"result": "CANCELED", "end_time": time.time()}
                    tenant_name = build.build_set.item.manager.tenant.name
                    pipeline_name = build.build_set.item.manager.pipeline.name
                    event = BuildCompletedEvent(
                        build_request.uuid, build_request.build_set_uuid,
                        build_request.job_uuid,
                        build_request.path, result)
                    self.result_events[tenant_name][pipeline_name].put(event)
                finally:
                    self.executor_api.unlock(build_request)
            else:
                log.debug(
                    "Sending cancel request for build %s because it is locked",
                    build_request,
                )
                # If the build request is locked, schedule a cancel request in
                # the executor server.
                self.executor_api.requestCancel(build_request)

            log.debug("Canceled build")
            return True

        return False

    def resumeBuild(self, build):
        log = get_annotated_logger(self.log, build.zuul_event_id)

        if not build.build_request_ref:
            log.debug("Build has not been submitted")
            return False

        build_request = self.executor_api.get(build.build_request_ref)
        if build_request:
            log.debug("Requesting resume for build %s", build)
            self.executor_api.requestResume(build_request)
            return True
        return False

    def removeBuild(self, build):
        log = get_annotated_logger(self.log, build.zuul_event_id)
        log.debug("Removing build %s", build.uuid)

        if not build.build_request_ref:
            log.debug("Build %s has not been submitted to ZooKeeper",
                      build.uuid)
            return

        build_request = self.executor_api.get(build.build_request_ref)
        if build_request:
            # TODO (felix): We could directly remove the build request via
            # its path in ZK to spare a read operation. Usually there should
            # be no need to look up the build request object from ZooKeeper
            # just to immediately remove it.
            self.executor_api.remove(build_request)

    def cleanupLostBuildRequests(self):
        for build_request in self.executor_api.lostRequests():
            try:
                self.cleanupLostBuildRequest(build_request)
            except Exception:
                self.log.exception("Exception cleaning up lost build request:")

    def cleanupLostBuildRequest(self, build_request):
        result = {"result": "ABORTED"}

        # TODO (felix): Once the builds are stored in ZooKeeper, we can store
        # the end_time directly on the build. But for now we have to use the
        # result dict for that.
        result["end_time"] = time.time()

        build_request.state = BuildRequest.COMPLETED
        try:
            self.executor_api.update(build_request)
        except JobRequestNotFound as e:
            self.log.warning("Could not complete build: %s", str(e))
            # In case we re-created the lock directory, still remove
            # the request for the side effect of removing the lock.
            self.executor_api.remove(build_request)
            return
        except BadVersionError:
            # There could be a race condition:
            # The build is found by lost_builds in state RUNNING
            # but gets completed/unlocked before the is_locked()
            # check. Since we use the znode version, the update
            # will fail in this case and we can simply ignore the
            # exception.
            return

        # No need to unlock the build, as it is by definition unlocked

        event = BuildCompletedEvent(
            build_request.uuid, build_request.build_set_uuid,
            build_request.job_uuid,
            build_request.path, result)
        self.result_events[build_request.tenant_name][
            build_request.pipeline_name].put(event)
