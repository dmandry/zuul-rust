# Copyright 2024 BMW Group
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
import threading
import time

from collections import defaultdict
from zuul.model import (
    NodesetInfo,
    NodesetRequest,
    ProviderNode,
    STATE_HOLD,
    STATE_IN_USE,
    STATE_READY,
    STATE_USED,
)
from zuul.lib import tracing
from zuul.lib.logutil import get_annotated_logger
from zuul.zk.vendor.watchers import ExistingDataWatch
from zuul.zk.zkobject import ZKContext

from kazoo.exceptions import NoNodeError
from opentelemetry import trace


class LauncherClient:
    log = logging.getLogger("zuul.LauncherClient")
    tracer = trace.get_tracer("zuul")
    # The kind of resources we report stats on.  We need a complete
    # list in order to report 0 level gauges.
    resource_types = ('ram', 'cores', 'instances')
    HOLD_REQUEST_ROOT = '/zuul/hold-requests'

    def __init__(self, zk_client, stop_event, component_info=None):
        self.component_info = component_info
        self.zk_client = zk_client
        self.stop_event = stop_event

    def requestNodeset(self, item, job, priority, relative_priority,
                       preferred_provider):
        log = get_annotated_logger(self.log, item.event)
        labels = [n.label for n in job.nodeset.getNodes()]

        buildset = item.current_build_set
        parent_span = tracing.restoreSpan(buildset.span_info)
        request_time = time.time()
        with trace.use_span(parent_span):
            request_span = self.tracer.start_span(
                "NodesetRequest", start_time=request_time)
        span_info = tracing.getSpanInfo(request_span)

        image_names = getattr(item.event, "image_names", None)
        image_upload_uuid = getattr(item.event, "image_upload_uuid", None)

        with self.createZKContext(None, self.log) as ctx:
            request = NodesetRequest.new(
                ctx,
                **self._getInitialRequestStates(job),
                tenant_name=item.manager.tenant.name,
                pipeline_name=item.manager.pipeline.name,
                buildset_uuid=buildset.uuid,
                job_uuid=job.uuid,
                job_name=job.name,
                labels=labels,
                priority=priority,
                _relative_priority=relative_priority,
                request_time=request_time,
                zuul_event_id=item.event.zuul_event_id,
                span_info=span_info,
                image_names=image_names,
                image_upload_uuid=image_upload_uuid,
                preferred_provider=preferred_provider,
            )
            log.info("Submitted nodeset request %s", request)
        return request

    def reviseRequest(self, request, relative_priority):
        log = get_annotated_logger(self.log, request)
        try:
            with self.createZKContext(None, self.log) as ctx:
                request.revise(ctx, relative_priority=relative_priority)
            log.info("Revised nodeset request %s; relative_priority=%s",
                     request, relative_priority)
        except NoNodeError:
            pass

    def getRequest(self, request_id):
        try:
            with self.createZKContext(None, self.log) as ctx:
                return NodesetRequest.fromZK(ctx, path=None, uuid=request_id)
        except NoNodeError:
            return None

    def deleteRequest(self, request):
        try:
            with self.createZKContext(None, self.log) as ctx:
                request.delete(ctx)
        except NoNodeError:
            pass

    def getRequestIds(self):
        # Do not use this if a cache is available; this should only be
        # used by components (like the scheduler) which do not
        # otherwise have a cache.
        path = f'{NodesetRequest.ROOT}/{NodesetRequest.REQUESTS_PATH}'
        try:
            return self.zk_client.client.get_children(path)
        except NoNodeError:
            return []

    def getProviderNode(self, node_id):
        # Do not use this if a cache is available; this should only be
        # used by components (like the scheduler) which do not
        # otherwise have a cache.
        path = ProviderNode._getPath(node_id)
        with self.createZKContext(None, self.log) as ctx:
            return ProviderNode.fromZK(ctx, path)

    def getNodesetInfo(self, request):
        # TODO: populated other nodeset info fields
        zone = request.executor_zones[0] if request.executor_zones else None
        return NodesetInfo(
            nodes=list(request.nodes),
            zone=zone)

    def acceptNodeset(self, request, nodeset):
        log = get_annotated_logger(self.log, request)
        log.debug("Accepting nodeset for request %s", request)
        try:
            with self.createZKContext(None, self.log) as ctx:
                for node_id, node in zip(
                    request.nodes, nodeset.getNodes()
                ):
                    provider_node = ProviderNode.fromZK(
                        ctx, path=ProviderNode._getPath(node_id))
                    node.updateFromDict({
                        "state": STATE_READY,
                        **provider_node.getNodeData()
                    })
                    node._provider_node = provider_node
                    log.debug("Locking node %s", provider_node)
                    if not provider_node.acquireLock(ctx):
                        raise Exception(f"Failed to lock node {provider_node}")
        except Exception:
            self.returnNodeset(nodeset)
            raise
        finally:
            self.deleteRequest(request)

    def useNodeset(self, nodeset, zuul_event_id=None):
        log = get_annotated_logger(self.log, zuul_event_id)
        log.info("Setting nodeset %s in use", nodeset)
        for node in nodeset.getNodes():
            provider_node = getattr(node, "_provider_node", None)
            if not provider_node:
                raise Exception("No associated provider node for %s", node)
            if not provider_node.hasLock():
                raise Exception("Provider node %s is not locked",
                                provider_node)
            node.state = STATE_IN_USE
            with self.createZKContext(provider_node._lock, log) as ctx:
                provider_node.updateAttributes(
                    ctx, state=ProviderNode.State.IN_USE)

    def returnNodeset(self, nodeset, zuul_event_id=None):
        log = get_annotated_logger(self.log, zuul_event_id)
        log.debug("Returning nodeset %s", nodeset)
        for node in nodeset.getNodes():
            provider_node = getattr(node, "_provider_node", None)
            if not provider_node:
                continue
            with self.createZKContext(provider_node._lock, log) as ctx:
                try:
                    if provider_node.state == provider_node.State.IN_USE:
                        node.state = STATE_USED
                        provider_node.updateAttributes(
                            ctx, state=ProviderNode.State.USED)
                        log.debug("Released %s", provider_node)
                except Exception:
                    log.exception("Unable to return node %s", provider_node)
                finally:
                    try:
                        provider_node.releaseLock(ctx)
                    except Exception:
                        log.exception("Error unlocking node %s", provider_node)

    def snapshotNodeset(self, nodeset, node_ids, zuul_event_id=None):
        log = get_annotated_logger(self.log, zuul_event_id)
        log.debug("Snapshotting nodes %s in nodeset %s", node_ids, nodeset)

        wait_event = threading.Event()
        wait_snapshots = []

        def node_watcher(self, data, zstat, event=None):
            wait_event.set()

        for node in nodeset.getNodes():
            provider_node = getattr(node, "_provider_node", None)
            if not provider_node:
                continue
            if provider_node.uuid not in node_ids:
                continue
            with self.createZKContext(provider_node._lock, log) as ctx:
                try:
                    if provider_node.state == provider_node.State.IN_USE:
                        with provider_node.activeContext(ctx):
                            provider_node.createSnapshot(ctx)
                            provider_node.setState(ProviderNode.State.SNAPSHOT)
                        ExistingDataWatch(self.zk_client.client,
                                          provider_node.snapshot.getPath(),
                                          node_watcher)
                        wait_snapshots.append(provider_node.snapshot)
                        log.debug("Set %s to snapshot", provider_node)
                except Exception:
                    log.exception("Unable to snapshot node %s", provider_node)

        if not wait_snapshots:
            log.debug("Nothing to snapshot in nodeset %s", nodeset)
            return
        done = False
        log.debug("Waiting for snapshots in nodeset %s", nodeset)
        while not done:
            wait_event.wait()
            wait_event.clear()
            done = True
            with self.createZKContext(provider_node._lock, log) as ctx:
                for snapshot in wait_snapshots:
                    snapshot.refresh(ctx)
                    if not snapshot.complete:
                        done = False
        log.debug("Finished snapshots in nodeset %s", nodeset)

    def addResources(self, target, source):
        for key, value in source.items():
            if key in self.resource_types:
                target[key] += value

    def holdNodeSet(self, zk_nodepool, nodeset, request, build, duration,
                    zuul_event_id=None):
        '''
        Perform a hold on the given set of nodes.

        :param NodeSet nodeset: The object containing the set of nodes to hold.
        :param HoldRequest request: Hold request associated with the NodeSet
        '''
        log = get_annotated_logger(self.log, zuul_event_id)
        log.info("Holding nodeset %s", nodeset)
        resources = defaultdict(int)
        nodes = nodeset.getNodes()

        log.info(
            "Nodeset %s with %s nodes was in use for %s seconds for build %s "
            "for project %s",
            nodeset, len(nodeset.nodes), duration, build, request.project)

        node_ids = []
        for node in nodes:
            provider_node = getattr(node, "_provider_node", None)
            if not provider_node:
                raise Exception("No associated provider node for %s", node)
            if not provider_node.hasLock():
                raise Exception("Provider node %s is not locked",
                                provider_node)
            if node.resources:
                self.addResources(resources, node.resources)

            node_ids.append(provider_node.uuid)
            with self.createZKContext(provider_node._lock, log) as ctx:
                with provider_node.activeContext(ctx):
                    provider_node.setState(ProviderNode.State.HOLD)
                    provider_node.comment = request.reason
                    if request.node_expiration:
                        provider_node.hold_expiration = request.node_expiration
                log.debug("Held %s", provider_node)
            node.state = STATE_HOLD

        request.nodes.append(dict(
            niz=True,
            build=build.uuid,
            nodes=node_ids,
        ))
        request.current_count += 1

        # Request has been used at least the maximum number of times so set
        # the expiration time so that it can be auto-deleted.
        if request.current_count >= request.max_count and not request.expired:
            request.expired = time.time()

        # Give ourselves a few seconds to try to obtain the lock rather than
        # immediately give up.
        zk_nodepool.lockHoldRequest(request, timeout=5)

        try:
            zk_nodepool.storeHoldRequest(request)
        except Exception:
            # If we fail to update the request count, we won't consider it
            # a real autohold error by passing the exception up. It will
            # just get used more than the original count specified.
            # It's possible to leak some held nodes, though, which would
            # require manual node deletes.
            log.exception("Unable to update hold request %s:", request)
        finally:
            # Although any exceptions thrown here are handled higher up in
            # _doBuildCompletedEvent, we always want to try to unlock it.
            zk_nodepool.unlockHoldRequest(request)

        # TODO: implement stats reporting
        # if resources and duration:
        #     self.emitStatsResourceCounters(
        #         request.tenant, request.project, resources, duration)

    def markHeldNodesAsUsed(self, hold_request, node_group):
        """
        Changes the state for each held node for the hold request to 'used'.

        :returns: True if all nodes marked USED, False otherwise.
        """

        failure = False
        for node_id in node_group['nodes']:
            provider_node = self.getProviderNode(node_id)
            if not provider_node:
                continue
            if provider_node.state == provider_node.State.USED:
                continue
            with self.createZKContext(None, self.log) as ctx:
                if not provider_node.acquireLock(ctx):
                    raise Exception(f"Failed to lock node {provider_node}")
            with self.createZKContext(provider_node._lock, self.log) as ctx:
                try:
                    with provider_node.activeContext(ctx):
                        provider_node.setState(ProviderNode.State.USED)
                    self.log.debug("Released %s", provider_node)
                except Exception:
                    failure = True
                    self.log.exception("Unable to return node %s",
                                       provider_node)
                finally:
                    try:
                        provider_node.releaseLock(ctx)
                    except Exception:
                        self.log.exception("Error unlocking node %s",
                                           provider_node)

        return not failure

    def createZKContext(self, lock, log):
        if self.component_info:
            identifier = self.component_info.hostname
        else:
            identifier = None
        return ZKContext(self.zk_client, lock, self.stop_event, log,
                         default_lock_identifier=identifier)

    def _getInitialRequestStates(self, job):
        if not job.nodeset.nodes:
            # Nodeless job
            return dict(
                state=NodesetRequest.State.FULFILLED,
                event_state=NodesetRequest.EventState.COMPLETE,
            )
        # Use default states
        return dict()

    def setNextNodeState(self, node, next_state, cache=None):
        if cache:
            getNode = cache.getItem
        else:
            getNode = self.getProviderNode

        if node.subnodes:
            nodes = [getNode(n) for n in node.subnodes]
        else:
            nodes = [node]

        with self.createZKContext(None, self.log) as ctx:
            for n in nodes:
                n.setNextState(ctx, next_state)

        return nodes
