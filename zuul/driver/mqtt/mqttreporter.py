# Copyright 2017 Red Hat, Inc.
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

import logging
import time
import voluptuous as v

from zuul.lib.logutil import get_annotated_logger
from zuul.lib.result_data import get_artifacts_from_result_data
from zuul.reporter import BaseReporter


class MQTTReporter(BaseReporter):
    """Publish messages to a topic via mqtt"""

    name = 'mqtt'
    log = logging.getLogger("zuul.MQTTReporter")
    invalid_topic_chars = str.maketrans({
        '+': '_',
        '#': '_',
    })

    def report(self, item, phase1=True, phase2=True):
        if not phase1:
            return
        include_returned_data = self.config.get('include-returned-data')
        log = get_annotated_logger(self.log, item.event)
        log.debug("Report %s, params %s", item, self.config)
        buildset = item.current_build_set
        changes = [
            {
                'project': change.project.name,
                'branch': getattr(change, 'branch', ''),
                'change_url': change.url,
                'change': getattr(change, 'number', ''),
                'patchset': getattr(change, 'patchset', ''),
                'commit_id': getattr(change, 'commit_id', ''),
                'owner': getattr(change, 'owner', ''),
                'ref': getattr(change, 'ref', ''),
                'topic': getattr(change, 'topic', ''),
            }
            for change in item.changes
        ]
        message = {
            'timestamp': time.time(),
            'action': self._action,
            'tenant': item.manager.tenant.name,
            'tenant_web_root': item.formatUrlPattern(
                item.manager.tenant.web_root),
            'zuul_ref': buildset.ref,
            'pipeline': item.manager.pipeline.name,
            'queue': item.queue.name,
            'changes': changes,
            'project': item.changes[0].project.name,
            'branch': getattr(item.changes[0], 'branch', ''),
            'change_url': item.changes[0].url,
            'change': getattr(item.changes[0], 'number', ''),
            'patchset': getattr(item.changes[0], 'patchset', ''),
            'commit_id': getattr(item.changes[0], 'commit_id', ''),
            'owner': getattr(item.changes[0], 'owner', ''),
            'ref': getattr(item.changes[0], 'ref', ''),
            'message': self._formatItemReport(
                item, with_jobs=False),
            'trigger_time': item.event.timestamp,
            'enqueue_time': item.enqueue_time,
            'uuid': item.uuid,
            'buildset': {
                'uuid': buildset.uuid,
                'result': buildset.result,
                'builds': [],
                'retry_builds': [],
            },
            'zuul_event_id': item.event.zuul_event_id,
        }
        for job in item.getJobs():
            job_informations = {
                'job_name': job.name,
                'job_uuid': job.uuid,
                'voting': job.voting,
            }
            build = buildset.getBuild(job)
            if build:
                # Report build data if available
                (result, web_url) = item.formatJobResult(job)
                change = item.getChangeForJob(job)
                change_info = {
                    'project': change.project.name,
                    'branch': getattr(change, 'branch', ''),
                    'change_url': change.url,
                    'change': getattr(change, 'number', ''),
                    'patchset': getattr(change, 'patchset', ''),
                    'commit_id': getattr(change, 'commit_id', ''),
                    'owner': getattr(change, 'owner', ''),
                    'ref': getattr(change, 'ref', ''),
                }
                # Build a list of job dependencies as `job.dependencies`
                # only gives us `JobDependency` instances that don't
                # have any information about the UUID of the exact job
                # dependency (there can be multiple jobs with the same
                # name in one job graph).
                dependencies = [
                    item.getJob(jid)for jid in
                    buildset.job_graph.job_dependencies[job.uuid]
                ]
                job_informations.update({
                    'change': change_info,
                    'uuid': build.uuid,
                    'start_time': build.start_time,
                    'end_time': build.end_time,
                    'execute_time': build.execute_time,
                    'log_url': build.log_url,
                    'web_url': web_url,
                    'result': result,
                    'dependencies': [j.name for j in dependencies],
                    'job_dependencies': {j.name: j.uuid for j in dependencies},
                    'artifacts': get_artifacts_from_result_data(
                        build.result_data, logger=log),
                    'events': [e.toDict() for e in build.events],
                })
                if include_returned_data:
                    rdata = build.result_data.copy()
                    rdata.pop('zuul', None)
                    job_informations['returned_data'] = rdata

                # Report build data of retried builds if available
                retry_builds = buildset.getRetryBuildsForJob(
                    job)
                for retry_build in retry_builds:
                    (result, web_url) = item.formatJobResult(job, retry_build)
                    retry_build_information = {
                        'job_name': job.name,
                        'job_uuid': job.uuid,
                        'voting': job.voting,
                        'uuid': retry_build.uuid,
                        'start_time': retry_build.start_time,
                        'end_time': retry_build.end_time,
                        'execute_time': retry_build.execute_time,
                        'log_url': retry_build.log_url,
                        'web_url': web_url,
                        'result': result,
                    }
                    message['buildset']['retry_builds'].append(
                        retry_build_information)

            message['buildset']['builds'].append(job_informations)
        topic = None
        try:
            topic = self.config['topic'].format(
                tenant=item.manager.tenant.name,
                pipeline=item.manager.pipeline.name,
                changes=changes,
                project=item.changes[0].project.name,
                branch=getattr(item.changes[0], 'branch', None),
                change=getattr(item.changes[0], 'number', None),
                patchset=getattr(item.changes[0], 'patchset', None),
                ref=getattr(item.changes[0], 'ref', None))
            topic = topic.translate(self.invalid_topic_chars)
        except Exception:
            log.exception("Error while formatting MQTT topic %s:",
                          self.config['topic'])
        if topic is not None:
            self.connection.publish(
                topic, message, self.config.get('qos', 0), item.event)


def topicValue(value):
    if not isinstance(value, str):
        raise v.Invalid("topic is not a string")
    try:
        value.format(
            tenant='test',
            pipeline='test',
            project='test',
            branch='test',
            change='test',
            patchset='test',
            ref='test')
    except KeyError as e:
        raise v.Invalid("topic component %s is invalid" % str(e))
    return value


def qosValue(value):
    if not isinstance(value, int):
        raise v.Invalid("qos is not a integer")
    if value not in (0, 1, 2):
        raise v.Invalid("qos can only be 0, 1 or 2")
    return value


def getSchema():
    return v.Schema({
        v.Required('topic'): topicValue,
        'qos': qosValue,
        'include-returned-data': bool,
    })
