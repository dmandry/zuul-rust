# Copyright 2017 Red Hat, Inc.
# Copyright 2026 Acme Gating, LLC
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
import voluptuous as v
from zuul.trigger import BaseTrigger
from zuul.driver.mqtt.mqttmodel import MQTTEventFilter
from zuul.driver.util import scalar_or_list, to_list, make_regex, ZUUL_REGEX


class MQTTTrigger(BaseTrigger):
    name = 'mqtt'
    log = logging.getLogger("zuul.MQTTTrigger")

    def getEventFilters(self, connection_name, trigger_conf,
                        parse_context):
        efilters = []
        pcontext = parse_context
        for trigger in to_list(trigger_conf):
            # TODO(clarkb) roughed in based on the git driver.
            # need to figure out what filters make sense for MQTT
            with pcontext.confAttr(trigger, 'topic') as attr:
                topic = [make_regex(x, pcontext)
                         for x in to_list(attr)]
            refs = None
            with pcontext.confAttr(trigger, 'ref') as attr:
                refs = [make_regex(x, pcontext)
                        for x in to_list(attr)]
            tenant = None
            if trigger.get('tenant'):
                with pcontext.confAttr(trigger, 'tenant') as attr:
                    tenant = [make_regex(x, pcontext)
                              for x in to_list(attr)]
            project = None
            if trigger.get('project'):
                with pcontext.confAttr(trigger, 'project') as attr:
                    project = [make_regex(x, pcontext)
                               for x in to_list(attr)]
            pipeline = None
            if trigger.get('pipeline'):
                with pcontext.confAttr(trigger, 'pipeline') as attr:
                    pipeline = [make_regex(x, pcontext)
                                for x in to_list(attr)]
            action = None
            if trigger.get('action'):
                action = to_list(trigger['action'])

            f = MQTTEventFilter(
                connection_name=connection_name,
                trigger=self,
                types=to_list(trigger['event']),
                topic=topic,
                refs=refs,
                action=action,
                tenant=tenant,
                project=project,
                pipeline=pipeline,
            )
            efilters.append(f)

        return efilters


def getSchema():
    actions = [
        'enqueue',
        'start',
        'success',
        'failure',
        'merge-conflict',
        'config-error',
        'no-jobs',
        'disabled',
        'dequeue',
    ]
    mqtt_trigger = {
        v.Required('event'):
            scalar_or_list(v.Any('ref-updated')),
        v.Required('topic'):
            scalar_or_list(v.Any(ZUUL_REGEX, str)),
        'ref': scalar_or_list(v.Any(ZUUL_REGEX, str)),
        'action': scalar_or_list(v.Any(*actions)),
        'tenant': scalar_or_list(v.Any(ZUUL_REGEX, str)),
        'project': scalar_or_list(v.Any(ZUUL_REGEX, str)),
        'pipeline': scalar_or_list(v.Any(ZUUL_REGEX, str))
    }

    return mqtt_trigger
