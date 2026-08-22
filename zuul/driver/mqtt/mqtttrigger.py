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
            payload = None
            with pcontext.confAttr(trigger, 'payload') as attr:
                payload = attr
            data = None
            with pcontext.confAttr(trigger, 'data') as attr:
                data = attr

            f = MQTTEventFilter(
                connection_name=connection_name,
                trigger=self,
                topic=topic,
                payload=payload,
                data=data,
            )
            efilters.append(f)

        return efilters


def getSchema():
    mqtt_trigger = {
        v.Required('topic'):
            scalar_or_list(v.Any(ZUUL_REGEX, str)),
        'payload': dict,
        'data': dict,
    }

    return mqtt_trigger
