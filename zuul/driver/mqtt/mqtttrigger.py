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
from zuul.driver.util import scalar_or_list, to_list


class MQTTTrigger(BaseTrigger):
    name = 'mqtt'
    log = logging.getLogger("zuul.MQTTTrigger")

    def getEventFilters(self, connection_name, trigger_conf,
                        parse_context):
        efilters = []
        pcontext = parse_context
        for trigger in to_list(trigger_conf):
            with pcontext.confAttr(trigger, 'subscription') as attr:
                subscription = to_list(attr)
            required_payload = None
            with pcontext.confAttr(trigger, 'required-payload') as attr:
                required_payload = attr
            data_mapping = None
            with pcontext.confAttr(trigger, 'data-mapping') as attr:
                data_mapping = attr
            with pcontext.confAttr(trigger, 'debug', default=False) as attr:
                debug = attr

            f = MQTTEventFilter(
                connection_name=connection_name,
                trigger=self,
                subscription=subscription,
                required_payload=required_payload,
                data_mapping=data_mapping,
                debug=debug,
            )
            efilters.append(f)

        return efilters


def getSchema():
    mqtt_trigger = {
        v.Required('subscription'):
            scalar_or_list(str),
        'required-payload': dict,
        v.Required('data-mapping'): dict,
        'debug': bool,
    }

    return mqtt_trigger
