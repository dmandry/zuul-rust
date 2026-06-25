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

import json
from uuid import uuid4

import jsonpath_rw
import paho.mqtt.client as mqtt

from zuul.model import TriggerEvent
from zuul.model import EventFilter
from zuul.model import FalseWithReason


class MQTTTriggerEvent(TriggerEvent):
    """Incoming event from an external system."""

    def toDict(self):
        ret = super().toDict()
        ret['topic'] = self.topic
        ret['payload'] = self.payload
        return ret

    def updateFromDict(self, d):
        super().updateFromDict(d)
        self.topic = d['topic']
        self.payload = d['payload']

    def __repr__(self):
        ret = '<MQTTTriggerEvent %s %s/%s' % \
            (self.topic, self.project_hostname, self.project_name)

        if hasattr(self, 'ref'):
            ret += ' ref:%s' % self.ref
        if hasattr(self, 'branch'):
            ret += ' branch:%s' % self.branch
        if hasattr(self, 'change_number'):
            ret += ' change:%s' % self.change_number
            if hasattr(self, 'patchset'):
                ret += ',%s' % self.patchset
        # TODO(clarkb) is payload too verbose? Should we drop it?
        ret += ' payload:%s' % self.payload
        ret += '>'

        return ret


class MQTTEventFilter(EventFilter):
    def __init__(self, connection_name, trigger, topic=None,
                 required_payload=None, data_mapping=None, debug=False):

        super().__init__(connection_name, trigger)

        self.topic = topic if topic is not None else []
        self.required_payload = []
        if required_payload:
            for k, v in required_payload.items():
                expr = jsonpath_rw.parse(k)
                self.required_payload.append((expr, v))
        self._required_payload = required_payload
        self.data_mapping = data_mapping
        self.debug = debug

    def __repr__(self):
        ret = '<MQTTEventFilter'
        ret += ' connection: %s' % self.connection_name

        if self.topic:
            ret += ' topic: %s' % ', '.join(self.topic)
        if self._required_payload:
            ret += f' payload: {self._required_payload}'
        ret += '>'

        return ret

    def matches(self, event, change):
        if not super().matches(event, change):
            return False

        # Topics are ORed
        matches_topic = False
        for topic in self.topic:
            if mqtt.topic_matches_sub(topic, event.topic):
                matches_topic = True
        if self.topic and not matches_topic:
            return FalseWithReason(
                'Trigger topic "%s" does not match event topic "%s"' % (
                    self.topic, event.topic))

        # Required payloads are ANDed
        for expr, value in self.required_payload:
            found = expr.find(event.payload)
            if found:
                found = found[0].value
            if value != found:
                return FalseWithReason(
                    'Payload filter "%s": "%s" does not match event '
                    'payload value "%s"' % (expr, value, found))

        return True

    def getTransformer(self):
        return MQTTEventTransformer(self.connection_name, self.topic,
                                    self.data_mapping, self.debug)


class MQTTEventTransformer:
    attr_map = {
        'change': 'change_number',
        'patchset': 'patch_number',
        'branch': 'branch',
        'oldrev': 'oldrev',
        'newrev': 'newrev',
        'ref': 'ref',
        'url': 'change_url',
        'project': 'project_name',
        'project-hostname': 'project_hostname',
        'event-id': 'zuul_event_id',
    }

    def __init__(self, connection_name, topic, data_mapping, debug):
        self.connection_name = connection_name
        self.topic = topic
        self.data_mapping = []
        self._data_mapping = data_mapping
        if data_mapping:
            for k, v in data_mapping.items():
                expr = jsonpath_rw.parse(v)
                self.data_mapping.append((expr, k))
        self._hash = hash(json.dumps({'topic': topic,
                                      'data_mapping': data_mapping},
                                     sort_keys=True).encode('utf8'))
        self.debug = debug

    def __hash__(self):
        return self._hash

    def __eq__(self, other):
        return (isinstance(other, MQTTEventTransformer) and
                self._hash == other._hash)

    def __repr__(self):
        ret = '<MQTTEventTransformer'
        ret += ' connection: %s' % self.connection_name

        if self.topic:
            ret += ' topic: %s' % ', '.join(self.topic)
        if self._data_mapping:
            ret += f' data: {self._data_mapping}'
        ret += '>'

        return ret

    def __ne__(self, other):
        return not self.__eq__(other)

    def getEvent(self, timestamp, event_topic, payload):
        matches_topic = False
        for topic in self.topic:
            if mqtt.topic_matches_sub(topic, event_topic):
                matches_topic = True
                break
        if self.topic and not matches_topic:
            return None

        mapped_data = {}
        for expr, key in self.data_mapping:
            found = expr.find(payload)
            if not found:
                return None
            mapped_data[key] = found[0].value

        event = MQTTTriggerEvent()
        event.connection_name = self.connection_name
        event.timestamp = timestamp
        event.topic = topic
        event.payload = payload

        if pcn := mapped_data.pop('project-canonical-name', None):
            canonical_name = pcn.split('/', 1)
            mapped_data['project-hostname'] = canonical_name[0]
            mapped_data['project'] = canonical_name[1]

        if not ('project-hostname' in mapped_data and
                'project' in mapped_data):
            return None

        if not ('ref' in mapped_data or 'change' in mapped_data):
            return None

        if 'event-id' not in mapped_data:
            mapped_data['event-id'] = uuid4().hex

        for k, v in mapped_data.items():
            mapped_k = self.attr_map.get(k, k)
            setattr(event, mapped_k, v)
        return event
