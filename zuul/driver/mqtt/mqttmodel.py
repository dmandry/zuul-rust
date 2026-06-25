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

from zuul.model import TriggerEvent
from zuul.model import EventFilter


class MQTTTriggerEvent(TriggerEvent):
    """Incoming event from an external system."""

    def __repr__(self):
        ret = '<MQTTTriggerEvent %s %s' % (self.type,
                                           self.topic)

        if self.action:
            ret += " action:%s" % self.action
        if self.tenant:
            ret += " tenant:%s" % self.tenant
        if self.project:
            ret += " project:%s" % self.project
        if self.project:
            ret += " pipeline:%s" % self.pipeline
        ret += '>'

        return ret


class MQTTEventFilter(EventFilter):
    def __init__(self, connection_name, trigger, types=None, topic=None,
                 refs=None, action=None, tenant=None, project=None,
                 pipeline=None):

        super().__init__(connection_name, trigger)

        self.types = types if types is not None else []
        self.topic = topic if topic is not None else []
        self._topic = [x.pattern for x in self.topic]
        self.refs = refs if refs is not None else []
        self._refs = [x.pattern for x in self.refs]
        self.action = action if action is not None else []
        self.tenant = tenant if tenant is not None else []
        self._tenant = [x.pattern for x in self.tenant]
        self.project = project if project is not None else []
        self._project = [x.pattern for x in self.project]
        self.pipeline = pipeline if pipeline is not None else []
        self._pipeline = [x.pattern for x in self.pipeline]

    def __repr__(self):
        ret = '<MQTTEventFilter'
        ret += ' connection: %s' % self.connection_name

        if self.types:
            ret += ' types: %s' % ', '.join(self.types)
        if self._topic:
            ret += ' topic: %s' % ', '.join(self._topic)
        if self._refs:
            ret += ' refs: %s' % ', '.join(self._refs)
        if self.action:
            ret += ' action: %s' % ', '.join(self.action)
        if self._tenant:
            ret += ' tenant: %s' % ', '.join(self._tenant)
        if self._project:
            ret += ' project: %s' % ', '.join(self._project)
        if self._pipeline:
            ret += ' pipeline: %s' % ', '.join(self._pipeline)
        ret += '>'

        return ret

    def matches(self, event, change):
        # TODO(clarkb) setup filters
        if not super().matches(event, change):
            return False

        return True
