# Copyright 2017 Red Hat, Inc.
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

from zuul.model import EventFilter, FalseWithReason, TriggerEvent


class TimerEventFilter(EventFilter):
    def __init__(self, connection_name, trigger, types=[], timespecs=[],
                 branches=[], dereference=False, debug=None):
        EventFilter.__init__(self, connection_name, trigger, debug)

        self._types = [x.pattern for x in types]
        self.types = types
        self.timespecs = timespecs
        self._branches = [x.pattern for x in branches]
        self.branches = branches
        self.dereference = dereference

    def __repr__(self):
        ret = '<TimerEventFilter'
        ret += ' connection: %s' % self.connection_name

        if self._types:
            ret += ' types: %s' % ', '.join(self._types)
        if self.timespecs:
            ret += ' timespecs: %s' % ', '.join(self.timespecs)
        if self._branches:
            ret += ' branches: %s' % ', '.join(self._branches)
        ret += '>'
        return ret

    def matches(self, event, change):
        # event types are ORed
        matches_type = False
        for etype in self.types:
            if etype.match(event.type):
                matches_type = True
        if self.types and not matches_type:
            return FalseWithReason("Types %s do not match %s" % (
                self.types, event.type))

        # branches are ORed
        matches_branch = False
        for branch in self.branches:
            if branch.match(event.branch):
                matches_branch = True
                break
        if self.branches and not matches_branch:
            return FalseWithReason("Branches %s do not match %s" % (
                self.branches, event.branch))

        matches_timespec = False
        for timespec in self.timespecs:
            if event.timespec == timespec:
                matches_timespec = True
                break
        if self.timespecs and not matches_timespec:
            return FalseWithReason("Timespecs %s do not match %s" % (
                self.timespecs, event.timespec))

        return True


class TimerTriggerEvent(TriggerEvent):
    def __init__(self):
        super(TimerTriggerEvent, self).__init__()
        self.timespec = None

    def toDict(self):
        d = super().toDict()
        d["timespec"] = self.timespec
        return d

    def updateFromDict(self, d):
        super().updateFromDict(d)
        self.timespec = d["timespec"]
