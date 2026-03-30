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

import logging
from zuul.model import EventFilter, TriggerEvent


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
        self.log = logging.getLogger("zuul.TimerEventFilter")

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
        #  Start of matching
        self.log.debug("TimerEventFilter.matches called for event: %s, branch: %s",
                      event.type, getattr(event, 'branch', 'None'))
        # event types are ORed
        matches_type = False
        for etype in self.types:
            if etype.match(event.type):
                matches_type = True
                self.log.debug("Event type %s matched pattern %s",
                              event.type, etype.pattern)
        if self.types and not matches_type:
            self.log.debug("Event type %s did not match any patterns: %s",
                          event.type, [t.pattern for t in self.types])
            return False

        # BRANCH MATCHING DEBUG
        matches_branch = False
        if self.branches:
            self.log.debug("Checking branch %s against patterns: %s",
                          event.branch, [b.pattern for b in self.branches])
            for branch in self.branches:
                if branch.match(event.branch):
                    matches_branch = True
                    self.log.debug("Branch %s matched pattern %s",
                                  event.branch, branch.pattern)
                    break
            if not matches_branch:
                self.log.debug("Branch %s did not match any patterns", event.branch)
                return False
        else:
            self.log.debug("No branch filters defined, accepting all branches")

        matches_timespec = False
        self.log.debug("Checking timespec %s against: %s",
                      event.timespec, self.timespecs)
        for timespec in self.timespecs:
            if (event.timespec == timespec):
                matches_timespec = True
                self.log.debug("Timespec %s matched", timespec)
                break
        if self.timespecs and not matches_timespec:
            self.log.debug("Timespec %s did not match any: %s",
                          event.timespec, self.timespecs)
            return False

        self.log.debug("TimerEventFilter MATCHED for event branch: %s", event.branch)
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
