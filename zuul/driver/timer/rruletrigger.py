# Copyright 2026 Electrifex Technologies Pvt Ltd.
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

import datetime
from datetime import timezone

from apscheduler.triggers.base import BaseTrigger
from dateutil.rrule import rrulestr


class ZuulRRuleTrigger(BaseTrigger):
    # Fixed UTC epoch as dtstart so .after() always finds the next occurrence
    # regardless of when the trigger object was created.  If the RRULE string
    # itself contains a DTSTART component, dateutil honours that instead.
    _EPOCH = datetime.datetime(2000, 1, 1, tzinfo=timezone.utc)

    def __init__(self, rrule_string, jitter=None):
        self._zuul_jitter = jitter
        self._rule = rrulestr(rrule_string, dtstart=self._EPOCH,
                              ignoretz=False)

    def get_next_fire_time(self, previous_fire_time, now):
        # Coerce `now` to be compatible with the rule's datetime type.
        # The rule is tz-aware (UTC epoch); APScheduler also passes tz-aware
        # datetimes, so this is a no-op in the normal path.
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)

        next_time = self._rule.after(now, inc=False)
        if next_time is None:
            return None

        # Ensure the result is always tz-aware (guards against naive dtstart
        # in a user-provided DTSTART component).
        if next_time.tzinfo is None:
            next_time = next_time.replace(tzinfo=timezone.utc)

        if self._zuul_jitter:
            next_time = next_time + datetime.timedelta(
                seconds=self._zuul_jitter)
        return next_time
