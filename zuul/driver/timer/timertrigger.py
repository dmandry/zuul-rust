# Copyright 2012 Hewlett-Packard Development Company, L.P.
# Copyright 2013 OpenStack Foundation
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

import json

import voluptuous as v

from zuul.trigger import BaseTrigger
from zuul.driver.timer.timermodel import TimerEventFilter
from zuul.driver.util import to_list, make_regex


class TimerTrigger(BaseTrigger):
    name = 'timer'

    def getEventFilters(self, connection_name, trigger_conf,
                        parse_context):
        efilters = []
        for trigger in to_list(trigger_conf):
            types = [make_regex('timer')]
            # Serialize dict timespecs to JSON strings so they
            # remain hashable and comparable throughout the system.
            timespecs = []
            for t in to_list(trigger['time']):
                if isinstance(t, dict):
                    timespecs.append(json.dumps(t, sort_keys=True))
                else:
                    timespecs.append(t)
            f = TimerEventFilter(
                connection_name=connection_name,
                trigger=self,
                types=types,
                timespecs=timespecs,
                dereference=trigger.get('dereference', False),
                debug=trigger.get('debug'),
            )

            efilters.append(f)

        return efilters


def getSchema():
    cron_fields = {
        v.Optional('year'): v.Any(str, int),
        v.Optional('month'): v.Any(str, int),
        v.Optional('day'): v.Any(str, int),
        v.Optional('week'): v.Any(str, int),
        v.Optional('day_of_week'): v.Any(str, int),
        v.Optional('hour'): v.Any(str, int),
        v.Optional('minute'): v.Any(str, int),
        v.Optional('second'): v.Any(str, int),
        v.Optional('jitter'): int,
    }
    timer_trigger = {
        v.Required('time'): v.Any(str, cron_fields),
        'dereference': bool,
        'debug': bool,
    }
    return timer_trigger
