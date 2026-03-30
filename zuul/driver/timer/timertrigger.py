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

import voluptuous as v

from zuul.trigger import BaseTrigger
from zuul.driver.timer.timermodel import TimerEventFilter
from zuul.driver.util import scalar_or_list, to_list, make_regex, ZUUL_REGEX


class TimerTrigger(BaseTrigger):
    name = 'timer'

    def getEventFilters(self, connection_name, trigger_conf, parse_context):
        efilters = []

        for trigger in to_list(trigger_conf):
            with parse_context.confAttr(trigger, 'branch') as attr:
                branches = [make_regex(x, parse_context)
                            for x in to_list(attr)]

            types = [make_regex('timer')]
            f = TimerEventFilter(
                connection_name=connection_name,
                trigger=self,
                types=types,
                timespecs=to_list(trigger['time']),
                branches=branches,
                dereference=trigger.get('dereference', False),
                debug=trigger.get('debug'),
            )
            efilters.append(f)
        return efilters


def getSchema():
    timer_trigger = {
        v.Required('time'): str,
        'branch': scalar_or_list(v.Any(ZUUL_REGEX, str)),
        'dereference': bool,
        'debug': bool,
    }
    return timer_trigger
