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

import logging
import voluptuous as v

from zuul.trigger import BaseTrigger
from zuul.driver.timer.timermodel import TimerEventFilter
from zuul.driver.util import to_list, make_regex


class TimerTrigger(BaseTrigger):
    name = 'timer'

    def __init__(self, driver, config=None):
        super().__init__(driver, config)
        self.log = logging.getLogger("zuul.TimerTrigger")

    def getEventFilters(self, connection_name, trigger_conf, parse_context):
        efilters = []
        pcontext = parse_context

        self.log.debug("Raw trigger configuration: %s", trigger_conf)

        for trigger in to_list(trigger_conf):
            self.log.debug("Processing timer trigger config: %s", trigger)

            self.log.debug("Available keys in trigger: %s",
                           list(trigger.keys()))

            with pcontext.confAttr(trigger, 'branch') as attr:
                branches = [make_regex(x, pcontext)
                            for x in to_list(attr)]
                self.log.debug("Parsed branch patterns: %s",
                               [b.pattern for b in branches])

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
            # ADD DEBUG: Show created filter details
            self.log.debug("Created TimerEventFilter: %s", f)
            efilters.append(f)
        return efilters


def getSchema():
    timer_trigger = {
        v.Required('time'): str,
        'branch': str,
        'dereference': bool,
        'debug': bool,
    }
    return timer_trigger
