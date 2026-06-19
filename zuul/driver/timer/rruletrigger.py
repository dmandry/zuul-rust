import datetime
from datetime import timezone as _utc

from apscheduler.triggers.base import BaseTrigger
from dateutil.rrule import rrulestr


class ZuulRRuleTrigger(BaseTrigger):
    # Fixed UTC epoch as dtstart so .after() always finds the next occurrence
    # regardless of when the trigger object was created.  If the RRULE string
    # itself contains a DTSTART component, dateutil honours that instead.
    _EPOCH = datetime.datetime(2000, 1, 1, tzinfo=_utc.utc)

    def __init__(self, rrule_string, jitter=None):
        self._zuul_jitter = jitter
        self._rule = rrulestr(rrule_string, dtstart=self._EPOCH,
                              ignoretz=False)

    def get_next_fire_time(self, previous_fire_time, now):
        # Coerce `now` to be compatible with the rule's datetime type.
        # The rule is tz-aware (UTC epoch); APScheduler also passes tz-aware
        # datetimes, so this is a no-op in the normal path.
        if now.tzinfo is None:
            now = now.replace(tzinfo=_utc.utc)

        next_time = self._rule.after(now, inc=False)
        if next_time is None:
            return None

        # Ensure the result is always tz-aware (guards against naive dtstart
        # in a user-provided DTSTART component).
        if next_time.tzinfo is None:
            next_time = next_time.replace(tzinfo=_utc.utc)

        if self._zuul_jitter:
            next_time = next_time + datetime.timedelta(
                seconds=self._zuul_jitter)
        return next_time
