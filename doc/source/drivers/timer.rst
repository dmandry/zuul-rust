:title: Timer Driver

Timer
=====

The timer driver supports triggers only.  It is used for configuring
pipelines so that jobs run at scheduled times.  No connection
configuration is required.

Trigger Configuration
---------------------

Timers don't require a special connection or driver. Instead they can
simply be used by listing ``timer`` as the trigger.

This trigger will run based on a cron-style time specification.  It
will enqueue an event into its pipeline for every project and branch
defined in the configuration.  Any job associated with the pipeline
will run in response to that event.

Zuul implements the timer using `apscheduler`_, Please check the
`apscheduler documentation`_ for more information about the syntax.

.. attr:: pipeline.trigger.timer

   The timer trigger supports the following attributes:

   .. attr:: time
      :required:

      The time specification.  Two formats are accepted:

      **Cron syntax**

      Only the 5 part syntax is supported, not the symbolic names.
      Example: ``0 0 * * *`` runs at midnight.

      An optional 6th part specifies seconds.  The optional 7th part
      specifies a jitter in seconds. This delays the trigger randomly,
      limited by the specified value.  Example ``0 0 * * * * 60`` runs
      at midnight or randomly up to 60 seconds later.  The jitter is
      applied individually to each project-branch combination.  While
      the jitter is initialized to a random value, the same value will
      often be used for a given project-branch combination (in other
      words, it is not guaranteed to vary from one run of the timer
      trigger to the next).

      **iCalendar RRULE syntax**

      The ``time`` value may alternatively be an `RFC 5545`_ RRULE
      string.  This is useful for recurrence patterns that are
      difficult or impossible to express in cron notation, such as
      the last day of the month or quarterly intervals.

      The driver auto-detects RRULE format by looking for a ``FREQ=``,
      ``DTSTART``, or ``RRULE:FREQ=`` prefix in the value (case-
      insensitive); no other configuration change is required.

      Examples:

      *Last day of the month at 22:00 UTC:*

      .. code-block:: yaml

         trigger:
           timer:
             - time: "FREQ=MONTHLY;BYMONTHDAY=-1;BYHOUR=22;BYMINUTE=0"

      *Quarterly — every three months on the first day at 06:00 UTC:*

      .. code-block:: yaml

         trigger:
           timer:
             - time: "FREQ=MONTHLY;INTERVAL=3;BYMONTHDAY=1;BYHOUR=6"

      *Weekdays only — Monday through Friday at 08:00 UTC:*

      .. code-block:: yaml

         trigger:
           timer:
             - time: "FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR;BYHOUR=8;BYMINUTE=0"

      *Fixed start date — daily at midnight, beginning 1 Jan 2026:*

      .. code-block:: yaml

         trigger:
           timer:
             - time: "DTSTART:20260101T000000Z\nRRULE:FREQ=DAILY"

      For RRULE triggers, use the separate :attr:`pipeline.trigger.timer.jitter`
      attribute instead of embedding jitter as the 7th cron field.

   .. attr:: jitter
      :default: none

      For RRULE triggers, an integer number of seconds by which the
      trigger may be randomly delayed.  The offset is applied after
      the next fire time is calculated, delaying it by up to this
      many seconds.  The offset is resolved deterministically from the
      ``(tenant, project, branch)`` tuple so that every scheduler in a
      cluster independently arrives at the same value — no coordination
      is needed.

      This attribute is ignored when using cron syntax; for cron
      triggers, embed the jitter as the optional 7th field of the
      ``time`` value instead.

   .. attr:: dereference
      :default: false

      Whether the branch tip should be dereferenced when enqueued.

      This controls the behavior when the timer trigger for a given
      project-branch activates a second or more time for a given
      project-branch while a queue item for that project-branch is
      still in the pipeline.

      If set to the default value of ``false``, then the triggering
      event and queue item will only include the name of the branch;
      this means that Zuul will see an identical queue item in the
      pipeline and will not enqueue a duplicate entry.

      If set to ``true`` then Zuul will look up the current Git sha of
      the tip of each project-branch when enqueueing that
      project-branch and include that information in the triggering
      event and queue item.  If the timer trigger activates a second
      time while a given project-branch is still in the pipeline, the
      behavior then depends on whether the Git commit sha differs.  If
      the branch has changed between the two activations, Zuul will
      treat the second activation as distinct and enqueue a new item
      for the same project-branch (but with a different ``newrev``
      value).  If the Git commit sha is the same on both activations,
      Zuul will not enqueue a second entry.

   .. warning::
       Be aware the day-of-week value differs from cron.
       The first weekday is Monday (0), and the last is Sunday (6).

   .. attr:: debug
      :default: false

      When set to `true`, this will cause debug messages to be
      included when the queue item is reported.  These debug messages
      may be used to help diagnose why certain jobs did or did not
      run, and in many cases, why the item was not ultimately enqueued
      into the pipeline.

      Setting this value also effectively sets
      :attr:`project.<pipeline>.debug` for affected queue items.

      This only applies to items that arrive at a pipeline via this
      particular trigger.  Since the output is very verbose and
      typically not needed or desired, this allows for a configuration
      where typical pipeline triggers omit the debug output, but
      triggers that match certain specific criteria may be used to
      request debug information.


.. _apscheduler: https://apscheduler.readthedocs.io/
.. _apscheduler documentation: https://apscheduler.readthedocs.io/en/3.x/modules/triggers/cron.html#module-apscheduler.triggers.cron
.. _RFC 5545: https://datatracker.ietf.org/doc/html/rfc5545
