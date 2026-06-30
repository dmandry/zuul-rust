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

      The time specification, in one of two formats:

      **Cron string format**

      The time specification in cron syntax.  Only the 5 part syntax
      is supported, not the symbolic names.  Example: ``0 0 * * *``
      runs at midnight.

      An optional 6th part specifies seconds.  The optional 7th part
      specifies a jitter in seconds. This delays the trigger randomly,
      limited by the specified value.  Example ``0 0 * * * * 60`` runs
      at midnight or randomly up to 60 seconds later.  The jitter is
      applied individually to each project-branch combination.  While
      the jitter is initialized to a random value, the same value will
      often be used for a given project-branch combination (in other
      words, it is not guaranteed to vary from one run of the timer
      trigger to the next).

      **Named fields format**

      The field names map to APScheduler's CronTrigger parameters
      (see `apscheduler documentation`_).  This format supports
      extended day expressions which contain spaces and cannot be
      expressed in the cron string format.  It also exposes the
      ``week`` field which is not available in the cron string format.

      .. attr:: year

         4-digit year or cron expression.

      .. attr:: month

         Month (1-12) or cron expression.  Accepts abbreviated
         names (``jan`` -- ``dec``).

      .. attr:: day

         Day of month (1-31) or cron expression.  Also supports
         APScheduler's extended expressions:

         * ``last`` -- last day of the month
         * ``last fri`` -- last Friday of the month
         * ``3rd wed`` -- third Wednesday of the month

      .. attr:: hour

         Hour (0-23) or cron expression.

      .. attr:: minute

         Minute (0-59) or cron expression.

      .. attr:: second

         Second (0-59) or cron expression.

      .. attr:: day-of-week

         Day of week (0-6 or ``mon``--``sun``, Monday is 0).

      .. attr:: week

         ISO week number (1-53) or cron expression.

      .. attr:: jitter

         Jitter in seconds (integer).

      Example -- last Friday of every month at 22:00

      .. code-block:: yaml

         trigger:
           timer:
             - time:
                 minute: '0'
                 hour: '22'
                 day: 'last fri'

      Example -- every other ISO week on Wednesday at 09:00

      .. code-block:: yaml

         trigger:
           timer:
             - time:
                 minute: '0'
                 hour: '9'
                 week: '*/2'
                 day-of-week: 'wed'

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
