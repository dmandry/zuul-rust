:title: Monitoring

Monitoring
==========

.. _statsd:

Statsd reporting
----------------

Zuul comes with support for the statsd protocol, when enabled and configured
(see below), the Zuul scheduler will emit raw metrics to a statsd receiver
which let you in turn generate nice graphics.

Configuration
~~~~~~~~~~~~~

Statsd support uses the ``statsd`` python module.  Note that support
is optional and Zuul will start without the statsd python module
present.

Configuration is in the :attr:`statsd` section of ``zuul.conf``.

Metrics
~~~~~~~

These metrics are emitted by the Zuul :ref:`scheduler`:

.. stat:: zuul.event.<driver>.<type>
   :type: counter

   Zuul will report counters for each type of event it receives from
   each of its configured drivers.

.. stat:: zuul.connection.<connection>

   Holds metrics specific to connections. This hierarchy includes:

   .. stat:: cache.data_size_compressed
      :type: gauge

      The number of bytes stored in ZooKeeper for all items in this
      connection's change cache.

   .. stat:: cache.data_size_uncompressed
      :type: gauge

      The number of bytes required to for the change cache (the
      decompressed value of ``data_size_compressed``).

.. stat:: zuul.tenant.<tenant>.event_enqueue_processing_time
   :type: timer

   A timer metric reporting the time from when the scheduler receives
   a trigger event from a driver until the corresponding item is
   enqueued in a pipeline.  This measures the performance of the
   scheduler in dispatching events.

.. stat:: zuul.tenant.<tenant>.event_enqueue_time
   :type: timer

   A timer metric reporting the time from when a trigger event was
   received from the remote system to when the corresponding item is
   enqueued in a pipeline.  This includes
   :stat:`zuul.tenant.<tenant>.event_enqueue_processing_time` and any
   driver-specific pre-processing of the event.

.. stat:: zuul.tenant.<tenant>.management_events
   :type: gauge

   The size of the tenant's management event queue.

.. stat:: zuul.tenant.<tenant>.reconfiguration_time
   :type: timer

   A timer metric reporting the time taken to reconfigure a tenant.
   This is performed by one scheduler after a tenant reconfiguration
   event is received.  During this time, all processing of that
   tenant's pipelines are halted.  This measures that time.

   Once the first scheduler completes a tenant reconfiguration, other
   schedulers may update their layout in the background without
   interrupting processing.  That is not reported in this metric.

.. stat:: zuul.tenant.<tenant>.trigger_events
   :type: gauge

   The size of the tenant's trigger event queue.

.. stat:: zuul.tenant.<tenant>.pipeline

   Holds metrics specific to jobs. This hierarchy includes:

   .. stat:: <pipeline>

      A set of metrics for each pipeline named as defined in the Zuul
      config.

      .. stat:: event_enqueue_time
         :type: timer

         The time elapsed from when a trigger event was received from
         the remote system to when the corresponding item is enqueued
         in a pipeline.

      .. stat:: merge_request_time
         :type: timer

         The amount of time spent waiting for the initial merge
         operation(s).  This will always include a request to a Zuul
         merger to speculatively merge the change, but it may also
         include a second request submitted in parallel to identify
         the files altered by the change.  Includes
         :stat:`zuul.tenant.<tenant>.pipeline.<pipeline>.merger_merge_op_time`
         and
         :stat:`zuul.tenant.<tenant>.pipeline.<pipeline>.merger_files_changes_op_time`.

      .. stat:: merger_merge_op_time
         :type: timer

         The amount of time the merger spent performing a merge
         operation.  This does not include any of the round-trip time
         from the scheduler to the merger, or any other merge
         operations.

      .. stat:: merger_files_changes_op_time
         :type: timer

         The amount of time the merger spent performing a files-changes
         operation to detect changed files (this is sometimes
         performed if the source does not provide this information).
         This does not include any of the round-trip time from the
         scheduler to the merger, or any other merge operations.

      .. stat:: layout_generation_time
         :type: timer

         The amount of time spent generating a dynamic configuration layout.

      .. stat:: job_freeze_time
         :type: timer

         The amount of time spent freezing the inheritance hierarchy
         and parameters of a job.

      .. stat:: repo_state_time
         :type: timer

         The amount of time waiting for a secondary Zuul merger
         operation to collect additional information about the repo
         state of required projects.  Includes
         :stat:`zuul.tenant.<tenant>.pipeline.<pipeline>.merger_repo_state_op_time`.

      .. stat:: merger_repo_state_op_time
         :type: timer

         The amount of time the merger spent performing a repo state
         operation to collect additional information about the repo
         state of required projects.  This does not include any of the
         round-trip time from the scheduler to the merger, or any
         other merge operations.

      .. stat:: node_request_time
         :type: timer

         The amount of time spent waiting for each node request to be
         fulfilled.

      .. stat:: job_wait_time
         :type: timer

         How long a job waited for an executor to start running it
         after the build was requested.

      .. stat:: event_job_time
         :type: timer

         The total amount of time elapsed from when a trigger event
         was received from the remote system until the item's first
         job is run.  This is only emitted once per queue item, even
         if its buildset is reset due to a speculative execution
         failure.

      .. stat:: all_jobs
         :type: counter

         Number of jobs triggered by the pipeline.

      .. stat:: current_changes
         :type: gauge

         The number of items currently being processed by this
         pipeline.

      .. stat:: window
         :type: gauge

         The configured window size for the pipeline.  Note that this
         will not change during operation.  This value is used to
         initialize each :term:`project queue`, and as changes in that
         queue succeed or fail, that queue's window will adjust.

      .. stat:: handling
         :type: timer

         The total time taken to refresh and process the pipeline.
         This is emitted every time a scheduler examines a pipeline
         regardless of whether it takes any actions.

      .. stat:: event_process
         :type: timer

         The time taken to process the event queues for the pipeline.
         This is emitted only if there are events to process.

      .. stat:: process
         :type: timer

         The time taken to process the pipeline.  This is emitted only
         if there were events to process.

      .. stat:: data_size_compressed
         :type: gauge

         The number of bytes stored in ZooKeeper to represent the
         serialized state of the pipeline.

      .. stat:: data_size_uncompressed
         :type: gauge

         The number of bytes required to represent the serialized
         state of the pipeline (the decompressed value of
         ``data_size_compressed``).

      .. stat:: queue

         This hierarchy holds more specific metrics for each
         :term:`project queue` in the pipeline.

         .. stat:: <queue>

            The name of the queue.  If the queue is automatically
            generated for a single project, the name of the project is
            used by default.  Embedded ``.`` characters will be
            translated to ``_``, and ``/`` to ``.``.

            If the queue is configured as per-branch, the metrics
            below are omitted and instead found under
            :stat:`zuul.tenant.<tenant>.pipeline.<pipeline>.queue.<queue>.branch`.

            .. stat:: current_changes
               :type: gauge

               The number of items currently in this queue.

            .. stat:: window
               :type: gauge

               The window size for the queue.  This will change as
               individual changes in the queue succeed or fail.

            .. stat:: resident_time
               :type: timer

               A timer metric reporting how long each item has been in
               the queue.

            .. stat:: total_changes
               :type: counter

               The number of changes processed by the queue.

            .. stat:: branch

               If the queue is configured as per-branch, this
               hierarchy will be present and will hold stats for each
               branch seen.

               .. stat:: <branch>

                  The name of the branch.  Embedded ``.`` characters
                  will be translated to ``_``, and ``/`` to ``.``.

                  Underneath this key are per-branch values of the
                  metrics above.

      .. stat:: project

         This hierarchy holds more specific metrics for each project
         participating in the pipeline.

         .. stat:: <canonical_hostname>

            The canonical hostname for the triggering project.
            Embedded ``.`` characters will be translated to ``_``.

            .. stat:: <project>

               The name of the triggering project.  Embedded ``/`` or
               ``.`` characters will be translated to ``_``.

               .. stat:: <branch>

                  The name of the triggering branch.  Embedded ``/`` or
                  ``.`` characters will be translated to ``_``.

                  .. stat:: job

                     Subtree detailing per-project job statistics:

                     .. stat:: <jobname>

                        The triggered job name.

                        .. stat:: <result>
                           :type: counter, timer

                           A counter for each type of result (e.g., ``SUCCESS`` or
                           ``FAILURE``, ``ERROR``, etc.) for the job.  If the
                           result is ``SUCCESS`` or ``FAILURE``, Zuul will
                           additionally report the duration of the build as a
                           timer.

                        .. stat:: wait_time
                           :type: timer

                           How long the job waited for an executor to
                           start running it after the build was requested.

                  .. stat:: current_changes
                     :type: gauge

                     The number of items of this project currently being
                     processed by this pipeline.

                  .. stat:: resident_time
                     :type: timer

                     A timer metric reporting how long each item for this
                     project has been in the pipeline.

                  .. stat:: total_changes
                     :type: counter

                     The number of changes for this project processed by the
                     pipeline.

      .. stat:: read_time
         :type: timer

         The time spent reading data from ZooKeeper during a single
         pipeline processing run.

      .. stat:: read_znodes
         :type: gauge

         The number of ZNodes read from ZooKeeper during a single
         pipeline processing run.

      .. stat:: read_objects
         :type: gauge

         The number of Zuul data model objects read from ZooKeeper
         during a single pipeline processing run.

      .. stat:: read_bytes
         :type: gauge

         The amount of data read from ZooKeeper during a single
         pipeline processing run.

      .. stat:: refresh
         :type: timer

         The time taken to refresh the state from ZooKeeper.

      .. stat:: resident_time
         :type: timer

         A timer metric reporting how long each item has been in the
         pipeline.

      .. stat:: total_changes
         :type: counter

         The number of changes processed by the pipeline.

      .. stat:: trigger_events
         :type: gauge

         The size of the pipeline's trigger event queue.

      .. stat:: result_events
         :type: gauge

         The size of the pipeline's result event queue.

      .. stat:: management_events
         :type: gauge

         The size of the pipeline's management event queue.

      .. stat:: write_time
         :type: timer

         The time spent writing data to ZooKeeper during a single
         pipeline processing run.

      .. stat:: write_znodes
         :type: gauge

         The number of ZNodes written to ZooKeeper during a single
         pipeline processing run.

      .. stat:: write_objects
         :type: gauge

         The number of Zuul data model objects written to ZooKeeper
         during a single pipeline processing run.

      .. stat:: write_bytes
         :type: gauge

         The amount of data written to ZooKeeper during a single
         pipeline processing run.

.. stat:: zuul.executor.<executor>

   Holds metrics emitted by individual executors.  The ``<executor>``
   component of the key will be replaced with the hostname of the
   executor.

   .. stat:: merger.<result>
      :type: counter

      Incremented to represent the status of a Zuul executor's merger
      operations. ``<result>`` can be either ``SUCCESS`` or ``FAILURE``.
      A failed merge operation which would be accounted for as a ``FAILURE``
      is what ends up being returned by Zuul as a ``MERGE_CONFLICT``.

   .. stat:: builds
      :type: counter

      Incremented each time the executor starts a build.

   .. stat:: starting_builds
      :type: gauge, timer

      The number of builds starting on this executor and a timer containing
      how long jobs were in this state. These are builds which have not yet
      begun their first pre-playbook.

      The timer needs special thoughts when interpreting it because it
      aggregates all jobs. It can be useful when aggregating it over a longer
      period of time (maybe a day) where fast rising graphs could indicate e.g.
      IO problems of the machines the executors are running on. But it has to
      be noted that a rising graph also can indicate a higher usage of complex
      jobs using more required projects. Also comparing several executors might
      give insight if the graphs differ a lot from each other. Typically the
      jobs are equally distributed over all executors (in the same zone when
      using the zone feature) and as such the starting jobs timers (aggregated
      over a large enough interval) should not differ much.

   .. stat:: running_builds
      :type: gauge

      The number of builds currently running on this executor.  This
      includes starting builds.

   .. stat:: paused_builds
      :type: gauge

      The number of currently paused builds on this executor.

   .. stat:: phase

      Subtree detailing per-phase execution statistics:

      .. stat:: <phase>

         ``<phase>`` represents a phase in the execution of a job.
         This can be an *internal* phase (such as ``setup`` or ``cleanup``) as
         well as *job* phases such as ``pre``, ``run`` or ``post``.

         .. stat:: <result>
            :type: counter

            A counter for each type of result.
            These results do not, by themselves, determine the status of a build
            but are indicators of the exit status provided by Ansible for the
            execution of a particular phase.

            Example of possible counters for each phase are: ``RESULT_NORMAL``,
            ``RESULT_TIMED_OUT``, ``RESULT_UNREACHABLE``, ``RESULT_ABORTED``.

   .. stat:: load_average
      :type: gauge

      The one-minute load average of this executor, multiplied by 100.

   .. stat:: pause
      :type: gauge

      Indicates if the executor is paused. 1 means paused else 0.

   .. stat:: pct_used_hdd
      :type: gauge

      The used disk on this executor, as a percentage multiplied by 100.

   .. stat:: pct_used_inodes
      :type: gauge

      The used inodes on this executor, as a percentage multiplied by 100.

   .. stat:: pct_used_ram
      :type: gauge

      The used RAM (excluding buffers and cache) on this executor, as
      a percentage multiplied by 100.

  .. stat:: pct_used_ram_cgroup
     :type: gauge

     The used RAM (excluding buffers and cache) on this executor allowed by
     the cgroup, as percentage multiplied by 100.

  .. stat:: max_process
     :type: gauge

     The maximum amount of processes that can be running on this executor.

  .. stat:: cur_process
     :type: gauge

     The current amount of running processes on this executor.

.. stat:: zuul.nodepool.requests

   Holds metrics related to Zuul requests and responses from Nodepool.

   States are one of:

      *requested*
        Node request submitted by Zuul to Nodepool
      *canceled*
        Node request was canceled by Zuul
      *failed*
        Nodepool failed to fulfill a node request
      *fulfilled*
        Nodes were assigned by Nodepool

   .. stat:: <state>
      :type: timer

      Records the elapsed time from request to completion for states
      `failed` and `fulfilled`.  For example,
      ``zuul.nodepool.request.fulfilled.mean`` will give the average
      time for all fulfilled requests within each ``statsd`` flush
      interval.

      A lower value for `fulfilled` requests is better.  Ideally,
      there will be no `failed` requests.

   .. stat:: <state>.total
      :type: counter

      Incremented when nodes are assigned or removed as described in
      the states above.

   .. stat:: <state>.size.<size>
      :type: counter, timer

      Increments for the node count of each request.  For example, a
      request for 3 nodes would use the key
      ``zuul.nodepool.requests.requested.size.3``; fulfillment of 3
      node requests can be tracked with
      ``zuul.nodepool.requests.fulfilled.size.3``.

      The timer is implemented for ``fulfilled`` and ``failed``
      requests.  For example, the timer
      ``zuul.nodepool.requests.failed.size.3.mean`` gives the average
      time of 3-node failed requests within the ``statsd`` flush
      interval.  A lower value for `fulfilled` requests is better.
      Ideally, there will be no `failed` requests.

   .. stat:: <state>.label.<label>
      :type: counter, timer

      Increments for the label of each request.  For example, requests
      for `centos7` nodes could be tracked with
      ``zuul.nodepool.requests.requested.centos7``.

      The timer is implemented for ``fulfilled`` and ``failed``
      requests.  For example, the timer
      ``zuul.nodepool.requests.fulfilled.label.centos7.mean`` gives
      the average time of ``centos7`` fulfilled requests within the
      ``statsd`` flush interval.  A lower value for `fulfilled`
      requests is better.  Ideally, there will be no `failed`
      requests.

.. stat:: zuul.nodepool

   .. stat:: current_requests
      :type: gauge

      The number of outstanding nodepool requests from Zuul.  Ideally
      this will be at zero, meaning all requests are fulfilled.
      Persistently high values indicate more testing node resources
      would be helpful.

   .. stat:: tenant.<tenant>.current_requests
      :type: gauge

      The number of outstanding nodepool requests from Zuul drilled down by
      <tenant>. If a tenant for a node request cannot be determined, it is
      reported as ``unknown``. This relates to
      ``zuul.nodepool.current_requests``.

.. stat:: zuul.nodepool.resources

   Holds metrics about resource usage by tenant or project if resources
   of nodes are reported by nodepool.

   .. stat:: in_use

      Holds metrics about resources currently in use by a build.

      .. stat:: tenant

         Holds resource usage metrics by tenant.

         .. stat:: <tenant>.<resource>
            :type: counter, gauge

            Counter with the summed usage by tenant as <resource> seconds and
            gauge with the currently in use resources by tenant.

      .. stat:: project

         Holds resource usage metrics by project.

         .. stat:: <project>.<resource>
            :type: counter, gauge

            Counter with the summed usage by project as <resource> seconds and
            gauge with the currently used resources by project.

   .. stat:: total

      Holds metrics about resources allocated in total. This includes
      resources that are currently in use, allocated but not yet in use, and
      scheduled to be deleted.

      .. stat:: tenant

         Holds resource usage metrics by tenant.

         .. stat:: <tenant>.<resource>
            :type: gauge

            Gauge with the currently used resources by tenant.


.. stat:: zuul.mergers

   Holds metrics related to Zuul mergers.

   .. stat:: online
      :type: gauge

      The number of Zuul merger processes online.

   .. stat:: jobs_running
      :type: gauge

      The number of merge jobs running.

   .. stat:: jobs_queued
      :type: gauge

      The number of merge jobs waiting for a merger.  This should
      ideally be zero; persistent higher values indicate more merger
      resources would be useful.

.. stat:: zuul.executors

   Holds metrics related to unzoned executors.

   This is a copy of :stat:`zuul.executors.unzoned`.  It does not
   include information about zoned executors.

   .. warning:: The metrics at this location are deprecated and will
                be removed in a future version.  Please begin using
                :stat:`zuul.executors.unzoned` instead.

   .. stat:: online
      :type: gauge

      The number of Zuul executor processes online.

   .. stat:: accepting
      :type: gauge

      The number of Zuul executor processes accepting new jobs.

   .. stat:: jobs_running
      :type: gauge

      The number of executor jobs running.

   .. stat:: jobs_queued
      :type: gauge

      The number of jobs allocated nodes, but queued waiting for an
      executor to run on.  This should ideally be at zero; persistent
      higher values indicate more executor resources would be useful.

   .. stat:: unzoned

      Holds metrics related to unzoned executors.

      .. stat:: online
         :type: gauge

         The number of unzoned Zuul executor processes online.

      .. stat:: accepting
         :type: gauge

         The number of unzoned Zuul executor processes accepting new
         jobs.

      .. stat:: jobs_running
         :type: gauge

         The number of unzoned executor jobs running.

      .. stat:: jobs_queued
         :type: gauge

         The number of jobs allocated nodes, but queued waiting for an
         unzoned executor to run on.  This should ideally be at zero;
         persistent higher values indicate more executor resources
         would be useful.

   .. stat:: zone

      Holds metrics related to zoned executors.

      .. stat:: <zone>.online
         :type: gauge

         The number of Zuul executor processes online in this zone.

      .. stat:: <zone>.accepting
         :type: gauge

         The number of Zuul executor processes accepting new jobs in
         this zone.

      .. stat:: <zone>.jobs_running
         :type: gauge

         The number of executor jobs running in this zone.

      .. stat:: <zone>.jobs_queued
         :type: gauge

         The number of jobs allocated nodes, but queued waiting for an
         executor in this zone to run on.  This should ideally be at
         zero; persistent higher values indicate more executor
         resources would be useful.

.. stat:: zuul.scheduler

   Holds metrics related to the Zuul scheduler.

   .. stat:: eventqueues

      Holds metrics about the event queue lengths in the Zuul scheduler.

      .. stat:: management
         :type: gauge

         The size of the current reconfiguration event queue.

      .. stat:: connection.<connection-name>
         :type: gauge

         The size of the current connection event queue.

   .. stat:: run_handler
      :type: timer

      A timer metric reporting the time taken for one scheduler run
      handler iteration.

   .. stat:: time_query
      :type: timer

      Each time the scheduler performs a query against the SQL
      database in order to determine an estimated time for a job, it
      emits this timer of the duration of the query.  Note this is a
      performance metric of how long the SQL query takes; it is not
      the estimated time value itself.

As an example, given a job named `myjob` in `mytenant` triggered by a
change to `myproject` on the `master` branch in the `gate` pipeline
which took 40 seconds to build, the Zuul scheduler will emit the
following statsd events:

  * ``zuul.tenant.mytenant.pipeline.gate.project.example_com.myproject.master.job.myjob.SUCCESS`` +1
  * ``zuul.tenant.mytenant.pipeline.gate.project.example_com.myproject.master.job.myjob.SUCCESS``  40 seconds
  * ``zuul.tenant.mytenant.pipeline.gate.all_jobs`` +1

.. stat:: zuul.web

   Holds metrics related to the Zuul web component.

   .. stat:: server.<hostname>

      Holds metrics from a specific zuul-web server.

      .. stat:: threadpool

         Metrics related to the web server thread pool.

         .. stat:: idle
            :type: gauge

            The number of idle workers.

         .. stat:: queue
            :type: gauge

            The number of requests queued for workers.

      .. stat:: streamers
         :type: gauge

         The number of log streamers currently in operation.

.. stat:: zuul.image

   Holds metrics related to Zuul images.

   .. stat:: <image>

      The canonical name of the image.

      .. stat:: upload

         Holds metrics related to image uploads.

         .. stat:: <endpoint>

            The canonical name of the upload endpoint.

            .. stat:: duration
               :type: timer

               The duration of the upload operation.

            .. stat:: state

               Holds metrics for each of the following states:

               * ready
               * deleting
               * pending
               * uploading
               * failed

               .. stat:: <state>
                  :type: gauge

                  The number of uploads for this image and endpoint
                  currently in this state.

.. stat:: zuul.uploads

   Holds metrics related to Zuul image upload operations.

   .. stat:: state

      Holds metrics for each of the following states:

      * ready
      * deleting
      * pending
      * uploading
      * failed

      .. stat:: <state>
         :type: gauge

         The number of uploads currently in this state.

.. stat:: zuul.provider

   Holds metrics related to individual providers.

   .. stat:: <provider>

      The canonical name of the provider.

      .. stat:: limit

         Quota limits for the named resource.  The actual resource
         names are driver-dependent.

         .. stat:: <resource>
            :type: gauge

            The limit for this resource in this provider.

      .. stat:: usage

         Quota usage for the named resource.  The actual resource
         names are driver-dependent.

         .. stat:: <resource>
            :type: gauge

            The current usage of this resource in this provider.

      .. stat:: nodes

         Holds metrics related to nodes in this provider.

         .. stat:: state

            Holds metrics for each of the following node states:

            * requested
            * building
            * ready
            * failed
            * tempfailed
            * in-use
            * used
            * outdated
            * deregister
            * hold
            * snapshot
            * slot-host

            .. stat:: <state>
               :type: gauge

               The number of nodes in this provider in this state.

      .. stat:: label

         Holds metrics related to specific labels.

         .. stat:: <label>

            The canonical name of the label.

            .. stat:: nodes

               Holds metrics related to label nodes in this provider.

               .. stat:: state

                  Holds metrics for each of the following node states:

                  * requested
                  * building
                  * ready
                  * failed
                  * tempfailed
                  * in-use
                  * used
                  * outdated
                  * deregister
                  * hold
                  * snapshot
                  * slot-host

                  .. stat:: <state>
                     :type: gauge

                     The number of nodes of this label in this
                     provider in this state.

.. stat:: zuul.provider_endpoint

   Holds metrics related to individual provider endpoints.

   .. stat:: <endpoint>

      The canonical name of the provider endpoint.

      .. stat:: leaked.<resource>
         :type: counter

         The number of leaked resources of a certain type in this
         endpoint.  The specific resources are driver-dependent.

.. stat:: zuul.nodes

   Holds metrics related to nodes.

   .. stat:: state

      Holds metrics for each of the following node states:

      * requested
      * building
      * ready
      * failed
      * tempfailed
      * in-use
      * used
      * outdated
      * deregister
      * hold
      * snapshot
      * slot-host

      .. stat:: <state>
         :type: gauge

         The number of nodes across all providers in this state.

.. stat:: zuul.nodeset_requests

   Holds metrics related to nodeset requests.  These are requests that
   are handled by zuul-launcher.

   .. stat:: state

      Holds metrics for each of the following states:

      * requested
      * accepted
      * fulfilled
      * failed

      .. stat:: <state>
         :type: gauge

         The number of requests in this state.

.. stat:: zuul.launcher

   Holds metrics related to the Zuul launcher component.

   .. stat:: server.<hostname>

      Holds metrics from a specific zuul-launcher server.

      .. stat:: upload_workers
         :type: gauge

         The number of worker threads currently processing an image upload.

      .. stat:: endpoint_upload_workers
         :type: gauge

         The number of worker threads currently uploading an image to
         a provider endpoint.

      .. stat:: endpoint_import_workers
         :type: gauge

         The number of worker threads currently importing an image to
         a provider endpoint.


Prometheus monitoring
---------------------

Zuul comes with support to start a prometheus_ metric server to be added as
prometheus's target.

.. _prometheus: https://prometheus.io/docs/introduction/overview/


Configuration
~~~~~~~~~~~~~

To enable the service, set the ``prometheus_port`` in a service section of
``zuul.conf``. For example setting :attr:`scheduler.prometheus_port` to 9091
starts a HTTP server to expose metrics to a prometheus services at:
http://scheduler:9091/metrics


Metrics
~~~~~~~

These metrics are exposed by default:

.. stat:: process_virtual_memory_bytes
   :type: gauge

.. stat:: process_resident_memory_bytes
   :type: gauge

.. stat:: process_open_fds
   :type: gauge

.. stat:: process_start_time_seconds
   :type: gauge

.. stat:: process_cpu_seconds_total
   :type: counter

On web servers the following additional metrics are exposed:

.. stat:: web_threadpool_idle
   :type: gauge

   The number of idle workers in the thread pool.

.. stat:: web_threadpool_queue
   :type: gauge

   The number of requests queued for thread pool workers.

.. stat:: web_streamers
   :type: gauge

   The number of log streamers currently in operation.

.. _prometheus_liveness:

Liveness Probes
~~~~~~~~~~~~~~~

The Prometheus server also supports liveness and ready probes at the
following URIS:

.. path:: health/live

   Returns 200 as long as the process is running.

.. path:: health/ready

   Returns 200 if the process is in `RUNNING` or `PAUSED` states.
   Otherwise, returns 503.  Note that 503 is returned for
   `INITIALIZED`, so this may be used to determine when a component
   has completely finished loading configuration.

.. path:: health/status

   This always returns 200, but includes the component status as the
   text body of the response.
