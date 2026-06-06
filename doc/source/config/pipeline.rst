.. _pipeline:

Pipeline
========

A pipeline describes a workflow operation in Zuul.  It associates jobs
for a given project with triggering and reporting events.

Its flexible configuration allows for characterizing any number of
workflows, and by specifying each as a named configuration, makes it
easy to apply similar workflow operations to projects or groups of
projects.

By way of example, one of the primary uses of Zuul is to perform
project gating.  To do so, one can create a :term:`gate` pipeline
which tells Zuul that when a certain event (such as approval by a code
reviewer) occurs, the corresponding change or pull request should be
enqueued into the pipeline.  When that happens, the jobs which have
been configured to run for that project in the gate pipeline are run,
and when they complete, the pipeline reports the results to the user.

Pipeline configuration items may only appear in :term:`config-projects
<config-project>`.

Generally, a Zuul administrator would define a small number of
pipelines which represent the workflow processes used in their
environment.  Each project can then be added to the available
pipelines as appropriate.

Here is an example :term:`check` pipeline, which runs whenever a new
patchset is created in Gerrit.  If the associated jobs all report
success, the pipeline reports back to Gerrit with ``Verified`` vote of
+1, or if at least one of them fails, a -1:

.. code-block:: yaml

   - pipeline:
       name: check
       manager: independent
       trigger:
         my_gerrit:
           - event: patchset-created
       success:
         my_gerrit:
           Verified: 1
       failure:
         my_gerrit:
           Verified: -1

.. TODO: See TODO for more annotated examples of common pipeline configurations.

.. attr:: pipeline

   The attributes available on a pipeline are as follows (all are
   optional unless otherwise specified):

   .. attr:: name
      :required:

      This is used later in the project definition to indicate what jobs
      should be run for events in the pipeline.

   .. attr:: manager
      :required:

      There are several schemes for managing pipelines.  The following
      table summarizes their features; each is described in detail
      below.

      ===========  =============================  ============  =====  =============  =========
      Manager      Use Case                       Dependencies  Merge  Shared Queues  Window
      ===========  =============================  ============  =====  =============  =========
      Independent  :term:`check`, :term:`post`    No            No     No             Unlimited
      Dependent    :term:`gate`                   Yes           Yes    Yes            Variable
      Serial       :term:`deploy`                 No            No     Yes            1
      Supercedent  :term:`post`, :term:`promote`  No            No     Project-ref    1
      ===========  =============================  ============  =====  =============  =========

      .. value:: independent

         Every event in this pipeline should be treated as independent
         of other events in the pipeline.  This is appropriate when
         the order of events in the pipeline doesn't matter because
         the results of the actions this pipeline performs can not
         affect other events in the pipeline.  For example, when a
         change is first uploaded for review, you may want to run
         tests on that change to provide early feedback to reviewers.
         At the end of the tests, the change is not going to be
         merged, so it is safe to run these tests in parallel without
         regard to any other changes in the pipeline.  They are
         independent.

         Another type of pipeline that is independent is a post-merge
         pipeline. In that case, the changes have already merged, so
         the results can not affect any other events in the pipeline.

      .. value:: dependent

         The dependent pipeline manager is designed for gating.  It
         ensures that every change is tested exactly as it is going to
         be merged into the repository.  An ideal gating system would
         test one change at a time, applied to the tip of the
         repository, and only if that change passed tests would it be
         merged.  Then the next change in line would be tested the
         same way.  In order to achieve parallel testing of changes,
         the dependent pipeline manager performs speculative execution
         on changes.  It orders changes based on their entry into the
         pipeline.  It begins testing all changes in parallel,
         assuming that each change ahead in the pipeline will pass its
         tests.  If they all succeed, all the changes can be tested
         and merged in parallel.  If a change near the front of the
         pipeline fails its tests, each change behind it ignores
         whatever tests have been completed and are tested again
         without the change in front.  This way gate tests may run in
         parallel but still be tested correctly, exactly as they will
         appear in the repository when merged.

         For more detail on the theory and operation of Zuul's
         dependent pipeline manager, see: :doc:`/gating`.

      .. value:: serial

         This pipeline manager supports shared queues (like dependent
         pipelines) but only one item in each shared queue is
         processed at a time.

         This may be useful for post-merge pipelines which perform
         partial production deployments (i.e., there are jobs with
         file matchers which only deploy to affected parts of the
         system).  In such a case it is important for every change to
         be processed, but they must still be processed one at a time
         in order to ensure that the production system is not
         inadvertently regressed.  Support for shared queues ensures
         that if multiple projects are involved deployment runs still
         execute sequentially.

      .. value:: supercedent

         This is like an independent pipeline, in that every item is
         distinct, except that items are grouped by project and ref,
         and only one item for each project-ref is processed at a
         time.  If more than one additional item is enqueued for the
         project-ref, previously enqueued items which have not started
         processing are removed.

         In other words, this pipeline manager will only run jobs for
         the most recent item enqueued for a given project-ref.

         This may be useful for post-merge pipelines which perform
         artifact builds where only the latest version is of use.  In
         these cases, build resources can be conserved by avoiding
         building intermediate versions.

         .. note:: Since this pipeline manager filters intermediate
                   buildsets using it in combination with file filters
                   on jobs is dangerous.  In this case jobs of in
                   between buildsets can be unexpectedly skipped
                   entirely. If file filters are needed the
                   ``independent`` or ``serial`` pipeline managers
                   should be used.

         .. note:: Since this pipeline manager creates a virtual queue
                   for each project-ref, it ignores manually
                   configured shared queues as well as circular
                   dependencies.

   .. attr:: post-review
      :default: false

      This is a boolean which indicates that this pipeline executes
      code that has been reviewed.  Some jobs perform actions which
      should not be permitted with unreviewed code.  When this value
      is ``false`` those jobs will not be permitted to run in the
      pipeline.  If a pipeline is designed only to be used after
      changes are reviewed or merged, set this value to ``true`` to
      permit such jobs.

      For more information, see :ref:`secret` and
      :attr:`job.post-review`.

   .. attr:: description

      This field may be used to provide a textual description of the
      pipeline.  It may appear in the status page or in documentation.

   .. attr:: variant-description
      :default: branch name

      This field may be used to provide a textual description of the
      variant. It may appear in the status page or in documentation.

   .. attr:: success-message
      :default: Build successful.

      The introductory text in reports when all the voting jobs are
      successful.

   .. attr:: failure-message
      :default: Build failed.

      The introductory text in reports when at least one voting job
      fails.

   .. attr:: start-message
      :default: Starting {pipeline.name} jobs.

      The introductory text in reports when jobs are started.
      The following replacement fields are available:

      pipeline
        The name of the pipeline
      item_url
        The full URL to information about the item in the web UI

   .. attr:: enqueue-message

      The introductory text in reports when an item is enqueued.
      Empty by default.
      The following replacement fields are available:

      pipeline
        The name of the pipeline
      item_url
        The full URL to information about the item in the web UI

   .. attr:: merge-conflict-message
      :default: Merge failed.

      The introductory text in the message reported when a change
      fails to merge with the current state of the repository.
      Defaults to "Merge failed."

   .. attr:: no-jobs-message

      The introductory text in reports when an item is dequeued
      without running any jobs.  Empty by default.
      The following replacement fields are available:

      pipeline
        The name of the pipeline
      item_url
        The full URL to information about the item in the web UI

   .. attr:: dequeue-message
      :default: Build canceled.

      The introductory text in reports when an item is dequeued.
      The dequeue message only applies if the item was dequeued without
      a result.

   .. attr:: footer-message

      Supplies additional information after test results.  Useful for
      adding information about the CI system such as debugging and
      contact details.

   .. attr:: trigger

      At least one trigger source must be supplied for each pipeline.
      Triggers are not exclusive -- matching events may be placed in
      multiple pipelines, and they will behave independently in each
      of the pipelines they match.

      Triggers are loaded from their connection name. The driver type
      of the connection will dictate which options are available.  See
      :ref:`drivers`.

   .. attr:: require

      If this section is present, it establishes prerequisites for
      any kind of item entering the Pipeline.  Regardless of how the
      item is to be enqueued (via any trigger or automatic dependency
      resolution), the conditions specified here must be met or the
      item will not be enqueued.  These requirements may vary
      depending on the source of the item being enqueued.

      Requirements are loaded from their connection name. The driver
      type of the connection will dictate which options are available.
      See :ref:`drivers`.

   .. attr:: reject

      If this section is present, it establishes prerequisites that
      can block an item from being enqueued. It can be considered a
      negative version of :attr:`pipeline.require`.

      Requirements are loaded from their connection name. The driver
      type of the connection will dictate which options are available.
      See :ref:`drivers`.

   .. attr:: allow-other-connections
      :default: true

      If this is set to `false` then any change enqueued into the
      pipeline (whether it is enqueued to run jobs or merely as a
      dependency) must be from one of the connections specified in the
      pipeline configuration (this includes any trigger, reporter, or
      source requirement).  When used in conjunction with
      :attr:`pipeline.require`, this can ensure that pipeline
      requirements are exhaustive.

   .. attr:: allow-parameters
      :default: false

      If this is set to `true` then projects will be permitted to
      specify parameters via :attr:`project.<pipeline>.parameters`
      that should be supplied when enqueing changes to this pipeline.
      The only way to specify parameters currently is via the web
      interface or API, so this setting is generally only useful for
      pipelines that are triggered manually.

   .. attr:: supercedes

      The name of a pipeline, or a list of names, that this pipeline
      supercedes.  When a change is enqueued in this pipeline, it will
      be removed from the pipelines listed here.  For example, a
      :term:`gate` pipeline may supercede a :term:`check` pipeline so
      that test resources are not spent running near-duplicate jobs
      simultaneously.

   .. attr:: dequeue-on-new-patchset
      :default: true

      Normally, if a new patchset is uploaded to a change that is in a
      pipeline, the existing entry in the pipeline will be removed
      (with jobs canceled and any dependent changes that can no longer
      merge as well.  To suppress this behavior (and allow jobs to
      continue running), set this to ``false``.

   .. attr:: ignore-dependencies
      :default: false

      In any kind of pipeline (dependent or independent), Zuul will
      attempt to enqueue all dependencies ahead of the current change
      so that they are tested together (independent pipelines report
      the results of each change regardless of the results of changes
      ahead).  To ignore dependencies completely in an independent
      pipeline, set this to ``true``.  This option is ignored by
      dependent pipelines.

   .. attr:: precedence
      :default: normal

      Indicates how the build scheduler should prioritize jobs for
      different pipelines.  Each pipeline may have one precedence,
      jobs for pipelines with a higher precedence will be run before
      ones with lower.  The value should be one of ``high``,
      ``normal``, or ``low``.  Default: ``normal``.

   .. _reporters:

   The following options configure :term:`reporters <reporter>`.
   Reporters are complementary to triggers; where a trigger is an
   event on a connection which causes Zuul to enqueue an item, a
   reporter is the action performed on a connection when an item is
   dequeued after its jobs complete.  The actual syntax for a reporter
   is defined by the driver which implements it.  See :ref:`drivers`
   for more information.

   .. attr:: success

      Describes where Zuul should report to if all the jobs complete
      successfully.  This section is optional; if it is omitted, Zuul
      will run jobs and do nothing on success -- it will not report at
      all.  If the section is present, the listed :term:`reporters
      <reporter>` will be asked to report on the jobs.  The reporters
      are listed by their connection name. The options available
      depend on the driver for the supplied connection.

   .. attr:: failure

      These reporters describe what Zuul should do if at least one job
      fails.

   .. attr:: merge-conflict

      These reporters describe what Zuul should do if it is unable to
      merge the patchset into the current state of the target
      branch. If no merge-conflict reporters are listed then the
      ``failure`` reporters will be used.

   .. attr:: config-error

      These reporters describe what Zuul should do if it encounters a
      configuration error while trying to enqueue the item.  If no
      config-error reporters are listed then the ``failure`` reporters
      will be used.

   .. attr:: enqueue

      These reporters describe what Zuul should do when an item is
      enqueued into the pipeline.  This may be used to indicate to a
      system or user that Zuul is aware of the triggering event even
      though it has not evaluated whether any jobs will run.

   .. attr:: start

      These reporters describe what Zuul should do when jobs start
      running for an item in the pipeline.  This can be used, for
      example, to reset a previously reported result.

   .. attr:: no-jobs

      These reporters describe what Zuul should do when an item is
      dequeued from a pipeline without running any jobs.  This may be
      used to indicate to a system or user that the pipeline is not
      relevant for a change.

   .. attr:: disabled

      These reporters describe what Zuul should do when a pipeline is
      disabled.  See ``disable-after-consecutive-failures``.

   .. attr:: dequeue

      These reporters describe what Zuul should do if an item is
      dequeued. The dequeue reporters will only apply if all of the
      following are true:

         * The pipeline has a start reporter
         * The item has reported start
         * The item was dequeued without a result

   The following options can be used to alter Zuul's behavior to
   mitigate situations in which jobs are failing frequently (perhaps
   due to a problem with an external dependency, or unusually high
   non-deterministic test failures).

   .. attr:: disable-after-consecutive-failures

      If set, a pipeline can enter a *disabled* state if too many
      changes in a row fail. When this value is exceeded the pipeline
      will stop reporting to any of the **success**, **failure** or
      **merge-conflict** reporters and instead only report to the
      **disabled** reporters.  (No **start** reports are made when a
      pipeline is disabled).

   .. attr:: window
      :default: 20

      Dependent pipeline managers only. Zuul can rate limit dependent
      pipelines in a manner similar to TCP flow control.  See
      :ref:`pipeline_window` for an overview.  Jobs are only started
      for items in the queue if they are within the active window for
      the pipeline. The initial length of this window is configurable
      with this value. The value given should be a positive integer
      value. A value of ``0`` disables rate limiting on the
      :value:`dependent pipeline manager
      <pipeline.manager.dependent>`.

   .. attr:: window-floor
      :default: 3

      Dependent pipeline managers only. This is the minimum value for
      the window described above. Should be a positive non zero
      integer value.

   .. attr:: window-ceiling

      Dependent pipeline managers only. This is the maximum value for
      the window described above.  When omitted or set to ``null``,
      there is no upper limit on the size of the window.  If set, must
      be greater than or equal to the floor.

   .. attr:: window-increase-type
      :default: linear

      Dependent pipeline managers only. This value describes how the
      window should grow when changes are successfully merged by zuul.

      .. value:: linear

         Indicates that **window-increase-factor** should be added to
         the previous window value.

      .. value:: exponential

         Indicates that **window-increase-factor** should be
         multiplied against the previous window value and the result
         will become the window size.

   .. attr:: window-increase-factor
      :default: 1

      Dependent pipeline managers only. The value to be added or
      multiplied against the previous window value to determine the
      new window after successful change merges.

   .. attr:: window-decrease-type
      :default: exponential

      Dependent pipeline managers only. This value describes how the
      window should shrink when changes are not able to be merged by
      Zuul.

      .. value:: linear

         Indicates that **window-decrease-factor** should be
         subtracted from the previous window value.

      .. value:: exponential

         Indicates that **window-decrease-factor** should be divided
         against the previous window value and the result will become
         the window size.

   .. attr:: window-decrease-factor
      :default: 2

      :value:`Dependent pipeline managers
      <pipeline.manager.dependent>` only. The value to be subtracted
      or divided against the previous window value to determine the
      new window after unsuccessful change merges.
