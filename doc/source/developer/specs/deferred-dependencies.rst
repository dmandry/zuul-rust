Deferred Job Dependencies
=========================

.. warning:: This is not authoritative documentation.  These features
   are not currently available in Zuul.  They may change significantly
   before final implementation, or may never be fully completed.

The following specification describes a proposed extension to Zuul's
job dependency model that allows a dependent job to start immediately
while deferring the actual dependency check to a point during
execution chosen by the job author.

Introduction
------------

Zuul already has features to manage job dependencies, such as
initializer jobs which automatically become a dependency of every
other job in the pipeline.  However, all dependencies today are
checked at start time: a job will not start until all of its
dependencies have completed.  This is fine when the job needs the
dependency output right away, but wasteful when it has a long setup
phase first.

Example: A ``build-artifact`` job takes 25 minutes, and a ``run-tests``
job needs that artifact but spends 40 minutes on setup before it actually
uses it.  Today these run one after the other (~65 min), even though
setup + build could run in parallel (~40 min).

This proposes a ``deferred`` attribute on job dependencies and a
``zuul_wait`` Ansible action plugin. This would allow a job to start
right away and wait for the dependency only when it actually needs the
output/results.

Proposal
--------

Two new user-facing constructs are proposed:

* A ``deferred`` attribute on the job dependency configuration.
* A ``zuul_wait`` Ansible action plugin.

A dependency marked ``deferred: true`` is excluded from the start-time
dependency check.  The dependent job is launched immediately, regardless
of the deferred dependency's state.  At a point chosen by the job author,
the ``zuul_wait`` task blocks execution until the named dependencies have
completed (or paused), then injects their result data for subsequent tasks.

Configuration example:

.. code-block:: yaml

   - job:
       name: build-artifact
       # ... builds and publishes an artifact ...

   - job:
       name: run-tests
       dependencies:
         - name: build-artifact
           deferred: true

   - project:
       check:
         jobs:
           - build-artifact
           - run-tests

And the corresponding playbook for ``run-tests``:

.. code-block:: yaml

   - hosts: all
     tasks:
       - name: Set up test environment
         include_role:
           name: setup-test-environment
         # This takes ~40 minutes and does not need build-artifact output.

       - name: Wait for build artifact
         zuul_wait:
           dependencies:
             - build-artifact

       - name: Deploy and run tests
         include_role:
           name: deploy-and-test
         # Result data from build-artifact is now available.

The ``zuul_wait`` task accepts a ``dependencies`` parameter, which is
a list of job names to wait for.  Each name must correspond to a
dependency declared with ``deferred: true`` in the job configuration.

When ``zuul_wait`` completes successfully, all result data returned by
the deferred dependencies via ``zuul_return`` is made available as
Ansible variables to the remaining tasks, just like for a normal
dependency.

Implementation
--------------

The implementation mirrors the existing pause/resume infrastructure
and introduces a new wait/resume cycle between the executor and the
scheduler.

Dependency Model
~~~~~~~~~~~~~~~~

``JobDependency`` gains a ``deferred`` boolean attribute which
defaults to ``false``.  The config loader schema is extended to
accept it.

Job Scheduling
~~~~~~~~~~~~~~

The dependency-gating logic in ``QueueItem.findJobsToRun`` and
``QueueItem.findJobsToRequest`` is modified so that dependencies
marked ``deferred: true`` are excluded from the "all parent jobs
successful" check.  This allows the dependent job to be requested and
started immediately.

Wait/Resume Protocol
~~~~~~~~~~~~~~~~~~~~

This proposal uses patterns already established in Zuul: files in
the job directory (``$ZUUL_JOBDIR/work/``) for communication between
action plugins and the executor, and ZooKeeper-based signaling
between the executor and the scheduler:

1. The ``zuul_wait`` Ansible action plugin writes a wait request file
   (``$ZUUL_JOBDIR/work/wait_request.json``) containing the list of
   dependency names being waited on, then polls for a ready file
   (``$ZUUL_JOBDIR/work/wait_ready.json``).

2. A watcher thread in the executor's ``AnsibleJob`` detects the wait
   request file and sends a ``BuildWaitingEvent`` to the scheduler,
   similar to the ``BuildPausedEvent`` sent when a job pauses.

3. The scheduler's pipeline manager receives the ``BuildWaitingEvent``,
   marks the build as waiting, and checks whether the deferred
   dependencies have already completed.  If so, it immediately sends
   the resume signal with result data.  If not, it records the wait
   and checks again whenever a build completes or pauses.

4. When the dependency completes or pauses with data, the manager checks
   all waiting builds, and for each one whose dependencies are now
   satisfied, sends a wait-resume signal through ZooKeeper carrying the
   result data.

5. The executor receives the wait-resume signal and writes the ready
   file containing result data.  The ``zuul_wait`` action plugin
   reads the file and returns the data as Ansible facts, unblocking
   the playbook.

New Event Types and ZK Plumbing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following are added, following the pattern of the existing
pause/resume infrastructure:

* ``BuildWaitingEvent`` -- a new ``BuildResultEvent`` subclass, sent
  from the executor to the scheduler when a job reaches a ``zuul_wait``
  task.

* ``Build.waiting`` -- a boolean field on the Build model indicating
  the build is blocked on a deferred dependency.

* ``requestWaitResume()`` -- a new method on the job request queue,
  following the pattern of ``requestResume()``, that creates a ZK
  child node carrying result data to unblock the waiting build.

* ``waitResumeBuild()`` -- a new method on the executor client, like
  ``resumeBuild()``, used by the manager to send the resume signal.

Limitations
-----------

Timeout Handling
~~~~~~~~~~~~~~~~

Time spent waiting in ``zuul_wait`` counts against the job's overall
timeout, consistent with the existing behavior for paused jobs.  This
ensures that a waiting job cannot hold a node indefinitely if its
dependency stalls.

Failure Propagation
~~~~~~~~~~~~~~~~~~~

If a waited-on dependency fails or is canceled, the scheduler tells
the waiting build about it.  The ``zuul_wait`` task then fails, which
makes the waiting job fail too.

If the dependency has ``soft: true`` in addition to ``deferred: true``
and is skipped, the ``zuul_wait`` task returns successfully but with
no result data, consistent with how soft dependencies behave at
start time.

Cancellation
~~~~~~~~~~~~

If the buildset is canceled while a job is waiting in ``zuul_wait``,
the executor writes a failure signal to the ready file, causing the
action plugin to exit with failure.  The normal build cancellation
machinery then terminates the Ansible process.

Alternatives
------------

* **Split the job** into separate setup and test jobs.  This works
  today but adds complexity -- state must be shared between jobs and
  the pipeline is hard to maintain.

* **Poll from Ansible** (hit the Zuul API to check if the dependency
  is done).  This has no scheduler integration, no result data
  injection, and no automatic failure handling.

Work Items
----------

* Add ``deferred`` attribute to ``JobDependency`` model and config loader
* Skip deferred dependencies in ``findJobsToRun`` and ``findJobsToRequest``
* Add ``BuildWaitingEvent`` and wait-resume ZK plumbing
* Create ``zuul_wait`` Ansible action plugin
* Add executor-side wait request file watcher and resume handler
* Add scheduler event handling to unblock waiting builds
* Documentation and release note
