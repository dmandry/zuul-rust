Parameterized Builds
====================

.. warning:: This is not authoritative documentation.  These features
   are not currently available in Zuul.  They may change significantly
   before final implementation, or may never be fully completed.

The following document describes a proposed change to allow users to
trigger builds via the API or the web UI while supplying parameters.

Introduction
------------

Some CI systems with less of a focus on project gating offer the
ability to specify parameters when manually triggering jobs.  This
allows users to do things like create customized build artifacts with
specific components, deploy systems with specific versions, or create
releases with specific version identifiers.

Due to Zuul's focus on project gating, we have not supported a similar
feature for a number of reasons:

* Zuul is designed to be entirely git-driven, meaning that all of the
  inputs to a build should come from a git repository.  This
  facilitates good process review practices, repeatability, and
  auditability.

* Zuul's primary triggers are code review systems where there is
  little or no opportunity to supply variable input values.

* Zuul's pipeline operations operate on refs, and run collections of
  jobs called buildsets, which do not lend themselves to ad-hoc
  builds.

To date, users with a need to run ad-hoc builds with specific input
values have been encouraged to simply create a throwaway change in
their code review system and use Zuul's dynamic configuration and
speculative execution features to specify the input values as a change
to the job variables.  This not only works, but incidentally produces
an audit trail with a complete record of the input values, and can be
easily reproduced.  The downside is that it requires users to use a
code review system when no actual code review may be needed.
Developers may be annoyed that the review system is polluted with
throwaway changes, and users creating those changes may be annoyed
they need to make changes to a git repository when not changing any
code.

Some users may address this by creating a wrapping script or
application so that users can supply their input data to the
application and it will create throwaway changes behind the scenes.

Since Zuul has almost all of the pieces necessary to implement this
functionality, let's add support for supplying parameters to builds so
that users don't need these workarounds.

Requirements
------------

* Custom field entry in the web UI; users should be able to define the
  field names that appear in the web UI.

* Auditability: the values supplied should be easily accessible after
  jobs run.

* Opt-in: not all jobs should be able to have values supplied this way.

Proposal
--------

Users should be able to use Zuul's web UI to enter parameters to use
when manually enqueing items.

When users trigger jobs in Zuul, they do not trigger individual jobs.
Instead, they enqueue one or more git refs of a project (or projects)
into a pipeline, and that ref (let's ignore circular dependencies for
now) determines what jobs are run.  We will not change anything about
this, therefore, if we provide parameters as part of the enqueing
process, those parameters involve the queue item as a whole rather
than individual jobs.

Zuul supports job variables, where within a job definition values may
be specified.  It also supports project variables, where variables
that appear within a `project` stanza are supplied to all jobs that
run in the context of that project.  Because our variables will be
specified when an item for a project is enqueued, they will behave
most like project variables: once set, they will be used for any job
in the buildset.

Pipelines are used to define workflow operations in Zuul.  Requesting
an on-demand job is a workflow operation.  OpenDev has "experimental"
pipelines for users to request that extra jobs run on a change.
Similarly, we should consider that users requesting jobs run with
certain parameters is a workflow that merits its own pipeline.

We want users to be able to control the input fields that appear in
the web UI, and the preceding considerations influence how we think
about the field definitions.  In order to determine what jobs will be
run for a queue item, we need to know the project and ref that will be
enqueued.  Given a project-ref and a pipeline, we can collect the
matching project stanzas, and from there, collect the matching jobs.
This process is known as freezing the job graph, and the web UI
already has a feature to do this.  Only after freezing the job graph
can we know what jobs will be run.  We can also use this process to
collect field definitions.

Since we are considering field definitions as part of a workflow
definition, it makes sense to put the field definitions in the
`project` stanza under the specific on-demand pipeline.  That means
the definition process will be:

* Developer defines a job that uses job variable to accomplish work
  (no change from current behavior).

* Tenant admin defines on-demand pipeline.

* Developer defines fields used for a particular project in the
  on-demand project-pipeline configuration.

* Developer attaches jobs that use those fields to the on-demand
  pipeline.

This is an example project stanza for an on-demand pipeline with input
fields:

.. code-block:: yaml

   - project:
       on-demand:
         parameters:
           - name: target_cluster
             description: Which cluster to deploy to
             type: selection
             values:
               - production
               - staging
               - dev
           - name: version
             description: What version to deploy
             type: string
         jobs:
           - deploy-app

The user will be prompted for ``target_cluster`` and ``version`` input
values, and they will be passed to the ``deploy-app`` job as variables.

The UX for a user requesting parameterized builds will be:

* User navigates to a page dedicated to enqueing builds at ``/enqueue``.

* User selects the project, branch (or other ref), and pipeline.

* User presses a submit button which triggers the freeze operations in the web server.

* Page updates with field entries for all the collected fields.

* User enters values.

* Web UI performs minimal validation that required fields are present.

* User presses a submit button to enqueue the item with the supplied values.

The existing API enqueue endpoint will accept the enqueue command with
the values in JSON.  The new web based RBAC system can be used to
restrict access to this endpoint to certain users and certain
conditions (such as projects).

As is typical for similar pages in the web UI, users will be able to
deep-link to a page with the project, ref, and pipeline already
supplied as query parameters.  Users can use this to bookmark
frequently used configurations.

In order to maintain auditability, we will add a new database field to
the `buildset` table to store input values (it will be a large blob
field so that we can store them all as a single JSON record).  We will
also add a field to store the user id of the authenticated user that
enqueued the item.

When displaying build information in the web UI, we will show these
values (on both the build and buildset pages).  When users use the web
UI to re-enqueue a buildset, we will include the same values in order
to make it easy to reproduce the results.

To give tenant administrators control over whether input values may be
used at all, or in which pipelines, the pipeline definitions
themselves will accept a new attribute, ``allow-parameters`` which
must be set to ``true`` in order for parameters to be accepted.  The
default will be ``false``.

Drawbacks
---------

For the most part, this change allows users a more convenient way to
do work that can already be accomplished other ways.  However, it does
create an opportunity to do something that we consider an
anti-pattern: release artifacts should never be created using this
method.

The information about the input values for builds is going to be
stored in the database, and we do not generally consider the database
to be archival quality.  Many Zuul users prune their databases after a
certain period of time in order to reduce storage and processing
requirements.  But a released artifact should always be traceable so
that it is known when and how it was built.  Producing release
artifacts based on the content of git repos as we do now ensures that
the input values are encoded in git repositories.

There is no way to prevent users from using this new feature to
produce release artifacts, but we should highlight the drawbacks in
the documentation and encourage users only to use it for ephemeral
workloads.
