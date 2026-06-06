.. _project:

Project
=======

A project corresponds to a source code repository with which Zuul is
configured to interact.  The main responsibility of the project
configuration item is to specify which jobs should run in which
pipelines for a given project.  Within each project definition, a
section for each :ref:`pipeline <pipeline>` may appear.  This
project-pipeline definition is what determines how a project
participates in a pipeline.

Multiple project definitions may appear for the same project (for
example, in a central :term:`config projects <config-project>` as well
as in a repo's own ``.zuul.yaml``).  In this case, all of the project
definitions for the relevant branch are combined (the jobs listed in
all of the matching definitions will be run).  In the case of an item
which does not have a branch (for example, a tag), all of the project
definitions will be combined.

Consider the following project definition::

  - project:
      name: yoyodyne
      queue: integrated
      check:
        jobs:
          - check-syntax
          - unit-tests
      gate:
        jobs:
          - unit-tests
          - integration-tests

The project has two project-pipeline stanzas, one for the ``check``
pipeline, and one for ``gate``.  Each specifies which jobs should run
when a change for that project enters the respective pipeline -- when
a change enters ``check``, the ``check-syntax`` and ``unit-test`` jobs
are run.

Pipelines which use the dependent pipeline manager (e.g., the ``gate``
example shown earlier) maintain separate queues for groups of
projects.  When Zuul serializes a set of changes which represent
future potential project states, it must know about all of the
projects within Zuul which may have an effect on the outcome of the
jobs it runs.  If project *A* uses project *B* as a library, then Zuul
must be told about that relationship so that it knows to serialize
changes to A and B together, so that it does not merge a change to B
while it is testing a change to A.

Zuul could simply assume that all projects are related, or even infer
relationships by which projects a job indicates it uses, however, in a
large system that would become unwieldy very quickly, and
unnecessarily delay changes to unrelated projects.  To allow for
flexibility in the construction of groups of related projects, the
change queues used by dependent pipeline managers are specified
manually.  To group two or more related projects into a shared queue
for a dependent pipeline, set the ``queue`` parameter to the same
value for those projects.

The ``gate`` project-pipeline definition above specifies that this
project participates in the ``integrated`` shared queue for that
pipeline.

.. attr:: project

   The following attributes may appear in a project:

   .. attr:: name

      The name of the project.  If Zuul is configured with two or more
      unique projects with the same name, the canonical hostname for
      the project should be included (e.g., `git.example.com/foo`).
      This can also be a regex. In this case the regex must start with ``^``
      and match the full project name following the same rule as name without
      regex. If not given it is implicitly derived from the project where this
      is defined.

   .. attr:: templates

      A list of :ref:`project-template` references; the
      project-pipeline definitions of each Project Template will be
      applied to this project.  If more than one template includes
      jobs for a given pipeline, they will be combined, as will any
      jobs specified in project-pipeline definitions on the project
      itself.

   .. attr:: branches

      A :ref:`regular expression <regex>` (or list of regular
      expressions) which describe to which branches this `project`
      stanza should apply. A partial match
      is treated as a valid match, so care must be taken when specifying
      branch names that are partial matches of other branch names
      (e.g., ``main`` matches ``maint``). A full match such as
      ``^main$`` may be specified to avoid ambiguity.

      If omitted on a `project` stanza within an
      :term:`untrusted-project` that is configuring its own project,
      the current branch will be used (:attr:`pragma` settings are
      ignored).  That means that in the typical case where this option
      is omitted on an untrusted project, the stanza is always
      interpreted as configuring the project on the branch where the
      definition is found.

      If omitted on a `project` stanza within a
      :term:`config-project`, the stanza will be interpreted as
      applying to all branches (but :attr:`pragma` settings are
      effective in this case, see below).

      If omitted when configuring a project other than the current
      project, if the current project is branched, then the current
      branch will be used (but :attr:`pragma` settings are effective
      in this case, see below).  If the current project has only one
      branch, then the stanza will be interpreted as applying to all
      branches.

      In the cases where :attr:`pragma` settings are effective, if
      :attr:`pragma.implied-branch-matchers` is in effect then
      :attr:`pragma.implied-branches` will be used.

      In all cases, explicit configuration of branches overrides
      implied branches.

      Note that use of this attribute when configuring the jobs run on
      the current project can produce undesirable behavior when
      combined with common project branching paradigms.  In
      particular, note that when a project is branched, the project
      stanzas are effectively copied onto that branch, and therefore
      additional explicit stanzas will be in effect.  It is
      recommended to only use this attribute inside unbranched
      projects and instead use the default implicit branch behavior
      for branched projects.

   .. attr:: default-branch
      :default: master

      The name of a branch that Zuul should check out in jobs if no
      better match is found.  Typically Zuul will check out the branch
      which matches the change under test, or if a job has specified
      an :attr:`job.override-checkout`, it will check that out.
      However, if there is no matching or override branch, then Zuul
      will checkout the default branch.

      Each project may only have one ``default-branch`` therefore Zuul
      will use the first value that it encounters for a given project
      (regardless of in which branch the definition appears).  It may
      not appear in a :ref:`project-template` definition.

      This setting also affects the order in which configuration
      objects are processed.  Zuul will process the default branch
      first before any other branches.

      The Gerrit and GitHub drivers will automatically use the default
      branch as specified for the repository in their respective
      systems as a default value for this setting.  It may be
      overridden by setting this value explicitly.

   .. attr:: merge-mode
      :default: (driver specific)

      The merge mode which is used by Git for this project.  Be sure
      this matches what the remote system which performs merges (i.e.,
      Gerrit). The requested merge mode will also be used by the
      GitHub and GitLab drivers when performing merges.

      Each project may only have one ``merge-mode`` therefore Zuul
      will use the first value that it encounters for a given project
      (regardless of in which branch the definition appears).  It may
      not appear in a :ref:`project-template` definition.

      It must be one of the following values:

      .. value:: merge

         Uses the default git merge strategy. This maps to the merge
         mode ``merge`` in GitHub and GitLab. This is the default
         merge mode for all drivers except gerrit and GitHub.

      .. value:: merge-resolve

         Uses the resolve git merge strategy. This is a very
         conservative merge strategy which most closely matches the
         behavior of Gerrit, and is the default merge mode for Gerrit.
         This maps to the merge mode ``merge`` in GitHub and GitLab.

      .. value:: merge-recursive

         Uses the ``recursive`` git merge strategy. This is the default
         merge mode for GitHub Enterprise version earlier than 3.8.

      .. value:: merge-ort

         Uses the ``ort`` git merge strategy. This is the default merge
         mode for github.com and GitHub Enterprise version 3.8 or newer.

      .. value:: cherry-pick

         Cherry-picks each change onto the branch rather than
         performing any merges. This is not supported by GitHub and GitLab.

      .. value:: squash-merge

         Squash merges each change onto the branch. This maps to the
         merge mode ``squash`` in GitHub and GitLab.

      .. value:: rebase

         Rebases the changes onto the branch.  This is only supported
         by GitHub and maps to the ``rebase`` merge mode (but
         does not alter committer information in the way that GitHub
         does in the repos that Zuul prepares for jobs).

   .. attr:: vars
      :default: None

      A dictionary of variables to be made available for all jobs in
      all pipelines of this project.  For more information see
      :ref:`variable inheritance <user_jobs_variable_inheritance>`.

   .. attr:: queue

      This specifies the
      name of the shared queue this project is in.  Any projects
      which interact with each other in tests should be part of the
      same shared queue in order to ensure that they don't merge
      changes which break the others.  This is a free-form string;
      just set the same value for each group of projects.

      The name can refer to the name of a :attr:`queue` which allows
      further configuration of the queue.

      All pipelines for a project-branch can only belong to one queue,
      therefore Zuul will use the first value that it encounters.
      It need not appear in the first instance of a :attr:`project`
      stanza; it may appear in secondary instances or even in a
      :ref:`project-template` definition.

      If a queue is :value:`queue.type.branch-assigned`, then
      different branches of the same project may belong to different
      queues.

      .. note:: This attribute is not evaluated speculatively and
                its setting shall be merged to be effective.

   .. attr:: <pipeline>

      Each pipeline that the project participates in should have an
      entry in the project.  The value for this key should be a
      dictionary with the following format:

      .. attr:: jobs
         :required:

         A list of jobs that should be run when items for this project
         are enqueued into the pipeline.  Each item of this list may
         be a string, in which case it is treated as a job name, or it
         may be a dictionary, in which case it is treated as a job
         variant local to this project and pipeline.  In that case,
         the format of the dictionary is the same as the top level
         :attr:`job` definition.  Any attributes set on the job here
         will override previous versions of the job.

      .. attr:: debug

         If this is set to `true`, Zuul will include debugging
         information in reports it makes about items in the pipeline.
         This should not normally be set, but in situations were it is
         difficult to determine why Zuul did or did not run a certain
         job, the additional information this provides may help.

      .. attr:: fail-fast
         :default: false

         If this is set to `true`, Zuul will report a build or node failure
         immediately and abort all still running builds. This can be used
         to save resources in resource constrained environments at the cost
         of potentially requiring multiple attempts if more than one problem
         is present.

         Once this is defined it cannot be overridden afterwards. So this
         can be forced to a specific value by e.g. defining it in a config
         repo.

      .. attr:: parameters
         :type: list

         If :attr:`pipeline.allow-parameters` for this pipeline is set
         to `true`, then this may be set to a list of parameters that
         should be supplied by the user when enqueing changes.

         .. attr:: name
            :required:

            The name of the parameter.  This must be a valid Ansible
            variable name.

         .. attr:: type

            One of the following input types:

            .. value:: string

            .. value:: selection

            .. value:: multiple-selection

            .. value:: bool

         .. attr:: description

            Descriptive text about the parameter.

         .. attr:: values
            :type: list

            If the parameter is a `selection` or `multiple-selection`,
            this is a list of the permissible values.

         .. attr:: default

            A suggested default value.  This may be used by the web
            interface or other client applications to supply a default
            value, but it is not automatically used by Zuul if no
            value is supplied with the enqueue command.

         .. attr:: required

            Whether the parameter is required.  This is provided to
            the web interface and other client applications but is not
            enforced by the enqueue command.


.. _project-template:

Project Template
================

A Project Template defines one or more project-pipeline definitions
which can be re-used by multiple projects.

A Project Template uses the same syntax as a :ref:`project`
definition, however, in the case of a template, the
:attr:`project.name` attribute does not refer to the name of a
project, but rather names the template so that it can be referenced in
a :ref:`project` definition.

Because Project Templates may be used outside of the projects where
they are defined, they honor the implied branch :ref:`pragmas <pragma>`
(unlike Projects).  The same heuristics described in
:attr:`job.branches` that determine what implied branches a :ref:`job`
will receive apply to Project Templates (with the exception that it is
not possible to explicitly set a branch matcher on a Project Template).
