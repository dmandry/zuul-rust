.. _job:

Job
===

A job is a unit of work performed by Zuul on an item enqueued into a
pipeline.  Items may run any number of jobs (which may depend on each
other).  Each job is an invocation of an Ansible playbook with a
specific inventory of hosts.  The actual tasks that are run by the job
appear in the playbook for that job while the attributes that appear in the
Zuul configuration specify information about when, where, and how the
job should be run.

Jobs in Zuul support inheritance.  Any job may specify a single parent
job, and any attributes not set on the child job are collected from
the parent job.  In this way, a configuration structure may be built
starting with very basic jobs which describe characteristics that all
jobs on the system should have, progressing through stages of
specialization before arriving at a particular job.  A job may inherit
from any other job in any project (however, if the other job is marked
as :attr:`job.final`, jobs may not inherit from it, and if any of its
attributes are marked as final with :attr:`job.attribute-control`,
those attributes may not be changed).

Generally, if an attribute is set on a child job, it will override (or
completely replace) attributes on the parent.  This is always true for
attributes that only accept single values, but attributes that accept
multiple values (lists, or mappings) are sometimes combined.
The default behavior varies; see the documentation for individual
attributes for details.  A special YAML tag may be used to control the
behavior explicitly.  For example, in order to specify that the tags
in the present job should override those in the parent:

.. code-block:: yaml

   - job:
       name: child
       tags: !override
         - foo

Or to indicate that they should be combined with those in the parent:

.. code-block:: yaml

   - job:
       name: child
       tags: !inherit
         - foo

Attributes which support this feature are indicated in this
documentation with "Supports override control".

When lists are combined, they are merged without duplication.

When mappings (or dictionaries, for example, those used for job
variables) are combined, they are deeply merged.  This means a leaf
node (an entry whose value is not another mapping) with the same name
will override a previous entry, but non-leaf nodes (entries whose
values are mappings) will have their entries updated in the same
manner, recursively.  New entries with unique names will be added to
mappings.

A job with no parent is called a *base job* and may only be defined in
a :term:`config-project`.  Every other job must have a parent, and so
ultimately, all jobs must have an inheritance path which terminates at
a base job.  Each tenant has a default parent job which will be used
if no explicit parent is specified.

Multiple job definitions with the same name are called variants.
These may have different selection criteria which indicate to Zuul
that, for instance, the job should behave differently on a different
git branch.  Unlike inheritance, all job variants must be defined in
the same project.  Some attributes of jobs marked :attr:`job.final`
may not be overridden.  Individual attributes marked as final with
with :attr:`job.attribute-control` may not be overridden.

When Zuul decides to run a job, it performs a process known as
freezing the job.  Because any number of job variants may be
applicable, Zuul collects all of the matching variants and applies
them in the order they appeared in the configuration.  The resulting
frozen job is built from attributes gathered from all of the
matching variants.  In this way, exactly what is run is dependent on
the pipeline, project, branch, and content of the item.

In addition to the job's main playbook, each job may specify one or
more pre- and post-playbooks.  These are run, in order, before and
after (respectively) the main playbook.  They may be used to set up
and tear down resources needed by the main playbook.  When combined
with inheritance, they provide powerful tools for job construction.  A
job only has a single main playbook, and when inheriting from a
parent, the child's main playbook overrides (or replaces) the
parent's.  However, the pre- and post-playbooks are appended and
prepended in a nesting fashion.  So if a parent job and child job both
specified pre and post playbooks, the sequence of playbooks run would
be:

* parent pre-run playbook
* child pre-run playbook
* child playbook
* child post-run playbook
* parent post-run playbook

Further inheritance would nest even deeper.  If a job fails or is
aborted before the main playbook starts, Zuul will run only the
post-run playbooks corresponding with the inheritance levels of the
pre-run playbooks which were run.  In other words, if the child
pre-run playbook was not run, then the child post-run playbook will
not be run, but the parent post-run playbook will.

Here is an example of two job definitions:

.. code-block:: yaml

   - job:
       name: base
       pre-run: copy-git-repos
       post-run: copy-logs

   - job:
       name: run-tests
       parent: base
       nodeset:
         nodes:
           - name: test-node
             label: fedora

.. attr:: job

   The following attributes are available on a job; all are optional
   unless otherwise specified:

   .. attr:: name
      :required:

      The name of the job.  By default, Zuul looks for a playbook with
      this name to use as the main playbook for the job.  This name is
      also referenced later in a project pipeline configuration.

   .. TODO: figure out how to link the parent default to tenant.default.parent

   .. attr:: parent
      :default: Tenant default-parent

      Specifies a job to inherit from.  The parent job can be defined
      in this or any other project.  Any attributes not specified on a
      job will be collected from its parent.  If no value is supplied
      here, the job specified by :attr:`tenant.default-parent` will be
      used.  If **parent** is set to ``null`` (which is only valid in
      a :term:`config-project`), this is a :term:`base job`.

   .. attr:: description

      A textual description of the job.  Not currently used directly
      by Zuul, but it is used by the zuul-sphinx extension to Sphinx
      to auto-document Zuul jobs (in which case it is interpreted as
      ReStructuredText.

   .. attr:: final
      :default: false

      To prevent other jobs from inheriting from this job, and also to
      prevent changing execution-related attributes when this job is
      specified in a project's pipeline, set this attribute to
      ``true``.

      .. warning::

         It is possible to circumvent the use of `final` in an
         :term:`untrusted-project` by creating a change which
         `Depends-On` a change which alters `final`.  This limitation
         does not apply to jobs in a :term:`config-project`.

   .. attr:: protected
      :default: false

      When set to ``true`` only jobs defined in the same project may inherit
      from this job. This includes changing execution-related attributes when
      this job is specified in a project's pipeline. Once this is set to
      ``true`` it cannot be reset to ``false``.

      .. warning::

         It is possible to circumvent the use of `protected` in an
         :term:`untrusted-project` by creating a change which
         `Depends-On` a change which alters `protected`.  This
         limitation does not apply to jobs in a
         :term:`config-project`.

   .. attr:: abstract
      :default: false

      To indicate a job is not intended to be run directly, but
      instead must be inherited from, set this attribute to ``true``.

      Once this is set to ``true`` in a job it cannot be reset to
      ``false`` within the same job by other variants; however jobs
      which inherit from it can (and by default do) reset it to
      ``false``.

      .. warning::

         It is possible to circumvent the use of `abstract` in an
         :term:`untrusted-project` by creating a change which
         `Depends-On` a change which alters `abstract`.  This
         limitation does not apply to jobs in a
         :term:`config-project`.

   .. attr:: intermediate
      :default: false

      An intermediate job must be inherited by an abstract job; it can
      not be inherited by a final job.  All ``intermediate`` jobs
      *must* also be ``abstract``; a configuration error will be
      raised if not.

      Once this is set to ``true`` in a job it cannot be reset to
      ``false`` within the same job by other variants; however jobs
      which inherit from it can (and by default do) reset it to
      ``false``.

      For example, you may define a base abstract job `foo` and create
      two abstract jobs that inherit from `foo` called
      `foo-production` and `foo-development`.  If it would be an error
      to accidentally inherit from the base job `foo` instead of
      choosing one of the two variants, `foo` could be marked as
      ``intermediate``.

   .. attr:: type
      :default: regular

      The type of job; see below for details.

      .. value:: regular

         This is a normal Zuul job and is appropriate for the vast
         majority of circumstances.

      .. value:: initializer

         An initializer job is always automatically inserted at the
         start of the job graph, so that it runs before any other jobs
         and acts as a dependency for all other jobs in the graph.  It
         behaves exactly as if all other jobs run for a queue item
         were declared with a dependency on this job using
         :attr:`job.dependencies`.

      .. value:: reporter

         An reporter job is run after all other jobs in the buildset
         have completed, and after all pipeline reporters have
         reported.  It only runs if the buildset is successful and no
         errors were encountered by the pipeline reporters.  Because
         of this, its results may not be used to influence the result
         of a buildset, or whether a change is merged.  In the case of
         a change that is merged (for example, in a :term:`gate`
         pipeline), the reporter job runs after the merge is complete.

         If a reporter job runs after a change is merged, the job
         variable :var:`zuul.buildset_refs.merge_commit_id` is
         available with information about the merged commit id.

         A reporter job may be used to synchronously update external
         references to git repositories that are gated by Zuul.

         While a reporter job is running, no action is taken for other
         items in the pipeline's shared queue.  It is recommended that
         reporter jobs run only on the executor and complete quickly
         in order to minimize delays.

         If a reporter job fails, any changes in the pipeline behind
         it will be reset and their jobs restarted with the current
         state of all relevant repositories.

         Normally, reporter jobs may only be attached to a
         project-pipeline within a config-project.  To allow an
         untrusted project to add its own reporter jobs, see the
         :attr:`tenant.untrusted-projects.<project>.allow-reporter-jobs`
         tenant configuration option.

   .. attr:: attribute-control

      Individual attributes may be set to final so that any attempt to
      set them by child jobs or variants will result in an error.

      This is a dictionary where each key is a job attribute; the
      value is another dictionary with ``final: true`` to set the
      attribute final.

      For example, to set the required-projects list fo final:

      .. code-block:: yaml

         - job:
             attribute-control:
               required-projects:
                 final: true

      The following attributes are supported:

        * requires
        * provides
        * tags
        * files
        * irrelevant-files
        * required-projects
        * include-projects
        * exclude-projects
        * vars
        * extra-vars
        * host-vars
        * group-vars
        * include-vars
        * dependencies
        * failure-output
        * type
        * voting
        * pre-run
        * run
        * post-run

   .. attr:: success-message
      :default: SUCCESS

      Normally when a job succeeds, the string ``SUCCESS`` is reported
      as the result for the job.  If set, this option may be used to
      supply a different string.

   .. attr:: failure-message
      :default: FAILURE

      Normally when a job fails, the string ``FAILURE`` is reported as
      the result for the job.  If set, this option may be used to
      supply a different string.

   .. attr:: hold-following-changes
      :default: false

      In a dependent pipeline, this option may be used to indicate
      that no jobs should start on any items which depend on the
      current item until this job has completed successfully.  This
      may be used to conserve build resources, at the expense of
      inhibiting the parallelization which speeds the processing of
      items in a dependent pipeline.

   .. attr:: voting
      :default: true

      Indicates whether the result of this job should be used in
      determining the overall result of the item.  Normally this
      attribute may be overidden even if the job is set to
      :attr:`job.final`, however, the value may be enforced
      nevertheless by using the :attr:`job.attribute-control` setting.

   .. attr:: semaphore

      A deprecated alias of :attr:`job.semaphores`.

   .. attr:: semaphores

      The name of a :ref:`semaphore` (or list of them) or
      :ref:`global_semaphore` which should be acquired and released
      when the job begins and ends.  If the semaphore is at maximum
      capacity, then Zuul will wait until it can be acquired before
      starting the job. The format is either a string, a dictionary,
      or a list of either of those in the case of multiple
      semaphores. If it's a string it references a semaphore using the
      default value for :attr:`job.semaphores.resources-first`.

      Also the name of a semaphore can be any string (without being
      previously defined via `semaphore` directive). In this case
      an implicit semaphore is created with capacity max=1.

      If multiple semaphores are requested, the job will not start
      until all have been acquired, and Zuul will wait until all are
      available before acquiring any.

      When inheriting jobs or applying variants, the list of
      semaphores is extended (semaphores specified in a job definition
      are added to any supplied by their parents).  This can not be
      changed via override control.

      .. attr:: name
         :required:

         The name of the referenced semaphore

      .. attr:: resources-first
         :default: False

         By default a semaphore is acquired before the resources are
         requested. However in some cases the user may want to run
         cheap jobs as quickly as possible in a consecutive manner. In
         this case `resources-first` can be enabled to request the
         resources before locking the semaphore. This can lead to some
         amount of blocked resources while waiting for the semaphore
         so this should be used with caution.

   .. attr:: tags

      Metadata about this job.  Tags are units of information attached
      to the job; they do not affect Zuul's behavior, but they can be
      used within the job to characterize the job.  For example, a job
      which tests a certain subsystem could be tagged with the name of
      that subsystem, and if the job's results are reported into a
      database, then the results of all jobs affecting that subsystem
      could be queried.  This attribute is specified as a list of
      strings.

      Supports override control.  The default is ``!inherit``: values
      are merged without duplication.

   .. attr:: provides

      A list of free-form strings which identifies resources provided
      by this job which may be used by other jobs for other changes
      using the :attr:`job.requires` attribute.

      Supports override control.  The default is ``!inherit``: values
      are merged without duplication.

   .. attr:: requires

      A list of free-form strings which identify resources which may
      be provided by other jobs for other changes (via the
      :attr:`job.provides` attribute) that are used by this job.

      When Zuul encounters a job with a `requires` attribute, it
      searches for those values in the `provides` attributes of any
      jobs associated with any queue items ahead of the current
      change.  In this way, if a change uses either git dependencies
      or a `Depends-On` header to indicate a dependency on another
      change, Zuul will be able to determine that the parent change
      affects the run-time environment of the child change.  If such a
      relationship is found, the job with `requires` will not start
      until all of the jobs with matching `provides` have completed or
      paused.  Additionally, the :ref:`artifacts <return_artifacts>`
      returned by the `provides` jobs will be made available to the
      `requires` job.

      If the child change is enqueued after the moment the `provides`
      job has finished artifacts are still made available to the
      `requires` job.

      If `provides` job has failed, then `requires` job is marked
      as failed and is not run.

      `provides`/`requires` artifact resolution is ignored for non-change
      items, e.g. for branch items in `supercedent` pipeline, branch items
      in periodic `independent` pipeline, tag items in `independent` pipeline.

      For example, a job which produces a builder container image in
      one project that is then consumed by a container image build job
      in another project might look like this:

      .. code-block:: yaml

         - job:
             name: build-builder-image
             provides: images

         - job:
             name: build-final-image
             requires: images

         - project:
             name: builder-project
             check:
               jobs:
                 - build-builder-image

         - project:
             name: final-project
             check:
               jobs:
                 - build-final-image

      Supports override control.  The default is ``!inherit``: values
      are merged without duplication.

   .. attr:: secrets

      A list of secrets which may be used by the job.  A
      :ref:`secret` is a named collection of private information
      defined separately in the configuration.  The secrets that
      appear here must be defined in the same project as this job
      definition.

      Each item in the list may be supplied either as a string,
      in which case it references the name of a :ref:`secret` definition,
      or as a dict. If an element in this list is given as a dict, it
      may have the following fields:

      .. attr:: name
         :required:

         The name to use for the Ansible variable into which the secret
         content will be placed.

      .. attr:: secret
         :required:

         The name to use to find the secret's definition in the
         configuration.

      .. attr:: pass-to-parent
         :default: false

         A boolean indicating that this secret should be made
         available to playbooks in parent jobs.  Use caution when
         setting this value -- parent jobs may be in different
         projects with different security standards.  Setting this to
         true makes the secret available to those playbooks and
         therefore subject to intentional or accidental exposure.

      For example:

      .. code-block:: yaml

         - secret:
             name: important-secret
             data:
               key: encrypted-secret-key-data

         - job:
             name: amazing-job
             secrets:
               - name: ssh_key
                 secret: important-secret

      will result in the following being passed as a variable to the playbooks
      in ``amazing-job``:

      .. code-block:: yaml

         ssh_key:
           key: decrypted-secret-key-data

   .. attr:: nodeset

      The nodes which should be supplied to the job.  This parameter
      may be supplied either as a string, in which case it references
      a :ref:`nodeset` definition which appears elsewhere in the
      configuration, or a dictionary, in which case it is interpreted
      in the same way as a Nodeset definition, though the top-level
      nodeset ``name`` attribute should be omitted (in essence, it is
      an anonymous Nodeset definition unique to this job; the nodes
      themselves still require names).  See the :ref:`nodeset`
      reference for the syntax to use in that case.

      If a job has an empty (or no) :ref:`nodeset` definition, it will
      still run and is able to perform limited actions within the Zuul
      executor sandbox.  Note so-called "executor-only" jobs run with
      an empty inventory, and hence Ansible's *implicit localhost*.
      This means an executor-only playbook must be written to match
      ``localhost`` directly; i.e.

      .. code-block:: yaml

          - hosts: localhost
            tasks:
             ...

      not with ``hosts: all`` (as this does not match the implicit
      localhost and the playbook will not run).  There are also
      caveats around things like enumerating the magic variable
      ``hostvars`` in this situation.  For more information see the
      Ansible `implicit localhost documentation
      <https://docs.ansible.com/ansible/latest/inventory/implicit_localhost.html>`__.

      A useful example of executor-only jobs is saving resources by
      directly utilising the prior results from testing a committed
      change.  For example, a review which updates documentation
      source files would generally test validity by building a
      documentation tree.  When this change is committed, the
      pre-built output can be copied in an executor-only job directly
      to the publishing location in a post-commit *promote* pipeline;
      avoiding having to use a node to rebuild the documentation for
      final publishing.

   .. attr:: override-checkout

      When Zuul runs jobs for a proposed change, it normally checks
      out the branch associated with that change on every project
      present in the job.  If jobs are running on a ref (such as a
      branch tip or tag), then that ref is normally checked out.  This
      attribute is used to override that behavior and indicate that
      this job should, regardless of the branch for the queue item,
      use the indicated ref (i.e., branch or tag) instead.  This can
      be used, for example, to run a previous version of the software
      (from a stable maintenance branch) under test even if the change
      being tested applies to a different branch (this is only likely
      to be useful if there is some cross-branch interaction with some
      component of the system being tested).  See also the
      project-specific :attr:`job.required-projects.override-checkout`
      attribute to apply this behavior to a subset of a job's
      projects.

      This value is also used to help select which variants of a job
      to run.  If ``override-checkout`` is set, then Zuul will use
      this value instead of the branch of the item being tested when
      collecting jobs to run.

   .. attr:: pre-timeout

      The time in seconds that the job will allow all pre-run playbooks to
      consume before timing out. If set this value must be less than or
      equal to the job ``timeout`` value as pre-run playbook runtime counts
      against the job ``timeout``. If left unset then the job ``timeout``
      value will be used.

   .. attr:: timeout

      The time in seconds that the job should be allowed to run before
      it is automatically aborted and failure is reported.  If no
      timeout is supplied, the job may run indefinitely.  Supplying a
      timeout is highly recommended.

      This timeout only applies to the pre-run and run playbooks in a
      job.

   .. attr:: post-timeout

      The time in seconds that each post playbook should be allowed to run
      before it is automatically aborted and failure is reported.  If no
      post-timeout is supplied, the job may run indefinitely.  Supplying a
      post-timeout is highly recommended.

      The post-timeout is handled separately from the above timeout because
      the post playbooks are typically where you will copy jobs logs.
      In the event of the pre-run or run playbooks timing out we want to
      do our best to copy the job logs in the post-run playbooks.

   .. attr:: attempts
      :default: 3

      When Zuul encounters an error running a job's pre-run playbook,
      Zuul will stop and restart the job.  Errors during the main or
      post-run -playbook phase of a job are not affected by this
      parameter (they are reported immediately).  This parameter
      controls the number of attempts to make before an error is
      reported.

   .. attr:: pre-run

      The name of a playbook or list of playbooks to run before the
      main body of a job.  Values are either a string describing the
      full path to the playbook in the repo where the job is defined,
      or a dictionary described below.

      When a job inherits from a parent, the child's pre-run playbooks
      are run after the parent's.  See :ref:`job` for more
      information.

      If the value is a dictionary, the following attributes are
      available:

      .. attr:: name

         The path to the playbook relative to the root of the repo.

      .. attr:: semaphore

         The name of a :ref:`semaphore` (or list of them) or
         :ref:`global_semaphore` which should be acquired and released
         when the playbook begins and ends.  If the semaphore is at
         maximum capacity, then Zuul will wait until it can be
         acquired before starting the playbook. The format is either a
         string, or a list of strings.

         If multiple semaphores are requested, the playbook will not
         start until all have been acquired, and Zuul will wait until
         all are available before acquiring any.  The time spent
         waiting for pre-run playbook semaphores is counted against
         the :attr:`job.timeout`.

         None of the semaphores specified for a playbook may also be
         specified in the same job.

   .. attr:: post-run

      The name of a playbook or list of playbooks to run after the
      main body of a job.  Values are either a string describing the
      full path to the playbook in the repo where the job is defined,
      or a dictionary described below.

      When a job inherits from a parent, the child's post-run playbooks
      are run before the parent's.  See :ref:`job` for more
      information.

      If the value is a dictionary, the following attributes are
      available:

      .. attr:: name

         The path to the playbook relative to the root of the repo.

      .. attr:: semaphore

         The name of a :ref:`semaphore` (or list of them) or
         :ref:`global_semaphore` which should be acquired and released
         when the playbook begins and ends.  If the semaphore is at
         maximum capacity, then Zuul will wait until it can be
         acquired before starting the playbook. The format is either a
         string, or a list of strings.

         If multiple semaphores are requested, the playbook will not
         start until all have been acquired, and Zuul will wait until
         all are available before acquiring any.  The time spent
         waiting for post-run playbook semaphores is counted against
         the :attr:`job.post-timeout`.

         None of the semaphores specified for a playbook may also be
         specified in the same job.

      .. attr:: cleanup
         :default: false

         A boolean value indicating whether this is a "cleanup"
         playbook.  Normally Zuul does not run post-run playbooks when
         it cancels a job, because the results of the job are
         discarded.  If this value is set, then Zuul will make an
         effort to run the playbook even if the job is canceled.

   .. attr:: cleanup-run

      .. warning:: This attribute is deprecated.  Use
                   :attr:`job.post-run.cleanup` instead.

      The name of a playbook or list of playbooks to run after job
      execution.  Values are either a string describing the full path
      to the playbook in the repo where the job is defined, or a
      dictionary described below.

      The cleanup phase is performed regardless of the job's result,
      even when the job is canceled.  Cleanup results are not taken
      into account when reporting the job result.

      When a job inherits from a parent, the child's cleanup-run playbooks
      are run before the parent's.  See :ref:`job` for more
      information.

      There is a hard-coded five minute timeout for cleanup playbooks.

      If the value is a dictionary, the following attributes are
      available:

      .. attr:: name

         The path to the playbook relative to the root of the repo.

      .. attr:: semaphore

         The name of a :ref:`semaphore` (or list of them) or
         :ref:`global_semaphore` which should be acquired and released
         when the playbook begins and ends.  If the semaphore is at
         maximum capacity, then Zuul will wait until it can be
         acquired before starting the playbook. The format is either a
         string, or a list of strings.

         If multiple semaphores are requested, the playbook will not
         start until all have been acquired, and Zuul will wait until
         all are available before acquiring any.  The time spent
         waiting for post-run playbook semaphores is counted against
         the cleanup phase timeout.

         None of the semaphores specified for a playbook may also be
         specified in the same job.

   .. attr:: run

      The name of a playbook or list of playbooks for this job.  If it
      is not supplied, the parent's playbook will be used (and
      likewise up the inheritance chain).  Values are either a string
      describing the full path to the playbook in the repo where the
      job is defined, or a dictionary described below.

      If the value is a dictionary, the following attributes are
      available:

      .. attr:: name

         The path to the playbook relative to the root of the repo.

      .. attr:: semaphore

         The name of a :ref:`semaphore` (or list of them) or
         :ref:`global_semaphore` which should be acquired and released
         when the playbook begins and ends.  If the semaphore is at
         maximum capacity, then Zuul will wait until it can be
         acquired before starting the playbook. The format is either a
         string, or a list of strings.

         If multiple semaphores are requested, the playbook will not
         start until all have been acquired, and Zuul will wait until
         all are available before acquiring any.  The time spent
         waiting for run playbook semaphores is counted against
         the :attr:`job.timeout`.

         None of the semaphores specified for a playbook may also be
         specified in the same job.

      Example:

      .. code-block:: yaml

         run: playbooks/job-playbook.yaml

      Or:

      .. code-block:: yaml

         run:
           - name: playbooks/job-playbook.yaml
             semaphores: playbook-semaphore

   .. attr:: ansible-split-streams
      :default: False

      Keep stdout/stderr of command and shell tasks separate (the Ansible
      default behavior) instead of merging stdout and stderr.

      Since version 3, Zuul has combined the stdout and stderr streams
      in Ansible command tasks, but will soon switch to using the
      normal Ansible behavior.  In an upcoming release of Zuul, this
      default will change to `True`, and in a later release, this
      option will be removed altogether.

      This option may be used in the interim to verify playbook
      compatibility and facilitate upgrading to the new behavior.

   .. attr:: ansible-version

      The ansible version to use for all playbooks of the job. This can be
      defined at the following layers of configuration where the first match
      takes precedence:

      * :attr:`job.ansible-version`
      * :attr:`tenant.default-ansible-version`
      * :attr:`scheduler.default_ansible_version`
      * Zuul default version

      The supported ansible versions are:

      .. program-output:: zuul-manage-ansible -l

   .. attr:: roles

      .. code-block:: yaml
         :name: job-roles-example

         - job:
             name: myjob
             roles:
               - zuul: myorg/our-roles-project
               - zuul: myorg/ansible-role-foo
                 name: foo

      A list of Ansible roles to prepare for the job.  Because a job
      runs an Ansible playbook, any roles which are used by the job
      must be prepared and installed by Zuul before the job begins.
      This value is a list of dictionaries, each of which indicates
      one of two types of roles: a Galaxy role, which is simply a role
      that is installed from Ansible Galaxy, or a Zuul role, which is
      a role provided by a project managed by Zuul.  Zuul roles are
      able to benefit from speculative merging and cross-project
      dependencies when used by playbooks in untrusted projects.
      Roles are added to the Ansible role path in the order they
      appear on the job -- roles earlier in the list will take
      precedence over those which follow.

      This attribute is not overridden on inheritance or variance;
      instead roles are added with each new job or variant.  In the
      case of job inheritance or variance, the roles used for each of
      the playbooks run by the job will be only those which were
      cumulatively defined up to that point in the inheritance
      hierarchy where that playbook was added.  If a child job
      inherits from a parent which defines a pre and post playbook,
      then the pre and post playbooks it inherits from the parent job
      will run only with the roles that were defined on the parent.
      If the child adds its own pre and post playbooks, then any roles
      added by the child will be available to the child's playbooks.
      This is so that a job which inherits from a parent does not
      inadvertently alter the behavior of the parent's playbooks by
      the addition of conflicting roles.  Roles added by a child will
      appear before those it inherits from its parent.

      If a project used for a Zuul role has branches, the usual
      process of selecting which branch should be checked out applies.
      See :attr:`job.override-checkout` for a description of that
      process and how to override it.  As a special case, if the role
      project is the project in which this job definition appears,
      then the branch in which this definition appears will be used.
      In other words, a playbook may not use a role from a different
      branch of the same project.

      If the job is run on a ref (for example, a branch tip or a tag)
      then a different form of the branch selection process is used.
      There is no single branch context available for selecting an
      appropriate branch of the role's repo to check out, so only the
      following are considered: First the ref specified by
      :attr:`job.required-projects.override-checkout`, or
      :attr:`job.override-checkout`.  Then if the role repo is the
      playbook repo or has a branch name that matches the branch of
      the playbook repo, that branch is used; otherwise the project's
      default branch is selected.

      .. warning::

         Keep this behavior difference in mind when designing jobs
         that run on both branches and tags.  If the same job must be
         used in both circumstances, ensure that any roles from other
         repos used by playbooks in the job originate only in
         un-branched repositories.  Otherwise different branches of
         the role repo may be checked out.

      A project which supplies a role may be structured in one of two
      configurations: a bare role (in which the role exists at the
      root of the project), or a contained role (in which the role
      exists within the ``roles/`` directory of the project, perhaps
      along with other roles).  In the case of a contained role, the
      ``roles/`` directory of the project is added to the role search
      path.  In the case of a bare role, the project itself is added
      to the role search path.  In case the name of the project is not
      the name under which the role should be installed (and therefore
      referenced from Ansible), the ``name`` attribute may be used to
      specify an alternate.

      A job automatically has the project in which it is defined added
      to the roles path if that project appears to contain a role or
      ``roles/`` directory.  By default, the project is added to the
      path under its own name, however, that may be changed by
      explicitly listing the project in the roles list in the usual
      way.

      .. attr:: galaxy

         .. warning:: Galaxy roles are not yet implemented.

         The name of the role in Ansible Galaxy.  If this attribute is
         supplied, Zuul will search Ansible Galaxy for a role by this
         name and install it.  Mutually exclusive with ``zuul``;
         either ``galaxy`` or ``zuul`` must be supplied.

      .. attr:: zuul

         The name of a Zuul project which supplies the role.  Mutually
         exclusive with ``galaxy``; either ``galaxy`` or ``zuul`` must
         be supplied.

      .. attr:: name

         The installation name of the role.  In the case of a bare
         role, the role will be made available under this name.
         Ignored in the case of a contained role.

   .. attr:: required-projects

      A list of other projects which are used by this job.  Any Zuul
      projects specified here will also be checked out by Zuul into
      the working directory for the job.  Speculative merging and
      cross-repo dependencies will be honored.  If there is not a
      change for the project ahead in the pipeline, its repo state as
      of the time the item was enqueued will be frozen and used for
      all jobs for a given change (see :ref:`global_repo_state`).

      The format for this attribute is either a list of strings or
      dictionaries.  Strings are interpreted as project names,
      dictionaries, if used, may have the following attributes:

      Supports override control.  The default is ``!inherit``: values
      are merged without duplication.

      .. attr:: name
         :required:

         The name of the required project.

      .. attr:: override-checkout

         When Zuul runs jobs for a proposed change, it normally checks
         out the branch associated with that change on every project
         present in the job.  If jobs are running on a ref (such as a
         branch tip or tag), then that ref is normally checked out.
         This attribute is used to override that behavior and indicate
         that this job should, regardless of the branch for the queue
         item, use the indicated ref (i.e., branch or tag) instead,
         for only this project.  See also the
         :attr:`job.override-checkout` attribute to apply the same
         behavior to all projects in a job.

         This value is also used to help select which variants of a
         job to run.  If ``override-checkout`` is set, then Zuul will
         use this value instead of the branch of the item being tested
         when collecting any jobs to run which are defined in this
         project.

   .. attr:: include-projects
      :default: null

      A list of projects that should be included when preparing the
      workspace for this job.  Any projects that are required for
      playbooks, roles, include-vars, or required-projects will be
      present and prepared regardless.  This may only be used to
      filter projects that would otherwise already be present in the
      workspace for other reasons.  That includes projects for changes
      related to any items ahead in the queue, dependencies of those
      changes, and also the changes under test for the current queue
      item, and any required projects for the job.

      Note that `include-projects` alone is not sufficient to cause a
      project to be prepared in the workspace.  It must be selected
      due to one of the preceding reasons.  See
      :attr:`job.required-projects` to force a project to appear in
      the workspace.

      This feature may be useful for jobs which are known to interact
      with a known set of repositories (or even no repositories).
      Such jobs may be able to run faster if only the necessary
      projects are prepared in the workspace.

      The default value of ``null`` indicates no filtering is to be
      performed.

      Supports override control.  The default is ``!inherit``: values
      are merged without duplication.

      The items in the list may either be a string, in which case they
      are interpreted as the name of a project, or a dictionary with
      the following form:

      .. attr:: type

         The type of entry.  May be one of the following values:

         .. attr:: name

            The entry is the name of a project (equivalent to a bare
            string).  If this entry is supplied, the ``name`` key in the
            dictionary must also be present.

         .. attr:: change

            The entry matches the project of the current change under test.

         .. attr:: item

            The entry matches any project in the current queue item.

      .. attr:: name

         Only used with the ``name`` type, this string is the name of
         the project to match.

   .. attr:: exclude-projects
      :default: null

      A list of projects that should be excluded when preparing the
      workspace for this job.

      See :attr:`job.include-projects` for a general description of
      the feature.

      This filter is applied after :attr:`job.include-projects`, so it
      may further refine a list of projects.

      The default value of ``null`` indicates no filtering is to be
      performed.

      Supports override control.  The default is ``!inherit``: values
      are merged without duplication.

      The items in the list may either be a string, in which case they
      are interpreted as the name of a project, or a dictionary with
      the following form:

      .. attr:: type

         The type of entry.  May be one of the following values:

         .. attr:: name

            The entry is the name of a project (equivalent to a bare
            string).  If this entry is supplied, the ``name`` key in the
            dictionary must also be present.

         .. attr:: change

            The entry matches the project of the current change under test.

         .. attr:: item

            The entry matches any project in the current queue item.

      .. attr:: name

         Only used with the ``name`` type, this string is the name of
         the project to match.

   .. attr:: vars

      A dictionary of variables to supply to Ansible.

      When running a trusted playbook, the value of variables will be
      frozen at the start of the job.  Therefore if the value of the
      variable is an Ansible Jinja template, it may only reference
      values which are known at the start of the job, and its value
      will not change.  Untrusted playbooks dynamically evaluate
      variables and are not limited by this restriction.

      Un-frozen versions of all the original job variables are
      available tagged with the ``!unsafe`` YAML tag under the
      ``unsafe_vars`` variable hierarchy.  This tag prevents Ansible
      from evaluating them as Jinja templates.  For example, the job
      variable `myvar` would be available under `unsafe_vars.myvar`.
      Advanced users may force Ansible to evaluate these values, but
      it is not recommended to do so except in the most controlled of
      circumstances.  They are almost impossible to render safely.

      Supports override control.  The default is ``!inherit``: values
      are deep-merged.

   .. attr:: extra-vars

      A dictionary of variables to supply to Ansible with higher
      precedence than job, host, or group vars. Note, that despite
      the name this is not passed to Ansible using the `--extra-vars`
      flag.

      Supports override control.  The default is ``!inherit``: values
      are deep-merged.

   .. attr:: host-vars

      A dictionary of host variables to supply to Ansible.  The keys
      of this dictionary are node names as defined in a
      :ref:`nodeset`, and the values are dictionaries of variables,
      just as in :attr:`job.vars`.

      Supports override control.  The default is ``!inherit``: values
      are deep-merged.

   .. attr:: group-vars

      A dictionary of group variables to supply to Ansible.  The keys
      of this dictionary are node groups as defined in a
      :ref:`nodeset`, and the values are dictionaries of variables,
      just as in :attr:`job.vars`.

      Supports override control.  The default is ``!inherit``: values
      are deep-merged.

   An example of three kinds of variables:

   .. code-block:: yaml

      - job:
          name: variable-example
          nodeset:
            nodes:
              - name: controller
                label: fedora-27
              - name: api1
                label: centos-7
              - name: api2
                label: centos-7
            groups:
              - name: api
                nodes:
                  - api1
                  - api2
         vars:
           foo: "this variable is visible to all nodes"
         host-vars:
           controller:
             bar: "this variable is visible only on the controller node"
         group-vars:
           api:
             baz: "this variable is visible on api1 and api2"

   .. attr:: include-vars

      A list of files from which to read variables.

      The value may be supplied as a list or a single item, and each
      value may be a string or a dictionary described below.  If
      supplied as a string, it is treated as the
      :attr:`job.include-vars.name`.

      Files are read in order, with later variable values overriding
      earlier ones.  Variables specified by :attr:`job.vars` and
      related attributes will override variables read from files.

      The file should be in YAML or JSON format.

      Supports override control.  The default is ``!inherit``: values
      are appended without duplication.

      .. attr:: name
         :required:

         The name (relative to the root of the repository) of the file
         to read.

      .. attr:: project

         The name of the project containing the file to read.  If this
         is left unspecified, the project containing the current job
         definition is used.  This option is mutually exclusive with
         :attr:`job.include-vars.zuul-project`.

      .. attr:: required
         :default: true

         A boolean indicating whether this file is required to be
         present.  If this is set to ``true`` and the file is not
         present, it is considered an error and the job result will
         reflect this.

      .. attr:: zuul-project
         :default: false

         A boolean indicating that instead of using a specified
         project, the project associated with the change under test
         (which can be found in the :var:`zuul.project` variable)
         should be used.  This permits the definition of jobs that may
         be centrally defined and used globally to read variables
         from files in the projects upon which they run.

         This option is mutually exclusive with
         :attr:`job.include-vars.project`.

      .. attr:: use-ref
         :default: true

         When this is ``true`` (the default) if the job is triggered
         by a ref, and that ref is for the include-vars project, then
         Zuul will checkout the ref and use the file from that ref
         checkout.  If the include-vars is for a different project
         than the triggering ref, or the job is not triggered by a
         ref, or this is set to ``false``, then Zuul will follow the
         normal fallback procedure for branches to determine from
         which branch to load the file.

      An example using job-vars:

      .. code-block:: yaml

         - job:
             name: central-job
             description: |
               This job reads versions.yaml from whatever project
               it is used to test.
             include-vars:
               - name: versions.yaml
                 zuul-project: true

         - job:
             name: integration-job
             description: |
               This job reads data/product-versions.yaml from the repo
               that contains this very job definition, no matter which
               project runs the job.
             include-vars: data/product-versions.yaml

   .. attr:: dependencies

      A list of other jobs upon which this job depends.  Zuul will not
      start executing this job until all of its dependencies have
      completed successfully or have been paused, and if one or more of
      them fail, this job will not be run.

      The dependent job is provided with :ref:`artifacts <return_artifacts>`
      returned by preceding jobs, e.g. if jobC depends on jobB, jobB
      depends on jobA, then jobC is provided with artifacts from both jobA
      and jobB.

      The format for this attribute is either a list of strings or
      dictionaries.  Strings are interpreted as job names,
      dictionaries, if used, may have the following attributes:

      .. attr:: name
         :required:

         The name of the required job.

      .. attr:: soft
         :default: false

         A boolean value which indicates whether this job is a *hard*
         or *soft* dependency.  A *hard* dependency will cause an
         error if the specified job is not run.  That is, if job B
         depends on job A, but job A is not run for any reason (for
         example, it contains a file matcher which does not match),
         then Zuul will not run any jobs and report an error.  A
         *soft* dependency will simply be ignored if the dependent job
         is not run.

      Supports override control.  The default is ``!override``: values
      are overridden.

   .. attr:: allowed-projects

      A list of Zuul projects which may use this job.  By default, a
      job may be used by any other project known to Zuul, however,
      some jobs use resources or perform actions which are not
      appropriate for other projects.  In these cases, a list of
      projects which are allowed to use this job may be supplied.  If
      this list is not empty, then it must be an exhaustive list of
      all projects permitted to use the job.  The current project
      (where the job is defined) is not automatically included, so if
      it should be able to run this job, then it must be explicitly
      listed.  This setting is ignored by :term:`config projects
      <config-project>` -- they may add any job to any project's
      pipelines.  By default, all projects may use the job.

      If a :attr:`job.secrets` is used in a job definition in an
      :term:`untrusted-project`, `allowed-projects` is automatically
      set to the current project only, and can not be overridden.
      However, a :term:`config-project` may still add such a job to
      any project's pipeline.  Apply caution when doing so as other
      projects may be able to expose the source project's secrets.

      This attribute is not overridden by inheritance; instead it is
      the intersection of all applicable parents and variants (i.e.,
      jobs can reduce but not expand the set of allowed projects when
      they inherit).

      .. warning::

         It is possible to circumvent the use of `allowed-projects` in
         an :term:`untrusted-project` by creating a change which
         `Depends-On` a change which alters `allowed-projects`.  This
         limitation does not apply to jobs in a
         :term:`config-project`, or jobs in an `untrusted-project`
         which use a secret.

   .. attr:: post-review
      :default: false

      A boolean value which indicates whether this job may only be
      used in pipelines where :attr:`pipeline.post-review` is
      ``true``.  This is automatically set to ``true`` if this job
      uses a :ref:`secret` and is defined in a :term:`untrusted-project`.
      It may be explicitly set to obtain the same behavior for jobs
      defined in :term:`config projects <config-project>`.  Once this
      is set to ``true`` anywhere in the inheritance hierarchy for a job,
      it will remain set for all child jobs and variants (it can not be
      set to ``false``).

      .. warning::

         It is possible to circumvent the use of `post-review` in an
         :term:`untrusted-project` by creating a change which
         `Depends-On` a change which alters `post-review`.  This
         limitation does not apply to jobs in a
         :term:`config-project`, or jobs in an `untrusted-project`
         which use a secret.

   .. attr:: branches

      A :ref:`regular expression <regex>` (or list of regular
      expressions) which describe on what branches a job should run
      (or in the case of variants, to alter the behavior of a job for
      a certain branch).

      This attribute is not inherited in the usual manner.  Instead,
      it is used to determine whether each variant on which it appears
      will be used when running the job.

      If none of the defined job variants contain a branches setting which
      matches the branch of an item, then that job is not run for the item.
      Otherwise, all of the job variants which match that branch are
      used when freezing the job.  However, if
      :attr:`job.override-checkout` or
      :attr:`job.required-projects.override-checkout` are set for a
      project, Zuul will attempt to use the job variants which match
      the values supplied in ``override-checkout`` for jobs defined in
      those projects.  This can be used to run a job defined in one
      project on another project without a matching branch.

      If a tag item is enqueued, we look up the branches which contain
      the commit referenced by the tag.  If any of those branches match a
      branch matcher, the matcher is considered to have matched.

      Additionally in the case of a tag item, if the expression
      matches the full name of the ref (eg, `refs/tags/foo`) then the
      job is considered to match.  The preceding section still
      applies, so the definition must appear in a branch containing
      the commit referenced by the tag to be considered, and then the
      expression must also match the tag.

      This example illustrates a job called *run-tests* which uses a
      nodeset based on the current release of an operating system to
      perform its tests, except when testing changes to the stable/2.0
      branch, in which case it uses an older release:

      .. code-block:: yaml

         - job:
             name: run-tests
             nodeset: current-release

         - job:
             name: run-tests
             branches: stable/2.0
             nodeset: old-release

      In some cases, Zuul uses an implied value for the branch
      specifier if none is supplied:

      * For a job definition in a :term:`config-project`, no implied
        branch specifier is used.  If no branch specifier appears, the
        job applies to all branches.

      * In the case of an :term:`untrusted-project`, if the project
        has only one branch, no implied branch specifier is applied to
        :ref:`job` definitions.  If the project has more than one
        branch, the branch containing the job definition is used as an
        implied branch specifier.

      This allows for the very simple and expected workflow where if a
      project defines a job on the ``master`` branch with no branch
      specifier, and then creates a new branch based on ``master``,
      any changes to that job definition within the new branch only
      affect that branch, and likewise, changes to the master branch
      only affect it.

      See :attr:`pragma.implied-branch-matchers` for how to override
      this behavior on a per-file basis.  The behavior may also be
      configured by a Zuul administrator using
      :attr:`tenant.untrusted-projects.<project>.implied-branch-matchers`.

   .. attr:: files

      This indicates that the job should only run on changes where the
      specified files are modified.  Unlike **branches**, this value
      is subject to inheritance and overriding, so only the final
      value is used to determine if the job should run. This is a
      :ref:`regular expression <regex>` or list of regular expressions.

      Supports override control.  The default is ``!override``: values
      are overridden.

      .. warning::

         File filters will be ignored for refs that don't have any
         files. This will be the case for merge commits (e.g. in a post
         pipeline) or empty commits created with
         ``git commit --allow-empty`` (which can be used in order to
         run all jobs).

   .. attr:: irrelevant-files

      This is a negative complement of **files**.  It indicates that
      the job should run unless *all* of the files changed match this
      list.  In other words, if the regular expression ``docs/.*`` is
      supplied, then this job will not run if the only files changed
      are in the docs directory.  A :ref:`regular expression <regex>`
      or list of regular expressions.

      Supports override control.  The default is ``!override``: values
      are overridden.

      .. warning::

         File filters will be ignored for refs that don't have any
         files. This will be the case for merge commits (e.g. in a post
         pipeline) or empty commits created with
         ``git commit --allow-empty`` (which can be used in order to
         run all jobs).

   .. attr:: match-on-config-updates
      :default: true

      If this is set to ``true`` (the default), then the job's file
      matchers are ignored if a change alters the job's configuration,
      any of its playbooks, or any of its :attr:`job.include-vars`
      files.  This means that changes to jobs with file matchers will
      be self-testing without requiring that the file matchers include
      the Zuul configuration file defining the job or other referenced
      files.

   .. attr:: deduplicate
      :default: auto

      In the case of a dependency cycle where multiple changes within
      the cycle run the same job, this setting indicates whether Zuul
      should attempt to deduplicate the job.  If it is deduplicated,
      then the job will only run for one queue item within the cycle
      and other items which run the same job will use the results of
      that build.

      This setting determines whether Zuul will consider deduplication.
      If it is set to ``false``, Zuul will never attempt to
      deduplicate the job.  If it is set to ``auto`` (the default),
      then Zuul will compare the job with other jobs of other queue
      items in the dependency cycle, and if they are equivalent and
      meet certain project criteria, it will deduplicate them.

      The project criteria that Zuul considers under the ``auto``
      setting are either:

      * The job must specify :attr:`job.required-projects`.
      * Or the queue items must be for the same project.

      This is because of the following heuristic: if a job specifies
      :attr:`job.required-projects`, it is most likely to be one which
      operates in the same way regardless of which project the change
      under test belongs to, therefore the result of the same job
      running on two queue items in the same dependency cycle should
      be the same.  If a job does not specify
      :attr:`job.required-projects` and runs with two different
      projects under test, the outcome is likely different for those
      two items.

      If this is not true for a job (e.g., the job ignores the project
      under test and interacts only with external resources)
      :attr:`job.deduplicate` may be set to ``true`` to ignore the
      heuristic and deduplicate anyway.

   .. attr:: failure-output

      A regular expression (or list of regular expressions) that
      should be matched against job output to determine if the job is
      going to fail.  Matches are performed line-by-line (multiline
      regular expressions will not be effective).

      This option is not required; job failure is determined by the
      result code from its Ansible playbooks.  However, if this option
      is supplied, and one of the regular expressions matches a line
      in the streaming output from the job, Zuul will be able to
      anticipate the failure before the completion of the playbook.
      In this case, it will be able to restart jobs for changes behind
      it in a dependent pipeline.

      Use caution when specifying this option.  If an early failure is
      triggered, the job result will be recorded as FAILURE even if
      the job playbooks ultimately succeed.

      Supports override control.  The default is ``!inherit``: values
      are merged without duplication.

   .. attr:: workspace-checkout
      :default: true

      Whether to perform a full checkout of projects in the workspace.

      This only applies to the workspace on the executor.  Most Zuul
      jobs copy repositories to remote :ref:`build_nodes` and
      independently checkout the appropriate refs.  Changing this
      setting should typically not affect the contents on remote
      nodes.

      Setting this option may be useful to save time or space when
      preparing large repositories which are not expected to be used
      on the Zuul executor, but care is needed to ensure that it does
      not affect job execution.

      When set to ``true`` (the default and recommended value under
      most circumstances), Zuul will perform a full git checkout of
      all of the involved repositories in the executor's workspace.

      When set to ``false``, Zuul will not perform a checkout of any
      of the involved repositories in the workspace.  Further, when it
      checks out repositories within private directories in order to
      run playbooks or roles, it will perform a sparse checkout with
      only the directories it expects to need.  This may cause
      problems if playbooks or roles reference files outside of the
      sparse checkout.  In this case, the option is unsuitable and
      should be set to ``true`` for the job so that full checkouts are
      performed.

      When set to the string ``auto``, Zuul will behave as if the
      value is ``true`` if and only if the job contains an empty
      nodeset, otherwise it will behave as if the value is ``false``.
      The reasoning is that jobs with no nodeset are likely to access
      the contents of the repos on the executor, whereas jobs with a
      nodeset may access them only on remote nodes.

   .. attr:: workspace-scheme
      :default: golang

      The scheme to use when placing git repositories in the
      workspace.

      .. value:: golang

         This writes the repository into a directory based on the
         canonical hostname and the full name of the repository.  For
         example::

           src/example.com/organization/project

         This is the default and, despite the name, is suitable and
         recommended for any language.

      .. value:: flat

         This writes the repository into a directory based only on the
         last component of the name.  For example::

           src/project

         In some cases the ``golang`` scheme can produce collisions
         (consider the projects `component` and
         `component/subcomponent`).  In this case it may be preferable
         to use the ``flat`` scheme (which would produce repositories
         at `component` and `subcomponent`).

         Note, however, that this scheme may produce collisions with
         `component` and `component/component`.

      .. value:: unique

         This writes the repository into a directory based on the
         organization name and the  ``urllib.parse.quote_plus`` formatted
         project name.  For example::

           src/example.com/organization/organization%2Fproject


         This scheme will produce unique workspace paths for every repository
         and won't cause collisions.

   .. attr:: image-build-name
      :type: str

      If this is an image build job (see :ref:`image-creation`), set
      this to the name of the image that is to be built.  This must
      match an :attr:`image` object defined in the same project.
