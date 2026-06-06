:title: Job Content

.. _job-content:

Job Content
===========

Zuul jobs are implemented as Ansible playbooks.  Zuul prepares the
repositories used for a job, installs any required Ansible roles, and
then executes the job's playbooks.  Any setup or artifact collection
required is the responsibility of the job itself.  While this flexible
arrangement allows for almost any kind of job to be run by Zuul,
batteries are included.  Zuul has a standard library of jobs upon
which to build.

Working Directory
-----------------

Before starting each job, the Zuul executor creates a directory to
hold all of the content related to the job.  This includes some
directories which are used by Zuul to configure and run Ansible and
may not be accessible, as well as a directory tree, under ``work/``,
that is readable and writable by the job.  The hierarchy is:

**work/**
  The working directory of the job.

**work/src/**
  Contains the prepared git repositories for the job.

**work/logs/**
  Where the Ansible log for the job is written; your job
  may place other logs here as well.

The home directory of the user running the job is outside this
hierarchy.  A future version of Zuul will restrict which files in the
home directory may be carried over between playbooks.  To use files in
later playbooks, put them under ``work/`` rather than ``$HOME``.

Git Repositories
----------------

The git repositories in ``work/src`` contain the repositories for all
of the projects specified in the ``required-projects`` section of the
job, plus the project associated with the queue item if it isn't
already in that list.  In the case of a proposed change, that change
and all of the changes ahead of it in the pipeline queue will already
be merged into their respective repositories and target branches.  The
change's project will have the change's branch checked out, as will
all of the other projects, if that branch exists (otherwise, a
fallback or default branch will be used).  If your job needs to
operate on multiple branches, simply checkout the appropriate branches
of these git repos to ensure that the job results reflect the proposed
future state that Zuul is testing, and all dependencies are present.

The git repositories will have a remote ``origin`` with refs pointing
to the previous speculative state, i.e., the state of the repository
before the speculative merge of any changes in the current queue item.
This means that e.g.  a ``git diff origin/<branch>..<branch>`` will
show the changes being tested. Note that the ``origin`` URL is set to
a bogus value (``file:///dev/null``) and can not be used for updating
the repository state; the local repositories are guaranteed to be up
to date.

The repositories will be placed on the filesystem in directories
corresponding with the canonical hostname of their source connection.
For example::

  work/src/git.example.com/project1
  work/src/github.com/project2

Is the layout that would be present for a job which included project1
from the connection associated to git.example.com and project2 from
GitHub.  This helps avoid collisions between projects with the same
name, and some language environments, such as Go, expect repositories
in this format.

Note that these git repositories are located on the executor; in order
to be useful to most kinds of jobs, they will need to be present on
the build nodes.  The ``base`` job in the standard library (see
`zuul-base-jobs documentation`_ for details) contains a
pre-playbook which copies the repositories to all of the job's nodes.
It is recommended to always inherit from this base job to ensure that
behavior.

.. _zuul-base-jobs documentation: https://zuul-ci.org/docs/zuul-base-jobs/jobs.html#job-base

.. TODO: document src (and logs?) directory

.. _user_jobs_variable_inheritance:

Variables
---------

There are several sources of variables which are available to Ansible:
variables defined in jobs, secrets, and site-wide variables.  The
order of precedence is:

#. :ref:`Site-wide variables <user_jobs_sitewide_variables>`
#. :ref:`Job extra variables <user_jobs_job_extra_variables>`
#. :ref:`Secrets <user_jobs_secrets>`
#. :ref:`Job variables <user_jobs_job_variables>`
#. :ref:`Parameters <user_jobs_parameters>`
#. :ref:`Project variables <user_jobs_project_variables>`
#. :ref:`File variables <user_jobs_file_variables>`
#. :ref:`Parent job results <user_jobs_parent_results>`

Meaning that a site-wide variable with the same name as any other will
override its value, and similarly, secrets override job variables of
the same name which override data returned from parent jobs.  Each of
the sources is described below.

.. _user_jobs_sitewide_variables:

Site-wide Variables
~~~~~~~~~~~~~~~~~~~

The Zuul administrator may define variables which will be available to
all jobs running in the system.  These are statically defined and may
not be altered by jobs.  See the :ref:`Administrator's Guide
<admin_sitewide_variables>` for information on how a site
administrator may define these variables.

.. _user_jobs_job_extra_variables:

Job Extra Variables
~~~~~~~~~~~~~~~~~~~

Any extra variables in the job definition (using the :attr:`job.extra-vars`
attribute) are available to Ansible but not added into the inventory file.

.. _user_jobs_secrets:

Secrets
~~~~~~~

:ref:`Secrets <secret>` also appear as variables available to Ansible.
Unlike job variables, these are not added to the inventory file (so
that the inventory file may be kept for debugging purposes without
revealing secrets).  But they are still available to Ansible as normal
variables.  Because secrets are groups of variables, they will appear
as a dictionary structure in templates, with the dictionary itself
being the name of the secret, and its members the individual items in
the secret.  For example, a secret defined as:

.. code-block:: yaml

  - secret:
      name: credentials
      data:
        username: foo
        password: bar

Might be used in a template as::

 {{ credentials.username }} {{ credentials.password }}

Secrets are only available to playbooks associated with the job
definition which uses the secret; they are not available to playbooks
associated with child jobs or job variants.

.. _user_jobs_job_variables:

Job Variables
~~~~~~~~~~~~~

Any variables specified in the job definition (using the
:attr:`job.vars` attribute) are available as Ansible host variables.
They are added to the ``vars`` section of the inventory file under the
``all`` hosts group, so they are available to all hosts.  Simply refer
to them by the name specified in the job's ``vars`` section.

.. _user_jobs_parameters:

Parameters
~~~~~~~~~~

Any parameters specified when the item was enqueued are available to
jobs as Ansible host variables in the same way as :ref:`job variables
<user_jobs_job_variables>`.

This is only supported for items manually enqueued using the web
interface or API, and only in pipelines that support parameters with
the :attr:`pipeline.allow-parameters` and
:attr:`project.<pipeline>.parameters` configuration attributes.

.. _user_jobs_project_variables:

Project Variables
~~~~~~~~~~~~~~~~~

Any variables specified in the project definition (using the
:attr:`project.vars` attribute) are available to jobs as Ansible host
variables in the same way as :ref:`job variables
<user_jobs_job_variables>`.  Variables set in a ``project-template``
are merged into the project variables when the template is included by
a project.

.. code-block:: yaml

  - project-template:
      name: sample-template
      description: Description
      vars:
        var_from_template: foo
      post:
        jobs:
          - template_job
      release:
        jobs:
          - template_job

  - project:
      name: Sample project
      description: Description
      templates:
        - sample-template
      vars:
        var_for_all_jobs: value
      check:
        jobs:
          - job1
          - job2:
              vars:
                var_for_all_jobs: override

.. _user_jobs_file_variables:

File Variables
~~~~~~~~~~~~~~

Any variables specified in files loaded from project repositories
(using the :attr:`job.include-vars` attribute) are available to jobs
as Ansible host variables in the same way as :ref:`job variables
<user_jobs_job_variables>`.

.. _user_jobs_parent_results:

Parent Job Results
~~~~~~~~~~~~~~~~~~

A job may return data to Zuul for later use by jobs which depend on
it.  For details, see :ref:`return_values`.

.. _user_jobs_zuul_variables:

Zuul Variables
--------------

Zuul supplies not only the variables specified by the job definition
to Ansible, but also some variables from Zuul itself.

When a pipeline is triggered by an action, it enqueues items which may
vary based on the pipeline's configuration.  For example, when a new
change is created, that change may be enqueued into the pipeline,
while a tag may be enqueued into the pipeline when it is pushed.

An item typically refers to a single git reference, but in the case of
a dependency cycle among changes, the item may be composed of multiple
changes.

Information about these items is available to jobs.  Since all of the
items enqueued in a pipeline represent one or more git references, the
different types of item share some attributes in common.  But other
attributes may vary based on the type of ref.  The different types of
ref are:

Change
   A change to the repository.  Most often, this will be a git
   reference which has not yet been merged into the repository (e.g.,
   a Gerrit change or a GitHub pull request).

Branch
   This represents a branch tip.  This item may have been enqueued
   because the branch was updated (via a change having merged, or a
   direct push).  Or it may have been enqueued by a timer for the
   purpose of verifying the current condition of the branch.

Tag
   This represents a git tag.  The item may have been enqueued because
   a tag was created or deleted.

Ref
   This represents a git reference that is neither a change, branch, or
   tag.

If a build is running for a queue item with a single ref, the values
below are straightforward.  Things are a little more complex if the
queue item represents multiple changes in a dependency cycle.  In that
case, the same job may be run multiple times, each for a different
change in the cycle.  If that happens, then many of the attributes
below (such as :var:`zuul.change` and :var:`zuul.project`, etc) will
refer to the particular change that is assigned to that build.
However, if a job is deduplicated, then one build is run for several
changes simultaneously.  In that case, one of the changes which
triggered the job will arbitrarily be selected for those values.  If
possible, Zuul will use the change that originally caused the item to
be enqueued, but that is not always possible, and that behavior should
not be relied upon.

Job Ref
~~~~~~~

The following variables related to the job's selected ref (as
described above) are available:

.. var:: zuul

   .. var:: project

      The job's project.  If the job is running for a single change,
      then this will be the project of that change.  In the case of a
      circular dependency queue item where this job is run more than
      once for different changes in the item, this will be set to the
      project of the particular change assigned to this build of the
      job.  If the job is deduplicated, then this is arbitrarily set
      to one of the changes in the queue item that triggered the job.

      This is a data structure with the following fields:

      .. var:: name

         The name of the project, excluding hostname.  E.g., `org/project`.

      .. var:: short_name

         The name of the project, excluding directories or
         organizations.  E.g., `project`.

      .. var:: canonical_hostname

         The canonical hostname where the project lives.  E.g.,
         `git.example.com`.

      .. var:: canonical_name

         The full canonical name of the project including hostname.
         E.g., `git.example.com/org/project`.

      .. var:: src_dir

         The path to the source code relative to the work dir.  E.g.,
         `src/git.example.com/org/project`.

   .. var:: branch

      This field is present for the following item types:

      Branch
         The item's branch (without the `refs/heads/` prefix).

      Change
         The target branch of the change (without the `refs/heads/`
         prefix).

   .. var:: change

      This field is present for the following item type:

      Change
         The identifier for the change.

   .. var:: message

      The commit or pull request message of the change base64 encoded. Use the
      `b64decode` filter in ansible when working with it.

      .. warning:: This variable is deprecated and will be removed in
                   a future version.  Use :var:`zuul.change_message`
                   instead.

   .. var:: change_message

      This field is present for the following item type:

      Change
         The commit or pull request message of the change.  When
         Zuul runs Ansible, this variable is tagged with the
         ``!unsafe`` YAML tag so that Ansible will not interpolate
         values into it.  Note, however, that the `inventory.yaml`
         file placed in the build's workspace for debugging and
         inspection purposes does not include the ``!unsafe`` tag.

   .. var:: change_url

      This field is present for the following item types:

      Change
         The URL to the source location of the given change.
         E.g., `https://review.example.org/#/c/123456/` or
         `https://github.com/example/example/pull/1234`.
      Branch
         The URL to the commit browser for the branch.
      Tag
         The URL to the commit browser for the tag.
      Ref
         The URL to the commit browser for the ref.

   .. var:: patchset

      This field is present for the following item types:

      Change
         The patchset identifier for the change.  If a change is
         revised, this will have a different value.

   .. var:: project

      The item's project.  This is a data structure with the
      following fields:

      .. var:: name

         The name of the project, excluding hostname.  E.g.,
         `org/project`.

      .. var:: short_name

         The name of the project, excluding directories or
         organizations.  E.g., `project`.

      .. var:: canonical_hostname

         The canonical hostname where the project lives.  E.g.,
         `git.example.com`.

      .. var:: canonical_name

         The full canonical name of the project including hostname.
         E.g., `git.example.com/org/project`.

      .. var:: src_dir

         The path to the source code on the remote host, relative
         to the home dir of the remote user.
         E.g., `src/git.example.com/org/project`.

   .. var:: oldrev

      This field is present for the following item types:

      Branch
         If the item was enqueued as the result of a change merging
         or being pushed to the branch, the git sha of the old
         revision will be included here.

      Tag
         If the item was enqueued as the result of a tag being
         deleted, the previous git sha of the tag will be included
         here.  If the tag was created, this variable will be
         undefined.

      Ref
         If the item was enqueued as the result of a ref being
         deleted, the previous git sha of the ref will be included
         here.  If the ref was created, this variable will be
         undefined.

   .. var:: newrev

      This field is present for the following item types:

      Branch
         If the item was enqueued as the result of a change merging or
         being pushed to the branch, the git sha of the new revision
         will be included here.  If the item was enqueued due to a
         timer and the ``dereference`` flag was set on the timer
         trigger, it will contain the git sha of the branch at the
         time it was enqueued.

      Tag
         If the item was enqueued as the result of a tag being
         created, the new git sha of the tag will be included here.
         If the tag was deleted, this variable will be undefined.

      Ref
         If the item was enqueued as the result of a ref being
         created, the new git sha of the ref will be included here.
         If the ref was deleted, this variable will be undefined.

   .. var:: commit_id

      This field is present for the following item types:

      Branch
         The git sha of the branch.  Identical to ``newrev`` or
         ``oldrev`` if defined.
      Tag
         The git sha of the tag.  Identical to ``newrev`` or
         ``oldrev`` if defined.
      Ref
         The git sha of the ref.  Identical to ``newrev`` or
         ``oldrev`` if defined.

   .. var:: tag

      This field is present for the following item types:

      Tag
         The name of the item's tag (without the `refs/tags/` prefix).

   .. var:: topic

      This field is present for the following item types:

      Change
         The topic of the change (if any).

   .. var:: ref

      The git ref of the item.  This will be the full path (e.g.,
      `refs/heads/master` or `refs/changes/...`).


Item
~~~~

The following variables related to the queue item are available:

.. var:: zuul

   .. var:: items
      :type: list

      .. note::

         ``zuul.items`` conflicts with the ``items()`` builtin so the
         variable can only be accessed with python dictionary like syntax,
         e.g: ``zuul['items']``

      A list of dictionaries, each representing a ref being tested
      with this change.

      .. var:: branch

         This field is present for the following item types:

         Branch
            The item's branch (without the `refs/heads/` prefix).

         Change
            The target branch of the change (without the `refs/heads/`
            prefix).

      .. var:: bundle_id

         This field is present for the following item type:

         Change
            The id of the bundle if the change is in a circular
            dependency cycle.

            Only available for items with more than one change.

         .. warning:: This variable is deprecated and will be removed in
                      a future version.  Use :var:`zuul.buildset_refs` to
                      identify if the item is for a dependency cycle and
                      the associated changes instead.

      .. var:: change

         This field is present for the following item type:

         Change
            The identifier for the change.

      .. var:: change_message

         This field is present for the following item type:

         Change
            The commit or pull request message of the change.  When
            Zuul runs Ansible, this variable is tagged with the
            ``!unsafe`` YAML tag so that Ansible will not interpolate
            values into it.  Note, however, that the `inventory.yaml`
            file placed in the build's workspace for debugging and
            inspection purposes does not include the ``!unsafe`` tag.

      .. var:: change_url

         This field is present for the following item types:

         Change
            The URL to the source location of the given change.
            E.g., `https://review.example.org/#/c/123456/` or
            `https://github.com/example/example/pull/1234`.
         Branch
            The URL to the commit browser for the branch.
         Tag
            The URL to the commit browser for the tag.
         Ref
            The URL to the commit browser for the ref.

      .. var:: patchset

         This field is present for the following item types:

         Change
            The patchset identifier for the change.  If a change is
            revised, this will have a different value.

      .. var:: project

         The item's project.  This is a data structure with the
         following fields:

         .. var:: name

            The name of the project, excluding hostname.  E.g.,
            `org/project`.

         .. var:: short_name

            The name of the project, excluding directories or
            organizations.  E.g., `project`.

         .. var:: canonical_hostname

            The canonical hostname where the project lives.  E.g.,
            `git.example.com`.

         .. var:: canonical_name

            The full canonical name of the project including hostname.
            E.g., `git.example.com/org/project`.

         .. var:: src_dir

            The path to the source code on the remote host, relative
            to the home dir of the remote user.
            E.g., `src/git.example.com/org/project`.

      .. var:: oldrev

         This field is present for the following item types:

         Branch
            If the item was enqueued as the result of a change merging
            or being pushed to the branch, the git sha of the old
            revision will be included here.

         Tag
            If the item was enqueued as the result of a tag being
            deleted, the previous git sha of the tag will be included
            here.  If the tag was created, this variable will be
            undefined.

         Ref
            If the item was enqueued as the result of a ref being
            deleted, the previous git sha of the ref will be included
            here.  If the ref was created, this variable will be
            undefined.

      .. var:: newrev

         This field is present for the following item types:

         Branch
            If the item was enqueued as the result of a change merging
            or being pushed to the branch, the git sha of the new
            revision will be included here.

         Tag
            If the item was enqueued as the result of a tag being
            created, the new git sha of the tag will be included here.
            If the tag was deleted, this variable will be undefined.

         Ref
            If the item was enqueued as the result of a ref being
            created, the new git sha of the ref will be included here.
            If the ref was deleted, this variable will be undefined.

      .. var:: commit_id

         This field is present for the following item types:

         Branch
            The git sha of the branch.  Identical to ``newrev`` or
            ``oldrev`` if defined.
         Tag
            The git sha of the tag.  Identical to ``newrev`` or
            ``oldrev`` if defined.
         Ref
            The git sha of the ref.  Identical to ``newrev`` or
            ``oldrev`` if defined.

      .. var:: tag

         This field is present for the following item types:

         Tag
            The name of the item's tag (without the `refs/tags/` prefix).

      .. var:: topic

         This field is present for the following item types:

         Change
            The topic of the change (if any).

   .. var:: build_refs
      :type: list

      A list of dictionaries, each representing a ref associated with
      this build.  Normally there is only one item in this list, but
      if the queue item is a dependency cycle, more than one item in
      the cycle requested the job be run, and the job has been
      deduplicated, then each item for which this build is being run
      will be present.  It is possible for a job to be deduplicated
      against all items in the cycle, only some of them, or none.  If
      deduplication happens for some or none, then multiple builds of
      the job will be run, and this variable will indicate for which
      of those items this particular build applies.

      .. var:: branch

         This field is present for the following item types:

         Branch
            The item's branch (without the `refs/heads/` prefix).

         Change
            The target branch of the change (without the `refs/heads/`
            prefix).

      .. var:: change

         This field is present for the following item type:

         Change
            The identifier for the change.

      .. var:: change_message

         This field is present for the following item type:

         Change
            The commit or pull request message of the change.  When
            Zuul runs Ansible, this variable is tagged with the
            ``!unsafe`` YAML tag so that Ansible will not interpolate
            values into it.  Note, however, that the `inventory.yaml`
            file placed in the build's workspace for debugging and
            inspection purposes does not include the ``!unsafe`` tag.

      .. var:: change_url

         This field is present for the following item types:

         Change
            The URL to the source location of the given change.
            E.g., `https://review.example.org/#/c/123456/` or
            `https://github.com/example/example/pull/1234`.
         Branch
            The URL to the commit browser for the branch.
         Tag
            The URL to the commit browser for the tag.
         Ref
            The URL to the commit browser for the ref.

      .. var:: patchset

         This field is present for the following item types:

         Change
            The patchset identifier for the change.  If a change is
            revised, this will have a different value.

      .. var:: project

         The item's project.  This is a data structure with the
         following fields:

         .. var:: name

            The name of the project, excluding hostname.  E.g.,
            `org/project`.

         .. var:: short_name

            The name of the project, excluding directories or
            organizations.  E.g., `project`.

         .. var:: canonical_hostname

            The canonical hostname where the project lives.  E.g.,
            `git.example.com`.

         .. var:: canonical_name

            The full canonical name of the project including hostname.
            E.g., `git.example.com/org/project`.

         .. var:: src_dir

            The path to the source code on the remote host, relative
            to the home dir of the remote user.
            E.g., `src/git.example.com/org/project`.

      .. var:: oldrev

         This field is present for the following item types:

         Branch
            If the item was enqueued as the result of a change merging
            or being pushed to the branch, the git sha of the old
            revision will be included here.

         Tag
            If the item was enqueued as the result of a tag being
            deleted, the previous git sha of the tag will be included
            here.  If the tag was created, this variable will be
            undefined.

         Ref
            If the item was enqueued as the result of a ref being
            deleted, the previous git sha of the ref will be included
            here.  If the ref was created, this variable will be
            undefined.

      .. var:: newrev

         This field is present for the following item types:

         Branch
            If the item was enqueued as the result of a change merging
            or being pushed to the branch, the git sha of the new
            revision will be included here.

         Tag
            If the item was enqueued as the result of a tag being
            created, the new git sha of the tag will be included here.
            If the tag was deleted, this variable will be undefined.

         Ref
            If the item was enqueued as the result of a ref being
            created, the new git sha of the ref will be included here.
            If the ref was deleted, this variable will be undefined.

      .. var:: commit_id

         This field is present for the following item types:

         Branch
            The git sha of the branch.  Identical to ``newrev`` or
            ``oldrev`` if defined.
         Tag
            The git sha of the tag.  Identical to ``newrev`` or
            ``oldrev`` if defined.
         Ref
            The git sha of the ref.  Identical to ``newrev`` or
            ``oldrev`` if defined.

      .. var:: tag

         This field is present for the following item types:

         Tag
            The name of the item's tag (without the `refs/tags/` prefix).

      .. var:: topic

         This field is present for the following item types:

         Change
            The topic of the change (if any).

      .. var:: merge_commit_id

         This field is present only for reporter jobs run for Changes
         when those changes are being merged.  This may not be
         available for all sources (currently supported for Gerrit,
         GitHub, and GitLab).

         The git sha of the target branch after the change was merged.

   .. var:: buildset_refs
      :type: list

      A list of dictionaries, each representing a ref associated with
      this queue item.  Normally there is only one item in this list,
      but if the queue item is a dependency cycle, each change in the
      cycle will be present.

      .. var:: branch

         This field is present for the following item types:

         Branch
            The item's branch (without the `refs/heads/` prefix).

         Change
            The target branch of the change (without the `refs/heads/`
            prefix).

      .. var:: change

         This field is present for the following item type:

         Change
            The identifier for the change.

      .. var:: change_message

         This field is present for the following item type:

         Change
            The commit or pull request message of the change.  When
            Zuul runs Ansible, this variable is tagged with the
            ``!unsafe`` YAML tag so that Ansible will not interpolate
            values into it.  Note, however, that the `inventory.yaml`
            file placed in the build's workspace for debugging and
            inspection purposes does not include the ``!unsafe`` tag.

      .. var:: change_url

         This field is present for the following item types:

         Change
            The URL to the source location of the given change.
            E.g., `https://review.example.org/#/c/123456/` or
            `https://github.com/example/example/pull/1234`.
         Branch
            The URL to the commit browser for the branch.
         Tag
            The URL to the commit browser for the tag.
         Ref
            The URL to the commit browser for the ref.

      .. var:: patchset

         This field is present for the following item types:

         Change
            The patchset identifier for the change.  If a change is
            revised, this will have a different value.

      .. var:: project

         The item's project.  This is a data structure with the
         following fields:

         .. var:: name

            The name of the project, excluding hostname.  E.g.,
            `org/project`.

         .. var:: short_name

            The name of the project, excluding directories or
            organizations.  E.g., `project`.

         .. var:: canonical_hostname

            The canonical hostname where the project lives.  E.g.,
            `git.example.com`.

         .. var:: canonical_name

            The full canonical name of the project including hostname.
            E.g., `git.example.com/org/project`.

         .. var:: src_dir

            The path to the source code on the remote host, relative
            to the home dir of the remote user.
            E.g., `src/git.example.com/org/project`.

      .. var:: oldrev

         This field is present for the following item types:

         Branch
            If the item was enqueued as the result of a change merging
            or being pushed to the branch, the git sha of the old
            revision will be included here.

         Tag
            If the item was enqueued as the result of a tag being
            deleted, the previous git sha of the tag will be included
            here.  If the tag was created, this variable will be
            undefined.

         Ref
            If the item was enqueued as the result of a ref being
            deleted, the previous git sha of the ref will be included
            here.  If the ref was created, this variable will be
            undefined.

      .. var:: newrev

         This field is present for the following item types:

         Branch
            If the item was enqueued as the result of a change merging
            or being pushed to the branch, the git sha of the new
            revision will be included here.

         Tag
            If the item was enqueued as the result of a tag being
            created, the new git sha of the tag will be included here.
            If the tag was deleted, this variable will be undefined.

         Ref
            If the item was enqueued as the result of a ref being
            created, the new git sha of the ref will be included here.
            If the ref was deleted, this variable will be undefined.

      .. var:: commit_id

         This field is present for the following item types:

         Branch
            The git sha of the branch.  Identical to ``newrev`` or
            ``oldrev`` if defined.
         Tag
            The git sha of the tag.  Identical to ``newrev`` or
            ``oldrev`` if defined.
         Ref
            The git sha of the ref.  Identical to ``newrev`` or
            ``oldrev`` if defined.

      .. var:: tag

         This field is present for the following item types:

         Tag
            The name of the item's tag (without the `refs/tags/` prefix).

      .. var:: topic

         This field is present for the following item types:

         Change
            The topic of the change (if any).

      .. var:: merge_commit_id

         This field is present only for reporter jobs run for Changes
         when those changes are being merged.  This may not be
         available for all sources (currently supported for Gerrit,
         GitHub, and GitLab).

         The git sha of the target branch after the change was merged.

Job
~~~

The following variables related to the job are available:

.. var:: zuul

   .. var:: artifacts
      :type: list

      If the job has a :attr:`job.requires` attribute, and Zuul has
      found changes ahead of this change in the pipeline with matching
      :attr:`job.provides` attributes, then information about any
      :ref:`artifacts returned <return_artifacts>` from those jobs
      will appear here.

      This value is a list of dictionaries with the following format:

      .. var:: project

         The name of the project which supplied this artifact.

      .. var:: change

         The change number which supplied this artifact.

      .. var:: patchset

         The patchset of the change.

      .. var:: job

         The name of the job which produced the artifact.

      .. var:: name

         The name of the artifact (as supplied to :ref:`return_artifacts`).

      .. var:: url

         The URL of the artifact (as supplied to :ref:`return_artifacts`).

      .. var:: metadata

         The metadata of the artifact (as supplied to :ref:`return_artifacts`).

   .. var:: build

      The UUID of the build.  A build is a single execution of a job.
      When an item is enqueued into a pipeline, this usually results
      in one build of each job triggered by that item.  However, items
      may be re-enqueued in which case another build may run.  In
      dependent pipelines, the same job may run multiple times for the
      same item as circumstances change ahead in the queue.  Each time
      a job is run, for whatever reason, it is accompanied with a new
      unique id.

   .. var:: buildset

      The buildset UUID.  When Zuul runs jobs for an item, the
      collection of those jobs is known as a buildset.  If the
      configuration of items ahead in a dependent pipeline changes,
      Zuul creates a new buildset and restarts all of the jobs.

   .. var:: child_jobs

      A list of the first level dependent jobs to be run after this job
      has finished successfully.

   .. var:: override_checkout

      If the job was configured to override the branch or tag checked
      out, this will contain the specified value.  Otherwise, this
      variable will be undefined.

   .. var:: pipeline

      The name of the pipeline in which the job is being run.

   .. var:: post_review
      :type: bool

      Whether the current job is running in a post-review pipeline or not.

   .. var:: job

      The name of the job being run.

   .. var:: event_id

      The UUID of the event that triggered this execution. This is mainly
      useful for debugging purposes.

   .. var:: voting

      A boolean indicating whether the job is voting.

   .. var:: attempts

      An integer count of how many attempts have been made to run this
      job for the current buildset. If there are pre-run failures or network
      connectivity issues then previous attempts may have been cancelled,
      and this value will be greater than 1.

   .. var:: max_attempts

      The number of attempts that will be be made for this job when
      encountering an error in a pre-playbook before it is reported as failed.
      This value is taken from :attr:`job.attempts`.

   .. var:: ansible_version

      The version of the Ansible community package release used for executing
      the job.

   .. var:: projects
      :type: dict

      A dictionary of all projects prepared by Zuul for the item.  It
      includes, at least, the item's own projects.  It also includes
      the projects of any items this item depends on, as well as the
      projects that appear in :attr:`job.required-projects`.

      This is a dictionary of dictionaries.  Each value has a key of
      the `canonical_name`, then each entry consists of:

      .. var:: name

         The name of the project, excluding hostname.  E.g., `org/project`.

      .. var:: short_name

         The name of the project, excluding directories or
         organizations.  E.g., `project`.

      .. var:: canonical_hostname

         The canonical hostname where the project lives.  E.g.,
         `git.example.com`.

      .. var:: canonical_name

         The full canonical name of the project including hostname.
         E.g., `git.example.com/org/project`.

      .. var:: src_dir

         The path to the source code, relative to the work dir.  E.g.,
         `src/git.example.com/org/project`.

      .. var:: required

         A boolean indicating whether this project appears in the
         :attr:`job.required-projects` list for this job.

      .. var:: checkout

         The branch or tag that Zuul checked out for this project.
         This may be influenced by the branch or tag associated with
         the item as well as the job configuration.

      .. var:: checkout_description

         A human-readable description of why Zuul chose this
         particular branch or tag to be checked out.  This is intended
         as a debugging aid in the case of complex jobs.  The specific
         text is not defined and is subject to change.

      .. var:: commit

         The hex SHA of the commit checked out.  This commit may
         appear in the upstream repository, or if it the result of a
         speculative merge, it may only exist during the run of this
         job.

      For example, to access the source directory of a single known
      project, you might use::

        {{ zuul.projects['git.example.com/org/project'].src_dir }}

      To iterate over the project list, you might write a task
      something like::

        - name: Sample project iteration
          debug:
            msg: "Project {{ item.name }} is at {{ item.src_dir }}
          with_items: {{ zuul.projects.values() | list }}

   .. var:: playbook_context

      This dictionary contains information about the execution of each
      playbook in the job.  This may be useful for understanding
      exactly what playbooks and roles Zuul executed.

      All paths herein are located under the root of the build
      directory (note that is one level higher than the workspace
      directory accessible to jobs on the executor).

      .. var:: playbook_projects
         :type: dict

         A dictionary of projects that have been checked out for
         playbook execution.  When used in the trusted execution
         context, these will contain only merged commits in upstream
         repositories.  In the case of the untrusted context, they may
         contain speculatively merged code.

         The key is the path and each value is another dictionary with
         the following keys:

         .. var:: canonical_name

            The canonical name of the repository.

         .. var:: checkout

            The branch or tag checked out.

         .. var:: commit

            The hex SHA of the commit checked out.  As above, this
            commit may or may not exist in the upstream repository
            depending on whether it was the result of a speculative
            merge.


      .. var:: pre_playbooks
         :type: list

         See :var:`zuul.playbook_context.playbooks`

      .. var:: playbooks
         :type: list

         An ordered list of pre, run or post playbooks executed for
         the job.  Each item is a dictionary with the following keys:

         .. var:: path

            The path to the playbook.

         .. var:: roles
            :type: list

            Information about the roles available to the playbook.
            The actual `role path` supplied to Ansible is the
            concatenation of the ``role_path`` entry in each of the
            following dictionaries.  The rest of the information
            describes what is in the role path.

            In order to deal with the many possible role layouts and
            aliases, each element in the role path gets its own
            directory.  Depending on the contents and alias
            configuration for that role repo, a symlink is added to
            one of the repo checkouts in
            :var:`zuul.playbook_context.playbook_projects` so that the
            role may be supplied to Ansible with the correct name.

            .. var:: checkout

               The branch or tag checked out.

            .. var:: checkout_description

               A human-readable description of why Zuul chose this
               particular branch or tag to be checked out.  This is
               intended as a debugging aid in the case of complex
               jobs.  The specific text is not defined and is subject
               to change.

            .. var:: link_name

               The name of the symbolic link.

            .. var:: link_target

               The target of the symbolic_link.

            .. var:: role_path

               The role path passed to Ansible.

      .. var:: post_playbooks
         :type: list

         See :var:`zuul.playbook_context.playbooks`

   .. var:: tenant

      The name of the current Zuul tenant.

   .. var:: pre_timeout

      The pre-run playbook timeout, in seconds.

   .. var:: timeout

      The job timeout, in seconds.

   .. var:: post_timeout

      The post-run playbook timeout, in seconds.

   .. var:: jobtags

      A list of tags associated with the job.  Not to be confused with
      git tags, these are simply free-form text fields that can be
      used by the job for reporting or classification purposes.

   .. var:: resources

      A job using a container build resources has access to a
      resources variable that describes the resource. Resources is
      a dictionary of group keys, each value consists of:

     .. var:: namespace

         The resource's namespace name.

     .. var:: context

         The kube config context name.

     .. var:: pod

         The name of the pod when the label defines a kubectl connection.

     Project or namespace resources might be used in a template as:

     .. code-block:: yaml

         - hosts: localhost
             tasks:
             - name: Create a k8s resource
               k8s_raw:
                 state: present
                 context: "{{ zuul.resources['node-name'].context }}"
                 namespace: "{{ zuul.resources['node-name'].namespace }}"

     Kubectl resources might be used in a template as:

     .. code-block:: yaml

         - hosts: localhost
             tasks:
             - name: Copy src repos to the pod
               command: >
                 oc rsync -q --progress=false
                     {{ zuul.executor.src_root }}/
                     {{ zuul.resources['node-name'].pod }}:src/
                 no_log: true

Working Directory
~~~~~~~~~~~~~~~~~

Additionally, some information about the working directory and the
executor running the job is available:

.. var:: zuul

   .. var:: executor

      A number of values related to the executor running the job are
      available:

      .. var:: hostname

         The hostname of the executor.

      .. var:: src_root

         The path to the source directory.

      .. var:: log_root

         The path to the logs directory.

      .. var:: work_root

         The path to the working directory.

      .. var:: inventory_file

         The path to the inventory. This variable is needed for jobs running
         without a nodeset since Ansible doesn't set it for localhost; see
         this `porting guide
         <https://docs.ansible.com/ansible/latest/porting_guides/porting_guide_2.4.html#inventory>`_.

         The inventory file is only readable by jobs running in a
         :term:`trusted execution context`.

.. var:: zuul_success

   Post run playbook(s) will be passed this variable to indicate if the run
   phase of the job was successful or not. This variable is meant to be used
   with the `bool` filter.

   .. code-block:: yaml

     tasks:
       - shell: echo example
         when: zuul_success | bool

.. var:: zuul_will_retry

   Post run and cleanup playbook(s) will be passed this variable to indicate
   if the job will be retried. This variable is meant to be used with the
   `bool` filter.

   .. code-block:: yaml

     tasks:
       - shell: echo example
         when: zuul_will_retry | bool

.. var:: zuul_node

   Information about each host is supplied in the `zuul_node` host variable.
   Availability of values varies based on the node and the driver that supplied
   it. Values may be ``null`` if they are not applicable. The `zuul_node`
   variable is a superset of the `nodepool` variable, providing additional
   information which is not available for Nodepool.

   .. var:: uuid

      The uuid of this node.

   .. var:: label

      The label of this node.

   .. var:: az

      The availability zone in which this node was placed.

   .. var:: cloud

      The name of the cloud in which this node was created.

   .. var:: provider

      The name of the provider of this node.

   .. var:: region

      The name of the provider's region.

   .. var:: host_id

      The cloud's host identification for this node's hypervisor.

   .. var:: external_id

      The cloud's identifier for this node (only available for nodes
      provided by Nodepool).

   .. var:: slot

      If the node supports running multiple jobs on the node, a unique
      numeric ID for the subdivision of the node assigned to this job.
      This may be used to avoid build directory collisions.

   .. var:: interface_ip

      The best IP address to use to contact the node as determined by
      the cloud provider.

   .. var:: public_ipv4

      A public IPv4 address of the node.

   .. var:: private_ipv4

      A private IPv4 address of the node.

   .. var:: public_ipv6

      A public IPv6 address of the node.

   .. var:: private_ipv6

      A private IPv6 address of the node.

   .. var:: reuse

      A flag indicating if the node is reusable.

.. _node-properties:

   .. var:: node_properties

      Arbitrary driver-specific data. More information can be found in the
      driver's documentation, e.g. the AWS driver's
      :ref:`aws-driver-node-properties`.

.. var:: zuul_user_dir

   This variable is equivalent to ``{{ ansible_user_dir }}`` on remote
   nodes, but is set to ``{{ zuul.executor.work_root }}`` on the
   executor (``localhost``).  It is recommended to use this variable
   rather than `ansible_user_dir` in roles that are designed to work
   either locally on the executor or on remote nodes.

   .. note::

      A future version of Zuul will separate the home directory and
      work directory on the executor, so that roles that currently use
      `ansible_user_dir` in both places will no longer work.

.. var:: nodepool

   Information about each host from Nodepool is supplied in the
   `nodepool` host variable.  Availability of values varies based on
   the node and the driver that supplied it.  Values may be ``null``
   if they are not applicable.

   .. note::

      This variable is deprecated and may be removed in the future. Use the
      :var:`zuul_node` host variable instead.

   .. var:: label

      The nodepool label of this node.

   .. var:: az

      The availability zone in which this node was placed.

   .. var:: cloud

      The name of the cloud in which this node was created.

   .. var:: provider

      The name of the nodepool provider of this node.

   .. var:: region

      The name of the nodepool provider's region.

   .. var:: host_id

      The cloud's host identification for this node's hypervisor.

   .. var:: external_id

      The cloud's identifier for this node.

   .. var:: slot

      If the node supports running multiple jobs on the node, a unique
      numeric ID for the subdivision of the node assigned to this job.
      This may be used to avoid build directory collisions.

   .. var:: interface_ip

      The best IP address to use to contact the node as determined by
      the cloud provider and nodepool.

   .. var:: public_ipv4

      A public IPv4 address of the node.

   .. var:: private_ipv4

      A private IPv4 address of the node.

   .. var:: public_ipv6

      A public IPv6 address of the node.

   .. var:: private_ipv6

      A private IPv6 address of the node.

   .. var:: node_properties

      Arbitrary mapping of node properties, such as boolean flags representing
      criteria that were taken into account when a node is allocated to Zuul by
      Nodepool. Notable properties are `spot` for when a node is an AWS spot
      instance or `fleet` when the node was created by Nodepool using the AWS
      fleet API.


SSH Keys
--------

Zuul starts each job with an SSH agent running and at least one key
added to that agent.  Generally you won't need to be aware of this
since Ansible will use this when performing any tasks on remote nodes.
However, under some circumstances you may want to interact with the
agent.  For example, you may wish to add a key provided as a secret to
the job in order to access a specific host, or you may want to, in a
pre-playbook, replace the key used to log into the assigned nodes in
order to further protect it from being abused by untrusted job
content.

A description of each of the keys added to the SSH agent follows.

Nodepool Key
~~~~~~~~~~~~

This key is supplied by the system administrator.  It is expected to
be accepted by every node supplied by Nodepool and is generally the
key that will be used by Zuul when running jobs.  Because of the
potential for an unrelated job to add an arbitrary host to the Ansible
inventory which might accept this key (e.g., a node for another job,
or a static host), the use of the `add-build-sshkey
<https://zuul-ci.org/docs/zuul-jobs/general-roles.html#role-add-build-sshkey>`_
role is recommended.

.. _tenant-key:

Tenant Key
~~~~~~~~~~

Each tenant in Zuul has its own SSH keypair.  This key is added to the
SSH agent for all jobs running in that tenant.  Note this differs from
the project key in that it is available for all pipelines.  This may
be useful to restrict access to certain nodes to certain tenants.  The
systems may be added to the inventory using the ``add_host`` Ansible
module, or they may be supplied by static nodes in Nodepool.

Zuul serves each tenant's public SSH key using its build-in webserver.
They can be fetched at the path
``/api/tenant/<tenant>/tenant-ssh-key/ssh.pub`` where ``<tenant>`` is
the name of the tenant.

Project Key
~~~~~~~~~~~

Each project in Zuul has its own SSH keypair.  This key is added to
the SSH agent for all jobs running in a post-review pipeline.  If a
system administrator trusts that project, they can add the project's
public key to systems to allow post-review jobs to access those
systems.  The systems may be added to the inventory using the
``add_host`` Ansible module, or they may be supplied by static nodes
in Nodepool.

Zuul serves each project's public SSH key using its build-in
webserver.  They can be fetched at the path
``/api/tenant/<tenant>/project-ssh-key/<project>.pub`` where
``<project>`` is the canonical name of a project and ``<tenant>`` is
the name of a tenant with that project.

.. _return_values:

Return Values
-------------

A job may return some values to Zuul to affect its behavior and for
use by dependent jobs.  To return a value, use the ``zuul_return``
Ansible module in a job playbook.
For example:

.. code-block:: yaml

  tasks:
    - zuul_return:
        data:
          foo: bar

Will return the dictionary ``{'foo': 'bar'}`` to Zuul.

Optionally, if you have a large supply of data to return, you may specify the
path to a JSON-formatted file with that data. For example:

.. code-block:: yaml

  tasks:
    - zuul_return:
        file: /path/to/data.json

Normally returned data are provided to dependent jobs in the inventory
file, which may end up in the log archive of a job.  In the case where
sensitive data must be provided to dependent jobs, the ``secret_data``
attribute may be used instead, and the data will be provided via the
same mechanism as job secrets, where the data are not written to disk
in the work directory.  Care must still be taken to avoid displaying
or storing sensitive data within the job.  For example:

.. code-block:: yaml

  tasks:
    - zuul_return:
        secret_data:
          password: foobar

.. TODO: xref to section describing formatting

Any values other than those in the ``zuul`` hierarchy will be supplied
as Ansible variables to dependent jobs.  These variables have less
precedence than any other type of variable in Zuul, so be sure their
names are not shared by any job variables.  If more than one parent
job returns the same variable, the value from the later job in the job
graph will take precedence.

The values in the ``zuul`` hierarchy are special variables that influence the
behavior of zuul itself. The following paragraphs describe the currently
supported special variables and their meaning.

Returning the log url
~~~~~~~~~~~~~~~~~~~~~

To set the log URL for a build, use *zuul_return* to set the
**zuul.log_url** value.  For example:

.. code-block:: yaml

  tasks:
    - zuul_return:
        data:
          zuul:
            log_url: http://logs.example.com/path/to/build/logs

.. _return_artifacts:

Returning artifact URLs
~~~~~~~~~~~~~~~~~~~~~~~

If a build produces artifacts, any number of URLs may be returned to
Zuul and stored in the SQL database.  These will then be available via
the web interface and subsequent jobs.

To provide artifact URLs for a build, use *zuul_return* to set keys
under the :var:`zuul.artifacts` dictionary.  For example:

.. code-block:: yaml

  tasks:
    - zuul_return:
        data:
          zuul:
            artifacts:
              - name: tarball
                url: http://example.com/path/to/package.tar.gz
                metadata:
                  version: 3.0
              - name: docs
                url: build/docs/

If the value of **url** is a relative URL, it will be combined with
the **zuul.log_url** value if set to create an absolute URL.  The
**metadata** key is optional; if it is provided, it must be a
dictionary; its keys and values may be anything.

If *zuul_return* is invoked multiple times (e.g., via multiple
playbooks), then the elements of :var:`zuul.artifacts` from each
invocation will be appended.

.. _skipping child jobs:

Skipping dependent jobs
~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   In the following section the use of 'child jobs' refers to dependent jobs
   configured by `job.dependencies` and should not be confused with jobs
   that inherit from a parent job.

To skip a dependent job for the current build, use *zuul_return* to set the
:var:`zuul.child_jobs` value. For example:

.. code-block:: yaml

  tasks:
    - zuul_return:
        data:
          zuul:
            child_jobs:
              - dependent_jobA
              - dependent_jobC

Will tell zuul to only run the dependent_jobA and dependent_jobC for pre-configured
dependent jobs. If dependent_jobB was configured, it would be now marked as SKIPPED. If
zuul.child_jobs is empty, all jobs will be marked as SKIPPED. Invalid dependent jobs
are stripped and ignored, if only invalid jobs are listed it is the same as
providing an empty list to zuul.child_jobs.

Leaving warnings
~~~~~~~~~~~~~~~~

A job can leave warnings that will be appended to the comment zuul leaves on
the change. Use *zuul_return* to add a list of warnings. For example:

.. code-block:: yaml

  tasks:
    - zuul_return:
        data:
          zuul:
            warnings:
              - This warning will be posted on the change.

If *zuul_return* is invoked multiple times (e.g., via multiple
playbooks), then the elements of **zuul.warnings** from each
invocation will be appended.

Leaving file comments
~~~~~~~~~~~~~~~~~~~~~

To instruct the reporters to leave line comments on files in the
change, set the **zuul.file_comments** value.  For example:

.. code-block:: yaml

  tasks:
    - zuul_return:
        data:
          zuul:
            file_comments:
              path/to/file.py:
                - line: 42
                  message: "Line too long"
                  level: info
                - line: 82
                  message: "Line too short"
                - line: 119
                  message: "This block is indented too far."
                  level: warning
                  range:
                    start_line: 117
                    start_character:    0
                    end_line:   119
                    end_character:  37

Not all reporters currently support line comments (or all of the
features of line comments); in these cases, reporters will simply
ignore this data. The ``level`` is optional, but if provided must
be one of ``info``, ``warning``, ``error``.

Zuul will attempt to automatically translate the supplied line numbers
to the corresponding lines in the original change as written (they may
differ due to other changes which may have merged since the change was
written).  If this produces erroneous results for a job, the behavior
may be disabled by setting the
**zuul.disable_file_comment_line_mapping** variable to ``true`` in
*zuul_return*.

If *zuul_return* is invoked multiple times (e.g., via multiple playbooks), then
the elements of `zuul.file_comments` from each invocation will be appended.

Pausing the job
~~~~~~~~~~~~~~~

A job can be paused after the run phase by notifying zuul during the run phase.
In this case the dependent jobs can start and the prior job stays paused until
all dependent jobs are finished. This for example can be useful to start
a docker registry in a job that will be used by the dependent job.
To indicate that the job should be paused use *zuul_return* to
set the **zuul.pause** value.
You still can at the same time supply any arbitrary data to the dependent jobs.
For example:

.. code-block:: yaml

  tasks:
    - zuul_return:
        data:
          zuul:
            pause: true
          registry_ip_address: "{{ hostvars[groups.all[0]].ansible_host }}"

Skipping retries
~~~~~~~~~~~~~~~~

It's possible to skip the retry caused by a failure in ``pre-run``
by setting **zuul.retry** to ``false``.

For example the following would skip retrying the build:

.. code-block:: yaml

  tasks:
    - zuul_return:
        data:
          zuul:
            retry: false

.. _build_status:

Ansible Groups
--------------

Ansible host groups may be configured via the job's :attr:`nodeset`.
In addition to these, Zuul automatically creates a group named
`zuul_unreachable`.  It is always present, and is empty when the job
starts.  If any playbook encounters an unreachable host, that host is
added to the group for all subsequent playbooks.  This can be used to
avoid executing certain post-run playbook steps on hosts that are
already known to be unreachable.  For example, to avoid copying logs
from a remote host, a play might look something like:

.. code-block:: yaml

   - hosts: all:!zuul_unreachable
     gather_facts: no
     tasks:
       - name: Copy logs
         ...

The group name `zuul_unreachable` is reserved by zuul and will
automatically override any similarly named group defined by the
nodeset.

Build Status
------------

A job build may have the following status:

**SUCCESS**
  Nominal job execution.

**FAILURE**
  Job executed correctly, but exited with a failure.

**RETRY**
  The ``pre-run`` playbook failed or the node became unreachable
  and the job will be retried.

**RETRY_LIMIT**
  The ``pre-run`` playbook failed or the node became unreachable
  more than the maximum number of retry ``attempts``.

**POST_FAILURE**
  The ``post-run`` playbook failed.

**SKIPPED**
  One of the build dependencies failed and this job was not executed.

**NODE_FAILURE**
  The :ref:`provider` was unable to fulfill the nodeset request.  This
  can happen if Zuul (or Nodepool) is unable to provide the requested
  node(s) for the request.
