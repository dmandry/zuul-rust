:title: Tenant Configuration

.. _tenant-config:

Tenant Configuration
====================

After ``zuul.conf`` is configured, Zuul component servers will be able
to start, but a tenant configuration is required in order for Zuul to
perform any actions.  The tenant configuration file specifies upon
which projects Zuul should operate.  These repositories are grouped
into tenants.  The configuration of each tenant is separate from the
rest (no pipelines, jobs, etc are shared between them).

A project may appear in more than one tenant; this may be useful if
you wish to use common job definitions across multiple tenants.

Actions normally available to the Zuul operator only can be performed by specific
users on Zuul's REST API if admin rules are listed for the tenant. Authorization rules
are also defined in the tenant configuration file.

The tenant configuration file is specified by the
:attr:`scheduler.tenant_config` setting in ``zuul.conf``.  It is a
YAML file which, like other Zuul configuration files, is a list of
configuration objects, though only a few types of objects (described
below) are supported.

Alternatively the :attr:`scheduler.tenant_config_script`
can be the path to an executable that will be executed and its stdout
used as the tenant configuration. The executable must return a valid
tenant YAML formatted output.

Tenant configuration is checked for updates any time a scheduler is
started, and changes to it are read automatically. If the tenant
configuration is altered during operation, you can signal a scheduler
to read and apply the updated state in order to avoid restarting. See
the section on :ref:`reconfiguration` for instructions. Ideally,
tenant configuration deployment via configuration management should
also be made to trigger a smart-reconfigure once the file is replaced.

Tenant
------

A tenant is a collection of projects which share a Zuul
configuration. Some examples of tenant definitions are:

.. code-block:: yaml

   - tenant:
       name: my-tenant
       max-nodes-per-job: 5
       exclude-unprotected-branches: false
       source:
         gerrit:
           config-projects:
             - common-config
             - shared-jobs:
                 include: job
           untrusted-projects:
             - zuul/zuul-jobs:
                 shadow: common-config
             - project1
             - project2:
                 exclude-unprotected-branches: true

.. code-block:: yaml

   - tenant:
       name: my-tenant
       admin-rules:
         - acl1
         - acl2
       source:
         gerrit:
           config-projects:
             - common-config
           untrusted-projects:
             - exclude:
                 - job
                 - semaphore
                 - project
                 - project-template
                 - nodeset
                 - secret
               projects:
                 - project1
                 - project2:
                     exclude-unprotected-branches: true

.. attr:: tenant

   The following attributes are supported:

   .. attr:: name
      :required:

      The name of the tenant.  This may appear in URLs, paths, and
      monitoring fields, and so should be restricted to URL friendly
      characters (ASCII letters, numbers, hyphen and underscore) and
      you should avoid changing it unless necessary.

   .. attr:: source
      :required:

      A dictionary of sources to consult for projects.  A tenant may
      contain projects from multiple sources; each of those sources
      must be listed here, along with the projects it supports.  The
      name of a :ref:`connection<connections>` is used as the
      dictionary key (e.g. ``gerrit`` in the example above), and the
      value is a further dictionary containing the keys below.

   The next two attributes, **config-projects** and
   **untrusted-projects** provide the bulk of the information for
   tenant configuration.  They list all of the projects upon which
   Zuul will act.

   The order of the projects listed in a tenant is important.  A job
   which is defined in one project may not be redefined in another
   project; therefore, once a job appears in one project, a project
   listed later will be unable to define a job with that name.
   Further, some aspects of project configuration (such as the merge
   mode) may only be set on the first appearance of a project
   definition.

   Zuul loads the configuration from all **config-projects** in the
   order listed, followed by all **untrusted-projects** in order.

   .. attr:: config-projects

      A list of projects to be treated as :term:`config projects
      <config-project>` in this tenant.  The jobs in a config project
      are trusted, which means they run with extra privileges, do not
      have their configuration dynamically loaded for proposed
      changes, and Zuul config files are only searched for in the
      ``master`` branch.

      The items in the list follow the same format described in
      **untrusted-projects**.

      .. attr:: <project>

         The config-projects have an additional config option that
         may be specified optionally.

         .. attr:: load-branch
            :default: master

            Define which branch is loaded from a config project. By
            default config projects load Zuul configuration only
            from the master branch.

   .. attr:: untrusted-projects

      A list of projects to be treated as untrusted in this tenant.
      An :term:`untrusted-project` is the typical project operated on
      by Zuul.  Their jobs run in a more restrictive environment, they
      may not define pipelines, their configuration dynamically
      changes in response to proposed changes, and Zuul will read
      configuration files in all of their branches.

      .. attr:: <project>

         The items in the list may either be simple string values of
         the project names, or a dictionary with the project name as
         key and the following values:

         .. attr:: include

            Normally Zuul will load all of the :ref:`configuration-items`
            appropriate for the type of project (config or untrusted)
            in question.  However, if you only want to load some
            items, the **include** attribute can be used to specify
            that *only* the specified items should be loaded.
            Supplied as a string, or a list of strings.

            The following **configuration items** are recognized:

            * pipeline
            * job
            * semaphore
            * project
            * project-template
            * nodeset
            * secret
            * image
            * flavor
            * label
            * section
            * provider

         .. attr:: exclude

            A list of **configuration items** that should not be loaded.

         .. attr:: include-provider-config
            :type: bool

            Add the configuration items related to provider
            configuration to the set of items to be loaded.  Normally,
            :term:`untrusted projects <untrusted-project>` do not load
            these items, but :term:`config projects <config-project>`
            do.  To allow an untrusted-project to do so (or to add
            back this set of items after using **include**), set this
            attribute to ``true``.  This takes effect after
            **include** and before **exclude**.

            The following **configuration items** are considered
            **provider config**:

            * image
            * flavor
            * label
            * section
            * provider

         .. attr:: shadow

            Normally, only one project in Zuul may contain
            definitions for a given job.  If a project earlier in the
            configuration defines a job which a later project
            redefines, the later definition is considered an error and
            is not permitted.  The **shadow** attribute of a project
            indicates that job definitions in this project which
            conflict with the named projects should be ignored, and
            those in the named project should be used instead.  The
            named projects must still appear earlier in the
            configuration.  In the example above, if a job definition
            appears in both the ``common-config`` and ``zuul-jobs``
            projects, the definition in ``common-config`` will be
            used.

         .. attr:: exclude-unprotected-branches

            Define if unprotected branches should be processed.
            Defaults to the tenant wide setting of
            exclude-unprotected-branches. This currently only affects
            GitHub and GitLab projects.

         .. attr:: exclude-locked-branches

            Define if locked branches should be processed.
            Defaults to the tenant wide setting of
            exclude-locked-branches. This currently only affects
            GitHub projects.

         .. attr:: include-branches

            A list of regexes matching branches which should be
            processed.  If omitted, all branches are included.
            Operates after *exclude-unprotected-branches* and
            *exclude-locked-branches* and so may be used to further
            reduce the set of branches (but not increase it).

            It has priority over *exclude-branches*.

         .. attr:: exclude-branches

            A list of regexes matching branches which should be
            processed.  If omitted, all branches are included.
            Operates after *exclude-unprotected-branches* and
            *exclude-locked-branches* and so may be used to further
            reduce the set of branches (but not increase it).

            It will not exclude a branch which already matched
            *include-branches*.

         .. attr:: always-dynamic-branches

            A list of regular expressions matching branches which
            should be treated as if every change newly proposes
            dynamic Zuul configuration.  In other words, the only time
            Zuul will realize any configuration related to these
            branches is during the time it is running jobs for a
            proposed change.

            This is potentially useful for situations with large
            numbers of rarely used feature branches, but comes at the
            cost of a significant reduction in Zuul features for these
            branches.

            Every regular expression listed here will also implicitly
            be included in *exclude-branches*, therefore Zuul will not
            load any static in-repo configuration from this branch.
            These branches will not be available for use in overriding
            checkouts of repos, nor will they be included in the git
            repos that Zuul prepares for *required-projects* (unless
            there is a change in the dependency tree for this branch).

            In particular, this means that the only jobs which can be
            specified for these branches are pre-merge and gating jobs
            (such as :term:`check` and :term:`gate`).  No post-merge
            or periodic jobs will run for these branches.

            Using this setting also incurs additional processing for
            each change submitted for these branches as Zuul must
            recalculate the configuration layout it uses for such a
            change as if it included a change to a ``zuul.yaml`` file,
            even if the change does not alter the configuration).

            With all these caveats in mind, this can be useful for
            repos with large numbers of rarely used branches as it
            allows Zuul to omit their configuration in most
            circumstances and only calculate the configuration of a
            single additional branch when it is used.

         .. attr:: implied-branch-matchers

            This is a boolean, which, if set, may be used to enable
            (``true``) or disable (``false``) the addition of implied
            branch matchers to job and project-template definitions.
            Normally Zuul decides whether to add these based on
            heuristics described in :attr:`job.branches`.  This
            attribute overrides that behavior.

            This can be useful if branch settings for this project may
            produce an unpredictable number of branches to load from.
            Setting this value explicitly here can avoid unexpected
            behavior changes as branches are added or removed from the
            load set.

            The :attr:`pragma.implied-branch-matchers` pragma will
            override the setting here if present.

            Note that if a job contains an explicit branch matcher, it
            will be used regardless of the value supplied here.

         .. attr:: extra-config-paths

            Normally Zuul loads in-repo configuration from the first
            of these paths:

            * zuul.yaml
            * zuul.d/*
            * .zuul.yaml
            * .zuul.d/*

            If this option is supplied then, after the normal process
            completes, Zuul will also load any configuration found in
            the files or paths supplied here.  This can be a string or
            a list.  If a list of multiple items, Zuul will load
            configuration from *all* of the items in the list (it will
            not stop at the first extra configuration found).
            Directories should be listed with a trailing ``/``.  Example:

            .. code-block:: yaml

               extra-config-paths:
                 - zuul-extra.yaml
                 - zuul-extra.d/

            This feature may be useful to allow a project that
            primarily holds shared jobs or roles to include additional
            in-repo configuration for its own testing (which may not
            be relevant to other users of the project).

         .. attr:: allow-reporter-jobs
            :type: bool

            Set to ``true`` to allow this project to configure
            :value:`job.type.reporter` jobs to run on its changes (or
            the changes of other projects it is permitted to configure
            via
            :attr:`tenant.untrusted-projects.<project>.configure-projects`).
            This behavior is normally reserved for for :term:`config
            projects <config-project>`.

         .. attr:: configure-projects

            A list of project names (or :ref:`regular expressions
            <regex>` to match project names) that this project is
            permitted to configure.  The use of this setting will
            allow this project to specify :attr:`project` stanzas that
            apply to untrusted-projects specified here.  This is an
            advanced and potentially dangerous configuration setting
            since it would allow one project to cause another project
            to run certain jobs.  This behavior is normally reserved
            for :term:`config projects <config-project>`.

            This should only be used in situations where there is a
            strong trust relationship between this project and the
            projects it is permitted to configure.

      .. attr:: <project-group>

         The items in the list are dictionaries with the following
         attributes. A **configuration items** definition is applied
         to the list of projects.

         .. attr:: include

            A list of **configuration items** that should be loaded.

         .. attr:: exclude

            A list of **configuration items** that should not be loaded.

         .. attr:: projects

            A list of **project** items.

   .. attr:: max-dependencies

      This setting can be used to limit the number of dependencies
      that Zuul will consider when enqueuing a change in any pipeline
      in this tenant.  If used, it should be set to a value that is
      higher than the highest number of dependencies that are expected
      to be encountered.  If, when enqueuing a change, Zuul detects
      that the dependencies will exceed this value, Zuul will not
      enqueue the change and will provide no feedback to the user.
      This is meant only to protect the Zuul server from resource
      exhaustion when excessive dependencies are present.  The default
      (unset) is no limit.  Note that the value ``0`` does not disable
      this option; instead it limits Zuul to zero dependencies.  This
      is distinct from :attr:`<gerrit connection>.max_dependencies`.

   .. attr:: max-changes-per-pipeline

      The number of changes (not queue items) allowed in any
      individual pipeline in this tenant.  Live changes, non-live
      changes used for dependencies, and changes that are part of a
      dependency cycle are all counted.  If a change appears in more
      than one queue item, it is counted multiple times.

      For example, if this value was set to 100, then Zuul would allow
      any of the following (but no more):

      * 100 changes in individual queue items
      * 1 queue item of 100 changes in a dependency cycle
      * 1 queue item with 99 changes in a cycle plus one item depending
        on that cycle

      This counts changes across all queues in the pipeline; it is
      therefore possible for a set of projects in one queue to affect
      others in the same tenant.

      This value is not set by default, which means there is no limit.
      It is generally expected that the pipeline window configuration
      should be sufficient to protect against excessive resource
      usage.  However in some circumstances with large dependency
      cycles, setting this value may be useful.  Note that the value
      ``0`` does not disable this option; instead it limits Zuul to
      zero changes.

   .. attr:: max-nodes-per-job
      :default: 5

      The maximum number of nodes a job can request.  A value of
      '-1' value removes the limit.

   .. attr:: max-job-timeout
      :default: 10800

      The maximum timeout for jobs. A value of '-1' value removes the limit.

   .. attr:: max-oidc-ttl
      :default: 10800

      The maximum value that can be configured for the ``ttl``
      attribute of an OpenID Connect (OIDC) secret in this tenant.  It
      must be a positive integer.

   .. attr:: default-oidc-ttl
      :default: 3600

      The default ``ttl`` value for the ID tokens if not specified in the
      secret configuration.  It must be a positive integer and not greater
      than :attr:`tenant.max-oidc-ttl`.

   .. attr:: allowed-oidc-issuers
      :default: []

      A list of allowed issuers that can override the ``iss`` claim in an
      OIDC token in this tenant.

   .. attr:: exclude-unprotected-branches
      :default: false

      When using a branch and pull model on a shared repository
      there are usually one or more protected branches which are gated
      and a dynamic number of personal/feature branches which are the
      source for the pull requests. These branches can potentially
      include broken Zuul config and therefore break the global tenant
      wide configuration. In order to deal with this Zuul's operations
      can be limited to the protected branches which are gated. This
      is a tenant wide setting and can be overridden per project.
      This currently only affects GitHub and GitLab projects.

   .. attr:: exclude-locked-branches
      :default: false

      Some code review systems support read-only, or "locked"
      branches.  Enabling this setting will cause Zuul to ignore these
      branches.  This is a tenant wide setting and can be overridden
      per project.  This currently only affects GitHub and GitLab
      projects.

   .. attr:: default-parent
      :default: base

      If a job is defined without an explicit :attr:`job.parent`
      attribute, this job will be configured as the job's parent.
      This allows an administrator to configure a default base job to
      implement local policies such as node setup and artifact
      publishing.

   .. attr:: default-ansible-version

      Default ansible version to use for jobs that doesn't specify a version.
      See :attr:`job.ansible-version` for details.

   .. attr:: allowed-triggers
      :default: all connections

      The list of connections a tenant can trigger from. When set, this setting
      can be used to restrict what connections a tenant can use as trigger.
      Without this setting, the tenant can use any connection as a trigger.

   .. attr:: allowed-reporters
      :default: all connections

      The list of connections a tenant can report to. When set, this setting
      can be used to restrict what connections a tenant can use as reporter.
      Without this setting, the tenant can report to any connection.

   .. attr:: allowed-labels
      :default: []

      .. note:: This option is only used with Nodepool.  It is
                deprecated and will be removed in a future version of
                Zuul.

      The list of labels (as strings or :ref:`regular expressions <regex>`)
      a tenant can use in a job's nodeset. When set, this setting can
      be used to restrict what labels a tenant can use.  Without this
      setting, the tenant can use any labels.

   .. attr:: disallowed-labels
      :default: []

      .. note:: This option is only used with Nodepool.  It is
                deprecated and will be removed in a future version of
                Zuul.

      The list of labels (as strings or :ref:`regular expressions <regex>`)
      a tenant is forbidden to use in a job's nodeset. When set, this
      setting can be used to restrict what labels a tenant can use.
      Without this setting, the tenant can use any labels permitted by
      :attr:`tenant.allowed-labels`.  This check is applied after the
      check for `allowed-labels` and may therefore be used to further
      restrict the set of permitted labels.

   .. attr:: web-root

      If this tenant has a whitelabeled installation of zuul-web, set
      its externally visible URL here (e.g.,
      ``https://tenant.example.com/``).  This will override the
      :attr:`web.root` setting when constructing URLs for this tenant.

   .. attr:: admin-rules

      .. note:: This option is deprecated and will be removed in a
                future version of Zuul.  See
                :attr:`tenant.role-mappings` instead.

      A list of authorization rules to be checked in order to grant
      administrative access to the tenant through Zuul's REST API and
      web interface.

      At least one rule in the list must match for the user to be allowed to
      execute privileged actions.  A matching rule will also allow the user
      access to the tenant in general (i.e., the rule does not need to be
      duplicated in `access-rules`).

      More information on tenant-scoped actions can be found in
      :ref:`authentication`.

   .. attr:: access-rules

      .. note:: This option is deprecated and will be removed in a
                future version of Zuul.  See
                :attr:`tenant.role-mappings` instead.

      A list of authorization rules to be checked in order to grant
      read access to the tenant through Zuul's REST API and web
      interface.

      If no rules are listed, then anonymous access to the tenant is
      permitted.  If any rules are present then at least one rule in
      the list must match for the user to be allowed to access the
      tenant.

      More information on tenant-scoped actions can be found in
      :ref:`authentication`.

   .. attr:: anonymous-read-access
      :default: true

      Whether anonymous users are permitted read access to this
      tenant.  If this is set to ``false`` then at least one entry
      must appear in :attr:`tenant.role-mappings` in order to grant
      the ``read`` permission.

   .. attr:: role-mappings

      A dictionary that maps :attr:`tenant.authorization-rule` entries
      to one or more :attr:`tenant.role` entries.  For example:

      .. code-block:: yaml

         - authorization-rule:
             name: admin-user
             conditions:
               - preferred_username: admin

         - authorization-rule:
             name: alice
             conditions:
               - preferred_username: alice

         - authorization-rule:
             name: everyone
             conditions:
               - iss: our-institution

         - role:
             name: autohold
             permissions:
               autohold: true

         - role:
             name: enqueue-post
             permissions:
               enqueue:
                 conditions:
                   pipeline: post
                   project: foo

         - tenant:
             name: example
             anonymous-read-access: false
             role-mappings:
               admin-user: admin
               everyone: [read, autohold]
               alice: enqueue-post

      This indicates that the `admin` (identified by the `admin-user`
      rule) has full administrative access (granted via the built-in
      `admin` role); all authenticated users have permissions to read
      and set autoholds (via the built-in `read` role, and the
      `autohold` role defined above); and `alice` additionally has
      permissions to enqueue items for the `foo` project in the `post`
      pipeline.

      If any role-mappings are provided, then
      :attr:`tenant.admin-rules` and :attr:`tenant.access-rules` are
      disregarded.

      See also :attr:`tenant.anonymous-read-access` for configuring
      read access.

   .. attr:: authentication-realm

      Each authenticator defined in Zuul's configuration is associated to a realm.
      When authenticating through Zuul's Web User Interface under this tenant, the
      Web UI will redirect the user to this realm's authentication service. The
      authenticator must be of the type ``OpenIDConnect``.

      .. note::

         Defining a default realm for a tenant will not invalidate
         access tokens issued from other configured realms. This is
         intended so that an operator can issue an overriding access
         token manually. If this is an issue, it is advised to add
         finer filtering to admin rules, for example, filtering by the
         ``iss`` claim (generally equal to the issuer ID).

   .. attr:: semaphores

      A list of names of :attr:`global-semaphore` objects to allow
      jobs in this tenant to access.

   .. attr:: use-nodepool
      :default: true

      Once a tenant has been migrated to use `zuul-launcher` instead
      of Nodepool, set this value to ``false`` in order to disable the
      Nodepool label fallback behavior.  This is important since, with
      no Nodepool running, Zuul needs to know that any requests for
      nonexistent labels should be declined.

.. _global_semaphore:

Global Semaphore
----------------

Semaphores are normally defined in in-repo configuration (see
:ref:`semaphore`), however to support use-cases where semaphores are
used to represent constrained global resources that may be used by
multiple Zuul tenants, semaphores may be defined within the main
tenant configuration file.

In order for a job to use a global semaphore, the semaphore must first
be defined in the tenant configuration file with
:attr:`global-semaphore` and then added to each tenant which should
have access to it with :attr:`tenant.semaphores`.  Once that is done,
Zuul jobs may use that semaphore in the same way they would use a
normal tenant-scoped semaphore.

If any tenant which is granted access to a global semaphore also has a
tenant-scoped semaphore defined with the same name, that definition
will be treated as a configuration error and subsequently ignored in
favor of the global semaphore.

An example definition looks similar to the normal semaphore object:

.. code-block:: yaml

   - global-semaphore:
       name: global-semaphore-foo
       max: 5

.. attr:: global-semaphore

   The following attributes are available:

   .. attr:: name
      :required:

      The name of the semaphore, referenced by jobs.

   .. attr:: max
      :default: 1

      The maximum number of running jobs which can use this semaphore.


.. _authz_rule_definition:

Authorization Rule
------------------

An authorization rule is a set of conditions the claims of a user's
JWT must match in order to be allowed to perform actions at a tenant's
level.

Authorization rules may be used with the deprecated
:attr:`tenant.admin-rules` system, in which case they grant all
permissions, or via the newer :attr:`tenant.role-mappings` system, in
which case they may be used to grant fine-grained permissions.

.. note::

   Rules can be overridden by the ``zuul.admin`` claim in a token if if matches
   an authenticator configuration where `allow_authz_override` is set to true.
   See :ref:`authentication` for more details.

Below are some examples of how authorization rules can be defined:

.. code-block:: yaml

   - authorization-rule:
       name: affiliate_or_admin
       conditions:
         - resources_access:
             account:
               roles: "affiliate"
           iss: external_institution
         - resources_access.account.roles: "admin"
   - authorization-rule:
       name: alice_or_bob
       conditions:
         - zuul_uid: alice
         - zuul_uid: bob

Zuul previously used ``admin-rule`` for these definitions.  That form
is still permitted for backwards compatibility, but is deprecated and
will be removed in a future version of Zuul.

.. attr:: authorization-rule

   The following attributes are supported:

   .. attr:: name
      :required:

      The name of the rule, so that it can be referenced in the
      tenant's definition. It must be unique.

   .. attr:: conditions
      :required:

      This is the list of conditions that define a rule. A JWT must match **at
      least one** of the conditions for the rule to apply. A condition is a
      dictionary where keys are claims. **All** the associated values must
      match the claims in the user's token; in other words the condition dictionary
      must be a "sub-dictionary" of the user's JWT.

      Zuul's authorization engine will adapt matching tests depending on the
      nature of the claim in the token, eg:

      * if the claim is a JSON list, check that the condition value is in the
        claim
      * if the claim is a string or a boolean, check that the condition value is
        equal to the claim's value

      The claim names can also be written in the XPath format for clarity: the
      condition

      .. code-block:: yaml

        resources_access:
          account:
            roles: "affiliate"

      is equivalent to the condition

      .. code-block:: yaml

        resources_access.account.roles: "affiliate"

      The special ``zuul_uid`` claim refers to the ``uid_claim`` setting in an
      authenticator's configuration. By default it refers to the ``sub`` claim
      of a token. For more details see the :ref:`authentication`.

      Under the above example, the following token would match rules
      ``affiliate_or_admin`` and ``alice_or_bob``:

      .. code-block:: javascript

        {
         'iss': 'external_institution',
         'aud': 'my_zuul_deployment',
         'exp': 1234567890,
         'iat': 1234556780,
         'sub': 'alice',
         'resources_access': {
             'account': {
                 'roles': ['affiliate', 'other_role']
             }
         },
        }

      And this token would only match rule ``affiliate_or_admin``:

      .. code-block:: javascript

        {
         'iss': 'some_other_institution',
         'aud': 'my_zuul_deployment',
         'exp': 1234567890,
         'sub': 'carol',
         'iat': 1234556780,
         'resources_access': {
             'account': {
                 'roles': ['admin', 'other_role']
             }
         },
        }

Authorization Rule Templating
-----------------------------

The special word "{tenant.name}" can be used in conditions' values. It will be automatically
substituted for the relevant tenant when evaluating authorizations for a given
set of claims. For example, consider the following rule:

.. code-block:: yaml

   - authorization-rule:
       name: tenant_in_groups
       conditions:
         - groups: "{tenant.name}"

If applied to the following tenants:

.. code-block:: yaml

   - tenant:
       name: tenant-one
       admin-rules:
         - tenant_in_groups
   - tenant:
       name: tenant-two
       admin-rules:
         - tenant_in_groups

Then this set of claims will be allowed to perform protected actions on **tenant-one**:

.. code-block:: javascript

  {
   'iss': 'some_other_institution',
   'aud': 'my_zuul_deployment',
   'exp': 1234567890,
   'sub': 'carol',
   'iat': 1234556780,
   'groups': ['tenant-one', 'some-other-group'],
  }

And this set of claims will be allowed to perform protected actions on **tenant-one**
and **tenant-two**:

.. code-block:: javascript

    {
     'iss': 'some_other_institution',
     'aud': 'my_zuul_deployment',
     'exp': 1234567890,
     'sub': 'carol',
     'iat': 1234556780,
     'groups': ['tenant-one', 'tenant-two'],
    }

.. _authz_role_definition:

Role
----

A role is used in conjunction with :attr:`authorization-rule` and
:attr:`tenant.role-mappings` to specify fine-grained permissions for
use with authorization.

Zuul contains two built-in roles: ``admin`` which always grants all
permissions (and will continue to do so as new permissions are added
in the future), and ``read`` which grants only the ``read``
permission.

.. attr:: role

   The following attributes are supported:

   .. attr:: name
      :required:

      The name of the role, so that it can be referenced in the
      tenant's definition. It must be unique.

   .. attr:: permissions

      A dictionary where keys are the names of permissions and the
      values are either ``true`` to grant that permission, or a
      dictionary of conditions that the request must match in order to
      grant the permission.

      .. attr:: read
         :type: bool

         Grant read access to the tenant.  This is only effective if
         :attr:`tenant.anonymous-read-access` is set to false.  In
         that case, either the built-in ``read`` role, or a
         user-defined role with the ``read`` permission must be used.

         Setting this value to ``False`` may not be effective.

      .. attr:: promote
         :type: bool

         Permission to promote items in any pipeline.  Only accepts a
         boolean value.

      .. attr:: set-tenant-state
         :type: bool

         Permission to modify the tenant state (such as disabling
         event processing).  Only accepts a boolean value.

      .. attr:: build-image
         :type: bool

         Permission to build any image in the tenant.
         Only accepts a boolean value.

      .. attr:: upload-image
         :type: bool

         Permission to upload any image in the tenant.
         Only accepts a boolean value.

      .. attr:: delete-image-build-artifact
         :type: bool

         Permission to delete any image build artifact in the tenant.
         Only accepts a boolean value.

      .. attr:: delete-image-upload
         :type: bool

         Permission to delete any image upload in the tenant.
         Only accepts a boolean value.

      .. attr:: validate-image-upload
         :type: bool

         Permission to validate any image upload in the tenant.
         Only accepts a boolean value.

      .. attr:: modify-node
         :type: bool

         Permission to modify any node in the tenant.
         Only accepts a boolean value.

      .. attr:: modify-nodeset-request
         :type: bool

         Permission to modify any nodeset request in the tenant.
         Only accepts a boolean value.

      .. attr:: autohold
         :type: bool

         Permission to create or delete any autohold in the tenant.
         Only accepts a boolean value.

      .. attr:: dequeue
         :type: boolean or dict

         Permission to dequeue a change.  If set to ``true``,
         permission is granted to dequeue any change.

         .. attr:: conditions
            :type: dict

            Further conditions may be applied with the following
            attributes:

            .. attr:: project

               The name of a project.

            .. attr:: ref

               The name of a ref (e.g., `refs/heads/stable`).

      .. attr:: enqueue
         :type: boolean or dict

         Permission to enqueue a change.  If set to ``true``,
         permission is granted to enqueue any change.

         .. attr:: conditions
            :type: dict

            Further conditions may be applied with the following
            attributes:

            .. attr:: project

               The name of a project.

            .. attr:: ref

               The name of a ref (e.g., `refs/heads/stable`).

API Root
--------

Most actions in zuul-web, zuul-client, and the REST API are understood
to be within the context of a specific tenant and therefore the
authorization rules specified by that tenant apply.  When zuul-web is
deployed in a multi-tenant scenario (the default), there are a few
extra actions or API methods which are outside of the context of an
individual tenant (for example, listing the tenants or observing the
state of Zuul system components).  To control access to these methods,
an `api-root` object can be used.

At most one `api-root` object may appear in the tenant configuration
file.  If more than one appears, it is an error.  If there is no
`api-root` object, then anonymous read-only access to the tenant list
and other root-level API methods is assumed.

The ``/api/info`` endpoint is never protected by Zuul since it
supplies the authentication information needed by the web UI.

API root access is not a pre-requisite to access tenant-specific URLs.

.. attr:: api-root

   The following attributes are supported:

   .. attr:: authentication-realm

      Each authenticator defined in Zuul's configuration is associated
      to a realm.  When authenticating through Zuul's Web User
      Interface at the multi-tenant root, the Web UI will redirect the
      user to this realm's authentication service. The authenticator
      must be of the type ``OpenIDConnect``.

      .. note::

         Defining a default realm for the root API will not invalidate
         access tokens issued from other configured realms.  This is
         intended so that an operator can issue an overriding access
         token manually. If this is an issue, it is advised to add
         finer filtering to admin rules, for example, filtering by the
         ``iss`` claim (generally equal to the issuer ID).

   .. attr:: access-rules

      A list of authorization rules to be checked in order to grant
      read access to the top-level (i.e., non-tenant-specific) portion
      of Zuul's REST API and web interface.

      If no rules are listed, then anonymous access to top-level
      methods is permitted.  If any rules are present then at at least
      one rule in the list must match for the user to be allowed
      access.

      More information on tenant-scoped actions can be found in
      :ref:`authentication`.
