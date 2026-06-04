:title: GitHub Driver

.. _github_driver:

GitHub
======

The GitHub driver supports sources, triggers, and reporters.  It can
interact with the public GitHub service as well as site-local
installations of GitHub enterprise.

Configure GitHub
----------------

Zuul needs to receive notification of events from GitHub, and it needs
to interact with GitHub in response to those events.  There are two
options available for configuring these connections.  A GitHub project's
owner can either manually setup a web-hook or install a GitHub
Application.  In the first case, the project's owner needs to know
the Zuul endpoint and the webhook secrets and configure them manually.
In the second, the project (or organization) owner need only install
pre-existing GitHub Application into the project or organization.

In general, the Application method is recommended.  Both options are
described in the following sections.

Regardless of which option is chosen, there are several types of
authentication between Zuul and GitHub used for various purposes. Some
are required and some are optional depending on the intended use and
configuration.

In all cases Zuul needs to authenticate messages received from GitHub
as being valid. To do this a `webhook_token` is configured.

Zuul also needs to authenticate to GitHub to make certain requests. At
a high level, this is the sort of authentication that is required for
various Zuul Github functionality:

  * Reporting: Requires authentication with write access to the project so
    that comments can be posted.
  * Enqueuing a pull request (including Depends-On: of a pull request): The
    API queries needed to examine PR's so that Zuul can enqueue them
    requires authentication with read access.
  * :attr:`job.required-projects` listing: No authentication required.
    For example, you may have a project where you are only interested in
    testing against a specific branch of a GitHub project. In this case you
    do not need any authentication to have Zuul pull the project.
    However, note that if you will ever need to speculatively test a PR in
    this project, you will require authenticated read access (see note above).

There are two different ways Zuul can Authenticate its requests to
GitHub. The first is the `api_token`. This `api_token` is used by the
web-hook option for all authentication. When using the GitHub
Application system Zuul uses an `app_id` and `app_key` which is
used to generate an application token behind the scenes. But this only
works against projects that have installed your application. As a
fallback for interaction with projects that haven't installed your
application you can also configure an `api_token` when using the
application system. This is particularly useful for supporting
Depends-On functionality against GitHub projects.

Finally, authenticated requests receive much larger GitHub API rate limits.
It is worthwhile to configure both an `app_id`/`app_key` and `api_token`
when operating in application mode to avoid rate limits as much as possible.


Web-Hook
........

To configure a project's `webhook events
<https://developer.github.com/webhooks/creating/>`_:

* Set *Payload URL* to
  ``http://<zuul-hostname>:<port>/api/connection/<connection-name>/payload``.

* Set *Content Type* to ``application/json``.

Select *Events* you are interested in. See below for the supported events.

You will also need to have a GitHub user created for your zuul:

* Zuul public key needs to be added to the GitHub account

* A api_token needs to be created too, see this `article
  <https://help.github.com/articles/creating-an-access-token-for-command-line-use/>`_

Then in the zuul.conf, set webhook_token and api_token.

Application
...........

To create a `GitHub application
<https://developer.github.com/apps/building-integrations/setting-up-and-registering-github-apps/registering-github-apps/>`_:

* Go to your organization settings page to create the application, e.g.:
  https://github.com/organizations/my-org/settings/apps/new
* Set GitHub App name to "my-org-zuul"
* Set Setup URL to your setup documentation, when user install the application
  they are redirected to this url
* Set Webhook URL to
  ``http://<zuul-hostname>:<port>/api/connection/<connection-name>/payload``.
* Create a Webhook secret
* Set permissions:

  * Repository administration: Read
  * Checks: Read & Write
  * Repository contents: Read & Write (write to let zuul merge change)
  * Issues: Read & Write
  * Pull requests: Read & Write
  * Commit statuses: Read & Write

* Set events subscription:

  * Check run
  * Commit comment
  * Create
  * Push
  * Release
  * Issue comment
  * Issues
  * Label
  * Pull request
  * Pull request review
  * Pull request review comment
  * Status

* Set Where can this GitHub App be installed to "Any account"
* Create the App
* Generate a Private key in the app settings page
* Optionally configure an api_token. Please see this `article
  <https://help.github.com/articles/creating-an-access-token-for-command-line-use/>`_
  for more information.

Then in the zuul.conf, set `webhook_token`, `app_id`, `app_key` and
optionally `api_token`.  After restarting zuul-scheduler, verify in
the 'Advanced' tab that the Ping payload works (green tick and 200
response)

Users can now install the application using its public page, e.g.:
https://github.com/apps/my-org-zuul

.. note::
   GitHub Pull Requests that modify GitHub Actions workflow configuration
   files cannot be merged by application credentials (this is any Pull Request
   that edits the .github/workflows directory and its contents). These Pull
   Requests must be merged by a normal user account. This means that Zuul
   will be limited to posting test results and cannot merge these PRs
   automatically when they pass testing.

   GitHub Actions are still in Beta and this behavior may change.


Connection Configuration
------------------------

There are two forms of operation. Either the Zuul installation can be
configured as a `Github App`_ or it can be configured as a Webhook.

If the `Github App`_ approach is taken, the config settings
``app_id``, ``app_key`` and optionally ``api_token`` are required. If
the Webhook approach is taken, the ``api_token`` setting is required.

The supported options in ``zuul.conf`` connections are:

.. attr:: <github connection>

   .. attr:: driver
      :required:

      .. value:: github

         The connection must set ``driver=github`` for GitHub connections.

   .. attr:: app_id

      App ID if you are using a *GitHub App*. Can be found under the
      **Public Link** on the right hand side labeled **ID**.

   .. attr:: app_key

      Path to a file containing the secret key Zuul will use to create
      tokens for the API interactions. In Github this is known as
      **Private key** and must be collected when generated.

   .. attr:: api_token

      API token for accessing GitHub if Zuul is configured with
      Webhooks.  See `Creating an access token for command-line use
      <https://help.github.com/articles/creating-an-access-token-for-command-line-use/>`_.

   .. attr:: webhook_token

      Required token for validating the webhook event payloads.  In
      the GitHub App Configuration page, this is called **Webhook
      secret**.  See `Securing your webhooks
      <https://developer.github.com/webhooks/securing/>`_.

   .. attr:: sshkey
      :default: ~/.ssh/id_rsa

      Path to SSH key to use when cloning github repositories if Zuul
      is configured with Webhooks.

   .. attr:: server
      :default: github.com

      Hostname of the github install (such as a GitHub Enterprise).

   .. attr:: canonical_hostname

      The canonical hostname associated with the git repos on the
      GitHub server.  Defaults to the value of :attr:`<github
      connection>.server`.  This is used to identify projects from
      this connection by name and in preparing repos on the filesystem
      for use by jobs.  Note that Zuul will still only communicate
      with the GitHub server identified by **server**; this option is
      useful if users customarily use a different hostname to clone or
      pull git repos so that when Zuul places them in the job's
      working directory, they appear under this directory name.

   .. attr:: verify_ssl
      :default: true

      Enable or disable ssl verification for GitHub Enterprise.  This
      is useful for a connection to a test installation.

   .. attr:: rate_limit_logging
      :default: true

      Enable or disable GitHub rate limit logging. If rate limiting is disabled
      in GitHub Enterprise this can save some network round trip times.

   .. attr:: repo_cache

      To configure Zuul to use a GitHub Enterprise `repository cache
      <https://docs.github.com/en/enterprise-server@3.3/admin/enterprise-management/caching-repositories/about-repository-caching>`_
      set this value to the hostname of the cache (e.g.,
      ``europe-ci.github.example.com``).  Zuul will fetch commits as
      well as determine the global repo state of repositories used in
      jobs from this host.

      This setting is incompatible with :attr:`<github
      connection>.sshkey`.

      Because the repository cache may be several minutes behind the
      canonical site, enabling this setting automatically sets the
      default :attr:`<github connection>.repo_retry_timeout` to 600
      seconds.  That setting may still be overridden to specify a
      different value.

   .. attr:: repo_retry_timeout

      This setting is only used if :attr:`<github
      connection>.repo_cache` is set.  It specifies the amount of time
      in seconds that Zuul mergers and executors should spend
      attempting to fetch git commits which are not available from the
      GitHub repository cache host.

      When :attr:`<github connection>.repo_cache` is set, this value
      defaults to 600 seconds, but it can be overridden.  Zuul retries
      git fetches every 30 seconds, and this value will be rounded up
      to the next highest multiple of 30 seconds.

   .. attr:: max_threads_per_installation
      :default: 1

      The GitHub driver performs event pre-processing in parallel
      before forwarding the events (in the correct order) to the
      scheduler for processing.  By default, this parallel
      pre-processing is restricted to a single request for each GitHub
      App installation that Zuul uses when interacting with GitHub.
      This is to avoid running afoul of GitHub's abuse detection
      mechanisms.  Some high-traffic installations of GitHub
      Enterprise may wish to increase this value to allow more
      parallel requests if resources permit.  If GitHub Enterprise
      resource usage is not a concern, setting this value to ``10`` or
      greater may be reasonable.


Trigger Configuration
---------------------
GitHub webhook events can be configured as triggers.

A connection name with the GitHub driver can take multiple events with
the following options.

.. attr:: pipeline.trigger.<github source>

   The dictionary passed to the GitHub pipeline ``trigger`` attribute
   supports the following attributes:

   .. attr:: event
      :required:

      .. warning:: While it is currently possible to list more than
                   one event in a single trigger, that is deprecated
                   and support will be removed in a future version.
                   Instead, specify a single event type per trigger,
                   and list multiple triggers as necessary to cover
                   all intended events.

      The event from github. Supported events are:

      .. value:: pull_request

      .. value:: pull_request_review

      .. value:: push

      .. value:: check_run

   .. attr:: action

      A :value:`pipeline.trigger.<github source>.event.pull_request`
      event will have associated action(s) to trigger from. The
      supported actions are:

      .. value:: opened

         Pull request opened.

      .. value:: changed

         Pull request synchronized.

      .. value:: closed

         Pull request closed.

      .. value:: reopened

         Pull request reopened.

      .. value:: comment

         Comment added to pull request.

      .. value:: labeled

         Label added to pull request.

      .. value:: unlabeled

         Label removed from pull request.

      .. value:: converted_to_draft

         The pull request was converted to a draft.

      .. value:: ready_for_review

         The pull request was marked as ready for review (not a draft).

      .. value:: status

         Status set on commit. The syntax is ``user:status:value``.
         This also can be a regular expression.

      A :value:`pipeline.trigger.<github
      source>.event.pull_request_review` event will have associated
      action(s) to trigger from. The supported actions are:

      .. value:: submitted

         Pull request review added.

      .. value:: dismissed

         Pull request review removed.

      A :value:`pipeline.trigger.<github source>.event.check_run`
      event will have associated action(s) to trigger from. The
      supported actions are:

      .. value:: requested

         .. warning:: This is deprecated and will be removed in a
                      future version.  Use
                      :value:`pipeline.trigger.<github
                      source>.action.rerequested` instead.

         Deprecated alias for :value:`pipeline.trigger.<github
         source>.action.rerequested`.

      .. value:: rerequested

         A check run is rerequested.

      .. value:: completed

         A check run completed.

   .. attr:: branch

      The branch associated with the event. Example: ``master``.  This
      field is treated as a regular expression, and multiple branches
      may be listed. Used for ``pull_request`` and
      ``pull_request_review`` events.

   .. attr:: comment

      This is only used for ``pull_request`` ``comment`` actions.  It
      accepts a list of regexes that are searched for in the comment
      string. If any of these regexes matches a portion of the comment
      string the trigger is matched.  ``comment: retrigger`` will
      match when comments containing 'retrigger' somewhere in the
      comment text are added to a pull request.

   .. attr:: label

      This is only used for ``labeled`` and ``unlabeled``
      ``pull_request`` actions.  It accepts a list of strings each of
      which matches the label name in the event literally.  ``label:
      recheck`` will match a ``labeled`` action when pull request is
      labeled with a ``recheck`` label. ``label: 'do not test'`` will
      match a ``unlabeled`` action when a label with name ``do not
      test`` is removed from the pull request.

   .. attr:: permission

      If present, the account that performed the action must have this
      permission (or permissions) to match.  The available
      values are ``read``, ``write``, and ``admin``.

   .. attr:: state

      This is only used for ``pull_request_review`` events.  It
      accepts a list of strings each of which is matched to the review
      state, which can be one of ``approved``, ``commented``,
      ``changes_requested``, ``dismissed``, or ``pending``.

   .. attr:: status

      This is used for ``pull_request`` and ``status`` actions. It
      accepts a list of strings each of which matches the user setting
      the status, the status context, and the status itself in the
      format of ``user:context:status``.  For example,
      ``zuul_github_ci_bot:check_pipeline:success``.

   .. attr:: check

      This is only used for ``check_run`` events. It works similar to
      the ``status`` attribute and accepts a list of strings each of
      which matches the app requesting or updating the check run, the
      check run's name and the conclusion in the format of
      ``app:name::conclusion``.
      To make Zuul properly interact with Github's checks API, each
      pipeline that is using the checks API should have at least one
      trigger that matches the pipeline's name regardless of the result,
      e.g. ``zuul:cool-pipeline:.*``. This will enable the cool-pipeline
      to trigger whenever a user requests the ``cool-pipeline`` check
      run as part of the ``zuul`` check suite.
      Additionally, one could use ``.*:success`` to trigger a pipeline
      whenever a successful check run is reported (e.g. useful for
      gating).

   .. attr:: ref

      This is only used for ``push`` events. This field is treated as
      a regular expression and multiple refs may be listed. GitHub
      always sends full ref name, eg. ``refs/tags/bar`` and this
      string is matched against the regular expression.

   .. attr:: require-status

      .. warning:: This is deprecated and will be removed in a future
                   version.  Use :attr:`pipeline.trigger.<github
                   source>.require` instead.

      This may be used for any event.  It requires that a certain kind
      of status be present for the PR (the status could be added by
      the event in question).  It follows the same syntax as
      :attr:`pipeline.require.<github source>.status`. For each
      specified criteria there must exist a matching status.

      This is ignored if the :attr:`pipeline.trigger.<github
      source>.require` attribute is present.

   .. attr:: require

      This may be used for any event.  It describes conditions that
      must be met by the PR in order for the trigger event to match.
      Those conditions may be satisfied by the event in question.  It
      follows the same syntax as :ref:`github_requirements`.

   .. attr:: reject

      This may be used for any event and is the mirror of
      :attr:`pipeline.trigger.<github source>.require`.  It describes
      conditions that when met by the PR cause the trigger event not
      to match.  Those conditions may be satisfied by the event in
      question.  It follows the same syntax as
      :ref:`github_requirements`.

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


Reporter Configuration
----------------------
Zuul reports back to GitHub via GitHub API. Available reports include a PR
comment containing the build results, a commit status on start, success and
failure, an issue label addition/removal on the PR, and a merge of the PR
itself. Status name, description, and context is taken from the pipeline.

.. attr:: pipeline.<reporter>.<github source>

   To report to GitHub, the dictionaries passed to any of the pipeline
   :ref:`reporter<reporters>` attributes support the following
   attributes:

   .. attr:: status
      :type: str
      :default: None

      Report status via the Github `status API
      <https://docs.github.com/v3/repos/statuses/>`__.  Set to one of

      .. value:: pending

      .. value:: success

      .. value:: failure

      This is usually mutually exclusive with a value set in
      :attr:`pipeline.<reporter>.<github source>.check`, since this
      reports similar results via a different API.  This API is older
      and results do not show up on the "checks" tab in the Github UI.
      It is recommended to use `check` unless you have a specific
      reason to use the status API.

   .. attr:: check
      :type: string

      Report status via the Github `checks API
      <https://docs.github.com/v3/checks/>`__.  Set to one of

      .. value:: queued

      .. value:: cancelled

      .. value:: failure

      .. value:: in_progress

      .. value:: neutral

      .. value:: skipped

      .. value:: success

      This is usually mutually exclusive with a value set in
      :attr:`pipeline.<reporter>.<github source>.status`, since this
      reports similar results via a different API.

   .. attr:: comment
      :default: true

      Boolean value that determines if the reporter should add a
      comment to the pipeline status to the github pull request. Only
      used for Pull Request based items.

   .. attr:: review

      One of `approve`, `comment`, or `request-changes` that causes the
      reporter to submit a review with the specified status on Pull Request
      based items. Has no effect on other items.

   .. attr:: review-body

      Text that will be submitted as the body of the review. Required if review
      is set to `comment` or `request-changes`.

   .. attr:: merge
      :default: false

      Boolean value that determines if the reporter should merge the
      pull request. Only used for Pull Request based items.

   .. attr:: label

      List of strings each representing an exact label name which
      should be added to the pull request by reporter. Only used for
      Pull Request based items.

   .. attr:: unlabel

      List of strings each representing an exact label name which
      should be removed from the pull request by reporter. Only used
      for Pull Request based items.

.. _Github App: https://developer.github.com/apps/

.. _github_requirements:

Requirements Configuration
--------------------------

As described in :attr:`pipeline.require` and :attr:`pipeline.reject`,
pipelines may specify that items meet certain conditions in order to
be enqueued into the pipeline.  These conditions vary according to the
source of the project in question.  To supply requirements for changes
from a GitHub source named ``github``, create a configuration such
as the following::

  pipeline:
    require:
      github:
        review:
          - type: approved

This indicates that changes originating from the GitHub connection
named ``github`` must have an approved code review in order to be
enqueued into the pipeline.

.. attr:: pipeline.require.<github source>

   The dictionary passed to the GitHub pipeline `require` attribute
   supports the following attributes:

   .. attr:: review

      This requires that a certain kind of code review be present for
      the pull request (it could be added by the event in question).
      It takes several sub-parameters, all of which are optional and
      are combined together so that there must be a code review
      matching all specified requirements.

      .. attr:: username

         If present, a code review from this username matches.  It is
         treated as a regular expression.

      .. attr:: email

         If present, a code review with this email address matches.
         It is treated as a regular expression.

      .. attr:: older-than

         If present, the code review must be older than this amount of
         time to match.  Provide a time interval as a number with a
         suffix of "w" (weeks), "d" (days), "h" (hours), "m"
         (minutes), "s" (seconds).  Example ``48h`` or ``2d``.

      .. attr:: newer-than

         If present, the code review must be newer than this amount of
         time to match.  Same format as "older-than".

      .. attr:: type

         If present, the code review must match this type (or types).

         .. TODO: what types are valid?

      .. attr:: permission

         If present, the author of the code review must have this
         permission (or permissions) to match.  The available values
         are ``read``, ``write``, and ``admin``.

   .. attr:: open

      A boolean value (``true`` or ``false``) that indicates whether
      the change must be open or closed in order to be enqueued.

   .. attr:: merged

      A boolean value (``true`` or ``false``) that indicates whether
      the change must be merged or not in order to be enqueued.

   .. attr:: current-patchset

      A boolean value (``true`` or ``false``) that indicates whether
      the item must be associated with the latest commit in the pull
      request in order to be enqueued.

      .. TODO: this could probably be expanded upon -- under what
         circumstances might this happen with github

   .. attr:: draft

      A boolean value (``true`` or ``false``) that indicates whether
      or not the change must be marked as a draft in GitHub in order
      to be enqueued.

   .. attr:: status

      A string value that corresponds with the status of the pull
      request.  The syntax is ``user:status:value``. This can also
      be a regular expression.

      Zuul does not differentiate between a status reported via
      status API or via checks API (which is also how Github behaves
      in terms of branch protection and `status checks`_).
      Thus, the status could be reported by a
      :attr:`pipeline.<reporter>.<github source>.status` or a
      :attr:`pipeline.<reporter>.<github source>.check`.

      When a status is reported via the status API, Github will add
      a ``[bot]`` to the name of the app that reported the status,
      resulting in something like ``user[bot]:status:value``. For a
      status reported via the checks API, the app's slug will be
      used as is.

   .. attr:: label

      A string value indicating that the pull request must have the
      indicated label (or labels).

.. attr:: pipeline.reject.<github source>

   The `reject` attribute is the mirror of the `require` attribute and
   is used to specify pull requests which should not be enqueued into
   a pipeline.  It accepts a dictionary under the connection name and
   with the following attributes:

   .. attr:: review

      This requires that a certain kind of code review be absent for
      the pull request (it could be removed by the event in question).
      It takes several sub-parameters, all of which are optional and
      are combined together so that there must not be a code review
      matching all specified requirements.

      .. attr:: username

         If present, a code review from this username matches.  It is
         treated as a regular expression.

      .. attr:: email

         If present, a code review with this email address matches.
         It is treated as a regular expression.

      .. attr:: older-than

         If present, the code review must be older than this amount of
         time to match.  Provide a time interval as a number with a
         suffix of "w" (weeks), "d" (days), "h" (hours), "m"
         (minutes), "s" (seconds).  Example ``48h`` or ``2d``.

      .. attr:: newer-than

         If present, the code review must be newer than this amount of
         time to match.  Same format as "older-than".

      .. attr:: type

         If present, the code review must match this type (or types).

         .. TODO: what types are valid?

      .. attr:: permission

         If present, the author of the code review must have this
         permission (or permissions) to match.  The available values
         are ``read``, ``write``, and ``admin``.

   .. attr:: open

      A boolean value (``true`` or ``false``) that indicates whether
      the change must be open or closed in order to be rejected.

   .. attr:: merged

      A boolean value (``true`` or ``false``) that indicates whether
      the change must be merged or not in order to be rejected.

   .. attr:: current-patchset

      A boolean value (``true`` or ``false``) that indicates whether
      the item must be associated with the latest commit in the pull
      request in order to be rejected.

      .. TODO: this could probably be expanded upon -- under what
         circumstances might this happen with github

   .. attr:: draft

      A boolean value (``true`` or ``false``) that indicates whether
      or not the change must be marked as a draft in GitHub in order
      to be rejected.

   .. attr:: status

      A string value that corresponds with the status of the pull
      request.  The syntax is ``user:status:value``. This can also
      be a regular expression.

      Zuul does not differentiate between a status reported via
      status API or via checks API (which is also how Github behaves
      in terms of branch protection and `status checks`_).
      Thus, the status could be reported by a
      :attr:`pipeline.<reporter>.<github source>.status` or a
      :attr:`pipeline.<reporter>.<github source>.check`.

      When a status is reported via the status API, Github will add
      a ``[bot]`` to the name of the app that reported the status,
      resulting in something like ``user[bot]:status:value``. For a
      status reported via the checks API, the app's slug will be
      used as is.

   .. attr:: label

      A string value indicating that the pull request must not have
      the indicated label (or labels).

Reference pipelines configuration
---------------------------------

Branch protection rules
.......................

The rules prevent Pull requests to be merged on defined branches if they are
not met. For instance a branch might require that specific status are marked
as ``success`` before allowing the merge of the Pull request.

Zuul provides the attribute
:attr:`tenant.untrusted-projects.<project>.exclude-unprotected-branches`. This
attribute is by default set to ``false`` but we recommend to set it to
``true`` for the whole tenant. By doing so Zuul will benefit from:

 - excluding in-repo development branches used to open Pull requests. This will
   prevent Zuul to fetch and read useless branches data to find Zuul
   configuration files.
 - reading protection rules configuration from the Github API for a given branch
   to define whether a Pull request must enter the gate pipeline. As of now
   Zuul only takes in account "Require status checks to pass before merging" and
   the checked status checkboxes.

Likewise, it is recommended to set the
:attr:`tenant.untrusted-projects.<project>.exclude-locked-branches` setting to
avoid expending resources on read-only branches.

With the use of the reference pipelines below, the Zuul project recommends to
set the minimum following settings:

 - attribute tenant.untrusted-projects.exclude-unprotected-branches to ``true``
   in the tenant (main.yaml) configuration file.
 - on each Github repository, activate the branch protections rules and
   configure the name of the protected branches. Furthermore set
   "Require status checks to pass before merging" and check the status labels
   checkboxes (at least ```<tenant>/check```) that must be marked as success in
   order for Zuul to make the Pull request enter the gate pipeline to be merged.


Reference pipelines
...................

Here is an example of standard pipelines you may want to define:

.. literalinclude:: /examples/pipelines/github-reference-pipelines.yaml
   :language: yaml

Github Checks API
-----------------

Github provides two distinct methods for reporting results; a "checks"
and a "status" API.

The `checks API`_ provides some additional features compared to the
`status API`_ like file comments and custom actions (e.g. cancel a
running build).

Either can be chosen when configuring Zuul to report for your Github
project.  However, there are some considerations to take into account
when choosing the API.

Design decisions
................

The Github checks API defines the concepts of `Check Suites`_ and
`Check Runs`_.  *Check suites* are a collection of *check runs* for a
specific commit and summarize a final status

A priori the check suite appears to be a good mapping for a pipeline
execution in Zuul, where a check run maps to a single job execution
that is part of the pipeline run.  Unfortunately, there are a few
problematic restrictions mapping between Github and Zuul concepts.

Github check suites are opaque and the current status, duration and
the overall conclusion are all calculated and set automatically
whenever an included check run is updated.  Most importantly, there
can only be one check suite per commit SHA, per app.  Thus there is no
facility for Zuul to create multiple check suite results for a
change, e.g. one check suite for each pipeline such as check and gate.

The Github check suite thus does not map well to Zuul's concept of
multiple pipelines for a single change.  Since a check suite is unique
and global for the change, it can not be used to flag the status of
arbitrary pipelines.  This makes the check suite API insufficient for
recording details that Zuul needs such as "the check pipeline has
passed but the gate pipeline has failed".

Another issue is that Zuul only reports on the results of the whole
pipeline, not individual jobs.  Reporting each Zuul job as a separate
check is problematic for a number of reasons.

Zuul often runs the same job for the same change multiple times; for
example in the check and gate pipeline.  There is no facility for
these runs to be reported differently in the single check suite for
the Github change.

When configuring branch protection in Github, only a *check run* can
be selected as required status check.  This is in conflict with
managing jobs in pipelines with Zuul.  For example, to implement
branch protection on GitHub would mean listing each job as a dedicated
check, leading to a check run list that is not kept in sync with the
project's Zuul pipeline configuration.  Additionally, you lose some
of Zuul's features like non-voting jobs as Github branch protections
has no concept of a non-voting job.

Thus Zuul can integrate with the checks API, but only at a pipeline
level.  Each pipeline execution will map to a check-run result
reported to Github.

Behaviour in Zuul
.................

Reporting
~~~~~~~~~

The Github reporter is able to report both a status
:attr:`pipeline.<reporter>.<github source>.status` or a check
:attr:`pipeline.<reporter>.<github source>.check`. While it's possible to
configure a Github reporter to report both, it's recommended to use only one.
Reporting both might result in duplicated status check entries in the Github
PR (the section below the comments).

Trigger
~~~~~~~

The Github driver is able to trigger on a reported check
(:value:`pipeline.trigger.<github source>.event.check_run`) similar to a
reported status (:value:`pipeline.trigger.<github source>.action.status`).

Requirements
~~~~~~~~~~~~

While trigger and reporter differentiates between status and check, the Github
driver does not differentiate between them when it comes to pipeline
requirements. This is mainly because Github also doesn't differentiate between
both in terms of branch protection and `status checks`_.

Actions / Events
................

Github provides a set of default actions for check suites and check runs.
Those actions are available as buttons in the Github UI. Clicking on those
buttons will emit webhook events which will be handled by Zuul.

These actions are only available on failed check runs / check suites. So
far, a running or successful check suite / check run does not provide any
action from Github side.

Available actions are:

Re-run all checks
  Github emits a webhook event with type ``check_suite`` and action
  ``rerequested`` that is meant to re-run all check-runs contained in this
  check suite. Github does not provide the list of check-runs in that case,
  so it's up to the Github app what should run.

Re-run failed checks
  Github emits a webhook event with type ``check_run`` and action
  ``rerequested`` for each failed check run contained in this suite.

Re-run
  Github emits a webhook event with type ``check_run`` and action
  ``rerequested`` for the specific check run.

Zuul will handle all events except for the `Re-run all checks` event;
it does not make sense in the Zuul model to trigger all pipelines to
run simultaneously.

These events are unable to be customized in Github.  Github will
always report "You have successfully requested ..." despite nothing
listening to the event.  Therefore, it might be a solution to handle
the `Re-run all checks` event in Zuul similar to `Re-run failed
checks` just to not do anything while Github makes the user believe an
action was really triggered.


File comments (annotations)
...........................

Check runs can be used to post file comments directly in the files of the PR.
Those are similar to user comments, but must provide some more information.

Zuul jobs can already return file comments via ``zuul_return``
(see: :ref:`return_values`). We can simply use this return value, build the
necessary annotations (how Github calls it) from it and attach them to the
check run.


Custom actions
~~~~~~~~~~~~~~

Check runs can provide some custom actions which will result in additional
buttons being available in the Github UI for this specific check run.
Clicking on such a button will emit a webhook event with type ``check_run``
and action ``requested_action`` and will additionally contain the id/name of
the requested action which we can define when creating the action on the
check run.

We could use these custom actions to provide some "Re-run" action on a
running check run (which might otherwise be stuck in case a check run update
fails) or to abort a check run directly from the Github UI.


Restrictions and Recommendations
................................

Although both the checks API and the status API can be activated for a
Github reporter at the same time, it's not recommended to do so as this might
result in multiple status checks to be reported to the PR for the same pipeline
execution (which would result in duplicated entries in the status section below
the comments of a PR).

In case the update on a check run fails (e.g. request timeout when reporting
success or failure to Github), the check run will stay in status "in_progress"
and there will be no way to re-run the check run via the Github UI as the
predefined actions are only available on failed check runs.
Thus, it's recommended to configure a
:value:`pipeline.trigger.<github source>.action.comment` trigger on the
pipeline to still be able to trigger re-run of the stuck check run via e.g.
"recheck".

The check suite will only list check runs that were reported by Zuul. If
the requirements for a certain pipeline are not met and it is not run, the
check run for this pipeline won't be listed in the check suite. However,
this does not affect the required status checks. If the check run is enabled
as required, Github will still show it in the list of required status checks
- even if it didn't run yet - just not in the check suite.


.. _checks API: https://docs.github.com/v3/checks/
.. _status API: https://docs.github.com/v3/repos/statuses/
.. _Check Suites: https://docs.github.com/v3/checks/suites/
.. _Check Runs: https://docs.github.com/v3/checks/runs/
.. _status checks: https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/about-status-checks#types-of-status-checks-on-github
