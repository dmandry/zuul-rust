:title: GitLab Driver

.. _gitlab_driver:

GitLab
======

The GitLab driver supports sources, triggers, and reporters. It can
interact with the public GitLab.com service as well as site-local
installations of GitLab.

Configure GitLab
----------------

Zuul needs to interact with projects by:

- receiving events via web-hooks
- performing actions via the API

web-hooks
^^^^^^^^^

Projects to be integrated with Zuul needs to send events using webhooks.
This can be enabled at Group level or Project level in "Settings/Webhooks"

- "URL" set to
  ``http://<zuul-web>/api/connection/<conn-name>/payload``
- "Merge request events" set to "on"
- "Push events" set to "on"
- "Tag push events" set to "on"
- "Comments" set to "on"
- Define a "Secret Token"


API
^^^

| Even though bot users exist: https://docs.gitlab.com/ce/user/project/settings/project_access_tokens.html#project-bot-users
| They are only available at project level.

In order to manage multiple projects using a single connection, Zuul needs a
global access to projects, which can only be achieved by creating a specific
Zuul user. This user counts as a licensed seat.

The API token must be created in user Settings, Access tokens. The Zuul user's
API token configured in zuul.conf must have the following ACL rights: "api".


Connection Configuration
------------------------

The supported options in ``zuul.conf`` connections are:

.. attr:: <gitlab connection>

   .. attr:: driver
      :required:

      .. value:: gitlab

         The connection must set ``driver=gitlab`` for GitLab connections.

   .. attr:: api_token_name

      The user's personal access token name (Used if **cloneurl** is http(s))
      Set this parameter if authentication to clone projects is required

   .. attr:: api_token

      The user's personal access token

   .. attr:: webhook_token

      The webhook secret token.

   .. attr:: server
      :default: gitlab.com

      Hostname of the GitLab server.

   .. attr:: canonical_hostname

      The canonical hostname associated with the git repos on the
      GitLab server.  Defaults to the value of :attr:`<gitlab
      connection>.server`.  This is used to identify projects from
      this connection by name and in preparing repos on the filesystem
      for use by jobs.  Note that Zuul will still only communicate
      with the GitLab server identified by **server**; this option is
      useful if users customarily use a different hostname to clone or
      pull git repos so that when Zuul places them in the job's
      working directory, they appear under this directory name.

   .. attr:: baseurl
      :default: https://{server}

      Path to the GitLab web and API interface.

   .. attr:: sshkey

      Path to SSH key to use (Used if **cloneurl** is ssh)

   .. attr:: cloneurl
      :default: {baseurl}

      Omit to clone using http(s) or set to ``ssh://git@{server}``.
      If **api_token_name** is set and **cloneurl** is either omitted or is
      set without credentials, **cloneurl** will be modified to use credentials
      as this: ``http(s)://<api_token_name>:<api_token>@<server>``.
      If **cloneurl** is defined with credentials, it will be used as is,
      without modification from the driver.

   .. attr:: keepalive
      :default: 60

      TCP connection keepalive timeout; ``0`` disables.

   .. attr:: disable_connection_pool
      :default: false

      Connection pooling improves performance and resource usage under
      normal circumstances, but in adverse network conditions it can
      be problematic.  Set this to ``true`` to disable.



Trigger Configuration
---------------------

GitLab webhook events can be configured as triggers.

A connection name with the GitLab driver can take multiple events with
the following options.

.. attr:: pipeline.trigger.<gitlab source>

   The dictionary passed to the GitLab pipeline ``trigger`` attribute
   supports the following attributes:

   .. attr:: event
      :required:

      The event from GitLab. Supported events are:

      .. value:: gl_merge_request

      .. value:: gl_push

   .. attr:: action

      A :value:`pipeline.trigger.<gitlab source>.event.gl_merge_request`
      event will have associated action(s) to trigger from. The
      supported actions are:

      .. value:: opened

         Merge request opened.

      .. value:: reopened

         Merge request reopened.

      .. value:: changed

         Merge request synchronized.

      .. value:: merged

         Merge request merged.

      .. value:: comment

         Comment added to merge request.

      .. value:: approved

         Merge request approved.

      .. value:: unapproved

         Merge request unapproved.

      .. value:: labeled

         Merge request labeled.

   .. attr:: comment

      This is only used for ``gl_merge_request`` and ``comment`` actions.  It
      accepts a list of regexes that are searched for in the comment
      string. If any of these regexes matches a portion of the comment
      string the trigger is matched.  ``comment: retrigger`` will
      match when comments containing 'retrigger' somewhere in the
      comment text are added to a merge request.

   .. attr:: branch

      The branch associated with the event. Example: ``master``.  This
      field is treated as a regular expression, and multiple branches
      may be listed. Used for ``gl_merge_request`` events.

   .. attr:: labels

      This is only used for ``gl_merge_request`` and ``labeled``
      actions.  It accepts a string or a list of strings that are that
      must have been added for the event to match.

   .. attr:: unlabels

      This is only used for ``gl_merge_request`` and ``labeled``
      actions.  It accepts a string or a list of strings that are that
      must have been removed for the event to match.

   .. attr:: ref

      This is only used for ``gl_push`` events. This field is treated as
      a regular expression and multiple refs may be listed. GitLab
      always sends full ref name, eg. ``refs/heads/bar`` and this
      string is matched against the regular expression.

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
Zuul reports back to GitLab via the API. Available reports include a Merge Request
comment containing the build results. Status name, description, and context
is taken from the pipeline.

.. attr:: pipeline.<reporter>.<gitlab source>

   To report to GitLab, the dictionaries passed to any of the pipeline
   :ref:`reporter<reporters>` attributes support the following
   attributes:

   .. attr:: comment
      :default: true

      Boolean value that determines if the reporter should add a
      comment to the pipeline status to the GitLab Merge Request.

   .. attr:: approval

      Boolean value that determines whether to report *approve* or *unapprove*
      into the merge request approval system. To set an approval the Zuul user
      must be a *Developer* or *Maintainer* project's member. If not set approval
      won't be reported.

   .. attr:: merge
      :default: false

      Boolean value that determines if the reporter should merge the
      Merge Request. To merge a Merge Request the Zuul user must be a *Developer* or
      *Maintainer* project's member. In case of *developer*, the *Allowed to merge*
      setting in *protected branches* must be set to *Developers + Maintainers*.

   .. attr:: label

      A string or list of strings, each representing a label name
      which should be added to the merge request.

   .. attr:: unlabel

      A string or list of strings, each representing a label name
      which should be removed from the merge request.


Requirements Configuration
--------------------------

As described in :attr:`pipeline.require` and :attr:`pipeline.reject`,
pipelines may specify that items meet certain conditions in order to be
enqueued into the pipeline.  These conditions vary according to the source
of the project in question.  To supply requirements for changes
from a GitLab source named ``gitlab``, create a configuration
such as the following:

.. code-block:: yaml

   pipeline:
     require:
       gitlab:
         open: true
     reject:
       gitlab:
         labels:
           - do-not-merge

This indicates that changes originating from the GitLab connection must be in
the *opened* state (not merged yet) and must not contain `do-not-merge` label.

.. attr:: pipeline.require.<gitlab source>

   The dictionary passed to the GitLab pipeline `require` attribute
   is used to specify Merge Requests which should be enqueued into
   a pipeline.  If all of the defined conditions are met, the Merge
   Request will be enqueued.  It supports the following attributes:

   .. attr:: open

      A boolean value (``true`` or ``false``) that indicates whether
      the Merge Request must be open in order to be enqueued.

   .. attr:: merged

      A boolean value (``true`` or ``false``) that indicates whether
      the Merge Request must be merged or not in order to be enqueued.

   .. attr:: approved

      A boolean value (``true`` or ``false``) that indicates whether
      the Merge Request must be approved or not in order to be enqueued.

   .. attr:: labels

      A list of labels a Merge Request must have in order to be enqueued.

.. attr:: pipeline.reject.<gitlab source>

   The `reject` attribute is the mirror of the `require` attribute and
   is used to specify Merge Requests which should not be enqueued into
   a pipeline.  If any of the defined conditions is met, the Merge Request
   will be rejected.  It accepts a dictionary under the connection name
   and with the following attributes:

   .. attr:: open

      A boolean value (``true`` or ``false``) that indicates whether
      the Merge Request must be open or not in order to be rejected.

   .. attr:: merged

      A boolean value (``true`` or ``false``) that indicates whether
      the Merge Request must be merged or not in order to be rejected.

   .. attr:: approved

      A boolean value (``true`` or ``false``) that indicates whether
      the Merge Request must be approved or not in order to be rejected.

   .. attr:: labels

      A list of labels a Merge Request must not have.
      If any of these is present in the Merge Request, it will be rejected.


Reference pipelines configuration
---------------------------------

Here is an example of standard pipelines you may want to define:

.. literalinclude:: /examples/pipelines/gitlab-reference-pipelines.yaml
   :language: yaml
