:title: Gerrit Driver

Gerrit
======

`Gerrit`_ is a code review system.  The Gerrit driver supports
sources, triggers, and reporters.

.. _Gerrit: https://www.gerritcodereview.com/

Zuul will need access to a Gerrit user.

Give that user whatever permissions will be needed on the projects you
want Zuul to report on.  For instance, you may want to grant
``Verified +/-1`` and ``Submit`` to the user.  Additional categories
or values may be added to Gerrit.  Zuul is very flexible and can take
advantage of those.

If ``change.submitWholeTopic`` is configured in Gerrit, Zuul will
honor this by enqueuing changes with the same topic as circular
dependencies.  However, it is still necessary to enable circular
dependency support in any pipeline queues where such changes may
appear.  See :attr:`queue.allow-circular-dependencies` for information
on how to configure this.

Zuul interacts with Gerrit in up to three ways:

* Receiving trigger events
* Fetching source code
* Reporting results

Trigger events arrive over an event stream.  Zuul supports several
event sources: SSH (via the ``gerrit stream-events`` command), HTTP
webhooks (via the Gerrit ``webhooks`` plugin), Kafka, AWS Kinesis,
and Google Cloud Pub/Sub.

Fetching source code may happen over SSH or HTTP.

Reporting may happen over SSH or HTTP (strongly preferred).

The appropriate connection methods must be configured to satisfy the
interactions Zuul will have with Gerrit.  The recommended
configuration is to configure both SSH and HTTP access.

The section below describes common configuration settings.  Specific
settings for different connection methods follow.

.. note::

   If Gerrit is upgraded, or the value of ``change.submitWholeTopic``
   is changed while Zuul is running, all running Zuul schedulers
   should be restarted in order to see the change.

.. note::

   Since Gerrit 3.7 it has been possible to import projects and their
   changes from one Gerrit server to another. Doing so may result in
   change number collisions (change numbers that do not uniquely identify
   a single change). Zuul's Gerrit driver is not currently expected
   to work with non unique change numbers.

Connection Configuration
------------------------

The supported options in ``zuul.conf`` connections are:

.. attr:: <gerrit connection>

   .. attr:: driver
      :required:

      .. value:: gerrit

         The connection must set ``driver=gerrit`` for Gerrit connections.

   .. attr:: server
      :required:

      Fully qualified domain name of Gerrit server.

   .. attr:: canonical_hostname

      The canonical hostname associated with the git repos on the
      Gerrit server.  Defaults to the value of
      :attr:`<gerrit connection>.server`.  This is used to identify
      projects from this connection by name and in preparing repos on
      the filesystem for use by jobs.  Note that Zuul will still only
      communicate with the Gerrit server identified by ``server``;
      this option is useful if users customarily use a different
      hostname to clone or pull git repos so that when Zuul places
      them in the job's working directory, they appear under this
      directory name.

   .. attr:: baseurl
      :default: https://{server}

      Path to Gerrit web interface.  Omit the trailing ``/``.

   .. attr:: gitweb_url_template
      :default: {baseurl}/gitweb?p={project.name}.git;a=commitdiff;h={sha}

      Url template for links to specific git shas. By default this will
      point at Gerrit's built in gitweb but you can customize this value
      to point elsewhere (like cgit or github).

      The three values available for string interpolation are baseurl
      which points back to Gerrit, project and all of its safe attributes,
      and sha which is the git sha1.

   .. attr:: max_dependencies

      This setting can be used to limit the number of dependencies
      that Zuul will consider when processing events from Gerrit.  If
      used, it should be set to a value that is higher than the
      highest number of dependencies that are expected to be
      encountered.  If, when processing an event from Gerrit, Zuul
      detects that the dependencies will exceed this value, Zuul will
      ignore the event with no feedback to the user.  This is meant
      only to protect the Zuul server from resource exhaustion when
      excessive dependencies are present.  The default (unset) is no
      limit.  Note that the value ``0`` does not disable this option;
      instead it limits Zuul to zero dependencies.  This is distinct
      from :attr:`tenant.max-dependencies`.

   .. attr:: user
      :default: zuul

      User name to use when accessing Gerrit.

   .. attr:: replication_delay
      :default: 0

      Some Gerrit installations consist of multiple servers with
      various strategies for keeping these servers in sync (push
      replication, pull replication, multi-master, shared storage,
      etc.).  Since there is currently no reliable method to determine
      when all servers are updated, this setting may be used to
      introduce a delay in event processing.  Zuul will ensure that at
      least this amount of time (in seconds) has passed before
      processing the corresponding event.

   .. attr:: webhook_events
      :default: false

      When set to a true value, Zuul will receive trigger events via
      HTTP POST requests from Gerrit's ``webhooks`` plugin instead of
      via the SSH ``stream-events`` listener.  See `HTTP Webhook Event
      Support`_ for details and the required Gerrit-side
      configuration.


SSH Configuration
~~~~~~~~~~~~~~~~~

To prepare for SSH access, create an SSH keypair for Zuul to use if
there isn't one already, and create a Gerrit user with that key::

  cat ~/id_rsa.pub | ssh -p29418 review.example.com gerrit create-account --ssh-key - --full-name Zuul zuul

.. note:: If you use an RSA key, ensure it is encoded in the PEM
          format (use the ``-t rsa -m PEM`` arguments to
          `ssh-keygen`).

If using Gerrit 2.7 or later, make sure the user is a member of a group
that is granted the ``Stream Events`` permission, otherwise it will not
be able to invoke the ``gerrit stream-events`` command over SSH.

.. attr:: <gerrit ssh connection>

   .. attr:: ssh_server

      If SSH access to the Gerrit server should be via a different
      hostname than web access, set this value to the hostname to use
      for SSH connections.

   .. attr:: port
      :default: 29418

      Gerrit SSH server port.

   .. attr:: sshkey
      :default: ~zuul/.ssh/id_rsa

      Path to SSH key to use when logging into Gerrit.

   .. attr:: keepalive
      :default: 60

      SSH connection keepalive timeout; ``0`` disables.

   .. attr:: git_over_ssh
      :default: false

      This forces git operation over SSH even if the ``password``
      attribute is set.  This allow REST API access to the Gerrit
      server even when git-over-http operation is disabled on the
      server.


HTTP Configuration
~~~~~~~~~~~~~~~~~~

.. attr:: <gerrit ssh connection>

   .. attr:: password

      The HTTP authentication password for the user.  This is
      optional, but if it is provided, Zuul will report to Gerrit via
      HTTP rather than SSH.  It is required in order for file and line
      comments to reported (the Gerrit SSH API only supports review
      messages).  Retrieve this password from the ``HTTP Password``
      section of the ``Settings`` page in Gerrit.

   .. attr:: auth_type
      :default: basic

      The HTTP authentication mechanism.

      .. value:: basic

         HTTP Basic authentication; the default for most Gerrit
         installations.

      .. value:: digest

         HTTP Digest authentication; only used in versions of Gerrit
         prior to 2.15.

      .. value:: form

         Zuul will submit a username and password to a form in order
         to authenticate.

      .. value:: gcloud_service

         Only valid when running in Google Cloud.  This will use the
         default service account to authenticate to Gerrit.  Note that
         this will only be used for interacting with the Gerrit API;
         anonymous HTTP access will be used to access the git
         repositories, therefore private repos or draft changes will
         not be available.

   .. attr:: verify_ssl
      :default: true

      When using a self-signed certificate, this may be set to
      ``false`` to disable SSL certificate verification.

Kafka Event Support
~~~~~~~~~~~~~~~~~~~

Zuul includes support for Gerrit's `events-kafka` plugin.  This may be
used as an alternative to SSH for receiving trigger events.

Kafka does provide event delivery guarantees, so unlike SSH, if all
Zuul schedulers are unable to communicate with Gerrit or Kafka, they
will eventually receive queued events on reconnection.

All Zuul schedulers will attempt to connect to Kafka brokers.  There
are some implications for event delivery:

* All events will be delivered to Zuul at least once.  In the case of
  a disrupted connection, Zuul may receive duplicate events.

* Events should generally arrive in order, however some events in
  rapid succession may be received by Zuul out of order.

.. attr:: <gerrit kafka connection>

   .. attr:: kafka_bootstrap_servers
      :required:

      A comma-separated list of Kafka servers (optionally including
      port separated with `:`).

   .. attr:: kafka_topic
      :default: gerrit

      The Kafka topic to which Zuul should subscribe.

   .. attr:: kafka_client_id
      :default: zuul

      The Kafka client ID.

   .. attr:: kafka_group_id
      :default: zuul

      The Kafka group ID.

   .. attr:: kafka_tls_cert

      Path to TLS certificate to use when connecting to a Kafka broker.

   .. attr:: kafka_tls_key

      Path to TLS certificate key to use when connecting to a Kafka broker.

   .. attr:: kafka_tls_ca

      Path to TLS CA certificate to use when connecting to a Kafka broker.

AWS Kinesis Event Support
~~~~~~~~~~~~~~~~~~~~~~~~~

Zuul includes support for Gerrit's `events-aws-kinesis` plugin.  This
may be used as an alternative to SSH for receiving trigger events.

Kinesis does provide event delivery guarantees, so unlike SSH, if all
Zuul schedulers are unable to communicate with Gerrit or AWS, they
will eventually receive queued events on reconnection.

All Zuul schedulers will attempt to connect to AWS Kinesis, but only
one scheduler will process a given Kinesis shard at a time.  There are
some implications for event delivery:

* All events will be delivered to Zuul at least once.  In the case of
  a disrupted connection, Zuul may receive duplicate events.

* If a connection is disrupted longer than the Kinesis retention
  period for a shard, Zuul may skip to the latest event ignoring all
  previous events.

* Because shard processing happens in parallel, events may not arrive
  in order.

* If a long period with no events elapses and a connection is
  disrupted, it may take Zuul some time to catch up to the latest
  events.

.. attr:: <gerrit aws kinesis connection>

   .. attr:: aws_kinesis_region
      :required:

      The AWS region name in which the Kinesis stream is located.

   .. attr:: aws_kinesis_stream
      :default: gerrit

      The AWS Kinesis stream name.

   .. attr:: aws_kinesis_access_key

      The AWS access key to use.

   .. attr:: aws_kinesis_secret_key

      The AWS secret key to use.

Google Cloud Pub/Sub Event Support
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Zuul includes support for Gerrit's `events-gcloud-pubsub` plugin.  This may be
used as an alternative to SSH for receiving trigger events.

Google Cloud Pub/Sub does provide event delivery guarantees, so unlike
SSH, if all Zuul schedulers are unable to communicate with Gerrit or
Google Cloud Pub/Sub, they will eventually receive queued events on
reconnection.

All Zuul schedulers will attempt to connect to Google Cloud Pub/Sub.
There are some implications for event delivery:

* All events will be delivered to Zuul at least once.  In the case of
  a disrupted connection, Zuul may receive duplicate events.

* Because the `events-gcloud-pubsub` plugin does not at the time of
  this writing specify that messages are ordered, events may be
  received by Zuul out of order.  Since this behavior is under the
  control of the Gerrit plugin, it may change in the future.

.. attr:: <gerrit gcloud pubsub connection>

   .. attr:: gcloud_pubsub_project
      :required:

      The Google Cloud project name to use.

   .. attr:: gcloud_pubsub_topic
      :default: gerrit

      The Google Cloud Pub/Sub topic to which Zuul should subscribe.

   .. attr:: gcloud_pubsub_subscription_id
      :default: zuul

      The ID of the Google Cloud Pub/Sub subscription to use.  If the
      subscription does not exist, it will be created.

   .. attr:: gcloud_pubsub_private_key

      Path to a file containing the JSON encoded key of a service
      account.  If not provided, then Google Cloud local auth is used.
      If Zuul is not running in the same Google Cloud project as
      Gerrit, this is required.

HTTP Webhook Event Support
~~~~~~~~~~~~~~~~~~~~~~~~~~

Zuul supports receiving trigger events from Gerrit's ``webhooks``
plugin.  This may be used as an alternative to SSH stream-events and
is the simplest event source to configure — it requires no SSH key
management and no Gerrit ``Stream Events`` permission.

Unlike Kafka, Kinesis, and Google Cloud Pub/Sub, HTTP webhooks do not
provide event delivery guarantees.  If Zuul is unreachable when Gerrit
fires a webhook, the event is lost and Zuul will not be notified.  For
most installations this is acceptable, as Zuul will also detect changes
via branch polling.  If guaranteed delivery is required, use one of the
queued event sources instead.

To use this event source:

1. Set ``webhook_events=true`` in the Zuul connection configuration.

2. Optionally set ``webhook_token`` to a shared secret.  The bundled
   Gerrit ``webhooks`` plugin has no native token or custom-header
   config field, so the secret must be embedded in the configured
   webhook URL as a ``?token=`` query parameter (as shown in the
   ``curl`` example below).  Zuul reads the ``token`` query parameter
   and rejects requests with a missing or mismatched value.

3. Configure the Gerrit ``webhooks`` plugin (via its REST API) to POST
   events to Zuul's payload endpoint::

     curl -u "admin:password" -X PUT \
       "http://<gerrit>:8080/a/config/server/webhooks~projects/All-Projects/remotes/zuul" \
       -H "Content-Type: application/json" \
       -d '{
         "url": "http://<zuul-web>:9000/api/connection/<connection-name>/payload?token=<secret>",
         "events": ["patchset-created", "change-abandoned", "change-restored",
                    "change-merged", "comment-added", "ref-updated",
                    "reviewer-added", "topic-changed", "hashtags-changed"]
       }'

When Gerrit and Zuul are running in the same Docker Compose network,
use the ``zuul-web`` service name (rather than ``localhost`` or a host
IP) in the webhook URL so the request is routed over the Compose
network.  The Gerrit ``webhooks`` plugin stores its configuration in
the ``refs/meta/config`` branch of ``All-Projects``, not in
``$GERRIT_SITE/etc/webhooks.config``; the REST API call above is the
recommended way to manage it.

.. note::

   The Gerrit ``webhooks`` plugin must be installed on the Gerrit
   server.  It is bundled with the official Gerrit Docker image.  See
   the `Gerrit webhooks plugin documentation
   <https://gerrit.googlesource.com/plugins/webhooks/+/refs/heads/master/src/main/resources/Documentation/config.md>`_
   for installation and configuration details.

.. note::

   The ``pending-check`` event type (used by the Gerrit Checks plugin)
   is not delivered via the ``webhooks`` plugin.  It continues to
   function via Zuul's internal checks poller regardless of which event
   source is selected.

.. attr:: <gerrit webhook connection>

   .. attr:: webhook_events
      :required:

      Must be set to ``true`` to enable this event source.

      .. code-block:: ini

         [connection gerrit]
         driver=gerrit
         server=review.example.com
         user=zuul
         password=secret
         webhook_events=true
         webhook_token=mysecret

   .. attr:: webhook_token

      A shared secret used to authenticate incoming webhook requests
      from Gerrit.  If set, Zuul will reject any request that does not
      supply a matching value via the ``token`` query parameter.  If
      not set, Zuul will accept all requests to the endpoint without
      authentication.

      It is strongly recommended to set this value in any deployment
      where the Zuul web endpoint is reachable from outside a trusted
      network.

      To pass the token to Gerrit, append it to the webhook URL
      configured on the Gerrit side:

      .. code-block:: text

         http://<zuul-web>:9000/api/connection/<connection-name>/payload?token=<secret>


Trigger Configuration
---------------------

.. attr:: pipeline.trigger.<gerrit source>

   The dictionary passed to the Gerrit pipeline ``trigger`` attribute
   supports the following attributes:

   .. attr:: event
      :required:

      The event name from gerrit.  Examples: ``patchset-created``,
      ``comment-added``, ``ref-updated``.  This field is treated as a
      regular expression.

   .. attr:: branch

      The branch associated with the event.  Example: ``master``.
      This field is treated as a regular expression, and multiple
      branches may be listed.

   .. attr:: ref

      On ref-updated events, the branch parameter is not used, instead
      the ref is provided.  Currently Gerrit has the somewhat
      idiosyncratic behavior of specifying bare refs for branch names
      (e.g., ``master``), but full ref names for other kinds of refs
      (e.g., ``refs/tags/foo``).  Zuul matches this value exactly
      against what Gerrit provides.  This field is treated as a
      regular expression, and multiple refs may be listed.

   .. attr:: ignore-deletes
      :default: true

      When a branch is deleted, a ref-updated event is emitted with a
      newrev of all zeros specified. The ``ignore-deletes`` field is a
      boolean value that describes whether or not these newrevs
      trigger ref-updated events.

   .. attr:: approval

      This is only used for ``comment-added`` events.  It only matches
      if the event has a matching approval associated with it.
      Example: ``Code-Review: 2`` matches a ``+2`` vote on the code
      review category.  Multiple approvals may be listed.

   .. attr:: approval-change

      This is only used for ``comment-added`` events. It works the same way as
      ``approval``, with the additional requirement that the approval value
      must have changed from its previous value. This means that it only
      matches when a user modifies an approval score instead of any comment
      where the score is present.

   .. attr:: email

      This is used for any event.  It takes a regex applied on the
      performer email, i.e. Gerrit account email address.  If you want
      to specify several email filters, you must use a YAML list.
      Make sure to use non greedy matchers and to escapes dots!
      Example: ``email: ^.*?@example\.org$``.

   .. attr:: username

      This is used for any event.  It takes a regex applied on the
      performer username, i.e. Gerrit account name.  If you want to
      specify several username filters, you must use a YAML list.
      Make sure to use non greedy matchers and to escapes dots.
      Example: ``username: ^zuul$``.

   .. attr:: comment

      This is only used for ``comment-added`` events.  It accepts a
      list of regexes that are searched for in the comment string. If
      any of these regexes matches a portion of the comment string the
      trigger is matched. ``comment: retrigger`` will match when
      comments containing ``retrigger`` somewhere in the comment text
      are added to a change.

   .. attr:: added

      This is only used for ``hashtags-changed`` events.  It accepts a
      regex or list of regexes that are searched for in the list of
      hashtags added to the change in this event.  If any of these
      regexes match a portion of any of the added hashtags, the
      trigger is matched.

   .. attr:: removed

      This is only used for ``hashtags-changed`` events.  It accepts a
      regex or list of regexes that are searched for in the list of
      hashtags removed from the change in this event.  If any of these
      regexes match a portion of any of the removed hashtags, the
      trigger is matched.

   .. attr:: require-approval

      .. warning:: This is deprecated and will be removed in a future
                   version.  Use :attr:`pipeline.trigger.<gerrit
                   source>.require` instead.

      This may be used for any event.  It requires that a certain kind
      of approval be present for the current patchset of the change
      (the approval could be added by the event in question).  It
      follows the same syntax as :attr:`pipeline.require.<gerrit
      source>.approval`. For each specified criteria there must exist
      a matching approval.

      This is ignored if the :attr:`pipeline.trigger.<gerrit
      source>.require` attribute is present.

   .. attr:: reject-approval

      .. warning:: This is deprecated and will be removed in a future
                   version.  Use :attr:`pipeline.trigger.<gerrit
                   source>.reject` instead.

      This takes a list of approvals in the same format as
      :attr:`pipeline.trigger.<gerrit source>.require-approval` but
      the item will fail to enter the pipeline if there is a matching
      approval.

      This is ignored if the :attr:`pipeline.trigger.<gerrit
      source>.reject` attribute is present.

   .. attr:: require

      This may be used for any event.  It describes conditions that
      must be met by the change in order for the trigger event to
      match.  Those conditions may be satisfied by the event in
      question.  It follows the same syntax as
      :ref:`gerrit_requirements`.

   .. attr:: reject

      This may be used for any event and is the mirror of
      :attr:`pipeline.trigger.<gerrit source>.require`.  It describes
      conditions that when met by the change cause the trigger event
      not to match.  Those conditions may be satisfied by the event in
      question.  It follows the same syntax as
      :ref:`gerrit_requirements`.

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

.. attr:: pipeline.reporter.<gerrit reporter>

   The dictionary passed to the Gerrit reporter is used to provide label
   values to Gerrit.  To set the `Verified` label to `1`, add ``verified:
   1`` to the dictionary.

   The following additional keys are recognized:

   .. attr:: submit
      :default: False

      Set this to ``True`` to submit (merge) the change.

   .. attr:: comment
      :default: True

      If this is true (the default), Zuul will leave review messages
      on the change (including job results).  Set this to false to
      disable this behavior (file and line commands will still be sent
      if present).

   .. attr:: notify

      If this is set to a notify handling value then send
      notifications at the specified level. If not, use the default
      specified by the gerrit api. Some possible values include
      ``ALL`` and ``NONE``. See the gerrit api for available options
      and default value:

      `<https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#review-input>`_

A :ref:`connection<connections>` that uses the gerrit driver must be
supplied to the trigger.

.. _gerrit_requirements:

Requirements Configuration
--------------------------

As described in :attr:`pipeline.require` and :attr:`pipeline.reject`,
pipelines may specify that items meet certain conditions in order to
be enqueued into the pipeline.  These conditions vary according to the
source of the project in question.  To supply requirements for changes
from a Gerrit source named ``gerrit``, create a configuration such
as the following:

.. code-block:: yaml

   pipeline:
     require:
       gerrit:
         approval:
           - Code-Review: 2

This indicates that changes originating from the Gerrit connection
named ``gerrit`` must have a ``Code-Review`` vote of ``+2`` in
order to be enqueued into the pipeline.

.. attr:: pipeline.require.<gerrit source>

   The dictionary passed to the Gerrit pipeline `require` attribute
   supports the following attributes:

   .. attr:: approval

      This requires that a certain kind of approval be present for the
      current patchset of the change (the approval could be added by
      the event in question). Approval is a dictionary or a list of
      dictionaries with attributes listed below, all of which are
      optional and are combined together so that there must be an approval
      matching all specified requirements.

      .. attr:: username

         If present, an approval from this username is required.  It is
         treated as a regular expression.

      .. attr:: email

         If present, an approval with this email address is required.  It is
         treated as a regular expression.

      .. attr:: older-than

         If present, the approval must be older than this amount of time
         to match.  Provide a time interval as a number with a suffix of
         "w" (weeks), "d" (days), "h" (hours), "m" (minutes), "s"
         (seconds).  Example ``48h`` or ``2d``.

      .. attr:: newer-than

         If present, the approval must be newer than this amount
         of time to match.  Same format as "older-than".

      Any other field is interpreted as a review category and value
      pair.  For example ``Verified: 1`` would require that the
      approval be for a +1 vote in the "Verified" column.  The value
      may either be a single value or a list: ``Verified: [1, 2]``
      would match either a +1 or +2 vote.

   .. attr:: open

      A boolean value (``true`` or ``false``) that indicates whether
      the change must be open or closed in order to be enqueued.

   .. attr:: current-patchset

      A boolean value (``true`` or ``false``) that indicates whether the
      change must be the current patchset in order to be enqueued.

   .. attr:: wip

      A boolean value (``true`` or ``false``) that indicates whether the
      change must be wip or not wip in order to be enqueued.

   .. attr:: status

      A string value that corresponds with the status of the change
      reported by Gerrit.

   .. attr:: hashtags

      A regex or list of regexes.  Each of these must match at least
      one of the hashtags present on the change in order for the
      change to be enqueued.

.. attr:: pipeline.reject.<gerrit source>

   The `reject` attribute is the mirror of the `require` attribute.  It
   also accepts a dictionary under the connection name.  This
   dictionary supports the following attributes:

   .. attr:: approval

      This requires that a certain kind of approval not be present for the
      current patchset of the change (the approval could be added by
      the event in question). Approval is a dictionary or a list of
      dictionaries with attributes listed below, all of which are
      optional and are combined together so that there must be no approvals
      matching all specified requirements.

      Example to reject a change with any negative vote:

      .. code-block:: yaml

         reject:
           gerrit:
             approval:
               - Code-Review: [-1, -2]

      .. attr:: username

         If present, an approval from this username is required.  It is
         treated as a regular expression.

      .. attr:: email

         If present, an approval with this email address is required.  It is
         treated as a regular expression.

      .. attr:: older-than

         If present, the approval must be older than this amount of time
         to match.  Provide a time interval as a number with a suffix of
         "w" (weeks), "d" (days), "h" (hours), "m" (minutes), "s"
         (seconds).  Example ``48h`` or ``2d``.

      .. attr:: newer-than

         If present, the approval must be newer than this amount
         of time to match.  Same format as "older-than".

      Any other field is interpreted as a review category and value
      pair.  For example ``Verified: 1`` would require that the
      approval be for a +1 vote in the "Verified" column.  The value
      may either be a single value or a list: ``Verified: [1, 2]``
      would match either a +1 or +2 vote.

   .. attr:: open

      A boolean value (``true`` or ``false``) that indicates whether
      the change must be open or closed in order to be rejected.

   .. attr:: current-patchset

      A boolean value (``true`` or ``false``) that indicates whether the
      change must be the current patchset in order to be rejected.

   .. attr:: wip

      A boolean value (``true`` or ``false``) that indicates whether the
      change must be wip or not wip in order to be rejected.

   .. attr:: status

      A string value that corresponds with the status of the change
      reported by Gerrit.

   .. attr:: hashtags

      A regex or list of regexes.  If any of these match at least
      one of the hashtags present on the change, it will be rejected.


Reference Pipelines Configuration
---------------------------------

Here is an example of standard pipelines you may want to define:

.. literalinclude:: /examples/pipelines/gerrit-reference-pipelines.yaml
   :language: yaml


Checks Plugin Support (Deprecated)
------------------------------------

The Gerrit driver has support for Gerrit's `checks` plugin.  Due to
the deprecation of the checks plugin in Gerrit, support in Zuul is
also deprecated and likely to be removed in a future version.  It is
not recommended for use.

Caveats include (but are not limited to):

* This documentation is brief.

* Access control for the `checks` API in Gerrit depends on a single
  global administrative permission, ``administrateCheckers``.  This is
  required in order to use the `checks` API and can not be restricted
  by project.  This means that any system using the `checks` API can
  interfere with any other.

* Checkers are restricted to a single project.  This means that a
  system with many projects will require many checkers to be defined
  in Gerrit -- one for each project+pipeline.

* No support is provided for attaching checks to tags or commits,
  meaning that tag, release, and post pipelines are unable to be used
  with the `checks` API and must rely on `stream-events`.

* Sub-checks are not implemented yet, so in order to see the results
  of individual jobs on a change, users must either follow the
  buildset link, or the pipeline must be configured to leave a
  traditional comment.

* Familiarity with the `checks` API is recommended.

* Checkers may not be permanently deleted from Gerrit (only
  "soft-deleted" so they no longer apply), so any experiments you
  perform on a production system will leave data there forever.

In order to use the `checks` API, you must have HTTP access configured
in `zuul.conf`.

There are two ways to configure a pipeline for the `checks` API:
directly referencing the checker UUID, or referencing it's scheme.  It
is hoped that once multi-repository checks are supported, that an
administrator will be able to configure a single checker in Gerrit for
each Zuul pipeline, and those checkers can apply to all repositories.
If and when that happens, we will be able to reference the checker
UUID directly in Zuul's pipeline configuration.  If you only have a
single project, you may find this approach acceptable now.

To use this approach, create a checker named ``zuul:check`` and
configure a pipeline like this:

.. code-block:: yaml

   - pipeline:
       name: check
       manager: independent
       trigger:
         gerrit:
           - event: pending-check
             uuid: 'zuul:check'
       enqueue:
         gerrit:
           checks-api:
             uuid: 'zuul:check'
             state: SCHEDULED
             message: 'Change has been enqueued in check'
       start:
         gerrit:
           checks-api:
             uuid: 'zuul:check'
             state: RUNNING
             message: 'Jobs have started running'
       no-jobs:
         gerrit:
           checks-api:
             uuid: 'zuul:check'
             state: NOT_RELEVANT
             message: 'Change has no jobs configured'
       success:
         gerrit:
           checks-api:
             uuid: 'zuul:check'
             state: SUCCESSFUL
             message: 'Change passed all voting jobs'
       failure:
         gerrit:
           checks-api:
             uuid: 'zuul:check'
             state: FAILED
             message: 'Change failed'

For a system with multiple repositories and one or more checkers for
each repository, the `scheme` approach is recommended.  To use this,
create a checker for each pipeline in each repository.  Give them
names such as ``zuul_check:project1``, ``zuul_gate:project1``,
``zuul_check:project2``, etc.  The part before the ``:`` is the
`scheme`.  Then create a pipeline like this:

.. code-block:: yaml

   - pipeline:
       name: check
       manager: independent
       trigger:
         gerrit:
           - event: pending-check
             scheme: 'zuul_check'
       enqueue:
         gerrit:
           checks-api:
             scheme: 'zuul_check'
             state: SCHEDULED
             message: 'Change has been enqueued in check'
       start:
         gerrit:
           checks-api:
             scheme: 'zuul_check'
             state: RUNNING
             message: 'Jobs have started running'
       no-jobs:
         gerrit:
           checks-api:
             scheme: 'zuul_check'
             state: NOT_RELEVANT
             message: 'Change has no jobs configured'
       success:
         gerrit:
           checks-api:
             scheme: 'zuul_check'
             state: SUCCESSFUL
             message: 'Change passed all voting jobs'
       failure:
         gerrit:
           checks-api:
             scheme: 'zuul_check'
             state: FAILED
             message: 'Change failed'

This will match and report to the appropriate checker for a given
repository based on the scheme you provided.

.. The original design doc may be of use during development:
   https://gerrit-review.googlesource.com/c/gerrit/+/214733
