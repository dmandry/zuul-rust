:title: MQTT Driver

MQTT
====

The MQTT driver by default operates as a reporter only. It can optionally
be configured with a subscription topic that will listen for events
that trigger pipelines.

Connection Configuration
------------------------

.. attr:: <mqtt connection>

   .. attr:: driver
      :required:

      .. value:: mqtt

         The connection must set ``driver=mqtt`` for MQTT connections.

   .. attr:: server
      :default: localhost

      MQTT server hostname or address to use.

   .. attr:: port
      :default: 1883

      MQTT server port.

   .. attr:: keepalive
      :default: 60

      Maximum period in seconds allowed between communications with the broker.

   .. attr:: user

      Set a username for optional broker authentication.

   .. attr:: password

      Set a password for optional broker authentication.

   .. attr:: ca_certs

      A string path to the Certificate Authority certificate files to enable
      TLS connection.

   .. attr:: certfile

      A strings pointing to the PEM encoded client certificate to
      enable client TLS based authentication. This option requires keyfile to
      be set too.

   .. attr:: keyfile

      A strings pointing to the PEM encoded client private keys to
      enable client TLS based authentication. This option requires certfile to
      be set too.

   .. attr:: ciphers

      A string specifying which encryption ciphers are allowable for this
      connection. More information in this
      `openssl doc <https://www.openssl.org/docs/manmaster/man1/ciphers.html>`_.

   .. attr:: subscribe_topic

      A high level topic subscription for event triggering. Individual pipeline
      event triggers can be configured to filter on concrete topic values using
      regexes. In order to feed these events and their topics into the system
      you need to set an MQTT topic subscription here that will match them.

      If this is unset then the MQTT driver does not subscribe to any topics
      and will act as a reporter only.

Reporter Configuration
----------------------

A :ref:`connection<connections>` that uses the mqtt driver must be supplied to the
reporter. Each pipeline must provide a topic name. For example:

.. code-block:: yaml

   - pipeline:
       name: check
       success:
         mqtt:
           topic: "{tenant}/zuul/{pipeline}/{project}/{branch}/{change}"
           qos: 2


.. attr:: pipeline.<reporter>.<mqtt>

   To report via MQTT message, the dictionaries passed to any of the pipeline
   :ref:`reporter<reporters>` support the following attributes:

   .. attr:: topic

      The MQTT topic to publish messages. The topic can be a format string that
      can use the following parameters: ``tenant``, ``pipeline``, ``project``,
      ``branch``, ``change``, ``patchset`` and ``ref``.
      MQTT topic can have hierarchy separated by ``/``, more details in this
      `doc <https://mosquitto.org/man/mqtt-7.html>`_

   .. attr:: qos
      :default: 0

      The quality of service level to use, it can be 0, 1 or 2. Read more in this
      `guide <https://www.hivemq.com/blog/mqtt-essentials-part-6-mqtt-quality-of-service-levels>`_

   .. attr:: include-returned-data
      :default: false

      If set to ``true``, Zuul will include any data returned from the
      job via :ref:`return_values`.

Reporter Message Schema
-----------------------

An MQTT report uses this schema:

.. attr:: <mqtt schema>

   .. attr:: uuid

      The item UUID.  Each item enqueued into a Zuul pipeline is
      assigned a UUID which remains the same even as Zuul's
      speculative execution algorithm re-orders pipeline contents.

   .. attr:: action

      The reporter action name, e.g.: 'start', 'success', 'failure',
      'merge-conflict', ...

   .. attr:: tenant

      The tenant name.

   .. attr:: pipeline

      The pipeline name.

   .. attr:: project

      The project name.

   .. attr:: branch

      The branch name.

   .. attr:: change_url

      The change url.

   .. attr:: message

      The report message.

   .. attr:: change

      The change number.

   .. attr:: patchset

      The patchset number.

   .. attr:: commit_id

      The commit id number.

   .. attr:: owner

      The owner username of the change.

   .. attr:: ref

      The change reference.

   .. attr:: zuul_ref

      The internal zuul change reference.

   .. attr:: trigger_time

      The timestamp when the event was added to the scheduler.

   .. attr:: enqueue_time

      The timestamp when the event was added to the pipeline.

   .. attr:: buildset

      The buildset information.

      .. value:: uuid

      The buildset global uuid.

      .. attr:: result

      The buildset result

      .. attr:: builds

         The list of builds.

         .. attr:: job_name

            The job name.

         .. attr:: voting

            The job voting status.

         .. attr:: uuid

            The build uuid (not present in start report).

         .. attr:: execute_time

            The build execute time.

         .. attr:: start_time

            The build start time (not present in start report).

         .. attr:: end_time

            The build end time (not present in start report).

         .. attr:: log_url

            The build log url (not present in start report).

         .. attr:: web_url

            The url to the build result page.  Not present in start
            report.

         .. attr:: result

            The build results (not present in start report).

         .. attr:: artifacts
            :type: list

            The build artifacts (not present in start report).

            This is a list of dictionaries corresponding to the returned artifacts.

            .. attr:: name

               The name of the artifact.

            .. attr:: url

               The url of the artifact.

            .. attr:: metadata
               :type: dict

               The metadata of the artifact.  This is a dictionary of
               arbitrary key values determined by the job.

Here is an example of a start message:

.. code-block:: javascript

  {
    'action': 'start',
    'tenant': 'openstack.org',
    'pipeline': 'check',
    'project': 'sf-jobs',
    'branch': 'master',
    'change_url': 'https://gerrit.example.com/r/3',
    'message': 'Starting check jobs.',
    'trigger_time': '1524801056.2545864',
    'enqueue_time': '1524801093.5689457',
    'change': '3',
    'patchset': '1',
    'commit_id': '2db20c7fb26adf9ac9936a9e750ced9b4854a964',
    'owner': 'username',
    'ref': 'refs/changes/03/3/1',
    'zuul_ref': 'Zf8b3d7cd34f54cb396b488226589db8f',
    'buildset': {
      'uuid': 'f8b3d7cd34f54cb396b488226589db8f',
      'builds': [{
        'job_name': 'linters',
        'voting': True
      }],
    },
  }


Here is an example of a success message:

.. code-block:: javascript

  {
    'action': 'success',
    'tenant': 'openstack.org',
    'pipeline': 'check',
    'project': 'sf-jobs',
    'branch': 'master',
    'change_url': 'https://gerrit.example.com/r/3',
    'message': 'Build succeeded.',
    'trigger_time': '1524801056.2545864',
    'enqueue_time': '1524801093.5689457',
    'change': '3',
    'patchset': '1',
    'commit_id': '2db20c7fb26adf9ac9936a9e750ced9b4854a964',
    'owner': 'username',
    'ref': 'refs/changes/03/3/1',
    'zuul_ref': 'Zf8b3d7cd34f54cb396b488226589db8f',
    'buildset': {
      'uuid': 'f8b3d7cd34f54cb396b488226589db8f',
      'builds': [{
        'job_name': 'linters',
        'voting': True
        'uuid': '16e3e55aca984c6c9a50cc3c5b21bb83',
        'execute_time': 1524801120.75632954,
        'start_time': 1524801179.8557224,
        'end_time': 1524801208.928095,
        'log_url': 'https://logs.example.com/logs/3/3/1/check/linters/16e3e55/',
        'web_url': 'https://tenant.example.com/t/tenant-one/build/16e3e55aca984c6c9a50cc3c5b21bb83/',
        'result': 'SUCCESS',
        'dependencies': [],
        'artifacts': [],
      }],
    },
  }

Trigger Configuration
---------------------

.. attr:: pipeline.trigger.<mqtt trigger>

   The dictionary passed to the MQTT pipeline ``trigger`` attribute
   supports the following attributes:

   .. attr:: topic
      :required:
      :type: str

      The topic of the event that should trigger this pipeline. This can
      be supplied as a regex to match multiple topics. Note that this is
      different to MQTT topic matching rules.

   .. attr:: payload_required
      :type: dict

      A dictionary of jsonpath expression keys to values. All jsonpath
      keys must be found in the MQTT event payload and have matching values
      in the MQTT payload. If these keys are not found or the values do not
      match then the MQTT event is filtered out and does not trigger the
      pipeline.

   .. attr:: data_mapping
      :required:
      :type: dict

      Because MQTT is arbitrary and may differ from source to source on the
      same message bus we need to map the data in the MQTT event payload to
      data that Zuul knows how to handle. In particular Zuul needs two major
      pieces of information.

      First is the source of the "change" event. To lookup source info we need
      to map either ``project-canonical-name`` or ``project-hostname`` and
      ``project`` from the MQTT payload to Zuul. To do that add key value entries
      where the keys are ``project-canonical-name`` or ``project-hostname`` and
      ``project`` and the values are jsonpath expressions. These jsonpath
      expressions will be looked up in the payload and the resulting value is
      assigned to ``project-canonical-name`` etc.

      Next we need to know what "change" should be enqueued into the pipeline.
      For this information we need to map either a ``ref`` or ``change`` (with
      optional ``patchset``) from MQTT event payload into Zuul. To do this
      add key value entries where the keys are ``ref`` or ``change`` and the
      values are jsonpath expressions. These jsonpath expressions will be looked
      up in the payload and the resulting value is assigned to ``ref`` etc.

      If any of these jsonpath expressions fail the entire MQTT event is ignored.
      If a minimal subset of these required attributes is not properly mapped then
      the entire MQTT event is ignored.

   .. attr:: debug
      :type: bool
      :default: False

      Because MQTT payload data and how jsonpath may apply against that payload
      is somewhat opqaue triggers can be annotated with the debug flag to add
      additional debug logging around these events. Note that this logging occurs
      in the Zuul scheduler and won't be exposed to end users.

   For example::

     - topic: some/topic/path
       payload_required:
         "$.action": enqueue
       data_mapping:
         project: "$.project_name"
         project-hostname: "$.project_hostname"
         change: "$.change"
         patchset: "$.patchset"
