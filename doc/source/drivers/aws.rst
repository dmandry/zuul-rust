:title: AWS Driver

.. _aws:

AWS
===

Zuul can use AWS as a source for build nodes.

If using the AWS driver to upload images, see `VM Import/Export
service role`_ for information on configuring the required permissions
in AWS.  You must also create an S3 Bucket for use by Zuul if
uploading images (except when using the ``ebs-direct`` upload method).

A number of methods for configuration authentication are available:

* Supplying values directly in ``zuul.conf``
* A shared credential file
* `Environment variables`_

Zuul will try to obtain credential information from those sources in
that order.

Connection Configuration
------------------------

The supported options in ``zuul.conf`` connections are:

.. attr:: <aws connection>

   .. attr:: driver
      :required:

      .. value:: aws

         The connection must set ``driver=aws`` for AWS connections.

   .. attr:: shared_credentials_file

      A path to a `configuration file`_ with shared access
      credentials.  If this is supplied, no other credential settings
      need to be present.

   .. attr:: access_key_id

      The AWS access key id.

   .. attr:: secret_access_key

      The AWS secret access key.

   .. attr:: profile

      The AWS profile.

   .. attr:: role_arn

      When using a federated web identity token, this specifies the
      AWS IAM role that should be assumed.  If this is specified, then
      :attr:`<aws connection>.web_identity_token_file` should be
      provided, and the access key settings should be omitted.

   .. attr:: web_identity_token_file

      The path to a file containing a federated web identity token.
      Generally created by a cloud or Kubernetes environment.

   .. attr:: endpoint_url

      If set, this will override the endpoint URLs for all AWS
      services.

   .. attr:: rate
      :default: 2

      The API rate limit (in requests per second) to use when
      performing API calls with AWS.

Provider Configuration
----------------------

The ``aws`` driver adds the following options to the :attr:`provider`
and :attr:`section` configurations:

.. include:: aws-attrs.rstinc


.. _aws-driver-node-properties:

Node properties
---------------

Nodes provided via the AWS driver provide the following :var:`node_properties`:

.. var:: instance_id

   The AWS instance ID of the provided node.

.. var:: host_id

   The AWS host ID (if a dedicated host was used).

.. var:: ebs_volume_id

   The volume ID of the EBS volume which is mounted as root device.

.. var:: fleet

   A boolean flag indicating if the node was created by using the AWS
   fleet API.

.. var:: spot

   A boolean flag indicating if the node is an AWS spot instance.

.. _`VM Import/Export service role`: https://docs.aws.amazon.com/vm-import/latest/userguide/vmie_prereqs.html#vmimport-role
.. _`configuration file`: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file
.. _`Environment variables`: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-environment-variables`
