:title: OpenStack Driver

OpenStack
=========

Zuul can use OpenStack clouds as a source for build nodes.

Information about OpenStack clouds, including authentication
information, may be provided via a configuration file (e.g.,
``clouds.yaml``) or environment variables.  See the `OpenStack SDK`_
for more information.

Image uploads are supported using the
:value:`provider[openstack].images[zuul].upload-methods.upload` and
:value:`provider[openstack].images[zuul].upload-methods.copy` methods.
The :value:`provider[openstack].images[zuul].upload-methods.import`
method is not currently supported.  In order for Zuul to use the
`copy` method, the image must be created as a `shared` image by adding
``visibility: shared`` to
:attr:`provider[openstack].images[zuul].tags`.

Connection Configuration
------------------------

The supported options in ``zuul.conf`` connections are:

.. attr:: <openstack connection>

   .. attr:: driver
      :required:

      .. value:: openstack

         The connection must set ``driver=openstack`` for OpenStack connections.

   .. attr:: client_config_file

      A path to a configuration file (e.g., ``clouds.yaml``) with
      connection information for one or more OpenStack clouds.

   .. attr:: cloud
      :required:

      The name of the OpenStack cloud (as it appears in the client config file).

   .. attr:: rate
      :default: 2

      The API rate limit (in requests per second) to use when
      performing API calls with this OpenStack cloud.

Provider Configuration
----------------------

The ``openstack`` driver adds the following options to the :attr:`provider`
and :attr:`section` configurations:

.. include:: openstack-attrs.rstinc

.. _`OpenStack SDK`: https://docs.openstack.org/openstacksdk/latest/user/config/index.html
