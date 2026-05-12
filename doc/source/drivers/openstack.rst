:title: OpenStack Driver

OpenStack
=========

Zuul can use OpenStack clouds as a source for build nodes.

Information about OpenStack clouds, including authentication
information, may be provided via a configuration file (e.g.,
``clouds.yaml``) or environment variables.  See the `OpenStack SDK`_
for more information.

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

.. _openstack-driver-node-properties:

Node properties
---------------

Nodes provided via the OpenStack driver provide the following
``node_properties``:

.. var:: server_id

   The OpenStack server ID of the provided node.

.. _`OpenStack SDK`: https://docs.openstack.org/openstacksdk/latest/user/config/index.html
