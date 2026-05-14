:title: Heat Driver

.. _heat:

Heat
====

Zuul can use OpenStack Heat as a source for build nodes. Heat orchestrates
complex environments with multiple instances, private networks, and volumes
using Heat Orchestration Templates (HOT). Each Heat stack corresponds to a
single Zuul node (typically a bastion host).

Connection Configuration
------------------------

Authentication configuration is provided via the `OpenStack SDK`_ (e.g.,
``clouds.yaml``).

The supported options in ``zuul.conf`` connections are:

.. attr:: <heat connection>

   .. attr:: driver
      :required:

      .. value:: heat

         The connection must set ``driver=heat`` for Heat connections.

   .. attr:: client_config_file

      A path to a configuration file (e.g., ``clouds.yaml``) with
      connection information for one or more OpenStack clouds.

   .. attr:: cloud
      :required:

      The name of the OpenStack cloud (as it appears in the client config
      file).

   .. attr:: rate
      :default: 2

      The API rate limit (in requests per second) to use when
      performing API calls with this OpenStack cloud.

Provider Configuration
----------------------

The ``heat`` driver creates Heat stacks from templates stored in project
repositories. Templates define the complete environment (compute, network,
storage). Resource limits and policies can be enforced per-provider.

Heat provider configuration follows Zuul's standard provider model:

* Top-level ``flavor``, ``image``, and ``label`` definitions declare the
  basic objects
* A ``section`` defines the connection and detailed configuration (including
  flavor limits, image mappings, and label attributes)
* A ``provider`` references the section and specifies which labels to expose,
  along with optional resource-limits and resource-policy

The ``heat`` driver adds the following options to the :attr:`provider`
and :attr:`section` configurations:

.. include:: heat-attrs.rstinc

Example provider configuration with resource policy:

.. code-block:: yaml

   # Top-level flavor definition
   - flavor:
       name: lab-flavor

   # Top-level image definition (placeholder for Heat templates)
   - image:
       name: heat-placeholder
       type: cloud

   # Top-level label definition
   - label:
       name: heat-lab
       flavor: lab-flavor
       image: heat-placeholder

   # Section with connection and detailed configuration
   - section:
       name: heat
       connection: heat-cloud
       flavors:
         - name: lab-flavor
           max-floating-ips: 1
       images:
         - name: heat-placeholder
       labels:
         - name: heat-lab
           region: regionOne

   # Provider references section and applies resource limits/policy
   - provider:
       name: heat-provider
       section: heat
       resource-limits:
         stacks: 10
       resource-policy:
         disallowed-resource-types:
           - OS::Heat::ResourceGroup
       labels:
         - name: heat-lab

Template Usage
--------------

Heat templates must be stored in the project repository and specified via
``node-properties`` in the nodeset configuration:

.. code-block:: yaml

   - nodeset:
       name: heat-nodeset
       nodes:
         - name: controller
           label: heat-lab
           node-properties:
             heat-template-path: .zuul/heat/lab-stack.yaml

Templates must output ``bastion_ip`` for Zuul connectivity:

.. code-block:: yaml

   heat_template_version: 2015-04-30

   parameters:
     bastion_flavor:
       type: string
     bastion_image:
       type: string

   resources:
     private_net:
       type: OS::Neutron::Net

     private_subnet:
       type: OS::Neutron::Subnet
       properties:
         network: { get_resource: private_net }
         cidr: 192.168.100.0/24

     bastion_port:
       type: OS::Neutron::Port
       properties:
         network: { get_resource: private_net }
         fixed_ips:
           - subnet: { get_resource: private_subnet }

     bastion:
       type: OS::Nova::Server
       properties:
         flavor: { get_param: bastion_flavor }
         image: { get_param: bastion_image }
         networks:
           - port: { get_resource: bastion_port }

     bastion_fip:
       type: OS::Neutron::FloatingIP
       properties:
         floating_network: public
         port_id: { get_resource: bastion_port }

   outputs:
     bastion_ip:
       value: { get_attr: [bastion_fip, floating_ip_address] }

Resource Cleanup
----------------

Test-created resources (images, volumes) must be tagged with
``zuul_node_uuid`` and ``zuul_system_id`` properties for automatic cleanup:

.. code-block:: bash

   openstack image create --property zuul_node_uuid=$ZUUL_NODE_UUID \
     --property zuul_system_id=$ZUUL_SYSTEM_ID test-image

.. _`OpenStack SDK`: https://docs.openstack.org/openstacksdk/latest/
