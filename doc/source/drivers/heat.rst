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

   # Top-level image definition
   - image:
       name: bastion-image
       type: cloud

   # Top-level label definition
   - label:
       name: heat-lab
       flavor: lab-flavor
       image: bastion-image

   # Section with connection and detailed configuration
   - section:
       name: heat
       connection: heat-cloud
       region: regionOne
       label-defaults:
         key-name: zuul-keypair
       flavors:
         - name: lab-flavor
           max-floating-ips: 1
       images:
         - name: bastion-image
           image-id: "abc123-glance-uuid"
           username: zuul
           connection-type: ssh

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

Heat templates are stored in the project repository and referenced via
``heat-template-path`` in the nodeset's ``node-properties``.  The path
is relative to the repository root.  During configuration loading, Zuul
looks up the template content from files already fetched by the merger
and passes it to the Heat API at stack creation time.

Declaring template directories
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For Zuul to fetch the template files, the directories containing them
must be declared using the ``heat-template-dirs`` option in the tenant
configuration:

.. code-block:: yaml

   - tenant:
       name: my-tenant
       source:
         gerrit:
           untrusted-projects:
             - my-project:
                 heat-template-dirs:
                   - zuul.heat.d

This tells Zuul to include all YAML files under ``zuul.heat.d/`` when
fetching repository content.  Because the files are fetched from the
**merged commit** state, templates added or modified in a proposed
change are resolved correctly — no separate fetch step is required.

See :attr:`tenant.untrusted-projects.<project>.heat-template-dirs`
for the full option reference.

Referencing templates in nodesets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Once the directories are declared, reference individual templates in
nodeset ``node-properties``:

.. code-block:: yaml

   - nodeset:
       name: heat-nodeset
       nodes:
         - name: controller
           label: heat-lab
           node-properties:
             heat-template-path: zuul.heat.d/lab-stack.yaml

Zuul-injected template parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Heat driver automatically passes the following parameters to every
stack creation.  Templates **must** declare any parameter they receive —
Heat rejects stack creation if it receives undeclared parameters.

``zuul_key_name``
  The name of the cloud keypair (from the label's ``key-name`` setting).
  Assign this to the bastion's ``key_name`` property so the Zuul
  executor can SSH in.  Only set when ``key-name`` is configured on the
  label.

``zuul_username``
  The SSH username (from the image's ``username`` setting).  Use this in
  cloud-init ``users`` directives to create the correct account.  Only
  set when ``username`` is configured on the image.

``zuul_image``
  The Glance image ID or name for the bastion node.  For cloud images
  this comes from the section-level ``image-id``; for Zuul-managed
  images it is resolved automatically by the launcher.  Only set when
  an image reference is available.

``zuul_ephemeral_public_key``
  An Ed25519 public key in OpenSSH format, generated fresh for each
  stack.  Use this with ``OS::Nova::KeyPair`` (as the ``public_key``
  property) or inject it via cloud-init ``ssh_authorized_keys`` on
  internal nodes.

``zuul_ephemeral_private_key``
  The corresponding Ed25519 private key in PEM format.  Inject this
  onto the bastion via cloud-init ``write_files`` so it can SSH to
  internal nodes.  Mark the parameter ``hidden: true`` in the template
  to prevent it from appearing in stack show output.

Templates must also output ``bastion_ip`` for Zuul connectivity.

Example template:

.. code-block:: yaml

   heat_template_version: 2015-04-30

   parameters:
     zuul_key_name:
       type: string
       description: Cloud keypair name injected by Zuul for executor access
     zuul_username:
       type: string
       default: zuul
       description: Username injected by Zuul for SSH access
     zuul_image:
       type: string
       description: Glance image for the bastion node
     zuul_ephemeral_public_key:
       type: string
       description: Ed25519 public key for internal node access
     zuul_ephemeral_private_key:
       type: string
       hidden: true
       description: Ed25519 private key for bastion to reach internal nodes

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
         flavor: m1.small
         image: { get_param: zuul_image }
         key_name: { get_param: zuul_key_name }
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

Multi-host SSH Access
---------------------

Heat stacks often contain multiple instances — for example, a bastion
(jump host) that Zuul connects to and one or more internal nodes on a
private network. The executor needs to reach the bastion via the cloud
keypair (``zuul_key_name``), and the bastion needs to reach internal
nodes via a separate, per-stack keypair.

The Heat driver generates an ephemeral Ed25519 keypair for each stack
and passes it as ``zuul_ephemeral_public_key`` and
``zuul_ephemeral_private_key`` template parameters.  There are two
approaches to using them:

Using OS::Nova::KeyPair
~~~~~~~~~~~~~~~~~~~~~~~

Create an ``OS::Nova::KeyPair`` with the public key (compatible with
Nova API >= 2.92 which requires ``public_key``), assign it as
``key_name`` on internal servers, and inject the private key onto the
bastion via cloud-init ``write_files``.  This approach is convenient
when you have many internal nodes since they only need
``key_name: { get_resource: internal_keypair }``.

Example template with bastion and internal node:

.. code-block:: yaml

   heat_template_version: 2015-04-30

   parameters:
     zuul_key_name:
       type: string
       description: Cloud keypair name injected by Zuul for executor access
     zuul_username:
       type: string
       default: zuul
       description: Username injected by Zuul for SSH access
     zuul_image:
       type: string
       description: Glance image for the bastion node
     zuul_ephemeral_public_key:
       type: string
       description: Ed25519 public key for internal node access
     zuul_ephemeral_private_key:
       type: string
       hidden: true
       description: Ed25519 private key for bastion to reach internal nodes

   resources:
     internal_keypair:
       type: OS::Nova::KeyPair
       properties:
         name:
           str_replace:
             template: $STACK_NAME-internal
             params:
               $STACK_NAME: { get_param: "OS::stack_name" }
         public_key: { get_param: zuul_ephemeral_public_key }

     private_net:
       type: OS::Neutron::Net

     private_subnet:
       type: OS::Neutron::Subnet
       properties:
         network: { get_resource: private_net }
         cidr: 192.168.100.0/24

     bastion_config:
       type: OS::Heat::CloudConfig
       properties:
         cloud_config:
           users:
             - name: { get_param: zuul_username }
           write_files:
             # defer ensures the file is written after user creation,
             # so the .ssh directory has correct ownership.
             - defer: true
               path:
                 str_replace:
                   template: /home/$USER/.ssh/id_ed25519
                   params:
                     $USER: { get_param: zuul_username }
               permissions: '0600'
               owner:
                 str_replace:
                   template: $USER:$USER
                   params:
                     $USER: { get_param: zuul_username }
               content: { get_param: zuul_ephemeral_private_key }

     bastion_port:
       type: OS::Neutron::Port
       properties:
         network: { get_resource: private_net }
         fixed_ips:
           - subnet: { get_resource: private_subnet }

     bastion:
       type: OS::Nova::Server
       properties:
         flavor: m1.small
         image: { get_param: zuul_image }
         key_name: { get_param: zuul_key_name }
         networks:
           - port: { get_resource: bastion_port }
         user_data: { get_resource: bastion_config }
         user_data_format: RAW

     internal_node:
       type: OS::Nova::Server
       properties:
         flavor: m1.small
         image: { get_param: zuul_image }
         key_name: { get_resource: internal_keypair }
         networks:
           - network: { get_resource: private_net }

     bastion_fip:
       type: OS::Neutron::FloatingIP
       properties:
         floating_network: public
         port_id: { get_resource: bastion_port }

   outputs:
     bastion_ip:
       value: { get_attr: [bastion_fip, floating_ip_address] }

In this template:

* The **bastion** uses ``zuul_key_name`` (the cloud keypair injected by
  Zuul) so the executor can SSH in, and receives the ephemeral private
  key via ``write_files`` so it can reach internal nodes.
* **Internal nodes** use the ``internal_keypair`` as their ``key_name``,
  allowing the bastion to SSH to them using the injected private key.
* The ``OS::Nova::KeyPair`` resource receives ``public_key`` from the
  ``zuul_ephemeral_public_key`` parameter, which is compatible with
  Nova API >= 2.92 (which no longer allows server-side key generation).
* The ``zuul_image`` parameter provides the Glance image for server
  resources. It is set from the image's ``image-id`` (cloud images) or
  resolved automatically by the launcher (Zuul-managed images).
* The ``zuul_username`` parameter ensures the correct user account is
  created on all nodes via cloud-init.
* The ephemeral keypair is generated fresh for each stack and discarded
  when the stack is deleted.

Using cloud-init only
~~~~~~~~~~~~~~~~~~~~~

Alternatively, skip ``OS::Nova::KeyPair`` entirely and distribute keys
purely via cloud-init.  This avoids the Nova keypair API, which can be
useful on clouds where ``OS::Nova::KeyPair`` is restricted or
unavailable.

Remove the ``internal_keypair`` resource from the template above and
replace the ``internal_node`` with a cloud-init user data block that
injects the public key via ``ssh_authorized_keys``:

.. code-block:: yaml

     internal_config:
       type: OS::Heat::CloudConfig
       properties:
         cloud_config:
           users:
             - default
             - name: { get_param: zuul_username }
               sudo: ALL=(ALL) NOPASSWD:ALL
               ssh_authorized_keys:
                 - { get_param: zuul_ephemeral_public_key }

     internal_node:
       type: OS::Nova::Server
       properties:
         flavor: m1.small
         image: { get_param: zuul_image }
         networks:
           - network: { get_resource: private_net }
         user_data: { get_resource: internal_config }
         user_data_format: RAW

The bastion configuration (``write_files`` with the private key)
remains the same.  Internal nodes no longer need a Nova keypair —
they receive the public key directly through cloud-init.

Resource Cleanup
----------------

Test-created resources (images, volumes) must be tagged with
``zuul_node_uuid`` and ``zuul_system_id`` properties for automatic cleanup:

.. code-block:: bash

   openstack image create --property zuul_node_uuid=$ZUUL_NODE_UUID \
     --property zuul_system_id=$ZUUL_SYSTEM_ID test-image

Template Limitations
--------------------

Zuul sends a single template body to the Heat ``create_stack`` API.  It
does not pre-process the template or bundle additional files, so the
following multi-file features are **not supported**:

* ``get_file`` — requires a client-side ``files`` dict that Zuul does
  not populate.
* Resource registry overrides (environment files) — requires the
  ``environment`` and ``files`` API parameters.
* Nested stacks (``type: nested/file.yaml``) — resolved from the
  ``files`` dict, same as ``get_file``.

All of these depend on the Heat client reading files relative to the
template on disk and sending them alongside the template.  Since Zuul
fetches only the file at ``heat-template-path`` from the repository,
there is no local filesystem context to resolve against.

To work within these constraints, keep all resources in a single
self-contained template.  Inline any file content using
``OS::Heat::CloudConfig``, ``str_replace``, or multi-line YAML scalars.

.. _`OpenStack SDK`: https://docs.openstack.org/openstacksdk/latest/
