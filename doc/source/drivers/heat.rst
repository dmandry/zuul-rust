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

The ``heat`` driver adds the following options to the :attr:`provider`
and :attr:`section` configurations:

.. include:: heat-attrs.rstinc

Template Usage
--------------

Heat templates are stored in the project repository and referenced via
``heat-template-path`` in the nodeset's ``template-config``.  The path
is relative to the repository root.

For Zuul to fetch the template files, the directories containing them
must be declared using ``heat-template-dirs`` in the tenant
configuration.  See
:attr:`tenant.untrusted-projects.<project>.heat-template-dirs`
for the full option reference.

.. code-block:: yaml

   - tenant:
       name: my-tenant
       source:
         gerrit:
           untrusted-projects:
             - my-project:
                 heat-template-dirs:
                   - zuul.heat.d

Reference templates in nodeset ``template-config``:

.. code-block:: yaml

   - nodeset:
       name: heat-nodeset
       nodes:
         - name: controller
           label: heat-lab
           template-config:
             heat-template-path: zuul.heat.d/lab-stack.yaml
             heat-parameters:
               my_param: my-value

.. _heat-injected-parameters:

Zuul-injected Template Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Heat driver passes the following parameters to the Heat API **only
when the template declares them**.  These take precedence over
user-defined ``heat-parameters`` with the same name.

``zuul_key_name``
  The cloud keypair name (from the label's ``key-name``).

``zuul_username``
  The SSH username (from the image's ``username``).

``zuul_image``
  The Glance image ID or name (from the section-level ``image-id``).

``zuul_external_network``
  The external network for floating IPs.  Uses the label's
  ``external-network`` if configured, otherwise auto-discovers from
  Neutron.

``zuul_ephemeral_public_key``
  An Ed25519 public key in OpenSSH format, generated fresh for each
  stack.  Use in cloud-init ``ssh_authorized_keys`` on internal nodes.

``zuul_ephemeral_private_key``
  The corresponding Ed25519 private key in PEM format.  Use in
  cloud-init ``write_files`` on the bastion.  Mark ``hidden: true``
  in the template.

Templates must output ``bastion_ip`` for Zuul connectivity.

Accessing Stack Outputs
-----------------------

All outputs defined in the Heat template are available to Ansible
playbooks via ``nodepool.node_properties.heat_stack_outputs``.

The ``bastion_ip`` output is required.  The driver uses it to set the
node's connection address.  All other outputs are optional.

Limitations
-----------

- The Heat driver does not implement node snapshots.  The
  ``snapshot-timeout`` and ``snapshot-expiration`` label attributes are
  inherited from base schemas but have no effect.

- The Heat driver only supports ``type: cloud`` images (pre-existing
  Glance images).  It does not support ``type: zuul`` images
  (Zuul-managed image uploads).

Template Limitations
~~~~~~~~~~~~~~~~~~~~

The following template features are unconditionally rejected,
regardless of ``template-policy`` settings:

* ``external_id`` property on any resource.  External IDs reference
  pre-existing resources that Zuul does not own.
* ``deletion_policy`` set to ``Retain`` or ``Snapshot``.  These would
  leak resources after stack deletion.
* ``get_file`` intrinsic function.  It requires a ``files`` dict that
  Zuul does not populate.

The following resource types are rejected:

* ``OS::Heat::ResourceGroup``.  The quota parser cannot expand groups.
* ``OS::Heat::AutoScalingGroup``.  Dynamic resources break quota
  tracking.
* ``OS::Heat::Stack``.  Requires ``template_url`` or nested templates
  via the ``files`` API parameter.

Multi-file features such as ``get_file``, resource registry overrides
(environment files), and nested stacks (``type: nested/file.yaml``)
all depend on the Heat ``files`` API parameter, which Zuul does not
use.  Keep all resources in a single self-contained template.

.. _`OpenStack SDK`: https://docs.openstack.org/openstacksdk/latest/
