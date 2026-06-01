:title: Heat Driver

.. _heat:

Heat
====

Zuul can use OpenStack Heat as a source for build nodes. Heat orchestrates
complex environments with multiple instances, private networks, and volumes
using Heat Orchestration Templates (HOT). Each Heat stack corresponds to a
single Zuul node (typically a bastion host).

Limitations
-----------

- **Snapshots not supported**: The Heat driver does not implement node snapshots.
  The ``snapshot-timeout`` and ``snapshot-expiration`` label attributes are inherited
  from base schemas but have no effect.

- **Only cloud images**: The Heat driver only supports ``type: cloud`` images
  (pre-existing Glance images). It does not support ``type: zuul`` images
  (Zuul-managed image uploads).

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
storage). Resource usage and capacity are controlled through three
mechanisms: global resource-limits (provider capacity), template-policy
(allowed resource types and intrinsic functions), and per-flavor max-*
limits (per-stack limits). See :ref:`heat-resource-management` for details.

Heat provider configuration follows Zuul's standard provider model:

* Top-level ``flavor``, ``image``, and ``label`` definitions declare the
  basic objects
* A ``section`` defines the connection and detailed configuration (including
  flavor limits, image mappings, and label attributes)
* A ``provider`` references the section and specifies which labels to expose,
  along with optional resource-limits and template-policy

The ``heat`` driver adds the following options to the :attr:`provider`
and :attr:`section` configurations:

.. include:: heat-attrs.rstinc

Example provider configuration with template policy:

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
         external-network: public
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
         instances: 40
         cores: 80
         ram: 163840
         floating-ips: 10
       template-policy:
         allowed-resource-types:
           # Only allow compute, network, and storage resources
           # (excluding OS::Nova::KeyPair and OS::Neutron::RBACPolicy)
           - ^OS::(Nova::(?!KeyPair$)|Neutron::(?!RBACPolicy$)|Cinder)::.*
         allowed-intrinsic-functions:
           # Only allow basic template functions
           - ^(get_param|get_resource|get_attr|str_replace)$
       labels:
         - name: heat-lab

Alternative approach using negative lookahead to block specific types:

.. code-block:: yaml

   - provider:
       name: heat-provider-restricted
       section: heat
       template-policy:
         allowed-resource-types:
           # Negative lookahead: allow everything except Keystone and Swift
           - ^(?!OS::Keystone::)(?!OS::Swift::).*
       labels:
         - name: heat-lab

.. _heat-resource-management:

Resource Management and Enforcement
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Heat driver provides three distinct enforcement mechanisms to control
resource usage and capacity:

**resource-limits** (Provider-level global capacity)
  Defines the total capacity available across *all stacks* managed by the
  provider.  These act as a quota ceiling shared by all concurrent builds.
  For example, ``cores: 80`` means the provider can launch stacks totaling
  up to 80 cores across all active stacks combined.  When capacity is
  exhausted, new node requests wait until resources are freed by stack
  deletion.  Resource limits are declared in the ``provider`` configuration
  and apply provider-wide.

**template-policy** (Template content constraints)
  Controls which Heat template features are permitted.  Template policy
  has two sub-options: ``allowed-resource-types`` and
  ``allowed-intrinsic-functions``.

  **allowed-resource-types**: Defines which Heat resource types are allowed
  in templates used by this provider. When configured, every resource in
  the template must match at least one pattern. If not specified, all
  resource types are allowed.

  Patterns are treated as Python regular expressions matched against the
  full resource type string (using fullmatch semantics). This enables
  flexible matching. Examples:

  - **Exact match**: ``OS::Nova::Server`` allows only that type
  - **Prefix match**: ``^OS::Nova::.*`` allows all Nova resources
  - **Complex pattern**:
    ``^OS::(Nova::(?!KeyPair$)|Neutron::(?!RBACPolicy$)|Cinder)::.*``
    allows compute, network, and storage but excludes
    ``OS::Nova::KeyPair`` and ``OS::Neutron::RBACPolicy``
  - **Negative lookahead**: ``^(?!OS::Keystone::).*`` allows
    everything except Keystone resources

  Common use cases:

  - Allow only compute, network, and storage (excluding keypairs and
    RBAC policies):
    ``^OS::(Nova::(?!KeyPair$)|Neutron::(?!RBACPolicy$)|Cinder)::.*``
  - Allow compute and network only (excluding keypairs and RBAC
    policies):
    ``^OS::(Nova::(?!KeyPair$)|Neutron::(?!RBACPolicy$))::.*``
  - Allow a single resource type: ``^OS::Nova::Server$``

  .. note::

     ``OS::Heat::ResourceGroup``, ``OS::Heat::AutoScalingGroup``, and
     ``OS::Heat::Stack`` are already unconditionally rejected (see
     `Unsupported resource types`_), so they do not need to be blocked
     via ``allowed-resource-types``.

  **allowed-intrinsic-functions**: Defines which HOT intrinsic functions
  are permitted in templates used by this provider. When configured,
  every intrinsic function call in the template must match at least one
  pattern. If not specified, all intrinsic functions are allowed.

  Common use cases:

  - Allow only basic functions:
    ``^(get_param|get_resource|get_attr|str_replace)$``
  - Block ``yaql`` (which can execute arbitrary expressions):
    ``^(?!yaql$).*``

  .. note::

     ``get_file`` is already unconditionally rejected at configuration
     load time (see `Security checks (unconditional)`_), so it does not
     need to be blocked via ``allowed-intrinsic-functions``.

  **Resource types and intrinsic functions with security implications:**

  .. important::

     This is not an exhaustive list — operators should review the full
     `Heat resource type reference
     <https://docs.openstack.org/heat/latest/template_guide/index.html>`_
     to evaluate their own requirements.  The recommended approach is to
     build a strict allow-list of the specific resource types and
     functions needed, rather than trying to exclude individual risky
     items from a broad allow-all policy.

  The following are not unconditionally blocked but have security
  implications that operators should consider when building allow-lists:

  *Resource types:*

  - ``OS::Nova::KeyPair`` — can overwrite or replace an existing Nova
    keypair in the cloud project.  If a template references a keypair
    name that already exists, it may mutate shared infrastructure.
    Use cloud-init ``ssh_authorized_keys`` instead (see
    `Multi-host SSH Access`_).
  - ``OS::Neutron::RBACPolicy`` — can share networks, QoS policies, or
    security groups with other OpenStack projects, granting access
    outside the Zuul-managed scope.

  *Intrinsic functions:*

  - ``yaql`` — executes arbitrary YaQL expressions inside the Heat
    engine, which can inspect and manipulate template data in ways that
    are difficult to audit.
  - ``make_url`` — constructs URLs from template data, which could be
    used to exfiltrate parameters (e.g. secrets passed via
    ``zuul_ephemeral_private_key``) to external endpoints if combined
    with other resources.

  Template policies are enforced at launch time (when provider config is
  available) and apply provider-wide. They are declared in the
  ``provider`` configuration.

**flavor max-*** (Per-stack resource limits)
  Defines the maximum resources a *single stack* can consume, regardless of
  provider capacity.  These act as per-stack limits to prevent any
  one build from monopolizing resources.  For example, ``max-cores: 8`` on
  a flavor means that even if the provider has 80 cores available, a single
  stack using this flavor cannot exceed 8 cores.  Flavor limits
  (``max-instances``, ``max-cores``, ``max-ram``, ``max-floating-ips``,
  ``max-volumes``, ``max-volume-gb``) are declared in the ``section``
  configuration under the ``flavors`` list.  They are enforced at stack
  creation time — before submitting a stack to Heat, the driver parses the
  template and compares resource counts against the flavor's per-stack
  limits.  If any limit is exceeded the node request fails immediately with
  a ``FlavorLimitExceeded`` error that lists the violated limits.

**Example enforcement scenario**:
  A provider is configured with ``resource-limits: {cores: 80}`` (global
  capacity), a flavor with ``max-cores: 8`` (per-stack limit), and
  ``template-policy: {allowed-resource-types: ['^OS::(Nova::(?!KeyPair$)|Neutron::(?!RBACPolicy$)|Cinder)::.*']}``
  (resource type constraint).  A template that uses ``OS::Keystone::User``
  will be rejected immediately due to template policy violation.  A template
  requesting 12 cores will fail due to flavor limit violation (exceeds 8).
  A template requesting 6 cores will only be created if the provider has at
  least 6 cores available from its global capacity pool of 80.  If 10 other
  stacks are already consuming 76 cores, the request will wait until enough
  capacity is freed.

Template Usage
--------------

Heat templates are stored in the project repository and referenced via
``heat-template-path`` in the nodeset's ``template-config``.  The path
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
nodeset ``template-config``:

.. code-block:: yaml

   - nodeset:
       name: heat-nodeset
       nodes:
         - name: controller
           label: heat-lab
           template-config:
             heat-template-path: zuul.heat.d/lab-stack.yaml

User-defined template parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Custom parameters can be passed to the Heat template using the
``heat-parameters`` key in ``template-config``.  These are merged into
the parameters dict sent to the Heat API at stack creation time.
The following Zuul-internal parameters take precedence over user-defined
values with the same name, but only when the template declares them:

* ``zuul_key_name``
* ``zuul_username``
* ``zuul_image``
* ``zuul_external_network``
* ``zuul_ephemeral_public_key``
* ``zuul_ephemeral_private_key``

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

The template must declare these parameters so that Heat accepts them:

.. code-block:: yaml

   parameters:
     my_param:
       type: string
       description: A user-defined parameter

Zuul-injected template parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Heat driver passes the following parameters to the Heat API **only
when the template declares them**.  Heat rejects stack creation if it
receives parameters that are not declared in the template, so the driver
inspects the template's ``parameters`` section and omits any ``zuul_*``
parameter that is absent.  Declare only the parameters the template
actually uses.

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
  The Glance image ID or name for the bastion node.  This comes from
  the section-level ``image-id``.  Only set when an image reference is
  available.

``zuul_external_network``
  The name of the external (provider) network for floating IPs.  When
  the label's ``external-network`` is configured, that value is used.
  Otherwise the driver auto-discovers the first external IPv4 network
  from Neutron.  Use this as the ``floating_network`` property on
  ``OS::Neutron::FloatingIP`` resources or for router external gateway
  configuration.  Only passed when declared in the template — omit this
  parameter from templates that do not use floating IPs.

``zuul_ephemeral_public_key``
  An Ed25519 public key in OpenSSH format, generated fresh for each
  stack.  Inject it via cloud-init ``ssh_authorized_keys`` on internal
  nodes.

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
     zuul_external_network:
       type: string
       description: External network for floating IPs (auto-discovered or configured)
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
         floating_network: { get_param: zuul_external_network }
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
``zuul_ephemeral_private_key`` template parameters.  Use
``OS::Heat::CloudConfig`` to distribute the keys via cloud-init:

* The **bastion** receives the ephemeral private key via ``write_files``
  so it can SSH to internal nodes.
* **Internal nodes** receive the ephemeral public key via
  ``ssh_authorized_keys`` so the bastion can connect.

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
     zuul_external_network:
       type: string
       description: External network for floating IPs
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

     internal_config:
       type: OS::Heat::CloudConfig
       properties:
         cloud_config:
           users:
             - name: { get_param: zuul_username }
               sudo: ALL=(ALL) NOPASSWD:ALL
               ssh_authorized_keys:
                 - { get_param: zuul_ephemeral_public_key }

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
         networks:
           - network: { get_resource: private_net }
         user_data: { get_resource: internal_config }
         user_data_format: RAW

     bastion_fip:
       type: OS::Neutron::FloatingIP
       properties:
         floating_network: { get_param: zuul_external_network }
         port_id: { get_resource: bastion_port }

   outputs:
     bastion_ip:
       value: { get_attr: [bastion_fip, floating_ip_address] }

In this template:

* The **bastion** uses ``zuul_key_name`` (the cloud keypair injected by
  Zuul) so the executor can SSH in, and receives the ephemeral private
  key via ``write_files`` so it can reach internal nodes.
* **Internal nodes** receive the ephemeral public key via
  ``ssh_authorized_keys`` — no ``OS::Nova::KeyPair`` resource is needed.
* The ``zuul_image`` parameter provides the Glance image for server
  resources. It is set from the image's ``image-id``.
* The ``zuul_username`` parameter ensures the correct user account is
  created on all nodes via cloud-init.
* The ephemeral keypair is generated fresh for each stack and discarded
  when the stack is deleted.

This approach distributes keys purely via cloud-init, avoiding the
``OS::Nova::KeyPair`` resource which is typically excluded by
``template-policy``.

Accessing Stack Outputs
-----------------------

All outputs defined in the Heat template are available to Ansible
playbooks via ``nodepool.node_properties.heat_stack_outputs``.  For example,
a template that outputs ``bastion_ip`` and ``internal_ips``:

.. code-block:: yaml

   outputs:
     bastion_ip:
       value: { get_attr: [bastion_fip, floating_ip_address] }
     internal_ips:
       value: { get_attr: [internal_node, first_address] }

These values can be referenced in a playbook:

.. code-block:: yaml

   - hosts: all
     tasks:
       - name: Show stack outputs
         debug:
           msg: "Internal IPs: {{ nodepool.node_properties.heat_stack_outputs.internal_ips }}"

The ``bastion_ip`` output is required — the driver uses it to set the
node's connection address.  All other outputs are optional and passed
through as-is.

Resource Cleanup
----------------

Test-created resources (images, volumes) must be tagged with
``zuul_node_uuid`` and ``zuul_system_id`` properties for automatic cleanup:

.. code-block:: bash

   openstack image create --property zuul_node_uuid=$ZUUL_NODE_UUID \
     --property zuul_system_id=$ZUUL_SYSTEM_ID test-image

Template Limitations
--------------------

Security checks (unconditional)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following template features are **unconditionally rejected** at
configuration load time, regardless of operator ``template-policy``
settings.  These checks protect Zuul's resource lifecycle guarantees:

* **external_id** — any resource with an ``external_id`` property is
  rejected.  External IDs reference pre-existing resources that Zuul
  does not own; deleting a stack with such resources could destroy
  infrastructure outside the build.

* **deletion_policy: Retain or Snapshot** — resources with
  ``deletion_policy`` set to ``Retain`` or ``Snapshot`` are rejected.
  Retained resources would leak after stack deletion, and snapshots
  create untracked resources outside Zuul's quota management.

* **get_file** — the ``get_file`` intrinsic function is rejected.
  It requires a client-side ``files`` dict that Zuul does not populate
  (see `Multi-file features`_ below).

These checks run during configuration loading (before the job is
scheduled) and produce configuration errors visible to the change
author.

Unsupported resource types
~~~~~~~~~~~~~~~~~~~~~~~~~~

The following Heat resource types are **rejected** at template
validation time (before the stack is submitted to Heat):

* ``OS::Heat::ResourceGroup`` — the quota parser cannot expand resource
  groups (it would need to multiply ``resource_def`` by ``count``),
  causing quota tracking to undercount and over-provision.  Define
  resources explicitly in the template instead.

* ``OS::Heat::AutoScalingGroup`` — autoscaling creates and deletes
  resources dynamically at runtime, making quota tracking impossible.
  Define resources explicitly in the template instead.

* ``OS::Heat::Stack`` — remote stacks require ``template_url`` or nested
  templates via the ``files`` API parameter, which the driver does not
  pass.  The stack would fail at the Heat API regardless.

These restrictions are enforced independently of ``template-policy``.
Even if no policy is configured, templates containing these types will
be rejected with a ``TemplatePolicyViolation`` error.

Multi-file features
~~~~~~~~~~~~~~~~~~~

Zuul sends a single template body to the Heat ``create_stack`` API.  It
does not pre-process the template or bundle additional files, so the
following multi-file features are **not supported**:

* ``get_file`` — requires a client-side ``files`` dict that Zuul does
  not populate.  Rejected unconditionally at config load time.
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
