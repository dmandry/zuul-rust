Via Nodes
=========

.. warning:: This is not authoritative documentation.  These features
   are not currently available in Zuul.  They may change significantly
   before final implementation, or may never be fully completed.

The following specification describes a mechanism for a single allocated
node to supply connection details for additional hosts that are added to
the Ansible inventory alongside it.

Introduction
------------

Zuul's node model maps each entry in a nodeset directly to an independent
provider node request: one nodeset node means one node allocated by the
launcher.  This works well for most drivers where each node is a standalone
virtual machine provisioned independently.

Some providers, however, create environments where a single allocation event
produces a *collection* of hosts.  The OpenStack Heat driver (under review
at https://review.opendev.org/c/zuul/zuul/+/988758) is the primary
motivating case: a single Heat stack can contain a gateway host with a
floating IP together with any number of internal compute nodes on a private
network.  From the launcher's perspective this is one resource (the stack),
but a job running against it may need to target individual internal nodes by
name — for example, ``primary``, ``worker0``, ``worker1`` — with SSH
access routed through the gateway.

This specification proposes a first-class mechanism — the ``via`` nodeset
attribute — that lets a nodeset declare additional nodes whose connection
details are supplied by an already-allocated node, rather than by an
independent provider allocation.  The mechanism is intentionally
driver-agnostic: any driver can participate by writing a standardized
``via_nodes`` key into the gateway node's ``node_properties``.

The name ``via`` follows the convention established by ``ip route`` — a
route goes *via* a gateway, and a via-node is reached *via* its gateway
node.

Proposal
--------

``via`` nodeset attribute
~~~~~~~~~~~~~~~~~~~~~~~~~

A new optional attribute, ``via``, is added to nodeset node definitions.
Its value is the name of another node in the *same* nodeset that acts as
the SSH gateway.  A node carrying this attribute is called a **via-node**.
The node it references is called its **gateway node**.

.. code-block:: yaml

   - nodeset:
       name: heat-multinode
       nodes:
         - name: bastion
           label: heat-lab
           template-config:
             heat-template-path: zuul.heat.d/multinode.yaml
         - name: primary
           via: bastion
         - name: worker0
           via: bastion
         - name: worker1
           via: bastion
       groups:
         - name: workers
           nodes:
             - worker0
             - worker1

Validation rules
~~~~~~~~~~~~~~~~

The following rules are enforced at configuration load time:

* The value of ``via`` must be the name of another node declared in the
  same nodeset.
* A via-node must not itself carry ``via`` (no chaining).
* A via-node must not carry a ``label`` — it has no independent label
  because it will not be allocated separately.
* A via-node *may* appear in nodeset groups alongside ordinary nodes.
* The same gateway node name may be referenced by multiple via-nodes.

Launcher behaviour
~~~~~~~~~~~~~~~~~~

When the scheduler creates a ``NodesetRequest`` from a nodeset, it excludes
via-nodes from the ``labels`` list.  Only ordinary (non-``via``) nodes
produce label entries and therefore independent provider node allocations.
The request is considered fulfilled once all ordinary nodes are ready,
regardless of how many via-nodes the nodeset declares.

No changes are required to the core allocation loop in the launcher.

``via_nodes`` convention
~~~~~~~~~~~~~~~~~~~~~~~~~

For the executor to resolve a via-node it needs connection details from the
gateway node.  Drivers communicate these details through a standardized key
in the gateway node's ``node_properties``:

.. code-block:: python

   node.node_properties['via_nodes'] = {
       'primary': {
           'host': '192.168.100.10',
           'user': 'zuul',
           'port': 22,         # optional, defaults to 22
       },
       'worker0': {
           'host': '192.168.100.11',
           'user': 'zuul',
       },
       'worker1': {
           'host': '192.168.100.12',
           'user': 'zuul',
       },
   }

The key is ``via_nodes``.  The value is a dictionary keyed by the via-node's
name.  Each entry carries:

``host`` (required)
  The IP address or hostname used to reach the via-node.  This will
  typically be a private IP address, reachable only through the gateway
  node.  The executor maps this to the ``ansible_host`` inventory variable.

``connection_type`` (optional)
  The connection protocol.  Defaults to ``ssh`` if absent.  This mirrors
  the ``connection_type`` attribute on ordinary Zuul nodes.  Only ``ssh``
  is implemented by this specification; support for additional connection
  types (e.g. ``winrm``) is left to future work.

``user`` (optional)
  The connection username.  If absent the executor falls back to the image's
  configured username, then to the executor's default username.  The
  executor maps this to the ``ansible_user`` inventory variable.

``port`` (optional)
  The connection port.  Defaults to 22 if absent.  The executor maps this
  to the ``ansible_port`` inventory variable.

``python_path`` (optional)
  The path to the Python interpreter on the via-node.  Defaults to
  ``auto`` if absent, which lets Ansible select the appropriate interpreter.
  Set this explicitly when the via-node's Python is in a non-standard
  location or differs from the gateway node.  The executor maps this to
  the ``ansible_python_interpreter`` inventory variable.

``shell_type`` (optional)
  The shell type on the via-node (e.g. ``sh``, ``cmd``, ``powershell``).
  If absent, Ansible's default shell is used.  The executor maps this to
  the ``ansible_shell_type`` inventory variable.

A driver populates this dictionary at the end of its create state machine,
once the resources it manages are fully available.  If the driver does not
populate ``via_nodes``, any via-node that references the gateway will cause
a fatal inventory build error.

Executor behaviour
~~~~~~~~~~~~~~~~~~

The executor's ``getHostList()`` method is extended to handle via-nodes.
The overall flow becomes:

1. Build a name-to-allocated-node mapping from the list of allocated nodes.
2. Iterate over all nodes declared in the nodeset (both ordinary and via).
3. For an **ordinary node**: build the inventory entry as today — use
   ``node.interface_ip`` as ``ansible_host``, apply connection type, etc.
4. For a **via-node**:

   a. Look up the gateway node by name in the allocated node map.
   b. Read ``gateway_node.node_properties['via_nodes'][node_name]``.
      If the key is missing, raise a fatal error describing which via-node
      and which gateway node are involved.
   c. Translate fields from the entry into Ansible inventory variables:

      - ``host`` → ``ansible_host``
      - ``user`` → ``ansible_user`` (fallback: image username, executor default)
      - ``port`` → ``ansible_port`` (default: 22)
      - ``python_path`` → ``ansible_python_interpreter`` (default: ``auto``)
      - ``shell_type`` → ``ansible_shell_type`` (omitted if absent)
   d. Read ``connection_type`` from the entry (defaulting to ``ssh``).
      If the value is not ``ssh``, raise a fatal error — other connection
      types are reserved for future implementation.
   e. Set ``ansible_connection`` to ``ssh`` and set
      ``ansible_ssh_common_args`` to configure SSH ProxyJump through the
      gateway node::

        -o StrictHostKeyChecking=no
        -o ProxyJump=<ansible_user>@<gateway_node.interface_ip>

      where ``<ansible_user>`` is the gateway node's configured SSH username
      (image username, or executor default).
   f. Build the ``nodepool`` and ``zuul_node`` host variables as for ordinary
      nodes, substituting the via-node's IPs and recording the ``via``
      relationship.

5. Group membership in the generated inventory follows the nodeset ``groups``
   declaration unchanged — via-nodes appear in the same groups as any other
   declared node.

This means a job whose nodeset declares ``primary``, ``worker0``, and
``worker1`` as via-nodes will find all three in the Ansible inventory by
those names, ready to be targeted by ``hosts: workers`` or
``hosts: primary`` without any ``add_host`` boilerplate.

Heat driver implementation
~~~~~~~~~~~~~~~~~~~~~~~~~~

The Heat driver participates in this mechanism by:

1. Requiring that templates output a ``zuul_via_nodes`` key whose value is a
   dictionary conforming to the ``via_nodes`` structure described above.
2. After a stack reaches ``CREATE_COMPLETE``, the create state machine reads
   the ``zuul_via_nodes`` output and writes it to
   ``node.node_properties['via_nodes']``.

Example stack output declaration:

.. code-block:: yaml

   outputs:
     bastion_ip:
       value: { get_attr: [bastion_fip, floating_ip_address] }
     zuul_via_nodes:
       value:
         primary:
           host: { get_attr: [primary_server, first_address] }
           user: zuul
         worker0:
           host: { get_attr: [worker0_server, first_address] }
           user: zuul
         worker1:
           host: { get_attr: [worker1_server, first_address] }
           user: zuul

The ``bastion_ip`` output continues to be required (it sets the Zuul node's
``interface_ip`` and becomes the ProxyJump address for via-nodes).

If the nodeset declares no via-nodes, the ``zuul_via_nodes`` output is
optional and silently ignored.  If the nodeset declares via-nodes but the
template does not output ``zuul_via_nodes``, the executor raises a fatal
error at inventory-build time.

End-to-end example
------------------

The following example illustrates the complete configuration for a job that
tests a three-node environment.

**Provider / section configuration** (``zuul.conf`` or provider YAML):

.. code-block:: yaml

   - section:
       name: heat
       connection: heat-cloud
       label-defaults:
         key-name: zuul-keypair
       flavors:
         - name: multinode-flavor
           max-floating-ips: 1
          max-cores: 12
          max-ram: 16384
       images:
         - name: centos-stream-9
           image-id: "abc123-glance-uuid"
           username: zuul
           connection-type: ssh

   - provider:
       name: heat-provider
       section: heat
       resource-limits:
         stacks: 20
         cores: 200
       labels:
         - name: heat-lab
           flavor: multinode-flavor
           image: centos-stream-9

**Nodeset declaration**:

.. code-block:: yaml

   - nodeset:
       name: heat-multinode
       nodes:
         - name: bastion
           label: heat-lab
           template-config:
             heat-template-path: zuul.heat.d/multinode.yaml
         - name: primary
           via: bastion
         - name: worker0
           via: bastion
         - name: worker1
           via: bastion
       groups:
         - name: workers
           nodes:
             - worker0
             - worker1

**Job definition**:

.. code-block:: yaml

   - job:
       name: multinode-test
       nodeset: heat-multinode
       run: playbooks/multinode-test.yaml

**Heat template** (``zuul.heat.d/multinode.yaml``, abbreviated):

.. code-block:: yaml

   heat_template_version: 2015-04-30

   parameters:
     zuul_key_name:
       type: string
     zuul_image:
       type: string
     zuul_external_network:
       type: string
     zuul_ephemeral_public_key:
       type: string
     zuul_ephemeral_private_key:
       type: string
       hidden: true

   resources:
     internal_keypair:
       type: OS::Nova::KeyPair
       properties:
         name: { get_param: "OS::stack_name" }
         public_key: { get_param: zuul_ephemeral_public_key }

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

     bastion_server:
       type: OS::Nova::Server
       properties:
         flavor: m1.small
         image: { get_param: zuul_image }
         key_name: { get_param: zuul_key_name }
         networks:
           - port: { get_resource: bastion_port }
         # cloud-init injects zuul_ephemeral_private_key for internal access

     bastion_fip:
       type: OS::Neutron::FloatingIP
       properties:
         floating_network: { get_param: zuul_external_network }
         port_id: { get_resource: bastion_port }

     primary_server:
       type: OS::Nova::Server
       properties:
         flavor: m1.medium
         image: { get_param: zuul_image }
         key_name: { get_resource: internal_keypair }
         networks:
           - network: { get_resource: private_net }

     worker0_server:
       type: OS::Nova::Server
       properties:
         flavor: m1.medium
         image: { get_param: zuul_image }
         key_name: { get_resource: internal_keypair }
         networks:
           - network: { get_resource: private_net }

     worker1_server:
       type: OS::Nova::Server
       properties:
         flavor: m1.medium
         image: { get_param: zuul_image }
         key_name: { get_resource: internal_keypair }
         networks:
           - network: { get_resource: private_net }

   outputs:
     bastion_ip:
       value: { get_attr: [bastion_fip, floating_ip_address] }
     zuul_via_nodes:
       value:
         primary:
           host: { get_attr: [primary_server, first_address] }
           user: zuul
         worker0:
           host: { get_attr: [worker0_server, first_address] }
           user: zuul
         worker1:
           host: { get_attr: [worker1_server, first_address] }
           user: zuul

**Resulting Ansible inventory** (as seen by the job playbook):

.. code-block:: ini

   [all]
   bastion  ansible_host=203.0.113.5   ansible_user=zuul
   primary  ansible_host=192.168.100.10 ansible_user=zuul \
                ansible_ssh_common_args="-o ProxyJump=zuul@203.0.113.5"
   worker0  ansible_host=192.168.100.11 ansible_user=zuul \
                ansible_ssh_common_args="-o ProxyJump=zuul@203.0.113.5"
   worker1  ansible_host=192.168.100.12 ansible_user=zuul \
                ansible_ssh_common_args="-o ProxyJump=zuul@203.0.113.5"

   [workers]
   worker0
   worker1

**Example playbook** targeting the inventory directly — no ``add_host``
required:

.. code-block:: yaml

   - hosts: primary
     tasks:
       - name: Configure primary node
         include_role:
           name: configure-primary

   - hosts: workers
     tasks:
       - name: Configure worker nodes
         include_role:
           name: configure-worker

Implementation plan
-------------------

The implementation is split into the following incremental Gerrit reviews:

* Add this specification to ``doc/source/developer/specs/`` and update the
  index.  Merged before any implementation begins.

* Core model: ``via`` attribute

  * Add ``via`` attribute to ``Node`` (``model.py``).
  * Update ``Node.toDict()`` / ``Node.fromDict()`` for serialization.
  * Update ``NodeSet.addNode()`` validation: enforce the rules in
    `Validation rules`_.
  * Update ``NodesetRequest`` label-list construction to exclude via-nodes.
  * Add schema support in the configuration loader.
  * Unit tests for validation rules and label exclusion.

* Executor: inventory resolution

  * Extend ``getHostList()`` (``executor/server.py``) to handle via-nodes.
  * Look up ``gateway_node.node_properties['via_nodes'][name]``.
  * Inject ``ansible_ssh_common_args`` with ``ProxyJump``.
  * Fatal error when ``via_nodes`` key is absent.
  * Unit tests for inventory building with via-nodes.

* Heat driver: ``zuul_via_nodes`` output support

  * After ``CREATE_COMPLETE``, copy ``zuul_via_nodes`` stack output into
    ``node.node_properties['via_nodes']`` in the create state machine.
  * Make ``zuul_via_nodes`` optional (no error if nodeset has no via-nodes).
  * Update ``heat.rst`` and ``heat-attrs.rstinc`` with the
    ``zuul_via_nodes`` output contract and the ``via`` nodeset example.
  * Unit tests for ``via_nodes`` population.

Alternatives
------------

``add_host`` in playbooks
~~~~~~~~~~~~~~~~~~~~~~~~~~

The current workaround is to use ``add_host`` tasks at the start of every
playbook to dynamically register internal nodes.  The connection
configuration (IP address, proxy jump, username) must be reconstructed from
stack outputs stored in ``nodepool.node_properties``.  This is error-prone,
repeated across every job, and invisible to Zuul's inventory model.  It also
means jobs cannot use ``hosts: <name>`` targeting for pre-run or post-run
playbooks without duplicating the ``add_host`` logic in each.

The ``via`` attribute eliminates this boilerplate and makes the full
inventory visible to Zuul from the start of the build.


Future work
-----------

* **Node-level variables**: Allow ``via_nodes`` entries to carry arbitrary
  ``host_vars`` beyond the SSH connection attributes, giving drivers a way
  to pass per-host data into Ansible variables.

* **Additional connection types**: This specification only implements
  ``connection_type: ssh`` with SSH ProxyJump.  Future work could extend
  executor handling for other connection types — for example, WinRM
  connections tunnelled over an SSH gateway, or other proxy mechanisms.
  The ``connection_type`` field is already present in the ``via_nodes``
  structure to allow this without a breaking change.

* **Other drivers**: The ``via_nodes`` convention is available to any driver.
  The Kubernetes driver could use it to expose individual pods within a
  namespace allocation as named inventory hosts.
