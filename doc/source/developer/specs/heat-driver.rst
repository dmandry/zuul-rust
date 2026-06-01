Heat Provider Driver
====================

.. warning:: This is not authoritative documentation.  These features
   are not currently available in Zuul.  They may change significantly
   before final implementation, or may never be fully completed.

This document describes a proposed ``heat`` provider driver for Zuul that
provisions multi-resource test environments from HOT (Heat Orchestration
Template) files stored in project repositories.  A work-in-progress
implementation exists (see `Work Items`_ for review links).

Introduction
------------

Zuul's OpenStack driver provisions individual Nova VMs as build nodes.
Each node maps to one VM with a single SSH endpoint.  This cannot
accommodate jobs that require custom network topologies, multi-tier
infrastructure, or coordinated sets of resources created atomically —
capabilities needed when the system under test *is itself infrastructure*.

Infrastructure-as-code projects (OpenStack deployment frameworks,
Kubernetes platform operators, network fabric controllers) need CI
environments with multiple networks, VLAN segmentation, trunk ports,
routers, floating IPs, boot-from-volume nodes, and structured outputs.
These are naturally expressed as Heat templates: declarative,
parameterizable, and version-controlled alongside project code.

Today, projects work around this by passing cloud credentials as Zuul
secrets and provisioning infrastructure in job playbooks (see
`Alternatives`_).  A native Heat driver would instead let the launcher
call the Heat API, track the stack lifecycle, and hand a ready node to
the executor — the same pattern used by the OpenStack and Kubernetes
drivers.

Proposal
--------

Add a ``heat`` provider driver that:

1. Lets project repositories define Heat templates describing their test
   infrastructure.

2. Integrates with Zuul's launcher, quota, and cleanup subsystems — one
   Heat stack maps to one Zuul node, connected via the ``bastion_ip``
   stack output.

3. Exposes stack outputs as node properties for job playbooks.

4. Enforces operator-defined resource policies and quota limits.

Why a Separate Driver
~~~~~~~~~~~~~~~~~~~~~

The OpenStack driver is a Nova VM launcher: its node model stores a single
``server_id``, quota derives from flavor size, cleanup relies on Nova server
metadata, and labels are static operator-curated configuration.  Heat
orchestrates multiple OpenStack services via declarative templates with
variable resource combinations and per-job parameterization.  Combining these
in one driver would require reworking the node model, state machines,
quota logic, cleanup, and configuration schema while complicating the existing
VM path.

A separate driver defines its own node model, state machines, quota
semantics, and configuration schema without affecting the OpenStack
driver.  Several capabilities introduced for Heat — ``template_config``
on nodes, template resolution in the configloader, and resource-ID-based
quota tracking — are not Heat-specific and could be reused by future
orchestration-based drivers for other clouds (e.g., AWS CloudFormation,
Azure ARM templates, GCP Deployment Manager).

Use Cases
~~~~~~~~~

- **OpenStack-on-Kubernetes operators** needing control-plane networks,
  tenant networks, storage networks, trunk ports, and multiple
  compute/networker nodes wired together before deploying the operator
  under test.

- **Bare-metal provisioning workflows** (Ironic, Metal3) needing virtual
  BMC endpoints, provisioning networks with PXE/iPXE, and
  boot-from-volume via Cinder.

- **Network fabric testing** (spine-leaf, VXLAN/EVPN, provider VLANs)
  with per-link Neutron networks and scenario-specific startup configs.

- **Multi-tier application stacks** requiring load balancer, application
  servers, and database on separate subnets.

Architecture
------------

Stack-per-Node Model
~~~~~~~~~~~~~~~~~~~~

One Heat stack maps to one Zuul node.  A stack may contain many VMs,
networks, and other resources, but Zuul treats it as a single unit with
one SSH connection point.

The required stack output ``bastion_ip`` provides the floating IP that
the executor uses for SSH access.  Only the bastion is accessed by the
executor — other instances in the stack (e.g., virtual switches running
a network OS, compute nodes, storage nodes) are not managed by Zuul and
are only accessed by the job's playbooks if needed.  A separate
`via-nodes proposal <https://review.opendev.org/c/zuul/zuul/+/989872>`_
would allow the executor to reach internal nodes via ProxyJump through
the bastion.  Topology details are exposed to jobs via ``node_properties``.

This preserves Zuul's existing node lifecycle (request → create → ready →
in-use → deleting → deleted) without launcher changes.

Adapting Labels, Flavors, and Images
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Zuul's provider architecture has three abstractions — **labels**,
**flavors**, and **images** — validated by per-driver schemas and
participating in section-based inheritance.  The Heat driver reuses these
but adapts their semantics.

**Labels** would carry ``region``, ``external-network``, and SSH attributes
(``key-name``, ``host-key-checking``).  The template defines what
resources are created; the label provides connectivity parameters.

**Flavors** would define per-stack resource budgets via ``max-*`` attributes
(``max-instances``, ``max-cores``, ``max-ram``, ``max-volumes``,
``max-networks``, etc.).  These are enforced at stack-creation time by
parsing the template and comparing resource counts against the limits.
Zuul's flavor concept is "resource sizing characteristics separate from
image and label" — per-stack budgets fit this definition.

**Images** would provide an optional ``image-id`` passed to the template as
``zuul_image``.  The template decides how to use it — typically for the
bastion node, but the author may apply it to any instance or ignore it.
The image's ``username`` determines the SSH user.  Initially only
``type: cloud`` (pre-existing Glance images) is supported; ``type: zuul``
(Zuul-managed images) may be added later.

All three follow the standard ``schema`` / ``internal_schema`` /
``inheritable_schema`` pattern and participate in section → provider
inheritance.

Template Resolution
~~~~~~~~~~~~~~~~~~~

Templates live in project repositories.  Tenant configuration declares
which directories contain them:

.. code-block:: yaml

   - tenant:
       name: my-tenant
       source:
         my-gerrit:
           untrusted-projects:
             - name: org/my-project
               heat-template-dirs:
                 - zuul.heat.d/

The merger fetches template files from these directories.  At job freeze
time, the configloader resolves ``heat-template-path`` references in
nodeset definitions to template content from the merged repository state.

This would extend Zuul's provider model: in existing drivers, "what to
build" is determined by label, flavor, and image at the provider level.
Here, the **nodeset** carries the template path, resolved by the
configloader.  ``template_config`` would flow through the model (Node,
NodeRequest) via ZooKeeper to the launcher.  The tenant project controls
the template; the operator controls resource budgets via flavors,
resource limits, and resource policy.

Nodeset Configuration
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   - nodeset:
       name: integration-lab
       nodes:
         - name: lab
           label: heat-integration
           template-config:
             heat-template-path: zuul.heat.d/integration-stack.yaml
             heat-parameters:
               controller_flavor: m1.large
               compute_flavor: m1.medium
               compute_count: 2

``heat-template-path`` is relative to the project root.
``heat-parameters`` is passed to the Heat stack creation call.  The
driver also injects Zuul-specific parameters (below) if the template
declares them.

Jobs can also define nodesets inline, allowing per-job parameterization
of the same template:

.. code-block:: yaml

   - job:
       name: integration-large
       nodeset:
         nodes:
           - name: lab
             label: heat-integration
             template-config:
               heat-template-path: zuul.heat.d/integration-stack.yaml
               heat-parameters:
                 controller_flavor: m1.xlarge
                 compute_flavor: m1.large

Zuul-Injected Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~

The driver injects parameters into the stack creation call only if the
template declares them (Heat rejects unknown parameters with HTTP 400):

- ``zuul_key_name`` — cloud keypair for SSH access
- ``zuul_username`` — login user for the bastion
- ``zuul_image`` — resolved image ID or name
- ``zuul_external_network`` — external network for floating IPs
- ``zuul_ephemeral_private_key`` / ``zuul_ephemeral_public_key`` —
  per-stack Ed25519 keypair for internal node communication

Stack Outputs
~~~~~~~~~~~~~

Stack outputs would be exposed as
``node_properties['heat_stack_outputs']``:

.. code-block:: yaml

   - name: Configure deployment
     hosts: lab
     tasks:
       - debug:
           msg: "Controller: {{ node_properties.heat_stack_outputs.controller_ip }}"

Templates can output structured data (inventory groups, Ironic enrollment
data, installer configs) consumed by downstream automation.

Quota and Resource Limits
~~~~~~~~~~~~~~~~~~~~~~~~~

The driver provides multi-level quota enforcement:

**Template parsing** — The driver parses the HOT template to count
resources (instances, cores, RAM, volumes, networks, routers, floating
IPs).  For Nova servers, flavor references are resolved via the Nova API
to determine vCPU and RAM counts.

**Provider resource limits** — Operators set maximum resource counts at
the provider level, capping total consumption across all stacks.

**Flavor limits** — Per-stack maximums (``max-instances``,
``max-cores``, ``max-ram``, etc.).  Templates exceeding any limit are
rejected before any cloud API call.

**Cloud API resource tracking** — The endpoint enumerates resources via
Nova, Cinder, and Neutron APIs.  This is necessary because resources
inside Heat stacks do not carry Zuul metadata individually (unlike the
OpenStack driver where each Nova server has Zuul tags).  The proposed
tracked resource types are Nova servers (instances, cores, RAM), Cinder
volumes, Neutron networks, routers, and floating IPs.  Additional
resource types (e.g., keypairs, Glance images, security groups) can be
added to the tracked set and corresponding quota dimensions as needed.

The tracking mechanism:

1. On ``CREATE_COMPLETE``, the state machine queries the stack's physical
   resources and persists their UUIDs in the ``HeatProviderNode`` as
   ``heat_physical_resource_ids``.

2. Before each quota cycle, the cleanup worker builds a
   ``managed_resource_ids`` map (``{resource_uuid: node_uuid}``) from
   all active Heat nodes in ZooKeeper.

3. ``listInstances()`` enumerates all project resources.  For each
   resource, it checks the map: managed resources get Zuul metadata
   (causing the launcher to skip them — already tracked via node quota
   in ZK), unmanaged resources get empty metadata and are summed as
   unmanaged usage.

Results would be cached with a short TTL to avoid redundant API calls.

When both OpenStack and Heat drivers share a cloud project, each sees
the other's resources as unmanaged consumption.  Both account for total
project usage without double-counting.

Resource Policy
~~~~~~~~~~~~~~~

Operators would restrict permitted Heat resource types:

.. code-block:: yaml

   - provider:
       name: heat-prod
       resource-policy:
         allowed-resource-types:
           - "OS::Nova::Server"
           - "OS::Neutron::.*"
           - "OS::Cinder::Volume"
           - "OS::Heat::(CloudConfig|MultipartMime|Value)"

Patterns are Python regular expressions matched against resource type
strings.  Templates with disallowed types would be rejected before any
cloud API call.

State Machines
~~~~~~~~~~~~~~

**Create:** Submit stack → poll for ``CREATE_COMPLETE`` → extract
``bastion_ip`` → record physical resource IDs → mark node ready.  On
``CREATE_FAILED``, raises a launch exception; the launcher handles
cleanup via the delete state machine.

**Delete:** Submit delete → poll until gone → mark deleted.  On
unexpected status, resubmits the delete.  On ``DELETE_FAILED``, raises
an exception; the stack is marked leaked for the cleanup worker.

Both state machines would persist state to ZooKeeper.  On launcher
restart mid-create, the state machine would detect the existing
``heat_stack_id``, confirm the stack exists via the Heat API, and resume
polling instead of creating a duplicate.

API calls would run asynchronously in a thread pool and be polled on
each ``advance()`` call so the launcher loop is not blocked.

Leak Detection and Cleanup
~~~~~~~~~~~~~~~~~~~~~~~~~~

Stacks would be tagged with ``zuul_system_id`` and ``zuul_node_uuid``.
The cleanup worker:

1. Lists Zuul-tagged stacks in the project.
2. Compares against active node records in ZooKeeper.
3. Deletes orphaned stacks after two consecutive cycles (avoiding races
   with in-progress creation).

Physical resource IDs from stacks distinguish managed resources from
pre-existing project resources for quota calculation (see
`Quota and Resource Limits`_).

Provider Configuration
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   - provider:
       name: heat-lab
       section: heat-section
       region: RegionOne
       stack-create-timeout: 900
       resource-limits:
         stacks: 20
         instances: 100
         cores: 400
         ram: 819200
         volumes: 50
         networks: 100
         routers: 50
         floating-ips: 40
       resource-policy:
         allowed-resource-types:
           - "OS::Nova::Server"
           - "OS::Neutron::.*"
           - "OS::Cinder::Volume"
           - "OS::Heat::(CloudConfig|MultipartMime|Value)"
       flavors:
         - name: small-lab
           max-instances: 8
           max-cores: 32
           max-ram: 65536
       images:
         - name: ubuntu-noble
           type: cloud
           image-id: abc123
           username: ubuntu
       labels:
         - name: heat-integration
           flavor-name: small-lab
           image-name: ubuntu-noble
           key-name: zuul

``resource-limits`` would cap total resources across all stacks.
``resource-policy`` would restrict allowed Heat resource types.  Flavors
would cap per-stack resources (e.g., ``small-lab`` allows at most 8
servers, 32 vCPUs, 64 GiB RAM).

Connection Configuration
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: ini

   [connection heat-cloud]
   driver=heat
   cloud=my-heat-cloud
   rate=2

Would use ``clouds.yaml`` for authentication, same as the OpenStack
driver.  ``rate`` would limit API requests per second.

Alternatives
------------

**Extend the OpenStack driver.**  The provisioning model (single VM vs.
declarative stack), lifecycle (independent nodes vs. atomic stacks),
quota model, and configuration are different enough that combining them
would require changes at every layer.  A separate driver keeps both
paths clean.

**External pre-provisioning (current approach).**  Projects pass cloud
credentials as a Zuul secret and provision infrastructure in job
playbooks by calling OpenStack APIs directly.  This lacks centralized
quota enforcement and leak detection.  Cleanup depends on teardown
playbooks which may themselves fail, leading to leaked resources that
accumulate until operators notice quota exhaustion.

**Nodepool plugins.**  With Nodepool merged into Zuul
(``zuul-launcher``), provider drivers are part of the Zuul codebase.
The Heat driver follows the same pattern as OpenStack, AWS, Azure,
Kubernetes, and Static providers.

Limitations
-----------

- **No snapshot support.**  Stacks cannot be snapshotted.  Each job gets
  a fresh stack.

- **Longer provisioning times.**  Complex stacks take longer than single
  VMs.  Mitigated by ``stack-create-timeout`` and quota sizing.

- **Single connection point.**  Zuul connects only to the bastion.
  Internal nodes require ProxyJump in playbooks or `via-nodes support
  <https://review.opendev.org/c/zuul/zuul/+/989872>`_.

- **Single template file.**  One HOT YAML file per stack.  No
  environment files, resource registry overrides, or multiple template
  files.  Nested stacks (``OS::Heat::Stack``), resource groups
  (``OS::Heat::ResourceGroup``), and auto-scaling groups
  (``OS::Heat::AutoScalingGroup``) are disallowed.  ``get_file`` is
  unsupported (Heat resolves it client-side; the driver does not).
  These restrictions may be relaxed later.

- **Template correctness.**  The driver validates resource types and
  counts but cannot verify a template produces a working environment.

Work Items
----------

* Add ``template_config`` to the Node model (MODEL_API version bump).
  (`review 988758
  <https://review.opendev.org/c/zuul/zuul/+/988758>`_)

* Wire ``template-config`` through configloader and merger for template
  resolution at job freeze time.
  (`review 989840
  <https://review.opendev.org/c/zuul/zuul/+/989840>`_)

* Implement the Heat driver: connection, provider, endpoint, state
  machines, model, and template parser.
  (`review 989841
  <https://review.opendev.org/c/zuul/zuul/+/989841>`_)

* Add Heat driver documentation.
  (`review 989842
  <https://review.opendev.org/c/zuul/zuul/+/989842>`_)

* Future: via-nodes support for ProxyJump-based SSH to internal stack
  nodes (`via-nodes spec
  <https://review.opendev.org/c/zuul/zuul/+/989872>`_).

  Proposed changes:

  - `Add Node.via attribute
    <https://review.opendev.org/c/zuul/zuul/+/989881>`_
  - `Add via-node support to NodeSetParser
    <https://review.opendev.org/c/zuul/zuul/+/989882>`_
  - `Wire via-node allocation and ProxyJump inventory
    <https://review.opendev.org/c/zuul/zuul/+/989883>`_
  - `Support zuul_via_nodes stack output
    <https://review.opendev.org/c/zuul/zuul/+/989884>`_

References
----------

All proposed changes are linked from the work items above.  The full
series is rooted at `review 988758
<https://review.opendev.org/c/zuul/zuul/+/988758>`_.
