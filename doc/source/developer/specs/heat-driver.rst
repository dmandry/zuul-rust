Template-Driven Provider Framework and Heat Driver
===================================================

.. warning:: This is not authoritative documentation.  These features
   are not currently available in Zuul.  They may change significantly
   before final implementation, or may never be fully completed.

This document describes a proposed template-driven provider
framework for Zuul that enables orchestration-based drivers to
provision multi-resource test environments from declarative templates
stored in project repositories.  The framework introduces generic
primitives that any template-oriented driver can implement.

The first driver built on this framework is the ``heat`` provider
driver, which provisions infrastructure from HOT (Heat Orchestration
Template) files via the OpenStack Heat API.  Future drivers for
other orchestration platforms would reuse the same framework without
further changes to the launcher, model, or configloader.

A work-in-progress implementation exists (see `Work Items`_ for
review links).

Introduction
------------

Zuul's existing provider drivers provision individual compute
resources (VMs or pods) as build nodes.  Each node maps to one
resource with a single SSH endpoint.  This cannot accommodate jobs
that require custom network topologies, multi-tier infrastructure,
or coordinated sets of resources created atomically.  These
capabilities are needed when the system under test *is itself
infrastructure*.

Infrastructure-as-code projects need CI environments with multiple
coordinated resources that are naturally expressed as declarative
templates, version-controlled alongside project code, and submitted
to an orchestration API that provisions all resources atomically.

Today, projects work around this by passing cloud credentials as Zuul
secrets and provisioning infrastructure in job playbooks (see
`Alternatives`_).  A native orchestration driver would instead let the
launcher call the orchestration API, track the resource lifecycle, and
hand a ready node to the executor.

Use Cases
~~~~~~~~~

- OpenStack-on-Kubernetes operators needing control-plane networks,
  tenant networks, storage networks, trunk ports, and multiple
  compute/networker nodes wired together before deploying the operator
  under test.
- Bare-metal provisioning workflows (Ironic, Metal3) needing virtual
  BMC endpoints, provisioning networks with PXE/iPXE, and
  boot-from-volume via Cinder.
- Network fabric testing (spine-leaf, VXLAN/EVPN, provider VLANs)
  with per-link Neutron networks and scenario-specific startup configs
  for switch simulator appliances.
- Multi-tier application stacks requiring load balancer, application
  servers, and database.

Proposal
--------

The implementation is split into two parts:

1. A generic `Template-Driven Provider Framework`_ that extends Zuul's
   provider architecture with the primitives needed by any
   orchestration-based driver.
2. A `Heat Provider Driver`_ that implements the framework for OpenStack
   Heat, serving as the first concrete consumer.


Template-Driven Provider Framework
----------------------------------

Node Model
~~~~~~~~~~

A new ``template_config`` dict is added to ``Node``, ``NodesetRequest``,
and ``ProviderNode``.  This carries driver-specific template metadata
from configuration through ZooKeeper to the launcher.

Drivers that do not use templates ignore this field.  Drivers that do
use templates populate it with their own keys, validated by their schema.

Interface Extensions
~~~~~~~~~~~~~~~~~~~~

The ``ProviderInterface`` base class and ``ProviderNode`` model are
extended with optional methods that template-driven drivers override:

* ``ProviderInterface.getTemplateConfigSchema()``
  Returns voluptuous schema keys for the driver's ``template-config``
  block in nodeset node definitions.  The configloader unions schemas
  from all registered drivers to build the nodeset validation schema.
* ``ProviderInterface.getProjectConfigKeys()``
  Returns additional tenant project configuration keys (e.g.,
  ``heat-template-dirs``).  These are injected into the tenant source
  schema so operators can declare which repository directories contain
  template files.
* ``ProviderInterface.getFileDiscoveryDirs()``
  Returns directory paths that the merger should fetch from git for
  this driver's templates.
* ``ProviderInterface.resolveTemplateConfig()``
  Called by the configloader after fetching repository files.  The
  driver resolves template paths to content, validates it, and stores
  the result in ``node.template_config``.
* ``ProviderInterface.checkTemplatePolicy()``
  Called by the launcher during provider selection.  Checks the
  template against operator-configured policy.  Raises
  ``TemplatePolicyViolation`` if rejected.  The base implementation
  is a no-op.
* ``ProviderNode.getManagedResources()``
  Returns a list of cloud resource IDs created by this node.  Used
  by the cleanup worker to build a managed-resource map for quota
  accounting.

Per-Instance Tenant Schema
~~~~~~~~~~~~~~~~~~~~~~~~~~

The tenant source schema is built per-instance rather than as a
class-level constant.  This allows each driver to inject its own
project configuration keys (via ``getProjectConfigKeys()``) into
the schema dynamically.

Template Resolution in the Configloader
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When configloader parses all nodesets and jobs,
``_resolveProviderTemplates()`` iterates over every nodeset node that
has a ``template_config``.  For each node, it calls
``resolveTemplateConfig()`` on every registered provider driver.  The
driver that recognizes its own keys resolves the template content
from the files cache and validates it.

Policy-Aware Provider Selection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When multiple providers serve the same label, each may have different
template policy.  The launcher evaluates each provider's policy during
provider selection, filtering before quota checks:

1. The launcher iterates candidate providers that serve the requested label.
2. For each provider, ``checkTemplatePolicy()`` is called with the node's
   ``template_config``.  Providers whose policy rejects the template are
   excluded from selection.
3. Remaining providers proceed to quota checks and normal selection logic.
4. If no provider accepts the template, the request fails.

Managed Resource Tracking
~~~~~~~~~~~~~~~~~~~~~~~~~

Orchestration drivers create cloud resources inside opaque constructs
(stacks, state files) that do not carry Zuul metadata individually.
The framework lets drivers report which resource IDs they own:

1. After successful creation, the driver persists physical resource
   IDs in the ``ProviderNode`` (via ``getManagedResources()``).
2. Before each quota cycle, the cleanup worker builds a
   ``managed_resource_ids`` map (``{resource_uuid: node_uuid}``)
   from all active nodes in ZooKeeper.
3. ``listInstances()`` enumerates all project resources.  For each
   resource, it checks the map: managed resources get Zuul metadata
   (the launcher skips them since they are tracked via node quota in
   ZK), unmanaged resources get empty metadata and are summed as
   unmanaged usage.

When multiple drivers share a cloud project (e.g. OpenStack and
Heat), each sees the other's resources as unmanaged consumption.
Both account for total project usage without double-counting.

Nodeset Configuration
~~~~~~~~~~~~~~~~~~~~~

The ``template-config`` dict in nodeset node definitions carries
driver-specific keys.  The schema is the union of all registered
drivers' schemas:

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

A hypothetical Terraform driver would add its own keys:

.. code-block:: yaml

   - nodeset:
       name: tf-lab
       nodes:
         - name: infra
           label: terraform-aws
           template-config:
             terraform-plan-path: zuul.tf.d/vpc-lab/main.tf
             terraform-variables:
               instance_type: t3.large

Because Zuul already supports inline nodeset definitions on jobs,
per-job parameterization of the same template works without any
additional changes.

Tenant Project Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Drivers declare which repository directories contain templates via
tenant project configuration.  The configloader fetches files from
these directories for template resolution:

.. code-block:: yaml

   - tenant:
       name: my-tenant
       source:
         my-gerrit:
           untrusted-projects:
             - name: org/my-project
               heat-template-dirs:
                 - zuul.heat.d/

A Terraform driver would register ``terraform-module-dirs`` similarly.

Future Drivers
~~~~~~~~~~~~~~

Adding a new orchestration driver requires only implementing the
``ProviderInterface`` extension methods and driver-specific classes.
No changes to the launcher, configloader, or node request handling
are required.

Examples of possible future drivers:

- Terraform
- AWS CloudFormation
- Azure ARM / Bicep
- GCP Deployment Manager

Heat Provider Driver
--------------------

The Heat driver is the first implementation of the template-driven
provider framework.  It provisions multi-resource test environments
from HOT files via the OpenStack Heat API.

Stack-per-Node Model
~~~~~~~~~~~~~~~~~~~~

One Heat stack maps to one Zuul node.  A stack may contain many VMs,
networks, and other resources, but Zuul treats it as a single unit
with one SSH connection point.  The required stack output ``bastion_ip``
provides the floating IP that the executor uses for SSH access.

.. note::

   A separate `via-nodes proposal
   <https://review.opendev.org/c/zuul/zuul/+/989872>`_ would allow
   the executor to reach internal nodes via ProxyJump through the
   bastion.

Adapting Labels, Flavors, and Images
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Zuul's provider architecture has three abstractions: labels, flavors,
and images. These are validated by per-driver schemas with section-based
inheritance.  The Heat driver reuses these but adapts their semantics:

* Labels 
  Carry ``region``, ``external-network``, and SSH attributes (``key-name``,
  ``host-key-checking``).  The template defines what resources are created;
  the label provides connectivity parameters.
* Flavors 
  Define per-stack resource budgets via ``max-*`` attributes (``max-instances``,
  ``max-cores``, ``max-ram``, ``max-volumes``, ``max-networks``, etc.).  These
  are enforced at stack-creation time by parsing the template and comparing
  resource counts against the limits.
* Images 
  Provide an optional ``image-id`` passed to the template as ``zuul_image``.
  The template decides how to use it.  The image's ``username`` determines the
  SSH user.  Initially only pre-existing Glance images is supported.

Template Resolution
~~~~~~~~~~~~~~~~~~~

The Heat drivers ``resolveTemplateConfig()`` implementation:

1. Looks up the ``heat_template_path`` in the files cache.
2. Parse the YAML content.
3. Run unconditional safety checks (see `Config Load Time Safety Checks`_).
4. Store validated content. (``node.template_config['heat_template_content']``)

If the template is not found or fails validation, a ``ConfigurationError`` is
appended and the job will not be scheduled.

Zuul-Injected Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~

The driver injects parameters into the stack creation call only if
the template declares them (Heat rejects unknown parameters with
HTTP 400):

- ``zuul_key_name``: cloud keypair for SSH access
- ``zuul_username``: login user for the bastion
- ``zuul_image``: resolved image ID or name
- ``zuul_external_network``: external network for floating IPs
- ``zuul_ephemeral_private_key`` / ``zuul_ephemeral_public_key``:
  per-stack Ed25519 keypair for internal node communication

Stack Outputs
~~~~~~~~~~~~~

Stack outputs are exposed as ``node_properties['heat_stack_outputs']``
and can contain structured data (e.g. IP addresses, inventory groups,
installer configs) consumed by downstream automation:

.. code-block:: yaml

   - name: Configure deployment
     hosts: lab
     tasks:
       - debug:
           msg: "Controller: {{ node_properties.heat_stack_outputs.controller_ip }}"

Quota and Resource Limits
~~~~~~~~~~~~~~~~~~~~~~~~~

The Heat driver provides multi-level quota enforcement.  It parses
the HOT template to count resources (instances, cores, RAM, volumes,
networks, routers, floating IPs).  For Nova servers, flavor
references are resolved via the Nova API to determine vCPU and RAM
counts.  Operators set maximum resource counts at the provider level
via ``resource-limits``, capping total consumption across all stacks.
Per-stack maximums are set via flavor attributes (``max-instances``,
``max-cores``, ``max-ram``, etc.).  Templates exceeding any limit are
rejected before any cloud API call.

The endpoint also enumerates resources via Nova, Cinder, and Neutron
APIs.  This is necessary because resources inside Heat stacks do not
carry Zuul metadata individually.  The tracked resource types are
Nova servers (instances, cores, RAM), Cinder volumes, Neutron
networks, routers, and floating IPs.

The tracking mechanism uses the generic ``getManagedResources()``
infrastructure (see `Managed Resource Tracking`_):

1. On ``CREATE_COMPLETE``, the state machine queries the stack's
   physical resources and persists their UUIDs in ``HeatProviderNode``.
2. ``getManagedResources()`` returns these IDs to the cleanup worker.
3. ``listInstances()`` classifies resources as managed or unmanaged.

Results are cached with a short TTL to avoid redundant API calls.

Template Policy
~~~~~~~~~~~~~~~

The Heat driver's ``template-policy`` provides operator-configurable
allow-lists for resource types and intrinsic functions.  Patterns are
matched as full regular expressions.  A restrictive default might be:

.. code-block:: yaml

   - provider:
       name: heat-prod
       template-policy:
         allowed-resource-types:
           - "OS::Nova::Server"
           - "OS::Neutron::.*"
           - "OS::Heat::(CloudConfig|MultipartMime|Value)"
         allowed-intrinsic-functions:
           - "get_param"
           - "get_resource"
           - "get_attr"
           - "str_replace(_strict|_vstrict)?"
           - "list_join"
           - "if"

State Machines
~~~~~~~~~~~~~~

Create: submit stack, poll for ``CREATE_COMPLETE``, extract ``bastion_ip``,
record physical resource IDs, mark node ready.  On ``CREATE_FAILED``,
raises a launch exception.  The launcher handles cleanup via the delete
state machine.

Delete: submit delete, poll until gone, mark deleted.  On unexpected status,
resubmits the delete.  On ``DELETE_FAILED``, raises an exception.  The stack
is marked leaked for the cleanup worker.

Both state machines persist state to ZooKeeper.  On launcher restart
mid-create, the state machine detects the existing ``heat_stack_id``,
confirms the stack exists, and resumes polling instead of creating a
duplicate.

Leak Detection and Cleanup
~~~~~~~~~~~~~~~~~~~~~~~~~~

Stacks are tagged with ``zuul_system_id`` and ``zuul_node_uuid``.
The cleanup worker lists Zuul-tagged stacks, compares against active
node records in ZooKeeper, and deletes orphaned stacks.

Provider Configuration
~~~~~~~~~~~~~~~~~~~~~~

The provider configuration specifies ``resource-limits`` to cap total
resources across all stacks, ``template-policy`` to restrict which
resource types and intrinsic functions are allowed, and per-flavor
``max-*`` attributes to limit individual stack sizes.

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
       template-policy:
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

Connection Configuration
~~~~~~~~~~~~~~~~~~~~~~~~

The connection uses ``clouds.yaml`` for authentication.

.. code-block:: ini

   [connection heat-cloud]
   driver=heat
   cloud=my-heat-cloud
   rate=2

Security
--------

Templates in untrusted projects are authored by users who may not be
fully trusted.  The driver validates templates before submitting them
to Heat.

The following sections detail the controls available to operators.

Tenant Isolation
~~~~~~~~~~~~~~~~

The first layer of defence is OpenStack's own tenant scoping.  Heat
stacks run within the OpenStack project configured in the provider's
``clouds.yaml``.  All resources are created under that project's
credentials and subject to its Keystone RBAC policies and service
quotas.

Operators should use a dedicated OpenStack project for Zuul Heat
stacks, separate from production workloads.

Template Parser
~~~~~~~~~~~~~~~

The second layer is the driver's YAML template parser, which
validates templates in two phases.

Two-Phase Validation
^^^^^^^^^^^^^^^^^^^^

Template validation is split into two phases because the configloader
and the launcher have different scopes of knowledge.

At config load time, the configloader resolves template content via
``resolveTemplateConfig()`` and runs unconditional safety checks.
These reject constructs that break Zuul's resource lifecycle
guarantees, use unsupported features, or introduce unpredictable
resource counts.  Invalid templates are rejected as configuration
errors before the job is scheduled.

At launch time, the launcher calls ``checkTemplatePolicy()`` with
the operator-configurable policy (``allowed-resource-types``,
``allowed-intrinsic-functions``).  Templates violating policy are
rejected before any Heat API call is made.  The split exists because
the configloader has no access to provider configuration, and a
label may match multiple providers with different policies.

Config Load Time Safety Checks
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following constructs are unconditionally denied:

- ``external_id`` on any resource, which would allow referencing
  cloud resources not owned by the stack.
- ``deletion_policy: Retain`` or ``Snapshot``, which would orphan
  resources when Zuul deletes the stack.
- ``OS::Heat::Stack``, ``OS::Heat::ResourceGroup``,
  ``OS::Heat::AutoScalingGroup``, which make resource counting
  unpredictable.
- ``get_file``, which is resolved client-side and not supported by
  the driver since template content is passed as a string.

.. note::

   Supporting ``get_file`` is out of scope for this spec.  A future
   implementation could populate the ``files`` dict from the merged
   repository, but would need sandboxing to ensure only files within
   a declared template files directory are accessible.

Launch Time Policy Checks
^^^^^^^^^^^^^^^^^^^^^^^^^

The operator configures ``template-policy`` with regex allow-lists
for ``allowed-resource-types`` and ``allowed-intrinsic-functions``.
The driver documentation will highlight types and functions with
security implications (e.g. ``OS::Neutron::RBACPolicy``,
``OS::Nova::KeyPair``, ``yaql``, ``make_url``).

Limitations
-----------

* Stacks cannot be snapshotted; each job gets a fresh stack.
* Zuul connects only to the bastion.  Internal nodes require
  ProxyJump in playbooks or `via-nodes support
  <https://review.opendev.org/c/zuul/zuul/+/989872>`_.
* One HOT YAML file per stack.  No environment files, resource
  registry overrides, or multiple template files.
* The driver validates resource types and counts but cannot verify
  a template produces a working environment.

Alternatives
------------

* Extending the OpenStack driver was considered, but the provisioning
  model (single VM vs. declarative stack), lifecycle (independent
  nodes vs. atomic stacks), quota model, and configuration are
  different enough that combining them would require changes at every
  layer.  A separate driver keeps both paths clean.
* The current approach of external pre-provisioning has projects pass
  cloud credentials as a Zuul secret and provision infrastructure in
  job playbooks by calling OpenStack APIs directly.  This lacks
  centralized quota enforcement and leak detection.  Cleanup depends
  on teardown playbooks which may themselves fail, leading to leaked
  resources that accumulate until operators notice quota exhaustion.
* Implementing everything as Heat-specific code touching the
  configloader, launcher, and model directly was also considered.  The
  framework approach was chosen because the primitives
  (``template_config`` on nodes, driver-extensible schema, template
  resolution, managed-resource tracking, policy-filtering) are
  inherently generic.  Building them as Heat-only code would require
  refactoring if another orchestration driver is added.

Work Items
----------

* Add ``template_config`` to the node model
  (`review 988758 <https://review.opendev.org/c/zuul/zuul/+/988758>`_)
* Add template-driven provider infrastructure
  (`review 989840 <https://review.opendev.org/c/zuul/zuul/+/989840>`_)
* Implement the Heat driver
  (`review 989841 <https://review.opendev.org/c/zuul/zuul/+/989841>`_)
* Add Heat driver documentation
  (`review 989842 <https://review.opendev.org/c/zuul/zuul/+/989842>`_)
* Via-nodes support for internal stack nodes
  (`via-nodes spec <https://review.opendev.org/c/zuul/zuul/+/989872>`_)

References
----------

All proposed changes are linked from the work items above.  The full
series is rooted at `review 988758
<https://review.opendev.org/c/zuul/zuul/+/988758>`_.
