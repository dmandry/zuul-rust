.. _nodeset:

Nodeset
=======

A Nodeset is a named collection of nodes for use by a job.  Jobs may
specify what nodes they require individually, however, by defining
groups of node types once and referring to them by name, job
configuration may be simplified.

Nodesets, like most configuration items, are unique within a tenant,
though a nodeset may be defined on multiple branches of the same
project as long as the contents are the same.  This is to aid in
branch maintenance, so that creating a new branch based on an existing
branch will not immediately produce a configuration error.

.. code-block:: yaml

   - nodeset:
       name: nodeset1
       nodes:
         - name: controller
           label: controller-label
         - name: compute1
           label: compute-label
         - name:
             - compute2
             - web
           label: compute-label
       groups:
         - name: ceph-osd
           nodes:
             - controller
         - name: ceph-monitor
           nodes:
             - controller
             - compute1
             - compute2
          - name: ceph-web
            nodes:
              - web

Nodesets may also be used to express that Zuul should use the first of
multiple alternative node configurations to run a job.  When a Nodeset
specifies a list of :attr:`nodeset.alternatives`, Zuul will request
the first Nodeset in the series, and if allocation fails for any
reason, Zuul will re-attempt the request with the subsequent Nodeset
and so on.  The first Nodeset which is fulfilled
will be used to run the job.  Additionally, if Zuul
encounters an unrechable node during the build, it will use the next
alternative when retrying the job.  An example of a nodeset
configuration with alternatives follows.

.. code-block:: yaml

   - nodeset:
       name: fast-nodeset
       nodes:
         - label: fast-label
           name: controller

   - nodeset:
       name: slow-nodeset
       nodes:
         - label: slow-label
           name: controller

   - nodeset:
       name: fast-or-slow
       alternatives:
         - fast-nodeset
         - slow-nodeset

In the above example, a job that requested the `fast-or-slow` nodeset
would receive `fast-label` nodes if a provider was able to supply
them, otherwise it would receive `slow-label` nodes.  A Nodeset may
specify nodes and groups, or alternative nodesets, but not both.

Via nodes
---------

A nodeset may declare *via nodes* — nodes that are not independently
allocated by the node provider but are instead reached through another
node in the same nodeset (the *gateway node*).  Via nodes appear in
the Ansible inventory and are connected through their gateway node
using SSH ProxyJump.

.. code-block:: yaml

   - nodeset:
       name: bastion-set
       nodes:
         - name: bastion
           label: bastion-label
           template-config:
             heat_template_path: ci/heat/bastion.yaml
         - name: internal
           via: bastion

In this example, ``bastion`` is allocated by the provider and
``internal`` is reached through ``bastion`` via SSH ProxyJump.
Connection details for ``internal`` (host, user, port, etc.) are
supplied by the gateway node at runtime through the
``node_properties.via_nodes`` mapping on the gateway node.

Constraints on via nodes:

* A node must have either ``label`` or ``via``, not both.
* A via node may not carry ``template-config``.
* The ``via`` value must name another node declared in the same
  nodeset.
* A gateway node (a node referenced by another node's ``via``) may not
  itself carry a ``via`` attribute (chaining is not allowed).

.. attr:: nodeset

   A Nodeset requires two attributes:

   .. attr:: name
      :required:

      The name of the Nodeset, to be referenced by a :ref:`job`.

      This is required when defining a standalone Nodeset in Zuul.
      When defining an in-line anonymous nodeset within a job
      definition, this attribute should be omitted.

   .. attr:: nodes

      This attribute is required unless `alternatives` is supplied.

      A list of node definitions, each of which has the following format:

      .. attr:: name
         :required:

         The name of the node.  This will appear in the Ansible inventory
         for the job.

         This can also be as a list of strings. If so, then the list of hosts in
         the Ansible inventory will share a common ansible_host address.

      .. attr:: label

         The label for the node.  Zuul will request a node with this
         label.  Required unless :attr:`nodeset.nodes.via` is set;
         mutually exclusive with ``via``.

      .. attr:: via

         The name of another node in the same nodeset through which
         this node is reached.  When set, the node is not independently
         allocated by the provider; instead, Zuul connects to it via
         SSH ProxyJump through the named gateway node.  The gateway
         node must carry a ``label`` (not ``via``), and chaining (a
         via node as the gateway of another via node) is not allowed.
         Mutually exclusive with :attr:`nodeset.nodes.label` and
         ``template-config``.

   .. attr:: groups

      Additional groups can be defined which are accessible from the ansible
      playbooks.

      .. attr:: name
         :required:

         The name of the group to be referenced by an ansible playbook.

      .. attr:: nodes
         :required:

         The nodes that shall be part of the group. This is specified as a list
         of strings.

   .. attr:: alternatives
      :type: list

      A list of alternative nodesets for which requests should be
      attempted in series.  The first request which succeeds will be
      used for the job.

      The items in the list may be either strings, in which case they
      refer to other Nodesets within the layout, or they may be a
      dictionary which is a nested anonymous Nodeset definition.  The
      two types (strings or nested definitions) may be mixed.

      An alternative Nodeset definition may in turn refer to other
      alternative nodeset definitions.  In this case, the tree of
      definitions will be flattened in a breadth-first manner to
      create the ordered list of alternatives.

      A Nodeset which specifies alternatives may not also specify
      nodes or groups (this attribute is exclusive with
      :attr:`nodeset.nodes` and :attr:`nodeset.groups`.
