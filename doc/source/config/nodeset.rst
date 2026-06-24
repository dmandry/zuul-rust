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
         :required:

         The label for the node.  Zuul will request a node with
         this label.

      .. attr:: template-config
         :type: dict

         Optional driver-specific template configuration for this node.
         The available keys depend on which provider drivers are
         configured.  When a provider driver implements template-based
         provisioning, the keys it contributes are documented in that
         driver's documentation.

         See :ref:`template-driven-providers` for an overview of how
         template configuration works.

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
