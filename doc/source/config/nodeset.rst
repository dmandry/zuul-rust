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

.. _multi_label_jobs:

Multi-label support for jobs
----------------------------

When ``multi_label`` is ``true`` in the ``[launcher]`` section of
``zuul.conf`` on each scheduler and launcher (see :ref:`launcher`), Zuul
keeps retrying node
allocation after failures instead of giving up as soon as alternatives
are exhausted.  Specifically, after every alternative in a list has
failed, the scheduler wraps back to the first alternative and submits
another node request, repeating until one attempt succeeds.  The same
retry path applies when a job has only one effective nodeset (including
when no ``alternatives`` list is used): a failed node request is retried
against that same nodeset.  The launcher also fails a nodeset request
explicitly when the tenant is out of quota and no provider nodes are
assigned yet, so the scheduler can retry like other provisioning
failures.  When ``multi_label`` is ``false`` (the default), Zuul keeps
the previous behavior: the last failed alternative ends node-request
fallback for that failure, and quota exhaustion before any node is
assigned may leave the request unaccepted until conditions change.

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
will be used to run the job.  If :ref:`multi_label support for jobs
<multi_label_jobs>` is enabled and every alternative in the list has
failed, Zuul advances the nodeset index past the last alternative back
to the first and submits a new node request, so allocation is retried
in a cycle until one attempt succeeds.  The same retry behavior applies
when the job has only a single effective nodeset (including a job with
no ``alternatives`` list): a failed node request is retried against
that same nodeset rather than failing the item at once.  Additionally,
if Zuul encounters an unreachable node during the build, it will use
the next alternative when retrying the job (and with multi_label
enabled, continues to advance through alternatives as described above).
An example of a nodeset
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
      used for the job.  If a request fails (for example the cloud is
      out of quota or capacity), Zuul tries the next alternative.  With
      :ref:`multi_label support for jobs <multi_label_jobs>` enabled,
      after the last alternative fails, Zuul returns to the first
      alternative and issues another request, repeating until a
      request is fulfilled.

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
