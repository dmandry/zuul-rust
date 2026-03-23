.. _flavor:

Flavor
======

A flavor is used by :ref:`provider` objects to configure the
characteristics of :ref:`build_nodes` related to the hardware (virtualized
or not) that they run on.  Clouds variously call this concept
`flavors`, `instance types`, `sizes`, etc.  Generally they embody
characteristics as machine architecture, RAM, storage, CPU, and
others.  By abstracting the concept in Zuul, users can express an
intention to run a job on a node with certain characteristics, and
differing flavor configurations in different providers can be used to
select the appropriate system from the cloud.

The standalone flavor configuration object itself is little more than
a name reservation.  It represents the idea of a particular flavor
separate from any implementation of that flavor in a provider, and
therefore there are very few configuration options below.  But once a
flavor is defined in Zuul, when that flavor is attached to a
:ref:`section`, the driver-specific options for flavors are available
and may be applied to it.  See the driver-specific provider options
for details.

For example, a user might decide to use a very small VM size for
launching build nodes.  They would then define a flavor object:

.. code-block:: yaml

   - flavor:
       name: small-vm
       description: A very small VM for running simple tests.

That expresses the idea of a `small-vm` flavor, but it doesn't
contain any of the necessary information to identify the flavor in a
cloud -- because that information depends on the provider.  A section
may then use that flavor like this:

.. code-block:: yaml

   - section:
       name: aws
       flavors:
         - name: small-vm
           instance-type: t3.small

This tells Zuul that to use the `small-vm` flavor on the `aws` cloud,
it should use the ``t3.small`` instance type.  The same flavor could
also be added to OpenStack or Azure clouds, with different attributes
for identification.

The attributes available to top-level flavor objects are:

.. attr:: flavor

   .. attr:: name
      :type: str
      :required:

      The name of the flavor.  Used to refer to the flavor in Zuul
      configuration.

   .. attr:: description
      :type: str

      A textual description of the flavor.

.. _flavor_connection_address:

Connection address selection
----------------------------

For cloud drivers that support the ``public-ipv4`` and ``public-ipv6``
flavor options (for example :attr:`provider[aws]`, :attr:`provider[azure]`,
and :attr:`provider[openstack]`), the Zuul launcher picks the address it
uses to reach a build node over SSH and Ansible, stored as the
``interface_ip`` field on the node and exposed to jobs as
:var:`nodepool.interface_ip`, as follows:

* If ``public-ipv4`` is false and the cloud reports a private IPv4
  address, that address is used.
* Otherwise, if ``public-ipv6`` is false and the cloud reports a private
  IPv6 address, that address is used.
* Otherwise, if ``public-ipv4`` is true and a public IPv4 address is
  available, that address is used; if ``public-ipv6`` is true and a
  public IPv6 address is available, that address is used.
* Otherwise, the provider’s default address for the instance
  (its ``interface_ip``) is used.

The cloud’s ``public_ipv4``, ``private_ipv4``, ``public_ipv6``, and
``private_ipv6`` values on the node and job are still set from what the
provider reports, regardless of which of those addresses is chosen for
``interface_ip``.  This matters when a VM has no public address (for
example, ``public-ipv4: false``) and Zuul must use a private address on
a reachable network.

SSH private key verification (optional)
---------------------------------------

Provider image configuration may include ``ssh-private-key``: a path on
zuul-launcher to a private key file. When that path exists, zuul-launcher
uses it during node scanning (after it can open an SSH transport to the
node) to verify public-key authentication, aligning readiness with SSH
keys that zuul-executor uses for Ansible. Drivers document this attribute
alongside other image settings (for example OpenStack lists it under
:attr:`provider[openstack]` image stanzas). Soft reboot of an instance when
that check stalls is implemented for OpenStack only.
