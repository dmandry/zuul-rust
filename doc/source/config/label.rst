.. _label:

Label
=====

A label is used by :ref:`provider` objects to configure the
operational characteristics of :ref:`build_nodes`.  Unlike
:ref:`flavor` or :ref:`image` objects, a label does not correspond to
any existing concept in a cloud.  Instead, a label incorporates the
settings controlled by images and flavors by reference, and adds some
other operational settings to complete the information needed to
launch a node.

Jobs request nodes by using labels, and labels, along with the images
and flavors they reference, are used by providers to determine what
nodes to provide.

Every label has a reference to a single flavor and a single image.
That reference is by name, which means that a given label always
references the same flavor and image.  Within a single tenant, all
providers with a label will reference the same flavor and image
objects.  But different tenants may have different definitions of
flavors and images with the same names, so even if multiple tenants
include the same label objects, those objects may reference different
flavor and image objects.

The standalone label configuration object itself is where the flavor
and image objects are referenced.  Some other globally-applicable
options for the label are also available.  More options become
available when the label is attached to a :ref:`section` or
:ref:`provider` object.  See the driver-specific provider options for
details.

For example, a user might decide to use a small vm running the latest
version of Debian for launching nodes.  They would then define a label
object that references pre-existing image and flavor objects:

.. code-block:: yaml

   - label:
       name: small-debian-vm
       description: A very small VM for running simple tests on Debian.
       image: debian-latest
       flavor: small-vm

That expresses the idea of a label that runs Debian on a small VM, but
it doesn't contain any of the necessary information to actually launch
that VM in a cloud.  When the label is attached to a provider, the
associated flavor and images will also need to be attached.  Once all
three are attached, the provider will have enough information to
create the requested VM.

The attributes available to top-level label objects are:

.. attr:: label

   .. attr:: name
      :type: str
      :required:

      The name of the label.  Used to refer to the label in Zuul
      configuration.

   .. attr:: description
      :type: str

      A textual description of the label.

   .. attr:: image
      :type: str
      :required:

      The name of the :ref:`image` to use with this label.

   .. attr:: flavor
      :type: str
      :required:

      The name of the :ref:`flavor` to use with this label.

   .. attr:: max-age
      :type: int
      :default: 0

      The time (in seconds) since creation that a node may be
      available for use.  Ready nodes older than this time will be
      deleted.

   .. attr:: max-ready-age
      :type: int
      :default: 0

      The time (in seconds) an unassigned node should stay in ready state.

   .. attr:: min-ready
      :type: int
      :required:

      Minimum number of instances that should be in a ready
      state. Zuul always creates more nodes as necessary in response
      to demand, but setting ``min-ready`` can speed processing by
      attempting to keep nodes on-hand and ready for immedate use.
      This is best-effort based on available capacity and is not a
      guaranteed allocation.  The default of 0 means that Zuul will
      only create nodes of this label when there is demand.

   .. attr:: min-retention-time
      :type: int
      :default: 0

      The time (in seconds) since an instance was launched, during
      which a node will not be deleted. For node resources with
      minimum billing times, this can be used to ensure that the
      instance is retained for at least the minimum billing interval.

      This setting takes precedence over `max-[ready-]age`.
