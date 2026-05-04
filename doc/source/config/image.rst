.. _image:

Image
=====

An image is used by :ref:`provider` objects to configure the images
that are available for launching :ref:`build_nodes`.  Zuul can work
with images that already exist in a provider (whether they are
externally managed, or supplied by the provider itself), or images
where it manages the lifecycle (building, uploading or snapshotting,
and deleting).  Regardless of what type of image is used, it needs to
be configured in Zuul, and the image configuration object is used for
that.

The standalone image configuration object itself is little more than a
name reservation.  It represents the idea of a particular image with
certain contents, but the actual set of bytes that make up that image,
and the configuration options that go along with it, may be different
on different providers.  Therefore, there are very few configuration
options in the section below.  But once an image is defined in Zuul,
when that image is attached to a :ref:`section` or :ref:`provider`,
the driver-specific options for images are available and may be
applied to it.  See the driver-specific provider options for details.

For example, a user might decide to use the latest Debian release
available in their cloud for launching nodes.  They would then define
an image object:

.. code-block:: yaml

   - image:
       name: debian-latest
       description: The latest version of debian
       type: cloud

That expresses the idea of a `debian-latest` image, but it doesn't
contain any of the necessary information to identify the image in a
cloud -- because that information depends on the provider.  A provider
may then use that image like this:

.. code-block:: yaml

   - provider:
       name: openstack
       images:
         - name: debian-latest
           image-id: deadbeef

This tells Zuul that to use the `debian-latest` image on the
`openstack` cloud, it should use the image-id ``deadbeef`` (OpenStack
image IDs are usually hexadecimal strings).  The same image could also
be added to AWS or Azure clouds, with different attributes for
identification.

The attributes available to top-level image objects are:

.. attr:: image

   .. attr:: name
      :type: str
      :required:

      The name of the image.  Used to refer to the image in Zuul
      configuration.

   .. attr:: type
      :required:

      The type of image.

      .. value:: cloud

         An existing image available in the cloud.

      .. value:: zuul

         An image managed by Zuul.

   .. attr:: description
      :type: str

      A textual description of the image.

   .. attr:: retain-count
      :type: int
      :default: 2

      For :value:`image.type.zuul` images only.

      How many images to keep available.  As new images are created,
      the most recent `retain-count` validated images will be kept
      available.  Keeping more than one image available allows
      tenant admins to choose to fall-back on older images in case
      new images exhibit problems.  The most recent (and therefore,
      in-use) image is included in the count.  So a `retain-count`
      of 1 means to only keep the current in-use image; a count of 2
      means to keep one extra image as a backup.

   .. attr:: max-retain-count
      :type: int

      For :value:`image.type.zuul` images only.

      A limit for how many images to keep available.  This provides
      an upper limit for the `retain-count` attribute; any attempt
      to set retain-count higher than this value in any images,
      sections or providers that inherit this value will result in
      an error.

      Once max-retain-count, sections and providers that inherit it
      may not set it to a larger value, but may further reduce it.
