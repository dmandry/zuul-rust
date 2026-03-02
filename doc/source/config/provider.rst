.. _provider:

Provider
========

A provider is responsible for managing :ref:`build_nodes` (which may
be VMs, real servers, containers, or other resources) for Zuul.  A
provider may appear in more than one tenant; if it does, it is still
treated as a single provider that is shared by those tenants.

The provider may have resource limits applied to it; if it does, those
limits are shared across all tenants (in other words, if a provider
has a limit of 10 instances, then all of the tenants that use that
provider share the 10 instance limit).

Providers refer to other objects, such as :ref:`label`, :ref:`image`
and, indirectly, :ref:`flavor` objects.  These references are made by
name, and since they may be loaded from other projects, it is possible
for the same provider to reference different label, flavor, and image
objects in different tenants.  This will work as expected: the cloud
will have different images uploaded if they reference different
images, or it will use the same image if they reference the same
image.

If there are settings that are common to most or all flavors, images,
or labels, they may be able to be added to the associated `defaults`
section (`flavor-defaults`, `label-defaults`, and `image-defaults`).
In these cases, a specific setting for an individual flavor, label, or
image would override one set as a default.  Further, some settings are
available on the top-level flavor, label, and image objects; some of
these may also be overridden at the provider.

Each provider must inherit from exactly one :ref:`section`.  A section
represents a portion of a cloud or other system that supplies
resources.  Sections may inherit from other sections, but eventually
one of them must be associated with one of the Zuul :ref:`connections`
which represent the entire cloud or resource supplier.

Configuration information that is common to multiple providers should
be added to to a section rather than the providers.  For the most
part, sections and providers have the same configuration attributes
available to them.  The few exceptions are the `section` attribute
which is only available to providers, the `connection` attribute,
which is only available to sections (and may not be overridden in a
chain of inheritance of multiple sections), and the `flavors`
attribute, which is only available to sections.

This inheritance chain guarantees that any provider and its associated
sections are only ever associated with a single connection and
therefore driver (such as AWS, OpenStack, etc.).  Because each remote
system behaves differently, the options available for each system
differ, and therefore the options available to a given provider or
section depend on which driver is used for the underlying connection.
A small number of very basic options which are available in all cases
are documented here, but the full list of available options is
provided with each driver's documentation.

.. attr:: provider

   For the full list of attributes available to a provider based on
   its driver, see:

   * :attr:`provider[aws]`
   * :attr:`provider[azure]`
   * :attr:`provider[static]`
   * :attr:`provider[openstack]`

   .. attr:: name
      :type: str
      :required:

      The name of the provider.

   .. attr:: section
      :type: str
      :required:

      The :ref:`section` to which this provider should be attached.

   .. attr:: description
      :type: str

      A textual description of the provider.

   .. attr:: images
      :type: list

      A list of :ref:`image` objects available to this provider.

   .. attr:: labels
      :type: list

      A list of :ref:`label` objects available to this provider.
