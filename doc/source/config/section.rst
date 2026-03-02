.. _section:

Section
=======

A section is a portion of a cloud or other system that supplies test
resources.  Sections are used by :ref:`provider` objects to complete
their configuration.

Sections may inherit from other sections, but eventually one of them
must be associated with one of the Zuul :ref:`connections` which
represent the entire cloud or test resource supplier.

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

.. attr:: section

   For the full list of attributes available to a section based on
   its driver, see:

   * :attr:`provider[aws]`
   * :attr:`provider[azure]`
   * :attr:`provider[static]`
   * :attr:`provider[openstack]`

   .. attr:: name
      :type: str
      :required:

      The name of the section.

   .. attr:: parent
      :type: str

      A :ref:`section` from which to inherit common configuration settings.

   .. attr:: description
      :type: str

      A textual description of the section.

   .. attr:: connection
      :type: str

      The name of a Zuul :ref:`connection <connections>` this section
      should use to communicate with the cloud or other resource
      supplier.

   .. attr:: abstract
      :type: bool

      Whether a section is intended to be inherited by
      another :ref:`section` or a :ref:`provider`.  This
      setting is currently unused (but may be used in the
      future).  If a section is used to provide common
      values to other sections, set this to `true`.
      Otherwise, the default of `false` indicates that the
      section should be referenced directly by providers.

   .. attr:: images
      :type: list

      A list of :ref:`image` objects available to this section.

   .. attr:: labels
      :type: list

      A list of :ref:`label` objects available to this section.

   .. attr:: flavors
      :type: list

      A list of :ref:`flavor` objects available to this section.
