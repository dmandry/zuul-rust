:title: Build Nodes

.. _build_nodes:

Build Nodes
===========

.. note:: Zuul previously used a companion program, Nodepool, to
          manage build nodes.  Nodepool is deprecated and its usage
          should be replaced by the system described here.

Zuul manages resources for builds (such as virtual machines from a
cloud, Kubernetes pods, or static hosts), known as `build nodes` or
simply `nodes`, using the same configuration system used for jobs and
pipelines.  Because Zuul is interacting with remote systems and
causing real resource usage (which may come at a cost), there are some
differences, but most of the configuration is contained in git
repositories and control of this configuration can be retained by the
administrators of a Zuul system or delegated to its users.

Zuul has a dedicated component, the `zuul-launcher` which manages the
lifecycle of build nodes.  Additionally, it can manage the building of
custom virtual machine images that are used for them.

There are a number of Zuul configuration objects that are related to
node and image management:

* :ref:`provider`
* :ref:`section`
* :ref:`label`
* :ref:`flavor`
* :ref:`image`

The :ref:`provider` object is the main configuration object related
node and image management.  It may represent a Kubernetes cluster, or
a region of a cloud, or a collection of static nodes, any of which are
made available to a tenant.  If you use more than one cloud, or region
of a cloud, you will have at least one provider for each cloud or
region.  If you have tenants which should share a cloud region, then
you may put that provider in both tenants.  If they should not share
cloud resources, then you may create a unique provider in each tenant.

The :ref:`section` object is a flexible system of configuration
related to providers to facilitate all of these different options.  A
section represent a portion of a provider (such as a cloud region).  A
provider builds its configuration by inheriting from a section.
Sections may inherit from another section, so many layers of
abstraction may be accommodated.

Ultimately, a section is associated with one Zuul :ref:`connection
<connections>`, which is how the actual authenticated connection to
the cloud is made.  Consider the following example of how to structure
sections and providers:

.. graphviz::

   digraph foo {
     bgcolor="transparent";
     rankdir="LR";
     node [shape=box];
     edge [dir=back];

     subgraph cluster_connection {
       label="Connection";
       style=filled;
       color=lightgrey;
       node [style=filled,color=black,fillcolor=white];
       aws_conn [label="aws"];
     }

     subgraph cluster_section {
       label="Sections";
       style=filled;
       color=lightgrey;
       node [style=filled,color=black,fillcolor=white];
       aws_eu_north_1 [label="aws-eu-north-1"];
       aws_eu_central_1 [label="aws-eu-central-1"];
       aws -> aws_eu_north_1;
       aws -> aws_eu_central_1;
     }

     subgraph cluster_provider {
       label="Providers";
       style=filled;
       color=lightgrey;
       node [style=filled,color=black,fillcolor=white];
       aws_eu_north_1_main [label="aws-eu-north-1-main"];
       aws_eu_north_1_restricted [label="aws-eu-north-1-restricted"];
       aws_eu_central_1_main [label="aws-eu-central-1-main"];
     }

     aws_conn -> aws;
     aws_eu_north_1 -> aws_eu_north_1_main;
     aws_eu_north_1 -> aws_eu_north_1_restricted;
     aws_eu_central_1 -> aws_eu_central_1_main;
   }

This shows a system with three providers.  Two of the providers use
the eu-north-1 region, one of them uses eu-central-1.  Those sections
both inherit from a single section named `aws`, which in turn
references the `aws` connection.  The `aws` section can be used to
configure settings that are universally applicable to AWS.  The
`north` and `central` sections may add settings applicable to just
those regions, and finally, the providers can further refine settings.

Template-Driven Providers
-------------------------

Some provider drivers support defining the infrastructure for a build
node using a template from the project repository rather than fixed
provider configuration alone.  In this case, the nodeset node definition
includes a :attr:`nodeset.nodes.template-config` dict whose keys are
defined by the driver.  See :ref:`template-driven-providers` for more
details.

Node Reuse
----------

In general, the assumption is that Zuul will create a node, use it
once for one build of a job, and then delete the node after the
completion of the build.  Depending on the driver, it may be possible
to configure Zuul to re-use nodes, using the ``reuse`` option.  If
this is set, then nodes will be returned to service after the
completion of a build.  Note that this option can be dangerous since a
job may have compromised the security of the node, and could obtain
information or credentials from jobs that subsequently run on it.
This behavior is automatically enabled (and can not be disabled) by
the static driver.

A related option is the ``slots`` option, which configures how many
builds may run on a node simultaneously.  If set to a value greater
than ``1``, then a single underlying node (VM, static host, etc) will
serve as host to multiple sub-nodes in Zuul, each of which can be used
by a different build simultaneously.  This has similar security
implications as ``reuse``, so care should be taken when using it.

.. note:: The combination of ``reuse`` and ``slots`` enables similar
          functionality to the ``metastatic`` Nodepool driver.

.. _image-creation:

Image Creation
--------------

Normal Zuul jobs can be used to build an image, and then Zuul can
upload the resulting file (qcow2, vhd, etc) to a cloud for use in
launching nodes.

Image build jobs must run in a pipeline with a special reporter.  The
pipeline may be triggered by any of the usual triggers, but a special
trigger for missing image builds is also available.  The reporter
configuration required for image builds is this:

.. code-block:: yaml

   - pipeline:
       success:
         zuul:
           image-built: true
           image-validated: true

(See below for more about ``image-validated``).

The trigger configuration that will cause Zuul to run a build for a
missing image is:

.. code-block:: yaml

   - pipeline:
       trigger:
        zuul:
          - event: image-build

To tell Zuul a job is an image build job, use the
:attr:`job.image-build-name` attribute to indicate to Zuul that job is
used to build an image with that name.  The name must match a
:ref:`image` object, and the job must be defined in the same
repository as the image object.  The job is responsible for building
the image and uploading it to an object storage system.  It must
return information about where the image is stored using
:ref:`return_values`.  Here is an example `zuul_return` stanza showing
the expected information for a qcow2 image:

.. code-block:: yaml

   - name: Return Image information to Zuul
     zuul_return:
       data:
         zuul:
           artifacts:
             - name: 'qcow2 image'
               url: '<url of image>'
               metadata:
                 type: 'zuul_image'
                 image_name: '<image name>'
                 format: 'qcow2'
                 sha256: '<sha256 value of image file>'
                 md5sum: '<md5sum value of image file>'

Drivers may implement a number of methods for handling images.  Most
of them can handle the simple case where the build job uploads the
image to an object storage system accessible over HTTP, and the driver
will download the file and then upload it to the cloud.  Some drivers
support a configuration where the image is uploaded to the cloud's
object storage system by the job, and then Zuul can directly import
the file into the cloud as an image.  See the individual driver
documentation for details.

In addition to building image artifacts, some drivers also support
snapshot-based image builds.  In this case, the image build job should
manipulate the node it is running on, and when the node is ready for
the snapshot to be taken, it should execute this task at the end of
the `run` playbook to tell Zuul to perform the snapshot:

.. code-block:: yaml

    - name: Return snapshot command to Zuul
      zuul_return:
        data:
          zuul:
            snapshot_nodes:
              - node: "{{ zuul_node.uuid }}"
                image_name: "<image name>"

Control will return to Zuul, the snapshot will be taken, then the
`post-run` playbooks will run as normal.

Image Validation
----------------

Zuul may be configured to run validation jobs on an image after the
image is made available in the cloud, but before it is used for any
normal Zuul builds.  To configure this, omit the ``image-validated``
field in the image build pipeline.  Then create a new pipeline just
for image validation.  It should look like this:

.. code-block:: yaml

   - pipeline:
       name: image-validate
       manager: independent
       trigger:
         zuul:
           - event: image-validate
       success:
         zuul:
           image-validated: true

Then attach jobs to the new `image-validate` pipeline.  If those jobs
pass, the image will be considered validated and placed into service.
If they fail, the image will be deleted.

Nodepool Migration
------------------

Zuul previously used the companion program Nodepool to manage the
lifecycle of nodes and images.  The new system managed by
`zuul-launcher` is designed to provide a seamless migration.  When
Zuul prepares to run a job and needs to obtain a node of a certain
label, it will first check to see if that label is defined by a
:attr:`label` object in the current tenant.  If it is, then it will be
provided by the new `zuul-launcher` system.  If not, it will fall back
on Nodepool.  With this behavior, a Zuul system may be migrated from
Nodepool one tenant and one label at a time.

Once a tenant is completely migrated, the :attr:`tenant.use-nodepool`
setting should be set to ``false`` to disable the Nodepool fallback
behavior, and a void a situation where Zuul submits a request to
Nodepool when it is not running.
