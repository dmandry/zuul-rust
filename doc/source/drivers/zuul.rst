:title: Zuul Driver

Zuul
====

The Zuul driver supports triggers only.  It is used for triggering
pipelines based on internal Zuul events.

Trigger Configuration
---------------------

Zuul events don't require a special connection or driver. Instead they
can simply be used by listing ``zuul`` as the trigger.

.. attr:: pipeline.trigger.zuul

   The Zuul trigger supports the following attributes:

   .. attr:: event
      :required:

      The event name.  Currently supported events:

      .. value:: project-change-merged

         When Zuul merges a change to a project, it generates this
         event for every open change in the project.  If there are a
         large number of open changes, this may produce a large number
         of events and result in poor performance.

         .. warning::

            Triggering on this event can cause poor performance when
            using the GitHub driver with a large number of
            installations.

      .. value:: parent-change-enqueued

         When Zuul enqueues a change into any pipeline, it generates
         this event for every child of that change.  If there are a
         large number of open changes, this may produce a large number
         of events and result in poor performance.

         .. note:: The dependent pipeline manager automatically
                   enqueues forward, reverse, and if configured,
                   circular dependencies of any change that is
                   enqueued.  It is not necessary to add this trigger
                   to :term:`gate` pipelines.

      .. value:: image-build

         This event is emitted via the API or web interface when an
         authenticated user requests an image to be built or re-built.

      .. value:: image-validate

         This event is emitted whenever an image is built and reported
         by the Zuul reporter, and that reporter has `validate` set to
         `false`, and the image is uploaded to a provider.  This
         trigger will respond to that condition and enqueue a special
         queue item that can be used to validate the new upload of
         that image before it is put into regular service.  The jobs
         that run in response to that queue item that request the
         unvalidated image label will automatically use the new
         (unvalidated) upload instead of the most recent validated
         upload.

   .. attr:: pipeline

      Only available for ``parent-change-enqueued`` events.  This is
      the name of the pipeline in which the parent change was
      enqueued.

   .. attr:: debug
      :default: false

      When set to `true`, this will cause debug messages to be
      included when the queue item is reported.  These debug messages
      may be used to help diagnose why certain jobs did or did not
      run, and in many cases, why the item was not ultimately enqueued
      into the pipeline.

      Setting this value also effectively sets
      :attr:`project.<pipeline>.debug` for affected queue items.

      This only applies to items that arrive at a pipeline via this
      particular trigger.  Since the output is very verbose and
      typically not needed or desired, this allows for a configuration
      where typical pipeline triggers omit the debug output, but
      triggers that match certain specific criteria may be used to
      request debug information.

Reporter Configuration
----------------------

.. attr:: pipeline.reporter.zuul

   The Zuul reporter supports the following attributes:

   .. attr:: image-built
      :type: bool

      If this value is set to `true`, then any jobs in the buildset
      which are configured as image jobs (i.e., they have
      :attr:`job.image-name` set) will have their image build
      artifacts stored in Zuul's image registry, and the zuul-launcher
      will begin uploading those artifacts to providers.

   .. attr:: image-validated
      :type: bool

      If this value is set to `true`, then any uploads associated with
      the buildset will be marked as validated.  If this is set to
      `true` on the initial build of the images, then no validation
      step will occur (the images will be assumed to be in working
      order and will begin to be used as soon as their upload is
      complete).  If it is set on a secondary pipeline that responds
      to :value:`pipeline.trigger.zuul.event.image-validate` events,
      then it causes Zuul to mark those images as validated after that
      pipeline completes.

      If this is set to `false`, then uploads will not be marked as
      validated.

      Whether this is set to `true` or `false`, the builds in the
      buildset will be associated with the uploads so that users will
      be able to find the validation builds for those uploads.
