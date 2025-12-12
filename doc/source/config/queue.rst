.. _queue:

Queue
=====

Projects that interact with each other should share a ``queue``.
This is especially used in a :value:`dependent <pipeline.manager.dependent>`
pipeline. The :attr:`project.queue` can optionally refer
to a specific :attr:`queue` object that can further configure the
behavior of the queue.

Here is an example ``queue`` configuration.

.. code-block:: yaml

   - queue:
       name: integrated
       per-branch: false


.. attr:: queue

   The attributes available on a queue are as follows (all are
   optional unless otherwise specified):

   .. attr:: name
      :required:

      This is used later in the project definition to refer to this queue.

   .. attr:: per-branch
      :default: false

      .. note:: This attribute is deprecated and will be removed in a
                future version of Zuul.  Use :attr:`queue.type`
                instead.

      Queues by default define a single queue for all projects and
      branches that use it. This is especially important if projects
      want to do upgrade tests between different branches in
      the :term:`gate`. If a set of projects doesn't have this use case
      it can configure the queue to create a shared queue per branch for
      all projects. This can be useful for large projects to improve the
      throughput of a gate pipeline as this results in shorter queues
      and thus less impact when a job fails in the gate. Note that this
      means that all projects that should be gated must have aligned branch
      names when using per branch queues. Otherwise changes that belong
      together end up in different queues.

   .. attr:: type
      :default: all-branches

      Specifies the type of queue from one of the choices below:

      .. value:: all-branches

         Queues by default define a single queue for all projects and
         branches that use it. This is especially important if
         projects want to do upgrade tests between different branches
         in the :term:`gate`.

      .. value:: per-branch

         A shared queue per branch for all projects. Note that this
         means that all projects that should be gated must have
         aligned branch names when using per branch queues. Otherwise
         changes that belong together end up in different queues.

      .. value:: branch-assigned

         Each branch of a project may be arbitrarily assigned to a
         queue.  This supports systems where projects have multiple
         independent branches, but the coordination of those branches
         across multiple projects is not done by sharing a consistent
         branch name.

         To assign a particular project-branch to a queue, create a
         :attr:`project` stanza with a :attr:`project.queue` attribute
         that specifies the queue name in the desired project-branch,
         or create it with a :attr:`project.branches` attribute that
         specifies the branch name.  Zuul will use the normal rules
         for implied branch matchers if they are applicable, which may
         include matching any branch if the project stanza appears in
         a :term:`config-project`.

         If a particular project-branch remains unassigned, Zuul will
         attempt to fall back on a `per-branch` queue if any project
         stanza for the project specified one, or an `all-branches`
         queue otherwise.

   .. attr:: allow-circular-dependencies
      :default: false

      Determines whether Zuul is allowed to process circular
      dependencies between changes for this queue. All projects that
      are part of a dependency cycle must share the same change queue.

      If Zuul detects a dependency cycle it will ensure that every
      change also includes all other changes that are part of the
      cycle.  However each change will still be a normal item in the
      queue with its own jobs.

      Reporting of success will be postponed until all items in the cycle
      succeed. In the case of a failure in any of those items the whole cycle
      will be dequeued.

      An error message will be posted to all items of the cycle if some
      items fail to report (e.g. merge failure when some items were already
      merged). In this case the target branch(es) might be in a broken state.

      In general, circular dependencies are considered to be an
      antipattern since they add extra constraints to continuous
      deployment systems.  Additionally, due to the lack of atomicity
      in merge operations in code review systems (this includes
      Gerrit, even with submitWholeTopic set), it may be possible for
      only part of a cycle to be merged.  In that case, manual
      interventions (such as reverting a commit, or bypassing gating
      to force-merge the remaining commits) may be required.

      .. warning:: If the remote system is able to merge the first but
                   unable to merge the second or later change in a
                   dependency cycle, then the gating system for a
                   project may be broken and may require an
                   intervention to correct.

   .. attr:: dependencies-by-topic
      :default: false

      Determines whether Zuul should query the code review system for
      changes under the same topic and treat those as a set of
      circular dependencies.

      Note that the Gerrit code review system supports a setting
      called ``change.submitWholeTopic``, which, when set, will cause
      all changes under the same topic to be merged simultaneously.
      Zuul automatically observes this setting and treats all changes
      to be submitted together as circular dependencies.  If this
      setting is enabled in gerrit, do not enable
      ``dependencies-by-topic`` in associated Zuul queues.

      Because ``change.submitWholeTopic`` is applied system-wide in
      Gerrit, some Zuul users may wish to emulate the behavior for
      some projects without enabling it for all of Gerrit.  In this
      case, setting ``dependencies-by-topic`` will cause Zuul to
      approximate the Gerrit behavior only for changes enqueued into
      queues where this is set.

      This setting requires :attr:`queue.allow-circular-dependencies`
      to also be set.  All of the caveats noted there continue to
      apply.
