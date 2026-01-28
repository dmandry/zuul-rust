ZooKeeper
=========

Overview
--------

Zuul has a microservices architecture with the goal of no single point of
failure in mind.

Zuul is an event driven system with several event loops that interact
with each other:

* Driver event loop: Drivers like GitHub or Gerrit have their own event loops.
  They perform preprocessing of the received events and add events into the
  scheduler event loop.

* Scheduler event loop: This event loop processes the pipelines and
  reconfigurations.

Each of these event loops persists data in ZooKeeper so that other
components can share or resume processing.

A key aspect of scalability is maintaining an event queue per
pipeline. This makes it easy to process several pipelines in
parallel. A new driver event is first processed in the driver event
queue. This adds a new event into the scheduler event queue. The
scheduler event queue then checks which pipeline may be interested in
this event according to the tenant configuration and layout. Based on
this the event is dispatched to all matching pipeline queues.

In order to make reconfigurations efficient we store the parsed branch
config in Zookeeper. This makes it possible to create the current
layout without the need to ask the mergers multiple times for the
configuration. This is used by zuul-web to keep an up-to-date layout
for API requests.

We store the pipeline state in Zookeeper.  This contains the complete
information about queue items, jobs and builds, as well as a separate
abbreviated state for quick access by zuul-web for the status page.

Driver Event Ingestion
----------------------

There are three types of event receiving mechanisms in Zuul:

* Active event gathering: The connection actively listens to events (Gerrit)
  or generates them itself (git, timer, zuul)

* Passive event gathering: The events are sent to Zuul from outside (GitHub
  webhooks)

* Internal event generation: The events are generated within Zuul itself and
  typically get injected directly into the scheduler event loop.

The active event gathering needs to be handled differently from
passive event gathering.

Active Event Gathering
~~~~~~~~~~~~~~~~~~~~~~

This is mainly done by the Gerrit driver. We actively maintain a
connection to the target and receive events. We utilize a leader
election to make sure there is exactly one instance receiving the
events.

Passive Event Gathering
~~~~~~~~~~~~~~~~~~~~~~~

In case of passive event gathering the events are sent to Zuul
typically via webhooks. These types of events are received in zuul-web
which then stores them in Zookeeper. This type of event gathering is
used by GitHub and other drivers. In this case we can have multiple
instances but still receive only one event so that we don't need to
take special care of event deduplication or leader election.  Multiple
instances behind a load balancer are safe to use and recommended for
such passive event gathering.

Configuration Storage
---------------------

Zookeeper is not designed as a database with a large amount of data,
so we should store as little as possible in zookeeper. Thus we only
store the per project-branch unparsed config in zookeeper. From this,
every part of Zuul, like the scheduler or zuul-web, can quickly
recalculate the layout of each tenant and keep it up to date by
watching for changes in the unparsed project-branch-config.

We store the actual config sharded in multiple nodes, and those nodes
are stored under per project and branch znodes. This is needed because
of the 1MB limit per znode in zookeeper. It further makes it less
expensive to cache the global config in each component as this cache
is updated incrementally.

Executor and Merger Queues
--------------------------

The executors and mergers each have an execution queue (and in the
case of executors, optionally per-zone queues).  This makes it easy
for executors and mergers to simply pick the next job to run without
needing to inspect the entire pipeline state.  The scheduler is
responsible for submitting job requests as the state changes.

Zookeeper Map
-------------

This is a reference for object layout in Zookeeper.

.. path:: zuul

   All ephemeral data stored here.  Remove the entire tree to "reset"
   the system.

.. path:: zuul/cache/connection/<connection>

   The connection cache root.  Each connection has a dedicated space
   for its caches.  Two types of caches are currently implemented:
   change and branch.

.. path:: zuul/cache/connection/<connection>/branches

   The connection branch cache root.  Contains the cache itself and a
   lock.

.. path:: zuul/cache/connection/<connection>/project/<project>

   The branch and project info cache for an individual project.

.. path:: zuul/cache/connection/<connection>/project/<project>/data

   This is a single sharded JSON blob that contains info about the project.

.. path:: zuul/cache/connection/<connection>/project/<project>/lock

   The lock for this project's branch cache.

.. path:: zuul/cache/connection/<connection>/branches/data
   :type: BranchCacheZKObject (sharded)

   The connection branch cache data.  This is a single sharded JSON blob.

.. path:: zuul/cache/connection/<connection>/branches/lock
   :type: RWLock

   The connection branch cache read/write lock.

.. path:: zuul/cache/connection/<connection>/cache

   The connection change cache.  Each node under this node is an entry
   in the change cache.  The node ID is a sha256 of the cache key, the
   contents are the JSON serialization of the cache entry metadata.
   One of the included items is the `data_uuid` which is used to
   retrieve the actual change data.

   When a cache entry is updated, a new data node is created without
   deleting the old data node.  They are eventually garbage collected.

.. path:: zuul/cache/connection/<connection>/data

   Data for the change cache.  These nodes are identified by a UUID
   referenced from the cache entries.

   These are sharded JSON blobs of the change data.

.. path:: zuul/cache/blob/data

   Data for the blob store.  These nodes are identified by a
   sha256sum of the secret content.

   These are sharded blobs of data.

.. path:: zuul/cache/blob/lock

   Side-channel lock directory for the blob store.  The store locks
   by key id under this znode when writing.

.. path:: zuul/cleanup

   This node holds locks for the cleanup routines to make sure that
   only one scheduler runs them at a time.

   .. path:: build_requests
   .. path:: connection
   .. path:: general
   .. path:: merge_requests
   .. path:: node_request
   .. path:: semaphores

.. path:: zuul/components

   The component registry.  Each Zuul process registers itself under
   the appropriate node in this hierarchy so the system has a holistic
   view of what's running.  The name of the node is based on the
   hostname but is a sequence node in order to handle multiple
   processes.  The nodes are ephemeral so an outage is automatically
   detected.

   The contents of each node contain information about the running
   process and may be updated periodically.

   .. path:: executor
   .. path:: fingergw
   .. path:: merger
   .. path:: scheduler
   .. path:: web

.. path:: zuul/config/cache

   The unparsed config cache.  This contains the contents of every
   Zuul config file returned by the mergers for use in configuration.
   Organized by repo canonical name, branch, and filename.  The files
   themselves are sharded.

.. path:: zuul/config/lock

   Locks for the unparsed config cache.

.. path:: zuul/events/connection/<connection>/events
   :type: ConnectionEventQueue

   The connection event queue root.  Each connection has an event
   queue where incoming events are recorded before being moved to the
   tenant event queue.

.. path:: zuul/events/connection/<connection>/events/queue

   The actual event queue.  Entries in the queue reference separate
   data nodes.  These are sequence nodes to maintain the event order.

.. path:: zuul/events/connection/<connection>/events/data

   Event data nodes referenced by queue items.  These are sharded.

.. path:: zuul/events/connection/<connection>/events/election

   An election to determine which scheduler processes the event queue
   and moves events to the tenant event queues.

   Drivers may have additional elections as well.  For example, Gerrit
   has an election for the watcher and poller.

.. path:: zuul/events/tenant/<tenant>

   Tenant-specific event queues.  Each queue described below has a
   data and queue subnode.

.. path:: zuul/events/tenant/<tenant>/management

   The tenant-specific management event queue.

.. path:: zuul/events/tenant/<tenant>/trigger

   The tenant-specific trigger event queue.

.. path:: zuul/events/tenant/<tenant>/pipelines

   Holds a set of queues for each pipeline.

.. path:: zuul/events/tenant/<tenant>/pipelines/<pipeline>/management

   The pipeline management event queue.

.. path:: zuul/events/tenant/<tenant>/pipelines/<pipeline>/result

   The pipeline result event queue.

.. path:: zuul/events/tenant/<tenant>/pipelines/<pipeline>/trigger

   The pipeline trigger event queue.

.. path:: zuul/events/tenant/<tenant>/state

   A Znode that, if exists, can be used to pause event
   processing.

.. path:: zuul/executor/unzoned
   :type: JobRequestQueue

   The unzoned executor build request queue.  The generic description
   of a job request queue follows:

   .. path:: requests/<request uuid>

      Requests are added by UUID.  Consumers watch the entire tree and
      order the requests by znode creation time.

   .. path:: locks/<request uuid>
      :type: Lock

      A consumer will create a lock under this node before processing
      a request.  The znode containing the lock and the request znode
      have the same UUID.  This is a side-channel lock so that the
      lock can be held while the request itself is deleted.

   .. path:: params/<request uuid>

      Parameters can be quite large, so they are kept in a separate
      znode and only read when needed, and may be removed during
      request processing to save space in ZooKeeper.  The data may be
      sharded.

   .. path:: result-data/<request uuid>

      When a job is complete, the results of the merge are written
      here.  The results may be quite large, so they are sharded.

   .. path:: results/<request uuid>

      Since writing sharded data is not atomic, once the results are
      written to ``result-data``, a small znode is written here to
      indicate the results are ready to read.  The submitter can watch
      this znode to be notified that it is ready.

   .. path:: waiters/<request uuid>
      :ephemeral:

      A submitter who requires the results of the job creates an
      ephemeral node here to indicate their interest in the results.
      This is used by the cleanup routines to ensure that they don't
      prematurely delete the result data.  Used for merge jobs

.. path:: zuul/executor/zones/<zone>

   A zone-specific executor build request queue.  The contents are the
   same as above.

.. path:: zuul/launcher/stats-election
   :type: LauncherStatsElection

   An election to decide which launcher will report system-wide
   launcher stats (such as total nodes).

.. path:: zuul/layout/<tenant>

   The layout state for the tenant.  Contains the cache and time data
   needed for a component to determine if its in-memory layout is out
   of date and update it if so.

.. path:: zuul/layout-data/<layout uuid>

   Additional information about the layout.  This is sharded data for
   each layout UUID.

.. path:: zuul/locks

   Holds various types of locks so that multiple components can coordinate.

.. path:: zuul/locks/connection

   Locks related to connections.

.. path:: zuul/locks/connection/<connection>

   Locks related to a single connection.

.. path:: zuul/locks/connection/database/migration
   :type: Lock

   Only one component should run a database migration; this lock
   ensures that.

.. path:: zuul/locks/events

   Locks related to tenant event queues.

.. path:: zuul/locks/events/trigger/<tenant>
   :type: Lock

   The scheduler locks the trigger event queue for each tenant before
   processing it.  This lock is only needed when processing and
   removing items from the queue; no lock is required to add items.

.. path:: zuul/locks/events/management/<tenant>
   :type: Lock

   The scheduler locks the management event queue for each tenant
   before processing it.  This lock is only needed when processing and
   removing items from the queue; no lock is required to add items.

.. path:: zuul/locks/pipeline

   Locks related to pipelines.

.. path:: zuul/locks/pipeline/<tenant>/<pipeline>
   :type: Lock

   The scheduler obtains a lock before processing each pipeline.

.. path:: zuul/locks/tenant

   Tenant configuration locks.

.. path:: zuul/locks/tenant/<tenant>
   :type: RWLock

   A write lock is obtained at this location before creating a new
   tenant layout and storing its metadata in ZooKeeper.  Components
   which later determine that they need to update their tenant
   configuration to match the state in ZooKeeper will obtain a read
   lock at this location to ensure the state isn't mutated again while
   the components are updating their layout to match.

.. path:: zuul/ltime

   An empty node which serves to coordinate logical timestamps across
   the cluster.  Components may update this znode which will cause the
   latest ZooKeeper transaction ID to appear in the zstat for this
   znode.  This is known as the `ltime` and can be used to communicate
   that any subsequent transactions have occurred after this `ltime`.
   This is frequently used for cache validation.  Any cache which was
   updated after a specified `ltime` may be determined to be
   sufficiently up-to-date for use without invalidation.

.. path:: zuul/merger
   :type: JobRequestQueue

   A JobRequestQueue for mergers.  See :path:`zuul/executor/unzoned`.

.. path:: zuul/nodepool
   :type: NodepoolEventElection

   An election to decide which scheduler will monitor nodepool
   requests and generate node completion events as they are completed.

.. path:: zuul/results/management

   Stores results from management events (such as an enqueue event).

.. path:: zuul/scheduler/timer-election
   :type: SessionAwareElection

   An election to decide which scheduler will generate events for
   timer pipeline triggers.

.. path:: zuul/scheduler/stats-election
   :type: SchedulerStatsElection

   An election to decide which scheduler will report system-wide stats
   (such as total node requests).

.. path:: zuul/global-semaphores/<semaphore>
   :type: SemaphoreHandler

   Represents a global semaphore (shared by multiple tenants).
   Information about which builds hold the semaphore is stored in the
   znode data.

.. path:: zuul/semaphores/<tenant>/<semaphore>
   :type: SemaphoreHandler

   Represents a semaphore.  Information about which builds hold the
   semaphore is stored in the znode data.

.. path:: zuul/system
   :type: SystemConfigCache

   System-wide configuration data.

   .. path:: conf

      The serialized version of the unparsed abide configuration as
      well as system attributes (such as the tenant list).

   .. path:: conf-lock
      :type: WriteLock

      A lock to be acquired before updating :path:`zuul/system/conf`

.. path:: zuul/tenant/<tenant>

   Tenant-specific information here.

.. path:: zuul/tenant/<tenant>/pipeline/<pipeline>

   Pipeline state.

.. path:: zuul/tenant/<tenant>/pipeline/<pipeline>/dirty

   A flag indicating that the pipeline state is "dirty"; i.e., it
   needs to have the pipeline processor run.

.. path:: zuul/tenant/<tenant>/pipeline/<pipeline>/queue

   Holds queue objects.

.. path:: zuul/tenant/<tenant>/pipeline/<pipeline>/item/<item uuid>

   Items belong to queues, but are held in their own hierarchy since
   they may shift to different queues during reconfiguration.

.. path:: zuul/tenant/<tenant>/pipeline/<pipeline>/item/<item uuid>/buildset/<buildset uuid>

   There will only be one buildset under the buildset/ node.  If we
   reset it, we will get a new uuid and delete the old one.  Any
   external references to it will be automatically invalidated.

.. path:: zuul/tenant/<tenant>/pipeline/<pipeline>/item/<item uuid>/buildset/<buildset uuid>/repo_state

   The global repo state for the buildset is kept in its own node
   since it can be large, and is also common for all jobs in this
   buildset.

.. path:: zuul/tenant/<tenant>/pipeline/<pipeline>/item/<item uuid>/buildset/<buildset uuid>/job/<job name>

   The frozen job.

.. path:: zuul/tenant/<tenant>/pipeline/<pipeline>/item/<item uuid>/buildset/<buildset uuid>/job/<job name>/build/<build uuid>

   Information about this build of the job.  Similar to buildset,
   there should only be one entry, and using the UUID automatically
   invalidates any references.

.. path:: zuul/tenant/<tenant>/pipeline/<pipeline>/item/<item uuid>/buildset/<buildset uuid>/job/<job name>/build/<build uuid>/parameters

   Parameters for the build; these can be large so they're in their
   own znode and will be read only if needed.

.. path:: zuul/nodeset/requests/<request uuid>
   :type: NodesetRequest

   A new-style (nodepool-in-zuul) node request.  This will replace
   `nodepool/requests`.  The two may operate in parallel for a time.

   Schedulers create requests and may delete them at any time
   (regardless of lock state).

.. path:: zuul/nodeset/locks/<request uuid>

   A lock for the new-style node request.  Launchers will acquire a
   lock when operating on the request.

.. path:: zuul/nodes/nodes/<node uuid>
   :type: ProviderNode

   A new-style (nodepool-in-zuul) node record.  This holds information
   about the node (mostly supplied by the provider).  It also holds
   enough information to get the endpoint responsible for the node.

.. path:: zuul/nodes/nodes/<node uuid>/snapshot
   :type: ProviderNodeSnapshot

   Information about a provider node snapshot.  The launcher locks the
   companion snapshot-lock znode and updates this znode while the
   executor continues to hold the lock on the main node's znode.

.. path:: zuul/nodes/nodes/<node uuid>/snapshot-lock

   The lock for the snapshot child znode.

.. path:: zuul/nodes/locks/<node uuid>

   A lock for the new-style node.  Launchers or executors will hold
   this lock while operating on the node.

.. path:: zuul/tenant/<tenant name>/provider/<provider canonical name>/config

   The flattened configuration for a provider.  This holds the
   complete information about the images, labels, and flavors the
   provider supports.  It is the combination of the provider stanza
   plus any inherited sections.

   References to images, labels, and flavors are made using canonical
   names since the same short names may be different in different
   tenants.  Since the same canonically-named provider may appear in
   different tenants with different images, labels, and flavors, the
   provider itself is tenant scoped.

   Only updated by schedulers upon reconfiguration.  Read-only for launchers.

.. path:: zuul/images/artifacts/<uuid>

   Stores information about an image build artifact.  Each build job
   may produce any number of artifacts (each corresponding to an
   image+format).  Information about each is stored here under a
   random uuid.

.. path:: zuul/image-uploads/<image canonical name>/<image build uuid>/endpoint/<endpoint id>

   Stores information about an image upload to a particular cloud endpoint.

.. path:: zuul/endpoint/<endpoint name>/quota
   :type: QuotaCache

   A TreeCache that stores quota information for the endpoint.

.. path:: zuul/endpoint/<endpoint name>/quota/limits

   Stores information about the quota limits of the endpoint.

.. path:: zuul/endpoint/<endpoint name>/quota/unmanaged-usage

   Stores information about the unmanaged quota usage of the endpoint.

.. path:: zuul/endpoint/<endpoint name>/quota/resource/<resource id>

   Stores information about the quota requirements of driver-defined
   resources (such as instance or volume types).  This allows any
   launcher to calculate the estimated quota usage of any request,
   without having an active endpoint.
