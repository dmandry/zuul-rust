Data Model Changelog
====================

Record changes to the ZooKeeper data model which require API version
increases here.

When making a model change:

* Increment the value of ``MODEL_API`` in ``model_api.py``.
* Update code to use the new API by default and add
  backwards-compatibility handling for older versions.  This makes it
  easier to clean up backwards-compatibility handling in the future.
* Make sure code that special cases model versions either references a
  ``model_api`` variable or has a comment like `MODEL_API: >
  {version}` so that we can grep for that and clean up compatibility
  code that is no longer needed.
* Add a test to ``test_model_upgrade.py``.
* Add an entry to this log so we can decide when to remove
  backwards-compatibility handlers.

Version 0
---------

:Prior Zuul version: 4.11.0
:Description: This is an implied version as of Zuul 4.12.0 to
              initialize the series.

Version 1
---------

:Prior Zuul version: 4.11.0
:Description: No change since Version 0.  This explicitly records the
              component versions in ZooKeeper.

Version 2
---------

:Prior Zuul version: 5.0.0
:Description: Changes the semaphore handle format from `<item_uuid>-<job_name>`
              to a dictionary with buildset path and job name.

Version 3
---------

:Prior Zuul version: 5.0.0
:Description: Add a new `SupercedeEvent` and use that for dequeuing of
              superceded items from other pipelines. This only affects the
              schedulers.

Version 4
---------

:Prior Zuul version: 5.1.0
:Description: Adds QueueItem.dequeued_missing_requirements and sets it to True
              if a change no longer meets merge requirements in dependent
              pipelines.  This only affects schedulers.

Version 5
---------

:Prior Zuul version: 5.1.0
:Description: Changes the result data attributes on Build from
              ResultData to JobData instances and uses the
              inline/offloading paradigm from FrozenJob.  This affects
              schedulers and executors.

Version 6
---------

:Prior Zuul version: 5.2.0
:Description: Stores the complete layout min_ltimes in /zuul/layout-data.
              This only affects schedulers.

Version 7
---------

:Prior Zuul version: 5.2.2
:Description: Adds the blob store and stores large secrets in it.
              Playbook secret references are now either an integer
              index into the job secret list, or a dict with a blob
              store key.  This affects schedulers and executors.

Version 8
---------

:Prior Zuul version: 6.0.0
:Description: Deduplicates jobs in dependency cycles.  Affects
              schedulers only.

Version 9
---------

:Prior Zuul version: 6.3.0
:Description: Adds nodeset_alternatives and nodeset_index to frozen job.
              Removes nodeset from frozen job.  Affects schedulers and executors.

Version 10
----------

:Prior Zuul version: 6.4.0
:Description: Renames admin_rules to authz_rules in unparsed abide.
              Affects schedulers and web.

Version 11
----------

:Prior Zuul version: 8.0.1
:Description: Adds merge_modes to branch cache.  Affects schedulers and web.

Version 12
----------
:Prior Zuul version: 8.0.1
:Description: Adds job_versions and build_versions to BuildSet.
              Affects schedulers.

Version 13
----------
:Prior Zuul version: 8.2.0
:Description: Stores only the necessary event info as part of a queue item
              instead of the full trigger event.
              Affects schedulers.

Version 14
----------
:Prior Zuul version: 8.2.0
:Description: Adds the pre_fail attribute to builds.
              Affects schedulers.

Version 15
----------
:Prior Zuul version: 9.0.0
:Description: Adds ansible_split_streams to FrozenJob.
              Affects schedulers and executors.

Version 16
----------
:Prior Zuul version: 9.0.0
:Description: Adds default_branch to the branch cache.
              Affects schedulers.

Version 17
----------
:Prior Zuul version: 9.1.0
:Description: Adds ZuulRegex and adjusts SourceContext serialization.
              Affects schedulers and web.

Version 18
----------
:Prior Zuul version: 9.2.0
:Description: Adds new merge modes 'recursive' and 'ort' for the Github
              driver.

Version 19
----------
:Prior Zuul version: 9.2.0
:Description: Changes the storage path of a frozen job to use the job's UUID
              instead of the name as identifier.

Version 20
----------
:Prior Zuul version: 9.2.0
:Description: Send (secret) job parent and artifact data via build request
              parameters instead of updating the job.
              Affects schedulers and executors.

Version 21
----------
:Prior Zuul version: 9.3.0
:Description: Add job_dependencies and job_dependents fields to job graphs.
              Affects schedulers.

Version 22
----------
:Prior Zuul version: 9.3.0
:Description: Add model_version field to job graphs and index jobs by uuid.
              Affects schedulers.

Version 23
----------
:Prior Zuul version: 9.3.0
:Description: Add model_version field to build sets.
              Affects schedulers.

Version 24
----------
:Prior Zuul version: 9.3.0
:Description: Add job_uuid to NodeRequests.
              Affects schedulers.

Version 25
----------
:Prior Zuul version: 9.3.0
:Description: Add job_uuid to BuildRequests and BuildResultEvents.
              Affects schedulers and executors.

Version 26
----------
:Prior Zuul version: 9.5.0
:Description: Refactor circular dependencies.
              Affects schedulers and executors.

Version 27
----------
:Prior Zuul version: 10.0.0
:Description: Refactor branch cache.
              Affects schedulers and web.

Version 28
----------
:Prior Zuul version: 10.1.0
:Description: Store repo state in blobstore.
              Affects schedulers and executor.

Version 29
----------
:Prior Zuul version: 10.1.0
:Description: Store BuildSet.dependent_changes as change refs.
              Affects schedulers.

Version 30
----------
:Prior Zuul version: 10.2.0
:Description: Store playbook nesting_level and cleanup on frozen job.
              Affects schedulers and executors.

Version 31
----------
:Prior Zuul version: 11.0.1
:Description: Upgrade sharded zkobject format.

Version 32
----------
:Prior Zuul version: 11.1.0
:Description: Add topic query timestamp.
              Affects schedulers.

Version 33
----------
:Prior Zuul version: 11.2.0
:Description: Send SemaphoreReleaseEvents to the tenant management event queue
              instead of the pipeline trigger event queue.
              Affects schedulers and executors.

Version 34
----------
:Prior Zuul version: 11.3.0
:Description: Don't store deprecated web ``status_url`` in system attributes anymore.
              Affects schedulers and web.

Version 35
----------
:Prior Zuul version: 11.3.0
:Description: Updated Secret configuration format to support OIDC token.
              Affects schedulers and executors.

Version 36
----------
:Prior Zuul version: 13.0.0
:Description: Supply tenant_name in ssh_keys to executors.
              Affects schedulers and executors.

Version 37
----------
:Prior Zuul version: 14.0.0
:Description: Upgrade to per-project branch cache.
              Affects schedulers and web.
