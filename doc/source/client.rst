:title: Zuul Admin Client

Zuul Admin Client
=================

Zuul includes a simple command line client that may be used to affect Zuul's
behavior while running.

.. note:: For operations related to normal workflow like enqueue, dequeue, autohold and promote, the `zuul-client` CLI should be used instead.

Configuration
-------------

The client uses the same zuul.conf file as the server, and will look
for it in the same locations if not specified on the command line.

Usage
-----
The general options that apply to all subcommands are:

.. program-output:: zuul-admin --help

The following subcommands are supported:

tenant-conf-check
^^^^^^^^^^^^^^^^^

.. program-output:: zuul-admin tenant-conf-check --help

Example::

  zuul-admin tenant-conf-check

This command validates the tenant configuration schema. It exits '-1' in
case of errors detected.

create-auth-token
^^^^^^^^^^^^^^^^^

.. note:: This command is only available if an authenticator is configured in
          ``zuul.conf``. Furthermore the authenticator's configuration must
          include a signing secret.

.. program-output:: zuul-admin create-auth-token --help

Example::

    zuul-admin create-auth-token --auth-config zuul-operator --user alice --tenant tenantA --expires-in 1800 --print-meta-info

The return value is the value of the ``Authorization`` header the user must set
when querying a protected endpoint on Zuul's REST API. When the ``--tenant`` is
specified, ``zuul.admin`` claim with the value of the tenant will be added
to the token. The meta information of the token will be printed when
"--print-meta-info" is specified.

Example::

    Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NDEzNTAzMTksImV4cCI6MTc0MTM1MjExOSwiaXNzIjoienV1bF9vcGVyYXRvciIsImF1ZCI6Inp1dWwuZXhhbXBsZS5jb20iLCJzdWIiOiJhbGljZSIsInp1dWwiOnsiYWRtaW4iOlsidGVuYW50QSJdfX0.cW3U5LEFJS0TM-EDELZS9_hhbxdw-xObLwvDQKL55fM
    ---------------- Meta Info ----------------
    Tenant:         tenantA
    User:           alice
    Generated At:   2025-03-07 12:25:19 UTC
    Expired At:     2025-03-07 12:55:19 UTC
    SHA1 Checksum:  f52018044a209ce7eed5e587c41cc8b360af891d
    -------------------------------------------

.. _export-keys:

export-keys
^^^^^^^^^^^

.. program-output:: zuul-admin export-keys --help

Example::

  zuul-admin export-keys /var/backup/zuul-keys.json

.. _import-keys:

import-keys
^^^^^^^^^^^

.. program-output:: zuul-admin import-keys --help

Example::

  zuul-admin import-keys /var/backup/zuul-keys.json

copy-keys
^^^^^^^^^

.. program-output:: zuul-admin copy-keys --help

Example::

  zuul-admin copy-keys gerrit old_project gerrit new_project

delete-keys
^^^^^^^^^^^

.. program-output:: zuul-admin delete-keys --help

Example::

  zuul-admin delete-keys gerrit old_project

new-project-ssh-key
^^^^^^^^^^^^^^^^^^^

.. program-output:: zuul-admin new-project-ssh-key --help

Example::

  zuul-admin new-project-ssh-key gerrit project

new-project-secrets-key
^^^^^^^^^^^^^^^^^^^^^^^

.. program-output:: zuul-admin new-project-secrets-key --help

Example::

  zuul-admin new-project-secrets-key gerrit project

new-tenant-ssh-key
^^^^^^^^^^^^^^^^^^

.. program-output:: zuul-admin new-tenant-ssh-key --help

Example::

  zuul-admin new-tenant-ssh-key example-tenant

list-project-ssh-keys
^^^^^^^^^^^^^^^^^^^^^

.. program-output:: zuul-admin list-project-ssh-keys --help

Example::

  zuul-admin list-project-ssh-keys gerrit project

list-project-secrets-keys
^^^^^^^^^^^^^^^^^^^^^^^^^

.. program-output:: zuul-admin list-project-secrets-key --help

Example::

  zuul-admin list-project-secrets-keys gerrit project

list-tenant-ssh-keys
^^^^^^^^^^^^^^^^^^^^

.. program-output:: zuul-admin list-tenant-ssh-keys --help

Example::

  zuul-admin list-tenant-ssh-keys example-tenant

delete-project-ssh-key
^^^^^^^^^^^^^^^^^^^^^^

.. program-output:: zuul-admin delete-project-ssh-key --help

Example::

  zuul-admin delete-project-ssh-key gerrit project 0

delete-project-secrets-key
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. program-output:: zuul-admin delete-project-secrets-key --help

Example::

  zuul-admin delete-project-secrets-key gerrit project 0

delete-tenant-ssh-key
^^^^^^^^^^^^^^^^^^^^^

.. program-output:: zuul-admin delete-tenant-ssh-key --help

Example::

  zuul-admin delete-tenant-ssh-key example-tenant

delete-oidc-signing-keys
^^^^^^^^^^^^^^^^^^^^^^^^

.. program-output:: zuul-admin delete-oidc-signing-keys --help

Example::

  zuul-admin delete-oidc-signing-keys RS256

delete-state
^^^^^^^^^^^^

.. program-output:: zuul-admin delete-state --help

Example::

  zuul-admin delete-state

delete-pipeline-state
^^^^^^^^^^^^^^^^^^^^^

.. program-output:: zuul-admin delete-pipeline-state --help

Example::

  zuul-admin delete-pipeline-state tenant pipeline

prune-database
^^^^^^^^^^^^^^

.. program-output:: zuul-admin prune-database --help

Example::

  zuul-admin prune-database --older-than 180d

Deprecated commands
-------------------

The following commands are deprecated in the zuul-admin CLI, and thus may not be entirely supported in Zuul's current version.
They will be removed in a future release of Zuul. They can still be performed via the `zuul-client` CLI.
Please refer to `zuul-client's documentation <https://zuul-ci.org/docs/zuul-client/>`__
for more details.

In order to run these commands, the ``webclient`` section is required in the configuration file.

It is also possible to run the client without a configuration file, by using the
``--zuul-url`` option to specify the base URL of the Zuul web server.

Autohold
^^^^^^^^
.. program-output:: zuul-admin autohold --help

Example::

  zuul-admin autohold --tenant openstack --project example_project --job example_job --reason "reason text" --count 1

Autohold Delete
^^^^^^^^^^^^^^^
.. program-output:: zuul-admin autohold-delete --help

Example::

  zuul-admin autohold-delete --id 0000000123

Autohold Info
^^^^^^^^^^^^^
.. program-output:: zuul-admin autohold-info --help

Example::

  zuul-admin autohold-info --id 0000000123

Autohold List
^^^^^^^^^^^^^
.. program-output:: zuul-admin autohold-list --help

Example::

  zuul-admin autohold-list --tenant openstack

Dequeue
^^^^^^^
.. program-output:: zuul-admin dequeue --help

Examples::

    zuul-admin dequeue --tenant openstack --pipeline check --project example_project --change 5,1
    zuul-admin dequeue --tenant openstack --pipeline periodic --project example_project --ref refs/heads/master

Enqueue
^^^^^^^
.. program-output:: zuul-admin enqueue --help

Example::

  zuul-admin enqueue --tenant openstack --trigger gerrit --pipeline check --project example_project --change 12345,1

Note that the format of change id is <number>,<patchset>.

Enqueue-ref
^^^^^^^^^^^

.. program-output:: zuul-admin enqueue-ref --help

This command is provided to manually simulate a trigger from an
external source.  It can be useful for testing or replaying a trigger
that is difficult or impossible to recreate at the source.  The
arguments to ``enqueue-ref`` will vary depending on the source and
type of trigger.  Some familiarity with the arguments emitted by
``gerrit`` `update hooks
<https://gerrit-review.googlesource.com/admin/projects/plugins/hooks>`__
such as ``patchset-created`` and ``ref-updated`` is recommended.  Some
examples of common operations are provided below.

Manual enqueue examples
***********************

It is common to have a ``release`` pipeline that listens for new tags
coming from ``gerrit`` and performs a range of code packaging jobs.
If there is an unexpected issue in the release jobs, the same tag can
not be recreated in ``gerrit`` and the user must either tag a new
release or request a manual re-triggering of the jobs.  To re-trigger
the jobs, pass the failed tag as the ``ref`` argument and set
``newrev`` to the change associated with the tag in the project
repository (i.e. what you see from ``git show X.Y.Z``)::

  zuul-admin enqueue-ref --tenant openstack --trigger gerrit --pipeline release --project openstack/example_project --ref refs/tags/X.Y.Z --newrev abc123..

The command can also be used asynchronously trigger a job in a
``periodic`` pipeline that would usually be run at a specific time by
the ``timer`` driver.  For example, the following command would
trigger the ``periodic`` jobs against the current ``master`` branch
top-of-tree for a project::

  zuul-admin enqueue-ref --tenant openstack --trigger timer --pipeline periodic --project openstack/example_project --ref refs/heads/master

Another common pipeline is a ``post`` queue listening for ``gerrit``
merge results.  Triggering here is slightly more complicated as you
wish to recreate the full ``ref-updated`` event from ``gerrit``.  For
a new commit on ``master``, the gerrit ``ref-updated`` trigger
expresses "reset ``refs/heads/master`` for the project from ``oldrev``
to ``newrev``" (``newrev`` being the committed change).  Thus to
replay the event, you could ``git log`` in the project and take the
current ``HEAD`` and the prior change, then enqueue the event::

  NEW_REF=$(git rev-parse HEAD)
  OLD_REF=$(git rev-parse HEAD~1)

  zuul-admin enqueue-ref --tenant openstack --trigger gerrit --pipeline post --project openstack/example_project --ref refs/heads/master --newrev $NEW_REF --oldrev $OLD_REF

Note that zero values for ``oldrev`` and ``newrev`` can indicate
branch creation and deletion; the source code is the best reference
for these more advanced operations.


Promote
^^^^^^^

.. program-output:: zuul-admin promote --help

Example::

  zuul-admin promote --tenant openstack --pipeline gate --changes 12345,1 13336,3

Note that the format of changes id is <number>,<patchset>.

The promote action is used to reorder the changes in a pipeline, by
putting the provided changes at the top of the queue.

The most common use case for the promote action is the need to merge
an urgent fix when the gate pipeline has several patches queued
ahead. This is especially needed if there is concern that one or more
changes ahead in the queue may fail, thus increasing the time to land
for the fix; or concern that the fix may not pass validation if
applied on top of the current patch queue in the gate.

Any items in a dependent pipeline which have had items ahead of them
changed will have their jobs canceled and restarted based on the new
ordering.

If items in independent pipelines are promoted, no jobs will be
restarted, but their change queues within the pipeline will be
re-ordered so that they will be processed first and their node request
priorities will increase.
