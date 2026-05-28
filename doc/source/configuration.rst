:title: Configuration

Configuration
=============

All Zuul processes read the ``/etc/zuul/zuul.conf`` file (an alternate
location may be supplied on the command line) which uses an INI file
syntax.  Each component may have its own configuration file, though
you may find it simpler to use the same file for all components.

Zuul will interpolate environment variables starting with the
``ZUUL_`` prefix given in the config file escaped as python string
expansion.  ``foo=%(ZUUL_HOME)s`` will set the value of ``foo`` to the
same value as the environment variable named ``ZUUL_HOME``.

An example ``zuul.conf``:

.. code-block:: ini

   [zookeeper]
   hosts=zk1.example.com,zk2.example.com,zk3.example.com

   [database]
   dburi=<URI>

   [keystore]
   password=MY_SECRET_PASSWORD

   [web]
   root=https://zuul.example.com/

   [scheduler]
   log_config=/etc/zuul/scheduler-logging.yaml

Common Options
--------------

The following sections of ``zuul.conf`` are used by all Zuul components:

Statsd
~~~~~~

.. attr:: statsd

   Information about the optional statsd server.  If the ``statsd``
   python module is installed and this section is configured,
   statistics will be reported to statsd.  See :ref:`statsd` for more
   information.

   .. attr:: server

      Hostname or IP address of the statsd server.

   .. attr:: port
      :default: 8125

      The UDP port on which the statsd server is listening.

   .. attr:: prefix

      If present, this will be prefixed to all of the keys before
      transmitting to the statsd server.

Tracing
~~~~~~~

.. attr:: tracing

   Information about the optional OpenTelemetry tracing configuration.
   See :ref:`tracing` for more information.

   .. attr:: enabled
      :required:

      To enable tracing, set this value to ``true``.  This is the only
      required parameter in order to export to a collector running
      locally.

   .. attr:: protocol
      :default: grpc

      The OTLP wire protocol to use.

      .. value:: grpc

         Use gRPC (the default).

      .. value:: http/protobuf

         Use HTTP with protobuf encoding.

   .. attr:: endpoint

      The endpoint to use.  The default is protocol specific, but
      defaults to localhost in all cases.

   .. attr:: service_name
      :default: zuul

      The service name may be specified here.  Multiple Zuul
      installations should use different values.

   .. attr:: tls_cert

      The path to the PEM encoded certificate file.  Used only by
      :value:`tracing.protocol.grpc`.

   .. attr:: tls_key

      The path to the PEM encoded key file.  Used only by
      :value:`tracing.protocol.grpc`.

   .. attr:: tls_ca

      The path to the PEM encoded CA certificate file.  Used only by
      :value:`tracing.protocol.grpc`.

   .. attr:: certificate_file

      The path to the PEM encoded certificate file used to verify the
      endpoint.  Used only by :value:`tracing.protocol.http/protobuf`.

   .. attr:: insecure

      Whether to allow an insecure connection.  Used only by
      :value:`tracing.protocol.grpc`.

   .. attr:: timeout
      :default: 10000

      The timeout for outgoing data in milliseconds.

   .. attr:: compression

      The compression algorithm to use.  Available values depend on
      the protocol and endpoint.  The only universally supported value
      is ``gzip``.

ZooKeeper
~~~~~~~~~

.. attr:: zookeeper

   Client connection information for ZooKeeper.  TLS is required.

   .. attr:: hosts
      :required:

      A list of zookeeper hosts for Zuul to use.

   .. attr:: tls_cert
      :required:

      The path to the PEM encoded certificate file.

   .. attr:: tls_key
      :required:

      The path to the PEM encoded key file.

   .. attr:: tls_ca
      :required:

      The path to the PEM encoded CA certificate file.

   .. attr:: session_timeout
      :default: 10.0

      The ZooKeeper session timeout, in seconds.


.. _database:

Database
~~~~~~~~

.. attr:: database

   .. attr:: dburi
      :required:

      Database connection information in the form of a URI understood
      by SQLAlchemy.  See `The SQLAlchemy manual
      <https://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls>`_
      for more information.

      Zuul supports PostgreSQL, MySQL, and MariaDB.  Supported
      SQLAlchemy dialects and drivers are: ``postgresql://``,
      ``mysql+pymysql://``, and ``mariadb+pymysql``.

      If using MariaDB, be sure to use the ``mariadb`` dialect.

      The driver will automatically set up the database creating and managing
      the necessary tables. Therefore the provided user should have sufficient
      permissions to manage the database. For example:

      .. code-block:: sql

        GRANT ALL ON my_database TO 'my_user'@'%';

   .. attr:: pool_recycle
      :default: 1

      Tune the pool_recycle value. See `The SQLAlchemy manual on pooling
      <http://docs.sqlalchemy.org/en/latest/core/pooling.html#setting-pool-recycle>`_
      for more information.

   .. attr:: table_prefix
      :default: ''

      The string to prefix the table names. This makes it possible to run
      several zuul deployments against the same database. This can be useful
      if you rely on external databases which are not under your control.
      The default is to have no prefix.

OIDC
~~~~

.. attr:: oidc

   This optional section of ``zuul.conf``, if present, can be used to
   customize the configuration of Zuul as an OpenId Connect (OIDC)
   Identity Provider.

   .. attr:: supported_signing_algorithms
      :default: RS256

      Algorithms that should be supported for signing the OIDC tokens,
      a string of algorithm names separated by ``,``. Currently
      ``RS256`` is supported.

   .. attr:: default_signing_algorithm
      :default: RS256

      The default algorithm used for signing the OIDC tokens if not
      specified in secret configuration.

   .. attr:: signing_key_rotation_interval
      :default: 604800

      The rotation interval of the signing key, in seconds.

.. _scheduler:

Scheduler
---------

The scheduler is the primary component of Zuul.  The scheduler is a
scalable component; one or more schedulers must be running at all
times for Zuul to be operational.  It receives events from any
connections to remote systems which have been configured, enqueues
items into pipelines, distributes jobs to executors, and reports
results.

The scheduler must be able to connect to the ZooKeeper cluster.  It
must also be able to connect to any services for which connections are
configured (Gerrit, GitHub, etc).

The following sections of ``zuul.conf`` are used by the scheduler:


.. attr:: web

   .. attr:: root
      :required:

      The root URL of the web service (e.g.,
      ``https://zuul.example.com/``).

      See :attr:`tenant.web-root` for additional options for
      whitelabeled tenant configuration.

.. attr:: keystore

   .. _keystore-password:

   .. attr:: password
      :required:

      Encryption password for private data stored in Zookeeper.

.. attr:: scheduler

   .. attr:: command_socket
      :default: /var/lib/zuul/scheduler.socket

      Path to command socket file for the scheduler process.

   .. attr:: tenant_config

      Path to :ref:`tenant-config` file. This attribute
      is exclusive with :attr:`scheduler.tenant_config_script`.

   .. attr:: tenant_config_script

      Path to a script to execute and load the tenant
      config from. This attribute is exclusive with
      :attr:`scheduler.tenant_config`.

   .. attr:: default_ansible_version

      Default ansible version to use for jobs that doesn't specify a version.
      See :attr:`job.ansible-version` for details.

   .. attr:: log_config

      Path to log config file.

   .. attr:: pidfile
      :default: /var/run/zuul/scheduler.pid

      Path to PID lock file.

   .. attr:: relative_priority
      :default: False

      A boolean which indicates whether the scheduler should supply
      relative priority information for node requests.

      In all cases, each pipeline may specify a precedence value which
      is used to satisfy requests from higher-precedence
      pipelines first.  If ``relative_priority`` is set to ``True``,
      then Zuul will additionally group items in the same pipeline by
      pipeline queue and weight each request by its position in that
      project's group.  A request for the first change in a given
      queue will have the highest relative priority, and the second
      change a lower relative priority.  The first change of each
      queue in a pipeline has the same relative priority, regardless
      of the order of submission or how many other changes are in the
      pipeline.  This can be used to make node allocations complete
      faster for projects with fewer changes in a system dominated by
      projects with more changes.

      After the first 10 changes, the relative priority becomes more
      coarse (batching groups of 10 changes at the same priority).
      Likewise, after 100 changes they are batched in groups of 100.
      This is to avoid causing additional load with unnecessary
      priority changes if queues are long.

      If this value is ``False`` (the default), then node requests are
      sorted by pipeline precedence followed by the order in which
      they were submitted.  If this is ``True``, they are sorted by
      pipeline precedence, followed by relative priority, and finally
      the order in which they were submitted.

   .. attr:: default_hold_expiration
      :default: max_hold_expiration

      The default value for held node expiration if not supplied. This
      will default to the value of ``max_hold_expiration`` if not changed,
      or if it is set to a higher value than the max.

   .. attr:: max_hold_expiration
      :default: 0

      Maximum number of seconds any nodes held for an autohold request
      will remain available. A value of 0 disables this, and the nodes
      will remain held until the autohold request is manually deleted.
      If a value higher than ``max_hold_expiration`` is supplied during
      hold request creation, it will be lowered to this value.

   .. attr:: prometheus_port

      Set a TCP port to start the prometheus metrics client.

   .. attr:: prometheus_addr
      :default: 0.0.0.0

      The IPv4 addr to listen for prometheus metrics poll.
      To use IPv6, python>3.8 is required `issue24209 <https://bugs.python.org/issue24209>`_.



Merger
------

Mergers are an optional Zuul service; they are not required for Zuul
to operate, but some high volume sites may benefit from running them.
Zuul performs quite a lot of git operations in the course of its work.
Each change that is to be tested must be speculatively merged with the
current state of its target branch to ensure that it can merge, and to
ensure that the tests that Zuul perform accurately represent the
outcome of merging the change.  Because Zuul's configuration is stored
in the git repos it interacts with, and is dynamically evaluated, Zuul
often needs to perform a speculative merge in order to determine
whether it needs to perform any further actions.

All of these git operations add up, and while Zuul executors can also
perform them, large numbers may impact their ability to run jobs.
Therefore, administrators may wish to run standalone mergers in order
to reduce the load on executors.

Mergers need to be able to connect to the ZooKeeper cluster as well as
any services for which connections are configured (Gerrit, GitHub,
etc).

The following section of ``zuul.conf`` is used by the merger:

.. attr:: merger

   .. attr:: command_socket
      :default: /var/lib/zuul/merger.socket

      Path to command socket file for the merger process.

   .. attr:: git_dir
      :default: /var/lib/zuul/merger-git

      Directory in which Zuul should clone git repositories.

   .. attr:: git_http_low_speed_limit
      :default: 1000

      If the HTTP transfer speed is less then git_http_low_speed_limit for
      longer then git_http_low_speed_time, the transfer is aborted.

      Value in bytes, setting to 0 will disable.

   .. attr:: git_http_low_speed_time
      :default: 30

      If the HTTP transfer speed is less then git_http_low_speed_limit for
      longer then git_http_low_speed_time, the transfer is aborted.

      Value in seconds, setting to 0 will disable.

   .. attr:: git_timeout
      :default: 300

      Timeout for git clone and fetch operations. This can be useful when
      dealing with large repos. Note that large timeouts can increase startup
      and reconfiguration times if repos are not cached so be cautious when
      increasing this value.

      Value in seconds.

   .. attr:: git_user_email

      Value to pass to `git config user.email
      <https://git-scm.com/book/en/v2/Getting-Started-First-Time-Git-Setup>`_.

   .. attr:: git_user_name

      Value to pass to `git config user.name
      <https://git-scm.com/book/en/v2/Getting-Started-First-Time-Git-Setup>`_.

   .. attr:: log_config

      Path to log config file for the merger process.

   .. attr:: pidfile
      :default: /var/run/zuul/merger.pid

      Path to PID lock file for the merger process.

   .. attr:: prometheus_port

      Set a TCP port to start the prometheus metrics client.

   .. attr:: prometheus_addr
      :default: 0.0.0.0

      The IPv4 addr to listen for prometheus metrics poll.
      To use IPv6, python>3.8 is required `issue24209 <https://bugs.python.org/issue24209>`_.

.. _executor:

Executor
--------

Executors are responsible for running jobs.  At the start of each job,
an executor prepares an environment in which to run Ansible which
contains all of the git repositories specified by the job with all
dependent changes merged into their appropriate branches.  The branch
corresponding to the proposed change will be checked out (in all
projects, if it exists).  Any roles specified by the job will also be
present (also with dependent changes merged, if appropriate) and added
to the Ansible role path.  The executor also prepares an Ansible
inventory file with all of the nodes requested by the job.

The executor also contains a merger.  This is used by the executor to
prepare the git repositories used by jobs, but is also available to
perform any tasks normally performed by standalone mergers.  Because
the executor performs both roles, small Zuul installations may not
need to run standalone mergers.

Executors need to be able to connect to the ZooKeeper cluster, any
services for which connections are configured (Gerrit, GitHub, etc),
as well as directly to the build nodes.

Trusted and Untrusted Playbooks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The executor runs playbooks in one of two execution contexts depending
on whether the project containing the playbook is a
:term:`config-project` or an :term:`untrusted-project`.  If the
playbook is in a config project, the executor runs the playbook in the
*trusted* execution context, otherwise, it is run in the *untrusted*
execution context.

Both execution contexts use `bubblewrap`_ to create a namespace to
ensure that playbook executions are isolated and are unable to access
files outside of a restricted environment.  The administrator may
configure additional local directories on the executor to be made
available to the restricted environment.

.. _bubblewrap: https://github.com/projectatomic/bubblewrap

.. _executor_security:

Security Considerations
~~~~~~~~~~~~~~~~~~~~~~~

Bubblewrap restricts access to files outside of the build environment
in both execution contexts.  Operators may allow either read-only or
read-write access to additional paths in either the `trusted` context
or both contexts with additional options described below.  Be careful
when adding additional paths, and consider that any `trusted` or
`untrusted` (as appropriate) playbook will have access to these paths.

If executors are configured to use WinRM certificates, these must be
made available to the bubblewrap environment in order for Ansible to
use them.  This invariably makes them accessible to any playbook in
that execution context.  Operators may want to consider only supplying
WinRM credentials to trusted playbooks and installing per-build
certificates in a pre-playbook; or using Ansible's experimental SSH
support instead of WinRM.

Local code execution is permitted on the executor, so if a
vulnerability in bubblewrap or the kernel allows for an escape from
the restricted environment, users may be able to escalate their
privileges and obtain access to any data or secrets available to the
executor.

Playbooks which run on the executor will have the same network access
as the executor itself.  This should be kept in mind when considering
IP-based network access control within an organization.  Zuul's
internal communication is via ZooKeeper which is authenticated and
secured by TLS certificates, so as long as these certificates are not
made available to jobs, users should not be able to access or disrupt
Zuul's internal communications.  However, statsd is an unauthenticated
protocol, so a malicious user could emit false statsd information.

If the Zuul executor is running in a cloud environment with a network
metadata service, users may be able to access that service.  If it
supplies credentials, they may be able to obtain those credentials and
access cloud resources.  Operators should ensure that in these
environments, the executors are configured with appropriately
restricted IAM profiles.

Configuration
~~~~~~~~~~~~~

The following sections of ``zuul.conf`` are used by the executor:

.. attr:: executor

   .. attr:: command_socket
      :default: /var/lib/zuul/executor.socket

      Path to command socket file for the executor process.

   .. attr:: finger_port
      :default: 7900

      Port to use for finger log streamer.

   .. attr:: state_dir
      :default: /var/lib/zuul

      Path to directory in which Zuul should save its state.

   .. attr:: git_dir
      :default: /var/lib/zuul/executor-git

      Directory that Zuul should clone local git repositories to.  The
      executor keeps a local copy of every git repository it works
      with to speed operations and perform speculative merging.

      This should be on the same filesystem as
      :attr:`executor.job_dir` so that when git repos are cloned into
      the job workspaces, they can be hard-linked to the local git
      cache.

   .. attr:: job_dir
      :default: /var/lib/zuul/builds

      Directory that Zuul should use to hold temporary job directories.
      When each job is run, a new entry will be created under this
      directory to hold the configuration and scratch workspace for
      that job.  It will be deleted at the end of the job (unless the
      `--keep-jobdir` command line option is specified).

      This should be on the same filesystem as :attr:`executor.git_dir`
      so that when git repos are cloned into the job workspaces, they
      can be hard-linked to the local git cache.

   .. attr:: log_config

      Path to log config file for the executor process.

   .. attr:: pidfile
      :default: /var/run/zuul/executor.pid

      Path to PID lock file for the executor process.

   .. attr:: private_key_file
      :default: ~/.ssh/id_rsa

      SSH private key file to be used when logging into build nodes.

      .. note:: If you use an RSA key, ensure it is encoded in the PEM
                format (use the ``-t rsa -m PEM`` arguments to
                `ssh-keygen`).

   .. attr:: default_username
      :default: zuul

      Username to use when logging into build nodes, if none is
      supplied by the image configuration.

   .. attr:: winrm_cert_key_file
      :default: ~/.winrm/winrm_client_cert.key

      The private key file of the client certificate to use for winrm
      connections to Windows nodes.

   .. attr:: winrm_cert_pem_file
      :default: ~/.winrm/winrm_client_cert.pem

      The certificate file of the client certificate to use for winrm
      connections to Windows nodes.

      .. note:: Currently certificate verification is disabled when
                connecting to Windows nodes via winrm.

   .. attr:: winrm_operation_timeout_sec
      :default: None. The Ansible default of 20 is used in this case.

      The timeout for WinRM operations.

   .. attr:: winrm_read_timeout_sec
      :default: None. The Ansible default of 30 is used in this case.

      The timeout for WinRM read. Increase this if there are intermittent
      network issues and read timeout errors keep occurring.

   .. _admin_sitewide_variables:

   .. attr:: variables

      Path to an Ansible variables file to supply site-wide variables.
      This should be a YAML-formatted file consisting of a single
      dictionary.  The contents will be made available to all jobs as
      Ansible variables.  These variables take precedence over all
      other forms (job variables and secrets).  Care should be taken
      when naming these variables to avoid potential collisions with
      those used by jobs.  Prefixing variable names with a
      site-specific identifier is recommended.  The default is not to
      add any site-wide variables.  See the :ref:`User's Guide
      <user_jobs_sitewide_variables>` for more information.

   .. attr:: manage_ansible
      :default: True

      Specifies whether the zuul-executor should install the supported ansible
      versions during startup or not. If this is ``True`` the zuul-executor
      will install the ansible versions into :attr:`executor.ansible_root`.

      It is recommended to set this to ``False`` and manually install Ansible
      after the Zuul installation by running ``zuul-manage-ansible``. This has
      the advantage that possible errors during Ansible installation can be
      spotted earlier. Further especially containerized deployments of Zuul
      will have the advantage of predictable versions.

   .. attr:: ansible_root
      :default: <state_dir>/ansible-bin

      Specifies where the zuul-executor should look for its supported ansible
      installations. By default it looks in the following directories and uses
      the first which it can find.

      * ``<zuul_install_dir>/lib/zuul/ansible``
      * ``<ansible_root>``

      The ``ansible_root`` setting allows you to override the second location
      which is also used for installation if ``manage_ansible`` is ``True``.

   .. attr:: ansible_setup_timeout
      :default: 60

      Timeout of the ansible setup playbook in seconds that runs before
      the first playbook of the job.

   .. attr:: disk_limit_per_job
      :default: 250

      This integer is the maximum number of megabytes that any one job
      is allowed to consume on disk while it is running. If a job's
      scratch space has more than this much space consumed, it will be
      aborted. Set to -1 to disable the limit.

   .. attr:: trusted_ro_paths

      List of paths, separated by ``:`` to read-only bind mount into
      trusted bubblewrap contexts.

   .. attr:: trusted_rw_paths

      List of paths, separated by ``:`` to read-write bind mount into
      trusted bubblewrap contexts.

   .. attr:: untrusted_ro_paths

      List of paths, separated by ``:`` to read-only bind mount into
      untrusted bubblewrap contexts.

   .. attr:: untrusted_rw_paths

      List of paths, separated by ``:`` to read-write bind mount into
      untrusted bubblewrap contexts.

   .. attr:: load_multiplier
      :default: 2.5

      When an executor host gets too busy, the system may suffer
      timeouts and other ill effects. The executor will stop accepting
      more than 1 job at a time until load has lowered below a safe
      level.  This level is determined by multiplying the number of
      CPU's by `load_multiplier`.

      So for example, if the system has 2 CPUs, and load_multiplier
      is 2.5, the safe load for the system is 5.00. Any time the
      system load average is over 5.00, the executor will quit
      accepting multiple jobs at one time.

      The executor will observe system load and determine whether
      to accept more jobs every 30 seconds.

   .. attr:: max_starting_builds
      :default: None

      An executor is accepting up to as many starting builds as defined by the
      :attr:`executor.load_multiplier` on systems with more than four CPU cores,
      and up to twice as many on systems with four or less CPU cores. For
      example, on a system with two CPUs: 2 * 2.5 * 2 - up to ten starting
      builds may run on such executor; on systems with eight CPUs: 2.5 * 8 - up
      to twenty starting builds may run on such executor.

      On systems with high CPU/vCPU count an executor may accept too many
      starting builds. This can be overwritten using this option providing a
      fixed number of maximum starting builds on an executor.

   .. attr:: min_avail_hdd
      :default: 5.0

      This is the minimum percentage of HDD storage available for the
      :attr:`executor.state_dir` directory. The executor will stop accepting
      more than 1 job at a time until more HDD storage is available. The
      available HDD percentage is calculated from the total available
      disk space divided by the total real storage capacity multiplied by
      100.

   .. attr:: min_avail_inodes
      :default: 5.0

      This is the minimum percentage of HDD inodes available for the
      :attr:`executor.state_dir` directory. The executor will stop accepting
      more than 1 job at a time until more inodes are available. The
      available inode percentage is calculated from the total available
      inodes divided by the total real inode capacity multiplied by
      100.

   .. attr:: min_avail_mem
      :default: 5.0

      This is the minimum percentage of system RAM available. The
      executor will stop accepting more than 1 job at a time until
      more memory is available. The available memory percentage is
      calculated from the total available memory divided by the
      total real memory multiplied by 100. Buffers and cache are
      considered available in the calculation.

   .. attr:: output_max_bytes
      :default: 1073741824

      .. warning:: This option is deprecated.  In the future, the
                   default value of 1GiB is likely to become fixed and
                   unable to be changed.  Set this option only if
                   needed and only as long as needed to adjust
                   existing jobs to avoid the limit.

      Zuul limits the total number of bytes output via stdout or
      stderr from a single Ansible command to this value.  If the
      command outputs more than this number of bytes, the command
      execution will fail.  This is to protect the executor from being
      required to read an excessively large amount of data from an
      ansible task result.

      If a job fails due to this limit, consider adjusting the command
      task to redirect output to a file and collecting the file
      separately.

   .. attr:: hostname
      :default: hostname of the server

      The executor needs to know its hostname under which it is reachable by
      zuul-web. Otherwise live console log streaming doesn't work. In most cases
      This is automatically detected correctly. But when running in environments
      where it cannot determine its hostname correctly this can be overridden
      here.

   .. attr:: paused_on_start
      :default: false

      Whether the executor should start in a paused mode. Such executor will not
      accept tasks until it is unpaused.

   .. attr:: zone
      :default: None

      Name of the executor-zone to exclusively execute all jobs that
      have nodes with the specified executor-zone attribute.  As an
      example, it is possible for nodes to exist in a cloud without
      public accessible IP address. By adding an executor to a zone,
      nodes may be configured to use private ip addresses.

      To enable this in Nodepool, you'll use the node-attributes setting in a
      provider pool. For example:

      .. code-block:: yaml

        pools:
          - name: main
            node-attributes:
              executor-zone: vpn

      To enable this with Zuul's provider configuration, set the
      ``executor-zone`` attribute of the label.

   .. attr:: allow_unzoned
      :default: False

      If :attr:`executor.zone` is set it by default only processes jobs with
      nodes of that specific zone even if the nodes have no zone at all.
      Enabling ``allow_unzoned`` lets the executor also take jobs with nodes
      without zone.

   .. attr:: merge_jobs
      :default: True

      To disable global merge job, set it to false. This is useful for zoned
      executors that are running on slow network where you don't want them to
      perform merge operations for any events. The executor will still perform
      the merge operations required for the build they are executing.

   .. attr:: sigterm_method
      :default: graceful

      Determines how the executor responds to a ``SIGTERM`` signal.

      .. value:: graceful

         Stop accepting new jobs and wait for all running jobs to
         complete before exiting.

      .. value:: stop

         Abort all running jobs and exit as soon as possible.

   .. attr:: prometheus_port

      Set a TCP port to start the prometheus metrics client.

   .. attr:: prometheus_addr
      :default: 0.0.0.0

      The IPv4 addr to listen for prometheus metrics poll.
      To use IPv6, python>3.8 is required `issue24209 <https://bugs.python.org/issue24209>`_.

   The following attributes are available for experimental control of
   cgroups.  This feature may change or be removed at any time.

   .. attr:: user

      User ID for the executor process.  In order to configure cgroup
      control, the executor must perform some actions as root when
      starting.  If this option is set, it will drop privileges to
      this user during startup.  It is recommended to set this option
      to an unprivileged user.

      .. note:: This option is experimental and may change or be
                removed at any time.

   .. attr:: cgroup_memory_limit

      If this option is set, the executor will configure cgroups to
      limit the total memory collectively available to all builds that
      it runs.  This should be set to a portion of the available
      memory.  For example, if the system has 10GiB of RAM available,
      this might be set to 8589934592 (8GiB) to allow all currently
      running builds to use up to that value, reserving 2GiB for the
      executor itself and any ancillary processes.  If any one job
      exceeds the limit, the kernel OOM killer will kill build
      processes rather than the executor.

      .. note:: This option is experimental and may change or be
                removed at any time.


.. attr:: keystore

   .. attr:: password
      :required:

      Encryption password for private data stored in Zookeeper.

.. attr:: merger

   .. attr:: git_user_email

      Value to pass to `git config user.email
      <https://git-scm.com/book/en/v2/Getting-Started-First-Time-Git-Setup>`_.

   .. attr:: git_user_name

      Value to pass to `git config user.name
      <https://git-scm.com/book/en/v2/Getting-Started-First-Time-Git-Setup>`_.

   .. attr:: prometheus_port

      Set a TCP port to start the prometheus metrics client.

   .. attr:: prometheus_addr
      :default: 0.0.0.0

      The IPv4 addr to listen for prometheus metrics poll.
      To use IPv6, python>3.8 is required `issue24209 <https://bugs.python.org/issue24209>`_.

.. attr:: ansible_callback "<name>"

   To whitelist ansible callback ``<name>``. Any attributes found is this section
   will be added to the ``callback_<name>`` section in ansible.cfg.

   An example of what configuring the builtin mail callback would look like.
   The configuration in zuul.conf.

   .. code-block:: ini

      [ansible_callback "mail"]
      to = user@example.org
      sender = zuul@example.org

   Would generate the following in ansible.cfg:

   .. code-block:: ini

      [defaults]
      callback_whitelist = mail

      [callback_mail]
      to = user@example.org
      sender = zuul@example.org

.. _web-server:

Web Server
----------

.. TODO: Turn REST API into a link to swagger docs when we grow them

The Zuul web server serves as the single process handling all HTTP
interactions with Zuul. This includes the websocket interface for live
log streaming, the REST API and the html/javascript dashboard. All three are
served as a holistic web application. For information on additional supported
deployment schemes, see :ref:`web-deployment-options`.

Web servers need to be able to connect to the ZooKeeper cluster and
the SQL database.  If a GitHub, Gitlab, or Pagure connection is
configured, they need to be reachable so they may receive
notifications.

In addition to the common configuration sections, the following
sections of ``zuul.conf`` are used by the web server:

.. attr:: web

   .. attr:: command_socket
      :default: /var/lib/zuul/web.socket

      Path to command socket file for the web process.

   .. attr:: listen_address
      :default: 127.0.0.1

      IP address or domain name on which to listen.

   .. attr:: log_config

      Path to log config file for the web server process.

   .. attr:: pidfile
      :default: /var/run/zuul/web.pid

      Path to PID lock file for the web server process.

   .. attr:: port
      :default: 9000

      Port to use for web server process.

   .. attr:: websocket_url

      Base URL on which the websocket service is exposed, if different
      than the base URL of the web app.

   .. attr:: stats_url

      Base URL from which statistics emitted via statsd can be queried.

   .. attr:: stats_type
      :default: graphite

      Type of server hosting the statistics information. Currently only
      'graphite' is supported by the dashboard.

   .. attr:: static_path
      :default: zuul/web/static

      Path containing the static web assets.

   .. attr:: static_cache_expiry
      :default: 3600

      The Cache-Control max-age response header value for static files served
      by the zuul-web. Set to 0 during development to disable Cache-Control.

   .. attr:: zone

      The zone in which zuul-web is deployed. This is only needed if
      there are executors with different zones in the environment and
      not all executors are directly addressable from zuul-web. The
      parameter specifies the zone where the executors are directly
      addressable. Live log streaming will go directly to the executors
      of the same zone and be routed to a finger gateway of the target
      zone if the zones are different.

      In a mixed system (with zoned and unzoned executors) there may
      also be zoned and unzoned zuul-web services. Omit the zone
      parameter for any unzoned zuul-web servers.

      If this is used the finger gateways should be configured accordingly.

   .. attr:: auth_log_file_requests
      :default: false

      If set to true, the JavaScript web client will pass an Authorization
      header with HTTP requests for log files if the origin of a log file is
      the same as the Zuul API. This is useful when build logs are served
      through the same authenticated endpoint as the API (e.g. a reverse
      proxy).

.. attr:: keystore

   .. attr:: password
      :required:

      Encryption password for private data stored in Zookeeper.

Authentication
~~~~~~~~~~~~~~

A user can be granted access to protected REST API endpoints by providing a
valid JWT (JSON Web Token) as a bearer token when querying the API endpoints.

JWTs are signed and therefore Zuul must be configured so that signatures can be
verified. More information about the JWT standard can be found on the `IETF's
RFC page <https://tools.ietf.org/html/rfc7519>`_.

This optional section of ``zuul.conf``, if present, will activate the
protected endpoints and configure JWT validation:

.. attr:: auth <authenticator name>

   .. attr:: driver

      The signing algorithm to use. Accepted values are ``HS256``, ``RS256``,
      ``RS256withJWKS`` or ``OpenIDConnect``. See below for driver-specific
      configuration options.

   .. attr:: allow_authz_override
      :default: false

      Allow a JWT to override predefined access rules. See the section on
      :ref:`JWT contents <jwt-format>` for more details on how to grant access
      to tenants with a JWT.

   .. attr:: realm

      The authentication realm.

   .. attr:: default
      :default: false

      If set to ``true``, use this realm as the default authentication realm
      when handling HTTP authentication errors.

   .. attr:: client_id

      The expected value of the "aud" claim in the JWT. This is required for
      validation.

   .. attr:: issuer_id

      The expected value of the "iss" claim in the JWT. This is required for
      validation.

   .. attr:: uid_claim
      :default: sub

      The JWT claim that Zuul will use as a unique identifier for the bearer of
      a token. This is "sub" by default, as it is usually the purpose of this
      claim in a JWT. This identifier is used in audit logs.

   .. attr:: max_validity_time

      Optional value to ensure a JWT cannot be valid for more than this amount
      of time in seconds. This is useful if the Zuul operator has no control
      over the service issuing JWTs, and the tokens are too long-lived.

   .. attr:: skew
      :default: 0

      Optional integer value to compensate for skew between Zuul's and the
      JWT emitter's respective clocks. Use a negative value if Zuul's clock
      is running behind.

This section can be repeated as needed with different authenticators, allowing
access to privileged API actions from several JWT issuers.

Driver-specific attributes
..........................

HS256
,,,,,

This is a symmetrical encryption algorithm that only requires a shared secret
between the JWT issuer and the JWT consumer (ie Zuul). This driver should be
used in test deployments, or in deployments where JWTs may be issued manually
to users.

.. note:: At least one HS256 authenticator should be configured in order to use admin commands with the Zuul command line interface.

.. attr:: secret
   :noindex:

   The shared secret used to sign JWTs and validate signatures.

RS256
,,,,,

This is an asymmetrical encryption algorithm that requires an RSA key pair. Only
the public key is needed by Zuul for signature validation.

.. attr:: public_key

   The path to the public key of the RSA key pair. It must be readable by Zuul.

.. attr:: private_key

   Optional. The path to the private key of the RSA key pair. It must be
   readable by Zuul.

RS256withJWKS
,,,,,,,,,,,,,

.. warning::

   This driver is deprecated, use ``OpenIDConnect`` instead.

Some Identity Providers use key sets (also known as **JWKS**), therefore the key to
use when verifying the Authentication Token's signatures cannot be known in
advance; the key's id is stored in the JWT's header and the key must then be
found in the remote key set.
The key set is usually available at a specific URL that can be found in the
"well-known" configuration of an OpenID Connect Identity Provider.

.. attr:: keys_url

   The URL where the Identity Provider's key set can be found. For example, for
   Google's OAuth service: https://www.googleapis.com/oauth2/v3/certs

OpenIDConnect
,,,,,,,,,,,,,

Use a third-party Identity Provider implementing the OpenID Connect protocol.
The issuer ID should be an URI, from which the "well-known" configuration URI
of the Identity Provider can be inferred. This is intended to be used for
authentication on Zuul's web user interface.

.. attr:: scope
   :default: openid profile

   The scope(s) to use when requesting access to a user's details. This attribute
   can be multivalued (values must be separated by a space). Most OpenID Connect
   Identity Providers support the default scopes "openid profile". A full list
   of supported scopes can be found in the well-known configuration of the
   Identity Provider under the key "scopes_supported".

.. attr:: keys_url

   Optional. The URL where the Identity Provider's key set can be found.
   For example, for Google's OAuth service: https://www.googleapis.com/oauth2/v3/certs
   The well-known configuration of the Identity Provider should provide this URL
   under the key "jwks_uri", therefore this attribute is usually not necessary.

Some providers may not conform to the JWT specification and further
configuration may be necessary.  In these cases, the following
additional values may be used:

.. attr:: authority
   :default: issuer_id

   If the authority in the token response is not the same as the
   issuer_id in the request, it may be explicitly set here.

.. attr:: audience
   :default: client_id

   If the audience in the token response is not the same as the
   issuer_id in the request, it may be explicitly set here.

.. attr:: load_user_info
   :default: true

   If the web UI should skip accessing the "UserInfo" endpoint and
   instead rely only on the information returned in the token, set
   this to ``false``.

.. _launcher:

Launcher
--------

The launcher is responsible for the lifecycle of build nodes and
other resources.  They may be virtual machines (ephemeral or
long-lived), real hardware with static addresses, containers, or other
resources.  There are a number of drivers that implement functionality
for static hosts and various cloud providers.

The launcher must communicate with ZooKeeper and also the build nodes
that it is responsible for.  Multiple launchers may be run, and they
will work cooperatively to scale out their handling of requests.
Certain launchers may be restricetd by configuration to servicing only
certain connections.

In addition to the common configuration sections, the following
sections of ``zuul.conf`` are used by the launcher:

.. attr:: launcher

   .. attr:: command_socket
      :default: /var/lib/zuul/launcher.socket

      Path to command socket file for the launcher process.

   .. attr:: temp_dir
      :default: /tmp

      Temporary directory used for downloading images while uploading
      them to providers.  Depending on the number of images and their
      sizes that are managed by Zuul, this space may need to be very
      large.

   .. attr:: connection_filter

      If this launcher should only be used for certain connections,
      specify them here as a comma separated string.

   .. attr:: log_config

      Path to log config file for the launcher process.

Client
------

Zuul's command line client may be configured to make calls to Zuul's web
server. The client will then look for a ``zuul.conf`` file with a ``webclient``
section to set up the connection over HTTP.

.. note:: At least one authenticator must be configured in Zuul for admin commands to be enabled in the client.

.. attr:: webclient

   .. attr:: url

      The root URL of Zuul's web server.

   .. attr:: verify_ssl
      :default: true

      Enforce SSL verification when sending requests over to Zuul's web server.
      This should only be disabled when working with test servers.


Finger Gateway
--------------

The Zuul finger gateway listens on the standard finger port (79) for
finger requests specifying a build UUID for which it should stream log
results. The gateway will determine which executor is currently running that
build and query that executor for the log stream.

This is intended to be used with the standard finger command line client.
For example::

    finger UUID@zuul.example.com

The above would stream the logs for the build identified by `UUID`.

Finger gateway servers need to be able to connect to the ZooKeeper
cluster, as well as the console streaming port on the executors
(usually 7900).

Finger gateways are optional.  They may be run for either or both of
the following purposes:

* Allowing end-users to connect to the finger port to stream logs.

* Providing an accessible log streaming port for remote zoned
  executors which are otherwise inaccessible.

  In this case, log streaming requests from finger gateways or
  zuul-web will route to the executors via finger gateways in the same
  zone.

In addition to the common configuration sections, the following
sections of ``zuul.conf`` are used by the finger gateway:

.. attr:: fingergw

   .. attr:: command_socket
      :default: /var/lib/zuul/fingergw.socket

      Path to command socket file for the executor process.

   .. attr:: listen_address
      :default: all addresses

      IP address or domain name on which to listen.

   .. attr:: log_config

      Path to log config file for the finger gateway process.

   .. attr:: pidfile
      :default: /var/run/zuul/fingergw.pid

      Path to PID lock file for the finger gateway process.

   .. attr:: port
      :default: 79

      Port to use for the finger gateway. Note that since command line
      finger clients cannot usually specify the port, leaving this set to
      the default value is highly recommended.

   .. attr:: user

      User ID for the zuul-fingergw process. In normal operation as a
      daemon, the finger gateway should be started as the ``root``
      user, but if this option is set, it will drop privileges to this
      user during startup.  It is recommended to set this option to an
      unprivileged user.

   .. attr:: hostname
      :default: hostname of the server

      When running finger gateways in a multi-zone configuration, the
      gateway needs to know its hostname under which it is reachable
      by zuul-web. Otherwise live console log streaming doesn't
      work. In most cases This is automatically detected
      correctly. But when running in environments where it cannot
      determine its hostname correctly this can be overridden here.

   .. attr:: zone

      The zone where the finger gateway is located. This is only needed for
      live log streaming if the zuul deployment is spread over multiple
      zones without the ability to directly connect to all executors from
      zuul-web. See :attr:`executor.zone` for further information.

      In a mixed system (with zoned and unzoned executors) there may
      also be zoned and unzoned finger gateway services. Omit the zone
      parameter for any unzoned finger gateway servers.

  If the Zuul installation spans an untrusted network (for example, if
  there are remote executor zones), it may be necessary to use TLS
  between the components that handle log streaming (zuul-executor,
  zuul-fingergw, and zuul-web).  If so, set the following options.

  Note that this section is also read by zuul-web in order to load a
  client certificate to use when connecting to a finger gateway which
  requires TLS, and it is also read by zuul-executor to load a server
  certificate for its console streaming port.

  If any of these are present, all three certificate options must be
  provided.

   .. attr:: tls_cert

      The path to the PEM encoded certificate file.

   .. attr:: tls_key

      The path to the PEM encoded key file.

   .. attr:: tls_ca

      The path to the PEM encoded CA certificate file.

   .. attr:: tls_verify_hostnames
      :default: true

      In the case of a private CA it may be both safe and convenient
      to disable hostname checks.  However, if the certificates are
      issued by a public CA, hostname verification should be enabled.

   .. attr:: tls_client_only
      :default: false

      In order to provide a finger gateway which can reach remote
      finger gateways and executors which use TLS, but does not itself
      serve end-users via TLS (i.e., it runs within a protected
      network and users access it directly via the finger port), set
      this to ``true`` and the finger gateway will not listen on TLS,
      but will still use the supplied certificate to make remote TLS
      connections.

.. _connections:

Connections
===========

Most of Zuul's configuration is contained in the git repositories upon
which Zuul operates, however, some configuration outside of git
repositories is still required to bootstrap the system.  This includes
information on connections between Zuul and other systems, as well as
identifying the projects Zuul uses.

In order to interact with external systems, Zuul must have a
*connection* to that system configured.  Zuul includes a number of
:ref:`drivers <drivers>`, each of which implements the functionality
necessary to connect to a system.  Each connection in Zuul is
associated with a driver.

Connections are also used for communicating with cloud providers used
by the Zuul launcher.

To configure a connection in Zuul, select a unique name for the
connection and add a section to ``zuul.conf`` with the form
``[connection NAME]``.  For example, a connection to a gerrit server
may appear as:

.. code-block:: ini

  [connection mygerritserver]
  driver=gerrit
  server=review.example.com

Zuul needs to use a single connection to look up information about
changes hosted by a given system.  When it looks up changes, it will
do so using the first connection it finds that matches the server name
it's looking for.  It's generally best to use only a single connection
for a given server, however, if you need more than one (for example,
to satisfy unique reporting requirements) be sure to list the primary
connection first as that is what Zuul will use to look up all changes
for that server.
