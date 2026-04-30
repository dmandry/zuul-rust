.. _secret:

Secret
======

Zuul supports two types of secrets:

* A collection of private data for use by one or more jobs,
  hereafter referred to as a `data secret`.

* Automatic OpenID Connect (OIDC) token generation for use by jobs,
  hereafter referred to as a `token secret`.

A given secret defined in Zuul may only perform one of those roles
(they are mutually exclusive).

Data Secrets
------------

A `data secret` is one where values are specified using the
:attr:`secret.data` attribute described below.

In order to maintain the security of the data, the values are usually
encrypted, however, data which are not sensitive may be provided
unencrypted as well for convenience.

Token Secrets
-------------

A `token secret` is one where an OpenID Connect (OIDC) token is
automatically generated for the job being run.  Configuration of the
token is described below in :attr:`secret.oidc`.

Zuul acts as an OpenID Connect Identity Provider which enables it to
provide an identity to a job which can be trusted by federated third
party services.  When configured in :attr:`secret.oidc`, Zuul will
generate an OIDC ID token dynamically and make it available to the jobs
that are configured to use the secret.

In addition to the standard claims, the ID token will contain the
following Zuul claims by default:

**sub**
   This is the most important claim.  Most third-party services are
   likely to match on this claim to determine permissions.  This claim
   acts as a fully-qualified name to uniquely identify the Zuul
   secret used.  It takes the form:

     ``secret:{zuul tenant}/{canonical project name}/{secret name}``

   The `canonical project name` in the sub is the location of the
   definition of the secret (not the location of the job where it is
   used).  That is because the sub is intended to identify the secret
   itself, so that services can base their authorization systems on
   the basis of Zuul's own rules about what projects and jobs are
   permitted to use the secret.

The following items are not stable identifiers and may not be suitable
for use in matching.  Caution is advised when using these.

**build-uuid**
   The UUID that uniquely identifies the current build.

**job-name**
   The name of the currently running job.

**playbook**
   The name of the currently running playbook.

**pipeline**
   The name of the pipeline the currently running build is in.

**tenant**
   The name of the Zuul tenant the running build is in.

Custom claims can be added to the ID token in :attr:`secret.oidc.claims`

Signing key rotation is handled by Zuul automatically where the rotation
interval can be specified in :attr:`oidc.signing_key_rotation_interval`
system-wide in the main Zuul configuration file.

In case a key is compromised, the ``zuul-admin`` command
``delete-oidc-signing-keys`` can be used to delete the signing keys of
a specific algorithm and Zuul will automatically generate a new
signing key.

Usage
-----

A Secret may only be used by jobs defined within the same project.
Note that they can be used by any branch of that project, so if a
project's branches have different access controls, consider whether
all branches of that project are equally trusted before using secrets.

To use a secret, a :ref:`job` must specify the secret in
:attr:`job.secrets`.  With one exception, secrets are bound to the
playbooks associated with the specific job definition where they were
declared.  Additional pre or post playbooks which appear in child jobs
will not have access to the secrets, nor will playbooks which override
the main playbook (if any) of the job which declared the secret.  This
protects against jobs in other repositories declaring a job with a
secret as a parent and then exposing that secret.

The exception to the above is if the
:attr:`job.secrets.pass-to-parent` attribute is set to true.  In that
case, the secret is made available not only to the playbooks in the
current job definition, but to all playbooks in all parent jobs as
well.  This allows for jobs which are designed to work with secrets
while leaving it up to child jobs to actually supply the secret.  Use
this option with care, as it may allow the authors of parent jobs to
accidentally or intentionally expose secrets.  If a secret with
`pass-to-parent` set in a child job has the same name as a secret
available to a parent job's playbook, the secret in the child job will
not override the parent, instead it will simply not be available to
that playbook (but will remain available to others).

It is possible to use secrets for jobs defined in :term:`config
projects <config-project>` as well as :term:`untrusted projects
<untrusted-project>`, however their use differs slightly.  Because
playbooks in a config project which use secrets run in the
:term:`trusted execution context` where proposed changes are not used
in executing jobs, it is safe for those secrets to be used in all
types of pipelines.  However, because playbooks defined in an
untrusted project are run in the :term:`untrusted execution context`
where proposed changes are used in job execution, it is dangerous to
allow those secrets to be used in pipelines which are used to execute
proposed but unreviewed changes.  By default, pipelines are considered
`pre-review` and will refuse to run jobs which have playbooks that use
secrets in the untrusted execution context (including those subject to
:attr:`job.secrets.pass-to-parent` secrets) in order to protect
against someone proposing a change which exposes a secret.  To permit
this (for instance, in a pipeline which only runs after code review),
the :attr:`pipeline.post-review` attribute may be explicitly set to
``true``.

In some cases, it may be desirable to prevent a job which is defined
in a config project from running in a pre-review pipeline (e.g., a job
used to publish an artifact).  In these cases, the
:attr:`job.post-review` attribute may be explicitly set to ``true`` to
indicate the job should only run in post-review pipelines.

If a job with secrets is unsafe to be used by other projects, the
:attr:`job.allowed-projects` attribute can be used to restrict the
projects which can invoke that job.  If a job with secrets is defined
in an `untrusted-project`, `allowed-projects` is automatically set to
that project only, and can not be overridden (though a
:term:`config-project` may still add the job to any project's pipeline
regardless of this setting; do so with caution as other projects may
expose the source project's secrets).

Secrets, like most configuration items, are unique within a tenant,
though a secret may be defined on multiple branches of the same
project as long as the contents are the same.  This is to aid in
branch maintenance, so that creating a new branch based on an existing
branch will not immediately produce a configuration error.

When the values of secrets are passed to Ansible, the ``!unsafe`` YAML
tag is added which prevents them from being evaluated as Jinja
expressions.  This is to avoid a situation where a child job might
expose a parent job's secrets via template expansion.

However, if it is known that a given secret value can be trusted, then
this limitation can be worked around by using the following construct
in a playbook:

.. code-block:: yaml

   - set_fact:
       unsafe_var_eval: "{{ hostvars['localhost'].secretname.var }}"

This will force an explicit template evaluation of the `var` attribute
on the `secretname` secret.  The results will be stored in
unsafe_var_eval.

.. attr:: secret

   The following attributes must appear on a secret:

   .. attr:: name
      :required:

      The name of the secret, used in a :ref:`job` definition to
      request the secret.

   .. attr:: data

      Mutually exclusive with ``oidc``, either ``data`` or ``oidc``
      must be supplied.

      Use of this attribute generates a :term:`data secret`.

      A dictionary which will be added to the Ansible variables
      available to the job.  The values can be any of the normal YAML
      data types (strings, integers, dictionaries or lists) or
      encrypted strings.  See :ref:`encryption` for more information.

   .. attr:: oidc

      Mutually exclusive with ``data``, either ``data`` or ``oidc``
      must be supplied.

      Use of this attribute generates a :term:`token secret`.

      If this value is set, then an OIDC ID token in string form will
      be generated dynamically before running the playbook, and will
      be added to the Ansible variables available to the job.  It can
      be used to authenticate to external services that trust Zuul.

      Since all attributes below are optional, to request a `token secret`
      without supplying any options, use the following form:

      .. code-block:: yaml

         - secret:
             name: my-oidc-secret
             oidc:

      .. attr:: ttl

         TTL (Time-To-Live) of the ID token in seconds, it is used to
         calculate ``exp`` claim. It should be longer than the duration
         between the playbook start and the task execution that uses
         the secret, otherwise the token may be expired. It must not
         be greater than the :attr:`tenant.max-oidc-ttl` and if not
         specified, the default value is :attr:`tenant.default-oidc-ttl`.

      .. attr:: iss

         Custom ``iss`` claim, it must be one of the allowed issuers
         defined in :attr:`tenant.allowed-oidc-issuers`.

      .. attr:: algorithm

         Specify the signing algorithm of the ID token.  It must be one of
         :attr:`oidc.supported_signing_algorithms` and if not specified,
         the default value is :attr:`oidc.default_signing_algorithm`.

      .. attr:: claims

         A dictionary of custom claims to be added to the ID token. For example,
         the ``aud`` claim can be specified here.  The custom claims are not
         able to overwrite the Zuul default claims mentioned above.
