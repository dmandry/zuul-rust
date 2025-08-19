Installation
============

External Dependencies
---------------------

Zuul interacts with several other systems described below.

Nodepool
~~~~~~~~

In order to run all but the simplest jobs, Zuul uses a companion
program `Nodepool <https://opendev.org/zuul/nodepool>`__ to supply the
nodes (whether dynamic cloud instances or static hardware) used by
jobs.  Before starting Zuul, ensure you have Nodepool installed and
any images you require built.

Zuul must be able to log into the nodes provisioned by Nodepool with a
given username and SSH private key.  Executors should also be able to
talk to nodes on TCP port 19885 for log streaming; see
:ref:`nodepool_console_streaming`.

ZooKeeper
~~~~~~~~~

.. TODO: SpamapS any zookeeper config recommendations?

Zuul and Nodepool use ZooKeeper to communicate internally among their
components, and also to communicate with each other.  You can run a
simple single-node ZooKeeper instance, or a multi-node cluster.
Ensure that all Zuul and Nodepool hosts have access to the cluster.

Zuul stores all possible state within ZooKeeper so that it can be
effectively shared and coordinated between instances of its component
services. Most of this is ephemeral and can be recreated or is of low
value if lost, but a clustered deployment will provide improved
continuity and resilience in the event of an incident adversely
impacting a ZooKeeper server.

Zuul's keystore (project-specific keys for asymmetric encryption of
job secrets and SSH access) is also stored in ZooKeeper, and unlike
the other data it **cannot be recreated** if lost. As such,
periodic :ref:`export and backup <backup>` of these keys is strongly
recommended.

.. _ansible-installation-options:

Executor Deployment
-------------------

The Zuul executor requires Ansible to run jobs.  There are two
approaches that can be used to install Ansible for Zuul.

First you may set ``manage_ansible`` to True in the executor config. If you
do this Zuul will install all supported Ansible versions on zuul-executor
startup. These installations end up in Zuul's state dir,
``/var/lib/zuul/ansible-bin`` if unchanged.

The second option is to use ``zuul-manage-ansible`` to install the supported
Ansible versions. By default this will install Ansible to
``zuul_install_prefix/lib/zuul/ansible``. This method is preferable to the
first because it speeds up zuul-executor start time and allows you to
preinstall ansible in containers (avoids problems with bind mounted zuul
state dirs).

.. program-output:: zuul-manage-ansible -h

In both cases if using a non default path you will want to set
``ansible_root`` in the executor config file.

.. _web-deployment-options:

Web Deployment
--------------

The ``zuul-web`` service provides a web dashboard, a REST API and a websocket
log streaming service as a single holistic web application. For production use
it is recommended to run it behind a reverse proxy, such as Apache or Nginx.

The ``zuul-web`` service is entirely self-contained and can be run
with minimal configuration, however, more advanced users may desire to
do one or more of the following:

White Label
  Serve the dashboard of an individual tenant at the root of its own domain.
  https://zuul.openstack.org is an example of a Zuul dashboard that has been
  white labeled for the ``openstack`` tenant of its Zuul.

Static Offload
  Shift the duties of serving static files, such as HTML, Javascript, CSS or
  images to the reverse proxy server.

Static External
  Serve the static files from a completely separate location that does not
  support programmatic rewrite rules such as a Swift Object Store.

Sub-URL
  Serve a Zuul dashboard from a location below the root URL as part of
  presenting integration with other application.
  https://softwarefactory-project.io/zuul/ is an example of a Zuul dashboard
  that is being served from a Sub-URL.

Most deployments shouldn't need these, so the following discussion
will assume that the ``zuul-web`` service is exposed via a reverse
proxy. Where rewrite rule examples are given, they will be given with
Apache syntax, but any other reverse proxy should work just fine.

Reverse Proxy
~~~~~~~~~~~~~

Using Apache as the reverse proxy requires the ``mod_proxy``,
``mod_proxy_http`` and ``mod_proxy_wstunnel`` modules to be installed
and enabled.

All of the cases require a rewrite rule for the websocket streaming, so the
simplest reverse-proxy case is::

  RewriteEngine on
  RewriteRule ^/api/tenant/(.*)/console-stream ws://localhost:9000/api/tenant/$1/console-stream [P]
  RewriteRule ^/(.*)$ http://localhost:9000/$1 [P]

This is the recommended configuration unless one of the following
features is required.

Static Offload
~~~~~~~~~~~~~~

To have the reverse proxy serve the static html/javascript assets
instead of proxying them to the REST layer, enable the ``mod_rewrite``
Apache module, register the location where you unpacked the web
application as the document root and add rewrite rules::

  <Directory /usr/share/zuul>
    Require all granted
  </Directory>
  Alias / /usr/share/zuul/
  <Location />
    RewriteEngine on
    RewriteBase /
    # Rewrite api to the zuul-web endpoint
    RewriteRule api/tenant/(.*)/console-stream ws://localhost:9000/api/tenant/$1/console-stream [P,L]
    RewriteRule api/(.*)$ http://localhost:9000/api/$1 [P,L]
    RewriteRule oidc/(.*)$ http://localhost:9000/oidc/$1 [P,L]
    RewriteRule .well-known/openid-configuration$ http://localhost:9000/.well-known/openid-configuration [P,L]
    # Backward compatible rewrite
    RewriteRule t/(.*)/(.*).html(.*) /t/$1/$2$3 [R=301,L,NE]

    # Don't rewrite files or directories
    RewriteCond %{REQUEST_FILENAME} !-f
    RewriteCond %{REQUEST_FILENAME} !-d
    RewriteRule . /index.html [L]
  </Location>


Sub directory serving
~~~~~~~~~~~~~~~~~~~~~

The web application needs to be rebuilt to update the internal
location of the static files. Set the environment variable
`PUBLIC_URL` to an absolute path when building. For example, to deploy
the web interface through a `/zuul` sub directory:

.. note::

   The web dashboard source code and package.json are located in the ``web``
   directory. All the yarn commands need to be executed from the ``web``
   directory.

.. code-block:: bash

   PUBLIC_URL=/zuul yarn build

Then assuming the web application is unpacked in /usr/share/zuul,
enable the ``mod_rewrite`` Apache module and add the following rewrite
rules::

  <Directory /usr/share/zuul>
    Require all granted
  </Directory>
  Alias /zuul /usr/share/zuul/
  <Location /zuul>
    RewriteEngine on
    RewriteBase /zuul
    # Rewrite api to the zuul-web endpoint
    RewriteRule api/tenant/(.*)/console-stream ws://localhost:9000/api/tenant/$1/console-stream [P,L]
    RewriteRule api/(.*)$ http://localhost:9000/api/$1 [P,L]
    RewriteRule oidc/(.*)$ http://localhost:9000/oidc/$1 [P,L]
    RewriteRule .well-known/openid-configuration$ http://localhost:9000/.well-known/openid-configuration [P,L]
    # Backward compatible rewrite
    RewriteRule t/(.*)/(.*).html(.*) /t/$1/$2$3 [R=301,L,NE]

    # Don't rewrite files or directories
    RewriteCond %{REQUEST_FILENAME} !-f
    RewriteCond %{REQUEST_FILENAME} !-d
    RewriteRule . /zuul/index.html [L]
  </Location>


White Labeled Tenant
~~~~~~~~~~~~~~~~~~~~

Running a white-labeled tenant is similar to the offload case, but adds a
rule to ensure connection webhooks don't try to get put into the tenant scope.

.. note::

   It's possible to do white-labeling without static offload, but it
   is more complex with no benefit.

Enable the ``mod_rewrite`` Apache module, and assuming the Zuul tenant
name is ``example``, the rewrite rules are::

  <Directory /usr/share/zuul>
    Require all granted
  </Directory>
  Alias / /usr/share/zuul/
  <Location />
    RewriteEngine on
    RewriteBase /
    # Rewrite api to the zuul-web endpoint
    RewriteRule api/connection/(.*)$ http://localhost:9000/api/connection/$1 [P,L]
    RewriteRule api/console-stream ws://localhost:9000/api/tenant/example/console-stream [P,L]
    RewriteRule api/(.*)$ http://localhost:9000/api/tenant/example/$1 [P,L]
    RewriteRule oidc/(.*)$ http://localhost:9000/oidc/tenant/example/$1 [P,L]
    RewriteRule .well-known/openid-configuration$ http://localhost:9000/oidc/tenant/.well-known/openid-configuration [P,L]
    # Backward compatible rewrite
    RewriteRule t/(.*)/(.*).html(.*) /t/$1/$2$3 [R=301,L,NE]

    # Don't rewrite files or directories
    RewriteCond %{REQUEST_FILENAME} !-f
    RewriteCond %{REQUEST_FILENAME} !-d
    RewriteRule . /index.html [L]
  </Location>



Static External
~~~~~~~~~~~~~~~

.. note::

   Hosting the Zuul dashboard on an external static location that does
   not support dynamic url rewrite rules only works for white-labeled
   deployments.

In order to serve the zuul dashboard code from an external static location,
``REACT_APP_ZUUL_API`` must be set at javascript build time:

.. code-block:: bash

   REACT_APP_ZUUL_API='http://zuul-web.example.com' yarn build
