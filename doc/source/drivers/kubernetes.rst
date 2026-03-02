:title: Kubernetes Driver

Kubernetes
==========

Zuul can use pods or namespaces from Kubernetes as a source for build
nodes.

Connection Configuration
------------------------

The supported options in ``zuul.conf`` connections are:

.. attr:: <kubernetes connection>

   .. attr:: driver
      :required:

      .. value:: kubernetes

         The connection must set ``driver=kubernetes`` for Kubernetes
         connections.

   .. attr:: kubeconfig_file

      A path to a kubeconfig file with credentials to access the
      Kubernetes cluster.  If this is not present, Zuul will use the
      value of the `KUBECONFIG` environment variable.  If neither this
      setting or the environment variable is present, then any
      credentials obtainable by the Kubernetes client library will be
      used.

   .. attr:: context

      If the kubeconfig file has more than one context available, this
      option may be used to specify which one Zuul should use for this
      connection.

   .. attr:: rate
      :default: 2

      The API rate limit (in requests per second) to use when
      performing Kubernetes API calls.


Provider Configuration
----------------------

The ``kubernetes`` driver adds the following options to the :attr:`provider`
and :attr:`section` configurations:

.. include:: kubernetes-attrs.rstinc
