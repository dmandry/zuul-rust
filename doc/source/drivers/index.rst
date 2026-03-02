.. _drivers:

Drivers
=======

Drivers may support any of the following functions:

* Sources -- hosts git repositories for projects.  Zuul can clone git
  repos for projects and fetch refs.
* Triggers -- emits events to which Zuul may respond.  Triggers are
  configured in pipelines to cause changes or other refs to be
  enqueued.
* Reporters -- outputs information when a pipeline is finished
  processing an item.
* Providers -- manages build nodes in a cloud.

Zuul includes the following source drivers (most support trigger and
reporting as well):

.. toctree::
   :maxdepth: 2

   gerrit
   git
   github
   gitlab
   pagure

Zuul includes the following trigger or reporting-only drivers:

.. toctree::
   :maxdepth: 2

   elasticsearch
   mqtt
   smtp
   timer
   zuul

Zuul includes the following provider drivers:

.. toctree::
   :maxdepth: 2

   aws
   azure
   openstack
   static
