========================
Team and repository tags
========================

.. image:: http://governance.openstack.org/badges/swift.svg
    :target: http://governance.openstack.org/reference/tags/index.html

.. Change things from this point on

Swift
=====

A distributed object storage system designed to scale from a single
machine to thousands of servers. Swift is optimized for multi-tenancy
and high concurrency. Swift is ideal for backups, web and mobile
content, and any other unstructured data that can grow without bound.

Swift provides a simple, REST-based API fully documented at
http://docs.openstack.org/.

Swift was originally developed as the basis for Rackspace's Cloud Files
and was open-sourced in 2010 as part of the OpenStack project. It has
since grown to include contributions from many companies and has spawned
a thriving ecosystem of 3rd party tools. Swift's contributors are listed
in the AUTHORS file.

Docs
----

To build documentation install sphinx (``pip install sphinx``), run
``python setup.py build_sphinx``, and then browse to
/doc/build/html/index.html. These docs are auto-generated after every
commit and available online at
http://docs.openstack.org/developer/swift/.

For Developers
--------------

Getting Started
~~~~~~~~~~~~~~~

Swift is part of OpenStack and follows the code contribution, review, and testing processes common to all OpenStack projects.

If you would like to start contributing, check out these
`notes <CONTRIBUTING.rst>`__ to help you get started.

The best place to get started is the
`"SAIO - Swift All In One" <http://docs.openstack.org/developer/swift/development_saio.html>`__.
This document will walk you through setting up a development cluster of
Swift in a VM. The SAIO environment is ideal for running small-scale
tests against swift and trying out new features and bug fixes.

Tests
~~~~~

There are three types of tests included in Swift's source tree.

 #. Unit tests
 #. Functional tests
 #. Probe tests

Unit tests check that small sections of the code behave properly. For example,
a unit test may test a single function to ensure that various input gives the
expected output. This validates that the code is correct and regressions are
not introduced.

Functional tests check that the client API is working as expected. These can
be run against any endpoint claiming to support the Swift API (although some
tests require multiple accounts with different privilege levels). These are
"black box" tests that ensure that client apps written against Swift will
continue to work.

Probe tests are "white box" tests that validate the internal workings of a
Swift cluster. They are written to work against the
`"SAIO - Swift All In One" <http://docs.openstack.org/developer/swift/development_saio.html>`__
dev environment. For example, a probe test may create an object, delete one
replica, and ensure that the background consistency processes find and correct
the error.

You can run unit tests with ``.unittests``, functional tests with
``.functests``, and probe tests with ``.probetests``. There is an
additional ``.alltests`` script that wraps the other three.

Code Organization
~~~~~~~~~~~~~~~~~

-  bin/: Executable scripts that are the processes run by the deployer
-  doc/: Documentation
-  etc/: Sample config files
-  examples/: Config snippets used in the docs
-  swift/: Core code

   -  account/: account server
   -  cli/: code that backs some of the CLI tools in bin/
   -  common/: code shared by different modules

      -  middleware/: "standard", officially-supported middleware
      -  ring/: code implementing Swift's ring

   -  container/: container server
   -  locale/: internationalization (translation) data
   -  obj/: object server
   -  proxy/: proxy server

-  test/: Unit, functional, and probe tests

Data Flow
~~~~~~~~~

Swift is a WSGI application and uses eventlet's WSGI server. After the
processes are running, the entry point for new requests is the
``Application`` class in ``swift/proxy/server.py``. From there, a
controller is chosen, and the request is processed. The proxy may choose
to forward the request to a back- end server. For example, the entry
point for requests to the object server is the ``ObjectController``
class in ``swift/obj/server.py``.

For Deployers
-------------

Deployer docs are also available at
http://docs.openstack.org/developer/swift/. A good starting point is at
http://docs.openstack.org/developer/swift/deployment_guide.html

There is an `ops runbook <http://docs.openstack.org/developer/swift/ops_runbook/>`__
that gives information about how to diagnose and troubleshoot common issues
when running a Swift cluster.

You can run functional tests against a swift cluster with
``.functests``. These functional tests require ``/etc/swift/test.conf``
to run. A sample config file can be found in this source tree in
``test/sample.conf``.

For Client Apps
---------------

For client applications, official Python language bindings are provided
at http://github.com/openstack/python-swiftclient.

Complete API documentation at
http://docs.openstack.org/api/openstack-object-storage/1.0/content/

There is a large ecosystem of applications and libraries that support and
work with OpenStack Swift. Several are listed on the
`associated projects <http://docs.openstack.org/developer/swift/associated_projects.html>`__
page.

--------------

For more information come hang out in #openstack-swift on freenode.

Thanks,

The Swift Development Team
