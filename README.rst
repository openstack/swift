===============
OpenStack Swift
===============

.. image:: https://governance.openstack.org/tc/badges/swift.svg
    :target: https://governance.openstack.org/tc/reference/tags/index.html

.. Change things from this point on

OpenStack Swift is a distributed object storage system designed to scale
from a single machine to thousands of servers. Swift is optimized for
multi-tenancy and high concurrency. Swift is ideal for backups, web and mobile
content, and any other unstructured data that can grow without bound.

Swift provides a simple, REST-based API fully documented at
https://docs.openstack.org/swift/latest/.

Swift was originally developed as the basis for Rackspace's Cloud Files
and was open-sourced in 2010 as part of the OpenStack project. It has
since grown to include contributions from many companies and has spawned
a thriving ecosystem of 3rd party tools. Swift's contributors are listed
in the AUTHORS file.

Docs
----

To build documentation run::

    pip install -r requirements.txt -r doc/requirements.txt
    sphinx-build -W -b html doc/source doc/build/html

and then browse to doc/build/html/index.html. These docs are auto-generated
after every commit and available online at
https://docs.openstack.org/swift/latest/.

For Developers
--------------

Getting Started
~~~~~~~~~~~~~~~

Swift is part of OpenStack and follows the code contribution, review, and
testing processes common to all OpenStack projects.

If you would like to start contributing, check out these
`notes <CONTRIBUTING.rst>`__ to help you get started.

The best place to get started is the
`"SAIO - Swift All In One" <https://docs.openstack.org/swift/latest/development_saio.html>`__.
This document will walk you through setting up a development cluster of
Swift in a VM. The SAIO environment is ideal for running small-scale
tests against Swift and trying out new features and bug fixes.

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
`"SAIO - Swift All In One" <https://docs.openstack.org/swift/latest/development_saio.html>`__
dev environment. For example, a probe test may create an object, delete one
replica, and ensure that the background consistency processes find and correct
the error.

You can run unit tests with ``.unittests``, functional tests with
``.functests``, and probe tests with ``.probetests``. There is an
additional ``.alltests`` script that wraps the other three.

To fully run the tests, the target environment must use a filesystem that
supports large xattrs. XFS is strongly recommended. For unit tests and in-
process functional tests, either mount ``/tmp`` with XFS or provide another
XFS filesystem via the ``TMPDIR`` environment variable. Without this setting,
tests should still pass, but a very large number will be skipped.

Code Organization
~~~~~~~~~~~~~~~~~

-  doc/: Documentation
-  etc/: Sample config files
-  examples/: Config snippets used in the docs
-  swift/: Core code

   -  account/: account server
   -  cli/: code that backs some of the CLI tools
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
to forward the request to a back-end server. For example, the entry
point for requests to the object server is the ``ObjectController``
class in ``swift/obj/server.py``.

For Deployers
-------------

Deployer docs are also available at
https://docs.openstack.org/swift/latest/. A good starting point is at
https://docs.openstack.org/swift/latest/deployment_guide.html
There is an `ops runbook <https://docs.openstack.org/swift/latest/ops_runbook/index.html>`__
that gives information about how to diagnose and troubleshoot common issues
when running a Swift cluster.

You can run functional tests against a Swift cluster with
``.functests``. These functional tests require ``/etc/swift/test.conf``
to run. A sample config file can be found in this source tree in
``test/sample.conf``.

For Client Apps
---------------

For client applications, official Python language bindings are provided
at https://opendev.org/openstack/python-swiftclient.

Complete API documentation at
https://docs.openstack.org/api-ref/object-store/

There is a large ecosystem of applications and libraries that support and
work with OpenStack Swift. Several are listed on the
`associated projects <https://docs.openstack.org/swift/latest/associated_projects.html>`__
page.

--------------

For more information come hang out in #openstack-swift on OFTC.

Thanks,

The Swift Development Team
