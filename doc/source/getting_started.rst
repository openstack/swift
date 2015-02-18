===============
Getting Started
===============

-------------------
System Requirements
-------------------

Swift development currently targets Ubuntu Server 14.04, but should work on
most Linux platforms.

Swift is written in Python and has these dependencies:

* Python 2.7
* rsync 3.0
* The Python packages listed in `the requirements file <https://github.com/openstack/swift/blob/master/requirements.txt>`_
* Testing additionally requires `the test dependencies <https://github.com/openstack/swift/blob/master/test-requirements.txt>`_

Python 2.6 should work, but it's not actively tested. There is no current
support for Python 3.

-------------
Getting Swift
-------------

Swift's source code is hosted on github and managed with git.  The current
trunk can be checked out like this:

    ``git clone https://github.com/openstack/swift.git``

A source tarball for the latest release of Swift is available on the
`launchpad project page <https://launchpad.net/swift>`_.

Prebuilt packages for Ubuntu and RHEL variants are available.

* `Swift Ubuntu Packages <https://launchpad.net/ubuntu/+source/swift>`_
* `Swift RDO Packages <https://openstack.redhat.com/Repositories>`_

-----------
Development
-----------

To get started with development with Swift, or to just play around, the
following docs will be useful:

* :doc:`Swift All in One <development_saio>` - Set up a VM with Swift
  installed
* :doc:`Development Guidelines <development_guidelines>`
* `Associated Projects <http://docs.openstack.org/developer/swift/associated_projects.html>`

--------------------------
CLI client and SDK library
--------------------------

There are many clients in the `ecosystem <http://docs.openstack.org/developer/swift/associated_projects.html#application-bindings>`_. The official CLI
and SDK is python-swiftclient.

* `Source code <https://github.com/openstack/python-swiftclient>`_
* `Python Package Index <https://pypi.python.org/pypi/python-swiftclient>`_

----------
Production
----------

If you want to set up and configure Swift for a production cluster, the
following doc should be useful:

* :doc:`Multiple Server Swift Installation <howto_installmultinode>`
