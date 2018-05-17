===============
Getting Started
===============

-------------------
System Requirements
-------------------

Swift development currently targets Ubuntu Server 16.04, but should work on
most Linux platforms.

Swift is written in Python and has these dependencies:

* Python 2.7
* rsync 3.0
* The Python packages listed in `the requirements file <https://github.com/openstack/swift/blob/master/requirements.txt>`_
* Testing additionally requires `the test dependencies <https://github.com/openstack/swift/blob/master/test-requirements.txt>`_
* Testing requires `these distribution packages <https://github.com/openstack/swift/blob/master/bindep.txt>`_

There is no current support for Python 3.

-----------
Development
-----------

To get started with development with Swift, or to just play around, the
following docs will be useful:

* :doc:`Swift All in One <development_saio>` - Set up a VM with Swift installed
* :doc:`Development Guidelines <development_guidelines>`
* :doc:`First Contribution to Swift <first_contribution_swift>`
* :doc:`Associated Projects <associated_projects>`

--------------------------
CLI client and SDK library
--------------------------

There are many clients in the :ref:`ecosystem <application-bindings>`. The official CLI
and SDK is python-swiftclient.

* `Source code <https://github.com/openstack/python-swiftclient>`_
* `Python Package Index <https://pypi.org/project/python-swiftclient>`_

----------
Production
----------

If you want to set up and configure Swift for a production cluster, the
following doc should be useful:

* :doc:`Multiple Server Swift Installation <howto_installmultinode>`
