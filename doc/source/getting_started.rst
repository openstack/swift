===============
Getting Started
===============

-------------------
System Requirements
-------------------

Swift development currently targets Ubuntu Server 22.04, but should work on
most Linux platforms.

Swift is written in Python and has these dependencies:

* Python (3.6-3.12)
* rsync 3.x
* `liberasurecode <https://opendev.org/openstack/liberasurecode/>`__
* The Python packages listed in `the requirements file <https://github.com/openstack/swift/blob/master/requirements.txt>`__
* Testing additionally requires `the test dependencies <https://github.com/openstack/swift/blob/master/test-requirements.txt>`__
* Testing requires `these distribution packages <https://github.com/openstack/swift/blob/master/bindep.txt>`__

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

* `Source code <https://opendev.org/openstack/python-swiftclient>`__
* `Python Package Index <https://pypi.org/project/python-swiftclient>`__

----------
Production
----------

If you want to set up and configure Swift for a production cluster, the
following doc should be useful:

* :doc:`install/index`
