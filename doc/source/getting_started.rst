===============
Getting Started
===============

-------------------
System Requirements
-------------------

Swift development currently targets Ubuntu Server 10.04, but should work on 
most Linux platforms with the following software:

* Python 2.6
* rsync 3.0

And the following python libraries:

* Eventlet 0.9.8
* WebOb 0.9.8
* Setuptools
* Simplejson
* Xattr
* Nose
* Sphinx
* netifaces

-------------
Getting Swift
-------------

Swift's source code is hosted on github and managed with git.  The current trunk can be checked out like this:

    ``git clone https://github.com/openstack/swift.git``

A source tarball for the latest release of Swift is available on the `launchpad project page <https://launchpad.net/swift>`_.

Prebuilt packages for Ubuntu are available starting with Natty, or from PPAs for earlier releases.

* `Swift Latest Release PPA <https://launchpad.net/~swift-core/+archive/release>`_
* `Swift Current Trunk PPA <https://launchpad.net/~swift-core/+archive/trunk>`_

-----------
Development
-----------

To get started with development with Swift, or to just play around, the
following docs will be useful:

* :doc:`Swift All in One <development_saio>` - Set up a VM with Swift installed
* :doc:`Development Guidelines <development_guidelines>`

----------
Production
----------

If you want to set up and configure Swift for a production cluster, the following doc should be useful:

* :doc:`Multiple Server Swift Installation <howto_installmultinode>`
