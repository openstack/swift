.. _associated_projects:

Associated Projects
===================

.. _application-bindings:

Application Bindings
--------------------

* OpenStack supported binding:

  * `Python-SwiftClient <https://pypi.python.org/pypi/python-swiftclient>`_

* Unofficial libraries and bindings:

  * `PHP-opencloud <http://php-opencloud.com>`_ - Official Rackspace PHP bindings that should work for other Swift deployments too.
  * `PyRAX <https://github.com/pycontribs/pyrax>`_ - Official Rackspace Python bindings for CloudFiles that should work for other Swift deployments too.
  * `openstack.net <https://github.com/rackspace/openstack.net/>`_ - Official Rackspace .NET bindings that should work for other Swift deployments too.
  * `RSwift <https://github.com/pandemicsyn/RSwift>`_ - R API bindings.
  * `Go language bindings <https://github.com/ncw/swift>`_
  * `supload <https://github.com/selectel/supload>`_ - Bash script to upload file to cloud storage based on OpenStack Swift API.
  * `libcloud <http://libcloud.apache.org>`_ - Apache Libcloud - a unified interface in Python for different clouds with OpenStack Swift support.
  * `SwiftBox <https://github.com/suniln/SwiftBox>`_ - C# library using RestSharp
  * `jclouds <http://jclouds.apache.org/guides/openstack/>`_ - Java library offering bindings for all OpenStack projects
  * `java-openstack-swift <https://github.com/iterate-ch/java-openstack-swift>`_ - Java bindings for OpenStack Swift
  * `swift_client <https://github.com/mrkamel/swift_client>`_ - Small but powerful Ruby client to interact with OpenStack Swift
  * `nightcrawler_swift <https://github.com/tulios/nightcrawler_swift>`_ - This Ruby gem teleports your assets to a OpenStack Swift bucket/container
  * `swift storage <https://rubygems.org/gems/swift-storage>`_ - Simple OpenStack Swift storage client.
  * `javaswift <http://javaswift.org/>`_ - Collection of Java tools for Swift

Authentication
--------------

* `Keystone <https://github.com/openstack/keystone>`_ - Official Identity Service for OpenStack.
* `Swauth <https://github.com/openstack/swauth>`_ - An alternative Swift authentication service that only requires Swift itself.
* `Basicauth <https://github.com/CloudVPS/swift-basicauth>`_ - HTTP Basic authentication support (keystone backed).


Command Line Access
-------------------

* `Swiftly <https://github.com/gholt/swiftly>`_ - Alternate command line access to Swift with direct (no proxy) access capabilities as well.


Log Processing
--------------

* `Slogging <https://github.com/notmyname/slogging>`_ - Basic stats and logging tools.


Monitoring & Statistics
-----------------------

* `Swift Informant <https://github.com/pandemicsyn/swift-informant>`_ - Swift Proxy Middleware to send events to a statsd instance.
* `Swift Inspector <https://github.com/hurricanerix/swift-inspector>`_ - Swift middleware to relay information about a request back to the client.


Content Distribution Network Integration
----------------------------------------

* `SOS <https://github.com/dpgoetz/sos>`_ - Swift Origin Server.


Alternative API
---------------

* `Swift3 <https://github.com/openstack/swift3>`_ - Amazon S3 API emulation.
* `CDMI <https://github.com/osaddon/cdmi>`_ - CDMI support
* `SwiftHLM <https://github.com/ibm-research/SwiftHLM>`_ - a middleware for using OpenStack Swift with tape and other high latency media storage backends


Benchmarking/Load Generators
----------------------------

* `getput <https://github.com/markseger/getput>`_ - getput tool suite
* `COSbench <https://github.com/intel-cloud/cosbench>`_ - COSbench tool suite
* `ssbench <https://github.com/swiftstack/ssbench>`_ - ssbench tool suite


.. _custom-logger-hooks-label:

Custom Logger Hooks
-------------------

* `swift-sentry <https://github.com/pandemicsyn/swift-sentry>`_ - Sentry exception reporting for Swift

Storage Backends (DiskFile API implementations)
-----------------------------------------------
* `Swift-on-File <https://github.com/openstack/swiftonfile>`_ - Enables objects created using Swift API to be accessed as files on a POSIX filesystem and vice versa.
* `swift-ceph-backend <https://github.com/openstack/swift-ceph-backend>`_ - Ceph RADOS object server implementation for Swift.
* `kinetic-swift <https://github.com/swiftstack/kinetic-swift>`_ - Seagate Kinetic Drive as backend for Swift
* `swift-scality-backend <https://github.com/scality/ScalitySproxydSwift>`_ - Scality sproxyd object server implementation for Swift.

Developer Tools
---------------
* `SAIO bash scripts <https://github.com/ntata/swift-setup-scripts>`_ -
  Well commented simple bash scripts for Swift all in one setup.
* `vagrant-swift-all-in-one
  <https://github.com/swiftstack/vagrant-swift-all-in-one>`_ - Quickly setup a
  standard development environment using Vagrant and Chef cookbooks in an
  Ubuntu virtual machine.
* `SAIO Ansible playbook <https://github.com/thiagodasilva/ansible-saio>`_ -
  Quickly setup a standard development environment using Vagrant and Ansible in
  a Fedora virtual machine (with built-in `Swift-on-File
  <https://github.com/openstack/swiftonfile>`_ support).

Other
-----

* `Glance <https://github.com/openstack/glance>`_ - Provides services for discovering, registering, and retrieving virtual machine images (for OpenStack Compute [Nova], for example).
* `Better Staticweb <https://github.com/CloudVPS/better-staticweb>`_ - Makes swift containers accessible by default.
* `Django Swiftbrowser <https://github.com/cschwede/django-swiftbrowser>`_ - Simple Django web app to access OpenStack Swift.
* `Swift-account-stats <https://github.com/redhat-cip/swift-account-stats>`_ - Swift-account-stats is a tool to report statistics on Swift usage at tenant and global levels.
* `PyECLib <https://github.com/openstack/pyeclib>`_ - High Level Erasure Code library used by Swift
* `liberasurecode <https://github.com/openstack/liberasurecode>`_ - Low Level Erasure Code library used by PyECLib
* `Swift Browser <https://github.com/mgeisler/swift-browser>`_ - JavaScript interface for Swift
* `swift-ui <https://github.com/fanatic/swift-ui>`_ - OpenStack Swift web browser
* `Swift Durability Calculator <https://github.com/redhat-cip/swift-durability-calculator>`_ - Data Durability Calculation Tool for Swift
* `swiftbackmeup <https://github.com/redhat-cip/swiftbackmeup>`_ -  Utility that allows one to create backups and upload them to OpenStack Swift
* `Multi Swift <https://github.com/ntata/multi-swift-POC>`_ - Bash scripts to spin up multiple Swift clusters sharing the same hardware
