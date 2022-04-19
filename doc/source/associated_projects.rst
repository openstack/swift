.. _associated_projects:

Associated Projects
===================

.. _application-bindings:

Application Bindings
--------------------

* OpenStack supported binding:

  * `Python-SwiftClient <https://pypi.org/project/python-swiftclient>`_

* Unofficial libraries and bindings:

  * PHP

    * `PHP-opencloud <http://php-opencloud.com>`_ - Official Rackspace PHP
      bindings that should work for other Swift deployments too.

  * Ruby

    * `swift_client <https://github.com/mrkamel/swift_client>`_ -
      Small but powerful Ruby client to interact with OpenStack Swift
    * `nightcrawler_swift <https://github.com/tulios/nightcrawler_swift>`_ -
      This Ruby gem teleports your assets to an OpenStack Swift bucket/container
    * `swift storage <https://rubygems.org/gems/swift-storage>`_ -
      Simple OpenStack Swift storage client.

  * Java

    * `libcloud <http://libcloud.apache.org>`_ - Apache Libcloud - a unified
      interface in Python for different clouds with OpenStack Swift support.
    * `jclouds <http://jclouds.apache.org/guides/openstack/>`_ -
      Java library offering bindings for all OpenStack projects
    * `java-openstack-swift <https://github.com/iterate-ch/java-openstack-swift>`_ -
      Java bindings for OpenStack Swift
    * `javaswift <http://javaswift.org/>`_ - Collection of Java tools for Swift

  * Bash

    * `supload <https://github.com/selectel/supload>`_ - Bash script to
      upload file to cloud storage based on OpenStack Swift API.

  * .NET

    * `openstacknetsdk.org <http://www.openstacknetsdk.org>`_ - An OpenStack
      Cloud SDK for Microsoft .NET.

  * Go

    * `Go language bindings <https://github.com/ncw/swift>`_
    * `Gophercloud an OpenStack SDK for Go <https://godoc.org/github.com/gophercloud/gophercloud>`_


Authentication
--------------

* `Keystone <https://opendev.org/openstack/keystone>`_ - Official Identity
  Service for OpenStack.
* `Swauth <https://opendev.org/x/swauth/>`_ - **RETIRED**: An alternative Swift
  authentication service that only requires Swift itself.
* `Basicauth <https://github.com/CloudVPS/swift-basicauth>`_ - HTTP Basic
  authentication support (keystone backed).


Command Line Access
-------------------

* `Swiftly <https://github.com/gholt/swiftly>`_ - Alternate command line
  access to Swift with direct (no proxy) access capabilities as well.


Log Processing
--------------

* `slogging <https://opendev.org/x/slogging>`_ - Basic stats and
  logging tools.


Monitoring & Statistics
-----------------------

* `Swift Informant <https://github.com/pandemicsyn/swift-informant>`_ -
  Swift proxy Middleware to send events to a statsd instance.
* `Swift Inspector <https://github.com/hurricanerix/swift-inspector>`_ -
  Swift middleware to relay information about a request back to the client.


Content Distribution Network Integration
----------------------------------------

* `SOS <https://github.com/dpgoetz/sos>`_ - Swift Origin Server.


Alternative API
---------------

* `ProxyFS <https://github.com/NVIDIA/proxyfs>`_ - Integrated file and
  object access for Swift object storage
* `SwiftHLM <https://github.com/ibm-research/SwiftHLM>`_ - a middleware for
  using OpenStack Swift with tape and other high latency media storage
  backends.


Benchmarking/Load Generators
----------------------------

* `getput <https://github.com/markseger/getput>`_ - getput tool suite
* `COSbench <https://github.com/intel-cloud/cosbench>`_ - COSbench tool suite


.. _custom-logger-hooks-label:

Custom Logger Hooks
-------------------

* `swift-sentry <https://github.com/pandemicsyn/swift-sentry>`_ -
  Sentry exception reporting for Swift

Storage Backends (DiskFile API implementations)
-----------------------------------------------
* `Swift-on-File <https://opendev.org/x/swiftonfile>`_ -
  Enables objects created using Swift API to be accessed as files on a POSIX
  filesystem and vice versa.
* `swift-scality-backend <https://github.com/scality/ScalitySproxydSwift>`_ -
  Scality sproxyd object server implementation for Swift.

Developer Tools
---------------
* `SAIO bash scripts <https://github.com/ntata/swift-setup-scripts>`_ -
  Well commented simple bash scripts for Swift all in one setup.
* `vagrant-swift-all-in-one
  <https://github.com/NVIDIA/vagrant-swift-all-in-one>`_ - Quickly setup a
  standard development environment using Vagrant and Chef cookbooks in an
  Ubuntu virtual machine.
* `SAIO Ansible playbook <https://github.com/thiagodasilva/ansible-saio>`_ -
  Quickly setup a standard development environment using Vagrant and Ansible in
  a Fedora virtual machine (with built-in `Swift-on-File
  <https://opendev.org/x/swiftonfile>`_ support).
* `Multi Swift <https://github.com/ntata/multi-swift-POC>`_ -
  Bash scripts to spin up multiple Swift clusters sharing the same hardware


Other
-----

* `Glance <https://opendev.org/openstack/glance>`_ - Provides services for
  discovering, registering, and retrieving virtual machine images
  (for OpenStack Compute [Nova], for example).
* `Django Swiftbrowser <https://github.com/cschwede/django-swiftbrowser>`_ -
  Simple Django web app to access OpenStack Swift.
* `Swift-account-stats <https://github.com/redhat-cip/swift-account-stats>`_ -
  Swift-account-stats is a tool to report statistics on Swift usage at
  tenant and global levels.
* `PyECLib <https://opendev.org/openstack/pyeclib>`_ - High-level erasure code
  library used by Swift
* `liberasurecode <https://opendev.org/openstack/liberasurecode>`_ - Low-level
  erasure code library used by PyECLib
* `Swift Browser <https://github.com/mgeisler/swift-browser>`_ - JavaScript
  interface for Swift
* `swift-ui <https://github.com/fanatic/swift-ui>`_ - OpenStack Swift
  web browser
* `swiftbackmeup <https://github.com/redhat-cip/swiftbackmeup>`_ -
  Utility that allows one to create backups and upload them to OpenStack Swift
