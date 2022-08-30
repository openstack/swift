.. _common_middleware:

**********
Middleware
**********

.. _account-quotas:

Account Quotas
==============

.. automodule:: swift.common.middleware.account_quotas
    :members:
    :show-inheritance:

.. _s3api:

AWS S3 Api
==========

.. automodule:: swift.common.middleware.s3api.s3api
    :members:
    :show-inheritance:

.. automodule:: swift.common.middleware.s3api.s3token
    :members:
    :show-inheritance:

.. automodule:: swift.common.middleware.s3api.s3request
    :members:
    :show-inheritance:

.. automodule:: swift.common.middleware.s3api.s3response
    :members:
    :show-inheritance:

.. automodule:: swift.common.middleware.s3api.exception
    :members:
    :show-inheritance:

.. automodule:: swift.common.middleware.s3api.etree
    :members: _Element
    :show-inheritance:

.. automodule:: swift.common.middleware.s3api.utils
    :members:
    :show-inheritance:

.. automodule:: swift.common.middleware.s3api.subresource
    :members:
    :show-inheritance:

.. automodule:: swift.common.middleware.s3api.acl_handlers
    :members:
    :show-inheritance:

.. automodule:: swift.common.middleware.s3api.acl_utils
    :members:
    :show-inheritance:

.. automodule:: swift.common.middleware.s3api.controllers.base
    :members:
    :show-inheritance:

.. automodule:: swift.common.middleware.s3api.controllers.service
    :members:
    :show-inheritance:

.. automodule:: swift.common.middleware.s3api.controllers.bucket
    :members:
    :show-inheritance:

.. automodule:: swift.common.middleware.s3api.controllers.obj
    :members:
    :show-inheritance:

.. automodule:: swift.common.middleware.s3api.controllers.acl
    :members:
    :show-inheritance:

.. automodule:: swift.common.middleware.s3api.controllers.s3_acl
    :members:
    :show-inheritance:

.. automodule:: swift.common.middleware.s3api.controllers.multi_upload
    :members:
    :show-inheritance:

.. automodule:: swift.common.middleware.s3api.controllers.multi_delete
    :members:
    :show-inheritance:

.. automodule:: swift.common.middleware.s3api.controllers.versioning
    :members:
    :show-inheritance:

.. automodule:: swift.common.middleware.s3api.controllers.location
    :members:
    :show-inheritance:

.. automodule:: swift.common.middleware.s3api.controllers.logging
    :members:
    :show-inheritance:

Backend Ratelimit
=================

.. automodule:: swift.common.middleware.backend_ratelimit
    :members:
    :show-inheritance:

.. _bulk:

Bulk Operations (Delete and Archive Auto Extraction)
====================================================

.. automodule:: swift.common.middleware.bulk
    :members:
    :show-inheritance:

.. _catch_errors:

CatchErrors
=============

.. automodule:: swift.common.middleware.catch_errors
    :members:
    :show-inheritance:

CNAME Lookup
============

.. automodule:: swift.common.middleware.cname_lookup
    :members:
    :show-inheritance:

.. _container-quotas:

Container Quotas
================

.. automodule:: swift.common.middleware.container_quotas
    :members:
    :show-inheritance:

.. _container-sync:

Container Sync Middleware
=========================

.. automodule:: swift.common.middleware.container_sync
    :members:
    :show-inheritance:

Cross Domain Policies
=====================

.. automodule:: swift.common.middleware.crossdomain
    :members:
    :show-inheritance:

.. _discoverability:

Discoverability
===============

Swift will by default provide clients with an interface providing details
about the installation. Unless disabled (i.e ``expose_info=false`` in
:ref:`proxy-server-config`), a GET request to ``/info`` will return configuration
data in JSON format.  An example response::

    {"swift": {"version": "1.11.0"}, "staticweb": {}, "tempurl": {}}

This would signify to the client that swift version 1.11.0 is running and that
staticweb and tempurl are available in this installation.

There may be administrator-only information available via ``/info``. To
retrieve it, one must use an HMAC-signed request, similar to TempURL.
The signature may be produced like so::

    swift tempurl GET 3600 /info secret 2>/dev/null | sed s/temp_url/swiftinfo/g

Domain Remap
============

.. automodule:: swift.common.middleware.domain_remap
    :members:
    :show-inheritance:

Dynamic Large Objects
=====================

DLO support centers around a user specified filter that matches
segments and concatenates them together in object listing order. Please see
the DLO docs for :ref:`dlo-doc` further details.

.. _encryption:

Encryption
==========

Encryption middleware should be deployed in conjunction with the
:ref:`keymaster` middleware.

.. automodule:: swift.common.middleware.crypto
    :members:
    :show-inheritance:

.. automodule:: swift.common.middleware.crypto.encrypter
    :members:
    :show-inheritance:

.. automodule:: swift.common.middleware.crypto.decrypter
    :members:
    :show-inheritance:

.. _etag_quoter:

Etag Quoter
===========

.. automodule:: swift.common.middleware.etag_quoter
    :members:
    :show-inheritance:

.. _formpost:

FormPost
========

.. automodule:: swift.common.middleware.formpost
    :members:
    :show-inheritance:

.. _gatekeeper:

GateKeeper
==========

.. automodule:: swift.common.middleware.gatekeeper
    :members:
    :show-inheritance:

.. _healthcheck:

Healthcheck
===========

.. automodule:: swift.common.middleware.healthcheck
    :members:
    :show-inheritance:

.. _keymaster:

Keymaster
=========

Keymaster middleware should be deployed in conjunction with the
:ref:`encryption` middleware.

.. automodule:: swift.common.middleware.crypto.keymaster
    :members:
    :show-inheritance:

.. _keystoneauth:

KeystoneAuth
============

.. automodule:: swift.common.middleware.keystoneauth
    :members:
    :show-inheritance:

.. _list_endpoints:

List Endpoints
==============

.. automodule:: swift.common.middleware.list_endpoints
    :members:
    :show-inheritance:

Memcache
========

.. automodule:: swift.common.middleware.memcache
    :members:
    :show-inheritance:

Name Check (Forbidden Character Filter)
=======================================

.. automodule:: swift.common.middleware.name_check
    :members:
    :show-inheritance:

.. _object_versioning:

Object Versioning
=================

.. automodule:: swift.common.middleware.versioned_writes.object_versioning
    :members:
    :show-inheritance:

Proxy Logging
=============

.. automodule:: swift.common.middleware.proxy_logging
    :members:
    :show-inheritance:

Ratelimit
=========

.. automodule:: swift.common.middleware.ratelimit
    :members:
    :show-inheritance:

.. _read_only:

Read Only
=========

.. automodule:: swift.common.middleware.read_only
    :members:
    :show-inheritance:

.. _recon:

Recon
=====

.. automodule:: swift.common.middleware.recon
    :members:
    :show-inheritance:

.. _copy:

Server Side Copy
================

.. automodule:: swift.common.middleware.copy
    :members:
    :show-inheritance:

Static Large Objects
====================

Please see
the SLO docs for :ref:`slo-doc` further details.


.. _staticweb:

StaticWeb
=========

.. automodule:: swift.common.middleware.staticweb
    :members:
    :show-inheritance:

.. _symlink:

Symlink
=======

.. automodule:: swift.common.middleware.symlink
    :members:
    :show-inheritance:

.. _common_tempauth:

TempAuth
========

.. automodule:: swift.common.middleware.tempauth
    :members:
    :show-inheritance:

.. _tempurl:

TempURL
=======

.. automodule:: swift.common.middleware.tempurl
    :members:
    :show-inheritance:

.. _versioned_writes:

Versioned Writes
=================

.. automodule:: swift.common.middleware.versioned_writes.legacy
    :members:
    :show-inheritance:

XProfile
==============

.. automodule:: swift.common.middleware.xprofile
    :members:
    :show-inheritance:
