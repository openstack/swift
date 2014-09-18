.. _common_middleware:

**********
Middleware
**********

Account Quotas
==============

.. automodule:: swift.common.middleware.account_quotas
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

    swift-temp-url GET 3600 /info secret 2>/dev/null | sed s/temp_url/swiftinfo/g

Domain Remap
============

.. automodule:: swift.common.middleware.domain_remap
    :members:
    :show-inheritance:

Dynamic Large Objects
=====================

.. automodule:: swift.common.middleware.dlo
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
=============

.. automodule:: swift.common.middleware.gatekeeper
    :members:
    :show-inheritance:

.. _healthcheck:

Healthcheck
===========

.. automodule:: swift.common.middleware.healthcheck
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

.. _recon:

Recon
===========

.. automodule:: swift.common.middleware.recon
    :members:
    :show-inheritance:

.. _slo-doc:

Static Large Objects
====================

.. automodule:: swift.common.middleware.slo
    :members:
    :show-inheritance:

.. _staticweb:

StaticWeb
=========

.. automodule:: swift.common.middleware.staticweb
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

XProfile
==============

.. automodule:: swift.common.middleware.xprofile
    :members:
    :show-inheritance:
