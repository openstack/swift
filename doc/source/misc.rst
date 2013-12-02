.. _misc:

****
Misc
****

.. _exceptions:

Exceptions
==========

.. automodule:: swift.common.exceptions
    :members:
    :undoc-members:
    :show-inheritance:

.. _constraints:

Constraints
===========

.. automodule:: swift.common.constraints
    :members:
    :undoc-members:
    :show-inheritance:

.. _utils:

Utils
=====

.. automodule:: swift.common.utils
    :members:
    :show-inheritance:

.. _common_tempauth:

TempAuth
========

.. automodule:: swift.common.middleware.tempauth
    :members:
    :show-inheritance:

.. _acls:

KeystoneAuth
============

.. automodule:: swift.common.middleware.keystoneauth
    :members:
    :show-inheritance:


ACLs
====

.. automodule:: swift.common.middleware.acl
    :members:
    :show-inheritance:

.. _wsgi:

WSGI
====

.. automodule:: swift.common.wsgi
    :members:
    :show-inheritance:

.. _direct_client:

Direct Client
=============

.. automodule:: swift.common.direct_client
    :members:
    :undoc-members:
    :show-inheritance:

.. _internal_client:

Internal Client
===============

.. automodule:: swift.common.internal_client
    :members:
    :undoc-members:
    :show-inheritance:

.. _buffered_http:

Buffered HTTP
=============

.. automodule:: swift.common.bufferedhttp
    :members:
    :show-inheritance:

.. _healthcheck:

Healthcheck
===========

.. automodule:: swift.common.middleware.healthcheck
    :members:
    :show-inheritance:

.. _recon:

Recon
===========

.. automodule:: swift.common.middleware.recon
    :members:
    :show-inheritance:

.. _memecached:

MemCacheD
=========

.. automodule:: swift.common.memcached
    :members:
    :show-inheritance:

Manager
=========

.. automodule:: swift.common.manager
    :members:
    :show-inheritance:

Ratelimit
=========

.. automodule:: swift.common.middleware.ratelimit
    :members:
    :show-inheritance:

StaticWeb
=========

.. automodule:: swift.common.middleware.staticweb
    :members:
    :show-inheritance:

TempURL
=======

.. automodule:: swift.common.middleware.tempurl
    :members:
    :show-inheritance:

FormPost
========

.. automodule:: swift.common.middleware.formpost
    :members:
    :show-inheritance:

Domain Remap
============

.. automodule:: swift.common.middleware.domain_remap
    :members:
    :show-inheritance:

CNAME Lookup
============

.. automodule:: swift.common.middleware.cname_lookup
    :members:
    :show-inheritance:

Proxy Logging
=============

.. automodule:: swift.common.middleware.proxy_logging
    :members:
    :show-inheritance:

Bulk Operations (Delete and Archive Auto Extraction)
====================================================

.. automodule:: swift.common.middleware.bulk
    :members:
    :show-inheritance:

Container Quotas
================

.. automodule:: swift.common.middleware.container_quotas
    :members:
    :show-inheritance:

Account Quotas
==============

.. automodule:: swift.common.middleware.account_quotas
    :members:
    :show-inheritance:

.. _slo-doc:

Static Large Objects
====================

.. automodule:: swift.common.middleware.slo
    :members:
    :show-inheritance:

List Endpoints
==============

.. automodule:: swift.common.middleware.list_endpoints
    :members:
    :show-inheritance:

Discoverability
===============

Swift can optionally be configured to provide clients with an interface
providing details about the installation. If configured, a GET request to
/info will return configuration data in JSON format.  An example
response::

    {"swift": {"version": "1.8.1"}, "staticweb": {}, "tempurl": {}}

This would signify to the client that swift version 1.8.1 is running and that
staticweb and tempurl are available in this installation.

There may be administrator-only information available via /info. To
retrieve it, one must use an HMAC-signed request, similar to TempURL.
The signature may be produced like so:

    swift-temp-url GET 3600 /info secret 2>/dev/null | sed s/temp_url/swiftinfo/g
