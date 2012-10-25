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

CORS Headers
============

Cross Origin RequestS or CORS allows the browser to make requests against
Swift from another origin via the browser.  This enables the use of HTML5
forms and javascript uploads to swift.  The owner of a container can set
three headers:

+---------------------------------------------+-------------------------------+
|Metadata                                     | Use                           |
+=============================================+===============================+
|X-Container-Meta-Access-Control-Allow-Origin | Origins to be allowed to      |
|                                             | make Cross Origin Requests,   |
|                                             | space separated               |
+---------------------------------------------+-------------------------------+
|X-Container-Meta-Access-Control-Max-Age      | Max age for the Origin to     |
|                                             | hold the preflight results.   |
+---------------------------------------------+-------------------------------+
|X-Container-Meta-Access-Control-Allow-Headers| Headers to be allowed in      |
|                                             | actual request by browser.    |
+---------------------------------------------+-------------------------------+

When the browser does a request it can issue a preflight request.  The 
preflight request is the OPTIONS call that verifies the Origin is allowed
to make the request.

* Browser makes OPTIONS request to Swift
* Swift returns 200/401 to browser based on allowed origins
* If 200, browser makes PUT, POST, DELETE, HEAD, GET request to Swift

CORS should be used in conjunction with TempURL and FormPost.
