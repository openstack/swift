.. -*- rst -*-

=========
Endpoints
=========

If configured, lists endpoints for an account.


List endpoints
==============

.. rest_method::  GET /v1/endpoints

Lists endpoints for an object, account, or container.

When the cloud provider enables middleware to list the
``/endpoints/`` path, software that needs data location information
can use this call to avoid network overhead. The cloud provider can
map the ``/endpoints/`` path to another resource, so this exact
resource might vary from provider to provider. Because it goes
straight to the middleware, the call is not authenticated, so be
sure you have tightly secured the environment and network when
using this call.

Error response codes:201,


Request
-------

This operation does not accept a request body.





