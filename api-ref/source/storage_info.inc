.. -*- rst -*-

===============
Discoverability
===============

If configured, lists the activated capabilities for this version of
the OpenStack Object Storage API.


List activated capabilities
===========================

.. rest_method::  GET /info

Lists the activated capabilities for this version of the OpenStack Object Storage API.

Most of the information is "public" i.e. visible to all callers. However, some
configuration and capability items are reserved for the administrators of the
system. To access this data, the ``swiftinfo_sig`` and ``swiftinfo_expires``
query parameters must be added to the request.


Normal response codes: 200
Error response codes:


Request
-------

.. rest_parameters:: parameters.yaml

   - swiftinfo_sig: swiftinfo_sig
   - swiftinfo_expires: swiftinfo_expires




Response Example
----------------

.. literalinclude:: samples/capabilities-list-response.json
   :language: javascript



