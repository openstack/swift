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



