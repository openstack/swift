====================================
Use the Content-Disposition metadata
====================================

To override the default behavior for a browser, use the
``Content-Disposition`` header to specify the override behavior and
assign this header to an object. For example, this header might specify
that the browser use a download program to save this file rather than
show the file, which is the default.

**Example Override browser default behavior request: HTTP**

This example assigns an attachment type to the ``Content-Disposition``
header. This attachment type indicates that the file is to be downloaded
as ``goodbye.txt``:

.. code:: console

   # curl -i  $publicURL/marktwain/goodbye -X POST -H "X-Auth-Token: $token" -H "Content-Length: 14" -H "Content-Type: application/octet-stream" -H "Content-Disposition: attachment; filename=goodbye.txt"

.. code:: console

   HTTP/1.1 202 Accepted
   Content-Length: 76
   Content-Type: text/html; charset=UTF-8
   X-Trans-Id: txa9b5e57d7f354d7ea9f57-0052e17e13
   X-Openstack-Request-Id: txa9b5e57d7f354d7ea9f57-0052e17e13
   Date: Thu, 23 Jan 2014 20:39:47 GMT

   <html><h1>Accepted</h1><p>The request is accepted for processing.</p></html>

