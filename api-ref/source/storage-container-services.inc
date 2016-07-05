.. -*- rst -*-

==========
Containers
==========

Lists objects in a container. Creates, shows details for, and
deletes containers. Creates, updates, shows, and deletes container
metadata.


Show container details and list objects
=======================================

.. rest_method::  GET /v1/{account}/{container}

Shows details for a container and lists objects, sorted by name, in the container.

Specify query parameters in the request to filter the list and
return a subset of object names. Omit query parameters to return
the complete list of object names that are stored in the container,
up to 10,000 names. The 10,000 maximum value is configurable. To
view the value for the cluster, issue a GET ``/info`` request.

Example requests and responses:

- ``OK (200)``. Success. The response body lists the objects.

- ``No Content (204)``. Success. The response body shows no objects.
  Either the container has no objects or you are paging through a
  long list of names by using the ``marker``, ``limit``, or
  ``end_marker`` query parameter and you have reached the end of
  the list.

If the container does not exist, the call returns the ``Not Found
(404)`` response code.

The operation returns the ``Range Not Satisfiable (416)`` response
code for any ranged GET requests that specify more than:

- Fifty ranges.

- Three overlapping ranges.

- Eight non-increasing ranges.


Normal response codes: 200
Error response codes:416,404,204,


Request
-------

.. rest_parameters:: parameters.yaml

   - account: account
   - container: container
   - limit: limit
   - marker: marker
   - end_marker: end_marker
   - prefix: prefix
   - format: format
   - delimiter: delimiter
   - path: path
   - X-Auth-Token: X-Auth-Token
   - X-Newest: X-Newest
   - Accept: Accept
   - X-Container-Meta-Temp-URL-Key: X-Container-Meta-Temp-URL-Key
   - X-Container-Meta-Temp-URL-Key-2: X-Container-Meta-Temp-URL-Key-2
   - X-Trans-Id-Extra: X-Trans-Id-Extra


Response Parameters
-------------------

.. rest_parameters:: parameters.yaml

   - X-Container-Meta-name: X-Container-Meta-name
   - Content-Length: Content-Length
   - X-Container-Object-Count: X-Container-Object-Count
   - Accept-Ranges: Accept-Ranges
   - X-Container-Meta-Temp-URL-Key: X-Container-Meta-Temp-URL-Key
   - X-Container-Bytes-Used: X-Container-Bytes-Used
   - X-Container-Meta-Temp-URL-Key-2: X-Container-Meta-Temp-URL-Key-2
   - X-Timestamp: X-Timestamp
   - X-Trans-Id: X-Trans-Id
   - Date: Date
   - Content-Type: Content-Type
   - hash: hash
   - last_modified: last_modified
   - bytes: bytes
   - name: name
   - content_type: content_type



Response Example
----------------

.. literalinclude:: samples/objects-list-http-response-xml.txt
   :language: javascript







Create container
================

.. rest_method::  PUT /v1/{account}/{container}

Creates a container.

You do not need to check whether a container already exists before
issuing a PUT operation because the operation is idempotent: It
creates a container or updates an existing container, as
appropriate.

Example requests and responses:

- Create a container with no metadata:

  ::

     curl -i $publicURL/steven -X PUT -H "Content-Length: 0" -H "X-Auth-Token: $token"




  ::

     HTTP/1.1 201 Created
     Content-Length: 0
     Content-Type: text/html; charset=UTF-8
     X-Trans-Id: tx7f6b7fa09bc2443a94df0-0052d58b56
     Date: Tue, 14 Jan 2014 19:09:10 GMT


- Create a container with metadata:

  ::

     curl -i $publicURL/marktwain -X PUT -H "X-Auth-Token: $token" -H "X-Container-Meta-Book: TomSawyer"




  ::

     HTTP/1.1 201 Created
     Content-Length: 0
     Content-Type: text/html; charset=UTF-8
     X-Trans-Id: tx06021f10fc8642b2901e7-0052d58f37
     Date: Tue, 14 Jan 2014 19:25:43 GMT

Error response codes:201,204,


Request
-------

.. rest_parameters:: parameters.yaml

   - account: account
   - container: container
   - X-Auth-Token: X-Auth-Token
   - X-Container-Read: X-Container-Read
   - X-Container-Write: X-Container-Write
   - X-Container-Sync-To: X-Container-Sync-To
   - X-Container-Sync-Key: X-Container-Sync-Key
   - X-Versions-Location: X-Versions-Location
   - X-Container-Meta-name: X-Container-Meta-name
   - X-Container-Meta-Access-Control-Allow-Origin: X-Container-Meta-Access-Control-Allow-Origin
   - X-Container-Meta-Access-Control-Max-Age: X-Container-Meta-Access-Control-Max-Age
   - X-Container-Meta-Access-Control-Expose-Headers: X-Container-Meta-Access-Control-Expose-Headers
   - Content-Type: Content-Type
   - X-Detect-Content-Type: X-Detect-Content-Type
   - X-Container-Meta-Temp-URL-Key: X-Container-Meta-Temp-URL-Key
   - X-Container-Meta-Temp-URL-Key-2: X-Container-Meta-Temp-URL-Key-2
   - X-Trans-Id-Extra: X-Trans-Id-Extra


Response Parameters
-------------------

.. rest_parameters:: parameters.yaml

   - Date: Date
   - X-Timestamp: X-Timestamp
   - Content-Length: Content-Length
   - Content-Type: Content-Type
   - X-Trans-Id: X-Trans-Id






Create, update, or delete container metadata
============================================

.. rest_method::  POST /v1/{account}/{container}

Creates, updates, or deletes custom metadata for a container.

To create, update, or delete a custom metadata item, use the ``X
-Container-Meta-{name}`` header, where ``{name}`` is the name of
the metadata item.

Subsequent requests for the same key and value pair overwrite the
previous value.

To delete container metadata, send an empty value for that header,
such as for the ``X-Container-Meta-Book`` header. If the tool you
use to communicate with Object Storage, such as an older version of
cURL, does not support empty headers, send the ``X-Remove-
Container-Meta-{name}`` header with an arbitrary value. For
example, ``X-Remove-Container-Meta-Book: x``. The operation ignores
the arbitrary value.

If the container already has other custom metadata items, a request
to create, update, or delete metadata does not affect those items.

Example requests and responses:

- Create container metadata:

  ::

     curl -i $publicURL/marktwain -X POST -H "X-Auth-Token: $token" -H "X-Container-Meta-Author: MarkTwain" -H "X-Container-Meta-Web-Directory-Type: text/directory" -H "X-Container-Meta-Century: Nineteenth"




  ::

     HTTP/1.1 204 No Content
     Content-Length: 0
     Content-Type: text/html; charset=UTF-8
     X-Trans-Id: tx05dbd434c651429193139-0052d82635
     Date: Thu, 16 Jan 2014 18:34:29 GMT


- Update container metadata:

  ::

     curl -i $publicURL/marktwain -X POST -H "X-Auth-Token: $token" -H "X-Container-Meta-Author: SamuelClemens"




  ::

     HTTP/1.1 204 No Content
     Content-Length: 0
     Content-Type: text/html; charset=UTF-8
     X-Trans-Id: txe60c7314bf614bb39dfe4-0052d82653
     Date: Thu, 16 Jan 2014 18:34:59 GMT


- Delete container metadata:

  ::

     curl -i $publicURL/marktwain -X POST -H "X-Auth-Token: $token" -H "X-Remove-Container-Meta-Century: x"




  ::

     HTTP/1.1 204 No Content
     Content-Length: 0
     Content-Type: text/html; charset=UTF-8
     X-Trans-Id: tx7997e18da2a34a9e84ceb-0052d826d0
     Date: Thu, 16 Jan 2014 18:37:04 GMT


If the request succeeds, the operation returns the ``No Content
(204)`` response code.

To confirm your changes, issue a show container metadata request.

Error response codes:204,


Request
-------

.. rest_parameters:: parameters.yaml

   - account: account
   - container: container
   - X-Auth-Token: X-Auth-Token
   - X-Container-Read: X-Container-Read
   - X-Remove-Container-name: X-Remove-Container-name
   - X-Container-Write: X-Container-Write
   - X-Container-Sync-To: X-Container-Sync-To
   - X-Container-Sync-Key: X-Container-Sync-Key
   - X-Versions-Location: X-Versions-Location
   - X-Remove-Versions-Location: X-Remove-Versions-Location
   - X-Container-Meta-name: X-Container-Meta-name
   - X-Container-Meta-Access-Control-Allow-Origin: X-Container-Meta-Access-Control-Allow-Origin
   - X-Container-Meta-Access-Control-Max-Age: X-Container-Meta-Access-Control-Max-Age
   - X-Container-Meta-Access-Control-Expose-Headers: X-Container-Meta-Access-Control-Expose-Headers
   - X-Container-Meta-Quota-Bytes: X-Container-Meta-Quota-Bytes
   - X-Container-Meta-Quota-Count: X-Container-Meta-Quota-Count
   - X-Container-Meta-Web-Directory-Type: X-Container-Meta-Web-Directory-Type
   - Content-Type: Content-Type
   - X-Detect-Content-Type: X-Detect-Content-Type
   - X-Container-Meta-Temp-URL-Key: X-Container-Meta-Temp-URL-Key
   - X-Container-Meta-Temp-URL-Key-2: X-Container-Meta-Temp-URL-Key-2
   - X-Trans-Id-Extra: X-Trans-Id-Extra


Response Parameters
-------------------

.. rest_parameters:: parameters.yaml

   - Date: Date
   - X-Timestamp: X-Timestamp
   - Content-Length: Content-Length
   - Content-Type: Content-Type
   - X-Trans-Id: X-Trans-Id





Show container metadata
=======================

.. rest_method::  HEAD /v1/{account}/{container}

Shows container metadata, including the number of objects and the total bytes of all objects stored in the container.

Show container metadata request:

::

   curl -i $publicURL/marktwain -X HEAD -H "X-Auth-Token: $token"




::

   HTTP/1.1 204 No Content
   Content-Length: 0
   X-Container-Object-Count: 1
   Accept-Ranges: bytes
   X-Container-Meta-Book: TomSawyer
   X-Timestamp: 1389727543.65372
   X-Container-Meta-Author: SamuelClemens
   X-Container-Bytes-Used: 14
   Content-Type: text/plain; charset=utf-8
   X-Trans-Id: tx0287b982a268461b9ec14-0052d826e2
   Date: Thu, 16 Jan 2014 18:37:22 GMT


If the request succeeds, the operation returns the ``No Content
(204)`` response code.

Error response codes:204,


Request
-------

.. rest_parameters:: parameters.yaml

   - account: account
   - container: container
   - X-Auth-Token: X-Auth-Token
   - X-Newest: X-Newest
   - X-Container-Meta-Temp-URL-Key: X-Container-Meta-Temp-URL-Key
   - X-Container-Meta-Temp-URL-Key-2: X-Container-Meta-Temp-URL-Key-2
   - X-Trans-Id-Extra: X-Trans-Id-Extra


Response Parameters
-------------------

.. rest_parameters:: parameters.yaml

   - X-Container-Sync-Key: X-Container-Sync-Key
   - X-Container-Meta-name: X-Container-Meta-name
   - Content-Length: Content-Length
   - X-Container-Object-Count: X-Container-Object-Count
   - X-Container-Write: X-Container-Write
   - X-Container-Meta-Quota-Count: X-Container-Meta-Quota-Count
   - Accept-Ranges: Accept-Ranges
   - X-Container-Read: X-Container-Read
   - X-Container-Meta-Access-Control-Expose-Headers: X-Container-Meta-Access-Control-Expose-Headers
   - X-Container-Meta-Temp-URL-Key: X-Container-Meta-Temp-URL-Key
   - X-Container-Bytes-Used: X-Container-Bytes-Used
   - X-Container-Meta-Temp-URL-Key-2: X-Container-Meta-Temp-URL-Key-2
   - X-Timestamp: X-Timestamp
   - X-Container-Meta-Access-Control-Allow-Origin: X-Container-Meta-Access-Control-Allow-Origin
   - X-Container-Meta-Access-Control-Max-Age: X-Container-Meta-Access-Control-Max-Age
   - Date: Date
   - X-Trans-Id: X-Trans-Id
   - X-Container-Sync-To: X-Container-Sync-To
   - Content-Type: Content-Type
   - X-Container-Meta-Quota-Bytes: X-Container-Meta-Quota-Bytes
   - X-Versions-Location: X-Versions-Location





Delete container
================

.. rest_method::  DELETE /v1/{account}/{container}

Deletes an empty container.

This operation fails unless the container is empty. An empty
container has no objects.

Delete the ``steven`` container:

::

   curl -i $publicURL/steven -X DELETE -H "X-Auth-Token: $token"


If the container does not exist, the response is:

::

   HTTP/1.1 404 Not Found
   Content-Length: 70
   Content-Type: text/html; charset=UTF-8
   X-Trans-Id: tx4d728126b17b43b598bf7-0052d81e34
   Date: Thu, 16 Jan 2014 18:00:20 GMT


If the container exists and the deletion succeeds, the response is:

::

   HTTP/1.1 204 No Content
   Content-Length: 0
   Content-Type: text/html; charset=UTF-8
   X-Trans-Id: txf76c375ebece4df19c84c-0052d81f14
   Date: Thu, 16 Jan 2014 18:04:04 GMT


If the container exists but is not empty, the response is:

::

   HTTP/1.1 409 Conflict
   Content-Length: 95
   Content-Type: text/html; charset=UTF-8
   X-Trans-Id: tx7782dc6a97b94a46956b5-0052d81f6b
   Date: Thu, 16 Jan 2014 18:05:31 GMT
   <html>
   <h1>Conflict
   </h1>
   <p>There was a conflict when trying to complete your request.
   </p>
   </html>

Error response codes:404,204,409,


Request
-------

.. rest_parameters:: parameters.yaml

   - account: account
   - container: container
   - X-Auth-Token: X-Auth-Token
   - X-Container-Meta-Temp-URL-Key: X-Container-Meta-Temp-URL-Key
   - X-Container-Meta-Temp-URL-Key-2: X-Container-Meta-Temp-URL-Key-2
   - X-Trans-Id-Extra: X-Trans-Id-Extra


Response Parameters
-------------------

.. rest_parameters:: parameters.yaml

   - Date: Date
   - X-Timestamp: X-Timestamp
   - Content-Length: Content-Length
   - Content-Type: Content-Type
   - X-Trans-Id: X-Trans-Id






