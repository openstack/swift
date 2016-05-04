.. -*- rst -*-

========
Accounts
========

Lists containers for an account. Creates, updates, shows, and
deletes account metadata.

Account metadata operations work differently than container and
object metadata operations work. Depending on the contents of your
POST account metadata request, the Object Storage API updates the
metadata in one of these ways:

**Account metadata operations**

+----------------------------------------------------------+---------------------------------------------------------------+
| POST request body contains                               | Description                                                   |
+----------------------------------------------------------+---------------------------------------------------------------+
| A metadata key without a value.                          | The API removes the metadata item from the account.           |
|                                                          |                                                               |
| The metadata key already exists for the account.         |                                                               |
+----------------------------------------------------------+---------------------------------------------------------------+
| A metadata key without a value.                          | The API ignores the metadata key.                             |
|                                                          |                                                               |
| The metadata key does not already exist for the account. |                                                               |
+----------------------------------------------------------+---------------------------------------------------------------+
| A metadata key value.                                    | The API updates the metadata key value for the account.       |
|                                                          |                                                               |
| The metadata key already exists for the account.         |                                                               |
+----------------------------------------------------------+---------------------------------------------------------------+
| A metadata key value.                                    | The API adds the metadata key and value pair, or item, to the |
|                                                          | account.                                                      |
| The metadata key does not already exist for the account. |                                                               |
+----------------------------------------------------------+---------------------------------------------------------------+
| One or more account metadata items are omitted.          | The API does not change the existing metadata items.          |
|                                                          |                                                               |
| The metadata items already exist for the account.        |                                                               |
+----------------------------------------------------------+---------------------------------------------------------------+



For these requests, specifying the ``X-Remove-Account-Meta-*``
request header for the key with any value is equivalent to
specifying the ``X-Account-Meta-*`` request header with an empty
value.

Metadata keys must be treated as case-insensitive at all times.
These keys can contain ASCII 7-bit characters that are not control
(0-31) characters, DEL, or a separator character, according to
`HTTP/1.1 <http://www.w3.org/Protocols/rfc2616/rfc2616.html>`_ .
Also, Object Storage does not support the underscore character,
which it silently converts to a hyphen.

The metadata values in Object Storage do not follow HTTP/1.1 rules
for character encodings. You must use a UTF-8 encoding to get a
byte array for any string that contains characters that are not in
the 7-bit ASCII 0-127 range. Otherwise, Object Storage returns the
404 response code for ISO-8859-1 characters in the 128-255 range,
which is a direct violation of the HTTP/1.1 `basic rules
<http://www.w3.org/Protocols/rfc2616/rfc2616-sec2.html#sec2.2>`_.


Show account details and list containers
========================================

.. rest_method::  GET /v1/{account}

Shows details for an account and lists containers, sorted by name, in the account.

The sort order for the name is based on a binary comparison, a
single built-in collating sequence that compares string data by
using the SQLite memcmp() function, regardless of text encoding.
See `Collating Sequences
<http://www.sqlite.org/datatype3.html#collation>`_.

Example requests and responses:

- Show account details and list containers and ask for a JSON
  response:

  ::

     curl -i $publicURL?format=json -X GET -H "X-Auth-Token: $token"


- List containers and ask for an XML response:

  ::

     curl -i $publicURL?format=xml -X GET -H "X-Auth-Token: $token"


The response body returns a list of containers. The default
response (``text/plain``) returns one container per line.

If you use query parameters to page through a long list of
containers, you have reached the end of the list if the number of
items in the returned list is less than the request ``limit``
value. The list contains more items if the number of items in the
returned list equals the ``limit`` value.

When asking for a list of containers and there are none, the
response behavior changes depending on whether the request format
is text, JSON, or XML. For a text response, you get a 204 , because
there is no content. However, for a JSON or XML response, you get a
200 with content indicating an empty array.

If the request succeeds, the operation returns one of these status
codes:

- ``OK (200)``. Success. The response body lists the containers.

- ``No Content (204)``. Success. The response body shows no
  containers. Either the account has no containers or you are
  paging through a long list of names by using the ``marker``,
  ``limit``, or ``end_marker`` query parameter and you have reached
  the end of the list.


Normal response codes: 200
Error response codes:204,


Request
-------

.. rest_parameters:: parameters.yaml

   - account: account
   - limit: limit
   - marker: marker
   - end_marker: end_marker
   - format: format
   - prefix: prefix
   - delimiter: delimiter
   - X-Auth-Token: X-Auth-Token
   - X-Newest: X-Newest
   - Accept: Accept
   - X-Trans-Id-Extra: X-Trans-Id-Extra


Response Parameters
-------------------

.. rest_parameters:: parameters.yaml

   - Content-Length: Content-Length
   - X-Account-Meta-name: X-Account-Meta-name
   - X-Account-Object-Count: X-Account-Object-Count
   - X-Account-Meta-Temp-URL-Key-2: X-Account-Meta-Temp-URL-Key-2
   - X-Timestamp: X-Timestamp
   - X-Account-Meta-Temp-URL-Key: X-Account-Meta-Temp-URL-Key
   - X-Trans-Id: X-Trans-Id
   - Date: Date
   - X-Account-Bytes-Used: X-Account-Bytes-Used
   - X-Account-Container-Count: X-Account-Container-Count
   - Content-Type: Content-Type
   - count: count
   - bytes: bytes
   - name: name



Response Example
----------------

.. literalinclude:: samples/account-containers-list-http-response-xml.txt
   :language: javascript





Create, update, or delete account metadata
==========================================

.. rest_method::  POST /v1/{account}

Creates, updates, or deletes account metadata.

To create, update, or delete metadata, use the ``X-Account-
Meta-{name}`` request header, where ``{name}`` is the name of the
metadata item.

Subsequent requests for the same key and value pair overwrite the
existing value.

To delete a metadata header, send an empty value for that header,
such as for the ``X-Account-Meta-Book`` header. If the tool you use
to communicate with Object Storage, such as an older version of
cURL, does not support empty headers, send the ``X-Remove-Account-
Meta-{name}`` header with an arbitrary value. For example, ``X
-Remove-Account-Meta-Book: x``. The operation ignores the arbitrary
value.

If the container already has other custom metadata items, a request
to create, update, or delete metadata does not affect those items.

This operation does not accept a request body.

Example requests and responses:

- Create account metadata:

  ::

     curl -i $publicURL -X POST -H "X-Auth-Token: $token" -H "X-Account-Meta-Book: MobyDick" -H "X-Account-Meta-Subject: Literature"




  ::

     HTTP/1.1 204 No Content
     Content-Length: 0
     Content-Type: text/html; charset=UTF-8
     X-Trans-Id: tx8c2dd6aee35442a4a5646-0052d954fb
     Date: Fri, 17 Jan 2014 16:06:19 GMT


- Update account metadata:

  ::

     curl -i $publicURL -X POST -H "X-Auth-Token: $token" -H "X-Account-Meta-Subject: AmericanLiterature"




  ::

     HTTP/1.1 204 No Content
     Content-Length: 0
     Content-Type: text/html; charset=UTF-8
     X-Trans-Id: tx1439b96137364ab581156-0052d95532
     Date: Fri, 17 Jan 2014 16:07:14 GMT


- Delete account metadata:

  ::

     curl -i $publicURL -X POST -H "X-Auth-Token: $token" -H "X-Remove-Account-Meta-Subject: x"




  ::

     HTTP/1.1 204 No Content
     Content-Length: 0
     Content-Type: text/html; charset=UTF-8
     X-Trans-Id: tx411cf57701424da99948a-0052d9556f
     Date: Fri, 17 Jan 2014 16:08:15 GMT


If the request succeeds, the operation returns the ``No Content
(204)`` response code.

To confirm your changes, issue a show account metadata request.

Error response codes:204,


Request
-------

.. rest_parameters:: parameters.yaml

   - account: account
   - X-Auth-Token: X-Auth-Token
   - X-Account-Meta-Temp-URL-Key: X-Account-Meta-Temp-URL-Key
   - X-Account-Meta-Temp-URL-Key-2: X-Account-Meta-Temp-URL-Key-2
   - X-Account-Meta-name: X-Account-Meta-name
   - Content-Type: Content-Type
   - X-Detect-Content-Type: X-Detect-Content-Type
   - X-Trans-Id-Extra: X-Trans-Id-Extra


Response Parameters
-------------------

.. rest_parameters:: parameters.yaml

   - Date: Date
   - X-Timestamp: X-Timestamp
   - Content-Length: Content-Length
   - Content-Type: Content-Type
   - X-Trans-Id: X-Trans-Id





Show account metadata
=====================

.. rest_method::  HEAD /v1/{account}

Shows metadata for an account.

Metadata for the account includes:

- Number of containers

- Number of objects

- Total number of bytes that are stored in Object Storage for the
  account

Because the storage system can store large amounts of data, take
care when you represent the total bytes response as an integer;
when possible, convert it to a 64-bit unsigned integer if your
platform supports that primitive type.

Do not include metadata headers in this request.

Show account metadata request:

::

   curl -i $publicURL -X HEAD -H "X-Auth-Token: $token"




::

   HTTP/1.1 204 No Content
   Content-Length: 0
   X-Account-Object-Count: 1
   X-Account-Meta-Book: MobyDick
   X-Timestamp: 1389453423.35964
   X-Account-Bytes-Used: 14
   X-Account-Container-Count: 2
   Content-Type: text/plain; charset=utf-8
   Accept-Ranges: bytes
   X-Trans-Id: txafb3504870144b8ca40f7-0052d955d4
   Date: Fri, 17 Jan 2014 16:09:56 GMT


If the account or authentication token is not valid, the operation
returns the ``Unauthorized (401)`` response code.

Error response codes:204,401,


Request
-------

.. rest_parameters:: parameters.yaml

   - account: account
   - X-Auth-Token: X-Auth-Token
   - X-Newest: X-Newest
   - X-Trans-Id-Extra: X-Trans-Id-Extra


Response Parameters
-------------------

.. rest_parameters:: parameters.yaml

   - Content-Length: Content-Length
   - X-Account-Meta-name: X-Account-Meta-name
   - X-Account-Object-Count: X-Account-Object-Count
   - X-Account-Meta-Temp-URL-Key-2: X-Account-Meta-Temp-URL-Key-2
   - X-Timestamp: X-Timestamp
   - X-Account-Meta-Temp-URL-Key: X-Account-Meta-Temp-URL-Key
   - X-Trans-Id: X-Trans-Id
   - Date: Date
   - X-Account-Bytes-Used: X-Account-Bytes-Used
   - X-Account-Container-Count: X-Account-Container-Count
   - Content-Type: Content-Type





