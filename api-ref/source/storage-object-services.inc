.. -*- rst -*-

=======
Objects
=======

Creates, replaces, shows details for, and deletes objects. Copies
objects from another object with a new or different name. Updates
object metadata. For more information and concepts about
objects see `Object Storage API overview
<https://docs.openstack.org/swift/latest/api/object_api_v1_overview.html>`_
and `Large Objects
<https://docs.openstack.org/swift/latest/api/large_objects.html>`_.


Get object content and metadata
===============================

.. rest_method::  GET /v1/{account}/{container}/{object}

Downloads the object content and gets the object metadata.

This operation returns the object metadata in the response headers
and the object content in the response body.

If this is a large object, the response body contains the
concatenated content of the segment objects. To get the manifest
instead of concatenated segment objects for a static large object,
use the ``multipart-manifest`` query parameter.

Example requests and responses:

- Show object details for the ``goodbye`` object in the
  ``marktwain`` container:

  ::

     curl -i $publicURL/marktwain/goodbye -X GET -H "X-Auth-Token: $token"




  ::

     HTTP/1.1 200 OK
     Content-Length: 14
     Accept-Ranges: bytes
     Last-Modified: Wed, 15 Jan 2014 16:41:49 GMT
     Etag: 451e372e48e0f6b1114fa0724aa79fa1
     X-Timestamp: 1389804109.39027
     X-Object-Meta-Orig-Filename: goodbyeworld.txt
     Content-Type: application/octet-stream
     X-Trans-Id: tx8145a190241f4cf6b05f5-0052d82a34
     X-Openstack-Request-Id: tx8145a190241f4cf6b05f5-0052d82a34
     Date: Thu, 16 Jan 2014 18:51:32 GMT
     Goodbye World!


- Show object details for the ``goodbye`` object, which does not
  exist, in the ``janeausten`` container:

  ::

     curl -i $publicURL/janeausten/goodbye -X GET -H "X-Auth-Token: $token"




  ::

     HTTP/1.1 404 Not Found
     Content-Length: 70
     Content-Type: text/html; charset=UTF-8
     X-Trans-Id: tx073f7cbb850c4c99934b9-0052d82b04
     X-Openstack-Request-Id: tx073f7cbb850c4c99934b9-0052d82b04
     Date: Thu, 16 Jan 2014 18:55:00 GMT
     <html>
     <h1>Not Found
     </h1>
     <p>The resource could not be found.
     </p>
     </html>


The operation returns the ``Range Not Satisfiable (416)`` response
code for any ranged GET requests that specify more than:

- Fifty ranges.

- Three overlapping ranges.

- Eight non-increasing ranges.


Normal response codes: 200

Error response codes: 416, 404


Request
-------

.. rest_parameters:: parameters.yaml

   - account: account
   - container: container
   - object: object
   - X-Auth-Token: X-Auth-Token
   - X-Service-Token: X-Service-Token
   - X-Newest: X-Newest
   - temp_url_sig: temp_url_sig
   - temp_url_expires: temp_url_expires
   - filename: filename
   - multipart-manifest: multipart-manifest_get
   - symlink: symlink
   - Range: Range
   - If-Match: If-Match
   - If-None-Match: If-None-Match-get-request
   - If-Modified-Since: If-Modified-Since
   - If-Unmodified-Since: If-Unmodified-Since
   - X-Trans-Id-Extra: X-Trans-Id-Extra


Response Parameters
-------------------

.. rest_parameters:: parameters.yaml

   - Content-Length: Content-Length_get_resp
   - Content-Type: Content-Type_obj_resp
   - X-Object-Meta-name: X-Object-Meta-name_resp
   - Content-Disposition: Content-Disposition_resp
   - Content-Encoding: Content-Encoding_resp
   - X-Delete-At: X-Delete-At_resp
   - Accept-Ranges: Accept-Ranges
   - X-Object-Manifest: X-Object-Manifest_resp
   - Last-Modified: Last-Modified
   - ETag: ETag_obj_resp
   - X-Timestamp: X-Timestamp
   - X-Trans-Id: X-Trans-Id
   - X-Openstack-Request-Id: X-Openstack-Request-Id
   - Date: Date
   - X-Static-Large-Object: X-Static-Large-Object
   - X-Symlink-Target: X-Symlink-Target_resp
   - X-Symlink-Target-Account: X-Symlink-Target-Account_resp


Response Example
----------------

See examples above.


Create or replace object
========================

.. rest_method::  PUT /v1/{account}/{container}/{object}

Creates an object with data content and metadata, or replaces an existing object with data content and metadata.

The PUT operation always creates an object. If you use this
operation on an existing object, you replace the existing object
and metadata rather than modifying the object. Consequently, this
operation returns the ``Created (201)`` response code.

If you use this operation to copy a manifest object, the new object
is a normal object and not a copy of the manifest. Instead it is a
concatenation of all the segment objects. This means that you
cannot copy objects larger than 5 GB.

Note that the provider may have limited the characters which are allowed
in an object name. Any name limits are exposed under the ``name_check`` key
in the ``/info`` discoverability response. Regardless of ``name_check``
limitations, names must be URL quoted UTF-8.

To create custom metadata, use the
``X-Object-Meta-name`` header, where ``name`` is the name of the metadata
item.

.. include:: metadata_header_syntax.inc

Example requests and responses:

- Create object:

  ::

     curl -i $publicURL/janeausten/helloworld.txt -X PUT -d "Hello" -H "Content-Type: text/html; charset=UTF-8" -H "X-Auth-Token: $token"




  ::

     HTTP/1.1 201 Created
     Last-Modified: Fri, 17 Jan 2014 17:28:35 GMT
     Content-Length: 0
     Etag: 8b1a9953c4611296a827abf8c47804d7
     Content-Type: text/html; charset=UTF-8
     X-Trans-Id: tx4d5e4f06d357462bb732f-0052d96843
     X-Openstack-Request-Id: tx4d5e4f06d357462bb732f-0052d96843
     Date: Fri, 17 Jan 2014 17:28:35 GMT


- Replace object:

  ::

     curl -i $publicURL/janeausten/helloworld.txt -X PUT -d "Hola" -H "X-Auth-Token: $token"




  ::

     HTTP/1.1 201 Created
     Last-Modified: Fri, 17 Jan 2014 17:28:35 GMT
     Content-Length: 0
     Etag: f688ae26e9cfa3ba6235477831d5122e
     Content-Type: text/html; charset=UTF-8
     X-Trans-Id: tx4d5e4f06d357462bb732f-0052d96843
     X-Openstack-Request-Id: tx4d5e4f06d357462bb732f-0052d96843
     Date: Fri, 17 Jan 2014 17:28:35 GMT


The ``Created (201)`` response code indicates a successful write.

If the container for the object does not already exist, the operation
returns the ``404 Not Found`` response code.

If the request times out, the operation returns the ``Request
Timeout (408)`` response code.

The ``Length Required (411)`` response code indicates a missing
``Transfer-Encoding`` or ``Content-Length`` request header.

If the MD5 checksum of the data that is written to the object store
does not match the optional ``ETag`` value, the operation returns
the ``Unprocessable Entity (422)`` response code.

Normal response codes: 201

Error response codes: 404, 408, 411, 422


Request
-------

.. rest_parameters:: parameters.yaml

   - account: account
   - container: container
   - object: object
   - multipart-manifest: multipart-manifest_put
   - temp_url_sig: temp_url_sig
   - temp_url_expires: temp_url_expires
   - X-Object-Manifest: X-Object-Manifest
   - X-Auth-Token: X-Auth-Token
   - X-Service-Token: X-Service-Token
   - Content-Length: Content-Length_put_req
   - Transfer-Encoding: Transfer-Encoding
   - Content-Type: Content-Type_obj_cu_req
   - X-Detect-Content-Type: X-Detect-Content-Type
   - X-Copy-From: X-Copy-From
   - X-Copy-From-Account: X-Copy-From-Account
   - ETag: ETag_obj_req
   - Content-Disposition: Content-Disposition
   - Content-Encoding: Content-Encoding
   - X-Delete-At: X-Delete-At
   - X-Delete-After: X-Delete-After
   - X-Object-Meta-name: X-Object-Meta-name
   - If-None-Match: If-None-Match-put-request
   - X-Trans-Id-Extra: X-Trans-Id-Extra
   - X-Symlink-Target: X-Symlink-Target
   - X-Symlink-Target-Account: X-Symlink-Target-Account


Response Parameters
-------------------

.. rest_parameters:: parameters.yaml

   - Content-Length: Content-Length_cud_resp
   - ETag: ETag_obj_received
   - X-Timestamp: X-Timestamp
   - X-Trans-Id: X-Trans-Id
   - X-Openstack-Request-Id: X-Openstack-Request-Id
   - Date: Date
   - Content-Type: Content-Type_obj_resp
   - last_modified: last_modified








Copy object
===========

.. rest_method::  COPY /v1/{account}/{container}/{object}

Copies an object to another object in the object store.

You can copy an object to a new object with the same name. Copying
to the same name is an alternative to using POST to add metadata to
an object. With POST, you must specify all the metadata. With COPY,
you can add additional metadata to the object.

With COPY, you can set the ``X-Fresh-Metadata`` header to ``true``
to copy the object without any existing metadata.

Alternatively, you can use PUT with the ``X-Copy-From`` request
header to accomplish the same operation as the COPY object
operation.

The COPY operation always creates an object. If you use this
operation on an existing object, you replace the existing object
and metadata rather than modifying the object. Consequently, this
operation returns the ``Created (201)`` response code.

Normally, if you use this operation to copy a manifest object, the new object
is a normal object and not a copy of the manifest. Instead it is a
concatenation of all the segment objects. This means that you
cannot copy objects larger than 5 GB in size.

To copy the manifest object, you include the
``multipart-manifest=get`` query string in the COPY request.
The new object contains the same manifest as the original.
The segment objects are not copied. Instead, both the original
and new manifest objects share the same set of segment objects.

To copy a symlink either with a COPY or a PUT with the
``X-Copy-From`` request, include the ``symlink=get`` query string.
The new symlink will have the same target as the original.
The target object is not copied. Instead, both the original
and new symlinks point to the same target object.

All metadata is
preserved during the object copy. If you specify metadata on the
request to copy the object, either PUT or COPY , the metadata
overwrites any conflicting keys on the target (new) object.

Example requests and responses:

- Copy the ``goodbye`` object from the ``marktwain`` container to
  the ``janeausten`` container:

  ::

     curl -i $publicURL/marktwain/goodbye -X COPY -H "X-Auth-Token: $token" -H "Destination: janeausten/goodbye"




  ::

     HTTP/1.1 201 Created
     Content-Length: 0
     X-Copied-From-Last-Modified: Thu, 16 Jan 2014 21:19:45 GMT
     X-Copied-From: marktwain/goodbye
     Last-Modified: Fri, 17 Jan 2014 18:22:57 GMT
     Etag: 451e372e48e0f6b1114fa0724aa79fa1
     Content-Type: text/html; charset=UTF-8
     X-Object-Meta-Movie: AmericanPie
     X-Trans-Id: txdcb481ad49d24e9a81107-0052d97501
     X-Openstack-Request-Id: txdcb481ad49d24e9a81107-0052d97501
     Date: Fri, 17 Jan 2014 18:22:57 GMT


- Alternatively, you can use PUT to copy the ``goodbye`` object from
  the ``marktwain`` container to the ``janeausten`` container. This
  request requires a ``Content-Length`` header, even if it is set
  to zero (0).

  ::

     curl -i $publicURL/janeausten/goodbye -X PUT -H "X-Auth-Token: $token" -H "X-Copy-From: /marktwain/goodbye" -H "Content-Length: 0"




  ::

     HTTP/1.1 201 Created
     Content-Length: 0
     X-Copied-From-Last-Modified: Thu, 16 Jan 2014 21:19:45 GMT
     X-Copied-From: marktwain/goodbye
     Last-Modified: Fri, 17 Jan 2014 18:22:57 GMT
     Etag: 451e372e48e0f6b1114fa0724aa79fa1
     Content-Type: text/html; charset=UTF-8
     X-Object-Meta-Movie: AmericanPie
     X-Trans-Id: txdcb481ad49d24e9a81107-0052d97501
     X-Openstack-Request-Id: txdcb481ad49d24e9a81107-0052d97501
     Date: Fri, 17 Jan 2014 18:22:57 GMT


When several replicas exist, the system copies from the most recent
replica. That is, the COPY operation behaves as though the
``X-Newest`` header is in the request.

Normal response codes: 201


Request
-------

.. rest_parameters:: parameters.yaml

   - account: account
   - container: container
   - object: object
   - multipart-manifest: multipart-manifest_copy
   - symlink: symlink_copy
   - X-Auth-Token: X-Auth-Token
   - X-Service-Token: X-Service-Token
   - Destination: Destination
   - Destination-Account: Destination-Account
   - Content-Type: Content-Type_obj_cu_req
   - Content-Encoding: Content-Encoding
   - Content-Disposition: Content-Disposition
   - X-Object-Meta-name: X-Object-Meta-name
   - X-Fresh-Metadata: X-Fresh-Metadata
   - X-Trans-Id-Extra: X-Trans-Id-Extra


Response Parameters
-------------------

.. rest_parameters:: parameters.yaml

   - Content-Length: Content-Length_cud_resp
   - X-Copied-From-Last-Modified: X-Copied-From-Last-Modified
   - X-Copied-From: X-Copied-From
   - X-Copied-From-Account: X-Copied-From-Account
   - Last-Modified: Last-Modified
   - ETag: ETag_obj_copied
   - X-Timestamp: X-Timestamp
   - X-Trans-Id: X-Trans-Id
   - X-Openstack-Request-Id: X-Openstack-Request-Id
   - Date: Date
   - Content-Type: Content-Type_obj_resp





Delete object
=============

.. rest_method::  DELETE /v1/{account}/{container}/{object}

Permanently deletes an object from the object store.

Object deletion occurs as soon as possible. Subsequent GET, HEAD, POST,
or DELETE operations should return a ``404 Not Found`` error code, but
may return stale data due to eventual consistency.

For static large object manifests, you can add the
``?multipart-manifest=delete`` query parameter. This operation deletes
the segment objects and, if all deletions succeed, this operation
deletes the manifest object.

A DELETE request made to a symlink path will delete the symlink
rather than the target object.

An alternative to using the DELETE operation is to use
the POST operation with the ``bulk-delete`` query parameter.

Example request and response:

- Delete the ``helloworld`` object from the ``marktwain`` container:

  ::

     curl -i $publicURL/marktwain/helloworld -X DELETE -H "X-Auth-Token: $token"




  ::

     HTTP/1.1 204 No Content
     Content-Length: 0
     Content-Type: text/html; charset=UTF-8
     X-Trans-Id: tx36c7606fcd1843f59167c-0052d6fdac
     X-Openstack-Request-Id: tx36c7606fcd1843f59167c-0052d6fdac
     Date: Wed, 15 Jan 2014 21:29:16 GMT


Typically, the DELETE operation does not return a response body.
However, with the ``multipart-manifest=delete`` query parameter,
the response body contains a list of manifest and segment objects
and the status of their DELETE operations.

Normal response codes: 204


Request
-------

.. rest_parameters:: parameters.yaml

   - account: account
   - container: container
   - object: object
   - multipart-manifest: multipart-manifest_delete
   - X-Auth-Token: X-Auth-Token
   - X-Service-Token: X-Service-Token
   - X-Trans-Id-Extra: X-Trans-Id-Extra


Response Parameters
-------------------

.. rest_parameters:: parameters.yaml

   - Date: Date
   - X-Timestamp: X-Timestamp
   - Content-Length: Content-Length_cud_resp
   - Content-Type: Content-Type_cud_resp
   - X-Trans-Id: X-Trans-Id
   - X-Openstack-Request-Id: X-Openstack-Request-Id





Show object metadata
====================

.. rest_method::  HEAD /v1/{account}/{container}/{object}

Shows object metadata.



Example requests and responses:

- Show object metadata:

  ::

     curl $publicURL/marktwain/goodbye --head -H "X-Auth-Token: $token"




  ::

     HTTP/1.1 200 OK
     Content-Length: 14
     Accept-Ranges: bytes
     Last-Modified: Thu, 16 Jan 2014 21:12:31 GMT
     Etag: 451e372e48e0f6b1114fa0724aa79fa1
     X-Timestamp: 1389906751.73463
     X-Object-Meta-Book: GoodbyeColumbus
     Content-Type: application/octet-stream
     X-Trans-Id: tx37ea34dcd1ed48ca9bc7d-0052d84b6f
     X-Openstack-Request-Id: tx37ea34dcd1ed48ca9bc7d-0052d84b6f
     Date: Thu, 16 Jan 2014 21:13:19 GMT

  Note: The ``--head`` option was used in the above example. If we had
  used ``-i -X HEAD`` and the ``Content-Length`` response header is non-zero,
  the cURL command stalls after it prints the response headers because it
  is waiting for a response body. However, the Object Storage system
  does not return a response body for the HEAD operation.


If the request succeeds, the operation returns the ``200`` response
code.


Normal response codes: 200


Request
-------

.. rest_parameters:: parameters.yaml

   - account: account
   - container: container
   - object: object
   - X-Auth-Token: X-Auth-Token
   - X-Service-Token: X-Service-Token
   - temp_url_sig: temp_url_sig
   - temp_url_expires: temp_url_expires
   - filename: filename
   - multipart-manifest: multipart-manifest_head
   - symlink: symlink
   - X-Newest: X-Newest
   - If-Match: If-Match
   - If-None-Match: If-None-Match-get-request
   - If-Modified-Since: If-Modified-Since
   - If-Unmodified-Since: If-Unmodified-Since
   - X-Trans-Id-Extra: X-Trans-Id-Extra


Response Parameters
-------------------

.. rest_parameters:: parameters.yaml

   - Content-Length: Content-Length_obj_head_resp
   - X-Object-Meta-name: X-Object-Meta-name
   - Content-Disposition: Content-Disposition_resp
   - Content-Encoding: Content-Encoding_resp
   - X-Delete-At: X-Delete-At_resp
   - X-Object-Manifest: X-Object-Manifest_resp
   - Last-Modified: Last-Modified
   - ETag: ETag_obj_resp
   - X-Timestamp: X-Timestamp
   - X-Trans-Id: X-Trans-Id
   - X-Openstack-Request-Id: X-Openstack-Request-Id
   - Date: Date
   - X-Static-Large-Object: X-Static-Large-Object
   - Content-Type: Content-Type_obj_resp
   - X-Symlink-Target: X-Symlink-Target_resp
   - X-Symlink-Target-Account: X-Symlink-Target-Account_resp


Response Example
----------------

See examples above.



Create or update object metadata
================================

.. rest_method::  POST /v1/{account}/{container}/{object}

Creates or updates object metadata.

To create or update custom metadata, use the
``X-Object-Meta-name`` header, where ``name`` is the name of the metadata
item.

.. include:: metadata_header_syntax.inc

In addition to the custom metadata, you can update the
``Content-Type``, ``Content-Encoding``, ``Content-Disposition``, and
``X-Delete-At`` system metadata items. However you cannot update other
system metadata, such as ``Content-Length`` or ``Last-Modified``.

You can use COPY as an alternate to the POST operation by copying
to the same object. With the POST operation you must specify all
metadata items, whereas with the COPY operation, you need to
specify only changed or additional items.
All metadata is preserved during the object copy. If you specify
metadata on the request to copy the object, either PUT or COPY ,
the metadata overwrites any conflicting keys on the target (new)
object.

.. note::

  While using COPY instead of POST allows sending only a subset of
  the metadata, it carries the cost of reading and rewriting the entire
  contents of the object.

A POST request deletes any existing custom metadata that you added
with a previous PUT or POST request. Consequently, you must specify
all custom metadata in the request. However, system metadata is
unchanged by the POST request unless you explicitly supply it in a
request header.

You can also set the ``X-Delete-At`` or ``X-Delete-After`` header
to define when to expire the object.

When used as described in this section, the POST operation creates
or replaces metadata. This form of the operation has no request
body. There are alternate uses of the POST operation as follows:

- You can also use the `form POST feature
  <http://docs.openstack.org/liberty/config-reference/content/object-
  storage-form-post.html>`_ to upload objects.

- The POST operation when used with the ``bulk-delete`` query parameter
  can be used to delete multiple objects and containers in a single
  operation.

- The POST operation when used with the ``extract-archive`` query parameter
  can be used to upload an archive (tar file). The archive is then extracted
  to create objects.

A POST request must not include X-Symlink-Target header. If it does then a
400 status code is returned and the object metadata is not modified.

When a POST request is sent to a symlink, the metadata will be applied to the
symlink, but the request will result in a ``307 Temporary Redirect`` response
to the client. The POST is never redirected to the target object, thus a
GET/HEAD request to the symlink without ``symlink=get`` will not return the
metadata that was sent as part of the POST request.

Example requests and responses:

- Create object metadata:

  ::

     curl -i $publicURL/marktwain/goodbye -X POST -H "X-Auth-Token: $token" -H "X-Object-Meta-Book: GoodbyeColumbus"




  ::

     HTTP/1.1 202 Accepted
     Content-Length: 76
     Content-Type: text/html; charset=UTF-8
     X-Trans-Id: txb5fb5c91ba1f4f37bb648-0052d84b3f
     X-Openstack-Request-Id: txb5fb5c91ba1f4f37bb648-0052d84b3f
     Date: Thu, 16 Jan 2014 21:12:31 GMT
     <html>
     <h1>Accepted
     </h1>
     <p>The request is accepted for processing.
     </p>
     </html>


- Update object metadata:

  ::

     curl -i $publicURL/marktwain/goodbye -X POST -H "X-Auth-Token: $token" -H "X-Object-Meta-Book: GoodbyeOldFriend"




  ::

     HTTP/1.1 202 Accepted
     Content-Length: 76
     Content-Type: text/html; charset=UTF-8
     X-Trans-Id: tx5ec7ab81cdb34ced887c8-0052d84ca4
     X-Openstack-Request-Id: tx5ec7ab81cdb34ced887c8-0052d84ca4
     Date: Thu, 16 Jan 2014 21:18:28 GMT
     <html>
     <h1>Accepted
     </h1>
     <p>The request is accepted for processing.
     </p>
     </html>

Normal response codes: 202


Request
-------

.. rest_parameters:: parameters.yaml

   - account: account
   - container: container
   - object: object
   - bulk-delete: bulk-delete
   - extract-archive: extract-archive
   - X-Auth-Token: X-Auth-Token
   - X-Service-Token: X-Service-Token
   - X-Object-Meta-name: X-Object-Meta-name
   - X-Delete-At: X-Delete-At
   - X-Delete-After: X-Delete-After
   - Content-Disposition: Content-Disposition
   - Content-Encoding: Content-Encoding
   - Content-Type: Content-Type_obj_cu_req
   - X-Trans-Id-Extra: X-Trans-Id-Extra


Response Parameters
-------------------

.. rest_parameters:: parameters.yaml

   - Date: Date
   - X-Timestamp: X-Timestamp
   - Content-Length: Content-Length_cud_resp
   - Content-Type: Content-Type_cud_resp
   - X-Trans-Id: X-Trans-Id
   - X-Openstack-Request-Id: X-Openstack-Request-Id




