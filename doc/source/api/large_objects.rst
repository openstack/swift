=============
Large objects
=============

By default, the content of an object cannot be greater than 5 GB.
However, you can use a number of smaller objects to construct a large
object. The large object is comprised of two types of objects:

-  **Segment objects** store the object content. You can divide your
   content into segments, and upload each segment into its own segment
   object. Segment objects do not have any special features. You create,
   update, download, and delete segment objects just as you would normal
   objects.

-  A **manifest object** links the segment objects into one logical
   large object. When you download a manifest object, Object Storage
   concatenates and returns the contents of the segment objects in the
   response body of the request. This behavior extends to the response
   headers returned by **GET** and **HEAD** requests. The
   ``Content-Length`` response header value is the total size of all
   segment objects. Object Storage calculates the ``ETag`` response
   header value by taking the ``ETag`` value of each segment,
   concatenating them together, and returning the MD5 checksum of the
   result. The manifest object types are:

   **Static large objects**
       The manifest object content is an ordered list of the names of
       the segment objects in JSON format.

   **Dynamic large objects**
       The manifest object has a ``X-Object-Manifest`` metadata header.
       The value of this header is ``{container}/{prefix}``,
       where ``{container}`` is the name of the container where the
       segment objects are stored, and ``{prefix}`` is a string that all
       segment objects have in common. The manifest object should have
       no content. However, this is not enforced.

Note
~~~~

If you make a **COPY** request by using a manifest object as the source,
the new object is a normal, and not a segment, object. If the total size
of the source segment objects exceeds 5 GB, the **COPY** request fails.
However, you can make a duplicate of the manifest object and this new
object can be larger than 5 GB.

Static large objects
~~~~~~~~~~~~~~~~~~~~

To create a static large object, divide your content into pieces and
create (upload) a segment object to contain each piece.

Create a manifest object. Include the ``multipart-manifest=put``
query parameter at the end of the manifest object name to indicate that
this is a manifest object.

The body of the **PUT** request on the manifest object comprises a json
list, where each element is an object representing a segment. These objects
may contain the following attributes:

-  ``path`` (required). The container and object name in the format:
   ``{container-name}/{object-name}``

-  ``etag`` (optional). If provided, this value must match the ``ETag``
   of the segment object. This was included in the response headers when
   the segment was created. Generally, this will be the MD5 sum of the
   segment.

-  ``size_bytes`` (optional). The size of the segment object. If provided,
   this value must match the ``Content-Length`` of that object.

-  ``range`` (optional). The subset of the referenced object that should
   be used for segment data. This behaves similar to the ``Range`` header.
   If omitted, the entire object will be used.

Providing the optional ``etag`` and ``size_bytes`` attributes for each
segment ensures that the upload cannot corrupt your data.

**Example Static large object manifest list**

This example shows three segment objects. You can use several containers
and the object names do not have to conform to a specific pattern, in
contrast to dynamic large objects.

.. code:: json

    [
        {
            "path": "mycontainer/objseg1",
            "etag": "0228c7926b8b642dfb29554cd1f00963",
            "size_bytes": 1468006
        },
        {
            "path": "mycontainer/pseudodir/seg-obj2",
            "etag": "5bfc9ea51a00b790717eeb934fb77b9b",
            "size_bytes": 1572864
        },
        {
            "path": "other-container/seg-final",
            "etag": "b9c3da507d2557c1ddc51f27c54bae51",
            "size_bytes": 256
        }
    ]

|

The ``Content-Length`` request header must contain the length of the
json content—not the length of the segment objects. However, after the
**PUT** operation completes, the ``Content-Length`` metadata is set to
the total length of all the object segments. When using the ``ETag``
request header in a **PUT** operation, it  must contain the MD5 checksum
of the concatenated ``ETag`` values of the object segments. You can also
set the ``Content-Type`` request header and custom object metadata.

When the **PUT** operation sees the ``multipart-manifest=put`` query
parameter, it reads the request body and verifies that each segment
object exists and that the sizes and ETags match. If there is a
mismatch, the **PUT** operation fails.

This verification process can take a long time to complete, particularly
as the number of segments increases. You may include a ``heartbeat=on``
query parameter to have the server:

1. send a ``202 Accepted`` response before it begins validating segments,
2. periodically send whitespace characters to keep the connection alive, and
3. send a final response code in the body.

.. note::
    The server may still immediately respond with ``400 Bad Request``
    if it can determine that the request is invalid before making
    backend requests.

If everything matches, the manifest object is created. The
``X-Static-Large-Object`` metadata is set to ``true`` indicating that
this is a static object manifest.

Normally when you perform a **GET** operation on the manifest object,
the response body contains the concatenated content of the segment
objects. To download the manifest list, use the
``multipart-manifest=get`` query parameter. The resulting list is not
formatted the same as the manifest you originally used in the **PUT**
operation.

If you use the **DELETE** operation on a manifest object, the manifest
object is deleted. The segment objects are not affected. However, if you
add the ``multipart-manifest=delete`` query parameter, the segment
objects are deleted and if all are successfully deleted, the manifest
object is also deleted.

To change the manifest, use a **PUT** operation with the
``multipart-manifest=put`` query parameter. This request creates a
manifest object. You can also update the object metadata in the usual
way.

Dynamic large objects
~~~~~~~~~~~~~~~~~~~~~

You must segment objects that are larger than 5 GB before you can upload
them. You then upload the segment objects like you would any other
object and create a dynamic large manifest object. The manifest object
tells Object Storage how to find the segment objects that comprise the
large object. The segments remain individually addressable, but
retrieving the manifest object streams all the segments concatenated.
There is no limit to the number of segments that can be a part of a
single large object, but ``Content-Length`` is included in **GET** or **HEAD**
response only if the number of segments is smaller than container listing
limit. In other words, the number of segments that fit within a single
container listing page.

To ensure the download works correctly, you must upload all the object
segments to the same container and ensure that each object name is
prefixed in such a way that it sorts in the order in which it should be
concatenated. You also create and upload a manifest file. The manifest
file is a zero-byte file with the extra ``X-Object-Manifest``
``{container}/{prefix}`` header, where ``{container}`` is the container
the object segments are in and ``{prefix}`` is the common prefix for all
the segments. You must UTF-8-encode and then URL-encode the container
and common prefix in the ``X-Object-Manifest`` header.

It is best to upload all the segments first and then create or update
the manifest. With this method, the full object is not available for
downloading until the upload is complete. Also, you can upload a new set
of segments to a second location and update the manifest to point to
this new location. During the upload of the new segments, the original
manifest is still available to download the first set of segments.

.. note::

  When updating a manifest object using a POST request, a
  ``X-Object-Manifest`` header must be included for the
  object to continue to behave as a manifest object.

**Example Upload segment of large object request: HTTP**

.. code:: none

    PUT /{api_version}/{account}/{container}/{object} HTTP/1.1
    Host: storage.clouddrive.com
    X-Auth-Token: eaaafd18-0fed-4b3a-81b4-663c99ec1cbb
    ETag: 8a964ee2a5e88be344f36c22562a6486
    Content-Length: 1
    X-Object-Meta-PIN: 1234


No response body is returned. A status code of 2\ *``nn``* (between 200
and 299, inclusive) indicates a successful write; status 411 Length
Required denotes a missing ``Content-Length`` or ``Content-Type`` header
in the request. If the MD5 checksum of the data written to the storage
system does NOT match the (optionally) supplied ETag value, a 422
Unprocessable Entity response is returned.

You can continue uploading segments like this example shows, prior to
uploading the manifest.

**Example Upload next segment of large object request: HTTP**

.. code:: none

    PUT /{api_version}/{account}/{container}/{object} HTTP/1.1
    Host: storage.clouddrive.com
    X-Auth-Token: eaaafd18-0fed-4b3a-81b4-663c99ec1cbb
    ETag: 8a964ee2a5e88be344f36c22562a6486
    Content-Length: 1
    X-Object-Meta-PIN: 1234


Next, upload the manifest you created that indicates the container the
object segments reside within. Note that uploading additional segments
after the manifest is created causes the concatenated object to be that
much larger but you do not need to recreate the manifest file for
subsequent additional segments.

**Example Upload manifest request: HTTP**

.. code:: none

    PUT /{api_version}/{account}/{container}/{object} HTTP/1.1
    Host: storage.clouddrive.com
    X-Auth-Token: eaaafd18-0fed-4b3a-81b4-663c99ec1cbb
    Content-Length: 0
    X-Object-Meta-PIN: 1234
    X-Object-Manifest: {container}/{prefix}


**Example Upload manifest response: HTTP**

.. code:: none

    [...]


The ``Content-Type`` in the response for a **GET** or **HEAD** on the
manifest is the same as the ``Content-Type`` set during the **PUT**
request that created the manifest. You can easily change the
``Content-Type`` by reissuing the **PUT** request.

Comparison of static and dynamic large objects
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

While static and dynamic objects have similar behavior, here are
their differences:

End-to-end integrity
--------------------

With static large objects, integrity can be assured.
The list of segments may include the MD5 checksum (``ETag``) of each segment.
You cannot upload the manifest object if the ``ETag`` in the list differs
from the uploaded segment object. If a segment is somehow lost, an attempt
to download the manifest object results in an error.

With dynamic large objects, integrity is not guaranteed. The eventual
consistency model means that although you have uploaded a segment object, it
might not appear in the container listing until later. If you download the
manifest before it appears in the container, it does not form part of the
content returned in response to a **GET** request.

Upload Order
------------

With static large objects, you must upload the
segment objects before you upload the manifest object.

With dynamic large objects, you can upload manifest and segment objects
in any order. In case a premature download of the manifest occurs, we
recommend users upload the manifest object after the segments. However,
the system does not enforce the order.

Removal or addition of segment objects
--------------------------------------

With static large objects, you cannot add or
remove segment objects from the manifest. However, you can create a
completely new manifest object of the same name with a different manifest
list.

With dynamic large objects, you can upload new segment objects or remove
existing segments. The names must simply match the ``{prefix}`` supplied
in ``X-Object-Manifest``.

Segment object size and number
------------------------------

With static large objects, the segment objects must be at least 1 byte in size.
However, if the segment objects are less than 1MB (by default),
the SLO download is (by default) rate limited. At most,
1000 segments are supported (by default) and the manifest has a limit
(by default) of 2MB in size.

With dynamic large objects, segment objects can be any size.

Segment object container name
-----------------------------

With static large objects, the manifest list includes the container name of each object.
Segment objects can be in different containers.

With dynamic large objects, all segment objects must be in the same container.

Manifest object metadata
------------------------

With static large objects, the manifest object has ``X-Static-Large-Object``
set to ``true``. You do not set this
metadata directly. Instead the system sets it when you **PUT** a static
manifest object.

With dynamic large objects, the ``X-Object-Manifest`` value is the
``{container}/{prefix}``, which indicates
where the segment objects are located. You supply this request header in the
**PUT** operation.

Copying the manifest object
---------------------------

The semantics are the same for both static and dynamic large objects.
When copying large objects, the **COPY** operation does not create
a manifest object but a normal object with content same as what you would
get on a **GET** request to the original manifest object.

To copy the manifest object, you include the ``multipart-manifest=get``
query parameter in the **COPY**  request. The new object contains the same
manifest as the original. The segment objects are not copied. Instead,
both the original and new manifest objects share the same set of segment
objects.


