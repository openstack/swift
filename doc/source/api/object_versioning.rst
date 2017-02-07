=================
Object versioning
=================

You can store multiple versions of your content so that you can recover
from unintended overwrites. Object versioning is an easy way to
implement version control, which you can use with any type of content.

.. note::
    You cannot version a large-object manifest file, but the large-object
    manifest file can point to versioned segments.

.. note::
    It is strongly recommended that you put non-current objects in a
    different container than the container where current object versions
    reside.

To allow object versioning within a cluster, the cloud provider should add the
``versioned_writes`` filter to the pipeline and set the
``allow_versioned_writes`` option to ``true`` in the
``[filter:versioned_writes]`` section of the proxy-server configuration file.

To enable object versioning for a container, you must specify an "archive
container" that will retain non-current versions via either the
``X-Versions-Location`` or ``X-History-Location`` header. These two headers
enable two distinct modes of operation. Either mode may be used within a
cluster, but only one mode may be active for any given container. You must
UTF-8-encode and then URL-encode the container name before you include it in
the header.

For both modes, **PUT** requests will archive any pre-existing objects before
writing new data, and **GET** requests will serve the current version. **COPY**
requests behave like a **GET** followed by a **PUT**; that is, if the copy
*source* is in a versioned container then the current version will be copied,
and if the copy *destination* is in a versioned container then any pre-existing
object will be archived before writing new data.

If object versioning was enabled using ``X-History-Location``, then object
**DELETE** requests will copy the current version to the archive container then
remove it from the versioned container.

If object versioning was enabled using ``X-Versions-Location``, then object
**DELETE** requests will restore the most-recent version from the archive
container, overwriting the current version.

Example Using ``X-Versions-Location``
-------------------------------------

#.   Create the ``current`` container:

   .. code::

       # curl -i $publicURL/current -X PUT -H "Content-Length: 0" -H "X-Auth-Token: $token" -H "X-Versions-Location: archive"

   .. code::

       HTTP/1.1 201 Created
       Content-Length: 0
       Content-Type: text/html; charset=UTF-8
       X-Trans-Id: txb91810fb717347d09eec8-0052e18997
       X-Openstack-Request-Id: txb91810fb717347d09eec8-0052e18997
       Date: Thu, 23 Jan 2014 21:28:55 GMT

#. Create the first version of an object in the ``current`` container:

   .. code::

       # curl -i $publicURL/current/my_object --data-binary 1 -X PUT -H "Content-Length: 0" -H "X-Auth-Token: $token"

   .. code::

       HTTP/1.1 201 Created
       Last-Modified: Thu, 23 Jan 2014 21:31:22 GMT
       Content-Length: 0
       Etag: d41d8cd98f00b204e9800998ecf8427e
       Content-Type: text/html; charset=UTF-8
       X-Trans-Id: tx5992d536a4bd4fec973aa-0052e18a2a
       X-Openstack-Request-Id: tx5992d536a4bd4fec973aa-0052e18a2a
       Date: Thu, 23 Jan 2014 21:31:22 GMT

   Nothing is written to the non-current version container when you
   initially **PUT** an object in the ``current`` container. However,
   subsequent **PUT** requests that edit an object trigger the creation
   of a version of that object in the ``archive`` container.

   These non-current versions are named as follows:

   .. code::

       <length><object_name>/<timestamp>

   Where ``length`` is the 3-character, zero-padded hexadecimal
   character length of the object, ``<object_name>`` is the object name,
   and ``<timestamp>`` is the time when the object was initially created
   as a current version.

#. Create a second version of the object in the ``current`` container:

   .. code::

       # curl -i $publicURL/current/my_object --data-binary 2 -X PUT -H "Content-Length: 0" -H "X-Auth-Token: $token"

   .. code::

       HTTP/1.1 201 Created
       Last-Modified: Thu, 23 Jan 2014 21:41:32 GMT
       Content-Length: 0
       Etag: d41d8cd98f00b204e9800998ecf8427e
       Content-Type: text/html; charset=UTF-8
       X-Trans-Id: tx468287ce4fc94eada96ec-0052e18c8c
       X-Openstack-Request-Id: tx468287ce4fc94eada96ec-0052e18c8c
       Date: Thu, 23 Jan 2014 21:41:32 GMT

#. Issue a **GET** request to a versioned object to get the current
   version of the object. You do not have to do any request redirects or
   metadata lookups.

   List older versions of the object in the ``archive`` container:

   .. code::

       # curl -i $publicURL/archive?prefix=009my_object -X GET -H "X-Auth-Token: $token"

   .. code::

       HTTP/1.1 200 OK
       Content-Length: 30
       X-Container-Object-Count: 1
       Accept-Ranges: bytes
       X-Timestamp: 1390513280.79684
       X-Container-Bytes-Used: 0
       Content-Type: text/plain; charset=utf-8
       X-Trans-Id: tx9a441884997542d3a5868-0052e18d8e
       X-Openstack-Request-Id: tx9a441884997542d3a5868-0052e18d8e
       Date: Thu, 23 Jan 2014 21:45:50 GMT

       009my_object/1390512682.92052

   .. note::
      A **POST** request to a versioned object updates only the metadata
      for the object and does not create a new version of the object. New
      versions are created only when the content of the object changes.

#. Issue a **DELETE** request to a versioned object to remove the
   current version of the object and replace it with the next-most
   current version in the non-current container.

   .. code::

       # curl -i $publicURL/current/my_object -X DELETE -H "X-Auth-Token: $token"

   .. code::

       HTTP/1.1 204 No Content
       Content-Length: 0
       Content-Type: text/html; charset=UTF-8
       X-Trans-Id: tx006d944e02494e229b8ee-0052e18edd
       X-Openstack-Request-Id: tx006d944e02494e229b8ee-0052e18edd
       Date: Thu, 23 Jan 2014 21:51:25 GMT

   List objects in the ``archive`` container to show that the archived
   object was moved back to the ``current`` container:

   .. code::

       # curl -i $publicURL/archive?prefix=009my_object -X GET -H "X-Auth-Token: $token"

   .. code::

       HTTP/1.1 204 No Content
       Content-Length: 0
       X-Container-Object-Count: 0
       Accept-Ranges: bytes
       X-Timestamp: 1390513280.79684
       X-Container-Bytes-Used: 0
       Content-Type: text/html; charset=UTF-8
       X-Trans-Id: tx044f2a05f56f4997af737-0052e18eed
       X-Openstack-Request-Id: tx044f2a05f56f4997af737-0052e18eed
       Date: Thu, 23 Jan 2014 21:51:41 GMT

   This next-most current version carries with it any metadata last set
   on it. If want to completely remove an object and you have five
   versions of it, you must **DELETE** it five times.

Example Using ``X-History-Location``
------------------------------------

#.   Create the ``current`` container:

   .. code::

       # curl -i $publicURL/current -X PUT -H "Content-Length: 0" -H "X-Auth-Token: $token" -H "X-History-Location: archive"

   .. code::

       HTTP/1.1 201 Created
       Content-Length: 0
       Content-Type: text/html; charset=UTF-8
       X-Trans-Id: txb91810fb717347d09eec8-0052e18997
       X-Openstack-Request-Id: txb91810fb717347d09eec8-0052e18997
       Date: Thu, 23 Jan 2014 21:28:55 GMT

#. Create the first version of an object in the ``current`` container:

   .. code::

       # curl -i $publicURL/current/my_object --data-binary 1 -X PUT -H "Content-Length: 0" -H "X-Auth-Token: $token"

   .. code::

       HTTP/1.1 201 Created
       Last-Modified: Thu, 23 Jan 2014 21:31:22 GMT
       Content-Length: 0
       Etag: d41d8cd98f00b204e9800998ecf8427e
       Content-Type: text/html; charset=UTF-8
       X-Trans-Id: tx5992d536a4bd4fec973aa-0052e18a2a
       X-Openstack-Request-Id: tx5992d536a4bd4fec973aa-0052e18a2a
       Date: Thu, 23 Jan 2014 21:31:22 GMT

   Nothing is written to the non-current version container when you
   initially **PUT** an object in the ``current`` container. However,
   subsequent **PUT** requests that edit an object trigger the creation
   of a version of that object in the ``archive`` container.

   These non-current versions are named as follows:

   .. code::

       <length><object_name>/<timestamp>

   Where ``length`` is the 3-character, zero-padded hexadecimal
   character length of the object, ``<object_name>`` is the object name,
   and ``<timestamp>`` is the time when the object was initially created
   as a current version.

#. Create a second version of the object in the ``current`` container:

   .. code::

       # curl -i $publicURL/current/my_object --data-binary 2 -X PUT -H "Content-Length: 0" -H "X-Auth-Token: $token"

   .. code::

       HTTP/1.1 201 Created
       Last-Modified: Thu, 23 Jan 2014 21:41:32 GMT
       Content-Length: 0
       Etag: d41d8cd98f00b204e9800998ecf8427e
       Content-Type: text/html; charset=UTF-8
       X-Trans-Id: tx468287ce4fc94eada96ec-0052e18c8c
       X-Openstack-Request-Id: tx468287ce4fc94eada96ec-0052e18c8c
       Date: Thu, 23 Jan 2014 21:41:32 GMT

#. Issue a **GET** request to a versioned object to get the current
   version of the object. You do not have to do any request redirects or
   metadata lookups.

   List older versions of the object in the ``archive`` container:

   .. code::

       # curl -i $publicURL/archive?prefix=009my_object -X GET -H "X-Auth-Token: $token"

   .. code::

       HTTP/1.1 200 OK
       Content-Length: 30
       X-Container-Object-Count: 1
       Accept-Ranges: bytes
       X-Timestamp: 1390513280.79684
       X-Container-Bytes-Used: 0
       Content-Type: text/plain; charset=utf-8
       X-Trans-Id: tx9a441884997542d3a5868-0052e18d8e
       X-Openstack-Request-Id: tx9a441884997542d3a5868-0052e18d8e
       Date: Thu, 23 Jan 2014 21:45:50 GMT

       009my_object/1390512682.92052

   .. note::
      A **POST** request to a versioned object updates only the metadata
      for the object and does not create a new version of the object. New
      versions are created only when the content of the object changes.

#. Issue a **DELETE** request to a versioned object to copy the
   current version of the object to the archive container then delete it from
   the current container. Subsequent **GET** requests to the object in the
   current container will return ``404 Not Found``.

   .. code::

       # curl -i $publicURL/current/my_object -X DELETE -H "X-Auth-Token: $token"

   .. code::

       HTTP/1.1 204 No Content
       Content-Length: 0
       Content-Type: text/html; charset=UTF-8
       X-Trans-Id: tx006d944e02494e229b8ee-0052e18edd
       X-Openstack-Request-Id: tx006d944e02494e229b8ee-0052e18edd
       Date: Thu, 23 Jan 2014 21:51:25 GMT

   List older versions of the object in the ``archive`` container::

   .. code::

       # curl -i $publicURL/archive?prefix=009my_object -X GET -H "X-Auth-Token: $token"

   .. code::

       HTTP/1.1 200 OK
       Content-Length: 90
       X-Container-Object-Count: 3
       Accept-Ranges: bytes
       X-Timestamp: 1390513280.79684
       X-Container-Bytes-Used: 0
       Content-Type: text/html; charset=UTF-8
       X-Trans-Id: tx044f2a05f56f4997af737-0052e18eed
       X-Openstack-Request-Id: tx044f2a05f56f4997af737-0052e18eed
       Date: Thu, 23 Jan 2014 21:51:41 GMT

       009my_object/1390512682.92052
       009my_object/1390512692.23062
       009my_object/1390513885.67732

   In addition to the two previous versions of the object, the archive
   container has a "delete marker" to record when the object was deleted.

   To permanently delete a previous version, issue a **DELETE** to the version
   in the archive container.

Disabling Object Versioning
---------------------------

To disable object versioning for the ``current`` container, remove
its ``X-Versions-Location`` metadata header by sending an empty key
value.

.. code::

    # curl -i $publicURL/current -X PUT -H "Content-Length: 0" -H "X-Auth-Token: $token" -H "X-Versions-Location: "

.. code::

    HTTP/1.1 202 Accepted
    Content-Length: 76
    Content-Type: text/html; charset=UTF-8
    X-Trans-Id: txe2476de217134549996d0-0052e19038
    X-Openstack-Request-Id: txe2476de217134549996d0-0052e19038
    Date: Thu, 23 Jan 2014 21:57:12 GMT

    <html><h1>Accepted</h1><p>The request is accepted for processing.</p></html>

