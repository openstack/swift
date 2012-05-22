====================
Large Object Support
====================

--------
Overview
--------

Swift has a limit on the size of a single uploaded object; by default this is
5GB. However, the download size of a single object is virtually unlimited with
the concept of segmentation. Segments of the larger object are uploaded and a
special manifest file is created that, when downloaded, sends all the segments
concatenated as a single object. This also offers much greater upload speed
with the possibility of parallel uploads of the segments.

-------------------------------------
Using ``swift`` for Segmented Objects
-------------------------------------

The quickest way to try out this feature is use the ``swift`` Swift Tool
included with the `python-swiftclient`_ library.  You can use the ``-S``
option to specify the segment size to use when splitting a large file. For
example::

    swift upload test_container -S 1073741824 large_file

This would split the large_file into 1G segments and begin uploading those
segments in parallel. Once all the segments have been uploaded, ``swift`` will
then create the manifest file so the segments can be downloaded as one.

So now, the following ``swift`` command would download the entire large object::

    swift download test_container large_file

``swift`` uses a strict convention for its segmented object
support. In the above example it will upload all the segments into a
second container named test_container_segments. These segments will
have names like large_file/1290206778.25/21474836480/00000000,
large_file/1290206778.25/21474836480/00000001, etc.

The main benefit for using a separate container is that the main container
listings will not be polluted with all the segment names. The reason for using
the segment name format of <name>/<timestamp>/<size>/<segment> is so that an
upload of a new file with the same name won't overwrite the contents of the
first until the last moment when the manifest file is updated.

``swift`` will manage these segment files for you, deleting old segments on
deletes and overwrites, etc. You can override this behavior with the
``--leave-segments`` option if desired; this is useful if you want to have
multiple versions of the same large object available.

.. _`python-swiftclient`: http://github.com/openstack/python-swiftclient

----------
Direct API
----------

You can also work with the segments and manifests directly with HTTP
requests instead of having ``swift`` do that for you. You can just
upload the segments like you would any other object and the manifest
is just a zero-byte file with an extra ``X-Object-Manifest`` header.

All the object segments need to be in the same container, have a common object
name prefix, and their names sort in the order they should be concatenated.
They don't have to be in the same container as the manifest file will be, which
is useful to keep container listings clean as explained above with ``swift``.

The manifest file is simply a zero-byte file with the extra
``X-Object-Manifest: <container>/<prefix>`` header, where ``<container>`` is
the container the object segments are in and ``<prefix>`` is the common prefix
for all the segments.

It is best to upload all the segments first and then create or update the
manifest. In this way, the full object won't be available for downloading until
the upload is complete. Also, you can upload a new set of segments to a second
location and then update the manifest to point to this new location. During the
upload of the new segments, the original manifest will still be available to
download the first set of segments.

Here's an example using ``curl`` with tiny 1-byte segments::

    # First, upload the segments
    curl -X PUT -H 'X-Auth-Token: <token>' \
        http://<storage_url>/container/myobject/1 --data-binary '1'
    curl -X PUT -H 'X-Auth-Token: <token>' \
        http://<storage_url>/container/myobject/2 --data-binary '2'
    curl -X PUT -H 'X-Auth-Token: <token>' \
        http://<storage_url>/container/myobject/3 --data-binary '3'

    # Next, create the manifest file
    curl -X PUT -H 'X-Auth-Token: <token>' \
        -H 'X-Object-Manifest: container/myobject/' \
        http://<storage_url>/container/myobject --data-binary ''

    # And now we can download the segments as a single object
    curl -H 'X-Auth-Token: <token>' \
        http://<storage_url>/container/myobject

----------------
Additional Notes
----------------

* With a ``GET`` or ``HEAD`` of a manifest file, the ``X-Object-Manifest:
  <container>/<prefix>`` header will be returned with the concatenated object
  so you can tell where it's getting its segments from.

* The response's ``Content-Length`` for a ``GET`` or ``HEAD`` on the manifest
  file will be the sum of all the segments in the ``<container>/<prefix>``
  listing, dynamically. So, uploading additional segments after the manifest is
  created will cause the concatenated object to be that much larger; there's no
  need to recreate the manifest file.

* The response's ``Content-Type`` for a ``GET`` or ``HEAD`` on the manifest
  will be the same as the ``Content-Type`` set during the ``PUT`` request that
  created the manifest. You can easily change the ``Content-Type`` by reissuing
  the ``PUT``.

* The response's ``ETag`` for a ``GET`` or ``HEAD`` on the manifest file will
  be the MD5 sum of the concatenated string of ETags for each of the segments
  in the ``<container>/<prefix>`` listing, dynamically. Usually in Swift the
  ETag is the MD5 sum of the contents of the object, and that holds true for
  each segment independently. But, it's not feasible to generate such an ETag
  for the manifest itself, so this method was chosen to at least offer change
  detection.


.. note::

    If you are using the container sync feature you will need to ensure both
    your manifest file and your segment files are synced if they happen to be
    in different containers.

-------
History
-------

Large object support has gone through various iterations before settling on
this implementation.

The primary factor driving the limitation of object size in swift is
maintaining balance among the partitions of the ring.  To maintain an even
dispersion of disk usage throughout the cluster the obvious storage pattern
was to simply split larger objects into smaller segments, which could then be
glued together during a read.

Before the introduction of large object support some applications were already
splitting their uploads into segments and re-assembling them on the client
side after retrieving the individual pieces.  This design allowed the client
to support backup and archiving of large data sets, but was also frequently
employed to improve performance or reduce errors due to network interruption.
The major disadvantage of this method is that knowledge of the original
partitioning scheme is required to properly reassemble the object, which is
not practical for some use cases, such as CDN origination.

In order to eliminate any barrier to entry for clients wanting to store
objects larger than 5GB, initially we also prototyped fully transparent
support for large object uploads.  A fully transparent implementation would
support a larger max size by automatically splitting objects into segments
during upload within the proxy without any changes to the client API.  All
segments were completely hidden from the client API.

This solution introduced a number of challenging failure conditions into the
cluster, wouldn't provide the client with any option to do parallel uploads,
and had no basis for a resume feature.  The transparent implementation was
deemed just too complex for the benefit.

The current "user manifest" design was chosen in order to provide a
transparent download of large objects to the client and still provide the
uploading client a clean API to support segmented uploads.

Alternative "explicit" user manifest options were discussed which would have
required a pre-defined format for listing the segments to "finalize" the
segmented upload.  While this may offer some potential advantages, it was
decided that pushing an added burden onto the client which could potentially
limit adoption should be avoided in favor of a simpler "API" (essentially just
the format of the 'X-Object-Manifest' header).

During development it was noted that this "implicit" user manifest approach
which is based on the path prefix can be potentially affected by the eventual
consistency window of the container listings, which could theoretically cause
a GET on the manifest object to return an invalid whole object for that short
term.  In reality you're unlikely to encounter this scenario unless you're
running very high concurrency uploads against a small testing environment
which isn't running the object-updaters or container-replicators.

Like all of swift, Large Object Support is living feature which will continue
to improve and may change over time.
