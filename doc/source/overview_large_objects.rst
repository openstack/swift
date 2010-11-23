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

----------------------------------
Using ``st`` for Segmented Objects
----------------------------------

The quickest way to try out this feature is use the included ``st`` Swift Tool.
You can use the ``-S`` option to specify the segment size to use when splitting
a large file. For example::

    st upload test_container -S 1073741824 large_file

This would split the large_file into 1G segments and begin uploading those
segments in parallel. Once all the segments have been uploaded, ``st`` will
then create the manifest file so the segments can be downloaded as one.

So now, the following ``st`` command would download the entire large object::

    st download test_container large_file

``st`` uses a strict convention for its segmented object support. In the above
example it will upload all the segments into a second container named
test_container_segments. These segments will have names like
large_file/1290206778.25/21474836480/00000000,
large_file/1290206778.25/21474836480/00000001, etc.

The main benefit for using a separate container is that the main container
listings will not be polluted with all the segment names. The reason for using
the segment name format of <name>/<timestamp>/<size>/<segment> is so that an
upload of a new file with the same name won't overwrite the contents of the
first until the last moment when the manifest file is updated.

``st`` will manage these segment files for you, deleting old segments on
deletes and overwrites, etc. You can override this behavior with the
``--leave-segments`` option if desired; this is useful if you want to have
multiple versions of the same large object available.

----------
Direct API
----------

You can also work with the segments and manifests directly with HTTP requests
instead of having ``st`` do that for you. You can just upload the segments like
you would any other object and the manifest is just a zero-byte file with an
extra ``X-Object-Manifest`` header.

All the object segments need to be in the same container, have a common object
name prefix, and their names sort in the order they should be concatenated.
They don't have to be in the same container as the manifest file will be, which
is useful to keep container listings clean as explained above with ``st``.

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
