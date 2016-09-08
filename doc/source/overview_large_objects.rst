.. _large-objects:

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

.. _dynamic-large-objects:

.. _dlo-doc:

---------------------
Dynamic Large Objects
---------------------

.. automodule:: swift.common.middleware.dlo
    :members:
    :show-inheritance:

.. _static-large-objects:

.. _slo-doc:

--------------------
Static Large Objects
--------------------

.. automodule:: swift.common.middleware.slo
    :members:
    :show-inheritance:

----------
Direct API
----------

SLO support centers around the user generated manifest file. After the user
has uploaded the segments into their account a manifest file needs to be
built and uploaded. All object segments, must be at least 1 byte
in size. Please see the SLO docs for :ref:`slo-doc` further
details.

----------------
Additional Notes
----------------

* With a ``GET`` or ``HEAD`` of a manifest file, the ``X-Object-Manifest:
  <container>/<prefix>`` header will be returned with the concatenated object
  so you can tell where it's getting its segments from.

* When updating a manifest object using a POST request, a
  ``X-Object-Manifest`` header must be included for the object to
  continue to behave as a manifest object.

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
  in the manifest (for DLO, from the listing ``<container>/<prefix>``).
  Usually in Swift the ETag is the MD5 sum of the contents of the object, and
  that holds true for each segment independently. But it's not meaningful to
  generate such an ETag for the manifest itself so this method was chosen to
  at least offer change detection.


.. note::

    If you are using the container sync feature you will need to ensure both
    your manifest file and your segment files are synced if they happen to be
    in different containers.

-------
History
-------

Dynamic large object support has gone through various iterations before
settling on this implementation.

The primary factor driving the limitation of object size in Swift is
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

To meet an many use cases as possible Swift supports two types of large
object manifests. Dynamic and static large object manifests both support
the same idea of allowing the user to upload many segments to be later
downloaded as a single file.

Dynamic large objects rely on a container listing to provide the manifest.
This has the advantage of allowing the user to add/removes segments from the
manifest at any time. It has the disadvantage of relying on eventually
consistent container listings. All three copies of the container dbs must
be updated for a complete list to be guaranteed. Also, all segments must
be in a single container, which can limit concurrent upload speed.

Static large objects rely on a user provided manifest file. A user can
upload objects into multiple containers and then reference those objects
(segments) in a self generated manifest file. Future GETs to that file will
download the concatenation of the specified segments. This has the advantage of
being able to immediately download the complete object once the manifest has
been successfully PUT. Being able to upload segments into separate containers
also improves concurrent upload speed. It has the disadvantage that the
manifest is finalized once PUT. Any changes to it means it has to be replaced.

Between these two methods the user has great flexibility in how (s)he chooses
to upload and retrieve large objects to Swift. Swift does not, however, stop
the user from harming themselves. In both cases the segments are deletable by
the user at any time. If a segment was deleted by mistake, a dynamic large
object, having no way of knowing it was ever there, would happily ignore the
deleted file and the user will get an incomplete file. A static large object
would, when failing to retrieve the object specified in the manifest, drop the
connection and the user would receive partial results.
