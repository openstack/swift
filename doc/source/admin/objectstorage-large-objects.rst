====================
Large object support
====================

Object Storage (swift) uses segmentation to support the upload of large
objects. By default, Object Storage limits the download size of a single
object to 5GB. Using segmentation, uploading a single object is virtually
unlimited. The segmentation process works by fragmenting the object,
and automatically creating a file that sends the segments together as
a single object. This option offers greater upload speed with the possibility
of parallel uploads.

Large objects
~~~~~~~~~~~~~
The large object is comprised of two types of objects:

-  **Segment objects** store the object content. You can divide your
   content into segments, and upload each segment into its own segment
   object. Segment objects do not have any special features. You create,
   update, download, and delete segment objects just as you would normal
   objects.

-  A **manifest object** links the segment objects into one logical
   large object. When you download a manifest object, Object Storage
   concatenates and returns the contents of the segment objects in the
   response body of the request. The manifest object types are:

   - **Static large objects**
   - **Dynamic large objects**

To find out more information on large object support,
see :doc:`/overview_large_objects` in the developer documentation.
