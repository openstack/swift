=============================
Use Content-Encoding metadata
=============================

When you create an object or update its metadata, you can optionally set
the ``Content-Encoding`` metadata. This metadata enables you to indicate
that the object content is compressed without losing the identity of the
underlying media type (``Content-Type``) of the file, such as a video.

**Example Content-Encoding header request: HTTP**

This example assigns an attachment type to the ``Content-Encoding``
header that indicates how the file is downloaded:

.. code::

    PUT /<api version>/<account>/<container>/<object> HTTP/1.1
    Host: storage.clouddrive.com
    X-Auth-Token: eaaafd18-0fed-4b3a-81b4-663c99ec1cbb
    Content-Type: video/mp4
    Content-Encoding: gzip

