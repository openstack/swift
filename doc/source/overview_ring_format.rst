=================
Ring File Formats
=================

The ring is the most important data structure in Swift. How this data structure
been serialized to disk has changed over the years.

Initially ring files contain three key pieces of information:

* the part_power value (often stored as ``part_shift := 32 - part_power``)

  * which determines how many partitions are in the ring,

* the device list

  * which includes all the disks participating in the ring, and

* the replica-to-part-to-device table

  * which has all ``replica_count * (2 ** part_power)`` partition assignments.

But the ability to extend the serialization format to add more data structures
to the ring serialization format has meant a new ring v2 format has been created.

Ring files have always been gzipped when serialized, though the inner,
raw format has evolved over the years.

Ring v0
-------

Initially, rings were simply pickle dumps of the RingData object. `With
Swift 1.3.0 <https://opendev.org/openstack/swift/commit/fc6391ea>`__, this
changed to pickling a pure-stdlib data structure, but the core concept
was the same.

.. note:

    Swift 2.36.0 dropped support for v0 rings.

Ring v1
-------

Pickle presented some problems, however. While `there are security
concerns <https://docs.python.org/3/library/pickle.html>`__ around unpickling
untrusted data, security boundaries are generally drawn such that rings are
assumed to be trusted. Ultimately, what pushed us to a new format were
`performance considerations <https://bugs.launchpad.net/swift/+bug/1031954>`__.

Starting in `Swift 1.7.0 <https://opendev.org/openstack/swift/commit/f8ce43a2>`__,
Swift began using a new format (while still being willing to read the old one).
The new format starts with some magic so we may identify it as such::

    +---------------+-------+
    |'R' '1' 'N' 'G'| <vrs> |
    +---------------+-------+

where ``<vrs>`` is a network-order two-byte version number (which is always 1).
After that, a JSON object is serialized as::

    +---------------+-------...---+
    | <data-length> | <data ... > |
    +---------------+-------...---+

where ``<data-length>`` is the network-order four-byte length (in bytes) of
``<data>``, which is the ASCII-encoded JSON-serialized object. This object
has at minimum three keys:

* ``devs`` for the device list
* ``part_shift`` (i.e., ``32 - part_power``)
* ``replica_count`` for the integer number of part-to-device rows to read

The replica-to-part-to-device table then follows::

    +-------+-------+...+-------+-------+
    | <dev> | <dev> |...| <dev> | <dev> |
    +-------+-------+...+-------+-------+
    | <dev> | <dev> |...| <dev> | <dev> |
    +-------+-------+...+-------+-------+
    |                ...                |
    +-------+-------+...+-------+-------+
    | <dev> | <dev> |...|
    +-------+-------+...+

Each ``<dev>`` is a host-order two-byte index into the ``devs`` list. Every row
except the last has exactly ``2 ** part_power`` entries; the last row may
have the same or fewer.

The metadata object has proven quite versatile: new keys have been added
to provide additional information while remaining backwards-compatible.
In order, the following new fields have been added:

* ``byteorder`` specifies whether the host-order for the
  replica-to-part-to-device table is "big" or "little" endian. Added in
  `Swift 2.12.0 <https://opendev.org/openstack/swift/commit/1ec6e2bb>`__,
  this allows rings written on big-endian machines to be read on
  little-endian machines and vice-versa.
* ``next_part_power`` indicates whether a partition-power increase is in
  progress. Added in `Swift 2.15.0 <https://opendev.org/openstack/swift/commit/e1140666>`__,
  this will have one of two values, if present: the ring's current
  ``part_power``, indicating that there may be hardlinks to clean up,
  or ``part_power + 1`` indicating that hardlinks may need to be created.
  See :ref:`the documentation<modify_part_power>`
  for more information.
* ``version`` specifies the version number of the ring-builder that was used
  to write this ring. Added in `Swift 2.24.0 <https://opendev.org/openstack/swift/commit/6853616a>`__,
  this allows the comparing of rings from different machines to determine
  which is newer.

Ring v2
-------

The way that v1 rings dealt with fractional replicas made it impossible
to reliably serialize additional large data structures after the
replica-to-part-to-device table. The v2 format has been designed to be
extensable.

The new format starts with magic similar to v1::

    +---------------+-------+
    |'R' '1' 'N' 'G'| <vrs> |
    +---------------+-------+

where <vrs> is again a network-order two-byte version number (which is now 2).
By bumping the version number, we ensure that old versions of Swift refuse to
read the ring, rather than misinterpret the content.

After that, a series of BLOBs are serialized, each as::

    +-------------------------------+-------...---+
    | <data-length>                 | <data ... > |
    +-------------------------------+-------...---+

where ``<data-length>`` is the network-order eight-byte length (in bytes) of
``<data>``. Each BLOB is preceded by a ``Z_FULL_FLUSH`` to allow it to be
decompressed without reading the whole file.

The order of the BLOBs isn't important, although they do tend to be written
in the order Swift will read them while loading. This reduces the disk seeks
necessary to load.

The final BLOB is an index: a JSON object mapping named sections to an array
of offsets within the file, like

.. code::

   {
       section: [
           compressed start,
           uncompressed start,
           compressed end,
           uncompressed end,
           checksum method,
           checksum value
       ],
       ...
   }

Section names may be arbitrary strings, but the "swift/" prefix is reserved
for upstream use. The start/end values mark the beginning and ending of the
section's BLOB. Note that some end values may be ``null`` if they were not
known when the index was written -- in particular, this *will* be true for
the index itself. The checksum method should be one of ``"md5"``, ``"sha1"``,
``"sha256"``, or ``"sha512"``; other values will be ignored in anticipation
of a need to support further algorithms. The checksum value will be the
hex-encoded digest of the uncompressed section's bytes. Like end values,
checksum data may be ``null`` if not known when the index is written.

Finally, a "tail" is written:

* the gzip stream is flushed with another ``Z_FULL_FLUSH``,
* the stream is switched to uncompressed,
* the eight-byte offset of the uncompressed start of the index is written,
* the gzip stream is flushed with another ``Z_FULL_FLUSH``,
* the eight-byte offset of the compressed start of the index is written,
* the gzip stream is flushed with another ``Z_FULL_FLUSH``, and
* the gzip stream is closed; this involves:

  * flushing the underlying deflate stream with ``Z_FINISH``
  * writing ``CRC32`` (of the full uncompressed data)
  * writing ``ISIZE`` (the length of the full uncompressed data ``mod 2 ** 32``)

By switching to uncompressed, we can know exactly how many bytes will be
written in the tail, so that when reading we can quickly seek to and read the
index offset, seek to the index start, and read the index. From there we
can do similar things for any other section.


* Seek to the end of the file
* Go back 31 bytes in the underlying file; this should leave us at the start of
  the deflate block containing the offset for the compressed start
* Decompress 8 bytes from the deflate stream to get the location of the
  compressed start of the index BLOB
* Seek to that location
* Read/decompress the size of the index BLOB
* Read/decompress the json serialized index.

.. note:: This 31 bytes is the deflate block containing the 8 byte location,
   a ``Z_FULL_FLUSH`` block, the ``Z_FINISH`` block, and the ``CRC32`` and
   ``ISIZE``. For more information, see `RFC 1951`_ (for the deflate stream)
   and `RFC 1952`_ (for the gzip format).

The currently defined section and section names upstream are as follows:

* ``swift/index`` - The swift index
* ``swift/ring/metadata`` - Ring metadata serialized as json
* ``swift/ring/devices`` - Devices json serialized data structure.

  * This has been seperated from the ring metadata structure in v1 as it
    gets large

* ``swift/ring/assignments`` - The ring replica2part2dev_id data structure

.. note::
   Third-parties may find it useful to add their own sections; however,
   the ``swift/`` prefix is reserved for future upstream enhancements.

swift/ring/metadata
~~~~~~~~~~~~~~~~~~~
This BLOB is an ASCII-encoded JSON object full of metadata, similar
to v1 rings. It has the following required keys:

* ``part_shift``
* ``dev_id_bytes`` specifies the number of bytes used for each ``<dev>`` in the
  replica-to-part-to-device table; will be one of 2, 4, or 8

Additionally, there are several optional keys which may be present:

* ``next_part_power``
* ``version``

Notice that two keys are no longer present: ``replica_count`` is no longer
needed as the size of the replica-to-part-to-device table is explicit, and
``byteorder`` is not needed as all data in v2 rings should be written using
network-order.

swift/ring/devices
~~~~~~~~~~~~~~~~~~
This BLOB contains a list of swift device dictionarys. And was seperated out
from the metadata BLOB as this can become a large structure in it's own right.

swift/ring/assignments
~~~~~~~~~~~~~~~~~~~~~~
This BLOB is the replica-to-part-to-device table. It's length will be
``replicas * (2 ** part_power) * dev_id_bytes``, where ``replicas`` is the exact
(potentially fractional) replica count for the ring. Unlike in v1, each
``<dev>`` is written using network-order.

Note that this is why we increased the size of ``<data-length>`` as compared to
the v1 format -- otherwise, we may not be able to represent rings with both
high ``replica_count`` and high ``part_power``.

.. _RFC 1952: https://rfc-editor.org/rfc/rfc1952
.. _RFC 1951: https://rfc-editor.org/rfc/rfc1951
