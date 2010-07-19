=========
The Rings
=========

The rings determine where data should reside in the cluster. There is a
separate ring for account databases, container databases, and individual
objects but each ring works in the same way. These rings are externally
managed, in that the server processes themselves do not modify the rings, they
are instead given new rings modified by other tools.

The ring uses a configurable number of bits from a path's MD5 hash as a
partition index that designates a device. The number of bits kept from the hash
is known as the partition power, and 2 to the partition power indicates the
partition count. Partitioning the full MD5 hash ring allows other parts of the
cluster to work in batches of items at once which ends up either more efficient
or at least less complex than working with each item separately or the entire
cluster all at once.

Another configurable value is the replica count, which indicates how many of
the partition->device assignments comprise a single ring. For a given partition
number, each replica's device will not be in the same zone as any other
replica's device. Zones can be used to group devices based on physical
locations, power separations, network separations, or any other attribute that
would lessen multiple replicas being unavailable at the same time.

------------
Ring Builder
------------

The rings are built and managed manually by a utility called the ring-builder.
The ring-builder assigns partitions to devices and writes an optimized Python
structure to a gzipped, pickled file on disk for shipping out to the servers.
The server processes just check the modification time of the file occasionally
and reload their in-memory copies of the ring structure as needed. Because of
how the ring-builder manages changes to the ring, using a slightly older ring
usually just means one of the three replicas for a subset of the partitions
will be incorrect, which can be easily worked around.

The ring-builder also keeps its own builder file with the ring information and
additional data required to build future rings. It is very important to keep
multiple backup copies of these builder files. One option is to copy the
builder files out to every server while copying the ring files themselves.
Another is to upload the builder files into the cluster itself. Complete loss
of a builder file will mean creating a new ring from scratch, nearly all
partitions will end up assigned to different devices, and therefore nearly all
data stored will have to be replicated to new locations. So, recovery from a
builder file loss is possible, but data will definitely be unreachable for an
extended time.

-------------------
Ring Data Structure
-------------------

The ring data structure consists of three top level fields: a list of devices
in the cluster, a list of lists of device ids indicating partition to device
assignments, and an integer indicating the number of bits to shift an MD5 hash
to calculate the partition for the hash.

***************
List of Devices
***************

The list of devices is known internally to the Ring class as devs. Each item in
the list of devices is a dictionary with the following keys:

======  =======  ==============================================================
id      integer  The index into the list devices.
zone    integer  The zone the devices resides in.
weight  float    The relative weight of the device in comparison to other
                 devices. This usually corresponds directly to the amount of
                 disk space the device has compared to other devices. For
                 instance a device with 1 terabyte of space might have a weight
                 of 100.0 and another device with 2 terabytes of space might
                 have a weight of 200.0. This weight can also be used to bring
                 back into balance a device that has ended up with more or less
                 data than desired over time. A good average weight of 100.0
                 allows flexibility in lowering the weight later if necessary.
ip      string   The IP address of the server containing the device.
port    int      The TCP port the listening server process uses that serves
                 requests for the device.
device  string   The on disk name of the device on the server.
                 For example: sdb1
meta    string   A general-use field for storing additional information for the
                 device. This information isn't used directly by the server
                 processes, but can be useful in debugging. For example, the
                 date and time of installation and hardware manufacturer could
                 be stored here.
======  =======  ==============================================================

Note: The list of devices may contain holes, or indexes set to None, for
devices that have been removed from the cluster. Generally, device ids are not
reused. Also, some devices may be temporarily disabled by setting their weight
to 0.0. To obtain a list of active devices (for uptime polling, for example)
the Python code would look like: ``devices = [device for device in self.devs if
device and device['weight']]``

*************************
Partition Assignment List
*************************

This is a list of array('I') of devices ids. The outermost list contains an
array('I') for each replica. Each array('I') has a length equal to the
partition count for the ring. Each integer in the array('I') is an index into
the above list of devices. The partition list is known internally to the Ring
class as _replica2part2dev_id.

So, to create a list of device dictionaries assigned to a partition, the Python
code would look like: ``devices = [self.devs[part2dev_id[partition]] for
part2dev_id in self._replica2part2dev_id]``

array('I') is used for memory conservation as there may be millions of
partitions.

*********************
Partition Shift Value
*********************

The partition shift value is known internally to the Ring class as _part_shift.
This value used to shift an MD5 hash to calculate the partition on which the
data for that hash should reside. Only the top four bytes of the hash is used
in this process. For example, to compute the partition for the path
/account/container/object the Python code might look like: ``partition =
unpack_from('>I', md5('/account/container/object').digest())[0] >>
self._part_shift``

-----------------
Building the Ring
-----------------

The initial building of the ring first calculates the number of partitions that
should ideally be assigned to each device based the device's weight. For
example, if the partition power of 20 the ring will have 1,048,576 partitions.
If there are 1,000 devices of equal weight they will each desire 1,048.576
partitions. The devices are then sorted by the number of partitions they desire
and kept in order throughout the initialization process.

Then, the ring builder assigns each partition's replica to the device that
desires the most partitions at that point, with the restriction that the device
is not in the same zone as any other replica for that partition. Once assigned,
the device's desired partition count is decremented and moved to its new sorted
location in the list of devices and the process continues.

When building a new ring based on an old ring, the desired number of partitions
each device wants is recalculated. Next the partitions to be reassigned are
gathered up. Any removed devices have all their assigned partitions unassigned
and added to the gathered list. Any devices that have more partitions than they
now desire have random partitions unassigned from them and added to the
gathered list. Lastly, the gathered partitions are then reassigned to devices
using a similar method as in the initial assignment described above.

Whenever a partition has a replica reassigned, the time of the reassignment is
recorded. This is taken into account when gathering partitions to reassign so
that no partition is moved twice in a configurable amount of time. This
configurable amount of time is known internally to the RingBuilder class as
min_part_hours. This restriction is ignored for replicas of partitions on
devices that have been removed, as removing a device only happens on device
failure and there's no choice but to make a reassignment.

The above processes don't always perfectly rebalance a ring due to the random
nature of gathering partitions for reassignment. To help reach a more balanced
ring, the rebalance process is repeated until near perfect (less 1% off) or
when the balance doesn't improve by at least 1% (indicating we probably can't
get perfect balance due to wildly imbalanced zones or too many partitions
recently moved).

-------
History
-------

The ring code went through many iterations before arriving at what it is now
and while it has been stable for a while now, the algorithm may be tweaked or
perhaps even fundamentally changed if new ideas emerge. This section will try
to describe the previous ideas attempted and attempt to explain why they were
discarded.

A "live ring" option was considered where each server could maintain its own
copy of the ring and the servers would use a gossip protocol to communicate the
changes they made. This was discarded as too complex and error prone to code
correctly in the project time span available. One bug could easily gossip bad
data out to the entire cluster and be difficult to recover from. Having an
externally managed ring simplifies the process, allows full validation of data
before it's shipped out to the servers, and guarantees each server is using a
ring from the same timeline. It also means that the servers themselves aren't
spending a lot of resources maintaining rings.

A couple of "ring server" options were considered. One was where all ring
lookups would be done by calling a service on a separate server or set of
servers, but this was discarded due to the latency involved. Another was much
like the current process but where servers could submit change requests to the
ring server to have a new ring built and shipped back out to the servers. This
was discarded due to project time constraints and because ring changes are
currently infrequent enough that manual control was sufficient. However, lack
of quick automatic ring changes did mean that other parts of the system had to
be coded to handle devices being unavailable for a period of hours until
someone could manually update the ring.

The current ring process has each replica of a partition independently assigned
to a device. A version of the ring that used a third of the memory was tried,
where the first replica of a partition was directly assigned and the other two
were determined by "walking" the ring until finding additional devices in other
zones. This was discarded as control was lost as to how many replicas for a
given partition moved at once. Keeping each replica independent allows for
moving only one partition replica within a given time window (except due to
device failures). Using the additional memory was deemed a good tradeoff for
moving data around the cluster much less often.

Another ring design was tried where the partition to device assignments weren't
stored in a big list in memory but instead each device was assigned a set of
hashes, or anchors. The partition would be determined from the data item's hash
and the nearest device anchors would determine where the replicas should be
stored. However, to get reasonable distribution of data each device had to have
a lot of anchors and walking through those anchors to find replicas started to
add up. In the end, the memory savings wasn't that great and more processing
power was used, so the idea was discarded.

A completely non-partitioned ring was also tried but discarded as the
partitioning helps many other parts of the system, especially replication.
Replication can be attempted and retried in a partition batch with the other
replicas rather than each data item independently attempted and retried. Hashes
of directory structures can be calculated and compared with other replicas to
reduce directory walking and network traffic.

Partitioning and independently assigning partition replicas also allowed for
the best balanced cluster. The best of the other strategies tended to give
+-10% variance on device balance with devices of equal weight and +-15% with
devices of varying weights. The current strategy allows us to get +-3% and +-8%
respectively.

Various hashing algorithms were tried. SHA offers better security, but the ring
doesn't need to be cryptographically secure and SHA is slower. Murmur was much
faster, but MD5 was built-in and hash computation is a small percentage of the
overall request handling time. In all, once it was decided the servers wouldn't
be maintaining the rings themselves anyway and only doing hash lookups, MD5 was
chosen for its general availability, good distribution, and adequate speed.
