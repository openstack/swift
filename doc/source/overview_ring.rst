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
structure to a gzipped, serialized file on disk for shipping out to the servers.
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
ip      string   The IP address or hostname of the server containing the device.
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

This is a list of array('H') of devices ids. The outermost list contains an
array('H') for each replica. Each array('H') has a length equal to the
partition count for the ring. Each integer in the array('H') is an index into
the above list of devices. The partition list is known internally to the Ring
class as _replica2part2dev_id.

So, to create a list of device dictionaries assigned to a partition, the Python
code would look like: ``devices = [self.devs[part2dev_id[partition]] for
part2dev_id in self._replica2part2dev_id]``

That code is a little simplistic, as it does not account for the
removal of duplicate devices. If a ring has more replicas than
devices, then a partition will have more than one replica on one
device; that's simply the pigeonhole principle at work.

array('H') is used for memory conservation as there may be millions of
partitions.

*******************
Fractional Replicas
*******************

A ring is not restricted to having an integer number of replicas. In order to
support the gradual changing of replica counts, the ring is able to have a real
number of replicas.

When the number of replicas is not an integer, then the last element of
_replica2part2dev_id will have a length that is less than the partition count
for the ring. This means that some partitions will have more replicas than
others. For example, if a ring has 3.25 replicas, then 25% of its partitions
will have four replicas, while the remaining 75% will have just three.

********
Overload
********

The ring builder tries to keep replicas as far apart as possible while
still respecting device weights. When it can't do both, the overload
factor determines what happens. Each device will take some extra
fraction of its desired partitions to allow for replica dispersion;
once that extra fraction is exhausted, replicas will be placed closer
together than optimal.

Essentially, the overload factor lets the operator trade off replica
dispersion (durability) against data dispersion (uniform disk usage).

The default overload factor is 0, so device weights will be strictly
followed.

With an overload factor of 0.1, each device will accept 10% more
partitions than it otherwise would, but only if needed to maintain
partition dispersion.

Example: Consider a 3-node cluster of machines with equal-size disks;
let node A have 12 disks, node B have 12 disks, and node C have only
11 disks. Let the ring have an overload factor of 0.1 (10%).

Without the overload, some partitions would end up with replicas only
on nodes A and B. However, with the overload, every device is willing
to accept up to 10% more partitions for the sake of dispersion. The
missing disk in C means there is one disk's worth of partitions that
would like to spread across the remaining 11 disks, which gives each
disk in C an extra 9.09% load. Since this is less than the 10%
overload, there is one replica of each partition on each node.

However, this does mean that the disks in node C will have more data
on them than the disks in nodes A and B. If 80% full is the warning
threshold for the cluster, node C's disks will reach 80% full while A
and B's disks are only 72.7% full.

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

For a ring generated with part_power P, the partition shift value is
32 - P.

-----------------
Building the Ring
-----------------

The initial building of the ring first calculates the number of partitions that
should ideally be assigned to each device based the device's weight. For
example, given a partition power of 20, the ring will have 1,048,576 partitions.
If there are 1,000 devices of equal weight they will each desire 1,048.576
partitions. The devices are then sorted by the number of partitions they desire
and kept in order throughout the initialization process.

Note: each device is also assigned a random tiebreaker value that is used when
two devices desire the same number of partitions. This tiebreaker is not stored
on disk anywhere, and so two different rings created with the same parameters
will have different partition assignments. For repeatable partition assignments,
``RingBuilder.rebalance()`` takes an optional seed value that will be used to
seed Python's pseudo-random number generator.

Then, the ring builder assigns each replica of each partition to the device that
desires the most partitions at that point while keeping it as far away as
possible from other replicas. The ring builder prefers to assign a replica to a
device in a regions that has no replicas already; should there be no such region
available, the ring builder will try to find a device in a different zone; if
not possible, it will look on a different server; failing that, it will just
look for a device that has no replicas; finally, if all other options are
exhausted, the ring builder will assign the replica to the device that has the
fewest replicas already assigned. Note that assignment of multiple replicas to
one device will only happen if the ring has fewer devices than it has replicas.

When building a new ring based on an old ring, the desired number of partitions
each device wants is recalculated. Next the partitions to be reassigned are
gathered up. Any removed devices have all their assigned partitions unassigned
and added to the gathered list. Any partition replicas that (due to the
addition of new devices) can be spread out for better durability are unassigned
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
device failures). Using the additional memory was deemed a good trade-off for
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

The placement algorithm has seen a number of behavioral changes for
unbalanceable rings. The ring builder wants to keep replicas as far
apart as possible while still respecting device weights. In most
cases, the ring builder can achieve both, but sometimes they conflict.
At first, the behavior was to keep the replicas far apart and ignore
device weight, but that made it impossible to gradually go from one
region to two, or from two to three. Then it was changed to favor
device weight over dispersion, but that wasn't so good for rings that
were close to balanceable, like 3 machines with 60TB, 60TB, and 57TB
of disk space; operators were expecting one replica per machine, but
didn't always get it. After that, overload was added to the ring
builder so that operators could choose a balance between dispersion
and device weights.
