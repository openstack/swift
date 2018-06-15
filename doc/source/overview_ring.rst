=========
The Rings
=========

The rings determine where data should reside in the cluster. There is a
separate ring for account databases, container databases, and individual
object storage policies but each ring works in the same way. These rings are
externally managed. The server processes themselves do not modify the
rings; they are instead given new rings modified by other tools.

The ring uses a configurable number of bits from the MD5 hash of an item's path
as a partition index that designates the device(s) on which that item should
be stored. The number of bits kept from the hash is known as the partition
power, and 2 to the partition power indicates the partition count. Partitioning
the full MD5 hash ring allows the cluster components to process resources in
batches. This ends up either more efficient or at least less complex than
working with each item separately or the entire cluster all at once.

Another configurable value is the replica count, which indicates how many
devices to assign for each partition in the ring. By having multiple devices
responsible for each partition, the cluster can recover from drive or network
failures.

Devices are added to the ring to describe the capacity available for
partition replica assignments.  Devices are placed into failure domains
consisting of region, zone, and server.  Regions can be used to describe
geographical systems characterized by lower bandwidth or higher latency between
machines in different regions.  Many rings will consist of only a single
region.  Zones can be used to group devices based on physical locations, power
separations, network separations, or any other attribute that would lessen
multiple replicas being unavailable at the same time.

Devices are given a weight which describes the relative storage capacity
contributed by the device in comparison to other devices.

When building a ring, replicas for each partition will be assigned to devices
according to the devices' weights.  Additionally, each replica of a partition
will preferentially be assigned to a device whose failure domain does not
already have a replica for that partition.  Only a single replica of a
partition may be assigned to each device - you must have at least as many
devices as replicas.

.. _ring_builder:

------------
Ring Builder
------------

The rings are built and managed manually by a utility called the ring-builder.
The ring-builder assigns partitions to devices and writes an optimized
structure to a gzipped, serialized file on disk for shipping out to the
servers. The server processes check the modification time of the file
occasionally and reload their in-memory copies of the ring structure as needed.
Because of how the ring-builder manages changes to the ring, using a slightly
older ring usually just means that for a subset of the partitions the device
for one of the replicas  will be incorrect, which can be easily worked around.

The ring-builder also keeps a separate builder file which includes the ring
information as well as additional data required to build future rings. It is
very important to keep multiple backup copies of these builder files. One
option is to copy the builder files out to every server while copying the ring
files themselves. Another is to upload the builder files into the cluster
itself. Complete loss of a builder file will mean creating a new ring from
scratch, nearly all partitions will end up assigned to different devices, and
therefore nearly all data stored will have to be replicated to new locations.
So, recovery from a builder file loss is possible, but data will definitely be
unreachable for an extended time.

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

The list of devices is known internally to the Ring class as ``devs``. Each
item in the list of devices is a dictionary with the following keys:

======  =======  ==============================================================
id      integer  The index into the list of devices.
zone    integer  The zone in which the device resides.
region  integer  The region in which the zone resides.
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
port    int      The TCP port on which the server process listens to serve
                 requests for the device.
device  string   The on-disk name of the device on the server.
                 For example: ``sdb1``
meta    string   A general-use field for storing additional information for the
                 device. This information isn't used directly by the server
                 processes, but can be useful in debugging. For example, the
                 date and time of installation and hardware manufacturer could
                 be stored here.
======  =======  ==============================================================

.. note::
    The list of devices may contain holes, or indexes set to ``None``, for
    devices that have been removed from the cluster. However, device ids are
    reused. Device ids are reused to avoid potentially running out of device id
    slots when there are available slots (from prior removal of devices). A
    consequence of this device id reuse is that the device id (integer value)
    does not necessarily correspond with the chronology of when the device was
    added to the ring. Also, some devices may be temporarily disabled by
    setting their weight to ``0.0``. To obtain a list of active devices (for
    uptime polling, for example) the Python code would look like::

        devices = list(self._iter_devs())

*************************
Partition Assignment List
*************************

The partition assignment list is known internally to the Ring class as
``_replica2part2dev_id``. This is a list of ``array('H')``\s, one for each
replica. Each ``array('H')`` has a length equal to the partition count for the
ring. Each integer in the ``array('H')`` is an index into the above list of
devices.

So, to create a list of device dictionaries assigned to a partition, the Python
code would look like::

    devices = [self.devs[part2dev_id[partition]]
               for part2dev_id in self._replica2part2dev_id]

``array('H')`` is used for memory conservation as there may be millions of
partitions.

*********************
Partition Shift Value
*********************

The partition shift value is known internally to the Ring class as
``_part_shift``. This value is used to shift an MD5 hash of an item's path to
calculate the partition on which the data for that item should reside. Only the
top four bytes of the hash are used in this process. For example, to compute
the partition for the path ``/account/container/object``, the Python code might
look like::

    objhash = md5('/account/container/object').digest()
    partition = struct.unpack_from('>I', objhash)[0] >> self._part_shift

For a ring generated with partition power ``P``, the partition shift value is
``32 - P``.

*******************
Fractional Replicas
*******************

A ring is not restricted to having an integer number of replicas. In order to
support the gradual changing of replica counts, the ring is able to have a real
number of replicas.

When the number of replicas is not an integer, the last element of
``_replica2part2dev_id`` will have a length that is less than the partition
count for the ring. This means that some partitions will have more replicas
than others. For example, if a ring has ``3.25`` replicas, then 25% of its
partitions will have four replicas, while the remaining 75% will have just
three.

.. _ring_dispersion:

**********
Dispersion
**********

With each rebalance, the ring builder calculates a dispersion metric. This is
the percentage of partitions in the ring that have too many replicas within a
particular failure domain.

For example, if you have three servers in a cluster but two replicas for a
partition get placed onto the same server, that partition will count towards
the dispersion metric.

A lower dispersion value is better, and the value can be used to find the
proper value for "overload".

.. _ring_overload:

********
Overload
********

The ring builder tries to keep replicas as far apart as possible while
still respecting device weights. When it can't do both, the overload
factor determines what happens. Each device may take some extra
fraction of its desired partitions to allow for replica dispersion;
once that extra fraction is exhausted, replicas will be placed closer
together than is optimal for durability.

Essentially, the overload factor lets the operator trade off replica
dispersion (durability) against device balance (uniform disk usage).

The default overload factor is ``0``, so device weights will be strictly
followed.

With an overload factor of ``0.1``, each device will accept 10% more
partitions than it otherwise would, but only if needed to maintain
dispersion.

Example: Consider a 3-node cluster of machines with equal-size disks;
let node A have 12 disks, node B have 12 disks, and node C have only
11 disks. Let the ring have an overload factor of ``0.1`` (10%).

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

-------------------------------
Partition & Replica Terminology
-------------------------------

All descriptions of consistent hashing describe the process of breaking the
keyspace up into multiple ranges (vnodes, buckets, etc.) - many more than the
number of "nodes" to which keys in the keyspace must be assigned.  Swift calls
these ranges `partitions` - they are partitions of the total keyspace.

Each partition will have multiple replicas.  Every replica of each partition
must be assigned to a device in the ring.  When describing a specific replica
of a partition (like when it's assigned a device) it is described as a
`part-replica` in that it is a specific `replica` of the specific `partition`.
A single device will likely be assigned different replicas from many
partitions, but it may not be assigned multiple replicas of a single partition.

The total number of partitions in a ring is calculated as ``2 **
<part-power>``.  The total number of part-replicas in a ring is calculated as
``<replica-count> * 2 ** <part-power>``.

When considering a device's `weight` it is useful to describe the number of
part-replicas it would like to be assigned.  A single device, regardless of
weight, will never hold more than ``2 ** <part-power>`` part-replicas because
it can not have more than one replica of any partition assigned.  The number of
part-replicas a device can take by weights is calculated as its `parts-wanted`.
The true number of part-replicas assigned to a device can be compared to its
parts-wanted similarly to a calculation of percentage error - this deviation in
the observed result from the idealized target is called a device's `balance`.

When considering a device's `failure domain` it is useful to describe the number
of part-replicas it would like to be assigned.  The number of part-replicas
wanted in a failure domain of a tier is the sum of the part-replicas wanted in
the failure domains of its sub-tier.  However, collectively when the total
number of part-replicas in a failure domain exceeds or is equal to ``2 **
<part-power>`` it is most obvious that it's no longer sufficient to consider
only the number of total part-replicas, but rather the fraction of each
replica's partitions.  Consider for example a ring with 3 replicas and 3
servers: while dispersion requires that each server hold only ⅓ of the total
part-replicas, placement is additionally constrained to require ``1.0`` replica
of *each* partition per server.  It would not be sufficient to satisfy
dispersion if two devices on one of the servers each held a replica of a single
partition, while another server held none.  By considering a decimal fraction
of one replica's worth of partitions in a failure domain we can derive the
total part-replicas wanted in a failure domain (``1.0 * 2 ** <part-power>``).
Additionally we infer more about `which` part-replicas must go in the failure
domain.  Consider a ring with three replicas and two zones, each with two
servers (four servers total). The three replicas worth of partitions will be
assigned into two failure domains at the zone tier.  Each zone must hold more
than one replica of some partitions.  We represent this improper fraction of a
replica's worth of partitions in decimal form as ``1.5`` (``3.0 / 2``).  This
tells us not only the *number* of total partitions (``1.5 * 2 **
<part-power>``) but also that *each* partition must have `at least` one replica
in this failure domain (in fact ``0.5`` of the partitions will have 2
replicas).  Within each zone the two servers will hold ``0.75`` of a replica's
worth of partitions - this is equal both to "the fraction of a replica's worth
of partitions assigned to each zone (``1.5``) divided evenly among the number
of failure domains in its sub-tier (2 servers in each zone, i.e.  ``1.5 / 2``)"
but *also* "the total number of replicas (``3.0``) divided evenly among the
total number of failure domains in the server tier (2 servers × 2 zones = 4,
i.e.  ``3.0 / 4``)". It is useful to consider that each server in this ring
will hold only ``0.75`` of a replica's worth of partitions which tells that any
server should have `at most` one replica of a given partition assigned.  In the
interests of brevity, some variable names will often refer to the concept
representing the fraction of a replica's worth of partitions in decimal form as
*replicanths* - this is meant to invoke connotations similar to ordinal numbers
as applied to fractions, but generalized to a replica instead of a four\*th* or
a fif\*th*.  The "n" was probably thrown in because of Blade Runner.

-----------------
Building the Ring
-----------------

First the ring builder calculates the replicanths wanted at each tier in the
ring's topology based on weight.

Then the ring builder calculates the replicanths wanted at each tier in the
ring's topology based on dispersion.

Then the ring builder calculates the maximum deviation on a single device
between its weighted replicanths and wanted replicanths.

Next we interpolate between the two replicanth values (weighted & wanted) at
each tier using the specified overload (up to the maximum required overload).
It's a linear interpolation, similar to solving for a point on a line between
two points - we calculate the slope across the max required overload and then
calculate the intersection of the line with the desired overload.  This
becomes the target.

From the target we calculate the minimum and maximum number of replicas any
partition may have in a tier.  This becomes the `replica-plan`.

Finally, we calculate the number of partitions that should ideally be assigned
to each device based the replica-plan.

On initial balance (i.e., the first time partitions are placed to generate a
ring) we must assign each replica of each partition to the device that desires
the most partitions excluding any devices that already have their maximum
number of replicas of that partition assigned to some parent tier of that
device's failure domain.

When building a new ring based on an old ring, the desired number of
partitions each device wants is recalculated from the current replica-plan.
Next the partitions to be reassigned are gathered up. Any removed devices have
all their assigned partitions unassigned and added to the gathered list. Any
partition replicas that (due to the addition of new devices) can be spread out
for better durability are unassigned and added to the gathered list. Any
devices that have more partitions than they now desire have random partitions
unassigned from them and added to the gathered list. Lastly, the gathered
partitions are then reassigned to devices using a similar method as in the
initial assignment described above.

Whenever a partition has a replica reassigned, the time of the reassignment is
recorded. This is taken into account when gathering partitions to reassign so
that no partition is moved twice in a configurable amount of time. This
configurable amount of time is known internally to the RingBuilder class as
``min_part_hours``. This restriction is ignored for replicas of partitions on
devices that have been removed, as device removal should only happens on device
failure and there's no choice but to make a reassignment.

The above processes don't always perfectly rebalance a ring due to the random
nature of gathering partitions for reassignment. To help reach a more balanced
ring, the rebalance process is repeated a fixed number of times until the
replica-plan is fulfilled or unable to be fulfilled (indicating we probably
can't get perfect balance due to too many partitions recently moved).


.. _composite_rings:

---------------
Composite Rings
---------------
.. automodule:: swift.common.ring.composite_builder

**********************************
swift-ring-composer (Experimental)
**********************************
.. automodule:: swift.cli.ringcomposer

---------------------
Ring Builder Analyzer
---------------------
.. automodule:: swift.cli.ring_builder_analyzer

-------
History
-------

The ring code went through many iterations before arriving at what it is now
and while it has largely been stable, the algorithm has seen a few tweaks or
perhaps even fundamentally changed as new ideas emerge. This section will try
to describe the previous ideas attempted and attempt to explain why they were
discarded.

A "live ring" option was considered where each server could maintain its own
copy of the ring and the servers would use a gossip protocol to communicate the
changes they made. This was discarded as too complex and error prone to code
correctly in the project timespan available. One bug could easily gossip bad
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
of quick automatic ring changes did mean that other components of the system
had to be coded to handle devices being unavailable for a period of hours until
someone could manually update the ring.

The current ring process has each replica of a partition independently assigned
to a device. A version of the ring that used a third of the memory was tried,
where the first replica of a partition was directly assigned and the other two
were determined by "walking" the ring until finding additional devices in other
zones. This was discarded due to the loss of control over how many replicas for
a given partition moved at once. Keeping each replica independent allows for
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
partitioning helps many other components of the system, especially replication.
Replication can be attempted and retried in a partition batch with the other
replicas rather than each data item independently attempted and retried. Hashes
of directory structures can be calculated and compared with other replicas to
reduce directory walking and network traffic.

Partitioning and independently assigning partition replicas also allowed for
the best-balanced cluster. The best of the other strategies tended to give
±10% variance on device balance with devices of equal weight and ±15% with
devices of varying weights. The current strategy allows us to get ±3% and ±8%
respectively.

Various hashing algorithms were tried. SHA offers better security, but the ring
doesn't need to be cryptographically secure and SHA is slower. Murmur was much
faster, but MD5 was built-in and hash computation is a small percentage of the
overall request handling time. In all, once it was decided the servers wouldn't
be maintaining the rings themselves anyway and only doing hash lookups, MD5 was
chosen for its general availability, good distribution, and adequate speed.

The placement algorithm has seen a number of behavioral changes for
unbalanceable rings. The ring builder wants to keep replicas as far apart as
possible while still respecting device weights. In most cases, the ring
builder can achieve both, but sometimes they conflict.  At first, the behavior
was to keep the replicas far apart and ignore device weight, but that made it
impossible to gradually go from one region to two, or from two to three. Then
it was changed to favor device weight over dispersion, but that wasn't so good
for rings that were close to balanceable, like 3 machines with 60TB, 60TB, and
57TB of disk space; operators were expecting one replica per machine, but
didn't always get it. After that, overload was added to the ring builder so
that operators could choose a balance between dispersion and device weights.
In time the overload concept was improved and made more accurate.

For more background on consistent hashing rings, please see
:doc:`ring_background`.
