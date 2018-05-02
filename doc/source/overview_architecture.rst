============================
Swift Architectural Overview
============================

------------
Proxy Server
------------

The Proxy Server is responsible for tying together the rest of the Swift
architecture. For each request, it will look up the location of the account,
container, or object in the ring (see below) and route the request accordingly.
For Erasure Code type policies, the Proxy Server is also responsible for
encoding and decoding object data.  See :doc:`overview_erasure_code` for
complete information on Erasure Code support.  The public API is also exposed
through the Proxy Server.

A large number of failures are also handled in the Proxy Server. For
example, if a server is unavailable for an object PUT, it will ask the
ring for a handoff server and route there instead.

When objects are streamed to or from an object server, they are streamed
directly through the proxy server to or from the user -- the proxy server
does not spool them.

--------
The Ring
--------

A ring represents a mapping between the names of entities stored on disk and
their physical location. There are separate rings for accounts, containers, and
one object ring per storage policy. When other components need to perform any
operation on an object, container, or account, they need to interact with the
appropriate ring to determine its location in the cluster.

The Ring maintains this mapping using zones, devices, partitions, and replicas.
Each partition in the ring is replicated, by default, 3 times across the
cluster, and the locations for a partition are stored in the mapping maintained
by the ring. The ring is also responsible for determining which devices are
used for handoff in failure scenarios.

The replicas of each partition will be isolated onto as many distinct regions,
zones, servers and devices as the capacity of these failure domains allow.  If
there are less failure domains at a given tier than replicas of the partition
assigned within a tier (e.g. a 3 replica cluster with 2 servers), or the
available capacity across the failure domains within a tier are not well
balanced it will not be possible to achieve both even capacity distribution
(`balance`) as well as complete isolation of replicas across failure domains
(`dispersion`).  When this occurs the ring management tools will display a
warning so that the operator can evaluate the cluster topology.

Data is evenly distributed across the capacity available in the cluster as
described by the devices weight.  Weights can be used to balance the
distribution of partitions on drives across the cluster. This can be useful,
for example, when different sized drives are used in a cluster.  Device
weights can also be used when adding or removing capacity or failure domains
to control how many partitions are reassigned during a rebalance to be moved
as soon as replication bandwidth allows.

.. note::
    Prior to Swift 2.1.0 it was not possible to restrict partition movement by
    device weight when adding new failure domains, and would allow extremely
    unbalanced rings.  The greedy dispersion algorithm is now subject to the
    constraints of the physical capacity in the system, but can be adjusted
    with-in reason via the overload option.  Artificially unbalancing the
    partition assignment without respect to capacity can introduce unexpected
    full devices when a given failure domain does not physically support its
    share of the used capacity in the tier.

When partitions need to be moved around (for example if a device is added to
the cluster), the ring ensures that a minimum number of partitions are moved
at a time, and only one replica of a partition is moved at a time.

The ring is used by the Proxy server and several background processes
(like replication). See :doc:`overview_ring` for complete information on the
ring.

----------------
Storage Policies
----------------

Storage Policies provide a way for object storage providers to differentiate
service levels, features and behaviors of a Swift deployment.  Each Storage
Policy configured in Swift is exposed to the client via an abstract name.
Each device in the system is assigned to one or more Storage Policies.  This
is accomplished through the use of multiple object rings, where each Storage
Policy has an independent object ring, which may include a subset of hardware
implementing a particular differentiation.

For example, one might have the default policy with 3x replication, and create
a second policy which, when applied to new containers only uses 2x replication.
Another might add SSDs to a set of storage nodes and create a performance tier
storage policy for certain containers to have their objects stored there.  Yet
another might be the use of Erasure Coding to define a cold-storage tier.

This mapping is then exposed on a per-container basis, where each container
can be assigned a specific storage policy when it is created, which remains in
effect for the lifetime of the container.  Applications require minimal
awareness of storage policies to use them; once a container has been created
with a specific policy, all objects stored in it will be done so in accordance
with that policy.

The Storage Policies feature is implemented throughout the entire code base so
it is an important concept in understanding Swift architecture.

See :doc:`overview_policies` for complete information on storage policies.

-------------
Object Server
-------------

The Object Server is a very simple blob storage server that can store,
retrieve and delete objects stored on local devices. Objects are stored
as binary files on the filesystem with metadata stored in the file's
extended attributes (xattrs). This requires that the underlying filesystem
choice for object servers support xattrs on files. Some filesystems,
like ext3, have xattrs turned off by default.

Each object is stored using a path derived from the object name's hash and
the operation's timestamp. Last write always wins, and ensures that the
latest object version will be served. A deletion is also treated as a
version of the file (a 0 byte file ending with ".ts", which stands for
tombstone). This ensures that deleted files are replicated correctly and
older versions don't magically reappear due to failure scenarios.

----------------
Container Server
----------------

The Container Server's primary job is to handle listings of objects. It
doesn't know where those object's are, just what objects are in a specific
container. The listings are stored as sqlite database files, and replicated
across the cluster similar to how objects are. Statistics are also tracked
that include the total number of objects, and total storage usage for that
container.

--------------
Account Server
--------------

The Account Server is very similar to the Container Server, excepting that
it is responsible for listings of containers rather than objects.

-----------
Replication
-----------

Replication is designed to keep the system in a consistent state in the face
of temporary error conditions like network outages or drive failures.

The replication processes compare local data with each remote copy to ensure
they all contain the latest version. Object replication uses a hash list to
quickly compare subsections of each partition, and container and account
replication use a combination of hashes and shared high water marks.

Replication updates are push based. For object replication, updating is
just a matter of rsyncing files to the peer. Account and container
replication push missing records over HTTP or rsync whole database files.

The replicator also ensures that data is removed from the system. When an
item (object, container, or account) is deleted, a tombstone is set as the
latest version of the item. The replicator will see the tombstone and ensure
that the item is removed from the entire system.

See :doc:`overview_replication` for complete information on replication.

--------------
Reconstruction
--------------

The reconstructor is used by Erasure Code policies and is analogous to the
replicator for Replication type policies.  See :doc:`overview_erasure_code`
for complete information on both Erasure Code support as well as the
reconstructor.

.. _architecture_updaters:

--------
Updaters
--------

There are times when container or account data can not be immediately
updated. This usually occurs during failure scenarios or periods of high
load. If an update fails, the update is queued locally on the filesystem,
and the updater will process the failed updates. This is where an eventual
consistency window will most likely come in to play. For example, suppose a
container server is under load and a new object is put in to the system. The
object will be immediately available for reads as soon as the proxy server
responds to the client with success. However, the container server did not
update the object listing, and so the update would be queued for a later
update. Container listings, therefore, may not immediately contain the object.

In practice, the consistency window is only as large as the frequency at
which the updater runs and may not even be noticed as the proxy server will
route listing requests to the first container server which responds. The
server under load may not be the one that serves subsequent listing
requests -- one of the other two replicas may handle the listing.

--------
Auditors
--------

Auditors crawl the local server checking the integrity of the objects,
containers, and accounts. If corruption is found (in the case of bit rot,
for example), the file is quarantined, and replication will replace the bad
file from another replica. If other errors are found they are logged (for
example, an object's listing can't be found on any container server it
should be).