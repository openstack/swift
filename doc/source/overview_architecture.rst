============================
Swift Architectural Overview
============================

.. TODO - add links to more detailed overview in each section below.

------------
Proxy Server
------------

The Proxy Server is responsible for tying together the rest of the Swift
architecture. For each request, it will look up the location of the account,
container, or object in the ring (see below) and route the request accordingly.
The public API is also exposed through the Proxy Server.

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
objects. When other components need to perform any operation on an object,
container, or account, they need to interact with the appropriate ring to
determine its location in the cluster.

The Ring maintains this mapping using zones, devices, partitions, and replicas.
Each partition in the ring is replicated, by default, 3 times across the
cluster, and the locations for a partition are stored in the mapping maintained
by the ring. The ring is also responsible for determining which devices are
used for handoff in failure scenarios.

Data can be isolated with the concept of zones in the ring. Each replica
of a partition is guaranteed to reside in a different zone. A zone could
represent a drive, a server, a cabinet, a switch, or even a datacenter.

The partitions of the ring are equally divided among all the devices in the
Swift installation. When partitions need to be moved around (for example if a
device is added to the cluster), the ring ensures that a minimum number of
partitions are moved at a time, and only one replica of a partition is moved at
a time.

Weights can be used to balance the distribution of partitions on drives
across the cluster. This can be useful, for example, when different sized
drives are used in a cluster.

The ring is used by the Proxy server and several background processes
(like replication).

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

