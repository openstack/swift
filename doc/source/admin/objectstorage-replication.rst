===========
Replication
===========

Because each replica in Object Storage functions independently and
clients generally require only a simple majority of nodes to respond to
consider an operation successful, transient failures like network
partitions can quickly cause replicas to diverge. These differences are
eventually reconciled by asynchronous, peer-to-peer replicator
processes. The replicator processes traverse their local file systems
and concurrently perform operations in a manner that balances load
across physical disks.

Replication uses a push model, with records and files generally only
being copied from local to remote replicas. This is important because
data on the node might not belong there (as in the case of hand offs and
ring changes), and a replicator cannot know which data it should pull in
from elsewhere in the cluster. Any node that contains data must ensure
that data gets to where it belongs. The ring handles replica placement.

To replicate deletions in addition to creations, every deleted record or
file in the system is marked by a tombstone. The replication process
cleans up tombstones after a time period known as the ``consistency
window``. This window defines the duration of the replication and how
long transient failure can remove a node from the cluster. Tombstone
cleanup must be tied to replication to reach replica convergence.

If a replicator detects that a remote drive has failed, the replicator
uses the ``get_more_nodes`` interface for the ring to choose an
alternate node with which to synchronize. The replicator can maintain
desired levels of replication during disk failures, though some replicas
might not be in an immediately usable location.

.. note::

   The replicator does not maintain desired levels of replication when
   failures such as entire node failures occur; most failures are
   transient.

The main replication types are:

- Database replication
    Replicates containers and objects.

- Object replication
    Replicates object data.

Database replication
~~~~~~~~~~~~~~~~~~~~

Database replication completes a low-cost hash comparison to determine
whether two replicas already match. Normally, this check can quickly
verify that most databases in the system are already synchronized. If
the hashes differ, the replicator synchronizes the databases by sharing
records added since the last synchronization point.

This synchronization point is a high water mark that notes the last
record at which two databases were known to be synchronized, and is
stored in each database as a tuple of the remote database ID and record
ID. Database IDs are unique across all replicas of the database, and
record IDs are monotonically increasing integers. After all new records
are pushed to the remote database, the entire synchronization table of
the local database is pushed, so the remote database can guarantee that
it is synchronized with everything with which the local database was
previously synchronized.

If a replica is missing, the whole local database file is transmitted to
the peer by using rsync(1) and is assigned a new unique ID.

In practice, database replication can process hundreds of databases per
concurrency setting per second (up to the number of available CPUs or
disks) and is bound by the number of database transactions that must be
performed.

Object replication
~~~~~~~~~~~~~~~~~~

The initial implementation of object replication performed an rsync to
push data from a local partition to all remote servers where it was
expected to reside. While this worked at small scale, replication times
skyrocketed once directory structures could no longer be held in RAM.
This scheme was modified to save a hash of the contents for each suffix
directory to a per-partition hashes file. The hash for a suffix
directory is no longer valid when the contents of that suffix directory
is modified.

The object replication process reads in hash files and calculates any
invalidated hashes. Then, it transmits the hashes to each remote server
that should hold the partition, and only suffix directories with
differing hashes on the remote server are rsynced. After pushing files
to the remote server, the replication process notifies it to recalculate
hashes for the rsynced suffix directories.

The number of uncached directories that object replication must
traverse, usually as a result of invalidated suffix directory hashes,
impedes performance. To provide acceptable replication speeds, object
replication is designed to invalidate around 2 percent of the hash space
on a normal node each day.
