===========
Replication
===========

Because each replica in Swift functions independently, and clients generally
require only a simple majority of nodes responding to consider an operation
successful, transient failures like network partitions can quickly cause
replicas to diverge. These differences are eventually reconciled by
asynchronous, peer-to-peer replicator processes. The replicator processes
traverse their local filesystems, concurrently performing operations in a
manner that balances load across physical disks.

Replication uses a push model, with records and files generally only being
copied from local to remote replicas. This is important because data on the
node may not belong there (as in the case of handoffs and ring changes), and a
replicator can't know what data exists elsewhere in the cluster that it should
pull in. It's the duty of any node that contains data to ensure that data gets
to where it belongs. Replica placement is handled by the ring.

Every deleted record or file in the system is marked by a tombstone, so that
deletions can be replicated alongside creations. The replication process cleans
up tombstones after a time period known as the consistency window.
The consistency window encompasses replication duration and how long transient
failure can remove a node from the cluster. Tombstone cleanup must
be tied to replication to reach replica convergence.

If a replicator detects that a remote drive has failed, the replicator uses
the get_more_nodes interface for the ring to choose an alternate node with
which to synchronize. The replicator can maintain desired levels of replication
in the face of disk failures, though some replicas may not be in an immediately
usable location. Note that the replicator doesn't maintain desired levels of
replication when other failures, such as entire node failures, occur because
most failure are transient.

Replication is an area of active development, and likely rife with potential
improvements to speed and correctness.

There are two major classes of replicator - the db replicator, which
replicates accounts and containers, and the object replicator, which
replicates object data.

--------------
DB Replication
--------------

The first step performed by db replication is a low-cost hash comparison to
determine whether two replicas already match. Under normal operation,
this check is able to verify that most databases in the system are already
synchronized very quickly. If the hashes differ, the replicator brings the
databases in sync by sharing records added since the last sync point.

This sync point is a high water mark noting the last record at which two
databases were known to be in sync, and is stored in each database as a tuple
of the remote database id and record id. Database ids are unique amongst all
replicas of the database, and record ids are monotonically increasing
integers. After all new records have been pushed to the remote database, the
entire sync table of the local database is pushed, so the remote database
can guarantee that it is in sync with everything with which the local database
has previously synchronized.

If a replica is found to be missing entirely, the whole local database file is
transmitted to the peer using rsync(1) and vested with a new unique id.

In practice, DB replication can process hundreds of databases per concurrency
setting per second (up to the number of available CPUs or disks) and is bound
by the number of DB transactions that must be performed.


------------------
Object Replication
------------------

The initial implementation of object replication simply performed an rsync to
push data from a local partition to all remote servers it was expected to
exist on. While this performed adequately at small scale, replication times
skyrocketed once directory structures could no longer be held in RAM. We now
use a modification of this scheme in which a hash of the contents for each
suffix directory is saved to a per-partition hashes file. The hash for a
suffix directory is invalidated when the contents of that suffix directory are
modified.

The object replication process reads in these hash files, calculating any
invalidated hashes. It then transmits the hashes to each remote server that
should hold the partition, and only suffix directories with differing hashes
on the remote server are rsynced. After pushing files to the remote server,
the replication process notifies it to recalculate hashes for the rsynced
suffix directories.

Performance of object replication is generally bound by the number of uncached
directories it has to traverse, usually as a result of invalidated suffix
directory hashes. Using write volume and partition counts from our running
systems, it was designed so that around 2% of the hash space on a normal node
will be invalidated per day, which has experimentally given us acceptable
replication speeds.

.. _ssync:

Work continues with a new ssync method where rsync is not used at all and
instead all-Swift code is used to transfer the objects. At first, this ssync
will just strive to emulate the rsync behavior. Once deemed stable it will open
the way for future improvements in replication since we'll be able to easily
add code in the replication path instead of trying to alter the rsync code
base and distributing such modifications.

One of the first improvements planned is an "index.db" that will replace the
hashes.pkl. This will allow quicker updates to that data as well as more
streamlined queries. Quite likely we'll implement a better scheme than the
current one hashes.pkl uses (hash-trees, that sort of thing).

Another improvement planned all along the way is separating the local disk
structure from the protocol path structure. This separation will allow ring
resizing at some point, or at least ring-doubling.

Note that for objects being stored with an Erasure Code policy, the replicator
daemon is not involved.  Instead, the reconstructor is used by Erasure Code
policies and is analogous to the replicator for Replication type policies.
See :doc:`overview_erasure_code` for complete information on both Erasure Code
support as well as the reconstructor.

----------
Hashes.pkl
----------

The hashes.pkl file is a key element for both replication and reconstruction
(for Erasure Coding).  Both daemons use this file to determine if any kind of
action is required between nodes that are participating in the durability
scheme.  The file itself is a pickled dictionary with slightly different
formats depending on whether the policy is Replication or Erasure Code.  In
either case, however, the same basic information is provided between the
nodes.  The dictionary contains a dictionary where the key is a suffix
directory name and the value is the MD5 hash of the directory listing for
that suffix.  In this manner, the daemon can quickly identify differences
between local and remote suffix directories on a per partition basis as the
scope of any one hashes.pkl file is a partition directory.

For Erasure Code policies, there is a little more information required.  An
object's hash directory may contain multiple fragments of a single object in
the event that the node is acting as a handoff or perhaps if a rebalance is
underway.  Each fragment of an object is stored with a fragment index, so
the hashes.pkl for an Erasure Code partition will still be a dictionary
keyed on the suffix directory name, however, the value is another dictionary
keyed on the fragment index with subsequent MD5 hashes for each one as
values.  Some files within an object hash directory don't require a fragment
index so None is used to represent those.  Below are examples of what these
dictionaries might look like.

Replication hashes.pkl::

    {'a43': '72018c5fbfae934e1f56069ad4425627',
     'b23': '12348c5fbfae934e1f56069ad4421234'}

Erasure Code hashes.pkl::

    {'a43': {None: '72018c5fbfae934e1f56069ad4425627',
             2: 'b6dd6db937cb8748f50a5b6e4bc3b808'},
     'b23': {None: '12348c5fbfae934e1f56069ad4421234',
             1: '45676db937cb8748f50a5b6e4bc34567'}}





-----------------------------
Dedicated replication network
-----------------------------

Swift has support for using dedicated network for replication traffic.
For more information see :ref:`Overview of dedicated replication network
<Dedicated-replication-network>`.
