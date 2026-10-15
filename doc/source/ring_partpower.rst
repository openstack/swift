.. _modify_part_power:

==============================
Modifying Ring Partition Power
==============================

The ring partition power determines the on-disk location of data files and is
selected when creating a new ring. In normal operation, it is a fixed value.
This is because a different partition power results in a different on-disk
location for all data files.

However, increasing the partition power by 1 can be done by choosing locations
that are on the same disk. As a result, we can create hard-links for both the
new and old locations, avoiding data movement without impacting availability.

To enable a partition power change without interrupting user access, object
servers need to be aware of it in advance. Therefore a partition power change
needs to be done in multiple steps.

.. note::

    Do not increase the partition power on account and container rings.
    Increasing the partition power is *only* supported for object rings.
    Trying to increase the part_power for account and container rings *will*
    result in unavailability, maybe even data loss.


-------
Caveats
-------

Before increasing the partition power, consider the possible drawbacks.
There are a few caveats when increasing the partition power:

* Almost all diskfiles in the cluster need to be relinked then cleaned up,
  and all partition directories need to be rehashed. This imposes significant
  I/O load on object servers, which may impact client requests. Consider using
  cgroups, ``ionice``, or even just the built-in ``--files-per-second``
  rate-limiting to reduce client impact.
* Object replicators and reconstructors will skip affected policies during the
  partition power increase. Replicators are not aware of hard-links, and would
  simply copy the content; this would result in heavy data movement and the
  worst case would be that all data is stored twice.
* Due to the fact that each object will now be hard linked from two locations,
  many more inodes will be used temporarily - expect around twice the amount.
  You need to check the free inode count *before* increasing the partition
  power. Even after the increase is complete and extra hardlinks are cleaned
  up, expect increased inode usage since there will be twice as many partition
  and suffix directories.
* Also, object auditors might read each object twice before cleanup removes the
  second hard link.
* Due to the new inodes more memory is needed to cache them, and your
  object servers should have plenty of available memory to avoid running out of
  inode cache. Setting ``vfs_cache_pressure`` to 1 might help with that.
* All nodes in the cluster *must* run at least Swift version 2.13.0 or later.

Due to these caveats you should only increase the partition power if really
needed, i.e. if the number of partitions per disk is extremely low and the data
is distributed unevenly across disks.

-----------------------------------
1. Prepare partition power increase
-----------------------------------

The swift-ring-builder is used to prepare the ring for an upcoming partition
power increase. It will store a new variable ``next_part_power`` with the current
partition power + 1. Object servers recognize this, and hard links to the new
location will be created (or deleted) on every PUT or DELETE.  This will make
it possible to access newly written objects using the future partition power::

    swift-ring-builder <builder-file> prepare_increase_partition_power
    swift-ring-builder <builder-file> write_ring

Now you need to copy the updated .ring.gz to all nodes. Already existing data
needs to be relinked too; therefore an operator has to run a relinker command
on all object servers in this phase::

    swift-object-relinker relink

.. note::

    Start relinking after *all* the servers re-read the modified ring files,
    which normally happens within 15 seconds after writing a modified ring.
    Also, make sure the modified rings are pushed to all nodes running object
    services (replicators, reconstructors and reconcilers)- they have to skip
    the policy during relinking.

.. note::

    The relinking command must run as the same user as the daemon processes
    (usually swift). It will create files and directories that must be
    manipulable by the daemon processes (server, auditor, replicator, ...).
    If necessary, the ``--user`` option may be used to drop privileges.

Relinking might take some time; while there is no data copied or actually
moved, the tool still needs to walk the whole file system and create new hard
links as required.

---------------------------
2. Increase partition power
---------------------------

Now that all existing data can be found using the new location, it's time to
actually increase the partition power itself::

    swift-ring-builder <builder-file> increase_partition_power
    swift-ring-builder <builder-file> write_ring

Now you need to copy the updated .ring.gz again to all nodes. Object servers
are now using the new, increased partition power and no longer create
additional hard links.


.. note::

    The object servers will create additional hard links for each modified or
    new object, and this requires more inodes.

.. note::

    If you decide you don't want to increase the partition power, you should
    instead cancel the increase. It is not possible to revert this operation
    once started. To abort the partition power increase, execute the following
    commands, copy the updated .ring.gz files to all nodes and continue with
    `3. Cleanup`_ afterwards::

        swift-ring-builder <builder-file> cancel_increase_partition_power
        swift-ring-builder <builder-file> write_ring


----------
3. Cleanup
----------

Existing hard links in the old locations need to be removed, and a cleanup tool
is provided to do this. Run the following command on each storage node::

    swift-object-relinker cleanup

.. note::

    The cleanup must be finished within your object servers ``reclaim_age``
    period (which is by default 1 week). Otherwise objects that have been
    overwritten between step #1 and step #2 and deleted afterwards can't be
    cleaned up anymore. You may want to increase your ``reclaim_age`` before
    or during relinking.

Afterwards it is required to update the rings one last
time to inform servers that all steps to increase the partition power are done,
and replicators should resume their job::

    swift-ring-builder <builder-file> finish_increase_partition_power
    swift-ring-builder <builder-file> write_ring

Now you need to copy the updated .ring.gz again to all nodes.

---------------------------------
Audit after an early cleanup exit
---------------------------------

Audit is not a normal fourth step in a partition power increase. Its only
intended use is recovery from exceptional circumstances in which requirements
outside the operator's control forced a PPI to be marked finished before
cleanup completed. Use it only when consistency-engine warnings or discovered
dark data indicate that stale hash directories remain in old partition
locations after such an early cleanup exit.

Run the audit on every object storage node::

    swift-object-relinker audit --policy <policy-name-or-index>

.. note::

    Unlike relink and cleanup, audit has no hint from the finished ring about
    which policy had its partition power increased. The ``--policy`` flag is
    therefore required for ``audit``.

The audit is only available when no partition power increase is in progress;
in other words, the ring's ``next_part_power`` must be ``None``. It scans the
old, lower half of the partition namespace and calculates the expected current
partition for each hash directory. A hash directory found in an ancestor of
its expected partition is quarantined. A misplaced hash directory that is not
such an ancestor is left in place and logged as a warning. By default, the
audit considers ancestors from the two most recent partition power increases.
This limit may be changed with ``--max-audit-history-quarantine-threshold``
or the corresponding option in the ``[object-relinker]`` configuration section.

.. warning::

    A replicated policy requires special handling. Stop ``object-replicator``
    on the affected storage nodes before starting audit, keep it stopped until
    audit completes, and then restart it. Otherwise, primary peers may restore
    a stale hash directory after audit quarantines it, preventing audit from
    making lasting progress.

    This peer restoration does not occur for an erasure-coded policy. A lone
    stale fragment instead produces reconstructor errors when peers cannot
    provide enough fragments to rebuild it. Those failures prevent the stale
    fragment from becoming fully rebuilt, durable dark data. For example::

        Unable to get enough responses (1/10 from 1 ok responses) to reconstruct ...

.. important::

    Because audit immediately quarantines matching hash directories, it is not
    intended for use immediately after cleanup or as a substitute for
    completing cleanup. If a PPI has only just been prematurely finished,
    re-publishing the previous ring and re-running cleanup may be more
    appropriate. Defining and validating that recovery procedure is future
    work; audit does not implement it.

The relinker persists progress for each device and policy data directory in
``<devices>/<device>/relink.<data-dir>.json``. For example, with the default
devices path, policy index 1 on device ``sda`` uses
``/srv/node/sda/relink.objects-1.json``. An interrupted audit uses this file to
resume at incomplete partitions, and a subsequent audit skips partitions that
are already marked complete. Unlike relink and cleanup, a completed audit does
not itself cause another ring-state change, so merely running the command again
does not rescan those partitions.

To perform a full rescan, make sure that no relinker process is running, remove
the relevant state file from every device, and then run the audit again::

    rm /srv/node/sda/relink.objects-1.json
    swift-object-relinker audit --policy 1

----------
Background
----------

An existing object that is currently located on partition X will be placed
either on partition 2*X or 2*X+1 after the partition power is increased. The
reason for this is the Ring.get_part() method, that does a bitwise shift to the
right.

To avoid actual data movement to different disks or even nodes, the allocation
of partitions to nodes needs to be changed. The allocation is pairwise due to
the above mentioned new partition scheme. Therefore devices are allocated like
this, with the partition being the index and the value being the device id::

        old        new
    part  dev   part  dev
    ----  ---   ----  ---
    0     0     0     0
                1     0
    1     3     2     3
                3     3
    2     7     4     7
                5     7
    3     5     6     5
                7     5
    4     2     8     2
                9     2
    5     1     10    1
                11    1

There is a helper method to compute the new path, and the following example
shows the mapping between old and new location::

    >>> from swift.common.utils import replace_partition_in_path
    >>> old='objects/16003/a38/fa0fcec07328d068e24ccbf2a62f2a38/1467658208.57179.data'
    >>> replace_partition_in_path('', '/sda/' + old, 14)
    'objects/16003/a38/fa0fcec07328d068e24ccbf2a62f2a38/1467658208.57179.data'
    >>> replace_partition_in_path('', '/sda/' + old, 15)
    'objects/32007/a38/fa0fcec07328d068e24ccbf2a62f2a38/1467658208.57179.data'

Using the original partition power (14) it returned the same path; however
after an increase to 15 it returns the new path, and the new partition is 2*X+1
in this case.
