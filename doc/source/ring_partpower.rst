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

* All hashes.pkl files will become invalid once hard links are created, and the
  replicators will need significantly more time on the first run after finishing
  the partition power increase.
* Object replicators will skip partitions during the partition power increase.
  Replicators are not aware of hard-links, and would simply copy the content;
  this would result in heavy data movement and the worst case would be that all
  data is stored twice.
* Due to the fact that each object will now be hard linked from two locations,
  many more inodes will be used - expect around twice the amount. You need to
  check the free inode count *before* increasing the partition power.
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
    partitions during relinking.

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

    The cleanup must be finished within your object servers reclaim_age period
    (which is by default 1 week). Otherwise objects that have been overwritten
    between step #1 and step #2 and deleted afterwards can't be cleaned up
    anymore.

Afterwards it is required to update the rings one last
time to inform servers that all steps to increase the partition power are done,
and replicators should resume their job::

    swift-ring-builder <builder-file> finish_increase_partition_power
    swift-ring-builder <builder-file> write_ring

Now you need to copy the updated .ring.gz again to all nodes.

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
    >>> replace_partition_in_path(old, 14)
    'objects/16003/a38/fa0fcec07328d068e24ccbf2a62f2a38/1467658208.57179.data'
    >>> replace_partition_in_path(old, 15)
    'objects/32007/a38/fa0fcec07328d068e24ccbf2a62f2a38/1467658208.57179.data'

Using the original partition power (14) it returned the same path; however
after an increase to 15 it returns the new path, and the new partition is 2*X+1
in this case.
