==================
Server maintenance
==================

General assumptions
~~~~~~~~~~~~~~~~~~~

-  It is assumed that anyone attempting to replace hardware components
   will have already read and understood the appropriate maintenance and
   service guides.

-  It is assumed that where servers need to be taken off-line for
   hardware replacement, that this will be done in series, bringing the
   server back on-line before taking the next off-line.

-  It is assumed that the operations directed procedure will be used for
   identifying hardware for replacement.

Assessing the health of swift
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can run the swift-recon tool on a Swift proxy node to get a quick
check of how Swift is doing. Please note that the numbers below are
necessarily somewhat subjective. Sometimes parameters for which we
say 'low values are good' will have pretty high values for a time. Often
if you wait a while things get better.

For example:

.. code::

   sudo swift-recon -rla
   ===============================================================================
   [2012-03-10 12:57:21] Checking async pendings on 384 hosts...
   Async stats: low: 0, high: 1, avg: 0, total: 1
   ===============================================================================

   [2012-03-10 12:57:22] Checking replication times on 384 hosts...
   [Replication Times] shortest: 1.4113877813, longest: 36.8293570836, avg: 4.86278064749
   ===============================================================================

   [2012-03-10 12:57:22] Checking load avg's on 384 hosts...
   [5m load average] lowest: 2.22, highest: 9.5, avg: 4.59578125
   [15m load average] lowest: 2.36, highest: 9.45, avg: 4.62622395833
   [1m load average] lowest: 1.84, highest: 9.57, avg: 4.5696875
   ===============================================================================

In the example above we ask for information on replication times (-r),
load averages (-l) and async pendings (-a). This is a healthy Swift
system. Rules-of-thumb for 'good' recon output are:

-  Nodes that respond are up and running Swift. If all nodes respond,
   that is a good sign. But some nodes may time out. For example:

   .. code::

      -> [http://<redacted>.29:6200/recon/load:] <urlopen error [Errno 111] ECONNREFUSED>
      -> [http://<redacted>.31:6200/recon/load:] <urlopen error timed out>

-  That could be okay or could require investigation.

-  Low values (say < 10 for high and average) for async pendings are
   good. Higher values occur when disks are down and/or when the system
   is heavily loaded. Many simultaneous PUTs to the same container can
   drive async pendings up. This may be normal, and may resolve itself
   after a while. If it persists, one way to track down the problem is
   to find a node with high async pendings (with ``swift-recon -av | sort
   -n -k4``), then check its Swift logs, Often async pendings are high
   because a node cannot write to a container on another node. Often
   this is because the node or disk is offline or bad. This may be okay
   if we know about it.

-  Low values for replication times are good. These values rise when new
   rings are pushed, and when nodes and devices are brought back on
   line.

-  Our 'high' load average values are typically in the 9-15 range. If
   they are a lot bigger it is worth having a look at the systems
   pushing the average up. Run ``swift-recon -av`` to get the individual
   averages. To sort the entries with the highest at the end,
   run ``swift-recon -av | sort -n -k4``.

For comparison here is the recon output for the same system above when
two entire racks of Swift are down:

.. code::

   [2012-03-10 16:56:33] Checking async pendings on 384 hosts...
   -> http://<redacted>.22:6200/recon/async: <urlopen error timed out>
   -> http://<redacted>.18:6200/recon/async: <urlopen error timed out>
   -> http://<redacted>.16:6200/recon/async: <urlopen error timed out>
   -> http://<redacted>.13:6200/recon/async: <urlopen error timed out>
   -> http://<redacted>.30:6200/recon/async: <urlopen error timed out>
   -> http://<redacted>.6:6200/recon/async: <urlopen error timed out>
   .........
   -> http://<redacted>.5:6200/recon/async: <urlopen error timed out>
   -> http://<redacted>.15:6200/recon/async: <urlopen error timed out>
   -> http://<redacted>.9:6200/recon/async: <urlopen error timed out>
   -> http://<redacted>.27:6200/recon/async: <urlopen error timed out>
   -> http://<redacted>.4:6200/recon/async: <urlopen error timed out>
   -> http://<redacted>.8:6200/recon/async: <urlopen error timed out>
   Async stats: low: 243, high: 659, avg: 413, total: 132275
   ===============================================================================
   [2012-03-10 16:57:48] Checking replication times on 384 hosts...
   -> http://<redacted>.22:6200/recon/replication: <urlopen error timed out>
   -> http://<redacted>.18:6200/recon/replication: <urlopen error timed out>
   -> http://<redacted>.16:6200/recon/replication: <urlopen error timed out>
   -> http://<redacted>.13:6200/recon/replication: <urlopen error timed out>
   -> http://<redacted>.30:6200/recon/replication: <urlopen error timed out>
   -> http://<redacted>.6:6200/recon/replication: <urlopen error timed out>
   ............
   -> http://<redacted>.5:6200/recon/replication: <urlopen error timed out>
   -> http://<redacted>.15:6200/recon/replication: <urlopen error timed out>
   -> http://<redacted>.9:6200/recon/replication: <urlopen error timed out>
   -> http://<redacted>.27:6200/recon/replication: <urlopen error timed out>
   -> http://<redacted>.4:6200/recon/replication: <urlopen error timed out>
   -> http://<redacted>.8:6200/recon/replication: <urlopen error timed out>
   [Replication Times] shortest: 1.38144306739, longest: 112.620954418, avg: 10.285
   9475361
   ===============================================================================
   [2012-03-10 16:59:03] Checking load avg's on 384 hosts...
   -> http://<redacted>.22:6200/recon/load: <urlopen error timed out>
   -> http://<redacted>.18:6200/recon/load: <urlopen error timed out>
   -> http://<redacted>.16:6200/recon/load: <urlopen error timed out>
   -> http://<redacted>.13:6200/recon/load: <urlopen error timed out>
   -> http://<redacted>.30:6200/recon/load: <urlopen error timed out>
   -> http://<redacted>.6:6200/recon/load: <urlopen error timed out>
   ............
   -> http://<redacted>.15:6200/recon/load: <urlopen error timed out>
   -> http://<redacted>.9:6200/recon/load: <urlopen error timed out>
   -> http://<redacted>.27:6200/recon/load: <urlopen error timed out>
   -> http://<redacted>.4:6200/recon/load: <urlopen error timed out>
   -> http://<redacted>.8:6200/recon/load: <urlopen error timed out>
   [5m load average] lowest: 1.71, highest: 4.91, avg: 2.486375
   [15m load average] lowest: 1.79, highest: 5.04, avg: 2.506125
   [1m load average] lowest: 1.46, highest: 4.55, avg: 2.4929375
   ===============================================================================

.. note::

   The replication times and load averages are within reasonable
   parameters, even with 80 object stores down. Async pendings, however is
   quite high. This is due to the fact that the containers on the servers
   which are down cannot be updated. When those servers come back up, async
   pendings should drop. If async pendings were at this level without an
   explanation, we have a problem.

Recon examples
~~~~~~~~~~~~~~

Here is an example of noting and tracking down a problem with recon.

Running reccon shows some async pendings:

.. code::

   bob@notso:~/swift-1.4.4/swift$ ssh -q <redacted>.132.7 sudo swift-recon -alr
   ===============================================================================
   [2012-03-14 17:25:55] Checking async pendings on 384 hosts...
   Async stats: low: 0, high: 23, avg: 8, total: 3356
   ===============================================================================
   [2012-03-14 17:25:55] Checking replication times on 384 hosts...
   [Replication Times] shortest: 1.49303831657, longest: 39.6982825994, avg: 4.2418222066
   ===============================================================================
   [2012-03-14 17:25:56] Checking load avg's on 384 hosts...
   [5m load average] lowest: 2.35, highest: 8.88, avg: 4.45911458333
   [15m load average] lowest: 2.41, highest: 9.11, avg: 4.504765625
   [1m load average] lowest: 1.95, highest: 8.56, avg: 4.40588541667
    ===============================================================================

Why? Running recon again with -av swift (not shown here) tells us that
the node with the highest (23) is <redacted>.72.61. Looking at the log
files on <redacted>.72.61 we see:

.. code::

   souzab@<redacted>:~$ sudo tail -f /var/log/swift/background.log | - grep -i ERROR
   Mar 14 17:28:06 <redacted> container-replicator ERROR Remote drive not mounted
   {'zone': 5, 'weight': 1952.0, 'ip': '<redacted>.204.119', 'id': 5481, 'meta': '', 'device': 'disk6', 'port': 6201}
   Mar 14 17:28:06 <redacted> container-replicator ERROR Remote drive not mounted
   {'zone': 5, 'weight': 1952.0, 'ip': '<redacted>.204.119', 'id': 5481, 'meta': '', 'device': 'disk6', 'port': 6201}
   Mar 14 17:28:09 <redacted> container-replicator ERROR Remote drive not mounted
   {'zone': 5, 'weight': 1952.0, 'ip': '<redacted>.204.20', 'id': 2311, 'meta': '', 'device': 'disk5', 'port': 6201}
   Mar 14 17:28:11 <redacted> container-replicator ERROR Remote drive not mounted
   {'zone': 5, 'weight': 1952.0, 'ip': '<redacted>.204.20', 'id': 2311, 'meta': '', 'device': 'disk5', 'port': 6201}
   Mar 14 17:28:13 <redacted> container-replicator ERROR Remote drive not mounted
   {'zone': 5, 'weight': 1952.0, 'ip': '<redacted>.204.119', 'id': 5481, 'meta': '', 'device': 'disk6', 'port': 6201}
   Mar 14 17:28:13 <redacted> container-replicator ERROR Remote drive not mounted
   {'zone': 5, 'weight': 1952.0, 'ip': '<redacted>.204.119', 'id': 5481, 'meta': '', 'device': 'disk6', 'port': 6201}
   Mar 14 17:28:15 <redacted> container-replicator ERROR Remote drive not mounted
   {'zone': 5, 'weight': 1952.0, 'ip': '<redacted>.204.20', 'id': 2311, 'meta': '', 'device': 'disk5', 'port': 6201}
   Mar 14 17:28:15 <redacted> container-replicator ERROR Remote drive not mounted
   {'zone': 5, 'weight': 1952.0, 'ip': '<redacted>.204.20', 'id': 2311, 'meta': '', 'device': 'disk5', 'port': 6201}
   Mar 14 17:28:19 <redacted> container-replicator ERROR Remote drive not mounted
   {'zone': 5, 'weight': 1952.0, 'ip': '<redacted>.204.20', 'id': 2311, 'meta': '', 'device': 'disk5', 'port': 6201}
   Mar 14 17:28:19 <redacted> container-replicator ERROR Remote drive not mounted
   {'zone': 5, 'weight': 1952.0, 'ip': '<redacted>.204.20', 'id': 2311, 'meta': '', 'device': 'disk5', 'port': 6201}
   Mar 14 17:28:20 <redacted> container-replicator ERROR Remote drive not mounted
   {'zone': 5, 'weight': 1952.0, 'ip': '<redacted>.204.119', 'id': 5481, 'meta': '', 'device': 'disk6', 'port': 6201}
   Mar 14 17:28:21 <redacted> container-replicator ERROR Remote drive not mounted
   {'zone': 5, 'weight': 1952.0, 'ip': '<redacted>.204.20', 'id': 2311, 'meta': '', 'device': 'disk5', 'port': 6201}
   Mar 14 17:28:21 <redacted> container-replicator ERROR Remote drive not mounted
   {'zone': 5, 'weight': 1952.0, 'ip': '<redacted>.204.20', 'id': 2311, 'meta': '', 'device': 'disk5', 'port': 6201}
   Mar 14 17:28:22 <redacted> container-replicator ERROR Remote drive not mounted
   {'zone': 5, 'weight': 1952.0, 'ip': '<redacted>.204.20', 'id': 2311, 'meta': '', 'device': 'disk5', 'port': 6201}

That is why this node has a lot of async pendings: a bunch of disks that
are not mounted on <redacted> and <redacted>. There may be other issues,
but clearing this up will likely drop the async pendings a fair bit, as
other nodes will be having the same problem.

Assessing the availability risk when multiple storage servers are down
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   This procedure will tell you if you have a problem, however, in practice
   you will find that you will not use this procedure frequently.

If three storage nodes (or, more precisely, three disks on three
different storage nodes) are down, there is a small but nonzero
probability that user objects, containers, or accounts will not be
available.

Procedure
---------

.. note::

   swift has three rings: one each for objects, containers and accounts.
   This procedure should be run three times, each time specifying the
   appropriate ``*.builder`` file.

#. Determine whether all three nodes are in different Swift zones by
   running the ring builder on a proxy node to determine which zones
   the storage nodes are in. For example:

   .. code::

      % sudo swift-ring-builder /etc/swift/object.builder
      /etc/swift/object.builder, build version 1467
      2097152 partitions, 3 replicas, 5 zones, 1320 devices, 0.02 balance
      The minimum number of hours before a partition can be reassigned is 24
      Devices:    id  zone     ip address    port     name  weight  partitions balance meta
                   0     1     <redacted>.4  6200     disk0 1708.00       4259   -0.00
                   1     1     <redacted>.4  6200     disk1 1708.00       4260    0.02
                   2     1     <redacted>.4  6200     disk2 1952.00       4868    0.01
                   3     1     <redacted>.4  6200     disk3 1952.00       4868    0.01
                   4     1     <redacted>.4  6200     disk4 1952.00       4867   -0.01

#. Here, node <redacted>.4 is in zone 1. If two or more of the three
   nodes under consideration are in the same Swift zone, they do not
   have any ring partitions in common; there is little/no data
   availability risk if all three nodes are down.

#. If the nodes are in three distinct Swift zones it is necessary to
   whether the nodes have ring partitions in common. Run ``swift-ring``
   builder again, this time with the ``list_parts`` option and specify
   the nodes under consideration. For example:

   .. code::

      % sudo swift-ring-builder /etc/swift/object.builder list_parts <redacted>.8 <redacted>.15 <redacted>.72.2
      Partition   Matches
      91           2
      729          2
      3754         2
      3769         2
      3947         2
      5818         2
      7918         2
      8733         2
      9509         2
      10233        2

#. The ``list_parts`` option to the ring builder indicates how many ring
   partitions the nodes have in common. If, as in this case,  the
   first entry in the list has a 'Matches' column of 2 or less,  there
   is no data availability risk if all three nodes are down.

#. If the 'Matches' column has entries equal to 3, there is some data
   availability risk if all three nodes are down. The risk is generally
   small, and is proportional to the number of entries that have a 3 in
   the Matches column. For example:

   .. code::

      Partition   Matches
      26865          3
      362367         3
      745940         3
      778715         3
      797559         3
      820295         3
      822118         3
      839603         3
      852332         3
      855965         3
      858016         3

#. A quick way to count the number of rows with 3 matches is:

   .. code::

      % sudo swift-ring-builder /etc/swift/object.builder list_parts <redacted>.8 <redacted>.15 <redacted>.72.2 | grep "3$" | wc -l

      30

#. In this case the nodes have 30 out of a total of 2097152 partitions
   in common; about 0.001%. In this case the risk is small/nonzero.
   Recall that a partition is simply a portion of the ring mapping
   space, not actual data. So having partitions in common is a necessary
   but not sufficient condition for data unavailability.

   .. note::

      We should not bring down a node for repair if it shows
      Matches entries of 3 with other nodes that are also down.

      If three nodes that have 3 partitions in common are all down, there is
      a nonzero probability that data are unavailable and we should work to
      bring some or all of the nodes up ASAP.

Swift startup/shutdown
~~~~~~~~~~~~~~~~~~~~~~

-  Use reload - not stop/start/restart.

-  Try to roll sets of servers (especially proxy) in groups of less
   than 20% of your servers.
