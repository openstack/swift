==================================
Building a Consistent Hashing Ring
==================================

------------------------------------
Authored by Greg Holt, February 2011
------------------------------------

This is a compilation of five posts I made earlier discussing how to build
a consistent hashing ring. The posts seemed to be accessed quite frequently,
so I've gathered them all here on one page for easier reading.

.. note::
    This is an historical document; as such, all code examples are Python 2.
    If this makes you squirm, think of it as pseudo-code. Regardless of
    implementation language, the state of the art in consistent-hashing and
    distributed systems more generally has advanced. We hope that this
    introduction from first principles will still prove informative,
    particularly with regard to how data is distributed within a Swift
    cluster.

Part 1
======
"Consistent Hashing" is a term used to describe a process where data is
distributed using a hashing algorithm to determine its location. Using
only the hash of the id of the data you can determine exactly where that
data should be. This mapping of hashes to locations is usually termed a
"ring".

Probably the simplest hash is just a modulus of the id. For instance, if
all ids are numbers and you have two machines you wish to distribute data
to, you could just put all odd numbered ids on one machine and even numbered
ids on the other. Assuming you have a balanced number of odd and even
numbered ids, and a balanced data size per id, your data would be balanced
between the two machines.

Since data ids are often textual names and not numbers, like paths for
files or URLs, it makes sense to use a "real" hashing algorithm to convert
the names to numbers first. Using MD5 for instance, the hash of the name
'mom.png' is '4559a12e3e8da7c2186250c2f292e3af' and the hash of 'dad.png'
is '096edcc4107e9e18d6a03a43b3853bea'. Now, using the modulus, we can
place 'mom.jpg' on the odd machine and 'dad.png' on the even one. Another
benefit of using a hashing algorithm like MD5 is that the resulting hashes
have a known even distribution, meaning your ids will be evenly distributed
without worrying about keeping the id values themselves evenly distributed.

Here is a simple example of this in action:

.. code-block:: python

  from hashlib import md5
  from struct import unpack_from

  NODE_COUNT = 100
  DATA_ID_COUNT = 10000000

  node_counts = [0] * NODE_COUNT
  for data_id in range(DATA_ID_COUNT):
      data_id = str(data_id)
      # This just pulls part of the hash out as an integer
      hsh = unpack_from('>I', md5(data_id).digest())[0]
      node_id = hsh % NODE_COUNT
      node_counts[node_id] += 1
  desired_count = DATA_ID_COUNT / NODE_COUNT
  print '%d: Desired data ids per node' % desired_count
  max_count = max(node_counts)
  over = 100.0 * (max_count - desired_count) / desired_count
  print '%d: Most data ids on one node, %.02f%% over' % \
      (max_count, over)
  min_count = min(node_counts)
  under = 100.0 * (desired_count - min_count) / desired_count
  print '%d: Least data ids on one node, %.02f%% under' % \
      (min_count, under)

::

  100000: Desired data ids per node
  100695: Most data ids on one node, 0.69% over
  99073: Least data ids on one node, 0.93% under

So that's not bad at all; less than a percent over/under for distribution
per node. In the next part of this series we'll examine where modulus
distribution causes problems and how to improve our ring to overcome them.

Part 2
======
In Part 1 of this series, we did a simple test of using the modulus of a
hash to locate data. We saw very good distribution, but that's only part
of the story. Distributed systems not only need to distribute load, but
they often also need to grow as more and more data is placed in it.

So let's imagine we have a 100 node system up and running using our
previous algorithm, but it's starting to get full so we want to add
another node. When we add that 101st node to our algorithm we notice
that many ids now map to different nodes than they previously did.
We're going to have to shuffle a ton of data around our system to get
it all into place again.

Let's examine what's happened on a much smaller scale: just 2 nodes
again, node 0 gets even ids and node 1 gets odd ids. So data id 100
would map to node 0, data id 101 to node 1, data id 102 to node 0, etc.
This is simply node = id % 2. Now we add a third node (node 2) for more
space, so we want node = id % 3. So now data id 100 maps to node id 1,
data id 101 to node 2, and data id 102 to node 0. So we have to move
data for 2 of our 3 ids so they can be found again.

Let's examine this at a larger scale:

.. code-block:: python

  from hashlib import md5
  from struct import unpack_from

  NODE_COUNT = 100
  NEW_NODE_COUNT = 101
  DATA_ID_COUNT = 10000000

  moved_ids = 0
  for data_id in range(DATA_ID_COUNT):
      data_id = str(data_id)
      hsh = unpack_from('>I', md5(str(data_id)).digest())[0]
      node_id = hsh % NODE_COUNT
      new_node_id = hsh % NEW_NODE_COUNT
      if node_id != new_node_id:
          moved_ids += 1
  percent_moved = 100.0 * moved_ids / DATA_ID_COUNT
  print '%d ids moved, %.02f%%' % (moved_ids, percent_moved)

::

  9900989 ids moved, 99.01%

Wow, that's severe. We'd have to shuffle around 99% of our data just
to increase our capacity 1%! We need a new algorithm that combats this
behavior.

This is where the "ring" really comes in. We can assign ranges of hashes
directly to nodes and then use an algorithm that minimizes the changes
to those ranges. Back to our small scale, let's say our ids range from 0
to 999. We have two nodes and we'll assign data ids 0–499 to node 0 and
500–999 to node 1. Later, when we add node 2, we can take half the data
ids from node 0 and half from node 1, minimizing the amount of data that
needs to move.

Let's examine this at a larger scale:

.. code-block:: python

  from bisect import bisect_left
  from hashlib import md5
  from struct import unpack_from

  NODE_COUNT = 100
  NEW_NODE_COUNT = 101
  DATA_ID_COUNT = 10000000

  node_range_starts = []
  for node_id in range(NODE_COUNT):
      node_range_starts.append(DATA_ID_COUNT /
                               NODE_COUNT * node_id)
  new_node_range_starts = []
  for new_node_id in range(NEW_NODE_COUNT):
      new_node_range_starts.append(DATA_ID_COUNT /
                                NEW_NODE_COUNT * new_node_id)
  moved_ids = 0
  for data_id in range(DATA_ID_COUNT):
      data_id = str(data_id)
      hsh = unpack_from('>I', md5(str(data_id)).digest())[0]
      node_id = bisect_left(node_range_starts,
                            hsh % DATA_ID_COUNT) % NODE_COUNT
      new_node_id = bisect_left(new_node_range_starts,
                            hsh % DATA_ID_COUNT) % NEW_NODE_COUNT
      if node_id != new_node_id:
          moved_ids += 1
  percent_moved = 100.0 * moved_ids / DATA_ID_COUNT
  print '%d ids moved, %.02f%%' % (moved_ids, percent_moved)

::

  4901707 ids moved, 49.02%

Okay, that is better. But still, moving 50% of our data to add 1% capacity
is not very good. If we examine what happened more closely we'll see what
is an "accordion effect". We shrunk node 0's range a bit to give to the
new node, but that shifted all the other node's ranges by the same amount.

We can minimize the change to a node's assigned range by assigning several
smaller ranges instead of the single broad range we were before. This can
be done by creating "virtual nodes" for each node. So 100 nodes might have
1000 virtual nodes. Let's examine how that might work.

.. code-block:: python

  from bisect import bisect_left
  from hashlib import md5
  from struct import unpack_from

  NODE_COUNT = 100
  DATA_ID_COUNT = 10000000
  VNODE_COUNT = 1000

  vnode_range_starts = []
  vnode2node = []
  for vnode_id in range(VNODE_COUNT):
      vnode_range_starts.append(DATA_ID_COUNT /
                                VNODE_COUNT * vnode_id)
      vnode2node.append(vnode_id % NODE_COUNT)
  new_vnode2node = list(vnode2node)
  new_node_id = NODE_COUNT
  NEW_NODE_COUNT = NODE_COUNT + 1
  vnodes_to_reassign = VNODE_COUNT / NEW_NODE_COUNT
  while vnodes_to_reassign > 0:
      for node_to_take_from in range(NODE_COUNT):
          for vnode_id, node_id in enumerate(new_vnode2node):
              if node_id == node_to_take_from:
                  new_vnode2node[vnode_id] = new_node_id
                  vnodes_to_reassign -= 1
                  break
          if vnodes_to_reassign <= 0:
              break
  moved_ids = 0
  for data_id in range(DATA_ID_COUNT):
      data_id = str(data_id)
      hsh = unpack_from('>I', md5(str(data_id)).digest())[0]
      vnode_id = bisect_left(vnode_range_starts,
                           hsh % DATA_ID_COUNT) % VNODE_COUNT
      node_id = vnode2node[vnode_id]
      new_node_id = new_vnode2node[vnode_id]
      if node_id != new_node_id:
          moved_ids += 1
  percent_moved = 100.0 * moved_ids / DATA_ID_COUNT
  print '%d ids moved, %.02f%%' % (moved_ids, percent_moved)

::

  90423 ids moved, 0.90%

There we go, we added 1% capacity and only moved 0.9% of existing data.
The vnode_range_starts list seems a bit out of place though. Its values
are calculated and never change for the lifetime of the cluster, so let's
optimize that out.

.. code-block:: python

  from bisect import bisect_left
  from hashlib import md5
  from struct import unpack_from

  NODE_COUNT = 100
  DATA_ID_COUNT = 10000000
  VNODE_COUNT = 1000

  vnode2node = []
  for vnode_id in range(VNODE_COUNT):
      vnode2node.append(vnode_id % NODE_COUNT)
  new_vnode2node = list(vnode2node)
  new_node_id = NODE_COUNT
  vnodes_to_reassign = VNODE_COUNT / (NODE_COUNT + 1)
  while vnodes_to_reassign > 0:
      for node_to_take_from in range(NODE_COUNT):
          for vnode_id, node_id in enumerate(vnode2node):
              if node_id == node_to_take_from:
                  vnode2node[vnode_id] = new_node_id
                  vnodes_to_reassign -= 1
                  break
          if vnodes_to_reassign <= 0:
              break
  moved_ids = 0
  for data_id in range(DATA_ID_COUNT):
      data_id = str(data_id)
      hsh = unpack_from('>I', md5(str(data_id)).digest())[0]
      vnode_id = hsh % VNODE_COUNT
      node_id = vnode2node[vnode_id]
      new_node_id = new_vnode2node[vnode_id]
      if node_id != new_node_id:
          moved_ids += 1
  percent_moved = 100.0 * moved_ids / DATA_ID_COUNT
  print '%d ids moved, %.02f%%' % (moved_ids, percent_moved)

::

  89841 ids moved, 0.90%

There we go. In the next part of this series, will further examine the
algorithm's limitations and how to improve on it.

Part 3
======
In Part 2 of this series, we reached an algorithm that performed well
even when adding new nodes to the cluster. We used 1000 virtual nodes
that could be independently assigned to nodes, allowing us to minimize
the amount of data moved when a node was added.

The number of virtual nodes puts a cap on how many real nodes you can
have. For example, if you have 1000 virtual nodes and you try to add a
1001st real node, you can't assign a virtual node to it without leaving
another real node with no assignment, leaving you with just 1000 active
real nodes still.

Unfortunately, the number of virtual nodes created at the beginning can
never change for the life of the cluster without a lot of careful work.
For example, you could double the virtual node count by splitting each
existing virtual node in half and assigning both halves to the same real
node. However, if the real node uses the virtual node's id to optimally
store the data (for example, all data might be stored in /[virtual node
id]/[data id]) it would have to move data around locally to reflect the
change. And it would have to resolve data using both the new and old
locations while the moves were taking place, making atomic operations
difficult or impossible.

Let's continue with this assumption that changing the virtual node
count is more work than it's worth, but keep in mind that some applications
might be fine with this.

The easiest way to deal with this limitation is to make the limit high
enough that it won't matter. For instance, if we decide our cluster will
never exceed 60,000 real nodes, we can just make 60,000 virtual nodes.

Also, we should include in our calculations the relative size of our
nodes. For instance, a year from now we might have real nodes that can
handle twice the capacity of our current nodes. So we'd want to assign
twice the virtual nodes to those future nodes, so maybe we should raise
our virtual node estimate to 120,000.

A good rule to follow might be to calculate 100 virtual nodes to each
real node at maximum capacity. This would allow you to alter the load
on any given node by 1%, even at max capacity, which is pretty fine
tuning. So now we're at 6,000,000 virtual nodes for a max capacity cluster
of 60,000 real nodes.

6 million virtual nodes seems like a lot, and it might seem like we'd
use up way too much memory. But the only structure this affects is the
virtual node to real node mapping. The base amount of memory required
would be 6 million times 2 bytes (to store a real node id from 0 to
65,535). 12 megabytes of memory just isn't that much to use these days.

Even with all the overhead of flexible data types, things aren't that
bad. I changed the code from the previous part in this series to have
60,000 real and 6,000,000 virtual nodes, changed the list to an array('H'),
and python topped out at 27m of resident memory – and that includes two
rings.

To change terminology a bit, we're going to start calling these virtual
nodes "partitions". This will make it a bit easier to discern between the
two types of nodes we've been talking about so far. Also, it makes sense
to talk about partitions as they are really just unchanging sections
of the hash space.

We're also going to always keep the partition count a power of two. This
makes it easy to just use bit manipulation on the hash to determine the
partition rather than modulus. It isn't much faster, but it is a little.
So, here's our updated ring code, using 8,388,608 (2 ** 23) partitions
and 65,536 nodes. We've upped the sample data id set and checked the
distribution to make sure we haven't broken anything.

.. code-block:: python

  from array import array
  from hashlib import md5
  from struct import unpack_from

  PARTITION_POWER = 23
  PARTITION_SHIFT = 32 - PARTITION_POWER
  NODE_COUNT = 65536
  DATA_ID_COUNT = 100000000

  part2node = array('H')
  for part in range(2 ** PARTITION_POWER):
      part2node.append(part % NODE_COUNT)
  node_counts = [0] * NODE_COUNT
  for data_id in range(DATA_ID_COUNT):
      data_id = str(data_id)
      part = unpack_from('>I',
          md5(str(data_id)).digest())[0] >> PARTITION_SHIFT
      node_id = part2node[part]
      node_counts[node_id] += 1
  desired_count = DATA_ID_COUNT / NODE_COUNT
  print '%d: Desired data ids per node' % desired_count
  max_count = max(node_counts)
  over = 100.0 * (max_count - desired_count) / desired_count
  print '%d: Most data ids on one node, %.02f%% over' % \
      (max_count, over)
  min_count = min(node_counts)
  under = 100.0 * (desired_count - min_count) / desired_count
  print '%d: Least data ids on one node, %.02f%% under' % \
      (min_count, under)

::

  1525: Desired data ids per node
  1683: Most data ids on one node, 10.36% over
  1360: Least data ids on one node, 10.82% under

Hmm. +–10% seems a bit high, but I reran with 65,536 partitions and
256 nodes and got +–0.4% so it's just that our sample size (100m) is
too small for our number of partitions (8m). It'll take way too long
to run experiments with an even larger sample size, so let's reduce
back down to these lesser numbers. (To be certain, I reran at the full
version with a 10 billion data id sample set and got +–1%, but it took
6.5 hours to run.)

In the next part of this series, we'll talk about how to increase the
durability of our data in the cluster.

Part 4
======
In Part 3 of this series, we just further discussed partitions (virtual
nodes) and cleaned up our code a bit based on that. Now, let's talk
about how to increase the durability and availability of our data in the
cluster.

For many distributed data stores, durability is quite important. Either
RAID arrays or individually distinct copies of data are required. While
RAID will increase the durability, it does nothing to increase the
availability – if the RAID machine crashes, the data may be safe but
inaccessible until repairs are done. If we keep distinct copies of the
data on different machines and a machine crashes, the other copies will
still be available while we repair the broken machine.

An easy way to gain this multiple copy durability/availability is to
just use multiple rings and groups of nodes. For instance, to achieve
the industry standard of three copies, you'd split the nodes into three
groups and each group would have its own ring and each would receive a
copy of each data item. This can work well enough, but has the drawback
that expanding capacity requires adding three nodes at a time and that
losing one node essentially lowers capacity by three times that node's
capacity.

Instead, let's use a different, but common, approach of meeting our
requirements with a single ring. This can be done by walking the ring
from the starting point and looking for additional distinct nodes.
Here's code that supports a variable number of replicas (set to 3 for
testing):

.. code-block:: python

  from array import array
  from hashlib import md5
  from struct import unpack_from

  REPLICAS = 3
  PARTITION_POWER = 16
  PARTITION_SHIFT = 32 - PARTITION_POWER
  PARTITION_MAX = 2 ** PARTITION_POWER - 1
  NODE_COUNT = 256
  DATA_ID_COUNT = 10000000

  part2node = array('H')
  for part in range(2 ** PARTITION_POWER):
      part2node.append(part % NODE_COUNT)
  node_counts = [0] * NODE_COUNT
  for data_id in range(DATA_ID_COUNT):
      data_id = str(data_id)
      part = unpack_from('>I',
          md5(str(data_id)).digest())[0] >> PARTITION_SHIFT
      node_ids = [part2node[part]]
      node_counts[node_ids[0]] += 1
      for replica in range(1, REPLICAS):
          while part2node[part] in node_ids:
              part += 1
              if part > PARTITION_MAX:
                  part = 0
          node_ids.append(part2node[part])
          node_counts[node_ids[-1]] += 1
  desired_count = DATA_ID_COUNT / NODE_COUNT * REPLICAS
  print '%d: Desired data ids per node' % desired_count
  max_count = max(node_counts)
  over = 100.0 * (max_count - desired_count) / desired_count
  print '%d: Most data ids on one node, %.02f%% over' % \
      (max_count, over)
  min_count = min(node_counts)
  under = 100.0 * (desired_count - min_count) / desired_count
  print '%d: Least data ids on one node, %.02f%% under' % \
      (min_count, under)

::

  117186: Desired data ids per node
  118133: Most data ids on one node, 0.81% over
  116093: Least data ids on one node, 0.93% under

That's pretty good; less than 1% over/under. While this works well,
there are a couple of problems.

First, because of how we've initially assigned the partitions to nodes,
all the partitions for a given node have their extra copies on the same
other two nodes. The problem here is that when a machine fails, the load
on these other nodes will jump by that amount. It'd be better if we
initially shuffled the partition assignment to distribute the failover
load better.

The other problem is a bit harder to explain, but deals with physical
separation of machines. Imagine you can only put 16 machines in a rack
in your datacenter. The 256 nodes we've been using would fill 16 racks.
With our current code, if a rack goes out (power problem, network issue,
etc.) there is a good chance some data will have all three copies in that
rack, becoming inaccessible. We can fix this shortcoming by adding the
concept of zones to our nodes, and then ensuring that replicas are stored
in distinct zones.

.. code-block:: python

  from array import array
  from hashlib import md5
  from random import shuffle
  from struct import unpack_from

  REPLICAS = 3
  PARTITION_POWER = 16
  PARTITION_SHIFT = 32 - PARTITION_POWER
  PARTITION_MAX = 2 ** PARTITION_POWER - 1
  NODE_COUNT = 256
  ZONE_COUNT = 16
  DATA_ID_COUNT = 10000000

  node2zone = []
  while len(node2zone) < NODE_COUNT:
      zone = 0
      while zone < ZONE_COUNT and len(node2zone) < NODE_COUNT:
          node2zone.append(zone)
          zone += 1
  part2node = array('H')
  for part in range(2 ** PARTITION_POWER):
      part2node.append(part % NODE_COUNT)
  shuffle(part2node)
  node_counts = [0] * NODE_COUNT
  zone_counts = [0] * ZONE_COUNT
  for data_id in range(DATA_ID_COUNT):
      data_id = str(data_id)
      part = unpack_from('>I',
          md5(str(data_id)).digest())[0] >> PARTITION_SHIFT
      node_ids = [part2node[part]]
      zones = [node2zone[node_ids[0]]]
      node_counts[node_ids[0]] += 1
      zone_counts[zones[0]] += 1
      for replica in range(1, REPLICAS):
          while part2node[part] in node_ids and \
                  node2zone[part2node[part]] in zones:
              part += 1
              if part > PARTITION_MAX:
                  part = 0
          node_ids.append(part2node[part])
          zones.append(node2zone[node_ids[-1]])
          node_counts[node_ids[-1]] += 1
          zone_counts[zones[-1]] += 1
  desired_count = DATA_ID_COUNT / NODE_COUNT * REPLICAS
  print '%d: Desired data ids per node' % desired_count
  max_count = max(node_counts)
  over = 100.0 * (max_count - desired_count) / desired_count
  print '%d: Most data ids on one node, %.02f%% over' % \
      (max_count, over)
  min_count = min(node_counts)
  under = 100.0 * (desired_count - min_count) / desired_count
  print '%d: Least data ids on one node, %.02f%% under' % \
      (min_count, under)
  desired_count = DATA_ID_COUNT / ZONE_COUNT * REPLICAS
  print '%d: Desired data ids per zone' % desired_count
  max_count = max(zone_counts)
  over = 100.0 * (max_count - desired_count) / desired_count
  print '%d: Most data ids in one zone, %.02f%% over' % \
      (max_count, over)
  min_count = min(zone_counts)
  under = 100.0 * (desired_count - min_count) / desired_count
  print '%d: Least data ids in one zone, %.02f%% under' % \
      (min_count, under)

::

  117186: Desired data ids per node
  118782: Most data ids on one node, 1.36% over
  115632: Least data ids on one node, 1.33% under
  1875000: Desired data ids per zone
  1878533: Most data ids in one zone, 0.19% over
  1869070: Least data ids in one zone, 0.32% under

So the shuffle and zone distinctions affected our distribution some,
but still definitely good enough. This test took about 64 seconds to
run on my machine.

There's a completely alternate, and quite common, way of accomplishing
these same requirements. This alternate method doesn't use partitions
at all, but instead just assigns anchors to the nodes within the hash
space. Finding the first node for a given hash just involves walking
this anchor ring for the next node, and finding additional nodes works
similarly as before. To attain the equivalent of our virtual nodes,
each real node is assigned multiple anchors.

.. code-block:: python

  from bisect import bisect_left
  from hashlib import md5
  from struct import unpack_from

  REPLICAS = 3
  NODE_COUNT = 256
  ZONE_COUNT = 16
  DATA_ID_COUNT = 10000000
  VNODE_COUNT = 100

  node2zone = []
  while len(node2zone) < NODE_COUNT:
      zone = 0
      while zone < ZONE_COUNT and len(node2zone) < NODE_COUNT:
          node2zone.append(zone)
          zone += 1
  hash2index = []
  index2node = []
  for node in range(NODE_COUNT):
      for vnode in range(VNODE_COUNT):
          hsh = unpack_from('>I', md5(str(node)).digest())[0]
          index = bisect_left(hash2index, hsh)
          if index > len(hash2index):
              index = 0
          hash2index.insert(index, hsh)
          index2node.insert(index, node)
  node_counts = [0] * NODE_COUNT
  zone_counts = [0] * ZONE_COUNT
  for data_id in range(DATA_ID_COUNT):
      data_id = str(data_id)
      hsh = unpack_from('>I', md5(str(data_id)).digest())[0]
      index = bisect_left(hash2index, hsh)
      if index >= len(hash2index):
          index = 0
      node_ids = [index2node[index]]
      zones = [node2zone[node_ids[0]]]
      node_counts[node_ids[0]] += 1
      zone_counts[zones[0]] += 1
      for replica in range(1, REPLICAS):
          while index2node[index] in node_ids and \
                  node2zone[index2node[index]] in zones:
              index += 1
              if index >= len(hash2index):
                  index = 0
          node_ids.append(index2node[index])
          zones.append(node2zone[node_ids[-1]])
          node_counts[node_ids[-1]] += 1
          zone_counts[zones[-1]] += 1
  desired_count = DATA_ID_COUNT / NODE_COUNT * REPLICAS
  print '%d: Desired data ids per node' % desired_count
  max_count = max(node_counts)
  over = 100.0 * (max_count - desired_count) / desired_count
  print '%d: Most data ids on one node, %.02f%% over' % \
      (max_count, over)
  min_count = min(node_counts)
  under = 100.0 * (desired_count - min_count) / desired_count
  print '%d: Least data ids on one node, %.02f%% under' % \
      (min_count, under)
  desired_count = DATA_ID_COUNT / ZONE_COUNT * REPLICAS
  print '%d: Desired data ids per zone' % desired_count
  max_count = max(zone_counts)
  over = 100.0 * (max_count - desired_count) / desired_count
  print '%d: Most data ids in one zone, %.02f%% over' % \
      (max_count, over)
  min_count = min(zone_counts)
  under = 100.0 * (desired_count - min_count) / desired_count
  print '%d: Least data ids in one zone, %.02f%% under' % \
      (min_count, under)

::

  117186: Desired data ids per node
  351282: Most data ids on one node, 199.76% over
  15965: Least data ids on one node, 86.38% under
  1875000: Desired data ids per zone
  2248496: Most data ids in one zone, 19.92% over
  1378013: Least data ids in one zone, 26.51% under

This test took over 15 minutes to run! Unfortunately, this method also
gives much less control over the distribution. To get better distribution,
you have to add more virtual nodes, which eats up more memory and takes
even more time to build the ring and perform distinct node lookups. The
most common operation, data id lookup, can be improved (by predetermining
each virtual node's failover nodes, for instance) but it starts off so
far behind our first approach that we'll just stick with that.

In the next part of this series, we'll start to wrap all this up into
a useful Python module.

Part 5
======
In Part 4 of this series, we ended up with a multiple copy, distinctly
zoned ring. Or at least the start of it. In this final part we'll package
the code up into a useable Python module and then add one last feature.
First, let's separate the ring itself from the building of the data for
the ring and its testing.

.. code-block:: python

  from array import array
  from hashlib import md5
  from random import shuffle
  from struct import unpack_from
  from time import time

  class Ring(object):

      def __init__(self, nodes, part2node, replicas):
          self.nodes = nodes
          self.part2node = part2node
          self.replicas = replicas
          partition_power = 1
          while 2 ** partition_power < len(part2node):
              partition_power += 1
          if len(part2node) != 2 ** partition_power:
              raise Exception("part2node's length is not an "
                              "exact power of 2")
          self.partition_shift = 32 - partition_power

      def get_nodes(self, data_id):
          data_id = str(data_id)
          part = unpack_from('>I',
             md5(data_id).digest())[0] >> self.partition_shift
          node_ids = [self.part2node[part]]
          zones = [self.nodes[node_ids[0]]]
          for replica in range(1, self.replicas):
              while self.part2node[part] in node_ids and \
                     self.nodes[self.part2node[part]] in zones:
                  part += 1
                  if part >= len(self.part2node):
                      part = 0
              node_ids.append(self.part2node[part])
              zones.append(self.nodes[node_ids[-1]])
          return [self.nodes[n] for n in node_ids]

  def build_ring(nodes, partition_power, replicas):
      begin = time()
      part2node = array('H')
      for part in range(2 ** partition_power):
          part2node.append(part % len(nodes))
      shuffle(part2node)
      ring = Ring(nodes, part2node, replicas)
      print '%.02fs to build ring' % (time() - begin)
      return ring

  def test_ring(ring):
      begin = time()
      DATA_ID_COUNT = 10000000
      node_counts = {}
      zone_counts = {}
      for data_id in range(DATA_ID_COUNT):
          for node in ring.get_nodes(data_id):
              node_counts[node['id']] = \
                  node_counts.get(node['id'], 0) + 1
              zone_counts[node['zone']] = \
                  zone_counts.get(node['zone'], 0) + 1
      print '%ds to test ring' % (time() - begin)
      desired_count = \
          DATA_ID_COUNT / len(ring.nodes) * REPLICAS
      print '%d: Desired data ids per node' % desired_count
      max_count = max(node_counts.values())
      over = \
          100.0 * (max_count - desired_count) / desired_count
      print '%d: Most data ids on one node, %.02f%% over' % \
          (max_count, over)
      min_count = min(node_counts.values())
      under = \
          100.0 * (desired_count - min_count) / desired_count
      print '%d: Least data ids on one node, %.02f%% under' % \
          (min_count, under)
      zone_count = \
          len(set(n['zone'] for n in ring.nodes.values()))
      desired_count = \
          DATA_ID_COUNT / zone_count * ring.replicas
      print '%d: Desired data ids per zone' % desired_count
      max_count = max(zone_counts.values())
      over = \
          100.0 * (max_count - desired_count) / desired_count
      print '%d: Most data ids in one zone, %.02f%% over' % \
          (max_count, over)
      min_count = min(zone_counts.values())
      under = \
          100.0 * (desired_count - min_count) / desired_count
      print '%d: Least data ids in one zone, %.02f%% under' % \
          (min_count, under)

  if __name__ == '__main__':
      PARTITION_POWER = 16
      REPLICAS = 3
      NODE_COUNT = 256
      ZONE_COUNT = 16
      nodes = {}
      while len(nodes) < NODE_COUNT:
          zone = 0
          while zone < ZONE_COUNT and len(nodes) < NODE_COUNT:
              node_id = len(nodes)
              nodes[node_id] = {'id': node_id, 'zone': zone}
              zone += 1
      ring = build_ring(nodes, PARTITION_POWER, REPLICAS)
      test_ring(ring)

::

  0.06s to build ring
  82s to test ring
  117186: Desired data ids per node
  118773: Most data ids on one node, 1.35% over
  115801: Least data ids on one node, 1.18% under
  1875000: Desired data ids per zone
  1878339: Most data ids in one zone, 0.18% over
  1869914: Least data ids in one zone, 0.27% under

It takes a bit longer to test our ring, but that's mostly because of
the switch to dictionaries from arrays for various items. Having node
dictionaries is nice because you can attach any node information you
want directly there (ip addresses, tcp ports, drive paths, etc.). But
we're still on track for further testing; our distribution is still good.

Now, let's add our one last feature to our ring: the concept of weights.
Weights are useful because the nodes you add later in a ring's life are
likely to have more capacity than those you have at the outset. For this
test, we'll make half our nodes have twice the weight. We'll have to
change build_ring to give more partitions to the nodes with more weight
and we'll change test_ring to take into account these weights. Since
we've changed so much I'll just post the entire module again:

.. code-block:: python

  from array import array
  from hashlib import md5
  from random import shuffle
  from struct import unpack_from
  from time import time

  class Ring(object):

      def __init__(self, nodes, part2node, replicas):
          self.nodes = nodes
          self.part2node = part2node
          self.replicas = replicas
          partition_power = 1
          while 2 ** partition_power < len(part2node):
              partition_power += 1
          if len(part2node) != 2 ** partition_power:
              raise Exception("part2node's length is not an "
                              "exact power of 2")
          self.partition_shift = 32 - partition_power

      def get_nodes(self, data_id):
          data_id = str(data_id)
          part = unpack_from('>I',
             md5(data_id).digest())[0] >> self.partition_shift
          node_ids = [self.part2node[part]]
          zones = [self.nodes[node_ids[0]]]
          for replica in range(1, self.replicas):
              while self.part2node[part] in node_ids and \
                     self.nodes[self.part2node[part]] in zones:
                  part += 1
                  if part >= len(self.part2node):
                      part = 0
              node_ids.append(self.part2node[part])
              zones.append(self.nodes[node_ids[-1]])
          return [self.nodes[n] for n in node_ids]

  def build_ring(nodes, partition_power, replicas):
      begin = time()
      parts = 2 ** partition_power
      total_weight = \
          float(sum(n['weight'] for n in nodes.values()))
      for node in nodes.values():
          node['desired_parts'] = \
              parts / total_weight * node['weight']
      part2node = array('H')
      for part in range(2 ** partition_power):
          for node in nodes.values():
              if node['desired_parts'] >= 1:
                  node['desired_parts'] -= 1
                  part2node.append(node['id'])
                  break
          else:
              for node in nodes.values():
                  if node['desired_parts'] >= 0:
                      node['desired_parts'] -= 1
                      part2node.append(node['id'])
                      break
      shuffle(part2node)
      ring = Ring(nodes, part2node, replicas)
      print '%.02fs to build ring' % (time() - begin)
      return ring

  def test_ring(ring):
      begin = time()
      DATA_ID_COUNT = 10000000
      node_counts = {}
      zone_counts = {}
      for data_id in range(DATA_ID_COUNT):
          for node in ring.get_nodes(data_id):
              node_counts[node['id']] = \
                  node_counts.get(node['id'], 0) + 1
              zone_counts[node['zone']] = \
                  zone_counts.get(node['zone'], 0) + 1
      print '%ds to test ring' % (time() - begin)
      total_weight = float(sum(n['weight'] for n in
                               ring.nodes.values()))
      max_over = 0
      max_under = 0
      for node in ring.nodes.values():
          desired = DATA_ID_COUNT * REPLICAS * \
              node['weight'] / total_weight
          diff = node_counts[node['id']] - desired
          if diff > 0:
              over = 100.0 * diff / desired
              if over > max_over:
                  max_over = over
          else:
              under = 100.0 * (-diff) / desired
              if under > max_under:
                  max_under = under
      print '%.02f%% max node over' % max_over
      print '%.02f%% max node under' % max_under
      max_over = 0
      max_under = 0
      for zone in set(n['zone'] for n in
                      ring.nodes.values()):
          zone_weight = sum(n['weight'] for n in
              ring.nodes.values() if n['zone'] == zone)
          desired = DATA_ID_COUNT * REPLICAS * \
              zone_weight / total_weight
          diff = zone_counts[zone] - desired
          if diff > 0:
              over = 100.0 * diff / desired
              if over > max_over:
                  max_over = over
          else:
              under = 100.0 * (-diff) / desired
              if under > max_under:
                  max_under = under
      print '%.02f%% max zone over' % max_over
      print '%.02f%% max zone under' % max_under

  if __name__ == '__main__':
      PARTITION_POWER = 16
      REPLICAS = 3
      NODE_COUNT = 256
      ZONE_COUNT = 16
      nodes = {}
      while len(nodes) < NODE_COUNT:
          zone = 0
          while zone < ZONE_COUNT and len(nodes) < NODE_COUNT:
              node_id = len(nodes)
              nodes[node_id] = {'id': node_id, 'zone': zone,
                                'weight': 1.0 + (node_id % 2)}
              zone += 1
      ring = build_ring(nodes, PARTITION_POWER, REPLICAS)
      test_ring(ring)

::

  0.88s to build ring
  86s to test ring
  1.66% max over
  1.46% max under
  0.28% max zone over
  0.23% max zone under

So things are still good, even though we have differently weighted nodes.
I ran another test with this code using random weights from 1 to 100 and
got over/under values for nodes of 7.35%/18.12% and zones of 0.24%/0.22%,
still pretty good considering the crazy weight ranges.

Summary
=======
Hopefully this series has been a good introduction to building a ring.
This code is essentially how the OpenStack Swift ring works, except that
Swift's ring has lots of additional optimizations, such as storing each
replica assignment separately, and lots of extra features for building,
validating, and otherwise working with rings.
