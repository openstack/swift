===============
Global Clusters
===============

--------
Overview
--------

Swift's default configuration is currently designed to work in a
single region, where a region is defined as a group of machines with
high-bandwidth, low-latency links between them. However, configuration
options exist that make running a performant multi-region Swift
cluster possible.

For the rest of this section, we will assume a two-region Swift
cluster: region 1 in San Francisco (SF), and region 2 in New York
(NY). Each region shall contain within it 3 zones, numbered 1, 2, and
3, for a total of 6 zones.

.. _configuring_global_clusters:

---------------------------
Configuring Global Clusters
---------------------------

.. note::

    The proxy-server configuration options described below can be given generic
    settings in the ``[app:proxy-server]`` configuration section and/or given
    specific settings for individual policies using
    :ref:`proxy_server_per_policy_config`.

~~~~~~~~~~~~~
read_affinity
~~~~~~~~~~~~~

This setting, combined with sorting_method setting, makes the proxy
server prefer local backend servers for GET and HEAD requests over
non-local ones. For example, it is preferable for an SF proxy server
to service object GET requests by talking to SF object servers, as the
client will receive lower latency and higher throughput.

By default, Swift randomly chooses one of the three replicas to give
to the client, thereby spreading the load evenly. In the case of a
geographically-distributed cluster, the administrator is likely to
prioritize keeping traffic local over even distribution of results.
This is where the read_affinity setting comes in.

Example::

    [app:proxy-server]
    sorting_method = affinity
    read_affinity = r1=100

This will make the proxy attempt to service GET and HEAD requests from
backends in region 1 before contacting any backends in region 2.
However, if no region 1 backends are available (due to replica
placement, failed hardware, or other reasons), then the proxy will
fall back to backend servers in other regions.

Example::

    [app:proxy-server]
    sorting_method = affinity
    read_affinity = r1z1=100, r1=200

This will make the proxy attempt to service GET and HEAD requests from
backends in region 1 zone 1, then backends in region 1, then any other
backends. If a proxy is physically close to a particular zone or
zones, this can provide bandwidth savings. For example, if a zone
corresponds to servers in a particular rack, and the proxy server is
in that same rack, then setting read_affinity to prefer reads from
within the rack will result in less traffic between the top-of-rack
switches.

The read_affinity setting may contain any number of region/zone
specifiers; the priority number (after the equals sign) determines the
ordering in which backend servers will be contacted. A lower number
means higher priority.

Note that read_affinity only affects the ordering of primary nodes
(see ring docs for definition of primary node), not the ordering of
handoff nodes.

~~~~~~~~~~~~~~
write_affinity
~~~~~~~~~~~~~~

This setting makes the proxy server prefer local backend servers for
object PUT requests over non-local ones. For example, it may be
preferable for an SF proxy server to service object PUT requests
by talking to SF object servers, as the client will receive lower
latency and higher throughput. However, if this setting is used, note
that a NY proxy server handling a GET request for an object that was
PUT using write affinity may have to fetch it across the WAN link, as
the object won't immediately have any replicas in NY. However,
replication will move the object's replicas to their proper homes in
both SF and NY.

One potential issue with write_affinity is, end user may get 404 error when
deleting objects before replication. The write_affinity_handoff_delete_count
setting is used together with write_affinity in order to solve that issue.
With its default configuration, Swift will calculate the proper number of
handoff nodes to send requests to.

Note that only object PUT/DELETE requests are affected by the write_affinity
setting; POST, GET, HEAD, OPTIONS, and account/container PUT requests are
not affected.

This setting lets you trade data distribution for throughput. If
write_affinity is enabled, then object replicas will initially be
stored all within a particular region or zone, thereby decreasing the
quality of the data distribution, but the replicas will be distributed
over fast WAN links, giving higher throughput to clients. Note that
the replicators will eventually move objects to their proper,
well-distributed homes.

The write_affinity setting is useful only when you don't typically
read objects immediately after writing them. For example, consider a
workload of mainly backups: if you have a bunch of machines in NY that
periodically write backups to Swift, then odds are that you don't then
immediately read those backups in SF. If your workload doesn't look
like that, then you probably shouldn't use write_affinity.

The write_affinity_node_count setting is only useful in conjunction
with write_affinity; it governs how many local object servers will be
tried before falling back to non-local ones.

Example::

    [app:proxy-server]
    write_affinity = r1
    write_affinity_node_count = 2 * replicas

Assuming 3 replicas, this configuration will make object PUTs try
storing the object's replicas on up to 6 disks ("2 * replicas") in
region 1 ("r1"). Proxy server tries to find 3 devices for storing the
object. While a device is unavailable, it queries the ring for the 4th
device and so on until 6th device. If the 6th disk is still unavailable,
the last replica will be sent to other region. It doesn't mean there'll
have 6 replicas in region 1.


You should be aware that, if you have data coming into SF faster than
your replicators are transferring it to NY, then your cluster's data
distribution will get worse and worse over time as objects pile up in SF.
If this happens, it is recommended to disable write_affinity and simply let
object PUTs traverse the WAN link, as that will naturally limit the
object growth rate to what your WAN link can handle.
