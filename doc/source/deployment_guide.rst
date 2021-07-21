
Deployment Guide
================

This document provides general guidance for deploying and configuring Swift.
Detailed descriptions of configuration options can be found in the
:doc:`configuration documentation <config/index>`.

-----------------------
Hardware Considerations
-----------------------

Swift is designed to run on commodity hardware. RAID on the storage drives is
not required and not recommended. Swift's disk usage pattern is the worst case
possible for RAID, and performance degrades very quickly using RAID 5 or 6.

------------------
Deployment Options
------------------

The Swift services run completely autonomously, which provides for a lot of
flexibility when architecting the hardware deployment for Swift. The 4 main
services are:

#. Proxy Services
#. Object Services
#. Container Services
#. Account Services

The Proxy Services are more CPU and network I/O intensive. If you are using
10g networking to the proxy, or are terminating SSL traffic at the proxy,
greater CPU power will be required.

The Object, Container, and Account Services (Storage Services) are more disk
and network I/O intensive.

The easiest deployment is to install all services on each server. There is
nothing wrong with doing this, as it scales each service out horizontally.

Alternatively, one set of servers may be dedicated to the Proxy Services and a
different set of servers dedicated to the Storage Services. This allows faster
networking to be configured to the proxy than the storage servers, and keeps
load balancing to the proxies more manageable.  Storage Services scale out
horizontally as storage servers are added, and the overall API throughput can
be scaled by adding more proxies.

If you need more throughput to either Account or Container Services, they may
each be deployed to their own servers. For example you might use faster (but
more expensive) SAS or even SSD drives to get faster disk I/O to the databases.

A high-availability (HA) deployment of Swift requires that multiple proxy
servers are deployed and requests are load-balanced between them. Each proxy
server instance is stateless and able to respond to requests for the entire
cluster.

Load balancing and network design is left as an exercise to the reader,
but this is a very important part of the cluster, so time should be spent
designing the network for a Swift cluster.


---------------------
Web Front End Options
---------------------

Swift comes with an integral web front end. However, it can also be deployed
as a request processor of an Apache2 using mod_wsgi as described in
:doc:`Apache Deployment Guide <apache_deployment_guide>`.

.. _ring-preparing:

------------------
Preparing the Ring
------------------

The first step is to determine the number of partitions that will be in the
ring. We recommend that there be a minimum of 100 partitions per drive to
insure even distribution across the drives. A good starting point might be
to figure out the maximum number of drives the cluster will contain, and then
multiply by 100, and then round up to the nearest power of two.

For example, imagine we are building a cluster that will have no more than
5,000 drives. That would mean that we would have a total number of 500,000
partitions, which is pretty close to 2^19, rounded up.

It is also a good idea to keep the number of partitions small (relatively).
The more partitions there are, the more work that has to be done by the
replicators and other backend jobs and the more memory the rings consume in
process. The goal is to find a good balance between small rings and maximum
cluster size.

The next step is to determine the number of replicas to store of the data.
Currently it is recommended to use 3 (as this is the only value that has
been tested). The higher the number, the more storage that is used but the
less likely you are to lose data.

It is also important to determine how many zones the cluster should have. It is
recommended to start with a minimum of 5 zones. You can start with fewer, but
our testing has shown that having at least five zones is optimal when failures
occur. We also recommend trying to configure the zones at as high a level as
possible to create as much isolation as possible. Some example things to take
into consideration can include physical location, power availability, and
network connectivity. For example, in a small cluster you might decide to
split the zones up by cabinet, with each cabinet having its own power and
network connectivity. The zone concept is very abstract, so feel free to use
it in whatever way best isolates your data from failure. Each zone exists
in a region.

A region is also an abstract concept that may be used to distinguish between
geographically separated areas as well as can be used within same datacenter.
Regions and zones are referenced by a positive integer.

You can now start building the ring with::

    swift-ring-builder <builder_file> create <part_power> <replicas> <min_part_hours>

This will start the ring build process creating the <builder_file> with
2^<part_power> partitions. <min_part_hours> is the time in hours before a
specific partition can be moved in succession (24 is a good value for this).

Devices can be added to the ring with::

    swift-ring-builder <builder_file> add r<region>z<zone>-<ip>:<port>/<device_name>_<meta> <weight>

This will add a device to the ring where <builder_file> is the name of the
builder file that was created previously, <region> is the number of the region
the zone is in, <zone> is the number of the zone this device is in, <ip> is
the ip address of the server the device is in, <port> is the port number that
the server is running on, <device_name> is the name of the device on the server
(for example: sdb1), <meta> is a string of metadata for the device (optional),
and <weight> is a float weight that determines how many partitions are put on
the device relative to the rest of the devices in the cluster (a good starting
point is 100.0 x TB on the drive).Add each device that will be initially in the
cluster.

Once all of the devices are added to the ring, run::

    swift-ring-builder <builder_file> rebalance

This will distribute the partitions across the drives in the ring. It is
important whenever making changes to the ring to make all the changes
required before running rebalance. This will ensure that the ring stays as
balanced as possible, and as few partitions are moved as possible.

The above process should be done to make a ring for each storage service
(Account, Container and Object). The builder files will be needed in future
changes to the ring, so it is very important that these be kept and backed up.
The resulting .tar.gz ring file should be pushed to all of the servers in the
cluster. For more information about building rings, running
swift-ring-builder with no options will display help text with available
commands and options. More information on how the ring works internally
can be found in the :doc:`Ring Overview <overview_ring>`.

.. _server-per-port-configuration:

-------------------------------
Running object-servers Per Disk
-------------------------------

The lack of true asynchronous file I/O on Linux leaves the object-server
workers vulnerable to misbehaving disks.  Because any object-server worker can
service a request for any disk, and a slow I/O request blocks the eventlet hub,
a single slow disk can impair an entire storage node.  This also prevents
object servers from fully utilizing all their disks during heavy load.

Another way to get full I/O isolation is to give each disk on a storage node a
different port in the storage policy rings.  Then set the
:ref:`servers_per_port <object-server-default-options>`
option in the object-server config.  NOTE: while the purpose of this config
setting is to run one or more object-server worker processes per *disk*, the
implementation just runs object-servers per unique port of local devices in the
rings.  The deployer must combine this option with appropriately-configured
rings to benefit from this feature.

Here's an example (abbreviated) old-style ring (2 node cluster with 2 disks
each)::

 Devices:    id  region  zone      ip address  port  replication ip  replication port      name
              0       1     1       1.1.0.1    6200       1.1.0.1                6200      d1
              1       1     1       1.1.0.1    6200       1.1.0.1                6200      d2
              2       1     2       1.1.0.2    6200       1.1.0.2                6200      d3
              3       1     2       1.1.0.2    6200       1.1.0.2                6200      d4

And here's the same ring set up for ``servers_per_port``::

 Devices:    id  region  zone      ip address  port  replication ip  replication port      name
              0       1     1       1.1.0.1    6200       1.1.0.1                6200      d1
              1       1     1       1.1.0.1    6201       1.1.0.1                6201      d2
              2       1     2       1.1.0.2    6200       1.1.0.2                6200      d3
              3       1     2       1.1.0.2    6201       1.1.0.2                6201      d4

When migrating from normal to ``servers_per_port``, perform these steps in order:

#. Upgrade Swift code to a version capable of doing ``servers_per_port``.

#. Enable ``servers_per_port`` with a value greater than zero.

#. Restart ``swift-object-server`` processes with a SIGHUP.  At this point, you
   will have the ``servers_per_port`` number of ``swift-object-server`` processes
   serving all requests for all disks on each node.  This preserves
   availability, but you should perform the next step as quickly as possible.

#. Push out new rings that actually have different ports per disk on each
   server.  One of the ports in the new ring should be the same as the port
   used in the old ring ("6200" in the example above).  This will cover
   existing proxy-server processes who haven't loaded the new ring yet.  They
   can still talk to any storage node regardless of whether or not that
   storage node has loaded the ring and started object-server processes on the
   new ports.

If you do not run a separate object-server for replication, then this setting
must be available to the object-replicator and object-reconstructor (i.e.
appear in the [DEFAULT] config section).

.. _general-service-configuration:

-----------------------------
General Service Configuration
-----------------------------

Most Swift services fall into two categories.  Swift's wsgi servers and
background daemons.

For more information specific to the configuration of Swift's wsgi servers
with paste deploy see :ref:`general-server-configuration`.

Configuration for servers and daemons can be expressed together in the same
file for each type of server, or separately.  If a required section for the
service trying to start is missing there will be an error.  The sections not
used by the service are ignored.

Consider the example of an object storage node.  By convention, configuration
for the object-server, object-updater, object-replicator, object-auditor, and
object-reconstructor exist in a single file ``/etc/swift/object-server.conf``::

    [DEFAULT]
    reclaim_age = 604800

    [pipeline:main]
    pipeline = object-server

    [app:object-server]
    use = egg:swift#object

    [object-replicator]

    [object-updater]

    [object-auditor]

Swift services expect a configuration path as the first argument::

    $ swift-object-auditor
    Usage: swift-object-auditor CONFIG [options]

    Error: missing config path argument

If you omit the object-auditor section this file could not be used as the
configuration path when starting the ``swift-object-auditor`` daemon::

    $ swift-object-auditor /etc/swift/object-server.conf
    Unable to find object-auditor config section in /etc/swift/object-server.conf

If the configuration path is a directory instead of a file all of the files in
the directory with the file extension ".conf" will be combined to generate the
configuration object which is delivered to the Swift service.  This is
referred to generally as "directory based configuration".

Directory based configuration leverages ConfigParser's native multi-file
support.  Files ending in ".conf" in the given directory are parsed in
lexicographical order.  Filenames starting with '.' are ignored.  A mixture of
file and directory configuration paths is not supported - if the configuration
path is a file only that file will be parsed.

The Swift service management tool ``swift-init`` has adopted the convention of
looking for ``/etc/swift/{type}-server.conf.d/`` if the file
``/etc/swift/{type}-server.conf`` file does not exist.

When using directory based configuration, if the same option under the same
section appears more than once in different files, the last value parsed is
said to override previous occurrences.  You can ensure proper override
precedence by prefixing the files in the configuration directory with
numerical values.::

    /etc/swift/
        default.base
        object-server.conf.d/
            000_default.conf -> ../default.base
            001_default-override.conf
            010_server.conf
            020_replicator.conf
            030_updater.conf
            040_auditor.conf

You can inspect the resulting combined configuration object using the
``swift-config`` command line tool

.. _general-server-configuration:

----------------------------
General Server Configuration
----------------------------

Swift uses paste.deploy (https://pypi.org/project/Paste/) to manage server
configurations. Detailed descriptions of configuration options can be found in
the :doc:`configuration documentation <config/index>`.

Default configuration options are set in the ``[DEFAULT]`` section, and any
options specified there can be overridden in any of the other sections BUT
ONLY BY USING THE SYNTAX ``set option_name = value``. This is the unfortunate
way paste.deploy works and I'll try to explain it in full.

First, here's an example paste.deploy configuration file::

    [DEFAULT]
    name1 = globalvalue
    name2 = globalvalue
    name3 = globalvalue
    set name4 = globalvalue

    [pipeline:main]
    pipeline = myapp

    [app:myapp]
    use = egg:mypkg#myapp
    name2 = localvalue
    set name3 = localvalue
    set name5 = localvalue
    name6 = localvalue

The resulting configuration that myapp receives is::

    global {'__file__': '/etc/mypkg/wsgi.conf', 'here': '/etc/mypkg',
            'name1': 'globalvalue',
            'name2': 'globalvalue',
            'name3': 'localvalue',
            'name4': 'globalvalue',
            'name5': 'localvalue',
            'set name4': 'globalvalue'}
    local {'name6': 'localvalue'}

So, ``name1`` got the global value which is fine since it's only in the ``DEFAULT``
section anyway.

``name2`` got the global value from ``DEFAULT`` even though it appears to be
overridden in the ``app:myapp`` subsection. This is just the unfortunate way
paste.deploy works (at least at the time of this writing.)

``name3`` got the local value from the ``app:myapp`` subsection because it is using
the special paste.deploy syntax of ``set option_name = value``. So, if you want
a default value for most app/filters but want to override it in one
subsection, this is how you do it.

``name4`` got the global value from ``DEFAULT`` since it's only in that section
anyway. But, since we used the ``set`` syntax in the ``DEFAULT`` section even
though we shouldn't, notice we also got a ``set name4`` variable. Weird, but
probably not harmful.

``name5`` got the local value from the ``app:myapp`` subsection since it's only
there anyway, but notice that it is in the global configuration and not the
local configuration. This is because we used the ``set`` syntax to set the
value. Again, weird, but not harmful since Swift just treats the two sets of
configuration values as one set anyway.

``name6`` got the local value from ``app:myapp`` subsection since it's only there,
and since we didn't use the ``set`` syntax, it's only in the local
configuration and not the global one. Though, as indicated above, there is no
special distinction with Swift.

That's quite an explanation for something that should be so much simpler, but
it might be important to know how paste.deploy interprets configuration files.
The main rule to remember when working with Swift configuration files is:

.. note::

    Use the ``set option_name = value`` syntax in subsections if the option is
    also set in the ``[DEFAULT]`` section. Don't get in the habit of always
    using the ``set`` syntax or you'll probably mess up your non-paste.deploy
    configuration files.


.. _proxy_server_per_policy_config:

************************
Per policy configuration
************************

Some proxy-server configuration options may be overridden for individual
:doc:`overview_policies` by including per-policy config section(s). These
options are:

- ``sorting_method``
- ``read_affinity``
- ``write_affinity``
- ``write_affinity_node_count``
- ``write_affinity_handoff_delete_count``

The per-policy config section name must be of the form::

    [proxy-server:policy:<policy index>]

.. note::

    The per-policy config section name should refer to the policy index, not
    the policy name.

.. note::

    The first part of proxy-server config section name must match the name of
    the proxy-server config section. This is typically ``proxy-server`` as
    shown above, but if different then the names of any per-policy config
    sections must be changed accordingly.

The value of an option specified in a per-policy section will override any
value given in the proxy-server section for that policy only. Otherwise the
value of these options will be that specified in the proxy-server section.

For example, the following section provides policy-specific options for a
policy with index ``3``::

    [proxy-server:policy:3]
    sorting_method = affinity
    read_affinity = r2=1
    write_affinity = r2
    write_affinity_node_count = 1 * replicas
    write_affinity_handoff_delete_count = 2

.. note::

    It is recommended that per-policy config options are *not* included in the
    ``[DEFAULT]`` section. If they are then the following behavior applies.

    Per-policy config sections will inherit options in the ``[DEFAULT]``
    section of the config file, and any such inheritance will take precedence
    over inheriting options from the proxy-server config section.

    Per-policy config section options will override options in the
    ``[DEFAULT]`` section. Unlike the behavior described under `General Server
    Configuration`_ for paste-deploy ``filter`` and ``app`` sections, the
    ``set`` keyword is not required for options to override in per-policy
    config sections.

    For example, given the following settings in a config file::

        [DEFAULT]
        sorting_method = affinity
        read_affinity = r0=100
        write_affinity = r0

        [app:proxy-server]
        use = egg:swift#proxy
        # use of set keyword here overrides [DEFAULT] option
        set read_affinity = r1=100
        # without set keyword, [DEFAULT] option overrides in a paste-deploy section
        write_affinity = r1

        [proxy-server:policy:0]
        sorting_method = affinity
        # set keyword not required here to override [DEFAULT] option
        write_affinity = r1

    would result in policy with index ``0`` having settings:

    * ``read_affinity = r0=100`` (inherited from the ``[DEFAULT]`` section)
    * ``write_affinity = r1`` (specified in the policy 0 section)

    and any other policy would have the default settings of:

    * ``read_affinity = r1=100`` (set in the proxy-server section)
    * ``write_affinity = r0`` (inherited from the ``[DEFAULT]`` section)

*****************
Proxy Middlewares
*****************

Many features in Swift are implemented as middleware in the proxy-server
pipeline. See :doc:`middleware` and the ``proxy-server.conf-sample`` file for
more information. In particular, the use of some type of :doc:`authentication
and authorization middleware <overview_auth>` is highly recommended.


------------------------
Memcached Considerations
------------------------

Several of the Services rely on Memcached for caching certain types of lookups,
such as auth tokens, and container/account existence.  Swift does not do any
caching of actual object data.  Memcached should be able to run on any servers
that have available RAM and CPU.  Typically Memcached is run on the proxy
servers.  The ``memcache_servers`` config option in the ``proxy-server.conf``
should contain all memcached servers.

*************************
Shard Range Listing Cache
*************************

When a container gets :ref:`sharded<sharding_doc>` the root container will still be the
primary entry point to many container requests, as it provides the list of shards.
To take load off the root container Swift by default caches the list of shards returned.

As the number of shards for a root container grows to more than 3k the memcache default max
size of 1MB can be reached.

If you over-run your max configured memcache size you'll see messages like::

  Error setting value in memcached: 127.0.0.1:11211: SERVER_ERROR object too large for cache

When you see these messages your root containers are getting hammered and
probably returning 503 reponses to clients.  Override the default 1MB limit to
5MB with something like::

  /usr/bin/memcached -I 5000000 ...

Memcache has a ``stats sizes`` option that can point out the current size usage. As this
reaches the current max an increase might be in order::

  # telnet <memcache server> 11211
  > stats sizes
  STAT 160 2
  STAT 448 1
  STAT 576 1
  END


-----------
System Time
-----------

Time may be relative but it is relatively important for Swift!  Swift uses
timestamps to determine which is the most recent version of an object.
It is very important for the system time on each server in the cluster to
by synced as closely as possible (more so for the proxy server, but in general
it is a good idea for all the servers).  Typical deployments use NTP with a
local NTP server to ensure that the system times are as close as possible.
This should also be monitored to ensure that the times do not vary too much.

.. _general-service-tuning:

----------------------
General Service Tuning
----------------------

Most services support either a ``workers`` or ``concurrency`` value in the
settings.  This allows the services to make effective use of the cores
available. A good starting point is to set the concurrency level for the proxy
and storage services to 2 times the number of cores available. If more than
one service is sharing a server, then some experimentation may be needed to
find the best balance.

For example, one operator reported using the following settings in a production
Swift cluster:

- Proxy servers have dual quad core processors (i.e. 8 cores); testing has
  shown 16 workers to be a pretty good balance when saturating a 10g network
  and gives good CPU utilization.

- Storage server processes all run together on the same servers. These servers
  have dual quad core processors, for 8 cores total. The Account, Container,
  and Object servers are run with 8 workers each. Most of the background jobs
  are run at a concurrency of 1, with the exception of the replicators which
  are run at a concurrency of 2.

The ``max_clients`` parameter can be used to adjust the number of client
requests an individual worker accepts for processing. The fewer requests being
processed at one time, the less likely a request that consumes the worker's
CPU time, or blocks in the OS, will negatively impact other requests. The more
requests being processed at one time, the more likely one worker can utilize
network and disk capacity.

On systems that have more cores, and more memory, where one can afford to run
more workers, raising the number of workers and lowering the maximum number of
clients serviced per worker can lessen the impact of CPU intensive or stalled
requests.

The ``nice_priority`` parameter can be used to set program scheduling priority.
The ``ionice_class`` and ``ionice_priority`` parameters can be used to set I/O scheduling
class and priority on the systems that use an I/O scheduler that supports
I/O priorities. As at kernel 2.6.17 the only such scheduler is the Completely
Fair Queuing (CFQ) I/O scheduler. If you run your Storage servers all together
on the same servers, you can slow down the auditors or prioritize
object-server I/O via these parameters (but probably do not need to change
it on the proxy). It is a new feature and the best practices are still
being developed. On some systems it may be required to run the daemons as root.
For more info also see setpriority(2) and ioprio_set(2).

The above configuration setting should be taken as suggestions and testing
of configuration settings should be done to ensure best utilization of CPU,
network connectivity, and disk I/O.

-------------------------
Filesystem Considerations
-------------------------

Swift is designed to be mostly filesystem agnostic--the only requirement
being that the filesystem supports extended attributes (xattrs). After
thorough testing with our use cases and hardware configurations, XFS was
the best all-around choice. If you decide to use a filesystem other than
XFS, we highly recommend thorough testing.

For distros with more recent kernels (for example Ubuntu 12.04 Precise),
we recommend using the default settings (including the default inode size
of 256 bytes) when creating the file system::

    mkfs.xfs -L D1 /dev/sda1

In the last couple of years, XFS has made great improvements in how inodes
are allocated and used.  Using the default inode size no longer has an
impact on performance.

For distros with older kernels (for example Ubuntu 10.04 Lucid),
some settings can dramatically impact performance. We recommend the
following when creating the file system::

    mkfs.xfs -i size=1024 -L D1 /dev/sda1

Setting the inode size is important, as XFS stores xattr data in the inode.
If the metadata is too large to fit in the inode, a new extent is created,
which can cause quite a performance problem. Upping the inode size to 1024
bytes provides enough room to write the default metadata, plus a little
headroom.

The following example mount options are recommended when using XFS::

    mount -t xfs -o noatime -L D1 /srv/node/d1

We do not recommend running Swift on RAID, but if you are using
RAID it is also important to make sure that the proper sunit and swidth
settings get set so that XFS can make most efficient use of the RAID array.

For a standard Swift install, all data drives are mounted directly under
``/srv/node`` (as can be seen in the above example of mounting label ``D1``
as ``/srv/node/d1``). If you choose to mount the drives in another directory,
be sure to set the ``devices`` config option in all of the server configs to
point to the correct directory.

The mount points for each drive in ``/srv/node/`` should be owned by the root user
almost exclusively (``root:root 755``). This is required to prevent rsync from
syncing files into the root drive in the event a drive is unmounted.

Swift uses system calls to reserve space for new objects being written into
the system. If your filesystem does not support ``fallocate()`` or
``posix_fallocate()``, be sure to set the ``disable_fallocate = true`` config
parameter in account, container, and object server configs.

Most current Linux distributions ship with a default installation of updatedb.
This tool runs periodically and updates the file name database that is used by
the GNU locate tool. However, including Swift object and container database
files is most likely not required and the periodic update affects the
performance quite a bit. To disable the inclusion of these files add the path
where Swift stores its data to the setting PRUNEPATHS in ``/etc/updatedb.conf``::

    PRUNEPATHS="... /tmp ... /var/spool ... /srv/node"


---------------------
General System Tuning
---------------------

The following changes have been found to be useful when running Swift on Ubuntu
Server 10.04.

The following settings should be in ``/etc/sysctl.conf``::

    # disable TIME_WAIT.. wait..
    net.ipv4.tcp_tw_recycle=1
    net.ipv4.tcp_tw_reuse=1

    # disable syn cookies
    net.ipv4.tcp_syncookies = 0

    # double amount of allowed conntrack
    net.netfilter.nf_conntrack_max = 262144

To load the updated sysctl settings, run ``sudo sysctl -p``.

A note about changing the TIME_WAIT values.  By default the OS will hold
a port open for 60 seconds to ensure that any remaining packets can be
received.  During high usage, and with the number of connections that are
created, it is easy to run out of ports.  We can change this since we are
in control of the network.  If you are not in control of the network, or
do not expect high loads, then you may not want to adjust those values.

----------------------
Logging Considerations
----------------------

Swift is set up to log directly to syslog. Every service can be configured
with the ``log_facility`` option to set the syslog log facility destination.
We recommended using syslog-ng to route the logs to specific log
files locally on the server and also to remote log collecting servers.
Additionally, custom log handlers can be used via the custom_log_handlers
setting.
