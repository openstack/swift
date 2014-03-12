================
Deployment Guide
================

-----------------------
Hardware Considerations
-----------------------

Swift is designed to run on commodity hardware. At Rackspace, our storage
servers are currently running fairly generic 4U servers with 24 2T SATA
drives and 8 cores of processing power. RAID on the storage drives is not
required and not recommended. Swift's disk usage pattern is the worst
case possible for RAID, and performance degrades very quickly using RAID 5
or 6.

------------------
Deployment Options
------------------

The swift services run completely autonomously, which provides for a lot of
flexibility when architecting the hardware deployment for swift. The 4 main
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

At Rackspace, we put the Proxy Services on their own servers and all of the
Storage Services on the same server. This allows us to send 10g networking to
the proxy and 1g to the storage servers, and keep load balancing to the
proxies more manageable.  Storage Services scale out horizontally as storage
servers are added, and we can scale overall API throughput by adding more
Proxies.

If you need more throughput to either Account or Container Services, they may
each be deployed to their own servers. For example you might use faster (but
more expensive) SAS or even SSD drives to get faster disk I/O to the databases.

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
it in whatever way best isolates your data from failure. Zones are referenced
by number, beginning with 1.

You can now start building the ring with::

    swift-ring-builder <builder_file> create <part_power> <replicas> <min_part_hours>

This will start the ring build process creating the <builder_file> with
2^<part_power> partitions. <min_part_hours> is the time in hours before a
specific partition can be moved in succession (24 is a good value for this).

Devices can be added to the ring with::

    swift-ring-builder <builder_file> add z<zone>-<ip>:<port>/<device_name>_<meta> <weight>

This will add a device to the ring where <builder_file> is the name of the
builder file that was created previously, <zone> is the number of the zone
this device is in, <ip> is the ip address of the server the device is in,
<port> is the port number that the server is running on, <device_name> is
the name of the device on the server (for example: sdb1), <meta> is a string
of metadata for the device (optional), and <weight> is a float weight that
determines how many partitions are put on the device relative to the rest of
the devices in the cluster (a good starting point is 100.0 x TB on the drive).
Add each device that will be initially in the cluster.

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

.. _general-service-configuration:

-----------------------------
General Service Configuration
-----------------------------

Most Swift services fall into two categories.  Swift's wsgi servers and
background daemons.  

For more information specific to the configuration of Swift's wsgi servers
with paste deploy see :ref:`general-server-configuration`

Configuration for servers and daemons can be expressed together in the same
file for each type of server, or separately.  If a required section for the
service trying to start is missing there will be an error.  The sections not
used by the service are ignored.

Consider the example of an object storage node.  By convention configuration
for the object-server, object-updater, object-replicator, and object-auditor
exist in a single file ``/etc/swift/object-server.conf``::

    [DEFAULT]

    [pipeline:main]
    pipeline = object-server

    [app:object-server]
    use = egg:swift#object

    [object-replicator]
    reclaim_age = 259200

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

The swift service management tool ``swift-init`` has adopted the convention of
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

Swift uses paste.deploy (http://pythonpaste.org/deploy/) to manage server
configurations.

Default configuration options are set in the `[DEFAULT]` section, and any
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

So, `name1` got the global value which is fine since it's only in the `DEFAULT`
section anyway.

`name2` got the global value from `DEFAULT` even though it appears to be
overridden in the `app:myapp` subsection. This is just the unfortunate way
paste.deploy works (at least at the time of this writing.)

`name3` got the local value from the `app:myapp` subsection because it is using
the special paste.deploy syntax of ``set option_name = value``. So, if you want
a default value for most app/filters but want to overridde it in one
subsection, this is how you do it.

`name4` got the global value from `DEFAULT` since it's only in that section
anyway. But, since we used the ``set`` syntax in the `DEFAULT` section even
though we shouldn't, notice we also got a ``set name4`` variable. Weird, but
probably not harmful.

`name5` got the local value from the `app:myapp` subsection since it's only
there anyway, but notice that it is in the global configuration and not the
local configuration. This is because we used the ``set`` syntax to set the
value. Again, weird, but not harmful since Swift just treats the two sets of
configuration values as one set anyway.

`name6` got the local value from `app:myapp` subsection since it's only there,
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

--------------------
Common configuration
--------------------

An example of common configuration file can be found at etc/swift.conf-sample

The following configuration options are available:

===================  ==========  =============================================
Option               Default     Description
-------------------  ----------  ---------------------------------------------
max_header_size      8192        max_header_size is the max number of bytes in
                                 the utf8 encoding of each header. Using 8192
                                 as default because eventlet use 8192 as max
                                 size of header line. This value may need to
                                 be increased when using identity v3 API
                                 tokens including more than 7 catalog entries.
                                 See also include_service_catalog in
                                 proxy-server.conf-sample (documented in
                                 overview_auth.rst)
===================  ==========  =============================================

---------------------------
Object Server Configuration
---------------------------

An Example Object Server configuration can be found at
etc/object-server.conf-sample in the source code repository.

The following configuration options are available:

[DEFAULT]

===================  ==========  =============================================
Option               Default     Description
-------------------  ----------  ---------------------------------------------
swift_dir            /etc/swift  Swift configuration directory
devices              /srv/node   Parent directory of where devices are mounted
mount_check          true        Whether or not check if the devices are
                                 mounted to prevent accidentally writing
                                 to the root device
bind_ip              0.0.0.0     IP Address for server to bind to
bind_port            6000        Port for server to bind to
bind_timeout         30          Seconds to attempt bind before giving up
workers              auto        Override the number of pre-forked workers
                                 that will accept connections.  If set it
                                 should be an integer, zero means no fork.  If
                                 unset, it will try to default to the number
                                 of effective cpu cores and fallback to one.
                                 Increasing the number of workers may reduce
                                 the possibility of slow file system
                                 operations in one request from negatively
                                 impacting other requests, but may not be as
                                 efficient as tuning :ref:`threads_per_disk
                                 <object-server-options>`
max_clients          1024        Maximum number of clients one worker can
                                 process simultaneously (it will actually
                                 accept(2) N + 1). Setting this to one (1)
                                 will only handle one request at a time,
                                 without accepting another request
                                 concurrently.
disable_fallocate    false       Disable "fast fail" fallocate checks if the
                                 underlying filesystem does not support it.
log_custom_handlers  None        Comma-separated list of functions to call
                                 to setup custom log handlers.
eventlet_debug       false       If true, turn on debug logging for eventlet
fallocate_reserve    0           You can set fallocate_reserve to the number of
                                 bytes you'd like fallocate to reserve, whether
                                 there is space for the given file size or not.
                                 This is useful for systems that behave badly
                                 when they completely run out of space; you can
                                 make the services pretend they're out of space
                                 early.
conn_timeout         0.5         Time to wait while attempting to connect to
                                 another backend node.
node_timeout         3           Time to wait while sending each chunk of data
                                 to another backend node.
client_timeout       60          Time to wait while receiving each chunk of
                                 data from a client or another backend node.
network_chunk_size   65536       Size of chunks to read/write over the network
disk_chunk_size      65536       Size of chunks to read/write to disk
===================  ==========  =============================================

.. _object-server-options:

[object-server]

=============================  =============  =================================
Option                         Default        Description
-----------------------------  -------------  ---------------------------------
use                                           paste.deploy entry point for the
                                              object server.  For most cases,
                                              this should be
                                              `egg:swift#object`.
set log_name                   object-server  Label used when logging
set log_facility               LOG_LOCAL0     Syslog log facility
set log_level                  INFO           Logging level
set log_requests               True           Whether or not to log each
                                              request
user                           swift          User to run as
max_upload_time                86400          Maximum time allowed to upload an
                                              object
slow                           0              If > 0, Minimum time in seconds
                                              for a PUT or DELETE request to
                                              complete
mb_per_sync                    512            On PUT requests, sync file every
                                              n MB
keep_cache_size                5242880        Largest object size to keep in
                                              buffer cache
keep_cache_private             false          Allow non-public objects to stay
                                              in kernel's buffer cache
threads_per_disk               0              Size of the per-disk thread pool
                                              used for performing disk I/O. The
                                              default of 0 means to not use a
                                              per-disk thread pool. It is
                                              recommended to keep this value
                                              small, as large values can result
                                              in high read latencies due to
                                              large queue depths. A good
                                              starting point is 4 threads per
                                              disk.
replication_concurrency        4              Set to restrict the number of
                                              concurrent incoming REPLICATION
                                              requests; set to 0 for unlimited
replication_one_per_device     True           Restricts incoming REPLICATION
                                              requests to one per device,
                                              replication_currency above
                                              allowing. This can help control
                                              I/O to each device, but you may
                                              wish to set this to False to
                                              allow multiple REPLICATION
                                              requests (up to the above
                                              replication_concurrency setting)
                                              per device.
replication_lock_timeout       15             Number of seconds to wait for an
                                              existing replication device lock
                                              before giving up.
replication_failure_threshold  100            The number of subrequest failures
                                              before the
                                              replication_failure_ratio is
                                              checked
replication_failure_ratio      1.0            If the value of failures /
                                              successes of REPLICATION
                                              subrequests exceeds this ratio,
                                              the overall REPLICATION request
                                              will be aborted
=============================  =============  =================================

[object-replicator]

==================  =================  =======================================
Option              Default            Description
------------------  -----------------  ---------------------------------------
log_name            object-replicator  Label used when logging
log_facility        LOG_LOCAL0         Syslog log facility
log_level           INFO               Logging level
daemonize           yes                Whether or not to run replication as a
                                       daemon
run_pause           30                 Time in seconds to wait between
                                       replication passes
concurrency         1                  Number of replication workers to spawn
timeout             5                  Timeout value sent to rsync --timeout
                                       and --contimeout options
stats_interval      3600               Interval in seconds between logging
                                       replication statistics
reclaim_age         604800             Time elapsed in seconds before an
                                       object can be reclaimed
handoffs_first      false              If set to True, partitions that are
                                       not supposed to be on the node will be
                                       replicated first.  The default setting
                                       should not be changed, except for
                                       extreme situations.
handoff_delete      auto               By default handoff partitions will be
                                       removed when it has successfully
                                       replicated to all the canonical nodes.
                                       If set to an integer n, it will remove
                                       the partition if it is successfully
                                       replicated to n nodes.  The default
                                       setting should not be changed, except
                                       for extreme situations.
node_timeout        DEFAULT or 10      Request timeout to external services.
                                       This uses what's set here, or what's set
                                       in the DEFAULT section, or 10 (though
                                       other sections use 3 as the final
                                       default).
==================  =================  =======================================

[object-updater]

==================  ==============  ==========================================
Option              Default         Description
------------------  --------------  ------------------------------------------
log_name            object-updater  Label used when logging
log_facility        LOG_LOCAL0      Syslog log facility
log_level           INFO            Logging level
interval            300             Minimum time for a pass to take
concurrency         1               Number of updater workers to spawn
node_timeout        DEFAULT or 10   Request timeout to external services. This
                                    uses what's set here, or what's set in the
                                    DEFAULT section, or 10 (though other
                                    sections use 3 as the final default).
slowdown            0.01            Time in seconds to wait between objects
==================  ==============  ==========================================

[object-auditor]

==================  ==============  ==========================================
Option              Default         Description
------------------  --------------  ------------------------------------------
log_name            object-auditor  Label used when logging
log_facility        LOG_LOCAL0      Syslog log facility
log_level           INFO            Logging level
log_time            3600            Frequency of status logs in seconds.
files_per_second    20              Maximum files audited per second. Should
                                    be tuned according to individual system
                                    specs. 0 is unlimited.
bytes_per_second    10000000        Maximum bytes audited per second. Should
                                    be tuned according to individual system
                                    specs. 0 is unlimited.
==================  ==============  ==========================================

------------------------------
Container Server Configuration
------------------------------

An example Container Server configuration can be found at
etc/container-server.conf-sample in the source code repository.

The following configuration options are available:

[DEFAULT]

===================  ==========  ============================================
Option               Default     Description
-------------------  ----------  --------------------------------------------
swift_dir            /etc/swift  Swift configuration directory
devices              /srv/node   Parent directory of where devices are mounted
mount_check          true        Whether or not check if the devices are
                                 mounted to prevent accidentally writing
                                 to the root device
bind_ip              0.0.0.0     IP Address for server to bind to
bind_port            6001        Port for server to bind to
bind_timeout         30          Seconds to attempt bind before giving up
workers              auto        Override the number of pre-forked workers
                                 that will accept connections.  If set it
                                 should be an integer, zero means no fork.  If
                                 unset, it will try to default to the number
                                 of effective cpu cores and fallback to one.
                                 Increasing the number of workers may reduce
                                 the possibility of slow file system
                                 operations in one request from negatively
                                 impacting other requests.  See
                                 :ref:`general-service-tuning`
max_clients          1024        Maximum number of clients one worker can
                                 process simultaneously (it will actually
                                 accept(2) N + 1). Setting this to one (1)
                                 will only handle one request at a time,
                                 without accepting another request
                                 concurrently.
user                 swift       User to run as
disable_fallocate    false       Disable "fast fail" fallocate checks if the
                                 underlying filesystem does not support it.
log_custom_handlers  None        Comma-separated list of functions to call
                                 to setup custom log handlers.
eventlet_debug       false       If true, turn on debug logging for eventlet
fallocate_reserve    0           You can set fallocate_reserve to the number of
                                 bytes you'd like fallocate to reserve, whether
                                 there is space for the given file size or not.
                                 This is useful for systems that behave badly
                                 when they completely run out of space; you can
                                 make the services pretend they're out of space
                                 early.
===================  ==========  ============================================

[container-server]

==================  ================  ========================================
Option              Default           Description
------------------  ----------------  ----------------------------------------
use                                   paste.deploy entry point for the
                                      container server.  For most cases, this
                                      should be `egg:swift#container`.
set log_name        container-server  Label used when logging
set log_facility    LOG_LOCAL0        Syslog log facility
set log_level       INFO              Logging level
node_timeout        3                 Request timeout to external services
conn_timeout        0.5               Connection timeout to external services
allow_versions      false             Enable/Disable object versioning feature
==================  ================  ========================================

[container-replicator]

==================  ====================  ====================================
Option              Default               Description
------------------  --------------------  ------------------------------------
log_name            container-replicator  Label used when logging
log_facility        LOG_LOCAL0            Syslog log facility
log_level           INFO                  Logging level
per_diff            1000
concurrency         8                     Number of replication workers to
                                          spawn
run_pause           30                    Time in seconds to wait between
                                          replication passes
node_timeout        10                    Request timeout to external services
conn_timeout        0.5                   Connection timeout to external
                                          services
reclaim_age         604800                Time elapsed in seconds before a
                                          container can be reclaimed
==================  ====================  ====================================

[container-updater]

========================  =================  ==================================
Option                    Default            Description
------------------------  -----------------  ----------------------------------
log_name                  container-updater  Label used when logging
log_facility              LOG_LOCAL0         Syslog log facility
log_level                 INFO               Logging level
interval                  300                Minimum time for a pass to take
concurrency               4                  Number of updater workers to spawn
node_timeout              3                  Request timeout to external
                                             services
conn_timeout              0.5                Connection timeout to external
                                             services
slowdown                  0.01               Time in seconds to wait between
                                             containers
account_suppression_time  60                 Seconds to suppress updating an
                                             account that has generated an
                                             error (timeout, not yet found,
                                             etc.)
========================  =================  ==================================

[container-auditor]

=====================  =================  =======================================
Option                 Default            Description
---------------------  -----------------  ---------------------------------------
log_name               container-auditor  Label used when logging
log_facility           LOG_LOCAL0         Syslog log facility
log_level              INFO               Logging level
interval               1800               Minimum time for a pass to take
containers_per_second  200                Maximum containers audited per second.
                                          Should be tuned according to individual
                                          system specs. 0 is unlimited.
=====================  =================  =======================================

----------------------------
Account Server Configuration
----------------------------

An example Account Server configuration can be found at
etc/account-server.conf-sample in the source code repository.

The following configuration options are available:

[DEFAULT]

===================  ==========  =============================================
Option               Default     Description
-------------------  ----------  ---------------------------------------------
swift_dir            /etc/swift  Swift configuration directory
devices              /srv/node   Parent directory or where devices are mounted
mount_check          true        Whether or not check if the devices are
                                 mounted to prevent accidentally writing
                                 to the root device
bind_ip              0.0.0.0     IP Address for server to bind to
bind_port            6002        Port for server to bind to
bind_timeout         30          Seconds to attempt bind before giving up
workers              auto        Override the number of pre-forked workers
                                 that will accept connections.  If set it
                                 should be an integer, zero means no fork.  If
                                 unset, it will try to default to the number
                                 of effective cpu cores and fallback to one.
                                 Increasing the number of workers may reduce
                                 the possibility of slow file system
                                 operations in one request from negatively
                                 impacting other requests.  See
                                 :ref:`general-service-tuning`
max_clients          1024        Maximum number of clients one worker can
                                 process simultaneously (it will actually
                                 accept(2) N + 1). Setting this to one (1)
                                 will only handle one request at a time,
                                 without accepting another request
                                 concurrently.
user                 swift       User to run as
db_preallocation     off         If you don't mind the extra disk space usage in
                                 overhead, you can turn this on to preallocate
                                 disk space with SQLite databases to decrease
                                 fragmentation.
disable_fallocate    false       Disable "fast fail" fallocate checks if the
                                 underlying filesystem does not support it.
log_custom_handlers  None        Comma-separated list of functions to call
                                 to setup custom log handlers.
eventlet_debug       false       If true, turn on debug logging for eventlet
fallocate_reserve    0           You can set fallocate_reserve to the number of
                                 bytes you'd like fallocate to reserve, whether
                                 there is space for the given file size or not.
                                 This is useful for systems that behave badly
                                 when they completely run out of space; you can
                                 make the services pretend they're out of space
                                 early.
===================  ==========  =============================================

[account-server]

==================  ==============  ==========================================
Option              Default         Description
------------------  --------------  ------------------------------------------
use                                 Entry point for paste.deploy for the account
                                    server.  For most cases, this should be
                                    `egg:swift#account`.
set log_name        account-server  Label used when logging
set log_facility    LOG_LOCAL0      Syslog log facility
set log_level       INFO            Logging level
==================  ==============  ==========================================

[account-replicator]

==================  ==================  ======================================
Option              Default             Description
------------------  ------------------  --------------------------------------
log_name            account-replicator  Label used when logging
log_facility        LOG_LOCAL0          Syslog log facility
log_level           INFO                Logging level
per_diff            1000
concurrency         8                   Number of replication workers to spawn
run_pause           30                  Time in seconds to wait between
                                        replication passes
node_timeout        10                  Request timeout to external services
conn_timeout        0.5                 Connection timeout to external services
reclaim_age         604800              Time elapsed in seconds before an
                                        account can be reclaimed
==================  ==================  ======================================

[account-auditor]

====================  ===============  =======================================
Option                Default          Description
--------------------  ---------------  ---------------------------------------
log_name              account-auditor  Label used when logging
log_facility          LOG_LOCAL0       Syslog log facility
log_level             INFO             Logging level
interval              1800             Minimum time for a pass to take
accounts_per_second   200              Maximum accounts audited per second.
                                       Should be tuned according to individual
                                       system specs. 0 is unlimited.
====================  ===============  =======================================

[account-reaper]

==================  ===============  =========================================
Option              Default          Description
------------------  ---------------  -----------------------------------------
log_name            account-auditor  Label used when logging
log_facility        LOG_LOCAL0       Syslog log facility
log_level           INFO             Logging level
concurrency         25               Number of replication workers to spawn
interval            3600             Minimum time for a pass to take
node_timeout        10               Request timeout to external services
conn_timeout        0.5              Connection timeout to external services
delay_reaping       0                Normally, the reaper begins deleting
                                     account information for deleted accounts
                                     immediately; you can set this to delay
                                     its work however. The value is in seconds,
                                     2592000 = 30 days, for example.
==================  ===============  =========================================

.. _proxy-server-config:

--------------------------
Proxy Server Configuration
--------------------------

An example Proxy Server configuration can be found at
etc/proxy-server.conf-sample in the source code repository.

The following configuration options are available:

[DEFAULT]

============================  ===============  =============================
Option                        Default          Description
----------------------------  ---------------  -----------------------------
bind_ip                       0.0.0.0          IP Address for server to
                                               bind to
bind_port                     80               Port for server to bind to
bind_timeout                  30               Seconds to attempt bind before
                                               giving up
swift_dir                     /etc/swift       Swift configuration directory
workers                       auto             Override the number of
                                               pre-forked workers that will
                                               accept connections.  If set it
                                               should be an integer, zero
                                               means no fork.  If unset, it
                                               will try to default to the
                                               number of effective cpu cores
                                               and fallback to one.  See
                                               :ref:`general-service-tuning`
max_clients                   1024             Maximum number of clients one
                                               worker can process
                                               simultaneously (it will
                                               actually accept(2) N +
                                               1). Setting this to one (1)
                                               will only handle one request at
                                               a time, without accepting
                                               another request
                                               concurrently.
user                          swift            User to run as
cert_file                                      Path to the ssl .crt. This
                                               should be enabled for testing
                                               purposes only.
key_file                                       Path to the ssl .key. This
                                               should be enabled for testing
                                               purposes only.
cors_allow_origin                              This is a list of hosts that
                                               are included with any CORS
                                               request by default and
                                               returned with the
                                               Access-Control-Allow-Origin
                                               header in addition to what
                                               the container has set.
log_custom_handlers           None             Comma separated list of functions
                                               to call to setup custom log
                                               handlers.
eventlet_debug                false            If true, turn on debug logging
                                               for eventlet

expose_info                   true             Enables exposing configuration
                                               settings via HTTP GET /info.

admin_key                                      Key to use for admin calls that
                                               are HMAC signed.  Default
                                               is empty, which will
                                               disable admin calls to
                                               /info.
============================  ===============  =============================

[proxy-server]

============================  ===============  =============================
Option                        Default          Description
----------------------------  ---------------  -----------------------------
use                                            Entry point for paste.deploy for
                                               the proxy server.  For most
                                               cases, this should be
                                               `egg:swift#proxy`.
set log_name                  proxy-server     Label used when logging
set log_facility              LOG_LOCAL0       Syslog log facility
set log_level                 INFO             Log level
set log_headers               True             If True, log headers in each
                                               request
set log_handoffs              True             If True, the proxy will log
                                               whenever it has to failover to a
                                               handoff node
recheck_account_existence     60               Cache timeout in seconds to
                                               send memcached for account
                                               existence
recheck_container_existence   60               Cache timeout in seconds to
                                               send memcached for container
                                               existence
object_chunk_size             65536            Chunk size to read from
                                               object servers
client_chunk_size             65536            Chunk size to read from
                                               clients
memcache_servers              127.0.0.1:11211  Comma separated list of
                                               memcached servers ip:port
memcache_max_connections      2                Max number of connections to
                                               each memcached server per
                                               worker
node_timeout                  10               Request timeout to external
                                               services
recoverable_node_timeout      node_timeout     Request timeout to external
                                               services for requests that, on
                                               failure, can be recovered
                                               from. For example, object GET.
client_timeout                60               Timeout to read one chunk
                                               from a client
conn_timeout                  0.5              Connection timeout to
                                               external services
error_suppression_interval    60               Time in seconds that must
                                               elapse since the last error
                                               for a node to be considered
                                               no longer error limited
error_suppression_limit       10               Error count to consider a
                                               node error limited
allow_account_management      false            Whether account PUTs and DELETEs
                                               are even callable
object_post_as_copy           true             Set object_post_as_copy = false
                                               to turn on fast posts where only
                                               the metadata changes are stored
                                               anew and the original data file
                                               is kept in place. This makes for
                                               quicker posts; but since the
                                               container metadata isn't updated
                                               in this mode, features like
                                               container sync won't be able to
                                               sync posts.
account_autocreate            false            If set to 'true' authorized
                                               accounts that do not yet exist
                                               within the Swift cluster will
                                               be automatically created.
max_containers_per_account    0                If set to a positive value,
                                               trying to create a container
                                               when the account already has at
                                               least this maximum containers
                                               will result in a 403 Forbidden.
                                               Note: This is a soft limit,
                                               meaning a user might exceed the
                                               cap for
                                               recheck_account_existence before
                                               the 403s kick in.
max_containers_whitelist                       This is a comma separated list
                                               of account names that ignore
                                               the max_containers_per_account
                                               cap.
rate_limit_after_segment      10               Rate limit the download of
                                               large object segments after
                                               this segment is downloaded.
rate_limit_segments_per_sec   1                Rate limit large object
                                               downloads at this rate.
request_node_count            2 * replicas     Set to the number of nodes to
                                               contact for a normal request.
                                               You can use '* replicas' at the
                                               end to have it use the number
                                               given times the number of
                                               replicas for the ring being used
                                               for the request.
swift_owner_headers           <see the sample  These are the headers whose
                              conf file for    values will only be shown to
                              the list of      swift_owners. The exact
                              default          definition of a swift_owner is
                              headers>         up to the auth system in use,
                                               but usually indicates
                                               administrative responsibilities.
============================  ===============  =============================

[tempauth]

=====================  =============================== =======================
Option                 Default                         Description
---------------------  ------------------------------- -----------------------
use                                                    Entry point for
                                                       paste.deploy to use for
                                                       auth. To use tempauth
                                                       set to:
                                                       `egg:swift#tempauth`
set log_name           tempauth                        Label used when logging
set log_facility       LOG_LOCAL0                      Syslog log facility
set log_level          INFO                            Log level
set log_headers        True                            If True, log headers in
                                                       each request
reseller_prefix        AUTH                            The naming scope for the
                                                       auth service. Swift
                                                       storage accounts and
                                                       auth tokens will begin
                                                       with this prefix.
auth_prefix            /auth/                          The HTTP request path
                                                       prefix for the auth
                                                       service. Swift itself
                                                       reserves anything
                                                       beginning with the
                                                       letter `v`.
token_life             86400                           The number of seconds a
                                                       token is valid.
storage_url_scheme     default                         Scheme to return with
                                                       storage urls: http,
                                                       https, or default
                                                       (chooses based on what
                                                       the server is running
                                                       as) This can be useful
                                                       with an SSL load
                                                       balancer in front of a
                                                       non-SSL server.
=====================  =============================== =======================

Additionally, you need to list all the accounts/users you want here. The format
is::

    user_<account>_<user> = <key> [group] [group] [...] [storage_url]

or if you want to be able to include underscores in the ``<account>`` or
``<user>`` portions, you can base64 encode them (with *no* equal signs) in a
line like this::

    user64_<account_b64>_<user_b64> = <key> [group] [group] [...] [storage_url]

There are special groups of::

    .reseller_admin = can do anything to any account for this auth
    .admin = can do anything within the account

If neither of these groups are specified, the user can only access containers
that have been explicitly allowed for them by a .admin or .reseller_admin.

The trailing optional storage_url allows you to specify an alternate url to
hand back to the user upon authentication. If not specified, this defaults to::

    $HOST/v1/<reseller_prefix>_<account>

Where $HOST will do its best to resolve to what the requester would need to use
to reach this host, <reseller_prefix> is from this section, and <account> is
from the user_<account>_<user> name. Note that $HOST cannot possibly handle
when you have a load balancer in front of it that does https while TempAuth
itself runs with http; in such a case, you'll have to specify the
storage_url_scheme configuration value as an override.

Here are example entries, required for running the tests::

    user_admin_admin = admin .admin .reseller_admin
    user_test_tester = testing .admin
    user_test2_tester2 = testing2 .admin
    user_test_tester3 = testing3

    # account "test_y" and user "tester_y" (note the lack of padding = chars)
    user64_dGVzdF95_dGVzdGVyX3k = testing4 .admin

------------------------
Memcached Considerations
------------------------

Several of the Services rely on Memcached for caching certain types of
lookups, such as auth tokens, and container/account existence.  Swift does
not do any caching of actual object data.  Memcached should be able to run
on any servers that have available RAM and CPU.  At Rackspace, we run
Memcached on the proxy servers.  The `memcache_servers` config option
in the `proxy-server.conf` should contain all memcached servers.

-----------
System Time
-----------

Time may be relative but it is relatively important for Swift!  Swift uses
timestamps to determine which is the most recent version of an object.
It is very important for the system time on each server in the cluster to
by synced as closely as possible (more so for the proxy server, but in general
it is a good idea for all the servers).  At Rackspace, we use NTP with a local
NTP server to ensure that the system times are as close as possible.  This
should also be monitored to ensure that the times do not vary too much.

.. _general-service-tuning:

----------------------
General Service Tuning
----------------------

Most services support either a `worker` or `concurrency` value in the
settings.  This allows the services to make effective use of the cores
available. A good starting point to set the concurrency level for the proxy
and storage services to 2 times the number of cores available. If more than
one service is sharing a server, then some experimentation may be needed to
find the best balance.

At Rackspace, our Proxy servers have dual quad core processors, giving us 8
cores. Our testing has shown 16 workers to be a pretty good balance when
saturating a 10g network and gives good CPU utilization.

Our Storage servers all run together on the same servers. These servers have
dual quad core processors, for 8 cores total. We run the Account, Container,
and Object servers with 8 workers each. Most of the background jobs are run at
a concurrency of 1, with the exception of the replicators which are run at a
concurrency of 2.

The `max_clients` parameter can be used to adjust the number of client
requests an individual worker accepts for processing. The fewer requests being
processed at one time, the less likely a request that consumes the worker's
CPU time, or blocks in the OS, will negatively impact other requests. The more
requests being processed at one time, the more likely one worker can utilize
network and disk capacity.

On systems that have more cores, and more memory, where one can afford to run
more workers, raising the number of workers and lowering the maximum number of
clients serviced per worker can lessen the impact of CPU intensive or stalled
requests.

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

    mkfs.xfs /dev/sda1

In the last couple of years, XFS has made great improvements in how inodes
are allocated and used.  Using the default inode size no longer has an
impact on performance.

For distros with older kernels (for example Ubuntu 10.04 Lucid),
some settings can dramatically impact performance. We recommend the
following when creating the file system::

    mkfs.xfs -i size=1024 /dev/sda1

Setting the inode size is important, as XFS stores xattr data in the inode.
If the metadata is too large to fit in the inode, a new extent is created,
which can cause quite a performance problem. Upping the inode size to 1024
bytes provides enough room to write the default metadata, plus a little
headroom.

The following example mount options are recommended when using XFS::

    mount -t xfs -o noatime,nodiratime,nobarrier,logbufs=8 /dev/sda1 /srv/node/sda

We do not recommend running Swift on RAID, but if you are using
RAID it is also important to make sure that the proper sunit and swidth
settings get set so that XFS can make most efficient use of the RAID array.

For a standard swift install, all data drives are mounted directly under
/srv/node (as can be seen in the above example of mounting /def/sda1 as
/srv/node/sda). If you choose to mount the drives in another directory,
be sure to set the `devices` config option in all of the server configs to
point to the correct directory.

Swift uses system calls to reserve space for new objects being written into
the system. If your filesystem does not support `fallocate()` or
`posix_fallocate()`, be sure to set the `disable_fallocate = true` config
parameter in account, container, and object server configs.

---------------------
General System Tuning
---------------------

Rackspace currently runs Swift on Ubuntu Server 10.04, and the following
changes have been found to be useful for our use cases.

The following settings should be in `/etc/sysctl.conf`::

    # disable TIME_WAIT.. wait..
    net.ipv4.tcp_tw_recycle=1
    net.ipv4.tcp_tw_reuse=1

    # disable syn cookies
    net.ipv4.tcp_syncookies = 0

    # double amount of allowed conntrack
    net.ipv4.netfilter.ip_conntrack_max = 262144

To load the updated sysctl settings, run ``sudo sysctl -p``

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
with the `log_facility` option to set the syslog log facility destination.
We recommended using syslog-ng to route the logs to specific log
files locally on the server and also to remote log collecting servers.
Additionally, custom log handlers can be used via the custom_log_handlers
setting.
