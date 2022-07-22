.. _container-server-config:

------------------------------
Container Server Configuration
------------------------------

This document describes the configuration options available for the container
server. Documentation for other swift configuration options can be found at
:doc:`index`.

An example Container Server configuration can be found at
etc/container-server.conf-sample in the source code repository.

The following configuration sections are available:

* :ref:`[DEFAULT] <container_server_default_options>`
* `[container-server]`_
* `[container-replicator]`_
* `[container-sharder]`_
* `[container-updater]`_
* `[container-auditor]`_

.. _container_server_default_options:

*********
[DEFAULT]
*********

===============================  ==========  ============================================
Option                           Default     Description
-------------------------------  ----------  --------------------------------------------
swift_dir                        /etc/swift  Swift configuration directory
devices                          /srv/node   Parent directory of where devices are mounted
mount_check                      true        Whether or not check if the devices are
                                             mounted to prevent accidentally writing
                                             to the root device
bind_ip                          0.0.0.0     IP Address for server to bind to
bind_port                        6201        Port for server to bind to
keep_idle                        600         Value to set for socket TCP_KEEPIDLE
bind_timeout                     30          Seconds to attempt bind before giving up
backlog                          4096        Maximum number of allowed pending
                                             connections
workers                          auto        Override the number of pre-forked workers
                                             that will accept connections.  If set it
                                             should be an integer, zero means no fork.  If
                                             unset, it will try to default to the number
                                             of effective cpu cores and fallback to one.
                                             Increasing the number of workers may reduce
                                             the possibility of slow file system
                                             operations in one request from negatively
                                             impacting other requests.  See
                                             :ref:`general-service-tuning`.
max_clients                      1024        Maximum number of clients one worker can
                                             process simultaneously (it will actually
                                             accept(2) N + 1). Setting this to one (1)
                                             will only handle one request at a time,
                                             without accepting another request
                                             concurrently.
user                             swift       User to run as
disable_fallocate                false       Disable "fast fail" fallocate checks if the
                                             underlying filesystem does not support it.
log_name                         swift       Label used when logging
log_facility                     LOG_LOCAL0  Syslog log facility
log_level                        INFO        Logging level
log_address                      /dev/log    Logging directory
log_max_line_length              0           Caps the length of log lines to the
                                             value given; no limit if set to 0, the
                                             default.
log_custom_handlers              None        Comma-separated list of functions to call
                                             to setup custom log handlers.
log_udp_host                                 Override log_address
log_udp_port                     514         UDP log port
log_statsd_host                  None        Enables StatsD logging; IPv4/IPv6
                                             address or a hostname.  If a
                                             hostname resolves to an IPv4 and IPv6
                                             address, the IPv4 address will be
                                             used.
log_statsd_port                  8125
log_statsd_default_sample_rate   1.0
log_statsd_sample_rate_factor    1.0
log_statsd_metric_prefix
eventlet_debug                   false       If true, turn on debug logging for eventlet
fallocate_reserve                1%          You can set fallocate_reserve to the
                                             number of bytes or percentage of disk
                                             space you'd like fallocate to reserve,
                                             whether there is space for the given
                                             file size or not. Percentage will be used
                                             if the value ends with a '%'. This is
                                             useful for systems that behave badly when
                                             they completely run out of space; you can
                                             make the services pretend they're out of
                                             space early.
db_preallocation                 off         If you don't mind the extra disk space usage
                                             in overhead, you can turn this on to preallocate
                                             disk space with SQLite databases to decrease
                                             fragmentation.
nice_priority                    None        Scheduling priority of server processes.
                                             Niceness values range from -20 (most
                                             favorable to the process) to 19 (least
                                             favorable to the process). The default
                                             does not modify priority.
ionice_class                     None        I/O scheduling class of server processes.
                                             I/O niceness class values are IOPRIO_CLASS_RT
                                             (realtime), IOPRIO_CLASS_BE (best-effort),
                                             and IOPRIO_CLASS_IDLE (idle).
                                             The default does not modify class and
                                             priority. Linux supports io scheduling
                                             priorities and classes since 2.6.13
                                             with the CFQ io scheduler.
                                             Work only with ionice_priority.
ionice_priority                  None        I/O scheduling priority of server processes.
                                             I/O niceness priority is a number which
                                             goes from 0 to 7. The higher the value,
                                             the lower the I/O priority of the process.
                                             Work only with ionice_class.
                                             Ignored if IOPRIO_CLASS_IDLE is set.
===============================  ==========  ============================================

******************
[container-server]
******************

==============================  ================  ========================================
Option                          Default           Description
------------------------------  ----------------  ----------------------------------------
use                                               paste.deploy entry point for the
                                                  container server.  For most cases, this
                                                  should be ``egg:swift#container``.
set log_name                    container-server  Label used when logging
set log_facility                LOG_LOCAL0        Syslog log facility
set log_level                   INFO              Logging level
set log_requests                True              Whether or not to log each
                                                  request
set log_address                 /dev/log          Logging directory
node_timeout                    3                 Request timeout to external services
conn_timeout                    0.5               Connection timeout to external services
allow_versions                  false             Enable/Disable object versioning feature
replication_server                                Configure parameter for creating
                                                  specific server. To handle all verbs,
                                                  including replication verbs, do not
                                                  specify "replication_server"
                                                  (this is the default). To only
                                                  handle replication, set to a True
                                                  value (e.g. "True" or "1").
                                                  To handle only non-replication
                                                  verbs, set to "False". Unless you
                                                  have a separate replication network, you
                                                  should not specify any value for
                                                  "replication_server".
nice_priority                   None              Scheduling priority of server processes.
                                                  Niceness values range from -20 (most
                                                  favorable to the process) to 19 (least
                                                  favorable to the process). The default
                                                  does not modify priority.
ionice_class                    None              I/O scheduling class of server processes.
                                                  I/O niceness class values are
                                                  IOPRIO_CLASS_RT (realtime),
                                                  IOPRIO_CLASS_BE (best-effort),
                                                  and IOPRIO_CLASS_IDLE (idle).
                                                  The default does not modify class and
                                                  priority. Linux supports io scheduling
                                                  priorities and classes since 2.6.13 with
                                                  the CFQ io scheduler.
                                                  Work only with ionice_priority.
ionice_priority                 None              I/O scheduling priority of server
                                                  processes. I/O niceness priority is
                                                  a number which goes from 0 to 7.
                                                  The higher the value, the lower the I/O
                                                  priority of the process. Work only with
                                                  ionice_class.
                                                  Ignored if IOPRIO_CLASS_IDLE is set.
==============================  ================  ========================================

**********************
[container-replicator]
**********************

==================== ===========================  =======================================
Option               Default                      Description
-------------------- ---------------------------  ---------------------------------------
log_name             container-replicator         Label used when logging
log_facility         LOG_LOCAL0                   Syslog log facility
log_level            INFO                         Logging level
log_address          /dev/log                     Logging directory
per_diff             1000                         Maximum number of database
                                                  rows that will be sync'd in a
                                                  single HTTP replication
                                                  request. Databases with less
                                                  than or equal to this number
                                                  of differing rows will always
                                                  be sync'd using an HTTP
                                                  replication request rather
                                                  than using rsync.
max_diffs            100                          Maximum number of HTTP
                                                  replication requests attempted
                                                  on each replication pass for
                                                  any one container. This caps
                                                  how long the replicator will
                                                  spend trying to sync a given
                                                  database per pass so the other
                                                  databases don't get starved.
concurrency          8                            Number of replication workers
                                                  to spawn
interval             30                           Time in seconds to wait
                                                  between replication passes
databases_per_second 50                           Maximum databases to process
                                                  per second.  Should be tuned
                                                  according to individual
                                                  system specs.  0 is unlimited.
node_timeout         10                           Request timeout to external
                                                  services
conn_timeout         0.5                          Connection timeout to external
                                                  services
reclaim_age          604800                       Time elapsed in seconds before
                                                  a container can be reclaimed
rsync_module         {replication_ip}::container  Format of the rsync module
                                                  where the replicator will send
                                                  data. The configuration value
                                                  can include some variables
                                                  that will be extracted from
                                                  the ring. Variables must
                                                  follow the format {NAME} where
                                                  NAME is one of: ip, port,
                                                  replication_ip,
                                                  replication_port, region,
                                                  zone, device, meta. See
                                                  etc/rsyncd.conf-sample for
                                                  some examples.
rsync_compress       no                           Allow rsync to compress data
                                                  which is transmitted to
                                                  destination node during sync.
                                                  However, this is applicable
                                                  only when destination node is
                                                  in a different region than the
                                                  local one. NOTE: Objects that
                                                  are already compressed (for
                                                  example: .tar.gz, mp3) might
                                                  slow down the syncing process.
recon_cache_path     /var/cache/swift             Path to recon cache
nice_priority        None                         Scheduling priority of server
                                                  processes. Niceness values
                                                  range from -20 (most favorable
                                                  to the process) to 19 (least
                                                  favorable to the process).
                                                  The default does not modify
                                                  priority.
ionice_class         None                         I/O scheduling class of server
                                                  processes. I/O niceness class
                                                  values are
                                                  IOPRIO_CLASS_RT (realtime),
                                                  IOPRIO_CLASS_BE (best-effort),
                                                  and IOPRIO_CLASS_IDLE (idle).
                                                  The default does not modify
                                                  class and priority. Linux
                                                  supports io scheduling
                                                  priorities and classes since
                                                  2.6.13 with the CFQ io
                                                  scheduler.
                                                  Work only with ionice_priority.
ionice_priority      None                         I/O scheduling priority of
                                                  server processes. I/O niceness
                                                  priority is a number which goes
                                                  from 0 to 7.
                                                  The higher the value, the lower
                                                  the I/O priority of the process.
                                                  Work only with ionice_class.
                                                  Ignored if IOPRIO_CLASS_IDLE
                                                  is set.
handoffs_only        no                           When handoffs_only mode is enabled
                                                  the replicator will *only* replicate
                                                  from handoff nodes to primary nodes
                                                  and will not sync primary nodes
                                                  with other primary nodes.
handoff_delete       auto                         the number of replicas which are
                                                  ensured in swift. If the number
                                                  less than the number of replicas
                                                  is set, container-replicator
                                                  could delete local handoffs even
                                                  if all replicas are not ensured in
                                                  the cluster. The replicator would
                                                  remove local handoff container database
                                                  after syncing when the number of
                                                  successful responses is greater than
                                                  or equal to this number. By default
                                                  handoff partitions will be removed
                                                  when it has successfully replicated
                                                  to all the canonical nodes.
==================== ===========================  =======================================

*******************
[container-sharder]
*******************

The container-sharder re-uses features of the container-replicator and inherits
the following configuration options defined for the `[container-replicator]`_:

* interval
* databases_per_second
* per_diff
* max_diffs
* concurrency
* node_timeout
* conn_timeout
* reclaim_age
* rsync_compress
* rsync_module
* recon_cache_path

Some config options in this section may also be used by the
:ref:`swift-manage-shard-ranges CLI tool <swift-manage-shard-ranges>`.

================================= =================  =======================================
Option                            Default            Description
--------------------------------- -----------------  ---------------------------------------
log_name                          container-sharder  Label used when logging
log_facility                      LOG_LOCAL0         Syslog log facility
log_level                         INFO               Logging level
log_address                       /dev/log           Logging directory


auto_shard                        false               If the auto_shard option
                                                      is true then the sharder
                                                      will automatically select
                                                      containers to shard, scan
                                                      for shard ranges, and
                                                      select shards to shrink.
                                                      Warning: auto-sharding is
                                                      still under development
                                                      and should not be used in
                                                      production; do not set
                                                      this option to true in a
                                                      production cluster.

shard_container_threshold         1000000             This defines the
                                                      object count at which a
                                                      container with
                                                      container-sharding
                                                      enabled will start to
                                                      shard. This also
                                                      indirectly determines the
                                                      the defaults for
                                                      rows_per_shard,
                                                      shrink_threshold and
                                                      expansion_limit.

rows_per_shard                    500000              This defines the initial
                                                      nominal size of shard
                                                      containers. The default
                                                      is shard_container_threshold // 2.

minimum_shard_size                100000              Minimum size of the final
                                                      shard range. If this is
                                                      greater than one then the
                                                      final shard range may be
                                                      extended to more than
                                                      rows_per_shard in order
                                                      to avoid a further shard
                                                      range with less than
                                                      minimum_shard_size rows.
                                                      The default value is
                                                      rows_per_shard // 5.

shrink_threshold                                      This defines the
                                                      object count below which
                                                      a 'donor' shard container
                                                      will be considered for
                                                      shrinking into another
                                                      'acceptor' shard
                                                      container. The default is
                                                      determined by
                                                      shard_shrink_point. If
                                                      set, shrink_threshold
                                                      will take precedence over
                                                      shard_shrink_point.

shard_shrink_point                10                  Deprecated: shrink_threshold
                                                      is recommended and if set
                                                      will take precedence over
                                                      shard_shrink_point.
                                                      This defines the
                                                      object count below which
                                                      a 'donor' shard container
                                                      will be considered for
                                                      shrinking into another
                                                      'acceptor' shard
                                                      container.
                                                      shard_shrink_point is a
                                                      percentage of
                                                      shard_container_threshold
                                                      e.g. the default value of
                                                      10 means 10% of the
                                                      shard_container_threshold.

expansion_limit                                       This defines the
                                                      maximum allowed size of
                                                      an acceptor shard
                                                      container after having a
                                                      donor merged into it. The
                                                      default is determined by
                                                      shard_shrink_merge_point.
                                                      If set, expansion_limit
                                                      will take precedence over
                                                      shard_shrink_merge_point.

shard_shrink_merge_point          75                  Deprecated: expansion_limit
                                                      is recommended and if set
                                                      will take precedence over
                                                      shard_shrink_merge_point.
                                                      This defines the
                                                      maximum allowed size of
                                                      an acceptor shard
                                                      container after having a
                                                      donor merged into it.
                                                      Shard_shrink_merge_point
                                                      is a percentage of
                                                      shard_container_threshold.
                                                      e.g. the default value of
                                                      75 means that the
                                                      projected sum of a donor
                                                      object count and acceptor
                                                      count must be less than
                                                      75% of shard_container_threshold
                                                      for the donor to be
                                                      allowed to merge into the
                                                      acceptor.

                                                      For example, if
                                                      shard_container_threshold
                                                      is 1 million,
                                                      shard_shrink_point is 10,
                                                      and shard_shrink_merge_point
                                                      is 75 then a shard will
                                                      be considered for
                                                      shrinking if it has less
                                                      than or equal to 100
                                                      thousand objects but will
                                                      only merge into an
                                                      acceptor if the combined
                                                      object count would be
                                                      less than or equal to 750
                                                      thousand objects.


shard_scanner_batch_size          10                  When auto-sharding is
                                                      enabled this defines the
                                                      maximum number of shard
                                                      ranges that will be found
                                                      each time the sharder
                                                      daemon visits a sharding
                                                      container. If necessary
                                                      the sharder daemon will
                                                      continue to search for
                                                      more shard ranges each
                                                      time it visits the
                                                      container.

cleave_batch_size                 2                   Defines the number of
                                                      shard ranges that will be
                                                      cleaved each time the
                                                      sharder daemon visits a
                                                      sharding container.

cleave_row_batch_size             10000               Defines the size of
                                                      batches of object rows
                                                      read from a sharding
                                                      container and merged to a
                                                      shard container during
                                                      cleaving.

shard_replication_quorum          auto                Defines the number of
                                                      successfully replicated
                                                      shard dbs required when
                                                      cleaving a previously
                                                      uncleaved shard range
                                                      before the sharder will
                                                      progress to the next
                                                      shard range. The value
                                                      should be less than or
                                                      equal to the container
                                                      ring replica count. The
                                                      default of 'auto' causes
                                                      the container ring quorum
                                                      value to be used. This
                                                      option only applies to
                                                      the container-sharder
                                                      replication and does not
                                                      affect the number of
                                                      shard container replicas
                                                      that will eventually be
                                                      replicated by the
                                                      container-replicator.


existing_shard_replication_quorum auto                Defines the number of
                                                      successfully replicated
                                                      shard dbs required when
                                                      cleaving a shard range
                                                      that has been previously
                                                      cleaved on another node
                                                      before the sharder will
                                                      progress to the next
                                                      shard range. The value
                                                      should be less than or
                                                      equal to the container
                                                      ring replica count. The
                                                      default of 'auto' causes
                                                      the shard_replication_quorum
                                                      value to be used. This
                                                      option only applies to
                                                      the container-sharder
                                                      replication and does not
                                                      affect the number of
                                                      shard container replicas
                                                      that will eventually be
                                                      replicated by the
                                                      container-replicator.

internal_client_conf_path         see description     The sharder uses an
                                                      internal client to create
                                                      and make requests to
                                                      containers. The absolute
                                                      path to the client config
                                                      file can be configured.
                                                      Defaults to
                                                      /etc/swift/internal-client.conf

request_tries                     3                   The number of time the
                                                      internal client will
                                                      retry requests.

recon_candidates_limit            5                   Each time the sharder
                                                      dumps stats to the recon
                                                      cache file it includes a
                                                      list of containers that
                                                      appear to need sharding
                                                      but are not yet sharding.
                                                      By default this list is
                                                      limited to the top 5
                                                      containers, ordered by
                                                      object count. The limit
                                                      may be changed by setting
                                                      recon_candidates_limit to
                                                      an integer value. A
                                                      negative value implies no
                                                      limit.

broker_timeout                    60                  Large databases tend to
                                                      take a while to work
                                                      with, but we want to make
                                                      sure we write down our
                                                      progress. Use a
                                                      larger-than-normal broker
                                                      timeout to make us less
                                                      likely to bomb out on a
                                                      LockTimeout.
================================= =================  =======================================

*******************
[container-updater]
*******************

========================  =================  ==================================
Option                    Default            Description
------------------------  -----------------  ----------------------------------
log_name                  container-updater  Label used when logging
log_facility              LOG_LOCAL0         Syslog log facility
log_level                 INFO               Logging level
log_address               /dev/log           Logging directory
interval                  300                Minimum time for a pass to take
concurrency               4                  Number of updater workers to spawn
node_timeout              3                  Request timeout to external
                                             services
conn_timeout              0.5                Connection timeout to external
                                             services
containers_per_second     50                 Maximum containers updated per second.
                                             Should be tuned according to individual
                                             system specs. 0 is unlimited.

slowdown                  0.01               Time in seconds to wait between
                                             containers. Deprecated in favor of
                                             containers_per_second.
account_suppression_time  60                 Seconds to suppress updating an
                                             account that has generated an
                                             error (timeout, not yet found,
                                             etc.)
recon_cache_path          /var/cache/swift   Path to recon cache
nice_priority             None               Scheduling priority of server
                                             processes. Niceness values range
                                             from -20 (most favorable to the
                                             process) to 19 (least favorable
                                             to the process). The default does
                                             not modify priority.
ionice_class              None               I/O scheduling class of server
                                             processes. I/O niceness class
                                             values are IOPRIO_CLASS_RT (realtime),
                                             IOPRIO_CLASS_BE (best-effort),
                                             and IOPRIO_CLASS_IDLE (idle).
                                             The default does not modify class and
                                             priority. Linux supports io scheduling
                                             priorities and classes since 2.6.13 with
                                             the CFQ io scheduler.
                                             Work only with ionice_priority.
ionice_priority           None               I/O scheduling priority of server
                                             processes. I/O niceness priority is
                                             a number which goes from 0 to 7.
                                             The higher the value, the lower
                                             the I/O priority of the process.
                                             Work only with ionice_class.
                                             Ignored if IOPRIO_CLASS_IDLE is set.
========================  =================  ==================================

*******************
[container-auditor]
*******************

=====================  =================  =======================================
Option                 Default            Description
---------------------  -----------------  ---------------------------------------
log_name               container-auditor  Label used when logging
log_facility           LOG_LOCAL0         Syslog log facility
log_level              INFO               Logging level
log_address            /dev/log           Logging directory
interval               1800               Minimum time for a pass to take
containers_per_second  200                Maximum containers audited per second.
                                          Should be tuned according to individual
                                          system specs. 0 is unlimited.
recon_cache_path       /var/cache/swift   Path to recon cache
nice_priority          None               Scheduling priority of server processes.
                                          Niceness values range from -20 (most
                                          favorable to the process) to 19 (least
                                          favorable to the process). The default
                                          does not modify priority.
ionice_class           None               I/O scheduling class of server processes.
                                          I/O niceness class values are
                                          IOPRIO_CLASS_RT (realtime),
                                          IOPRIO_CLASS_BE (best-effort),
                                          and IOPRIO_CLASS_IDLE (idle).
                                          The default does not modify class and
                                          priority. Linux supports io scheduling
                                          priorities and classes since 2.6.13 with
                                          the CFQ io scheduler.
                                          Work only with ionice_priority.
ionice_priority        None               I/O scheduling priority of server
                                          processes. I/O niceness priority is
                                          a number which goes from 0 to 7.
                                          The higher the value, the lower the I/O
                                          priority of the process. Work only with
                                          ionice_class.
                                          Ignored if IOPRIO_CLASS_IDLE is set.
=====================  =================  =======================================
