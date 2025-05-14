.. _object-server-config:

---------------------------
Object Server Configuration
---------------------------

This document describes the configuration options available for the object
server. Documentation for other swift configuration options can be found at
:doc:`index`.

An Example Object Server configuration can be found at
etc/object-server.conf-sample in the source code repository.

The following configuration sections are available:

* :ref:`[DEFAULT] <object-server-default-options>`
* `[object-server]`_
* `[object-replicator]`_
* `[object-reconstructor]`_
* `[object-updater]`_
* `[object-auditor]`_
* `[object-expirer]`_

.. _object-server-default-options:

*********
[DEFAULT]
*********

================================ ==========  ============================================
Option                           Default     Description
-------------------------------- ----------  --------------------------------------------
swift_dir                        /etc/swift  Swift configuration directory
devices                          /srv/node   Parent directory of where devices are
                                             mounted
mount_check                      true        Whether or not check if the devices are
                                             mounted to prevent accidentally writing
                                             to the root device
bind_ip                          0.0.0.0     IP Address for server to bind to
bind_port                        6200        Port for server to bind to
keep_idle                        600         Value to set for socket TCP_KEEPIDLE
bind_timeout                     30          Seconds to attempt bind before giving up
backlog                          4096        Maximum number of allowed pending
                                             connections
workers                          auto        Override the number of pre-forked workers
                                             that will accept connections.  If set it
                                             should be an integer, zero means no fork.
                                             If unset, it will try to default to the
                                             number of effective cpu cores and fallback
                                             to one. Increasing the number of workers
                                             helps slow filesystem operations in one
                                             request from negatively impacting other
                                             requests, but only the
                                             :ref:`servers_per_port
                                             <server-per-port-configuration>` option
                                             provides complete I/O isolation with no
                                             measurable overhead.
servers_per_port                 0           If each disk in each storage policy ring
                                             has unique port numbers for its "ip"
                                             value, you can use this setting to have
                                             each object-server worker only service
                                             requests for the single disk matching the
                                             port in the ring. The value of this
                                             setting determines how many worker
                                             processes run for each port (disk) in the
                                             ring. If you have 24 disks per server, and
                                             this setting is 4, then each storage node
                                             will have 1 + (24 * 4) = 97 total
                                             object-server processes running. This
                                             gives complete I/O isolation, drastically
                                             reducing the impact of slow disks on
                                             storage node performance. The
                                             object-replicator and object-reconstructor
                                             need to see this setting too, so it must
                                             be in the [DEFAULT] section.
                                             See :ref:`server-per-port-configuration`.
max_clients                      1024        Maximum number of clients one worker can
                                             process simultaneously (it will actually
                                             accept(2) N + 1). Setting this to one (1)
                                             will only handle one request at a time,
                                             without accepting another request
                                             concurrently.
disable_fallocate                false       Disable "fast fail" fallocate checks if
                                             the underlying filesystem does not support
                                             it.
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
eventlet_debug                   false       If true, turn on debug logging for
                                             eventlet
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
conn_timeout                     0.5         Time to wait while attempting to connect
                                             to another backend node.
node_timeout                     3           Time to wait while sending each chunk of
                                             data to another backend node.
client_timeout                   60          Time to wait while receiving each chunk of
                                             data from a client or another backend node
network_chunk_size               65536       Size of chunks to read/write over the
                                             network
disk_chunk_size                  65536       Size of chunks to read/write to disk
container_update_timeout         1           Time to wait while sending a container
                                             update on object update.
reclaim_age                      604800      Time elapsed in seconds before the tombstone
                                             file representing a deleted object can be
                                             reclaimed.  This is the maximum window for
                                             your consistency engine.  If a node that was
                                             disconnected from the cluster because of a
                                             fault is reintroduced into the cluster after
                                             this window without having its data purged
                                             it will result in dark data.  This setting
                                             should be consistent across all object
                                             services.
commit_window                    60          Non-durable data files may also
                                             get reclaimed if they are older
                                             than reclaim_age, but not if the
                                             time they were written to disk
                                             (i.e. mtime) is less than
                                             commit_window seconds ago. A
                                             commit_window greater than zero is
                                             strongly recommended to avoid
                                             unintended reclamation of data
                                             files that were about to become
                                             durable; commit_window should be
                                             much less than reclaim_age.
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
                                             priorities and classes since 2.6.13 with
                                             the CFQ io scheduler.
                                             Work only with ionice_priority.
ionice_priority                  None        I/O scheduling priority of server
                                             processes. I/O niceness priority is
                                             a number which goes from 0 to 7.
                                             The higher the value, the lower the I/O
                                             priority of the process. Work only with
                                             ionice_class.
                                             Ignored if IOPRIO_CLASS_IDLE is set.
================================ ==========  ============================================

.. _object-server-options:

***************
[object-server]
***************

================================== ====================== ===============================================
Option                             Default                Description
---------------------------------- ---------------------- -----------------------------------------------
use                                                       paste.deploy entry point for the
                                                          object server.  For most cases,
                                                          this should be
                                                          ``egg:swift#object``.
set log_name                       object-server          Label used when logging
set log_facility                   LOG_LOCAL0             Syslog log facility
set log_level                      INFO                   Logging level
set log_requests                   True                   Whether or not to log each
                                                          request
set log_address                    /dev/log               Logging directory
user                               swift                  User to run as
max_upload_time                    86400                  Maximum time allowed to upload an
                                                          object
slow                               0                      If > 0, Minimum time in seconds for a PUT or
                                                          DELETE request to complete.  This is only
                                                          useful to simulate slow devices during testing
                                                          and development.
mb_per_sync                        512                    On PUT requests, sync file every
                                                          n MB
keep_cache_size                    5242880                Largest object size to keep in
                                                          buffer cache
keep_cache_private                 false                  Allow non-public objects to stay
                                                          in kernel's buffer cache
keep_cache_slo_manifest            false                  Allow SLO object's manifest file to stay in
                                                          kernel's buffer cache if its size is under
                                                          keep_cache_size. This config will only matter
                                                          when 'keep_cache_private' is false.
allowed_headers                    Content-Disposition,   Comma separated list of headers
                                   Content-Encoding,      that can be set in metadata on an object.
                                   X-Delete-At,           This list is in addition to
                                   X-Object-Manifest,     X-Object-Meta-* headers and cannot include
                                   X-Static-Large-Object  Content-Type, etag, Content-Length, or deleted
                                   Cache-Control,
                                   Content-Language,
                                   Expires,
                                   X-Robots-Tag
replication_server                                        Configure parameter for creating
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
replication_concurrency            4                      Set to restrict the number of
                                                          concurrent incoming SSYNC
                                                          requests; set to 0 for unlimited
replication_concurrency_per_device 1                      Set to restrict the number of
                                                          concurrent incoming SSYNC
                                                          requests per device; set to 0 for
                                                          unlimited requests per devices.
                                                          This can help control I/O to each
                                                          device. This does not override
                                                          replication_concurrency described
                                                          above, so you may need to adjust
                                                          both parameters depending on your
                                                          hardware or network capacity.
replication_lock_timeout           15                     Number of seconds to wait for an
                                                          existing replication device lock
                                                          before giving up.
replication_failure_threshold      100                    The number of subrequest failures
                                                          before the
                                                          replication_failure_ratio is
                                                          checked
replication_failure_ratio          1.0                    If the value of failures /
                                                          successes of SSYNC
                                                          subrequests exceeds this ratio,
                                                          the overall SSYNC request
                                                          will be aborted
splice                             no                     Use splice() for zero-copy object
                                                          GETs. This requires Linux kernel
                                                          version 3.0 or greater. If you set
                                                          "splice = yes" but the kernel
                                                          does not support it, error messages
                                                          will appear in the object server
                                                          logs at startup, but your object
                                                          servers should continue to function.
nice_priority                      None                   Scheduling priority of server processes.
                                                          Niceness values range from -20 (most
                                                          favorable to the process) to 19 (least
                                                          favorable to the process). The default
                                                          does not modify priority.
ionice_class                       None                   I/O scheduling class of server processes.
                                                          I/O niceness class values are IOPRIO_CLASS_RT
                                                          (realtime), IOPRIO_CLASS_BE (best-effort),
                                                          and IOPRIO_CLASS_IDLE (idle).
                                                          The default does not modify class and
                                                          priority. Linux supports io scheduling
                                                          priorities and classes since 2.6.13 with
                                                          the CFQ io scheduler.
                                                          Work only with ionice_priority.
ionice_priority                    None                   I/O scheduling priority of server
                                                          processes. I/O niceness priority is
                                                          a number which goes from 0 to 7.
                                                          The higher the value, the lower the I/O
                                                          priority of the process. Work only with
                                                          ionice_class.
                                                          Ignored if IOPRIO_CLASS_IDLE is set.
eventlet_tpool_num_threads         auto                   The number of threads in eventlet's thread pool.
                                                          Most IO will occur in the object server's main
                                                          thread, but certain "heavy" IO operations will
                                                          occur in separate IO threads, managed by
                                                          eventlet.
                                                          The default value is auto, whose actual value
                                                          is dependent on the servers_per_port value.
                                                          If servers_per_port is zero then it uses
                                                          eventlet's default (currently 20 threads).
                                                          If the servers_per_port is nonzero then it'll
                                                          only use 1 thread per process.
                                                          This value can be overridden with an integer
                                                          value.
================================== ====================== ===============================================

*******************
[object-replicator]
*******************

===========================  ========================  ================================
Option                       Default                   Description
---------------------------  ------------------------  --------------------------------
log_name                     object-replicator         Label used when logging
log_facility                 LOG_LOCAL0                Syslog log facility
log_level                    INFO                      Logging level
log_address                  /dev/log                  Logging directory
daemonize                    yes                       Whether or not to run replication
                                                       as a daemon
interval                     30                        Time in seconds to wait between
                                                       replication passes
concurrency                  1                         Number of replication jobs to
                                                       run per worker process
replicator_workers           0                         Number of worker processes to use.
                                                       No matter how big this number is,
                                                       at most one worker per disk will
                                                       be used. The default value of 0
                                                       means no forking; all work is done
                                                       in the main process.
sync_method                  rsync                     The sync method to use; default
                                                       is rsync but you can use ssync to
                                                       try the EXPERIMENTAL
                                                       all-swift-code-no-rsync-callouts
                                                       method. Once ssync is verified as
                                                       or better than, rsync, we plan to
                                                       deprecate rsync so we can move on
                                                       with more features for
                                                       replication.
rsync_timeout                900                       Max duration of a partition rsync
rsync_bwlimit                0                         Bandwidth limit for rsync in kB/s.
                                                       0 means unlimited.
rsync_io_timeout             30                        Timeout value sent to rsync
                                                       --timeout and --contimeout
                                                       options
rsync_compress               no                        Allow rsync to compress data
                                                       which is transmitted to destination
                                                       node during sync. However, this
                                                       is applicable only when destination
                                                       node is in a different region
                                                       than the local one.
                                                       NOTE: Objects that are already
                                                       compressed (for example: .tar.gz,
                                                       .mp3) might slow down the syncing
                                                       process.
stats_interval               300                       Interval in seconds between
                                                       logging replication statistics
handoffs_first               false                     If set to True, partitions that
                                                       are not supposed to be on the
                                                       node will be replicated first.
                                                       The default setting should not be
                                                       changed, except for extreme
                                                       situations.
handoff_delete               auto                      By default handoff partitions
                                                       will be removed when it has
                                                       successfully replicated to all
                                                       the canonical nodes. If set to an
                                                       integer n, it will remove the
                                                       partition if it is successfully
                                                       replicated to n nodes.  The
                                                       default setting should not be
                                                       changed, except for extreme
                                                       situations.
node_timeout                 DEFAULT or 10             Request timeout to external
                                                       services. This uses what's set
                                                       here, or what's set in the
                                                       DEFAULT section, or 10 (though
                                                       other sections use 3 as the final
                                                       default).
http_timeout                 60                        Max duration of an http request.
                                                       This is for REPLICATE finalization
                                                       calls and so should be longer
                                                       than node_timeout.
lockup_timeout               1800                      Attempts to kill all workers if
                                                       nothing replicates for
                                                       lockup_timeout seconds
rsync_module                 {replication_ip}::object  Format of the rsync module where
                                                       the replicator will send data.
                                                       The configuration value can
                                                       include some variables that will
                                                       be extracted from the ring.
                                                       Variables must follow the format
                                                       {NAME} where NAME is one of: ip,
                                                       port, replication_ip,
                                                       replication_port, region, zone,
                                                       device, meta. See
                                                       etc/rsyncd.conf-sample for some
                                                       examples.
rsync_error_log_line_length  0                         Limits how long rsync error log
                                                       lines are
ring_check_interval          15                        Interval for checking new ring
                                                       file
recon_cache_path             /var/cache/swift          Path to recon cache
nice_priority                None                      Scheduling priority of server
                                                       processes. Niceness values
                                                       range from -20 (most favorable
                                                       to the process) to 19 (least
                                                       favorable to the process).
                                                       The default does not modify
                                                       priority.
ionice_class                 None                      I/O scheduling class of server
                                                       processes. I/O niceness class
                                                       values are IOPRIO_CLASS_RT (realtime),
                                                       IOPRIO_CLASS_BE (best-effort),
                                                       and IOPRIO_CLASS_IDLE (idle).
                                                       The default does not modify
                                                       class and priority.
                                                       Linux supports io scheduling
                                                       priorities and classes since
                                                       2.6.13 with the CFQ io scheduler.
                                                       Work only with ionice_priority.
ionice_priority              None                      I/O scheduling priority of server
                                                       processes. I/O niceness priority
                                                       is a number which goes from
                                                       0 to 7. The higher the value,
                                                       the lower the I/O priority of
                                                       the process.
                                                       Work only with ionice_class.
                                                       Ignored if IOPRIO_CLASS_IDLE
                                                       is set.
===========================  ========================  ================================

**********************
[object-reconstructor]
**********************

===========================  ========================  ================================
Option                       Default                   Description
---------------------------  ------------------------  --------------------------------
log_name                     object-reconstructor      Label used when logging
log_facility                 LOG_LOCAL0                Syslog log facility
log_level                    INFO                      Logging level
log_address                  /dev/log                  Logging directory
daemonize                    yes                       Whether or not to run
                                                       reconstruction as a daemon
interval                     30                        Time in seconds to wait between
                                                       reconstruction passes
reconstructor_workers        0                         Maximum number of worker processes
                                                       to spawn.  Each worker will handle
                                                       a subset of devices.  Devices will
                                                       be assigned evenly among the workers
                                                       so that workers cycle at similar
                                                       intervals (which can lead to fewer
                                                       workers than requested).  You can not
                                                       have more workers than devices.  If
                                                       you have no devices only a single
                                                       worker is spawned.
concurrency                  1                         Number of reconstruction threads to
                                                       spawn per reconstructor process.
stats_interval               300                       Interval in seconds between
                                                       logging reconstruction statistics
handoffs_only                false                     The handoffs_only mode option is for
                                                       special case emergency situations
                                                       during rebalance such as disk full in
                                                       the cluster.  This option SHOULD NOT
                                                       BE CHANGED, except for extreme
                                                       situations.  When handoffs_only mode
                                                       is enabled the reconstructor will
                                                       *only* revert fragments from handoff
                                                       nodes to primary nodes and will not
                                                       sync primary nodes with neighboring
                                                       primary nodes.  This will force the
                                                       reconstructor to sync and delete
                                                       handoffs' fragments more quickly and
                                                       minimize the time of the rebalance by
                                                       limiting the number of rebuilds.  The
                                                       handoffs_only option is only for
                                                       temporary use and should be disabled
                                                       as soon as the emergency situation
                                                       has been resolved.
rebuild_handoff_node_count   2                         The default strategy for unmounted
                                                       drives will stage
                                                       rebuilt data on a
                                                       handoff node until
                                                       updated rings are
                                                       deployed.  Because
                                                       fragments are rebuilt on
                                                       offset handoffs based on
                                                       fragment index and the
                                                       proxy limits how deep it
                                                       will search for EC frags
                                                       we restrict how many
                                                       nodes we'll try.
                                                       Setting to 0 will
                                                       disable rebuilds to
                                                       handoffs and only
                                                       rebuild fragments for
                                                       unmounted devices to
                                                       mounted primaries after
                                                       a ring change. Setting
                                                       to -1 means "no limit".
max_objects_per_revert       0                         By default the reconstructor
                                                       attempts to revert all
                                                       objects from handoff
                                                       partitions in a single
                                                       batch using a single
                                                       SSYNC request. In
                                                       exceptional
                                                       circumstances
                                                       max_objects_per_revert
                                                       can be used to
                                                       temporarily limit the
                                                       number of objects
                                                       reverted by each
                                                       reconstructor revert
                                                       type job. If more than
                                                       max_objects_per_revert
                                                       are available in a
                                                       sender's handoff
                                                       partition, the remaining
                                                       objects will remain in
                                                       the handoff partition
                                                       and will not be reverted
                                                       until the next time the
                                                       reconstructor visits
                                                       that handoff partition
                                                       i.e. with this option
                                                       set, a single cycle of
                                                       the reconstructor may
                                                       not completely revert
                                                       all handoff partitions.
                                                       The option has no effect
                                                       on reconstructor sync
                                                       type jobs between
                                                       primary partitions. A
                                                       value of 0 (the default)
                                                       means there is no limit.
node_timeout                 DEFAULT or 10             Request timeout to external
                                                       services. The value used is the value
                                                       set in this section, or the value set
                                                       in the DEFAULT section, or 10.
http_timeout                 60                        Max duration of an http request.
                                                       This is for REPLICATE finalization
                                                       calls and so should be longer
                                                       than node_timeout.
lockup_timeout               1800                      Attempts to kill all threads if
                                                       no fragment has been reconstructed
                                                       for lockup_timeout seconds.
ring_check_interval          15                        Interval for checking new ring
                                                       file
recon_cache_path             /var/cache/swift          Path to recon cache
nice_priority                None                      Scheduling priority of server
                                                       processes. Niceness values
                                                       range from -20 (most favorable
                                                       to the process) to 19 (least
                                                       favorable to the process).
                                                       The default does not modify
                                                       priority.
ionice_class                 None                      I/O scheduling class of server
                                                       processes. I/O niceness class
                                                       values are IOPRIO_CLASS_RT (realtime),
                                                       IOPRIO_CLASS_BE (best-effort),
                                                       and IOPRIO_CLASS_IDLE (idle).
                                                       The default does not modify
                                                       class and priority.
                                                       Linux supports io scheduling
                                                       priorities and classes since
                                                       2.6.13 with the CFQ io scheduler.
                                                       Work only with ionice_priority.
ionice_priority              None                      I/O scheduling priority of server
                                                       processes. I/O niceness priority
                                                       is a number which goes from
                                                       0 to 7. The higher the value,
                                                       the lower the I/O priority of
                                                       the process.
                                                       Work only with ionice_class.
                                                       Ignored if IOPRIO_CLASS_IDLE
                                                       is set.
quarantine_threshold         0                         The reconstructor may quarantine
                                                       stale isolated fragments
                                                       when it fails to fetch
                                                       more than the
                                                       quarantine_threshold
                                                       number of fragments
                                                       (including the stale
                                                       fragment) during an
                                                       attempt to reconstruct.
quarantine_age               reclaim_age               Fragments are not quarantined
                                                       until they are older than
                                                       quarantine_age, which defaults
                                                       to the value of reclaim_age.
===========================  ========================  ================================

****************
[object-updater]
****************

=================== =================== ==========================================
Option              Default             Description
------------------- ------------------- ------------------------------------------
log_name            object-updater      Label used when logging
log_facility        LOG_LOCAL0          Syslog log facility
log_level           INFO                Logging level
log_address         /dev/log            Logging directory
interval            300                 Minimum time for a pass to take
updater_workers     1                   Number of worker processes
concurrency         8                   Number of updates to run concurrently in
                                        each worker process
node_timeout        DEFAULT or 10       Request timeout to external services. This
                                        uses what's set here, or what's set in the
                                        DEFAULT section, or 10 (though other
                                        sections use 3 as the final default).
objects_per_second  50                  Maximum objects updated per second.
                                        Should be tuned according to individual
                                        system specs. 0 is unlimited.
slowdown            0.01                Time in seconds to wait between objects.
                                        Deprecated in favor of objects_per_second.
report_interval     300                 Interval in seconds between logging
                                        statistics about the current update pass.
recon_cache_path    /var/cache/swift    Path to recon cache
nice_priority       None                Scheduling priority of server processes.
                                        Niceness values range from -20 (most
                                        favorable to the process) to 19 (least
                                        favorable to the process). The default
                                        does not modify priority.
ionice_class        None                I/O scheduling class of server processes.
                                        I/O niceness class values are IOPRIO_CLASS_RT
                                        (realtime), IOPRIO_CLASS_BE (best-effort),
                                        and IOPRIO_CLASS_IDLE (idle).
                                        The default does not modify class and
                                        priority. Linux supports io scheduling
                                        priorities and classes since 2.6.13 with
                                        the CFQ io scheduler.
                                        Work only with ionice_priority.
ionice_priority     None                I/O scheduling priority of server
                                        processes. I/O niceness priority is
                                        a number which goes from 0 to 7.
                                        The higher the value, the lower the I/O
                                        priority of the process. Work only with
                                        ionice_class.
                                        Ignored if IOPRIO_CLASS_IDLE is set.
=================== =================== ==========================================

****************
[object-auditor]
****************

=========================== =================== ==========================================
Option                      Default             Description
--------------------------- ------------------- ------------------------------------------
log_name                    object-auditor      Label used when logging
log_facility                LOG_LOCAL0          Syslog log facility
log_level                   INFO                Logging level
log_address                 /dev/log            Logging directory
log_time                    3600                Frequency of status logs in seconds.
interval                    30                  Time in seconds to wait between
                                                auditor passes
disk_chunk_size             65536               Size of chunks read during auditing
files_per_second            20                  Maximum files audited per second per
                                                auditor process. Should be tuned according
                                                to individual system specs. 0 is unlimited.
bytes_per_second            10000000            Maximum bytes audited per second per
                                                auditor process. Should be tuned according
                                                to individual system specs. 0 is unlimited.
concurrency                 1                   The number of parallel processes to use
                                                for checksum auditing.
zero_byte_files_per_second  50
object_size_stats
recon_cache_path            /var/cache/swift    Path to recon cache
rsync_tempfile_timeout      auto                Time elapsed in seconds before rsync
                                                tempfiles will be unlinked. Config value
                                                of "auto" try to use object-replicator's
                                                rsync_timeout + 900 or fallback to 86400
                                                (1 day).
nice_priority               None                Scheduling priority of server processes.
                                                Niceness values range from -20 (most
                                                favorable to the process) to 19 (least
                                                favorable to the process). The default
                                                does not modify priority.
ionice_class                None                I/O scheduling class of server processes.
                                                I/O niceness class values are IOPRIO_CLASS_RT
                                                (realtime), IOPRIO_CLASS_BE (best-effort),
                                                and IOPRIO_CLASS_IDLE (idle).
                                                The default does not modify class and
                                                priority. Linux supports io scheduling
                                                priorities and classes since 2.6.13 with
                                                the CFQ io scheduler.
                                                Work only with ionice_priority.
ionice_priority             None                I/O scheduling priority of server
                                                processes. I/O niceness priority is
                                                a number which goes from 0 to 7.
                                                The higher the value, the lower the I/O
                                                priority of the process. Work only with
                                                ionice_class.
                                                Ignored if IOPRIO_CLASS_IDLE is set.
=========================== =================== ==========================================

****************
[object-expirer]
****************

============================= =============================== ==========================================
Option                        Default                         Description
----------------------------- ------------------------------- ------------------------------------------
log_name                      object-expirer                  Label used when logging
log_facility                  LOG_LOCAL0                      Syslog log facility
log_level                     INFO                            Logging level
log_address                   /dev/log                        Logging directory
interval                      300                             Time in seconds to wait between
                                                              expirer passes
report_interval               300                             Frequency of status logs in seconds.
concurrency                   1                               Level of concurrency to use to do the work,
                                                              this value must be set to at least 1
dequeue_from_legacy           False                           This service will look for jobs on the
                                                              legacy expirer task queue.
round_robin_task_cache_size   100000                          Number of tasks objects to cache before processing.
processes                     0                               How many parts to divide the legacy work into,
                                                              one part per process that will be doing the work.
                                                              When set 0 means that a single legacy
                                                              process will be doing all the work.
                                                              This can only be used in conjunction with
                                                              ``dequeue_from_legacy``.
process                       0                               Which of the parts a particular legacy process will
                                                              work on. It is "zero based", if you want to use 3
                                                              processes, you should run processes with process
                                                              set to 0, 1, and 2.
                                                              This can only be used in conjunction with
                                                              ``dequeue_from_legacy``.
reclaim_age                   604800                          How long an un-processable expired object
                                                              marker will be retried before it is abandoned.
                                                              It is not coupled with the tombstone reclaim age
                                                              in the consistency engine.
request_tries                 3                               The number of times the expirer's internal client
                                                              will attempt any given request in the event
                                                              of failure
recon_cache_path              /var/cache/swift                Path to recon cache
nice_priority                 None                            Scheduling priority of server processes.
                                                              Niceness values range from -20 (most
                                                              favorable to the process) to 19 (least
                                                              favorable to the process). The default
                                                              does not modify priority.
ionice_class                  None                            I/O scheduling class of server processes.
                                                              I/O niceness class values are IOPRIO_CLASS_RT
                                                              (realtime), IOPRIO_CLASS_BE (best-effort),
                                                              and IOPRIO_CLASS_IDLE (idle).
                                                              The default does not modify class and
                                                              priority. Linux supports io scheduling
                                                              priorities and classes since 2.6.13 with
                                                              the CFQ io scheduler.
                                                              Work only with ionice_priority.
ionice_priority               None                            I/O scheduling priority of server
                                                              processes. I/O niceness priority is
                                                              a number which goes from 0 to 7.
                                                              The higher the value, the lower the I/O
                                                              priority of the process. Work only with
                                                              ionice_class.
                                                              Ignored if IOPRIO_CLASS_IDLE is set.
delay_reaping_<ACCT>           0.0                            A dynamic configuration option for
                                                              setting account level delay_reaping values.
                                                              The delay_reaping value is configured for
                                                              the account with the name placed in
                                                              <ACCT>. The object expirer will reap objects in
                                                              this account from disk only after this delay
                                                              following their x-delete-at time.
delay_reaping_<ACCT>/<CNTR>    0.0                            A dynamic configuration option for
                                                              setting container level delay_reaping values.
                                                              The delay_reaping value is configured for
                                                              the container with the account name placed
                                                              in <ACCT> and the container name in <CNTR>.
                                                              The object expirer will reap objects in this
                                                              container from disk only after this delay
                                                              following their x-delete-at time.
============================= =============================== ==========================================
