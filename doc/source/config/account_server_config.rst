.. _account-server-config:

----------------------------
Account Server Configuration
----------------------------

This document describes the configuration options available for the account
server. Documentation for other swift configuration options can be found at
:doc:`index`.

An example Account Server configuration can be found at
etc/account-server.conf-sample in the source code repository.

The following configuration sections are available:

* :ref:`[DEFAULT] <account_server_default_options>`
* `[account-server]`_
* `[account-replicator]`_
* `[account-auditor]`_
* `[account-reaper]`_

.. _account_server_default_options:

*********
[DEFAULT]
*********

===============================  ==========  =============================================
Option                           Default     Description
-------------------------------  ----------  ---------------------------------------------
swift_dir                        /etc/swift  Swift configuration directory
devices                          /srv/node   Parent directory or where devices are mounted
mount_check                      true        Whether or not check if the devices are
                                             mounted to prevent accidentally writing
                                             to the root device
bind_ip                          0.0.0.0     IP Address for server to bind to
bind_port                        6202        Port for server to bind to
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
db_preallocation                 off         If you don't mind the extra disk space usage in
                                             overhead, you can turn this on to preallocate
                                             disk space with SQLite databases to decrease
                                             fragmentation.
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
ionice_priority                  None        I/O scheduling priority of server processes.
                                             I/O niceness priority is a number which
                                             goes from 0 to 7. The higher the value,
                                             the lower the I/O priority of the process.
                                             Work only with ionice_class.
                                             Ignored if IOPRIO_CLASS_IDLE is set.
===============================  ==========  =============================================

****************
[account-server]
****************

=============================  ==============  ==========================================
Option                         Default         Description
-----------------------------  --------------  ------------------------------------------
use                                            Entry point for paste.deploy for the account
                                               server.  For most cases, this should be
                                               ``egg:swift#account``.
set log_name                   account-server  Label used when logging
set log_facility               LOG_LOCAL0      Syslog log facility
set log_level                  INFO            Logging level
set log_requests               True            Whether or not to log each
                                               request
set log_address                /dev/log        Logging directory
replication_server                             Configure parameter for creating
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
nice_priority                  None            Scheduling priority of server processes.
                                               Niceness values range from -20 (most
                                               favorable to the process) to 19 (least
                                               favorable to the process). The default
                                               does not modify priority.
ionice_class                   None            I/O scheduling class of server processes.
                                               I/O niceness class values are IOPRIO_CLASS_RT
                                               (realtime), IOPRIO_CLASS_BE (best-effort),
                                               and IOPRIO_CLASS_IDLE (idle).
                                               The default does not modify class and
                                               priority. Linux supports io scheduling
                                               priorities and classes since 2.6.13 with
                                               the CFQ io scheduler.
                                               Work only with ionice_priority.
ionice_priority                None            I/O scheduling priority of server
                                               processes. I/O niceness priority is
                                               a number which goes from 0 to 7.
                                               The higher the value, the lower the I/O
                                               priority of the process. Work only with
                                               ionice_class.
                                               Ignored if IOPRIO_CLASS_IDLE is set.
=============================  ==============  ==========================================

********************
[account-replicator]
********************

==================== =========================  =====================================
Option               Default                    Description
-------------------- -------------------------  -------------------------------------
log_name             account-replicator         Label used when logging
log_facility         LOG_LOCAL0                 Syslog log facility
log_level            INFO                       Logging level
log_address          /dev/log                   Logging directory
per_diff             1000                       Maximum number of database rows
                                                that will be sync'd in a single
                                                HTTP replication request.
                                                Databases with less than or
                                                equal to this number of
                                                differing rows will always be
                                                sync'd using an HTTP replication
                                                request rather than using rsync.
max_diffs            100                        Maximum number of HTTP
                                                replication requests attempted
                                                on each replication pass for any
                                                one container. This caps how
                                                long the replicator will spend
                                                trying to sync a given database
                                                per pass so the other databases
                                                don't get starved.
concurrency          8                          Number of replication workers
                                                to spawn
interval             30                         Time in seconds to wait between
                                                replication passes
databases_per_second 50                         Maximum databases to process
                                                per second.  Should be tuned
                                                according to individual
                                                system specs.  0 is unlimited.
node_timeout         10                         Request timeout to external
                                                services
conn_timeout         0.5                        Connection timeout to external
                                                services
reclaim_age          604800                     Time elapsed in seconds before
                                                an account can be reclaimed
rsync_module         {replication_ip}::account  Format of the rsync module where
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
rsync_compress       no                         Allow rsync to compress data
                                                which is transmitted to
                                                destination node during sync.
                                                However, this is applicable only
                                                when destination node is in a
                                                different region than the local
                                                one. NOTE: Objects that are
                                                already compressed (for example:
                                                .tar.gz, mp3) might slow down
                                                the syncing process.
recon_cache_path     /var/cache/swift           Path to recon cache
nice_priority        None                       Scheduling priority of server
                                                processes. Niceness values
                                                range from -20 (most favorable
                                                to the process) to 19 (least
                                                favorable to the process).
                                                The default does not modify
                                                priority.
ionice_class         None                       I/O scheduling class of server
                                                processes. I/O niceness class
                                                values are IOPRIO_CLASS_RT
                                                (realtime), IOPRIO_CLASS_BE
                                                (best-effort), and IOPRIO_CLASS_IDLE
                                                (idle).
                                                The default does not modify
                                                class and priority. Linux supports
                                                io scheduling priorities and classes
                                                since 2.6.13 with the CFQ io scheduler.
                                                Work only with ionice_priority.
ionice_priority      None                       I/O scheduling priority of server
                                                processes. I/O niceness priority
                                                is a number which goes from 0 to 7.
                                                The higher the value, the lower
                                                the I/O priority of the process.
                                                Work only with ionice_class.
                                                Ignored if IOPRIO_CLASS_IDLE
                                                is set.
handoffs_only        no                         When handoffs_only mode is enabled
                                                the replicator will *only* replicate
                                                from handoff nodes to primary nodes
                                                and will not sync primary nodes
                                                with other primary nodes.
handoff_delete       auto                       the number of replicas which are
                                                ensured in swift. If the number
                                                less than the number of replicas
                                                is set, account-replicator
                                                could delete local handoffs even
                                                if all replicas are not ensured in
                                                the cluster. The replicator would
                                                remove local handoff account database
                                                after syncing when the number of
                                                successful responses is greater than
                                                or equal to this number. By default
                                                handoff partitions will be removed
                                                when it has successfully replicated
                                                to all the canonical nodes.
==================== =========================  =====================================

*****************
[account-auditor]
*****************

====================  ================  =======================================
Option                Default           Description
--------------------  ----------------  ---------------------------------------
log_name              account-auditor   Label used when logging
log_facility          LOG_LOCAL0        Syslog log facility
log_level             INFO              Logging level
log_address           /dev/log          Logging directory
interval              1800              Minimum time for a pass to take
accounts_per_second   200               Maximum accounts audited per second.
                                        Should be tuned according to individual
                                        system specs. 0 is unlimited.
recon_cache_path      /var/cache/swift  Path to recon cache
nice_priority         None              Scheduling priority of server processes.
                                        Niceness values range from -20 (most
                                        favorable to the process) to 19 (least
                                        favorable to the process). The default
                                        does not modify priority.
ionice_class          None              I/O scheduling class of server processes.
                                        I/O niceness class values are
                                        IOPRIO_CLASS_RT (realtime),
                                        IOPRIO_CLASS_BE (best-effort),
                                        and IOPRIO_CLASS_IDLE (idle).
                                        The default does not modify class and
                                        priority. Linux supports io scheduling
                                        priorities and classes since 2.6.13 with
                                        the CFQ io scheduler.
                                        Work only with ionice_priority.
ionice_priority       None              I/O scheduling priority of server
                                        processes. I/O niceness priority is
                                        a number which goes from 0 to 7.
                                        The higher the value, the lower the I/O
                                        priority of the process. Work only with
                                        ionice_class.
                                        Ignored if IOPRIO_CLASS_IDLE is set.
====================  ================  =======================================

****************
[account-reaper]
****************

==================  ===============  =========================================
Option              Default          Description
------------------  ---------------  -----------------------------------------
log_name            account-reaper   Label used when logging
log_facility        LOG_LOCAL0       Syslog log facility
log_level           INFO             Logging level
log_address         /dev/log         Logging directory
concurrency         25               Number of replication workers to spawn
interval            3600             Minimum time for a pass to take
node_timeout        10               Request timeout to external services
conn_timeout        0.5              Connection timeout to external services
delay_reaping       0                Normally, the reaper begins deleting
                                     account information for deleted accounts
                                     immediately; you can set this to delay
                                     its work however. The value is in seconds,
                                     2592000 = 30 days, for example. The sum of
                                     this value and the container-updater
                                     ``interval`` should be less than the
                                     account-replicator ``reclaim_age``. This
                                     ensures that once the account-reaper has
                                     deleted a container there is sufficient
                                     time for the container-updater to report
                                     to the account before the account DB is
                                     removed.
reap_warn_after     2892000          If the account fails to be reaped due
                                     to a persistent error, the account reaper
                                     will log a message such as:
                                     Account <name> has not been reaped since <date>
                                     You can search logs for this message if
                                     space is not being reclaimed after you
                                     delete account(s). This is in addition to
                                     any time requested by delay_reaping.
nice_priority       None             Scheduling priority of server processes.
                                     Niceness values range from -20 (most
                                     favorable to the process) to 19 (least
                                     favorable to the process). The default
                                     does not modify priority.
ionice_class        None             I/O scheduling class of server processes.
                                     I/O niceness class values are IOPRIO_CLASS_RT
                                     (realtime), IOPRIO_CLASS_BE (best-effort),
                                     and IOPRIO_CLASS_IDLE (idle).
                                     The default does not modify class and
                                     priority. Linux supports io scheduling
                                     priorities and classes since 2.6.13 with
                                     the CFQ io scheduler.
                                     Work only with ionice_priority.
ionice_priority     None             I/O scheduling priority of server
                                     processes. I/O niceness priority is
                                     a number which goes from 0 to 7.
                                     The higher the value, the lower the I/O
                                     priority of the process. Work only with
                                     ionice_class.
                                     Ignored if IOPRIO_CLASS_IDLE is set.
==================  ===============  =========================================
