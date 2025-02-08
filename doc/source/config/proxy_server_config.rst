.. _proxy-server-config:

--------------------------
Proxy Server Configuration
--------------------------

This document describes the configuration options available for the proxy
server. Some proxy server options may be configured on a :ref:`per-policy
<proxy_server_per_policy_config>` basis. Additional documentation for
proxy-server middleware can be found at :doc:`../middleware` and
:doc:`../overview_auth`.

Documentation for other swift configuration options can be found at
:doc:`index`.

An example Proxy Server configuration can be found at
etc/proxy-server.conf-sample in the source code repository.

The following configuration sections are available:

* :ref:`[DEFAULT] <proxy_server_default_options>`
* `[proxy-server]`_


.. _proxy_server_default_options:

*********
[DEFAULT]
*********

====================================  ========================  ========================================
Option                                Default                   Description
------------------------------------  ------------------------  ----------------------------------------
bind_ip                               0.0.0.0                   IP Address for server to
                                                                bind to
bind_port                             80                        Port for server to bind to
keep_idle                             600                       Value to set for socket TCP_KEEPIDLE
bind_timeout                          30                        Seconds to attempt bind before
                                                                giving up
backlog                               4096                      Maximum number of allowed pending
                                                                connections
swift_dir                             /etc/swift                Swift configuration directory
workers                               auto                      Override the number of
                                                                pre-forked workers that will
                                                                accept connections.  If set it
                                                                should be an integer, zero
                                                                means no fork.  If unset, it
                                                                will try to default to the
                                                                number of effective cpu cores
                                                                and fallback to one.  See
                                                                :ref:`general-service-tuning`.
max_clients                           1024                      Maximum number of clients one
                                                                worker can process
                                                                simultaneously (it will
                                                                actually accept(2) N +
                                                                1). Setting this to one (1)
                                                                will only handle one request at
                                                                a time, without accepting
                                                                another request
                                                                concurrently.
user                                  swift                     User to run as
cert_file                                                       Path to the ssl .crt. This
                                                                should be enabled for testing
                                                                purposes only.
key_file                                                        Path to the ssl .key. This
                                                                should be enabled for testing
                                                                purposes only.
cors_allow_origin                                               List of origin hosts that are allowed
                                                                for CORS requests in addition to what
                                                                the container has set.
strict_cors_mode                      True                      If True (default) then CORS
                                                                requests are only allowed if their
                                                                Origin header matches an allowed
                                                                origin. Otherwise, any Origin is
                                                                allowed.
cors_expose_headers                                             This is a list of headers that
                                                                are included in the header
                                                                Access-Control-Expose-Headers
                                                                in addition to what the container
                                                                has set.
client_timeout                        60
trans_id_suffix                                                 This optional suffix (default is empty)
                                                                that would be appended to the swift
                                                                transaction id allows one to easily
                                                                figure out from which cluster that
                                                                X-Trans-Id belongs to. This is very
                                                                useful when one is managing more than
                                                                one swift cluster.
log_name                              swift                     Label used when logging
log_facility                          LOG_LOCAL0                Syslog log facility
log_level                             INFO                      Logging level
log_headers                           False
log_address                           /dev/log                  Logging directory
log_max_line_length                   0                         Caps the length of log
                                                                lines to the value given;
                                                                no limit if set to 0, the
                                                                default.
log_custom_handlers                   None                      Comma separated list of functions
                                                                to call to setup custom log
                                                                handlers.
log_udp_host                                                    Override log_address
log_udp_port                          514                       UDP log port
log_statsd_host                       None                      Enables StatsD logging; IPv4/IPv6
                                                                address or a hostname.  If a
                                                                hostname resolves to an IPv4 and IPv6
                                                                address, the IPv4 address will be
                                                                used.
log_statsd_port                       8125
log_statsd_default_sample_rate        1.0
log_statsd_sample_rate_factor         1.0
log_statsd_metric_prefix
eventlet_debug                        false                     If true, turn on debug logging
                                                                for eventlet

expose_info                           true                      Enables exposing configuration
                                                                settings via HTTP GET /info.
admin_key                                                       Key to use for admin calls that
                                                                are HMAC signed.  Default
                                                                is empty, which will
                                                                disable admin calls to
                                                                /info.
disallowed_sections                   swift.valid_api_versions  Allows the ability to withhold
                                                                sections from showing up in the
                                                                public calls to /info. You can
                                                                withhold subsections by separating
                                                                the dict level with a ".".
nice_priority                         None                      Scheduling priority of server
                                                                processes.
                                                                Niceness values range from -20 (most
                                                                favorable to the process) to 19 (least
                                                                favorable to the process). The default
                                                                does not modify priority.
ionice_class                          None                      I/O scheduling class of server
                                                                processes. I/O niceness class values
                                                                are IOPRIO_CLASS_RT (realtime),
                                                                IOPRIO_CLASS_BE (best-effort) and
                                                                IOPRIO_CLASS_IDLE (idle).
                                                                The default does not
                                                                modify class and priority. Linux
                                                                supports io scheduling priorities
                                                                and classes since 2.6.13 with
                                                                the CFQ io scheduler.
                                                                Work only with ionice_priority.
ionice_priority                       None                      I/O scheduling priority of server
                                                                processes. I/O niceness priority is
                                                                a number which goes from 0 to 7.
                                                                The higher the value, the lower
                                                                the I/O priority of the process.
                                                                Work only with ionice_class.
                                                                Ignored if IOPRIO_CLASS_IDLE is set.
====================================  ========================  ========================================

**************
[proxy-server]
**************

==============================================  ===============  =====================================
Option                                          Default          Description
----------------------------------------------  ---------------  -------------------------------------
use                                                              Entry point for paste.deploy for
                                                                 the proxy server.  For most
                                                                 cases, this should be
                                                                 ``egg:swift#proxy``.
set log_name                                    proxy-server     Label used when logging
set log_facility                                LOG_LOCAL0       Syslog log facility
set log_level                                   INFO             Log level
set log_headers                                 True             If True, log headers in each
                                                                 request
set log_handoffs                                True             If True, the proxy will log
                                                                 whenever it has to failover to a
                                                                 handoff node
recheck_account_existence                       60               Cache timeout in seconds to
                                                                 send memcached for account
                                                                 existence
recheck_container_existence                     60               Cache timeout in seconds to
                                                                 send memcached for container
                                                                 existence
account_existence_skip_cache_pct                0.0              Periodically, bypass the cache
                                                                 for account info requests and
                                                                 goto disk to refresh the data
                                                                 in the cache. This is a percentage
                                                                 of requests should randomly skip.
                                                                 Values around 0.0 - 0.1 (1 in every
                                                                 1000) are recommended.
container_existence_skip_cache_pct              0.0              Periodically, bypass the cache
                                                                 for container info requests and
                                                                 goto disk to refresh the data
                                                                 in the cache. This is a percentage
                                                                 of requests should randomly skip.
                                                                 Values around 0.0 - 0.1 (1 in every
                                                                 1000) are recommended.
container_updating_shard_ranges_skip_cache_pct  0.0              Periodically, bypass the cache
                                                                 for shard_range update requests and
                                                                 goto disk to refresh the data
                                                                 in the cache. This is a percentage
                                                                 of requests should randomly skip.
                                                                 Values around 0.0 - 0.1 (1 in every
                                                                 1000) are recommended.
container_listing_shard_ranges_skip_cache_pct   0.0              Periodically, bypass the cache
                                                                 for shard_range listing info requests
                                                                 and goto disk to refresh the data
                                                                 in the cache. This is a percentage
                                                                 of requests should randomly skip.
                                                                 Values around 0.0 - 0.1 (1 in every
                                                                 1000) are recommended.
object_chunk_size                               65536            Chunk size to read from
                                                                 object servers
client_chunk_size                               65536            Chunk size to read from
                                                                 clients
memcache_servers                                127.0.0.1:11211  Comma separated list of
                                                                 memcached servers
                                                                 ip:port or [ipv6addr]:port,
                                                                 if this value is
                                                                 empty, the memcache client will look
                                                                 for a :ref:`[memcache.conf] <memcache-config>`
memcache_max_connections                        2                Max number of connections to
                                                                 each memcached server per
                                                                 worker
node_timeout                                    10               Request timeout to external
                                                                 services
recoverable_node_timeout                        node_timeout     Request timeout to external
                                                                 services for requests that, on
                                                                 failure, can be recovered
                                                                 from. For example, object GET.
client_timeout                                  60               Timeout to read one chunk
                                                                 from a client
conn_timeout                                    0.5              Connection timeout to
                                                                 external services
error_suppression_interval                      60               Time in seconds that must
                                                                 elapse since the last error
                                                                 for a node to be considered
                                                                 no longer error limited
error_suppression_limit                         10               Error count to consider a
                                                                 node error limited
allow_account_management                        false            Whether account PUTs and DELETEs
                                                                 are even callable
account_autocreate                              false            If set to 'true' authorized
                                                                 accounts that do not yet exist
                                                                 within the Swift cluster will
                                                                 be automatically created.
max_containers_per_account                      0                If set to a positive value,
                                                                 trying to create a container
                                                                 when the account already has at
                                                                 least this maximum containers
                                                                 will result in a 403 Forbidden.
                                                                 Note: This is a soft limit,
                                                                 meaning a user might exceed the
                                                                 cap for
                                                                 recheck_account_existence before
                                                                 the 403s kick in.
max_containers_whitelist                                         This is a comma separated list
                                                                 of account names that ignore
                                                                 the max_containers_per_account
                                                                 cap.
rate_limit_after_segment                        10               Rate limit the download of
                                                                 large object segments after
                                                                 this segment is downloaded.
rate_limit_segments_per_sec                     1                Rate limit large object
                                                                 downloads at this rate.
request_node_count                              2 * replicas     Set to the number of nodes to
                                                                 contact for a normal request.
                                                                 You can use '* replicas' at the
                                                                 end to have it use the number
                                                                 given times the number of
                                                                 replicas for the ring being used
                                                                 for the request.
swift_owner_headers                             <see the sample  These are the headers whose
                                                conf file for    values will only be shown to
                                                the list of      swift_owners. The exact
                                                default          definition of a swift_owner is
                                                headers>         up to the auth system in use,
                                                                 but usually indicates
                                                                 administrative responsibilities.
sorting_method                                  shuffle          Storage nodes can be chosen at
                                                                 random (shuffle), by using timing
                                                                 measurements (timing), or by using
                                                                 an explicit match (affinity).
                                                                 Using timing measurements may allow
                                                                 for lower overall latency, while
                                                                 using affinity allows for finer
                                                                 control. In both the timing and
                                                                 affinity cases, equally-sorting nodes
                                                                 are still randomly chosen to spread
                                                                 load. This option may be overridden
                                                                 in a per-policy configuration
                                                                 section.
timing_expiry                                   300              If the "timing" sorting_method is
                                                                 used, the timings will only be valid
                                                                 for the number of seconds configured
                                                                 by timing_expiry.
concurrent_gets                                 off              Use replica count number of
                                                                 threads concurrently during a
                                                                 GET/HEAD and return with the
                                                                 first successful response. In
                                                                 the EC case, this parameter only
                                                                 affects an EC HEAD as an EC GET
                                                                 behaves differently.
concurrency_timeout                             conn_timeout     This parameter controls how long
                                                                 to wait before firing off the
                                                                 next concurrent_get thread. A
                                                                 value of 0 would we fully concurrent,
                                                                 any other number will stagger the
                                                                 firing of the threads. This number
                                                                 should be between 0 and node_timeout.
                                                                 The default is conn_timeout (0.5).
nice_priority                                   None             Scheduling priority of server
                                                                 processes.
                                                                 Niceness values range from -20 (most
                                                                 favorable to the process) to 19 (least
                                                                 favorable to the process). The default
                                                                 does not modify priority.
ionice_class                                    None             I/O scheduling class of server
                                                                 processes. I/O niceness class values
                                                                 are IOPRIO_CLASS_RT (realtime),
                                                                 IOPRIO_CLASS_BE (best-effort),
                                                                 and IOPRIO_CLASS_IDLE (idle).
                                                                 The default does not modify class and
                                                                 priority. Linux supports io scheduling
                                                                 priorities and classes since 2.6.13
                                                                 with the CFQ io scheduler.
                                                                 Work only with ionice_priority.
ionice_priority                                 None             I/O scheduling priority of server
                                                                 processes. I/O niceness priority is
                                                                 a number which goes from 0 to 7.
                                                                 The higher the value, the lower the
                                                                 I/O priority of the process. Work
                                                                 only with ionice_class.
                                                                 Ignored if IOPRIO_CLASS_IDLE is set.
read_affinity                                   None             Specifies which backend servers to
                                                                 prefer on reads; used in conjunction
                                                                 with the sorting_method option being
                                                                 set to 'affinity'. Format is a comma
                                                                 separated list of affinity descriptors
                                                                 of the form <selection>=<priority>.
                                                                 The <selection> may be r<N> for
                                                                 selecting nodes in region N or
                                                                 r<N>z<M> for selecting nodes in
                                                                 region N, zone M. The <priority>
                                                                 value should be a whole number
                                                                 that represents the priority to
                                                                 be given to the selection; lower
                                                                 numbers are higher priority.
                                                                 Default is empty, meaning no
                                                                 preference. This option may be
                                                                 overridden in a per-policy
                                                                 configuration section.
write_affinity                                  None             Specifies which backend servers to
                                                                 prefer on writes. Format is a comma
                                                                 separated list of affinity
                                                                 descriptors of the form r<N> for
                                                                 region N or r<N>z<M> for region N,
                                                                 zone M. Default is empty, meaning no
                                                                 preference. This option may be
                                                                 overridden in a per-policy
                                                                 configuration section.
write_affinity_node_count                       2 * replicas     The number of local (as governed by
                                                                 the write_affinity setting) nodes to
                                                                 attempt to contact first on writes,
                                                                 before any non-local ones. The value
                                                                 should be an integer number, or use
                                                                 '* replicas' at the end to have it
                                                                 use the number given times the number
                                                                 of replicas for the ring being used
                                                                 for the request. This option may be
                                                                 overridden in a per-policy
                                                                 configuration section.
write_affinity_handoff_delete_count             auto             The number of local (as governed by
                                                                 the write_affinity setting) handoff
                                                                 nodes to attempt to contact on
                                                                 deletion, in addition to primary
                                                                 nodes. Example: in geographically
                                                                 distributed deployment, If replicas=3,
                                                                 sometimes there may be 1 primary node
                                                                 and 2 local handoff nodes in one region
                                                                 holding the object after uploading but
                                                                 before object replicated to the
                                                                 appropriate locations in other regions.
                                                                 In this case, include these handoff
                                                                 nodes to send request when deleting
                                                                 object could help make correct decision
                                                                 for the response. The default value 'auto'
                                                                 means Swift will calculate the number
                                                                 automatically, the default value is
                                                                 (replicas - len(local_primary_nodes)).
                                                                 This option may be overridden in a
                                                                 per-policy configuration section.
allow_open_expired                              false            If true (default is false), an object that
                                                                 has expired but not yet been reaped can be
                                                                 can be accessed by setting the
                                                                 'x-open-expired' header to true in
                                                                 GET, HEAD, and POST requests.
==============================================  ===============  =====================================
