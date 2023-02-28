.. _memcache-config:

-----------------------------
Global Memcache Configuration
-----------------------------

This document describes the configuration options available for the global swift memcache configuration
which usually lives under /etc/swift/memcache.conf.
Documentation for other swift configuration options can be found at
:doc:`index`.

An example memcache.conf configuration can be found at
etc/memcache.conf-sample in the source code repository.

There is only 1 configuration section available:

* :ref:`[memcache] <memcache_conf_memcache_section>`


.. _memcache_conf_memcache_section:

**********
[memcache]
**********

===========================  ===============    =============================================
Option                       Default            Description
---------------------------  ---------------    ---------------------------------------------
memcache_servers             127.0.0.1:11211    Comma separated list of memcached servers
                                                ip:port or [ipv6addr]:port
memcache_max_connections     2                  Max number of connections to each memcached
                                                server per worker
connect_timeout              0.3                Timeout for connection
pool_timeout                 1.0                Timeout for pooled connection
tries                        3                  Number of servers to retry on failures
                                                getting a pooled connection
io_timeout                   2.0                Timeout for read and writes
error_suppression_interval   60.0               How long without an error before a server's
                                                error count is reset. This will also be how
                                                long before a server is reenabled after
                                                suppression is triggered.
                                                Set to 0 to disable error-limiting.
error_suppression_limit      10                 How many errors can accumulate before a
                                                server is temporarily ignored
item_size_warning_threshold  -1                 If an item size ever gets above
                                                item_size_warning_threshold then a warning
                                                will be logged. This can be used to alert
                                                when memcache item sizes are getting to
                                                their limit. 
                                                It's an absolute size in bytes. Setting the
                                                value to 0 will warn on every memcache set.
                                                A value of -1 disables the warning
tls_enabled                  False              (Optional) Global toggle for TLS usage
                                                when comunicating with the caching servers
tls_cafile                                      (Optional) Path to a file of concatenated
                                                CA certificates in PEM format necessary to
                                                establish the caching server's authenticity.
                                                If tls_enabled is False, this option is
                                                ignored.
tls_certfile                                    (Optional) Path to a single file in PEM
                                                format containing the client's certificate
                                                as well as any number of CA certificates
                                                needed to establish the certificate's
                                                authenticity. This file is only required
                                                when client side authentication is
                                                necessary. If tls_enabled is False,
                                                this option is ignored
tls_keyfile                                     (Optional) Path to a single file containing
                                                the client's private key in. Otherwhise the
                                                private key will be taken from the file
                                                specified in tls_certfile. If tls_enabled
                                                is False, this option is ignored
===========================  ===============    =============================================