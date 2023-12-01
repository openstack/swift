.. _swift-common-config:

--------------------
Common configuration
--------------------

This document describes the configuration options common to all swift servers.
Documentation for other swift configuration options can be found at
:doc:`index`.

An example of common configuration file can be found at etc/swift.conf-sample

The following configuration options are available:

==========================  ==========  =============================================
Option                      Default     Description
--------------------------  ----------  ---------------------------------------------
max_header_size             8192        max_header_size is the max number of bytes in
                                        the utf8 encoding of each header. Using 8192
                                        as default because eventlet use 8192 as max
                                        size of header line. This value may need to
                                        be increased when using identity v3 API
                                        tokens including more than 7 catalog entries.
                                        See also include_service_catalog in
                                        proxy-server.conf-sample (documented in
                                        overview_auth.rst).
extra_header_count          0           By default the maximum number of allowed
                                        headers depends on the number of max
                                        allowed metadata settings plus a default
                                        value of 32 for regular http  headers.
                                        If for some reason this is not enough (custom
                                        middleware for example) it can be increased
                                        with the extra_header_count constraint.
==========================  ==========  =============================================

