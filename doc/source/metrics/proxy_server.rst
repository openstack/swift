``proxy-server`` Metrics
========================

In the table, ``<type>`` is the proxy-server controller responsible for the
request and will be one of ``account``, ``container``, or ``object``.

==========================================  ====================================================
Metric Name                                 Description
------------------------------------------  ----------------------------------------------------
``proxy-server.errors``                     Count of errors encountered while serving requests
                                            before the controller type is determined.  Includes
                                            invalid Content-Length, errors finding the internal
                                            controller to handle the request, invalid utf8, and
                                            bad URLs.
``proxy-server.<type>.handoff_count``       Count of node hand-offs; only tracked if log_handoffs
                                            is set in the proxy-server config.
``proxy-server.<type>.handoff_all_count``   Count of times *only* hand-off locations were
                                            utilized; only tracked if log_handoffs is set in the
                                            proxy-server config.
``proxy-server.<type>.client_timeouts``     Count of client timeouts (client did not read within
                                            ``client_timeout`` seconds during a GET or did not
                                            supply data within ``client_timeout`` seconds during
                                            a PUT).
``proxy-server.<type>.client_disconnects``  Count of detected client disconnects during PUT
                                            operations (does NOT include caught Exceptions in
                                            the proxy-server which caused a client disconnect).
==========================================  ====================================================

Additionally, middleware often emit their own metrics

``proxy-logging`` Middleware
----------------------------

In the table, ``<type>`` is either the proxy-server controller responsible
for the request: ``account``, ``container``, ``object``, or the string
``SOS`` if the request came from the `Swift Origin Server`_ middleware.
The ``<verb>`` portion will be one of ``GET``, ``HEAD``, ``POST``, ``PUT``,
``DELETE``, ``COPY``, ``OPTIONS``, or ``BAD_METHOD``.  The list of valid
HTTP methods is configurable via the ``log_statsd_valid_http_methods``
config variable and the default setting yields the above behavior.

.. _Swift Origin Server: https://github.com/dpgoetz/sos

======================================================  ============================================
Metric Name                                             Description
------------------------------------------------------  --------------------------------------------
``proxy-server.<type>.<verb>.<status>.timing``          Timing data for requests, start to finish.
                                                        The <status> portion is the numeric HTTP
                                                        status code for the request (e.g.  "200" or
                                                        "404").
``proxy-server.<type>.GET.<status>.first-byte.timing``  Timing data up to completion of sending the
                                                        response headers (only for GET requests).
                                                        <status> and <type> are as for the main
                                                        timing metric.
``proxy-server.<type>.<verb>.<status>.xfer``            This counter metric is the sum of bytes
                                                        transferred in (from clients) and out (to
                                                        clients) for requests.  The <type>, <verb>,
                                                        and <status> portions of the metric are just
                                                        like the main timing metric.
======================================================  ============================================

The ``proxy-logging`` middleware also groups these metrics by policy.  The
``<policy-index>`` portion represents a policy index:

============================================================================  =====================================
Metric Name                                                                   Description
----------------------------------------------------------------------------  -------------------------------------
``proxy-server.object.policy.<policy-index>.<verb>.<status>.timing``          Timing data for requests, aggregated
                                                                              by policy index.
``proxy-server.object.policy.<policy-index>.GET.<status>.first-byte.timing``  Timing data up to completion of
                                                                              sending the response headers,
                                                                              aggregated by policy index.
``proxy-server.object.policy.<policy-index>.<verb>.<status>.xfer``            Sum of bytes transferred in and out,
                                                                              aggregated by policy index.
============================================================================  =====================================

``tempauth`` Middleware
-----------------------
In the table, ``<reseller_prefix>`` represents the actual configured
reseller_prefix or ``NONE`` if the reseller_prefix is the empty string:

===========================================  ====================================================
Metric Name                                  Description
-------------------------------------------  ----------------------------------------------------
``tempauth.<reseller_prefix>.unauthorized``  Count of regular requests which were denied with
                                             HTTPUnauthorized.
``tempauth.<reseller_prefix>.forbidden``     Count of regular requests which were denied with
                                             HTTPForbidden.
``tempauth.<reseller_prefix>.token_denied``  Count of token requests which were denied.
``tempauth.<reseller_prefix>.errors``        Count of errors.
===========================================  ====================================================

``tempurl`` Middleware
----------------------

==========================================  ====================================================
Metric Name                                 Description
------------------------------------------  ----------------------------------------------------
``proxy-server.tempurl.digests.<digest>``   Count of requests authorized using the specified
                                            ``<digest>``; may be one of ``sha1``, ``sha256``,
                                            or ``sha512``.
==========================================  ====================================================

``formpost`` Middleware
-----------------------

==========================================  ====================================================
Metric Name                                 Description
------------------------------------------  ----------------------------------------------------
``proxy-server.formpost.digests.<digest>``  Count of requests authorized using the specified
                                            ``<digest>``; may be one of ``sha1``, ``sha256``,
                                            or ``sha512``.
==========================================  ====================================================
