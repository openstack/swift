=============
Rate Limiting
=============

Rate limiting in swift is implemented as a pluggable middleware.  Rate
limiting is performed on requests that result in database writes to the
account and container sqlite dbs.  It uses memcached and is dependent on
the proxy servers having highly synchronized time.  The rate limits are
limited by the accuracy of the proxy server clocks.

--------------
Configuration
--------------

All configuration is optional.  If no account or container limits are provided
there will be no rate limiting.  Configuration available:

======================== =========  ===========================================
Option                   Default     Description
------------------------ ---------  -------------------------------------------
clock_accuracy           1000       Represents how accurate the proxy servers'
                                    system clocks are with each other. 1000
                                    means that all the proxies' clock are
                                    accurate to each other within 1
                                    millisecond.  No ratelimit should be
                                    higher than the clock accuracy.
max_sleep_time_seconds   60         App will immediately return a 498 response
                                    if the necessary sleep time ever exceeds
                                    the given max_sleep_time_seconds.
log_sleep_time_seconds   0          To allow visibility into rate limiting set
                                    this value > 0 and all sleeps greater than
                                    the number will be logged.
rate_buffer_seconds      5          Number of seconds the rate counter can
                                    drop and be allowed to catch up (at a
                                    faster than listed rate). A larger number
                                    will result in larger spikes in rate but
                                    better average accuracy.
account_ratelimit        0          If set, will limit PUT and DELETE requests
                                    to /account_name/container_name.
                                    Number is in requests per second.
account_whitelist        ''         Comma separated lists of account names that
                                    will not be rate limited.
account_blacklist        ''         Comma separated lists of account names that
                                    will not be allowed. Returns a 497 response.
container_ratelimit_size ''         When set with container_limit_x = r:
                                    for containers of size x, limit requests
                                    per second to r.  Will limit PUT, DELETE,
                                    and POST requests to /a/c/o.
======================== =========  ===========================================

The container rate limits are linearly interpolated from the values given.  A
sample container rate limiting could be:

container_ratelimit_100 = 100

container_ratelimit_200 = 50

container_ratelimit_500 = 20

This would result in

================    ============
Container Size      Rate Limit
----------------    ------------
0-99                No limiting
100                 100
150                 75
500                 20
1000                20
================    ============


