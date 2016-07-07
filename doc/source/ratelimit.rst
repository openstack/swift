.. _ratelimit:

=============
Rate Limiting
=============

Rate limiting in Swift is implemented as a pluggable middleware.  Rate
limiting is performed on requests that result in database writes to the
account and container sqlite dbs.  It uses memcached and is dependent on
the proxy servers having highly synchronized time.  The rate limits are
limited by the accuracy of the proxy server clocks.

--------------
Configuration
--------------

All configuration is optional.  If no account or container limits are provided
there will be no rate limiting.  Configuration available:

================================ ======= ======================================
Option                           Default Description
-------------------------------- ------- --------------------------------------
clock_accuracy                   1000    Represents how accurate the proxy
                                         servers' system clocks are with each
                                         other. 1000 means that all the
                                         proxies' clock are accurate to each
                                         other within 1 millisecond. No
                                         ratelimit should be higher than the
                                         clock accuracy.
max_sleep_time_seconds           60      App will immediately return a 498
                                         response if the necessary sleep time
                                         ever exceeds the given
                                         max_sleep_time_seconds.
log_sleep_time_seconds           0       To allow visibility into rate limiting
                                         set this value > 0 and all sleeps
                                         greater than the number will be
                                         logged.
rate_buffer_seconds              5       Number of seconds the rate counter can
                                         drop and be allowed to catch up (at a
                                         faster than listed rate). A larger
                                         number will result in larger spikes in
                                         rate but better average accuracy.
account_ratelimit                0       If set, will limit PUT and DELETE
                                         requests to
                                         /account_name/container_name. Number
                                         is in requests per second.
container_ratelimit_size         ''      When set with container_ratelimit_x =
                                         r: for containers of size x, limit
                                         requests per second to r. Will limit
                                         PUT, DELETE, and POST requests to
                                         /a/c/o.
container_listing_ratelimit_size ''      When set with
                                         container_listing_ratelimit_x = r: for
                                         containers of size x, limit listing
                                         requests per second to r. Will limit
                                         GET requests to /a/c.
================================ ======= ======================================

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


-----------------------------
Account Specific Ratelimiting
-----------------------------


The above ratelimiting is to prevent the "many writes to a single container"
bottleneck from causing a problem. There could also be a problem where a single
account is just using too much of the cluster's resources.  In this case, the
container ratelimits may not help because the customer could be doing thousands
of reqs/sec to distributed containers each getting a small fraction of the
total so those limits would never trigger. If a system administrator notices
this, he/she can set the X-Account-Sysmeta-Global-Write-Ratelimit on an account
and that will limit the total number of write requests (PUT, POST, DELETE,
COPY) that account can do for the whole account. This limit will be in addition
to the applicable account/container limits from above. This header will be
hidden from the user, because of the gatekeeper middleware, and can only be set
using a direct client to the account nodes. It accepts a float value and will
only limit requests if the value is > 0.

-------------------
Black/White-listing
-------------------

To blacklist or whitelist an account set:

X-Account-Sysmeta-Global-Write-Ratelimit: BLACKLIST

or

X-Account-Sysmeta-Global-Write-Ratelimit: WHITELIST

in the account headers.
