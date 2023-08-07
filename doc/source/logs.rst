====
Logs
====

Swift has quite verbose logging, and the generated logs can be used for
cluster monitoring, utilization calculations, audit records, and more. As an
overview, Swift's logs are sent to syslog and organized by log level and
syslog facility. All log lines related to the same request have the same
transaction id. This page documents the log formats used in the system.

.. note::

    By default, Swift will log full log lines. However, with the
    ``log_max_line_length`` setting and depending on your logging server
    software, lines may be truncated or shortened. With ``log_max_line_length <
    7``, the log line will be truncated. With ``log_max_line_length >= 7``, the
    log line will be "shortened": about half the max length followed by " ... "
    followed by the other half the max length. Unless you use exceptionally
    short values, you are unlikely to run across this with the following
    documented log lines, but you may see it with debugging and error log
    lines.

----------
Proxy Logs
----------

The proxy logs contain the record of all external API requests made to the
proxy server. Swift's proxy servers log requests using a custom format
designed to provide robust information and simple processing. It is possible
to change this format with  the ``log_msg_template`` config parameter.
The default log format is::

    {client_ip} {remote_addr} {end_time.datetime} {method} {path} {protocol}
        {status_int} {referer} {user_agent} {auth_token} {bytes_recvd}
        {bytes_sent} {client_etag} {transaction_id} {headers} {request_time}
        {source} {log_info} {start_time} {end_time} {policy_index}

Some keywords, signaled by the (anonymizable) flag, can be anonymized by
using the transformer 'anonymized'. The data are applied the hashing method of
``log_anonymization_method`` and an optional salt ``log_anonymization_salt``.

Some keywords, signaled by the (timestamp) flag, can be converted to standard
dates formats using the matching transformers: 'datetime', 'asctime' or
'iso8601'. Other transformers for timestamps are 's', 'ms', 'us' and 'ns' for
seconds, milliseconds, microseconds and nanoseconds. Python's strftime
directives can also be used as tranformers (a, A, b, B, c, d, H, I, j, m, M, p,
S, U, w, W, x, X, y, Y, Z).

Example::

    {client_ip.anonymized} {remote_addr.anonymized} {start_time.iso8601}
        {end_time.H}:{end_time.M} {method} acc:{account} cnt:{container}
        obj:{object.anonymized}

=================== ==========================================================
**Log Field**       **Value**
------------------- ----------------------------------------------------------
client_ip           Swift's guess at the end-client IP, taken from various
                    headers in the request. (anonymizable)
remote_addr         The IP address of the other end of the TCP connection.
                    (anonymizable)
end_time            Timestamp of the request. (timestamp)
method              The HTTP verb in the request.
domain              The domain in the request. (anonymizable)
path                The path portion of the request. (anonymizable)
protocol            The transport protocol used (currently one of http or
                    https).
status_int          The response code for the request.
referer             The value of the HTTP Referer header. (anonymizable)
user_agent          The value of the HTTP User-Agent header. (anonymizable)
auth_token          The value of the auth token. This may be truncated or
                    otherwise obscured.
bytes_recvd         The number of bytes read from the client for this request.
bytes_sent          The number of bytes sent to the client in the body of the
                    response. This is how many bytes were yielded to the WSGI
                    server.
client_etag         The etag header value given by the client. (anonymizable)
transaction_id      The transaction id of the request.
headers             The headers given in the request. (anonymizable)
request_time        The duration of the request.
source              The "source" of the request. This may be set for requests
                    that are generated in order to fulfill client requests,
                    e.g. bulk uploads.
log_info            Various info that may be useful for diagnostics, e.g. the
                    value of any x-delete-at header.
start_time          High-resolution timestamp from the start of the request.
                    (timestamp)
end_time            High-resolution timestamp from the end of the request.
                    (timestamp)
ttfb                Duration between the request and the first bytes is sent.
policy_index        The value of the storage policy index.
account             The account part extracted from the path of the request.
                    (anonymizable)
container           The container part extracted from the path of the request.
                    (anonymizable)
object              The object part extracted from the path of the request.
                    (anonymizable)
pid                 PID of the process emitting the log line.
wire_status_int     The status sent to the client, which may be different than
                    the logged response code if there was an error during the
                    body of the request or a disconnect.
=================== ==========================================================

In one log line, all of the above fields are space-separated and url-encoded.
If any value is empty, it will be logged as a "-". This allows for simple
parsing by splitting each line on whitespace. New values may be placed at the
end of the log line from time to time, but the order of the existing values
will not change. Swift log processing utilities should look for the first N
fields they require (e.g. in Python using something like
``log_line.split()[:14]`` to get up through the transaction id).

.. note::

    Some log fields (like the request path) are already url quoted, so the
    logged value will be double-quoted. For example, if a client uploads an
    object name with a ``:`` in it, it will be url-quoted as ``%3A``. The log
    module will then quote this value as ``%253A``.

Swift Source
============

The ``source`` value in the proxy logs is used to identify the originator of a
request in the system. For example, if the client initiates a bulk upload, the
proxy server may end up doing many requests. The initial bulk upload request
will be logged as normal, but all of the internal "child requests" will have a
source value indicating they came from the bulk functionality.

======================= =============================
**Logged Source Value** **Originator of the Request**
----------------------- -----------------------------
FP                      :ref:`formpost`
SLO                     :ref:`static-large-objects`
SW                      :ref:`staticweb`
TU                      :ref:`tempurl`
BD                      :ref:`bulk` (delete)
EA                      :ref:`bulk` (extract)
AQ                      :ref:`account-quotas`
CQ                      :ref:`container-quotas`
CS                      :ref:`container-sync`
TA                      :ref:`common_tempauth`
DLO                     :ref:`dynamic-large-objects`
LE                      :ref:`list_endpoints`
KS                      :ref:`keystoneauth`
RL                      :ref:`ratelimit`
RO                      :ref:`read_only`
VW                      :ref:`versioned_writes`
SSC                     :ref:`copy`
SYM                     :ref:`symlink`
SH                      :ref:`sharding_doc`
S3                      :ref:`s3api`
OV                      :ref:`object_versioning`
EQ                      :ref:`etag_quoter`
======================= =============================


-----------------
Storage Node Logs
-----------------

Swift's account, container, and object server processes each log requests
that they receive, if they have been configured to do so with the
``log_requests`` config parameter (which defaults to true). The format for
these log lines is::

    remote_addr - - [datetime] "request_method request_path" status_int
        content_length "referer" "transaction_id" "user_agent" request_time
        additional_info server_pid policy_index

=================== ==========================================================
**Log Field**       **Value**
------------------- ----------------------------------------------------------
remote_addr         The IP address of the other end of the TCP connection.
datetime            Timestamp of the request, in
                    "day/month/year:hour:minute:second +0000" format.
request_method      The HTTP verb in the request.
request_path        The path portion of the request.
status_int          The response code for the request.
content_length      The value of the Content-Length header in the response.
referer             The value of the HTTP Referer header.
transaction_id      The transaction id of the request.
user_agent          The value of the HTTP User-Agent header. Swift services
                    report a user-agent string of the service name followed by
                    the process ID, such as ``"proxy-server <pid of the
                    proxy>"`` or ``"object-updater <pid of the object
                    updater>"``.
request_time        The time between request received and response started.
                    **Note**: This includes transfer time on PUT, but not GET.
additional_info     Additional useful information.
server_pid          The process id of the server
policy_index        The value of the storage policy index.
=================== ==========================================================
