# Copyright (c) 2010-2012 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Internal client library for making calls directly to the servers rather than
through the proxy.
"""

import json
import os
import socket

from eventlet import sleep, Timeout
import six
import six.moves.cPickle as pickle
from six.moves.http_client import HTTPException

from swift.common.bufferedhttp import http_connect
from swift.common.exceptions import ClientException
from swift.common.utils import Timestamp, FileLikeIter
from swift.common.http import HTTP_NO_CONTENT, HTTP_INSUFFICIENT_STORAGE, \
    is_success, is_server_error
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.utils import quote


class DirectClientException(ClientException):

    def __init__(self, stype, method, node, part, path, resp, host=None):
        # host can be used to override the node ip and port reported in
        # the exception
        host = host if host is not None else node
        if not isinstance(path, six.text_type):
            path = path.decode("utf-8")
        full_path = quote('/%s/%s%s' % (node['device'], part, path))
        msg = '%s server %s:%s direct %s %r gave status %s' % (
            stype, host['ip'], host['port'], method, full_path, resp.status)
        headers = HeaderKeyDict(resp.getheaders())
        super(DirectClientException, self).__init__(
            msg, http_host=host['ip'], http_port=host['port'],
            http_device=node['device'], http_status=resp.status,
            http_reason=resp.reason, http_headers=headers)


def _make_path(*components):
    return u'/' + u'/'.join(
        x.decode('utf-8') if isinstance(x, six.binary_type) else x
        for x in components)


def _make_req(node, part, method, path, headers, stype,
              conn_timeout=5, response_timeout=15, send_timeout=15,
              contents=None, content_length=None, chunk_size=65535):
    """
    Make request to backend storage node.
    (i.e. 'Account', 'Container', 'Object')
    :param node: a node dict from a ring
    :param part: an integer, the partition number
    :param method: a string, the HTTP method (e.g. 'PUT', 'DELETE', etc)
    :param path: a string, the request path
    :param headers: a dict, header name => value
    :param stype: a string, describing the type of service
    :param conn_timeout: timeout while waiting for connection; default is 5
        seconds
    :param response_timeout: timeout while waiting for response; default is 15
        seconds
    :param send_timeout: timeout for sending request body; default is 15
        seconds
    :param contents: an iterable or string to read object data from
    :param content_length: value to send as content-length header
    :param chunk_size: if defined, chunk size of data to send
    :returns: an HTTPResponse object
    :raises DirectClientException: if the response status is not 2xx
    :raises eventlet.Timeout: if either conn_timeout or response_timeout is
        exceeded
    """
    if contents is not None:
        if content_length is not None:
            headers['Content-Length'] = str(content_length)
        else:
            for n, v in headers.items():
                if n.lower() == 'content-length':
                    content_length = int(v)
        if not contents:
            headers['Content-Length'] = '0'
        if isinstance(contents, six.string_types):
            contents = [contents]
        if content_length is None:
            headers['Transfer-Encoding'] = 'chunked'

    with Timeout(conn_timeout):
        conn = http_connect(node['ip'], node['port'], node['device'], part,
                            method, path, headers=headers)

    if contents is not None:
        contents_f = FileLikeIter(contents)

        with Timeout(send_timeout):
            if content_length is None:
                chunk = contents_f.read(chunk_size)
                while chunk:
                    conn.send(b'%x\r\n%s\r\n' % (len(chunk), chunk))
                    chunk = contents_f.read(chunk_size)
                conn.send(b'0\r\n\r\n')
            else:
                left = content_length
                while left > 0:
                    size = chunk_size
                    if size > left:
                        size = left
                    chunk = contents_f.read(size)
                    if not chunk:
                        break
                    conn.send(chunk)
                    left -= len(chunk)

    with Timeout(response_timeout):
        resp = conn.getresponse()
        resp.read()
    if not is_success(resp.status):
        raise DirectClientException(stype, method, node, part, path, resp)
    return resp


def _get_direct_account_container(path, stype, node, part,
                                  marker=None, limit=None,
                                  prefix=None, delimiter=None,
                                  conn_timeout=5, response_timeout=15,
                                  end_marker=None, reverse=None, headers=None):
    """Base class for get direct account and container.

    Do not use directly use the get_direct_account or
    get_direct_container instead.
    """
    params = ['format=json']
    if marker:
        params.append('marker=%s' % quote(marker))
    if limit:
        params.append('limit=%d' % limit)
    if prefix:
        params.append('prefix=%s' % quote(prefix))
    if delimiter:
        params.append('delimiter=%s' % quote(delimiter))
    if end_marker:
        params.append('end_marker=%s' % quote(end_marker))
    if reverse:
        params.append('reverse=%s' % quote(reverse))
    qs = '&'.join(params)
    with Timeout(conn_timeout):
        conn = http_connect(node['ip'], node['port'], node['device'], part,
                            'GET', path, query_string=qs,
                            headers=gen_headers(hdrs_in=headers))
    with Timeout(response_timeout):
        resp = conn.getresponse()
    if not is_success(resp.status):
        resp.read()
        raise DirectClientException(stype, 'GET', node, part, path, resp)

    resp_headers = HeaderKeyDict()
    for header, value in resp.getheaders():
        resp_headers[header] = value
    if resp.status == HTTP_NO_CONTENT:
        resp.read()
        return resp_headers, []
    return resp_headers, json.loads(resp.read())


def gen_headers(hdrs_in=None, add_ts=True):
    """
    Get the headers ready for a request. All requests should have a User-Agent
    string, but if one is passed in don't over-write it. Not all requests will
    need an X-Timestamp, but if one is passed in do not over-write it.

    :param headers: dict or None, base for HTTP headers
    :param add_ts: boolean, should be True for any "unsafe" HTTP request

    :returns: HeaderKeyDict based on headers and ready for the request
    """
    hdrs_out = HeaderKeyDict(hdrs_in) if hdrs_in else HeaderKeyDict()
    if add_ts and 'X-Timestamp' not in hdrs_out:
        hdrs_out['X-Timestamp'] = Timestamp.now().internal
    if 'user-agent' not in hdrs_out:
        hdrs_out['User-Agent'] = 'direct-client %s' % os.getpid()
    return hdrs_out


def direct_get_account(node, part, account, marker=None, limit=None,
                       prefix=None, delimiter=None, conn_timeout=5,
                       response_timeout=15, end_marker=None, reverse=None):
    """
    Get listings directly from the account server.

    :param node: node dictionary from the ring
    :param part: partition the account is on
    :param account: account name
    :param marker: marker query
    :param limit: query limit
    :param prefix: prefix query
    :param delimiter: delimiter for the query
    :param conn_timeout: timeout in seconds for establishing the connection
    :param response_timeout: timeout in seconds for getting the response
    :param end_marker: end_marker query
    :param reverse: reverse the returned listing
    :returns: a tuple of (response headers, a list of containers) The response
              headers will HeaderKeyDict.
    """
    path = _make_path(account)
    return _get_direct_account_container(path, "Account", node, part,
                                         marker=marker,
                                         limit=limit, prefix=prefix,
                                         delimiter=delimiter,
                                         end_marker=end_marker,
                                         reverse=reverse,
                                         conn_timeout=conn_timeout,
                                         response_timeout=response_timeout)


def direct_delete_account(node, part, account, conn_timeout=5,
                          response_timeout=15, headers=None):
    if headers is None:
        headers = {}

    path = _make_path(account)
    _make_req(node, part, 'DELETE', path, gen_headers(headers, True),
              'Account', conn_timeout, response_timeout)


def direct_head_container(node, part, account, container, conn_timeout=5,
                          response_timeout=15):
    """
    Request container information directly from the container server.

    :param node: node dictionary from the ring
    :param part: partition the container is on
    :param account: account name
    :param container: container name
    :param conn_timeout: timeout in seconds for establishing the connection
    :param response_timeout: timeout in seconds for getting the response
    :returns: a dict containing the response's headers in a HeaderKeyDict
    :raises ClientException: HTTP HEAD request failed
    """
    path = _make_path(account, container)
    resp = _make_req(node, part, 'HEAD', path, gen_headers(),
                     'Container', conn_timeout, response_timeout)

    resp_headers = HeaderKeyDict()
    for header, value in resp.getheaders():
        resp_headers[header] = value
    return resp_headers


def direct_get_container(node, part, account, container, marker=None,
                         limit=None, prefix=None, delimiter=None,
                         conn_timeout=5, response_timeout=15, end_marker=None,
                         reverse=None, headers=None):
    """
    Get container listings directly from the container server.

    :param node: node dictionary from the ring
    :param part: partition the container is on
    :param account: account name
    :param container: container name
    :param marker: marker query
    :param limit: query limit
    :param prefix: prefix query
    :param delimiter: delimiter for the query
    :param conn_timeout: timeout in seconds for establishing the connection
    :param response_timeout: timeout in seconds for getting the response
    :param end_marker: end_marker query
    :param reverse: reverse the returned listing
    :param headers: headers to be included in the request
    :returns: a tuple of (response headers, a list of objects) The response
              headers will be a HeaderKeyDict.
    """
    path = _make_path(account, container)
    return _get_direct_account_container(path, "Container", node,
                                         part, marker=marker,
                                         limit=limit, prefix=prefix,
                                         delimiter=delimiter,
                                         end_marker=end_marker,
                                         reverse=reverse,
                                         conn_timeout=conn_timeout,
                                         response_timeout=response_timeout,
                                         headers=headers)


def direct_delete_container(node, part, account, container, conn_timeout=5,
                            response_timeout=15, headers=None):
    """
    Delete container directly from the container server.

    :param node: node dictionary from the ring
    :param part: partition the container is on
    :param account: account name
    :param container: container name
    :param conn_timeout: timeout in seconds for establishing the connection
    :param response_timeout: timeout in seconds for getting the response
    :param headers: dict to be passed into HTTPConnection headers
    :raises ClientException: HTTP DELETE request failed
    """
    if headers is None:
        headers = {}

    path = _make_path(account, container)
    add_timestamp = 'x-timestamp' not in (k.lower() for k in headers)
    _make_req(node, part, 'DELETE', path, gen_headers(headers, add_timestamp),
              'Container', conn_timeout, response_timeout)


def direct_put_container(node, part, account, container, conn_timeout=5,
                         response_timeout=15, headers=None, contents=None,
                         content_length=None, chunk_size=65535):
    """
    Make a PUT request to a container server.

    :param node: node dictionary from the ring
    :param part: partition the container is on
    :param account: account name
    :param container: container name
    :param conn_timeout: timeout in seconds for establishing the connection
    :param response_timeout: timeout in seconds for getting the response
    :param headers: additional headers to include in the request
    :param contents: an iterable or string to send in request body (optional)
    :param content_length: value to send as content-length header (optional)
    :param chunk_size: chunk size of data to send (optional)
    :raises ClientException: HTTP PUT request failed
    """
    if headers is None:
        headers = {}

    lower_headers = set(k.lower() for k in headers)
    headers_out = gen_headers(headers,
                              add_ts='x-timestamp' not in lower_headers)
    path = _make_path(account, container)
    _make_req(node, part, 'PUT', path, headers_out, 'Container', conn_timeout,
              response_timeout, contents=contents,
              content_length=content_length, chunk_size=chunk_size)


def direct_put_container_object(node, part, account, container, obj,
                                conn_timeout=5, response_timeout=15,
                                headers=None):
    if headers is None:
        headers = {}

    have_x_timestamp = 'x-timestamp' in (k.lower() for k in headers)

    path = _make_path(account, container, obj)
    _make_req(node, part, 'PUT', path,
              gen_headers(headers, add_ts=(not have_x_timestamp)),
              'Container', conn_timeout, response_timeout)


def direct_delete_container_object(node, part, account, container, obj,
                                   conn_timeout=5, response_timeout=15,
                                   headers=None):
    if headers is None:
        headers = {}

    headers = gen_headers(headers, add_ts='x-timestamp' not in (
        k.lower() for k in headers))

    path = _make_path(account, container, obj)
    _make_req(node, part, 'DELETE', path, headers,
              'Container', conn_timeout, response_timeout)


def direct_head_object(node, part, account, container, obj, conn_timeout=5,
                       response_timeout=15, headers=None):
    """
    Request object information directly from the object server.

    :param node: node dictionary from the ring
    :param part: partition the container is on
    :param account: account name
    :param container: container name
    :param obj: object name
    :param conn_timeout: timeout in seconds for establishing the connection
    :param response_timeout: timeout in seconds for getting the response
    :param headers: dict to be passed into HTTPConnection headers
    :returns: a dict containing the response's headers in a HeaderKeyDict
    :raises ClientException: HTTP HEAD request failed
    """
    if headers is None:
        headers = {}

    headers = gen_headers(headers)

    path = _make_path(account, container, obj)
    resp = _make_req(node, part, 'HEAD', path, headers,
                     'Object', conn_timeout, response_timeout)

    resp_headers = HeaderKeyDict()
    for header, value in resp.getheaders():
        resp_headers[header] = value
    return resp_headers


def direct_get_object(node, part, account, container, obj, conn_timeout=5,
                      response_timeout=15, resp_chunk_size=None, headers=None):
    """
    Get object directly from the object server.

    :param node: node dictionary from the ring
    :param part: partition the container is on
    :param account: account name
    :param container: container name
    :param obj: object name
    :param conn_timeout: timeout in seconds for establishing the connection
    :param response_timeout: timeout in seconds for getting the response
    :param resp_chunk_size: if defined, chunk size of data to read.
    :param headers: dict to be passed into HTTPConnection headers
    :returns: a tuple of (response headers, the object's contents) The response
              headers will be a HeaderKeyDict.
    :raises ClientException: HTTP GET request failed
    """
    if headers is None:
        headers = {}

    path = _make_path(account, container, obj)
    with Timeout(conn_timeout):
        conn = http_connect(node['ip'], node['port'], node['device'], part,
                            'GET', path, headers=gen_headers(headers))
    with Timeout(response_timeout):
        resp = conn.getresponse()
    if not is_success(resp.status):
        resp.read()
        raise DirectClientException('Object', 'GET', node, part, path, resp)

    if resp_chunk_size:

        def _object_body():
            buf = resp.read(resp_chunk_size)
            while buf:
                yield buf
                buf = resp.read(resp_chunk_size)
        object_body = _object_body()
    else:
        object_body = resp.read()
    resp_headers = HeaderKeyDict()
    for header, value in resp.getheaders():
        resp_headers[header] = value
    return resp_headers, object_body


def direct_put_object(node, part, account, container, name, contents,
                      content_length=None, etag=None, content_type=None,
                      headers=None, conn_timeout=5, response_timeout=15,
                      chunk_size=65535):
    """
    Put object directly from the object server.

    :param node: node dictionary from the ring
    :param part: partition the container is on
    :param account: account name
    :param container: container name
    :param name: object name
    :param contents: an iterable or string to read object data from
    :param content_length: value to send as content-length header
    :param etag: etag of contents
    :param content_type: value to send as content-type header
    :param headers: additional headers to include in the request
    :param conn_timeout: timeout in seconds for establishing the connection
    :param response_timeout: timeout in seconds for getting the response
    :param chunk_size: if defined, chunk size of data to send.
    :returns: etag from the server response
    :raises ClientException: HTTP PUT request failed
    """

    path = _make_path(account, container, name)
    if headers is None:
        headers = {}
    if etag:
        headers['ETag'] = etag.strip('"')
    if content_type is not None:
        headers['Content-Type'] = content_type
    else:
        headers['Content-Type'] = 'application/octet-stream'
    # Incase the caller want to insert an object with specific age
    add_ts = 'X-Timestamp' not in headers

    resp = _make_req(
        node, part, 'PUT', path, gen_headers(headers, add_ts=add_ts),
        'Object', conn_timeout, response_timeout, contents=contents,
        content_length=content_length, chunk_size=chunk_size)

    return resp.getheader('etag').strip('"')


def direct_post_object(node, part, account, container, name, headers,
                       conn_timeout=5, response_timeout=15):
    """
    Direct update to object metadata on object server.

    :param node: node dictionary from the ring
    :param part: partition the container is on
    :param account: account name
    :param container: container name
    :param name: object name
    :param headers: headers to store as metadata
    :param conn_timeout: timeout in seconds for establishing the connection
    :param response_timeout: timeout in seconds for getting the response
    :raises ClientException: HTTP POST request failed
    """
    path = _make_path(account, container, name)
    _make_req(node, part, 'POST', path, gen_headers(headers, True),
              'Object', conn_timeout, response_timeout)


def direct_delete_object(node, part, account, container, obj,
                         conn_timeout=5, response_timeout=15, headers=None):
    """
    Delete object directly from the object server.

    :param node: node dictionary from the ring
    :param part: partition the container is on
    :param account: account name
    :param container: container name
    :param obj: object name
    :param conn_timeout: timeout in seconds for establishing the connection
    :param response_timeout: timeout in seconds for getting the response
    :raises ClientException: HTTP DELETE request failed
    """
    if headers is None:
        headers = {}

    headers = gen_headers(headers, add_ts='x-timestamp' not in (
        k.lower() for k in headers))

    path = _make_path(account, container, obj)
    _make_req(node, part, 'DELETE', path, headers,
              'Object', conn_timeout, response_timeout)


def direct_get_suffix_hashes(node, part, suffixes, conn_timeout=5,
                             response_timeout=15, headers=None):
    """
    Get suffix hashes directly from the object server.

    :param node: node dictionary from the ring
    :param part: partition the container is on
    :param conn_timeout: timeout in seconds for establishing the connection
    :param response_timeout: timeout in seconds for getting the response
    :param headers: dict to be passed into HTTPConnection headers
    :returns: dict of suffix hashes
    :raises ClientException: HTTP REPLICATE request failed
    """
    if headers is None:
        headers = {}

    path = '/%s' % '-'.join(suffixes)
    with Timeout(conn_timeout):
        conn = http_connect(node['replication_ip'], node['replication_port'],
                            node['device'], part, 'REPLICATE', path,
                            headers=gen_headers(headers))
    with Timeout(response_timeout):
        resp = conn.getresponse()
    if not is_success(resp.status):
        raise DirectClientException('Object', 'REPLICATE',
                                    node, part, path, resp,
                                    host={'ip': node['replication_ip'],
                                          'port': node['replication_port']}
                                    )
    return pickle.loads(resp.read())


def retry(func, *args, **kwargs):
    """
    Helper function to retry a given function a number of times.

    :param func: callable to be called
    :param retries: number of retries
    :param error_log: logger for errors
    :param args: arguments to send to func
    :param kwargs: keyward arguments to send to func (if retries or
                   error_log are sent, they will be deleted from kwargs
                   before sending on to func)
    :returns: result of func
    :raises ClientException: all retries failed
    """
    retries = kwargs.pop('retries', 5)
    error_log = kwargs.pop('error_log', None)
    attempts = 0
    backoff = 1
    while attempts <= retries:
        attempts += 1
        try:
            return attempts, func(*args, **kwargs)
        except (socket.error, HTTPException, Timeout) as err:
            if error_log:
                error_log(err)
            if attempts > retries:
                raise
        except ClientException as err:
            if error_log:
                error_log(err)
            if attempts > retries or not is_server_error(err.http_status) or \
                    err.http_status == HTTP_INSUFFICIENT_STORAGE:
                raise
        sleep(backoff)
        backoff *= 2
    # Shouldn't actually get down here, but just in case.
    if args and 'ip' in args[0]:
        raise ClientException('Raise too many retries',
                              http_host=args[0]['ip'],
                              http_port=args[0]['port'],
                              http_device=args[0]['device'])
    else:
        raise ClientException('Raise too many retries')
