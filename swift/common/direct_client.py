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

import os
import socket
from httplib import HTTPException
from time import time

from eventlet import sleep, Timeout

from swift.common.bufferedhttp import http_connect
from swift.common.exceptions import ClientException
from swift.common.utils import Timestamp, FileLikeIter
from swift.common.http import HTTP_NO_CONTENT, HTTP_INSUFFICIENT_STORAGE, \
    is_success, is_server_error
from swift.common.swob import HeaderKeyDict
from swift.common.utils import quote

try:
    import simplejson as json
except ImportError:
    import json


class DirectClientException(ClientException):

    def __init__(self, stype, method, node, part, path, resp):
        full_path = quote('/%s/%s%s' % (node['device'], part, path))
        msg = '%s server %s:%s direct %s %r gave status %s' % (
            stype, node['ip'], node['port'], method, full_path, resp.status)
        headers = HeaderKeyDict(resp.getheaders())
        super(DirectClientException, self).__init__(
            msg, http_host=node['ip'], http_port=node['port'],
            http_device=node['device'], http_status=resp.status,
            http_reason=resp.reason, http_headers=headers)


def _get_direct_account_container(path, stype, node, part,
                                  account, marker=None, limit=None,
                                  prefix=None, delimiter=None, conn_timeout=5,
                                  response_timeout=15):
    """Base class for get direct account and container.

    Do not use directly use the get_direct_account or
    get_direct_container instead.
    """
    qs = 'format=json'
    if marker:
        qs += '&marker=%s' % quote(marker)
    if limit:
        qs += '&limit=%d' % limit
    if prefix:
        qs += '&prefix=%s' % quote(prefix)
    if delimiter:
        qs += '&delimiter=%s' % quote(delimiter)
    with Timeout(conn_timeout):
        conn = http_connect(node['ip'], node['port'], node['device'], part,
                            'GET', path, query_string=qs,
                            headers=gen_headers())
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


def gen_headers(hdrs_in=None, add_ts=False):
    hdrs_out = HeaderKeyDict(hdrs_in) if hdrs_in else HeaderKeyDict()
    if add_ts:
        hdrs_out['X-Timestamp'] = Timestamp(time()).internal
    hdrs_out['User-Agent'] = 'direct-client %s' % os.getpid()
    return hdrs_out


def direct_get_account(node, part, account, marker=None, limit=None,
                       prefix=None, delimiter=None, conn_timeout=5,
                       response_timeout=15):
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
    :returns: a tuple of (response headers, a list of containers) The response
              headers will HeaderKeyDict.
    """
    path = '/' + account
    return _get_direct_account_container(path, "Account", node, part,
                                         account, marker=marker,
                                         limit=limit, prefix=prefix,
                                         delimiter=delimiter,
                                         conn_timeout=conn_timeout,
                                         response_timeout=response_timeout)


def direct_delete_account(node, part, account, conn_timeout=5,
                          response_timeout=15, headers=None):
    if headers is None:
        headers = {}

    path = '/%s' % account
    with Timeout(conn_timeout):
        conn = http_connect(node['ip'], node['port'], node['device'], part,
                            'DELETE', path,
                            headers=gen_headers(headers, True))
    with Timeout(response_timeout):
        resp = conn.getresponse()
        resp.read()
    if not is_success(resp.status):
        raise DirectClientException('Account', 'DELETE',
                                    node, part, path, resp)


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
    """
    path = '/%s/%s' % (account, container)
    with Timeout(conn_timeout):
        conn = http_connect(node['ip'], node['port'], node['device'], part,
                            'HEAD', path, headers=gen_headers())
    with Timeout(response_timeout):
        resp = conn.getresponse()
        resp.read()
    if not is_success(resp.status):
        raise DirectClientException('Container', 'HEAD',
                                    node, part, path, resp)
    resp_headers = HeaderKeyDict()
    for header, value in resp.getheaders():
        resp_headers[header] = value
    return resp_headers


def direct_get_container(node, part, account, container, marker=None,
                         limit=None, prefix=None, delimiter=None,
                         conn_timeout=5, response_timeout=15):
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
    :returns: a tuple of (response headers, a list of objects) The response
              headers will be a HeaderKeyDict.
    """
    path = '/%s/%s' % (account, container)
    return _get_direct_account_container(path, "Container", node,
                                         part, account, marker=marker,
                                         limit=limit, prefix=prefix,
                                         delimiter=delimiter,
                                         conn_timeout=conn_timeout,
                                         response_timeout=response_timeout)


def direct_delete_container(node, part, account, container, conn_timeout=5,
                            response_timeout=15, headers=None):
    if headers is None:
        headers = {}

    path = '/%s/%s' % (account, container)
    with Timeout(conn_timeout):
        conn = http_connect(node['ip'], node['port'], node['device'], part,
                            'DELETE', path,
                            headers=gen_headers(headers, True))
    with Timeout(response_timeout):
        resp = conn.getresponse()
        resp.read()
    if not is_success(resp.status):
        raise DirectClientException('Container', 'DELETE',
                                    node, part, path, resp)


def direct_put_container_object(node, part, account, container, obj,
                                conn_timeout=5, response_timeout=15,
                                headers=None):
    if headers is None:
        headers = {}

    have_x_timestamp = 'x-timestamp' in (k.lower() for k in headers)

    path = '/%s/%s/%s' % (account, container, obj)
    with Timeout(conn_timeout):
        conn = http_connect(node['ip'], node['port'], node['device'], part,
                            'PUT', path,
                            headers=gen_headers(headers,
                                                add_ts=(not have_x_timestamp)))
    with Timeout(response_timeout):
        resp = conn.getresponse()
        resp.read()
    if not is_success(resp.status):
        raise DirectClientException('Container', 'PUT',
                                    node, part, path, resp)


def direct_delete_container_object(node, part, account, container, obj,
                                   conn_timeout=5, response_timeout=15,
                                   headers=None):
    if headers is None:
        headers = {}

    headers = gen_headers(headers, add_ts='x-timestamp' not in (
        k.lower() for k in headers))

    path = '/%s/%s/%s' % (account, container, obj)
    with Timeout(conn_timeout):
        conn = http_connect(node['ip'], node['port'], node['device'], part,
                            'DELETE', path, headers=headers)

    with Timeout(response_timeout):
        resp = conn.getresponse()
        resp.read()
    if not is_success(resp.status):
        raise DirectClientException('Container', 'DELETE',
                                    node, part, path, resp)


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
    """
    if headers is None:
        headers = {}

    headers = gen_headers(headers)

    path = '/%s/%s/%s' % (account, container, obj)
    with Timeout(conn_timeout):
        conn = http_connect(node['ip'], node['port'], node['device'], part,
                            'HEAD', path, headers=headers)
    with Timeout(response_timeout):
        resp = conn.getresponse()
        resp.read()
    if not is_success(resp.status):
        raise DirectClientException('Object', 'HEAD',
                                    node, part, path, resp)
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
    """
    if headers is None:
        headers = {}

    path = '/%s/%s/%s' % (account, container, obj)
    with Timeout(conn_timeout):
        conn = http_connect(node['ip'], node['port'], node['device'], part,
                            'GET', path, headers=gen_headers(headers))
    with Timeout(response_timeout):
        resp = conn.getresponse()
    if not is_success(resp.status):
        resp.read()
        raise DirectClientException('Object', 'GET',
                                    node, part, path, resp)
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
    """

    path = '/%s/%s/%s' % (account, container, name)
    if headers is None:
        headers = {}
    if etag:
        headers['ETag'] = etag.strip('"')
    if content_length is not None:
        headers['Content-Length'] = str(content_length)
    else:
        for n, v in headers.iteritems():
            if n.lower() == 'content-length':
                content_length = int(v)
    if content_type is not None:
        headers['Content-Type'] = content_type
    else:
        headers['Content-Type'] = 'application/octet-stream'
    if not contents:
        headers['Content-Length'] = '0'
    if isinstance(contents, basestring):
        contents = [contents]
    #Incase the caller want to insert an object with specific age
    add_ts = 'X-Timestamp' not in headers

    if content_length is None:
        headers['Transfer-Encoding'] = 'chunked'

    with Timeout(conn_timeout):
        conn = http_connect(node['ip'], node['port'], node['device'], part,
                            'PUT', path, headers=gen_headers(headers, add_ts))

    contents_f = FileLikeIter(contents)

    if content_length is None:
        chunk = contents_f.read(chunk_size)
        while chunk:
            conn.send('%x\r\n%s\r\n' % (len(chunk), chunk))
            chunk = contents_f.read(chunk_size)
        conn.send('0\r\n\r\n')
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
        raise DirectClientException('Object', 'PUT',
                                    node, part, path, resp)
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
    path = '/%s/%s/%s' % (account, container, name)
    with Timeout(conn_timeout):
        conn = http_connect(node['ip'], node['port'], node['device'], part,
                            'POST', path, headers=gen_headers(headers, True))
    with Timeout(response_timeout):
        resp = conn.getresponse()
        resp.read()
    if not is_success(resp.status):
        raise DirectClientException('Object', 'POST',
                                    node, part, path, resp)


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
    :returns: response from server
    """
    if headers is None:
        headers = {}

    headers = gen_headers(headers, add_ts='x-timestamp' not in (
        k.lower() for k in headers))

    path = '/%s/%s/%s' % (account, container, obj)
    with Timeout(conn_timeout):
        conn = http_connect(node['ip'], node['port'], node['device'], part,
                            'DELETE', path, headers=headers)
    with Timeout(response_timeout):
        resp = conn.getresponse()
        resp.read()
    if not is_success(resp.status):
        raise DirectClientException('Object', 'DELETE',
                                    node, part, path, resp)


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
    :returns: restult of func
    """
    retries = 5
    if 'retries' in kwargs:
        retries = kwargs['retries']
        del kwargs['retries']
    error_log = None
    if 'error_log' in kwargs:
        error_log = kwargs['error_log']
        del kwargs['error_log']
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
                              http_host=args[
                              0]['ip'], http_port=args[0]['port'],
                              http_device=args[0]['device'])
    else:
        raise ClientException('Raise too many retries')
