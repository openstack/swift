# Copyright (c) 2010-2012 OpenStack, LLC.
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

import socket
from httplib import HTTPException
from time import time
from urllib import quote as _quote

from eventlet import sleep, Timeout

from swift.common.bufferedhttp import http_connect
from swiftclient import ClientException, json_loads
from swift.common.utils import normalize_timestamp
from swift.common.http import HTTP_NO_CONTENT, HTTP_INSUFFICIENT_STORAGE, \
    is_success, is_server_error


def quote(value, safe='/'):
    if isinstance(value, unicode):
        value = value.encode('utf8')
    return _quote(value, safe)


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
    :param delimeter: delimeter for the query
    :param conn_timeout: timeout in seconds for establishing the connection
    :param response_timeout: timeout in seconds for getting the response
    :returns: a tuple of (response headers, a list of containers) The response
              headers will be a dict and all header names will be lowercase.
    """
    path = '/' + account
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
                            'GET', path, query_string=qs)
    with Timeout(response_timeout):
        resp = conn.getresponse()
    if not is_success(resp.status):
        resp.read()
        raise ClientException(
            'Account server %s:%s direct GET %s gave status %s' %
            (node['ip'], node['port'],
             repr('/%s/%s%s' % (node['device'], part, path)),
             resp.status),
            http_host=node['ip'], http_port=node['port'],
            http_device=node['device'], http_status=resp.status,
            http_reason=resp.reason)
    resp_headers = {}
    for header, value in resp.getheaders():
        resp_headers[header.lower()] = value
    if resp.status == HTTP_NO_CONTENT:
        resp.read()
        return resp_headers, []
    return resp_headers, json_loads(resp.read())


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
    :returns: a dict containing the response's headers (all header names will
              be lowercase)
    """
    path = '/%s/%s' % (account, container)
    with Timeout(conn_timeout):
        conn = http_connect(node['ip'], node['port'], node['device'], part,
                            'HEAD', path)
    with Timeout(response_timeout):
        resp = conn.getresponse()
        resp.read()
    if not is_success(resp.status):
        raise ClientException(
            'Container server %s:%s direct HEAD %s gave status %s' %
            (node['ip'], node['port'],
             repr('/%s/%s%s' % (node['device'], part, path)),
             resp.status),
            http_host=node['ip'], http_port=node['port'],
            http_device=node['device'], http_status=resp.status,
            http_reason=resp.reason)
    resp_headers = {}
    for header, value in resp.getheaders():
        resp_headers[header.lower()] = value
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
    :param delimeter: delimeter for the query
    :param conn_timeout: timeout in seconds for establishing the connection
    :param response_timeout: timeout in seconds for getting the response
    :returns: a tuple of (response headers, a list of objects) The response
              headers will be a dict and all header names will be lowercase.
    """
    path = '/%s/%s' % (account, container)
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
                            'GET', path, query_string=qs)
    with Timeout(response_timeout):
        resp = conn.getresponse()
    if not is_success(resp.status):
        resp.read()
        raise ClientException(
            'Container server %s:%s direct GET %s gave stats %s' %
            (node['ip'], node['port'],
             repr('/%s/%s%s' % (node['device'], part, path)),
             resp.status),
            http_host=node['ip'], http_port=node['port'],
            http_device=node['device'], http_status=resp.status,
            http_reason=resp.reason)
    resp_headers = {}
    for header, value in resp.getheaders():
        resp_headers[header.lower()] = value
    if resp.status == HTTP_NO_CONTENT:
        resp.read()
        return resp_headers, []
    return resp_headers, json_loads(resp.read())


def direct_delete_container(node, part, account, container, conn_timeout=5,
                            response_timeout=15, headers={}):
    path = '/%s/%s' % (account, container)
    headers['X-Timestamp'] = normalize_timestamp(time())
    with Timeout(conn_timeout):
        conn = http_connect(node['ip'], node['port'], node['device'], part,
                            'DELETE', path, headers)
    with Timeout(response_timeout):
        resp = conn.getresponse()
        resp.read()
    if not is_success(resp.status):
        raise ClientException(
            'Container server %s:%s direct DELETE %s gave status %s' %
            (node['ip'], node['port'],
             repr('/%s/%s%s' % (node['device'], part, path)), resp.status),
            http_host=node['ip'], http_port=node['port'],
            http_device=node['device'], http_status=resp.status,
            http_reason=resp.reason)


def direct_head_object(node, part, account, container, obj, conn_timeout=5,
                       response_timeout=15):
    """
    Request object information directly from the object server.

    :param node: node dictionary from the ring
    :param part: partition the container is on
    :param account: account name
    :param container: container name
    :param obj: object name
    :param conn_timeout: timeout in seconds for establishing the connection
    :param response_timeout: timeout in seconds for getting the response
    :returns: a dict containing the response's headers (all header names will
              be lowercase)
    """
    path = '/%s/%s/%s' % (account, container, obj)
    with Timeout(conn_timeout):
        conn = http_connect(node['ip'], node['port'], node['device'], part,
                            'HEAD', path)
    with Timeout(response_timeout):
        resp = conn.getresponse()
        resp.read()
    if not is_success(resp.status):
        raise ClientException(
            'Object server %s:%s direct HEAD %s gave status %s' %
            (node['ip'], node['port'],
             repr('/%s/%s%s' % (node['device'], part, path)),
             resp.status),
            http_host=node['ip'], http_port=node['port'],
            http_device=node['device'], http_status=resp.status,
            http_reason=resp.reason)
    resp_headers = {}
    for header, value in resp.getheaders():
        resp_headers[header.lower()] = value
    return resp_headers


def direct_get_object(node, part, account, container, obj, conn_timeout=5,
                      response_timeout=15, resp_chunk_size=None, headers={}):
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
              headers will be a dict and all header names will be lowercase.
    """
    path = '/%s/%s/%s' % (account, container, obj)
    with Timeout(conn_timeout):
        conn = http_connect(node['ip'], node['port'], node['device'], part,
                            'GET', path, headers=headers)
    with Timeout(response_timeout):
        resp = conn.getresponse()
    if not is_success(resp.status):
        resp.read()
        raise ClientException(
            'Object server %s:%s direct GET %s gave status %s' %
            (node['ip'], node['port'],
             repr('/%s/%s%s' % (node['device'], part, path)), resp.status),
            http_host=node['ip'], http_port=node['port'],
            http_device=node['device'], http_status=resp.status,
            http_reason=resp.reason)
    if resp_chunk_size:

        def _object_body():
            buf = resp.read(resp_chunk_size)
            while buf:
                yield buf
                buf = resp.read(resp_chunk_size)
        object_body = _object_body()
    else:
        object_body = resp.read()
    resp_headers = {}
    for header, value in resp.getheaders():
        resp_headers[header.lower()] = value
    return resp_headers, object_body


def direct_put_object(node, part, account, container, name, contents,
                      content_length=None, etag=None, content_type=None,
                      headers=None, conn_timeout=5, response_timeout=15,
                      resp_chunk_size=None):
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
    # TODO: Add chunked puts
    path = '/%s/%s/%s' % (account, container, name)
    if headers is None:
        headers = {}
    if etag:
        headers['ETag'] = etag.strip('"')
    if content_length is not None:
        headers['Content-Length'] = str(content_length)
    if content_type is not None:
        headers['Content-Type'] = content_type
    else:
        headers['Content-Type'] = 'application/octet-stream'
    if not contents:
        headers['Content-Length'] = '0'
    if isinstance(contents, basestring):
        contents = [contents]
    headers['X-Timestamp'] = normalize_timestamp(time())
    with Timeout(conn_timeout):
        conn = http_connect(node['ip'], node['port'], node['device'], part,
                            'PUT', path, headers=headers)
    for chunk in contents:
        conn.send(chunk)
    with Timeout(response_timeout):
        resp = conn.getresponse()
        resp.read()
    if not is_success(resp.status):
        raise ClientException(
            'Object server %s:%s direct PUT %s gave status %s' %
            (node['ip'], node['port'],
             repr('/%s/%s%s' % (node['device'], part, path)),
             resp.status),
            http_host=node['ip'], http_port=node['port'],
            http_device=node['device'], http_status=resp.status,
            http_reason=resp.reason)
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
    headers['X-Timestamp'] = normalize_timestamp(time())
    with Timeout(conn_timeout):
        conn = http_connect(node['ip'], node['port'], node['device'], part,
                            'POST', path, headers=headers)
    with Timeout(response_timeout):
        resp = conn.getresponse()
        resp.read()
    if not is_success(resp.status):
        raise ClientException(
            'Object server %s:%s direct POST %s gave status %s' %
            (node['ip'], node['port'],
             repr('/%s/%s%s' % (node['device'], part, path)),
             resp.status),
            http_host=node['ip'], http_port=node['port'],
            http_device=node['device'], http_status=resp.status,
            http_reason=resp.reason)


def direct_delete_object(node, part, account, container, obj,
                         conn_timeout=5, response_timeout=15, headers={}):
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
    path = '/%s/%s/%s' % (account, container, obj)
    headers['X-Timestamp'] = normalize_timestamp(time())
    with Timeout(conn_timeout):
        conn = http_connect(node['ip'], node['port'], node['device'], part,
                            'DELETE', path, headers)
    with Timeout(response_timeout):
        resp = conn.getresponse()
        resp.read()
    if not is_success(resp.status):
        raise ClientException(
            'Object server %s:%s direct DELETE %s gave status %s' %
            (node['ip'], node['port'],
             repr('/%s/%s%s' % (node['device'], part, path)),
             resp.status),
            http_host=node['ip'], http_port=node['port'],
            http_device=node['device'], http_status=resp.status,
            http_reason=resp.reason)


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
        except (socket.error, HTTPException, Timeout), err:
            if error_log:
                error_log(err)
            if attempts > retries:
                raise
        except ClientException, err:
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
