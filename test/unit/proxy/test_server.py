# -*- coding: utf-8 -*-
# Copyright (c) 2010-2016 OpenStack Foundation
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

from __future__ import print_function
import email.parser
import logging
import json
import math
import os
import posix
import socket
import sys
import traceback
import unittest
from contextlib import contextmanager
from shutil import rmtree, copyfile, move
import gc
import time
from textwrap import dedent
from hashlib import md5
import collections
from pyeclib.ec_iface import ECDriverError
from tempfile import mkdtemp, NamedTemporaryFile
import weakref
import operator
import functools
from swift.obj import diskfile
import re
import random
from collections import defaultdict
import uuid

import mock
from eventlet import sleep, spawn, wsgi, Timeout, debug
from eventlet.green import httplib
from six import BytesIO
from six import StringIO
from six.moves import range
from six.moves.urllib.parse import quote, parse_qsl

from test import listen_zero
from test.unit import (
    connect_tcp, readuntil2crlfs, FakeLogger, fake_http_connect, FakeRing,
    FakeMemcache, debug_logger, patch_policies, write_fake_ring,
    mocked_http_conn, DEFAULT_TEST_EC_TYPE, make_timestamp_iter,
    skip_if_no_xattrs)
from test.unit.helpers import setup_servers, teardown_servers
from swift.proxy import server as proxy_server
from swift.proxy.controllers.obj import ReplicatedObjectController
from swift.obj import server as object_server
from swift.common.bufferedhttp import BufferedHTTPResponse
from swift.common.middleware import proxy_logging, versioned_writes, \
    copy, listing_formats
from swift.common.middleware.acl import parse_acl, format_acl
from swift.common.exceptions import ChunkReadTimeout, DiskFileNotExist, \
    APIVersionError, ChunkWriteTimeout, ChunkReadError
from swift.common import utils, constraints
from swift.common.utils import hash_path, storage_directory, \
    parse_content_type, parse_mime_headers, \
    iter_multipart_mime_documents, public, mkdirs, NullLogger
from swift.common.wsgi import loadapp, ConfigString
from swift.proxy.controllers import base as proxy_base
from swift.proxy.controllers.base import get_cache_key, cors_validation, \
    get_account_info, get_container_info
import swift.proxy.controllers
import swift.proxy.controllers.obj
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.swob import Request, Response, HTTPUnauthorized, \
    HTTPException, HTTPBadRequest
from swift.common.storage_policy import StoragePolicy, POLICIES
import swift.common.request_helpers
from swift.common.request_helpers import get_sys_meta_prefix

# mocks
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


STATIC_TIME = time.time()
_test_context = _test_servers = _test_sockets = _testdir = \
    _test_POLICIES = None


def do_setup(object_server):
    # setup test context and break out some globals for convenience
    global _test_context, _testdir, _test_servers, _test_sockets, \
        _test_POLICIES
    _test_context = setup_servers(object_server)
    _testdir = _test_context["testdir"]
    _test_servers = _test_context["test_servers"]
    _test_sockets = _test_context["test_sockets"]
    _test_POLICIES = _test_context["test_POLICIES"]


def unpatch_policies(f):
    """
    This will unset a TestCase level patch_policies to use the module level
    policies setup for the _test_servers instead.

    N.B. You should NEVER modify the _test_server policies or rings during a
    test because they persist for the life of the entire module!
    """
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        with patch_policies(_test_POLICIES):
            return f(*args, **kwargs)
    return wrapper


def setUpModule():
    do_setup(object_server)


def tearDownModule():
    teardown_servers(_test_context)


def sortHeaderNames(headerNames):
    """
    Return the given string of header names sorted.

    headerName: a comma-delimited list of header names
    """
    headers = [a.strip() for a in headerNames.split(',') if a.strip()]
    headers.sort()
    return ', '.join(headers)


def parse_headers_string(headers_str):
    headers_dict = HeaderKeyDict()
    for line in headers_str.split('\r\n'):
        if ': ' in line:
            header, value = line.split(': ', 1)
            headers_dict[header] = value
    return headers_dict


def node_error_count(proxy_app, ring_node):
    # Reach into the proxy's internals to get the error count for a
    # particular node
    node_key = proxy_app._error_limit_node_key(ring_node)
    return proxy_app._error_limiting.get(node_key, {}).get('errors', 0)


def node_last_error(proxy_app, ring_node):
    # Reach into the proxy's internals to get the last error for a
    # particular node
    node_key = proxy_app._error_limit_node_key(ring_node)
    return proxy_app._error_limiting.get(node_key, {}).get('last_error')


def set_node_errors(proxy_app, ring_node, value, last_error):
    # Set the node's error count to value
    node_key = proxy_app._error_limit_node_key(ring_node)
    stats = proxy_app._error_limiting.setdefault(node_key, {})
    stats['errors'] = value
    stats['last_error'] = last_error


class FakeMemcacheReturnsNone(FakeMemcache):

    def get(self, key):
        # Returns None as the timestamp of the container; assumes we're only
        # using the FakeMemcache for container existence checks.
        return None


@contextmanager
def save_globals():
    orig_http_connect = getattr(swift.proxy.controllers.base, 'http_connect',
                                None)
    orig_account_info = getattr(swift.proxy.controllers.Controller,
                                'account_info', None)
    orig_container_info = getattr(swift.proxy.controllers.Controller,
                                  'container_info', None)

    try:
        yield True
    finally:
        swift.proxy.controllers.Controller.account_info = orig_account_info
        swift.proxy.controllers.base.http_connect = orig_http_connect
        swift.proxy.controllers.obj.http_connect = orig_http_connect
        swift.proxy.controllers.account.http_connect = orig_http_connect
        swift.proxy.controllers.container.http_connect = orig_http_connect
        swift.proxy.controllers.Controller.container_info = orig_container_info


def set_http_connect(*args, **kwargs):
    new_connect = fake_http_connect(*args, **kwargs)
    swift.proxy.controllers.base.http_connect = new_connect
    swift.proxy.controllers.obj.http_connect = new_connect
    swift.proxy.controllers.account.http_connect = new_connect
    swift.proxy.controllers.container.http_connect = new_connect
    return new_connect


def _make_callback_func(calls):
    def callback(ipaddr, port, device, partition, method, path,
                 headers=None, query_string=None, ssl=False):
        context = {}
        context['method'] = method
        context['path'] = path
        context['headers'] = headers or {}
        calls.append(context)
    return callback


def _limit_max_file_size(f):
    """
    This will limit constraints.MAX_FILE_SIZE for the duration of the
    wrapped function, based on whether MAX_FILE_SIZE exceeds the
    sys.maxsize limit on the system running the tests.

    This allows successful testing on 32 bit systems.
    """
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        test_max_file_size = constraints.MAX_FILE_SIZE
        if constraints.MAX_FILE_SIZE >= sys.maxsize:
            test_max_file_size = (2 ** 30 + 2)
        with mock.patch.object(constraints, 'MAX_FILE_SIZE',
                               test_max_file_size):
            return f(*args, **kwargs)
    return wrapper


# tests
class TestController(unittest.TestCase):

    def setUp(self):
        skip_if_no_xattrs()
        self.account_ring = FakeRing()
        self.container_ring = FakeRing()
        self.memcache = FakeMemcache()
        app = proxy_server.Application(None, self.memcache,
                                       account_ring=self.account_ring,
                                       container_ring=self.container_ring)
        self.controller = swift.proxy.controllers.Controller(app)

        class FakeReq(object):
            def __init__(self):
                self.url = "/foo/bar"
                self.method = "METHOD"

            def as_referer(self):
                return self.method + ' ' + self.url

        self.account = 'some_account'
        self.container = 'some_container'
        self.request = FakeReq()
        self.read_acl = 'read_acl'
        self.write_acl = 'write_acl'

    def test_transfer_headers(self):
        src_headers = {'x-remove-base-meta-owner': 'x',
                       'x-base-meta-size': '151M',
                       'new-owner': 'Kun'}
        dst_headers = {'x-base-meta-owner': 'Gareth',
                       'x-base-meta-size': '150M'}
        self.controller.transfer_headers(src_headers, dst_headers)
        expected_headers = {'x-base-meta-owner': '',
                            'x-base-meta-size': '151M'}
        self.assertEqual(dst_headers, expected_headers)

    def check_account_info_return(self, partition, nodes, is_none=False):
        if is_none:
            p, n = None, None
        else:
            p, n = self.account_ring.get_nodes(self.account)
        self.assertEqual(p, partition)
        self.assertEqual(n, nodes)

    def test_account_info_container_count(self):
        with save_globals():
            set_http_connect(200, count=123)
            partition, nodes, count = \
                self.controller.account_info(self.account)
            self.assertEqual(count, 123)
        with save_globals():
            set_http_connect(200, count='123')
            partition, nodes, count = \
                self.controller.account_info(self.account)
            self.assertEqual(count, 123)
        with save_globals():
            cache_key = get_cache_key(self.account)
            account_info = {'status': 200, 'container_count': 1234}
            self.memcache.set(cache_key, account_info)
            partition, nodes, count = \
                self.controller.account_info(self.account)
            self.assertEqual(count, 1234)
        with save_globals():
            cache_key = get_cache_key(self.account)
            account_info = {'status': 200, 'container_count': '1234'}
            self.memcache.set(cache_key, account_info)
            partition, nodes, count = \
                self.controller.account_info(self.account)
            self.assertEqual(count, 1234)

    def test_make_requests(self):
        with save_globals():
            set_http_connect(200)
            partition, nodes, count = \
                self.controller.account_info(self.account, self.request)
            set_http_connect(201, raise_timeout_exc=True)
            self.controller._make_request(
                nodes, partition, 'POST', '/', '', '',
                self.controller.app.logger.thread_locals)

    # tests if 200 is cached and used
    def test_account_info_200(self):
        with save_globals():
            set_http_connect(200)
            partition, nodes, count = \
                self.controller.account_info(self.account, self.request)
            self.check_account_info_return(partition, nodes)
            self.assertEqual(count, 12345)

            # Test the internal representation in memcache
            # 'container_count' changed from int to str
            cache_key = get_cache_key(self.account)
            container_info = {'status': 200,
                              'account_really_exists': True,
                              'container_count': '12345',
                              'total_object_count': None,
                              'bytes': None,
                              'storage_policies': {p.idx: {
                                  'container_count': 0,
                                  'object_count': 0,
                                  'bytes': 0} for p in POLICIES},
                              'meta': {},
                              'sysmeta': {}}
            self.assertEqual(container_info,
                             self.memcache.get(cache_key))

            set_http_connect()
            partition, nodes, count = \
                self.controller.account_info(self.account, self.request)
            self.check_account_info_return(partition, nodes)
            self.assertEqual(count, 12345)

    # tests if 404 is cached and used
    def test_account_info_404(self):
        with save_globals():
            set_http_connect(404, 404, 404)
            partition, nodes, count = \
                self.controller.account_info(self.account, self.request)
            self.check_account_info_return(partition, nodes, True)
            self.assertIsNone(count)

            # Test the internal representation in memcache
            # 'container_count' changed from 0 to None
            cache_key = get_cache_key(self.account)
            account_info = {'status': 404,
                            'container_count': None,  # internally keep None
                            'total_object_count': None,
                            'bytes': None,
                            'storage_policies': {p.idx: {
                                'container_count': 0,
                                'object_count': 0,
                                'bytes': 0} for p in POLICIES},
                            'meta': {},
                            'sysmeta': {}}
            self.assertEqual(account_info,
                             self.memcache.get(cache_key))

            set_http_connect()
            partition, nodes, count = \
                self.controller.account_info(self.account, self.request)
            self.check_account_info_return(partition, nodes, True)
            self.assertIsNone(count)

    # tests if some http status codes are not cached
    def test_account_info_no_cache(self):
        def test(*status_list):
            set_http_connect(*status_list)
            partition, nodes, count = \
                self.controller.account_info(self.account, self.request)
            self.assertEqual(len(self.memcache.keys()), 0)
            self.check_account_info_return(partition, nodes, True)
            self.assertIsNone(count)

        with save_globals():
            # We cache if we have two 404 responses - fail if only one
            test(503, 503, 404)
            test(504, 404, 503)
            test(404, 507, 503)
            test(503, 503, 503)

    def test_account_info_no_account(self):
        with save_globals():
            self.memcache.store = {}
            set_http_connect(404, 404, 404)
            partition, nodes, count = \
                self.controller.account_info(self.account, self.request)
            self.check_account_info_return(partition, nodes, is_none=True)
            self.assertIsNone(count)

    def check_container_info_return(self, ret, is_none=False):
        if is_none:
            partition, nodes, read_acl, write_acl = None, None, None, None
        else:
            partition, nodes = self.container_ring.get_nodes(self.account,
                                                             self.container)
            read_acl, write_acl = self.read_acl, self.write_acl
        self.assertEqual(partition, ret['partition'])
        self.assertEqual(nodes, ret['nodes'])
        self.assertEqual(read_acl, ret['read_acl'])
        self.assertEqual(write_acl, ret['write_acl'])

    def test_container_info_invalid_account(self):
        def account_info(self, account, request, autocreate=False):
            return None, None

        with save_globals():
            swift.proxy.controllers.Controller.account_info = account_info
            ret = self.controller.container_info(self.account,
                                                 self.container,
                                                 self.request)
            self.check_container_info_return(ret, True)

    # tests if 200 is cached and used
    def test_container_info_200(self):

        with save_globals():
            headers = {'x-container-read': self.read_acl,
                       'x-container-write': self.write_acl}
            set_http_connect(200,  # account_info is found
                             200, headers=headers)  # container_info is found
            ret = self.controller.container_info(
                self.account, self.container, self.request)
            self.check_container_info_return(ret)

            cache_key = get_cache_key(self.account, self.container)
            cache_value = self.memcache.get(cache_key)
            self.assertIsInstance(cache_value, dict)
            self.assertEqual(200, cache_value.get('status'))

            set_http_connect()
            ret = self.controller.container_info(
                self.account, self.container, self.request)
            self.check_container_info_return(ret)

    # tests if 404 is cached and used
    def test_container_info_404(self):
        def account_info(self, account, request):
            return True, True, 0

        with save_globals():
            set_http_connect(503, 204,  # account_info found
                             504, 404, 404)  # container_info 'NotFound'
            ret = self.controller.container_info(
                self.account, self.container, self.request)
            self.check_container_info_return(ret, True)

            cache_key = get_cache_key(self.account, self.container)
            cache_value = self.memcache.get(cache_key)
            self.assertIsInstance(cache_value, dict)
            self.assertEqual(404, cache_value.get('status'))

            set_http_connect()
            ret = self.controller.container_info(
                self.account, self.container, self.request)
            self.check_container_info_return(ret, True)

            set_http_connect(503, 404, 404)  # account_info 'NotFound'
            ret = self.controller.container_info(
                self.account, self.container, self.request)
            self.check_container_info_return(ret, True)

            cache_key = get_cache_key(self.account, self.container)
            cache_value = self.memcache.get(cache_key)
            self.assertIsInstance(cache_value, dict)
            self.assertEqual(404, cache_value.get('status'))

            set_http_connect()
            ret = self.controller.container_info(
                self.account, self.container, self.request)
            self.check_container_info_return(ret, True)

    # tests if some http status codes are not cached
    def test_container_info_no_cache(self):
        def test(*status_list):
            set_http_connect(*status_list)
            ret = self.controller.container_info(
                self.account, self.container, self.request)
            self.assertEqual(len(self.memcache.keys()), 0)
            self.check_container_info_return(ret, True)

        with save_globals():
            # We cache if we have two 404 responses - fail if only one
            test(503, 503, 404)
            test(504, 404, 503)
            test(404, 507, 503)
            test(503, 503, 503)

    def test_get_account_info_returns_values_as_strings(self):
        app = mock.MagicMock()
        app.memcache = mock.MagicMock()
        app.memcache.get = mock.MagicMock()
        app.memcache.get.return_value = {
            u'foo': u'\u2603',
            u'meta': {u'bar': u'\u2603'},
            u'sysmeta': {u'baz': u'\u2603'}}
        env = {'PATH_INFO': '/v1/a'}
        ai = get_account_info(env, app)

        # Test info is returned as strings
        self.assertEqual(ai.get('foo'), '\xe2\x98\x83')
        self.assertIsInstance(ai.get('foo'), str)

        # Test info['meta'] is returned as strings
        m = ai.get('meta', {})
        self.assertEqual(m.get('bar'), '\xe2\x98\x83')
        self.assertIsInstance(m.get('bar'), str)

        # Test info['sysmeta'] is returned as strings
        m = ai.get('sysmeta', {})
        self.assertEqual(m.get('baz'), '\xe2\x98\x83')
        self.assertIsInstance(m.get('baz'), str)

    def test_get_container_info_returns_values_as_strings(self):
        app = mock.MagicMock()
        app.memcache = mock.MagicMock()
        app.memcache.get = mock.MagicMock()
        app.memcache.get.return_value = {
            u'foo': u'\u2603',
            u'meta': {u'bar': u'\u2603'},
            u'sysmeta': {u'baz': u'\u2603'},
            u'cors': {u'expose_headers': u'\u2603'}}
        env = {'PATH_INFO': '/v1/a/c'}
        ci = get_container_info(env, app)

        # Test info is returned as strings
        self.assertEqual(ci.get('foo'), '\xe2\x98\x83')
        self.assertIsInstance(ci.get('foo'), str)

        # Test info['meta'] is returned as strings
        m = ci.get('meta', {})
        self.assertEqual(m.get('bar'), '\xe2\x98\x83')
        self.assertIsInstance(m.get('bar'), str)

        # Test info['sysmeta'] is returned as strings
        m = ci.get('sysmeta', {})
        self.assertEqual(m.get('baz'), '\xe2\x98\x83')
        self.assertIsInstance(m.get('baz'), str)

        # Test info['cors'] is returned as strings
        m = ci.get('cors', {})
        self.assertEqual(m.get('expose_headers'), '\xe2\x98\x83')
        self.assertIsInstance(m.get('expose_headers'), str)


@patch_policies([StoragePolicy(0, 'zero', True, object_ring=FakeRing())])
class TestProxyServerConfiguration(unittest.TestCase):
    def _make_app(self, conf):
        # helper function to instantiate a proxy server instance
        return proxy_server.Application(conf, FakeMemcache(),
                                        container_ring=FakeRing(),
                                        account_ring=FakeRing())

    def test_node_timeout(self):
        # later config should be extended to assert more config options
        app = self._make_app({'node_timeout': '3.5',
                              'recoverable_node_timeout': '1.5'})
        self.assertEqual(app.node_timeout, 3.5)
        self.assertEqual(app.recoverable_node_timeout, 1.5)

    def test_cors_options(self):
        # check defaults
        app = self._make_app({})
        self.assertFalse(app.cors_allow_origin)
        self.assertFalse(app.cors_expose_headers)
        self.assertTrue(app.strict_cors_mode)

        # check custom configs
        app = self._make_app({
            'cors_allow_origin': '',
            'cors_expose_headers': '',
            'strict_cors_mode': 'True'})
        self.assertTrue(app.strict_cors_mode)

        app = self._make_app({
            'cors_allow_origin': ' http://X.com,http://Y.com ,,  http://Z.com',
            'cors_expose_headers': ' custom1,,,  custom2,custom3,,',
            'strict_cors_mode': 'False'})
        self.assertEqual({'http://X.com', 'http://Y.com', 'http://Z.com'},
                         set(app.cors_allow_origin))
        self.assertEqual({'custom1', 'custom2', 'custom3'},
                         set(app.cors_expose_headers))
        self.assertFalse(app.strict_cors_mode)


@patch_policies([StoragePolicy(0, 'zero', True, object_ring=FakeRing())])
class TestProxyServer(unittest.TestCase):

    def test_get_object_ring(self):
        baseapp = proxy_server.Application({},
                                           FakeMemcache(),
                                           container_ring=FakeRing(),
                                           account_ring=FakeRing())
        with patch_policies([
            StoragePolicy(0, 'a', False, object_ring=123),
            StoragePolicy(1, 'b', True, object_ring=456),
            StoragePolicy(2, 'd', False, object_ring=789)
        ]):
            # None means legacy so always use policy 0
            ring = baseapp.get_object_ring(None)
            self.assertEqual(ring, 123)
            ring = baseapp.get_object_ring('')
            self.assertEqual(ring, 123)
            ring = baseapp.get_object_ring('0')
            self.assertEqual(ring, 123)
            ring = baseapp.get_object_ring('1')
            self.assertEqual(ring, 456)
            ring = baseapp.get_object_ring('2')
            self.assertEqual(ring, 789)
            # illegal values
            self.assertRaises(ValueError, baseapp.get_object_ring, '99')
            self.assertRaises(ValueError, baseapp.get_object_ring, 'asdf')

    def test_unhandled_exception(self):

        class MyApp(proxy_server.Application):

            def get_controller(self, path):
                raise Exception('this shouldn\'t be caught')

        app = MyApp(None, FakeMemcache(), account_ring=FakeRing(),
                    container_ring=FakeRing())
        req = Request.blank('/v1/account', environ={'REQUEST_METHOD': 'HEAD'})
        app.update_request(req)
        resp = app.handle_request(req)
        self.assertEqual(resp.status_int, 500)

    def test_internal_method_request(self):
        baseapp = proxy_server.Application({},
                                           FakeMemcache(),
                                           container_ring=FakeRing(),
                                           account_ring=FakeRing())
        resp = baseapp.handle_request(
            Request.blank('/v1/a', environ={'REQUEST_METHOD': '__init__'}))
        self.assertEqual(resp.status, '405 Method Not Allowed')

    def test_inexistent_method_request(self):
        baseapp = proxy_server.Application({},
                                           FakeMemcache(),
                                           container_ring=FakeRing(),
                                           account_ring=FakeRing())
        resp = baseapp.handle_request(
            Request.blank('/v1/a', environ={'REQUEST_METHOD': '!invalid'}))
        self.assertEqual(resp.status, '405 Method Not Allowed')

    def test_calls_authorize_allow(self):
        called = [False]

        def authorize(req):
            called[0] = True
        with save_globals():
            set_http_connect(200)
            app = proxy_server.Application(None, FakeMemcache(),
                                           account_ring=FakeRing(),
                                           container_ring=FakeRing())
            req = Request.blank('/v1/a')
            req.environ['swift.authorize'] = authorize
            app.update_request(req)
            app.handle_request(req)
        self.assertTrue(called[0])

    def test_calls_authorize_deny(self):
        called = [False]

        def authorize(req):
            called[0] = True
            return HTTPUnauthorized(request=req)
        app = proxy_server.Application(None, FakeMemcache(),
                                       account_ring=FakeRing(),
                                       container_ring=FakeRing())
        req = Request.blank('/v1/a')
        req.environ['swift.authorize'] = authorize
        app.update_request(req)
        app.handle_request(req)
        self.assertTrue(called[0])

    def test_negative_content_length(self):
        swift_dir = mkdtemp()
        try:
            baseapp = proxy_server.Application({'swift_dir': swift_dir},
                                               FakeMemcache(), FakeLogger(),
                                               FakeRing(), FakeRing())
            resp = baseapp.handle_request(
                Request.blank('/', environ={'CONTENT_LENGTH': '-1'}))
            self.assertEqual(resp.status, '400 Bad Request')
            self.assertEqual(resp.body, 'Invalid Content-Length')
            resp = baseapp.handle_request(
                Request.blank('/', environ={'CONTENT_LENGTH': '-123'}))
            self.assertEqual(resp.status, '400 Bad Request')
            self.assertEqual(resp.body, 'Invalid Content-Length')
        finally:
            rmtree(swift_dir, ignore_errors=True)

    def test_adds_transaction_id(self):
        swift_dir = mkdtemp()
        try:
            logger = FakeLogger()
            baseapp = proxy_server.Application({'swift_dir': swift_dir},
                                               FakeMemcache(), logger,
                                               container_ring=FakeLogger(),
                                               account_ring=FakeRing())
            baseapp.handle_request(
                Request.blank('/info',
                              environ={'HTTP_X_TRANS_ID_EXTRA': 'sardine',
                                       'REQUEST_METHOD': 'GET'}))
            # This is kind of a hokey way to get the transaction ID; it'd be
            # better to examine response headers, but the catch_errors
            # middleware is what sets the X-Trans-Id header, and we don't have
            # that available here.
            self.assertTrue(logger.txn_id.endswith('-sardine'))
        finally:
            rmtree(swift_dir, ignore_errors=True)

    def test_adds_transaction_id_length_limit(self):
        swift_dir = mkdtemp()
        try:
            logger = FakeLogger()
            baseapp = proxy_server.Application({'swift_dir': swift_dir},
                                               FakeMemcache(), logger,
                                               container_ring=FakeLogger(),
                                               account_ring=FakeRing())
            baseapp.handle_request(
                Request.blank('/info',
                              environ={'HTTP_X_TRANS_ID_EXTRA': 'a' * 1000,
                                       'REQUEST_METHOD': 'GET'}))
            self.assertTrue(logger.txn_id.endswith(
                '-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'))
        finally:
            rmtree(swift_dir, ignore_errors=True)

    def test_denied_host_header(self):
        swift_dir = mkdtemp()
        try:
            baseapp = proxy_server.Application({'swift_dir': swift_dir,
                                                'deny_host_headers':
                                                'invalid_host.com'},
                                               FakeMemcache(),
                                               container_ring=FakeLogger(),
                                               account_ring=FakeRing())
            resp = baseapp.handle_request(
                Request.blank('/v1/a/c/o',
                              environ={'HTTP_HOST': 'invalid_host.com'}))
            self.assertEqual(resp.status, '403 Forbidden')
        finally:
            rmtree(swift_dir, ignore_errors=True)

    def test_node_timing(self):
        baseapp = proxy_server.Application({'sorting_method': 'timing'},
                                           FakeMemcache(),
                                           container_ring=FakeRing(),
                                           account_ring=FakeRing())
        self.assertEqual(baseapp.node_timings, {})

        req = Request.blank('/v1/account', environ={'REQUEST_METHOD': 'HEAD'})
        baseapp.update_request(req)
        resp = baseapp.handle_request(req)
        self.assertEqual(resp.status_int, 503)  # couldn't connect to anything
        exp_timings = {}
        self.assertEqual(baseapp.node_timings, exp_timings)

        times = [time.time()]
        exp_timings = {'127.0.0.1': (0.1, times[0] + baseapp.timing_expiry)}
        with mock.patch('swift.proxy.server.time', lambda: times.pop(0)):
            baseapp.set_node_timing({'ip': '127.0.0.1'}, 0.1)
        self.assertEqual(baseapp.node_timings, exp_timings)

        nodes = [{'ip': '127.0.0.1'}, {'ip': '127.0.0.2'}, {'ip': '127.0.0.3'}]
        with mock.patch('swift.proxy.server.shuffle', lambda l: l):
            res = baseapp.sort_nodes(nodes)
        exp_sorting = [{'ip': '127.0.0.2'}, {'ip': '127.0.0.3'},
                       {'ip': '127.0.0.1'}]
        self.assertEqual(res, exp_sorting)

    def _do_sort_nodes(self, conf, policy_conf, nodes, policy,
                       node_timings=None):
        # Note with shuffling mocked out, sort_nodes will by default return
        # nodes in the order they are given
        nodes = list(nodes)
        conf = dict(conf, policy_config=policy_conf)
        baseapp = proxy_server.Application(conf,
                                           FakeMemcache(),
                                           logger=FakeLogger(),
                                           container_ring=FakeRing(),
                                           account_ring=FakeRing())
        if node_timings:
            for i, n in enumerate(nodes):
                baseapp.set_node_timing(n, node_timings[i])
        with mock.patch('swift.proxy.server.shuffle', lambda x: x):
            app_sorted = baseapp.sort_nodes(nodes, policy)
        self.assertFalse(baseapp.logger.get_lines_for_level('warning'))
        return baseapp, app_sorted

    def test_sort_nodes_default(self):
        nodes = [{'region': 0, 'zone': 1, 'ip': '127.0.0.3'},
                 {'region': 1, 'zone': 1, 'ip': '127.0.0.1'},
                 {'region': 2, 'zone': 2, 'ip': '127.0.0.2'}]

        # sanity check - no affinity conf results in node order unchanged
        app, actual = self._do_sort_nodes({}, {}, nodes, None)
        self.assertEqual(nodes, actual)

    def test_sort_nodes_by_affinity_proxy_server_config(self):
        nodes = [{'region': 0, 'zone': 1, 'ip': '127.0.0.3'},
                 {'region': 1, 'zone': 1, 'ip': '127.0.0.1'},
                 {'region': 2, 'zone': 2, 'ip': '127.0.0.2'}]

        # proxy-server affinity conf is to prefer r2
        conf = {'sorting_method': 'affinity', 'read_affinity': 'r2=1'}
        app, actual = self._do_sort_nodes(conf, {}, nodes, None)
        self.assertEqual([nodes[2], nodes[0], nodes[1]], actual)
        app, actual = self._do_sort_nodes(conf, {}, nodes, POLICIES[0])
        self.assertEqual([nodes[2], nodes[0], nodes[1]], actual)
        # check that node timings are not collected if sorting_method != timing
        self.assertFalse(app.sorts_by_timing)  # sanity check
        self.assertFalse(app.node_timings)  # sanity check

        # proxy-server affinity conf is to prefer region 1
        conf = {'sorting_method': 'affinity', 'read_affinity': 'r1=1'}
        app, actual = self._do_sort_nodes(conf, {}, nodes, None)
        self.assertEqual([nodes[1], nodes[0], nodes[2]], actual)
        app, actual = self._do_sort_nodes(conf, {}, nodes, POLICIES[0])
        self.assertEqual([nodes[1], nodes[0], nodes[2]], actual)

    @patch_policies([StoragePolicy(0, 'zero', True, object_ring=FakeRing()),
                     StoragePolicy(1, 'one', False, object_ring=FakeRing())])
    def test_sort_nodes_by_affinity_per_policy(self):
        nodes = [{'region': 0, 'zone': 1, 'ip': '127.0.0.4'},
                 {'region': 1, 'zone': 0, 'ip': '127.0.0.3'},
                 {'region': 2, 'zone': 1, 'ip': '127.0.0.1'},
                 {'region': 3, 'zone': 0, 'ip': '127.0.0.2'}]
        conf = {'sorting_method': 'affinity', 'read_affinity': 'r3=1'}
        per_policy = {'0': {'sorting_method': 'affinity',
                            'read_affinity': 'r1=1'},
                      '1': {'sorting_method': 'affinity',
                            'read_affinity': 'r2=1'}}
        # policy 0 affinity prefers r1
        app, actual = self._do_sort_nodes(conf, per_policy, nodes, POLICIES[0])
        self.assertEqual([nodes[1], nodes[0], nodes[2], nodes[3]], actual)
        # policy 1 affinity prefers r2
        app, actual = self._do_sort_nodes(conf, per_policy, nodes, POLICIES[1])
        self.assertEqual([nodes[2], nodes[0], nodes[1], nodes[3]], actual)
        # default affinity prefers r3
        app, actual = self._do_sort_nodes(conf, per_policy, nodes, None)
        self.assertEqual([nodes[3], nodes[0], nodes[1], nodes[2]], actual)

    def test_sort_nodes_by_affinity_per_policy_with_no_default(self):
        # no proxy-server setting but policy 0 prefers r0
        nodes = [{'region': 1, 'zone': 1, 'ip': '127.0.0.1'},
                 {'region': 0, 'zone': 2, 'ip': '127.0.0.2'}]
        conf = {}
        per_policy = {'0': {'sorting_method': 'affinity',
                            'read_affinity': 'r0=0'}}
        # policy 0 uses affinity sorting
        app, actual = self._do_sort_nodes(conf, per_policy, nodes, POLICIES[0])
        self.assertEqual([nodes[1], nodes[0]], actual)
        # any other policy will use default sorting
        app, actual = self._do_sort_nodes(conf, per_policy, nodes, None)
        self.assertEqual(nodes, actual)

    def test_sort_nodes_by_affinity_per_policy_inherits(self):
        # policy 0 has read_affinity but no sorting_method override,
        nodes = [{'region': 1, 'zone': 1, 'ip': '127.0.0.1'},
                 {'region': 0, 'zone': 2, 'ip': '127.0.0.2'}]
        conf = {}
        per_policy = {'0': {'read_affinity': 'r0=0'}}
        # policy 0 uses the default sorting method instead of affinity sorting
        app, actual = self._do_sort_nodes(conf, per_policy, nodes, POLICIES[0])
        self.assertEqual(nodes, actual)
        # but if proxy-server sorting_method is affinity then policy 0 inherits
        conf = {'sorting_method': 'affinity'}
        app, actual = self._do_sort_nodes(conf, per_policy, nodes, POLICIES[0])
        self.assertEqual([nodes[1], nodes[0]], actual)

    def test_sort_nodes_by_affinity_per_policy_overrides(self):
        # default setting is to sort by timing but policy 0 uses read affinity
        nodes = [{'region': 0, 'zone': 1, 'ip': '127.0.0.3'},
                 {'region': 1, 'zone': 1, 'ip': '127.0.0.1'},
                 {'region': 2, 'zone': 2, 'ip': '127.0.0.2'}]
        node_timings = [10, 1, 100]
        conf = {'sorting_method': 'timing'}
        per_policy = {'0': {'sorting_method': 'affinity',
                            'read_affinity': 'r1=1,r2=2'}}
        app, actual = self._do_sort_nodes(conf, per_policy, nodes, POLICIES[0],
                                          node_timings=node_timings)
        self.assertEqual([nodes[1], nodes[2], nodes[0]], actual)
        # check that timings are collected despite one policy using affinity
        self.assertTrue(app.sorts_by_timing)
        self.assertEqual(3, len(app.node_timings))
        # check app defaults to sorting by timing when no policy specified
        app, actual = self._do_sort_nodes(conf, per_policy, nodes, None,
                                          node_timings=node_timings)
        self.assertEqual([nodes[1], nodes[0], nodes[2]], actual)

    @patch_policies([StoragePolicy(0, 'zero', True, object_ring=FakeRing()),
                     StoragePolicy(1, 'one', False, object_ring=FakeRing())])
    def test_sort_nodes_by_timing_per_policy(self):
        # default setting is to sort by affinity but policy 0 uses timing
        nodes = [{'region': 0, 'zone': 1, 'ip': '127.0.0.3'},
                 {'region': 1, 'zone': 1, 'ip': '127.0.0.1'},
                 {'region': 2, 'zone': 2, 'ip': '127.0.0.2'}]
        node_timings = [10, 1, 100]

        conf = {'sorting_method': 'affinity', 'read_affinity': 'r1=1,r2=2'}
        per_policy = {'0': {'sorting_method': 'timing',
                            'read_affinity': 'r1=1,r2=2'},  # should be ignored
                      '1': {'read_affinity': 'r2=1'}}
        # policy 0 uses timing
        app, actual = self._do_sort_nodes(conf, per_policy, nodes, POLICIES[0],
                                          node_timings=node_timings)
        self.assertEqual([nodes[1], nodes[0], nodes[2]], actual)
        self.assertTrue(app.sorts_by_timing)
        self.assertEqual(3, len(app.node_timings))

        # policy 1 uses policy specific read affinity
        app, actual = self._do_sort_nodes(conf, per_policy, nodes, POLICIES[1],
                                          node_timings=node_timings)
        self.assertEqual([nodes[2], nodes[0], nodes[1]], actual)

        # check that with no policy specified the default read affinity is used
        app, actual = self._do_sort_nodes(conf, per_policy, nodes, None,
                                          node_timings=node_timings)
        self.assertEqual([nodes[1], nodes[2], nodes[0]], actual)

    def test_node_concurrency(self):
        nodes = [{'region': 1, 'zone': 1, 'ip': '127.0.0.1', 'port': 6010,
                  'device': 'sda'},
                 {'region': 2, 'zone': 2, 'ip': '127.0.0.2', 'port': 6010,
                  'device': 'sda'},
                 {'region': 3, 'zone': 3, 'ip': '127.0.0.3', 'port': 6010,
                  'device': 'sda'}]
        timings = {'127.0.0.1': 2, '127.0.0.2': 1, '127.0.0.3': 0}
        statuses = {'127.0.0.1': 200, '127.0.0.2': 200, '127.0.0.3': 200}
        req = Request.blank('/v1/account', environ={'REQUEST_METHOD': 'GET'})

        def fake_iter_nodes(*arg, **karg):
            return iter(nodes)

        class FakeConn(object):
            def __init__(self, ip, *args, **kargs):
                self.ip = ip
                self.args = args
                self.kargs = kargs

            def getresponse(self):
                body = 'Response from %s' % self.ip

                def mygetheader(header, *args, **kargs):
                    if header == "Content-Type":
                        return ""
                    elif header == "Content-Length":
                        return str(len(body))
                    else:
                        return 1

                resp = mock.Mock()
                resp.read.side_effect = [body, '']
                resp.getheader = mygetheader
                resp.getheaders.return_value = {}
                resp.reason = ''
                resp.status = statuses[self.ip]
                sleep(timings[self.ip])
                return resp

        def myfake_http_connect_raw(ip, *args, **kargs):
            conn = FakeConn(ip, *args, **kargs)
            return conn

        with mock.patch('swift.proxy.server.Application.iter_nodes',
                        fake_iter_nodes):
            with mock.patch('swift.common.bufferedhttp.http_connect_raw',
                            myfake_http_connect_raw):
                app_conf = {'concurrent_gets': 'on',
                            'concurrency_timeout': 0}
                baseapp = proxy_server.Application(app_conf,
                                                   FakeMemcache(),
                                                   container_ring=FakeRing(),
                                                   account_ring=FakeRing())
                self.assertTrue(baseapp.concurrent_gets)
                self.assertEqual(baseapp.concurrency_timeout, 0)
                baseapp.update_request(req)
                resp = baseapp.handle_request(req)

                # Should get 127.0.0.3 as this has a wait of 0 seconds.
                self.assertEqual(resp.body, 'Response from 127.0.0.3')

                # lets try again, with 127.0.0.1 with 0 timing but returns an
                # error.
                timings['127.0.0.1'] = 0
                statuses['127.0.0.1'] = 500

                # Should still get 127.0.0.3 as this has a wait of 0 seconds
                # and a success
                baseapp.update_request(req)
                resp = baseapp.handle_request(req)
                self.assertEqual(resp.body, 'Response from 127.0.0.3')

                # Now lets set the concurrency_timeout
                app_conf['concurrency_timeout'] = 2
                baseapp = proxy_server.Application(app_conf,
                                                   FakeMemcache(),
                                                   container_ring=FakeRing(),
                                                   account_ring=FakeRing())
                self.assertEqual(baseapp.concurrency_timeout, 2)
                baseapp.update_request(req)
                resp = baseapp.handle_request(req)

                # Should get 127.0.0.2 as this has a wait of 1 seconds.
                self.assertEqual(resp.body, 'Response from 127.0.0.2')

    def test_info_defaults(self):
        app = proxy_server.Application({}, FakeMemcache(),
                                       account_ring=FakeRing(),
                                       container_ring=FakeRing())

        self.assertTrue(app.expose_info)
        self.assertIsInstance(app.disallowed_sections, list)
        self.assertEqual(1, len(app.disallowed_sections))
        self.assertEqual(['swift.valid_api_versions'],
                         app.disallowed_sections)
        self.assertIsNone(app.admin_key)

    def test_get_info_controller(self):
        req = Request.blank('/info')
        app = proxy_server.Application({}, FakeMemcache(),
                                       account_ring=FakeRing(),
                                       container_ring=FakeRing())

        controller, path_parts = app.get_controller(req)

        self.assertIn('version', path_parts)
        self.assertIsNone(path_parts['version'])
        self.assertIn('disallowed_sections', path_parts)
        self.assertIn('expose_info', path_parts)
        self.assertIn('admin_key', path_parts)

        self.assertEqual(controller.__name__, 'InfoController')

    def test_exception_occurred(self):
        def do_test(additional_info):
            logger = debug_logger('test')
            app = proxy_server.Application({}, FakeMemcache(),
                                           account_ring=FakeRing(),
                                           container_ring=FakeRing(),
                                           logger=logger)
            node = app.container_ring.get_part_nodes(0)[0]
            node_key = app._error_limit_node_key(node)
            self.assertNotIn(node_key, app._error_limiting)  # sanity
            try:
                raise Exception('kaboom1!')
            except Exception as err:
                app.exception_occurred(node, 'server-type', additional_info)

            self.assertEqual(1, app._error_limiting[node_key]['errors'])
            line = logger.get_lines_for_level('error')[-1]
            self.assertIn('server-type server', line)
            self.assertIn(additional_info.decode('utf8'), line)
            self.assertIn(node['ip'], line)
            self.assertIn(str(node['port']), line)
            self.assertIn(node['device'], line)
            log_args, log_kwargs = logger.log_dict['error'][-1]
            self.assertTrue(log_kwargs['exc_info'])
            self.assertEqual(err, log_kwargs['exc_info'][1])

        do_test('success')
        do_test('succès')
        do_test(u'success')

    def test_error_occurred(self):
        def do_test(msg):
            logger = debug_logger('test')
            app = proxy_server.Application({}, FakeMemcache(),
                                           account_ring=FakeRing(),
                                           container_ring=FakeRing(),
                                           logger=logger)
            node = app.container_ring.get_part_nodes(0)[0]
            node_key = app._error_limit_node_key(node)
            self.assertNotIn(node_key, app._error_limiting)  # sanity

            app.error_occurred(node, msg)

            self.assertEqual(1, app._error_limiting[node_key]['errors'])
            line = logger.get_lines_for_level('error')[-1]
            self.assertIn(msg.decode('utf8'), line)
            self.assertIn(node['ip'], line)
            self.assertIn(str(node['port']), line)
            self.assertIn(node['device'], line)

        do_test('success')
        do_test('succès')
        do_test(u'success')

    def test_error_limit_methods(self):
        logger = debug_logger('test')
        app = proxy_server.Application({}, FakeMemcache(),
                                       account_ring=FakeRing(),
                                       container_ring=FakeRing(),
                                       logger=logger)
        node = app.container_ring.get_part_nodes(0)[0]
        # error occurred
        app.error_occurred(node, 'test msg')
        self.assertTrue('test msg' in
                        logger.get_lines_for_level('error')[-1])
        self.assertEqual(1, node_error_count(app, node))

        # exception occurred
        try:
            raise Exception('kaboom1!')
        except Exception as e1:
            app.exception_occurred(node, 'test1', 'test1 msg')
        line = logger.get_lines_for_level('error')[-1]
        self.assertIn('test1 server', line)
        self.assertIn('test1 msg', line)
        log_args, log_kwargs = logger.log_dict['error'][-1]
        self.assertTrue(log_kwargs['exc_info'])
        self.assertEqual(log_kwargs['exc_info'][1], e1)
        self.assertEqual(2, node_error_count(app, node))

        # warning exception occurred
        try:
            raise Exception('kaboom2!')
        except Exception as e2:
            app.exception_occurred(node, 'test2', 'test2 msg',
                                   level=logging.WARNING)
        line = logger.get_lines_for_level('warning')[-1]
        self.assertIn('test2 server', line)
        self.assertIn('test2 msg', line)
        log_args, log_kwargs = logger.log_dict['warning'][-1]
        self.assertTrue(log_kwargs['exc_info'])
        self.assertEqual(log_kwargs['exc_info'][1], e2)
        self.assertEqual(3, node_error_count(app, node))

        # custom exception occurred
        try:
            raise Exception('kaboom3!')
        except Exception as e3:
            e3_info = sys.exc_info()
            try:
                raise Exception('kaboom4!')
            except Exception:
                pass
            app.exception_occurred(node, 'test3', 'test3 msg',
                                   level=logging.WARNING, exc_info=e3_info)
        line = logger.get_lines_for_level('warning')[-1]
        self.assertIn('test3 server', line)
        self.assertIn('test3 msg', line)
        log_args, log_kwargs = logger.log_dict['warning'][-1]
        self.assertTrue(log_kwargs['exc_info'])
        self.assertEqual(log_kwargs['exc_info'][1], e3)
        self.assertEqual(4, node_error_count(app, node))

    def test_valid_api_version(self):
        app = proxy_server.Application({}, FakeMemcache(),
                                       account_ring=FakeRing(),
                                       container_ring=FakeRing())

        # The version string is only checked for account, container and object
        # requests; the raised APIVersionError returns a 404 to the client
        for path in [
                '/v2/a',
                '/v2/a/c',
                '/v2/a/c/o']:
            req = Request.blank(path)
            self.assertRaises(APIVersionError, app.get_controller, req)

        # Default valid API versions are ok
        for path in [
                '/v1/a',
                '/v1/a/c',
                '/v1/a/c/o',
                '/v1.0/a',
                '/v1.0/a/c',
                '/v1.0/a/c/o']:
            req = Request.blank(path)
            controller, path_parts = app.get_controller(req)
            self.assertIsNotNone(controller)

        # Ensure settings valid API version constraint works
        for version in ["42", 42]:
            try:
                with NamedTemporaryFile() as f:
                    f.write('[swift-constraints]\n')
                    f.write('valid_api_versions = %s\n' % version)
                    f.flush()
                    with mock.patch.object(utils, 'SWIFT_CONF_FILE', f.name):
                        constraints.reload_constraints()

                    req = Request.blank('/%s/a' % version)
                    controller, _ = app.get_controller(req)
                    self.assertIsNotNone(controller)

                    # In this case v1 is invalid
                    req = Request.blank('/v1/a')
                    self.assertRaises(APIVersionError, app.get_controller, req)
            finally:
                constraints.reload_constraints()

        # Check that the valid_api_versions is not exposed by default
        req = Request.blank('/info')
        controller, path_parts = app.get_controller(req)
        self.assertTrue('swift.valid_api_versions' in
                        path_parts.get('disallowed_sections'))


@patch_policies([
    StoragePolicy(0, 'zero', is_default=True),
    StoragePolicy(1, 'one'),
])
class TestProxyServerLoading(unittest.TestCase):

    def setUp(self):
        self._orig_hash_suffix = utils.HASH_PATH_SUFFIX
        utils.HASH_PATH_SUFFIX = b'endcap'
        self.tempdir = mkdtemp()

    def tearDown(self):
        rmtree(self.tempdir)
        utils.HASH_PATH_SUFFIX = self._orig_hash_suffix
        for policy in POLICIES:
            policy.object_ring = None

    def test_load_policy_rings(self):
        for policy in POLICIES:
            self.assertFalse(policy.object_ring)
        conf_path = os.path.join(self.tempdir, 'proxy-server.conf')
        conf_body = """
        [DEFAULT]
        swift_dir = %s

        [pipeline:main]
        pipeline = catch_errors cache proxy-server

        [app:proxy-server]
        use = egg:swift#proxy

        [filter:cache]
        use = egg:swift#memcache

        [filter:catch_errors]
        use = egg:swift#catch_errors
        """ % self.tempdir
        with open(conf_path, 'w') as f:
            f.write(dedent(conf_body))
        account_ring_path = os.path.join(self.tempdir, 'account.ring.gz')
        write_fake_ring(account_ring_path)
        container_ring_path = os.path.join(self.tempdir, 'container.ring.gz')
        write_fake_ring(container_ring_path)
        for policy in POLICIES:
            object_ring_path = os.path.join(self.tempdir,
                                            policy.ring_name + '.ring.gz')
            write_fake_ring(object_ring_path)
        app = loadapp(conf_path)
        # find the end of the pipeline
        while hasattr(app, 'app'):
            app = app.app

        # validate loaded rings
        self.assertEqual(app.account_ring.serialized_path,
                         account_ring_path)
        self.assertEqual(app.container_ring.serialized_path,
                         container_ring_path)
        for policy in POLICIES:
            self.assertEqual(policy.object_ring,
                             app.get_object_ring(int(policy)))

    def test_missing_rings(self):
        conf_path = os.path.join(self.tempdir, 'proxy-server.conf')
        conf_body = """
        [DEFAULT]
        swift_dir = %s

        [pipeline:main]
        pipeline = catch_errors cache proxy-server

        [app:proxy-server]
        use = egg:swift#proxy

        [filter:cache]
        use = egg:swift#memcache

        [filter:catch_errors]
        use = egg:swift#catch_errors
        """ % self.tempdir
        with open(conf_path, 'w') as f:
            f.write(dedent(conf_body))
        ring_paths = [
            os.path.join(self.tempdir, 'account.ring.gz'),
            os.path.join(self.tempdir, 'container.ring.gz'),
        ]
        for policy in POLICIES:
            self.assertFalse(policy.object_ring)
            object_ring_path = os.path.join(self.tempdir,
                                            policy.ring_name + '.ring.gz')
            ring_paths.append(object_ring_path)
        for policy in POLICIES:
            self.assertFalse(policy.object_ring)
        for ring_path in ring_paths:
            self.assertFalse(os.path.exists(ring_path))
            self.assertRaises(IOError, loadapp, conf_path)
            write_fake_ring(ring_path)
        # all rings exist, app should load
        loadapp(conf_path)
        for policy in POLICIES:
            self.assertTrue(policy.object_ring)


@patch_policies()
class TestProxyServerConfigLoading(unittest.TestCase):

    def setUp(self):
        skip_if_no_xattrs()
        self.tempdir = mkdtemp()
        account_ring_path = os.path.join(self.tempdir, 'account.ring.gz')
        write_fake_ring(account_ring_path)
        container_ring_path = os.path.join(self.tempdir, 'container.ring.gz')
        write_fake_ring(container_ring_path)

    def tearDown(self):
        rmtree(self.tempdir)

    def _write_conf(self, conf_body):
        # this is broken out to a method so that subclasses can override
        conf_path = os.path.join(self.tempdir, 'proxy-server.conf')
        with open(conf_path, 'w') as f:
            f.write(dedent(conf_body))
        return conf_path

    def _write_conf_and_load_app(self, conf_sections, app_name='proxy-server'):
        # write proxy-server.conf file, load app
        conf_body = dedent("""
        [DEFAULT]
        swift_dir = %s

        [pipeline:main]
        pipeline = %s

        %s
        """) % (self.tempdir, app_name, dedent(conf_sections))

        conf_path = self._write_conf(conf_body)
        with mock.patch('swift.proxy.server.get_logger',
                        return_value=FakeLogger()):
            app = loadapp(conf_path, allow_modify_pipeline=False)
        return app

    def _check_policy_options(self, app, exp_options, exp_is_local):
        # verify expected config
        for policy, options in exp_options.items():
            for k, v in options.items():
                actual = getattr(app.get_policy_options(policy), k)
                if k == "write_affinity_node_count_fn":
                    if policy:  # this check only applies when using a policy
                        actual = actual(policy.object_ring.replica_count)
                        self.assertEqual(v, actual)
                    continue
                self.assertEqual(v, actual,
                                 "Expected %s=%s but got %s=%s for policy %s" %
                                 (k, v, k, actual, policy))

        for policy, nodes in exp_is_local.items():
            fn = app.get_policy_options(policy).write_affinity_is_local_fn
            if nodes is None:
                self.assertIsNone(fn)
                continue
            for node, expected_result in nodes:
                actual = fn(node)
                self.assertIs(expected_result, actual,
                              "Expected %s but got %s for %s, policy %s" %
                              (expected_result, actual, node, policy))

    def test_per_policy_conf_none_configured(self):
        conf_sections = """
        [app:proxy-server]
        use = egg:swift#proxy
        """
        expected_default = {"read_affinity": "",
                            "sorting_method": "shuffle",
                            "write_affinity": "",
                            "write_affinity_node_count_fn": 6}
        exp_options = {None: expected_default,
                       POLICIES[0]: expected_default,
                       POLICIES[1]: expected_default}
        exp_is_local = {POLICIES[0]: None,
                        POLICIES[1]: None}
        app = self._write_conf_and_load_app(conf_sections)
        self._check_policy_options(app, exp_options, exp_is_local)

    def test_per_policy_conf_one_configured(self):
        conf_sections = """
        [app:proxy-server]
        use = egg:swift#proxy

        [proxy-server:policy:0]
        sorting_method = affinity
        read_affinity = r1=100
        write_affinity = r1
        write_affinity_node_count = 1 * replicas
        write_affinity_handoff_delete_count = 4
        """
        expected_default = {"read_affinity": "",
                            "sorting_method": "shuffle",
                            "write_affinity": "",
                            "write_affinity_node_count_fn": 6,
                            "write_affinity_handoff_delete_count": None}
        exp_options = {None: expected_default,
                       POLICIES[0]: {"read_affinity": "r1=100",
                                     "sorting_method": "affinity",
                                     "write_affinity": "r1",
                                     "write_affinity_node_count_fn": 3,
                                     "write_affinity_handoff_delete_count": 4},
                       POLICIES[1]: expected_default}
        exp_is_local = {POLICIES[0]: [({'region': 1, 'zone': 2}, True),
                                      ({'region': 2, 'zone': 1}, False)],
                        POLICIES[1]: None}
        app = self._write_conf_and_load_app(conf_sections)
        self._check_policy_options(app, exp_options, exp_is_local)

        default_options = app.get_policy_options(None)
        self.assertEqual(
            "ProxyOverrideOptions({}, {'sorting_method': 'shuffle', "
            "'read_affinity': '', 'write_affinity': '', "
            "'write_affinity_node_count': '2 * replicas', "
            "'write_affinity_handoff_delete_count': None})",
            repr(default_options))
        self.assertEqual(default_options, eval(repr(default_options), {
            'ProxyOverrideOptions': default_options.__class__}))

        policy_0_options = app.get_policy_options(POLICIES[0])
        self.assertEqual(
            "ProxyOverrideOptions({}, {'sorting_method': 'affinity', "
            "'read_affinity': 'r1=100', 'write_affinity': 'r1', "
            "'write_affinity_node_count': '1 * replicas', "
            "'write_affinity_handoff_delete_count': 4})",
            repr(policy_0_options))
        self.assertEqual(policy_0_options, eval(repr(policy_0_options), {
            'ProxyOverrideOptions': policy_0_options.__class__}))
        self.assertNotEqual(default_options, policy_0_options)

        policy_1_options = app.get_policy_options(POLICIES[1])
        self.assertIs(default_options, policy_1_options)

    def test_per_policy_conf_inherits_defaults(self):
        conf_sections = """
        [app:proxy-server]
        use = egg:swift#proxy
        sorting_method = affinity
        write_affinity_node_count = 1 * replicas
        write_affinity_handoff_delete_count = 3

        [proxy-server:policy:0]
        read_affinity = r1=100
        write_affinity = r1
        """
        expected_default = {"read_affinity": "",
                            "sorting_method": "affinity",
                            "write_affinity": "",
                            "write_affinity_node_count_fn": 3,
                            "write_affinity_handoff_delete_count": 3}
        exp_options = {None: expected_default,
                       POLICIES[0]: {"read_affinity": "r1=100",
                                     "sorting_method": "affinity",
                                     "write_affinity": "r1",
                                     "write_affinity_node_count_fn": 3,
                                     "write_affinity_handoff_delete_count": 3},
                       POLICIES[1]: expected_default}
        exp_is_local = {POLICIES[0]: [({'region': 1, 'zone': 2}, True),
                                      ({'region': 2, 'zone': 1}, False)],
                        POLICIES[1]: None}
        app = self._write_conf_and_load_app(conf_sections)
        self._check_policy_options(app, exp_options, exp_is_local)

    def test_per_policy_conf_overrides_default_affinity(self):
        conf_sections = """
        [app:proxy-server]
        use = egg:swift#proxy
        sorting_method = affinity
        read_affinity = r2=10
        write_affinity_node_count = 1 * replicas
        write_affinity = r2
        write_affinity_handoff_delete_count = 2

        [proxy-server:policy:0]
        read_affinity = r1=100
        write_affinity = r1
        write_affinity_node_count = 5
        write_affinity_handoff_delete_count = 3

        [proxy-server:policy:1]
        read_affinity = r1=1
        write_affinity = r3
        write_affinity_node_count = 4
        write_affinity_handoff_delete_count = 4
        """
        exp_options = {None: {"read_affinity": "r2=10",
                              "sorting_method": "affinity",
                              "write_affinity": "r2",
                              "write_affinity_node_count_fn": 3,
                              "write_affinity_handoff_delete_count": 2},
                       POLICIES[0]: {"read_affinity": "r1=100",
                                     "sorting_method": "affinity",
                                     "write_affinity": "r1",
                                     "write_affinity_node_count_fn": 5,
                                     "write_affinity_handoff_delete_count": 3},
                       POLICIES[1]: {"read_affinity": "r1=1",
                                     "sorting_method": "affinity",
                                     "write_affinity": "r3",
                                     "write_affinity_node_count_fn": 4,
                                     "write_affinity_handoff_delete_count": 4}}
        exp_is_local = {POLICIES[0]: [({'region': 1, 'zone': 2}, True),
                                      ({'region': 2, 'zone': 1}, False)],
                        POLICIES[1]: [({'region': 3, 'zone': 2}, True),
                                      ({'region': 1, 'zone': 1}, False),
                                      ({'region': 2, 'zone': 1}, False)]}
        app = self._write_conf_and_load_app(conf_sections)
        self._check_policy_options(app, exp_options, exp_is_local)

    def test_per_policy_conf_overrides_default_sorting_method(self):
        conf_sections = """
        [app:proxy-server]
        use = egg:swift#proxy
        sorting_method = timing

        [proxy-server:policy:0]
        sorting_method = affinity
        read_affinity = r1=100

        [proxy-server:policy:1]
        sorting_method = affinity
        read_affinity = r1=1
        """
        exp_options = {None: {"read_affinity": "",
                              "sorting_method": "timing"},
                       POLICIES[0]: {"read_affinity": "r1=100",
                                     "sorting_method": "affinity"},
                       POLICIES[1]: {"read_affinity": "r1=1",
                                     "sorting_method": "affinity"}}
        app = self._write_conf_and_load_app(conf_sections)
        self._check_policy_options(app, exp_options, {})

    def test_per_policy_conf_with_DEFAULT_options(self):
        conf_body = """
        [DEFAULT]
        write_affinity = r0
        read_affinity = r0=100
        swift_dir = %s

        [pipeline:main]
        pipeline = proxy-server

        [app:proxy-server]
        use = egg:swift#proxy
        # in a paste-deploy section, DEFAULT section value overrides
        write_affinity = r2
        # ...but the use of 'set' overrides the DEFAULT section value
        set read_affinity = r1=100

        [proxy-server:policy:0]
        # not a paste-deploy section so any value here overrides DEFAULT
        sorting_method = affinity
        write_affinity = r2
        read_affinity = r2=100

        [proxy-server:policy:1]
        sorting_method = affinity
        """ % self.tempdir

        # Don't just use _write_conf_and_load_app, as we don't want to have
        # duplicate DEFAULT sections
        conf_path = self._write_conf(conf_body)
        with mock.patch('swift.proxy.server.get_logger',
                        return_value=FakeLogger()):
            app = loadapp(conf_path, allow_modify_pipeline=False)

        exp_options = {
            # default read_affinity is r1, set in proxy-server section
            None: {"read_affinity": "r1=100",
                   "sorting_method": "shuffle",
                   "write_affinity": "r0",
                   "write_affinity_node_count_fn": 6,
                   "write_affinity_handoff_delete_count": None},
            # policy 0 read affinity is r2, dictated by policy 0 section
            POLICIES[0]: {"read_affinity": "r2=100",
                          "sorting_method": "affinity",
                          "write_affinity": "r2",
                          "write_affinity_node_count_fn": 6,
                          "write_affinity_handoff_delete_count": None},
            # policy 1 read_affinity is r0, dictated by DEFAULT section,
            # overrides proxy server section
            POLICIES[1]: {"read_affinity": "r0=100",
                          "sorting_method": "affinity",
                          "write_affinity": "r0",
                          "write_affinity_node_count_fn": 6,
                          "write_affinity_handoff_delete_count": None}}
        exp_is_local = {
            # default write_affinity is r0, dictated by DEFAULT section
            None: [({'region': 0, 'zone': 2}, True),
                   ({'region': 1, 'zone': 1}, False)],
            # policy 0 write_affinity is r2, dictated by policy 0 section
            POLICIES[0]: [({'region': 0, 'zone': 2}, False),
                          ({'region': 2, 'zone': 1}, True)],
            # policy 1 write_affinity is r0, inherited from default
            POLICIES[1]: [({'region': 0, 'zone': 2}, True),
                          ({'region': 1, 'zone': 1}, False)]}
        self._check_policy_options(app, exp_options, exp_is_local)

    def test_per_policy_conf_warns_about_sorting_method_mismatch(self):
        # verify that policy specific warnings are emitted when read_affinity
        # is set but sorting_method is not affinity
        conf_sections = """
        [app:proxy-server]
        use = egg:swift#proxy
        read_affinity = r2=10
        sorting_method = timing

        [proxy-server:policy:0]
        read_affinity = r1=100

        [proxy-server:policy:1]
        sorting_method = affinity
        read_affinity = r1=1
        """
        exp_options = {None: {"read_affinity": "r2=10",
                              "sorting_method": "timing"},
                       POLICIES[0]: {"read_affinity": "r1=100",
                                     "sorting_method": "timing"},
                       POLICIES[1]: {"read_affinity": "r1=1",
                                     "sorting_method": "affinity"}}
        app = self._write_conf_and_load_app(conf_sections)
        self._check_policy_options(app, exp_options, {})
        lines = app.logger.get_lines_for_level('warning')
        labels = {'default', 'policy 0 (nulo)'}
        for line in lines[:2]:
            self.assertIn(
                "sorting_method is set to 'timing', not 'affinity'", line)
            for label in labels:
                if label in line:
                    labels.remove(label)
                    break
            else:
                self.fail("None of %s found in warning: %r" % (labels, line))
        self.assertFalse(labels)

    def test_per_policy_conf_warns_override_sorting_method_mismatch(self):
        # verify that policy specific warnings are emitted when read_affinity
        # is set but sorting_method is not affinity in a policy config
        conf_sections = """
        [app:proxy-server]
        use = egg:swift#proxy
        sorting_method = affinity
        read_affinity = r2=10

        [proxy-server:policy:0]
        sorting_method = timing
        """
        exp_options = {None: {"read_affinity": "r2=10",
                              "write_affinity": "",
                              "sorting_method": "affinity"},
                       POLICIES[0]: {"read_affinity": "r2=10",
                                     "write_affinity": "",
                                     "sorting_method": "timing"}}
        app = self._write_conf_and_load_app(conf_sections)
        self._check_policy_options(app, exp_options, {})
        lines = app.logger.get_lines_for_level('warning')
        for line in lines:
            # proxy-server gets instantiated twice during loadapp so expect two
            # warnings; check that both warnings refer to policy 0 and not the
            # default config
            self.assertIn(
                "sorting_method is set to 'timing', not 'affinity'", line)
            self.assertIn('policy 0 (nulo)', line)
        self.assertFalse(lines[2:])

    def test_per_policy_conf_section_name_inherits_from_app_section_name(self):
        conf_sections = """
        [app:proxy-srv]
        use = egg:swift#proxy
        sorting_method = affinity

        [proxy-server:policy:0]
        sorting_method = timing
        # ignored!

        [proxy-srv:policy:1]
        sorting_method = shuffle
        """
        exp_options = {None: {'sorting_method': 'affinity'},
                       POLICIES[0]: {'sorting_method': 'affinity'},
                       POLICIES[1]: {'sorting_method': 'shuffle'}}
        app = self._write_conf_and_load_app(conf_sections, 'proxy-srv')
        self._check_policy_options(app, exp_options, {})

    def test_per_policy_conf_with_unknown_policy(self):
        # verify that unknown policy section raises an error
        conf_sections = """
        [app:proxy-server]
        use = egg:swift#proxy
        read_affinity = r2=10
        sorting_method = affinity

        [proxy-server:policy:999]
        read_affinity = r2z1=1
        """
        with self.assertRaises(ValueError) as cm:
            self._write_conf_and_load_app(conf_sections)
        self.assertIn('No policy found for override config, index: 999',
                      cm.exception.message)

    def test_per_policy_conf_sets_timing_sorting_method(self):
        conf_sections = """
        [app:proxy-server]
        use = egg:swift#proxy
        sorting_method = affinity

        [proxy-server:policy:0]
        sorting_method = timing

        [proxy-server:policy:1]
        read_affinity = r1=1
        """
        exp_options = {None: {"read_affinity": "",
                              "sorting_method": "affinity"},
                       POLICIES[0]: {"read_affinity": "",
                                     "sorting_method": "timing"},
                       POLICIES[1]: {"read_affinity": "r1=1",
                                     "sorting_method": "affinity"}}
        app = self._write_conf_and_load_app(conf_sections)
        self._check_policy_options(app, exp_options, {})

    def test_per_policy_conf_invalid_sorting_method_value(self):
        def do_test(conf_sections, scope):
            with self.assertRaises(ValueError) as cm:
                self._write_conf_and_load_app(conf_sections)
            self.assertEqual(
                'Invalid sorting_method value; must be one of shuffle, '
                "timing, affinity, not 'broken' for %s" % scope,
                cm.exception.message)

        conf_sections = """
        [app:proxy-server]
        use = egg:swift#proxy
        sorting_method = shuffle

        [proxy-server:policy:0]
        sorting_method = broken
        """
        do_test(conf_sections, 'policy 0 (nulo)')

        conf_sections = """
        [app:proxy-server]
        use = egg:swift#proxy
        sorting_method = broken

        [proxy-server:policy:0]
        sorting_method = shuffle
        """
        do_test(conf_sections, '(default)')

    def test_per_policy_conf_invalid_read_affinity_value(self):
        def do_test(conf_sections, label):
            with self.assertRaises(ValueError) as cm:
                self._write_conf_and_load_app(conf_sections)
            self.assertIn('broken', cm.exception.message)
            self.assertIn(
                'Invalid read_affinity value:', cm.exception.message)
            self.assertIn(label, cm.exception.message)

        conf_sections = """
        [app:proxy-server]
        use = egg:swift#proxy
        sorting_method = affinity
        read_affinity = r1=1

        [proxy-server:policy:0]
        sorting_method = affinity
        read_affinity = broken
        """
        do_test(conf_sections, 'policy 0 (nulo)')

        conf_sections = """
        [app:proxy-server]
        use = egg:swift#proxy
        sorting_method = affinity
        read_affinity = broken

        [proxy-server:policy:0]
        sorting_method = affinity
        read_affinity = r1=1
        """
        do_test(conf_sections, '(default)')

    def test_per_policy_conf_invalid_write_affinity_value(self):
        def do_test(conf_sections, label):
            with self.assertRaises(ValueError) as cm:
                self._write_conf_and_load_app(conf_sections)
            self.assertIn('broken', cm.exception.message)
            self.assertIn(
                'Invalid write_affinity value:', cm.exception.message)
            self.assertIn(label, cm.exception.message)

        conf_sections = """
        [app:proxy-server]
        use = egg:swift#proxy
        write_affinity = r1

        [proxy-server:policy:0]
        sorting_method = affinity
        write_affinity = broken
        """
        do_test(conf_sections, 'policy 0 (nulo)')

        conf_sections = """
        [app:proxy-server]
        use = egg:swift#proxy
        write_affinity = broken

        [proxy-server:policy:0]
        write_affinity = r1
        """
        do_test(conf_sections, '(default)')

    def test_per_policy_conf_invalid_write_affinity_node_count_value(self):
        def do_test(conf_sections, label):
            with self.assertRaises(ValueError) as cm:
                self._write_conf_and_load_app(conf_sections)
            self.assertIn('2* replicas', cm.exception.message)
            self.assertIn('Invalid write_affinity_node_count value:',
                          cm.exception.message)
            self.assertIn(label, cm.exception.message)

        conf_sections = """
        [app:proxy-server]
        use = egg:swift#proxy
        write_affinity_node_count = 2 * replicas

        [proxy-server:policy:0]
        sorting_method = affinity
        write_affinity_node_count = 2* replicas
        """
        do_test(conf_sections, 'policy 0 (nulo)')

        conf_sections = """
        [app:proxy-server]
        use = egg:swift#proxy
        write_affinity_node_count = 2* replicas

        [proxy-server:policy:0]
        write_affinity_node_count = 2 * replicas
        """
        do_test(conf_sections, '(default)')

    def test_per_policy_conf_bad_section_name(self):
        def do_test(policy):
            conf_sections = """
                [app:proxy-server]
                use = egg:swift#proxy

                [proxy-server:policy:%s]
            """ % policy
            with self.assertRaises(ValueError) as cm:
                self._write_conf_and_load_app(conf_sections)
            self.assertEqual(
                "Override config must refer to policy index: %r" % policy,
                cm.exception.message)

        do_test('')
        do_test('uno')
        do_test('0.0')


class TestProxyServerConfigStringLoading(TestProxyServerConfigLoading):
    # The proxy may be loaded from a conf string rather than a conf file, for
    # example when ContainerSync creates an InternalClient from a default
    # config string. So repeat super-class tests using a string loader.
    def _write_conf(self, conf_body):
        # this is broken out to a method so that subclasses can override
        return ConfigString(conf_body)


class BaseTestObjectController(object):
    """
    A root of TestObjController that implements helper methods for child
    TestObjControllers.
    """
    def setUp(self):
        # clear proxy logger result for each test
        _test_servers[0].logger._clear()

    def assert_status_map(self, method, statuses, expected, raise_exc=False):
        with save_globals():
            kwargs = {}
            if raise_exc:
                kwargs['raise_exc'] = raise_exc

            set_http_connect(*statuses, **kwargs)
            self.app.memcache.store = {}
            req = Request.blank('/v1/a/c/o',
                                headers={'Content-Length': '0',
                                         'Content-Type': 'text/plain'})
            self.app.update_request(req)
            try:
                res = method(req)
            except HTTPException as res:
                pass
            self.assertEqual(res.status_int, expected)

            # repeat test
            set_http_connect(*statuses, **kwargs)
            self.app.memcache.store = {}
            req = Request.blank('/v1/a/c/o',
                                headers={'Content-Length': '0',
                                         'Content-Type': 'text/plain'})
            self.app.update_request(req)
            try:
                res = method(req)
            except HTTPException as res:
                pass
            self.assertEqual(res.status_int, expected)

    def _sleep_enough(self, condition):
        for sleeptime in (0.1, 1.0):
            sleep(sleeptime)
            if condition():
                break

    def put_container(self, policy_name, container_name):
        # Note: only works if called with unpatched policies
        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%s HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'Content-Length: 0\r\n'
                 'X-Storage-Token: t\r\n'
                 'X-Storage-Policy: %s\r\n'
                 '\r\n' % (container_name, policy_name))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 2'
        self.assertEqual(headers[:len(exp)], exp)

    def _test_conditional_GET(self, policy):
        container_name = uuid.uuid4().hex
        object_path = '/v1/a/%s/conditionals' % container_name
        self.put_container(policy.name, container_name)

        obj = 'this object has an etag and is otherwise unimportant'
        etag = md5(obj).hexdigest()
        not_etag = md5(obj + "blahblah").hexdigest()

        prolis = _test_sockets[0]
        prosrv = _test_servers[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT %s HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'Content-Length: %d\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 '\r\n%s' % (object_path, len(obj), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

        for verb, body in (('GET', obj), ('HEAD', '')):
            # If-Match
            req = Request.blank(
                object_path,
                environ={'REQUEST_METHOD': verb},
                headers={'If-Match': etag})
            resp = req.get_response(prosrv)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.body, body)
            self.assertEqual(etag, resp.headers.get('etag'))
            self.assertEqual('bytes', resp.headers.get('accept-ranges'))

            req = Request.blank(
                object_path,
                environ={'REQUEST_METHOD': verb},
                headers={'If-Match': not_etag})
            resp = req.get_response(prosrv)
            self.assertEqual(resp.status_int, 412)
            self.assertEqual(etag, resp.headers.get('etag'))

            req = Request.blank(
                object_path,
                environ={'REQUEST_METHOD': verb},
                headers={'If-Match': "*"})
            resp = req.get_response(prosrv)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.body, body)
            self.assertEqual(etag, resp.headers.get('etag'))
            self.assertEqual('bytes', resp.headers.get('accept-ranges'))

            # If-None-Match
            req = Request.blank(
                object_path,
                environ={'REQUEST_METHOD': verb},
                headers={'If-None-Match': etag})
            resp = req.get_response(prosrv)
            self.assertEqual(resp.status_int, 304)
            self.assertEqual(etag, resp.headers.get('etag'))
            self.assertEqual('bytes', resp.headers.get('accept-ranges'))

            req = Request.blank(
                object_path,
                environ={'REQUEST_METHOD': verb},
                headers={'If-None-Match': not_etag})
            resp = req.get_response(prosrv)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.body, body)
            self.assertEqual(etag, resp.headers.get('etag'))
            self.assertEqual('bytes', resp.headers.get('accept-ranges'))

            req = Request.blank(
                object_path,
                environ={'REQUEST_METHOD': verb},
                headers={'If-None-Match': "*"})
            resp = req.get_response(prosrv)
            self.assertEqual(resp.status_int, 304)
            self.assertEqual(etag, resp.headers.get('etag'))
            self.assertEqual('bytes', resp.headers.get('accept-ranges'))

        error_lines = prosrv.logger.get_lines_for_level('error')
        warn_lines = prosrv.logger.get_lines_for_level('warning')
        self.assertEqual(len(error_lines), 0)  # sanity
        self.assertEqual(len(warn_lines), 0)  # sanity


@patch_policies([StoragePolicy(0, 'zero', True,
                               object_ring=FakeRing(base_port=3000))])
class TestReplicatedObjectController(
        BaseTestObjectController, unittest.TestCase):
    """
    Test suite for replication policy
    """
    def setUp(self):
        skip_if_no_xattrs()
        self.app = proxy_server.Application(
            None, FakeMemcache(),
            logger=debug_logger('proxy-ut'),
            account_ring=FakeRing(),
            container_ring=FakeRing())
        super(TestReplicatedObjectController, self).setUp()

    def tearDown(self):
        self.app.account_ring.set_replicas(3)
        self.app.container_ring.set_replicas(3)
        for policy in POLICIES:
            policy.object_ring = FakeRing(base_port=3000)

    @unpatch_policies
    def test_policy_IO(self):
        def check_file(policy, cont, devs, check_val):
            partition, nodes = policy.object_ring.get_nodes('a', cont, 'o')
            conf = {'devices': _testdir, 'mount_check': 'false'}
            df_mgr = diskfile.DiskFileManager(conf, FakeLogger())
            for dev in devs:
                file = df_mgr.get_diskfile(dev, partition, 'a',
                                           cont, 'o',
                                           policy=policy)
                if check_val is True:
                    file.open()

        prolis = _test_sockets[0]
        prosrv = _test_servers[0]

        # check policy 0: put file on c, read it back, check loc on disk
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        obj = 'test_object0'
        path = '/v1/a/c/o'
        fd.write('PUT %s HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Length: %s\r\n'
                 'Content-Type: text/plain\r\n'
                 '\r\n%s' % (path, str(len(obj)), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)
        req = Request.blank(path,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Content-Type':
                                     'text/plain'})
        res = req.get_response(prosrv)
        self.assertEqual(res.status_int, 200)
        self.assertEqual(res.body, obj)

        check_file(POLICIES[0], 'c', ['sda1', 'sdb1'], True)
        check_file(POLICIES[0], 'c', ['sdc1', 'sdd1', 'sde1', 'sdf1'], False)

        # check policy 1: put file on c1, read it back, check loc on disk
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        path = '/v1/a/c1/o'
        obj = 'test_object1'
        fd.write('PUT %s HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Length: %s\r\n'
                 'Content-Type: text/plain\r\n'
                 '\r\n%s' % (path, str(len(obj)), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        self.assertEqual(headers[:len(exp)], exp)
        req = Request.blank(path,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Content-Type':
                                     'text/plain'})
        res = req.get_response(prosrv)
        self.assertEqual(res.status_int, 200)
        self.assertEqual(res.body, obj)

        check_file(POLICIES[1], 'c1', ['sdc1', 'sdd1'], True)
        check_file(POLICIES[1], 'c1', ['sda1', 'sdb1', 'sde1', 'sdf1'], False)

        # check policy 2: put file on c2, read it back, check loc on disk
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        path = '/v1/a/c2/o'
        obj = 'test_object2'
        fd.write('PUT %s HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Length: %s\r\n'
                 'Content-Type: text/plain\r\n'
                 '\r\n%s' % (path, str(len(obj)), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        self.assertEqual(headers[:len(exp)], exp)
        req = Request.blank(path,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Content-Type':
                                     'text/plain'})
        res = req.get_response(prosrv)
        self.assertEqual(res.status_int, 200)
        self.assertEqual(res.body, obj)

        check_file(POLICIES[2], 'c2', ['sde1', 'sdf1'], True)
        check_file(POLICIES[2], 'c2', ['sda1', 'sdb1', 'sdc1', 'sdd1'], False)

    @unpatch_policies
    def test_policy_IO_override(self):
        if hasattr(_test_servers[-1], '_filesystem'):
            # ironically, the _filesystem attribute on the object server means
            # the in-memory diskfile is in use, so this test does not apply
            return

        prosrv = _test_servers[0]

        # validate container policy is 1
        req = Request.blank('/v1/a/c1', method='HEAD')
        res = req.get_response(prosrv)
        self.assertEqual(res.status_int, 204)  # sanity check
        self.assertEqual(POLICIES[1].name, res.headers['x-storage-policy'])

        # check overrides: put it in policy 2 (not where the container says)
        req = Request.blank(
            '/v1/a/c1/wrong-o',
            environ={'REQUEST_METHOD': 'PUT',
                     'wsgi.input': BytesIO(b"hello")},
            headers={'Content-Type': 'text/plain',
                     'Content-Length': '5',
                     'X-Backend-Storage-Policy-Index': '2'})
        res = req.get_response(prosrv)
        self.assertEqual(res.status_int, 201)  # sanity check

        # go to disk to make sure it's there
        partition, nodes = prosrv.get_object_ring(2).get_nodes(
            'a', 'c1', 'wrong-o')
        node = nodes[0]
        conf = {'devices': _testdir, 'mount_check': 'false'}
        df_mgr = diskfile.DiskFileManager(conf, FakeLogger())
        df = df_mgr.get_diskfile(node['device'], partition, 'a',
                                 'c1', 'wrong-o', policy=POLICIES[2])
        with df.open():
            contents = ''.join(df.reader())
            self.assertEqual(contents, "hello")

        # can't get it from the normal place
        req = Request.blank('/v1/a/c1/wrong-o',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Content-Type': 'text/plain'})
        res = req.get_response(prosrv)
        self.assertEqual(res.status_int, 404)  # sanity check

        # but we can get it from policy 2
        req = Request.blank('/v1/a/c1/wrong-o',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Content-Type': 'text/plain',
                                     'X-Backend-Storage-Policy-Index': '2'})

        res = req.get_response(prosrv)
        self.assertEqual(res.status_int, 200)
        self.assertEqual(res.body, 'hello')

        # and we can delete it the same way
        req = Request.blank('/v1/a/c1/wrong-o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Content-Type': 'text/plain',
                                     'X-Backend-Storage-Policy-Index': '2'})

        res = req.get_response(prosrv)
        self.assertEqual(res.status_int, 204)

        df = df_mgr.get_diskfile(node['device'], partition, 'a',
                                 'c1', 'wrong-o', policy=POLICIES[2])
        try:
            df.open()
        except DiskFileNotExist as e:
            self.assertGreater(float(e.timestamp), 0)
        else:
            self.fail('did not raise DiskFileNotExist')

    @unpatch_policies
    def test_GET_newest_large_file(self):
        prolis = _test_sockets[0]
        prosrv = _test_servers[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        obj = 'a' * (1024 * 1024)
        path = '/v1/a/c/o.large'
        fd.write('PUT %s HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Length: %s\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 '\r\n%s' % (path, str(len(obj)), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)
        req = Request.blank(path,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Content-Type':
                                     'application/octet-stream',
                                     'X-Newest': 'true'})
        res = req.get_response(prosrv)
        self.assertEqual(res.status_int, 200)
        self.assertEqual(res.body, obj)

    @unpatch_policies
    def test_GET_ranges(self):
        prolis = _test_sockets[0]
        prosrv = _test_servers[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        obj = (''.join(
            ('beans lots of beans lots of beans lots of beans yeah %04d ' % i)
            for i in range(100)))

        path = '/v1/a/c/o.beans'
        fd.write('PUT %s HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Length: %s\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 '\r\n%s' % (path, str(len(obj)), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

        # one byte range
        req = Request.blank(
            path,
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Content-Type': 'application/octet-stream',
                     'Range': 'bytes=10-200'})
        res = req.get_response(prosrv)
        self.assertEqual(res.status_int, 206)
        self.assertEqual(res.body, obj[10:201])

        # multiple byte ranges
        req = Request.blank(
            path,
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Content-Type': 'application/octet-stream',
                     'Range': 'bytes=10-200,1000-1099,4123-4523'})
        res = req.get_response(prosrv)
        self.assertEqual(res.status_int, 206)
        ct, params = parse_content_type(res.headers['Content-Type'])
        self.assertEqual(ct, 'multipart/byteranges')

        boundary = dict(params).get('boundary')
        self.assertIsNotNone(boundary)

        got_mime_docs = []
        for mime_doc_fh in iter_multipart_mime_documents(StringIO(res.body),
                                                         boundary):
            headers = parse_mime_headers(mime_doc_fh)
            body = mime_doc_fh.read()
            got_mime_docs.append((headers, body))
        self.assertEqual(len(got_mime_docs), 3)

        first_range_headers = got_mime_docs[0][0]
        first_range_body = got_mime_docs[0][1]
        self.assertEqual(first_range_headers['Content-Range'],
                         'bytes 10-200/5800')
        self.assertEqual(first_range_body, obj[10:201])

        second_range_headers = got_mime_docs[1][0]
        second_range_body = got_mime_docs[1][1]
        self.assertEqual(second_range_headers['Content-Range'],
                         'bytes 1000-1099/5800')
        self.assertEqual(second_range_body, obj[1000:1100])

        second_range_headers = got_mime_docs[2][0]
        second_range_body = got_mime_docs[2][1]
        self.assertEqual(second_range_headers['Content-Range'],
                         'bytes 4123-4523/5800')
        self.assertEqual(second_range_body, obj[4123:4524])

    @unpatch_policies
    def test_GET_bad_range_zero_byte(self):
        prolis = _test_sockets[0]
        prosrv = _test_servers[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()

        path = '/v1/a/c/o.zerobyte'
        fd.write('PUT %s HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Length: 0\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 '\r\n' % (path,))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

        # bad byte-range
        req = Request.blank(
            path,
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Content-Type': 'application/octet-stream',
                     'Range': 'bytes=spaghetti-carbonara'})
        res = req.get_response(prosrv)
        self.assertEqual(res.status_int, 200)
        self.assertEqual(res.body, '')

        # not a byte-range
        req = Request.blank(
            path,
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Content-Type': 'application/octet-stream',
                     'Range': 'Kotta'})
        res = req.get_response(prosrv)
        self.assertEqual(res.status_int, 200)
        self.assertEqual(res.body, '')

    @unpatch_policies
    def test_GET_short_read(self):
        prolis = _test_sockets[0]
        prosrv = _test_servers[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        obj = (''.join(
            ('%d bottles of beer on the wall\n' % i)
            for i in reversed(range(1, 200))))

        # if the object is too short, then we don't have a mid-stream
        # exception after the headers are sent, but instead an early one
        # before the headers
        self.assertGreater(len(obj), wsgi.MINIMUM_CHUNK_SIZE)

        path = '/v1/a/c/o.bottles'
        fd.write('PUT %s HTTP/1.1\r\n'
                 'Connection: keep-alive\r\n'
                 'Host: localhost\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Length: %s\r\n'
                 'Content-Type: application/beer-stream\r\n'
                 '\r\n%s' % (path, str(len(obj)), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

        # go shorten that object by a few bytes
        shrinkage = 100   # bytes
        shortened = 0
        for dirpath, _, filenames in os.walk(_testdir):
            for filename in filenames:
                if filename.endswith(".data"):
                    with open(os.path.join(dirpath, filename), "r+") as fh:
                        fh.truncate(len(obj) - shrinkage)
                        shortened += 1
        self.assertGreater(shortened, 0)  # ensure test is working

        real_fstat = os.fstat

        # stop the object server from immediately quarantining the object
        # and returning 404
        def lying_fstat(fd):
            sr = real_fstat(fd)
            fake_stat_result = posix.stat_result((
                sr.st_mode, sr.st_ino, sr.st_dev, sr.st_nlink, sr.st_uid,
                sr.st_gid,
                sr.st_size + shrinkage,   # here's the lie
                sr.st_atime, sr.st_mtime, sr.st_ctime))
            return fake_stat_result

        # Read the object back
        with mock.patch('os.fstat', lying_fstat), \
                mock.patch.object(prosrv, 'client_chunk_size', 32), \
                mock.patch.object(prosrv, 'object_chunk_size', 32):
            fd.write('GET %s HTTP/1.1\r\n'
                     'Host: localhost\r\n'
                     'Connection: keep-alive\r\n'
                     'X-Storage-Token: t\r\n'
                     '\r\n' % (path,))
            fd.flush()
            headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEqual(headers[:len(exp)], exp)

        obj_parts = []
        while True:
            buf = fd.read(1024)
            if not buf:
                break
            obj_parts.append(buf)
        got_obj = ''.join(obj_parts)
        self.assertLessEqual(len(got_obj), len(obj) - shrinkage)

        # Make sure the server closed the connection
        with self.assertRaises(socket.error):
            # Two calls are necessary; you can apparently write to a socket
            # that the peer has closed exactly once without error, then the
            # kernel discovers that the connection is not open and
            # subsequent send attempts fail.
            sock.sendall('GET /info HTTP/1.1\r\n')
            sock.sendall('Host: localhost\r\n'
                         'X-Storage-Token: t\r\n'
                         '\r\n')

    @unpatch_policies
    def test_GET_short_read_resuming(self):
        prolis = _test_sockets[0]
        prosrv = _test_servers[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        obj = (''.join(
            ('%d bottles of beer on the wall\n' % i)
            for i in reversed(range(1, 200))))

        # if the object is too short, then we don't have a mid-stream
        # exception after the headers are sent, but instead an early one
        # before the headers
        self.assertGreater(len(obj), wsgi.MINIMUM_CHUNK_SIZE)

        path = '/v1/a/c/o.bottles'
        fd.write('PUT %s HTTP/1.1\r\n'
                 'Connection: keep-alive\r\n'
                 'Host: localhost\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Length: %s\r\n'
                 'Content-Type: application/beer-stream\r\n'
                 '\r\n%s' % (path, str(len(obj)), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

        # we shorten the first replica of the object by 200 bytes and leave
        # the others untouched
        _, obj_nodes = POLICIES.default.object_ring.get_nodes(
            "a", "c", "o.bottles")

        shortened = 0
        for dirpath, _, filenames in os.walk(
                os.path.join(_testdir, obj_nodes[0]['device'])):
            for filename in filenames:
                if filename.endswith(".data"):
                    if shortened == 0:
                        with open(os.path.join(dirpath, filename), "r+") as fh:
                            fh.truncate(len(obj) - 200)
                            shortened += 1
        self.assertEqual(shortened, 1)  # sanity check

        real_fstat = os.fstat

        # stop the object server from immediately quarantining the object
        # and returning 404
        def lying_fstat(fd):
            sr = real_fstat(fd)
            fake_stat_result = posix.stat_result((
                sr.st_mode, sr.st_ino, sr.st_dev, sr.st_nlink, sr.st_uid,
                sr.st_gid,
                len(obj),  # sometimes correct, sometimes not
                sr.st_atime, sr.st_mtime, sr.st_ctime))
            return fake_stat_result

        # Read the object back
        with mock.patch('os.fstat', lying_fstat), \
                mock.patch.object(prosrv, 'client_chunk_size', 32), \
                mock.patch.object(prosrv, 'object_chunk_size', 32), \
                mock.patch.object(prosrv, 'sort_nodes',
                                  lambda nodes, **kw: nodes):
            fd.write('GET %s HTTP/1.1\r\n'
                     'Host: localhost\r\n'
                     'Connection: close\r\n'
                     'X-Storage-Token: t\r\n'
                     '\r\n' % (path,))
            fd.flush()
            headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEqual(headers[:len(exp)], exp)

        obj_parts = []
        while True:
            buf = fd.read(1024)
            if not buf:
                break
            obj_parts.append(buf)
        got_obj = ''.join(obj_parts)

        # technically this is a redundant test, but it saves us from screens
        # full of error message when got_obj is shorter than obj
        self.assertEqual(len(obj), len(got_obj))
        self.assertEqual(obj, got_obj)

    @unpatch_policies
    def test_GET_ranges_resuming(self):
        prolis = _test_sockets[0]
        prosrv = _test_servers[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        obj = (''.join(
            ('Smurf! The smurfing smurf is completely smurfed. %03d ' % i)
            for i in range(1000)))

        path = '/v1/a/c/o.smurfs'
        fd.write('PUT %s HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Length: %s\r\n'
                 'Content-Type: application/smurftet-stream\r\n'
                 '\r\n%s' % (path, str(len(obj)), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

        kaboomed = [0]
        bytes_before_timeout = [None]

        class FileLikeKaboom(object):
            def __init__(self, inner_file_like):
                self.inner_file_like = inner_file_like

            # close(), etc.
            def __getattr__(self, attr):
                return getattr(self.inner_file_like, attr)

            def readline(self, *a, **kw):
                if bytes_before_timeout[0] <= 0:
                    kaboomed[0] += 1
                    raise ChunkReadTimeout(None)
                result = self.inner_file_like.readline(*a, **kw)
                if len(result) > bytes_before_timeout[0]:
                    result = result[:bytes_before_timeout[0]]
                bytes_before_timeout[0] -= len(result)
                return result

            def read(self, length=None):
                result = self.inner_file_like.read(length)
                if bytes_before_timeout[0] <= 0:
                    kaboomed[0] += 1
                    raise ChunkReadTimeout(None)
                if len(result) > bytes_before_timeout[0]:
                    result = result[:bytes_before_timeout[0]]
                bytes_before_timeout[0] -= len(result)
                return result

        orig_hrtdi = swift.common.request_helpers. \
            http_response_to_document_iters

        # Use this to mock out http_response_to_document_iters. On the first
        # call, the result will be sabotaged to blow up with
        # ChunkReadTimeout after some number of bytes are read. On
        # subsequent calls, no sabotage will be added.

        def sabotaged_hrtdi(*a, **kw):
            resp_parts = orig_hrtdi(*a, **kw)
            for sb, eb, l, h, range_file in resp_parts:
                if bytes_before_timeout[0] <= 0:
                    # simulate being unable to read MIME part of
                    # multipart/byteranges response
                    kaboomed[0] += 1
                    raise ChunkReadTimeout(None)
                boomer = FileLikeKaboom(range_file)
                yield sb, eb, l, h, boomer

        sabotaged = [False]

        def single_sabotage_hrtdi(*a, **kw):
            if not sabotaged[0]:
                sabotaged[0] = True
                return sabotaged_hrtdi(*a, **kw)
            else:
                return orig_hrtdi(*a, **kw)

        # We want sort of an end-to-end test of object resuming, so what we
        # do is mock out stuff so the proxy thinks it only read a certain
        # number of bytes before it got a timeout.
        bytes_before_timeout[0] = 300
        with mock.patch.object(proxy_base,
                               'http_response_to_document_iters',
                               single_sabotage_hrtdi):
            req = Request.blank(
                path,
                environ={'REQUEST_METHOD': 'GET'},
                headers={'Content-Type': 'application/octet-stream',
                         'Range': 'bytes=0-500'})
            res = req.get_response(prosrv)
            body = res.body   # read the whole thing
        self.assertEqual(kaboomed[0], 1)  # sanity check
        self.assertEqual(res.status_int, 206)
        self.assertEqual(len(body), 501)
        self.assertEqual(body, obj[:501])

        # Sanity-check for multi-range resume: make sure we actually break
        # in the middle of the second byterange. This test is partially
        # about what happens when all the object servers break at once, and
        # partially about validating all these mocks we do. After all, the
        # point of resuming is that the client can't tell anything went
        # wrong, so we need a test where we can't resume and something
        # *does* go wrong so we can observe it.
        bytes_before_timeout[0] = 700
        kaboomed[0] = 0
        sabotaged[0] = False
        prosrv._error_limiting = {}  # clear out errors
        with mock.patch.object(proxy_base,
                               'http_response_to_document_iters',
                               sabotaged_hrtdi):  # perma-broken
            req = Request.blank(
                path,
                environ={'REQUEST_METHOD': 'GET'},
                headers={'Range': 'bytes=0-500,1000-1500,2000-2500'})
            res = req.get_response(prosrv)
            body = ''
            try:
                for chunk in res.app_iter:
                    body += chunk
            except (ChunkReadTimeout, ChunkReadError):
                pass

        self.assertEqual(res.status_int, 206)
        self.assertGreater(kaboomed[0], 0)  # sanity check

        ct, params = parse_content_type(res.headers['Content-Type'])
        self.assertEqual(ct, 'multipart/byteranges')  # sanity check
        boundary = dict(params).get('boundary')
        self.assertIsNotNone(boundary)  # sanity check
        got_byteranges = []
        for mime_doc_fh in iter_multipart_mime_documents(StringIO(body),
                                                         boundary):
            parse_mime_headers(mime_doc_fh)
            body = mime_doc_fh.read()
            got_byteranges.append(body)

        self.assertEqual(len(got_byteranges), 2)
        self.assertEqual(len(got_byteranges[0]), 501)
        self.assertEqual(len(got_byteranges[1]), 199)  # partial

        # Multi-range resume, resuming in the middle of the first byterange
        bytes_before_timeout[0] = 300
        kaboomed[0] = 0
        sabotaged[0] = False
        prosrv._error_limiting = {}  # clear out errors
        with mock.patch.object(proxy_base,
                               'http_response_to_document_iters',
                               single_sabotage_hrtdi):
            req = Request.blank(
                path,
                environ={'REQUEST_METHOD': 'GET'},
                headers={'Range': 'bytes=0-500,1000-1500,2000-2500'})
            res = req.get_response(prosrv)
            body = ''.join(res.app_iter)

        self.assertEqual(res.status_int, 206)
        self.assertEqual(kaboomed[0], 1)  # sanity check

        ct, params = parse_content_type(res.headers['Content-Type'])
        self.assertEqual(ct, 'multipart/byteranges')  # sanity check
        boundary = dict(params).get('boundary')
        self.assertTrue(boundary is not None)  # sanity check
        got_byteranges = []
        for mime_doc_fh in iter_multipart_mime_documents(StringIO(body),
                                                         boundary):
            parse_mime_headers(mime_doc_fh)
            body = mime_doc_fh.read()
            got_byteranges.append(body)

        self.assertEqual(len(got_byteranges), 3)
        self.assertEqual(len(got_byteranges[0]), 501)
        self.assertEqual(got_byteranges[0], obj[:501])
        self.assertEqual(len(got_byteranges[1]), 501)
        self.assertEqual(got_byteranges[1], obj[1000:1501])
        self.assertEqual(len(got_byteranges[2]), 501)
        self.assertEqual(got_byteranges[2], obj[2000:2501])

        # Multi-range resume, first GET dies in the middle of the second set
        # of MIME headers
        bytes_before_timeout[0] = 501
        kaboomed[0] = 0
        sabotaged[0] = False
        prosrv._error_limiting = {}  # clear out errors
        with mock.patch.object(proxy_base,
                               'http_response_to_document_iters',
                               single_sabotage_hrtdi):
            req = Request.blank(
                path,
                environ={'REQUEST_METHOD': 'GET'},
                headers={'Range': 'bytes=0-500,1000-1500,2000-2500'})
            res = req.get_response(prosrv)
            body = ''.join(res.app_iter)

        self.assertEqual(res.status_int, 206)
        self.assertGreaterEqual(kaboomed[0], 1)  # sanity check

        ct, params = parse_content_type(res.headers['Content-Type'])
        self.assertEqual(ct, 'multipart/byteranges')  # sanity check
        boundary = dict(params).get('boundary')
        self.assertIsNotNone(boundary)  # sanity check
        got_byteranges = []
        for mime_doc_fh in iter_multipart_mime_documents(StringIO(body),
                                                         boundary):
            parse_mime_headers(mime_doc_fh)
            body = mime_doc_fh.read()
            got_byteranges.append(body)

        self.assertEqual(len(got_byteranges), 3)
        self.assertEqual(len(got_byteranges[0]), 501)
        self.assertEqual(got_byteranges[0], obj[:501])
        self.assertEqual(len(got_byteranges[1]), 501)
        self.assertEqual(got_byteranges[1], obj[1000:1501])
        self.assertEqual(len(got_byteranges[2]), 501)
        self.assertEqual(got_byteranges[2], obj[2000:2501])

        # Multi-range resume, first GET dies in the middle of the second
        # byterange
        bytes_before_timeout[0] = 750
        kaboomed[0] = 0
        sabotaged[0] = False
        prosrv._error_limiting = {}  # clear out errors
        with mock.patch.object(proxy_base,
                               'http_response_to_document_iters',
                               single_sabotage_hrtdi):
            req = Request.blank(
                path,
                environ={'REQUEST_METHOD': 'GET'},
                headers={'Range': 'bytes=0-500,1000-1500,2000-2500'})
            res = req.get_response(prosrv)
            body = ''.join(res.app_iter)

        self.assertEqual(res.status_int, 206)
        self.assertGreaterEqual(kaboomed[0], 1)  # sanity check

        ct, params = parse_content_type(res.headers['Content-Type'])
        self.assertEqual(ct, 'multipart/byteranges')  # sanity check
        boundary = dict(params).get('boundary')
        self.assertIsNotNone(boundary)  # sanity check
        got_byteranges = []
        for mime_doc_fh in iter_multipart_mime_documents(StringIO(body),
                                                         boundary):
            parse_mime_headers(mime_doc_fh)
            body = mime_doc_fh.read()
            got_byteranges.append(body)

        self.assertEqual(len(got_byteranges), 3)
        self.assertEqual(len(got_byteranges[0]), 501)
        self.assertEqual(got_byteranges[0], obj[:501])
        self.assertEqual(len(got_byteranges[1]), 501)
        self.assertEqual(got_byteranges[1], obj[1000:1501])
        self.assertEqual(len(got_byteranges[2]), 501)
        self.assertEqual(got_byteranges[2], obj[2000:2501])

    @unpatch_policies
    def test_conditional_GET_replication(self):
        policy = POLICIES[0]
        self.assertEqual('replication', policy.policy_type)  # sanity
        self._test_conditional_GET(policy)

    def test_PUT_expect_header_zero_content_length(self):
        test_errors = []

        def test_connect(ipaddr, port, device, partition, method, path,
                         headers=None, query_string=None):
            if path == '/a/c/o.jpg':
                if headers.get('Transfer-Encoding') != 'chunked':
                    test_errors.append('"Transfer-Encoding: chunked" should '
                                       'be in headers for object server!')
                if 'Expect' not in headers:
                    test_errors.append('Expect should be in headers for '
                                       'object server!')

        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            # The (201, Exception('test')) tuples in there have the effect of
            # changing the status of the initial expect response.  The default
            # expect response from FakeConn for 201 is 100.
            # But the object server won't send a 100 continue line if the
            # client doesn't send a expect 100 header (as is the case with
            # zero byte PUTs as validated by this test), nevertheless the
            # object controller calls getexpect without prejudice.  In this
            # case the status from the response shows up early in getexpect
            # instead of having to wait until getresponse.  The Exception is
            # in there to ensure that the object controller also *uses* the
            # result of getexpect instead of calling getresponse in which case
            # our FakeConn will blow up.
            success_codes = [(201, Exception('test'))] * 3
            set_http_connect(200, 200, *success_codes,
                             give_connect=test_connect)
            req = Request.blank('/v1/a/c/o.jpg', {})
            req.content_length = 0
            self.app.update_request(req)
            self.app.memcache.store = {}
            res = controller.PUT(req)
            self.assertEqual(test_errors, [])
            self.assertTrue(res.status.startswith('201 '), res.status)

    def test_PUT_expect_header_nonzero_content_length(self):
        test_errors = []

        def test_connect(ipaddr, port, device, partition, method, path,
                         headers=None, query_string=None):
            if path == '/a/c/o.jpg':
                if 'Expect' not in headers:
                    test_errors.append('Expect was not in headers for '
                                       'non-zero byte PUT!')

        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'a', 'c', 'o.jpg')
            # the (100, 201) tuples in there are just being extra explicit
            # about the FakeConn returning the 100 Continue status when the
            # object controller calls getexpect.  Which is FakeConn's default
            # for 201 if no expect_status is specified.
            success_codes = [(100, 201)] * 3
            set_http_connect(200, 200, *success_codes,
                             give_connect=test_connect)
            req = Request.blank('/v1/a/c/o.jpg', {})
            req.content_length = 1
            req.body = 'a'
            self.app.update_request(req)
            self.app.memcache.store = {}
            res = controller.PUT(req)
            self.assertEqual(test_errors, [])
            self.assertTrue(res.status.startswith('201 '))

    def _check_PUT_respects_write_affinity(self, conf, policy,
                                           expected_region):
        written_to = []

        def test_connect(ipaddr, port, device, partition, method, path,
                         headers=None, query_string=None):
            if path == '/a/c/o.jpg':
                written_to.append((ipaddr, port, device))

        # mock shuffle to be a no-op to ensure that the only way nodes would
        # not be used in ring order is if affinity is respected.
        with mock.patch('swift.proxy.server.shuffle', lambda x: x):
            app = proxy_server.Application(
                conf, FakeMemcache(),
                logger=debug_logger('proxy-ut'),
                account_ring=FakeRing(),
                container_ring=FakeRing())
            with save_globals():
                object_ring = app.get_object_ring(policy)
                object_ring.max_more_nodes = 100
                controller = \
                    ReplicatedObjectController(
                        app, 'a', 'c', 'o.jpg')
                # requests go to acc, con, obj, obj, obj
                set_http_connect(200, 200, 201, 201, 201,
                                 give_connect=test_connect)
                req = Request.blank(
                    '/v1/a/c/o.jpg', method='PUT', body='a',
                    headers={'X-Backend-Storage-Policy-Index': str(policy)})
                app.memcache.store = {}
                res = controller.PUT(req)
        self.assertTrue(res.status.startswith('201 '))
        self.assertEqual(3, len(written_to))
        for ip, port, device in written_to:
            # this is kind of a hokey test, but in FakeRing, the port is even
            # when the region is 0, and odd when the region is 1, so this test
            # asserts that we only wrote to nodes in region 0.
            self.assertEqual(expected_region, port % 2)

    @patch_policies([StoragePolicy(0, 'zero', True, object_ring=FakeRing()),
                     StoragePolicy(1, 'one', False, object_ring=FakeRing())])
    def test_PUT_respects_write_affinity(self):
        # nodes in fake ring order have r0z0, r1z1, r0z2
        # Check default conf via proxy server conf
        conf = {'write_affinity': 'r0'}
        self._check_PUT_respects_write_affinity(conf, 0, 0)

        # policy 0 and policy 1 have conf via per policy conf section
        conf = {
            'write_affinity': '',
            'policy_config': {
                '0': {'write_affinity': 'r0'},
                '1': {'write_affinity': 'r1'}
            }
        }
        self._check_PUT_respects_write_affinity(conf, 0, 0)
        self._check_PUT_respects_write_affinity(conf, 1, 1)

        # policy 0 conf via per policy conf section override proxy server conf,
        # policy 1 uses default
        conf = {
            'write_affinity': 'r0',
            'policy_config': {
                '0': {'write_affinity': 'r1'}
            }
        }
        self._check_PUT_respects_write_affinity(conf, 0, 1)
        self._check_PUT_respects_write_affinity(conf, 1, 0)

    def test_PUT_respects_write_affinity_with_507s(self):
        written_to = []

        def test_connect(ipaddr, port, device, partition, method, path,
                         headers=None, query_string=None):
            if path == '/a/c/o.jpg':
                written_to.append((ipaddr, port, device))

        with save_globals():
            def is_r0(node):
                return node['region'] == 0

            object_ring = self.app.get_object_ring(0)
            object_ring.max_more_nodes = 100
            policy_options = self.app.get_policy_options(POLICIES[0])
            policy_options.write_affinity_is_local_fn = is_r0
            policy_options.write_affinity_node_count_fn = lambda r: 3

            controller = \
                ReplicatedObjectController(
                    self.app, 'a', 'c', 'o.jpg')
            self.app.error_limit(
                object_ring.get_part_nodes(1)[0], 'test')
            set_http_connect(200, 200,        # account, container
                             201, 201, 201,   # 3 working backends
                             give_connect=test_connect)
            req = Request.blank('/v1/a/c/o.jpg', {})
            req.content_length = 1
            req.body = 'a'
            self.app.memcache.store = {}
            res = controller.PUT(req)
            self.assertTrue(res.status.startswith('201 '))

        # this is kind of a hokey test, but in FakeRing, the port is even when
        # the region is 0, and odd when the region is 1, so this test asserts
        # that we wrote to 2 nodes in region 0, then went to 1 non-r0 node.
        def get_region(x):
            return x[1] % 2  # it's (ip, port, device)

        self.assertEqual([0, 0, 1], [get_region(x) for x in written_to])

    @unpatch_policies
    def test_PUT_no_etag_fallocate(self):
        with mock.patch('swift.obj.diskfile.fallocate') as mock_fallocate:
            prolis = _test_sockets[0]
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            obj = 'hemoleucocytic-surfactant'
            fd.write('PUT /v1/a/c/o HTTP/1.1\r\n'
                     'Host: localhost\r\n'
                     'Connection: close\r\n'
                     'Content-Length: %d\r\n'
                     'X-Storage-Token: t\r\n'
                     'Content-Type: application/octet-stream\r\n'
                     '\r\n%s' % (len(obj), obj))
            fd.flush()
            headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)
        # one for each obj server; this test has 2
        self.assertEqual(len(mock_fallocate.mock_calls), 2)

    @unpatch_policies
    def test_PUT_message_length_using_content_length(self):
        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        obj = 'j' * 20
        fd.write('PUT /v1/a/c/o.content-length HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Length: %s\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 '\r\n%s' % (str(len(obj)), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

    @unpatch_policies
    def test_PUT_message_length_using_transfer_encoding(self):
        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/c/o.chunked HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 'Transfer-Encoding: chunked\r\n\r\n'
                 '2\r\n'
                 'oh\r\n'
                 '4\r\n'
                 ' say\r\n'
                 '4\r\n'
                 ' can\r\n'
                 '4\r\n'
                 ' you\r\n'
                 '4\r\n'
                 ' see\r\n'
                 '3\r\n'
                 ' by\r\n'
                 '4\r\n'
                 ' the\r\n'
                 '8\r\n'
                 ' dawns\'\n\r\n'
                 '0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

    @unpatch_policies
    def test_PUT_message_length_using_both(self):
        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/c/o.chunked HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 'Content-Length: 33\r\n'
                 'Transfer-Encoding: chunked\r\n\r\n'
                 '2\r\n'
                 'oh\r\n'
                 '4\r\n'
                 ' say\r\n'
                 '4\r\n'
                 ' can\r\n'
                 '4\r\n'
                 ' you\r\n'
                 '4\r\n'
                 ' see\r\n'
                 '3\r\n'
                 ' by\r\n'
                 '4\r\n'
                 ' the\r\n'
                 '8\r\n'
                 ' dawns\'\n\r\n'
                 '0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

    @unpatch_policies
    def test_PUT_bad_message_length(self):
        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/c/o.chunked HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 'Content-Length: 33\r\n'
                 'Transfer-Encoding: gzip\r\n\r\n'
                 '2\r\n'
                 'oh\r\n'
                 '4\r\n'
                 ' say\r\n'
                 '4\r\n'
                 ' can\r\n'
                 '4\r\n'
                 ' you\r\n'
                 '4\r\n'
                 ' see\r\n'
                 '3\r\n'
                 ' by\r\n'
                 '4\r\n'
                 ' the\r\n'
                 '8\r\n'
                 ' dawns\'\n\r\n'
                 '0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 400'
        self.assertEqual(headers[:len(exp)], exp)

    @unpatch_policies
    def test_PUT_message_length_unsup_xfr_encoding(self):
        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/c/o.chunked HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 'Content-Length: 33\r\n'
                 'Transfer-Encoding: gzip,chunked\r\n\r\n'
                 '2\r\n'
                 'oh\r\n'
                 '4\r\n'
                 ' say\r\n'
                 '4\r\n'
                 ' can\r\n'
                 '4\r\n'
                 ' you\r\n'
                 '4\r\n'
                 ' see\r\n'
                 '3\r\n'
                 ' by\r\n'
                 '4\r\n'
                 ' the\r\n'
                 '8\r\n'
                 ' dawns\'\n\r\n'
                 '0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 501'
        self.assertEqual(headers[:len(exp)], exp)

    @unpatch_policies
    def test_PUT_message_length_too_large(self):
        with mock.patch('swift.common.constraints.MAX_FILE_SIZE', 10):
            prolis = _test_sockets[0]
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('PUT /v1/a/c/o.chunked HTTP/1.1\r\n'
                     'Host: localhost\r\n'
                     'Connection: close\r\n'
                     'X-Storage-Token: t\r\n'
                     'Content-Type: application/octet-stream\r\n'
                     'Content-Length: 33\r\n\r\n'
                     'oh say can you see by the dawns\'\n')
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = 'HTTP/1.1 413'
            self.assertEqual(headers[:len(exp)], exp)

    @unpatch_policies
    def test_PUT_POST_last_modified(self):
        prolis = _test_sockets[0]

        def _do_HEAD():
            # do a HEAD to get reported last modified time
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('HEAD /v1/a/c/o.last_modified HTTP/1.1\r\n'
                     'Host: localhost\r\nConnection: close\r\n'
                     'X-Storage-Token: t\r\n\r\n')
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = 'HTTP/1.1 200'
            self.assertEqual(headers[:len(exp)], exp)
            last_modified_head = [line for line in headers.split('\r\n')
                                  if lm_hdr in line][0][len(lm_hdr):]
            return last_modified_head

        def _do_conditional_GET_checks(last_modified_time):
            # check If-(Un)Modified-Since GETs
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('GET /v1/a/c/o.last_modified HTTP/1.1\r\n'
                     'Host: localhost\r\nConnection: close\r\n'
                     'If-Modified-Since: %s\r\n'
                     'X-Storage-Token: t\r\n\r\n' % last_modified_time)
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = 'HTTP/1.1 304'
            self.assertEqual(headers[:len(exp)], exp)

            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('GET /v1/a/c/o.last_modified HTTP/1.1\r\n'
                     'Host: localhost\r\nConnection: close\r\n'
                     'If-Unmodified-Since: %s\r\n'
                     'X-Storage-Token: t\r\n\r\n' % last_modified_time)
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = 'HTTP/1.1 200'
            self.assertEqual(headers[:len(exp)], exp)

        # PUT the object
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/c/o.last_modified HTTP/1.1\r\n'
                 'Host: localhost\r\nConnection: close\r\n'
                 'X-Storage-Token: t\r\nContent-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        lm_hdr = 'Last-Modified: '
        self.assertEqual(headers[:len(exp)], exp)

        last_modified_put = [line for line in headers.split('\r\n')
                             if lm_hdr in line][0][len(lm_hdr):]

        last_modified_head = _do_HEAD()
        self.assertEqual(last_modified_put, last_modified_head)

        _do_conditional_GET_checks(last_modified_put)

        # now POST to the object
        # last-modified rounded in sec so sleep a sec to increment
        sleep(1)

        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('POST /v1/a/c/o.last_modified HTTP/1.1\r\n'
                 'Host: localhost\r\nConnection: close\r\n'
                 'X-Storage-Token: t\r\nContent-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 202'
        self.assertEqual(headers[:len(exp)], exp)
        for line in headers.split('\r\n'):
            self.assertFalse(line.startswith(lm_hdr))

        # last modified time will have changed due to POST
        last_modified_head = _do_HEAD()
        self.assertNotEqual(last_modified_put, last_modified_head)
        _do_conditional_GET_checks(last_modified_head)

    @unpatch_policies
    def test_PUT_auto_content_type(self):
        prolis = _test_sockets[0]

        def do_test(ext, content_type):
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile('rwb')
            fd.write(b'PUT /v1/a/c/o.%s HTTP/1.1\r\n'
                     b'Host: localhost\r\n'
                     b'X-Storage-Token: t\r\nContent-Length: 0\r\n\r\n' %
                     ext.encode())
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = b'HTTP/1.1 201'
            self.assertEqual(headers[:len(exp)], exp)

            fd.write(b'GET /v1/a/c/o.%s HTTP/1.1\r\n'
                     b'Host: localhost\r\nConnection: close\r\n'
                     b'X-Storage-Token: t\r\n\r\n' % ext.encode())
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = b'HTTP/1.1 200'
            self.assertIn(b'Content-Type: %s' % content_type.encode(),
                          headers.split(b'\r\n'))
            sock.close()

        do_test('jpg', 'image/jpeg')
        do_test('html', 'text/html')
        do_test('css', 'text/css')

    def test_custom_mime_types_files(self):
        swift_dir = mkdtemp()
        try:
            with open(os.path.join(swift_dir, 'mime.types'), 'w') as fp:
                fp.write('foo/bar foo\n')
            proxy_server.Application({'swift_dir': swift_dir},
                                     FakeMemcache(), FakeLogger(),
                                     FakeRing(), FakeRing())
            self.assertEqual(proxy_server.mimetypes.guess_type('blah.foo')[0],
                             'foo/bar')
            self.assertEqual(proxy_server.mimetypes.guess_type('blah.jpg')[0],
                             'image/jpeg')
        finally:
            rmtree(swift_dir, ignore_errors=True)

    def test_PUT(self):
        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')

            def test_status_map(statuses, expected):
                set_http_connect(*statuses)
                req = Request.blank('/v1/a/c/o.jpg', {})
                req.content_length = 0
                self.app.update_request(req)
                self.app.memcache.store = {}
                res = controller.PUT(req)
                expected = str(expected)
                self.assertEqual(res.status[:len(expected)], expected)
            test_status_map((200, 200, 201, 201, 201), 201)
            test_status_map((200, 200, 201, 201, 500), 201)
            test_status_map((200, 200, 204, 404, 404), 404)
            test_status_map((200, 200, 204, 500, 404), 503)
            test_status_map((200, 200, 202, 202, 204), 204)

    def test_PUT_connect_exceptions(self):
        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')

            def test_status_map(statuses, expected):
                set_http_connect(*statuses)
                self.app.memcache.store = {}
                req = Request.blank('/v1/a/c/o.jpg', {})
                req.content_length = 0
                self.app.update_request(req)
                try:
                    res = controller.PUT(req)
                except HTTPException as res:
                    pass
                expected = str(expected)
                self.assertEqual(res.status[:len(expected)], expected)
            test_status_map((200, 200, 201, 201, -1), 201)  # connect exc
            # connect errors
            test_status_map((200, 200, Timeout(), 201, 201, ), 201)
            test_status_map((200, 200, 201, 201, Exception()), 201)
            # expect errors
            test_status_map((200, 200, (Timeout(), None), 201, 201), 201)
            test_status_map((200, 200, (Exception(), None), 201, 201), 201)
            # response errors
            test_status_map((200, 200, (100, Timeout()), 201, 201), 201)
            test_status_map((200, 200, (100, Exception()), 201, 201), 201)
            test_status_map((200, 200, 507, 201, 201), 201)  # error limited
            test_status_map((200, 200, -1, 201, -1), 503)
            test_status_map((200, 200, 503, -1, 503), 503)

    def test_PUT_send_exceptions(self):
        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')

            def test_status_map(statuses, expected):
                self.app.memcache.store = {}
                set_http_connect(*statuses)
                req = Request.blank('/v1/a/c/o.jpg',
                                    environ={'REQUEST_METHOD': 'PUT'},
                                    body='some data')
                self.app.update_request(req)
                try:
                    res = controller.PUT(req)
                except HTTPException as res:
                    pass
                expected = str(expected)
                self.assertEqual(res.status[:len(expected)], expected)
            test_status_map((200, 200, 201, -1, 201), 201)
            test_status_map((200, 200, 201, -1, -1), 503)
            test_status_map((200, 200, 503, 503, -1), 503)

    def test_PUT_max_size(self):
        with save_globals():
            set_http_connect(201, 201, 201)
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            req = Request.blank('/v1/a/c/o', {}, headers={
                'Content-Length': str(constraints.MAX_FILE_SIZE + 1),
                'Content-Type': 'foo/bar'})
            self.app.update_request(req)
            res = controller.PUT(req)
            self.assertEqual(res.status_int, 413)

    def test_PUT_bad_content_type(self):
        with save_globals():
            set_http_connect(201, 201, 201)
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            req = Request.blank('/v1/a/c/o', {}, headers={
                'Content-Length': 0, 'Content-Type': 'foo/bar;swift_hey=45'})
            self.app.update_request(req)
            res = controller.PUT(req)
            self.assertEqual(res.status_int, 400)

    def test_PUT_getresponse_exceptions(self):

        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')

            def test_status_map(statuses, expected):
                self.app.memcache.store = {}
                set_http_connect(*statuses)
                req = Request.blank('/v1/a/c/o.jpg', {})
                req.content_length = 0
                self.app.update_request(req)
                try:
                    res = controller.PUT(req)
                except HTTPException as res:
                    pass
                expected = str(expected)
                self.assertEqual(res.status[:len(str(expected))],
                                 str(expected))
            test_status_map((200, 200, 201, 201, -1), 201)
            test_status_map((200, 200, 201, -1, -1), 503)
            test_status_map((200, 200, 503, 503, -1), 503)

    def test_POST(self):
        with save_globals():
            def test_status_map(statuses, expected):
                set_http_connect(*statuses)
                self.app.memcache.store = {}
                req = Request.blank('/v1/a/c/o', {}, method='POST',
                                    headers={'Content-Type': 'foo/bar'})
                self.app.update_request(req)
                res = req.get_response(self.app)
                expected = str(expected)
                self.assertEqual(res.status[:len(expected)], expected)
            test_status_map((200, 200, 202, 202, 202), 202)
            test_status_map((200, 200, 202, 202, 500), 202)
            test_status_map((200, 200, 202, 500, 500), 503)
            test_status_map((200, 200, 202, 404, 500), 503)
            test_status_map((200, 200, 202, 404, 404), 404)
            test_status_map((200, 200, 404, 500, 500), 503)
            test_status_map((200, 200, 404, 404, 404), 404)

    @patch_policies([
        StoragePolicy(0, 'zero', is_default=True, object_ring=FakeRing()),
        StoragePolicy(1, 'one', object_ring=FakeRing()),
    ])
    def test_POST_backend_headers(self):
        # reset the router post patch_policies
        self.app.obj_controller_router = proxy_server.ObjectControllerRouter()
        self.app.sort_nodes = lambda nodes, *args, **kwargs: nodes

        def do_test(resp_headers):
            self.app.memcache.store = {}
            backend_requests = []

            def capture_requests(ip, port, method, path, headers, *args,
                                 **kwargs):
                backend_requests.append((method, path, headers))

            req = Request.blank('/v1/a/c/o', {}, method='POST',
                                headers={'X-Object-Meta-Color': 'Blue',
                                         'Content-Type': 'text/plain'})

            # we want the container_info response to says a policy index of 1
            with mocked_http_conn(
                    200, 200, 202, 202, 202,
                    headers=resp_headers, give_connect=capture_requests
            ) as fake_conn:
                resp = req.get_response(self.app)
                self.assertRaises(StopIteration, fake_conn.code_iter.next)

            self.assertEqual(resp.status_int, 202)
            self.assertEqual(len(backend_requests), 5)

            def check_request(req, method, path, headers=None):
                req_method, req_path, req_headers = req
                self.assertEqual(method, req_method)
                # caller can ignore leading path parts
                self.assertTrue(req_path.endswith(path),
                                'expected path to end with %s, it was %s' % (
                                    path, req_path))
                headers = headers or {}
                # caller can ignore some headers
                for k, v in headers.items():
                    self.assertEqual(req_headers[k], v)
                self.assertNotIn('X-Backend-Container-Path', req_headers)

            account_request = backend_requests.pop(0)
            check_request(account_request, method='HEAD', path='/sda/0/a')
            container_request = backend_requests.pop(0)
            check_request(container_request, method='HEAD', path='/sda/0/a/c')
            # make sure backend requests included expected container headers
            container_headers = {}
            for request in backend_requests:
                req_headers = request[2]
                device = req_headers['x-container-device']
                host = req_headers['x-container-host']
                container_headers[device] = host
                expectations = {
                    'method': 'POST',
                    'path': '/0/a/c/o',
                    'headers': {
                        'X-Container-Partition': '0',
                        'Connection': 'close',
                        'User-Agent': 'proxy-server %s' % os.getpid(),
                        'Host': 'localhost:80',
                        'Referer': 'POST http://localhost/v1/a/c/o',
                        'X-Object-Meta-Color': 'Blue',
                        'X-Backend-Storage-Policy-Index': '1'
                    },
                }
                check_request(request, **expectations)

            expected = {}
            for i, device in enumerate(['sda', 'sdb', 'sdc']):
                expected[device] = '10.0.0.%d:100%d' % (i, i)
            self.assertEqual(container_headers, expected)

            # and again with policy override
            self.app.memcache.store = {}
            backend_requests = []
            req = Request.blank('/v1/a/c/o', {}, method='POST',
                                headers={'X-Object-Meta-Color': 'Blue',
                                         'Content-Type': 'text/plain',
                                         'X-Backend-Storage-Policy-Index': 0})
            with mocked_http_conn(
                    200, 200, 202, 202, 202,
                    headers=resp_headers, give_connect=capture_requests
            ) as fake_conn:
                resp = req.get_response(self.app)
                self.assertRaises(StopIteration, fake_conn.code_iter.next)
            self.assertEqual(resp.status_int, 202)
            self.assertEqual(len(backend_requests), 5)
            for request in backend_requests[2:]:
                expectations = {
                    'method': 'POST',
                    'path': '/0/a/c/o',  # ignore device bit
                    'headers': {
                        'X-Object-Meta-Color': 'Blue',
                        'X-Backend-Storage-Policy-Index': '0',
                    }
                }
                check_request(request, **expectations)

        resp_headers = {'X-Backend-Storage-Policy-Index': 1}
        do_test(resp_headers)
        resp_headers['X-Backend-Sharding-State'] = 'unsharded'
        do_test(resp_headers)

    @patch_policies([
        StoragePolicy(0, 'zero', is_default=True, object_ring=FakeRing()),
        StoragePolicy(1, 'one', object_ring=FakeRing()),
    ])
    def test_backend_headers_update_shard_container(self):
        # verify that when container is sharded the backend container update is
        # directed to the shard container
        # reset the router post patch_policies
        self.app.obj_controller_router = proxy_server.ObjectControllerRouter()
        self.app.sort_nodes = lambda nodes, *args, **kwargs: nodes

        def do_test(method, sharding_state):
            self.app.memcache.store = {}
            req = Request.blank('/v1/a/c/o', {}, method=method, body='',
                                headers={'Content-Type': 'text/plain'})

            # we want the container_info response to say policy index of 1 and
            # sharding state
            # acc HEAD, cont HEAD, cont shard GET, obj POSTs
            status_codes = (200, 200, 200, 202, 202, 202)
            resp_headers = {'X-Backend-Storage-Policy-Index': 1,
                            'x-backend-sharding-state': sharding_state,
                            'X-Backend-Record-Type': 'shard'}
            shard_range = utils.ShardRange(
                '.shards_a/c_shard', utils.Timestamp.now(), 'l', 'u')
            body = json.dumps([dict(shard_range)])
            with mocked_http_conn(*status_codes, headers=resp_headers,
                                  body=body) as fake_conn:
                resp = req.get_response(self.app)

            self.assertEqual(resp.status_int, 202)
            backend_requests = fake_conn.requests

            def check_request(req, method, path, headers=None, params=None):
                self.assertEqual(method, req['method'])
                # caller can ignore leading path parts
                self.assertTrue(req['path'].endswith(path),
                                'expected path to end with %s, it was %s' % (
                                    path, req['path']))
                headers = headers or {}
                # caller can ignore some headers
                for k, v in headers.items():
                    self.assertEqual(req['headers'][k], v,
                                     'Expected %s but got %s for key %s' %
                                     (v, req['headers'][k], k))
                params = params or {}
                req_params = dict(parse_qsl(req['qs'])) if req['qs'] else {}
                for k, v in params.items():
                    self.assertEqual(req_params[k], v,
                                     'Expected %s but got %s for key %s' %
                                     (v, req_params[k], k))

            account_request = backend_requests[0]
            check_request(account_request, method='HEAD', path='/sda/0/a')
            container_request = backend_requests[1]
            check_request(container_request, method='HEAD', path='/sda/0/a/c')
            container_request_shard = backend_requests[2]
            check_request(
                container_request_shard, method='GET', path='/sda/0/a/c',
                params={'includes': 'o'})

            # make sure backend requests included expected container headers
            container_headers = {}

            for request in backend_requests[3:]:
                req_headers = request['headers']
                device = req_headers['x-container-device']
                container_headers[device] = req_headers['x-container-host']
                expectations = {
                    'method': method,
                    'path': '/0/a/c/o',
                    'headers': {
                        'X-Container-Partition': '0',
                        'Host': 'localhost:80',
                        'Referer': '%s http://localhost/v1/a/c/o' % method,
                        'X-Backend-Storage-Policy-Index': '1',
                        'X-Backend-Container-Path': shard_range.name
                    },
                }
                check_request(request, **expectations)

            expected = {}
            for i, device in enumerate(['sda', 'sdb', 'sdc']):
                expected[device] = '10.0.0.%d:100%d' % (i, i)
            self.assertEqual(container_headers, expected)

        do_test('POST', 'sharding')
        do_test('POST', 'sharded')
        do_test('DELETE', 'sharding')
        do_test('DELETE', 'sharded')
        do_test('PUT', 'sharding')
        do_test('PUT', 'sharded')

    def test_DELETE(self):
        with save_globals():
            def test_status_map(statuses, expected):
                set_http_connect(*statuses)
                self.app.memcache.store = {}
                req = Request.blank('/v1/a/c/o', {'REQUEST_METHOD': 'DELETE'})
                self.app.update_request(req)
                res = req.get_response(self.app)
                self.assertEqual(res.status[:len(str(expected))],
                                 str(expected))
            test_status_map((200, 200, 204, 204, 204), 204)
            test_status_map((200, 200, 204, 204, 500), 204)
            test_status_map((200, 200, 204, 404, 404), 404)
            test_status_map((200, 204, 500, 500, 404), 503)
            test_status_map((200, 200, 404, 404, 404), 404)
            test_status_map((200, 200, 400, 400, 400), 400)

    def test_HEAD(self):
        with save_globals():
            def test_status_map(statuses, expected):
                set_http_connect(*statuses)
                self.app.memcache.store = {}
                req = Request.blank('/v1/a/c/o', {'REQUEST_METHOD': 'HEAD'})
                self.app.update_request(req)
                res = req.get_response(self.app)
                self.assertEqual(res.status[:len(str(expected))],
                                 str(expected))
                if expected < 400:
                    self.assertIn('x-works', res.headers)
                    self.assertEqual(res.headers['x-works'], 'yes')
                    self.assertIn('accept-ranges', res.headers)
                    self.assertEqual(res.headers['accept-ranges'], 'bytes')

            test_status_map((200, 200, 200, 404, 404), 200)
            test_status_map((200, 200, 200, 500, 404), 200)
            test_status_map((200, 200, 304, 500, 404), 304)
            test_status_map((200, 200, 404, 404, 404), 404)
            test_status_map((200, 200, 404, 404, 500), 404)
            test_status_map((200, 200, 500, 500, 500), 503)

    def test_HEAD_newest(self):
        with save_globals():
            def test_status_map(statuses, expected, timestamps,
                                expected_timestamp):
                set_http_connect(*statuses, timestamps=timestamps)
                self.app.memcache.store = {}
                req = Request.blank('/v1/a/c/o', {'REQUEST_METHOD': 'HEAD'},
                                    headers={'x-newest': 'true'})
                self.app.update_request(req)
                res = req.get_response(self.app)
                self.assertEqual(res.status[:len(str(expected))],
                                 str(expected))
                self.assertEqual(res.headers.get('last-modified'),
                                 expected_timestamp)

            #                acct cont obj  obj  obj
            test_status_map((200, 200, 200, 200, 200), 200, ('0', '0', '1',
                                                             '2', '3'), '3')
            test_status_map((200, 200, 200, 200, 200), 200, ('0', '0', '1',
                                                             '3', '2'), '3')
            test_status_map((200, 200, 200, 200, 200), 200, ('0', '0', '1',
                                                             '3', '1'), '3')
            test_status_map((200, 200, 200, 200, 200), 200, ('0', '0', '3',
                                                             '3', '1'), '3')
            test_status_map((200, 200, 200, 200, 200), 200, ('0', '0', None,
                                                             None, None), None)
            test_status_map((200, 200, 200, 200, 200), 200, ('0', '0', None,
                                                             None, '1'), '1')
            test_status_map((200, 200, 404, 404, 200), 200, ('0', '0', None,
                                                             None, '1'), '1')

    def test_GET_newest(self):
        with save_globals():
            def test_status_map(statuses, expected, timestamps,
                                expected_timestamp):
                set_http_connect(*statuses, timestamps=timestamps)
                self.app.memcache.store = {}
                req = Request.blank('/v1/a/c/o', {'REQUEST_METHOD': 'GET'},
                                    headers={'x-newest': 'true'})
                self.app.update_request(req)
                res = req.get_response(self.app)
                self.assertEqual(res.status[:len(str(expected))],
                                 str(expected))
                self.assertEqual(res.headers.get('last-modified'),
                                 expected_timestamp)

            test_status_map((200, 200, 200, 200, 200), 200, ('0', '0', '1',
                                                             '2', '3'), '3')
            test_status_map((200, 200, 200, 200, 200), 200, ('0', '0', '1',
                                                             '3', '2'), '3')
            test_status_map((200, 200, 200, 200, 200), 200, ('0', '0', '1',
                                                             '3', '1'), '3')
            test_status_map((200, 200, 200, 200, 200), 200, ('0', '0', '3',
                                                             '3', '1'), '3')
            test_status_map((200, 200, 200, 200, 200), 200, ('0', '0', None,
                                                             None, None), None)
            test_status_map((200, 200, 200, 200, 200), 200, ('0', '0', None,
                                                             None, '1'), '1')

        with save_globals():
            def test_status_map(statuses, expected, timestamps,
                                expected_timestamp):
                set_http_connect(*statuses, timestamps=timestamps)
                self.app.memcache.store = {}
                req = Request.blank('/v1/a/c/o', {'REQUEST_METHOD': 'HEAD'})
                self.app.update_request(req)
                res = req.get_response(self.app)
                self.assertEqual(res.status[:len(str(expected))],
                                 str(expected))
                self.assertEqual(res.headers.get('last-modified'),
                                 expected_timestamp)

            test_status_map((200, 200, 200, 200, 200), 200, ('0', '0', '1',
                                                             '2', '3'), '1')
            test_status_map((200, 200, 200, 200, 200), 200, ('0', '0', '1',
                                                             '3', '2'), '1')
            test_status_map((200, 200, 200, 200, 200), 200, ('0', '0', '1',
                                                             '3', '1'), '1')
            test_status_map((200, 200, 200, 200, 200), 200, ('0', '0', '3',
                                                             '3', '1'), '3')
            test_status_map((200, 200, 200, 200, 200), 200, ('0', '0', None,
                                                             '1', '2'), None)

    def test_POST_meta_val_len(self):
        with save_globals():
            limit = constraints.MAX_META_VALUE_LENGTH
            ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            set_http_connect(200, 200, 202, 202, 202)
            #                acct cont obj  obj  obj
            req = Request.blank('/v1/a/c/o', {'REQUEST_METHOD': 'POST'},
                                headers={'Content-Type': 'foo/bar',
                                         'X-Object-Meta-Foo': 'x' * limit})
            self.app.update_request(req)
            res = req.get_response(self.app)
            self.assertEqual(res.status_int, 202)
            set_http_connect(202, 202, 202)
            req = Request.blank(
                '/v1/a/c/o', {'REQUEST_METHOD': 'POST'},
                headers={'Content-Type': 'foo/bar',
                         'X-Object-Meta-Foo': 'x' * (limit + 1)})
            self.app.update_request(req)
            res = req.get_response(self.app)
            self.assertEqual(res.status_int, 400)

    def test_POST_meta_authorize(self):
        def authorize(req):
            req.headers['X-Object-Meta-Foo'] = 'x' * (limit + 1)
            return
        with save_globals():
            limit = constraints.MAX_META_VALUE_LENGTH
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            set_http_connect(200, 200, 202, 202, 202)
            #                acct cont obj  obj  obj
            req = Request.blank('/v1/a/c/o', {'REQUEST_METHOD': 'POST'},
                                headers={'Content-Type': 'foo/bar',
                                         'X-Object-Meta-Foo': 'x'})
            req.environ['swift.authorize'] = authorize
            self.app.update_request(req)
            res = controller.POST(req)
            self.assertEqual(res.status_int, 400)

    def test_POST_meta_key_len(self):
        with save_globals():
            limit = constraints.MAX_META_NAME_LENGTH
            set_http_connect(200, 200, 202, 202, 202)
            #                acct cont obj  obj  obj
            req = Request.blank(
                '/v1/a/c/o', {'REQUEST_METHOD': 'POST'},
                headers={'Content-Type': 'foo/bar',
                         ('X-Object-Meta-' + 'x' * limit): 'x'})
            self.app.update_request(req)
            res = req.get_response(self.app)
            self.assertEqual(res.status_int, 202)
            set_http_connect(202, 202, 202)
            req = Request.blank(
                '/v1/a/c/o', {'REQUEST_METHOD': 'POST'},
                headers={'Content-Type': 'foo/bar',
                         ('X-Object-Meta-' + 'x' * (limit + 1)): 'x'})
            self.app.update_request(req)
            res = req.get_response(self.app)
            self.assertEqual(res.status_int, 400)

    def test_POST_meta_count(self):
        with save_globals():
            limit = constraints.MAX_META_COUNT
            headers = dict(
                (('X-Object-Meta-' + str(i), 'a') for i in range(limit + 1)))
            headers.update({'Content-Type': 'foo/bar'})
            set_http_connect(202, 202, 202)
            req = Request.blank('/v1/a/c/o', {'REQUEST_METHOD': 'POST'},
                                headers=headers)
            self.app.update_request(req)
            res = req.get_response(self.app)
            self.assertEqual(res.status_int, 400)

    def test_POST_meta_size(self):
        with save_globals():
            limit = constraints.MAX_META_OVERALL_SIZE
            count = limit / 256  # enough to cause the limit to be reached
            headers = dict(
                (('X-Object-Meta-' + str(i), 'a' * 256)
                    for i in range(count + 1)))
            headers.update({'Content-Type': 'foo/bar'})
            set_http_connect(202, 202, 202)
            req = Request.blank('/v1/a/c/o', {'REQUEST_METHOD': 'POST'},
                                headers=headers)
            self.app.update_request(req)
            res = req.get_response(self.app)
            self.assertEqual(res.status_int, 400)

    def test_PUT_not_autodetect_content_type(self):
        with save_globals():
            headers = {'Content-Type': 'something/right', 'Content-Length': 0}
            it_worked = []

            def verify_content_type(ipaddr, port, device, partition,
                                    method, path, headers=None,
                                    query_string=None):
                if path == '/a/c/o.html':
                    it_worked.append(
                        headers['Content-Type'].startswith('something/right'))

            set_http_connect(204, 204, 201, 201, 201,
                             give_connect=verify_content_type)
            req = Request.blank('/v1/a/c/o.html', {'REQUEST_METHOD': 'PUT'},
                                headers=headers)
            self.app.update_request(req)
            req.get_response(self.app)
            self.assertNotEqual(it_worked, [])
            self.assertTrue(all(it_worked))

    def test_PUT_autodetect_content_type(self):
        with save_globals():
            headers = {'Content-Type': 'something/wrong', 'Content-Length': 0,
                       'X-Detect-Content-Type': 'True'}
            it_worked = []

            def verify_content_type(ipaddr, port, device, partition,
                                    method, path, headers=None,
                                    query_string=None):
                if path == '/a/c/o.html':
                    it_worked.append(
                        headers['Content-Type'].startswith('text/html'))

            set_http_connect(204, 204, 201, 201, 201,
                             give_connect=verify_content_type)
            req = Request.blank('/v1/a/c/o.html', {'REQUEST_METHOD': 'PUT'},
                                headers=headers)
            self.app.update_request(req)
            req.get_response(self.app)
            self.assertNotEqual(it_worked, [])
            self.assertTrue(all(it_worked))

    def test_client_timeout(self):
        with save_globals():
            self.app.account_ring.get_nodes('account')
            for dev in self.app.account_ring.devs:
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            self.app.container_ring.get_nodes('account')
            for dev in self.app.container_ring.devs:
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            object_ring = self.app.get_object_ring(None)
            object_ring.get_nodes('account')
            for dev in object_ring.devs:
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1

            class SlowBody(object):

                def __init__(self):
                    self.sent = 0

                def read(self, size=-1):
                    if self.sent < 4:
                        sleep(0.1)
                        self.sent += 1
                        return ' '
                    return ''

            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'PUT',
                                         'wsgi.input': SlowBody()},
                                headers={'Content-Length': '4',
                                         'Content-Type': 'text/plain'})
            self.app.update_request(req)
            set_http_connect(200, 200, 201, 201, 201)
            #                acct cont obj  obj  obj
            resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, 201)
            self.app.client_timeout = 0.05
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'PUT',
                                         'wsgi.input': SlowBody()},
                                headers={'Content-Length': '4',
                                         'Content-Type': 'text/plain'})
            self.app.update_request(req)
            set_http_connect(201, 201, 201)
            #                obj  obj  obj
            resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, 408)

    def test_client_disconnect(self):
        with save_globals():
            self.app.account_ring.get_nodes('account')
            for dev in self.app.account_ring.devs:
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            self.app.container_ring.get_nodes('account')
            for dev in self.app.container_ring.devs:
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            object_ring = self.app.get_object_ring(None)
            object_ring.get_nodes('account')
            for dev in object_ring.devs:
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1

            class DisconnectedBody(object):

                def __init__(self):
                    self.sent = 0

                def read(self, size=-1):
                    return ''

            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'PUT',
                                         'wsgi.input': DisconnectedBody()},
                                headers={'Content-Length': '4',
                                         'Content-Type': 'text/plain'})
            self.app.update_request(req)
            set_http_connect(200, 200, 201, 201, 201)
            #                acct cont obj  obj  obj
            resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, 499)

    def test_node_read_timeout(self):
        with save_globals():
            self.app.account_ring.get_nodes('account')
            for dev in self.app.account_ring.devs:
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            self.app.container_ring.get_nodes('account')
            for dev in self.app.container_ring.devs:
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            object_ring = self.app.get_object_ring(None)
            object_ring.get_nodes('account')
            for dev in object_ring.devs:
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
            self.app.update_request(req)
            set_http_connect(200, 200, 200, slow=0.1)
            req.sent_size = 0
            resp = req.get_response(self.app)
            got_exc = False
            try:
                resp.body
            except ChunkReadTimeout:
                got_exc = True
            self.assertFalse(got_exc)
            self.app.recoverable_node_timeout = 0.1
            set_http_connect(200, 200, 200, slow=1.0)
            resp = req.get_response(self.app)
            with self.assertRaises(ChunkReadTimeout):
                resp.body

    def test_node_read_timeout_retry(self):
        with save_globals():
            self.app.account_ring.get_nodes('account')
            for dev in self.app.account_ring.devs:
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            self.app.container_ring.get_nodes('account')
            for dev in self.app.container_ring.devs:
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            object_ring = self.app.get_object_ring(None)
            object_ring.get_nodes('account')
            for dev in object_ring.devs:
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
            self.app.update_request(req)

            self.app.recoverable_node_timeout = 0.1
            set_http_connect(200, 200, 200, slow=[1.0, 1.0, 1.0])
            resp = req.get_response(self.app)
            with self.assertRaises(ChunkReadTimeout):
                resp.body

            set_http_connect(200, 200, 200, body='lalala',
                             slow=[1.0, 1.0])
            resp = req.get_response(self.app)
            self.assertEqual(resp.body, 'lalala')

            set_http_connect(200, 200, 200, body='lalala',
                             slow=[1.0, 1.0], etags=['a', 'a', 'a'])
            resp = req.get_response(self.app)
            self.assertEqual(resp.body, 'lalala')

            set_http_connect(200, 200, 200, body='lalala',
                             slow=[1.0, 1.0], etags=['a', 'b', 'a'])
            resp = req.get_response(self.app)
            self.assertEqual(resp.body, 'lalala')

            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
            set_http_connect(200, 200, 200, body='lalala',
                             slow=[1.0, 1.0], etags=['a', 'b', 'b'])
            resp = req.get_response(self.app)
            with self.assertRaises(ChunkReadTimeout):
                resp.body

    def test_node_write_timeout(self):
        with save_globals():
            self.app.account_ring.get_nodes('account')
            for dev in self.app.account_ring.devs:
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            self.app.container_ring.get_nodes('account')
            for dev in self.app.container_ring.devs:
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            object_ring = self.app.get_object_ring(None)
            object_ring.get_nodes('account')
            for dev in object_ring.devs:
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '4',
                                         'Content-Type': 'text/plain'},
                                body='    ')
            self.app.update_request(req)
            set_http_connect(200, 200, 201, 201, 201, slow=0.1)
            resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, 201)
            self.app.node_timeout = 0.1
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '4',
                                         'Content-Type': 'text/plain'},
                                body='    ')
            self.app.update_request(req)
            set_http_connect(201, 201, 201, slow=1.0)
            resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, 503)

    def test_node_request_setting(self):
        baseapp = proxy_server.Application({'request_node_count': '3'},
                                           FakeMemcache(),
                                           container_ring=FakeRing(),
                                           account_ring=FakeRing())
        self.assertEqual(baseapp.request_node_count(3), 3)

    def test_iter_nodes(self):
        with save_globals():
            try:
                object_ring = self.app.get_object_ring(None)
                object_ring.max_more_nodes = 2
                partition, nodes = object_ring.get_nodes('account',
                                                         'container',
                                                         'object')
                collected_nodes = []
                for node in self.app.iter_nodes(object_ring,
                                                partition):
                    collected_nodes.append(node)
                self.assertEqual(len(collected_nodes), 5)

                object_ring.max_more_nodes = 6
                self.app.request_node_count = lambda r: 20
                partition, nodes = object_ring.get_nodes('account',
                                                         'container',
                                                         'object')
                collected_nodes = []
                for node in self.app.iter_nodes(object_ring,
                                                partition):
                    collected_nodes.append(node)
                self.assertEqual(len(collected_nodes), 9)

                # zero error-limited primary nodes -> no handoff warnings
                self.app.log_handoffs = True
                self.app.logger = FakeLogger()
                self.app.request_node_count = lambda r: 7
                object_ring.max_more_nodes = 20
                partition, nodes = object_ring.get_nodes('account',
                                                         'container',
                                                         'object')
                collected_nodes = []
                for node in self.app.iter_nodes(object_ring, partition):
                    collected_nodes.append(node)
                self.assertEqual(len(collected_nodes), 7)
                self.assertEqual(self.app.logger.log_dict['warning'], [])
                self.assertEqual(self.app.logger.get_increments(), [])

                # one error-limited primary node -> one handoff warning
                self.app.log_handoffs = True
                self.app.logger = FakeLogger()
                self.app.request_node_count = lambda r: 7
                self.app._error_limiting = {}  # clear out errors
                set_node_errors(self.app, object_ring._devs[0], 999,
                                last_error=(2 ** 63 - 1))

                collected_nodes = []
                for node in self.app.iter_nodes(object_ring, partition):
                    collected_nodes.append(node)
                self.assertEqual(len(collected_nodes), 7)
                self.assertEqual(self.app.logger.log_dict['warning'], [
                    (('Handoff requested (5)',), {})])
                self.assertEqual(self.app.logger.get_increments(),
                                 ['handoff_count'])

                # two error-limited primary nodes -> two handoff warnings
                self.app.log_handoffs = True
                self.app.logger = FakeLogger()
                self.app.request_node_count = lambda r: 7
                self.app._error_limiting = {}  # clear out errors
                for i in range(2):
                    set_node_errors(self.app, object_ring._devs[i], 999,
                                    last_error=(2 ** 63 - 1))

                collected_nodes = []
                for node in self.app.iter_nodes(object_ring, partition):
                    collected_nodes.append(node)
                self.assertEqual(len(collected_nodes), 7)
                self.assertEqual(self.app.logger.log_dict['warning'], [
                    (('Handoff requested (5)',), {}),
                    (('Handoff requested (6)',), {})])
                self.assertEqual(self.app.logger.get_increments(),
                                 ['handoff_count',
                                  'handoff_count'])

                # all error-limited primary nodes -> four handoff warnings,
                # plus a handoff-all metric
                self.app.log_handoffs = True
                self.app.logger = FakeLogger()
                self.app.request_node_count = lambda r: 10
                object_ring.set_replicas(4)  # otherwise we run out of handoffs
                self.app._error_limiting = {}  # clear out errors
                for i in range(4):
                    set_node_errors(self.app, object_ring._devs[i], 999,
                                    last_error=(2 ** 63 - 1))

                collected_nodes = []
                for node in self.app.iter_nodes(object_ring, partition):
                    collected_nodes.append(node)
                self.assertEqual(len(collected_nodes), 10)
                self.assertEqual(self.app.logger.log_dict['warning'], [
                    (('Handoff requested (7)',), {}),
                    (('Handoff requested (8)',), {}),
                    (('Handoff requested (9)',), {}),
                    (('Handoff requested (10)',), {})])
                self.assertEqual(self.app.logger.get_increments(),
                                 ['handoff_count',
                                  'handoff_count',
                                  'handoff_count',
                                  'handoff_count',
                                  'handoff_all_count'])

            finally:
                object_ring.max_more_nodes = 0

    def test_iter_nodes_calls_sort_nodes(self):
        with mock.patch.object(self.app, 'sort_nodes') as sort_nodes:
            object_ring = self.app.get_object_ring(None)
            for node in self.app.iter_nodes(object_ring, 0):
                pass
            sort_nodes.assert_called_once_with(
                object_ring.get_part_nodes(0), policy=None)

    def test_iter_nodes_skips_error_limited(self):
        with mock.patch.object(self.app, 'sort_nodes',
                               lambda n, *args, **kwargs: n):
            object_ring = self.app.get_object_ring(None)
            first_nodes = list(self.app.iter_nodes(object_ring, 0))
            second_nodes = list(self.app.iter_nodes(object_ring, 0))
            self.assertIn(first_nodes[0], second_nodes)

            self.app.error_limit(first_nodes[0], 'test')
            second_nodes = list(self.app.iter_nodes(object_ring, 0))
            self.assertNotIn(first_nodes[0], second_nodes)

    def test_iter_nodes_gives_extra_if_error_limited_inline(self):
        object_ring = self.app.get_object_ring(None)
        with mock.patch.object(self.app, 'sort_nodes',
                               lambda n, *args, **kwargs: n), \
                mock.patch.object(self.app, 'request_node_count',
                                  lambda r: 6), \
                mock.patch.object(object_ring, 'max_more_nodes', 99):
            first_nodes = list(self.app.iter_nodes(object_ring, 0))
            second_nodes = []
            for node in self.app.iter_nodes(object_ring, 0):
                if not second_nodes:
                    self.app.error_limit(node, 'test')
                second_nodes.append(node)
            self.assertEqual(len(first_nodes), 6)
            self.assertEqual(len(second_nodes), 7)

    def test_iter_nodes_with_custom_node_iter(self):
        object_ring = self.app.get_object_ring(None)
        node_list = [dict(id=n, ip='1.2.3.4', port=n, device='D')
                     for n in range(10)]
        with mock.patch.object(self.app, 'sort_nodes', lambda n, *args, **kwargs: n), \
                mock.patch.object(self.app, 'request_node_count',
                                  lambda r: 3):
            got_nodes = list(self.app.iter_nodes(object_ring, 0,
                                                 node_iter=iter(node_list)))
        self.assertEqual(node_list[:3], got_nodes)

        with mock.patch.object(self.app, 'sort_nodes', lambda n, *args, **kwargs: n), \
                mock.patch.object(self.app, 'request_node_count',
                                  lambda r: 1000000):
            got_nodes = list(self.app.iter_nodes(object_ring, 0,
                                                 node_iter=iter(node_list)))
        self.assertEqual(node_list, got_nodes)

    def test_best_response_sets_headers(self):
        controller = ReplicatedObjectController(
            self.app, 'account', 'container', 'object')
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = controller.best_response(req, [200] * 3, ['OK'] * 3, [''] * 3,
                                        'Object', headers=[{'X-Test': '1'},
                                                           {'X-Test': '2'},
                                                           {'X-Test': '3'}])
        self.assertEqual(resp.headers['X-Test'], '1')

    def test_best_response_sets_etag(self):
        controller = ReplicatedObjectController(
            self.app, 'account', 'container', 'object')
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = controller.best_response(req, [200] * 3, ['OK'] * 3, [''] * 3,
                                        'Object')
        self.assertIsNone(resp.etag)
        resp = controller.best_response(req, [200] * 3, ['OK'] * 3, [''] * 3,
                                        'Object',
                                        etag='68b329da9893e34099c7d8ad5cb9c940'
                                        )
        self.assertEqual(resp.etag, '68b329da9893e34099c7d8ad5cb9c940')

    def test_proxy_passes_content_type(self):
        with save_globals():
            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
            self.app.update_request(req)
            set_http_connect(200, 200, 200)
            resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.content_type, 'x-application/test')
            set_http_connect(200, 200, 200)
            resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.content_length, 0)
            set_http_connect(200, 200, 200, slow=True)
            resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.content_length, 4)

    def test_proxy_passes_content_length_on_head(self):
        with save_globals():
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'HEAD'})
            self.app.update_request(req)
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            set_http_connect(200, 200, 200)
            resp = controller.HEAD(req)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.content_length, 0)
            set_http_connect(200, 200, 200, slow=True)
            resp = controller.HEAD(req)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.content_length, 4)

    def test_error_limiting(self):
        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            controller.app.sort_nodes = lambda l, *args, **kwargs: l
            object_ring = controller.app.get_object_ring(None)
            self.assert_status_map(controller.HEAD, (200, 200, 503, 200, 200),
                                   200)
            self.assertEqual(
                node_error_count(controller.app, object_ring.devs[0]), 2)
            self.assertTrue(
                node_last_error(controller.app, object_ring.devs[0])
                is not None)
            for _junk in range(self.app.error_suppression_limit):
                self.assert_status_map(controller.HEAD, (200, 200, 503, 503,
                                                         503), 503)
            self.assertEqual(
                node_error_count(controller.app, object_ring.devs[0]),
                self.app.error_suppression_limit + 1)
            self.assert_status_map(controller.HEAD, (200, 200, 200, 200, 200),
                                   503)
            self.assertTrue(
                node_last_error(controller.app, object_ring.devs[0])
                is not None)
            self.assert_status_map(controller.PUT, (200, 200, 200, 201, 201,
                                                    201), 503)
            self.assert_status_map(controller.POST,
                                   (200, 200, 200, 200, 200, 200, 202, 202,
                                    202), 503)
            self.assert_status_map(controller.DELETE,
                                   (200, 200, 200, 204, 204, 204), 503)
            self.app.error_suppression_interval = -300
            self.assert_status_map(controller.HEAD, (200, 200, 200, 200, 200),
                                   200)
            self.assertRaises(BaseException,
                              self.assert_status_map, controller.DELETE,
                              (200, 200, 200, 204, 204, 204), 503,
                              raise_exc=True)

    def test_error_limiting_survives_ring_reload(self):
        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            controller.app.sort_nodes = lambda l, *args, **kwargs: l
            object_ring = controller.app.get_object_ring(None)
            self.assert_status_map(controller.HEAD, (200, 200, 503, 200, 200),
                                   200)
            self.assertEqual(
                node_error_count(controller.app, object_ring.devs[0]), 2)
            self.assertTrue(
                node_last_error(controller.app, object_ring.devs[0])
                is not None)
            for _junk in range(self.app.error_suppression_limit):
                self.assert_status_map(controller.HEAD, (200, 200, 503, 503,
                                                         503), 503)
            self.assertEqual(
                node_error_count(controller.app, object_ring.devs[0]),
                self.app.error_suppression_limit + 1)

            # wipe out any state in the ring
            for policy in POLICIES:
                policy.object_ring = FakeRing(base_port=3000)

            # and we still get an error, which proves that the
            # error-limiting info survived a ring reload
            self.assert_status_map(controller.HEAD, (200, 200, 200, 200, 200),
                                   503)

    def test_PUT_error_limiting(self):
        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            controller.app.sort_nodes = lambda l, *args, **kwargs: l
            object_ring = controller.app.get_object_ring(None)
            # acc con obj obj obj
            self.assert_status_map(controller.PUT, (200, 200, 503, 200, 200),
                                   200)

            # 2, not 1, because assert_status_map() calls the method twice
            odevs = object_ring.devs
            self.assertEqual(node_error_count(controller.app, odevs[0]), 2)
            self.assertEqual(node_error_count(controller.app, odevs[1]), 0)
            self.assertEqual(node_error_count(controller.app, odevs[2]), 0)
            self.assertTrue(
                node_last_error(controller.app, odevs[0]) is not None)
            self.assertTrue(node_last_error(controller.app, odevs[1]) is None)
            self.assertTrue(node_last_error(controller.app, odevs[2]) is None)

    def test_PUT_error_limiting_last_node(self):
        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            controller.app.sort_nodes = lambda l, *args, **kwargs: l
            object_ring = controller.app.get_object_ring(None)
            # acc con obj obj obj
            self.assert_status_map(controller.PUT, (200, 200, 200, 200, 503),
                                   200)

            # 2, not 1, because assert_status_map() calls the method twice
            odevs = object_ring.devs
            self.assertEqual(node_error_count(controller.app, odevs[0]), 0)
            self.assertEqual(node_error_count(controller.app, odevs[1]), 0)
            self.assertEqual(node_error_count(controller.app, odevs[2]), 2)
            self.assertTrue(node_last_error(controller.app, odevs[0]) is None)
            self.assertTrue(node_last_error(controller.app, odevs[1]) is None)
            self.assertTrue(
                node_last_error(controller.app, odevs[2]) is not None)

    def test_acc_or_con_missing_returns_404(self):
        with save_globals():
            self.app.memcache = FakeMemcacheReturnsNone()
            self.app._error_limiting = {}
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            set_http_connect(200, 200, 200, 200, 200, 200)
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'})
            self.app.update_request(req)
            resp = getattr(controller, 'DELETE')(req)
            self.assertEqual(resp.status_int, 200)

            set_http_connect(404, 404, 404)
            #                acct acct acct
            # make sure to use a fresh request without cached env
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'})
            resp = getattr(controller, 'DELETE')(req)
            self.assertEqual(resp.status_int, 404)

            set_http_connect(503, 404, 404)
            #                acct acct acct
            # make sure to use a fresh request without cached env
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'})
            resp = getattr(controller, 'DELETE')(req)
            self.assertEqual(resp.status_int, 404)

            set_http_connect(503, 503, 404)
            #                acct acct acct
            # make sure to use a fresh request without cached env
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'})
            resp = getattr(controller, 'DELETE')(req)
            self.assertEqual(resp.status_int, 404)

            set_http_connect(503, 503, 503)
            #                acct acct acct
            # make sure to use a fresh request without cached env
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'})
            resp = getattr(controller, 'DELETE')(req)
            self.assertEqual(resp.status_int, 404)

            set_http_connect(200, 200, 204, 204, 204)
            #                acct cont obj  obj  obj
            # make sure to use a fresh request without cached env
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'})
            resp = getattr(controller, 'DELETE')(req)
            self.assertEqual(resp.status_int, 204)

            set_http_connect(200, 404, 404, 404)
            #                acct cont cont cont
            # make sure to use a fresh request without cached env
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'})
            resp = getattr(controller, 'DELETE')(req)
            self.assertEqual(resp.status_int, 404)

            set_http_connect(200, 503, 503, 503)
            #                acct cont cont cont
            # make sure to use a fresh request without cached env
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'})
            resp = getattr(controller, 'DELETE')(req)
            self.assertEqual(resp.status_int, 404)

            for dev in self.app.account_ring.devs:
                set_node_errors(
                    self.app, dev, self.app.error_suppression_limit + 1,
                    time.time())
            set_http_connect(200)
            #                acct [isn't actually called since everything
            #                      is error limited]
            # make sure to use a fresh request without cached env
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'})
            resp = getattr(controller, 'DELETE')(req)
            self.assertEqual(resp.status_int, 404)

            for dev in self.app.account_ring.devs:
                set_node_errors(self.app, dev, 0, last_error=None)
            for dev in self.app.container_ring.devs:
                set_node_errors(self.app, dev,
                                self.app.error_suppression_limit + 1,
                                time.time())
            set_http_connect(200, 200)
            #                acct cont [isn't actually called since
            #                           everything is error limited]
            # make sure to use a fresh request without cached env
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'})
            resp = getattr(controller, 'DELETE')(req)
            self.assertEqual(resp.status_int, 404)

    def test_PUT_POST_requires_container_exist(self):
        with save_globals():
            self.app.memcache = FakeMemcacheReturnsNone()
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')

            set_http_connect(200, 404, 404, 404, 200, 200, 200)
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'PUT'})
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEqual(resp.status_int, 404)

            set_http_connect(200, 404, 404, 404, 200, 200)
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'POST'},
                                headers={'Content-Type': 'text/plain'})
            self.app.update_request(req)
            resp = controller.POST(req)
            self.assertEqual(resp.status_int, 404)

    def test_bad_metadata(self):
        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            set_http_connect(200, 200, 201, 201, 201)
            #                acct cont obj  obj  obj
            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '0'})
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEqual(resp.status_int, 201)

            set_http_connect(201, 201, 201)
            req = Request.blank(
                '/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={'Content-Length': '0',
                         'X-Object-Meta-' + (
                             'a' * constraints.MAX_META_NAME_LENGTH): 'v'})
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEqual(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            req = Request.blank(
                '/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={
                    'Content-Length': '0',
                    'X-Object-Meta-' + (
                        'a' * (constraints.MAX_META_NAME_LENGTH + 1)): 'v'})
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEqual(resp.status_int, 400)

            set_http_connect(201, 201, 201)
            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '0',
                                         'X-Object-Meta-Too-Long': 'a' *
                                         constraints.MAX_META_VALUE_LENGTH})
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEqual(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            req = Request.blank(
                '/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={'Content-Length': '0',
                         'X-Object-Meta-Too-Long': 'a' *
                         (constraints.MAX_META_VALUE_LENGTH + 1)})
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEqual(resp.status_int, 400)

            set_http_connect(201, 201, 201)
            headers = {'Content-Length': '0'}
            for x in range(constraints.MAX_META_COUNT):
                headers['X-Object-Meta-%d' % x] = 'v'
            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers=headers)
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEqual(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            headers = {'Content-Length': '0'}
            for x in range(constraints.MAX_META_COUNT + 1):
                headers['X-Object-Meta-%d' % x] = 'v'
            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers=headers)
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEqual(resp.status_int, 400)

            set_http_connect(201, 201, 201)
            headers = {'Content-Length': '0'}
            header_value = 'a' * constraints.MAX_META_VALUE_LENGTH
            size = 0
            x = 0
            while size < constraints.MAX_META_OVERALL_SIZE - 4 - \
                    constraints.MAX_META_VALUE_LENGTH:
                size += 4 + constraints.MAX_META_VALUE_LENGTH
                headers['X-Object-Meta-%04d' % x] = header_value
                x += 1
            if constraints.MAX_META_OVERALL_SIZE - size > 1:
                headers['X-Object-Meta-a'] = \
                    'a' * (constraints.MAX_META_OVERALL_SIZE - size - 1)
            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers=headers)
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEqual(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            headers['X-Object-Meta-a'] = \
                'a' * (constraints.MAX_META_OVERALL_SIZE - size)
            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers=headers)
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEqual(resp.status_int, 400)

    @contextmanager
    def controller_context(self, req, *args, **kwargs):
        _v, account, container, obj = utils.split_path(req.path, 4, 4, True)
        controller = ReplicatedObjectController(
            self.app, account, container, obj)
        self.app.update_request(req)
        self.app.memcache.store = {}
        with save_globals():
            new_connect = set_http_connect(*args, **kwargs)
            yield controller
            unused_status_list = []
            while True:
                try:
                    unused_status_list.append(next(new_connect.code_iter))
                except StopIteration:
                    break
            if unused_status_list:
                raise self.fail('UN-USED STATUS CODES: %r' %
                                unused_status_list)

    @unpatch_policies
    def test_chunked_put_bad_version(self):
        # Check bad version
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis,
         obj2lis, obj3lis, obj4lis, obj5lis, obj6lis) = _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v0 HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nContent-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 412'
        self.assertEqual(headers[:len(exp)], exp)

    @unpatch_policies
    def test_chunked_put_bad_path(self):
        # Check bad path
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis,
         obj2lis, obj3lis, obj4lis, obj5lis, obj6lis) = _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET invalid HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nContent-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 404'
        self.assertEqual(headers[:len(exp)], exp)

    @unpatch_policies
    def test_chunked_put_bad_utf8(self):
        # Check invalid utf-8
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis,
         obj2lis, obj3lis, obj4lis, obj5lis, obj6lis) = _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a%80 HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Auth-Token: t\r\n'
                 'Content-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 412'
        self.assertEqual(headers[:len(exp)], exp)

    @unpatch_policies
    def test_chunked_put_bad_path_no_controller(self):
        # Check bad path, no controller
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis,
         obj2lis, obj3lis, obj4lis, obj5lis, obj6lis) = _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1 HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Auth-Token: t\r\n'
                 'Content-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 412'
        self.assertEqual(headers[:len(exp)], exp)

    @unpatch_policies
    def test_chunked_put_bad_method(self):
        # Check bad method
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis,
         obj2lis, obj3lis, obj4lis, obj5lis, obj6lis) = _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('LICK /v1/a HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Auth-Token: t\r\n'
                 'Content-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 405'
        self.assertEqual(headers[:len(exp)], exp)

    @unpatch_policies
    def test_chunked_put_unhandled_exception(self):
        # Check unhandled exception
        (prosrv, acc1srv, acc2srv, con1srv, con2srv, obj1srv,
         obj2srv, obj3srv, obj4srv, obj5srv, obj6srv) = _test_servers
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis,
         obj2lis, obj3lis, obj4lis, obj5lis, obj6lis) = _test_sockets
        orig_update_request = prosrv.update_request

        def broken_update_request(*args, **kwargs):
            raise Exception('fake: this should be printed')

        prosrv.update_request = broken_update_request
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('HEAD /v1/a HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Auth-Token: t\r\n'
                 'Content-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 500'
        self.assertEqual(headers[:len(exp)], exp)
        prosrv.update_request = orig_update_request

    @unpatch_policies
    def test_chunked_put_head_account(self):
        # Head account, just a double check and really is here to test
        # the part Application.log_request that 'enforces' a
        # content_length on the response.
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis,
         obj2lis, obj3lis, obj4lis, obj5lis, obj6lis) = _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('HEAD /v1/a HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Auth-Token: t\r\n'
                 'Content-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 204'
        self.assertEqual(headers[:len(exp)], exp)
        self.assertIn('\r\nContent-Length: 0\r\n', headers)

    @unpatch_policies
    def test_chunked_put_utf8_all_the_way_down(self):
        # Test UTF-8 Unicode all the way through the system
        ustr = '\xe1\xbc\xb8\xce\xbf\xe1\xbd\xba \xe1\xbc\xb0\xce' \
               '\xbf\xe1\xbd\xbb\xce\x87 \xcf\x84\xe1\xbd\xb0 \xcf' \
               '\x80\xe1\xbd\xb1\xce\xbd\xcf\x84\xca\xbc \xe1\xbc' \
               '\x82\xce\xbd \xe1\xbc\x90\xce\xbe\xe1\xbd\xb5\xce' \
               '\xba\xce\xbf\xce\xb9 \xcf\x83\xce\xb1\xcf\x86\xe1' \
               '\xbf\x86.Test'
        ustr_short = '\xe1\xbc\xb8\xce\xbf\xe1\xbd\xbatest'
        # Create ustr container
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis,
         obj2lis, obj3lis, obj4lis, obj5lis, obj6lis) = _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%s HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n'
                 'Content-Length: 0\r\n\r\n' % quote(ustr))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)
        # List account with ustr container (test plain)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n'
                 'Content-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEqual(headers[:len(exp)], exp)
        containers = fd.read().split('\n')
        self.assertIn(ustr, containers)
        # List account with ustr container (test json)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a?format=json HTTP/1.1\r\n'
                 'Host: localhost\r\nConnection: close\r\n'
                 'X-Storage-Token: t\r\nContent-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEqual(headers[:len(exp)], exp)
        listing = json.loads(fd.read())
        self.assertIn(ustr.decode('utf8'), [l['name'] for l in listing])
        # List account with ustr container (test xml)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a?format=xml HTTP/1.1\r\n'
                 'Host: localhost\r\nConnection: close\r\n'
                 'X-Storage-Token: t\r\nContent-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEqual(headers[:len(exp)], exp)
        self.assertIn('<name>%s</name>' % ustr, fd.read())
        # Create ustr object with ustr metadata in ustr container
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%s/%s HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n'
                 'X-Object-Meta-%s: %s\r\nContent-Length: 0\r\n\r\n' %
                 (quote(ustr), quote(ustr), quote(ustr_short),
                  quote(ustr)))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)
        # List ustr container with ustr object (test plain)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/%s HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n'
                 'Content-Length: 0\r\n\r\n' % quote(ustr))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEqual(headers[:len(exp)], exp)
        objects = fd.read().split('\n')
        self.assertIn(ustr, objects)
        # List ustr container with ustr object (test json)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/%s?format=json HTTP/1.1\r\n'
                 'Host: localhost\r\nConnection: close\r\n'
                 'X-Storage-Token: t\r\nContent-Length: 0\r\n\r\n' %
                 quote(ustr))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEqual(headers[:len(exp)], exp)
        listing = json.loads(fd.read())
        self.assertEqual(listing[0]['name'], ustr.decode('utf8'))
        # List ustr container with ustr object (test xml)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/%s?format=xml HTTP/1.1\r\n'
                 'Host: localhost\r\nConnection: close\r\n'
                 'X-Storage-Token: t\r\nContent-Length: 0\r\n\r\n' %
                 quote(ustr))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEqual(headers[:len(exp)], exp)
        self.assertIn('<name>%s</name>' % ustr, fd.read())
        # Retrieve ustr object with ustr metadata
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/%s/%s HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n'
                 'Content-Length: 0\r\n\r\n' %
                 (quote(ustr), quote(ustr)))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEqual(headers[:len(exp)], exp)
        self.assertIn('\r\nX-Object-Meta-%s: %s\r\n' %
                      (quote(ustr_short).lower(), quote(ustr)), headers)

    @unpatch_policies
    def test_chunked_put_chunked_put(self):
        # Do chunked object put
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis,
         obj2lis, obj3lis, obj4lis, obj5lis, obj6lis) = _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        # Also happens to assert that x-storage-token is taken as a
        # replacement for x-auth-token.
        fd.write('PUT /v1/a/c/o/chunky HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n'
                 'Transfer-Encoding: chunked\r\n\r\n'
                 '2\r\noh\r\n4\r\n hai\r\nf\r\n123456789abcdef\r\n'
                 '0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)
        # Ensure we get what we put
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/c/o/chunky HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Auth-Token: t\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEqual(headers[:len(exp)], exp)
        body = fd.read()
        self.assertEqual(body, 'oh hai123456789abcdef')

    @unpatch_policies
    def test_conditional_range_get(self):
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis, obj2lis,
         obj3lis, obj4lis, obj5lis, obj6lis) = _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))

        # make a container
        fd = sock.makefile()
        fd.write('PUT /v1/a/con HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n'
                 'Content-Length: 0\r\n\r\n')
        fd.flush()
        exp = 'HTTP/1.1 201'
        headers = readuntil2crlfs(fd)
        self.assertEqual(headers[:len(exp)], exp)

        # put an object in it
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/con/o HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Length: 10\r\n'
                 'Content-Type: text/plain\r\n'
                 '\r\n'
                 'abcdefghij\r\n')
        fd.flush()
        exp = 'HTTP/1.1 201'
        headers = readuntil2crlfs(fd)
        self.assertEqual(headers[:len(exp)], exp)

        # request with both If-None-Match and Range
        etag = md5("abcdefghij").hexdigest()
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/con/o HTTP/1.1\r\n' +
                 'Host: localhost\r\n' +
                 'Connection: close\r\n' +
                 'X-Storage-Token: t\r\n' +
                 'If-None-Match: "' + etag + '"\r\n' +
                 'Range: bytes=3-8\r\n' +
                 '\r\n')
        fd.flush()
        exp = 'HTTP/1.1 304'
        headers = readuntil2crlfs(fd)
        self.assertEqual(headers[:len(exp)], exp)

    def test_mismatched_etags(self):
        with save_globals():
            # no etag supplied, object servers return success w/ diff values
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '0'})
            self.app.update_request(req)
            set_http_connect(200, 201, 201, 201,
                             etags=[None,
                                    '68b329da9893e34099c7d8ad5cb9c940',
                                    '68b329da9893e34099c7d8ad5cb9c940',
                                    '68b329da9893e34099c7d8ad5cb9c941'])
            resp = controller.PUT(req)
            self.assertEqual(resp.status_int // 100, 5)  # server error

            # req supplies etag, object servers return 422 - mismatch
            headers = {'Content-Length': '0',
                       'ETag': '68b329da9893e34099c7d8ad5cb9c940'}
            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers=headers)
            self.app.update_request(req)
            set_http_connect(200, 422, 422, 503,
                             etags=['68b329da9893e34099c7d8ad5cb9c940',
                                    '68b329da9893e34099c7d8ad5cb9c941',
                                    None,
                                    None])
            resp = controller.PUT(req)
            self.assertEqual(resp.status_int // 100, 4)  # client error

    def test_response_get_accept_ranges_header(self):
        with save_globals():
            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
            self.app.update_request(req)
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            set_http_connect(200, 200, 200)
            resp = controller.GET(req)
            self.assertIn('accept-ranges', resp.headers)
            self.assertEqual(resp.headers['accept-ranges'], 'bytes')

    def test_response_head_accept_ranges_header(self):
        with save_globals():
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'HEAD'})
            self.app.update_request(req)
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            set_http_connect(200, 200, 200)
            resp = controller.HEAD(req)
            self.assertIn('accept-ranges', resp.headers)
            self.assertEqual(resp.headers['accept-ranges'], 'bytes')

    def test_GET_calls_authorize(self):
        called = [False]

        def authorize(req):
            called[0] = True
            return HTTPUnauthorized(request=req)
        with save_globals():
            set_http_connect(200, 200, 201, 201, 201)
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            req = Request.blank('/v1/a/c/o')
            req.environ['swift.authorize'] = authorize
            self.app.update_request(req)
            controller.GET(req)
        self.assertTrue(called[0])

    def _check_GET_respects_read_affinity(self, conf, policy, expected_nodes):
        actual_nodes = []

        def test_connect(ipaddr, port, device, partition, method, path,
                         headers=None, query_string=None):
            if path == '/a/c/o.jpg':
                actual_nodes.append(ipaddr)

        # mock shuffle to be a no-op to ensure that the only way nodes would
        # not be used in ring order is if affinity is respected.
        with mock.patch('swift.proxy.server.shuffle', lambda x: x):
            app = proxy_server.Application(
                conf, FakeMemcache(),
                logger=debug_logger('proxy-ut'),
                account_ring=FakeRing(),
                container_ring=FakeRing())
            with save_globals():
                object_ring = app.get_object_ring(policy)
                object_ring.max_more_nodes = 100
                controller = \
                    ReplicatedObjectController(
                        app, 'a', 'c', 'o.jpg')
                # requests go to acc, con, obj, obj, obj
                set_http_connect(200, 200, 404, 404, 200,
                                 give_connect=test_connect)
                req = Request.blank(
                    '/v1/a/c/o.jpg',
                    headers={'X-Backend-Storage-Policy-Index': str(policy)})
                app.memcache.store = {}
                res = controller.GET(req)
        self.assertTrue(res.status.startswith('200 '))
        self.assertEqual(3, len(actual_nodes))
        self.assertEqual(expected_nodes, actual_nodes)

    @patch_policies([StoragePolicy(0, 'zero', True, object_ring=FakeRing()),
                     StoragePolicy(1, 'one', False, object_ring=FakeRing())])
    def test_GET_respects_read_affinity(self):
        # nodes in fake ring order have r0z0, r1z1, r0z2
        # Check default conf via proxy server conf
        conf = {'read_affinity': 'r0z2=1, r1=2',
                'sorting_method': 'affinity'}
        expected_nodes = ['10.0.0.2', '10.0.0.1', '10.0.0.0']
        self._check_GET_respects_read_affinity(conf, 0, expected_nodes)

        # policy 0 and policy 1 have conf via per policy conf section
        conf = {
            'read_affinity': '',
            'sorting_method': 'shuffle',
            'policy_config': {
                '0': {'read_affinity': 'r1z1=1, r0z2=2',
                      'sorting_method': 'affinity'},
                '1': {'read_affinity': 'r0z2=1, r0z0=2',
                      'sorting_method': 'affinity'}
            }
        }
        expected_nodes = ['10.0.0.1', '10.0.0.2', '10.0.0.0']
        self._check_GET_respects_read_affinity(conf, 0, expected_nodes)
        expected_nodes = ['10.0.0.2', '10.0.0.0', '10.0.0.1']
        self._check_GET_respects_read_affinity(conf, 1, expected_nodes)

        # policy 0 conf via per policy conf section overrides proxy server conf
        conf = {
            'read_affinity': 'r1z1=1, r0z2=2',
            'sorting_method': 'affinity',
            'policy_config': {
                '0': {'read_affinity': 'r0z2=1, r0=2',
                      'sorting_method': 'affinity'}
            }
        }
        expected_nodes = ['10.0.0.2', '10.0.0.0', '10.0.0.1']
        self._check_GET_respects_read_affinity(conf, 0, expected_nodes)

    def test_HEAD_calls_authorize(self):
        called = [False]

        def authorize(req):
            called[0] = True
            return HTTPUnauthorized(request=req)
        with save_globals():
            set_http_connect(200, 200, 201, 201, 201)
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            req = Request.blank('/v1/a/c/o', {'REQUEST_METHOD': 'HEAD'})
            req.environ['swift.authorize'] = authorize
            self.app.update_request(req)
            controller.HEAD(req)
        self.assertTrue(called[0])

    def test_POST_calls_authorize(self):
        called = [False]

        def authorize(req):
            called[0] = True
            return HTTPUnauthorized(request=req)
        with save_globals():
            set_http_connect(200, 200, 201, 201, 201)
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'POST'},
                                headers={'Content-Length': '5'}, body='12345')
            req.environ['swift.authorize'] = authorize
            self.app.update_request(req)
            controller.POST(req)
        self.assertTrue(called[0])

    def test_PUT_calls_authorize(self):
        called = [False]

        def authorize(req):
            called[0] = True
            return HTTPUnauthorized(request=req)
        with save_globals():
            set_http_connect(200, 200, 201, 201, 201)
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '5'}, body='12345')
            req.environ['swift.authorize'] = authorize
            self.app.update_request(req)
            controller.PUT(req)
        self.assertTrue(called[0])

    def test_POST_converts_delete_after_to_delete_at(self):
        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            set_http_connect(200, 200, 202, 202, 202)
            self.app.memcache.store = {}
            orig_time = time.time
            try:
                t = time.time()
                time.time = lambda: t
                req = Request.blank('/v1/a/c/o', {},
                                    headers={'Content-Type': 'foo/bar',
                                             'X-Delete-After': '60'})
                self.app.update_request(req)
                res = controller.POST(req)
                self.assertEqual(res.status, '202 Fake')
                self.assertEqual(req.headers.get('x-delete-at'),
                                 str(int(t + 60)))
            finally:
                time.time = orig_time

    @unpatch_policies
    def test_leak_1(self):
        _request_instances = weakref.WeakKeyDictionary()
        _orig_init = Request.__init__

        def request_init(self, *args, **kwargs):
            _orig_init(self, *args, **kwargs)
            _request_instances[self] = None

        with mock.patch.object(Request, "__init__", request_init):
            prolis = _test_sockets[0]
            prosrv = _test_servers[0]
            obj_len = prosrv.client_chunk_size * 2
            # PUT test file
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('PUT /v1/a/c/test_leak_1 HTTP/1.1\r\n'
                     'Host: localhost\r\n'
                     'Connection: close\r\n'
                     'X-Auth-Token: t\r\n'
                     'Content-Length: %s\r\n'
                     'Content-Type: application/octet-stream\r\n'
                     '\r\n%s' % (obj_len, 'a' * obj_len))
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = 'HTTP/1.1 201'
            self.assertEqual(headers[:len(exp)], exp)
            # Remember Request instance count, make sure the GC is run for
            # pythons without reference counting.
            for i in range(4):
                sleep(0)  # let eventlet do its thing
                gc.collect()
            else:
                sleep(0)
            before_request_instances = len(_request_instances)
            # GET test file, but disconnect early
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('GET /v1/a/c/test_leak_1 HTTP/1.1\r\n'
                     'Host: localhost\r\n'
                     'Connection: close\r\n'
                     'X-Auth-Token: t\r\n'
                     '\r\n')
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = 'HTTP/1.1 200'
            self.assertEqual(headers[:len(exp)], exp)
            fd.read(1)
            sock.fd._sock.close()
            # Make sure the GC is run again for pythons without reference
            # counting
            for i in range(4):
                sleep(0)  # let eventlet do its thing
                gc.collect()
            else:
                sleep(0)
            self.assertEqual(
                before_request_instances, len(_request_instances))

    def test_OPTIONS(self):
        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'a', 'c', 'o.jpg')

            def my_empty_container_info(*args):
                return {}
            controller.container_info = my_empty_container_info
            req = Request.blank(
                '/v1/a/c/o.jpg',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'http://foo.com',
                         'Access-Control-Request-Method': 'GET'})
            resp = controller.OPTIONS(req)
            self.assertEqual(401, resp.status_int)
            self.assertNotIn('Access-Control-Allow-Origin', resp.headers)
            self.assertNotIn('Vary', resp.headers)

            def my_empty_origin_container_info(*args):
                return {'cors': {'allow_origin': None}}
            controller.container_info = my_empty_origin_container_info
            req = Request.blank(
                '/v1/a/c/o.jpg',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'http://foo.com',
                         'Access-Control-Request-Method': 'GET'})
            resp = controller.OPTIONS(req)
            self.assertEqual(401, resp.status_int)
            self.assertNotIn('Access-Control-Allow-Origin', resp.headers)
            self.assertNotIn('Vary', resp.headers)

            def my_container_info(*args):
                return {
                    'cors': {
                        'allow_origin': 'http://foo.bar:8080 https://foo.bar',
                        'max_age': '999',
                    }
                }
            controller.container_info = my_container_info
            req = Request.blank(
                '/v1/a/c/o.jpg',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'https://foo.bar',
                         'Access-Control-Request-Method': 'GET'})
            req.content_length = 0
            resp = controller.OPTIONS(req)
            self.assertEqual(200, resp.status_int)
            self.assertEqual(
                'https://foo.bar',
                resp.headers['access-control-allow-origin'])
            self.assertEqual('Origin', resp.headers.get('vary'))
            self.assertEqual(
                sorted(resp.headers['access-control-allow-methods']
                       .split(', ')),
                sorted('OPTIONS GET POST PUT DELETE HEAD'.split()))
            self.assertEqual('999', resp.headers['access-control-max-age'])

            req = Request.blank(
                '/v1/a/c/o.jpg',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'https://foo.bar'})
            req.content_length = 0
            resp = controller.OPTIONS(req)
            self.assertEqual(401, resp.status_int)
            self.assertNotIn('Access-Control-Allow-Origin', resp.headers)
            self.assertNotIn('Vary', resp.headers)

            req = Request.blank('/v1/a/c/o.jpg', {'REQUEST_METHOD': 'OPTIONS'})
            req.content_length = 0
            resp = controller.OPTIONS(req)
            self.assertEqual(200, resp.status_int)
            self.assertEqual(
                sorted(resp.headers['Allow'].split(', ')),
                sorted('OPTIONS GET POST PUT DELETE HEAD'.split()))
            self.assertNotIn('Access-Control-Allow-Origin', resp.headers)
            self.assertNotIn('Vary', resp.headers)

            req = Request.blank(
                '/v1/a/c/o.jpg',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'http://foo.com'})
            resp = controller.OPTIONS(req)
            self.assertEqual(401, resp.status_int)
            self.assertNotIn('Access-Control-Allow-Origin', resp.headers)
            self.assertNotIn('Vary', resp.headers)

            req = Request.blank(
                '/v1/a/c/o.jpg',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'http://foo.bar',
                         'Access-Control-Request-Method': 'GET'})
            controller.app.cors_allow_origin = ['http://foo.bar', ]
            resp = controller.OPTIONS(req)
            self.assertEqual(200, resp.status_int)
            self.assertEqual('Origin', resp.headers.get('vary'))

            def my_container_info_wildcard(*args):
                return {
                    'cors': {
                        'allow_origin': '*',
                        'max_age': '999',
                    }
                }
            controller.container_info = my_container_info_wildcard
            req = Request.blank(
                '/v1/a/c/o.jpg',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'https://bar.baz',
                         'Access-Control-Request-Method': 'GET'})
            req.content_length = 0
            resp = controller.OPTIONS(req)
            self.assertEqual(200, resp.status_int)
            self.assertEqual('*', resp.headers['access-control-allow-origin'])
            self.assertNotIn('Vary', resp.headers)
            self.assertEqual(
                sorted(resp.headers['access-control-allow-methods']
                       .split(', ')),
                sorted('OPTIONS GET POST PUT DELETE HEAD'.split()))
            self.assertEqual('999', resp.headers['access-control-max-age'])

    def _get_CORS_response(self, container_cors, strict_mode, object_get=None):
        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'a', 'c', 'o')

            def stubContainerInfo(*args):
                return {
                    'cors': container_cors
                }

            controller.container_info = stubContainerInfo
            controller.app.strict_cors_mode = strict_mode

            def objectGET(controller, req):
                return Response(headers={
                    'X-Object-Meta-Color': 'red',
                    'X-Super-Secret': 'hush',
                })

            mock_object_get = object_get or objectGET

            req = Request.blank(
                '/v1/a/c/o.jpg',
                {'REQUEST_METHOD': 'GET'},
                headers={'Origin': 'http://foo.bar'})

            resp = cors_validation(mock_object_get)(controller, req)

            return resp

    def test_CORS_valid_non_strict(self):
        # test expose_headers to non-allowed origins
        container_cors = {'allow_origin': 'http://not.foo.bar',
                          'expose_headers': 'X-Object-Meta-Color '
                                            'X-Object-Meta-Color-Ex'}
        resp = self._get_CORS_response(
            container_cors=container_cors, strict_mode=False)

        self.assertEqual(200, resp.status_int)
        self.assertEqual('http://foo.bar',
                         resp.headers['access-control-allow-origin'])
        self.assertEqual('red', resp.headers['x-object-meta-color'])
        # X-Super-Secret is in the response, but not "exposed"
        self.assertEqual('hush', resp.headers['x-super-secret'])
        self.assertIn('access-control-expose-headers', resp.headers)
        exposed = set(
            h.strip() for h in
            resp.headers['access-control-expose-headers'].split(','))
        expected_exposed = set([
            'cache-control', 'content-language', 'content-type', 'expires',
            'last-modified', 'pragma', 'etag', 'x-timestamp', 'x-trans-id',
            'x-openstack-request-id', 'x-object-meta-color',
            'x-object-meta-color-ex'])
        self.assertEqual(expected_exposed, exposed)

        # test allow_origin *
        container_cors = {'allow_origin': '*'}

        resp = self._get_CORS_response(
            container_cors=container_cors, strict_mode=False)
        self.assertEqual(200, resp.status_int)
        self.assertEqual('*',
                         resp.headers['access-control-allow-origin'])

        # test allow_origin empty
        container_cors = {'allow_origin': ''}
        resp = self._get_CORS_response(
            container_cors=container_cors, strict_mode=False)
        self.assertEqual(200, resp.status_int)
        self.assertEqual('http://foo.bar',
                         resp.headers['access-control-allow-origin'])

    def test_CORS_valid_strict(self):
        # test expose_headers to non-allowed origins
        container_cors = {'allow_origin': 'http://not.foo.bar',
                          'expose_headers': 'X-Object-Meta-Color '
                                            'X-Object-Meta-Color-Ex'}
        resp = self._get_CORS_response(
            container_cors=container_cors, strict_mode=True)

        self.assertEqual(200, resp.status_int)
        self.assertNotIn('access-control-expose-headers', resp.headers)
        self.assertNotIn('access-control-allow-origin', resp.headers)

        # test allow_origin *
        container_cors = {'allow_origin': '*'}

        resp = self._get_CORS_response(
            container_cors=container_cors, strict_mode=True)
        self.assertEqual(200, resp.status_int)
        self.assertEqual('*',
                         resp.headers['access-control-allow-origin'])
        self.assertEqual('red', resp.headers['x-object-meta-color'])
        # X-Super-Secret is in the response, but not "exposed"
        self.assertEqual('hush', resp.headers['x-super-secret'])
        self.assertIn('access-control-expose-headers', resp.headers)
        exposed = set(
            h.strip() for h in
            resp.headers['access-control-expose-headers'].split(','))
        expected_exposed = set([
            'cache-control', 'content-language', 'content-type', 'expires',
            'last-modified', 'pragma', 'etag', 'x-timestamp', 'x-trans-id',
            'x-openstack-request-id', 'x-object-meta-color'])
        self.assertEqual(expected_exposed, exposed)

        # test allow_origin empty
        container_cors = {'allow_origin': ''}
        resp = self._get_CORS_response(
            container_cors=container_cors, strict_mode=True)
        self.assertNotIn('access-control-expose-headers', resp.headers)
        self.assertNotIn('access-control-allow-origin', resp.headers)

        # test proxy server cors_allow_origin option
        self.app.cors_allow_origin = ['http://foo.bar']
        resp = self._get_CORS_response(
            container_cors=container_cors, strict_mode=True)
        self.assertEqual('http://foo.bar',
                         resp.headers['access-control-allow-origin'])
        self.assertEqual(expected_exposed, exposed)

    def test_CORS_valid_with_obj_headers(self):
        container_cors = {'allow_origin': 'http://foo.bar'}

        def objectGET(controller, req):
            return Response(headers={
                'X-Object-Meta-Color': 'red',
                'X-Super-Secret': 'hush',
                'Access-Control-Allow-Origin': 'http://obj.origin',
                'Access-Control-Expose-Headers': 'x-trans-id'
            })

        resp = self._get_CORS_response(
            container_cors=container_cors, strict_mode=True,
            object_get=objectGET)

        self.assertEqual(200, resp.status_int)
        self.assertEqual('http://obj.origin',
                         resp.headers['access-control-allow-origin'])
        self.assertEqual('x-trans-id',
                         resp.headers['access-control-expose-headers'])

    def test_CORS_expose_headers(self):
        default_expected_exposed = set([
            'cache-control', 'content-language', 'content-type', 'expires',
            'last-modified', 'pragma', 'etag', 'x-timestamp', 'x-trans-id',
            'x-openstack-request-id'])

        def objectGET(controller, req):
                return Response(headers={
                    'X-Custom-Operator': 'hush',
                    'X-Custom-User': 'hush',
                })

        # test default expose_headers
        self.app.cors_expose_headers = []
        container_cors = {'allow_origin': 'http://foo.bar'}
        resp = self._get_CORS_response(container_cors=container_cors,
                                       strict_mode=False, object_get=objectGET)

        self.assertEqual(200, resp.status_int)
        self.assertIn('access-control-expose-headers', resp.headers)
        exposed = set(
            h.strip() for h in
            resp.headers['access-control-expose-headers'].split(','))
        self.assertEqual(default_expected_exposed, exposed)

        # test operator expose_headers
        self.app.cors_expose_headers = ['x-custom-operator', ]
        container_cors = {'allow_origin': 'http://foo.bar'}
        resp = self._get_CORS_response(container_cors=container_cors,
                                       strict_mode=False, object_get=objectGET)

        self.assertEqual(200, resp.status_int)
        self.assertIn('access-control-expose-headers', resp.headers)
        exposed = set(
            h.strip() for h in
            resp.headers['access-control-expose-headers'].split(','))
        self.assertEqual(default_expected_exposed | set(['x-custom-operator']),
                         exposed)

        # test user expose_headers
        self.app.cors_expose_headers = []
        container_cors = {'allow_origin': 'http://foo.bar',
                          'expose_headers': 'x-custom-user'}
        resp = self._get_CORS_response(container_cors=container_cors,
                                       strict_mode=False, object_get=objectGET)

        self.assertEqual(200, resp.status_int)
        self.assertIn('access-control-expose-headers', resp.headers)
        exposed = set(
            h.strip() for h in
            resp.headers['access-control-expose-headers'].split(','))
        self.assertEqual(default_expected_exposed | set(['x-custom-user']),
                         exposed)

        # test user and operator expose_headers
        self.app.cors_expose_headers = ['x-custom-operator', ]
        container_cors = {'allow_origin': 'http://foo.bar',
                          'expose_headers': 'x-custom-user'}
        resp = self._get_CORS_response(container_cors=container_cors,
                                       strict_mode=False, object_get=objectGET)

        self.assertEqual(200, resp.status_int)
        self.assertIn('access-control-expose-headers', resp.headers)
        exposed = set(
            h.strip() for h in
            resp.headers['access-control-expose-headers'].split(','))
        self.assertEqual(default_expected_exposed | set(['x-custom-user',
                                                         'x-custom-operator']),
                         exposed)

    def _gather_x_container_headers(self, controller_call, req, *connect_args,
                                    **kwargs):
        header_list = kwargs.pop('header_list', ['X-Container-Device',
                                                 'X-Container-Host',
                                                 'X-Container-Partition'])
        seen_headers = []

        def capture_headers(ipaddr, port, device, partition, method,
                            path, headers=None, query_string=None):
            captured = {}
            for header in header_list:
                captured[header] = headers.get(header)
            seen_headers.append(captured)

        with save_globals():
            self.app.allow_account_management = True

            set_http_connect(*connect_args, give_connect=capture_headers,
                             **kwargs)
            resp = controller_call(req)
            self.assertEqual(2, resp.status_int // 100)  # sanity check

            # don't care about the account/container HEADs, so chuck
            # the first two requests
            return sorted(seen_headers[2:],
                          key=lambda d: d.get(header_list[0]) or 'z')

    def test_PUT_x_container_headers_with_equal_replicas(self):
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '5'}, body='12345')
        controller = ReplicatedObjectController(
            self.app, 'a', 'c', 'o')
        seen_headers = self._gather_x_container_headers(
            controller.PUT, req,
            200, 200, 201, 201, 201)   # HEAD HEAD PUT PUT PUT
        self.assertEqual(
            seen_headers, [
                {'X-Container-Host': '10.0.0.0:1000',
                 'X-Container-Partition': '0',
                 'X-Container-Device': 'sda'},
                {'X-Container-Host': '10.0.0.1:1001',
                 'X-Container-Partition': '0',
                 'X-Container-Device': 'sdb'},
                {'X-Container-Host': '10.0.0.2:1002',
                 'X-Container-Partition': '0',
                 'X-Container-Device': 'sdc'}])

    def test_PUT_x_container_headers_with_fewer_container_replicas(self):
        self.app.container_ring.set_replicas(2)

        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '5'}, body='12345')
        controller = ReplicatedObjectController(
            self.app, 'a', 'c', 'o')
        seen_headers = self._gather_x_container_headers(
            controller.PUT, req,
            200, 200, 201, 201, 201)   # HEAD HEAD PUT PUT PUT

        self.assertEqual(
            seen_headers, [
                {'X-Container-Host': '10.0.0.0:1000',
                 'X-Container-Partition': '0',
                 'X-Container-Device': 'sda'},
                {'X-Container-Host': '10.0.0.1:1001',
                 'X-Container-Partition': '0',
                 'X-Container-Device': 'sdb'},
                {'X-Container-Host': None,
                 'X-Container-Partition': None,
                 'X-Container-Device': None}])

    def test_PUT_x_container_headers_with_many_object_replicas(self):
        POLICIES[0].object_ring.set_replicas(11)

        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '5'}, body='12345')
        controller = ReplicatedObjectController(
            self.app, 'a', 'c', 'o')
        seen_headers = self._gather_x_container_headers(
            controller.PUT, req,
            # HEAD HEAD PUT PUT PUT PUT PUT PUT PUT PUT PUT PUT PUT
            200, 200, 201, 201, 201, 201, 201, 201, 201, 201, 201, 201, 201)

        self.assertEqual(
            sorted(seen_headers), sorted([
                {'X-Container-Host': '10.0.0.0:1000',
                 'X-Container-Partition': '0',
                 'X-Container-Device': 'sda'},
                {'X-Container-Host': '10.0.0.0:1000',
                 'X-Container-Partition': '0',
                 'X-Container-Device': 'sda'},
                {'X-Container-Host': '10.0.0.0:1000',
                 'X-Container-Partition': '0',
                 'X-Container-Device': 'sda'},
                {'X-Container-Host': '10.0.0.1:1001',
                 'X-Container-Partition': '0',
                 'X-Container-Device': 'sdb'},
                {'X-Container-Host': '10.0.0.1:1001',
                 'X-Container-Partition': '0',
                 'X-Container-Device': 'sdb'},
                {'X-Container-Host': '10.0.0.2:1002',
                 'X-Container-Partition': '0',
                 'X-Container-Device': 'sdc'},
                {'X-Container-Host': '10.0.0.2:1002',
                 'X-Container-Partition': '0',
                 'X-Container-Device': 'sdc'},
                {'X-Container-Host': None,
                 'X-Container-Partition': None,
                 'X-Container-Device': None},
                {'X-Container-Host': None,
                 'X-Container-Partition': None,
                 'X-Container-Device': None},
                {'X-Container-Host': None,
                 'X-Container-Partition': None,
                 'X-Container-Device': None},
                {'X-Container-Host': None,
                 'X-Container-Partition': None,
                 'X-Container-Device': None},
            ]))

    def test_PUT_x_container_headers_with_more_container_replicas(self):
        self.app.container_ring.set_replicas(4)

        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '5'}, body='12345')
        controller = ReplicatedObjectController(
            self.app, 'a', 'c', 'o')
        seen_headers = self._gather_x_container_headers(
            controller.PUT, req,
            200, 200, 201, 201, 201)   # HEAD HEAD PUT PUT PUT

        self.assertEqual(
            seen_headers, [
                {'X-Container-Host': '10.0.0.0:1000,10.0.0.3:1003',
                 'X-Container-Partition': '0',
                 'X-Container-Device': 'sda,sdd'},
                {'X-Container-Host': '10.0.0.1:1001',
                 'X-Container-Partition': '0',
                 'X-Container-Device': 'sdb'},
                {'X-Container-Host': '10.0.0.2:1002',
                 'X-Container-Partition': '0',
                 'X-Container-Device': 'sdc'}])

    def test_POST_x_container_headers_with_more_container_replicas(self):
        self.app.container_ring.set_replicas(4)

        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Content-Type': 'application/stuff'})
        controller = ReplicatedObjectController(
            self.app, 'a', 'c', 'o')
        seen_headers = self._gather_x_container_headers(
            controller.POST, req,
            200, 200, 200, 200, 200)   # HEAD HEAD POST POST POST

        self.assertEqual(
            seen_headers, [
                {'X-Container-Host': '10.0.0.0:1000,10.0.0.3:1003',
                 'X-Container-Partition': '0',
                 'X-Container-Device': 'sda,sdd'},
                {'X-Container-Host': '10.0.0.1:1001',
                 'X-Container-Partition': '0',
                 'X-Container-Device': 'sdb'},
                {'X-Container-Host': '10.0.0.2:1002',
                 'X-Container-Partition': '0',
                 'X-Container-Device': 'sdc'}])

    def test_DELETE_x_container_headers_with_more_container_replicas(self):
        self.app.container_ring.set_replicas(4)

        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Content-Type': 'application/stuff'})
        controller = ReplicatedObjectController(
            self.app, 'a', 'c', 'o')
        seen_headers = self._gather_x_container_headers(
            controller.DELETE, req,
            200, 200, 200, 200, 200)   # HEAD HEAD DELETE DELETE DELETE

        self.assertEqual(seen_headers, [
            {'X-Container-Host': '10.0.0.0:1000,10.0.0.3:1003',
             'X-Container-Partition': '0',
             'X-Container-Device': 'sda,sdd'},
            {'X-Container-Host': '10.0.0.1:1001',
             'X-Container-Partition': '0',
             'X-Container-Device': 'sdb'},
            {'X-Container-Host': '10.0.0.2:1002',
             'X-Container-Partition': '0',
             'X-Container-Device': 'sdc'}
        ])

    @mock.patch('time.time', new=lambda: STATIC_TIME)
    def test_PUT_x_delete_at_with_fewer_container_replicas(self):
        self.app.container_ring.set_replicas(2)

        delete_at_timestamp = int(time.time()) + 100000
        delete_at_container = utils.get_expirer_container(
            delete_at_timestamp, self.app.expiring_objects_container_divisor,
            'a', 'c', 'o')
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Type': 'application/stuff',
                                     'Content-Length': '0',
                                     'X-Delete-At': str(delete_at_timestamp)})
        controller = ReplicatedObjectController(
            self.app, 'a', 'c', 'o')
        seen_headers = self._gather_x_container_headers(
            controller.PUT, req,
            200, 200, 201, 201, 201,   # HEAD HEAD PUT PUT PUT
            header_list=('X-Delete-At-Host', 'X-Delete-At-Device',
                         'X-Delete-At-Partition', 'X-Delete-At-Container'))

        self.assertEqual(seen_headers, [
            {'X-Delete-At-Host': '10.0.0.0:1000',
             'X-Delete-At-Container': delete_at_container,
             'X-Delete-At-Partition': '0',
             'X-Delete-At-Device': 'sda'},
            {'X-Delete-At-Host': '10.0.0.1:1001',
             'X-Delete-At-Container': delete_at_container,
             'X-Delete-At-Partition': '0',
             'X-Delete-At-Device': 'sdb'},
            {'X-Delete-At-Host': None,
             'X-Delete-At-Partition': None,
             'X-Delete-At-Container': None,
             'X-Delete-At-Device': None},
        ])

    @mock.patch('time.time', new=lambda: STATIC_TIME)
    def test_PUT_x_delete_at_with_more_container_replicas(self):
        self.app.container_ring.set_replicas(4)
        self.app.expiring_objects_account = 'expires'
        self.app.expiring_objects_container_divisor = 60

        delete_at_timestamp = int(time.time()) + 100000
        delete_at_container = utils.get_expirer_container(
            delete_at_timestamp, self.app.expiring_objects_container_divisor,
            'a', 'c', 'o')
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Type': 'application/stuff',
                                     'Content-Length': 0,
                                     'X-Delete-At': str(delete_at_timestamp)})
        controller = ReplicatedObjectController(
            self.app, 'a', 'c', 'o')
        seen_headers = self._gather_x_container_headers(
            controller.PUT, req,
            200, 200, 201, 201, 201,   # HEAD HEAD PUT PUT PUT
            header_list=('X-Delete-At-Host', 'X-Delete-At-Device',
                         'X-Delete-At-Partition', 'X-Delete-At-Container'))
        self.assertEqual(seen_headers, [
            {'X-Delete-At-Host': '10.0.0.0:1000,10.0.0.3:1003',
             'X-Delete-At-Container': delete_at_container,
             'X-Delete-At-Partition': '0',
             'X-Delete-At-Device': 'sda,sdd'},
            {'X-Delete-At-Host': '10.0.0.1:1001',
             'X-Delete-At-Container': delete_at_container,
             'X-Delete-At-Partition': '0',
             'X-Delete-At-Device': 'sdb'},
            {'X-Delete-At-Host': '10.0.0.2:1002',
             'X-Delete-At-Container': delete_at_container,
             'X-Delete-At-Partition': '0',
             'X-Delete-At-Device': 'sdc'}
        ])


class BaseTestECObjectController(BaseTestObjectController):
    def test_PUT_ec(self):
        self.put_container(self.ec_policy.name, self.ec_policy.name)

        obj = 'abCD' * 10  # small, so we don't get multiple EC stripes
        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%s/o1 HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'Etag: "%s"\r\n'
                 'Content-Length: %d\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 '\r\n%s' % (self.ec_policy.name, md5(obj).hexdigest(),
                             len(obj), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

        ecd = self.ec_policy.pyeclib_driver
        expected_pieces = set(ecd.encode(obj))

        # go to disk to make sure it's there and all erasure-coded
        partition, nodes = self.ec_policy.object_ring.get_nodes(
            'a', self.ec_policy.name, 'o1')
        conf = {'devices': _testdir, 'mount_check': 'false'}
        df_mgr = diskfile.DiskFileRouter(conf, FakeLogger())[self.ec_policy]

        got_pieces = set()
        got_indices = set()
        got_durable = []
        for node_index, node in enumerate(nodes):
            df = df_mgr.get_diskfile(node['device'], partition,
                                     'a', self.ec_policy.name, 'o1',
                                     policy=self.ec_policy)
            with df.open():
                meta = df.get_metadata()
                contents = ''.join(df.reader())
                got_pieces.add(contents)

                lmeta = dict((k.lower(), v) for k, v in meta.items())
                got_indices.add(
                    lmeta['x-object-sysmeta-ec-frag-index'])

                self.assertEqual(
                    lmeta['x-object-sysmeta-ec-etag'],
                    md5(obj).hexdigest())
                self.assertEqual(
                    lmeta['x-object-sysmeta-ec-content-length'],
                    str(len(obj)))
                self.assertEqual(
                    lmeta['x-object-sysmeta-ec-segment-size'],
                    '4096')
                self.assertEqual(
                    lmeta['x-object-sysmeta-ec-scheme'],
                    '%s 2+1' % DEFAULT_TEST_EC_TYPE)
                self.assertEqual(
                    lmeta['etag'],
                    md5(contents).hexdigest())

                # check presence for a durable data file for the timestamp
                durable_file = (
                    utils.Timestamp(df.timestamp).internal +
                    '#%s' % lmeta['x-object-sysmeta-ec-frag-index'] +
                    '#d.data')
                durable_file = os.path.join(
                    _testdir, node['device'], storage_directory(
                        diskfile.get_data_dir(self.ec_policy),
                        partition, hash_path('a', self.ec_policy.name, 'o1')),
                    durable_file)
                if os.path.isfile(durable_file):
                    got_durable.append(True)

        self.assertEqual(expected_pieces, got_pieces)
        self.assertEqual(set(('0', '1', '2')), got_indices)

        # verify at least 2 puts made it all the way to the end of 2nd
        # phase, ie at least 2 durable statuses were written
        num_durable_puts = sum(d is True for d in got_durable)
        self.assertGreaterEqual(num_durable_puts, 2)

    def test_PUT_ec_multiple_segments(self):
        self.put_container(self.ec_policy.name, self.ec_policy.name)

        pyeclib_header_size = len(self.ec_policy.pyeclib_driver.encode("")[0])
        segment_size = self.ec_policy.ec_segment_size

        # Big enough to have multiple segments. Also a multiple of the
        # segment size to get coverage of that path too.
        obj = 'ABC' * segment_size

        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%s/o2 HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'Content-Length: %d\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 '\r\n%s' % (self.ec_policy.name, len(obj), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

        # it's a 2+1 erasure code, so each fragment archive should be half
        # the length of the object, plus three inline pyeclib metadata
        # things (one per segment)
        expected_length = (len(obj) / 2 + pyeclib_header_size * 3)

        partition, nodes = self.ec_policy.object_ring.get_nodes(
            'a', self.ec_policy.name, 'o2')

        conf = {'devices': _testdir, 'mount_check': 'false'}
        df_mgr = diskfile.DiskFileRouter(conf, FakeLogger())[self.ec_policy]

        got_durable = []
        fragment_archives = []
        for node in nodes:
            df = df_mgr.get_diskfile(
                node['device'], partition, 'a',
                self.ec_policy.name, 'o2', policy=self.ec_policy)
            with df.open():
                meta = df.get_metadata()
                contents = ''.join(df.reader())
                fragment_archives.append(contents)
                self.assertEqual(len(contents), expected_length)

                durable_file = (
                    utils.Timestamp(df.timestamp).internal +
                    '#%s' % meta['X-Object-Sysmeta-Ec-Frag-Index'] +
                    '#d.data')
                durable_file = os.path.join(
                    _testdir, node['device'], storage_directory(
                        diskfile.get_data_dir(self.ec_policy),
                        partition, hash_path('a', self.ec_policy.name, 'o2')),
                    durable_file)
                if os.path.isfile(durable_file):
                    got_durable.append(True)

        # Verify that we can decode each individual fragment and that they
        # are all the correct size
        fragment_size = self.ec_policy.fragment_size
        nfragments = int(
            math.ceil(float(len(fragment_archives[0])) / fragment_size))

        for fragment_index in range(nfragments):
            fragment_start = fragment_index * fragment_size
            fragment_end = (fragment_index + 1) * fragment_size

            try:
                frags = [fa[fragment_start:fragment_end]
                         for fa in fragment_archives]
                seg = self.ec_policy.pyeclib_driver.decode(frags)
            except ECDriverError:
                self.fail("Failed to decode fragments %d; this probably "
                          "means the fragments are not the sizes they "
                          "should be" % fragment_index)

            segment_start = fragment_index * segment_size
            segment_end = (fragment_index + 1) * segment_size

            self.assertEqual(seg, obj[segment_start:segment_end])

        # verify at least 2 puts made it all the way to the end of 2nd
        # phase, ie at least 2 .durable statuses were written
        num_durable_puts = sum(d is True for d in got_durable)
        self.assertGreaterEqual(num_durable_puts, 2)

    def test_PUT_ec_object_etag_mismatch(self):
        self.put_container(self.ec_policy.name, self.ec_policy.name)

        obj = '90:6A:02:60:B1:08-96da3e706025537fc42464916427727e'
        prolis = _test_sockets[0]
        prosrv = _test_servers[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%s/o3 HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'Etag: %s\r\n'
                 'Content-Length: %d\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 '\r\n%s' % (self.ec_policy.name,
                             md5('something else').hexdigest(),
                             len(obj), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 422'
        self.assertEqual(headers[:len(exp)], exp)

        # nothing should have made it to disk on the object servers
        partition, nodes = prosrv.get_object_ring(
            int(self.ec_policy)).get_nodes('a', self.ec_policy.name, 'o3')
        conf = {'devices': _testdir, 'mount_check': 'false'}

        df_mgr = diskfile.DiskFileRouter(conf, FakeLogger())[self.ec_policy]

        for node in nodes:
            df = df_mgr.get_diskfile(node['device'], partition,
                                     'a', self.ec_policy.name, 'o3',
                                     policy=self.ec_policy)
            self.assertRaises(DiskFileNotExist, df.open)

    def test_PUT_ec_fragment_archive_etag_mismatch(self):
        self.put_container(self.ec_policy.name, self.ec_policy.name)

        # Cause a hash mismatch by feeding one particular MD5 hasher some
        # extra data. The goal here is to get exactly more than one of the
        # hashers in an object server.
        count = (
            self.ec_policy.object_ring.replica_count - self.ec_policy.ec_ndata)
        countdown = [count]

        def busted_md5_constructor(initial_str=""):
            hasher = md5(initial_str)
            if countdown[0] > 0:
                hasher.update('wrong')
            countdown[0] -= 1
            return hasher

        obj = 'uvarovite-esurience-cerated-symphysic'
        prolis = _test_sockets[0]
        prosrv = _test_servers[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        with mock.patch('swift.obj.diskfile.md5',
                        busted_md5_constructor):
            fd = sock.makefile()
            fd.write('PUT /v1/a/%s/pimento HTTP/1.1\r\n'
                     'Host: localhost\r\n'
                     'Connection: close\r\n'
                     'Etag: %s\r\n'
                     'Content-Length: %d\r\n'
                     'X-Storage-Token: t\r\n'
                     'Content-Type: application/octet-stream\r\n'
                     '\r\n%s' % (self.ec_policy.name, md5(obj).hexdigest(),
                                 len(obj), obj))
            fd.flush()
            headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 503'  # no quorum
        self.assertEqual(headers[:len(exp)], exp)

        # replica count - 1 of the fragment archives should have
        # landed on disk
        partition, nodes = prosrv.get_object_ring(
            int(self.ec_policy)).get_nodes('a', self.ec_policy.name, 'pimento')
        conf = {'devices': _testdir, 'mount_check': 'false'}

        df_mgr = diskfile.DiskFileRouter(conf, FakeLogger())[self.ec_policy]

        found = 0
        for node in nodes:
            df = df_mgr.get_diskfile(node['device'], partition,
                                     'a', self.ec_policy.name, 'pimento',
                                     policy=self.ec_policy)
            try:
                # diskfile open won't succeed because no durable was written,
                # so look under the hood for data files.
                files = os.listdir(df._datadir)
                if len(files) > 0:
                    # Although the third fragment archive hasn't landed on
                    # disk, the directory df._datadir is pre-maturely created
                    # and is empty when we use O_TMPFILE + linkat()
                    num_data_files = \
                        len([f for f in files if f.endswith('.data')])
                    self.assertEqual(1, num_data_files)
                    found += 1
            except OSError:
                pass
        self.assertEqual(found, self.ec_policy.ec_ndata)

    def test_PUT_ec_fragment_quorum_archive_etag_mismatch(self):
        self.put_container("ec", "ec-con")

        def busted_md5_constructor(initial_str=""):
            hasher = md5(initial_str)
            hasher.update('wrong')
            return hasher

        obj = 'uvarovite-esurience-cerated-symphysic'
        prolis = _test_sockets[0]
        prosrv = _test_servers[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))

        call_count = [0]

        def mock_committer(self):
            call_count[0] += 1

        commit_confirmation = \
            'swift.proxy.controllers.obj.MIMEPutter.send_commit_confirmation'

        with mock.patch('swift.obj.server.md5', busted_md5_constructor), \
                mock.patch(commit_confirmation, mock_committer):
            fd = sock.makefile()
            fd.write('PUT /v1/a/ec-con/quorum HTTP/1.1\r\n'
                     'Host: localhost\r\n'
                     'Connection: close\r\n'
                     'Etag: %s\r\n'
                     'Content-Length: %d\r\n'
                     'X-Storage-Token: t\r\n'
                     'Content-Type: application/octet-stream\r\n'
                     '\r\n%s' % (md5(obj).hexdigest(), len(obj), obj))
            fd.flush()
            headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 503'  # no quorum
        self.assertEqual(headers[:len(exp)], exp)
        # Don't send commit to object-server if quorum responses consist of 4xx
        self.assertEqual(0, call_count[0])

        # no fragment archives should have landed on disk
        partition, nodes = prosrv.get_object_ring(3).get_nodes(
            'a', 'ec-con', 'quorum')
        conf = {'devices': _testdir, 'mount_check': 'false'}

        df_mgr = diskfile.DiskFileRouter(conf, FakeLogger())[self.ec_policy]

        for node in nodes:
            df = df_mgr.get_diskfile(node['device'], partition,
                                     'a', 'ec-con', 'quorum',
                                     policy=self.ec_policy)
            if os.path.exists(df._datadir):
                self.assertFalse(os.listdir(df._datadir))  # should be empty

    def test_PUT_ec_fragment_quorum_bad_request(self):
        self.put_container("ec", "ec-con")

        obj = 'uvarovite-esurience-cerated-symphysic'
        prolis = _test_sockets[0]
        prosrv = _test_servers[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))

        call_count = [0]

        def mock_committer(self):
            call_count[0] += 1

        read_footer = \
            'swift.obj.server.ObjectController._read_metadata_footer'
        commit_confirmation = \
            'swift.proxy.controllers.obj.MIMEPutter.send_commit_confirmation'

        with mock.patch(read_footer) as read_footer_call, \
                mock.patch(commit_confirmation, mock_committer):
            # Emulate missing footer MIME doc in all object-servers
            read_footer_call.side_effect = HTTPBadRequest(
                body="couldn't find footer MIME doc")

            fd = sock.makefile()
            fd.write('PUT /v1/a/ec-con/quorum HTTP/1.1\r\n'
                     'Host: localhost\r\n'
                     'Connection: close\r\n'
                     'Etag: %s\r\n'
                     'Content-Length: %d\r\n'
                     'X-Storage-Token: t\r\n'
                     'Content-Type: application/octet-stream\r\n'
                     '\r\n%s' % (md5(obj).hexdigest(), len(obj), obj))
            fd.flush()
            headers = readuntil2crlfs(fd)

        # Don't show a result of the bad conversation between proxy-server
        # and object-server
        exp = 'HTTP/1.1 503'
        self.assertEqual(headers[:len(exp)], exp)
        # Don't send commit to object-server if quorum responses consist of 4xx
        self.assertEqual(0, call_count[0])

        # no fragment archives should have landed on disk
        partition, nodes = prosrv.get_object_ring(3).get_nodes(
            'a', 'ec-con', 'quorum')
        conf = {'devices': _testdir, 'mount_check': 'false'}

        df_mgr = diskfile.DiskFileRouter(conf, FakeLogger())[self.ec_policy]

        for node in nodes:
            df = df_mgr.get_diskfile(node['device'], partition,
                                     'a', 'ec-con', 'quorum',
                                     policy=self.ec_policy)
            if os.path.exists(df._datadir):
                self.assertFalse(os.listdir(df._datadir))  # should be empty

    def test_PUT_ec_if_none_match(self):
        self.put_container(self.ec_policy.name, self.ec_policy.name)

        obj = 'ananepionic-lepidophyllous-ropewalker-neglectful'
        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%s/inm HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'Etag: "%s"\r\n'
                 'Content-Length: %d\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 '\r\n%s' % (self.ec_policy.name, md5(obj).hexdigest(),
                             len(obj), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%s/inm HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'If-None-Match: *\r\n'
                 'Etag: "%s"\r\n'
                 'Content-Length: %d\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 '\r\n%s' % (self.ec_policy.name, md5(obj).hexdigest(),
                             len(obj), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 412'
        self.assertEqual(headers[:len(exp)], exp)

    def test_GET_ec(self):
        prolis = _test_sockets[0]
        prosrv = _test_servers[0]

        self.put_container(self.ec_policy.name, self.ec_policy.name)

        obj = '0123456' * 11 * 17

        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%s/go-get-it HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'Content-Length: %d\r\n'
                 'X-Storage-Token: t\r\n'
                 'X-Object-Meta-Color: chartreuse\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 '\r\n%s' % (self.ec_policy.name, len(obj), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/%s/go-get-it HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 '\r\n' % self.ec_policy.name)
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEqual(headers[:len(exp)], exp)

        headers = parse_headers_string(headers)
        self.assertEqual(str(len(obj)), headers['Content-Length'])
        self.assertEqual(md5(obj).hexdigest(), headers['Etag'])
        self.assertEqual('chartreuse', headers['X-Object-Meta-Color'])

        gotten_obj = ''
        while True:
            buf = fd.read(64)
            if not buf:
                break
            gotten_obj += buf
        self.assertEqual(gotten_obj, obj)
        error_lines = prosrv.logger.get_lines_for_level('error')
        warn_lines = prosrv.logger.get_lines_for_level('warning')
        self.assertEqual(len(error_lines), 0)  # sanity
        self.assertEqual(len(warn_lines), 0)  # sanity

    def test_conditional_GET_ec(self):
        # sanity
        self.assertEqual('erasure_coding', self.ec_policy.policy_type)
        self._test_conditional_GET(self.ec_policy)

    def test_GET_ec_big(self):
        prolis = _test_sockets[0]
        prosrv = _test_servers[0]

        self.put_container(self.ec_policy.name, self.ec_policy.name)

        # our EC segment size is 4 KiB, so this is multiple (3) segments;
        # we'll verify that with a sanity check
        obj = 'a moose once bit my sister' * 400
        self.assertGreater(
            len(obj), self.ec_policy.ec_segment_size * 2,
            "object is too small for proper testing")

        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%s/big-obj-get HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'Content-Length: %d\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 '\r\n%s' % (self.ec_policy.name, len(obj), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/%s/big-obj-get HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 '\r\n' % self.ec_policy.name)
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEqual(headers[:len(exp)], exp)

        headers = parse_headers_string(headers)
        self.assertEqual(str(len(obj)), headers['Content-Length'])
        self.assertEqual(md5(obj).hexdigest(), headers['Etag'])

        gotten_obj = ''
        while True:
            buf = fd.read(64)
            if not buf:
                break
            gotten_obj += buf
        # This may look like a redundant test, but when things fail, this
        # has a useful failure message while the subsequent one spews piles
        # of garbage and demolishes your terminal's scrollback buffer.
        self.assertEqual(len(gotten_obj), len(obj))
        self.assertEqual(gotten_obj, obj)
        error_lines = prosrv.logger.get_lines_for_level('error')
        warn_lines = prosrv.logger.get_lines_for_level('warning')
        self.assertEqual(len(error_lines), 0)  # sanity
        self.assertEqual(len(warn_lines), 0)  # sanity

    def test_GET_ec_failure_handling(self):
        self.put_container(self.ec_policy.name, self.ec_policy.name)

        obj = 'look at this object; it is simply amazing ' * 500
        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%s/crash-test-dummy HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'Content-Length: %d\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 '\r\n%s' % (self.ec_policy.name, len(obj), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

        def explodey_iter(inner_iter):
            yield next(inner_iter)
            raise Exception("doom ba doom")

        def explodey_doc_parts_iter(inner_iter_iter):
            try:
                for item in inner_iter_iter:
                    item = item.copy()  # paranoia about mutable data
                    item['part_iter'] = explodey_iter(item['part_iter'])
                    yield item
            except GeneratorExit:
                inner_iter_iter.close()
                raise

        real_ec_app_iter = swift.proxy.controllers.obj.ECAppIter

        def explodey_ec_app_iter(path, policy, iterators, *a, **kw):
            # Each thing in `iterators` here is a document-parts iterator,
            # and we want to fail after getting a little into each part.
            #
            # That way, we ensure we've started streaming the response to
            # the client when things go wrong.
            return real_ec_app_iter(
                path, policy,
                [explodey_doc_parts_iter(i) for i in iterators],
                *a, **kw)

        with mock.patch("swift.proxy.controllers.obj.ECAppIter",
                        explodey_ec_app_iter):
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('GET /v1/a/%s/crash-test-dummy HTTP/1.1\r\n'
                     'Host: localhost\r\n'
                     'Connection: close\r\n'
                     'X-Storage-Token: t\r\n'
                     '\r\n' % self.ec_policy.name)
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = 'HTTP/1.1 200'
            self.assertEqual(headers[:len(exp)], exp)

            headers = parse_headers_string(headers)
            self.assertEqual(str(len(obj)), headers['Content-Length'])
            self.assertEqual(md5(obj).hexdigest(), headers['Etag'])

            gotten_obj = ''
            try:
                # don't hang the test run when this fails
                with Timeout(300):
                    while True:
                        buf = fd.read(64)
                        if not buf:
                            break
                        gotten_obj += buf
            except Timeout:
                self.fail("GET hung when connection failed")

            # Ensure we failed partway through, otherwise the mocks could
            # get out of date without anyone noticing
            self.assertTrue(0 < len(gotten_obj) < len(obj))

    def test_HEAD_ec(self):
        prolis = _test_sockets[0]
        prosrv = _test_servers[0]

        self.put_container(self.ec_policy.name, self.ec_policy.name)

        obj = '0123456' * 11 * 17

        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%s/go-head-it HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'Content-Length: %d\r\n'
                 'X-Storage-Token: t\r\n'
                 'X-Object-Meta-Color: chartreuse\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 '\r\n%s' % (self.ec_policy.name, len(obj), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('HEAD /v1/a/%s/go-head-it HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 '\r\n' % self.ec_policy.name)
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEqual(headers[:len(exp)], exp)

        headers = parse_headers_string(headers)
        self.assertEqual(str(len(obj)), headers['Content-Length'])
        self.assertEqual(md5(obj).hexdigest(), headers['Etag'])
        self.assertEqual('chartreuse', headers['X-Object-Meta-Color'])

        error_lines = prosrv.logger.get_lines_for_level('error')
        warn_lines = prosrv.logger.get_lines_for_level('warning')
        self.assertEqual(len(error_lines), 0)  # sanity
        self.assertEqual(len(warn_lines), 0)  # sanity

    def test_GET_ec_404(self):
        prolis = _test_sockets[0]
        prosrv = _test_servers[0]

        self.put_container(self.ec_policy.name, self.ec_policy.name)

        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/%s/yes-we-have-no-bananas HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 '\r\n' % self.ec_policy.name)
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 404'
        self.assertEqual(headers[:len(exp)], exp)

        error_lines = prosrv.logger.get_lines_for_level('error')
        warn_lines = prosrv.logger.get_lines_for_level('warning')
        self.assertEqual(len(error_lines), 0)  # sanity
        self.assertEqual(len(warn_lines), 0)  # sanity

    def test_HEAD_ec_404(self):
        prolis = _test_sockets[0]
        prosrv = _test_servers[0]

        self.put_container(self.ec_policy.name, self.ec_policy.name)

        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('HEAD /v1/a/%s/yes-we-have-no-bananas HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 '\r\n' % self.ec_policy.name)
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 404'
        self.assertEqual(headers[:len(exp)], exp)

        error_lines = prosrv.logger.get_lines_for_level('error')
        warn_lines = prosrv.logger.get_lines_for_level('warning')
        self.assertEqual(len(error_lines), 0)  # sanity
        self.assertEqual(len(warn_lines), 0)  # sanity

    def test_reload_ring_ec(self):
        self.put_container("ec", "ec-con")

        orig_rtime = self.ec_policy.object_ring._rtime
        orig_replica_count = self.ec_policy.object_ring.replica_count
        # save original file as back up
        copyfile(self.ec_policy.object_ring.serialized_path,
                 self.ec_policy.object_ring.serialized_path + '.bak')

        try:
            # overwrite with 2 replica, 2 devices ring
            obj_devs = []
            obj_devs.append(
                {'port': _test_sockets[-3].getsockname()[1],
                 'device': 'sdg1'})
            obj_devs.append(
                {'port': _test_sockets[-2].getsockname()[1],
                 'device': 'sdh1'})
            write_fake_ring(self.ec_policy.object_ring.serialized_path,
                            *obj_devs)

            def get_ring_reloaded_response(method):
                # force to reload at the request
                self.ec_policy.object_ring._rtime = 0

                trans_data = ['%s /v1/a/ec-con/o2 HTTP/1.1\r\n' % method,
                              'Host: localhost\r\n',
                              'Connection: close\r\n',
                              'X-Storage-Token: t\r\n']

                if method == 'PUT':
                    # small, so we don't get multiple EC stripes
                    obj = 'abCD' * 10

                    extra_trans_data = [
                        'Etag: "%s"\r\n' % md5(obj).hexdigest(),
                        'Content-Length: %d\r\n' % len(obj),
                        'Content-Type: application/octet-stream\r\n',
                        '\r\n%s' % obj
                    ]
                    trans_data.extend(extra_trans_data)
                else:
                    trans_data.append('\r\n')

                prolis = _test_sockets[0]
                sock = connect_tcp(('localhost', prolis.getsockname()[1]))
                fd = sock.makefile()
                fd.write(''.join(trans_data))
                fd.flush()
                headers = readuntil2crlfs(fd)

                # use older ring with rollbacking
                return headers

            for method in ('PUT', 'HEAD', 'GET', 'POST', 'DELETE'):
                headers = get_ring_reloaded_response(method)
                exp = 'HTTP/1.1 20'
                self.assertEqual(headers[:len(exp)], exp)

                # proxy didn't load newest ring, use older one
                self.assertEqual(orig_replica_count,
                                 self.ec_policy.object_ring.replica_count)

                if method == 'POST':
                    headers = get_ring_reloaded_response(method)

                    exp = 'HTTP/1.1 20'
                    self.assertEqual(headers[:len(exp)], exp)
                    # sanity
                    self.assertEqual(orig_replica_count,
                                     self.ec_policy.object_ring.replica_count)

        finally:
            self.ec_policy.object_ring._rtime = orig_rtime
            os.rename(self.ec_policy.object_ring.serialized_path + '.bak',
                      self.ec_policy.object_ring.serialized_path)

    def test_ec_client_disconnect(self):
        prolis = _test_sockets[0]

        # create connection
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()

        # create container
        fd.write('PUT /v1/a/%s-discon HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Content-Length: 0\r\n'
                 'X-Storage-Token: t\r\n'
                 'X-Storage-Policy: %s\r\n'
                 '\r\n' % (self.ec_policy.name, self.ec_policy.name))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 2'
        self.assertEqual(headers[:len(exp)], exp)

        # create object
        obj = 'a' * 4 * 64 * 2 ** 10
        fd.write('PUT /v1/a/%s-discon/test HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Content-Length: %d\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Type: donuts\r\n'
                 '\r\n%s' % (self.ec_policy.name, len(obj), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

        class WrappedTimeout(ChunkWriteTimeout):
            def __enter__(self):
                timeouts[self] = traceback.extract_stack()
                return super(WrappedTimeout, self).__enter__()

            def __exit__(self, typ, value, tb):
                timeouts[self] = None
                return super(WrappedTimeout, self).__exit__(typ, value, tb)

        timeouts = {}
        with mock.patch('swift.proxy.controllers.base.ChunkWriteTimeout',
                        WrappedTimeout):
            with mock.patch.object(_test_servers[0], 'client_timeout', new=5):
                # get object
                fd.write('GET /v1/a/%s-discon/test HTTP/1.1\r\n'
                         'Host: localhost\r\n'
                         'Connection: close\r\n'
                         'X-Storage-Token: t\r\n'
                         '\r\n' % self.ec_policy.name)
                fd.flush()
                headers = readuntil2crlfs(fd)
                exp = 'HTTP/1.1 200'
                self.assertEqual(headers[:len(exp)], exp)

                # read most of the object, and disconnect
                fd.read(10)
                sock.fd._sock.close()
                self._sleep_enough(
                    lambda:
                    _test_servers[0].logger.get_lines_for_level('warning'))

        # check for disconnect message!
        expected = ['Client disconnected on read'] * 2
        self.assertEqual(
            _test_servers[0].logger.get_lines_for_level('warning'),
            expected)
        # check that no coro was left waiting to write
        self.assertTrue(timeouts)  # sanity - WrappedTimeout did get called
        missing_exits = [tb for tb in timeouts.values() if tb is not None]
        self.assertFalse(
            missing_exits, 'Failed to exit all ChunkWriteTimeouts.\n' +
            ''.join(['No exit from ChunkWriteTimeout entered at:\n' +
                     ''.join(traceback.format_list(tb)[:-1])
                     for tb in missing_exits]))
        # and check that the ChunkWriteTimeouts did not raise Exceptions
        self.assertFalse(_test_servers[0].logger.get_lines_for_level('error'))

    def test_ec_client_put_disconnect(self):
        prolis = _test_sockets[0]

        # create connection
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()

        # create container
        fd.write('PUT /v1/a/%s-discon HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Content-Length: 0\r\n'
                 'X-Storage-Token: t\r\n'
                 'X-Storage-Policy: %s\r\n'
                 '\r\n' % (self.ec_policy.name, self.ec_policy.name))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 2'
        self.assertEqual(headers[:len(exp)], exp)

        # create object
        obj = 'a' * 4 * 64 * 2 ** 10
        fd.write('PUT /v1/a/%s-discon/test HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Content-Length: %d\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Type: donuts\r\n'
                 '\r\n%s' % (self.ec_policy.name, len(obj), obj[:-10]))
        fd.flush()
        fd.close()
        sock.close()
        # sleep to trampoline enough
        condition = \
            lambda: _test_servers[0].logger.get_lines_for_level('warning')
        self._sleep_enough(condition)
        expected = ['Client disconnected without sending enough data']
        warns = _test_servers[0].logger.get_lines_for_level('warning')
        self.assertEqual(expected, warns)
        errors = _test_servers[0].logger.get_lines_for_level('error')
        self.assertEqual([], errors)


class TestECObjectController(BaseTestECObjectController, unittest.TestCase):
    def setUp(self):
        skip_if_no_xattrs()
        self.ec_policy = POLICIES[3]
        super(TestECObjectController, self).setUp()


class TestECDuplicationObjectController(
        BaseTestECObjectController, unittest.TestCase):
    def setUp(self):
        skip_if_no_xattrs()
        self.ec_policy = POLICIES[4]
        super(TestECDuplicationObjectController, self).setUp()


class TestECMismatchedFA(unittest.TestCase):
    def setUp(self):
        skip_if_no_xattrs()

    def tearDown(self):
        prosrv = _test_servers[0]
        # don't leak error limits and poison other tests
        prosrv._error_limiting = {}

    def test_mixing_different_objects_fragment_archives(self):
        (prosrv, acc1srv, acc2srv, con1srv, con2srv, obj1srv,
         obj2srv, obj3srv, obj4srv, obj5srv, obj6srv) = _test_servers

        ec_policy = POLICIES[3]

        @public
        def bad_disk(req):
            return Response(status=507, body="borken")

        ensure_container = Request.blank(
            "/v1/a/ec-crazytown",
            environ={"REQUEST_METHOD": "PUT"},
            headers={"X-Storage-Policy": "ec", "X-Auth-Token": "t"})
        resp = ensure_container.get_response(prosrv)
        self.assertIn(resp.status_int, (201, 202))

        obj1 = "first version..."
        put_req1 = Request.blank(
            "/v1/a/ec-crazytown/obj",
            environ={"REQUEST_METHOD": "PUT"},
            headers={"X-Auth-Token": "t"})
        put_req1.body = obj1

        obj2 = u"versión segundo".encode("utf-8")
        put_req2 = Request.blank(
            "/v1/a/ec-crazytown/obj",
            environ={"REQUEST_METHOD": "PUT"},
            headers={"X-Auth-Token": "t"})
        put_req2.body = obj2

        # pyeclib has checks for unequal-length; we don't want to trip those
        self.assertEqual(len(obj1), len(obj2))

        # Server obj1 will have the first version of the object (obj2 also
        # gets it, but that gets stepped on later)
        prosrv._error_limiting = {}
        with mock.patch.object(obj3srv, 'PUT', bad_disk), \
                mock.patch(
                    'swift.common.storage_policy.ECStoragePolicy.quorum'):
            type(ec_policy).quorum = mock.PropertyMock(return_value=2)
            resp = put_req1.get_response(prosrv)
        self.assertEqual(resp.status_int, 201)

        # Servers obj2 and obj3 will have the second version of the object.
        prosrv._error_limiting = {}
        with mock.patch.object(obj1srv, 'PUT', bad_disk), \
                mock.patch(
                    'swift.common.storage_policy.ECStoragePolicy.quorum'):
            type(ec_policy).quorum = mock.PropertyMock(return_value=2)
            resp = put_req2.get_response(prosrv)
        self.assertEqual(resp.status_int, 201)

        # A GET that only sees 1 fragment archive should fail
        get_req = Request.blank("/v1/a/ec-crazytown/obj",
                                environ={"REQUEST_METHOD": "GET"},
                                headers={"X-Auth-Token": "t"})
        prosrv._error_limiting = {}
        with mock.patch.object(obj1srv, 'GET', bad_disk), \
                mock.patch.object(obj2srv, 'GET', bad_disk):
            resp = get_req.get_response(prosrv)
        self.assertEqual(resp.status_int, 503)

        # A GET that sees 2 matching FAs will work
        get_req = Request.blank("/v1/a/ec-crazytown/obj",
                                environ={"REQUEST_METHOD": "GET"},
                                headers={"X-Auth-Token": "t"})
        prosrv._error_limiting = {}
        with mock.patch.object(obj1srv, 'GET', bad_disk):
            resp = get_req.get_response(prosrv)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body, obj2)

        # A GET that sees 2 mismatching FAs will fail
        get_req = Request.blank("/v1/a/ec-crazytown/obj",
                                environ={"REQUEST_METHOD": "GET"},
                                headers={"X-Auth-Token": "t"})
        prosrv._error_limiting = {}
        with mock.patch.object(obj2srv, 'GET', bad_disk):
            resp = get_req.get_response(prosrv)
        self.assertEqual(resp.status_int, 503)

    def test_mixing_different_objects_fragment_archives_with_dup_factor(self):
        (prosrv, acc1srv, acc2srv, con1srv, con2srv, obj1srv,
         obj2srv, obj3srv, obj4srv, obj5srv, obj6srv) = _test_servers

        ec_policy = POLICIES[4]

        @public
        def bad_disk(req):
            return Response(status=507, body="borken")

        ensure_container = Request.blank(
            "/v1/a/ec-dup-crazytown",
            environ={"REQUEST_METHOD": "PUT"},
            headers={"X-Storage-Policy": "ec-dup", "X-Auth-Token": "t"})
        resp = ensure_container.get_response(prosrv)
        self.assertIn(resp.status_int, (201, 202))

        obj1 = "first version..."
        put_req1 = Request.blank(
            "/v1/a/ec-dup-crazytown/obj",
            environ={"REQUEST_METHOD": "PUT"},
            headers={"X-Auth-Token": "t"})
        put_req1.body = obj1

        obj2 = u"versión segundo".encode("utf-8")
        put_req2 = Request.blank(
            "/v1/a/ec-dup-crazytown/obj",
            environ={"REQUEST_METHOD": "PUT"},
            headers={"X-Auth-Token": "t"})
        put_req2.body = obj2

        # pyeclib has checks for unequal-length; we don't want to trip those
        self.assertEqual(len(obj1), len(obj2))

        # First subset of object server will have the first version of the
        # object
        prosrv._error_limiting = {}
        with mock.patch.object(obj4srv, 'PUT', bad_disk), \
                mock.patch.object(obj5srv, 'PUT', bad_disk), \
                mock.patch.object(obj6srv, 'PUT', bad_disk), \
                mock.patch(
                    'swift.common.storage_policy.ECStoragePolicy.quorum'):
            type(ec_policy).quorum = mock.PropertyMock(return_value=3)
            resp = put_req1.get_response(prosrv)
        self.assertEqual(resp.status_int, 201)

        # Second subset will have the second version of the object.
        prosrv._error_limiting = {}
        with mock.patch.object(obj1srv, 'PUT', bad_disk), \
                mock.patch.object(obj2srv, 'PUT', bad_disk), \
                mock.patch.object(obj3srv, 'PUT', bad_disk), \
                mock.patch(
                    'swift.common.storage_policy.ECStoragePolicy.quorum'):
            type(ec_policy).quorum = mock.PropertyMock(return_value=3)
            resp = put_req2.get_response(prosrv)
        self.assertEqual(resp.status_int, 201)

        # A GET that only sees 1 fragment archive should fail
        get_req = Request.blank("/v1/a/ec-dup-crazytown/obj",
                                environ={"REQUEST_METHOD": "GET"},
                                headers={"X-Auth-Token": "t"})
        prosrv._error_limiting = {}
        with mock.patch.object(obj2srv, 'GET', bad_disk), \
                mock.patch.object(obj3srv, 'GET', bad_disk), \
                mock.patch.object(obj4srv, 'GET', bad_disk), \
                mock.patch.object(obj5srv, 'GET', bad_disk), \
                mock.patch.object(obj6srv, 'GET', bad_disk):
            resp = get_req.get_response(prosrv)
        self.assertEqual(resp.status_int, 503)

        # A GET that sees 2 matching FAs will work
        get_req = Request.blank("/v1/a/ec-dup-crazytown/obj",
                                environ={"REQUEST_METHOD": "GET"},
                                headers={"X-Auth-Token": "t"})
        prosrv._error_limiting = {}
        with mock.patch.object(obj1srv, 'GET', bad_disk), \
                mock.patch.object(obj2srv, 'GET', bad_disk), \
                mock.patch.object(obj3srv, 'GET', bad_disk), \
                mock.patch.object(obj6srv, 'GET', bad_disk):
            resp = get_req.get_response(prosrv)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body, obj2)

        # A GET that sees 2 mismatching FAs will fail
        get_req = Request.blank("/v1/a/ec-dup-crazytown/obj",
                                environ={"REQUEST_METHOD": "GET"},
                                headers={"X-Auth-Token": "t"})
        prosrv._error_limiting = {}
        with mock.patch.object(obj2srv, 'GET', bad_disk), \
                mock.patch.object(obj3srv, 'GET', bad_disk), \
                mock.patch.object(obj4srv, 'GET', bad_disk), \
                mock.patch.object(obj6srv, 'GET', bad_disk):
            resp = get_req.get_response(prosrv)
        self.assertEqual(resp.status_int, 503)


class TestECGets(unittest.TestCase):
    def setUp(self):
        super(TestECGets, self).setUp()
        skip_if_no_xattrs()
        self.tempdir = mkdtemp()

    def tearDown(self):
        rmtree(self.tempdir, ignore_errors=True)
        prosrv = _test_servers[0]
        # don't leak error limits and poison other tests
        prosrv._error_limiting = {}
        super(TestECGets, self).tearDown()

    def _setup_nodes_and_do_GET(self, objs, node_state):
        """
        A helper method that creates object fragments, stashes them in temp
        dirs, and then moves selected fragments back into the hash_dirs on each
        node according to a specified desired node state description.

        :param objs: a dict that maps object references to dicts that describe
                     the object timestamp and content. Object frags will be
                     created for each item in this dict.
        :param node_state: a dict that maps a node index to the desired state
                           for that node. Each desired state is a list of
                           dicts, with each dict describing object reference,
                           frag_index and whether the file moved to the node's
                           hash_dir should be marked as durable or not.
        """
        (prosrv, acc1srv, acc2srv, con1srv, con2srv, obj1srv,
         obj2srv, obj3srv, _obj4srv, _obj5srv, _obj6srv) = _test_servers
        ec_policy = POLICIES[3]
        container_name = uuid.uuid4().hex
        obj_name = uuid.uuid4().hex
        obj_path = os.path.join(os.sep, 'v1', 'a', container_name, obj_name)

        # PUT container, make sure it worked
        container_path = os.path.join(os.sep, 'v1', 'a', container_name)
        ec_container = Request.blank(
            container_path, environ={"REQUEST_METHOD": "PUT"},
            headers={"X-Storage-Policy": "ec", "X-Auth-Token": "t"})
        resp = ec_container.get_response(prosrv)
        self.assertIn(resp.status_int, (201, 202))

        partition, nodes = \
            ec_policy.object_ring.get_nodes('a', container_name, obj_name)

        # map nodes to hash dirs
        node_hash_dirs = {}
        node_tmp_dirs = collections.defaultdict(dict)
        for node in nodes:
            node_hash_dirs[node['index']] = os.path.join(
                _testdir, node['device'], storage_directory(
                    diskfile.get_data_dir(ec_policy),
                    partition, hash_path('a', container_name, obj_name)))

        def _put_object(ref, timestamp, body):
            # PUT an object and then move its disk files to a temp dir
            headers = {"X-Timestamp": timestamp.internal}
            put_req1 = Request.blank(obj_path, method='PUT', headers=headers)
            put_req1.body = body
            resp = put_req1.get_response(prosrv)
            self.assertEqual(resp.status_int, 201)

            # GET the obj, should work fine
            get_req = Request.blank(obj_path, method="GET")
            resp = get_req.get_response(prosrv)
            self.assertEqual(resp.status_int, 200)
            self.assertEqual(resp.body, body)

            # move all hash dir files to per-node, per-obj tempdir
            for node_index, hash_dir in node_hash_dirs.items():
                node_tmp_dirs[node_index][ref] = mkdtemp(dir=self.tempdir)
                for f in os.listdir(hash_dir):
                    move(os.path.join(hash_dir, f),
                         os.path.join(node_tmp_dirs[node_index][ref], f))

        for obj_ref, obj_info in objs.items():
            _put_object(obj_ref, **obj_info)

        # sanity check - all hash_dirs are empty and GET returns a 404
        for hash_dir in node_hash_dirs.values():
            self.assertFalse(os.listdir(hash_dir))
        get_req = Request.blank(obj_path, method="GET")
        resp = get_req.get_response(prosrv)
        self.assertEqual(resp.status_int, 404)

        # node state is in form:
        # {node_index: [{ref: object reference,
        #                frag_index: index,
        #                durable: True or False}, ...],
        #  node_index: ...}
        for node_index, state in node_state.items():
            dest = node_hash_dirs[node_index]
            for frag_info in state:
                src = node_tmp_dirs[frag_info['frag_index']][frag_info['ref']]
                src_files = os.listdir(src)
                # sanity check, expect just a single .data file
                self.assertFalse(src_files[1:])
                dest_file = src_files[0].replace(
                    '#d', '#d' if frag_info['durable'] else '')
                move(os.path.join(src, src_files[0]),
                     os.path.join(dest, dest_file))

        # do an object GET
        get_req = Request.blank(obj_path, method='GET')
        return get_req.get_response(prosrv)

    def test_GET_with_missing_durables(self):
        # verify object GET behavior when durable files are missing
        ts_iter = make_timestamp_iter()
        objs = {'obj1': dict(timestamp=next(ts_iter), body='body')}

        # durable missing from 2/3 nodes
        node_state = {
            0: [dict(ref='obj1', frag_index=0, durable=True)],
            1: [dict(ref='obj1', frag_index=1, durable=False)],
            2: [dict(ref='obj1', frag_index=2, durable=False)]
        }

        resp = self._setup_nodes_and_do_GET(objs, node_state)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body, objs['obj1']['body'])

        # all files missing on 1 node, durable missing from 1/2 other nodes
        # durable missing from 2/3 nodes
        node_state = {
            0: [dict(ref='obj1', frag_index=0, durable=True)],
            1: [],
            2: [dict(ref='obj1', frag_index=2, durable=False)]
        }

        resp = self._setup_nodes_and_do_GET(objs, node_state)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body, objs['obj1']['body'])

        # durable missing from all 3 nodes
        node_state = {
            0: [dict(ref='obj1', frag_index=0, durable=False)],
            1: [dict(ref='obj1', frag_index=1, durable=False)],
            2: [dict(ref='obj1', frag_index=2, durable=False)]
        }

        resp = self._setup_nodes_and_do_GET(objs, node_state)
        self.assertEqual(resp.status_int, 503)

    def test_GET_with_multiple_frags_per_node(self):
        # verify object GET behavior when multiple fragments are on same node
        ts_iter = make_timestamp_iter()
        objs = {'obj1': dict(timestamp=next(ts_iter), body='body')}

        # scenario: only two frags, both on same node
        node_state = {
            0: [],
            1: [dict(ref='obj1', frag_index=0, durable=True),
                dict(ref='obj1', frag_index=1, durable=False)],
            2: []
        }

        resp = self._setup_nodes_and_do_GET(objs, node_state)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body, objs['obj1']['body'])

        # scenario: all 3 frags on same node
        node_state = {
            0: [],
            1: [dict(ref='obj1', frag_index=0, durable=True),
                dict(ref='obj1', frag_index=1, durable=False),
                dict(ref='obj1', frag_index=2, durable=False)],
            2: []
        }

        resp = self._setup_nodes_and_do_GET(objs, node_state)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body, objs['obj1']['body'])

    def test_GET_with_multiple_timestamps_on_nodes(self):
        ts_iter = make_timestamp_iter()

        ts_1, ts_2, ts_3 = [next(ts_iter) for _ in range(3)]
        objs = {'obj1': dict(timestamp=ts_1, body='body1'),
                'obj2': dict(timestamp=ts_2, body='body2'),
                'obj3': dict(timestamp=ts_3, body='body3')}

        # newer non-durable frags do not prevent proxy getting the durable obj1
        node_state = {
            0: [dict(ref='obj3', frag_index=0, durable=False),
                dict(ref='obj2', frag_index=0, durable=False),
                dict(ref='obj1', frag_index=0, durable=True)],
            1: [dict(ref='obj3', frag_index=1, durable=False),
                dict(ref='obj2', frag_index=1, durable=False),
                dict(ref='obj1', frag_index=1, durable=True)],
            2: [dict(ref='obj3', frag_index=2, durable=False),
                dict(ref='obj2', frag_index=2, durable=False),
                dict(ref='obj1', frag_index=2, durable=True)],
        }

        resp = self._setup_nodes_and_do_GET(objs, node_state)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body, objs['obj1']['body'])

        # durable frags at two timestamps: in this scenario proxy is guaranteed
        # to see the durable at ts_2 with one of the first 2 responses, so will
        # then prefer that when requesting from third obj server
        node_state = {
            0: [dict(ref='obj3', frag_index=0, durable=False),
                dict(ref='obj2', frag_index=0, durable=False),
                dict(ref='obj1', frag_index=0, durable=True)],
            1: [dict(ref='obj3', frag_index=1, durable=False),
                dict(ref='obj2', frag_index=1, durable=True)],
            2: [dict(ref='obj3', frag_index=2, durable=False),
                dict(ref='obj2', frag_index=2, durable=True)],
        }

        resp = self._setup_nodes_and_do_GET(objs, node_state)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body, objs['obj2']['body'])

    def test_GET_with_same_frag_index_on_multiple_nodes(self):
        ts_iter = make_timestamp_iter()

        # this is a trick to be able to get identical frags placed onto
        # multiple nodes: since we cannot *copy* frags, we generate three sets
        # of identical frags at same timestamp so we have enough to *move*
        ts_1 = next(ts_iter)
        objs = {'obj1a': dict(timestamp=ts_1, body='body'),
                'obj1b': dict(timestamp=ts_1, body='body'),
                'obj1c': dict(timestamp=ts_1, body='body')}

        # arrange for duplicate frag indexes across nodes: because the object
        # server prefers the highest available frag index, proxy will first get
        # back two responses with frag index 1, and will then return to node 0
        # for frag_index 0.
        node_state = {
            0: [dict(ref='obj1a', frag_index=0, durable=False),
                dict(ref='obj1a', frag_index=1, durable=False)],
            1: [dict(ref='obj1b', frag_index=1, durable=True)],
            2: [dict(ref='obj1c', frag_index=1, durable=True)]
        }

        resp = self._setup_nodes_and_do_GET(objs, node_state)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body, objs['obj1a']['body'])

        # if all we have across nodes are frags with same index then expect a
        # 404 (the third, 'extra', obj server GET will return 404 because it
        # will be sent frag prefs that exclude frag_index 1)
        node_state = {
            0: [dict(ref='obj1a', frag_index=1, durable=False)],
            1: [dict(ref='obj1b', frag_index=1, durable=True)],
            2: [dict(ref='obj1c', frag_index=1, durable=False)]
        }

        resp = self._setup_nodes_and_do_GET(objs, node_state)
        self.assertEqual(resp.status_int, 404)


class TestObjectDisconnectCleanup(unittest.TestCase):

    # update this if you need to make more different devices in do_setup
    device_pattern = re.compile('sd[a-z][0-9]')

    def _cleanup_devices(self):
        # make sure all the object data is cleaned up
        for dev in os.listdir(_testdir):
            if not self.device_pattern.match(dev):
                continue
            device_path = os.path.join(_testdir, dev)
            for datadir in os.listdir(device_path):
                if 'object' not in datadir:
                    continue
                data_path = os.path.join(device_path, datadir)
                rmtree(data_path, ignore_errors=True)
                mkdirs(data_path)

    def setUp(self):
        skip_if_no_xattrs()
        debug.hub_exceptions(False)
        self._cleanup_devices()

    def tearDown(self):
        debug.hub_exceptions(True)
        self._cleanup_devices()

    def _check_disconnect_cleans_up(self, policy_name, is_chunked=False):
        proxy_port = _test_sockets[0].getsockname()[1]

        def put(path, headers=None, body=None):
            conn = httplib.HTTPConnection('localhost', proxy_port)
            try:
                conn.connect()
                conn.putrequest('PUT', path)
                for k, v in (headers or {}).items():
                    conn.putheader(k, v)
                conn.endheaders()
                body = body or ['']
                for chunk in body:
                    if is_chunked:
                        chunk = '%x\r\n%s\r\n' % (len(chunk), chunk)
                    conn.send(chunk)
                resp = conn.getresponse()
                body = resp.read()
            finally:
                # seriously - shut this mother down
                if conn.sock:
                    conn.sock.fd._sock.close()
            return resp, body

        # ensure container
        container_path = '/v1/a/%s-disconnect-test' % policy_name
        resp, _body = put(container_path, headers={
            'Connection': 'close',
            'X-Storage-Policy': policy_name,
            'Content-Length': '0',
        })
        self.assertIn(resp.status, (201, 202))

        def exploding_body():
            for i in range(3):
                yield '\x00' * (64 * 2 ** 10)
            raise Exception('kaboom!')

        headers = {}
        if is_chunked:
            headers['Transfer-Encoding'] = 'chunked'
        else:
            headers['Content-Length'] = 64 * 2 ** 20

        obj_path = container_path + '/disconnect-data'
        try:
            resp, _body = put(obj_path, headers=headers,
                              body=exploding_body())
        except Exception as e:
            if str(e) != 'kaboom!':
                raise
        else:
            self.fail('obj put connection did not ka-splod')

        sleep(0.1)

    def find_files(self):
        found_files = defaultdict(list)
        for root, dirs, files in os.walk(_testdir):
            for fname in files:
                filename, ext = os.path.splitext(fname)
                found_files[ext].append(os.path.join(root, fname))
        return found_files

    def test_repl_disconnect_cleans_up(self):
        self._check_disconnect_cleans_up('zero')
        found_files = self.find_files()
        self.assertEqual(found_files['.data'], [])

    def test_ec_disconnect_cleans_up(self):
        self._check_disconnect_cleans_up('ec')
        found_files = self.find_files()
        self.assertEqual(found_files['.data'], [])

    def test_repl_chunked_transfer_disconnect_cleans_up(self):
        self._check_disconnect_cleans_up('zero', is_chunked=True)
        found_files = self.find_files()
        self.assertEqual(found_files['.data'], [])

    def test_ec_chunked_transfer_disconnect_cleans_up(self):
        self._check_disconnect_cleans_up('ec', is_chunked=True)
        found_files = self.find_files()
        self.assertEqual(found_files['.data'], [])


class TestObjectECRangedGET(unittest.TestCase):
    def setUp(self):
        _test_servers[0].logger._clear()
        self.app = proxy_server.Application(
            None, FakeMemcache(),
            logger=debug_logger('proxy-ut'),
            account_ring=FakeRing(),
            container_ring=FakeRing())

    def tearDown(self):
        prosrv = _test_servers[0]
        self.assertFalse(prosrv.logger.get_lines_for_level('error'))
        self.assertFalse(prosrv.logger.get_lines_for_level('warning'))

    @classmethod
    def setUpClass(cls):
        skip_if_no_xattrs()
        cls.obj_name = 'range-get-test'
        cls.tiny_obj_name = 'range-get-test-tiny'
        cls.aligned_obj_name = 'range-get-test-aligned'
        cls.zero_byte_obj_name = 'range-get-test-zero-byte'

        # Note: only works if called with unpatched policies
        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/ec-con HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'Content-Length: 0\r\n'
                 'X-Storage-Token: t\r\n'
                 'X-Storage-Policy: ec\r\n'
                 '\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 2'
        assert headers[:len(exp)] == exp, "container PUT failed"

        seg_size = POLICIES.get_by_name("ec").ec_segment_size
        cls.seg_size = seg_size
        # EC segment size is 4 KiB, hence this gives 4 segments, which we
        # then verify with a quick sanity check
        cls.obj = ' my hovercraft is full of eels '.join(
            str(s) for s in range(431))
        assert seg_size * 4 > len(cls.obj) > seg_size * 3, \
            "object is wrong number of segments"
        cls.obj_etag = md5(cls.obj).hexdigest()
        cls.tiny_obj = 'tiny, tiny object'
        assert len(cls.tiny_obj) < seg_size, "tiny_obj too large"

        cls.aligned_obj = "".join(
            "abcdEFGHijkl%04d" % x for x in range(512))
        assert len(cls.aligned_obj) % seg_size == 0, "aligned obj not aligned"

        for obj_name, obj in ((cls.obj_name, cls.obj),
                              (cls.tiny_obj_name, cls.tiny_obj),
                              (cls.aligned_obj_name, cls.aligned_obj),
                              (cls.zero_byte_obj_name, b"")):
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('PUT /v1/a/ec-con/%s HTTP/1.1\r\n'
                     'Host: localhost\r\n'
                     'Connection: close\r\n'
                     'Content-Length: %d\r\n'
                     'X-Storage-Token: t\r\n'
                     'Content-Type: donuts\r\n'
                     '\r\n%s' % (obj_name, len(obj), obj))
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = 'HTTP/1.1 201'
            assert headers[:len(exp)] == exp, \
                "object PUT failed %s" % obj_name

    def _get_obj(self, range_value, obj_name=None):
        if obj_name is None:
            obj_name = self.obj_name

        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/ec-con/%s HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Range: %s\r\n'
                 '\r\n' % (obj_name, range_value))
        fd.flush()
        headers = readuntil2crlfs(fd)
        # e.g. "HTTP/1.1 206 Partial Content\r\n..."
        status_code = int(headers[9:12])
        headers = parse_headers_string(headers)

        gotten_obj = ''
        while True:
            buf = fd.read(64)
            if not buf:
                break
            gotten_obj += buf

        # if we get this wrong, clients will either get truncated data or
        # they'll hang waiting for bytes that aren't coming, so it warrants
        # being asserted for every test case
        if 'Content-Length' in headers:
            self.assertEqual(int(headers['Content-Length']), len(gotten_obj))

        # likewise, if we say MIME and don't send MIME or vice versa,
        # clients will be horribly confused
        if headers.get('Content-Type', '').startswith('multipart/byteranges'):
            self.assertEqual(gotten_obj[:2], "--")
        else:
            # In general, this isn't true, as you can start an object with
            # "--". However, in this test, we don't start any objects with
            # "--", or even include "--" in their contents anywhere.
            self.assertNotEqual(gotten_obj[:2], "--")

        return (status_code, headers, gotten_obj)

    def _parse_multipart(self, content_type, body):
        parser = email.parser.FeedParser()
        parser.feed("Content-Type: %s\r\n\r\n" % content_type)
        parser.feed(body)
        root_message = parser.close()
        self.assertTrue(root_message.is_multipart())
        byteranges = root_message.get_payload()
        self.assertFalse(root_message.defects)
        for i, message in enumerate(byteranges):
            self.assertFalse(message.defects, "Part %d had defects" % i)
            self.assertFalse(message.is_multipart(),
                             "Nested multipart at %d" % i)
        return byteranges

    def test_bogus(self):
        status, headers, gotten_obj = self._get_obj("tacos=3-5")
        self.assertEqual(status, 200)
        self.assertEqual(len(gotten_obj), len(self.obj))
        self.assertEqual(gotten_obj, self.obj)

    def test_unaligned(self):
        # One segment's worth of data, but straddling two segment boundaries
        # (so it has data from three segments)
        status, headers, gotten_obj = self._get_obj("bytes=3783-7878")
        self.assertEqual(status, 206)
        self.assertEqual(headers['Content-Length'], "4096")
        self.assertEqual(headers['Content-Range'], "bytes 3783-7878/14513")
        self.assertEqual(len(gotten_obj), 4096)
        self.assertEqual(gotten_obj, self.obj[3783:7879])

    def test_aligned_left(self):
        # First byte is aligned to a segment boundary, last byte is not
        status, headers, gotten_obj = self._get_obj("bytes=0-5500")
        self.assertEqual(status, 206)
        self.assertEqual(headers['Content-Length'], "5501")
        self.assertEqual(headers['Content-Range'], "bytes 0-5500/14513")
        self.assertEqual(len(gotten_obj), 5501)
        self.assertEqual(gotten_obj, self.obj[:5501])

    def test_aligned_range(self):
        # Ranged GET that wants exactly one segment
        status, headers, gotten_obj = self._get_obj("bytes=4096-8191")
        self.assertEqual(status, 206)
        self.assertEqual(headers['Content-Length'], "4096")
        self.assertEqual(headers['Content-Range'], "bytes 4096-8191/14513")
        self.assertEqual(len(gotten_obj), 4096)
        self.assertEqual(gotten_obj, self.obj[4096:8192])

    def test_aligned_range_end(self):
        # Ranged GET that wants exactly the last segment
        status, headers, gotten_obj = self._get_obj("bytes=12288-14512")
        self.assertEqual(status, 206)
        self.assertEqual(headers['Content-Length'], "2225")
        self.assertEqual(headers['Content-Range'], "bytes 12288-14512/14513")
        self.assertEqual(len(gotten_obj), 2225)
        self.assertEqual(gotten_obj, self.obj[12288:])

    def test_aligned_range_aligned_obj(self):
        # Ranged GET that wants exactly the last segment, which is full-size
        status, headers, gotten_obj = self._get_obj("bytes=4096-8191",
                                                    self.aligned_obj_name)
        self.assertEqual(status, 206)
        self.assertEqual(headers['Content-Length'], "4096")
        self.assertEqual(headers['Content-Range'], "bytes 4096-8191/8192")
        self.assertEqual(len(gotten_obj), 4096)
        self.assertEqual(gotten_obj, self.aligned_obj[4096:8192])

    def test_byte_0(self):
        # Just the first byte, but it's index 0, so that's easy to get wrong
        status, headers, gotten_obj = self._get_obj("bytes=0-0")
        self.assertEqual(status, 206)
        self.assertEqual(headers['Content-Length'], "1")
        self.assertEqual(headers['Content-Range'], "bytes 0-0/14513")
        self.assertEqual(gotten_obj, self.obj[0])

    def test_unsatisfiable(self):
        # Goes just one byte too far off the end of the object, so it's
        # unsatisfiable. This should be close enough that the object servers
        # actually responded 206
        obj_len = len(self.obj)
        status, headers, _junk = self._get_obj(
            "bytes=%d-%d" % (obj_len, obj_len + 100))
        self.assertEqual(status, 416)
        self.assertEqual(self.obj_etag, headers.get('Etag'))
        self.assertEqual('bytes', headers.get('Accept-Ranges'))
        self.assertIn('Content-Range', headers)
        self.assertEqual('bytes */%d' % obj_len, headers['Content-Range'])

        # Goes *way* too far off the end of the object, so we're looking at
        # the (massaged) 416 from an object server
        status, headers, _junk = self._get_obj(
            "bytes=%d-" % (obj_len + 2 ** 30))
        self.assertEqual(status, 416)
        self.assertEqual(self.obj_etag, headers.get('Etag'))
        self.assertEqual('bytes', headers.get('Accept-Ranges'))
        self.assertIn('Content-Range', headers)
        self.assertEqual('bytes */%d' % obj_len, headers['Content-Range'])

    def test_unsatisfiable_socket_leak(self):
        unclosed_http_responses = {}
        tracked_responses = [0]

        class LeakTrackingHTTPResponse(BufferedHTTPResponse):
            def begin(self):
                # no super(); we inherit from an old-style class (it's
                # httplib's fault; don't try and fix it).
                retval = BufferedHTTPResponse.begin(self)
                if self.status != 204:
                    # This mock is overly broad and catches account and
                    # container HEAD requests too. We don't care about
                    # those; it's the object GETs that were leaky.
                    #
                    # Unfortunately, we don't have access to the request
                    # path here, so we use "status == 204" as a crude proxy
                    # for "not an object response".
                    unclosed_http_responses[id(self)] = self
                    tracked_responses[0] += 1
                return retval

            def close(self, *args, **kwargs):
                rv = BufferedHTTPResponse.close(self, *args, **kwargs)
                unclosed_http_responses.pop(id(self), None)
                return rv

            def __repr__(self):
                swift_conn = getattr(self, 'swift_conn', None)
                method = getattr(swift_conn, '_method', '<unknown>')
                path = getattr(swift_conn, '_path', '<unknown>')
                return '%s<method=%r path=%r>' % (
                    self.__class__.__name__, method, path)

        obj_len = len(self.obj)
        with mock.patch('swift.common.bufferedhttp.BufferedHTTPConnection'
                        '.response_class', LeakTrackingHTTPResponse):
            status, headers, _junk = self._get_obj(
                "bytes=%d-%d" % (obj_len, obj_len + 100))
        self.assertEqual(status, 416)  # sanity check
        self.assertGreater(tracked_responses[0], 0)  # ensure tracking happened
        self.assertEqual(unclosed_http_responses, {})

    def test_off_end(self):
        # Ranged GET that's mostly off the end of the object, but overlaps
        # it in just the last byte
        status, headers, gotten_obj = self._get_obj(
            "bytes=%d-%d" % (len(self.obj) - 1, len(self.obj) + 100))
        self.assertEqual(status, 206)
        self.assertEqual(headers['Content-Length'], '1')
        self.assertEqual(headers['Content-Range'], 'bytes 14512-14512/14513')
        self.assertEqual(gotten_obj, self.obj[-1])

    def test_aligned_off_end(self):
        # Ranged GET that starts on a segment boundary but asks for a whole lot
        status, headers, gotten_obj = self._get_obj(
            "bytes=%d-%d" % (8192, len(self.obj) + 100))
        self.assertEqual(status, 206)
        self.assertEqual(headers['Content-Length'], '6321')
        self.assertEqual(headers['Content-Range'], 'bytes 8192-14512/14513')
        self.assertEqual(gotten_obj, self.obj[8192:])

    def test_way_off_end(self):
        # Ranged GET that's mostly off the end of the object, but overlaps
        # it in just the last byte, and wants multiple segments' worth off
        # the end
        status, headers, gotten_obj = self._get_obj(
            "bytes=%d-%d" % (len(self.obj) - 1, len(self.obj) * 1000))
        self.assertEqual(status, 206)
        self.assertEqual(headers['Content-Length'], '1')
        self.assertEqual(headers['Content-Range'], 'bytes 14512-14512/14513')
        self.assertEqual(gotten_obj, self.obj[-1])

    def test_boundaries(self):
        # Wants the last byte of segment 1 + the first byte of segment 2
        status, headers, gotten_obj = self._get_obj("bytes=4095-4096")
        self.assertEqual(status, 206)
        self.assertEqual(headers['Content-Length'], '2')
        self.assertEqual(headers['Content-Range'], 'bytes 4095-4096/14513')
        self.assertEqual(gotten_obj, self.obj[4095:4097])

    def test_until_end(self):
        # Wants the last byte of segment 1 + the rest
        status, headers, gotten_obj = self._get_obj("bytes=4095-")
        self.assertEqual(status, 206)
        self.assertEqual(headers['Content-Length'], '10418')
        self.assertEqual(headers['Content-Range'], 'bytes 4095-14512/14513')
        self.assertEqual(gotten_obj, self.obj[4095:])

    def test_small_suffix(self):
        # Small range-suffix GET: the last 100 bytes (less than one segment)
        status, headers, gotten_obj = self._get_obj("bytes=-100")
        self.assertEqual(status, 206)
        self.assertEqual(headers['Content-Length'], '100')
        self.assertEqual(headers['Content-Range'], 'bytes 14413-14512/14513')
        self.assertEqual(len(gotten_obj), 100)
        self.assertEqual(gotten_obj, self.obj[-100:])

    def test_small_suffix_aligned(self):
        # Small range-suffix GET: the last 100 bytes, last segment is
        # full-size
        status, headers, gotten_obj = self._get_obj("bytes=-100",
                                                    self.aligned_obj_name)
        self.assertEqual(status, 206)
        self.assertEqual(headers['Content-Length'], '100')
        self.assertEqual(headers['Content-Range'], 'bytes 8092-8191/8192')
        self.assertEqual(len(gotten_obj), 100)

    def test_suffix_zero_byte_object(self):
        status, headers, gotten_obj = self._get_obj("bytes=-100",
                                                    self.zero_byte_obj_name)
        self.assertEqual(status, 200)
        self.assertEqual(len(gotten_obj), 0)
        self.assertEqual(gotten_obj, b"")

    def test_suffix_two_segs(self):
        # Ask for enough data that we need the last two segments. The last
        # segment is short, though, so this ensures we compensate for that.
        #
        # Note that the total range size is less than one full-size segment.
        suffix_len = len(self.obj) % self.seg_size + 1

        status, headers, gotten_obj = self._get_obj("bytes=-%d" % suffix_len)
        self.assertEqual(status, 206)
        self.assertEqual(headers['Content-Length'], str(suffix_len))
        self.assertEqual(headers['Content-Range'],
                         'bytes %d-%d/%d' % (len(self.obj) - suffix_len,
                                             len(self.obj) - 1,
                                             len(self.obj)))
        self.assertEqual(len(gotten_obj), suffix_len)

    def test_large_suffix(self):
        # Large range-suffix GET: the last 5000 bytes (more than one segment)
        status, headers, gotten_obj = self._get_obj("bytes=-5000")
        self.assertEqual(status, 206)
        self.assertEqual(headers['Content-Length'], '5000')
        self.assertEqual(headers['Content-Range'], 'bytes 9513-14512/14513')
        self.assertEqual(len(gotten_obj), 5000)
        self.assertEqual(gotten_obj, self.obj[-5000:])

    def test_overlarge_suffix(self):
        # The last N+1 bytes of an N-byte object
        status, headers, gotten_obj = self._get_obj(
            "bytes=-%d" % (len(self.obj) + 1))
        self.assertEqual(status, 206)
        self.assertEqual(headers['Content-Length'], '14513')
        self.assertEqual(headers['Content-Range'], 'bytes 0-14512/14513')
        self.assertEqual(len(gotten_obj), len(self.obj))
        self.assertEqual(gotten_obj, self.obj)

    def test_small_suffix_tiny_object(self):
        status, headers, gotten_obj = self._get_obj(
            "bytes=-5", self.tiny_obj_name)
        self.assertEqual(status, 206)
        self.assertEqual(headers['Content-Length'], '5')
        self.assertEqual(headers['Content-Range'], 'bytes 12-16/17')
        self.assertEqual(gotten_obj, self.tiny_obj[12:])

    def test_overlarge_suffix_tiny_object(self):
        status, headers, gotten_obj = self._get_obj(
            "bytes=-1234567890", self.tiny_obj_name)
        self.assertEqual(status, 206)
        self.assertEqual(headers['Content-Length'], '17')
        self.assertEqual(headers['Content-Range'], 'bytes 0-16/17')
        self.assertEqual(len(gotten_obj), len(self.tiny_obj))
        self.assertEqual(gotten_obj, self.tiny_obj)

    def test_multiple_ranges(self):
        status, headers, gotten_obj = self._get_obj(
            "bytes=0-100,4490-5010", self.obj_name)
        self.assertEqual(status, 206)
        self.assertEqual(headers["Content-Length"], str(len(gotten_obj)))

        content_type, content_type_params = parse_content_type(
            headers['Content-Type'])
        content_type_params = dict(content_type_params)

        self.assertEqual(content_type, 'multipart/byteranges')
        boundary = content_type_params.get('boundary')
        self.assertIsNotNone(boundary)

        got_byteranges = self._parse_multipart(headers['Content-Type'],
                                               gotten_obj)
        self.assertEqual(len(got_byteranges), 2)
        first_byterange, second_byterange = got_byteranges

        self.assertEqual(first_byterange['Content-Range'],
                         'bytes 0-100/14513')
        self.assertEqual(first_byterange.get_payload(), self.obj[:101])

        self.assertEqual(second_byterange['Content-Range'],
                         'bytes 4490-5010/14513')
        self.assertEqual(second_byterange.get_payload(), self.obj[4490:5011])

    def test_multiple_ranges_overlapping_in_segment(self):
        status, headers, gotten_obj = self._get_obj(
            "bytes=0-9,20-29,40-49,60-69,80-89")
        self.assertEqual(status, 206)
        got_byteranges = self._parse_multipart(headers['Content-Type'],
                                               gotten_obj)
        self.assertEqual(len(got_byteranges), 5)

    def test_multiple_ranges_off_end(self):
        status, headers, gotten_obj = self._get_obj(
            "bytes=0-10,14500-14513")  # there is no byte 14513, only 0-14512
        self.assertEqual(status, 206)
        got_byteranges = self._parse_multipart(headers['Content-Type'],
                                               gotten_obj)
        self.assertEqual(len(got_byteranges), 2)
        self.assertEqual(got_byteranges[0]['Content-Range'],
                         "bytes 0-10/14513")
        self.assertEqual(got_byteranges[1]['Content-Range'],
                         "bytes 14500-14512/14513")

    def test_multiple_ranges_suffix_off_end(self):
        status, headers, gotten_obj = self._get_obj(
            "bytes=0-10,-13")
        self.assertEqual(status, 206)
        got_byteranges = self._parse_multipart(headers['Content-Type'],
                                               gotten_obj)
        self.assertEqual(len(got_byteranges), 2)
        self.assertEqual(got_byteranges[0]['Content-Range'],
                         "bytes 0-10/14513")
        self.assertEqual(got_byteranges[1]['Content-Range'],
                         "bytes 14500-14512/14513")

    def test_multiple_ranges_one_barely_unsatisfiable(self):
        # The thing about 14515-14520 is that it comes from the last segment
        # in the object. When we turn this range into a fragment range,
        # it'll be for the last fragment, so the object servers see
        # something satisfiable.
        #
        # Basically, we'll get 3 byteranges from the object server, but we
        # have to filter out the unsatisfiable one on our own.
        status, headers, gotten_obj = self._get_obj(
            "bytes=0-10,14515-14520,40-50")
        self.assertEqual(status, 206)
        got_byteranges = self._parse_multipart(headers['Content-Type'],
                                               gotten_obj)
        self.assertEqual(len(got_byteranges), 2)
        self.assertEqual(got_byteranges[0]['Content-Range'],
                         "bytes 0-10/14513")
        self.assertEqual(got_byteranges[0].get_payload(), self.obj[0:11])
        self.assertEqual(got_byteranges[1]['Content-Range'],
                         "bytes 40-50/14513")
        self.assertEqual(got_byteranges[1].get_payload(), self.obj[40:51])

    def test_multiple_ranges_some_unsatisfiable(self):
        status, headers, gotten_obj = self._get_obj(
            "bytes=0-100,4090-5010,999999-9999999", self.obj_name)
        self.assertEqual(status, 206)

        content_type, content_type_params = parse_content_type(
            headers['Content-Type'])
        content_type_params = dict(content_type_params)

        self.assertEqual(content_type, 'multipart/byteranges')
        boundary = content_type_params.get('boundary')
        self.assertIsNotNone(boundary)

        got_byteranges = self._parse_multipart(headers['Content-Type'],
                                               gotten_obj)
        self.assertEqual(len(got_byteranges), 2)
        first_byterange, second_byterange = got_byteranges

        self.assertEqual(first_byterange['Content-Range'],
                         'bytes 0-100/14513')
        self.assertEqual(first_byterange.get_payload(), self.obj[:101])

        self.assertEqual(second_byterange['Content-Range'],
                         'bytes 4090-5010/14513')
        self.assertEqual(second_byterange.get_payload(), self.obj[4090:5011])

    def test_two_ranges_one_unsatisfiable(self):
        status, headers, gotten_obj = self._get_obj(
            "bytes=0-100,999999-9999999", self.obj_name)
        self.assertEqual(status, 206)

        content_type, content_type_params = parse_content_type(
            headers['Content-Type'])

        # According to RFC 7233, this could be either a multipart/byteranges
        # response with one part or it could be a single-part response (just
        # the bytes, no MIME). We're locking it down here: single-part
        # response. That's what replicated objects do, and we don't want any
        # client-visible differences between EC objects and replicated ones.
        self.assertEqual(content_type, 'donuts')
        self.assertEqual(gotten_obj, self.obj[:101])

    def test_two_ranges_one_unsatisfiable_same_segment(self):
        # Like test_two_ranges_one_unsatisfiable(), but where both ranges
        # fall within the same EC segment.
        status, headers, gotten_obj = self._get_obj(
            "bytes=14500-14510,14520-14530")

        self.assertEqual(status, 206)

        content_type, content_type_params = parse_content_type(
            headers['Content-Type'])

        self.assertEqual(content_type, 'donuts')
        self.assertEqual(gotten_obj, self.obj[14500:14511])

    def test_multiple_ranges_some_unsatisfiable_out_of_order(self):
        status, headers, gotten_obj = self._get_obj(
            "bytes=0-100,99999998-99999999,4090-5010", self.obj_name)
        self.assertEqual(status, 206)

        content_type, content_type_params = parse_content_type(
            headers['Content-Type'])
        content_type_params = dict(content_type_params)

        self.assertEqual(content_type, 'multipart/byteranges')
        boundary = content_type_params.get('boundary')
        self.assertIsNotNone(boundary)

        got_byteranges = self._parse_multipart(headers['Content-Type'],
                                               gotten_obj)
        self.assertEqual(len(got_byteranges), 2)
        first_byterange, second_byterange = got_byteranges

        self.assertEqual(first_byterange['Content-Range'],
                         'bytes 0-100/14513')
        self.assertEqual(first_byterange.get_payload(), self.obj[:101])

        self.assertEqual(second_byterange['Content-Range'],
                         'bytes 4090-5010/14513')
        self.assertEqual(second_byterange.get_payload(), self.obj[4090:5011])


@patch_policies([
    StoragePolicy(0, 'zero', True, object_ring=FakeRing(base_port=3000)),
    StoragePolicy(1, 'one', False, object_ring=FakeRing(base_port=3000)),
    StoragePolicy(2, 'two', False, True, object_ring=FakeRing(base_port=3000))
])
class TestContainerController(unittest.TestCase):
    "Test swift.proxy_server.ContainerController"

    def setUp(self):
        self.app = proxy_server.Application(
            None, FakeMemcache(),
            account_ring=FakeRing(),
            container_ring=FakeRing(base_port=2000),
            logger=debug_logger())

    def test_convert_policy_to_index(self):
        controller = swift.proxy.controllers.ContainerController(self.app,
                                                                 'a', 'c')
        expected = {
            'zero': 0,
            'ZeRo': 0,
            'one': 1,
            'OnE': 1,
        }
        for name, index in expected.items():
            req = Request.blank('/a/c', headers={'Content-Length': '0',
                                                 'Content-Type': 'text/plain',
                                                 'X-Storage-Policy': name})
            self.assertEqual(controller._convert_policy_to_index(req), index)
        # default test
        req = Request.blank('/a/c', headers={'Content-Length': '0',
                                             'Content-Type': 'text/plain'})
        self.assertIsNone(controller._convert_policy_to_index(req))
        # negative test
        req = Request.blank('/a/c',
                            headers={'Content-Length': '0',
                                     'Content-Type': 'text/plain',
                                     'X-Storage-Policy': 'nada'})
        self.assertRaises(HTTPException, controller._convert_policy_to_index,
                          req)
        # storage policy two is deprecated
        req = Request.blank('/a/c', headers={'Content-Length': '0',
                                             'Content-Type': 'text/plain',
                                             'X-Storage-Policy': 'two'})
        self.assertRaises(HTTPException, controller._convert_policy_to_index,
                          req)

    def test_convert_index_to_name(self):
        policy = random.choice(list(POLICIES))
        req = Request.blank('/v1/a/c')
        with mocked_http_conn(
                200, 200,
                headers={'X-Backend-Storage-Policy-Index': int(policy)},
        ) as fake_conn:
            resp = req.get_response(self.app)
            self.assertRaises(StopIteration, fake_conn.code_iter.next)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['X-Storage-Policy'], policy.name)

    def test_no_convert_index_to_name_when_container_not_found(self):
        policy = random.choice(list(POLICIES))
        req = Request.blank('/v1/a/c')
        with mocked_http_conn(
                200, 404, 404, 404,
                headers={'X-Backend-Storage-Policy-Index':
                         int(policy)}) as fake_conn:
            resp = req.get_response(self.app)
            self.assertRaises(StopIteration, fake_conn.code_iter.next)
        self.assertEqual(resp.status_int, 404)
        self.assertIsNone(resp.headers['X-Storage-Policy'])

    def test_error_convert_index_to_name(self):
        req = Request.blank('/v1/a/c')
        with mocked_http_conn(
                200, 200,
                headers={'X-Backend-Storage-Policy-Index': '-1'}) as fake_conn:
            resp = req.get_response(self.app)
            self.assertRaises(StopIteration, fake_conn.code_iter.next)
        self.assertEqual(resp.status_int, 200)
        self.assertIsNone(resp.headers['X-Storage-Policy'])
        error_lines = self.app.logger.get_lines_for_level('error')
        self.assertEqual(2, len(error_lines))
        for msg in error_lines:
            expected = "Could not translate " \
                "X-Backend-Storage-Policy-Index ('-1')"
            self.assertIn(expected, msg)

    def test_transfer_headers(self):
        src_headers = {'x-remove-versions-location': 'x',
                       'x-container-read': '*:user',
                       'x-remove-container-sync-key': 'x'}
        dst_headers = {'x-versions-location': 'backup'}
        controller = swift.proxy.controllers.ContainerController(self.app,
                                                                 'a', 'c')
        controller.transfer_headers(src_headers, dst_headers)
        expected_headers = {'x-versions-location': '',
                            'x-container-read': '*:user',
                            'x-container-sync-key': ''}
        self.assertEqual(dst_headers, expected_headers)

    def assert_status_map(self, method, statuses, expected,
                          raise_exc=False, missing_container=False):
        with save_globals():
            kwargs = {'missing_container': missing_container}
            if raise_exc:
                kwargs['raise_exc'] = raise_exc
            set_http_connect(*statuses, **kwargs)
            self.app.memcache.store = {}
            req = Request.blank('/v1/a/c', headers={'Content-Length': '0',
                                'Content-Type': 'text/plain'})
            self.app.update_request(req)
            res = method(req)
            self.assertEqual(res.status_int, expected)
            set_http_connect(*statuses, **kwargs)
            self.app.memcache.store = {}
            req = Request.blank('/v1/a/c/', headers={'Content-Length': '0',
                                'Content-Type': 'text/plain'})
            self.app.update_request(req)
            res = method(req)
            self.assertEqual(res.status_int, expected)

    def test_HEAD_GET(self):
        with save_globals():
            controller = proxy_server.ContainerController(self.app, 'a', 'c')

            def test_status_map(statuses, expected,
                                c_expected=None, a_expected=None, **kwargs):
                set_http_connect(*statuses, **kwargs)
                self.app.memcache.store = {}
                req = Request.blank('/v1/a/c', {})
                self.app.update_request(req)
                res = controller.HEAD(req)
                self.assertEqual(res.status[:len(str(expected))],
                                 str(expected))
                infocache = res.environ.get('swift.infocache', {})
                if expected < 400:
                    self.assertIn('x-works', res.headers)
                    self.assertEqual(res.headers['x-works'], 'yes')
                if expected < 300:
                    self.assertIn('last-modified', res.headers)
                    self.assertEqual(res.headers['last-modified'], '1')
                if c_expected:
                    self.assertIn('container/a/c', infocache)
                    self.assertEqual(
                        infocache['container/a/c']['status'],
                        c_expected)
                else:
                    self.assertNotIn('container/a/c', infocache)
                if a_expected:
                    self.assertIn('account/a', infocache)
                    self.assertEqual(infocache['account/a']['status'],
                                     a_expected)
                else:
                    self.assertNotIn('account/a', res.environ)

                set_http_connect(*statuses, **kwargs)
                self.app.memcache.store = {}
                req = Request.blank('/v1/a/c', {})
                self.app.update_request(req)
                res = controller.GET(req)
                self.assertEqual(res.status[:len(str(expected))],
                                 str(expected))
                infocache = res.environ.get('swift.infocache', {})
                if expected < 400:
                    self.assertIn('x-works', res.headers)
                    self.assertEqual(res.headers['x-works'], 'yes')
                if expected < 300:
                    self.assertIn('last-modified', res.headers)
                    self.assertEqual(res.headers['last-modified'], '1')
                if c_expected:
                    self.assertIn('container/a/c', infocache)
                    self.assertEqual(
                        infocache['container/a/c']['status'],
                        c_expected)
                else:
                    self.assertNotIn('container/a/c', infocache)
                if a_expected:
                    self.assertIn('account/a', infocache)
                    self.assertEqual(infocache['account/a']['status'],
                                     a_expected)
                else:
                    self.assertNotIn('account/a', infocache)
            # In all the following tests cache 200 for account
            # return and cache vary for container
            # return 200 and cache 200 for account and container
            test_status_map((200, 200, 404, 404), 200, 200, 200)
            test_status_map((200, 200, 500, 404), 200, 200, 200)
            # return 304 don't cache container
            test_status_map((200, 304, 500, 404), 304, None, 200)
            # return 404 and cache 404 for container
            test_status_map((200, 404, 404, 404), 404, 404, 200)
            test_status_map((200, 404, 404, 500), 404, 404, 200)
            # return 503, don't cache container
            test_status_map((200, 500, 500, 500), 503, None, 200)
            self.assertFalse(self.app.account_autocreate)

            # return 404 (as account is not found) and don't cache container
            test_status_map((404, 404, 404), 404, None, 404)

            # cache a 200 for the account because it appears to be created
            self.app.account_autocreate = True
            test_status_map((404, 404, 404), 404, None, 200)

    def test_PUT_policy_headers(self):
        backend_requests = []

        def capture_requests(ipaddr, port, device, partition, method,
                             path, headers=None, query_string=None):
            if method == 'PUT':
                backend_requests.append(headers)

        def test_policy(requested_policy):
            with save_globals():
                mock_conn = set_http_connect(200, 201, 201, 201,
                                             give_connect=capture_requests)
                self.app.memcache.store = {}
                req = Request.blank('/v1/a/test', method='PUT',
                                    headers={'Content-Length': 0})
                if requested_policy:
                    expected_policy = requested_policy
                    req.headers['X-Storage-Policy'] = policy.name
                else:
                    expected_policy = POLICIES.default
                res = req.get_response(self.app)
                if expected_policy.is_deprecated:
                    self.assertEqual(res.status_int, 400)
                    self.assertEqual(0, len(backend_requests))
                    expected = 'is deprecated'
                    self.assertIn(expected, res.body,
                                  '%r did not include %r' % (
                                      res.body, expected))
                    return
                self.assertEqual(res.status_int, 201)
                self.assertEqual(
                    expected_policy.object_ring.replicas,
                    len(backend_requests))
                for headers in backend_requests:
                    if not requested_policy:
                        self.assertNotIn('X-Backend-Storage-Policy-Index',
                                         headers)
                        self.assertIn('X-Backend-Storage-Policy-Default',
                                      headers)
                        self.assertEqual(
                            int(expected_policy),
                            int(headers['X-Backend-Storage-Policy-Default']))
                    else:
                        self.assertIn('X-Backend-Storage-Policy-Index',
                                      headers)
                        self.assertEqual(int(headers
                                         ['X-Backend-Storage-Policy-Index']),
                                         int(policy))
                # make sure all mocked responses are consumed
                self.assertRaises(StopIteration, mock_conn.code_iter.next)

        test_policy(None)  # no policy header
        for policy in POLICIES:
            backend_requests = []  # reset backend requests
            test_policy(policy)

    def test_PUT(self):
        with save_globals():
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')

            def test_status_map(statuses, expected, **kwargs):
                set_http_connect(*statuses, **kwargs)
                self.app.memcache.store = {}
                req = Request.blank('/v1/a/c', {})
                req.content_length = 0
                self.app.update_request(req)
                res = controller.PUT(req)
                expected = str(expected)
                self.assertEqual(res.status[:len(expected)], expected)

            test_status_map((200, 201, 201, 201), 201, missing_container=True)
            test_status_map((200, 201, 201, 500), 201, missing_container=True)
            test_status_map((200, 204, 404, 404), 404, missing_container=True)
            test_status_map((200, 204, 500, 404), 503, missing_container=True)
            self.assertFalse(self.app.account_autocreate)
            test_status_map((404, 404, 404), 404, missing_container=True)
            self.app.account_autocreate = True
            # fail to retrieve account info
            test_status_map(
                (503, 503, 503),  # account_info fails on 503
                503, missing_container=True)
            # account fail after creation
            test_status_map(
                (404, 404, 404,   # account_info fails on 404
                 201, 201, 201,   # PUT account
                 404, 404, 404),  # account_info fail
                404, missing_container=True)
            test_status_map(
                (503, 503, 404,   # account_info fails on 404
                 503, 503, 503,   # PUT account
                 503, 503, 404),  # account_info fail
                503, missing_container=True)
            # put fails
            test_status_map(
                (404, 404, 404,   # account_info fails on 404
                 201, 201, 201,   # PUT account
                 200,             # account_info success
                 503, 503, 201),  # put container fail
                503, missing_container=True)
            # all goes according to plan
            test_status_map(
                (404, 404, 404,   # account_info fails on 404
                 201, 201, 201,   # PUT account
                 200,             # account_info success
                 201, 201, 201),  # put container success
                201, missing_container=True)
            test_status_map(
                (503, 404, 404,   # account_info fails on 404
                 503, 201, 201,   # PUT account
                 503, 200,        # account_info success
                 503, 201, 201),  # put container success
                201, missing_container=True)

    def test_PUT_autocreate_account_with_sysmeta(self):
        # x-account-sysmeta headers in a container PUT request should be
        # transferred to the account autocreate PUT request
        with save_globals():
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')

            def test_status_map(statuses, expected, headers=None, **kwargs):
                set_http_connect(*statuses, **kwargs)
                self.app.memcache.store = {}
                req = Request.blank('/v1/a/c', {}, headers=headers)
                req.content_length = 0
                self.app.update_request(req)
                res = controller.PUT(req)
                expected = str(expected)
                self.assertEqual(res.status[:len(expected)], expected)

            self.app.account_autocreate = True
            calls = []
            callback = _make_callback_func(calls)
            key, value = 'X-Account-Sysmeta-Blah', 'something'
            headers = {key: value}

            # all goes according to plan
            test_status_map(
                (404, 404, 404,   # account_info fails on 404
                 201, 201, 201,   # PUT account
                 200,             # account_info success
                 201, 201, 201),  # put container success
                201, missing_container=True,
                headers=headers,
                give_connect=callback)

            self.assertEqual(10, len(calls))
            for call in calls[3:6]:
                self.assertEqual('/account', call['path'])
                self.assertIn(key, call['headers'],
                              '%s call, key %s missing in headers %s' % (
                                  call['method'], key, call['headers']))
                self.assertEqual(value, call['headers'][key])

    def test_POST(self):
        with save_globals():
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')

            def test_status_map(statuses, expected, **kwargs):
                set_http_connect(*statuses, **kwargs)
                self.app.memcache.store = {}
                req = Request.blank('/v1/a/c', {})
                req.content_length = 0
                self.app.update_request(req)
                res = controller.POST(req)
                expected = str(expected)
                self.assertEqual(res.status[:len(expected)], expected)

            test_status_map((200, 201, 201, 201), 201, missing_container=True)
            test_status_map((200, 201, 201, 500), 201, missing_container=True)
            test_status_map((200, 204, 404, 404), 404, missing_container=True)
            test_status_map((200, 204, 500, 404), 503, missing_container=True)
            self.assertFalse(self.app.account_autocreate)
            test_status_map((404, 404, 404), 404, missing_container=True)
            self.app.account_autocreate = True
            test_status_map((404, 404, 404), 404, missing_container=True)

    def test_PUT_max_containers_per_account(self):
        with save_globals():
            self.app.max_containers_per_account = 12346
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            self.assert_status_map(controller.PUT,
                                   (200, 201, 201, 201), 201,
                                   missing_container=True)

            self.app.max_containers_per_account = 12345
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            self.assert_status_map(controller.PUT,
                                   (200, 200, 201, 201, 201), 201,
                                   missing_container=True)

            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container_new')

            self.assert_status_map(controller.PUT, (200, 404, 404, 404), 403,
                                   missing_container=True)

            self.app.max_containers_per_account = 12345
            self.app.max_containers_whitelist = ['account']
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            self.assert_status_map(controller.PUT,
                                   (200, 201, 201, 201), 201,
                                   missing_container=True)

    def test_PUT_max_container_name_length(self):
        limit = constraints.MAX_CONTAINER_NAME_LENGTH
        controller = proxy_server.ContainerController(self.app, 'account',
                                                      '1' * limit)
        self.assert_status_map(controller.PUT, (200, 201, 201, 201), 201,
                               missing_container=True)
        controller = proxy_server.ContainerController(self.app, 'account',
                                                      '2' * (limit + 1))
        self.assert_status_map(controller.PUT, (), 400,
                               missing_container=True)

        # internal auto-created-accounts get higher limits
        limit *= 2
        controller = proxy_server.ContainerController(self.app, '.account',
                                                      '3' * limit)
        self.assert_status_map(controller.PUT, (200, 201, 201, 201), 201,
                               missing_container=True)
        controller = proxy_server.ContainerController(self.app, '.account',
                                                      '4' * (limit + 1))
        self.assert_status_map(controller.PUT, (), 400,
                               missing_container=True)

        self.app.auto_create_account_prefix = 'acc'
        controller = proxy_server.ContainerController(self.app, 'account',
                                                      '1' * limit)
        self.assert_status_map(controller.PUT, (200, 201, 201, 201), 201,
                               missing_container=True)
        controller = proxy_server.ContainerController(self.app, 'account',
                                                      '2' * (limit + 1))
        self.assert_status_map(controller.PUT, (), 400,
                               missing_container=True)

    def test_PUT_connect_exceptions(self):
        with save_globals():
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            self.assert_status_map(controller.PUT, (200, 201, 201, -1), 201,
                                   missing_container=True)
            self.assert_status_map(controller.PUT, (200, 201, -1, -1), 503,
                                   missing_container=True)
            self.assert_status_map(controller.PUT, (200, 503, 503, -1), 503,
                                   missing_container=True)

    def test_acc_missing_returns_404(self):
        for meth in ('DELETE', 'PUT'):
            with save_globals():
                self.app.memcache = FakeMemcacheReturnsNone()
                self.app._error_limiting = {}
                controller = proxy_server.ContainerController(self.app,
                                                              'account',
                                                              'container')
                if meth == 'PUT':
                    set_http_connect(200, 200, 200, 200, 200, 200,
                                     missing_container=True)
                else:
                    set_http_connect(200, 200, 200, 200)
                self.app.memcache.store = {}
                req = Request.blank('/v1/a/c',
                                    environ={'REQUEST_METHOD': meth})
                self.app.update_request(req)
                resp = getattr(controller, meth)(req)
                self.assertEqual(resp.status_int, 200)

                set_http_connect(404, 404, 404, 200, 200, 200)
                # Make sure it is a blank request wthout env caching
                req = Request.blank('/v1/a/c',
                                    environ={'REQUEST_METHOD': meth})
                resp = getattr(controller, meth)(req)
                self.assertEqual(resp.status_int, 404)

                set_http_connect(503, 404, 404)
                # Make sure it is a blank request wthout env caching
                req = Request.blank('/v1/a/c',
                                    environ={'REQUEST_METHOD': meth})
                resp = getattr(controller, meth)(req)
                self.assertEqual(resp.status_int, 404)

                set_http_connect(503, 404, raise_exc=True)
                # Make sure it is a blank request wthout env caching
                req = Request.blank('/v1/a/c',
                                    environ={'REQUEST_METHOD': meth})
                resp = getattr(controller, meth)(req)
                self.assertEqual(resp.status_int, 404)

                for dev in self.app.account_ring.devs:
                    set_node_errors(self.app, dev,
                                    self.app.error_suppression_limit + 1,
                                    time.time())
                set_http_connect(200, 200, 200, 200, 200, 200)
                # Make sure it is a blank request wthout env caching
                req = Request.blank('/v1/a/c',
                                    environ={'REQUEST_METHOD': meth})
                resp = getattr(controller, meth)(req)
                self.assertEqual(resp.status_int, 404)

    def test_put_locking(self):

        class MockMemcache(FakeMemcache):

            def __init__(self, allow_lock=None):
                self.allow_lock = allow_lock
                super(MockMemcache, self).__init__()

            @contextmanager
            def soft_lock(self, key, timeout=0, retries=5):
                if self.allow_lock:
                    yield True
                else:
                    raise NotImplementedError

        with save_globals():
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            self.app.memcache = MockMemcache(allow_lock=True)
            set_http_connect(200, 201, 201, 201,
                             missing_container=True)
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': 'PUT'})
            self.app.update_request(req)
            res = controller.PUT(req)
            self.assertEqual(res.status_int, 201)

    def test_error_limiting(self):
        with save_globals():
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            container_ring = controller.app.container_ring
            controller.app.sort_nodes = lambda l, *args, **kwargs: l
            self.assert_status_map(controller.HEAD, (200, 503, 200, 200), 200,
                                   missing_container=False)

            self.assertEqual(
                node_error_count(controller.app, container_ring.devs[0]), 2)
            self.assertTrue(
                node_last_error(controller.app, container_ring.devs[0])
                is not None)
            for _junk in range(self.app.error_suppression_limit):
                self.assert_status_map(controller.HEAD,
                                       (200, 503, 503, 503), 503)
            self.assertEqual(
                node_error_count(controller.app, container_ring.devs[0]),
                self.app.error_suppression_limit + 1)
            self.assert_status_map(controller.HEAD, (200, 200, 200, 200), 503)
            self.assertTrue(
                node_last_error(controller.app, container_ring.devs[0])
                is not None)
            self.assert_status_map(controller.PUT, (200, 201, 201, 201), 503,
                                   missing_container=True)
            self.assert_status_map(controller.DELETE,
                                   (200, 204, 204, 204), 503)
            self.app.error_suppression_interval = -300
            self.assert_status_map(controller.HEAD, (200, 200, 200, 200), 200)
            self.assert_status_map(controller.DELETE, (200, 204, 204, 204),
                                   404, raise_exc=True)

    def test_DELETE(self):
        with save_globals():
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            self.assert_status_map(controller.DELETE,
                                   (200, 204, 204, 204), 204)
            self.assert_status_map(controller.DELETE,
                                   (200, 204, 204, 503), 204)
            self.assert_status_map(controller.DELETE,
                                   (200, 204, 503, 503), 503)
            self.assert_status_map(controller.DELETE,
                                   (200, 204, 404, 404), 404)
            self.assert_status_map(controller.DELETE,
                                   (200, 404, 404, 404), 404)
            self.assert_status_map(controller.DELETE,
                                   (200, 204, 503, 404), 503)

            self.app.memcache = FakeMemcacheReturnsNone()
            # 200: Account check, 404x3: Container check
            self.assert_status_map(controller.DELETE,
                                   (200, 404, 404, 404), 404)

    def test_response_get_accept_ranges_header(self):
        with save_globals():
            set_http_connect(200, 200, body='{}')
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            req = Request.blank('/v1/a/c?format=json')
            self.app.update_request(req)
            res = controller.GET(req)
            self.assertIn('accept-ranges', res.headers)
            self.assertEqual(res.headers['accept-ranges'], 'bytes')

    def test_response_head_accept_ranges_header(self):
        with save_globals():
            set_http_connect(200, 200, body='{}')
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            req = Request.blank('/v1/a/c?format=json')
            self.app.update_request(req)
            res = controller.HEAD(req)
            self.assertIn('accept-ranges', res.headers)
            self.assertEqual(res.headers['accept-ranges'], 'bytes')

    def test_PUT_metadata(self):
        self.metadata_helper('PUT')

    def test_POST_metadata(self):
        self.metadata_helper('POST')

    def metadata_helper(self, method):
        for test_header, test_value in (
                ('X-Container-Meta-TestHeader', 'TestValue'),
                ('X-Container-Meta-TestHeader', ''),
                ('X-Remove-Container-Meta-TestHeader', 'anything'),
                ('X-Container-Read', '.r:*'),
                ('X-Remove-Container-Read', 'anything'),
                ('X-Container-Write', 'anyone'),
                ('X-Remove-Container-Write', 'anything')):
            test_errors = []

            def test_connect(ipaddr, port, device, partition, method, path,
                             headers=None, query_string=None):
                if path == '/a/c':
                    find_header = test_header
                    find_value = test_value
                    if find_header.lower().startswith('x-remove-'):
                        find_header = \
                            find_header.lower().replace('-remove', '', 1)
                        find_value = ''
                    for k, v in headers.items():
                        if k.lower() == find_header.lower() and \
                                v == find_value:
                            break
                    else:
                        test_errors.append('%s: %s not in %s' %
                                           (find_header, find_value, headers))
            with save_globals():
                controller = \
                    proxy_server.ContainerController(self.app, 'a', 'c')
                set_http_connect(200, 201, 201, 201, give_connect=test_connect)
                req = Request.blank(
                    '/v1/a/c',
                    environ={'REQUEST_METHOD': method, 'swift_owner': True},
                    headers={test_header: test_value})
                self.app.update_request(req)
                getattr(controller, method)(req)
                self.assertEqual(test_errors, [])

    def test_PUT_bad_metadata(self):
        self.bad_metadata_helper('PUT')

    def test_POST_bad_metadata(self):
        self.bad_metadata_helper('POST')

    def bad_metadata_helper(self, method):
        with save_globals():
            controller = proxy_server.ContainerController(self.app, 'a', 'c')
            set_http_connect(200, 201, 201, 201)
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEqual(resp.status_int, 201)

            set_http_connect(201, 201, 201)
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers={'X-Container-Meta-' +
                                ('a' * constraints.MAX_META_NAME_LENGTH): 'v'})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEqual(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            req = Request.blank(
                '/v1/a/c', environ={'REQUEST_METHOD': method},
                headers={'X-Container-Meta-' +
                         ('a' * (constraints.MAX_META_NAME_LENGTH + 1)): 'v'})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEqual(resp.status_int, 400)

            set_http_connect(201, 201, 201)
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers={'X-Container-Meta-Too-Long':
                                'a' * constraints.MAX_META_VALUE_LENGTH})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEqual(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers={'X-Container-Meta-Too-Long':
                                'a' * (constraints.MAX_META_VALUE_LENGTH + 1)})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEqual(resp.status_int, 400)

            set_http_connect(201, 201, 201)
            headers = {}
            for x in range(constraints.MAX_META_COUNT):
                headers['X-Container-Meta-%d' % x] = 'v'
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers=headers)
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEqual(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            headers = {}
            for x in range(constraints.MAX_META_COUNT + 1):
                headers['X-Container-Meta-%d' % x] = 'v'
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers=headers)
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEqual(resp.status_int, 400)

            set_http_connect(201, 201, 201)
            headers = {}
            header_value = 'a' * constraints.MAX_META_VALUE_LENGTH
            size = 0
            x = 0
            while size < (constraints.MAX_META_OVERALL_SIZE - 4
                          - constraints.MAX_META_VALUE_LENGTH):
                size += 4 + constraints.MAX_META_VALUE_LENGTH
                headers['X-Container-Meta-%04d' % x] = header_value
                x += 1
            if constraints.MAX_META_OVERALL_SIZE - size > 1:
                headers['X-Container-Meta-a'] = \
                    'a' * (constraints.MAX_META_OVERALL_SIZE - size - 1)
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers=headers)
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEqual(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            headers['X-Container-Meta-a'] = \
                'a' * (constraints.MAX_META_OVERALL_SIZE - size)
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers=headers)
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEqual(resp.status_int, 400)

    def test_POST_calls_clean_acl(self):
        called = [False]

        def clean_acl(header, value):
            called[0] = True
            raise ValueError('fake error')
        with save_globals():
            set_http_connect(200, 201, 201, 201)
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': 'POST'},
                                headers={'X-Container-Read': '.r:*'})
            req.environ['swift.clean_acl'] = clean_acl
            self.app.update_request(req)
            controller.POST(req)
        self.assertTrue(called[0])
        called[0] = False
        with save_globals():
            set_http_connect(200, 201, 201, 201)
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': 'POST'},
                                headers={'X-Container-Write': '.r:*'})
            req.environ['swift.clean_acl'] = clean_acl
            self.app.update_request(req)
            controller.POST(req)
        self.assertTrue(called[0])

    def test_PUT_calls_clean_acl(self):
        called = [False]

        def clean_acl(header, value):
            called[0] = True
            raise ValueError('fake error')
        with save_globals():
            set_http_connect(200, 201, 201, 201)
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'X-Container-Read': '.r:*'})
            req.environ['swift.clean_acl'] = clean_acl
            self.app.update_request(req)
            controller.PUT(req)
        self.assertTrue(called[0])
        called[0] = False
        with save_globals():
            set_http_connect(200, 201, 201, 201)
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'X-Container-Write': '.r:*'})
            req.environ['swift.clean_acl'] = clean_acl
            self.app.update_request(req)
            controller.PUT(req)
        self.assertTrue(called[0])

    def test_GET_no_content(self):
        with save_globals():
            set_http_connect(200, 204, 204, 204)
            controller = proxy_server.ContainerController(self.app, 'a', 'c')
            req = Request.blank('/v1/a/c')
            self.app.update_request(req)
            res = controller.GET(req)
            self.assertEqual(res.status_int, 204)
            ic = res.environ['swift.infocache']
            self.assertEqual(ic['container/a/c']['status'], 204)
            self.assertEqual(res.content_length, 0)
            self.assertNotIn('transfer-encoding', res.headers)

    def test_GET_account_non_existent(self):
        with save_globals():
            set_http_connect(404, 404, 404)
            controller = proxy_server.ContainerController(self.app, 'a', 'c')
            req = Request.blank('/v1/a/c')
            self.app.update_request(req)
            res = controller.GET(req)
            self.assertEqual(res.status_int, 404)
            self.assertNotIn('container/a/c', res.environ['swift.infocache'])

    def test_GET_auto_create_prefix_account_non_existent(self):
        with save_globals():
            set_http_connect(404, 404, 404, 204, 204, 204)
            controller = proxy_server.ContainerController(self.app, '.a', 'c')
            req = Request.blank('/v1/a/c')
            self.app.update_request(req)
            res = controller.GET(req)
            self.assertEqual(res.status_int, 204)
            ic = res.environ['swift.infocache']
            self.assertEqual(ic['container/.a/c']['status'], 204)
            self.assertEqual(res.content_length, 0)
            self.assertNotIn('transfer-encoding', res.headers)

    def test_GET_calls_authorize(self):
        called = [False]

        def authorize(req):
            called[0] = True
            return HTTPUnauthorized(request=req)
        with save_globals():
            set_http_connect(200, 201, 201, 201)
            controller = proxy_server.ContainerController(self.app, 'a', 'c')
            req = Request.blank('/v1/a/c')
            req.environ['swift.authorize'] = authorize
            self.app.update_request(req)
            res = controller.GET(req)
        self.assertEqual(
            res.environ['swift.infocache']['container/a/c']['status'],
            201)
        self.assertTrue(called[0])

    def test_HEAD_calls_authorize(self):
        called = [False]

        def authorize(req):
            called[0] = True
            return HTTPUnauthorized(request=req)
        with save_globals():
            set_http_connect(200, 201, 201, 201)
            controller = proxy_server.ContainerController(self.app, 'a', 'c')
            req = Request.blank('/v1/a/c', {'REQUEST_METHOD': 'HEAD'})
            req.environ['swift.authorize'] = authorize
            self.app.update_request(req)
            controller.HEAD(req)
        self.assertTrue(called[0])

    def test_unauthorized_requests_when_account_not_found(self):
        # verify unauthorized container requests always return response
        # from swift.authorize
        called = [0, 0]

        def authorize(req):
            called[0] += 1
            return HTTPUnauthorized(request=req)

        def account_info(*args):
            called[1] += 1
            return None, None, None

        def _do_test(method):
            with save_globals():
                swift.proxy.controllers.Controller.account_info = account_info
                app = proxy_server.Application(None, FakeMemcache(),
                                               account_ring=FakeRing(),
                                               container_ring=FakeRing())
                set_http_connect(201, 201, 201)
                req = Request.blank('/v1/a/c', {'REQUEST_METHOD': method})
                req.environ['swift.authorize'] = authorize
                self.app.update_request(req)
                res = app.handle_request(req)
            return res

        for method in ('PUT', 'POST', 'DELETE'):
            # no delay_denial on method, expect one call to authorize
            called = [0, 0]
            res = _do_test(method)
            self.assertEqual(401, res.status_int)
            self.assertEqual([1, 0], called)

        for method in ('HEAD', 'GET'):
            # delay_denial on method, expect two calls to authorize
            called = [0, 0]
            res = _do_test(method)
            self.assertEqual(401, res.status_int)
            self.assertEqual([2, 1], called)

    def test_authorized_requests_when_account_not_found(self):
        # verify authorized container requests always return 404 when
        # account not found
        called = [0, 0]

        def authorize(req):
            called[0] += 1

        def account_info(*args):
            called[1] += 1
            return None, None, None

        def _do_test(method):
            with save_globals():
                swift.proxy.controllers.Controller.account_info = account_info
                app = proxy_server.Application(None, FakeMemcache(),
                                               account_ring=FakeRing(),
                                               container_ring=FakeRing())
                set_http_connect(201, 201, 201)
                req = Request.blank('/v1/a/c', {'REQUEST_METHOD': method})
                req.environ['swift.authorize'] = authorize
                self.app.update_request(req)
                res = app.handle_request(req)
            return res

        for method in ('PUT', 'POST', 'DELETE', 'HEAD', 'GET'):
            # expect one call to authorize
            called = [0, 0]
            res = _do_test(method)
            self.assertEqual(404, res.status_int)
            self.assertEqual([1, 1], called)

    def test_OPTIONS_get_info_drops_origin(self):
        with save_globals():
            controller = proxy_server.ContainerController(self.app, 'a', 'c')

            count = [0]

            def my_get_info(app, env, account, container=None,
                            ret_not_found=False, swift_source=None):
                if count[0] > 11:
                    return {}
                count[0] += 1
                if not container:
                    return {'some': 'stuff'}
                return proxy_base.was_get_info(
                    app, env, account, container, ret_not_found, swift_source)

            proxy_base.was_get_info = proxy_base.get_info
            with mock.patch.object(proxy_base, 'get_info', my_get_info):
                proxy_base.get_info = my_get_info
                req = Request.blank(
                    '/v1/a/c',
                    {'REQUEST_METHOD': 'OPTIONS'},
                    headers={'Origin': 'http://foo.com',
                             'Access-Control-Request-Method': 'GET'})
                controller.OPTIONS(req)
                self.assertLess(count[0], 11)

    def test_OPTIONS(self):
        with save_globals():
            controller = proxy_server.ContainerController(self.app, 'a', 'c')

            def my_empty_container_info(*args):
                return {}
            controller.container_info = my_empty_container_info
            req = Request.blank(
                '/v1/a/c',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'http://foo.com',
                         'Access-Control-Request-Method': 'GET'})
            resp = controller.OPTIONS(req)
            self.assertEqual(401, resp.status_int)

            def my_empty_origin_container_info(*args):
                return {'cors': {'allow_origin': None}}
            controller.container_info = my_empty_origin_container_info
            req = Request.blank(
                '/v1/a/c',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'http://foo.com',
                         'Access-Control-Request-Method': 'GET'})
            resp = controller.OPTIONS(req)
            self.assertEqual(401, resp.status_int)

            def my_container_info(*args):
                return {
                    'cors': {
                        'allow_origin': 'http://foo.bar:8080 https://foo.bar',
                        'max_age': '999',
                    }
                }
            controller.container_info = my_container_info
            req = Request.blank(
                '/v1/a/c',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'https://foo.bar',
                         'Access-Control-Request-Method': 'GET'})
            req.content_length = 0
            resp = controller.OPTIONS(req)
            self.assertEqual(200, resp.status_int)
            self.assertEqual(
                'https://foo.bar',
                resp.headers['access-control-allow-origin'])
            for verb in 'OPTIONS GET POST PUT DELETE HEAD'.split():
                self.assertIn(verb,
                              resp.headers['access-control-allow-methods'])
            self.assertEqual(
                len(resp.headers['access-control-allow-methods'].split(', ')),
                6)
            self.assertEqual('999', resp.headers['access-control-max-age'])
            req = Request.blank(
                '/v1/a/c',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'https://foo.bar'})
            req.content_length = 0
            resp = controller.OPTIONS(req)
            self.assertEqual(401, resp.status_int)
            req = Request.blank('/v1/a/c', {'REQUEST_METHOD': 'OPTIONS'})
            req.content_length = 0
            resp = controller.OPTIONS(req)
            self.assertEqual(200, resp.status_int)
            for verb in 'OPTIONS GET POST PUT DELETE HEAD'.split():
                self.assertIn(verb, resp.headers['Allow'])
            self.assertEqual(len(resp.headers['Allow'].split(', ')), 6)
            req = Request.blank(
                '/v1/a/c',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'http://foo.bar',
                         'Access-Control-Request-Method': 'GET'})
            resp = controller.OPTIONS(req)
            self.assertEqual(401, resp.status_int)
            req = Request.blank(
                '/v1/a/c',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'http://foo.bar',
                         'Access-Control-Request-Method': 'GET'})
            controller.app.cors_allow_origin = ['http://foo.bar', ]
            resp = controller.OPTIONS(req)
            self.assertEqual(200, resp.status_int)

            def my_container_info_wildcard(*args):
                return {
                    'cors': {
                        'allow_origin': '*',
                        'max_age': '999',
                    }
                }
            controller.container_info = my_container_info_wildcard
            req = Request.blank(
                '/v1/a/c/o.jpg',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'https://bar.baz',
                         'Access-Control-Request-Headers': ' , ,,',
                         'Access-Control-Request-Method': 'GET'})
            req.content_length = 0
            resp = controller.OPTIONS(req)
            self.assertEqual(200, resp.status_int)
            self.assertEqual('*', resp.headers['access-control-allow-origin'])
            self.assertNotIn('access-control-allow-headers', resp.headers)
            for verb in 'OPTIONS GET POST PUT DELETE HEAD'.split():
                self.assertIn(verb,
                              resp.headers['access-control-allow-methods'])
            self.assertEqual(
                len(resp.headers['access-control-allow-methods'].split(', ')),
                6)
            self.assertEqual('999', resp.headers['access-control-max-age'])

            req = Request.blank(
                '/v1/a/c/o.jpg',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'https://bar.baz',
                         'Access-Control-Request-Headers':
                         'x-foo, x-bar, , x-auth-token',
                         'Access-Control-Request-Method': 'GET'}
            )
            req.content_length = 0
            resp = controller.OPTIONS(req)
            self.assertEqual(200, resp.status_int)
            self.assertEqual(
                sortHeaderNames('x-foo, x-bar, x-auth-token'),
                sortHeaderNames(resp.headers['access-control-allow-headers']))
            self.assertEqual('Access-Control-Request-Headers',
                             resp.headers.get('vary'))

    def test_CORS_valid(self):
        with save_globals():
            controller = proxy_server.ContainerController(self.app, 'a', 'c')

            def stubContainerInfo(*args):
                return {
                    'cors': {
                        'allow_origin': 'http://foo.bar'
                    }
                }
            controller.container_info = stubContainerInfo

            def containerGET(controller, req):
                return Response(headers={
                    'X-Container-Meta-Color': 'red',
                    'X-Super-Secret': 'hush',
                })

            req = Request.blank(
                '/v1/a/c',
                {'REQUEST_METHOD': 'GET'},
                headers={'Origin': 'http://foo.bar'})

            resp = cors_validation(containerGET)(controller, req)

            self.assertEqual(200, resp.status_int)
            self.assertEqual('http://foo.bar',
                             resp.headers['access-control-allow-origin'])
            self.assertEqual('red', resp.headers['x-container-meta-color'])
            # X-Super-Secret is in the response, but not "exposed"
            self.assertEqual('hush', resp.headers['x-super-secret'])
            self.assertIn('access-control-expose-headers', resp.headers)
            exposed = set(
                h.strip() for h in
                resp.headers['access-control-expose-headers'].split(','))
            expected_exposed = set([
                'cache-control', 'content-language', 'content-type', 'expires',
                'last-modified', 'pragma', 'etag', 'x-timestamp', 'x-trans-id',
                'x-openstack-request-id', 'x-container-meta-color'])
            self.assertEqual(expected_exposed, exposed)

    def _gather_x_account_headers(self, controller_call, req, *connect_args,
                                  **kwargs):
        seen_headers = []
        to_capture = ('X-Account-Partition', 'X-Account-Host',
                      'X-Account-Device')

        def capture_headers(ipaddr, port, device, partition, method,
                            path, headers=None, query_string=None):
            captured = {}
            for header in to_capture:
                captured[header] = headers.get(header)
            seen_headers.append(captured)

        with save_globals():
            self.app.allow_account_management = True

            set_http_connect(*connect_args, give_connect=capture_headers,
                             **kwargs)
            resp = controller_call(req)
            self.assertEqual(2, resp.status_int // 100)  # sanity check

            # don't care about the account HEAD, so throw away the
            # first element
            return sorted(seen_headers[1:],
                          key=lambda d: d['X-Account-Host'] or 'Z')

    def test_PUT_x_account_headers_with_fewer_account_replicas(self):
        self.app.account_ring.set_replicas(2)
        req = Request.blank('/v1/a/c', headers={'': ''})
        controller = proxy_server.ContainerController(self.app, 'a', 'c')

        seen_headers = self._gather_x_account_headers(
            controller.PUT, req,
            200, 201, 201, 201)    # HEAD PUT PUT PUT
        self.assertEqual(seen_headers, [
            {'X-Account-Host': '10.0.0.0:1000',
             'X-Account-Partition': '0',
             'X-Account-Device': 'sda'},
            {'X-Account-Host': '10.0.0.1:1001',
             'X-Account-Partition': '0',
             'X-Account-Device': 'sdb'},
            {'X-Account-Host': None,
             'X-Account-Partition': None,
             'X-Account-Device': None}
        ])

    def test_PUT_x_account_headers_with_more_account_replicas(self):
        self.app.account_ring.set_replicas(4)
        req = Request.blank('/v1/a/c', headers={'': ''})
        controller = proxy_server.ContainerController(self.app, 'a', 'c')

        seen_headers = self._gather_x_account_headers(
            controller.PUT, req,
            200, 201, 201, 201)    # HEAD PUT PUT PUT
        self.assertEqual(seen_headers, [
            {'X-Account-Host': '10.0.0.0:1000,10.0.0.3:1003',
             'X-Account-Partition': '0',
             'X-Account-Device': 'sda,sdd'},
            {'X-Account-Host': '10.0.0.1:1001',
             'X-Account-Partition': '0',
             'X-Account-Device': 'sdb'},
            {'X-Account-Host': '10.0.0.2:1002',
             'X-Account-Partition': '0',
             'X-Account-Device': 'sdc'}
        ])

    def test_DELETE_x_account_headers_with_fewer_account_replicas(self):
        self.app.account_ring.set_replicas(2)
        req = Request.blank('/v1/a/c', headers={'': ''})
        controller = proxy_server.ContainerController(self.app, 'a', 'c')

        seen_headers = self._gather_x_account_headers(
            controller.DELETE, req,
            200, 204, 204, 204)    # HEAD DELETE DELETE DELETE
        self.assertEqual(seen_headers, [
            {'X-Account-Host': '10.0.0.0:1000',
             'X-Account-Partition': '0',
             'X-Account-Device': 'sda'},
            {'X-Account-Host': '10.0.0.1:1001',
             'X-Account-Partition': '0',
             'X-Account-Device': 'sdb'},
            {'X-Account-Host': None,
             'X-Account-Partition': None,
             'X-Account-Device': None}
        ])

    def test_DELETE_x_account_headers_with_more_account_replicas(self):
        self.app.account_ring.set_replicas(4)
        req = Request.blank('/v1/a/c', headers={'': ''})
        controller = proxy_server.ContainerController(self.app, 'a', 'c')

        seen_headers = self._gather_x_account_headers(
            controller.DELETE, req,
            200, 204, 204, 204)    # HEAD DELETE DELETE DELETE
        self.assertEqual(seen_headers, [
            {'X-Account-Host': '10.0.0.0:1000,10.0.0.3:1003',
             'X-Account-Partition': '0',
             'X-Account-Device': 'sda,sdd'},
            {'X-Account-Host': '10.0.0.1:1001',
             'X-Account-Partition': '0',
             'X-Account-Device': 'sdb'},
            {'X-Account-Host': '10.0.0.2:1002',
             'X-Account-Partition': '0',
             'X-Account-Device': 'sdc'}
        ])

    def test_PUT_backed_x_timestamp_header(self):
        timestamps = []

        def capture_timestamps(*args, **kwargs):
            headers = kwargs['headers']
            timestamps.append(headers.get('X-Timestamp'))

        req = Request.blank('/v1/a/c', method='PUT', headers={'': ''})
        with save_globals():
            new_connect = set_http_connect(200,  # account existence check
                                           201, 201, 201,
                                           give_connect=capture_timestamps)
            resp = self.app.handle_request(req)

        # sanity
        self.assertRaises(StopIteration, new_connect.code_iter.next)
        self.assertEqual(2, resp.status_int // 100)

        timestamps.pop(0)  # account existence check
        self.assertEqual(3, len(timestamps))
        for timestamp in timestamps:
            self.assertEqual(timestamp, timestamps[0])
            self.assertTrue(re.match('[0-9]{10}\.[0-9]{5}', timestamp))

    def test_DELETE_backed_x_timestamp_header(self):
        timestamps = []

        def capture_timestamps(*args, **kwargs):
            headers = kwargs['headers']
            timestamps.append(headers.get('X-Timestamp'))

        req = Request.blank('/v1/a/c', method='DELETE', headers={'': ''})
        self.app.update_request(req)
        with save_globals():
            new_connect = set_http_connect(200,  # account existence check
                                           201, 201, 201,
                                           give_connect=capture_timestamps)
            resp = self.app.handle_request(req)

        # sanity
        self.assertRaises(StopIteration, new_connect.code_iter.next)
        self.assertEqual(2, resp.status_int // 100)

        timestamps.pop(0)  # account existence check
        self.assertEqual(3, len(timestamps))
        for timestamp in timestamps:
            self.assertEqual(timestamp, timestamps[0])
            self.assertTrue(re.match('[0-9]{10}\.[0-9]{5}', timestamp))

    def test_node_read_timeout_retry_to_container(self):
        with save_globals():
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': 'GET'})
            self.app.node_timeout = 0.1
            set_http_connect(200, 200, 200, body='abcdef', slow=[1.0, 1.0])
            resp = req.get_response(self.app)
            got_exc = False
            try:
                resp.body
            except ChunkReadTimeout:
                got_exc = True
            self.assertTrue(got_exc)


@patch_policies([StoragePolicy(0, 'zero', True, object_ring=FakeRing())])
class TestAccountController(unittest.TestCase):

    def setUp(self):
        conf = {'error_suppression_interval': 0}
        self.app = proxy_server.Application(conf, FakeMemcache(),
                                            account_ring=FakeRing(),
                                            container_ring=FakeRing())

    def assert_status_map(self, method, statuses, expected, env_expected=None,
                          headers=None, **kwargs):
        headers = headers or {}
        with save_globals():
            set_http_connect(*statuses, **kwargs)
            req = Request.blank('/v1/a', {}, headers=headers)
            self.app.update_request(req)
            res = method(req)
            self.assertEqual(res.status_int, expected)
            infocache = res.environ.get('swift.infocache', {})
            if env_expected:
                self.assertEqual(infocache['account/a']['status'],
                                 env_expected)
            set_http_connect(*statuses)
            req = Request.blank('/v1/a/', {})
            self.app.update_request(req)
            res = method(req)
            infocache = res.environ.get('swift.infocache', {})
            self.assertEqual(res.status_int, expected)
            if env_expected:
                self.assertEqual(infocache['account/a']['status'],
                                 env_expected)

    def test_OPTIONS(self):
        with save_globals():
            self.app.allow_account_management = False
            controller = proxy_server.AccountController(self.app, 'account')
            req = Request.blank('/v1/account', {'REQUEST_METHOD': 'OPTIONS'})
            req.content_length = 0
            resp = controller.OPTIONS(req)
            self.assertEqual(200, resp.status_int)
            for verb in 'OPTIONS GET POST HEAD'.split():
                self.assertIn(verb, resp.headers['Allow'])
            self.assertEqual(len(resp.headers['Allow'].split(', ')), 4)

            # Test a CORS OPTIONS request (i.e. including Origin and
            # Access-Control-Request-Method headers)
            self.app.allow_account_management = False
            controller = proxy_server.AccountController(self.app, 'account')
            req = Request.blank(
                '/v1/account', {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'http://foo.com',
                         'Access-Control-Request-Method': 'GET'})
            req.content_length = 0
            resp = controller.OPTIONS(req)
            self.assertEqual(200, resp.status_int)
            for verb in 'OPTIONS GET POST HEAD'.split():
                self.assertIn(verb, resp.headers['Allow'])
            self.assertEqual(len(resp.headers['Allow'].split(', ')), 4)

            self.app.allow_account_management = True
            controller = proxy_server.AccountController(self.app, 'account')
            req = Request.blank('/v1/account', {'REQUEST_METHOD': 'OPTIONS'})
            req.content_length = 0
            resp = controller.OPTIONS(req)
            self.assertEqual(200, resp.status_int)
            for verb in 'OPTIONS GET POST PUT DELETE HEAD'.split():
                self.assertIn(verb, resp.headers['Allow'])
            self.assertEqual(len(resp.headers['Allow'].split(', ')), 6)

    def test_GET(self):
        with save_globals():
            controller = proxy_server.AccountController(self.app, 'a')
            # GET returns after the first successful call to an Account Server
            self.assert_status_map(controller.GET, (200,), 200, 200)
            self.assert_status_map(controller.GET, (503, 200), 200, 200)
            self.assert_status_map(controller.GET, (503, 503, 200), 200, 200)
            self.assert_status_map(controller.GET, (204,), 204, 204)
            self.assert_status_map(controller.GET, (503, 204), 204, 204)
            self.assert_status_map(controller.GET, (503, 503, 204), 204, 204)
            self.assert_status_map(controller.GET, (404, 200), 200, 200)
            self.assert_status_map(controller.GET, (404, 404, 200), 200, 200)
            self.assert_status_map(controller.GET, (404, 503, 204), 204, 204)
            # If Account servers fail, if autocreate = False, return majority
            # response
            self.assert_status_map(controller.GET, (404, 404, 404), 404, 404)
            self.assert_status_map(controller.GET, (404, 404, 503), 404, 404)
            self.assert_status_map(controller.GET, (404, 503, 503), 503)

            self.app.memcache = FakeMemcacheReturnsNone()
            self.assert_status_map(controller.GET, (404, 404, 404), 404, 404)

    def test_GET_autocreate(self):
        with save_globals():
            controller = proxy_server.AccountController(self.app, 'a')
            self.app.memcache = FakeMemcacheReturnsNone()
            self.assertFalse(self.app.account_autocreate)
            # Repeat the test for autocreate = False and 404 by all
            self.assert_status_map(controller.GET,
                                   (404, 404, 404), 404)
            self.assert_status_map(controller.GET,
                                   (404, 503, 404), 404)
            # When autocreate is True, if none of the nodes respond 2xx
            # And quorum of the nodes responded 404,
            # ALL nodes are asked to create the account
            # If successful, the GET request is repeated.
            controller.app.account_autocreate = True
            expected = 200
            self.assert_status_map(controller.GET, (404, 404, 404), expected)
            self.assert_status_map(controller.GET, (404, 503, 404), expected)
            # We always return 503 if no majority between 4xx, 3xx or 2xx found
            self.assert_status_map(controller.GET,
                                   (500, 500, 400), 503)

    def _check_autocreate_listing_with_query_string(self, query_string):
        controller = proxy_server.AccountController(self.app, 'a')
        controller.app.account_autocreate = True
        statuses = (404, 404, 404)
        expected = 200
        # get the response to check it has json content
        with save_globals():
            set_http_connect(*statuses)
            req = Request.blank('/v1/a' + query_string)
            self.app.update_request(req)
            res = controller.GET(req)
            headers = res.headers
            self.assertEqual(
                'yes', headers.get('X-Backend-Fake-Account-Listing'))
            self.assertEqual(
                'application/json; charset=utf-8',
                headers.get('Content-Type'))
            self.assertEqual([], json.loads(res.body))
            self.assertEqual(res.status_int, expected)

    def test_auto_create_account_listing_response_is_json(self):
        self._check_autocreate_listing_with_query_string('')
        self._check_autocreate_listing_with_query_string('?format=plain')
        self._check_autocreate_listing_with_query_string('?format=json')
        self._check_autocreate_listing_with_query_string('?format=xml')

    def test_HEAD(self):
        # Same behaviour as GET
        with save_globals():
            controller = proxy_server.AccountController(self.app, 'a')
            self.assert_status_map(controller.HEAD, (200,), 200, 200)
            self.assert_status_map(controller.HEAD, (503, 200), 200, 200)
            self.assert_status_map(controller.HEAD, (503, 503, 200), 200, 200)
            self.assert_status_map(controller.HEAD, (204,), 204, 204)
            self.assert_status_map(controller.HEAD, (503, 204), 204, 204)
            self.assert_status_map(controller.HEAD, (204, 503, 503), 204, 204)
            self.assert_status_map(controller.HEAD, (204,), 204, 204)
            self.assert_status_map(controller.HEAD, (404, 404, 404), 404, 404)
            self.assert_status_map(controller.HEAD, (404, 404, 200), 200, 200)
            self.assert_status_map(controller.HEAD, (404, 200), 200, 200)
            self.assert_status_map(controller.HEAD, (404, 404, 503), 404, 404)
            self.assert_status_map(controller.HEAD, (404, 503, 503), 503)
            self.assert_status_map(controller.HEAD, (404, 503, 204), 204, 204)

    def test_HEAD_autocreate(self):
        # Same behaviour as GET
        with save_globals():
            controller = proxy_server.AccountController(self.app, 'a')
            self.app.memcache = FakeMemcacheReturnsNone()
            self.assertFalse(self.app.account_autocreate)
            self.assert_status_map(controller.HEAD,
                                   (404, 404, 404), 404)
            controller.app.account_autocreate = True
            self.assert_status_map(controller.HEAD,
                                   (404, 404, 404), 200)
            self.assert_status_map(controller.HEAD,
                                   (500, 404, 404), 200)
            # We always return 503 if no majority between 4xx, 3xx or 2xx found
            self.assert_status_map(controller.HEAD,
                                   (500, 500, 400), 503)

    def test_POST_autocreate(self):
        with save_globals():
            controller = proxy_server.AccountController(self.app, 'a')
            self.app.memcache = FakeMemcacheReturnsNone()
            # first test with autocreate being False
            self.assertFalse(self.app.account_autocreate)
            self.assert_status_map(controller.POST,
                                   (404, 404, 404), 404)
            # next turn it on and test account being created than updated
            controller.app.account_autocreate = True
            self.assert_status_map(
                controller.POST,
                (404, 404, 404, 202, 202, 202, 201, 201, 201), 201)
            # account_info  PUT account  POST account
            self.assert_status_map(
                controller.POST,
                (404, 404, 503, 201, 201, 503, 204, 204, 504), 204)
            # what if create fails
            self.assert_status_map(
                controller.POST,
                (404, 404, 404, 403, 403, 403, 400, 400, 400), 400)

    def test_POST_autocreate_with_sysmeta(self):
        with save_globals():
            controller = proxy_server.AccountController(self.app, 'a')
            self.app.memcache = FakeMemcacheReturnsNone()
            # first test with autocreate being False
            self.assertFalse(self.app.account_autocreate)
            self.assert_status_map(controller.POST,
                                   (404, 404, 404), 404)
            # next turn it on and test account being created than updated
            controller.app.account_autocreate = True
            calls = []
            callback = _make_callback_func(calls)
            key, value = 'X-Account-Sysmeta-Blah', 'something'
            headers = {key: value}
            self.assert_status_map(
                controller.POST,
                (404, 404, 404, 202, 202, 202, 201, 201, 201), 201,
                #  POST       , autocreate PUT, POST again
                headers=headers,
                give_connect=callback)
            self.assertEqual(9, len(calls))
            for call in calls:
                self.assertIn(key, call['headers'],
                              '%s call, key %s missing in headers %s' %
                              (call['method'], key, call['headers']))
                self.assertEqual(value, call['headers'][key])

    def test_connection_refused(self):
        self.app.account_ring.get_nodes('account')
        for dev in self.app.account_ring.devs:
            dev['ip'] = '127.0.0.1'
            dev['port'] = 1  # can't connect on this port
        controller = proxy_server.AccountController(self.app, 'account')
        req = Request.blank('/v1/account', environ={'REQUEST_METHOD': 'HEAD'})
        self.app.update_request(req)
        resp = controller.HEAD(req)
        self.assertEqual(resp.status_int, 503)

    def test_other_socket_error(self):
        self.app.account_ring.get_nodes('account')
        for dev in self.app.account_ring.devs:
            dev['ip'] = '127.0.0.1'
            dev['port'] = -1  # invalid port number
        controller = proxy_server.AccountController(self.app, 'account')
        req = Request.blank('/v1/account', environ={'REQUEST_METHOD': 'HEAD'})
        self.app.update_request(req)
        resp = controller.HEAD(req)
        self.assertEqual(resp.status_int, 503)

    def test_response_get_accept_ranges_header(self):
        with save_globals():
            set_http_connect(200, 200, body='{}')
            controller = proxy_server.AccountController(self.app, 'account')
            req = Request.blank('/v1/a?format=json')
            self.app.update_request(req)
            res = controller.GET(req)
            self.assertIn('accept-ranges', res.headers)
            self.assertEqual(res.headers['accept-ranges'], 'bytes')

    def test_response_head_accept_ranges_header(self):
        with save_globals():
            set_http_connect(200, 200, body='{}')
            controller = proxy_server.AccountController(self.app, 'account')
            req = Request.blank('/v1/a?format=json')
            self.app.update_request(req)
            res = controller.HEAD(req)
            res.body
            self.assertIn('accept-ranges', res.headers)
            self.assertEqual(res.headers['accept-ranges'], 'bytes')

    def test_PUT(self):
        with save_globals():
            controller = proxy_server.AccountController(self.app, 'account')

            def test_status_map(statuses, expected, **kwargs):
                set_http_connect(*statuses, **kwargs)
                self.app.memcache.store = {}
                req = Request.blank('/v1/a', {})
                req.content_length = 0
                self.app.update_request(req)
                res = controller.PUT(req)
                expected = str(expected)
                self.assertEqual(res.status[:len(expected)], expected)
            test_status_map((201, 201, 201), 405)
            self.app.allow_account_management = True
            test_status_map((201, 201, 201), 201)
            test_status_map((201, 201, 500), 201)
            test_status_map((201, 500, 500), 503)
            test_status_map((204, 500, 404), 503)

    def test_PUT_max_account_name_length(self):
        self.app.allow_account_management = True
        limit = constraints.MAX_ACCOUNT_NAME_LENGTH
        controller = proxy_server.AccountController(self.app, '1' * limit)
        self.assert_status_map(controller.PUT, (201, 201, 201), 201)
        controller = proxy_server.AccountController(
            self.app, '2' * (limit + 1))
        self.assert_status_map(controller.PUT, (), 400)

        # internal auto-created accounts get higher limits
        limit *= 2
        controller = proxy_server.AccountController(
            self.app, '.' + '3' * (limit - 1))
        self.assert_status_map(controller.PUT, (201, 201, 201), 201)
        controller = proxy_server.AccountController(
            self.app, '.' + '4' * limit)
        self.assert_status_map(controller.PUT, (), 400)

        self.app.auto_create_account_prefix = 'FOO_'
        limit /= 2
        controller = proxy_server.AccountController(
            self.app, '.' + '5' * (limit - 1))
        self.assert_status_map(controller.PUT, (201, 201, 201), 201)
        controller = proxy_server.AccountController(
            self.app, '.' + '6' * limit)
        self.assert_status_map(controller.PUT, (), 400)

        limit *= 2
        controller = proxy_server.AccountController(
            self.app, 'FOO_' + '7' * (limit - 4))
        self.assert_status_map(controller.PUT, (201, 201, 201), 201)
        controller = proxy_server.AccountController(
            self.app, 'FOO_' + '8' * (limit - 3))
        self.assert_status_map(controller.PUT, (), 400)

    def test_PUT_connect_exceptions(self):
        with save_globals():
            self.app.allow_account_management = True
            controller = proxy_server.AccountController(self.app, 'account')
            self.assert_status_map(controller.PUT, (201, 201, -1), 201)
            self.assert_status_map(controller.PUT, (201, -1, -1), 503)
            self.assert_status_map(controller.PUT, (503, 503, -1), 503)

    def test_PUT_status(self):
        with save_globals():
            self.app.allow_account_management = True
            controller = proxy_server.AccountController(self.app, 'account')
            self.assert_status_map(controller.PUT, (201, 201, 202), 202)

    def test_PUT_metadata(self):
        self.metadata_helper('PUT')

    def test_POST_metadata(self):
        self.metadata_helper('POST')

    def metadata_helper(self, method):
        for test_header, test_value in (
                ('X-Account-Meta-TestHeader', 'TestValue'),
                ('X-Account-Meta-TestHeader', ''),
                ('X-Remove-Account-Meta-TestHeader', 'anything')):
            test_errors = []

            def test_connect(ipaddr, port, device, partition, method, path,
                             headers=None, query_string=None):
                if path == '/a':
                    find_header = test_header
                    find_value = test_value
                    if find_header.lower().startswith('x-remove-'):
                        find_header = \
                            find_header.lower().replace('-remove', '', 1)
                        find_value = ''
                    for k, v in headers.items():
                        if k.lower() == find_header.lower() and \
                                v == find_value:
                            break
                    else:
                        test_errors.append('%s: %s not in %s' %
                                           (find_header, find_value, headers))
            with save_globals():
                self.app.allow_account_management = True
                controller = \
                    proxy_server.AccountController(self.app, 'a')
                set_http_connect(201, 201, 201, give_connect=test_connect)
                req = Request.blank('/v1/a/c',
                                    environ={'REQUEST_METHOD': method},
                                    headers={test_header: test_value})
                self.app.update_request(req)
                getattr(controller, method)(req)
                self.assertEqual(test_errors, [])

    def test_PUT_bad_metadata(self):
        self.bad_metadata_helper('PUT')

    def test_POST_bad_metadata(self):
        self.bad_metadata_helper('POST')

    def bad_metadata_helper(self, method):
        with save_globals():
            self.app.allow_account_management = True
            controller = proxy_server.AccountController(self.app, 'a')
            set_http_connect(200, 201, 201, 201)
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEqual(resp.status_int, 201)

            set_http_connect(201, 201, 201)
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers={'X-Account-Meta-' +
                                ('a' * constraints.MAX_META_NAME_LENGTH): 'v'})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEqual(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            req = Request.blank(
                '/v1/a/c', environ={'REQUEST_METHOD': method},
                headers={'X-Account-Meta-' +
                         ('a' * (constraints.MAX_META_NAME_LENGTH + 1)): 'v'})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEqual(resp.status_int, 400)

            set_http_connect(201, 201, 201)
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers={'X-Account-Meta-Too-Long':
                                'a' * constraints.MAX_META_VALUE_LENGTH})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEqual(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers={'X-Account-Meta-Too-Long':
                                'a' * (constraints.MAX_META_VALUE_LENGTH + 1)})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEqual(resp.status_int, 400)

            set_http_connect(201, 201, 201)
            headers = {}
            for x in range(constraints.MAX_META_COUNT):
                headers['X-Account-Meta-%d' % x] = 'v'
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers=headers)
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEqual(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            headers = {}
            for x in range(constraints.MAX_META_COUNT + 1):
                headers['X-Account-Meta-%d' % x] = 'v'
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers=headers)
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEqual(resp.status_int, 400)

            set_http_connect(201, 201, 201)
            headers = {}
            header_value = 'a' * constraints.MAX_META_VALUE_LENGTH
            size = 0
            x = 0
            while size < (constraints.MAX_META_OVERALL_SIZE - 4
                          - constraints.MAX_META_VALUE_LENGTH):
                size += 4 + constraints.MAX_META_VALUE_LENGTH
                headers['X-Account-Meta-%04d' % x] = header_value
                x += 1
            if constraints.MAX_META_OVERALL_SIZE - size > 1:
                headers['X-Account-Meta-a'] = \
                    'a' * (constraints.MAX_META_OVERALL_SIZE - size - 1)
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers=headers)
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEqual(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            headers['X-Account-Meta-a'] = \
                'a' * (constraints.MAX_META_OVERALL_SIZE - size)
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers=headers)
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEqual(resp.status_int, 400)

    def test_DELETE(self):
        with save_globals():
            controller = proxy_server.AccountController(self.app, 'account')

            def test_status_map(statuses, expected, **kwargs):
                set_http_connect(*statuses, **kwargs)
                self.app.memcache.store = {}
                req = Request.blank('/v1/a', {'REQUEST_METHOD': 'DELETE'})
                req.content_length = 0
                self.app.update_request(req)
                res = controller.DELETE(req)
                expected = str(expected)
                self.assertEqual(res.status[:len(expected)], expected)
            test_status_map((201, 201, 201), 405)
            self.app.allow_account_management = True
            test_status_map((201, 201, 201), 201)
            test_status_map((201, 201, 500), 201)
            test_status_map((201, 500, 500), 503)
            test_status_map((204, 500, 404), 503)

    def test_DELETE_with_query_string(self):
        # Extra safety in case someone typos a query string for an
        # account-level DELETE request that was really meant to be caught by
        # some middleware.
        with save_globals():
            controller = proxy_server.AccountController(self.app, 'account')

            def test_status_map(statuses, expected, **kwargs):
                set_http_connect(*statuses, **kwargs)
                self.app.memcache.store = {}
                req = Request.blank('/v1/a?whoops',
                                    environ={'REQUEST_METHOD': 'DELETE'})
                req.content_length = 0
                self.app.update_request(req)
                res = controller.DELETE(req)
                expected = str(expected)
                self.assertEqual(res.status[:len(expected)], expected)
            test_status_map((201, 201, 201), 400)
            self.app.allow_account_management = True
            test_status_map((201, 201, 201), 400)
            test_status_map((201, 201, 500), 400)
            test_status_map((201, 500, 500), 400)
            test_status_map((204, 500, 404), 400)


@patch_policies([StoragePolicy(0, 'zero', True, object_ring=FakeRing())])
class TestAccountControllerFakeGetResponse(unittest.TestCase):
    """
    Test all the faked-out GET responses for accounts that don't exist. They
    have to match the responses for empty accounts that really exist.
    """
    def setUp(self):
        conf = {'account_autocreate': 'yes'}
        self.app = listing_formats.ListingFilter(
            proxy_server.Application(conf, FakeMemcache(),
                                     account_ring=FakeRing(),
                                     container_ring=FakeRing()))
        self.app.app.memcache = FakeMemcacheReturnsNone()

    def test_GET_autocreate_accept_json(self):
        with save_globals():
            set_http_connect(*([404] * 100))  # nonexistent: all backends 404
            req = Request.blank(
                '/v1/a', headers={'Accept': 'application/json'},
                environ={'REQUEST_METHOD': 'GET',
                         'PATH_INFO': '/v1/a'})
            resp = req.get_response(self.app)
            self.assertEqual(200, resp.status_int)
            self.assertEqual('application/json; charset=utf-8',
                             resp.headers['Content-Type'])
            self.assertEqual("[]", resp.body)

    def test_GET_autocreate_format_json(self):
        with save_globals():
            set_http_connect(*([404] * 100))  # nonexistent: all backends 404
            req = Request.blank('/v1/a?format=json',
                                environ={'REQUEST_METHOD': 'GET',
                                         'PATH_INFO': '/v1/a',
                                         'QUERY_STRING': 'format=json'})
            resp = req.get_response(self.app)
            self.assertEqual(200, resp.status_int)
            self.assertEqual('application/json; charset=utf-8',
                             resp.headers['Content-Type'])
            self.assertEqual("[]", resp.body)

    def test_GET_autocreate_accept_xml(self):
        with save_globals():
            set_http_connect(*([404] * 100))  # nonexistent: all backends 404
            req = Request.blank('/v1/a', headers={"Accept": "text/xml"},
                                environ={'REQUEST_METHOD': 'GET',
                                         'PATH_INFO': '/v1/a'})

            resp = req.get_response(self.app)
            self.assertEqual(200, resp.status_int)

            self.assertEqual('text/xml; charset=utf-8',
                             resp.headers['Content-Type'])
            empty_xml_listing = ('<?xml version="1.0" encoding="UTF-8"?>\n'
                                 '<account name="a">\n</account>')
            self.assertEqual(empty_xml_listing, resp.body)

    def test_GET_autocreate_format_xml(self):
        with save_globals():
            set_http_connect(*([404] * 100))  # nonexistent: all backends 404
            req = Request.blank('/v1/a?format=xml',
                                environ={'REQUEST_METHOD': 'GET',
                                         'PATH_INFO': '/v1/a',
                                         'QUERY_STRING': 'format=xml'})
            resp = req.get_response(self.app)
            self.assertEqual(200, resp.status_int)
            self.assertEqual('application/xml; charset=utf-8',
                             resp.headers['Content-Type'])
            empty_xml_listing = ('<?xml version="1.0" encoding="UTF-8"?>\n'
                                 '<account name="a">\n</account>')
            self.assertEqual(empty_xml_listing, resp.body)

    def test_GET_autocreate_accept_unknown(self):
        with save_globals():
            set_http_connect(*([404] * 100))  # nonexistent: all backends 404
            req = Request.blank('/v1/a', headers={"Accept": "mystery/meat"},
                                environ={'REQUEST_METHOD': 'GET',
                                         'PATH_INFO': '/v1/a'})
            resp = req.get_response(self.app)
            self.assertEqual(406, resp.status_int)

    def test_GET_autocreate_bad_accept(self):
        with save_globals():
            set_http_connect(*([404] * 100))  # nonexistent: all backends 404
            req = Request.blank('/v1/a', headers={"Accept": "a/b;q=nope"},
                                environ={'REQUEST_METHOD': 'GET',
                                         'PATH_INFO': '/v1/a'})
            resp = req.get_response(self.app)
            self.assertEqual(400, resp.status_int)
            self.assertEqual('Invalid Accept header', resp.body)

            set_http_connect(*([404] * 100))  # nonexistent: all backends 404
            req = Request.blank('/v1/a', headers={"Accept": "a/b;q=0.5;q=1"},
                                environ={'REQUEST_METHOD': 'GET',
                                         'PATH_INFO': '/v1/a'})
            resp = req.get_response(self.app)
            self.assertEqual(400, resp.status_int)
            self.assertEqual('Invalid Accept header', resp.body)

    def test_GET_autocreate_format_invalid_utf8(self):
        with save_globals():
            set_http_connect(*([404] * 100))  # nonexistent: all backends 404
            req = Request.blank('/v1/a?format=\xff\xfe',
                                environ={'REQUEST_METHOD': 'GET',
                                         'PATH_INFO': '/v1/a',
                                         'QUERY_STRING': 'format=\xff\xfe'})
            resp = req.get_response(self.app)
            self.assertEqual(400, resp.status_int)

    def test_account_acl_header_access(self):
        acl = {
            'admin': ['AUTH_alice'],
            'read-write': ['AUTH_bob'],
            'read-only': ['AUTH_carol'],
        }
        prefix = get_sys_meta_prefix('account')
        privileged_headers = {(prefix + 'core-access-control'): format_acl(
            version=2, acl_dict=acl)}

        app = proxy_server.Application(
            None, FakeMemcache(), account_ring=FakeRing(),
            container_ring=FakeRing())

        with save_globals():
            # Mock account server will provide privileged information (ACLs)
            set_http_connect(200, 200, 200, headers=privileged_headers)
            req = Request.blank('/v1/a', environ={'REQUEST_METHOD': 'GET'})
            resp = app.handle_request(req)

            # Not a swift_owner -- ACLs should NOT be in response
            header = 'X-Account-Access-Control'
            self.assertNotIn(header, resp.headers, '%r was in %r' % (
                header, resp.headers))

            # Same setup -- mock acct server will provide ACLs
            set_http_connect(200, 200, 200, headers=privileged_headers)
            req = Request.blank('/v1/a', environ={'REQUEST_METHOD': 'GET',
                                                  'swift_owner': True})
            resp = app.handle_request(req)

            # For a swift_owner, the ACLs *should* be in response
            self.assertIn(header, resp.headers, '%r not in %r' % (
                header, resp.headers))

    def test_account_acls_through_delegation(self):

        # Define a way to grab the requests sent out from the AccountController
        # to the Account Server, and a way to inject responses we'd like the
        # Account Server to return.
        resps_to_send = []

        @contextmanager
        def patch_account_controller_method(verb):
            old_method = getattr(proxy_server.AccountController, verb)
            new_method = lambda self, req, *_, **__: resps_to_send.pop(0)
            try:
                setattr(proxy_server.AccountController, verb, new_method)
                yield
            finally:
                setattr(proxy_server.AccountController, verb, old_method)

        def make_test_request(http_method, swift_owner=True):
            env = {
                'REQUEST_METHOD': http_method,
                'swift_owner': swift_owner,
            }
            acl = {
                'admin': ['foo'],
                'read-write': ['bar'],
                'read-only': ['bas'],
            }
            headers = {} if http_method in ('GET', 'HEAD') else {
                'x-account-access-control': format_acl(version=2, acl_dict=acl)
            }

            return Request.blank('/v1/a', environ=env, headers=headers)

        # Our AccountController will invoke methods to communicate with the
        # Account Server, and they will return responses like these:
        def make_canned_response(http_method):
            acl = {
                'admin': ['foo'],
                'read-write': ['bar'],
                'read-only': ['bas'],
            }
            headers = {'x-account-sysmeta-core-access-control': format_acl(
                version=2, acl_dict=acl)}
            canned_resp = Response(headers=headers)
            canned_resp.environ = {
                'PATH_INFO': '/acct',
                'REQUEST_METHOD': http_method,
            }
            resps_to_send.append(canned_resp)

        app = proxy_server.Application(
            None, FakeMemcache(), account_ring=FakeRing(),
            container_ring=FakeRing())
        app.allow_account_management = True

        ext_header = 'x-account-access-control'
        with patch_account_controller_method('GETorHEAD_base'):
            # GET/HEAD requests should remap sysmeta headers from acct server
            for verb in ('GET', 'HEAD'):
                make_canned_response(verb)
                req = make_test_request(verb)
                resp = app.handle_request(req)
                h = parse_acl(version=2, data=resp.headers.get(ext_header))
                self.assertEqual(h['admin'], ['foo'])
                self.assertEqual(h['read-write'], ['bar'])
                self.assertEqual(h['read-only'], ['bas'])

                # swift_owner = False: GET/HEAD shouldn't return sensitive info
                make_canned_response(verb)
                req = make_test_request(verb, swift_owner=False)
                resp = app.handle_request(req)
                h = resp.headers
                self.assertIsNone(h.get(ext_header))

                # swift_owner unset: GET/HEAD shouldn't return sensitive info
                make_canned_response(verb)
                req = make_test_request(verb, swift_owner=False)
                del req.environ['swift_owner']
                resp = app.handle_request(req)
                h = resp.headers
                self.assertIsNone(h.get(ext_header))

        # Verify that PUT/POST requests remap sysmeta headers from acct server
        with patch_account_controller_method('make_requests'):
            make_canned_response('PUT')
            req = make_test_request('PUT')
            resp = app.handle_request(req)

            h = parse_acl(version=2, data=resp.headers.get(ext_header))
            self.assertEqual(h['admin'], ['foo'])
            self.assertEqual(h['read-write'], ['bar'])
            self.assertEqual(h['read-only'], ['bas'])

            make_canned_response('POST')
            req = make_test_request('POST')
            resp = app.handle_request(req)

            h = parse_acl(version=2, data=resp.headers.get(ext_header))
            self.assertEqual(h['admin'], ['foo'])
            self.assertEqual(h['read-write'], ['bar'])
            self.assertEqual(h['read-only'], ['bas'])


class FakeObjectController(object):

    def __init__(self):
        self.app = self
        self.logger = self
        self.account_name = 'a'
        self.container_name = 'c'
        self.object_name = 'o'
        self.trans_id = 'tx1'
        self.object_ring = FakeRing()
        self.node_timeout = 1
        self.rate_limit_after_segment = 3
        self.rate_limit_segments_per_sec = 2
        self.GETorHEAD_base_args = []

    def exception(self, *args):
        self.exception_args = args
        self.exception_info = sys.exc_info()

    def GETorHEAD_base(self, *args):
        self.GETorHEAD_base_args.append(args)
        req = args[0]
        path = args[4]
        body = data = path[-1] * int(path[-1])
        if req.range:
            r = req.range.ranges_for_length(len(data))
            if r:
                (start, stop) = r[0]
                body = data[start:stop]
        resp = Response(app_iter=iter(body))
        return resp

    def iter_nodes(self, ring, partition):
        for node in ring.get_part_nodes(partition):
            yield node
        for node in ring.get_more_nodes(partition):
            yield node

    def sort_nodes(self, nodes):
        return nodes

    def set_node_timing(self, node, timing):
        return


class TestProxyObjectPerformance(unittest.TestCase):

    def setUp(self):
        # This is just a simple test that can be used to verify and debug the
        # various data paths between the proxy server and the object
        # server. Used as a play ground to debug buffer sizes for sockets.
        skip_if_no_xattrs()
        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        # Client is transmitting in 2 MB chunks
        fd = sock.makefile('wb', 2 * 1024 * 1024)
        # Small, fast for testing
        obj_len = 2 * 64 * 1024
        # Use 1 GB or more for measurements
        # obj_len = 2 * 512 * 1024 * 1024
        self.path = '/v1/a/c/o.large'
        fd.write('PUT %s HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Length: %s\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 '\r\n' % (self.path, str(obj_len)))
        fd.write('a' * obj_len)
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)
        self.obj_len = obj_len

    def test_GET_debug_large_file(self):
        for i in range(10):
            start = time.time()

            prolis = _test_sockets[0]
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            # Client is reading in 2 MB chunks
            fd = sock.makefile('wb', 2 * 1024 * 1024)
            fd.write('GET %s HTTP/1.1\r\n'
                     'Host: localhost\r\n'
                     'Connection: close\r\n'
                     'X-Storage-Token: t\r\n'
                     '\r\n' % self.path)
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = 'HTTP/1.1 200'
            self.assertEqual(headers[:len(exp)], exp)

            total = 0
            while True:
                buf = fd.read(100000)
                if not buf:
                    break
                total += len(buf)
            self.assertEqual(total, self.obj_len)

            end = time.time()
            print("Run %02d took %07.03f" % (i, end - start))


@patch_policies([StoragePolicy(0, 'migrated', object_ring=FakeRing()),
                 StoragePolicy(1, 'ernie', True, object_ring=FakeRing()),
                 StoragePolicy(2, 'deprecated', is_deprecated=True,
                               object_ring=FakeRing()),
                 StoragePolicy(3, 'bert', object_ring=FakeRing())])
class TestSwiftInfo(unittest.TestCase):
    def setUp(self):
        utils._swift_info = {}
        utils._swift_admin_info = {}

    def test_registered_defaults(self):
        proxy_server.Application({}, FakeMemcache(),
                                 account_ring=FakeRing(),
                                 container_ring=FakeRing())

        si = utils.get_swift_info()['swift']
        self.assertIn('version', si)
        self.assertEqual(si['max_file_size'], constraints.MAX_FILE_SIZE)
        self.assertEqual(si['max_meta_name_length'],
                         constraints.MAX_META_NAME_LENGTH)
        self.assertEqual(si['max_meta_value_length'],
                         constraints.MAX_META_VALUE_LENGTH)
        self.assertEqual(si['max_meta_count'], constraints.MAX_META_COUNT)
        self.assertEqual(si['max_header_size'], constraints.MAX_HEADER_SIZE)
        self.assertEqual(si['max_meta_overall_size'],
                         constraints.MAX_META_OVERALL_SIZE)
        self.assertEqual(si['account_listing_limit'],
                         constraints.ACCOUNT_LISTING_LIMIT)
        self.assertEqual(si['container_listing_limit'],
                         constraints.CONTAINER_LISTING_LIMIT)
        self.assertEqual(si['max_account_name_length'],
                         constraints.MAX_ACCOUNT_NAME_LENGTH)
        self.assertEqual(si['max_container_name_length'],
                         constraints.MAX_CONTAINER_NAME_LENGTH)
        self.assertEqual(si['max_object_name_length'],
                         constraints.MAX_OBJECT_NAME_LENGTH)
        self.assertIn('strict_cors_mode', si)
        self.assertFalse(si['allow_account_management'])
        self.assertFalse(si['account_autocreate'])
        # This setting is by default excluded by disallowed_sections
        self.assertEqual(si['valid_api_versions'],
                         constraints.VALID_API_VERSIONS)
        # this next test is deliberately brittle in order to alert if
        # other items are added to swift info
        self.assertEqual(len(si), 18)

        self.assertIn('policies', si)
        sorted_pols = sorted(si['policies'], key=operator.itemgetter('name'))
        self.assertEqual(len(sorted_pols), 3)
        for policy in sorted_pols:
            self.assertNotEqual(policy['name'], 'deprecated')
        self.assertEqual(sorted_pols[0]['name'], 'bert')
        self.assertEqual(sorted_pols[1]['name'], 'ernie')
        self.assertEqual(sorted_pols[2]['name'], 'migrated')


class TestSocketObjectVersions(unittest.TestCase):

    def setUp(self):
        global _test_sockets
        skip_if_no_xattrs()
        self.prolis = prolis = listen_zero()
        self._orig_prolis = _test_sockets[0]
        allowed_headers = ', '.join([
            'content-encoding',
            'x-object-manifest',
            'content-disposition',
            'foo'
        ])
        conf = {'devices': _testdir, 'swift_dir': _testdir,
                'mount_check': 'false', 'allowed_headers': allowed_headers}
        prosrv = listing_formats.ListingFilter(
            copy.ServerSideCopyMiddleware(
                versioned_writes.VersionedWritesMiddleware(
                    proxy_logging.ProxyLoggingMiddleware(
                        _test_servers[0], conf,
                        logger=_test_servers[0].logger), {}),
                {}
            )
        )
        self.coro = spawn(wsgi.server, prolis, prosrv, NullLogger())
        # replace global prosrv with one that's filtered with version
        # middleware
        self.sockets = list(_test_sockets)
        self.sockets[0] = prolis
        _test_sockets = tuple(self.sockets)

    def tearDown(self):
        self.coro.kill()
        # put the global state back
        global _test_sockets
        self.sockets[0] = self._orig_prolis
        _test_sockets = tuple(self.sockets)

    def test_version_manifest(self, oc='versions', vc='vers', o='name'):
        versions_to_create = 3
        # Create a container for our versioned object testing
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis,
         obj2lis, obj3lis, obj4lis, obj5lis, obj6lis) = _test_sockets
        pre = quote('%03x' % len(o))
        osub = '%s/sub' % o
        presub = quote('%03x' % len(osub))
        osub = quote(osub)
        presub = quote(presub)
        oc = quote(oc)
        vc = quote(vc)

        def put_container():
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('PUT /v1/a/%s HTTP/1.1\r\nHost: localhost\r\n'
                     'Connection: close\r\nX-Storage-Token: t\r\n'
                     'Content-Length: 0\r\nX-Versions-Location: %s\r\n\r\n'
                     % (oc, vc))
            fd.flush()
            headers = readuntil2crlfs(fd)
            fd.read()
            return headers

        headers = put_container()
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

        def get_container():
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('GET /v1/a/%s HTTP/1.1\r\nHost: localhost\r\n'
                     'Connection: close\r\n'
                     'X-Storage-Token: t\r\n\r\n\r\n' % oc)
            fd.flush()
            headers = readuntil2crlfs(fd)
            body = fd.read()
            return headers, body

        # check that the header was set
        headers, body = get_container()
        exp = 'HTTP/1.1 2'  # 2xx series response
        self.assertEqual(headers[:len(exp)], exp)
        self.assertIn('X-Versions-Location: %s' % vc, headers)

        def put_version_container():
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('PUT /v1/a/%s HTTP/1.1\r\nHost: localhost\r\n'
                     'Connection: close\r\nX-Storage-Token: t\r\n'
                     'Content-Length: 0\r\n\r\n' % vc)
            fd.flush()
            headers = readuntil2crlfs(fd)
            fd.read()
            return headers

        # make the container for the object versions
        headers = put_version_container()
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

        def put(version):
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('PUT /v1/a/%s/%s HTTP/1.1\r\nHost: '
                     'localhost\r\nConnection: close\r\nX-Storage-Token: '
                     't\r\nContent-Length: 5\r\nContent-Type: text/jibberish%s'
                     '\r\n\r\n%05d\r\n' % (oc, o, version, version))
            fd.flush()
            headers = readuntil2crlfs(fd)
            fd.read()
            return headers

        def get(container=oc, obj=o):
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('GET /v1/a/%s/%s HTTP/1.1\r\nHost: '
                     'localhost\r\nConnection: close\r\nX-Auth-Token: t\r\n'
                     '\r\n' % (container, obj))
            fd.flush()
            headers = readuntil2crlfs(fd)
            body = fd.read()
            return headers, body

        # Create the versioned file
        headers = put(0)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

        # Create the object versions
        for version in range(1, versions_to_create):
            sleep(.01)  # guarantee that the timestamp changes
            headers = put(version)
            exp = 'HTTP/1.1 201'
            self.assertEqual(headers[:len(exp)], exp)

            # Ensure retrieving the manifest file gets the latest version
            headers, body = get()
            exp = 'HTTP/1.1 200'
            self.assertEqual(headers[:len(exp)], exp)
            self.assertIn('Content-Type: text/jibberish%s' % version, headers)
            self.assertNotIn('X-Object-Meta-Foo: barbaz', headers)
            self.assertEqual(body, '%05d' % version)

        def get_version_container():
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('GET /v1/a/%s HTTP/1.1\r\nHost: localhost\r\n'
                     'Connection: close\r\n'
                     'X-Storage-Token: t\r\n\r\n' % vc)
            fd.flush()
            headers = readuntil2crlfs(fd)
            body = fd.read()
            return headers, body

        # Ensure we have the right number of versions saved
        headers, body = get_version_container()
        exp = 'HTTP/1.1 200'
        self.assertEqual(headers[:len(exp)], exp)
        versions = [x for x in body.split('\n') if x]
        self.assertEqual(len(versions), versions_to_create - 1)

        def delete():
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('DELETE /v1/a/%s/%s HTTP/1.1\r\nHost: localhost\r'
                     '\nConnection: close\r\nX-Storage-Token: t\r\n\r\n'
                     % (oc, o))
            fd.flush()
            headers = readuntil2crlfs(fd)
            fd.read()
            return headers

        def copy():
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('COPY /v1/a/%s/%s HTTP/1.1\r\nHost: '
                     'localhost\r\nConnection: close\r\nX-Auth-Token: '
                     't\r\nDestination: %s/copied_name\r\n'
                     'Content-Length: 0\r\n\r\n' % (oc, o, oc))
            fd.flush()
            headers = readuntil2crlfs(fd)
            fd.read()
            return headers

        # copy a version and make sure the version info is stripped
        headers = copy()
        exp = 'HTTP/1.1 2'  # 2xx series response to the COPY
        self.assertEqual(headers[:len(exp)], exp)

        def get_copy():
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('GET /v1/a/%s/copied_name HTTP/1.1\r\nHost: '
                     'localhost\r\nConnection: close\r\n'
                     'X-Auth-Token: t\r\n\r\n' % oc)
            fd.flush()
            headers = readuntil2crlfs(fd)
            body = fd.read()
            return headers, body

        headers, body = get_copy()
        exp = 'HTTP/1.1 200'
        self.assertEqual(headers[:len(exp)], exp)
        self.assertEqual(body, '%05d' % version)

        def post():
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('POST /v1/a/%s/%s HTTP/1.1\r\nHost: '
                     'localhost\r\nConnection: close\r\nX-Auth-Token: '
                     't\r\nContent-Type: foo/bar\r\nContent-Length: 0\r\n'
                     'X-Object-Meta-Bar: foo\r\n\r\n' % (oc, o))
            fd.flush()
            headers = readuntil2crlfs(fd)
            fd.read()
            return headers

        # post and make sure it's updated
        headers = post()
        exp = 'HTTP/1.1 2'  # 2xx series response to the POST
        self.assertEqual(headers[:len(exp)], exp)

        headers, body = get()
        self.assertIn('Content-Type: foo/bar', headers)
        self.assertIn('X-Object-Meta-Bar: foo', headers)
        self.assertEqual(body, '%05d' % version)

        # check container listing
        headers, body = get_container()
        exp = 'HTTP/1.1 200'
        self.assertEqual(headers[:len(exp)], exp)

        # Delete the object versions
        for segment in range(versions_to_create - 1, 0, -1):

            headers = delete()
            exp = 'HTTP/1.1 2'  # 2xx series response
            self.assertEqual(headers[:len(exp)], exp)

            # Ensure retrieving the manifest file gets the latest version
            headers, body = get()
            exp = 'HTTP/1.1 200'
            self.assertEqual(headers[:len(exp)], exp)
            self.assertIn('Content-Type: text/jibberish%s' % (segment - 1),
                          headers)
            self.assertEqual(body, '%05d' % (segment - 1))
            # Ensure we have the right number of versions saved
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('GET /v1/a/%s?prefix=%s%s/ HTTP/1.1\r\nHost: '
                     'localhost\r\nConnection: close\r\nX-Auth-Token: t\r\n\r'
                     '\n' % (vc, pre, o))
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = 'HTTP/1.1 2'  # 2xx series response
            self.assertEqual(headers[:len(exp)], exp)
            body = fd.read()
            versions = [x for x in body.split('\n') if x]
            self.assertEqual(len(versions), segment - 1)

        # there is now one version left (in the manifest)
        # Ensure we have no saved versions
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/%s?prefix=%s%s/ HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Auth-Token: t\r\n\r\n'
                 % (vc, pre, o))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 204 No Content'
        self.assertEqual(headers[:len(exp)], exp)

        # delete the last version
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('DELETE /v1/a/%s/%s HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n\r\n' % (oc, o))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 2'  # 2xx series response
        self.assertEqual(headers[:len(exp)], exp)

        # Ensure it's all gone
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/%s/%s HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Auth-Token: t\r\n\r\n'
                 % (oc, o))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 404'
        self.assertEqual(headers[:len(exp)], exp)

        # make sure manifest files are also versioned
        for _junk in range(0, versions_to_create):
            sleep(.01)  # guarantee that the timestamp changes
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('PUT /v1/a/%s/%s HTTP/1.1\r\nHost: '
                     'localhost\r\nConnection: close\r\nX-Storage-Token: '
                     't\r\nContent-Length: 0\r\n'
                     'Content-Type: text/jibberish0\r\n'
                     'Foo: barbaz\r\nX-Object-Manifest: %s/%s/\r\n\r\n'
                     % (oc, o, oc, o))
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = 'HTTP/1.1 201'
            self.assertEqual(headers[:len(exp)], exp)

        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/%s?prefix=%s%s/ HTTP/1.1\r\nhost: '
                 'localhost\r\nconnection: close\r\nx-auth-token: t\r\n\r\n'
                 % (vc, pre, o))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200 OK'
        self.assertEqual(headers[:len(exp)], exp)
        body = fd.read()
        versions = [x for x in body.split('\n') if x]
        self.assertEqual(versions_to_create - 1, len(versions))

        # DELETE v1/a/c/obj shouldn't delete v1/a/c/obj/sub versions
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%s/%s HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Storage-Token: '
                 't\r\nContent-Length: 5\r\nContent-Type: text/jibberish0\r\n'
                 'Foo: barbaz\r\n\r\n00000\r\n' % (oc, o))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%s/%s HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Storage-Token: '
                 't\r\nContent-Length: 5\r\nContent-Type: text/jibberish0\r\n'
                 'Foo: barbaz\r\n\r\n00001\r\n' % (oc, o))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%s/%s HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Storage-Token: '
                 't\r\nContent-Length: 4\r\nContent-Type: text/jibberish0\r\n'
                 'Foo: barbaz\r\n\r\nsub1\r\n' % (oc, osub))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%s/%s HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Storage-Token: '
                 't\r\nContent-Length: 4\r\nContent-Type: text/jibberish0\r\n'
                 'Foo: barbaz\r\n\r\nsub2\r\n' % (oc, osub))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('DELETE /v1/a/%s/%s HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n\r\n' % (oc, o))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 2'  # 2xx series response
        self.assertEqual(headers[:len(exp)], exp)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/%s?prefix=%s%s/ HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Auth-Token: t\r\n\r\n'
                 % (vc, presub, osub))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 2'  # 2xx series response
        self.assertEqual(headers[:len(exp)], exp)
        body = fd.read()
        versions = [x for x in body.split('\n') if x]
        self.assertEqual(len(versions), 1)

        # Check for when the versions target container doesn't exist
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%swhoops HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n'
                 'Content-Length: 0\r\nX-Versions-Location: none\r\n\r\n' % oc)
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)
        # Create the versioned file
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%swhoops/foo HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Storage-Token: '
                 't\r\nContent-Length: 5\r\n\r\n00000\r\n' % oc)
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)
        # Create another version
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%swhoops/foo HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Storage-Token: '
                 't\r\nContent-Length: 5\r\n\r\n00001\r\n' % oc)
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 412'
        self.assertEqual(headers[:len(exp)], exp)
        # Delete the object
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('DELETE /v1/a/%swhoops/foo HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n\r\n' % oc)
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 2'  # 2xx response
        self.assertEqual(headers[:len(exp)], exp)

    def test_version_manifest_utf8(self):
        oc = '0_oc_non_ascii\xc2\xa3'
        vc = '0_vc_non_ascii\xc2\xa3'
        o = '0_o_non_ascii\xc2\xa3'
        self.test_version_manifest(oc, vc, o)

    def test_version_manifest_utf8_container(self):
        oc = '1_oc_non_ascii\xc2\xa3'
        vc = '1_vc_ascii'
        o = '1_o_ascii'
        self.test_version_manifest(oc, vc, o)

    def test_version_manifest_utf8_version_container(self):
        oc = '2_oc_ascii'
        vc = '2_vc_non_ascii\xc2\xa3'
        o = '2_o_ascii'
        self.test_version_manifest(oc, vc, o)

    def test_version_manifest_utf8_containers(self):
        oc = '3_oc_non_ascii\xc2\xa3'
        vc = '3_vc_non_ascii\xc2\xa3'
        o = '3_o_ascii'
        self.test_version_manifest(oc, vc, o)

    def test_version_manifest_utf8_object(self):
        oc = '4_oc_ascii'
        vc = '4_vc_ascii'
        o = '4_o_non_ascii\xc2\xa3'
        self.test_version_manifest(oc, vc, o)

    def test_version_manifest_utf8_version_container_utf_object(self):
        oc = '5_oc_ascii'
        vc = '5_vc_non_ascii\xc2\xa3'
        o = '5_o_non_ascii\xc2\xa3'
        self.test_version_manifest(oc, vc, o)

    def test_version_manifest_utf8_container_utf_object(self):
        oc = '6_oc_non_ascii\xc2\xa3'
        vc = '6_vc_ascii'
        o = '6_o_non_ascii\xc2\xa3'
        self.test_version_manifest(oc, vc, o)


if __name__ == '__main__':
    unittest.main()
