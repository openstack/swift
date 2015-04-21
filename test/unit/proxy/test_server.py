# -*- coding: utf-8 -*-
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

import logging
import math
import os
import pickle
import sys
import unittest
from contextlib import closing, contextmanager, nested
from gzip import GzipFile
from shutil import rmtree
from StringIO import StringIO
import gc
import time
from textwrap import dedent
from urllib import quote
from hashlib import md5
from pyeclib.ec_iface import ECDriverError
from tempfile import mkdtemp, NamedTemporaryFile
import weakref
import operator
import functools
from swift.obj import diskfile
import re
import random

import mock
from eventlet import sleep, spawn, wsgi, listen, Timeout
from swift.common.utils import hash_path, json, storage_directory, public

from test.unit import (
    connect_tcp, readuntil2crlfs, FakeLogger, fake_http_connect, FakeRing,
    FakeMemcache, debug_logger, patch_policies, write_fake_ring,
    mocked_http_conn)
from swift.proxy import server as proxy_server
from swift.proxy.controllers.obj import ReplicatedObjectController
from swift.account import server as account_server
from swift.container import server as container_server
from swift.obj import server as object_server
from swift.common.middleware import proxy_logging
from swift.common.middleware.acl import parse_acl, format_acl
from swift.common.exceptions import ChunkReadTimeout, DiskFileNotExist, \
    APIVersionError
from swift.common import utils, constraints
from swift.common.ring import RingData
from swift.common.utils import mkdirs, normalize_timestamp, NullLogger
from swift.common.wsgi import monkey_patch_mimetools, loadapp
from swift.proxy.controllers import base as proxy_base
from swift.proxy.controllers.base import get_container_memcache_key, \
    get_account_memcache_key, cors_validation
import swift.proxy.controllers
import swift.proxy.controllers.obj
from swift.common.swob import Request, Response, HTTPUnauthorized, \
    HTTPException, HTTPForbidden, HeaderKeyDict
from swift.common import storage_policy
from swift.common.storage_policy import StoragePolicy, ECStoragePolicy, \
    StoragePolicyCollection, POLICIES
from swift.common.request_helpers import get_sys_meta_prefix

# mocks
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


STATIC_TIME = time.time()
_test_coros = _test_servers = _test_sockets = _orig_container_listing_limit = \
    _testdir = _orig_SysLogHandler = _orig_POLICIES = _test_POLICIES = None


def do_setup(the_object_server):
    utils.HASH_PATH_SUFFIX = 'endcap'
    global _testdir, _test_servers, _test_sockets, \
        _orig_container_listing_limit, _test_coros, _orig_SysLogHandler, \
        _orig_POLICIES, _test_POLICIES
    _orig_POLICIES = storage_policy._POLICIES
    _orig_SysLogHandler = utils.SysLogHandler
    utils.SysLogHandler = mock.MagicMock()
    monkey_patch_mimetools()
    # Since we're starting up a lot here, we're going to test more than
    # just chunked puts; we're also going to test parts of
    # proxy_server.Application we couldn't get to easily otherwise.
    _testdir = \
        os.path.join(mkdtemp(), 'tmp_test_proxy_server_chunked')
    mkdirs(_testdir)
    rmtree(_testdir)
    for drive in ('sda1', 'sdb1', 'sdc1', 'sdd1', 'sde1',
                  'sdf1', 'sdg1', 'sdh1', 'sdi1'):
        mkdirs(os.path.join(_testdir, drive, 'tmp'))
    conf = {'devices': _testdir, 'swift_dir': _testdir,
            'mount_check': 'false', 'allowed_headers':
            'content-encoding, x-object-manifest, content-disposition, foo',
            'allow_versions': 'True'}
    prolis = listen(('localhost', 0))
    acc1lis = listen(('localhost', 0))
    acc2lis = listen(('localhost', 0))
    con1lis = listen(('localhost', 0))
    con2lis = listen(('localhost', 0))
    obj1lis = listen(('localhost', 0))
    obj2lis = listen(('localhost', 0))
    obj3lis = listen(('localhost', 0))
    objsocks = [obj1lis, obj2lis, obj3lis]
    _test_sockets = \
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis, obj2lis, obj3lis)
    account_ring_path = os.path.join(_testdir, 'account.ring.gz')
    account_devs = [
        {'port': acc1lis.getsockname()[1]},
        {'port': acc2lis.getsockname()[1]},
    ]
    write_fake_ring(account_ring_path, *account_devs)
    container_ring_path = os.path.join(_testdir, 'container.ring.gz')
    container_devs = [
        {'port': con1lis.getsockname()[1]},
        {'port': con2lis.getsockname()[1]},
    ]
    write_fake_ring(container_ring_path, *container_devs)
    storage_policy._POLICIES = StoragePolicyCollection([
        StoragePolicy(0, 'zero', True),
        StoragePolicy(1, 'one', False),
        StoragePolicy(2, 'two', False),
        ECStoragePolicy(3, 'ec', ec_type='jerasure_rs_vand',
                        ec_ndata=2, ec_nparity=1, ec_segment_size=4096)])
    obj_rings = {
        0: ('sda1', 'sdb1'),
        1: ('sdc1', 'sdd1'),
        2: ('sde1', 'sdf1'),
        # sdg1, sdh1, sdi1 taken by policy 3 (see below)
    }
    for policy_index, devices in obj_rings.items():
        policy = POLICIES[policy_index]
        obj_ring_path = os.path.join(_testdir, policy.ring_name + '.ring.gz')
        obj_devs = [
            {'port': objsock.getsockname()[1], 'device': dev}
            for objsock, dev in zip(objsocks, devices)]
        write_fake_ring(obj_ring_path, *obj_devs)

    # write_fake_ring can't handle a 3-element ring, and the EC policy needs
    # at least 3 devs to work with, so we do it manually
    devs = [{'id': 0, 'zone': 0, 'device': 'sdg1', 'ip': '127.0.0.1',
             'port': obj1lis.getsockname()[1]},
            {'id': 1, 'zone': 0, 'device': 'sdh1', 'ip': '127.0.0.1',
             'port': obj2lis.getsockname()[1]},
            {'id': 2, 'zone': 0, 'device': 'sdi1', 'ip': '127.0.0.1',
             'port': obj3lis.getsockname()[1]}]
    pol3_replica2part2dev_id = [[0, 1, 2, 0],
                                [1, 2, 0, 1],
                                [2, 0, 1, 2]]
    obj3_ring_path = os.path.join(_testdir, POLICIES[3].ring_name + '.ring.gz')
    part_shift = 30
    with closing(GzipFile(obj3_ring_path, 'wb')) as fh:
        pickle.dump(RingData(pol3_replica2part2dev_id, devs, part_shift), fh)

    prosrv = proxy_server.Application(conf, FakeMemcacheReturnsNone(),
                                      logger=debug_logger('proxy'))
    for policy in POLICIES:
        # make sure all the rings are loaded
        prosrv.get_object_ring(policy.idx)
    # don't lose this one!
    _test_POLICIES = storage_policy._POLICIES
    acc1srv = account_server.AccountController(
        conf, logger=debug_logger('acct1'))
    acc2srv = account_server.AccountController(
        conf, logger=debug_logger('acct2'))
    con1srv = container_server.ContainerController(
        conf, logger=debug_logger('cont1'))
    con2srv = container_server.ContainerController(
        conf, logger=debug_logger('cont2'))
    obj1srv = the_object_server.ObjectController(
        conf, logger=debug_logger('obj1'))
    obj2srv = the_object_server.ObjectController(
        conf, logger=debug_logger('obj2'))
    obj3srv = the_object_server.ObjectController(
        conf, logger=debug_logger('obj3'))
    _test_servers = \
        (prosrv, acc1srv, acc2srv, con1srv, con2srv, obj1srv, obj2srv, obj3srv)
    nl = NullLogger()
    logging_prosv = proxy_logging.ProxyLoggingMiddleware(prosrv, conf,
                                                         logger=prosrv.logger)
    prospa = spawn(wsgi.server, prolis, logging_prosv, nl)
    acc1spa = spawn(wsgi.server, acc1lis, acc1srv, nl)
    acc2spa = spawn(wsgi.server, acc2lis, acc2srv, nl)
    con1spa = spawn(wsgi.server, con1lis, con1srv, nl)
    con2spa = spawn(wsgi.server, con2lis, con2srv, nl)
    obj1spa = spawn(wsgi.server, obj1lis, obj1srv, nl)
    obj2spa = spawn(wsgi.server, obj2lis, obj2srv, nl)
    obj3spa = spawn(wsgi.server, obj3lis, obj3srv, nl)
    _test_coros = \
        (prospa, acc1spa, acc2spa, con1spa, con2spa, obj1spa, obj2spa, obj3spa)
    # Create account
    ts = normalize_timestamp(time.time())
    partition, nodes = prosrv.account_ring.get_nodes('a')
    for node in nodes:
        conn = swift.proxy.controllers.obj.http_connect(node['ip'],
                                                        node['port'],
                                                        node['device'],
                                                        partition, 'PUT', '/a',
                                                        {'X-Timestamp': ts,
                                                         'x-trans-id': 'test'})
        resp = conn.getresponse()
        assert(resp.status == 201)
    # Create another account
    # used for account-to-account tests
    ts = normalize_timestamp(time.time())
    partition, nodes = prosrv.account_ring.get_nodes('a1')
    for node in nodes:
        conn = swift.proxy.controllers.obj.http_connect(node['ip'],
                                                        node['port'],
                                                        node['device'],
                                                        partition, 'PUT',
                                                        '/a1',
                                                        {'X-Timestamp': ts,
                                                         'x-trans-id': 'test'})
        resp = conn.getresponse()
        assert(resp.status == 201)
    # Create containers, 1 per test policy
    sock = connect_tcp(('localhost', prolis.getsockname()[1]))
    fd = sock.makefile()
    fd.write('PUT /v1/a/c HTTP/1.1\r\nHost: localhost\r\n'
             'Connection: close\r\nX-Auth-Token: t\r\n'
             'Content-Length: 0\r\n\r\n')
    fd.flush()
    headers = readuntil2crlfs(fd)
    exp = 'HTTP/1.1 201'
    assert headers[:len(exp)] == exp, "Expected '%s', encountered '%s'" % (
        exp, headers[:len(exp)])
    # Create container in other account
    # used for account-to-account tests
    sock = connect_tcp(('localhost', prolis.getsockname()[1]))
    fd = sock.makefile()
    fd.write('PUT /v1/a1/c1 HTTP/1.1\r\nHost: localhost\r\n'
             'Connection: close\r\nX-Auth-Token: t\r\n'
             'Content-Length: 0\r\n\r\n')
    fd.flush()
    headers = readuntil2crlfs(fd)
    exp = 'HTTP/1.1 201'
    assert headers[:len(exp)] == exp, "Expected '%s', encountered '%s'" % (
        exp, headers[:len(exp)])

    sock = connect_tcp(('localhost', prolis.getsockname()[1]))
    fd = sock.makefile()
    fd.write(
        'PUT /v1/a/c1 HTTP/1.1\r\nHost: localhost\r\n'
        'Connection: close\r\nX-Auth-Token: t\r\nX-Storage-Policy: one\r\n'
        'Content-Length: 0\r\n\r\n')
    fd.flush()
    headers = readuntil2crlfs(fd)
    exp = 'HTTP/1.1 201'
    assert headers[:len(exp)] == exp, \
        "Expected '%s', encountered '%s'" % (exp, headers[:len(exp)])

    sock = connect_tcp(('localhost', prolis.getsockname()[1]))
    fd = sock.makefile()
    fd.write(
        'PUT /v1/a/c2 HTTP/1.1\r\nHost: localhost\r\n'
        'Connection: close\r\nX-Auth-Token: t\r\nX-Storage-Policy: two\r\n'
        'Content-Length: 0\r\n\r\n')
    fd.flush()
    headers = readuntil2crlfs(fd)
    exp = 'HTTP/1.1 201'
    assert headers[:len(exp)] == exp, \
        "Expected '%s', encountered '%s'" % (exp, headers[:len(exp)])


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


def setup():
    do_setup(object_server)


def teardown():
    for server in _test_coros:
        server.kill()
    rmtree(os.path.dirname(_testdir))
    utils.SysLogHandler = _orig_SysLogHandler
    storage_policy._POLICIES = _orig_POLICIES


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
        self.assertEquals(dst_headers, expected_headers)

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
            self.assertEquals(count, 123)
        with save_globals():
            set_http_connect(200, count='123')
            partition, nodes, count = \
                self.controller.account_info(self.account)
            self.assertEquals(count, 123)
        with save_globals():
            cache_key = get_account_memcache_key(self.account)
            account_info = {'status': 200, 'container_count': 1234}
            self.memcache.set(cache_key, account_info)
            partition, nodes, count = \
                self.controller.account_info(self.account)
            self.assertEquals(count, 1234)
        with save_globals():
            cache_key = get_account_memcache_key(self.account)
            account_info = {'status': 200, 'container_count': '1234'}
            self.memcache.set(cache_key, account_info)
            partition, nodes, count = \
                self.controller.account_info(self.account)
            self.assertEquals(count, 1234)

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
            self.assertEquals(count, 12345)

            # Test the internal representation in memcache
            # 'container_count' changed from int to str
            cache_key = get_account_memcache_key(self.account)
            container_info = {'status': 200,
                              'container_count': '12345',
                              'total_object_count': None,
                              'bytes': None,
                              'meta': {},
                              'sysmeta': {}}
            self.assertEquals(container_info,
                              self.memcache.get(cache_key))

            set_http_connect()
            partition, nodes, count = \
                self.controller.account_info(self.account, self.request)
            self.check_account_info_return(partition, nodes)
            self.assertEquals(count, 12345)

    # tests if 404 is cached and used
    def test_account_info_404(self):
        with save_globals():
            set_http_connect(404, 404, 404)
            partition, nodes, count = \
                self.controller.account_info(self.account, self.request)
            self.check_account_info_return(partition, nodes, True)
            self.assertEquals(count, None)

            # Test the internal representation in memcache
            # 'container_count' changed from 0 to None
            cache_key = get_account_memcache_key(self.account)
            account_info = {'status': 404,
                            'container_count': None,  # internally keep None
                            'total_object_count': None,
                            'bytes': None,
                            'meta': {},
                            'sysmeta': {}}
            self.assertEquals(account_info,
                              self.memcache.get(cache_key))

            set_http_connect()
            partition, nodes, count = \
                self.controller.account_info(self.account, self.request)
            self.check_account_info_return(partition, nodes, True)
            self.assertEquals(count, None)

    # tests if some http status codes are not cached
    def test_account_info_no_cache(self):
        def test(*status_list):
            set_http_connect(*status_list)
            partition, nodes, count = \
                self.controller.account_info(self.account, self.request)
            self.assertEqual(len(self.memcache.keys()), 0)
            self.check_account_info_return(partition, nodes, True)
            self.assertEquals(count, None)

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
            self.assertEquals(count, None)

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

            cache_key = get_container_memcache_key(self.account,
                                                   self.container)
            cache_value = self.memcache.get(cache_key)
            self.assertTrue(isinstance(cache_value, dict))
            self.assertEquals(200, cache_value.get('status'))

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

            cache_key = get_container_memcache_key(self.account,
                                                   self.container)
            cache_value = self.memcache.get(cache_key)
            self.assertTrue(isinstance(cache_value, dict))
            self.assertEquals(404, cache_value.get('status'))

            set_http_connect()
            ret = self.controller.container_info(
                self.account, self.container, self.request)
            self.check_container_info_return(ret, True)

            set_http_connect(503, 404, 404)  # account_info 'NotFound'
            ret = self.controller.container_info(
                self.account, self.container, self.request)
            self.check_container_info_return(ret, True)

            cache_key = get_container_memcache_key(self.account,
                                                   self.container)
            cache_value = self.memcache.get(cache_key)
            self.assertTrue(isinstance(cache_value, dict))
            self.assertEquals(404, cache_value.get('status'))

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
        self.assertEquals(resp.status_int, 500)

    def test_internal_method_request(self):
        baseapp = proxy_server.Application({},
                                           FakeMemcache(),
                                           container_ring=FakeRing(),
                                           account_ring=FakeRing())
        resp = baseapp.handle_request(
            Request.blank('/v1/a', environ={'REQUEST_METHOD': '__init__'}))
        self.assertEquals(resp.status, '405 Method Not Allowed')

    def test_inexistent_method_request(self):
        baseapp = proxy_server.Application({},
                                           FakeMemcache(),
                                           container_ring=FakeRing(),
                                           account_ring=FakeRing())
        resp = baseapp.handle_request(
            Request.blank('/v1/a', environ={'REQUEST_METHOD': '!invalid'}))
        self.assertEquals(resp.status, '405 Method Not Allowed')

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
        self.assert_(called[0])

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
        self.assert_(called[0])

    def test_negative_content_length(self):
        swift_dir = mkdtemp()
        try:
            baseapp = proxy_server.Application({'swift_dir': swift_dir},
                                               FakeMemcache(), FakeLogger(),
                                               FakeRing(), FakeRing())
            resp = baseapp.handle_request(
                Request.blank('/', environ={'CONTENT_LENGTH': '-1'}))
            self.assertEquals(resp.status, '400 Bad Request')
            self.assertEquals(resp.body, 'Invalid Content-Length')
            resp = baseapp.handle_request(
                Request.blank('/', environ={'CONTENT_LENGTH': '-123'}))
            self.assertEquals(resp.status, '400 Bad Request')
            self.assertEquals(resp.body, 'Invalid Content-Length')
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
            self.assertEquals(resp.status, '403 Forbidden')
        finally:
            rmtree(swift_dir, ignore_errors=True)

    def test_node_timing(self):
        baseapp = proxy_server.Application({'sorting_method': 'timing'},
                                           FakeMemcache(),
                                           container_ring=FakeRing(),
                                           account_ring=FakeRing())
        self.assertEquals(baseapp.node_timings, {})

        req = Request.blank('/v1/account', environ={'REQUEST_METHOD': 'HEAD'})
        baseapp.update_request(req)
        resp = baseapp.handle_request(req)
        self.assertEquals(resp.status_int, 503)  # couldn't connect to anything
        exp_timings = {}
        self.assertEquals(baseapp.node_timings, exp_timings)

        times = [time.time()]
        exp_timings = {'127.0.0.1': (0.1, times[0] + baseapp.timing_expiry)}
        with mock.patch('swift.proxy.server.time', lambda: times.pop(0)):
            baseapp.set_node_timing({'ip': '127.0.0.1'}, 0.1)
        self.assertEquals(baseapp.node_timings, exp_timings)

        nodes = [{'ip': '127.0.0.1'}, {'ip': '127.0.0.2'}, {'ip': '127.0.0.3'}]
        with mock.patch('swift.proxy.server.shuffle', lambda l: l):
            res = baseapp.sort_nodes(nodes)
        exp_sorting = [{'ip': '127.0.0.2'}, {'ip': '127.0.0.3'},
                       {'ip': '127.0.0.1'}]
        self.assertEquals(res, exp_sorting)

    def test_node_affinity(self):
        baseapp = proxy_server.Application({'sorting_method': 'affinity',
                                            'read_affinity': 'r1=1'},
                                           FakeMemcache(),
                                           container_ring=FakeRing(),
                                           account_ring=FakeRing())

        nodes = [{'region': 2, 'zone': 1, 'ip': '127.0.0.1'},
                 {'region': 1, 'zone': 2, 'ip': '127.0.0.2'}]
        with mock.patch('swift.proxy.server.shuffle', lambda x: x):
            app_sorted = baseapp.sort_nodes(nodes)
            exp_sorted = [{'region': 1, 'zone': 2, 'ip': '127.0.0.2'},
                          {'region': 2, 'zone': 1, 'ip': '127.0.0.1'}]
            self.assertEquals(exp_sorted, app_sorted)

    def test_info_defaults(self):
        app = proxy_server.Application({}, FakeMemcache(),
                                       account_ring=FakeRing(),
                                       container_ring=FakeRing())

        self.assertTrue(app.expose_info)
        self.assertTrue(isinstance(app.disallowed_sections, list))
        self.assertEqual(1, len(app.disallowed_sections))
        self.assertEqual(['swift.valid_api_versions'],
                         app.disallowed_sections)
        self.assertTrue(app.admin_key is None)

    def test_get_info_controller(self):
        req = Request.blank('/info')
        app = proxy_server.Application({}, FakeMemcache(),
                                       account_ring=FakeRing(),
                                       container_ring=FakeRing())

        controller, path_parts = app.get_controller(req)

        self.assertTrue('version' in path_parts)
        self.assertTrue(path_parts['version'] is None)
        self.assertTrue('disallowed_sections' in path_parts)
        self.assertTrue('expose_info' in path_parts)
        self.assertTrue('admin_key' in path_parts)

        self.assertEqual(controller.__name__, 'InfoController')

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
        self.assertTrue('test1 server' in line)
        self.assertTrue('test1 msg' in line)
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
        self.assertTrue('test2 server' in line)
        self.assertTrue('test2 msg' in line)
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
        self.assertTrue('test3 server' in line)
        self.assertTrue('test3 msg' in line)
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
            self.assertTrue(controller is not None)

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
                    self.assertTrue(controller is not None)

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
        utils.HASH_PATH_SUFFIX = 'endcap'
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
            self.assert_(policy.object_ring)


@patch_policies([StoragePolicy(0, 'zero', True,
                               object_ring=FakeRing(base_port=3000))])
class TestObjectController(unittest.TestCase):

    def setUp(self):
        self.app = proxy_server.Application(
            None, FakeMemcache(),
            logger=debug_logger('proxy-ut'),
            account_ring=FakeRing(),
            container_ring=FakeRing())

    def tearDown(self):
        self.app.account_ring.set_replicas(3)
        self.app.container_ring.set_replicas(3)
        for policy in POLICIES:
            policy.object_ring = FakeRing(base_port=3000)

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
            self.assertEquals(res.status_int, expected)

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
            self.assertEquals(res.status_int, expected)

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
                     'wsgi.input': StringIO("hello")},
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
            self.assert_(float(e.timestamp) > 0)
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
    def test_PUT_ec(self):
        policy = POLICIES[3]
        self.put_container("ec", "ec-con")

        obj = 'abCD' * 10  # small, so we don't get multiple EC stripes
        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/ec-con/o1 HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'Etag: "%s"\r\n'
                 'Content-Length: %d\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 '\r\n%s' % (md5(obj).hexdigest(), len(obj), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

        ecd = policy.pyeclib_driver
        expected_pieces = set(ecd.encode(obj))

        # go to disk to make sure it's there and all erasure-coded
        partition, nodes = policy.object_ring.get_nodes('a', 'ec-con', 'o1')
        conf = {'devices': _testdir, 'mount_check': 'false'}
        df_mgr = diskfile.DiskFileManager(conf, FakeLogger())

        got_pieces = set()
        got_indices = set()
        got_durable = []
        for node_index, node in enumerate(nodes):
            df = df_mgr.get_diskfile(node['device'], partition,
                                     'a', 'ec-con', 'o1',
                                     policy=policy)
            with df.open():
                meta = df.get_metadata()
                contents = ''.join(df.reader())
                got_pieces.add(contents)

                # check presence for a .durable file for the timestamp
                durable_file = os.path.join(
                    _testdir, node['device'], storage_directory(
                        diskfile.get_data_dir(policy),
                        partition, hash_path('a', 'ec-con', 'o1')),
                    utils.Timestamp(df.timestamp).internal + '.durable')

                if os.path.isfile(durable_file):
                    got_durable.append(True)

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
                    'jerasure_rs_vand 2+1')
                self.assertEqual(
                    lmeta['etag'],
                    md5(contents).hexdigest())

        self.assertEqual(expected_pieces, got_pieces)
        self.assertEqual(set(('0', '1', '2')), got_indices)

        # verify at least 2 puts made it all the way to the end of 2nd
        # phase, ie at least 2 .durable statuses were written
        num_durable_puts = sum(d is True for d in got_durable)
        self.assertTrue(num_durable_puts >= 2)

    @unpatch_policies
    def test_PUT_ec_multiple_segments(self):
        ec_policy = POLICIES[3]
        self.put_container("ec", "ec-con")

        pyeclib_header_size = len(ec_policy.pyeclib_driver.encode("")[0])
        segment_size = ec_policy.ec_segment_size

        # Big enough to have multiple segments. Also a multiple of the
        # segment size to get coverage of that path too.
        obj = 'ABC' * segment_size

        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/ec-con/o2 HTTP/1.1\r\n'
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

        # it's a 2+1 erasure code, so each fragment archive should be half
        # the length of the object, plus three inline pyeclib metadata
        # things (one per segment)
        expected_length = (len(obj) / 2 + pyeclib_header_size * 3)

        partition, nodes = ec_policy.object_ring.get_nodes(
            'a', 'ec-con', 'o2')

        conf = {'devices': _testdir, 'mount_check': 'false'}
        df_mgr = diskfile.DiskFileManager(conf, FakeLogger())

        got_durable = []
        fragment_archives = []
        for node in nodes:
            df = df_mgr.get_diskfile(
                node['device'], partition, 'a',
                'ec-con', 'o2', policy=ec_policy)
            with df.open():
                contents = ''.join(df.reader())
                fragment_archives.append(contents)
                self.assertEqual(len(contents), expected_length)

                # check presence for a .durable file for the timestamp
                durable_file = os.path.join(
                    _testdir, node['device'], storage_directory(
                        diskfile.get_data_dir(ec_policy),
                        partition, hash_path('a', 'ec-con', 'o2')),
                    utils.Timestamp(df.timestamp).internal + '.durable')

                if os.path.isfile(durable_file):
                    got_durable.append(True)

        # Verify that we can decode each individual fragment and that they
        # are all the correct size
        fragment_size = ec_policy.fragment_size
        nfragments = int(
            math.ceil(float(len(fragment_archives[0])) / fragment_size))

        for fragment_index in range(nfragments):
            fragment_start = fragment_index * fragment_size
            fragment_end = (fragment_index + 1) * fragment_size

            try:
                frags = [fa[fragment_start:fragment_end]
                         for fa in fragment_archives]
                seg = ec_policy.pyeclib_driver.decode(frags)
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
        self.assertTrue(num_durable_puts >= 2)

    @unpatch_policies
    def test_PUT_ec_object_etag_mismatch(self):
        self.put_container("ec", "ec-con")

        obj = '90:6A:02:60:B1:08-96da3e706025537fc42464916427727e'
        prolis = _test_sockets[0]
        prosrv = _test_servers[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/ec-con/o3 HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'Etag: %s\r\n'
                 'Content-Length: %d\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 '\r\n%s' % (md5('something else').hexdigest(), len(obj), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 422'
        self.assertEqual(headers[:len(exp)], exp)

        # nothing should have made it to disk on the object servers
        partition, nodes = prosrv.get_object_ring(3).get_nodes(
            'a', 'ec-con', 'o3')
        conf = {'devices': _testdir, 'mount_check': 'false'}

        partition, nodes = prosrv.get_object_ring(3).get_nodes(
            'a', 'ec-con', 'o3')
        conf = {'devices': _testdir, 'mount_check': 'false'}
        df_mgr = diskfile.DiskFileManager(conf, FakeLogger())

        for node in nodes:
            df = df_mgr.get_diskfile(node['device'], partition,
                                     'a', 'ec-con', 'o3', policy=POLICIES[3])
            self.assertRaises(DiskFileNotExist, df.open)

    @unpatch_policies
    def test_PUT_ec_fragment_archive_etag_mismatch(self):
        self.put_container("ec", "ec-con")

        # Cause a hash mismatch by feeding one particular MD5 hasher some
        # extra data. The goal here is to get exactly one of the hashers in
        # an object server.
        countdown = [1]

        def busted_md5_constructor(initial_str=""):
            hasher = md5(initial_str)
            if countdown[0] == 0:
                hasher.update('wrong')
            countdown[0] -= 1
            return hasher

        obj = 'uvarovite-esurience-cerated-symphysic'
        prolis = _test_sockets[0]
        prosrv = _test_servers[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        with mock.patch('swift.obj.server.md5', busted_md5_constructor):
            fd = sock.makefile()
            fd.write('PUT /v1/a/ec-con/pimento HTTP/1.1\r\n'
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

        # 2/3 of the fragment archives should have landed on disk
        partition, nodes = prosrv.get_object_ring(3).get_nodes(
            'a', 'ec-con', 'pimento')
        conf = {'devices': _testdir, 'mount_check': 'false'}

        partition, nodes = prosrv.get_object_ring(3).get_nodes(
            'a', 'ec-con', 'pimento')
        conf = {'devices': _testdir, 'mount_check': 'false'}

        df_mgr = diskfile.DiskFileManager(conf, FakeLogger())

        found = 0
        for node in nodes:
            df = df_mgr.get_diskfile(node['device'], partition,
                                     'a', 'ec-con', 'pimento',
                                     policy=POLICIES[3])
            try:
                df.open()
                found += 1
            except DiskFileNotExist:
                pass
        self.assertEqual(found, 2)

    @unpatch_policies
    def test_PUT_ec_if_none_match(self):
        self.put_container("ec", "ec-con")

        obj = 'ananepionic-lepidophyllous-ropewalker-neglectful'
        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/ec-con/inm HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'Etag: "%s"\r\n'
                 'Content-Length: %d\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 '\r\n%s' % (md5(obj).hexdigest(), len(obj), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/ec-con/inm HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'If-None-Match: *\r\n'
                 'Etag: "%s"\r\n'
                 'Content-Length: %d\r\n'
                 'X-Storage-Token: t\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 '\r\n%s' % (md5(obj).hexdigest(), len(obj), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 412'
        self.assertEqual(headers[:len(exp)], exp)

    @unpatch_policies
    def test_GET_ec(self):
        self.put_container("ec", "ec-con")

        obj = '0123456' * 11 * 17

        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/ec-con/go-get-it HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'Content-Length: %d\r\n'
                 'X-Storage-Token: t\r\n'
                 'X-Object-Meta-Color: chartreuse\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 '\r\n%s' % (len(obj), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/ec-con/go-get-it HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 '\r\n')
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

    @unpatch_policies
    def test_conditional_GET_ec(self):
        self.put_container("ec", "ec-con")

        obj = 'this object has an etag and is otherwise unimportant'
        etag = md5(obj).hexdigest()
        not_etag = md5(obj + "blahblah").hexdigest()

        prolis = _test_sockets[0]
        prosrv = _test_servers[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/ec-con/conditionals HTTP/1.1\r\n'
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

        for verb in ('GET', 'HEAD'):
            # If-Match
            req = Request.blank(
                '/v1/a/ec-con/conditionals',
                environ={'REQUEST_METHOD': verb},
                headers={'If-Match': etag})
            resp = req.get_response(prosrv)
            self.assertEqual(resp.status_int, 200)

            req = Request.blank(
                '/v1/a/ec-con/conditionals',
                environ={'REQUEST_METHOD': verb},
                headers={'If-Match': not_etag})
            resp = req.get_response(prosrv)
            self.assertEqual(resp.status_int, 412)

            req = Request.blank(
                '/v1/a/ec-con/conditionals',
                environ={'REQUEST_METHOD': verb},
                headers={'If-Match': "*"})
            resp = req.get_response(prosrv)
            self.assertEqual(resp.status_int, 200)

            # If-None-Match
            req = Request.blank(
                '/v1/a/ec-con/conditionals',
                environ={'REQUEST_METHOD': verb},
                headers={'If-None-Match': etag})
            resp = req.get_response(prosrv)
            self.assertEqual(resp.status_int, 304)

            req = Request.blank(
                '/v1/a/ec-con/conditionals',
                environ={'REQUEST_METHOD': verb},
                headers={'If-None-Match': not_etag})
            resp = req.get_response(prosrv)
            self.assertEqual(resp.status_int, 200)

            req = Request.blank(
                '/v1/a/ec-con/conditionals',
                environ={'REQUEST_METHOD': verb},
                headers={'If-None-Match': "*"})
            resp = req.get_response(prosrv)
            self.assertEqual(resp.status_int, 304)

    @unpatch_policies
    def test_GET_ec_big(self):
        self.put_container("ec", "ec-con")

        # our EC segment size is 4 KiB, so this is multiple (3) segments;
        # we'll verify that with a sanity check
        obj = 'a moose once bit my sister' * 400
        self.assertTrue(
            len(obj) > POLICIES.get_by_name("ec").ec_segment_size * 2,
            "object is too small for proper testing")

        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/ec-con/big-obj-get HTTP/1.1\r\n'
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

        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/ec-con/big-obj-get HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 '\r\n')
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

    @unpatch_policies
    def test_GET_ec_failure_handling(self):
        self.put_container("ec", "ec-con")

        obj = 'look at this object; it is simply amazing ' * 500
        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/ec-con/crash-test-dummy HTTP/1.1\r\n'
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

        def explodey_iter(inner_iter):
            yield next(inner_iter)
            raise Exception("doom ba doom")

        real_ec_app_iter = swift.proxy.controllers.obj.ECAppIter

        def explodey_ec_app_iter(path, policy, iterators, *a, **kw):
            # Each thing in `iterators` here is a document-parts iterator,
            # and we want to fail after getting a little into each part.
            #
            # That way, we ensure we've started streaming the response to
            # the client when things go wrong.
            return real_ec_app_iter(
                path, policy,
                [explodey_iter(i) for i in iterators],
                *a, **kw)

        with mock.patch("swift.proxy.controllers.obj.ECAppIter",
                        explodey_ec_app_iter):
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('GET /v1/a/ec-con/crash-test-dummy HTTP/1.1\r\n'
                     'Host: localhost\r\n'
                     'Connection: close\r\n'
                     'X-Storage-Token: t\r\n'
                     '\r\n')
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = 'HTTP/1.1 200'
            self.assertEqual(headers[:len(exp)], exp)

            headers = parse_headers_string(headers)
            self.assertEqual(str(len(obj)), headers['Content-Length'])
            self.assertEqual(md5(obj).hexdigest(), headers['Etag'])

            gotten_obj = ''
            try:
                with Timeout(300):  # don't hang the testrun when this fails
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

    @unpatch_policies
    def test_HEAD_ec(self):
        self.put_container("ec", "ec-con")

        obj = '0123456' * 11 * 17

        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/ec-con/go-head-it HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'Content-Length: %d\r\n'
                 'X-Storage-Token: t\r\n'
                 'X-Object-Meta-Color: chartreuse\r\n'
                 'Content-Type: application/octet-stream\r\n'
                 '\r\n%s' % (len(obj), obj))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEqual(headers[:len(exp)], exp)

        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('HEAD /v1/a/ec-con/go-head-it HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 '\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEqual(headers[:len(exp)], exp)

        headers = parse_headers_string(headers)
        self.assertEqual(str(len(obj)), headers['Content-Length'])
        self.assertEqual(md5(obj).hexdigest(), headers['Etag'])
        self.assertEqual('chartreuse', headers['X-Object-Meta-Color'])

    @unpatch_policies
    def test_GET_ec_404(self):
        self.put_container("ec", "ec-con")

        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/ec-con/yes-we-have-no-bananas HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 '\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 404'
        self.assertEqual(headers[:len(exp)], exp)

    @unpatch_policies
    def test_HEAD_ec_404(self):
        self.put_container("ec", "ec-con")

        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('HEAD /v1/a/ec-con/yes-we-have-no-bananas HTTP/1.1\r\n'
                 'Host: localhost\r\n'
                 'Connection: close\r\n'
                 'X-Storage-Token: t\r\n'
                 '\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 404'
        self.assertEqual(headers[:len(exp)], exp)

    def test_PUT_expect_header_zero_content_length(self):
        test_errors = []

        def test_connect(ipaddr, port, device, partition, method, path,
                         headers=None, query_string=None):
            if path == '/a/c/o.jpg':
                if 'expect' in headers or 'Expect' in headers:
                    test_errors.append('Expect was in headers for object '
                                       'server!')

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

    def test_PUT_respects_write_affinity(self):
        written_to = []

        def test_connect(ipaddr, port, device, partition, method, path,
                         headers=None, query_string=None):
            if path == '/a/c/o.jpg':
                written_to.append((ipaddr, port, device))

        with save_globals():
            def is_r0(node):
                return node['region'] == 0

            object_ring = self.app.get_object_ring(None)
            object_ring.max_more_nodes = 100
            self.app.write_affinity_is_local_fn = is_r0
            self.app.write_affinity_node_count = lambda r: 3

            controller = \
                ReplicatedObjectController(
                    self.app, 'a', 'c', 'o.jpg')
            set_http_connect(200, 200, 201, 201, 201,
                             give_connect=test_connect)
            req = Request.blank('/v1/a/c/o.jpg', {})
            req.content_length = 1
            req.body = 'a'
            self.app.memcache.store = {}
            res = controller.PUT(req)
            self.assertTrue(res.status.startswith('201 '))

        self.assertEqual(3, len(written_to))
        for ip, port, device in written_to:
            # this is kind of a hokey test, but in FakeRing, the port is even
            # when the region is 0, and odd when the region is 1, so this test
            # asserts that we only wrote to nodes in region 0.
            self.assertEqual(0, port % 2)

    def test_PUT_respects_write_affinity_with_507s(self):
        written_to = []

        def test_connect(ipaddr, port, device, partition, method, path,
                         headers=None, query_string=None):
            if path == '/a/c/o.jpg':
                written_to.append((ipaddr, port, device))

        with save_globals():
            def is_r0(node):
                return node['region'] == 0

            object_ring = self.app.get_object_ring(None)
            object_ring.max_more_nodes = 100
            self.app.write_affinity_is_local_fn = is_r0
            self.app.write_affinity_node_count = lambda r: 3

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

        self.assertEqual(3, len(written_to))
        # this is kind of a hokey test, but in FakeRing, the port is even when
        # the region is 0, and odd when the region is 1, so this test asserts
        # that we wrote to 2 nodes in region 0, then went to 1 non-r0 node.
        self.assertEqual(0, written_to[0][1] % 2)   # it's (ip, port, device)
        self.assertEqual(0, written_to[1][1] % 2)
        self.assertNotEqual(0, written_to[2][1] % 2)

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
    def test_PUT_last_modified(self):
        prolis = _test_sockets[0]
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
        self.assertEqual(last_modified_put, last_modified_head)

        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/c/o.last_modified HTTP/1.1\r\n'
                 'Host: localhost\r\nConnection: close\r\n'
                 'If-Modified-Since: %s\r\n'
                 'X-Storage-Token: t\r\n\r\n' % last_modified_put)
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 304'
        self.assertEqual(headers[:len(exp)], exp)

        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/c/o.last_modified HTTP/1.1\r\n'
                 'Host: localhost\r\nConnection: close\r\n'
                 'If-Unmodified-Since: %s\r\n'
                 'X-Storage-Token: t\r\n\r\n' % last_modified_put)
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEqual(headers[:len(exp)], exp)

    def test_expirer_DELETE_on_versioned_object(self):
        test_errors = []

        def test_connect(ipaddr, port, device, partition, method, path,
                         headers=None, query_string=None):
            if method == 'DELETE':
                if 'x-if-delete-at' in headers or 'X-If-Delete-At' in headers:
                    test_errors.append('X-If-Delete-At in headers')

        body = json.dumps(
            [{"name": "001o/1",
              "hash": "x",
              "bytes": 0,
              "content_type": "text/plain",
              "last_modified": "1970-01-01T00:00:01.000000"}])
        body_iter = ('', '', body, '', '', '', '', '', '', '', '', '', '', '')
        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'a', 'c', 'o')
            #                HEAD HEAD GET  GET  HEAD GET  GET  GET  PUT  PUT
            #                PUT  DEL  DEL  DEL
            set_http_connect(200, 200, 200, 200, 200, 200, 200, 200, 201, 201,
                             201, 204, 204, 204,
                             give_connect=test_connect,
                             body_iter=body_iter,
                             headers={'x-versions-location': 'foo'})
            self.app.memcache.store = {}
            req = Request.blank('/v1/a/c/o',
                                headers={'X-If-Delete-At': 1},
                                environ={'REQUEST_METHOD': 'DELETE'})
            self.app.update_request(req)
            controller.DELETE(req)
            self.assertEquals(test_errors, [])

    @patch_policies([
        StoragePolicy(0, 'zero', False, object_ring=FakeRing()),
        StoragePolicy(1, 'one', True, object_ring=FakeRing())
    ])
    def test_DELETE_on_expired_versioned_object(self):
        # reset the router post patch_policies
        self.app.obj_controller_router = proxy_server.ObjectControllerRouter()
        methods = set()
        authorize_call_count = [0]

        def test_connect(ipaddr, port, device, partition, method, path,
                         headers=None, query_string=None):
            methods.add((method, path))

        def fake_container_info(account, container, req):
            return {'status': 200, 'sync_key': None,
                    'meta': {}, 'cors': {'allow_origin': None,
                                         'expose_headers': None,
                                         'max_age': None},
                    'sysmeta': {}, 'read_acl': None, 'object_count': None,
                    'write_acl': None, 'versions': 'foo',
                    'partition': 1, 'bytes': None, 'storage_policy': '1',
                    'nodes': [{'zone': 0, 'ip': '10.0.0.0', 'region': 0,
                               'id': 0, 'device': 'sda', 'port': 1000},
                              {'zone': 1, 'ip': '10.0.0.1', 'region': 1,
                               'id': 1, 'device': 'sdb', 'port': 1001},
                              {'zone': 2, 'ip': '10.0.0.2', 'region': 0,
                               'id': 2, 'device': 'sdc', 'port': 1002}]}

        def fake_list_iter(container, prefix, env):
            object_list = [{'name': '1'}, {'name': '2'}, {'name': '3'}]
            for obj in object_list:
                yield obj

        def fake_authorize(req):
            authorize_call_count[0] += 1
            return None  # allow the request

        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'a', 'c', 'o')
            controller.container_info = fake_container_info
            controller._listing_iter = fake_list_iter
            set_http_connect(404, 404, 404,  # get for the previous version
                             200, 200, 200,  # get for the pre-previous
                             201, 201, 201,  # put move the pre-previous
                             204, 204, 204,  # delete for the pre-previous
                             give_connect=test_connect)
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE',
                                         'swift.authorize': fake_authorize})

            self.app.memcache.store = {}
            self.app.update_request(req)
            controller.DELETE(req)
            exp_methods = [('GET', '/a/foo/3'),
                           ('GET', '/a/foo/2'),
                           ('PUT', '/a/c/o'),
                           ('DELETE', '/a/foo/2')]
            self.assertEquals(set(exp_methods), (methods))
            self.assertEquals(authorize_call_count[0], 2)

    @patch_policies([
        StoragePolicy(0, 'zero', False, object_ring=FakeRing()),
        StoragePolicy(1, 'one', True, object_ring=FakeRing())
    ])
    def test_denied_DELETE_of_versioned_object(self):
        """
        Verify that a request with read access to a versions container
        is unable to cause any write operations on the versioned container.
        """
        # reset the router post patch_policies
        self.app.obj_controller_router = proxy_server.ObjectControllerRouter()
        methods = set()
        authorize_call_count = [0]

        def test_connect(ipaddr, port, device, partition, method, path,
                         headers=None, query_string=None):
            methods.add((method, path))

        def fake_container_info(account, container, req):
            return {'status': 200, 'sync_key': None,
                    'meta': {}, 'cors': {'allow_origin': None,
                                         'expose_headers': None,
                                         'max_age': None},
                    'sysmeta': {}, 'read_acl': None, 'object_count': None,
                    'write_acl': None, 'versions': 'foo',
                    'partition': 1, 'bytes': None, 'storage_policy': '1',
                    'nodes': [{'zone': 0, 'ip': '10.0.0.0', 'region': 0,
                               'id': 0, 'device': 'sda', 'port': 1000},
                              {'zone': 1, 'ip': '10.0.0.1', 'region': 1,
                               'id': 1, 'device': 'sdb', 'port': 1001},
                              {'zone': 2, 'ip': '10.0.0.2', 'region': 0,
                               'id': 2, 'device': 'sdc', 'port': 1002}]}

        def fake_list_iter(container, prefix, env):
            object_list = [{'name': '1'}, {'name': '2'}, {'name': '3'}]
            for obj in object_list:
                yield obj

        def fake_authorize(req):
            # deny write access
            authorize_call_count[0] += 1
            return HTTPForbidden(req)  # allow the request

        with save_globals():
            controller = ReplicatedObjectController(self.app, 'a', 'c', 'o')
            controller.container_info = fake_container_info
            # patching _listing_iter simulates request being authorized
            # to list versions container
            controller._listing_iter = fake_list_iter
            set_http_connect(give_connect=test_connect)
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE',
                                         'swift.authorize': fake_authorize})

            self.app.memcache.store = {}
            self.app.update_request(req)
            resp = controller.DELETE(req)
            self.assertEqual(403, resp.status_int)
            self.assertFalse(methods, methods)
            self.assertEquals(authorize_call_count[0], 1)

    def test_PUT_auto_content_type(self):
        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')

            def test_content_type(filename, expected):
                # The three responses here are for account_info() (HEAD to
                # account server), container_info() (HEAD to container server)
                # and three calls to _connect_put_node() (PUT to three object
                # servers)
                set_http_connect(201, 201, 201, 201, 201,
                                 give_content_type=lambda content_type:
                                 self.assertEquals(content_type,
                                                   expected.next()))
                # We need into include a transfer-encoding to get past
                # constraints.check_object_creation()
                req = Request.blank('/v1/a/c/%s' % filename, {},
                                    headers={'transfer-encoding': 'chunked'})
                self.app.update_request(req)
                self.app.memcache.store = {}
                res = controller.PUT(req)
                # If we don't check the response here we could miss problems
                # in PUT()
                self.assertEquals(res.status_int, 201)

            test_content_type('test.jpg', iter(['', '', 'image/jpeg',
                                                'image/jpeg', 'image/jpeg']))
            test_content_type('test.html', iter(['', '', 'text/html',
                                                 'text/html', 'text/html']))
            test_content_type('test.css', iter(['', '', 'text/css',
                                                'text/css', 'text/css']))

    def test_custom_mime_types_files(self):
        swift_dir = mkdtemp()
        try:
            with open(os.path.join(swift_dir, 'mime.types'), 'w') as fp:
                fp.write('foo/bar foo\n')
            proxy_server.Application({'swift_dir': swift_dir},
                                     FakeMemcache(), FakeLogger(),
                                     FakeRing(), FakeRing())
            self.assertEquals(proxy_server.mimetypes.guess_type('blah.foo')[0],
                              'foo/bar')
            self.assertEquals(proxy_server.mimetypes.guess_type('blah.jpg')[0],
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
                self.assertEquals(res.status[:len(expected)], expected)
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
                self.assertEquals(res.status[:len(expected)], expected)
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
                self.assertEquals(res.status[:len(expected)], expected)
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
            self.assertEquals(res.status_int, 413)

    def test_PUT_bad_content_type(self):
        with save_globals():
            set_http_connect(201, 201, 201)
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            req = Request.blank('/v1/a/c/o', {}, headers={
                'Content-Length': 0, 'Content-Type': 'foo/bar;swift_hey=45'})
            self.app.update_request(req)
            res = controller.PUT(req)
            self.assertEquals(res.status_int, 400)

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
                self.assertEquals(res.status[:len(str(expected))],
                                  str(expected))
            test_status_map((200, 200, 201, 201, -1), 201)
            test_status_map((200, 200, 201, -1, -1), 503)
            test_status_map((200, 200, 503, 503, -1), 503)

    def test_POST(self):
        with save_globals():
            self.app.object_post_as_copy = False

            def test_status_map(statuses, expected):
                set_http_connect(*statuses)
                self.app.memcache.store = {}
                req = Request.blank('/v1/a/c/o', {}, method='POST',
                                    headers={'Content-Type': 'foo/bar'})
                self.app.update_request(req)
                res = req.get_response(self.app)
                expected = str(expected)
                self.assertEquals(res.status[:len(expected)], expected)
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
        self.app.object_post_as_copy = False
        self.app.sort_nodes = lambda nodes: nodes
        backend_requests = []

        def capture_requests(ip, port, method, path, headers, *args,
                             **kwargs):
            backend_requests.append((method, path, headers))

        req = Request.blank('/v1/a/c/o', {}, method='POST',
                            headers={'X-Object-Meta-Color': 'Blue'})

        # we want the container_info response to says a policy index of 1
        resp_headers = {'X-Backend-Storage-Policy-Index': 1}
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

        # and this time with post as copy
        self.app.object_post_as_copy = True
        self.app.memcache.store = {}
        backend_requests = []
        req = Request.blank('/v1/a/c/o', {}, method='POST',
                            headers={'X-Object-Meta-Color': 'Blue',
                                     'X-Backend-Storage-Policy-Index': 0})
        with mocked_http_conn(
                200, 200, 200, 200, 200, 201, 201, 201,
                headers=resp_headers, give_connect=capture_requests
        ) as fake_conn:
            resp = req.get_response(self.app)
            self.assertRaises(StopIteration, fake_conn.code_iter.next)
        self.assertEqual(resp.status_int, 202)
        self.assertEqual(len(backend_requests), 8)
        policy0 = {'X-Backend-Storage-Policy-Index': '0'}
        policy1 = {'X-Backend-Storage-Policy-Index': '1'}
        expected = [
            # account info
            {'method': 'HEAD', 'path': '/0/a'},
            # container info
            {'method': 'HEAD', 'path': '/0/a/c'},
            # x-newests
            {'method': 'GET', 'path': '/0/a/c/o', 'headers': policy1},
            {'method': 'GET', 'path': '/0/a/c/o', 'headers': policy1},
            {'method': 'GET', 'path': '/0/a/c/o', 'headers': policy1},
            # new writes
            {'method': 'PUT', 'path': '/0/a/c/o', 'headers': policy0},
            {'method': 'PUT', 'path': '/0/a/c/o', 'headers': policy0},
            {'method': 'PUT', 'path': '/0/a/c/o', 'headers': policy0},
        ]
        for request, expectations in zip(backend_requests, expected):
            check_request(request, **expectations)

    def test_POST_as_copy(self):
        with save_globals():
            def test_status_map(statuses, expected):
                set_http_connect(*statuses)
                self.app.memcache.store = {}
                req = Request.blank('/v1/a/c/o', {'REQUEST_METHOD': 'POST'},
                                    headers={'Content-Type': 'foo/bar'})
                self.app.update_request(req)
                res = req.get_response(self.app)
                expected = str(expected)
                self.assertEquals(res.status[:len(expected)], expected)
            test_status_map((200, 200, 200, 200, 200, 202, 202, 202), 202)
            test_status_map((200, 200, 200, 200, 200, 202, 202, 500), 202)
            test_status_map((200, 200, 200, 200, 200, 202, 500, 500), 503)
            test_status_map((200, 200, 200, 200, 200, 202, 404, 500), 503)
            test_status_map((200, 200, 200, 200, 200, 202, 404, 404), 404)
            test_status_map((200, 200, 200, 200, 200, 404, 500, 500), 503)
            test_status_map((200, 200, 200, 200, 200, 404, 404, 404), 404)

    def test_DELETE(self):
        with save_globals():
            def test_status_map(statuses, expected):
                set_http_connect(*statuses)
                self.app.memcache.store = {}
                req = Request.blank('/v1/a/c/o', {'REQUEST_METHOD': 'DELETE'})
                self.app.update_request(req)
                res = req.get_response(self.app)
                self.assertEquals(res.status[:len(str(expected))],
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
                self.assertEquals(res.status[:len(str(expected))],
                                  str(expected))
                if expected < 400:
                    self.assert_('x-works' in res.headers)
                    self.assertEquals(res.headers['x-works'], 'yes')
                    self.assert_('accept-ranges' in res.headers)
                    self.assertEquals(res.headers['accept-ranges'], 'bytes')

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
                self.assertEquals(res.status[:len(str(expected))],
                                  str(expected))
                self.assertEquals(res.headers.get('last-modified'),
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
                self.assertEquals(res.status[:len(str(expected))],
                                  str(expected))
                self.assertEquals(res.headers.get('last-modified'),
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
                self.assertEquals(res.status[:len(str(expected))],
                                  str(expected))
                self.assertEquals(res.headers.get('last-modified'),
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
            self.app.object_post_as_copy = False
            ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            set_http_connect(200, 200, 202, 202, 202)
            #                acct cont obj  obj  obj
            req = Request.blank('/v1/a/c/o', {'REQUEST_METHOD': 'POST'},
                                headers={'Content-Type': 'foo/bar',
                                         'X-Object-Meta-Foo': 'x' * limit})
            self.app.update_request(req)
            res = req.get_response(self.app)
            self.assertEquals(res.status_int, 202)
            set_http_connect(202, 202, 202)
            req = Request.blank(
                '/v1/a/c/o', {'REQUEST_METHOD': 'POST'},
                headers={'Content-Type': 'foo/bar',
                         'X-Object-Meta-Foo': 'x' * (limit + 1)})
            self.app.update_request(req)
            res = req.get_response(self.app)
            self.assertEquals(res.status_int, 400)

    def test_POST_as_copy_meta_val_len(self):
        with save_globals():
            limit = constraints.MAX_META_VALUE_LENGTH
            set_http_connect(200, 200, 200, 200, 200, 202, 202, 202)
            #                acct cont objc objc objc obj  obj  obj
            req = Request.blank('/v1/a/c/o', {'REQUEST_METHOD': 'POST'},
                                headers={'Content-Type': 'foo/bar',
                                         'X-Object-Meta-Foo': 'x' * limit})
            self.app.update_request(req)
            res = req.get_response(self.app)
            self.assertEquals(res.status_int, 202)
            set_http_connect(202, 202, 202)
            req = Request.blank(
                '/v1/a/c/o', {'REQUEST_METHOD': 'POST'},
                headers={'Content-Type': 'foo/bar',
                         'X-Object-Meta-Foo': 'x' * (limit + 1)})
            self.app.update_request(req)
            res = req.get_response(self.app)
            self.assertEquals(res.status_int, 400)

    def test_POST_meta_key_len(self):
        with save_globals():
            limit = constraints.MAX_META_NAME_LENGTH
            self.app.object_post_as_copy = False
            set_http_connect(200, 200, 202, 202, 202)
            #                acct cont obj  obj  obj
            req = Request.blank(
                '/v1/a/c/o', {'REQUEST_METHOD': 'POST'},
                headers={'Content-Type': 'foo/bar',
                         ('X-Object-Meta-' + 'x' * limit): 'x'})
            self.app.update_request(req)
            res = req.get_response(self.app)
            self.assertEquals(res.status_int, 202)
            set_http_connect(202, 202, 202)
            req = Request.blank(
                '/v1/a/c/o', {'REQUEST_METHOD': 'POST'},
                headers={'Content-Type': 'foo/bar',
                         ('X-Object-Meta-' + 'x' * (limit + 1)): 'x'})
            self.app.update_request(req)
            res = req.get_response(self.app)
            self.assertEquals(res.status_int, 400)

    def test_POST_as_copy_meta_key_len(self):
        with save_globals():
            limit = constraints.MAX_META_NAME_LENGTH
            set_http_connect(200, 200, 200, 200, 200, 202, 202, 202)
            #                acct cont objc objc objc obj  obj  obj
            req = Request.blank(
                '/v1/a/c/o', {'REQUEST_METHOD': 'POST'},
                headers={'Content-Type': 'foo/bar',
                         ('X-Object-Meta-' + 'x' * limit): 'x'})
            self.app.update_request(req)
            res = req.get_response(self.app)
            self.assertEquals(res.status_int, 202)
            set_http_connect(202, 202, 202)
            req = Request.blank(
                '/v1/a/c/o', {'REQUEST_METHOD': 'POST'},
                headers={'Content-Type': 'foo/bar',
                         ('X-Object-Meta-' + 'x' * (limit + 1)): 'x'})
            self.app.update_request(req)
            res = req.get_response(self.app)
            self.assertEquals(res.status_int, 400)

    def test_POST_meta_count(self):
        with save_globals():
            limit = constraints.MAX_META_COUNT
            headers = dict(
                (('X-Object-Meta-' + str(i), 'a') for i in xrange(limit + 1)))
            headers.update({'Content-Type': 'foo/bar'})
            set_http_connect(202, 202, 202)
            req = Request.blank('/v1/a/c/o', {'REQUEST_METHOD': 'POST'},
                                headers=headers)
            self.app.update_request(req)
            res = req.get_response(self.app)
            self.assertEquals(res.status_int, 400)

    def test_POST_meta_size(self):
        with save_globals():
            limit = constraints.MAX_META_OVERALL_SIZE
            count = limit / 256  # enough to cause the limit to be reached
            headers = dict(
                (('X-Object-Meta-' + str(i), 'a' * 256)
                    for i in xrange(count + 1)))
            headers.update({'Content-Type': 'foo/bar'})
            set_http_connect(202, 202, 202)
            req = Request.blank('/v1/a/c/o', {'REQUEST_METHOD': 'POST'},
                                headers=headers)
            self.app.update_request(req)
            res = req.get_response(self.app)
            self.assertEquals(res.status_int, 400)

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
            self.assertNotEquals(it_worked, [])
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
            self.assertNotEquals(it_worked, [])
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
            self.assertEquals(resp.status_int, 201)
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
            self.assertEquals(resp.status_int, 408)

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

            class SlowBody(object):

                def __init__(self):
                    self.sent = 0

                def read(self, size=-1):
                    raise Exception('Disconnected')

            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'PUT',
                                         'wsgi.input': SlowBody()},
                                headers={'Content-Length': '4',
                                         'Content-Type': 'text/plain'})
            self.app.update_request(req)
            set_http_connect(200, 200, 201, 201, 201)
            #                acct cont obj  obj  obj
            resp = req.get_response(self.app)
            self.assertEquals(resp.status_int, 499)

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
            self.assert_(not got_exc)
            self.app.recoverable_node_timeout = 0.1
            set_http_connect(200, 200, 200, slow=1.0)
            resp = req.get_response(self.app)
            got_exc = False
            try:
                resp.body
            except ChunkReadTimeout:
                got_exc = True
            self.assert_(got_exc)

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
            got_exc = False
            try:
                self.assertEquals('', resp.body)
            except ChunkReadTimeout:
                got_exc = True
            self.assert_(got_exc)

            set_http_connect(200, 200, 200, body='lalala',
                             slow=[1.0, 1.0])
            resp = req.get_response(self.app)
            got_exc = False
            try:
                self.assertEquals(resp.body, 'lalala')
            except ChunkReadTimeout:
                got_exc = True
            self.assert_(not got_exc)

            set_http_connect(200, 200, 200, body='lalala',
                             slow=[1.0, 1.0], etags=['a', 'a', 'a'])
            resp = req.get_response(self.app)
            got_exc = False
            try:
                self.assertEquals(resp.body, 'lalala')
            except ChunkReadTimeout:
                got_exc = True
            self.assert_(not got_exc)

            set_http_connect(200, 200, 200, body='lalala',
                             slow=[1.0, 1.0], etags=['a', 'b', 'a'])
            resp = req.get_response(self.app)
            got_exc = False
            try:
                self.assertEquals(resp.body, 'lalala')
            except ChunkReadTimeout:
                got_exc = True
            self.assert_(not got_exc)

            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
            set_http_connect(200, 200, 200, body='lalala',
                             slow=[1.0, 1.0], etags=['a', 'b', 'b'])
            resp = req.get_response(self.app)
            got_exc = False
            try:
                resp.body
            except ChunkReadTimeout:
                got_exc = True
            self.assert_(got_exc)

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
            self.assertEquals(resp.status_int, 201)
            self.app.node_timeout = 0.1
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '4',
                                         'Content-Type': 'text/plain'},
                                body='    ')
            self.app.update_request(req)
            set_http_connect(201, 201, 201, slow=1.0)
            resp = req.get_response(self.app)
            self.assertEquals(resp.status_int, 503)

    def test_node_request_setting(self):
        baseapp = proxy_server.Application({'request_node_count': '3'},
                                           FakeMemcache(),
                                           container_ring=FakeRing(),
                                           account_ring=FakeRing())
        self.assertEquals(baseapp.request_node_count(3), 3)

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
                self.assertEquals(len(collected_nodes), 5)

                object_ring.max_more_nodes = 20
                self.app.request_node_count = lambda r: 20
                partition, nodes = object_ring.get_nodes('account',
                                                         'container',
                                                         'object')
                collected_nodes = []
                for node in self.app.iter_nodes(object_ring,
                                                partition):
                    collected_nodes.append(node)
                self.assertEquals(len(collected_nodes), 9)

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
                self.assertEquals(len(collected_nodes), 7)
                self.assertEquals(self.app.logger.log_dict['warning'], [])
                self.assertEquals(self.app.logger.get_increments(), [])

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
                self.assertEquals(len(collected_nodes), 7)
                self.assertEquals(self.app.logger.log_dict['warning'], [
                    (('Handoff requested (5)',), {})])
                self.assertEquals(self.app.logger.get_increments(),
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
                self.assertEquals(len(collected_nodes), 7)
                self.assertEquals(self.app.logger.log_dict['warning'], [
                    (('Handoff requested (5)',), {}),
                    (('Handoff requested (6)',), {})])
                self.assertEquals(self.app.logger.get_increments(),
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
                self.assertEquals(len(collected_nodes), 10)
                self.assertEquals(self.app.logger.log_dict['warning'], [
                    (('Handoff requested (7)',), {}),
                    (('Handoff requested (8)',), {}),
                    (('Handoff requested (9)',), {}),
                    (('Handoff requested (10)',), {})])
                self.assertEquals(self.app.logger.get_increments(),
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
                object_ring.get_part_nodes(0))

    def test_iter_nodes_skips_error_limited(self):
        with mock.patch.object(self.app, 'sort_nodes', lambda n: n):
            object_ring = self.app.get_object_ring(None)
            first_nodes = list(self.app.iter_nodes(object_ring, 0))
            second_nodes = list(self.app.iter_nodes(object_ring, 0))
            self.assertTrue(first_nodes[0] in second_nodes)

            self.app.error_limit(first_nodes[0], 'test')
            second_nodes = list(self.app.iter_nodes(object_ring, 0))
            self.assertTrue(first_nodes[0] not in second_nodes)

    def test_iter_nodes_gives_extra_if_error_limited_inline(self):
        object_ring = self.app.get_object_ring(None)
        with nested(
                mock.patch.object(self.app, 'sort_nodes', lambda n: n),
                mock.patch.object(self.app, 'request_node_count',
                                  lambda r: 6),
                mock.patch.object(object_ring, 'max_more_nodes', 99)):
            first_nodes = list(self.app.iter_nodes(object_ring, 0))
            second_nodes = []
            for node in self.app.iter_nodes(object_ring, 0):
                if not second_nodes:
                    self.app.error_limit(node, 'test')
                second_nodes.append(node)
            self.assertEquals(len(first_nodes), 6)
            self.assertEquals(len(second_nodes), 7)

    def test_iter_nodes_with_custom_node_iter(self):
        object_ring = self.app.get_object_ring(None)
        node_list = [dict(id=n, ip='1.2.3.4', port=n, device='D')
                     for n in xrange(10)]
        with nested(
                mock.patch.object(self.app, 'sort_nodes', lambda n: n),
                mock.patch.object(self.app, 'request_node_count',
                                  lambda r: 3)):
            got_nodes = list(self.app.iter_nodes(object_ring, 0,
                                                 node_iter=iter(node_list)))
        self.assertEqual(node_list[:3], got_nodes)

        with nested(
                mock.patch.object(self.app, 'sort_nodes', lambda n: n),
                mock.patch.object(self.app, 'request_node_count',
                                  lambda r: 1000000)):
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
        self.assertEquals(resp.headers['X-Test'], '1')

    def test_best_response_sets_etag(self):
        controller = ReplicatedObjectController(
            self.app, 'account', 'container', 'object')
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = controller.best_response(req, [200] * 3, ['OK'] * 3, [''] * 3,
                                        'Object')
        self.assertEquals(resp.etag, None)
        resp = controller.best_response(req, [200] * 3, ['OK'] * 3, [''] * 3,
                                        'Object',
                                        etag='68b329da9893e34099c7d8ad5cb9c940'
                                        )
        self.assertEquals(resp.etag, '68b329da9893e34099c7d8ad5cb9c940')

    def test_proxy_passes_content_type(self):
        with save_globals():
            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
            self.app.update_request(req)
            set_http_connect(200, 200, 200)
            resp = req.get_response(self.app)
            self.assertEquals(resp.status_int, 200)
            self.assertEquals(resp.content_type, 'x-application/test')
            set_http_connect(200, 200, 200)
            resp = req.get_response(self.app)
            self.assertEquals(resp.status_int, 200)
            self.assertEquals(resp.content_length, 0)
            set_http_connect(200, 200, 200, slow=True)
            resp = req.get_response(self.app)
            self.assertEquals(resp.status_int, 200)
            self.assertEquals(resp.content_length, 4)

    def test_proxy_passes_content_length_on_head(self):
        with save_globals():
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'HEAD'})
            self.app.update_request(req)
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            set_http_connect(200, 200, 200)
            resp = controller.HEAD(req)
            self.assertEquals(resp.status_int, 200)
            self.assertEquals(resp.content_length, 0)
            set_http_connect(200, 200, 200, slow=True)
            resp = controller.HEAD(req)
            self.assertEquals(resp.status_int, 200)
            self.assertEquals(resp.content_length, 4)

    def test_error_limiting(self):
        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            controller.app.sort_nodes = lambda l: l
            object_ring = controller.app.get_object_ring(None)
            self.assert_status_map(controller.HEAD, (200, 200, 503, 200, 200),
                                   200)
            self.assertEquals(
                node_error_count(controller.app, object_ring.devs[0]), 2)
            self.assert_(node_last_error(controller.app, object_ring.devs[0])
                         is not None)
            for _junk in xrange(self.app.error_suppression_limit):
                self.assert_status_map(controller.HEAD, (200, 200, 503, 503,
                                                         503), 503)
            self.assertEquals(
                node_error_count(controller.app, object_ring.devs[0]),
                self.app.error_suppression_limit + 1)
            self.assert_status_map(controller.HEAD, (200, 200, 200, 200, 200),
                                   503)
            self.assert_(node_last_error(controller.app, object_ring.devs[0])
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
            controller.app.sort_nodes = lambda l: l
            object_ring = controller.app.get_object_ring(None)
            self.assert_status_map(controller.HEAD, (200, 200, 503, 200, 200),
                                   200)
            self.assertEquals(
                node_error_count(controller.app, object_ring.devs[0]), 2)
            self.assert_(node_last_error(controller.app, object_ring.devs[0])
                         is not None)
            for _junk in xrange(self.app.error_suppression_limit):
                self.assert_status_map(controller.HEAD, (200, 200, 503, 503,
                                                         503), 503)
            self.assertEquals(
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
            controller.app.sort_nodes = lambda l: l
            object_ring = controller.app.get_object_ring(None)
            # acc con obj obj obj
            self.assert_status_map(controller.PUT, (200, 200, 503, 200, 200),
                                   200)

            # 2, not 1, because assert_status_map() calls the method twice
            odevs = object_ring.devs
            self.assertEquals(node_error_count(controller.app, odevs[0]), 2)
            self.assertEquals(node_error_count(controller.app, odevs[1]), 0)
            self.assertEquals(node_error_count(controller.app, odevs[2]), 0)
            self.assert_(node_last_error(controller.app, odevs[0]) is not None)
            self.assert_(node_last_error(controller.app, odevs[1]) is None)
            self.assert_(node_last_error(controller.app, odevs[2]) is None)

    def test_PUT_error_limiting_last_node(self):
        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            controller.app.sort_nodes = lambda l: l
            object_ring = controller.app.get_object_ring(None)
            # acc con obj obj obj
            self.assert_status_map(controller.PUT, (200, 200, 200, 200, 503),
                                   200)

            # 2, not 1, because assert_status_map() calls the method twice
            odevs = object_ring.devs
            self.assertEquals(node_error_count(controller.app, odevs[0]), 0)
            self.assertEquals(node_error_count(controller.app, odevs[1]), 0)
            self.assertEquals(node_error_count(controller.app, odevs[2]), 2)
            self.assert_(node_last_error(controller.app, odevs[0]) is None)
            self.assert_(node_last_error(controller.app, odevs[1]) is None)
            self.assert_(node_last_error(controller.app, odevs[2]) is not None)

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
            self.assertEquals(resp.status_int, 200)

            set_http_connect(404, 404, 404)
            #                acct acct acct
            # make sure to use a fresh request without cached env
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'})
            resp = getattr(controller, 'DELETE')(req)
            self.assertEquals(resp.status_int, 404)

            set_http_connect(503, 404, 404)
            #                acct acct acct
            # make sure to use a fresh request without cached env
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'})
            resp = getattr(controller, 'DELETE')(req)
            self.assertEquals(resp.status_int, 404)

            set_http_connect(503, 503, 404)
            #                acct acct acct
            # make sure to use a fresh request without cached env
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'})
            resp = getattr(controller, 'DELETE')(req)
            self.assertEquals(resp.status_int, 404)

            set_http_connect(503, 503, 503)
            #                acct acct acct
            # make sure to use a fresh request without cached env
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'})
            resp = getattr(controller, 'DELETE')(req)
            self.assertEquals(resp.status_int, 404)

            set_http_connect(200, 200, 204, 204, 204)
            #                acct cont obj  obj  obj
            # make sure to use a fresh request without cached env
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'})
            resp = getattr(controller, 'DELETE')(req)
            self.assertEquals(resp.status_int, 204)

            set_http_connect(200, 404, 404, 404)
            #                acct cont cont cont
            # make sure to use a fresh request without cached env
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'})
            resp = getattr(controller, 'DELETE')(req)
            self.assertEquals(resp.status_int, 404)

            set_http_connect(200, 503, 503, 503)
            #                acct cont cont cont
            # make sure to use a fresh request without cached env
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'DELETE'})
            resp = getattr(controller, 'DELETE')(req)
            self.assertEquals(resp.status_int, 404)

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
            self.assertEquals(resp.status_int, 404)

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
            self.assertEquals(resp.status_int, 404)

    def test_PUT_POST_requires_container_exist(self):
        with save_globals():
            self.app.object_post_as_copy = False
            self.app.memcache = FakeMemcacheReturnsNone()
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')

            set_http_connect(200, 404, 404, 404, 200, 200, 200)
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'PUT'})
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 404)

            set_http_connect(200, 404, 404, 404, 200, 200)
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'POST'},
                                headers={'Content-Type': 'text/plain'})
            self.app.update_request(req)
            resp = controller.POST(req)
            self.assertEquals(resp.status_int, 404)

    def test_PUT_POST_as_copy_requires_container_exist(self):
        with save_globals():
            self.app.memcache = FakeMemcacheReturnsNone()
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            set_http_connect(200, 404, 404, 404, 200, 200, 200)
            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'})
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 404)

            set_http_connect(200, 404, 404, 404, 200, 200, 200, 200, 200, 200)
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'POST'},
                                headers={'Content-Type': 'text/plain'})
            self.app.update_request(req)
            resp = controller.POST(req)
            self.assertEquals(resp.status_int, 404)

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
            self.assertEquals(resp.status_int, 201)

            set_http_connect(201, 201, 201)
            req = Request.blank(
                '/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={'Content-Length': '0',
                         'X-Object-Meta-' + (
                             'a' * constraints.MAX_META_NAME_LENGTH): 'v'})
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            req = Request.blank(
                '/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={
                    'Content-Length': '0',
                    'X-Object-Meta-' + (
                        'a' * (constraints.MAX_META_NAME_LENGTH + 1)): 'v'})
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 400)

            set_http_connect(201, 201, 201)
            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '0',
                                         'X-Object-Meta-Too-Long': 'a' *
                                         constraints.MAX_META_VALUE_LENGTH})
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            req = Request.blank(
                '/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={'Content-Length': '0',
                         'X-Object-Meta-Too-Long': 'a' *
                         (constraints.MAX_META_VALUE_LENGTH + 1)})
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 400)

            set_http_connect(201, 201, 201)
            headers = {'Content-Length': '0'}
            for x in xrange(constraints.MAX_META_COUNT):
                headers['X-Object-Meta-%d' % x] = 'v'
            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers=headers)
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            headers = {'Content-Length': '0'}
            for x in xrange(constraints.MAX_META_COUNT + 1):
                headers['X-Object-Meta-%d' % x] = 'v'
            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers=headers)
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 400)

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
            self.assertEquals(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            headers['X-Object-Meta-a'] = \
                'a' * (constraints.MAX_META_OVERALL_SIZE - size)
            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers=headers)
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 400)

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
                    unused_status_list.append(new_connect.code_iter.next())
                except StopIteration:
                    break
            if unused_status_list:
                raise self.fail('UN-USED STATUS CODES: %r' %
                                unused_status_list)

    def test_basic_put_with_x_copy_from(self):
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': 'c/o'})
        status_list = (200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-copied-from'], 'c/o')

    def test_basic_put_with_x_copy_from_account(self):
        req = Request.blank('/v1/a1/c1/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': 'c/o',
                                     'X-Copy-From-Account': 'a'})
        status_list = (200, 200, 200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont acc1 con1 objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-copied-from'], 'c/o')
        self.assertEquals(resp.headers['x-copied-from-account'], 'a')

    def test_basic_put_with_x_copy_from_across_container(self):
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': 'c2/o'})
        status_list = (200, 200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont conc objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-copied-from'], 'c2/o')

    def test_basic_put_with_x_copy_from_across_container_and_account(self):
        req = Request.blank('/v1/a1/c1/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': 'c2/o',
                                     'X-Copy-From-Account': 'a'})
        status_list = (200, 200, 200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont acc1 con1 objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-copied-from'], 'c2/o')
        self.assertEquals(resp.headers['x-copied-from-account'], 'a')

    def test_copy_non_zero_content_length(self):
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '5',
                                     'X-Copy-From': 'c/o'})
        status_list = (200, 200)
        #                acct cont
        with self.controller_context(req, *status_list) as controller:
            resp = controller.PUT(req)
        self.assertEquals(resp.status_int, 400)

    def test_copy_non_zero_content_length_with_account(self):
        req = Request.blank('/v1/a1/c1/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '5',
                                     'X-Copy-From': 'c/o',
                                     'X-Copy-From-Account': 'a'})
        status_list = (200, 200)
        #                acct cont
        with self.controller_context(req, *status_list) as controller:
            resp = controller.PUT(req)
        self.assertEquals(resp.status_int, 400)

    def test_copy_with_slashes_in_x_copy_from(self):
        # extra source path parsing
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': 'c/o/o2'})
        status_list = (200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-copied-from'], 'c/o/o2')

    def test_copy_with_slashes_in_x_copy_from_and_account(self):
        # extra source path parsing
        req = Request.blank('/v1/a1/c1/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': 'c/o/o2',
                                     'X-Copy-From-Account': 'a'})
        status_list = (200, 200, 200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont acc1 con1 objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-copied-from'], 'c/o/o2')
        self.assertEquals(resp.headers['x-copied-from-account'], 'a')

    def test_copy_with_spaces_in_x_copy_from(self):
        # space in soure path
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': 'c/o%20o2'})
        status_list = (200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-copied-from'], 'c/o%20o2')

    def test_copy_with_spaces_in_x_copy_from_and_account(self):
        # space in soure path
        req = Request.blank('/v1/a1/c1/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': 'c/o%20o2',
                                     'X-Copy-From-Account': 'a'})
        status_list = (200, 200, 200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont acc1 con1 objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-copied-from'], 'c/o%20o2')
        self.assertEquals(resp.headers['x-copied-from-account'], 'a')

    def test_copy_with_leading_slash_in_x_copy_from(self):
        # repeat tests with leading /
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o'})
        status_list = (200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-copied-from'], 'c/o')

    def test_copy_with_leading_slash_in_x_copy_from_and_account(self):
        # repeat tests with leading /
        req = Request.blank('/v1/a1/c1/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o',
                                     'X-Copy-From-Account': 'a'})
        status_list = (200, 200, 200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont acc1 con1 objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-copied-from'], 'c/o')
        self.assertEquals(resp.headers['x-copied-from-account'], 'a')

    def test_copy_with_leading_slash_and_slashes_in_x_copy_from(self):
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o/o2'})
        status_list = (200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-copied-from'], 'c/o/o2')

    def test_copy_with_leading_slash_and_slashes_in_x_copy_from_acct(self):
        req = Request.blank('/v1/a1/c1/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o/o2',
                                     'X-Copy-From-Account': 'a'})
        status_list = (200, 200, 200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont acc1 con1 objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-copied-from'], 'c/o/o2')
        self.assertEquals(resp.headers['x-copied-from-account'], 'a')

    def test_copy_with_no_object_in_x_copy_from(self):
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c'})
        status_list = (200, 200)
        #              acct cont
        with self.controller_context(req, *status_list) as controller:
            try:
                controller.PUT(req)
            except HTTPException as resp:
                self.assertEquals(resp.status_int // 100, 4)  # client error
            else:
                raise self.fail('Invalid X-Copy-From did not raise '
                                'client error')

    def test_copy_with_no_object_in_x_copy_from_and_account(self):
        req = Request.blank('/v1/a1/c1/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c',
                                     'X-Copy-From-Account': 'a'})
        status_list = (200, 200)
        #              acct cont
        with self.controller_context(req, *status_list) as controller:
            try:
                controller.PUT(req)
            except HTTPException as resp:
                self.assertEquals(resp.status_int // 100, 4)  # client error
            else:
                raise self.fail('Invalid X-Copy-From did not raise '
                                'client error')

    def test_copy_server_error_reading_source(self):
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o'})
        status_list = (200, 200, 503, 503, 503)
        #              acct cont objc objc objc
        with self.controller_context(req, *status_list) as controller:
            resp = controller.PUT(req)
        self.assertEquals(resp.status_int, 503)

    def test_copy_server_error_reading_source_and_account(self):
        req = Request.blank('/v1/a1/c1/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o',
                                     'X-Copy-From-Account': 'a'})
        status_list = (200, 200, 200, 200, 503, 503, 503)
        #              acct cont acct cont objc objc objc
        with self.controller_context(req, *status_list) as controller:
            resp = controller.PUT(req)
        self.assertEquals(resp.status_int, 503)

    def test_copy_not_found_reading_source(self):
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o'})
        # not found
        status_list = (200, 200, 404, 404, 404)
        #              acct cont objc objc objc
        with self.controller_context(req, *status_list) as controller:
            resp = controller.PUT(req)
        self.assertEquals(resp.status_int, 404)

    def test_copy_not_found_reading_source_and_account(self):
        req = Request.blank('/v1/a1/c1/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o',
                                     'X-Copy-From-Account': 'a'})
        # not found
        status_list = (200, 200, 200, 200, 404, 404, 404)
        #              acct cont acct cont objc objc objc
        with self.controller_context(req, *status_list) as controller:
            resp = controller.PUT(req)
        self.assertEquals(resp.status_int, 404)

    def test_copy_with_some_missing_sources(self):
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o'})
        status_list = (200, 200, 404, 404, 200, 201, 201, 201)
        #              acct cont objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

    def test_copy_with_some_missing_sources_and_account(self):
        req = Request.blank('/v1/a1/c1/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o',
                                     'X-Copy-From-Account': 'a'})
        status_list = (200, 200, 200, 200, 404, 404, 200, 201, 201, 201)
        #              acct cont acct cont objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

    def test_copy_with_object_metadata(self):
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o',
                                     'X-Object-Meta-Ours': 'okay'})
        # test object metadata
        status_list = (200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers.get('x-object-meta-test'), 'testing')
        self.assertEquals(resp.headers.get('x-object-meta-ours'), 'okay')
        self.assertEquals(resp.headers.get('x-delete-at'), '9876543210')

    def test_copy_with_object_metadata_and_account(self):
        req = Request.blank('/v1/a1/c1/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o',
                                     'X-Object-Meta-Ours': 'okay',
                                     'X-Copy-From-Account': 'a'})
        # test object metadata
        status_list = (200, 200, 200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont acct cont objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.PUT(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers.get('x-object-meta-test'), 'testing')
        self.assertEquals(resp.headers.get('x-object-meta-ours'), 'okay')
        self.assertEquals(resp.headers.get('x-delete-at'), '9876543210')

    @_limit_max_file_size
    def test_copy_source_larger_than_max_file_size(self):
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o'})
        # copy-from object is too large to fit in target object

        class LargeResponseBody(object):

            def __len__(self):
                return constraints.MAX_FILE_SIZE + 1

            def __getitem__(self, key):
                return ''

        copy_from_obj_body = LargeResponseBody()
        status_list = (200, 200, 200, 200, 200)
        #              acct cont objc objc objc
        kwargs = dict(body=copy_from_obj_body)
        with self.controller_context(req, *status_list,
                                     **kwargs) as controller:
            self.app.update_request(req)

            self.app.memcache.store = {}
            try:
                resp = controller.PUT(req)
            except HTTPException as resp:
                pass
            self.assertEquals(resp.status_int, 413)

    def test_basic_COPY(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': 'c/o2'})
        status_list = (200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.COPY(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-copied-from'], 'c/o')

    def test_basic_COPY_account(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': 'c1/o2',
                                     'Destination-Account': 'a1'})
        status_list = (200, 200, 200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont acct cont objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.COPY(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-copied-from'], 'c/o')
        self.assertEquals(resp.headers['x-copied-from-account'], 'a')

    def test_COPY_across_containers(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': 'c2/o'})
        status_list = (200, 200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont c2   objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.COPY(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-copied-from'], 'c/o')

    def test_COPY_source_with_slashes_in_name(self):
        req = Request.blank('/v1/a/c/o/o2',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': 'c/o'})
        status_list = (200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.COPY(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-copied-from'], 'c/o/o2')

    def test_COPY_account_source_with_slashes_in_name(self):
        req = Request.blank('/v1/a/c/o/o2',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': 'c1/o',
                                     'Destination-Account': 'a1'})
        status_list = (200, 200, 200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont acct cont objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.COPY(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-copied-from'], 'c/o/o2')
        self.assertEquals(resp.headers['x-copied-from-account'], 'a')

    def test_COPY_destination_leading_slash(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c/o'})
        status_list = (200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.COPY(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-copied-from'], 'c/o')

    def test_COPY_account_destination_leading_slash(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c1/o',
                                     'Destination-Account': 'a1'})
        status_list = (200, 200, 200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont acct cont objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.COPY(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-copied-from'], 'c/o')
        self.assertEquals(resp.headers['x-copied-from-account'], 'a')

    def test_COPY_source_with_slashes_destination_leading_slash(self):
        req = Request.blank('/v1/a/c/o/o2',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c/o'})
        status_list = (200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.COPY(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-copied-from'], 'c/o/o2')

    def test_COPY_account_source_with_slashes_destination_leading_slash(self):
        req = Request.blank('/v1/a/c/o/o2',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c1/o',
                                     'Destination-Account': 'a1'})
        status_list = (200, 200, 200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont acct cont objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.COPY(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers['x-copied-from'], 'c/o/o2')
        self.assertEquals(resp.headers['x-copied-from-account'], 'a')

    def test_COPY_no_object_in_destination(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': 'c_o'})
        status_list = []  # no requests needed
        with self.controller_context(req, *status_list) as controller:
            self.assertRaises(HTTPException, controller.COPY, req)

    def test_COPY_account_no_object_in_destination(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': 'c_o',
                                     'Destination-Account': 'a1'})
        status_list = []  # no requests needed
        with self.controller_context(req, *status_list) as controller:
            self.assertRaises(HTTPException, controller.COPY, req)

    def test_COPY_server_error_reading_source(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c/o'})
        status_list = (200, 200, 503, 503, 503)
        #              acct cont objc objc objc
        with self.controller_context(req, *status_list) as controller:
            resp = controller.COPY(req)
        self.assertEquals(resp.status_int, 503)

    def test_COPY_account_server_error_reading_source(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c1/o',
                                     'Destination-Account': 'a1'})
        status_list = (200, 200, 200, 200, 503, 503, 503)
        #              acct cont acct cont objc objc objc
        with self.controller_context(req, *status_list) as controller:
            resp = controller.COPY(req)
        self.assertEquals(resp.status_int, 503)

    def test_COPY_not_found_reading_source(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c/o'})
        status_list = (200, 200, 404, 404, 404)
        #                acct cont objc objc objc
        with self.controller_context(req, *status_list) as controller:
            resp = controller.COPY(req)
        self.assertEquals(resp.status_int, 404)

    def test_COPY_account_not_found_reading_source(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c1/o',
                                     'Destination-Account': 'a1'})
        status_list = (200, 200, 200, 200, 404, 404, 404)
        #              acct cont acct cont objc objc objc
        with self.controller_context(req, *status_list) as controller:
            resp = controller.COPY(req)
        self.assertEquals(resp.status_int, 404)

    def test_COPY_with_some_missing_sources(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c/o'})
        status_list = (200, 200, 404, 404, 200, 201, 201, 201)
        #                acct cont objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.COPY(req)
        self.assertEquals(resp.status_int, 201)

    def test_COPY_account_with_some_missing_sources(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c1/o',
                                     'Destination-Account': 'a1'})
        status_list = (200, 200, 200, 200, 404, 404, 200, 201, 201, 201)
        #              acct cont acct cont objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.COPY(req)
        self.assertEquals(resp.status_int, 201)

    def test_COPY_with_metadata(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c/o',
                                     'X-Object-Meta-Ours': 'okay'})
        status_list = (200, 200, 200, 200, 200, 201, 201, 201)
        #                acct cont objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.COPY(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers.get('x-object-meta-test'),
                          'testing')
        self.assertEquals(resp.headers.get('x-object-meta-ours'), 'okay')
        self.assertEquals(resp.headers.get('x-delete-at'), '9876543210')

    def test_COPY_account_with_metadata(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c1/o',
                                     'X-Object-Meta-Ours': 'okay',
                                     'Destination-Account': 'a1'})
        status_list = (200, 200, 200, 200, 200, 200, 200, 201, 201, 201)
        #              acct cont acct cont objc objc objc obj  obj  obj
        with self.controller_context(req, *status_list) as controller:
            resp = controller.COPY(req)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(resp.headers.get('x-object-meta-test'),
                          'testing')
        self.assertEquals(resp.headers.get('x-object-meta-ours'), 'okay')
        self.assertEquals(resp.headers.get('x-delete-at'), '9876543210')

    @_limit_max_file_size
    def test_COPY_source_larger_than_max_file_size(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c/o'})

        class LargeResponseBody(object):

            def __len__(self):
                return constraints.MAX_FILE_SIZE + 1

            def __getitem__(self, key):
                return ''

        copy_from_obj_body = LargeResponseBody()
        status_list = (200, 200, 200, 200, 200)
        #              acct cont objc objc objc
        kwargs = dict(body=copy_from_obj_body)
        with self.controller_context(req, *status_list,
                                     **kwargs) as controller:
            try:
                resp = controller.COPY(req)
            except HTTPException as resp:
                pass
        self.assertEquals(resp.status_int, 413)

    @_limit_max_file_size
    def test_COPY_account_source_larger_than_max_file_size(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c1/o',
                                     'Destination-Account': 'a1'})

        class LargeResponseBody(object):

            def __len__(self):
                return constraints.MAX_FILE_SIZE + 1

            def __getitem__(self, key):
                return ''

        copy_from_obj_body = LargeResponseBody()
        status_list = (200, 200, 200, 200, 200)
        #              acct cont objc objc objc
        kwargs = dict(body=copy_from_obj_body)
        with self.controller_context(req, *status_list,
                                     **kwargs) as controller:
            try:
                resp = controller.COPY(req)
            except HTTPException as resp:
                pass
        self.assertEquals(resp.status_int, 413)

    def test_COPY_newest(self):
        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'a', 'c', 'o')
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'COPY'},
                                headers={'Destination': '/c/o'})
            req.account = 'a'
            controller.object_name = 'o'
            set_http_connect(200, 200, 200, 200, 200, 201, 201, 201,
                             #act cont objc objc objc obj  obj  obj
                             timestamps=('1', '1', '1', '3', '2', '4', '4',
                                         '4'))
            self.app.memcache.store = {}
            resp = controller.COPY(req)
            self.assertEquals(resp.status_int, 201)
            self.assertEquals(resp.headers['x-copied-from-last-modified'],
                              '3')

    def test_COPY_account_newest(self):
        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'a', 'c', 'o')
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'COPY'},
                                headers={'Destination': '/c1/o',
                                         'Destination-Account': 'a1'})
            req.account = 'a'
            controller.object_name = 'o'
            set_http_connect(200, 200, 200, 200, 200, 200, 200, 201, 201, 201,
                             #act cont acct cont objc objc objc obj  obj  obj
                             timestamps=('1', '1', '1', '1', '3', '2', '1',
                                         '4', '4', '4'))
            self.app.memcache.store = {}
            resp = controller.COPY(req)
            self.assertEquals(resp.status_int, 201)
            self.assertEquals(resp.headers['x-copied-from-last-modified'],
                              '3')

    def test_COPY_delete_at(self):
        with save_globals():
            backend_requests = []

            def capture_requests(ipaddr, port, device, partition, method, path,
                                 headers=None, query_string=None):
                backend_requests.append((method, path, headers))

            controller = ReplicatedObjectController(
                self.app, 'a', 'c', 'o')
            set_http_connect(200, 200, 200, 200, 200, 201, 201, 201,
                             give_connect=capture_requests)
            self.app.memcache.store = {}
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'COPY'},
                                headers={'Destination': '/c/o'})

            self.app.update_request(req)
            resp = controller.COPY(req)
            self.assertEqual(201, resp.status_int)  # sanity
            for method, path, given_headers in backend_requests:
                if method != 'PUT':
                    continue
                self.assertEquals(given_headers.get('X-Delete-At'),
                                  '9876543210')
                self.assertTrue('X-Delete-At-Host' in given_headers)
                self.assertTrue('X-Delete-At-Device' in given_headers)
                self.assertTrue('X-Delete-At-Partition' in given_headers)
                self.assertTrue('X-Delete-At-Container' in given_headers)

    def test_COPY_account_delete_at(self):
        with save_globals():
            backend_requests = []

            def capture_requests(ipaddr, port, device, partition, method, path,
                                 headers=None, query_string=None):
                backend_requests.append((method, path, headers))

            controller = ReplicatedObjectController(
                self.app, 'a', 'c', 'o')
            set_http_connect(200, 200, 200, 200, 200, 200, 200, 201, 201, 201,
                             give_connect=capture_requests)
            self.app.memcache.store = {}
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'COPY'},
                                headers={'Destination': '/c1/o',
                                         'Destination-Account': 'a1'})

            self.app.update_request(req)
            resp = controller.COPY(req)
            self.assertEqual(201, resp.status_int)  # sanity
            for method, path, given_headers in backend_requests:
                if method != 'PUT':
                    continue
                self.assertEquals(given_headers.get('X-Delete-At'),
                                  '9876543210')
                self.assertTrue('X-Delete-At-Host' in given_headers)
                self.assertTrue('X-Delete-At-Device' in given_headers)
                self.assertTrue('X-Delete-At-Partition' in given_headers)
                self.assertTrue('X-Delete-At-Container' in given_headers)

    def test_chunked_put(self):

        class ChunkedFile(object):

            def __init__(self, bytes):
                self.bytes = bytes
                self.read_bytes = 0

            @property
            def bytes_left(self):
                return self.bytes - self.read_bytes

            def read(self, amt=None):
                if self.read_bytes >= self.bytes:
                    raise StopIteration()
                if not amt:
                    amt = self.bytes_left
                data = 'a' * min(amt, self.bytes_left)
                self.read_bytes += len(data)
                return data

        with save_globals():
            set_http_connect(201, 201, 201, 201)
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'COPY'},
                                headers={'Transfer-Encoding': 'chunked',
                                         'Content-Type': 'foo/bar'})

            req.body_file = ChunkedFile(10)
            self.app.memcache.store = {}
            self.app.update_request(req)
            res = controller.PUT(req)
            self.assertEquals(res.status_int // 100, 2)  # success

            # test 413 entity to large
            set_http_connect(201, 201, 201, 201)
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'COPY'},
                                headers={'Transfer-Encoding': 'chunked',
                                'Content-Type': 'foo/bar'})
            req.body_file = ChunkedFile(11)
            self.app.memcache.store = {}
            self.app.update_request(req)

            with mock.patch('swift.common.constraints.MAX_FILE_SIZE', 10):
                res = controller.PUT(req)
                self.assertEquals(res.status_int, 413)

    @unpatch_policies
    def test_chunked_put_bad_version(self):
        # Check bad version
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis,
         obj2lis, obj3lis) = _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v0 HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nContent-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 412'
        self.assertEquals(headers[:len(exp)], exp)

    @unpatch_policies
    def test_chunked_put_bad_path(self):
        # Check bad path
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis,
         obj2lis, obj3lis) = _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET invalid HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nContent-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 404'
        self.assertEquals(headers[:len(exp)], exp)

    @unpatch_policies
    def test_chunked_put_bad_utf8(self):
        # Check invalid utf-8
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis,
         obj2lis, obj3lis) = _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a%80 HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Auth-Token: t\r\n'
                 'Content-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 412'
        self.assertEquals(headers[:len(exp)], exp)

    @unpatch_policies
    def test_chunked_put_bad_path_no_controller(self):
        # Check bad path, no controller
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis,
         obj2lis, obj3lis) = _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1 HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Auth-Token: t\r\n'
                 'Content-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 412'
        self.assertEquals(headers[:len(exp)], exp)

    @unpatch_policies
    def test_chunked_put_bad_method(self):
        # Check bad method
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis,
         obj2lis, obj3lis) = _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('LICK /v1/a HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Auth-Token: t\r\n'
                 'Content-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 405'
        self.assertEquals(headers[:len(exp)], exp)

    @unpatch_policies
    def test_chunked_put_unhandled_exception(self):
        # Check unhandled exception
        (prosrv, acc1srv, acc2srv, con1srv, con2srv, obj1srv,
         obj2srv, obj3srv) = _test_servers
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis,
         obj2lis, obj3lis) = _test_sockets
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
        self.assertEquals(headers[:len(exp)], exp)
        prosrv.update_request = orig_update_request

    @unpatch_policies
    def test_chunked_put_head_account(self):
        # Head account, just a double check and really is here to test
        # the part Application.log_request that 'enforces' a
        # content_length on the response.
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis,
         obj2lis, obj3lis) = _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('HEAD /v1/a HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Auth-Token: t\r\n'
                 'Content-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 204'
        self.assertEquals(headers[:len(exp)], exp)
        self.assert_('\r\nContent-Length: 0\r\n' in headers)

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
         obj2lis, obj3lis) = _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%s HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n'
                 'Content-Length: 0\r\n\r\n' % quote(ustr))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        # List account with ustr container (test plain)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n'
                 'Content-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        containers = fd.read().split('\n')
        self.assert_(ustr in containers)
        # List account with ustr container (test json)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a?format=json HTTP/1.1\r\n'
                 'Host: localhost\r\nConnection: close\r\n'
                 'X-Storage-Token: t\r\nContent-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        listing = json.loads(fd.read())
        self.assert_(ustr.decode('utf8') in [l['name'] for l in listing])
        # List account with ustr container (test xml)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a?format=xml HTTP/1.1\r\n'
                 'Host: localhost\r\nConnection: close\r\n'
                 'X-Storage-Token: t\r\nContent-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        self.assert_('<name>%s</name>' % ustr in fd.read())
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
        self.assertEquals(headers[:len(exp)], exp)
        # List ustr container with ustr object (test plain)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/%s HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n'
                 'Content-Length: 0\r\n\r\n' % quote(ustr))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        objects = fd.read().split('\n')
        self.assert_(ustr in objects)
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
        self.assertEquals(headers[:len(exp)], exp)
        listing = json.loads(fd.read())
        self.assertEquals(listing[0]['name'], ustr.decode('utf8'))
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
        self.assertEquals(headers[:len(exp)], exp)
        self.assert_('<name>%s</name>' % ustr in fd.read())
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
        self.assertEquals(headers[:len(exp)], exp)
        self.assert_('\r\nX-Object-Meta-%s: %s\r\n' %
                     (quote(ustr_short).lower(), quote(ustr)) in headers)

    @unpatch_policies
    def test_chunked_put_chunked_put(self):
        # Do chunked object put
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis,
         obj2lis, obj3lis) = _test_sockets
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
        self.assertEquals(headers[:len(exp)], exp)
        # Ensure we get what we put
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/c/o/chunky HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Auth-Token: t\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        body = fd.read()
        self.assertEquals(body, 'oh hai123456789abcdef')

    @unpatch_policies
    def test_version_manifest(self, oc='versions', vc='vers', o='name'):
        versions_to_create = 3
        # Create a container for our versioned object testing
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis,
         obj2lis, obj3lis) = _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        pre = quote('%03x' % len(o))
        osub = '%s/sub' % o
        presub = quote('%03x' % len(osub))
        osub = quote(osub)
        presub = quote(presub)
        oc = quote(oc)
        vc = quote(vc)
        fd.write('PUT /v1/a/%s HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n'
                 'Content-Length: 0\r\nX-Versions-Location: %s\r\n\r\n'
                 % (oc, vc))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        # check that the header was set
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/%s HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n\r\n\r\n' % oc)
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 2'  # 2xx series response
        self.assertEquals(headers[:len(exp)], exp)
        self.assert_('X-Versions-Location: %s' % vc in headers)
        # make the container for the object versions
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%s HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n'
                 'Content-Length: 0\r\n\r\n' % vc)
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        # Create the versioned file
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%s/%s HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Storage-Token: '
                 't\r\nContent-Length: 5\r\nContent-Type: text/jibberish0\r\n'
                 'X-Object-Meta-Foo: barbaz\r\n\r\n00000\r\n' % (oc, o))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        # Create the object versions
        for segment in xrange(1, versions_to_create):
            sleep(.01)  # guarantee that the timestamp changes
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('PUT /v1/a/%s/%s HTTP/1.1\r\nHost: '
                     'localhost\r\nConnection: close\r\nX-Storage-Token: '
                     't\r\nContent-Length: 5\r\nContent-Type: text/jibberish%s'
                     '\r\n\r\n%05d\r\n' % (oc, o, segment, segment))
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = 'HTTP/1.1 201'
            self.assertEquals(headers[:len(exp)], exp)
            # Ensure retrieving the manifest file gets the latest version
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('GET /v1/a/%s/%s HTTP/1.1\r\nHost: '
                     'localhost\r\nConnection: close\r\nX-Auth-Token: t\r\n'
                     '\r\n' % (oc, o))
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = 'HTTP/1.1 200'
            self.assertEquals(headers[:len(exp)], exp)
            self.assert_('Content-Type: text/jibberish%s' % segment in headers)
            self.assert_('X-Object-Meta-Foo: barbaz' not in headers)
            body = fd.read()
            self.assertEquals(body, '%05d' % segment)
        # Ensure we have the right number of versions saved
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/%s?prefix=%s%s/ HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Auth-Token: t\r\n\r\n'
                 % (vc, pre, o))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        body = fd.read()
        versions = [x for x in body.split('\n') if x]
        self.assertEquals(len(versions), versions_to_create - 1)
        # copy a version and make sure the version info is stripped
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('COPY /v1/a/%s/%s HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Auth-Token: '
                 't\r\nDestination: %s/copied_name\r\n'
                 'Content-Length: 0\r\n\r\n' % (oc, o, oc))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 2'  # 2xx series response to the COPY
        self.assertEquals(headers[:len(exp)], exp)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/%s/copied_name HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Auth-Token: t\r\n\r\n'
                 % oc)
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        body = fd.read()
        self.assertEquals(body, '%05d' % segment)
        # post and make sure it's updated
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('POST /v1/a/%s/%s HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Auth-Token: '
                 't\r\nContent-Type: foo/bar\r\nContent-Length: 0\r\n'
                 'X-Object-Meta-Bar: foo\r\n\r\n' % (oc, o))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 2'  # 2xx series response to the POST
        self.assertEquals(headers[:len(exp)], exp)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/%s/%s HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Auth-Token: t\r\n\r\n'
                 % (oc, o))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        self.assert_('Content-Type: foo/bar' in headers)
        self.assert_('X-Object-Meta-Bar: foo' in headers)
        body = fd.read()
        self.assertEquals(body, '%05d' % segment)
        # Delete the object versions
        for segment in xrange(versions_to_create - 1, 0, -1):
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('DELETE /v1/a/%s/%s HTTP/1.1\r\nHost: localhost\r'
                     '\nConnection: close\r\nX-Storage-Token: t\r\n\r\n'
                     % (oc, o))
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = 'HTTP/1.1 2'  # 2xx series response
            self.assertEquals(headers[:len(exp)], exp)
            # Ensure retrieving the manifest file gets the latest version
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('GET /v1/a/%s/%s HTTP/1.1\r\nHost: localhost\r\n'
                     'Connection: close\r\nX-Auth-Token: t\r\n\r\n'
                     % (oc, o))
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = 'HTTP/1.1 200'
            self.assertEquals(headers[:len(exp)], exp)
            self.assert_('Content-Type: text/jibberish%s' % (segment - 1)
                         in headers)
            body = fd.read()
            self.assertEquals(body, '%05d' % (segment - 1))
            # Ensure we have the right number of versions saved
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('GET /v1/a/%s?prefix=%s%s/ HTTP/1.1\r\nHost: '
                     'localhost\r\nConnection: close\r\nX-Auth-Token: t\r\n\r'
                     '\n' % (vc, pre, o))
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = 'HTTP/1.1 2'  # 2xx series response
            self.assertEquals(headers[:len(exp)], exp)
            body = fd.read()
            versions = [x for x in body.split('\n') if x]
            self.assertEquals(len(versions), segment - 1)
        # there is now one segment left (in the manifest)
        # Ensure we have no saved versions
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/%s?prefix=%s%s/ HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Auth-Token: t\r\n\r\n'
                 % (vc, pre, o))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 204 No Content'
        self.assertEquals(headers[:len(exp)], exp)
        # delete the last verision
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('DELETE /v1/a/%s/%s HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n\r\n' % (oc, o))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 2'  # 2xx series response
        self.assertEquals(headers[:len(exp)], exp)
        # Ensure it's all gone
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/%s/%s HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Auth-Token: t\r\n\r\n'
                 % (oc, o))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 404'
        self.assertEquals(headers[:len(exp)], exp)

        # make sure dlo manifest files don't get versioned
        for _junk in xrange(1, versions_to_create):
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
            self.assertEquals(headers[:len(exp)], exp)

        # Ensure we have no saved versions
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/%s?prefix=%s%s/ HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Auth-Token: t\r\n\r\n'
                 % (vc, pre, o))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 204 No Content'
        self.assertEquals(headers[:len(exp)], exp)

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
        self.assertEquals(headers[:len(exp)], exp)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%s/%s HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Storage-Token: '
                 't\r\nContent-Length: 5\r\nContent-Type: text/jibberish0\r\n'
                 'Foo: barbaz\r\n\r\n00001\r\n' % (oc, o))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%s/%s HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Storage-Token: '
                 't\r\nContent-Length: 4\r\nContent-Type: text/jibberish0\r\n'
                 'Foo: barbaz\r\n\r\nsub1\r\n' % (oc, osub))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%s/%s HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Storage-Token: '
                 't\r\nContent-Length: 4\r\nContent-Type: text/jibberish0\r\n'
                 'Foo: barbaz\r\n\r\nsub2\r\n' % (oc, osub))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('DELETE /v1/a/%s/%s HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n\r\n' % (oc, o))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 2'  # 2xx series response
        self.assertEquals(headers[:len(exp)], exp)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/%s?prefix=%s%s/ HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Auth-Token: t\r\n\r\n'
                 % (vc, presub, osub))
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 2'  # 2xx series response
        self.assertEquals(headers[:len(exp)], exp)
        body = fd.read()
        versions = [x for x in body.split('\n') if x]
        self.assertEquals(len(versions), 1)

        # Check for when the versions target container doesn't exist
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%swhoops HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n'
                 'Content-Length: 0\r\nX-Versions-Location: none\r\n\r\n' % oc)
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        # Create the versioned file
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%swhoops/foo HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Storage-Token: '
                 't\r\nContent-Length: 5\r\n\r\n00000\r\n' % oc)
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        # Create another version
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/%swhoops/foo HTTP/1.1\r\nHost: '
                 'localhost\r\nConnection: close\r\nX-Storage-Token: '
                 't\r\nContent-Length: 5\r\n\r\n00001\r\n' % oc)
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 412'
        self.assertEquals(headers[:len(exp)], exp)
        # Delete the object
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('DELETE /v1/a/%swhoops/foo HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n\r\n' % oc)
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 2'  # 2xx response
        self.assertEquals(headers[:len(exp)], exp)

    @unpatch_policies
    def test_version_manifest_utf8(self):
        oc = '0_oc_non_ascii\xc2\xa3'
        vc = '0_vc_non_ascii\xc2\xa3'
        o = '0_o_non_ascii\xc2\xa3'
        self.test_version_manifest(oc, vc, o)

    @unpatch_policies
    def test_version_manifest_utf8_container(self):
        oc = '1_oc_non_ascii\xc2\xa3'
        vc = '1_vc_ascii'
        o = '1_o_ascii'
        self.test_version_manifest(oc, vc, o)

    @unpatch_policies
    def test_version_manifest_utf8_version_container(self):
        oc = '2_oc_ascii'
        vc = '2_vc_non_ascii\xc2\xa3'
        o = '2_o_ascii'
        self.test_version_manifest(oc, vc, o)

    @unpatch_policies
    def test_version_manifest_utf8_containers(self):
        oc = '3_oc_non_ascii\xc2\xa3'
        vc = '3_vc_non_ascii\xc2\xa3'
        o = '3_o_ascii'
        self.test_version_manifest(oc, vc, o)

    @unpatch_policies
    def test_version_manifest_utf8_object(self):
        oc = '4_oc_ascii'
        vc = '4_vc_ascii'
        o = '4_o_non_ascii\xc2\xa3'
        self.test_version_manifest(oc, vc, o)

    @unpatch_policies
    def test_version_manifest_utf8_version_container_utf_object(self):
        oc = '5_oc_ascii'
        vc = '5_vc_non_ascii\xc2\xa3'
        o = '5_o_non_ascii\xc2\xa3'
        self.test_version_manifest(oc, vc, o)

    @unpatch_policies
    def test_version_manifest_utf8_container_utf_object(self):
        oc = '6_oc_non_ascii\xc2\xa3'
        vc = '6_vc_ascii'
        o = '6_o_non_ascii\xc2\xa3'
        self.test_version_manifest(oc, vc, o)

    @unpatch_policies
    def test_conditional_range_get(self):
        (prolis, acc1lis, acc2lis, con1lis, con2lis, obj1lis, obj2lis,
         obj3lis) = _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))

        # make a container
        fd = sock.makefile()
        fd.write('PUT /v1/a/con HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n'
                 'Content-Length: 0\r\n\r\n')
        fd.flush()
        exp = 'HTTP/1.1 201'
        headers = readuntil2crlfs(fd)
        self.assertEquals(headers[:len(exp)], exp)

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
        self.assertEquals(headers[:len(exp)], exp)

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
        self.assertEquals(headers[:len(exp)], exp)

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
            self.assertEquals(resp.status_int // 100, 5)  # server error

            # req supplies etag, object servers return 422 - mismatch
            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={
                                    'Content-Length': '0',
                                    'ETag': '68b329da9893e34099c7d8ad5cb9c940',
                                })
            self.app.update_request(req)
            set_http_connect(200, 422, 422, 503,
                             etags=['68b329da9893e34099c7d8ad5cb9c940',
                                    '68b329da9893e34099c7d8ad5cb9c941',
                                    None,
                                    None])
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int // 100, 4)  # client error

    def test_response_get_accept_ranges_header(self):
        with save_globals():
            req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'GET'})
            self.app.update_request(req)
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            set_http_connect(200, 200, 200)
            resp = controller.GET(req)
            self.assert_('accept-ranges' in resp.headers)
            self.assertEquals(resp.headers['accept-ranges'], 'bytes')

    def test_response_head_accept_ranges_header(self):
        with save_globals():
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'HEAD'})
            self.app.update_request(req)
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            set_http_connect(200, 200, 200)
            resp = controller.HEAD(req)
            self.assert_('accept-ranges' in resp.headers)
            self.assertEquals(resp.headers['accept-ranges'], 'bytes')

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
        self.assert_(called[0])

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
        self.assert_(called[0])

    def test_POST_calls_authorize(self):
        called = [False]

        def authorize(req):
            called[0] = True
            return HTTPUnauthorized(request=req)
        with save_globals():
            self.app.object_post_as_copy = False
            set_http_connect(200, 200, 201, 201, 201)
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'POST'},
                                headers={'Content-Length': '5'}, body='12345')
            req.environ['swift.authorize'] = authorize
            self.app.update_request(req)
            controller.POST(req)
        self.assert_(called[0])

    def test_POST_as_copy_calls_authorize(self):
        called = [False]

        def authorize(req):
            called[0] = True
            return HTTPUnauthorized(request=req)
        with save_globals():
            set_http_connect(200, 200, 200, 200, 200, 201, 201, 201)
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'POST'},
                                headers={'Content-Length': '5'}, body='12345')
            req.environ['swift.authorize'] = authorize
            self.app.update_request(req)
            controller.POST(req)
        self.assert_(called[0])

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
        self.assert_(called[0])

    def test_COPY_calls_authorize(self):
        called = [False]

        def authorize(req):
            called[0] = True
            return HTTPUnauthorized(request=req)
        with save_globals():
            set_http_connect(200, 200, 200, 200, 200, 201, 201, 201)
            controller = ReplicatedObjectController(
                self.app, 'account', 'container', 'object')
            req = Request.blank('/v1/a/c/o',
                                environ={'REQUEST_METHOD': 'COPY'},
                                headers={'Destination': 'c/o'})
            req.environ['swift.authorize'] = authorize
            self.app.update_request(req)
            controller.COPY(req)
        self.assert_(called[0])

    def test_POST_converts_delete_after_to_delete_at(self):
        with save_globals():
            self.app.object_post_as_copy = False
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
                self.assertEquals(res.status, '202 Fake')
                self.assertEquals(req.headers.get('x-delete-at'),
                                  str(int(t + 60)))
            finally:
                time.time = orig_time

    @patch_policies([
        StoragePolicy(0, 'zero', False, object_ring=FakeRing()),
        StoragePolicy(1, 'one', True, object_ring=FakeRing())
    ])
    def test_PUT_versioning_with_nonzero_default_policy(self):
        # reset the router post patch_policies
        self.app.obj_controller_router = proxy_server.ObjectControllerRouter()

        def test_connect(ipaddr, port, device, partition, method, path,
                         headers=None, query_string=None):
            if method == "HEAD":
                self.assertEquals(path, '/a/c/o.jpg')
                self.assertNotEquals(None,
                                     headers['X-Backend-Storage-Policy-Index'])
                self.assertEquals(1, int(headers
                                         ['X-Backend-Storage-Policy-Index']))

        def fake_container_info(account, container, req):
            return {'status': 200, 'sync_key': None, 'storage_policy': '1',
                    'meta': {}, 'cors': {'allow_origin': None,
                                         'expose_headers': None,
                                         'max_age': None},
                    'sysmeta': {}, 'read_acl': None, 'object_count': None,
                    'write_acl': None, 'versions': 'c-versions',
                    'partition': 1, 'bytes': None,
                    'nodes': [{'zone': 0, 'ip': '10.0.0.0', 'region': 0,
                               'id': 0, 'device': 'sda', 'port': 1000},
                              {'zone': 1, 'ip': '10.0.0.1', 'region': 1,
                               'id': 1, 'device': 'sdb', 'port': 1001},
                              {'zone': 2, 'ip': '10.0.0.2', 'region': 0,
                               'id': 2, 'device': 'sdc', 'port': 1002}]}
        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'a', 'c', 'o.jpg')

            controller.container_info = fake_container_info
            set_http_connect(200, 200, 200,  # head: for the last version
                             200, 200, 200,  # get: for the last version
                             201, 201, 201,  # put: move the current version
                             201, 201, 201,  # put: save the new version
                             give_connect=test_connect)
            req = Request.blank('/v1/a/c/o.jpg',
                                environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '0'})
            self.app.update_request(req)
            self.app.memcache.store = {}
            res = controller.PUT(req)
            self.assertEquals(201, res.status_int)

    @patch_policies([
        StoragePolicy(0, 'zero', False, object_ring=FakeRing()),
        StoragePolicy(1, 'one', True, object_ring=FakeRing())
    ])
    def test_cross_policy_DELETE_versioning(self):
        # reset the router post patch_policies
        self.app.obj_controller_router = proxy_server.ObjectControllerRouter()
        requests = []

        def capture_requests(ipaddr, port, device, partition, method, path,
                             headers=None, query_string=None):
            requests.append((method, path, headers))

        def fake_container_info(app, env, account, container, **kwargs):
            info = {'status': 200, 'sync_key': None, 'storage_policy': None,
                    'meta': {}, 'cors': {'allow_origin': None,
                                         'expose_headers': None,
                                         'max_age': None},
                    'sysmeta': {}, 'read_acl': None, 'object_count': None,
                    'write_acl': None, 'versions': None,
                    'partition': 1, 'bytes': None,
                    'nodes': [{'zone': 0, 'ip': '10.0.0.0', 'region': 0,
                               'id': 0, 'device': 'sda', 'port': 1000},
                              {'zone': 1, 'ip': '10.0.0.1', 'region': 1,
                               'id': 1, 'device': 'sdb', 'port': 1001},
                              {'zone': 2, 'ip': '10.0.0.2', 'region': 0,
                               'id': 2, 'device': 'sdc', 'port': 1002}]}
            if container == 'c':
                info['storage_policy'] = '1'
                info['versions'] = 'c-versions'
            elif container == 'c-versions':
                info['storage_policy'] = '0'
            else:
                self.fail('Unexpected call to get_info for %r' % container)
            return info
        container_listing = json.dumps([{'name': 'old_version'}])
        with save_globals():
            resp_status = (
                200, 200,  # listings for versions container
                200, 200, 200,  # get: for the last version
                201, 201, 201,  # put: move the last version
                200, 200, 200,  # delete: for the last version
            )
            body_iter = iter([container_listing] + [
                '' for x in range(len(resp_status) - 1)])
            set_http_connect(*resp_status, body_iter=body_iter,
                             give_connect=capture_requests)
            req = Request.blank('/v1/a/c/current_version', method='DELETE')
            self.app.update_request(req)
            self.app.memcache.store = {}
            with mock.patch('swift.proxy.controllers.base.get_info',
                            fake_container_info):
                resp = self.app.handle_request(req)
            self.assertEquals(200, resp.status_int)
            expected = [('GET', '/a/c-versions')] * 2 + \
                [('GET', '/a/c-versions/old_version')] * 3 + \
                [('PUT', '/a/c/current_version')] * 3 + \
                [('DELETE', '/a/c-versions/old_version')] * 3
            self.assertEqual(expected, [(m, p) for m, p, h in requests])
            for method, path, headers in requests:
                if 'current_version' in path:
                    expected_storage_policy = 1
                elif 'old_version' in path:
                    expected_storage_policy = 0
                else:
                    continue
                storage_policy_index = \
                    int(headers['X-Backend-Storage-Policy-Index'])
                self.assertEqual(
                    expected_storage_policy, storage_policy_index,
                    'Unexpected %s request for %s '
                    'with storage policy index %s' % (
                        method, path, storage_policy_index))

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
            for i in xrange(4):
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
            fd.close()
            sock.close()
            # Make sure the GC is run again for pythons without reference
            # counting
            for i in xrange(4):
                sleep(0)  # let eventlet do its thing
                gc.collect()
            else:
                sleep(0)
            self.assertEquals(
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
            self.assertEquals(401, resp.status_int)

            def my_empty_origin_container_info(*args):
                return {'cors': {'allow_origin': None}}
            controller.container_info = my_empty_origin_container_info
            req = Request.blank(
                '/v1/a/c/o.jpg',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'http://foo.com',
                         'Access-Control-Request-Method': 'GET'})
            resp = controller.OPTIONS(req)
            self.assertEquals(401, resp.status_int)

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
            self.assertEquals(200, resp.status_int)
            self.assertEquals(
                'https://foo.bar',
                resp.headers['access-control-allow-origin'])
            for verb in 'OPTIONS COPY GET POST PUT DELETE HEAD'.split():
                self.assertTrue(
                    verb in resp.headers['access-control-allow-methods'])
            self.assertEquals(
                len(resp.headers['access-control-allow-methods'].split(', ')),
                7)
            self.assertEquals('999', resp.headers['access-control-max-age'])
            req = Request.blank(
                '/v1/a/c/o.jpg',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'https://foo.bar'})
            req.content_length = 0
            resp = controller.OPTIONS(req)
            self.assertEquals(401, resp.status_int)
            req = Request.blank('/v1/a/c/o.jpg', {'REQUEST_METHOD': 'OPTIONS'})
            req.content_length = 0
            resp = controller.OPTIONS(req)
            self.assertEquals(200, resp.status_int)
            for verb in 'OPTIONS COPY GET POST PUT DELETE HEAD'.split():
                self.assertTrue(
                    verb in resp.headers['Allow'])
            self.assertEquals(len(resp.headers['Allow'].split(', ')), 7)
            req = Request.blank(
                '/v1/a/c/o.jpg',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'http://foo.com'})
            resp = controller.OPTIONS(req)
            self.assertEquals(401, resp.status_int)
            req = Request.blank(
                '/v1/a/c/o.jpg',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'http://foo.bar',
                         'Access-Control-Request-Method': 'GET'})
            controller.app.cors_allow_origin = ['http://foo.bar', ]
            resp = controller.OPTIONS(req)
            self.assertEquals(200, resp.status_int)

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
            self.assertEquals(200, resp.status_int)
            self.assertEquals('*', resp.headers['access-control-allow-origin'])
            for verb in 'OPTIONS COPY GET POST PUT DELETE HEAD'.split():
                self.assertTrue(
                    verb in resp.headers['access-control-allow-methods'])
            self.assertEquals(
                len(resp.headers['access-control-allow-methods'].split(', ')),
                7)
            self.assertEquals('999', resp.headers['access-control-max-age'])

    def test_CORS_valid(self):
        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'a', 'c', 'o')

            def stubContainerInfo(*args):
                return {
                    'cors': {
                        'allow_origin': 'http://not.foo.bar'
                    }
                }
            controller.container_info = stubContainerInfo
            controller.app.strict_cors_mode = False

            def objectGET(controller, req):
                return Response(headers={
                    'X-Object-Meta-Color': 'red',
                    'X-Super-Secret': 'hush',
                })

            req = Request.blank(
                '/v1/a/c/o.jpg',
                {'REQUEST_METHOD': 'GET'},
                headers={'Origin': 'http://foo.bar'})

            resp = cors_validation(objectGET)(controller, req)

            self.assertEquals(200, resp.status_int)
            self.assertEquals('http://foo.bar',
                              resp.headers['access-control-allow-origin'])
            self.assertEquals('red', resp.headers['x-object-meta-color'])
            # X-Super-Secret is in the response, but not "exposed"
            self.assertEquals('hush', resp.headers['x-super-secret'])
            self.assertTrue('access-control-expose-headers' in resp.headers)
            exposed = set(
                h.strip() for h in
                resp.headers['access-control-expose-headers'].split(','))
            expected_exposed = set(['cache-control', 'content-language',
                                    'content-type', 'expires', 'last-modified',
                                    'pragma', 'etag', 'x-timestamp',
                                    'x-trans-id', 'x-object-meta-color'])
            self.assertEquals(expected_exposed, exposed)

            controller.app.strict_cors_mode = True
            req = Request.blank(
                '/v1/a/c/o.jpg',
                {'REQUEST_METHOD': 'GET'},
                headers={'Origin': 'http://foo.bar'})

            resp = cors_validation(objectGET)(controller, req)

            self.assertEquals(200, resp.status_int)
            self.assertTrue('access-control-allow-origin' not in resp.headers)

    def test_CORS_valid_with_obj_headers(self):
        with save_globals():
            controller = ReplicatedObjectController(
                self.app, 'a', 'c', 'o')

            def stubContainerInfo(*args):
                return {
                    'cors': {
                        'allow_origin': 'http://foo.bar'
                    }
                }
            controller.container_info = stubContainerInfo

            def objectGET(controller, req):
                return Response(headers={
                    'X-Object-Meta-Color': 'red',
                    'X-Super-Secret': 'hush',
                    'Access-Control-Allow-Origin': 'http://obj.origin',
                    'Access-Control-Expose-Headers': 'x-trans-id'
                })

            req = Request.blank(
                '/v1/a/c/o.jpg',
                {'REQUEST_METHOD': 'GET'},
                headers={'Origin': 'http://foo.bar'})

            resp = cors_validation(objectGET)(controller, req)

            self.assertEquals(200, resp.status_int)
            self.assertEquals('http://obj.origin',
                              resp.headers['access-control-allow-origin'])
            self.assertEquals('x-trans-id',
                              resp.headers['access-control-expose-headers'])

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
        self.app.object_post_as_copy = False

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
             'X-Delete-At-Container': None,
             'X-Delete-At-Partition': None,
             'X-Delete-At-Device': None}
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


class TestECMismatchedFA(unittest.TestCase):
    def tearDown(self):
        prosrv = _test_servers[0]
        # don't leak error limits and poison other tests
        prosrv._error_limiting = {}

    def test_mixing_different_objects_fragment_archives(self):
        (prosrv, acc1srv, acc2srv, con1srv, con2srv, obj1srv,
         obj2srv, obj3srv) = _test_servers
        ec_policy = POLICIES[3]

        @public
        def bad_disk(req):
            return Response(status=507, body="borken")

        ensure_container = Request.blank(
            "/v1/a/ec-crazytown",
            environ={"REQUEST_METHOD": "PUT"},
            headers={"X-Storage-Policy": "ec", "X-Auth-Token": "t"})
        resp = ensure_container.get_response(prosrv)
        self.assertTrue(resp.status_int in (201, 202))

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

        # Servers obj1 and obj2 will have the first version of the object
        prosrv._error_limiting = {}
        with nested(
                mock.patch.object(obj3srv, 'PUT', bad_disk),
                mock.patch(
                    'swift.common.storage_policy.ECStoragePolicy.quorum')):
            type(ec_policy).quorum = mock.PropertyMock(return_value=2)
            resp = put_req1.get_response(prosrv)
        self.assertEqual(resp.status_int, 201)

        # Server obj3 (and, in real life, some handoffs) will have the
        # second version of the object.
        prosrv._error_limiting = {}
        with nested(
                mock.patch.object(obj1srv, 'PUT', bad_disk),
                mock.patch.object(obj2srv, 'PUT', bad_disk),
                mock.patch(
                    'swift.common.storage_policy.ECStoragePolicy.quorum'),
                mock.patch(
                    'swift.proxy.controllers.base.Controller._quorum_size',
                    lambda *a, **kw: 1)):
            type(ec_policy).quorum = mock.PropertyMock(return_value=1)
            resp = put_req2.get_response(prosrv)
        self.assertEqual(resp.status_int, 201)

        # A GET that only sees 1 fragment archive should fail
        get_req = Request.blank("/v1/a/ec-crazytown/obj",
                                environ={"REQUEST_METHOD": "GET"},
                                headers={"X-Auth-Token": "t"})
        prosrv._error_limiting = {}
        with nested(
                mock.patch.object(obj1srv, 'GET', bad_disk),
                mock.patch.object(obj2srv, 'GET', bad_disk)):
            resp = get_req.get_response(prosrv)
        self.assertEqual(resp.status_int, 503)

        # A GET that sees 2 matching FAs will work
        get_req = Request.blank("/v1/a/ec-crazytown/obj",
                                environ={"REQUEST_METHOD": "GET"},
                                headers={"X-Auth-Token": "t"})
        prosrv._error_limiting = {}
        with mock.patch.object(obj3srv, 'GET', bad_disk):
            resp = get_req.get_response(prosrv)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.body, obj1)

        # A GET that sees 2 mismatching FAs will fail
        get_req = Request.blank("/v1/a/ec-crazytown/obj",
                                environ={"REQUEST_METHOD": "GET"},
                                headers={"X-Auth-Token": "t"})
        prosrv._error_limiting = {}
        with mock.patch.object(obj2srv, 'GET', bad_disk):
            resp = get_req.get_response(prosrv)
        self.assertEqual(resp.status_int, 503)


class TestObjectECRangedGET(unittest.TestCase):
    def setUp(self):
        self.app = proxy_server.Application(
            None, FakeMemcache(),
            logger=debug_logger('proxy-ut'),
            account_ring=FakeRing(),
            container_ring=FakeRing())

    @classmethod
    def setUpClass(cls):
        cls.obj_name = 'range-get-test'
        cls.tiny_obj_name = 'range-get-test-tiny'
        cls.aligned_obj_name = 'range-get-test-aligned'

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

        cls.tiny_obj = 'tiny, tiny object'
        assert len(cls.tiny_obj) < seg_size, "tiny_obj too large"

        cls.aligned_obj = "".join(
            "abcdEFGHijkl%04d" % x for x in range(512))
        assert len(cls.aligned_obj) % seg_size == 0, "aligned obj not aligned"

        for obj_name, obj in ((cls.obj_name, cls.obj),
                              (cls.tiny_obj_name, cls.tiny_obj),
                              (cls.aligned_obj_name, cls.aligned_obj)):
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('PUT /v1/a/ec-con/%s HTTP/1.1\r\n'
                     'Host: localhost\r\n'
                     'Connection: close\r\n'
                     'Content-Length: %d\r\n'
                     'X-Storage-Token: t\r\n'
                     'Content-Type: application/octet-stream\r\n'
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

        return (status_code, headers, gotten_obj)

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
        # unsatisfiable
        status, _junk, _junk = self._get_obj(
            "bytes=%d-%d" % (len(self.obj), len(self.obj) + 100))
        self.assertEqual(status, 416)

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
        self.assertEqual(controller._convert_policy_to_index(req), None)
        # negative test
        req = Request.blank('/a/c', headers={'Content-Length': '0',
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
        self.assertEqual(resp.headers['X-Storage-Policy'], None)

    def test_error_convert_index_to_name(self):
        req = Request.blank('/v1/a/c')
        with mocked_http_conn(
                200, 200,
                headers={'X-Backend-Storage-Policy-Index': '-1'}) as fake_conn:
            resp = req.get_response(self.app)
            self.assertRaises(StopIteration, fake_conn.code_iter.next)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['X-Storage-Policy'], None)
        error_lines = self.app.logger.get_lines_for_level('error')
        self.assertEqual(2, len(error_lines))
        for msg in error_lines:
            expected = "Could not translate " \
                "X-Backend-Storage-Policy-Index ('-1')"
            self.assertTrue(expected in msg)

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
            kwargs = {}
            if raise_exc:
                kwargs['raise_exc'] = raise_exc
            kwargs['missing_container'] = missing_container
            set_http_connect(*statuses, **kwargs)
            self.app.memcache.store = {}
            req = Request.blank('/v1/a/c', headers={'Content-Length': '0',
                                'Content-Type': 'text/plain'})
            self.app.update_request(req)
            res = method(req)
            self.assertEquals(res.status_int, expected)
            set_http_connect(*statuses, **kwargs)
            self.app.memcache.store = {}
            req = Request.blank('/v1/a/c/', headers={'Content-Length': '0',
                                'Content-Type': 'text/plain'})
            self.app.update_request(req)
            res = method(req)
            self.assertEquals(res.status_int, expected)

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
                self.assertEquals(res.status[:len(str(expected))],
                                  str(expected))
                if expected < 400:
                    self.assert_('x-works' in res.headers)
                    self.assertEquals(res.headers['x-works'], 'yes')
                if c_expected:
                    self.assertTrue('swift.container/a/c' in res.environ)
                    self.assertEquals(
                        res.environ['swift.container/a/c']['status'],
                        c_expected)
                else:
                    self.assertTrue('swift.container/a/c' not in res.environ)
                if a_expected:
                    self.assertTrue('swift.account/a' in res.environ)
                    self.assertEquals(res.environ['swift.account/a']['status'],
                                      a_expected)
                else:
                    self.assertTrue('swift.account/a' not in res.environ)

                set_http_connect(*statuses, **kwargs)
                self.app.memcache.store = {}
                req = Request.blank('/v1/a/c', {})
                self.app.update_request(req)
                res = controller.GET(req)
                self.assertEquals(res.status[:len(str(expected))],
                                  str(expected))
                if expected < 400:
                    self.assert_('x-works' in res.headers)
                    self.assertEquals(res.headers['x-works'], 'yes')
                if c_expected:
                    self.assertTrue('swift.container/a/c' in res.environ)
                    self.assertEquals(
                        res.environ['swift.container/a/c']['status'],
                        c_expected)
                else:
                    self.assertTrue('swift.container/a/c' not in res.environ)
                if a_expected:
                    self.assertTrue('swift.account/a' in res.environ)
                    self.assertEquals(res.environ['swift.account/a']['status'],
                                      a_expected)
                else:
                    self.assertTrue('swift.account/a' not in res.environ)
            # In all the following tests cache 200 for account
            # return and ache vary for container
            # return 200 and cache 200 for and container
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

            # In all the following tests cache 404 for account
            # return 404 (as account is not found) and don't cache container
            test_status_map((404, 404, 404), 404, None, 404)
            # This should make no difference
            self.app.account_autocreate = True
            test_status_map((404, 404, 404), 404, None, 404)

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
                    self.assertEquals(res.status_int, 400)
                    self.assertEqual(0, len(backend_requests))
                    expected = 'is deprecated'
                    self.assertTrue(expected in res.body,
                                    '%r did not include %r' % (
                                        res.body, expected))
                    return
                self.assertEquals(res.status_int, 201)
                self.assertEqual(
                    expected_policy.object_ring.replicas,
                    len(backend_requests))
                for headers in backend_requests:
                    if not requested_policy:
                        self.assertFalse('X-Backend-Storage-Policy-Index' in
                                         headers)
                        self.assertTrue(
                            'X-Backend-Storage-Policy-Default' in headers)
                        self.assertEqual(
                            int(expected_policy),
                            int(headers['X-Backend-Storage-Policy-Default']))
                    else:
                        self.assertTrue('X-Backend-Storage-Policy-Index' in
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
                self.assertEquals(res.status[:len(expected)], expected)

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
                404, missing_container=True)
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
                404, missing_container=True)
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
                self.assertEquals(res.status[:len(expected)], expected)

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
                self.assertTrue(key in call['headers'],
                                '%s call, key %s missing in headers %s' %
                                (call['method'], key, call['headers']))
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
                self.assertEquals(res.status[:len(expected)], expected)

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
        with save_globals():
            limit = constraints.MAX_CONTAINER_NAME_LENGTH
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          '1' * limit)
            self.assert_status_map(controller.PUT,
                                   (200, 201, 201, 201), 201,
                                   missing_container=True)
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          '2' * (limit + 1))
            self.assert_status_map(controller.PUT, (201, 201, 201), 400,
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
                self.assertEquals(resp.status_int, 200)

                set_http_connect(404, 404, 404, 200, 200, 200)
                # Make sure it is a blank request wthout env caching
                req = Request.blank('/v1/a/c',
                                    environ={'REQUEST_METHOD': meth})
                resp = getattr(controller, meth)(req)
                self.assertEquals(resp.status_int, 404)

                set_http_connect(503, 404, 404)
                # Make sure it is a blank request wthout env caching
                req = Request.blank('/v1/a/c',
                                    environ={'REQUEST_METHOD': meth})
                resp = getattr(controller, meth)(req)
                self.assertEquals(resp.status_int, 404)

                set_http_connect(503, 404, raise_exc=True)
                # Make sure it is a blank request wthout env caching
                req = Request.blank('/v1/a/c',
                                    environ={'REQUEST_METHOD': meth})
                resp = getattr(controller, meth)(req)
                self.assertEquals(resp.status_int, 404)

                for dev in self.app.account_ring.devs:
                    set_node_errors(self.app, dev,
                                    self.app.error_suppression_limit + 1,
                                    time.time())
                set_http_connect(200, 200, 200, 200, 200, 200)
                # Make sure it is a blank request wthout env caching
                req = Request.blank('/v1/a/c',
                                    environ={'REQUEST_METHOD': meth})
                resp = getattr(controller, meth)(req)
                self.assertEquals(resp.status_int, 404)

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
            self.assertEquals(res.status_int, 201)

    def test_error_limiting(self):
        with save_globals():
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            container_ring = controller.app.container_ring
            controller.app.sort_nodes = lambda l: l
            self.assert_status_map(controller.HEAD, (200, 503, 200, 200), 200,
                                   missing_container=False)

            self.assertEquals(
                node_error_count(controller.app, container_ring.devs[0]), 2)
            self.assert_(
                node_last_error(controller.app, container_ring.devs[0])
                is not None)
            for _junk in xrange(self.app.error_suppression_limit):
                self.assert_status_map(controller.HEAD,
                                       (200, 503, 503, 503), 503)
            self.assertEquals(
                node_error_count(controller.app, container_ring.devs[0]),
                self.app.error_suppression_limit + 1)
            self.assert_status_map(controller.HEAD, (200, 200, 200, 200), 503)
            self.assert_(
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
            self.assert_('accept-ranges' in res.headers)
            self.assertEqual(res.headers['accept-ranges'], 'bytes')

    def test_response_head_accept_ranges_header(self):
        with save_globals():
            set_http_connect(200, 200, body='{}')
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            req = Request.blank('/v1/a/c?format=json')
            self.app.update_request(req)
            res = controller.HEAD(req)
            self.assert_('accept-ranges' in res.headers)
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
                    for k, v in headers.iteritems():
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
                self.assertEquals(test_errors, [])

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
            self.assertEquals(resp.status_int, 201)

            set_http_connect(201, 201, 201)
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers={'X-Container-Meta-' +
                                ('a' * constraints.MAX_META_NAME_LENGTH): 'v'})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            req = Request.blank(
                '/v1/a/c', environ={'REQUEST_METHOD': method},
                headers={'X-Container-Meta-' +
                         ('a' * (constraints.MAX_META_NAME_LENGTH + 1)): 'v'})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 400)

            set_http_connect(201, 201, 201)
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers={'X-Container-Meta-Too-Long':
                                'a' * constraints.MAX_META_VALUE_LENGTH})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers={'X-Container-Meta-Too-Long':
                                'a' * (constraints.MAX_META_VALUE_LENGTH + 1)})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 400)

            set_http_connect(201, 201, 201)
            headers = {}
            for x in xrange(constraints.MAX_META_COUNT):
                headers['X-Container-Meta-%d' % x] = 'v'
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers=headers)
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            headers = {}
            for x in xrange(constraints.MAX_META_COUNT + 1):
                headers['X-Container-Meta-%d' % x] = 'v'
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers=headers)
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 400)

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
            self.assertEquals(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            headers['X-Container-Meta-a'] = \
                'a' * (constraints.MAX_META_OVERALL_SIZE - size)
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers=headers)
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 400)

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
        self.assert_(called[0])
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
        self.assert_(called[0])

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
        self.assert_(called[0])
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
        self.assert_(called[0])

    def test_GET_no_content(self):
        with save_globals():
            set_http_connect(200, 204, 204, 204)
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            req = Request.blank('/v1/a/c')
            self.app.update_request(req)
            res = controller.GET(req)
            self.assertEquals(res.status_int, 204)
            self.assertEquals(
                res.environ['swift.container/a/c']['status'], 204)
            self.assertEquals(res.content_length, 0)
            self.assertTrue('transfer-encoding' not in res.headers)

    def test_GET_calls_authorize(self):
        called = [False]

        def authorize(req):
            called[0] = True
            return HTTPUnauthorized(request=req)
        with save_globals():
            set_http_connect(200, 201, 201, 201)
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            req = Request.blank('/v1/a/c')
            req.environ['swift.authorize'] = authorize
            self.app.update_request(req)
            res = controller.GET(req)
        self.assertEquals(res.environ['swift.container/a/c']['status'], 201)
        self.assert_(called[0])

    def test_HEAD_calls_authorize(self):
        called = [False]

        def authorize(req):
            called[0] = True
            return HTTPUnauthorized(request=req)
        with save_globals():
            set_http_connect(200, 201, 201, 201)
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            req = Request.blank('/v1/a/c', {'REQUEST_METHOD': 'HEAD'})
            req.environ['swift.authorize'] = authorize
            self.app.update_request(req)
            controller.HEAD(req)
        self.assert_(called[0])

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
                self.assertTrue(count[0] < 11)

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
            self.assertEquals(401, resp.status_int)

            def my_empty_origin_container_info(*args):
                return {'cors': {'allow_origin': None}}
            controller.container_info = my_empty_origin_container_info
            req = Request.blank(
                '/v1/a/c',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'http://foo.com',
                         'Access-Control-Request-Method': 'GET'})
            resp = controller.OPTIONS(req)
            self.assertEquals(401, resp.status_int)

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
            self.assertEquals(200, resp.status_int)
            self.assertEquals(
                'https://foo.bar',
                resp.headers['access-control-allow-origin'])
            for verb in 'OPTIONS GET POST PUT DELETE HEAD'.split():
                self.assertTrue(
                    verb in resp.headers['access-control-allow-methods'])
            self.assertEquals(
                len(resp.headers['access-control-allow-methods'].split(', ')),
                6)
            self.assertEquals('999', resp.headers['access-control-max-age'])
            req = Request.blank(
                '/v1/a/c',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'https://foo.bar'})
            req.content_length = 0
            resp = controller.OPTIONS(req)
            self.assertEquals(401, resp.status_int)
            req = Request.blank('/v1/a/c', {'REQUEST_METHOD': 'OPTIONS'})
            req.content_length = 0
            resp = controller.OPTIONS(req)
            self.assertEquals(200, resp.status_int)
            for verb in 'OPTIONS GET POST PUT DELETE HEAD'.split():
                self.assertTrue(
                    verb in resp.headers['Allow'])
            self.assertEquals(len(resp.headers['Allow'].split(', ')), 6)
            req = Request.blank(
                '/v1/a/c',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'http://foo.bar',
                         'Access-Control-Request-Method': 'GET'})
            resp = controller.OPTIONS(req)
            self.assertEquals(401, resp.status_int)
            req = Request.blank(
                '/v1/a/c',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'http://foo.bar',
                         'Access-Control-Request-Method': 'GET'})
            controller.app.cors_allow_origin = ['http://foo.bar', ]
            resp = controller.OPTIONS(req)
            self.assertEquals(200, resp.status_int)

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
            self.assertEquals(200, resp.status_int)
            self.assertEquals('*', resp.headers['access-control-allow-origin'])
            for verb in 'OPTIONS GET POST PUT DELETE HEAD'.split():
                self.assertTrue(
                    verb in resp.headers['access-control-allow-methods'])
            self.assertEquals(
                len(resp.headers['access-control-allow-methods'].split(', ')),
                6)
            self.assertEquals('999', resp.headers['access-control-max-age'])

            req = Request.blank(
                '/v1/a/c/o.jpg',
                {'REQUEST_METHOD': 'OPTIONS'},
                headers={'Origin': 'https://bar.baz',
                         'Access-Control-Request-Headers':
                         'x-foo, x-bar, x-auth-token',
                         'Access-Control-Request-Method': 'GET'}
            )
            req.content_length = 0
            resp = controller.OPTIONS(req)
            self.assertEquals(200, resp.status_int)
            self.assertEquals(
                sortHeaderNames('x-foo, x-bar, x-auth-token'),
                sortHeaderNames(resp.headers['access-control-allow-headers']))

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

            self.assertEquals(200, resp.status_int)
            self.assertEquals('http://foo.bar',
                              resp.headers['access-control-allow-origin'])
            self.assertEquals('red', resp.headers['x-container-meta-color'])
            # X-Super-Secret is in the response, but not "exposed"
            self.assertEquals('hush', resp.headers['x-super-secret'])
            self.assertTrue('access-control-expose-headers' in resp.headers)
            exposed = set(
                h.strip() for h in
                resp.headers['access-control-expose-headers'].split(','))
            expected_exposed = set(['cache-control', 'content-language',
                                    'content-type', 'expires', 'last-modified',
                                    'pragma', 'etag', 'x-timestamp',
                                    'x-trans-id', 'x-container-meta-color'])
            self.assertEquals(expected_exposed, exposed)

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
            self.assert_(re.match('[0-9]{10}\.[0-9]{5}', timestamp))

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
            self.assert_(re.match('[0-9]{10}\.[0-9]{5}', timestamp))

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
            self.assert_(got_exc)


@patch_policies([StoragePolicy(0, 'zero', True, object_ring=FakeRing())])
class TestAccountController(unittest.TestCase):

    def setUp(self):
        self.app = proxy_server.Application(None, FakeMemcache(),
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
            self.assertEquals(res.status_int, expected)
            if env_expected:
                self.assertEquals(res.environ['swift.account/a']['status'],
                                  env_expected)
            set_http_connect(*statuses)
            req = Request.blank('/v1/a/', {})
            self.app.update_request(req)
            res = method(req)
            self.assertEquals(res.status_int, expected)
            if env_expected:
                self.assertEquals(res.environ['swift.account/a']['status'],
                                  env_expected)

    def test_OPTIONS(self):
        with save_globals():
            self.app.allow_account_management = False
            controller = proxy_server.AccountController(self.app, 'account')
            req = Request.blank('/v1/account', {'REQUEST_METHOD': 'OPTIONS'})
            req.content_length = 0
            resp = controller.OPTIONS(req)
            self.assertEquals(200, resp.status_int)
            for verb in 'OPTIONS GET POST HEAD'.split():
                self.assertTrue(
                    verb in resp.headers['Allow'])
            self.assertEquals(len(resp.headers['Allow'].split(', ')), 4)

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
            self.assertEquals(200, resp.status_int)
            for verb in 'OPTIONS GET POST HEAD'.split():
                self.assertTrue(
                    verb in resp.headers['Allow'])
            self.assertEquals(len(resp.headers['Allow'].split(', ')), 4)

            self.app.allow_account_management = True
            controller = proxy_server.AccountController(self.app, 'account')
            req = Request.blank('/v1/account', {'REQUEST_METHOD': 'OPTIONS'})
            req.content_length = 0
            resp = controller.OPTIONS(req)
            self.assertEquals(200, resp.status_int)
            for verb in 'OPTIONS GET POST PUT DELETE HEAD'.split():
                self.assertTrue(
                    verb in resp.headers['Allow'])
            self.assertEquals(len(resp.headers['Allow'].split(', ')), 6)

    def test_GET(self):
        with save_globals():
            controller = proxy_server.AccountController(self.app, 'account')
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
            controller = proxy_server.AccountController(self.app, 'account')
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
            self.assert_status_map(controller.GET,
                                   (404, 404, 404), 204)
            self.assert_status_map(controller.GET,
                                   (404, 503, 404), 204)

            # We always return 503 if no majority between 4xx, 3xx or 2xx found
            self.assert_status_map(controller.GET,
                                   (500, 500, 400), 503)

    def test_HEAD(self):
        # Same behaviour as GET
        with save_globals():
            controller = proxy_server.AccountController(self.app, 'account')
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
            controller = proxy_server.AccountController(self.app, 'account')
            self.app.memcache = FakeMemcacheReturnsNone()
            self.assertFalse(self.app.account_autocreate)
            self.assert_status_map(controller.HEAD,
                                   (404, 404, 404), 404)
            controller.app.account_autocreate = True
            self.assert_status_map(controller.HEAD,
                                   (404, 404, 404), 204)
            self.assert_status_map(controller.HEAD,
                                   (500, 404, 404), 204)
            # We always return 503 if no majority between 4xx, 3xx or 2xx found
            self.assert_status_map(controller.HEAD,
                                   (500, 500, 400), 503)

    def test_POST_autocreate(self):
        with save_globals():
            controller = proxy_server.AccountController(self.app, 'account')
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
            controller = proxy_server.AccountController(self.app, 'account')
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
                self.assertTrue(key in call['headers'],
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
        self.assertEquals(resp.status_int, 503)

    def test_other_socket_error(self):
        self.app.account_ring.get_nodes('account')
        for dev in self.app.account_ring.devs:
            dev['ip'] = '127.0.0.1'
            dev['port'] = -1  # invalid port number
        controller = proxy_server.AccountController(self.app, 'account')
        req = Request.blank('/v1/account', environ={'REQUEST_METHOD': 'HEAD'})
        self.app.update_request(req)
        resp = controller.HEAD(req)
        self.assertEquals(resp.status_int, 503)

    def test_response_get_accept_ranges_header(self):
        with save_globals():
            set_http_connect(200, 200, body='{}')
            controller = proxy_server.AccountController(self.app, 'account')
            req = Request.blank('/v1/a?format=json')
            self.app.update_request(req)
            res = controller.GET(req)
            self.assert_('accept-ranges' in res.headers)
            self.assertEqual(res.headers['accept-ranges'], 'bytes')

    def test_response_head_accept_ranges_header(self):
        with save_globals():
            set_http_connect(200, 200, body='{}')
            controller = proxy_server.AccountController(self.app, 'account')
            req = Request.blank('/v1/a?format=json')
            self.app.update_request(req)
            res = controller.HEAD(req)
            res.body
            self.assert_('accept-ranges' in res.headers)
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
                self.assertEquals(res.status[:len(expected)], expected)
            test_status_map((201, 201, 201), 405)
            self.app.allow_account_management = True
            test_status_map((201, 201, 201), 201)
            test_status_map((201, 201, 500), 201)
            test_status_map((201, 500, 500), 503)
            test_status_map((204, 500, 404), 503)

    def test_PUT_max_account_name_length(self):
        with save_globals():
            self.app.allow_account_management = True
            limit = constraints.MAX_ACCOUNT_NAME_LENGTH
            controller = proxy_server.AccountController(self.app, '1' * limit)
            self.assert_status_map(controller.PUT, (201, 201, 201), 201)
            controller = proxy_server.AccountController(
                self.app, '2' * (limit + 1))
            self.assert_status_map(controller.PUT, (201, 201, 201), 400)

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
                    for k, v in headers.iteritems():
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
                self.assertEquals(test_errors, [])

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
            self.assertEquals(resp.status_int, 201)

            set_http_connect(201, 201, 201)
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers={'X-Account-Meta-' +
                                ('a' * constraints.MAX_META_NAME_LENGTH): 'v'})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            req = Request.blank(
                '/v1/a/c', environ={'REQUEST_METHOD': method},
                headers={'X-Account-Meta-' +
                         ('a' * (constraints.MAX_META_NAME_LENGTH + 1)): 'v'})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 400)

            set_http_connect(201, 201, 201)
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers={'X-Account-Meta-Too-Long':
                                'a' * constraints.MAX_META_VALUE_LENGTH})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers={'X-Account-Meta-Too-Long':
                                'a' * (constraints.MAX_META_VALUE_LENGTH + 1)})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 400)

            set_http_connect(201, 201, 201)
            headers = {}
            for x in xrange(constraints.MAX_META_COUNT):
                headers['X-Account-Meta-%d' % x] = 'v'
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers=headers)
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            headers = {}
            for x in xrange(constraints.MAX_META_COUNT + 1):
                headers['X-Account-Meta-%d' % x] = 'v'
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers=headers)
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 400)

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
            self.assertEquals(resp.status_int, 201)
            set_http_connect(201, 201, 201)
            headers['X-Account-Meta-a'] = \
                'a' * (constraints.MAX_META_OVERALL_SIZE - size)
            req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method},
                                headers=headers)
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 400)

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
                self.assertEquals(res.status[:len(expected)], expected)
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
                self.assertEquals(res.status[:len(expected)], expected)
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
        self.app = proxy_server.Application(conf, FakeMemcache(),
                                            account_ring=FakeRing(),
                                            container_ring=FakeRing())
        self.app.memcache = FakeMemcacheReturnsNone()

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
            self.assert_(header not in resp.headers, '%r was in %r' % (
                header, resp.headers))

            # Same setup -- mock acct server will provide ACLs
            set_http_connect(200, 200, 200, headers=privileged_headers)
            req = Request.blank('/v1/a', environ={'REQUEST_METHOD': 'GET',
                                                  'swift_owner': True})
            resp = app.handle_request(req)

            # For a swift_owner, the ACLs *should* be in response
            self.assert_(header in resp.headers, '%r not in %r' % (
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
                self.assertEqual(None, h.get(ext_header))

                # swift_owner unset: GET/HEAD shouldn't return sensitive info
                make_canned_response(verb)
                req = make_test_request(verb, swift_owner=False)
                del req.environ['swift_owner']
                resp = app.handle_request(req)
                h = resp.headers
                self.assertEqual(None, h.get(ext_header))

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
        prolis = _test_sockets[0]
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        # Client is transmitting in 2 MB chunks
        fd = sock.makefile('wb', 2 * 1024 * 1024)
        # Small, fast for testing
        obj_len = 2 * 64 * 1024
        # Use 1 GB or more for measurements
        #obj_len = 2 * 512 * 1024 * 1024
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
            print "Run %02d took %07.03f" % (i, end - start)


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
        self.assertTrue('version' in si)
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
        self.assertTrue('strict_cors_mode' in si)
        self.assertEqual(si['allow_account_management'], False)
        self.assertEqual(si['account_autocreate'], False)
        # This setting is by default excluded by disallowed_sections
        self.assertEqual(si['valid_api_versions'],
                         constraints.VALID_API_VERSIONS)
        # this next test is deliberately brittle in order to alert if
        # other items are added to swift info
        self.assertEqual(len(si), 17)

        self.assertTrue('policies' in si)
        sorted_pols = sorted(si['policies'], key=operator.itemgetter('name'))
        self.assertEqual(len(sorted_pols), 3)
        for policy in sorted_pols:
            self.assertNotEquals(policy['name'], 'deprecated')
        self.assertEqual(sorted_pols[0]['name'], 'bert')
        self.assertEqual(sorted_pols[1]['name'], 'ernie')
        self.assertEqual(sorted_pols[2]['name'], 'migrated')


if __name__ == '__main__':
    setup()
    try:
        unittest.main()
    finally:
        teardown()
