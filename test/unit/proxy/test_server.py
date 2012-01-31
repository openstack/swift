# Copyright (c) 2010-2011 OpenStack, LLC.
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

from __future__ import with_statement
import cPickle as pickle
import logging
from logging.handlers import SysLogHandler
import os
import sys
import unittest
from nose import SkipTest
from ConfigParser import ConfigParser
from contextlib import contextmanager
from cStringIO import StringIO
from gzip import GzipFile
from httplib import HTTPException
from shutil import rmtree
from time import time
from urllib import unquote, quote
from hashlib import md5
from tempfile import mkdtemp

import eventlet
from eventlet import sleep, spawn, Timeout, util, wsgi, listen
import simplejson
from webob import Request, Response
from webob.exc import HTTPNotFound, HTTPUnauthorized

from test.unit import connect_tcp, readuntil2crlfs
from swift.proxy import server as proxy_server
from swift.account import server as account_server
from swift.container import server as container_server
from swift.obj import server as object_server
from swift.common import ring
from swift.common.constraints import MAX_META_NAME_LENGTH, \
    MAX_META_VALUE_LENGTH, MAX_META_COUNT, MAX_META_OVERALL_SIZE, MAX_FILE_SIZE
from swift.common.utils import mkdirs, normalize_timestamp, NullLogger
from swift.common.wsgi import monkey_patch_mimetools

# mocks
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


def setup():
    global _testdir, _test_servers, _test_sockets, \
            _orig_container_listing_limit, _test_coros
    monkey_patch_mimetools()
    # Since we're starting up a lot here, we're going to test more than
    # just chunked puts; we're also going to test parts of
    # proxy_server.Application we couldn't get to easily otherwise.
    _testdir = \
        os.path.join(mkdtemp(), 'tmp_test_proxy_server_chunked')
    mkdirs(_testdir)
    rmtree(_testdir)
    mkdirs(os.path.join(_testdir, 'sda1'))
    mkdirs(os.path.join(_testdir, 'sda1', 'tmp'))
    mkdirs(os.path.join(_testdir, 'sdb1'))
    mkdirs(os.path.join(_testdir, 'sdb1', 'tmp'))
    _orig_container_listing_limit = proxy_server.CONTAINER_LISTING_LIMIT
    conf = {'devices': _testdir, 'swift_dir': _testdir,
            'mount_check': 'false', 'allowed_headers':
            'content-encoding, x-object-manifest, content-disposition, foo'}
    prolis = listen(('localhost', 0))
    acc1lis = listen(('localhost', 0))
    acc2lis = listen(('localhost', 0))
    con1lis = listen(('localhost', 0))
    con2lis = listen(('localhost', 0))
    obj1lis = listen(('localhost', 0))
    obj2lis = listen(('localhost', 0))
    _test_sockets = \
        (prolis, acc1lis, acc2lis, con2lis, con2lis, obj1lis, obj2lis)
    pickle.dump(ring.RingData([[0, 1, 0, 1], [1, 0, 1, 0]],
        [{'id': 0, 'zone': 0, 'device': 'sda1', 'ip': '127.0.0.1',
          'port': acc1lis.getsockname()[1]},
         {'id': 1, 'zone': 1, 'device': 'sdb1', 'ip': '127.0.0.1',
          'port': acc2lis.getsockname()[1]}], 30),
        GzipFile(os.path.join(_testdir, 'account.ring.gz'), 'wb'))
    pickle.dump(ring.RingData([[0, 1, 0, 1], [1, 0, 1, 0]],
        [{'id': 0, 'zone': 0, 'device': 'sda1', 'ip': '127.0.0.1',
          'port': con1lis.getsockname()[1]},
         {'id': 1, 'zone': 1, 'device': 'sdb1', 'ip': '127.0.0.1',
          'port': con2lis.getsockname()[1]}], 30),
        GzipFile(os.path.join(_testdir, 'container.ring.gz'), 'wb'))
    pickle.dump(ring.RingData([[0, 1, 0, 1], [1, 0, 1, 0]],
        [{'id': 0, 'zone': 0, 'device': 'sda1', 'ip': '127.0.0.1',
          'port': obj1lis.getsockname()[1]},
         {'id': 1, 'zone': 1, 'device': 'sdb1', 'ip': '127.0.0.1',
          'port': obj2lis.getsockname()[1]}], 30),
        GzipFile(os.path.join(_testdir, 'object.ring.gz'), 'wb'))
    prosrv = proxy_server.Application(conf, FakeMemcacheReturnsNone())
    acc1srv = account_server.AccountController(conf)
    acc2srv = account_server.AccountController(conf)
    con1srv = container_server.ContainerController(conf)
    con2srv = container_server.ContainerController(conf)
    obj1srv = object_server.ObjectController(conf)
    obj2srv = object_server.ObjectController(conf)
    _test_servers = \
        (prosrv, acc1srv, acc2srv, con2srv, con2srv, obj1srv, obj2srv)
    nl = NullLogger()
    prospa = spawn(wsgi.server, prolis, prosrv, nl)
    acc1spa = spawn(wsgi.server, acc1lis, acc1srv, nl)
    acc2spa = spawn(wsgi.server, acc2lis, acc2srv, nl)
    con1spa = spawn(wsgi.server, con1lis, con1srv, nl)
    con2spa = spawn(wsgi.server, con2lis, con2srv, nl)
    obj1spa = spawn(wsgi.server, obj1lis, obj1srv, nl)
    obj2spa = spawn(wsgi.server, obj2lis, obj2srv, nl)
    _test_coros = \
        (prospa, acc1spa, acc2spa, con2spa, con2spa, obj1spa, obj2spa)
    # Create account
    ts = normalize_timestamp(time())
    partition, nodes = prosrv.account_ring.get_nodes('a')
    for node in nodes:
        conn = proxy_server.http_connect(node['ip'], node['port'],
                node['device'], partition, 'PUT', '/a',
                {'X-Timestamp': ts, 'x-trans-id': 'test'})
        resp = conn.getresponse()
        assert(resp.status == 201)
    # Create container
    sock = connect_tcp(('localhost', prolis.getsockname()[1]))
    fd = sock.makefile()
    fd.write('PUT /v1/a/c HTTP/1.1\r\nHost: localhost\r\n'
        'Connection: close\r\nX-Auth-Token: t\r\n'
        'Content-Length: 0\r\n\r\n')
    fd.flush()
    headers = readuntil2crlfs(fd)
    exp = 'HTTP/1.1 201'
    assert(headers[:len(exp)] == exp)


def teardown():
    for server in _test_coros:
        server.kill()
    proxy_server.CONTAINER_LISTING_LIMIT = _orig_container_listing_limit
    rmtree(os.path.dirname(_testdir))


def fake_http_connect(*code_iter, **kwargs):

    class FakeConn(object):

        def __init__(self, status, etag=None, body='', timestamp='1'):
            self.status = status
            self.reason = 'Fake'
            self.host = '1.2.3.4'
            self.port = '1234'
            self.sent = 0
            self.received = 0
            self.etag = etag
            self.body = body
            self.timestamp = timestamp

        def getresponse(self):
            if kwargs.get('raise_exc'):
                raise Exception('test')
            if kwargs.get('raise_timeout_exc'):
                raise Timeout()
            return self

        def getexpect(self):
            return FakeConn(100)

        def getheaders(self):
            headers = {'content-length': len(self.body),
                       'content-type': 'x-application/test',
                       'x-timestamp': self.timestamp,
                       'last-modified': self.timestamp,
                       'x-object-meta-test': 'testing',
                       'etag':
                            self.etag or '"68b329da9893e34099c7d8ad5cb9c940"',
                       'x-works': 'yes',
                       }
            if not self.timestamp:
                del headers['x-timestamp']
            try:
                if container_ts_iter.next() is False:
                    headers['x-container-timestamp'] = '1'
            except StopIteration:
                pass
            if 'slow' in kwargs:
                headers['content-length'] = '4'
            if 'headers' in kwargs:
                headers.update(kwargs['headers'])
            return headers.items()

        def read(self, amt=None):
            if 'slow' in kwargs:
                if self.sent < 4:
                    self.sent += 1
                    sleep(0.1)
                    return ' '
            rv = self.body[:amt]
            self.body = self.body[amt:]
            return rv

        def send(self, amt=None):
            if 'slow' in kwargs:
                if self.received < 4:
                    self.received += 1
                    sleep(0.1)

        def getheader(self, name, default=None):
            return dict(self.getheaders()).get(name.lower(), default)

    timestamps_iter = iter(kwargs.get('timestamps') or ['1'] * len(code_iter))
    etag_iter = iter(kwargs.get('etags') or [None] * len(code_iter))
    x = kwargs.get('missing_container', [False] * len(code_iter))
    if not isinstance(x, (tuple, list)):
        x = [x] * len(code_iter)
    container_ts_iter = iter(x)
    code_iter = iter(code_iter)

    def connect(*args, **ckwargs):
        if 'give_content_type' in kwargs:
            if len(args) >= 7 and 'content_type' in args[6]:
                kwargs['give_content_type'](args[6]['content-type'])
            else:
                kwargs['give_content_type']('')
        if 'give_connect' in kwargs:
            kwargs['give_connect'](*args, **ckwargs)
        status = code_iter.next()
        etag = etag_iter.next()
        timestamp = timestamps_iter.next()
        if status == -1:
            raise HTTPException()
        return FakeConn(status, etag, body=kwargs.get('body', ''),
                        timestamp=timestamp)

    return connect


class FakeRing(object):

    def __init__(self):
        # 9 total nodes (6 more past the initial 3) is the cap, no matter if
        # this is set higher.
        self.max_more_nodes = 0
        self.devs = {}
        self.replica_count = 3

    def get_nodes(self, account, container=None, obj=None):
        devs = []
        for x in xrange(3):
            devs.append(self.devs.get(x))
            if devs[x] is None:
                self.devs[x] = devs[x] = \
                    {'ip': '10.0.0.%s' % x, 'port': 1000 + x, 'device': 'sda'}
        return 1, devs

    def get_part_nodes(self, part):
        return self.get_nodes('blah')[1]

    def get_more_nodes(self, nodes):
        # 9 is the true cap
        for x in xrange(3, min(3 + self.max_more_nodes, 9)):
            yield {'ip': '10.0.0.%s' % x, 'port': 1000 + x, 'device': 'sda'}


class FakeMemcache(object):

    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def keys(self):
        return self.store.keys()

    def set(self, key, value, timeout=0):
        self.store[key] = value
        return True

    def incr(self, key, timeout=0):
        self.store[key] = self.store.setdefault(key, 0) + 1
        return self.store[key]

    @contextmanager
    def soft_lock(self, key, timeout=0, retries=5):
        yield True

    def delete(self, key):
        try:
            del self.store[key]
        except Exception:
            pass
        return True


class FakeMemcacheReturnsNone(FakeMemcache):

    def get(self, key):
        # Returns None as the timestamp of the container; assumes we're only
        # using the FakeMemcache for container existence checks.
        return None


class NullLoggingHandler(logging.Handler):

    def emit(self, record):
        pass


@contextmanager
def save_globals():
    orig_http_connect = getattr(proxy_server, 'http_connect', None)
    orig_account_info = getattr(proxy_server.Controller, 'account_info', None)
    try:
        yield True
    finally:
        proxy_server.http_connect = orig_http_connect
        proxy_server.Controller.account_info = orig_account_info


# tests
class TestController(unittest.TestCase):

    def setUp(self):
        self.account_ring = FakeRing()
        self.container_ring = FakeRing()
        self.memcache = FakeMemcache()

        app = proxy_server.Application(None, self.memcache,
            account_ring=self.account_ring,
            container_ring=self.container_ring,
            object_ring=FakeRing())
        self.controller = proxy_server.Controller(app)

        self.account = 'some_account'
        self.container = 'some_container'
        self.read_acl = 'read_acl'
        self.write_acl = 'write_acl'

    def check_account_info_return(self, partition, nodes, is_none=False):
        if is_none:
            p, n = None, None
        else:
            p, n = self.account_ring.get_nodes(self.account)
        self.assertEqual(p, partition)
        self.assertEqual(n, nodes)

    def test_make_requests(self):
        with save_globals():
            proxy_server.http_connect = fake_http_connect(200)
            partition, nodes = self.controller.account_info(self.account)
            proxy_server.http_connect = fake_http_connect(201,
                                            raise_timeout_exc=True)
            self.controller._make_request(nodes, partition, 'POST',
                                            '/', '', '')

    # tests if 200 is cached and used
    def test_account_info_200(self):
        with save_globals():
            proxy_server.http_connect = fake_http_connect(200)
            partition, nodes = self.controller.account_info(self.account)
            self.check_account_info_return(partition, nodes)

            cache_key = proxy_server.get_account_memcache_key(self.account)
            self.assertEquals(200, self.memcache.get(cache_key))

            proxy_server.http_connect = fake_http_connect()
            partition, nodes = self.controller.account_info(self.account)
            self.check_account_info_return(partition, nodes)

    # tests if 404 is cached and used
    def test_account_info_404(self):
        with save_globals():
            proxy_server.http_connect = fake_http_connect(404, 404, 404)
            partition, nodes = self.controller.account_info(self.account)
            self.check_account_info_return(partition, nodes, True)

            cache_key = proxy_server.get_account_memcache_key(self.account)
            self.assertEquals(404, self.memcache.get(cache_key))

            proxy_server.http_connect = fake_http_connect()
            partition, nodes = self.controller.account_info(self.account)
            self.check_account_info_return(partition, nodes, True)

    # tests if some http status codes are not cached
    def test_account_info_no_cache(self):
        def test(*status_list):
            proxy_server.http_connect = fake_http_connect(*status_list)
            partition, nodes = self.controller.account_info(self.account)
            self.assertEqual(len(self.memcache.keys()), 0)
            self.check_account_info_return(partition, nodes, True)

        with save_globals():
            test(503, 404, 404)
            test(404, 404, 503)
            test(404, 507, 503)
            test(503, 503, 503)

    def test_account_info_account_autocreate(self):
        with save_globals():
            self.memcache.store = {}
            proxy_server.http_connect = \
                fake_http_connect(404, 404, 404, 201, 201, 201)
            partition, nodes = \
                self.controller.account_info(self.account, autocreate=False)
            self.check_account_info_return(partition, nodes, is_none=True)

            self.memcache.store = {}
            proxy_server.http_connect = \
                fake_http_connect(404, 404, 404, 201, 201, 201)
            partition, nodes = \
                self.controller.account_info(self.account)
            self.check_account_info_return(partition, nodes, is_none=True)

            self.memcache.store = {}
            proxy_server.http_connect = \
                fake_http_connect(404, 404, 404, 201, 201, 201)
            partition, nodes = \
                self.controller.account_info(self.account, autocreate=True)
            self.check_account_info_return(partition, nodes)

            self.memcache.store = {}
            proxy_server.http_connect = \
                fake_http_connect(404, 404, 404, 503, 201, 201)
            partition, nodes = \
                self.controller.account_info(self.account, autocreate=True)
            self.check_account_info_return(partition, nodes)

            self.memcache.store = {}
            proxy_server.http_connect = \
                fake_http_connect(404, 404, 404, 503, 201, 503)
            exc = None
            try:
                partition, nodes = \
                    self.controller.account_info(self.account, autocreate=True)
            except Exception, err:
                exc = err
            self.assertEquals(str(exc),
                              "Could not autocreate account '/some_account'")

    def check_container_info_return(self, ret, is_none=False):
        if is_none:
            partition, nodes, read_acl, write_acl = None, None, None, None
        else:
            partition, nodes = self.container_ring.get_nodes(self.account,
                self.container)
            read_acl, write_acl = self.read_acl, self.write_acl
        self.assertEqual(partition, ret[0])
        self.assertEqual(nodes, ret[1])
        self.assertEqual(read_acl, ret[2])
        self.assertEqual(write_acl, ret[3])

    def test_container_info_invalid_account(self):
        def account_info(self, account, autocreate=False):
            return None, None

        with save_globals():
            proxy_server.Controller.account_info = account_info
            ret = self.controller.container_info(self.account,
                self.container)
            self.check_container_info_return(ret, True)

    # tests if 200 is cached and used
    def test_container_info_200(self):
        def account_info(self, account, autocreate=False):
            return True, True

        with save_globals():
            headers = {'x-container-read': self.read_acl,
                'x-container-write': self.write_acl}
            proxy_server.Controller.account_info = account_info
            proxy_server.http_connect = fake_http_connect(200,
                headers=headers)
            ret = self.controller.container_info(self.account,
                self.container)
            self.check_container_info_return(ret)

            cache_key = proxy_server.get_container_memcache_key(self.account,
                self.container)
            cache_value = self.memcache.get(cache_key)
            self.assertTrue(isinstance(cache_value, dict))
            self.assertEquals(200, cache_value.get('status'))

            proxy_server.http_connect = fake_http_connect()
            ret = self.controller.container_info(self.account,
                 self.container)
            self.check_container_info_return(ret)

    # tests if 404 is cached and used
    def test_container_info_404(self):
        def account_info(self, account, autocreate=False):
            return True, True

        with save_globals():
            proxy_server.Controller.account_info = account_info
            proxy_server.http_connect = fake_http_connect(404, 404, 404)
            ret = self.controller.container_info(self.account,
                self.container)
            self.check_container_info_return(ret, True)

            cache_key = proxy_server.get_container_memcache_key(self.account,
                self.container)
            cache_value = self.memcache.get(cache_key)
            self.assertTrue(isinstance(cache_value, dict))
            self.assertEquals(404, cache_value.get('status'))

            proxy_server.http_connect = fake_http_connect()
            ret = self.controller.container_info(self.account,
                 self.container)
            self.check_container_info_return(ret, True)

    # tests if some http status codes are not cached
    def test_container_info_no_cache(self):
        def test(*status_list):
            proxy_server.http_connect = fake_http_connect(*status_list)
            ret = self.controller.container_info(self.account,
                self.container)
            self.assertEqual(len(self.memcache.keys()), 0)
            self.check_container_info_return(ret, True)

        with save_globals():
            test(503, 404, 404)
            test(404, 404, 503)
            test(404, 507, 503)
            test(503, 503, 503)


class TestProxyServer(unittest.TestCase):

    def test_access_log(self):

        class MyApp(proxy_server.Application):

            def handle_request(self, req):
                resp = Response(request=req)
                req.response = resp
                req.start_time = time()
                return resp

        def start_response(*args):
            pass

        class MockLogger():

            def __init__(self):
                self.buffer = StringIO()

            def info(self, msg, args=None):
                if args:
                    msg = msg % args
                self.buffer.write(msg)

            def strip_value(self):
                rv = self.buffer.getvalue()
                self.buffer.truncate(0)
                return rv

        class SnarfStream(object):
            # i can't seem to subclass cStringIO

            def __init__(self, *args, **kwargs):
                self.sio = StringIO()

            def strip_value(self):
                rv = self.getvalue().strip()
                self.truncate(0)
                return rv

            def __getattr__(self, name):
                try:
                    return object.__getattr__(self, name)
                except AttributeError:
                    try:
                        return getattr(self.sio, name)
                    except AttributeError:
                        return self.__getattribute__(name)

        snarf = SnarfStream()
        _orig_get_logger = proxy_server.get_logger

        def mock_get_logger(*args, **kwargs):
            if kwargs.get('log_route') != 'proxy-access':
                return _orig_get_logger(*args, **kwargs)
            kwargs['log_route'] = 'snarf'
            logger = _orig_get_logger(*args, **kwargs)
            if [h for h in logger.logger.handlers if
                isinstance(h, logging.StreamHandler) and h.stream is snarf]:
                # snarf handler already setup!
                return logger
            formatter = logger.logger.handlers[0].formatter
            formatter._fmt += ' %(levelname)s'
            snarf_handler = logging.StreamHandler(snarf)
            snarf_handler.setFormatter(formatter)
            logger.logger.addHandler(snarf_handler)
            return logger

        def test_conf(conf):
            app = MyApp(conf, memcache=FakeMemcache(), account_ring=FakeRing(),
                        container_ring=FakeRing(), object_ring=FakeRing())
            req = Request.blank('')
            app(req.environ, start_response)

        try:
            proxy_server.get_logger = mock_get_logger
            test_conf({})
            line = snarf.strip_value()
            print line
            self.assert_(line.startswith('swift'))
            self.assert_(line.endswith('INFO'))
            test_conf({'log_name': 'snarf-test'})
            line = snarf.strip_value()
            print line
            self.assert_(line.startswith('snarf-test'))
            self.assert_(line.endswith('INFO'))
            test_conf({'log_name': 'snarf-test', 'log_level': 'ERROR'})
            line = snarf.strip_value()
            print line
            self.assertFalse(line)
            test_conf({'log_name': 'snarf-test', 'log_level': 'ERROR',
                       'access_log_name': 'access-test',
                       'access_log_level': 'INFO'})
            line = snarf.strip_value()
            print line
            self.assert_(line.startswith('access-test'))
            self.assert_(line.endswith('INFO'))

            # test facility
            def get_facility(logger):
                h = [h for h in logger.logger.handlers if
                     isinstance(h, SysLogHandler)][0]
                return h.facility

            conf = {'log_facility': 'LOG_LOCAL0'}
            app = MyApp(conf, memcache=FakeMemcache(), account_ring=FakeRing(),
                        container_ring=FakeRing(), object_ring=FakeRing())
            self.assertEquals(get_facility(app.logger),
                              SysLogHandler.LOG_LOCAL0)
            self.assertEquals(get_facility(app.access_logger),
                              SysLogHandler.LOG_LOCAL0)
            conf = {'log_facility': 'LOG_LOCAL0',
                    'access_log_facility': 'LOG_LOCAL1'}
            app = MyApp(conf, memcache=FakeMemcache(), account_ring=FakeRing(),
                        container_ring=FakeRing(), object_ring=FakeRing())
            self.assertEquals(get_facility(app.logger),
                              SysLogHandler.LOG_LOCAL0)
            self.assertEquals(get_facility(app.access_logger),
                              SysLogHandler.LOG_LOCAL1)
            conf = {'access_log_facility': 'LOG_LOCAL1'}
            app = MyApp(conf, memcache=FakeMemcache(), account_ring=FakeRing(),
                        container_ring=FakeRing(), object_ring=FakeRing())
            self.assertEquals(get_facility(app.logger),
                              SysLogHandler.LOG_LOCAL0)
            self.assertEquals(get_facility(app.access_logger),
                              SysLogHandler.LOG_LOCAL1)

        finally:
            proxy_server.get_logger = _orig_get_logger

    def test_unhandled_exception(self):

        class MyApp(proxy_server.Application):

            def get_controller(self, path):
                raise Exception('this shouldnt be caught')

        app = MyApp(None, FakeMemcache(), account_ring=FakeRing(),
                container_ring=FakeRing(), object_ring=FakeRing())
        req = Request.blank('/account', environ={'REQUEST_METHOD': 'HEAD'})
        app.update_request(req)
        resp = app.handle_request(req)
        self.assertEquals(resp.status_int, 500)

    def test_calls_authorize_allow(self):
        called = [False]

        def authorize(req):
            called[0] = True
        with save_globals():
            proxy_server.http_connect = fake_http_connect(200)
            app = proxy_server.Application(None, FakeMemcache(),
                account_ring=FakeRing(), container_ring=FakeRing(),
                object_ring=FakeRing())
            req = Request.blank('/v1/a')
            req.environ['swift.authorize'] = authorize
            app.update_request(req)
            resp = app.handle_request(req)
        self.assert_(called[0])

    def test_calls_authorize_deny(self):
        called = [False]

        def authorize(req):
            called[0] = True
            return HTTPUnauthorized(request=req)
        app = proxy_server.Application(None, FakeMemcache(),
            account_ring=FakeRing(), container_ring=FakeRing(),
            object_ring=FakeRing())
        req = Request.blank('/v1/a')
        req.environ['swift.authorize'] = authorize
        app.update_request(req)
        resp = app.handle_request(req)
        self.assert_(called[0])

    def test_negative_content_length(self):
        swift_dir = mkdtemp()
        try:
            baseapp = proxy_server.BaseApplication({'swift_dir': swift_dir},
                FakeMemcache(), NullLoggingHandler(), FakeRing(), FakeRing(),
                FakeRing())
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


class TestObjectController(unittest.TestCase):

    def setUp(self):
        self.app = proxy_server.Application(None, FakeMemcache(),
            account_ring=FakeRing(), container_ring=FakeRing(),
            object_ring=FakeRing())
        monkey_patch_mimetools()

    def tearDown(self):
        proxy_server.CONTAINER_LISTING_LIMIT = _orig_container_listing_limit

    def assert_status_map(self, method, statuses, expected, raise_exc=False):
        with save_globals():
            kwargs = {}
            if raise_exc:
                kwargs['raise_exc'] = raise_exc

            proxy_server.http_connect = fake_http_connect(*statuses, **kwargs)
            self.app.memcache.store = {}
            req = Request.blank('/a/c/o', headers={'Content-Length': '0',
                    'Content-Type': 'text/plain'})
            self.app.update_request(req)
            res = method(req)
            self.assertEquals(res.status_int, expected)

            # repeat test
            proxy_server.http_connect = fake_http_connect(*statuses, **kwargs)
            self.app.memcache.store = {}
            req = Request.blank('/a/c/o', headers={'Content-Length': '0',
                    'Content-Type': 'text/plain'})
            self.app.update_request(req)
            res = method(req)
            self.assertEquals(res.status_int, expected)

    def test_PUT_auto_content_type(self):
        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')

            def test_content_type(filename, expected):
                proxy_server.http_connect = fake_http_connect(201, 201, 201,
                    give_content_type=lambda content_type:
                        self.assertEquals(content_type, expected.next()))
                req = Request.blank('/a/c/%s' % filename, {})
                self.app.update_request(req)
                res = controller.PUT(req)
            test_content_type('test.jpg', iter(['', '', '', 'image/jpeg',
                                                'image/jpeg', 'image/jpeg']))
            test_content_type('test.html', iter(['', '', '', 'text/html',
                                                 'text/html', 'text/html']))
            test_content_type('test.css', iter(['', '', '', 'text/css',
                                                'text/css', 'text/css']))

    def test_custom_mime_types_files(self):
        swift_dir = mkdtemp()
        try:
            with open(os.path.join(swift_dir, 'mime.types'), 'w') as fp:
                fp.write('foo/bar foo\n')
            ba = proxy_server.BaseApplication({'swift_dir': swift_dir},
                FakeMemcache(), NullLoggingHandler(), FakeRing(), FakeRing(),
                FakeRing())
            self.assertEquals(proxy_server.mimetypes.guess_type('blah.foo')[0],
                              'foo/bar')
            self.assertEquals(proxy_server.mimetypes.guess_type('blah.jpg')[0],
                              'image/jpeg')
        finally:
            rmtree(swift_dir, ignore_errors=True)

    def test_PUT(self):
        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')

            def test_status_map(statuses, expected):
                proxy_server.http_connect = fake_http_connect(*statuses)
                req = Request.blank('/a/c/o.jpg', {})
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

    def test_PUT_connect_exceptions(self):

        def mock_http_connect(*code_iter, **kwargs):

            class FakeConn(object):

                def __init__(self, status):
                    self.status = status
                    self.reason = 'Fake'

                def getresponse(self):
                    return self

                def read(self, amt=None):
                    return ''

                def getheader(self, name):
                    return ''

                def getexpect(self):
                    if self.status == -2:
                        raise HTTPException()
                    if self.status == -3:
                        return FakeConn(507)
                    return FakeConn(100)

            code_iter = iter(code_iter)

            def connect(*args, **ckwargs):
                status = code_iter.next()
                if status == -1:
                    raise HTTPException()
                return FakeConn(status)

            return connect

        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')

            def test_status_map(statuses, expected):
                proxy_server.http_connect = mock_http_connect(*statuses)
                self.app.memcache.store = {}
                req = Request.blank('/a/c/o.jpg', {})
                req.content_length = 0
                self.app.update_request(req)
                res = controller.PUT(req)
                expected = str(expected)
                self.assertEquals(res.status[:len(expected)], expected)
            test_status_map((200, 200, 201, 201, -1), 201)
            test_status_map((200, 200, 201, 201, -2), 201)  # expect timeout
            test_status_map((200, 200, 201, 201, -3), 201)  # error limited
            test_status_map((200, 200, 201, -1, -1), 503)
            test_status_map((200, 200, 503, 503, -1), 503)

    def test_PUT_send_exceptions(self):

        def mock_http_connect(*code_iter, **kwargs):

            class FakeConn(object):

                def __init__(self, status):
                    self.status = status
                    self.reason = 'Fake'
                    self.host = '1.2.3.4'
                    self.port = 1024
                    self.etag = md5()

                def getresponse(self):
                    self.etag = self.etag.hexdigest()
                    self.headers = {
                        'etag': self.etag,
                    }
                    return self

                def read(self, amt=None):
                    return ''

                def send(self, amt=None):
                    if self.status == -1:
                        raise HTTPException()
                    else:
                        self.etag.update(amt)

                def getheader(self, name):
                    return self.headers.get(name, '')

                def getexpect(self):
                    return FakeConn(100)
            code_iter = iter(code_iter)

            def connect(*args, **ckwargs):
                return FakeConn(code_iter.next())
            return connect
        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')

            def test_status_map(statuses, expected):
                self.app.memcache.store = {}
                proxy_server.http_connect = mock_http_connect(*statuses)
                req = Request.blank('/a/c/o.jpg',
                    environ={'REQUEST_METHOD': 'PUT'}, body='some data')
                self.app.update_request(req)
                res = controller.PUT(req)
                expected = str(expected)
                self.assertEquals(res.status[:len(expected)], expected)
            test_status_map((200, 200, 201, -1, 201), 201)
            test_status_map((200, 200, 201, -1, -1), 503)
            test_status_map((200, 200, 503, 503, -1), 503)

    def test_PUT_max_size(self):
        with save_globals():
            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            req = Request.blank('/a/c/o', {}, headers={
                'Content-Length': str(MAX_FILE_SIZE + 1),
                'Content-Type': 'foo/bar'})
            self.app.update_request(req)
            res = controller.PUT(req)
            self.assertEquals(res.status_int, 413)

    def test_PUT_getresponse_exceptions(self):

        def mock_http_connect(*code_iter, **kwargs):

            class FakeConn(object):

                def __init__(self, status):
                    self.status = status
                    self.reason = 'Fake'
                    self.host = '1.2.3.4'
                    self.port = 1024

                def getresponse(self):
                    if self.status == -1:
                        raise HTTPException()
                    return self

                def read(self, amt=None):
                    return ''

                def send(self, amt=None):
                    pass

                def getheader(self, name):
                    return ''

                def getexpect(self):
                    return FakeConn(100)
            code_iter = iter(code_iter)

            def connect(*args, **ckwargs):
                return FakeConn(code_iter.next())
            return connect
        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')

            def test_status_map(statuses, expected):
                self.app.memcache.store = {}
                proxy_server.http_connect = mock_http_connect(*statuses)
                req = Request.blank('/a/c/o.jpg', {})
                req.content_length = 0
                self.app.update_request(req)
                res = controller.PUT(req)
                expected = str(expected)
                self.assertEquals(res.status[:len(str(expected))],
                                  str(expected))
            test_status_map((200, 200, 201, 201, -1), 201)
            test_status_map((200, 200, 201, -1, -1), 503)
            test_status_map((200, 200, 503, 503, -1), 503)

    def test_POST(self):
        with save_globals():
            self.app.object_post_as_copy = False
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')

            def test_status_map(statuses, expected):
                proxy_server.http_connect = fake_http_connect(*statuses)
                self.app.memcache.store = {}
                req = Request.blank('/a/c/o', {}, headers={
                                                'Content-Type': 'foo/bar'})
                self.app.update_request(req)
                res = controller.POST(req)
                expected = str(expected)
                self.assertEquals(res.status[:len(expected)], expected)
            test_status_map((200, 200, 202, 202, 202), 202)
            test_status_map((200, 200, 202, 202, 500), 202)
            test_status_map((200, 200, 202, 500, 500), 503)
            test_status_map((200, 200, 202, 404, 500), 503)
            test_status_map((200, 200, 202, 404, 404), 404)
            test_status_map((200, 200, 404, 500, 500), 503)
            test_status_map((200, 200, 404, 404, 404), 404)

    def test_POST_as_copy(self):
        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')

            def test_status_map(statuses, expected):
                proxy_server.http_connect = fake_http_connect(*statuses)
                self.app.memcache.store = {}
                req = Request.blank('/a/c/o', {}, headers={
                                                'Content-Type': 'foo/bar'})
                self.app.update_request(req)
                res = controller.POST(req)
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
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')

            def test_status_map(statuses, expected):
                proxy_server.http_connect = fake_http_connect(*statuses)
                self.app.memcache.store = {}
                req = Request.blank('/a/c/o', {})
                self.app.update_request(req)
                res = controller.DELETE(req)
                self.assertEquals(res.status[:len(str(expected))],
                                  str(expected))
            test_status_map((200, 200, 204, 204, 204), 204)
            test_status_map((200, 200, 204, 204, 500), 204)
            test_status_map((200, 200, 204, 404, 404), 404)
            test_status_map((200, 200, 204, 500, 404), 503)
            test_status_map((200, 200, 404, 404, 404), 404)
            test_status_map((200, 200, 404, 404, 500), 404)

    def test_HEAD(self):
        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')

            def test_status_map(statuses, expected):
                proxy_server.http_connect = fake_http_connect(*statuses)
                self.app.memcache.store = {}
                req = Request.blank('/a/c/o', {})
                self.app.update_request(req)
                res = controller.HEAD(req)
                self.assertEquals(res.status[:len(str(expected))],
                                  str(expected))
                if expected < 400:
                    self.assert_('x-works' in res.headers)
                    self.assertEquals(res.headers['x-works'], 'yes')
                    self.assert_('accept-ranges' in res.headers)
                    self.assertEquals(res.headers['accept-ranges'], 'bytes')

            test_status_map((200, 404, 404), 200)
            test_status_map((200, 500, 404), 200)
            test_status_map((304, 500, 404), 304)
            test_status_map((404, 404, 404), 404)
            test_status_map((404, 404, 500), 404)
            test_status_map((500, 500, 500), 503)

    def test_HEAD_newest(self):
        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')

            def test_status_map(statuses, expected, timestamps,
                                expected_timestamp):
                proxy_server.http_connect = \
                    fake_http_connect(*statuses, timestamps=timestamps)
                self.app.memcache.store = {}
                req = Request.blank('/a/c/o', {}, headers={'x-newest': 'true'})
                self.app.update_request(req)
                res = controller.HEAD(req)
                self.assertEquals(res.status[:len(str(expected))],
                                  str(expected))
                self.assertEquals(res.headers.get('last-modified'),
                                  expected_timestamp)

            test_status_map((200, 200, 200), 200, ('1', '2', '3'), '3')
            test_status_map((200, 200, 200), 200, ('1', '3', '2'), '3')
            test_status_map((200, 200, 200), 200, ('1', '3', '1'), '3')
            test_status_map((200, 200, 200), 200, ('3', '3', '1'), '3')
            test_status_map((200, 200, 200), 200, (None, None, None), None)
            test_status_map((200, 200, 200), 200, (None, None, '1'), '1')

    def test_GET_newest(self):
        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')

            def test_status_map(statuses, expected, timestamps,
                                expected_timestamp):
                proxy_server.http_connect = \
                    fake_http_connect(*statuses, timestamps=timestamps)
                self.app.memcache.store = {}
                req = Request.blank('/a/c/o', {}, headers={'x-newest': 'true'})
                self.app.update_request(req)
                res = controller.GET(req)
                self.assertEquals(res.status[:len(str(expected))],
                                  str(expected))
                self.assertEquals(res.headers.get('last-modified'),
                                  expected_timestamp)

            test_status_map((200, 200, 200), 200, ('1', '2', '3'), '3')
            test_status_map((200, 200, 200), 200, ('1', '3', '2'), '3')
            test_status_map((200, 200, 200), 200, ('1', '3', '1'), '3')
            test_status_map((200, 200, 200), 200, ('3', '3', '1'), '3')
            test_status_map((200, 200, 200), 200, (None, None, None), None)
            test_status_map((200, 200, 200), 200, (None, None, '1'), '1')

        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')

            def test_status_map(statuses, expected, timestamps,
                                expected_timestamp):
                proxy_server.http_connect = \
                    fake_http_connect(*statuses, timestamps=timestamps)
                self.app.memcache.store = {}
                req = Request.blank('/a/c/o', {})
                self.app.update_request(req)
                res = controller.HEAD(req)
                self.assertEquals(res.status[:len(str(expected))],
                                  str(expected))
                self.assertEquals(res.headers.get('last-modified'),
                                  expected_timestamp)

            test_status_map((200, 200, 200), 200, ('1', '2', '3'), '1')
            test_status_map((200, 200, 200), 200, ('1', '3', '2'), '1')
            test_status_map((200, 200, 200), 200, ('1', '3', '1'), '1')
            test_status_map((200, 200, 200), 200, ('3', '3', '1'), '3')
            test_status_map((200, 200, 200), 200, (None, '1', '2'), None)

    def test_POST_meta_val_len(self):
        with save_globals():
            self.app.object_post_as_copy = False
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 202, 202, 202)
                #                 acct cont obj  obj  obj
            req = Request.blank('/a/c/o', {}, headers={
                                            'Content-Type': 'foo/bar',
                                            'X-Object-Meta-Foo': 'x' * 256})
            self.app.update_request(req)
            res = controller.POST(req)
            self.assertEquals(res.status_int, 202)
            proxy_server.http_connect = fake_http_connect(202, 202, 202)
            req = Request.blank('/a/c/o', {}, headers={
                                            'Content-Type': 'foo/bar',
                                            'X-Object-Meta-Foo': 'x' * 257})
            self.app.update_request(req)
            res = controller.POST(req)
            self.assertEquals(res.status_int, 400)

    def test_POST_as_copy_meta_val_len(self):
        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, 200, 200, 202, 202, 202)
                #                 acct cont objc objc objc obj  obj  obj
            req = Request.blank('/a/c/o', {}, headers={
                                            'Content-Type': 'foo/bar',
                                            'X-Object-Meta-Foo': 'x' * 256})
            self.app.update_request(req)
            res = controller.POST(req)
            self.assertEquals(res.status_int, 202)
            proxy_server.http_connect = fake_http_connect(202, 202, 202)
            req = Request.blank('/a/c/o', {}, headers={
                                            'Content-Type': 'foo/bar',
                                            'X-Object-Meta-Foo': 'x' * 257})
            self.app.update_request(req)
            res = controller.POST(req)
            self.assertEquals(res.status_int, 400)

    def test_POST_meta_key_len(self):
        with save_globals():
            self.app.object_post_as_copy = False
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 202, 202, 202)
                #                 acct cont obj  obj  obj
            req = Request.blank('/a/c/o', {}, headers={
                'Content-Type': 'foo/bar',
                ('X-Object-Meta-' + 'x' * 128): 'x'})
            self.app.update_request(req)
            res = controller.POST(req)
            self.assertEquals(res.status_int, 202)
            proxy_server.http_connect = fake_http_connect(202, 202, 202)
            req = Request.blank('/a/c/o', {}, headers={
                'Content-Type': 'foo/bar',
                ('X-Object-Meta-' + 'x' * 129): 'x'})
            self.app.update_request(req)
            res = controller.POST(req)
            self.assertEquals(res.status_int, 400)

    def test_POST_as_copy_meta_key_len(self):
        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, 200, 200, 202, 202, 202)
                #                 acct cont objc objc objc obj  obj  obj
            req = Request.blank('/a/c/o', {}, headers={
                'Content-Type': 'foo/bar',
                ('X-Object-Meta-' + 'x' * 128): 'x'})
            self.app.update_request(req)
            res = controller.POST(req)
            self.assertEquals(res.status_int, 202)
            proxy_server.http_connect = fake_http_connect(202, 202, 202)
            req = Request.blank('/a/c/o', {}, headers={
                'Content-Type': 'foo/bar',
                ('X-Object-Meta-' + 'x' * 129): 'x'})
            self.app.update_request(req)
            res = controller.POST(req)
            self.assertEquals(res.status_int, 400)

    def test_POST_meta_count(self):
        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            headers = dict(
                (('X-Object-Meta-' + str(i), 'a') for i in xrange(91)))
            headers.update({'Content-Type': 'foo/bar'})
            proxy_server.http_connect = fake_http_connect(202, 202, 202)
            req = Request.blank('/a/c/o', {}, headers=headers)
            self.app.update_request(req)
            res = controller.POST(req)
            self.assertEquals(res.status_int, 400)

    def test_POST_meta_size(self):
        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            headers = dict(
                (('X-Object-Meta-' + str(i), 'a' * 256) for i in xrange(1000)))
            headers.update({'Content-Type': 'foo/bar'})
            proxy_server.http_connect = fake_http_connect(202, 202, 202)
            req = Request.blank('/a/c/o', {}, headers=headers)
            self.app.update_request(req)
            res = controller.POST(req)
            self.assertEquals(res.status_int, 400)

    def test_client_timeout(self):
        with save_globals():
            self.app.account_ring.get_nodes('account')
            for dev in self.app.account_ring.devs.values():
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            self.app.container_ring.get_nodes('account')
            for dev in self.app.container_ring.devs.values():
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            self.app.object_ring.get_nodes('account')
            for dev in self.app.object_ring.devs.values():
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1

            class SlowBody():

                def __init__(self):
                    self.sent = 0

                def read(self, size=-1):
                    if self.sent < 4:
                        sleep(0.1)
                        self.sent += 1
                        return ' '
                    return ''

            req = Request.blank('/a/c/o',
                environ={'REQUEST_METHOD': 'PUT', 'wsgi.input': SlowBody()},
                headers={'Content-Length': '4', 'Content-Type': 'text/plain'})
            self.app.update_request(req)
            controller = proxy_server.ObjectController(self.app, 'account',
                                                       'container', 'object')
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 201, 201, 201)
                #                 acct cont obj  obj  obj
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
            self.app.client_timeout = 0.1
            req = Request.blank('/a/c/o',
                environ={'REQUEST_METHOD': 'PUT', 'wsgi.input': SlowBody()},
                headers={'Content-Length': '4', 'Content-Type': 'text/plain'})
            self.app.update_request(req)
            proxy_server.http_connect = \
                fake_http_connect(201, 201, 201)
                #                 obj  obj  obj
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 408)

    def test_client_disconnect(self):
        with save_globals():
            self.app.account_ring.get_nodes('account')
            for dev in self.app.account_ring.devs.values():
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            self.app.container_ring.get_nodes('account')
            for dev in self.app.container_ring.devs.values():
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            self.app.object_ring.get_nodes('account')
            for dev in self.app.object_ring.devs.values():
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1

            class SlowBody():

                def __init__(self):
                    self.sent = 0

                def read(self, size=-1):
                    raise Exception('Disconnected')

            req = Request.blank('/a/c/o',
                environ={'REQUEST_METHOD': 'PUT', 'wsgi.input': SlowBody()},
                headers={'Content-Length': '4', 'Content-Type': 'text/plain'})
            self.app.update_request(req)
            controller = proxy_server.ObjectController(self.app, 'account',
                                                       'container', 'object')
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 201, 201, 201)
                #                 acct cont obj  obj  obj
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 499)

    def test_node_read_timeout(self):
        with save_globals():
            self.app.account_ring.get_nodes('account')
            for dev in self.app.account_ring.devs.values():
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            self.app.container_ring.get_nodes('account')
            for dev in self.app.container_ring.devs.values():
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            self.app.object_ring.get_nodes('account')
            for dev in self.app.object_ring.devs.values():
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'GET'})
            self.app.update_request(req)
            controller = proxy_server.ObjectController(self.app, 'account',
                                                       'container', 'object')
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, slow=True)
            req.sent_size = 0
            resp = controller.GET(req)
            got_exc = False
            try:
                resp.body
            except proxy_server.ChunkReadTimeout:
                got_exc = True
            self.assert_(not got_exc)
            self.app.node_timeout = 0.1
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, slow=True)
            resp = controller.GET(req)
            got_exc = False
            try:
                resp.body
            except proxy_server.ChunkReadTimeout:
                got_exc = True
            self.assert_(got_exc)

    def test_node_write_timeout(self):
        with save_globals():
            self.app.account_ring.get_nodes('account')
            for dev in self.app.account_ring.devs.values():
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            self.app.container_ring.get_nodes('account')
            for dev in self.app.container_ring.devs.values():
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            self.app.object_ring.get_nodes('account')
            for dev in self.app.object_ring.devs.values():
                dev['ip'] = '127.0.0.1'
                dev['port'] = 1
            req = Request.blank('/a/c/o',
                environ={'REQUEST_METHOD': 'PUT'},
                headers={'Content-Length': '4', 'Content-Type': 'text/plain'},
                body='    ')
            self.app.update_request(req)
            controller = proxy_server.ObjectController(self.app, 'account',
                                                       'container', 'object')
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 201, 201, 201, slow=True)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
            self.app.node_timeout = 0.1
            proxy_server.http_connect = \
                fake_http_connect(201, 201, 201, slow=True)
            req = Request.blank('/a/c/o',
                environ={'REQUEST_METHOD': 'PUT'},
                headers={'Content-Length': '4', 'Content-Type': 'text/plain'},
                body='    ')
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 503)

    def test_iter_nodes(self):
        with save_globals():
            try:
                self.app.object_ring.max_more_nodes = 2
                controller = proxy_server.ObjectController(self.app, 'account',
                                'container', 'object')
                partition, nodes = self.app.object_ring.get_nodes('account',
                                    'container', 'object')
                collected_nodes = []
                for node in controller.iter_nodes(partition, nodes,
                                                  self.app.object_ring):
                    collected_nodes.append(node)
                self.assertEquals(len(collected_nodes), 5)

                self.app.object_ring.max_more_nodes = 20
                controller = proxy_server.ObjectController(self.app, 'account',
                                'container', 'object')
                partition, nodes = self.app.object_ring.get_nodes('account',
                                    'container', 'object')
                collected_nodes = []
                for node in controller.iter_nodes(partition, nodes,
                                                  self.app.object_ring):
                    collected_nodes.append(node)
                self.assertEquals(len(collected_nodes), 9)
            finally:
                self.app.object_ring.max_more_nodes = 0

    def test_best_response_sets_etag(self):
        controller = proxy_server.ObjectController(self.app, 'account',
                                                   'container', 'object')
        req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'GET'})
        resp = controller.best_response(req, [200] * 3, ['OK'] * 3, [''] * 3,
                                        'Object')
        self.assertEquals(resp.etag, None)
        resp = controller.best_response(req, [200] * 3, ['OK'] * 3, [''] * 3,
            'Object', etag='68b329da9893e34099c7d8ad5cb9c940')
        self.assertEquals(resp.etag, '68b329da9893e34099c7d8ad5cb9c940')

    def test_proxy_passes_content_type(self):
        with save_globals():
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'GET'})
            self.app.update_request(req)
            controller = proxy_server.ObjectController(self.app, 'account',
                                                       'container', 'object')
            proxy_server.http_connect = fake_http_connect(200, 200, 200)
            resp = controller.GET(req)
            self.assertEquals(resp.status_int, 200)
            self.assertEquals(resp.content_type, 'x-application/test')
            proxy_server.http_connect = fake_http_connect(200, 200, 200)
            resp = controller.GET(req)
            self.assertEquals(resp.status_int, 200)
            self.assertEquals(resp.content_length, 0)
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, slow=True)
            resp = controller.GET(req)
            self.assertEquals(resp.status_int, 200)
            self.assertEquals(resp.content_length, 4)

    def test_proxy_passes_content_length_on_head(self):
        with save_globals():
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'HEAD'})
            self.app.update_request(req)
            controller = proxy_server.ObjectController(self.app, 'account',
                                                       'container', 'object')
            proxy_server.http_connect = fake_http_connect(200, 200, 200)
            resp = controller.HEAD(req)
            self.assertEquals(resp.status_int, 200)
            self.assertEquals(resp.content_length, 0)
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, slow=True)
            resp = controller.HEAD(req)
            self.assertEquals(resp.status_int, 200)
            self.assertEquals(resp.content_length, 4)

    def test_error_limiting(self):
        with save_globals():
            proxy_server.shuffle = lambda l: None
            controller = proxy_server.ObjectController(self.app, 'account',
                                                       'container', 'object')
            self.assert_status_map(controller.HEAD, (503, 200, 200), 200)
            self.assertEquals(controller.app.object_ring.devs[0]['errors'], 2)
            self.assert_('last_error' in controller.app.object_ring.devs[0])
            for _junk in xrange(self.app.error_suppression_limit):
                self.assert_status_map(controller.HEAD, (503, 503, 503), 503)
            self.assertEquals(controller.app.object_ring.devs[0]['errors'],
                              self.app.error_suppression_limit + 1)
            self.assert_status_map(controller.HEAD, (200, 200, 200), 503)
            self.assert_('last_error' in controller.app.object_ring.devs[0])
            self.assert_status_map(controller.PUT, (200, 201, 201, 201), 503)
            self.assert_status_map(controller.POST,
                                   (200, 200, 200, 200, 202, 202, 202), 503)
            self.assert_status_map(controller.DELETE,
                                   (200, 204, 204, 204), 503)
            self.app.error_suppression_interval = -300
            self.assert_status_map(controller.HEAD, (200, 200, 200), 200)
            self.assertRaises(BaseException,
                self.assert_status_map, controller.DELETE,
                (200, 204, 204, 204), 503, raise_exc=True)

    def test_acc_or_con_missing_returns_404(self):
        with save_globals():
            self.app.memcache = FakeMemcacheReturnsNone()
            for dev in self.app.account_ring.devs.values():
                del dev['errors']
                del dev['last_error']
            for dev in self.app.container_ring.devs.values():
                del dev['errors']
                del dev['last_error']
            controller = proxy_server.ObjectController(self.app, 'account',
                                                     'container', 'object')
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, 200, 200, 200)
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'DELETE'})
            self.app.update_request(req)
            resp = getattr(controller, 'DELETE')(req)
            self.assertEquals(resp.status_int, 200)

            proxy_server.http_connect = \
                fake_http_connect(404, 404, 404)
                #                 acct acct acct
            resp = getattr(controller, 'DELETE')(req)
            self.assertEquals(resp.status_int, 404)

            proxy_server.http_connect = \
                fake_http_connect(503, 404, 404)
                #                 acct acct acct
            resp = getattr(controller, 'DELETE')(req)
            self.assertEquals(resp.status_int, 404)

            proxy_server.http_connect = \
                fake_http_connect(503, 503, 404)
                #                 acct acct acct
            resp = getattr(controller, 'DELETE')(req)
            self.assertEquals(resp.status_int, 404)

            proxy_server.http_connect = \
                fake_http_connect(503, 503, 503)
                #                 acct acct acct
            resp = getattr(controller, 'DELETE')(req)
            self.assertEquals(resp.status_int, 404)

            proxy_server.http_connect = \
                fake_http_connect(200, 200, 204, 204, 204)
                #                 acct cont obj  obj  obj
            resp = getattr(controller, 'DELETE')(req)
            self.assertEquals(resp.status_int, 204)

            proxy_server.http_connect = \
                fake_http_connect(200, 404, 404, 404)
                #                 acct cont cont cont
            resp = getattr(controller, 'DELETE')(req)
            self.assertEquals(resp.status_int, 404)

            proxy_server.http_connect = \
                fake_http_connect(200, 503, 503, 503)
                #                 acct cont cont cont
            resp = getattr(controller, 'DELETE')(req)
            self.assertEquals(resp.status_int, 404)

            for dev in self.app.account_ring.devs.values():
                dev['errors'] = self.app.error_suppression_limit + 1
                dev['last_error'] = time()
            proxy_server.http_connect = \
                fake_http_connect(200)
                #                 acct [isn't actually called since everything
                #                       is error limited]
            resp = getattr(controller, 'DELETE')(req)
            self.assertEquals(resp.status_int, 404)

            for dev in self.app.account_ring.devs.values():
                dev['errors'] = 0
            for dev in self.app.container_ring.devs.values():
                dev['errors'] = self.app.error_suppression_limit + 1
                dev['last_error'] = time()
            proxy_server.http_connect = \
                fake_http_connect(200, 200)
                #                 acct cont [isn't actually called since
                #                            everything is error limited]
            resp = getattr(controller, 'DELETE')(req)
            self.assertEquals(resp.status_int, 404)

    def test_PUT_POST_requires_container_exist(self):
        with save_globals():
            self.app.object_post_as_copy = False
            self.app.memcache = FakeMemcacheReturnsNone()
            controller = proxy_server.ObjectController(self.app, 'account',
                                                       'container', 'object')

            proxy_server.http_connect = \
                fake_http_connect(200, 404, 404, 404, 200, 200, 200)
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'})
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 404)

            proxy_server.http_connect = \
                fake_http_connect(200, 404, 404, 404, 200, 200)
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'POST'},
                                headers={'Content-Type': 'text/plain'})
            self.app.update_request(req)
            resp = controller.POST(req)
            self.assertEquals(resp.status_int, 404)

    def test_PUT_POST_as_copy_requires_container_exist(self):
        with save_globals():
            self.app.memcache = FakeMemcacheReturnsNone()
            controller = proxy_server.ObjectController(self.app, 'account',
                                                       'container', 'object')
            proxy_server.http_connect = \
                fake_http_connect(200, 404, 404, 404, 200, 200, 200)
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'})
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 404)

            proxy_server.http_connect = \
                fake_http_connect(200, 404, 404, 404, 200, 200, 200, 200, 200,
                                  200)
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'POST'},
                                headers={'Content-Type': 'text/plain'})
            self.app.update_request(req)
            resp = controller.POST(req)
            self.assertEquals(resp.status_int, 404)

    def test_bad_metadata(self):
        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'account',
                                                       'container', 'object')
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 201, 201, 201)
                #                 acct cont obj  obj  obj
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '0'})
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 201)

            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={'Content-Length': '0',
                         'X-Object-Meta-' + ('a' *
                            MAX_META_NAME_LENGTH): 'v'})
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={'Content-Length': '0',
                         'X-Object-Meta-' + ('a' *
                            (MAX_META_NAME_LENGTH + 1)): 'v'})
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 400)

            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={'Content-Length': '0',
                         'X-Object-Meta-Too-Long': 'a' *
                            MAX_META_VALUE_LENGTH})
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                headers={'Content-Length': '0',
                         'X-Object-Meta-Too-Long': 'a' *
                            (MAX_META_VALUE_LENGTH + 1)})
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 400)

            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            headers = {'Content-Length': '0'}
            for x in xrange(MAX_META_COUNT):
                headers['X-Object-Meta-%d' % x] = 'v'
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers=headers)
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            headers = {'Content-Length': '0'}
            for x in xrange(MAX_META_COUNT + 1):
                headers['X-Object-Meta-%d' % x] = 'v'
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers=headers)
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 400)

            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            headers = {'Content-Length': '0'}
            header_value = 'a' * MAX_META_VALUE_LENGTH
            size = 0
            x = 0
            while size < MAX_META_OVERALL_SIZE - 4 - \
                    MAX_META_VALUE_LENGTH:
                size += 4 + MAX_META_VALUE_LENGTH
                headers['X-Object-Meta-%04d' % x] = header_value
                x += 1
            if MAX_META_OVERALL_SIZE - size > 1:
                headers['X-Object-Meta-a'] = \
                    'a' * (MAX_META_OVERALL_SIZE - size - 1)
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers=headers)
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            headers['X-Object-Meta-a'] = \
                'a' * (MAX_META_OVERALL_SIZE - size)
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers=headers)
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 400)

    def test_copy_from(self):
        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'account',
                                                       'container', 'object')
            # initial source object PUT
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '0'})
            self.app.update_request(req)
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 201, 201, 201)
                #                 acct cont obj  obj  obj
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 201)

            # basic copy
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '0',
                                          'X-Copy-From': 'c/o'})
            self.app.update_request(req)
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, 200, 200, 200, 200, 201, 201,
                    201)
                #                 acct cont acct cont objc objc objc obj  obj
                #   obj
            self.app.memcache.store = {}
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
            self.assertEquals(resp.headers['x-copied-from'], 'c/o')

            # non-zero content length
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '5',
                                          'X-Copy-From': 'c/o'})
            self.app.update_request(req)
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, 200, 200, 200, 200)
                #                 acct cont acct cont objc objc objc
            self.app.memcache.store = {}
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 400)

            # extra source path parsing
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '0',
                                          'X-Copy-From': 'c/o/o2'})
            req.account = 'a'
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, 200, 200, 200, 200, 201, 201,
                    201)
                #                 acct cont acct cont objc objc objc obj  obj
                #   obj
            self.app.memcache.store = {}
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
            self.assertEquals(resp.headers['x-copied-from'], 'c/o/o2')

            # space in soure path
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '0',
                                          'X-Copy-From': 'c/o%20o2'})
            req.account = 'a'
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, 200, 200, 200, 200, 201, 201,
                    201)
                #                 acct cont acct cont objc objc objc obj  obj
                #   obj
            self.app.memcache.store = {}
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
            self.assertEquals(resp.headers['x-copied-from'], 'c/o%20o2')

            # repeat tests with leading /
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '0',
                                          'X-Copy-From': '/c/o'})
            self.app.update_request(req)
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, 200, 200, 200, 200, 201, 201,
                    201)
                #                 acct cont acct cont objc objc objc obj  obj
                #   obj
            self.app.memcache.store = {}
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
            self.assertEquals(resp.headers['x-copied-from'], 'c/o')

            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '0',
                                          'X-Copy-From': '/c/o/o2'})
            req.account = 'a'
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, 200, 200, 200, 200, 201, 201,
                    201)
                #                 acct cont acct cont objc objc objc obj  obj
                #   obj
            self.app.memcache.store = {}
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
            self.assertEquals(resp.headers['x-copied-from'], 'c/o/o2')

            # negative tests

            # invalid x-copy-from path
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '0',
                                          'X-Copy-From': '/c'})
            self.app.update_request(req)
            self.app.memcache.store = {}
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int // 100, 4)  # client error

            # server error
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '0',
                                          'X-Copy-From': '/c/o'})
            self.app.update_request(req)
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 503, 503, 503)
                #                 acct cont objc objc objc
            self.app.memcache.store = {}
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 503)

            # not found
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '0',
                                          'X-Copy-From': '/c/o'})
            self.app.update_request(req)
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 404, 404, 404)
                #                 acct cont objc objc objc
            self.app.memcache.store = {}
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 404)

            # some missing containers
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '0',
                                          'X-Copy-From': '/c/o'})
            self.app.update_request(req)
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 404, 404, 200, 201, 201, 201)
                #                 acct cont objc objc objc obj  obj  obj
            self.app.memcache.store = {}
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 201)

            # test object meta data
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '0',
                                          'X-Copy-From': '/c/o',
                                          'X-Object-Meta-Ours': 'okay'})
            self.app.update_request(req)
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, 200, 200, 201, 201, 201)
                #                 acct cont objc objc objc obj  obj  obj
            self.app.memcache.store = {}
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 201)
            self.assertEquals(resp.headers.get('x-object-meta-test'),
                              'testing')
            self.assertEquals(resp.headers.get('x-object-meta-ours'), 'okay')

    def test_COPY(self):
        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'a', 'c', 'o')
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '0'})
            req.account = 'a'
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 201, 201, 201)
                #                 acct cont obj  obj  obj
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 201)

            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'COPY'},
                                headers={'Destination': 'c/o'})
            req.account = 'a'
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, 200, 200, 200, 200, 201, 201,
                    201)
                #                 acct cont acct cont objc objc objc obj  obj
                #   obj
            self.app.memcache.store = {}
            resp = controller.COPY(req)
            self.assertEquals(resp.status_int, 201)
            self.assertEquals(resp.headers['x-copied-from'], 'c/o')

            req = Request.blank('/a/c/o/o2',
                                environ={'REQUEST_METHOD': 'COPY'},
                                headers={'Destination': 'c/o'})
            req.account = 'a'
            controller.object_name = 'o/o2'
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, 200, 200, 200, 200, 201, 201,
                    201)
                #                 acct cont acct cont objc objc objc obj  obj
                #   obj
            self.app.memcache.store = {}
            resp = controller.COPY(req)
            self.assertEquals(resp.status_int, 201)
            self.assertEquals(resp.headers['x-copied-from'], 'c/o/o2')

            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'COPY'},
                                headers={'Destination': '/c/o'})
            req.account = 'a'
            controller.object_name = 'o'
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, 200, 200, 200, 200, 201, 201,
                    201)
                #                 acct cont acct cont objc objc objc obj  obj
                #   obj
            self.app.memcache.store = {}
            resp = controller.COPY(req)
            self.assertEquals(resp.status_int, 201)
            self.assertEquals(resp.headers['x-copied-from'], 'c/o')

            req = Request.blank('/a/c/o/o2',
                                environ={'REQUEST_METHOD': 'COPY'},
                                headers={'Destination': '/c/o'})
            req.account = 'a'
            controller.object_name = 'o/o2'
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, 200, 200, 200, 200, 201, 201,
                    201)
                #                 acct cont acct cont objc objc objc obj  obj
                #   obj
            self.app.memcache.store = {}
            resp = controller.COPY(req)
            self.assertEquals(resp.status_int, 201)
            self.assertEquals(resp.headers['x-copied-from'], 'c/o/o2')

            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'COPY'},
                                headers={'Destination': 'c_o'})
            req.account = 'a'
            controller.object_name = 'o'
            proxy_server.http_connect = \
                fake_http_connect(200, 200)
                #                 acct cont
            self.app.memcache.store = {}
            resp = controller.COPY(req)
            self.assertEquals(resp.status_int, 412)

            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'COPY'},
                                headers={'Destination': '/c/o'})
            req.account = 'a'
            controller.object_name = 'o'
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 503, 503, 503)
                #                 acct cont objc objc objc
            self.app.memcache.store = {}
            resp = controller.COPY(req)
            self.assertEquals(resp.status_int, 503)

            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'COPY'},
                                headers={'Destination': '/c/o'})
            req.account = 'a'
            controller.object_name = 'o'
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 404, 404, 404)
                #                 acct cont objc objc objc
            self.app.memcache.store = {}
            resp = controller.COPY(req)
            self.assertEquals(resp.status_int, 404)

            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'COPY'},
                                headers={'Destination': '/c/o'})
            req.account = 'a'
            controller.object_name = 'o'
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 404, 404, 200, 201, 201, 201)
                #                 acct cont objc objc objc obj  obj  obj
            self.app.memcache.store = {}
            resp = controller.COPY(req)
            self.assertEquals(resp.status_int, 201)

            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'COPY'},
                                headers={'Destination': '/c/o',
                                          'X-Object-Meta-Ours': 'okay'})
            req.account = 'a'
            controller.object_name = 'o'
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, 200, 200, 201, 201, 201)
                #                 acct cont objc objc objc obj  obj  obj
            self.app.memcache.store = {}
            resp = controller.COPY(req)
            self.assertEquals(resp.status_int, 201)
            self.assertEquals(resp.headers.get('x-object-meta-test'),
                              'testing')
            self.assertEquals(resp.headers.get('x-object-meta-ours'), 'okay')

    def test_COPY_newest(self):
        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'a', 'c', 'o')
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'COPY'},
                                headers={'Destination': '/c/o'})
            req.account = 'a'
            controller.object_name = 'o'
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, 200, 200, 201, 201, 201,
                      timestamps=('1', '1', '1', '3', '2', '4', '4', '4'))
                #                 acct cont objc objc objc obj  obj  obj
            self.app.memcache.store = {}
            resp = controller.COPY(req)
            self.assertEquals(resp.status_int, 201)
            self.assertEquals(resp.headers['x-copied-from-last-modified'],
                              '3')

    def test_chunked_put(self):

        class ChunkedFile():

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
            proxy_server.http_connect = fake_http_connect(201, 201, 201, 201)
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'COPY'},
                headers={'Transfer-Encoding': 'chunked',
                'Content-Type': 'foo/bar'})

            req.body_file = ChunkedFile(10)
            self.app.memcache.store = {}
            self.app.update_request(req)
            res = controller.PUT(req)
            self.assertEquals(res.status_int // 100, 2)  # success

            # test 413 entity to large
            from swift.proxy import server
            proxy_server.http_connect = fake_http_connect(201, 201, 201, 201)
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'COPY'},
                headers={'Transfer-Encoding': 'chunked',
                'Content-Type': 'foo/bar'})
            req.body_file = ChunkedFile(11)
            self.app.memcache.store = {}
            self.app.update_request(req)
            try:
                server.MAX_FILE_SIZE = 10
                res = controller.PUT(req)
                self.assertEquals(res.status_int, 413)
            finally:
                server.MAX_FILE_SIZE = MAX_FILE_SIZE

    def test_chunked_put_bad_version(self):
        # Check bad version
        (prolis, acc1lis, acc2lis, con2lis, con2lis, obj1lis, obj2lis) = \
                 _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v0 HTTP/1.1\r\nHost: localhost\r\n'
            'Connection: close\r\nContent-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 412'
        self.assertEquals(headers[:len(exp)], exp)

    def test_chunked_put_bad_path(self):
        # Check bad path
        (prolis, acc1lis, acc2lis, con2lis, con2lis, obj1lis, obj2lis) = \
                 _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET invalid HTTP/1.1\r\nHost: localhost\r\n'
            'Connection: close\r\nContent-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 404'
        self.assertEquals(headers[:len(exp)], exp)

    def test_chunked_put_bad_utf8(self):
        # Check invalid utf-8
        (prolis, acc1lis, acc2lis, con2lis, con2lis, obj1lis, obj2lis) = \
                 _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a%80 HTTP/1.1\r\nHost: localhost\r\n'
            'Connection: close\r\nX-Auth-Token: t\r\n'
            'Content-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 412'
        self.assertEquals(headers[:len(exp)], exp)

    def test_chunked_put_bad_path_no_controller(self):
        # Check bad path, no controller
        (prolis, acc1lis, acc2lis, con2lis, con2lis, obj1lis, obj2lis) = \
                 _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1 HTTP/1.1\r\nHost: localhost\r\n'
            'Connection: close\r\nX-Auth-Token: t\r\n'
            'Content-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 412'
        self.assertEquals(headers[:len(exp)], exp)

    def test_chunked_put_bad_method(self):
        # Check bad method
        (prolis, acc1lis, acc2lis, con2lis, con2lis, obj1lis, obj2lis) = \
                 _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('LICK /v1/a HTTP/1.1\r\nHost: localhost\r\n'
            'Connection: close\r\nX-Auth-Token: t\r\n'
            'Content-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 405'
        self.assertEquals(headers[:len(exp)], exp)

    def test_chunked_put_unhandled_exception(self):
        # Check unhandled exception
        (prosrv, acc1srv, acc2srv, con2srv, con2srv, obj1srv, obj2srv) = \
                _test_servers
        (prolis, acc1lis, acc2lis, con2lis, con2lis, obj1lis, obj2lis) = \
                 _test_sockets
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

    def test_chunked_put_head_account(self):
        # Head account, just a double check and really is here to test
        # the part Application.log_request that 'enforces' a
        # content_length on the response.
        (prolis, acc1lis, acc2lis, con2lis, con2lis, obj1lis, obj2lis) = \
                 _test_sockets
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

    def test_client_ip_logging(self):
        # test that the client ip field in the log gets populated with the
        # ip instead of being blank
        (prosrv, acc1srv, acc2srv, con2srv, con2srv, obj1srv, obj2srv) = \
                _test_servers
        (prolis, acc1lis, acc2lis, con2lis, con2lis, obj1lis, obj2lis) = \
                 _test_sockets

        class Logger(object):

            def info(self, msg):
                self.msg = msg

        orig_logger, orig_access_logger = prosrv.logger, prosrv.access_logger
        prosrv.logger = prosrv.access_logger = Logger()
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write(
            'GET /v1/a?format=json HTTP/1.1\r\nHost: localhost\r\n'
            'Connection: close\r\nX-Auth-Token: t\r\n'
            'Content-Length: 0\r\n'
            '\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        exp = '127.0.0.1 127.0.0.1'
        self.assert_(exp in prosrv.logger.msg)

    def test_chunked_put_logging(self):
        # GET account with a query string to test that
        # Application.log_request logs the query string. Also, throws
        # in a test for logging x-forwarded-for (first entry only).
        (prosrv, acc1srv, acc2srv, con2srv, con2srv, obj1srv, obj2srv) = \
                _test_servers
        (prolis, acc1lis, acc2lis, con2lis, con2lis, obj1lis, obj2lis) = \
                 _test_sockets

        class Logger(object):

            def info(self, msg):
                self.msg = msg

        orig_logger, orig_access_logger = prosrv.logger, prosrv.access_logger
        prosrv.logger = prosrv.access_logger = Logger()
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write(
            'GET /v1/a?format=json HTTP/1.1\r\nHost: localhost\r\n'
            'Connection: close\r\nX-Auth-Token: t\r\n'
            'Content-Length: 0\r\nX-Forwarded-For: host1, host2\r\n'
            '\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        self.assert_('/v1/a%3Fformat%3Djson' in prosrv.logger.msg,
                     prosrv.logger.msg)
        exp = 'host1'
        self.assertEquals(prosrv.logger.msg[:len(exp)], exp)
        # Turn on header logging.

        prosrv.logger = prosrv.access_logger = Logger()
        prosrv.log_headers = True
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a HTTP/1.1\r\nHost: localhost\r\n'
            'Connection: close\r\nX-Auth-Token: t\r\n'
            'Content-Length: 0\r\nGoofy-Header: True\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        self.assert_('Goofy-Header%3A%20True' in prosrv.logger.msg,
                     prosrv.logger.msg)
        prosrv.log_headers = False
        prosrv.logger, prosrv.access_logger = orig_logger, orig_access_logger

    def test_chunked_put_utf8_all_the_way_down(self):
        # Test UTF-8 Unicode all the way through the system
        (prolis, acc1lis, acc2lis, con2lis, con2lis, obj1lis, obj2lis) = \
                 _test_sockets
        ustr = '\xe1\xbc\xb8\xce\xbf\xe1\xbd\xba \xe1\xbc\xb0\xce' \
               '\xbf\xe1\xbd\xbb\xce\x87 \xcf\x84\xe1\xbd\xb0 \xcf' \
               '\x80\xe1\xbd\xb1\xce\xbd\xcf\x84\xca\xbc \xe1\xbc' \
               '\x82\xce\xbd \xe1\xbc\x90\xce\xbe\xe1\xbd\xb5\xce' \
               '\xba\xce\xbf\xce\xb9 \xcf\x83\xce\xb1\xcf\x86\xe1' \
               '\xbf\x86.Test'
        ustr_short = '\xe1\xbc\xb8\xce\xbf\xe1\xbd\xbatest'
        # Create ustr container
        (prolis, acc1lis, acc2lis, con2lis, con2lis, obj1lis, obj2lis) = \
                 _test_sockets
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
        listing = simplejson.loads(fd.read())
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
        listing = simplejson.loads(fd.read())
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

    def test_chunked_put_chunked_put(self):
        # Do chunked object put
        (prolis, acc1lis, acc2lis, con2lis, con2lis, obj1lis, obj2lis) = \
                 _test_sockets
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

    def test_chunked_put_lobjects(self):
        # Create a container for our segmented/manifest object testing
        (prolis, acc1lis, acc2lis, con2lis, con2lis, obj1lis, obj2lis) = \
                 _test_sockets
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/segmented HTTP/1.1\r\nHost: localhost\r\n'
                 'Connection: close\r\nX-Storage-Token: t\r\n'
                 'Content-Length: 0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        # Create the object segments
        segment_etags = []
        for segment in xrange(5):
            sock = connect_tcp(('localhost', prolis.getsockname()[1]))
            fd = sock.makefile()
            fd.write('PUT /v1/a/segmented/name/%s HTTP/1.1\r\nHost: '
                'localhost\r\nConnection: close\r\nX-Storage-Token: '
                't\r\nContent-Length: 5\r\n\r\n1234 ' % str(segment))
            fd.flush()
            headers = readuntil2crlfs(fd)
            exp = 'HTTP/1.1 201'
            self.assertEquals(headers[:len(exp)], exp)
            segment_etags.append(md5('1234 ').hexdigest())
        # Create the object manifest file
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/segmented/name HTTP/1.1\r\nHost: '
            'localhost\r\nConnection: close\r\nX-Storage-Token: '
            't\r\nContent-Length: 0\r\nX-Object-Manifest: '
            'segmented/name/\r\nContent-Type: text/jibberish\r\n'
            'Foo: barbaz\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        # Ensure retrieving the manifest file gets the whole object
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/segmented/name HTTP/1.1\r\nHost: '
            'localhost\r\nConnection: close\r\nX-Auth-Token: '
            't\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        self.assert_('X-Object-Manifest: segmented/name/' in headers)
        self.assert_('Content-Type: text/jibberish' in headers)
        self.assert_('Foo: barbaz' in headers)
        expected_etag = md5(''.join(segment_etags)).hexdigest()
        self.assert_('Etag: "%s"' % expected_etag in headers)
        body = fd.read()
        self.assertEquals(body, '1234 1234 1234 1234 1234 ')
        # Do it again but exceeding the container listing limit
        proxy_server.CONTAINER_LISTING_LIMIT = 2
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/segmented/name HTTP/1.1\r\nHost: '
            'localhost\r\nConnection: close\r\nX-Auth-Token: '
            't\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        self.assert_('X-Object-Manifest: segmented/name/' in headers)
        self.assert_('Content-Type: text/jibberish' in headers)
        body = fd.read()
        # A bit fragile of a test; as it makes the assumption that all
        # will be sent in a single chunk.
        self.assertEquals(body,
            '19\r\n1234 1234 1234 1234 1234 \r\n0\r\n\r\n')
        # Make a copy of the manifested object, which should
        # error since the number of segments exceeds
        # CONTAINER_LISTING_LIMIT.
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/segmented/copy HTTP/1.1\r\nHost: '
            'localhost\r\nConnection: close\r\nX-Auth-Token: '
            't\r\nX-Copy-From: segmented/name\r\nContent-Length: '
            '0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 413'
        self.assertEquals(headers[:len(exp)], exp)
        body = fd.read()
        # After adjusting the CONTAINER_LISTING_LIMIT, make a copy of
        # the manifested object which should consolidate the segments.
        proxy_server.CONTAINER_LISTING_LIMIT = 10000
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/segmented/copy HTTP/1.1\r\nHost: '
            'localhost\r\nConnection: close\r\nX-Auth-Token: '
            't\r\nX-Copy-From: segmented/name\r\nContent-Length: '
            '0\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        body = fd.read()
        # Retrieve and validate the copy.
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/segmented/copy HTTP/1.1\r\nHost: '
            'localhost\r\nConnection: close\r\nX-Auth-Token: '
            't\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        self.assert_('x-object-manifest:' not in headers.lower())
        self.assert_('Content-Length: 25\r' in headers)
        body = fd.read()
        self.assertEquals(body, '1234 1234 1234 1234 1234 ')
        # Create an object manifest file pointing to nothing
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/segmented/empty HTTP/1.1\r\nHost: '
            'localhost\r\nConnection: close\r\nX-Storage-Token: '
            't\r\nContent-Length: 0\r\nX-Object-Manifest: '
            'segmented/empty/\r\nContent-Type: text/jibberish\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        # Ensure retrieving the manifest file gives a zero-byte file
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/segmented/empty HTTP/1.1\r\nHost: '
            'localhost\r\nConnection: close\r\nX-Auth-Token: '
            't\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        self.assert_('X-Object-Manifest: segmented/empty/' in headers)
        self.assert_('Content-Type: text/jibberish' in headers)
        body = fd.read()
        self.assertEquals(body, '')
        # Check copy content type
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/c/obj HTTP/1.1\r\nHost: '
            'localhost\r\nConnection: close\r\nX-Storage-Token: '
            't\r\nContent-Length: 0\r\nContent-Type: text/jibberish'
            '\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/c/obj2 HTTP/1.1\r\nHost: '
            'localhost\r\nConnection: close\r\nX-Storage-Token: '
            't\r\nContent-Length: 0\r\nX-Copy-From: c/obj\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        # Ensure getting the copied file gets original content-type
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/c/obj2 HTTP/1.1\r\nHost: '
            'localhost\r\nConnection: close\r\nX-Auth-Token: '
            't\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        print headers
        self.assert_('Content-Type: text/jibberish' in headers)
        # Check set content type
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/c/obj3 HTTP/1.1\r\nHost: '
            'localhost\r\nConnection: close\r\nX-Storage-Token: '
            't\r\nContent-Length: 0\r\nContent-Type: foo/bar'
            '\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        # Ensure getting the copied file gets original content-type
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/c/obj3 HTTP/1.1\r\nHost: '
            'localhost\r\nConnection: close\r\nX-Auth-Token: '
            't\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        self.assert_('Content-Type: foo/bar' in
                headers.split('\r\n'), repr(headers.split('\r\n')))
        # Check set content type with charset
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('PUT /v1/a/c/obj4 HTTP/1.1\r\nHost: '
            'localhost\r\nConnection: close\r\nX-Storage-Token: '
            't\r\nContent-Length: 0\r\nContent-Type: foo/bar'
            '; charset=UTF-8\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 201'
        self.assertEquals(headers[:len(exp)], exp)
        # Ensure getting the copied file gets original content-type
        sock = connect_tcp(('localhost', prolis.getsockname()[1]))
        fd = sock.makefile()
        fd.write('GET /v1/a/c/obj4 HTTP/1.1\r\nHost: '
            'localhost\r\nConnection: close\r\nX-Auth-Token: '
            't\r\n\r\n')
        fd.flush()
        headers = readuntil2crlfs(fd)
        exp = 'HTTP/1.1 200'
        self.assertEquals(headers[:len(exp)], exp)
        self.assert_('Content-Type: foo/bar; charset=UTF-8' in
                headers.split('\r\n'), repr(headers.split('\r\n')))

    def test_mismatched_etags(self):
        with save_globals():
            # no etag supplied, object servers return success w/ diff values
            controller = proxy_server.ObjectController(self.app, 'account',
                                                       'container', 'object')
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '0'})
            self.app.update_request(req)
            proxy_server.http_connect = fake_http_connect(200, 201, 201, 201,
                etags=[None,
                       '68b329da9893e34099c7d8ad5cb9c940',
                       '68b329da9893e34099c7d8ad5cb9c940',
                       '68b329da9893e34099c7d8ad5cb9c941'])
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int // 100, 5)  # server error

            # req supplies etag, object servers return 422 - mismatch
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={
                                    'Content-Length': '0',
                                    'ETag': '68b329da9893e34099c7d8ad5cb9c940',
                                })
            self.app.update_request(req)
            proxy_server.http_connect = fake_http_connect(200, 422, 422, 503,
                etags=['68b329da9893e34099c7d8ad5cb9c940',
                       '68b329da9893e34099c7d8ad5cb9c941',
                       None,
                       None])
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int // 100, 4)  # client error

    def test_request_bytes_transferred_attr(self):
        with save_globals():
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 201, 201, 201)
            controller = proxy_server.ObjectController(self.app, 'account',
                            'container', 'object')
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '10'},
                                body='1234567890')
            self.app.update_request(req)
            res = controller.PUT(req)
            self.assert_(hasattr(req, 'bytes_transferred'))
            self.assertEquals(req.bytes_transferred, 10)

    def test_copy_zero_bytes_transferred_attr(self):
        with save_globals():
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, 200, 200, 201, 201, 201,
                                  body='1234567890')
            controller = proxy_server.ObjectController(self.app, 'account',
                            'container', 'object')
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'X-Copy-From': 'c/o2',
                                         'Content-Length': '0'})
            self.app.update_request(req)
            res = controller.PUT(req)
            self.assert_(hasattr(req, 'bytes_transferred'))
            self.assertEquals(req.bytes_transferred, 0)

    def test_response_bytes_transferred_attr(self):
        with save_globals():
            proxy_server.http_connect = \
                fake_http_connect(200, body='1234567890')
            controller = proxy_server.ObjectController(self.app, 'account',
                            'container', 'object')
            req = Request.blank('/a/c/o')
            self.app.update_request(req)
            res = controller.GET(req)
            res.body
            self.assert_(hasattr(res, 'bytes_transferred'))
            self.assertEquals(res.bytes_transferred, 10)

    def test_request_client_disconnect_attr(self):
        with save_globals():
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 201, 201, 201)
            controller = proxy_server.ObjectController(self.app, 'account',
                            'container', 'object')
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '10'},
                                body='12345')
            self.app.update_request(req)
            res = controller.PUT(req)
            self.assertEquals(req.bytes_transferred, 5)
            self.assert_(hasattr(req, 'client_disconnect'))
            self.assert_(req.client_disconnect)

    def test_response_client_disconnect_attr(self):
        with save_globals():
            proxy_server.http_connect = \
                fake_http_connect(200, body='1234567890')
            controller = proxy_server.ObjectController(self.app, 'account',
                            'container', 'object')
            req = Request.blank('/a/c/o')
            self.app.update_request(req)
            orig_object_chunk_size = self.app.object_chunk_size
            try:
                self.app.object_chunk_size = 5
                res = controller.GET(req)
                ix = 0
                for v in res.app_iter:
                    ix += 1
                    if ix > 1:
                        break
                res.app_iter.close()
                self.assertEquals(res.bytes_transferred, 5)
                self.assert_(hasattr(res, 'client_disconnect'))
                self.assert_(res.client_disconnect)
            finally:
                self.app.object_chunk_size = orig_object_chunk_size

    def test_response_get_accept_ranges_header(self):
        with save_globals():
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'GET'})
            self.app.update_request(req)
            controller = proxy_server.ObjectController(self.app, 'account',
                                                       'container', 'object')
            proxy_server.http_connect = fake_http_connect(200, 200, 200)
            resp = controller.GET(req)
            self.assert_('accept-ranges' in resp.headers)
            self.assertEquals(resp.headers['accept-ranges'], 'bytes')

    def test_response_head_accept_ranges_header(self):
        with save_globals():
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'HEAD'})
            self.app.update_request(req)
            controller = proxy_server.ObjectController(self.app, 'account',
                                                       'container', 'object')
            proxy_server.http_connect = fake_http_connect(200, 200, 200)
            resp = controller.HEAD(req)
            self.assert_('accept-ranges' in resp.headers)
            self.assertEquals(resp.headers['accept-ranges'], 'bytes')

    def test_GET_calls_authorize(self):
        called = [False]

        def authorize(req):
            called[0] = True
            return HTTPUnauthorized(request=req)
        with save_globals():
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 201, 201, 201)
            controller = proxy_server.ObjectController(self.app, 'account',
                            'container', 'object')
            req = Request.blank('/a/c/o')
            req.environ['swift.authorize'] = authorize
            self.app.update_request(req)
            res = controller.GET(req)
        self.assert_(called[0])

    def test_HEAD_calls_authorize(self):
        called = [False]

        def authorize(req):
            called[0] = True
            return HTTPUnauthorized(request=req)
        with save_globals():
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 201, 201, 201)
            controller = proxy_server.ObjectController(self.app, 'account',
                            'container', 'object')
            req = Request.blank('/a/c/o', {'REQUEST_METHOD': 'HEAD'})
            req.environ['swift.authorize'] = authorize
            self.app.update_request(req)
            res = controller.HEAD(req)
        self.assert_(called[0])

    def test_POST_calls_authorize(self):
        called = [False]

        def authorize(req):
            called[0] = True
            return HTTPUnauthorized(request=req)
        with save_globals():
            self.app.object_post_as_copy = False
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 201, 201, 201)
            controller = proxy_server.ObjectController(self.app, 'account',
                            'container', 'object')
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'POST'},
                                headers={'Content-Length': '5'}, body='12345')
            req.environ['swift.authorize'] = authorize
            self.app.update_request(req)
            res = controller.POST(req)
        self.assert_(called[0])

    def test_POST_as_copy_calls_authorize(self):
        called = [False]

        def authorize(req):
            called[0] = True
            return HTTPUnauthorized(request=req)
        with save_globals():
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, 200, 200, 201, 201, 201)
            controller = proxy_server.ObjectController(self.app, 'account',
                            'container', 'object')
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'POST'},
                                headers={'Content-Length': '5'}, body='12345')
            req.environ['swift.authorize'] = authorize
            self.app.update_request(req)
            res = controller.POST(req)
        self.assert_(called[0])

    def test_PUT_calls_authorize(self):
        called = [False]

        def authorize(req):
            called[0] = True
            return HTTPUnauthorized(request=req)
        with save_globals():
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 201, 201, 201)
            controller = proxy_server.ObjectController(self.app, 'account',
                            'container', 'object')
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'Content-Length': '5'}, body='12345')
            req.environ['swift.authorize'] = authorize
            self.app.update_request(req)
            res = controller.PUT(req)
        self.assert_(called[0])

    def test_COPY_calls_authorize(self):
        called = [False]

        def authorize(req):
            called[0] = True
            return HTTPUnauthorized(request=req)
        with save_globals():
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, 200, 200, 201, 201, 201)
            controller = proxy_server.ObjectController(self.app, 'account',
                            'container', 'object')
            req = Request.blank('/a/c/o', environ={'REQUEST_METHOD': 'COPY'},
                                headers={'Destination': 'c/o'})
            req.environ['swift.authorize'] = authorize
            self.app.update_request(req)
            res = controller.COPY(req)
        self.assert_(called[0])

    def test_POST_converts_delete_after_to_delete_at(self):
        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, 200, 200, 202, 202, 202)
            self.app.memcache.store = {}
            orig_time = proxy_server.time.time
            try:
                t = time()
                proxy_server.time.time = lambda: t
                req = Request.blank('/a/c/o', {},
                   headers={'Content-Type': 'foo/bar', 'X-Delete-After': '60'})
                self.app.update_request(req)
                res = controller.POST(req)
                self.assertEquals(res.status, '202 Fake')
                self.assertEquals(req.headers.get('x-delete-at'), 
                                  str(int(t + 60)))

                self.app.object_post_as_copy = False
                controller = proxy_server.ObjectController(self.app, 'account',
                    'container', 'object')
                proxy_server.http_connect = \
                    fake_http_connect(200, 200, 202, 202, 202)
                self.app.memcache.store = {}
                req = Request.blank('/a/c/o', {},
                   headers={'Content-Type': 'foo/bar', 'X-Delete-After': '60'})
                self.app.update_request(req)
                res = controller.POST(req)
                self.assertEquals(res.status, '202 Fake')
                self.assertEquals(req.headers.get('x-delete-at'),
                                  str(int(t + 60)))
            finally:
                proxy_server.time.time = orig_time


    def test_POST_non_int_delete_after(self):
        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, 200, 200, 202, 202, 202)
            self.app.memcache.store = {}
            req = Request.blank('/a/c/o', {},
                headers={'Content-Type': 'foo/bar', 'X-Delete-After': '60.1'})
            self.app.update_request(req)
            res = controller.POST(req)
            self.assertEquals(res.status, '400 Bad Request')
            self.assertTrue('Non-integer X-Delete-After' in res.body)

    def test_POST_negative_delete_after(self):
        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 200, 200, 200, 202, 202, 202)
            self.app.memcache.store = {}
            req = Request.blank('/a/c/o', {},
                headers={'Content-Type': 'foo/bar', 'X-Delete-After': '-60'})
            self.app.update_request(req)
            res = controller.POST(req)
            self.assertEquals(res.status, '400 Bad Request')
            self.assertTrue('X-Delete-At in past' in res.body)

    def test_POST_delete_at(self):
        with save_globals():
            given_headers = {}

            def fake_make_requests(req, ring, part, method, path, headers,
                                   query_string=''):
                given_headers.update(headers[0])

            self.app.object_post_as_copy = False
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            controller.make_requests = fake_make_requests
            proxy_server.http_connect = fake_http_connect(200, 200)
            self.app.memcache.store = {}
            t = str(int(time() + 100))
            req = Request.blank('/a/c/o', {},
                headers={'Content-Type': 'foo/bar', 'X-Delete-At': t})
            self.app.update_request(req)
            controller.POST(req)
            self.assertEquals(given_headers.get('X-Delete-At'), t)
            self.assertTrue('X-Delete-At-Host' in given_headers)
            self.assertTrue('X-Delete-At-Device' in given_headers)
            self.assertTrue('X-Delete-At-Partition' in given_headers)

            t = str(int(time() + 100)) + '.1'
            req = Request.blank('/a/c/o', {},
                headers={'Content-Type': 'foo/bar', 'X-Delete-At': t})
            self.app.update_request(req)
            resp = controller.POST(req)
            self.assertEquals(resp.status_int, 400)
            self.assertTrue('Non-integer X-Delete-At' in resp.body)

            t = str(int(time() - 100))
            req = Request.blank('/a/c/o', {},
                headers={'Content-Type': 'foo/bar', 'X-Delete-At': t})
            self.app.update_request(req)
            resp = controller.POST(req)
            self.assertEquals(resp.status_int, 400)
            self.assertTrue('X-Delete-At in past' in resp.body)

    def test_PUT_converts_delete_after_to_delete_at(self):
        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 201, 201, 201)
            self.app.memcache.store = {}
            orig_time = proxy_server.time.time
            try:
                t = time()
                proxy_server.time.time = lambda: t
                req = Request.blank('/a/c/o', {},
                    headers={'Content-Length': '0', 'Content-Type': 'foo/bar',
                             'X-Delete-After': '60'})
                self.app.update_request(req)
                res = controller.PUT(req)
                self.assertEquals(res.status, '201 Fake')
                self.assertEquals(req.headers.get('x-delete-at'),
                                  str(int(t + 60)))
            finally:
                proxy_server.time.time = orig_time

    def test_PUT_non_int_delete_after(self):
        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 201, 201, 201)
            self.app.memcache.store = {}
            req = Request.blank('/a/c/o', {},
                headers={'Content-Length': '0', 'Content-Type': 'foo/bar',
                         'X-Delete-After': '60.1'})
            self.app.update_request(req)
            res = controller.PUT(req)
            self.assertEquals(res.status, '400 Bad Request')
            self.assertTrue('Non-integer X-Delete-After' in res.body)

    def test_PUT_negative_delete_after(self):
        with save_globals():
            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            proxy_server.http_connect = \
                fake_http_connect(200, 200, 201, 201, 201)
            self.app.memcache.store = {}
            req = Request.blank('/a/c/o', {},
                headers={'Content-Length': '0', 'Content-Type': 'foo/bar',
                         'X-Delete-After': '-60'})
            self.app.update_request(req)
            res = controller.PUT(req)
            self.assertEquals(res.status, '400 Bad Request')
            self.assertTrue('X-Delete-At in past' in res.body)

    def test_PUT_delete_at(self):
        with save_globals():
            given_headers = {}

            def fake_connect_put_node(nodes, part, path, headers):
                given_headers.update(headers)

            controller = proxy_server.ObjectController(self.app, 'account',
                'container', 'object')
            controller._connect_put_node = fake_connect_put_node
            proxy_server.http_connect = fake_http_connect(200, 200)
            self.app.memcache.store = {}
            t = str(int(time() + 100))
            req = Request.blank('/a/c/o', {},
                headers={'Content-Length': '0', 'Content-Type': 'foo/bar',
                         'X-Delete-At': t})
            self.app.update_request(req)
            controller.PUT(req)
            self.assertEquals(given_headers.get('X-Delete-At'), t)
            self.assertTrue('X-Delete-At-Host' in given_headers)
            self.assertTrue('X-Delete-At-Device' in given_headers)
            self.assertTrue('X-Delete-At-Partition' in given_headers)

            t = str(int(time() + 100)) + '.1'
            req = Request.blank('/a/c/o', {},
                headers={'Content-Length': '0', 'Content-Type': 'foo/bar',
                         'X-Delete-At': t})
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 400)
            self.assertTrue('Non-integer X-Delete-At' in resp.body)

            t = str(int(time() - 100))
            req = Request.blank('/a/c/o', {},
                headers={'Content-Length': '0', 'Content-Type': 'foo/bar',
                         'X-Delete-At': t})
            self.app.update_request(req)
            resp = controller.PUT(req)
            self.assertEquals(resp.status_int, 400)
            self.assertTrue('X-Delete-At in past' in resp.body)


class TestContainerController(unittest.TestCase):
    "Test swift.proxy_server.ContainerController"

    def setUp(self):
        self.app = proxy_server.Application(None, FakeMemcache(),
            account_ring=FakeRing(), container_ring=FakeRing(),
            object_ring=FakeRing())

    def assert_status_map(self, method, statuses, expected,
                          raise_exc=False, missing_container=False):
        with save_globals():
            kwargs = {}
            if raise_exc:
                kwargs['raise_exc'] = raise_exc
            kwargs['missing_container'] = missing_container
            proxy_server.http_connect = fake_http_connect(*statuses, **kwargs)
            self.app.memcache.store = {}
            req = Request.blank('/a/c', headers={'Content-Length': '0',
                    'Content-Type': 'text/plain'})
            self.app.update_request(req)
            res = method(req)
            self.assertEquals(res.status_int, expected)
            proxy_server.http_connect = fake_http_connect(*statuses, **kwargs)
            self.app.memcache.store = {}
            req = Request.blank('/a/c/', headers={'Content-Length': '0',
                    'Content-Type': 'text/plain'})
            self.app.update_request(req)
            res = method(req)
            self.assertEquals(res.status_int, expected)

    def test_HEAD(self):
        with save_globals():
            controller = proxy_server.ContainerController(self.app, 'account',
                'container')

            def test_status_map(statuses, expected, **kwargs):
                proxy_server.http_connect = fake_http_connect(*statuses,
                                                              **kwargs)
                self.app.memcache.store = {}
                req = Request.blank('/a/c', {})
                self.app.update_request(req)
                res = controller.HEAD(req)
                self.assertEquals(res.status[:len(str(expected))],
                                  str(expected))
                if expected < 400:
                    self.assert_('x-works' in res.headers)
                    self.assertEquals(res.headers['x-works'], 'yes')
            test_status_map((200, 200, 404, 404), 200)
            test_status_map((200, 200, 500, 404), 200)
            test_status_map((200, 304, 500, 404), 304)
            test_status_map((200, 404, 404, 404), 404)
            test_status_map((200, 404, 404, 500), 404)
            test_status_map((200, 500, 500, 500), 503)

    def test_PUT(self):
        with save_globals():
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')

            def test_status_map(statuses, expected, **kwargs):
                proxy_server.http_connect = fake_http_connect(*statuses,
                                                              **kwargs)
                self.app.memcache.store = {}
                req = Request.blank('/a/c', {})
                req.content_length = 0
                self.app.update_request(req)
                res = controller.PUT(req)
                expected = str(expected)
                self.assertEquals(res.status[:len(expected)], expected)
            test_status_map((200, 201, 201, 201), 201, missing_container=True)
            test_status_map((200, 201, 201, 500), 201, missing_container=True)
            test_status_map((200, 204, 404, 404), 404, missing_container=True)
            test_status_map((200, 204, 500, 404), 503, missing_container=True)

    def test_PUT_max_container_name_length(self):
        with save_globals():
            controller = proxy_server.ContainerController(self.app, 'account',
                                                              '1' * 256)
            self.assert_status_map(controller.PUT,
                                   (200, 200, 200, 201, 201, 201), 201,
                                   missing_container=True)
            controller = proxy_server.ContainerController(self.app, 'account',
                                                              '2' * 257)
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
                for dev in self.app.account_ring.devs.values():
                    del dev['errors']
                    del dev['last_error']
                controller = proxy_server.ContainerController(self.app,
                                'account', 'container')
                if meth == 'PUT':
                    proxy_server.http_connect = \
                        fake_http_connect(200, 200, 200, 200, 200, 200,
                                          missing_container=True)
                else:
                    proxy_server.http_connect = \
                        fake_http_connect(200, 200, 200, 200)
                self.app.memcache.store = {}
                req = Request.blank('/a/c', environ={'REQUEST_METHOD': meth})
                self.app.update_request(req)
                resp = getattr(controller, meth)(req)
                self.assertEquals(resp.status_int, 200)

                proxy_server.http_connect = \
                    fake_http_connect(404, 404, 404, 200, 200, 200)
                resp = getattr(controller, meth)(req)
                self.assertEquals(resp.status_int, 404)

                proxy_server.http_connect = \
                    fake_http_connect(503, 404, 404)
                resp = getattr(controller, meth)(req)
                self.assertEquals(resp.status_int, 404)

                proxy_server.http_connect = \
                    fake_http_connect(503, 404, raise_exc=True)
                resp = getattr(controller, meth)(req)
                self.assertEquals(resp.status_int, 404)

                for dev in self.app.account_ring.devs.values():
                    dev['errors'] = self.app.error_suppression_limit + 1
                    dev['last_error'] = time()
                proxy_server.http_connect = \
                    fake_http_connect(200, 200, 200, 200, 200, 200)
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
                    raise MemcacheLockError()

        with save_globals():
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            self.app.memcache = MockMemcache(allow_lock=True)
            proxy_server.http_connect = fake_http_connect(
                200, 200, 200, 201, 201, 201, missing_container=True)
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': 'PUT'})
            self.app.update_request(req)
            res = controller.PUT(req)
            self.assertEquals(res.status_int, 201)

    def test_error_limiting(self):
        with save_globals():
            proxy_server.shuffle = lambda l: None
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            self.assert_status_map(controller.HEAD, (200, 503, 200, 200), 200,
                                   missing_container=False)
            self.assertEquals(
                controller.app.container_ring.devs[0]['errors'], 2)
            self.assert_('last_error' in controller.app.container_ring.devs[0])
            for _junk in xrange(self.app.error_suppression_limit):
                self.assert_status_map(controller.HEAD,
                                       (200, 503, 503, 503), 503)
            self.assertEquals(controller.app.container_ring.devs[0]['errors'],
                              self.app.error_suppression_limit + 1)
            self.assert_status_map(controller.HEAD, (200, 200, 200, 200), 503)
            self.assert_('last_error' in controller.app.container_ring.devs[0])
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

    def test_response_bytes_transferred_attr(self):
        with save_globals():
            proxy_server.http_connect = fake_http_connect(200, 200, body='{}')
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            req = Request.blank('/a/c?format=json')
            self.app.update_request(req)
            res = controller.GET(req)
            res.body
            self.assert_(hasattr(res, 'bytes_transferred'))
            self.assertEquals(res.bytes_transferred, 2)

    def test_response_client_disconnect_attr(self):
        with save_globals():
            proxy_server.http_connect = fake_http_connect(200, 200, body='{}')
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            req = Request.blank('/a/c?format=json')
            self.app.update_request(req)
            orig_object_chunk_size = self.app.object_chunk_size
            try:
                self.app.object_chunk_size = 1
                res = controller.GET(req)
                ix = 0
                for v in res.app_iter:
                    ix += 1
                    if ix > 1:
                        break
                res.app_iter.close()
                self.assertEquals(res.bytes_transferred, 1)
                self.assert_(hasattr(res, 'client_disconnect'))
                self.assert_(res.client_disconnect)
            finally:
                self.app.object_chunk_size = orig_object_chunk_size

    def test_response_get_accept_ranges_header(self):
        with save_globals():
            proxy_server.http_connect = fake_http_connect(200, 200, body='{}')
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            req = Request.blank('/a/c?format=json')
            self.app.update_request(req)
            res = controller.GET(req)
            self.assert_('accept-ranges' in res.headers)
            self.assertEqual(res.headers['accept-ranges'], 'bytes')

    def test_response_head_accept_ranges_header(self):
        with save_globals():
            proxy_server.http_connect = fake_http_connect(200, 200, body='{}')
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            req = Request.blank('/a/c?format=json')
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
                ('X-Container-Meta-TestHeader', '')):
            test_errors = []

            def test_connect(ipaddr, port, device, partition, method, path,
                             headers=None, query_string=None):
                if path == '/a/c':
                    for k, v in headers.iteritems():
                        if k.lower() == test_header.lower() and \
                                v == test_value:
                            break
                    else:
                        test_errors.append('%s: %s not in %s' %
                                           (test_header, test_value, headers))
            with save_globals():
                controller = \
                    proxy_server.ContainerController(self.app, 'a', 'c')
                proxy_server.http_connect = fake_http_connect(200, 201, 201,
                    201, give_connect=test_connect)
                req = Request.blank('/a/c', environ={'REQUEST_METHOD': method},
                        headers={test_header: test_value})
                self.app.update_request(req)
                res = getattr(controller, method)(req)
                self.assertEquals(test_errors, [])

    def test_PUT_bad_metadata(self):
        self.bad_metadata_helper('PUT')

    def test_POST_bad_metadata(self):
        self.bad_metadata_helper('POST')

    def bad_metadata_helper(self, method):
        with save_globals():
            controller = proxy_server.ContainerController(self.app, 'a', 'c')
            proxy_server.http_connect = fake_http_connect(200, 201, 201, 201)
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': method})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 201)

            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': method},
                    headers={'X-Container-Meta-' +
                                ('a' * MAX_META_NAME_LENGTH): 'v'})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 201)
            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': method},
                    headers={'X-Container-Meta-' +
                                ('a' * (MAX_META_NAME_LENGTH + 1)): 'v'})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 400)

            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': method},
                    headers={'X-Container-Meta-Too-Long':
                                'a' * MAX_META_VALUE_LENGTH})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 201)
            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': method},
                    headers={'X-Container-Meta-Too-Long':
                                'a' * (MAX_META_VALUE_LENGTH + 1)})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 400)

            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            headers = {}
            for x in xrange(MAX_META_COUNT):
                headers['X-Container-Meta-%d' % x] = 'v'
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': method},
                                headers=headers)
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 201)
            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            headers = {}
            for x in xrange(MAX_META_COUNT + 1):
                headers['X-Container-Meta-%d' % x] = 'v'
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': method},
                                headers=headers)
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 400)

            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            headers = {}
            header_value = 'a' * MAX_META_VALUE_LENGTH
            size = 0
            x = 0
            while size < MAX_META_OVERALL_SIZE - 4 - MAX_META_VALUE_LENGTH:
                size += 4 + MAX_META_VALUE_LENGTH
                headers['X-Container-Meta-%04d' % x] = header_value
                x += 1
            if MAX_META_OVERALL_SIZE - size > 1:
                headers['X-Container-Meta-a'] = \
                    'a' * (MAX_META_OVERALL_SIZE - size - 1)
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': method},
                                headers=headers)
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 201)
            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            headers['X-Container-Meta-a'] = \
                'a' * (MAX_META_OVERALL_SIZE - size)
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': method},
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
            proxy_server.http_connect = fake_http_connect(200, 201, 201, 201)
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': 'POST'},
                                headers={'X-Container-Read': '.r:*'})
            req.environ['swift.clean_acl'] = clean_acl
            self.app.update_request(req)
            res = controller.POST(req)
        self.assert_(called[0])
        called[0] = False
        with save_globals():
            proxy_server.http_connect = fake_http_connect(200, 201, 201, 201)
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': 'POST'},
                                headers={'X-Container-Write': '.r:*'})
            req.environ['swift.clean_acl'] = clean_acl
            self.app.update_request(req)
            res = controller.POST(req)
        self.assert_(called[0])

    def test_PUT_calls_clean_acl(self):
        called = [False]

        def clean_acl(header, value):
            called[0] = True
            raise ValueError('fake error')
        with save_globals():
            proxy_server.http_connect = fake_http_connect(200, 201, 201, 201)
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'X-Container-Read': '.r:*'})
            req.environ['swift.clean_acl'] = clean_acl
            self.app.update_request(req)
            res = controller.PUT(req)
        self.assert_(called[0])
        called[0] = False
        with save_globals():
            proxy_server.http_connect = fake_http_connect(200, 201, 201, 201)
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': 'PUT'},
                                headers={'X-Container-Write': '.r:*'})
            req.environ['swift.clean_acl'] = clean_acl
            self.app.update_request(req)
            res = controller.PUT(req)
        self.assert_(called[0])

    def test_GET_calls_authorize(self):
        called = [False]

        def authorize(req):
            called[0] = True
            return HTTPUnauthorized(request=req)
        with save_globals():
            proxy_server.http_connect = \
                fake_http_connect(200, 201, 201, 201)
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            req = Request.blank('/a/c')
            req.environ['swift.authorize'] = authorize
            self.app.update_request(req)
            res = controller.GET(req)
        self.assert_(called[0])

    def test_HEAD_calls_authorize(self):
        called = [False]

        def authorize(req):
            called[0] = True
            return HTTPUnauthorized(request=req)
        with save_globals():
            proxy_server.http_connect = \
                fake_http_connect(200, 201, 201, 201)
            controller = proxy_server.ContainerController(self.app, 'account',
                                                          'container')
            req = Request.blank('/a/c', {'REQUEST_METHOD': 'HEAD'})
            req.environ['swift.authorize'] = authorize
            self.app.update_request(req)
            res = controller.HEAD(req)
        self.assert_(called[0])


class TestAccountController(unittest.TestCase):

    def setUp(self):
        self.app = proxy_server.Application(None, FakeMemcache(),
            account_ring=FakeRing(), container_ring=FakeRing(),
            object_ring=FakeRing)

    def assert_status_map(self, method, statuses, expected):
        with save_globals():
            proxy_server.http_connect = fake_http_connect(*statuses)
            req = Request.blank('/a', {})
            self.app.update_request(req)
            res = method(req)
            self.assertEquals(res.status_int, expected)
            proxy_server.http_connect = fake_http_connect(*statuses)
            req = Request.blank('/a/', {})
            self.app.update_request(req)
            res = method(req)
            self.assertEquals(res.status_int, expected)

    def test_GET(self):
        with save_globals():
            controller = proxy_server.AccountController(self.app, 'account')
            self.assert_status_map(controller.GET, (200, 200, 200), 200)
            self.assert_status_map(controller.GET, (200, 200, 503), 200)
            self.assert_status_map(controller.GET, (200, 503, 503), 200)
            self.assert_status_map(controller.GET, (204, 204, 204), 204)
            self.assert_status_map(controller.GET, (204, 204, 503), 204)
            self.assert_status_map(controller.GET, (204, 503, 503), 204)
            self.assert_status_map(controller.GET, (204, 204, 200), 204)
            self.assert_status_map(controller.GET, (204, 200, 200), 204)
            self.assert_status_map(controller.GET, (404, 404, 404), 404)
            self.assert_status_map(controller.GET, (404, 404, 200), 200)
            self.assert_status_map(controller.GET, (404, 200, 200), 200)
            self.assert_status_map(controller.GET, (404, 404, 503), 404)
            self.assert_status_map(controller.GET, (404, 503, 503), 503)
            self.assert_status_map(controller.GET, (404, 204, 503), 204)

            self.app.memcache = FakeMemcacheReturnsNone()
            self.assert_status_map(controller.GET, (404, 404, 404), 404)

    def test_GET_autocreate(self):
        with save_globals():
            controller = proxy_server.AccountController(self.app, 'account')
            self.app.memcache = FakeMemcacheReturnsNone()
            self.assert_status_map(controller.GET,
                (404, 404, 404, 201, 201, 201, 204), 404)
            controller.app.account_autocreate = True
            self.assert_status_map(controller.GET,
                (404, 404, 404, 201, 201, 201, 204), 204)

    def test_HEAD(self):
        with save_globals():
            controller = proxy_server.AccountController(self.app, 'account')
            self.assert_status_map(controller.HEAD, (200, 200, 200), 200)
            self.assert_status_map(controller.HEAD, (200, 200, 503), 200)
            self.assert_status_map(controller.HEAD, (200, 503, 503), 200)
            self.assert_status_map(controller.HEAD, (204, 204, 204), 204)
            self.assert_status_map(controller.HEAD, (204, 204, 503), 204)
            self.assert_status_map(controller.HEAD, (204, 503, 503), 204)
            self.assert_status_map(controller.HEAD, (204, 204, 200), 204)
            self.assert_status_map(controller.HEAD, (204, 200, 200), 204)
            self.assert_status_map(controller.HEAD, (404, 404, 404), 404)
            self.assert_status_map(controller.HEAD, (404, 404, 200), 200)
            self.assert_status_map(controller.HEAD, (404, 200, 200), 200)
            self.assert_status_map(controller.HEAD, (404, 404, 503), 404)
            self.assert_status_map(controller.HEAD, (404, 503, 503), 503)
            self.assert_status_map(controller.HEAD, (404, 204, 503), 204)

    def test_HEAD_autocreate(self):
        with save_globals():
            controller = proxy_server.AccountController(self.app, 'account')
            self.app.memcache = FakeMemcacheReturnsNone()
            self.assert_status_map(controller.HEAD,
                (404, 404, 404, 201, 201, 201, 204), 404)
            controller.app.account_autocreate = True
            self.assert_status_map(controller.HEAD,
                (404, 404, 404, 201, 201, 201, 204), 204)

    def test_POST_autocreate(self):
        with save_globals():
            controller = proxy_server.AccountController(self.app, 'account')
            self.app.memcache = FakeMemcacheReturnsNone()
            self.assert_status_map(controller.POST,
                (404, 404, 404, 201, 201, 201), 404)
            controller.app.account_autocreate = True
            self.assert_status_map(controller.POST,
                (404, 404, 404, 201, 201, 201), 201)

    def test_connection_refused(self):
        self.app.account_ring.get_nodes('account')
        for dev in self.app.account_ring.devs.values():
            dev['ip'] = '127.0.0.1'
            dev['port'] = 1  # can't connect on this port
        controller = proxy_server.AccountController(self.app, 'account')
        req = Request.blank('/account', environ={'REQUEST_METHOD': 'HEAD'})
        self.app.update_request(req)
        resp = controller.HEAD(req)
        self.assertEquals(resp.status_int, 503)

    def test_other_socket_error(self):
        self.app.account_ring.get_nodes('account')
        for dev in self.app.account_ring.devs.values():
            dev['ip'] = '127.0.0.1'
            dev['port'] = -1  # invalid port number
        controller = proxy_server.AccountController(self.app, 'account')
        req = Request.blank('/account', environ={'REQUEST_METHOD': 'HEAD'})
        self.app.update_request(req)
        resp = controller.HEAD(req)
        self.assertEquals(resp.status_int, 503)

    def test_response_bytes_transferred_attr(self):
        with save_globals():
            proxy_server.http_connect = fake_http_connect(200, 200, body='{}')
            controller = proxy_server.AccountController(self.app, 'account')
            req = Request.blank('/a?format=json')
            self.app.update_request(req)
            res = controller.GET(req)
            res.body
            self.assert_(hasattr(res, 'bytes_transferred'))
            self.assertEquals(res.bytes_transferred, 2)

    def test_response_get_accept_ranges_header(self):
        with save_globals():
            proxy_server.http_connect = fake_http_connect(200, 200, body='{}')
            controller = proxy_server.AccountController(self.app, 'account')
            req = Request.blank('/a?format=json')
            self.app.update_request(req)
            res = controller.GET(req)
            self.assert_('accept-ranges' in res.headers)
            self.assertEqual(res.headers['accept-ranges'], 'bytes')

    def test_response_head_accept_ranges_header(self):
        with save_globals():
            proxy_server.http_connect = fake_http_connect(200, 200, body='{}')
            controller = proxy_server.AccountController(self.app, 'account')
            req = Request.blank('/a?format=json')
            self.app.update_request(req)
            res = controller.HEAD(req)
            res.body
            self.assert_('accept-ranges' in res.headers)
            self.assertEqual(res.headers['accept-ranges'], 'bytes')

    def test_response_client_disconnect_attr(self):
        with save_globals():
            proxy_server.http_connect = fake_http_connect(200, 200, body='{}')
            controller = proxy_server.AccountController(self.app, 'account')
            req = Request.blank('/a?format=json')
            self.app.update_request(req)
            orig_object_chunk_size = self.app.object_chunk_size
            try:
                self.app.object_chunk_size = 1
                res = controller.GET(req)
                ix = 0
                for v in res.app_iter:
                    ix += 1
                    if ix > 1:
                        break
                res.app_iter.close()
                self.assertEquals(res.bytes_transferred, 1)
                self.assert_(hasattr(res, 'client_disconnect'))
                self.assert_(res.client_disconnect)
            finally:
                self.app.object_chunk_size = orig_object_chunk_size

    def test_PUT(self):
        with save_globals():
            controller = proxy_server.AccountController(self.app, 'account')

            def test_status_map(statuses, expected, **kwargs):
                proxy_server.http_connect = \
                    fake_http_connect(*statuses, **kwargs)
                self.app.memcache.store = {}
                req = Request.blank('/a', {})
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
            controller = proxy_server.AccountController(self.app, '1' * 256)
            self.assert_status_map(controller.PUT, (201, 201, 201), 201)
            controller = proxy_server.AccountController(self.app, '2' * 257)
            self.assert_status_map(controller.PUT, (201, 201, 201), 400)

    def test_PUT_connect_exceptions(self):
        with save_globals():
            self.app.allow_account_management = True
            controller = proxy_server.AccountController(self.app, 'account')
            self.assert_status_map(controller.PUT, (201, 201, -1), 201)
            self.assert_status_map(controller.PUT, (201, -1, -1), 503)
            self.assert_status_map(controller.PUT, (503, 503, -1), 503)

    def test_PUT_metadata(self):
        self.metadata_helper('PUT')

    def test_POST_metadata(self):
        self.metadata_helper('POST')

    def metadata_helper(self, method):
        for test_header, test_value in (
                ('X-Account-Meta-TestHeader', 'TestValue'),
                ('X-Account-Meta-TestHeader', '')):
            test_errors = []

            def test_connect(ipaddr, port, device, partition, method, path,
                             headers=None, query_string=None):
                if path == '/a':
                    for k, v in headers.iteritems():
                        if k.lower() == test_header.lower() and \
                                v == test_value:
                            break
                    else:
                        test_errors.append('%s: %s not in %s' %
                                           (test_header, test_value, headers))
            with save_globals():
                self.app.allow_account_management = True
                controller = \
                    proxy_server.AccountController(self.app, 'a')
                proxy_server.http_connect = fake_http_connect(201, 201, 201,
                                                give_connect=test_connect)
                req = Request.blank('/a/c', environ={'REQUEST_METHOD': method},
                        headers={test_header: test_value})
                self.app.update_request(req)
                res = getattr(controller, method)(req)
                self.assertEquals(test_errors, [])

    def test_PUT_bad_metadata(self):
        self.bad_metadata_helper('PUT')

    def test_POST_bad_metadata(self):
        self.bad_metadata_helper('POST')

    def bad_metadata_helper(self, method):
        with save_globals():
            self.app.allow_account_management = True
            controller = proxy_server.AccountController(self.app, 'a')
            proxy_server.http_connect = fake_http_connect(200, 201, 201, 201)
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': method})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 201)

            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': method},
                    headers={'X-Account-Meta-' +
                                ('a' * MAX_META_NAME_LENGTH): 'v'})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 201)
            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': method},
                    headers={'X-Account-Meta-' +
                                ('a' * (MAX_META_NAME_LENGTH + 1)): 'v'})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 400)

            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': method},
                    headers={'X-Account-Meta-Too-Long':
                                'a' * MAX_META_VALUE_LENGTH})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 201)
            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': method},
                    headers={'X-Account-Meta-Too-Long':
                                'a' * (MAX_META_VALUE_LENGTH + 1)})
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 400)

            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            headers = {}
            for x in xrange(MAX_META_COUNT):
                headers['X-Account-Meta-%d' % x] = 'v'
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': method},
                                headers=headers)
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 201)
            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            headers = {}
            for x in xrange(MAX_META_COUNT + 1):
                headers['X-Account-Meta-%d' % x] = 'v'
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': method},
                                headers=headers)
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 400)

            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            headers = {}
            header_value = 'a' * MAX_META_VALUE_LENGTH
            size = 0
            x = 0
            while size < MAX_META_OVERALL_SIZE - 4 - MAX_META_VALUE_LENGTH:
                size += 4 + MAX_META_VALUE_LENGTH
                headers['X-Account-Meta-%04d' % x] = header_value
                x += 1
            if MAX_META_OVERALL_SIZE - size > 1:
                headers['X-Account-Meta-a'] = \
                    'a' * (MAX_META_OVERALL_SIZE - size - 1)
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': method},
                                headers=headers)
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 201)
            proxy_server.http_connect = fake_http_connect(201, 201, 201)
            headers['X-Account-Meta-a'] = \
                'a' * (MAX_META_OVERALL_SIZE - size)
            req = Request.blank('/a/c', environ={'REQUEST_METHOD': method},
                                headers=headers)
            self.app.update_request(req)
            resp = getattr(controller, method)(req)
            self.assertEquals(resp.status_int, 400)

    def test_DELETE(self):
        with save_globals():
            controller = proxy_server.AccountController(self.app, 'account')

            def test_status_map(statuses, expected, **kwargs):
                proxy_server.http_connect = \
                    fake_http_connect(*statuses, **kwargs)
                self.app.memcache.store = {}
                req = Request.blank('/a', {'REQUEST_METHOD': 'DELETE'})
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

    def exception(self, *args):
        self.exception_args = args
        self.exception_info = sys.exc_info()

    def GETorHEAD_base(self, *args):
        self.GETorHEAD_base_args = args
        req = args[0]
        path = args[4]
        body = data = path[-1] * int(path[-1])
        if req.range and req.range.ranges:
            body = ''
            for start, stop in req.range.ranges:
                body += data[start:stop]
        resp = Response(app_iter=iter(body))
        return resp

    def iter_nodes(self, partition, nodes, ring):
        for node in nodes:
            yield node
        for node in ring.get_more_nodes(partition):
            yield node


class Stub(object):
    pass


class TestSegmentedIterable(unittest.TestCase):

    def setUp(self):
        self.controller = FakeObjectController()

    def test_load_next_segment_unexpected_error(self):
        # Iterator value isn't a dict
        self.assertRaises(Exception,
            proxy_server.SegmentedIterable(self.controller, None,
            [None])._load_next_segment)
        self.assert_(self.controller.exception_args[0].startswith(
            'ERROR: While processing manifest'))

    def test_load_next_segment_with_no_segments(self):
        self.assertRaises(StopIteration,
            proxy_server.SegmentedIterable(self.controller, 'lc',
            [])._load_next_segment)

    def test_load_next_segment_with_one_segment(self):
        segit = proxy_server.SegmentedIterable(self.controller, 'lc', [{'name':
            'o1'}])
        segit._load_next_segment()
        self.assertEquals(self.controller.GETorHEAD_base_args[4], '/a/lc/o1')
        data = ''.join(segit.segment_iter)
        self.assertEquals(data, '1')

    def test_load_next_segment_with_two_segments(self):
        segit = proxy_server.SegmentedIterable(self.controller, 'lc', [{'name':
            'o1'}, {'name': 'o2'}])
        segit._load_next_segment()
        self.assertEquals(self.controller.GETorHEAD_base_args[4], '/a/lc/o1')
        data = ''.join(segit.segment_iter)
        self.assertEquals(data, '1')
        segit._load_next_segment()
        self.assertEquals(self.controller.GETorHEAD_base_args[4], '/a/lc/o2')
        data = ''.join(segit.segment_iter)
        self.assertEquals(data, '22')

    def test_load_next_segment_with_two_segments_skip_first(self):
        segit = proxy_server.SegmentedIterable(self.controller, 'lc', [{'name':
            'o1'}, {'name': 'o2'}])
        segit.segment = 0
        segit.listing.next()
        segit._load_next_segment()
        self.assertEquals(self.controller.GETorHEAD_base_args[4], '/a/lc/o2')
        data = ''.join(segit.segment_iter)
        self.assertEquals(data, '22')

    def test_load_next_segment_with_seek(self):
        segit = proxy_server.SegmentedIterable(self.controller, 'lc', [{'name':
            'o1'}, {'name': 'o2'}])
        segit.segment = 0
        segit.listing.next()
        segit.seek = 1
        segit._load_next_segment()
        self.assertEquals(self.controller.GETorHEAD_base_args[4], '/a/lc/o2')
        self.assertEquals(str(self.controller.GETorHEAD_base_args[0].range),
            'bytes=1-')
        data = ''.join(segit.segment_iter)
        self.assertEquals(data, '2')

    def test_load_next_segment_with_get_error(self):

        def local_GETorHEAD_base(*args):
            return HTTPNotFound()

        self.controller.GETorHEAD_base = local_GETorHEAD_base
        self.assertRaises(Exception,
            proxy_server.SegmentedIterable(self.controller, 'lc', [{'name':
            'o1'}])._load_next_segment)
        self.assert_(self.controller.exception_args[0].startswith(
            'ERROR: While processing manifest'))
        self.assertEquals(str(self.controller.exception_info[1]),
            'Could not load object segment /a/lc/o1: 404')

    def test_iter_unexpected_error(self):
        # Iterator value isn't a dict
        self.assertRaises(Exception, ''.join,
            proxy_server.SegmentedIterable(self.controller, None, [None]))
        self.assert_(self.controller.exception_args[0].startswith(
            'ERROR: While processing manifest'))

    def test_iter_with_no_segments(self):
        segit = proxy_server.SegmentedIterable(self.controller, 'lc', [])
        self.assertEquals(''.join(segit), '')

    def test_iter_with_one_segment(self):
        segit = proxy_server.SegmentedIterable(self.controller, 'lc', [{'name':
            'o1'}])
        segit.response = Stub()
        self.assertEquals(''.join(segit), '1')
        self.assertEquals(segit.response.bytes_transferred, 1)

    def test_iter_with_two_segments(self):
        segit = proxy_server.SegmentedIterable(self.controller, 'lc', [{'name':
            'o1'}, {'name': 'o2'}])
        segit.response = Stub()
        self.assertEquals(''.join(segit), '122')
        self.assertEquals(segit.response.bytes_transferred, 3)

    def test_iter_with_get_error(self):

        def local_GETorHEAD_base(*args):
            return HTTPNotFound()

        self.controller.GETorHEAD_base = local_GETorHEAD_base
        self.assertRaises(Exception, ''.join,
            proxy_server.SegmentedIterable(self.controller, 'lc', [{'name':
            'o1'}]))
        self.assert_(self.controller.exception_args[0].startswith(
            'ERROR: While processing manifest'))
        self.assertEquals(str(self.controller.exception_info[1]),
            'Could not load object segment /a/lc/o1: 404')

    def test_app_iter_range_unexpected_error(self):
        # Iterator value isn't a dict
        self.assertRaises(Exception,
            proxy_server.SegmentedIterable(self.controller, None,
            [None]).app_iter_range(None, None).next)
        self.assert_(self.controller.exception_args[0].startswith(
            'ERROR: While processing manifest'))

    def test_app_iter_range_with_no_segments(self):
        self.assertEquals(''.join(proxy_server.SegmentedIterable(
            self.controller, 'lc', []).app_iter_range(None, None)), '')
        self.assertEquals(''.join(proxy_server.SegmentedIterable(
            self.controller, 'lc', []).app_iter_range(3, None)), '')
        self.assertEquals(''.join(proxy_server.SegmentedIterable(
            self.controller, 'lc', []).app_iter_range(3, 5)), '')
        self.assertEquals(''.join(proxy_server.SegmentedIterable(
            self.controller, 'lc', []).app_iter_range(None, 5)), '')

    def test_app_iter_range_with_one_segment(self):
        listing = [{'name': 'o1', 'bytes': 1}]

        segit = proxy_server.SegmentedIterable(self.controller, 'lc', listing)
        segit.response = Stub()
        self.assertEquals(''.join(segit.app_iter_range(None, None)), '1')
        self.assertEquals(segit.response.bytes_transferred, 1)

        segit = proxy_server.SegmentedIterable(self.controller, 'lc', listing)
        self.assertEquals(''.join(segit.app_iter_range(3, None)), '')

        segit = proxy_server.SegmentedIterable(self.controller, 'lc', listing)
        self.assertEquals(''.join(segit.app_iter_range(3, 5)), '')

        segit = proxy_server.SegmentedIterable(self.controller, 'lc', listing)
        segit.response = Stub()
        self.assertEquals(''.join(segit.app_iter_range(None, 5)), '1')
        self.assertEquals(segit.response.bytes_transferred, 1)

    def test_app_iter_range_with_two_segments(self):
        listing = [{'name': 'o1', 'bytes': 1}, {'name': 'o2', 'bytes': 2}]

        segit = proxy_server.SegmentedIterable(self.controller, 'lc', listing)
        segit.response = Stub()
        self.assertEquals(''.join(segit.app_iter_range(None, None)), '122')
        self.assertEquals(segit.response.bytes_transferred, 3)

        segit = proxy_server.SegmentedIterable(self.controller, 'lc', listing)
        segit.response = Stub()
        self.assertEquals(''.join(segit.app_iter_range(1, None)), '22')
        self.assertEquals(segit.response.bytes_transferred, 2)

        segit = proxy_server.SegmentedIterable(self.controller, 'lc', listing)
        segit.response = Stub()
        self.assertEquals(''.join(segit.app_iter_range(1, 5)), '22')
        self.assertEquals(segit.response.bytes_transferred, 2)

        segit = proxy_server.SegmentedIterable(self.controller, 'lc', listing)
        segit.response = Stub()
        self.assertEquals(''.join(segit.app_iter_range(None, 2)), '12')
        self.assertEquals(segit.response.bytes_transferred, 2)

    def test_app_iter_range_with_many_segments(self):
        listing = [{'name': 'o1', 'bytes': 1}, {'name': 'o2', 'bytes': 2},
            {'name': 'o3', 'bytes': 3}, {'name': 'o4', 'bytes': 4}, {'name':
            'o5', 'bytes': 5}]

        segit = proxy_server.SegmentedIterable(self.controller, 'lc', listing)
        segit.response = Stub()
        self.assertEquals(''.join(segit.app_iter_range(None, None)),
            '122333444455555')
        self.assertEquals(segit.response.bytes_transferred, 15)

        segit = proxy_server.SegmentedIterable(self.controller, 'lc', listing)
        segit.response = Stub()
        self.assertEquals(''.join(segit.app_iter_range(3, None)),
            '333444455555')
        self.assertEquals(segit.response.bytes_transferred, 12)

        segit = proxy_server.SegmentedIterable(self.controller, 'lc', listing)
        segit.response = Stub()
        self.assertEquals(''.join(segit.app_iter_range(5, None)), '3444455555')
        self.assertEquals(segit.response.bytes_transferred, 10)

        segit = proxy_server.SegmentedIterable(self.controller, 'lc', listing)
        segit.response = Stub()
        self.assertEquals(''.join(segit.app_iter_range(None, 6)), '122333')
        self.assertEquals(segit.response.bytes_transferred, 6)

        segit = proxy_server.SegmentedIterable(self.controller, 'lc', listing)
        segit.response = Stub()
        self.assertEquals(''.join(segit.app_iter_range(None, 7)), '1223334')
        self.assertEquals(segit.response.bytes_transferred, 7)

        segit = proxy_server.SegmentedIterable(self.controller, 'lc', listing)
        segit.response = Stub()
        self.assertEquals(''.join(segit.app_iter_range(3, 7)), '3334')
        self.assertEquals(segit.response.bytes_transferred, 4)

        segit = proxy_server.SegmentedIterable(self.controller, 'lc', listing)
        segit.response = Stub()
        self.assertEquals(''.join(segit.app_iter_range(5, 7)), '34')
        self.assertEquals(segit.response.bytes_transferred, 2)


if __name__ == '__main__':
    setup()
    try:
        unittest.main()
    finally:
        teardown()
