# Copyright (c) 2010 OpenStack Foundation
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

"""Tests for swift.common.wsgi"""

import errno
import logging
import mimetools
import socket
import unittest
import os
from textwrap import dedent
from contextlib import nested
from StringIO import StringIO
from collections import defaultdict
from urllib import quote

from eventlet import listen

import mock

import swift.common.middleware.catch_errors
import swift.common.middleware.gatekeeper
import swift.proxy.server

import swift.obj.server as obj_server
import swift.container.server as container_server
import swift.account.server as account_server
from swift.common.swob import Request
from swift.common import wsgi, utils
from swift.common.storage_policy import POLICIES

from test.unit import temptree, with_tempdir, write_fake_ring, patch_policies

from paste.deploy import loadwsgi


def _fake_rings(tmpdir):
    write_fake_ring(os.path.join(tmpdir, 'account.ring.gz'))
    write_fake_ring(os.path.join(tmpdir, 'container.ring.gz'))
    for policy in POLICIES:
        obj_ring_path = \
            os.path.join(tmpdir, policy.ring_name + '.ring.gz')
        write_fake_ring(obj_ring_path)
        # make sure there's no other ring cached on this policy
        policy.object_ring = None


@patch_policies
class TestWSGI(unittest.TestCase):
    """Tests for swift.common.wsgi"""

    def setUp(self):
        utils.HASH_PATH_PREFIX = 'startcap'
        self._orig_parsetype = mimetools.Message.parsetype

    def tearDown(self):
        mimetools.Message.parsetype = self._orig_parsetype

    def test_monkey_patch_mimetools(self):
        sio = StringIO('blah')
        self.assertEquals(mimetools.Message(sio).type, 'text/plain')
        sio = StringIO('blah')
        self.assertEquals(mimetools.Message(sio).plisttext, '')
        sio = StringIO('blah')
        self.assertEquals(mimetools.Message(sio).maintype, 'text')
        sio = StringIO('blah')
        self.assertEquals(mimetools.Message(sio).subtype, 'plain')
        sio = StringIO('Content-Type: text/html; charset=ISO-8859-4')
        self.assertEquals(mimetools.Message(sio).type, 'text/html')
        sio = StringIO('Content-Type: text/html; charset=ISO-8859-4')
        self.assertEquals(mimetools.Message(sio).plisttext,
                          '; charset=ISO-8859-4')
        sio = StringIO('Content-Type: text/html; charset=ISO-8859-4')
        self.assertEquals(mimetools.Message(sio).maintype, 'text')
        sio = StringIO('Content-Type: text/html; charset=ISO-8859-4')
        self.assertEquals(mimetools.Message(sio).subtype, 'html')

        wsgi.monkey_patch_mimetools()
        sio = StringIO('blah')
        self.assertEquals(mimetools.Message(sio).type, None)
        sio = StringIO('blah')
        self.assertEquals(mimetools.Message(sio).plisttext, '')
        sio = StringIO('blah')
        self.assertEquals(mimetools.Message(sio).maintype, None)
        sio = StringIO('blah')
        self.assertEquals(mimetools.Message(sio).subtype, None)
        sio = StringIO('Content-Type: text/html; charset=ISO-8859-4')
        self.assertEquals(mimetools.Message(sio).type, 'text/html')
        sio = StringIO('Content-Type: text/html; charset=ISO-8859-4')
        self.assertEquals(mimetools.Message(sio).plisttext,
                          '; charset=ISO-8859-4')
        sio = StringIO('Content-Type: text/html; charset=ISO-8859-4')
        self.assertEquals(mimetools.Message(sio).maintype, 'text')
        sio = StringIO('Content-Type: text/html; charset=ISO-8859-4')
        self.assertEquals(mimetools.Message(sio).subtype, 'html')

    def test_init_request_processor(self):
        config = """
        [DEFAULT]
        swift_dir = TEMPDIR

        [pipeline:main]
        pipeline = proxy-server

        [app:proxy-server]
        use = egg:swift#proxy
        conn_timeout = 0.2
        """
        contents = dedent(config)
        with temptree(['proxy-server.conf']) as t:
            conf_file = os.path.join(t, 'proxy-server.conf')
            with open(conf_file, 'w') as f:
                f.write(contents.replace('TEMPDIR', t))
            _fake_rings(t)
            app, conf, logger, log_name = wsgi.init_request_processor(
                conf_file, 'proxy-server')
        # verify pipeline is catch_errors -> dlo -> proxy-server
        expected = swift.common.middleware.catch_errors.CatchErrorMiddleware
        self.assert_(isinstance(app, expected))

        app = app.app
        expected = swift.common.middleware.gatekeeper.GatekeeperMiddleware
        self.assert_(isinstance(app, expected))

        app = app.app
        expected = swift.common.middleware.dlo.DynamicLargeObject
        self.assert_(isinstance(app, expected))

        app = app.app
        expected = swift.proxy.server.Application
        self.assert_(isinstance(app, expected))
        # config settings applied to app instance
        self.assertEquals(0.2, app.conn_timeout)
        # appconfig returns values from 'proxy-server' section
        expected = {
            '__file__': conf_file,
            'here': os.path.dirname(conf_file),
            'conn_timeout': '0.2',
            'swift_dir': t,
        }
        self.assertEquals(expected, conf)
        # logger works
        logger.info('testing')
        self.assertEquals('proxy-server', log_name)

    @with_tempdir
    def test_loadapp_from_file(self, tempdir):
        conf_path = os.path.join(tempdir, 'object-server.conf')
        conf_body = """
        [app:main]
        use = egg:swift#object
        """
        contents = dedent(conf_body)
        with open(conf_path, 'w') as f:
            f.write(contents)
        app = wsgi.loadapp(conf_path)
        self.assertTrue(isinstance(app, obj_server.ObjectController))

    def test_loadapp_from_string(self):
        conf_body = """
        [app:main]
        use = egg:swift#object
        """
        app = wsgi.loadapp(wsgi.ConfigString(conf_body))
        self.assertTrue(isinstance(app, obj_server.ObjectController))

    def test_init_request_processor_from_conf_dir(self):
        config_dir = {
            'proxy-server.conf.d/pipeline.conf': """
            [pipeline:main]
            pipeline = catch_errors proxy-server
            """,
            'proxy-server.conf.d/app.conf': """
            [app:proxy-server]
            use = egg:swift#proxy
            conn_timeout = 0.2
            """,
            'proxy-server.conf.d/catch-errors.conf': """
            [filter:catch_errors]
            use = egg:swift#catch_errors
            """
        }
        # strip indent from test config contents
        config_dir = dict((f, dedent(c)) for (f, c) in config_dir.items())
        with mock.patch('swift.proxy.server.Application.modify_wsgi_pipeline'):
            with temptree(*zip(*config_dir.items())) as conf_root:
                conf_dir = os.path.join(conf_root, 'proxy-server.conf.d')
                with open(os.path.join(conf_dir, 'swift.conf'), 'w') as f:
                    f.write('[DEFAULT]\nswift_dir = %s' % conf_root)
                _fake_rings(conf_root)
                app, conf, logger, log_name = wsgi.init_request_processor(
                    conf_dir, 'proxy-server')
        # verify pipeline is catch_errors -> proxy-server
        expected = swift.common.middleware.catch_errors.CatchErrorMiddleware
        self.assert_(isinstance(app, expected))
        self.assert_(isinstance(app.app, swift.proxy.server.Application))
        # config settings applied to app instance
        self.assertEquals(0.2, app.app.conn_timeout)
        # appconfig returns values from 'proxy-server' section
        expected = {
            '__file__': conf_dir,
            'here': conf_dir,
            'conn_timeout': '0.2',
            'swift_dir': conf_root,
        }
        self.assertEquals(expected, conf)
        # logger works
        logger.info('testing')
        self.assertEquals('proxy-server', log_name)

    def test_get_socket_bad_values(self):
        # first try with no port set
        self.assertRaises(wsgi.ConfigFilePortError, wsgi.get_socket, {})
        # next try with a bad port value set
        self.assertRaises(wsgi.ConfigFilePortError, wsgi.get_socket,
                          {'bind_port': 'abc'})
        self.assertRaises(wsgi.ConfigFilePortError, wsgi.get_socket,
                          {'bind_port': None})

    def test_get_socket(self):
        # stubs
        conf = {'bind_port': 54321}
        ssl_conf = conf.copy()
        ssl_conf.update({
            'cert_file': '',
            'key_file': '',
        })

        # mocks
        class MockSocket(object):
            def __init__(self):
                self.opts = defaultdict(dict)

            def setsockopt(self, level, optname, value):
                self.opts[level][optname] = value

        def mock_listen(*args, **kwargs):
            return MockSocket()

        class MockSsl(object):
            def __init__(self):
                self.wrap_socket_called = []

            def wrap_socket(self, sock, **kwargs):
                self.wrap_socket_called.append(kwargs)
                return sock

        # patch
        old_listen = wsgi.listen
        old_ssl = wsgi.ssl
        try:
            wsgi.listen = mock_listen
            wsgi.ssl = MockSsl()
            # test
            sock = wsgi.get_socket(conf)
            # assert
            self.assert_(isinstance(sock, MockSocket))
            expected_socket_opts = {
                socket.SOL_SOCKET: {
                    socket.SO_REUSEADDR: 1,
                    socket.SO_KEEPALIVE: 1,
                },
                socket.IPPROTO_TCP: {
                    socket.TCP_NODELAY: 1,
                }
            }
            if hasattr(socket, 'TCP_KEEPIDLE'):
                expected_socket_opts[socket.IPPROTO_TCP][
                    socket.TCP_KEEPIDLE] = 600
            self.assertEquals(sock.opts, expected_socket_opts)
            # test ssl
            sock = wsgi.get_socket(ssl_conf)
            expected_kwargs = {
                'certfile': '',
                'keyfile': '',
            }
            self.assertEquals(wsgi.ssl.wrap_socket_called, [expected_kwargs])
        finally:
            wsgi.listen = old_listen
            wsgi.ssl = old_ssl

    def test_address_in_use(self):
        # stubs
        conf = {'bind_port': 54321}

        # mocks
        def mock_listen(*args, **kwargs):
            raise socket.error(errno.EADDRINUSE)

        def value_error_listen(*args, **kwargs):
            raise ValueError('fake')

        def mock_sleep(*args):
            pass

        class MockTime(object):
            """Fast clock advances 10 seconds after every call to time
            """
            def __init__(self):
                self.current_time = old_time.time()

            def time(self, *args, **kwargs):
                rv = self.current_time
                # advance for next call
                self.current_time += 10
                return rv

        old_listen = wsgi.listen
        old_sleep = wsgi.sleep
        old_time = wsgi.time
        try:
            wsgi.listen = mock_listen
            wsgi.sleep = mock_sleep
            wsgi.time = MockTime()
            # test error
            self.assertRaises(Exception, wsgi.get_socket, conf)
            # different error
            wsgi.listen = value_error_listen
            self.assertRaises(ValueError, wsgi.get_socket, conf)
        finally:
            wsgi.listen = old_listen
            wsgi.sleep = old_sleep
            wsgi.time = old_time

    def test_run_server(self):
        config = """
        [DEFAULT]
        client_timeout = 30
        max_clients = 1000
        swift_dir = TEMPDIR

        [pipeline:main]
        pipeline = proxy-server

        [app:proxy-server]
        use = egg:swift#proxy
        # while "set" values normally override default
        set client_timeout = 20
        # this section is not in conf during run_server
        set max_clients = 10
        """

        contents = dedent(config)
        with temptree(['proxy-server.conf']) as t:
            conf_file = os.path.join(t, 'proxy-server.conf')
            with open(conf_file, 'w') as f:
                f.write(contents.replace('TEMPDIR', t))
            _fake_rings(t)
            with mock.patch('swift.proxy.server.Application.'
                            'modify_wsgi_pipeline'):
                with mock.patch('swift.common.wsgi.wsgi') as _wsgi:
                    with mock.patch('swift.common.wsgi.eventlet') as _eventlet:
                        with mock.patch('swift.common.wsgi.inspect'):
                            conf = wsgi.appconfig(conf_file)
                            logger = logging.getLogger('test')
                            sock = listen(('localhost', 0))
                            wsgi.run_server(conf, logger, sock)
        self.assertEquals('HTTP/1.0',
                          _wsgi.HttpProtocol.default_request_version)
        self.assertEquals(30, _wsgi.WRITE_TIMEOUT)
        _eventlet.hubs.use_hub.assert_called_with(utils.get_hub())
        _eventlet.patcher.monkey_patch.assert_called_with(all=False,
                                                          socket=True)
        _eventlet.debug.hub_exceptions.assert_called_with(False)
        _wsgi.server.assert_called()
        args, kwargs = _wsgi.server.call_args
        server_sock, server_app, server_logger = args
        self.assertEquals(sock, server_sock)
        self.assert_(isinstance(server_app, swift.proxy.server.Application))
        self.assertEquals(20, server_app.client_timeout)
        self.assert_(isinstance(server_logger, wsgi.NullLogger))
        self.assert_('custom_pool' in kwargs)
        self.assertEquals(1000, kwargs['custom_pool'].size)

    def test_run_server_with_latest_eventlet(self):
        config = """
        [DEFAULT]
        swift_dir = TEMPDIR

        [pipeline:main]
        pipeline = proxy-server

        [app:proxy-server]
        use = egg:swift#proxy
        """

        def argspec_stub(server):
            return mock.MagicMock(args=['capitalize_response_headers'])

        contents = dedent(config)
        with temptree(['proxy-server.conf']) as t:
            conf_file = os.path.join(t, 'proxy-server.conf')
            with open(conf_file, 'w') as f:
                f.write(contents.replace('TEMPDIR', t))
            _fake_rings(t)
            with nested(
                mock.patch('swift.proxy.server.Application.'
                           'modify_wsgi_pipeline'),
                mock.patch('swift.common.wsgi.wsgi'),
                mock.patch('swift.common.wsgi.eventlet'),
                mock.patch('swift.common.wsgi.inspect',
                           getargspec=argspec_stub)) as (_, _wsgi, _, _):
                conf = wsgi.appconfig(conf_file)
                logger = logging.getLogger('test')
                sock = listen(('localhost', 0))
                wsgi.run_server(conf, logger, sock)

        _wsgi.server.assert_called()
        args, kwargs = _wsgi.server.call_args
        self.assertEquals(kwargs.get('capitalize_response_headers'), False)

    def test_run_server_conf_dir(self):
        config_dir = {
            'proxy-server.conf.d/pipeline.conf': """
            [pipeline:main]
            pipeline = proxy-server
            """,
            'proxy-server.conf.d/app.conf': """
            [app:proxy-server]
            use = egg:swift#proxy
            """,
            'proxy-server.conf.d/default.conf': """
            [DEFAULT]
            client_timeout = 30
            """
        }
        # strip indent from test config contents
        config_dir = dict((f, dedent(c)) for (f, c) in config_dir.items())
        with temptree(*zip(*config_dir.items())) as conf_root:
            conf_dir = os.path.join(conf_root, 'proxy-server.conf.d')
            with open(os.path.join(conf_dir, 'swift.conf'), 'w') as f:
                f.write('[DEFAULT]\nswift_dir = %s' % conf_root)
            _fake_rings(conf_root)
            with mock.patch('swift.proxy.server.Application.'
                            'modify_wsgi_pipeline'):
                with mock.patch('swift.common.wsgi.wsgi') as _wsgi:
                    with mock.patch('swift.common.wsgi.eventlet') as _eventlet:
                        with mock.patch.dict('os.environ', {'TZ': ''}):
                            with mock.patch('swift.common.wsgi.inspect'):
                                conf = wsgi.appconfig(conf_dir)
                                logger = logging.getLogger('test')
                                sock = listen(('localhost', 0))
                                wsgi.run_server(conf, logger, sock)
                                self.assert_(os.environ['TZ'] is not '')

        self.assertEquals('HTTP/1.0',
                          _wsgi.HttpProtocol.default_request_version)
        self.assertEquals(30, _wsgi.WRITE_TIMEOUT)
        _eventlet.hubs.use_hub.assert_called_with(utils.get_hub())
        _eventlet.patcher.monkey_patch.assert_called_with(all=False,
                                                          socket=True)
        _eventlet.debug.hub_exceptions.assert_called_with(False)
        _wsgi.server.assert_called()
        args, kwargs = _wsgi.server.call_args
        server_sock, server_app, server_logger = args
        self.assertEquals(sock, server_sock)
        self.assert_(isinstance(server_app, swift.proxy.server.Application))
        self.assert_(isinstance(server_logger, wsgi.NullLogger))
        self.assert_('custom_pool' in kwargs)

    def test_run_server_debug(self):
        config = """
        [DEFAULT]
        eventlet_debug = yes
        client_timeout = 30
        max_clients = 1000
        swift_dir = TEMPDIR

        [pipeline:main]
        pipeline = proxy-server

        [app:proxy-server]
        use = egg:swift#proxy
        # while "set" values normally override default
        set client_timeout = 20
        # this section is not in conf during run_server
        set max_clients = 10
        """

        contents = dedent(config)
        with temptree(['proxy-server.conf']) as t:
            conf_file = os.path.join(t, 'proxy-server.conf')
            with open(conf_file, 'w') as f:
                f.write(contents.replace('TEMPDIR', t))
            _fake_rings(t)
            with mock.patch('swift.proxy.server.Application.'
                            'modify_wsgi_pipeline'):
                with mock.patch('swift.common.wsgi.wsgi') as _wsgi:
                    mock_server = _wsgi.server
                    _wsgi.server = lambda *args, **kwargs: mock_server(
                        *args, **kwargs)
                    with mock.patch('swift.common.wsgi.eventlet') as _eventlet:
                        conf = wsgi.appconfig(conf_file)
                        logger = logging.getLogger('test')
                        sock = listen(('localhost', 0))
                        wsgi.run_server(conf, logger, sock)
        self.assertEquals('HTTP/1.0',
                          _wsgi.HttpProtocol.default_request_version)
        self.assertEquals(30, _wsgi.WRITE_TIMEOUT)
        _eventlet.hubs.use_hub.assert_called_with(utils.get_hub())
        _eventlet.patcher.monkey_patch.assert_called_with(all=False,
                                                          socket=True)
        _eventlet.debug.hub_exceptions.assert_called_with(True)
        mock_server.assert_called()
        args, kwargs = mock_server.call_args
        server_sock, server_app, server_logger = args
        self.assertEquals(sock, server_sock)
        self.assert_(isinstance(server_app, swift.proxy.server.Application))
        self.assertEquals(20, server_app.client_timeout)
        self.assertEqual(server_logger, None)
        self.assert_('custom_pool' in kwargs)
        self.assertEquals(1000, kwargs['custom_pool'].size)

    def test_appconfig_dir_ignores_hidden_files(self):
        config_dir = {
            'server.conf.d/01.conf': """
            [app:main]
            use = egg:swift#proxy
            port = 8080
            """,
            'server.conf.d/.01.conf.swp': """
            [app:main]
            use = egg:swift#proxy
            port = 8081
            """,
        }
        # strip indent from test config contents
        config_dir = dict((f, dedent(c)) for (f, c) in config_dir.items())
        with temptree(*zip(*config_dir.items())) as path:
            conf_dir = os.path.join(path, 'server.conf.d')
            conf = wsgi.appconfig(conf_dir)
        expected = {
            '__file__': os.path.join(path, 'server.conf.d'),
            'here': os.path.join(path, 'server.conf.d'),
            'port': '8080',
        }
        self.assertEquals(conf, expected)

    def test_pre_auth_wsgi_input(self):
        oldenv = {}
        newenv = wsgi.make_pre_authed_env(oldenv)
        self.assertTrue('wsgi.input' in newenv)
        self.assertEquals(newenv['wsgi.input'].read(), '')

        oldenv = {'wsgi.input': StringIO('original wsgi.input')}
        newenv = wsgi.make_pre_authed_env(oldenv)
        self.assertTrue('wsgi.input' in newenv)
        self.assertEquals(newenv['wsgi.input'].read(), '')

        oldenv = {'swift.source': 'UT'}
        newenv = wsgi.make_pre_authed_env(oldenv)
        self.assertEquals(newenv['swift.source'], 'UT')

        oldenv = {'swift.source': 'UT'}
        newenv = wsgi.make_pre_authed_env(oldenv, swift_source='SA')
        self.assertEquals(newenv['swift.source'], 'SA')

    def test_pre_auth_req(self):
        class FakeReq(object):
            @classmethod
            def fake_blank(cls, path, environ=None, body='', headers=None):
                if environ is None:
                    environ = {}
                if headers is None:
                    headers = {}
                self.assertEquals(environ['swift.authorize']('test'), None)
                self.assertFalse('HTTP_X_TRANS_ID' in environ)
        was_blank = Request.blank
        Request.blank = FakeReq.fake_blank
        wsgi.make_pre_authed_request({'HTTP_X_TRANS_ID': '1234'},
                                     'PUT', '/', body='tester', headers={})
        wsgi.make_pre_authed_request({'HTTP_X_TRANS_ID': '1234'},
                                     'PUT', '/', headers={})
        Request.blank = was_blank

    def test_pre_auth_req_with_quoted_path(self):
        r = wsgi.make_pre_authed_request(
            {'HTTP_X_TRANS_ID': '1234'}, 'PUT', path=quote('/a space'),
            body='tester', headers={})
        self.assertEquals(r.path, quote('/a space'))

    def test_pre_auth_req_drops_query(self):
        r = wsgi.make_pre_authed_request(
            {'QUERY_STRING': 'original'}, 'GET', 'path')
        self.assertEquals(r.query_string, 'original')
        r = wsgi.make_pre_authed_request(
            {'QUERY_STRING': 'original'}, 'GET', 'path?replacement')
        self.assertEquals(r.query_string, 'replacement')
        r = wsgi.make_pre_authed_request(
            {'QUERY_STRING': 'original'}, 'GET', 'path?')
        self.assertEquals(r.query_string, '')

    def test_pre_auth_req_with_body(self):
        r = wsgi.make_pre_authed_request(
            {'QUERY_STRING': 'original'}, 'GET', 'path', 'the body')
        self.assertEquals(r.body, 'the body')

    def test_pre_auth_creates_script_name(self):
        e = wsgi.make_pre_authed_env({})
        self.assertTrue('SCRIPT_NAME' in e)

    def test_pre_auth_copies_script_name(self):
        e = wsgi.make_pre_authed_env({'SCRIPT_NAME': '/script_name'})
        self.assertEquals(e['SCRIPT_NAME'], '/script_name')

    def test_pre_auth_copies_script_name_unless_path_overridden(self):
        e = wsgi.make_pre_authed_env({'SCRIPT_NAME': '/script_name'},
                                     path='/override')
        self.assertEquals(e['SCRIPT_NAME'], '')
        self.assertEquals(e['PATH_INFO'], '/override')

    def test_pre_auth_req_swift_source(self):
        r = wsgi.make_pre_authed_request(
            {'QUERY_STRING': 'original'}, 'GET', 'path', 'the body',
            swift_source='UT')
        self.assertEquals(r.body, 'the body')
        self.assertEquals(r.environ['swift.source'], 'UT')

    def test_run_server_global_conf_callback(self):
        calls = defaultdict(lambda: 0)

        def _initrp(conf_file, app_section, *args, **kwargs):
            return (
                {'__file__': 'test', 'workers': 0},
                'logger',
                'log_name')

        def _global_conf_callback(preloaded_app_conf, global_conf):
            calls['_global_conf_callback'] += 1
            self.assertEqual(
                preloaded_app_conf, {'__file__': 'test', 'workers': 0})
            self.assertEqual(global_conf, {'log_name': 'log_name'})
            global_conf['test1'] = 'one'

        def _loadapp(uri, name=None, **kwargs):
            calls['_loadapp'] += 1
            self.assertTrue('global_conf' in kwargs)
            self.assertEqual(kwargs['global_conf'],
                             {'log_name': 'log_name', 'test1': 'one'})

        with nested(
                mock.patch.object(wsgi, '_initrp', _initrp),
                mock.patch.object(wsgi, 'get_socket'),
                mock.patch.object(wsgi, 'drop_privileges'),
                mock.patch.object(wsgi, 'loadapp', _loadapp),
                mock.patch.object(wsgi, 'capture_stdio'),
                mock.patch.object(wsgi, 'run_server')):
            wsgi.run_wsgi('conf_file', 'app_section',
                          global_conf_callback=_global_conf_callback)
        self.assertEqual(calls['_global_conf_callback'], 1)
        self.assertEqual(calls['_loadapp'], 1)

    def test_run_server_success(self):
        calls = defaultdict(lambda: 0)

        def _initrp(conf_file, app_section, *args, **kwargs):
            calls['_initrp'] += 1
            return (
                {'__file__': 'test', 'workers': 0},
                'logger',
                'log_name')

        def _loadapp(uri, name=None, **kwargs):
            calls['_loadapp'] += 1

        with nested(
                mock.patch.object(wsgi, '_initrp', _initrp),
                mock.patch.object(wsgi, 'get_socket'),
                mock.patch.object(wsgi, 'drop_privileges'),
                mock.patch.object(wsgi, 'loadapp', _loadapp),
                mock.patch.object(wsgi, 'capture_stdio'),
                mock.patch.object(wsgi, 'run_server')):
            rc = wsgi.run_wsgi('conf_file', 'app_section')
        self.assertEqual(calls['_initrp'], 1)
        self.assertEqual(calls['_loadapp'], 1)
        self.assertEqual(rc, 0)

    def test_run_server_failure1(self):
        calls = defaultdict(lambda: 0)

        def _initrp(conf_file, app_section, *args, **kwargs):
            calls['_initrp'] += 1
            raise wsgi.ConfigFileError('test exception')

        def _loadapp(uri, name=None, **kwargs):
            calls['_loadapp'] += 1

        with nested(
                mock.patch.object(wsgi, '_initrp', _initrp),
                mock.patch.object(wsgi, 'get_socket'),
                mock.patch.object(wsgi, 'drop_privileges'),
                mock.patch.object(wsgi, 'loadapp', _loadapp),
                mock.patch.object(wsgi, 'capture_stdio'),
                mock.patch.object(wsgi, 'run_server')):
            rc = wsgi.run_wsgi('conf_file', 'app_section')
        self.assertEqual(calls['_initrp'], 1)
        self.assertEqual(calls['_loadapp'], 0)
        self.assertEqual(rc, 1)

    def test_pre_auth_req_with_empty_env_no_path(self):
        r = wsgi.make_pre_authed_request(
            {}, 'GET')
        self.assertEquals(r.path, quote(''))
        self.assertTrue('SCRIPT_NAME' in r.environ)
        self.assertTrue('PATH_INFO' in r.environ)

    def test_pre_auth_req_with_env_path(self):
        r = wsgi.make_pre_authed_request(
            {'PATH_INFO': '/unquoted path with %20'}, 'GET')
        self.assertEquals(r.path, quote('/unquoted path with %20'))
        self.assertEquals(r.environ['SCRIPT_NAME'], '')

    def test_pre_auth_req_with_env_script(self):
        r = wsgi.make_pre_authed_request({'SCRIPT_NAME': '/hello'}, 'GET')
        self.assertEquals(r.path, quote('/hello'))

    def test_pre_auth_req_with_env_path_and_script(self):
        env = {'PATH_INFO': '/unquoted path with %20',
               'SCRIPT_NAME': '/script'}
        r = wsgi.make_pre_authed_request(env, 'GET')
        expected_path = quote(env['SCRIPT_NAME'] + env['PATH_INFO'])
        self.assertEquals(r.path, expected_path)
        env = {'PATH_INFO': '', 'SCRIPT_NAME': '/script'}
        r = wsgi.make_pre_authed_request(env, 'GET')
        self.assertEquals(r.path, '/script')
        env = {'PATH_INFO': '/path', 'SCRIPT_NAME': ''}
        r = wsgi.make_pre_authed_request(env, 'GET')
        self.assertEquals(r.path, '/path')
        env = {'PATH_INFO': '', 'SCRIPT_NAME': ''}
        r = wsgi.make_pre_authed_request(env, 'GET')
        self.assertEquals(r.path, '')

    def test_pre_auth_req_path_overrides_env(self):
        env = {'PATH_INFO': '/path', 'SCRIPT_NAME': '/script'}
        r = wsgi.make_pre_authed_request(env, 'GET', '/override')
        self.assertEquals(r.path, '/override')
        self.assertEquals(r.environ['SCRIPT_NAME'], '')
        self.assertEquals(r.environ['PATH_INFO'], '/override')


class TestWSGIContext(unittest.TestCase):

    def test_app_call(self):
        statuses = ['200 Ok', '404 Not Found']

        def app(env, start_response):
            start_response(statuses.pop(0), [('Content-Length', '3')])
            yield 'Ok\n'

        wc = wsgi.WSGIContext(app)
        r = Request.blank('/')
        it = wc._app_call(r.environ)
        self.assertEquals(wc._response_status, '200 Ok')
        self.assertEquals(''.join(it), 'Ok\n')
        r = Request.blank('/')
        it = wc._app_call(r.environ)
        self.assertEquals(wc._response_status, '404 Not Found')
        self.assertEquals(''.join(it), 'Ok\n')

    def test_app_iter_is_closable(self):

        def app(env, start_response):
            start_response('200 OK', [('Content-Length', '25')])
            yield 'aaaaa'
            yield 'bbbbb'
            yield 'ccccc'
            yield 'ddddd'
            yield 'eeeee'

        wc = wsgi.WSGIContext(app)
        r = Request.blank('/')
        iterable = wc._app_call(r.environ)
        self.assertEquals(wc._response_status, '200 OK')

        iterator = iter(iterable)
        self.assertEqual('aaaaa', iterator.next())
        self.assertEqual('bbbbb', iterator.next())
        iterable.close()
        self.assertRaises(StopIteration, iterator.next)


class TestPipelineWrapper(unittest.TestCase):

    def setUp(self):
        config = """
        [DEFAULT]
        swift_dir = TEMPDIR

        [pipeline:main]
        pipeline = healthcheck catch_errors tempurl proxy-server

        [app:proxy-server]
        use = egg:swift#proxy
        conn_timeout = 0.2

        [filter:catch_errors]
        use = egg:swift#catch_errors

        [filter:healthcheck]
        use = egg:swift#healthcheck

        [filter:tempurl]
        paste.filter_factory = swift.common.middleware.tempurl:filter_factory
        """

        contents = dedent(config)
        with temptree(['proxy-server.conf']) as t:
            conf_file = os.path.join(t, 'proxy-server.conf')
            with open(conf_file, 'w') as f:
                f.write(contents.replace('TEMPDIR', t))
            ctx = wsgi.loadcontext(loadwsgi.APP, conf_file, global_conf={})
            self.pipe = wsgi.PipelineWrapper(ctx)

    def _entry_point_names(self):
        # Helper method to return a list of the entry point names for the
        # filters in the pipeline.
        return [c.entry_point_name for c in self.pipe.context.filter_contexts]

    def test_startswith(self):
        self.assertTrue(self.pipe.startswith("healthcheck"))
        self.assertFalse(self.pipe.startswith("tempurl"))

    def test_startswith_no_filters(self):
        config = """
        [DEFAULT]
        swift_dir = TEMPDIR

        [pipeline:main]
        pipeline = proxy-server

        [app:proxy-server]
        use = egg:swift#proxy
        conn_timeout = 0.2
        """
        contents = dedent(config)
        with temptree(['proxy-server.conf']) as t:
            conf_file = os.path.join(t, 'proxy-server.conf')
            with open(conf_file, 'w') as f:
                f.write(contents.replace('TEMPDIR', t))
            ctx = wsgi.loadcontext(loadwsgi.APP, conf_file, global_conf={})
            pipe = wsgi.PipelineWrapper(ctx)
        self.assertTrue(pipe.startswith('proxy'))

    def test_insert_filter(self):
        original_modules = ['healthcheck', 'catch_errors', None]
        self.assertEqual(self._entry_point_names(), original_modules)

        self.pipe.insert_filter(self.pipe.create_filter('catch_errors'))
        expected_modules = ['catch_errors', 'healthcheck',
                            'catch_errors', None]
        self.assertEqual(self._entry_point_names(), expected_modules)

    def test_str(self):
        self.assertEqual(
            str(self.pipe),
            "healthcheck catch_errors tempurl proxy-server")

    def test_str_unknown_filter(self):
        del self.pipe.context.filter_contexts[0].__dict__['name']
        self.pipe.context.filter_contexts[0].object = 'mysterious'
        self.assertEqual(
            str(self.pipe),
            "<unknown> catch_errors tempurl proxy-server")


@patch_policies
@mock.patch('swift.common.utils.HASH_PATH_SUFFIX', new='endcap')
class TestPipelineModification(unittest.TestCase):
    def pipeline_modules(self, app):
        # This is rather brittle; it'll break if a middleware stores its app
        # anywhere other than an attribute named "app", but it works for now.
        pipe = []
        for _ in xrange(1000):
            pipe.append(app.__class__.__module__)
            if not hasattr(app, 'app'):
                break
            app = app.app
        return pipe

    def test_load_app(self):
        config = """
        [DEFAULT]
        swift_dir = TEMPDIR

        [pipeline:main]
        pipeline = healthcheck proxy-server

        [app:proxy-server]
        use = egg:swift#proxy
        conn_timeout = 0.2

        [filter:catch_errors]
        use = egg:swift#catch_errors

        [filter:healthcheck]
        use = egg:swift#healthcheck
        """

        def modify_func(app, pipe):
            new = pipe.create_filter('catch_errors')
            pipe.insert_filter(new)

        contents = dedent(config)
        with temptree(['proxy-server.conf']) as t:
            conf_file = os.path.join(t, 'proxy-server.conf')
            with open(conf_file, 'w') as f:
                f.write(contents.replace('TEMPDIR', t))
            _fake_rings(t)
            with mock.patch(
                    'swift.proxy.server.Application.modify_wsgi_pipeline',
                    modify_func):
                app = wsgi.loadapp(conf_file, global_conf={})
            exp = swift.common.middleware.catch_errors.CatchErrorMiddleware
            self.assertTrue(isinstance(app, exp), app)
            exp = swift.common.middleware.healthcheck.HealthCheckMiddleware
            self.assertTrue(isinstance(app.app, exp), app.app)
            exp = swift.proxy.server.Application
            self.assertTrue(isinstance(app.app.app, exp), app.app.app)

            # make sure you can turn off the pipeline modification if you want
            def blow_up(*_, **__):
                raise self.fail("needs more struts")

            with mock.patch(
                    'swift.proxy.server.Application.modify_wsgi_pipeline',
                    blow_up):
                app = wsgi.loadapp(conf_file, global_conf={},
                                   allow_modify_pipeline=False)

            # the pipeline was untouched
            exp = swift.common.middleware.healthcheck.HealthCheckMiddleware
            self.assertTrue(isinstance(app, exp), app)
            exp = swift.proxy.server.Application
            self.assertTrue(isinstance(app.app, exp), app.app)

    def test_proxy_unmodified_wsgi_pipeline(self):
        # Make sure things are sane even when we modify nothing
        config = """
        [DEFAULT]
        swift_dir = TEMPDIR

        [pipeline:main]
        pipeline = catch_errors gatekeeper proxy-server

        [app:proxy-server]
        use = egg:swift#proxy
        conn_timeout = 0.2

        [filter:catch_errors]
        use = egg:swift#catch_errors

        [filter:gatekeeper]
        use = egg:swift#gatekeeper
        """

        contents = dedent(config)
        with temptree(['proxy-server.conf']) as t:
            conf_file = os.path.join(t, 'proxy-server.conf')
            with open(conf_file, 'w') as f:
                f.write(contents.replace('TEMPDIR', t))
            _fake_rings(t)
            app = wsgi.loadapp(conf_file, global_conf={})

        self.assertEqual(self.pipeline_modules(app),
                         ['swift.common.middleware.catch_errors',
                          'swift.common.middleware.gatekeeper',
                          'swift.common.middleware.dlo',
                          'swift.proxy.server'])

    def test_proxy_modify_wsgi_pipeline(self):
        config = """
        [DEFAULT]
        swift_dir = TEMPDIR

        [pipeline:main]
        pipeline = healthcheck proxy-server

        [app:proxy-server]
        use = egg:swift#proxy
        conn_timeout = 0.2

        [filter:healthcheck]
        use = egg:swift#healthcheck
        """

        contents = dedent(config)
        with temptree(['proxy-server.conf']) as t:
            conf_file = os.path.join(t, 'proxy-server.conf')
            with open(conf_file, 'w') as f:
                f.write(contents.replace('TEMPDIR', t))
            _fake_rings(t)
            app = wsgi.loadapp(conf_file, global_conf={})

        self.assertEqual(self.pipeline_modules(app),
                         ['swift.common.middleware.catch_errors',
                          'swift.common.middleware.gatekeeper',
                          'swift.common.middleware.dlo',
                          'swift.common.middleware.healthcheck',
                          'swift.proxy.server'])

    def test_proxy_modify_wsgi_pipeline_ordering(self):
        config = """
        [DEFAULT]
        swift_dir = TEMPDIR

        [pipeline:main]
        pipeline = healthcheck proxy-logging bulk tempurl proxy-server

        [app:proxy-server]
        use = egg:swift#proxy
        conn_timeout = 0.2

        [filter:healthcheck]
        use = egg:swift#healthcheck

        [filter:proxy-logging]
        use = egg:swift#proxy_logging

        [filter:bulk]
        use = egg:swift#bulk

        [filter:tempurl]
        use = egg:swift#tempurl
        """

        new_req_filters = [
            # not in pipeline, no afters
            {'name': 'catch_errors'},
            # already in pipeline
            {'name': 'proxy_logging',
             'after_fn': lambda _: ['catch_errors']},
            # not in pipeline, comes after more than one thing
            {'name': 'container_quotas',
             'after_fn': lambda _: ['catch_errors', 'bulk']}]

        contents = dedent(config)
        with temptree(['proxy-server.conf']) as t:
            conf_file = os.path.join(t, 'proxy-server.conf')
            with open(conf_file, 'w') as f:
                f.write(contents.replace('TEMPDIR', t))
            _fake_rings(t)
            with mock.patch.object(swift.proxy.server, 'required_filters',
                                   new_req_filters):
                app = wsgi.loadapp(conf_file, global_conf={})

        self.assertEqual(self.pipeline_modules(app), [
            'swift.common.middleware.catch_errors',
            'swift.common.middleware.healthcheck',
            'swift.common.middleware.proxy_logging',
            'swift.common.middleware.bulk',
            'swift.common.middleware.container_quotas',
            'swift.common.middleware.tempurl',
            'swift.proxy.server'])

    def _proxy_modify_wsgi_pipeline(self, pipe):
        config = """
        [DEFAULT]
        swift_dir = TEMPDIR

        [pipeline:main]
        pipeline = %s

        [app:proxy-server]
        use = egg:swift#proxy
        conn_timeout = 0.2

        [filter:healthcheck]
        use = egg:swift#healthcheck

        [filter:catch_errors]
        use = egg:swift#catch_errors

        [filter:gatekeeper]
        use = egg:swift#gatekeeper
        """
        config = config % (pipe,)
        contents = dedent(config)
        with temptree(['proxy-server.conf']) as t:
            conf_file = os.path.join(t, 'proxy-server.conf')
            with open(conf_file, 'w') as f:
                f.write(contents.replace('TEMPDIR', t))
            _fake_rings(t)
            app = wsgi.loadapp(conf_file, global_conf={})
        return app

    def test_gatekeeper_insertion_catch_errors_configured_at_start(self):
        # catch_errors is configured at start, gatekeeper is not configured,
        # so gatekeeper should be inserted just after catch_errors
        pipe = 'catch_errors healthcheck proxy-server'
        app = self._proxy_modify_wsgi_pipeline(pipe)
        self.assertEqual(self.pipeline_modules(app), [
            'swift.common.middleware.catch_errors',
            'swift.common.middleware.gatekeeper',
            'swift.common.middleware.dlo',
            'swift.common.middleware.healthcheck',
            'swift.proxy.server'])

    def test_gatekeeper_insertion_catch_errors_configured_not_at_start(self):
        # catch_errors is configured, gatekeeper is not configured, so
        # gatekeeper should be inserted at start of pipeline
        pipe = 'healthcheck catch_errors proxy-server'
        app = self._proxy_modify_wsgi_pipeline(pipe)
        self.assertEqual(self.pipeline_modules(app), [
            'swift.common.middleware.gatekeeper',
            'swift.common.middleware.healthcheck',
            'swift.common.middleware.catch_errors',
            'swift.common.middleware.dlo',
            'swift.proxy.server'])

    def test_catch_errors_gatekeeper_configured_not_at_start(self):
        # catch_errors is configured, gatekeeper is configured, so
        # no change should be made to pipeline
        pipe = 'healthcheck catch_errors gatekeeper proxy-server'
        app = self._proxy_modify_wsgi_pipeline(pipe)
        self.assertEqual(self.pipeline_modules(app), [
            'swift.common.middleware.healthcheck',
            'swift.common.middleware.catch_errors',
            'swift.common.middleware.gatekeeper',
            'swift.common.middleware.dlo',
            'swift.proxy.server'])

    @with_tempdir
    def test_loadapp_proxy(self, tempdir):
        conf_path = os.path.join(tempdir, 'proxy-server.conf')
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
        """ % tempdir
        with open(conf_path, 'w') as f:
            f.write(dedent(conf_body))
        _fake_rings(tempdir)
        account_ring_path = os.path.join(tempdir, 'account.ring.gz')
        container_ring_path = os.path.join(tempdir, 'container.ring.gz')
        object_ring_paths = {}
        for policy in POLICIES:
            object_ring_paths[int(policy)] = os.path.join(
                tempdir, policy.ring_name + '.ring.gz')

        app = wsgi.loadapp(conf_path)
        proxy_app = app.app.app.app.app
        self.assertEqual(proxy_app.account_ring.serialized_path,
                         account_ring_path)
        self.assertEqual(proxy_app.container_ring.serialized_path,
                         container_ring_path)
        for policy_index, expected_path in object_ring_paths.items():
            object_ring = proxy_app.get_object_ring(policy_index)
            self.assertEqual(expected_path, object_ring.serialized_path)

    @with_tempdir
    def test_loadapp_storage(self, tempdir):
        expectations = {
            'object': obj_server.ObjectController,
            'container': container_server.ContainerController,
            'account': account_server.AccountController,
        }

        for server_type, controller in expectations.items():
            conf_path = os.path.join(
                tempdir, '%s-server.conf' % server_type)
            conf_body = """
            [DEFAULT]
            swift_dir = %s

            [app:main]
            use = egg:swift#%s
            """ % (tempdir, server_type)
            with open(conf_path, 'w') as f:
                f.write(dedent(conf_body))
            app = wsgi.loadapp(conf_path)
            self.assertTrue(isinstance(app, controller))

    def test_pipeline_property(self):
        depth = 3

        class FakeApp(object):
            pass

        class AppFilter(object):

            def __init__(self, app):
                self.app = app

        # make a pipeline
        app = FakeApp()
        filtered_app = app
        for i in range(depth):
            filtered_app = AppFilter(filtered_app)

        # AttributeError if no apps in the pipeline have attribute
        wsgi._add_pipeline_properties(filtered_app, 'foo')
        self.assertRaises(AttributeError, getattr, filtered_app, 'foo')

        # set the attribute
        self.assert_(isinstance(app, FakeApp))
        app.foo = 'bar'
        self.assertEqual(filtered_app.foo, 'bar')

        # attribute is cached
        app.foo = 'baz'
        self.assertEqual(filtered_app.foo, 'bar')


if __name__ == '__main__':
    unittest.main()
