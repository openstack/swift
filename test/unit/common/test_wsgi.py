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

from argparse import Namespace
import errno
import logging
import socket
import unittest
import os
from textwrap import dedent
from collections import defaultdict
import types

import eventlet.wsgi

from six import BytesIO
from six.moves.urllib.parse import quote

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

from test import listen_zero
from test.unit import (
    temptree, with_tempdir, write_fake_ring, patch_policies, FakeLogger)

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
        # verify pipeline is: catch_errors -> gatekeeper -> listing_formats ->
        #                     copy -> dlo -> proxy-server
        expected = swift.common.middleware.catch_errors.CatchErrorMiddleware
        self.assertIsInstance(app, expected)

        app = app.app
        expected = swift.common.middleware.gatekeeper.GatekeeperMiddleware
        self.assertIsInstance(app, expected)

        app = app.app
        expected = swift.common.middleware.listing_formats.ListingFilter
        self.assertIsInstance(app, expected)

        app = app.app
        expected = swift.common.middleware.copy.ServerSideCopyMiddleware
        self.assertIsInstance(app, expected)

        app = app.app
        expected = swift.common.middleware.dlo.DynamicLargeObject
        self.assertIsInstance(app, expected)

        app = app.app
        expected = \
            swift.common.middleware.versioned_writes.VersionedWritesMiddleware
        self.assertIsInstance(app, expected)

        app = app.app
        expected = swift.proxy.server.Application
        self.assertIsInstance(app, expected)
        # config settings applied to app instance
        self.assertEqual(0.2, app.conn_timeout)
        # appconfig returns values from 'proxy-server' section
        expected = {
            '__file__': conf_file,
            'here': os.path.dirname(conf_file),
            'conn_timeout': '0.2',
            'swift_dir': t,
            '__name__': 'proxy-server'
        }
        self.assertEqual(expected, conf)
        # logger works
        logger.info('testing')
        self.assertEqual('proxy-server', log_name)

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

    @with_tempdir
    def test_load_app_config(self, tempdir):
        conf_file = os.path.join(tempdir, 'file.conf')

        def _write_and_load_conf_file(conf):
            with open(conf_file, 'wt') as fd:
                fd.write(dedent(conf))
            return wsgi.load_app_config(conf_file)

        # typical case - DEFAULT options override same option in other sections
        conf_str = """
            [DEFAULT]
            dflt_option = dflt-value

            [pipeline:main]
            pipeline = proxy-logging proxy-server

            [filter:proxy-logging]
            use = egg:swift#proxy_logging

            [app:proxy-server]
            use = egg:swift#proxy
            proxy_option = proxy-value
            dflt_option = proxy-dflt-value
            """

        proxy_conf = _write_and_load_conf_file(conf_str)
        self.assertEqual('proxy-value', proxy_conf['proxy_option'])
        self.assertEqual('dflt-value', proxy_conf['dflt_option'])

        # 'set' overrides DEFAULT option
        conf_str = """
            [DEFAULT]
            dflt_option = dflt-value

            [pipeline:main]
            pipeline = proxy-logging proxy-server

            [filter:proxy-logging]
            use = egg:swift#proxy_logging

            [app:proxy-server]
            use = egg:swift#proxy
            proxy_option = proxy-value
            set dflt_option = proxy-dflt-value
            """

        proxy_conf = _write_and_load_conf_file(conf_str)
        self.assertEqual('proxy-value', proxy_conf['proxy_option'])
        self.assertEqual('proxy-dflt-value', proxy_conf['dflt_option'])

        # actual proxy server app name is dereferenced
        conf_str = """
            [pipeline:main]
            pipeline = proxy-logging proxyserverapp

            [filter:proxy-logging]
            use = egg:swift#proxy_logging

            [app:proxyserverapp]
            use = egg:swift#proxy
            proxy_option = proxy-value
            dflt_option = proxy-dflt-value
            """
        proxy_conf = _write_and_load_conf_file(conf_str)
        self.assertEqual('proxy-value', proxy_conf['proxy_option'])
        self.assertEqual('proxy-dflt-value', proxy_conf['dflt_option'])

        # no pipeline
        conf_str = """
            [filter:proxy-logging]
            use = egg:swift#proxy_logging

            [app:proxy-server]
            use = egg:swift#proxy
            proxy_option = proxy-value
            """
        proxy_conf = _write_and_load_conf_file(conf_str)
        self.assertEqual({}, proxy_conf)

        # no matching section
        conf_str = """
            [pipeline:main]
            pipeline = proxy-logging proxy-server

            [filter:proxy-logging]
            use = egg:swift#proxy_logging
            """
        proxy_conf = _write_and_load_conf_file(conf_str)
        self.assertEqual({}, proxy_conf)

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
        self.assertTrue(isinstance(app, expected))
        self.assertTrue(isinstance(app.app, swift.proxy.server.Application))
        # config settings applied to app instance
        self.assertEqual(0.2, app.app.conn_timeout)
        # appconfig returns values from 'proxy-server' section
        expected = {
            '__file__': conf_dir,
            'here': conf_dir,
            'conn_timeout': '0.2',
            'swift_dir': conf_root,
            '__name__': 'proxy-server'
        }
        self.assertEqual(expected, conf)
        # logger works
        logger.info('testing')
        self.assertEqual('proxy-server', log_name)

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
            self.assertTrue(isinstance(sock, MockSocket))
            expected_socket_opts = {
                socket.SOL_SOCKET: {
                    socket.SO_KEEPALIVE: 1,
                },
                socket.IPPROTO_TCP: {
                    socket.TCP_NODELAY: 1,
                }
            }
            if hasattr(socket, 'TCP_KEEPIDLE'):
                expected_socket_opts[socket.IPPROTO_TCP][
                    socket.TCP_KEEPIDLE] = 600
            self.assertEqual(sock.opts, expected_socket_opts)
            # test ssl
            sock = wsgi.get_socket(ssl_conf)
            expected_kwargs = {
                'certfile': '',
                'keyfile': '',
            }
            self.assertEqual(wsgi.ssl.wrap_socket_called, [expected_kwargs])

            # test keep_idle value
            keepIdle_value = 700
            conf['keep_idle'] = keepIdle_value
            sock = wsgi.get_socket(conf)
            # assert
            if hasattr(socket, 'TCP_KEEPIDLE'):
                expected_socket_opts[socket.IPPROTO_TCP][
                    socket.TCP_KEEPIDLE] = keepIdle_value
            self.assertEqual(sock.opts, expected_socket_opts)

            # test keep_idle for str -> int conversion
            keepIdle_value = '800'
            conf['keep_idle'] = keepIdle_value
            sock = wsgi.get_socket(conf)
            # assert
            if hasattr(socket, 'TCP_KEEPIDLE'):
                expected_socket_opts[socket.IPPROTO_TCP][
                    socket.TCP_KEEPIDLE] = int(keepIdle_value)
            self.assertEqual(sock.opts, expected_socket_opts)

            # test keep_idle for negative value
            conf['keep_idle'] = -600
            self.assertRaises(wsgi.ConfigFileError, wsgi.get_socket, conf)

            # test keep_idle for upperbound value
            conf['keep_idle'] = 2 ** 15
            self.assertRaises(wsgi.ConfigFileError, wsgi.get_socket, conf)

            # test keep_idle for Type mismatch
            conf['keep_idle'] = 'foobar'
            self.assertRaises(wsgi.ConfigFileError, wsgi.get_socket, conf)

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
                            'modify_wsgi_pipeline'), \
                    mock.patch('swift.common.wsgi.wsgi') as _wsgi, \
                    mock.patch('swift.common.wsgi.eventlet') as _wsgi_evt:
                conf = wsgi.appconfig(conf_file)
                logger = logging.getLogger('test')
                sock = listen_zero()
                wsgi.run_server(conf, logger, sock)
        self.assertEqual(30, _wsgi.WRITE_TIMEOUT)
        _wsgi_evt.hubs.use_hub.assert_called_with(utils.get_hub())
        _wsgi_evt.debug.hub_exceptions.assert_called_with(False)
        self.assertTrue(_wsgi.server.called)
        args, kwargs = _wsgi.server.call_args
        server_sock, server_app, server_logger = args
        self.assertEqual(sock, server_sock)
        self.assertTrue(isinstance(server_app, swift.proxy.server.Application))
        self.assertEqual(20, server_app.client_timeout)
        self.assertTrue(isinstance(server_logger, wsgi.NullLogger))
        self.assertTrue('custom_pool' in kwargs)
        self.assertEqual(1000, kwargs['custom_pool'].size)

        proto_class = kwargs['protocol']
        self.assertEqual(proto_class, wsgi.SwiftHttpProtocol)
        self.assertEqual('HTTP/1.0', proto_class.default_request_version)

    def test_run_server_proxied(self):
        config = """
        [DEFAULT]
        client_timeout = 30
        max_clients = 1000
        swift_dir = TEMPDIR

        [pipeline:main]
        pipeline = proxy-server

        [app:proxy-server]
        use = egg:swift#proxy
        # these "set" values override defaults
        set client_timeout = 20
        set max_clients = 10
        require_proxy_protocol = true
        """

        contents = dedent(config)
        with temptree(['proxy-server.conf']) as t:
            conf_file = os.path.join(t, 'proxy-server.conf')
            with open(conf_file, 'w') as f:
                f.write(contents.replace('TEMPDIR', t))
            _fake_rings(t)
            with mock.patch('swift.proxy.server.Application.'
                            'modify_wsgi_pipeline'), \
                    mock.patch('swift.common.wsgi.wsgi') as _wsgi, \
                    mock.patch('swift.common.wsgi.eventlet') as _eventlet:
                conf = wsgi.appconfig(conf_file,
                                      name='proxy-server')
                logger = logging.getLogger('test')
                sock = listen_zero()
                wsgi.run_server(conf, logger, sock)
        self.assertEqual(20, _wsgi.WRITE_TIMEOUT)
        _eventlet.hubs.use_hub.assert_called_with(utils.get_hub())
        _eventlet.debug.hub_exceptions.assert_called_with(False)
        self.assertTrue(_wsgi.server.called)
        args, kwargs = _wsgi.server.call_args
        server_sock, server_app, server_logger = args
        self.assertEqual(sock, server_sock)
        self.assertTrue(isinstance(server_app, swift.proxy.server.Application))
        self.assertEqual(20, server_app.client_timeout)
        self.assertTrue(isinstance(server_logger, wsgi.NullLogger))
        self.assertTrue('custom_pool' in kwargs)
        self.assertEqual(10, kwargs['custom_pool'].size)

        proto_class = kwargs['protocol']
        self.assertEqual(proto_class, wsgi.SwiftHttpProxiedProtocol)
        self.assertEqual('HTTP/1.0', proto_class.default_request_version)

    def test_run_server_with_latest_eventlet(self):
        config = """
        [DEFAULT]
        swift_dir = TEMPDIR

        [pipeline:main]
        pipeline = proxy-server

        [app:proxy-server]
        use = egg:swift#proxy
        """

        contents = dedent(config)
        with temptree(['proxy-server.conf']) as t:
            conf_file = os.path.join(t, 'proxy-server.conf')
            with open(conf_file, 'w') as f:
                f.write(contents.replace('TEMPDIR', t))
            _fake_rings(t)
            with mock.patch('swift.proxy.server.Application.'
                            'modify_wsgi_pipeline'), \
                    mock.patch('swift.common.wsgi.wsgi') as _wsgi, \
                    mock.patch('swift.common.wsgi.eventlet'):
                conf = wsgi.appconfig(conf_file)
                logger = logging.getLogger('test')
                sock = listen_zero()
                wsgi.run_server(conf, logger, sock)

        self.assertTrue(_wsgi.server.called)
        args, kwargs = _wsgi.server.call_args
        self.assertEqual(kwargs.get('capitalize_response_headers'), False)
        self.assertTrue('protocol' in kwargs)
        self.assertEqual('HTTP/1.0',
                         kwargs['protocol'].default_request_version)

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
                            'modify_wsgi_pipeline'), \
                    mock.patch('swift.common.wsgi.wsgi') as _wsgi, \
                    mock.patch('swift.common.wsgi.eventlet') as _wsgi_evt, \
                    mock.patch.dict('os.environ', {'TZ': ''}), \
                    mock.patch('time.tzset'):
                conf = wsgi.appconfig(conf_dir)
                logger = logging.getLogger('test')
                sock = listen_zero()
                wsgi.run_server(conf, logger, sock)
                self.assertTrue(os.environ['TZ'] is not '')

        self.assertEqual(30, _wsgi.WRITE_TIMEOUT)
        _wsgi_evt.hubs.use_hub.assert_called_with(utils.get_hub())
        _wsgi_evt.debug.hub_exceptions.assert_called_with(False)
        self.assertTrue(_wsgi.server.called)
        args, kwargs = _wsgi.server.call_args
        server_sock, server_app, server_logger = args
        self.assertEqual(sock, server_sock)
        self.assertTrue(isinstance(server_app, swift.proxy.server.Application))
        self.assertTrue(isinstance(server_logger, wsgi.NullLogger))
        self.assertTrue('custom_pool' in kwargs)
        self.assertTrue('protocol' in kwargs)
        self.assertEqual('HTTP/1.0',
                         kwargs['protocol'].default_request_version)

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
                            'modify_wsgi_pipeline'), \
                    mock.patch('swift.common.wsgi.wsgi') as _wsgi, \
                    mock.patch('swift.common.wsgi.eventlet') as _wsgi_evt:
                mock_server = _wsgi.server
                _wsgi.server = lambda *args, **kwargs: mock_server(
                    *args, **kwargs)
                conf = wsgi.appconfig(conf_file)
                logger = logging.getLogger('test')
                sock = listen_zero()
                wsgi.run_server(conf, logger, sock)
        self.assertEqual(30, _wsgi.WRITE_TIMEOUT)
        _wsgi_evt.hubs.use_hub.assert_called_with(utils.get_hub())
        _wsgi_evt.debug.hub_exceptions.assert_called_with(True)
        self.assertTrue(mock_server.called)
        args, kwargs = mock_server.call_args
        server_sock, server_app, server_logger = args
        self.assertEqual(sock, server_sock)
        self.assertTrue(isinstance(server_app, swift.proxy.server.Application))
        self.assertEqual(20, server_app.client_timeout)
        self.assertIsNone(server_logger)
        self.assertTrue('custom_pool' in kwargs)
        self.assertEqual(1000, kwargs['custom_pool'].size)
        self.assertTrue('protocol' in kwargs)
        self.assertEqual('HTTP/1.0',
                         kwargs['protocol'].default_request_version)

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
            'port': '8080', '__name__': 'main'
        }
        self.assertEqual(conf, expected)

    def test_pre_auth_wsgi_input(self):
        oldenv = {}
        newenv = wsgi.make_pre_authed_env(oldenv)
        self.assertTrue('wsgi.input' in newenv)
        self.assertEqual(newenv['wsgi.input'].read(), b'')

        oldenv = {'wsgi.input': BytesIO(b'original wsgi.input')}
        newenv = wsgi.make_pre_authed_env(oldenv)
        self.assertTrue('wsgi.input' in newenv)
        self.assertEqual(newenv['wsgi.input'].read(), b'')

        oldenv = {'swift.source': 'UT'}
        newenv = wsgi.make_pre_authed_env(oldenv)
        self.assertEqual(newenv['swift.source'], 'UT')

        oldenv = {'swift.source': 'UT'}
        newenv = wsgi.make_pre_authed_env(oldenv, swift_source='SA')
        self.assertEqual(newenv['swift.source'], 'SA')

    def test_pre_auth_req(self):
        class FakeReq(object):
            @classmethod
            def fake_blank(cls, path, environ=None, body=b'', headers=None):
                if environ is None:
                    environ = {}
                if headers is None:
                    headers = {}
                self.assertIsNone(environ['swift.authorize']('test'))
                self.assertFalse('HTTP_X_TRANS_ID' in environ)
        was_blank = Request.blank
        Request.blank = FakeReq.fake_blank
        wsgi.make_pre_authed_request({'HTTP_X_TRANS_ID': '1234'},
                                     'PUT', '/', body=b'tester', headers={})
        wsgi.make_pre_authed_request({'HTTP_X_TRANS_ID': '1234'},
                                     'PUT', '/', headers={})
        Request.blank = was_blank

    def test_pre_auth_req_with_quoted_path(self):
        r = wsgi.make_pre_authed_request(
            {'HTTP_X_TRANS_ID': '1234'}, 'PUT', path=quote('/a space'),
            body=b'tester', headers={})
        self.assertEqual(r.path, quote('/a space'))

    def test_pre_auth_req_drops_query(self):
        r = wsgi.make_pre_authed_request(
            {'QUERY_STRING': 'original'}, 'GET', 'path')
        self.assertEqual(r.query_string, 'original')
        r = wsgi.make_pre_authed_request(
            {'QUERY_STRING': 'original'}, 'GET', 'path?replacement')
        self.assertEqual(r.query_string, 'replacement')
        r = wsgi.make_pre_authed_request(
            {'QUERY_STRING': 'original'}, 'GET', 'path?')
        self.assertEqual(r.query_string, '')

    def test_pre_auth_req_with_body(self):
        r = wsgi.make_pre_authed_request(
            {'QUERY_STRING': 'original'}, 'GET', 'path', b'the body')
        self.assertEqual(r.body, b'the body')

    def test_pre_auth_creates_script_name(self):
        e = wsgi.make_pre_authed_env({})
        self.assertTrue('SCRIPT_NAME' in e)

    def test_pre_auth_copies_script_name(self):
        e = wsgi.make_pre_authed_env({'SCRIPT_NAME': '/script_name'})
        self.assertEqual(e['SCRIPT_NAME'], '/script_name')

    def test_pre_auth_copies_script_name_unless_path_overridden(self):
        e = wsgi.make_pre_authed_env({'SCRIPT_NAME': '/script_name'},
                                     path='/override')
        self.assertEqual(e['SCRIPT_NAME'], '')
        self.assertEqual(e['PATH_INFO'], '/override')

    def test_pre_auth_req_swift_source(self):
        r = wsgi.make_pre_authed_request(
            {'QUERY_STRING': 'original'}, 'GET', 'path', b'the body',
            swift_source='UT')
        self.assertEqual(r.body, b'the body')
        self.assertEqual(r.environ['swift.source'], 'UT')

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

        with mock.patch.object(wsgi, '_initrp', _initrp), \
                mock.patch.object(wsgi, 'get_socket'), \
                mock.patch.object(wsgi, 'drop_privileges'), \
                mock.patch.object(wsgi, 'loadapp', _loadapp), \
                mock.patch.object(wsgi, 'capture_stdio'), \
                mock.patch.object(wsgi, 'run_server'), \
                mock.patch('swift.common.utils.eventlet') as _utils_evt:
            wsgi.run_wsgi('conf_file', 'app_section',
                          global_conf_callback=_global_conf_callback)

        self.assertEqual(calls['_global_conf_callback'], 1)
        self.assertEqual(calls['_loadapp'], 1)
        _utils_evt.patcher.monkey_patch.assert_called_with(all=False,
                                                           socket=True,
                                                           select=True,
                                                           thread=True)

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

        with mock.patch.object(wsgi, '_initrp', _initrp), \
                mock.patch.object(wsgi, 'get_socket'), \
                mock.patch.object(wsgi, 'drop_privileges'), \
                mock.patch.object(wsgi, 'loadapp', _loadapp), \
                mock.patch.object(wsgi, 'capture_stdio'), \
                mock.patch.object(wsgi, 'run_server'), \
                mock.patch('swift.common.utils.eventlet') as _utils_evt:
            rc = wsgi.run_wsgi('conf_file', 'app_section')
        self.assertEqual(calls['_initrp'], 1)
        self.assertEqual(calls['_loadapp'], 1)
        self.assertEqual(rc, 0)
        _utils_evt.patcher.monkey_patch.assert_called_with(all=False,
                                                           socket=True,
                                                           select=True,
                                                           thread=True)

    @mock.patch('swift.common.wsgi.run_server')
    @mock.patch('swift.common.wsgi.WorkersStrategy')
    @mock.patch('swift.common.wsgi.ServersPerPortStrategy')
    def test_run_server_strategy_plumbing(self, mock_per_port, mock_workers,
                                          mock_run_server):
        # Make sure the right strategy gets used in a number of different
        # config cases.
        mock_per_port().do_bind_ports.return_value = 'stop early'
        mock_workers().do_bind_ports.return_value = 'stop early'
        logger = FakeLogger()
        stub__initrp = [
            {'__file__': 'test', 'workers': 2},  # conf
            logger,
            'log_name',
        ]
        with mock.patch.object(wsgi, '_initrp', return_value=stub__initrp), \
                mock.patch.object(wsgi, 'loadapp'), \
                mock.patch.object(wsgi, 'capture_stdio'):
            for server_type in ('account-server', 'container-server',
                                'object-server'):
                mock_per_port.reset_mock()
                mock_workers.reset_mock()
                logger._clear()
                self.assertEqual(1, wsgi.run_wsgi('conf_file', server_type))
                self.assertEqual([
                    'stop early',
                ], logger.get_lines_for_level('error'))
                self.assertEqual([], mock_per_port.mock_calls)
                self.assertEqual([
                    mock.call(stub__initrp[0], logger),
                    mock.call().do_bind_ports(),
                ], mock_workers.mock_calls)

            stub__initrp[0]['servers_per_port'] = 3
            for server_type in ('account-server', 'container-server'):
                mock_per_port.reset_mock()
                mock_workers.reset_mock()
                logger._clear()
                self.assertEqual(1, wsgi.run_wsgi('conf_file', server_type))
                self.assertEqual([
                    'stop early',
                ], logger.get_lines_for_level('error'))
                self.assertEqual([], mock_per_port.mock_calls)
                self.assertEqual([
                    mock.call(stub__initrp[0], logger),
                    mock.call().do_bind_ports(),
                ], mock_workers.mock_calls)

            mock_per_port.reset_mock()
            mock_workers.reset_mock()
            logger._clear()
            self.assertEqual(1, wsgi.run_wsgi('conf_file', 'object-server'))
            self.assertEqual([
                'stop early',
            ], logger.get_lines_for_level('error'))
            self.assertEqual([
                mock.call(stub__initrp[0], logger, servers_per_port=3),
                mock.call().do_bind_ports(),
            ], mock_per_port.mock_calls)
            self.assertEqual([], mock_workers.mock_calls)

    def test_run_server_failure1(self):
        calls = defaultdict(lambda: 0)

        def _initrp(conf_file, app_section, *args, **kwargs):
            calls['_initrp'] += 1
            raise wsgi.ConfigFileError('test exception')

        def _loadapp(uri, name=None, **kwargs):
            calls['_loadapp'] += 1

        with mock.patch.object(wsgi, '_initrp', _initrp), \
                mock.patch.object(wsgi, 'get_socket'), \
                mock.patch.object(wsgi, 'drop_privileges'), \
                mock.patch.object(wsgi, 'loadapp', _loadapp), \
                mock.patch.object(wsgi, 'capture_stdio'), \
                mock.patch.object(wsgi, 'run_server'):
            rc = wsgi.run_wsgi('conf_file', 'app_section')
        self.assertEqual(calls['_initrp'], 1)
        self.assertEqual(calls['_loadapp'], 0)
        self.assertEqual(rc, 1)

    def test_pre_auth_req_with_empty_env_no_path(self):
        r = wsgi.make_pre_authed_request(
            {}, 'GET')
        self.assertEqual(r.path, quote(''))
        self.assertTrue('SCRIPT_NAME' in r.environ)
        self.assertTrue('PATH_INFO' in r.environ)

    def test_pre_auth_req_with_env_path(self):
        r = wsgi.make_pre_authed_request(
            {'PATH_INFO': '/unquoted path with %20'}, 'GET')
        self.assertEqual(r.path, quote('/unquoted path with %20'))
        self.assertEqual(r.environ['SCRIPT_NAME'], '')

    def test_pre_auth_req_with_env_script(self):
        r = wsgi.make_pre_authed_request({'SCRIPT_NAME': '/hello'}, 'GET')
        self.assertEqual(r.path, quote('/hello'))

    def test_pre_auth_req_with_env_path_and_script(self):
        env = {'PATH_INFO': '/unquoted path with %20',
               'SCRIPT_NAME': '/script'}
        r = wsgi.make_pre_authed_request(env, 'GET')
        expected_path = quote(env['SCRIPT_NAME'] + env['PATH_INFO'])
        self.assertEqual(r.path, expected_path)
        env = {'PATH_INFO': '', 'SCRIPT_NAME': '/script'}
        r = wsgi.make_pre_authed_request(env, 'GET')
        self.assertEqual(r.path, '/script')
        env = {'PATH_INFO': '/path', 'SCRIPT_NAME': ''}
        r = wsgi.make_pre_authed_request(env, 'GET')
        self.assertEqual(r.path, '/path')
        env = {'PATH_INFO': '', 'SCRIPT_NAME': ''}
        r = wsgi.make_pre_authed_request(env, 'GET')
        self.assertEqual(r.path, '')

    def test_pre_auth_req_path_overrides_env(self):
        env = {'PATH_INFO': '/path', 'SCRIPT_NAME': '/script'}
        r = wsgi.make_pre_authed_request(env, 'GET', '/override')
        self.assertEqual(r.path, '/override')
        self.assertEqual(r.environ['SCRIPT_NAME'], '')
        self.assertEqual(r.environ['PATH_INFO'], '/override')

    def test_make_env_keep_user_project_id(self):
        oldenv = {'HTTP_X_USER_ID': '1234', 'HTTP_X_PROJECT_ID': '5678'}
        newenv = wsgi.make_env(oldenv)

        self.assertTrue('HTTP_X_USER_ID' in newenv)
        self.assertEqual(newenv['HTTP_X_USER_ID'], '1234')

        self.assertTrue('HTTP_X_PROJECT_ID' in newenv)
        self.assertEqual(newenv['HTTP_X_PROJECT_ID'], '5678')

    def test_make_env_keeps_referer(self):
        oldenv = {'HTTP_REFERER': 'http://blah.example.com'}
        newenv = wsgi.make_env(oldenv)

        self.assertTrue('HTTP_REFERER' in newenv)
        self.assertEqual(newenv['HTTP_REFERER'], 'http://blah.example.com')

    def test_make_env_keeps_infocache(self):
        oldenv = {'swift.infocache': {}}
        newenv = wsgi.make_env(oldenv)
        self.assertIs(newenv.get('swift.infocache'), oldenv['swift.infocache'])


class TestSwiftHttpProtocol(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch('swift.common.wsgi.wsgi.HttpProtocol')
        self.mock_super = patcher.start()
        self.addCleanup(patcher.stop)

    def _proto_obj(self):
        # Make an object we can exercise... note the base class's __init__()
        # does a bunch of work, so we just new up an object like eventlet.wsgi
        # does.
        proto_class = wsgi.SwiftHttpProtocol
        try:
            the_obj = types.InstanceType(proto_class)
        except AttributeError:
            the_obj = proto_class.__new__(proto_class)
        # Install some convenience mocks
        the_obj.server = Namespace(app=Namespace(logger=mock.Mock()),
                                   url_length_limit=777,
                                   log=mock.Mock())
        the_obj.send_error = mock.Mock()

        return the_obj

    def test_swift_http_protocol_log_request(self):
        proto_obj = self._proto_obj()
        self.assertEqual(None, proto_obj.log_request('ignored'))

    def test_swift_http_protocol_log_message(self):
        proto_obj = self._proto_obj()

        proto_obj.log_message('a%sc', 'b')
        self.assertEqual([mock.call.error('ERROR WSGI: a%sc', 'b')],
                         proto_obj.server.app.logger.mock_calls)

    def test_swift_http_protocol_log_message_no_logger(self):
        # If the app somehow had no logger attribute or it was None, don't blow
        # up
        proto_obj = self._proto_obj()
        delattr(proto_obj.server.app, 'logger')

        proto_obj.log_message('a%sc', 'b')
        self.assertEqual([mock.call.info('ERROR WSGI: a%sc', 'b')],
                         proto_obj.server.log.mock_calls)

        proto_obj.server.log.reset_mock()
        proto_obj.server.app.logger = None

        proto_obj.log_message('a%sc', 'b')
        self.assertEqual([mock.call.info('ERROR WSGI: a%sc', 'b')],
                         proto_obj.server.log.mock_calls)

    def test_swift_http_protocol_parse_request_no_proxy(self):
        proto_obj = self._proto_obj()
        proto_obj.raw_requestline = b'jimmy jam'
        proto_obj.client_address = ('a', '123')

        self.assertEqual(False, proto_obj.parse_request())

        self.assertEqual([], self.mock_super.mock_calls)
        self.assertEqual([
            mock.call(400, "Bad HTTP/0.9 request type ('jimmy')"),
        ], proto_obj.send_error.mock_calls)
        self.assertEqual(('a', '123'), proto_obj.client_address)


class TestProxyProtocol(unittest.TestCase):
    def _run_bytes_through_protocol(self, bytes_from_client, protocol_class):
        rfile = BytesIO(bytes_from_client)
        wfile = BytesIO()

        # All this fakery is needed to make the WSGI server process one
        # connection, possibly with multiple requests, in the main
        # greenthread. It doesn't hurt correctness if the function is called
        # in a separate greenthread, but it makes using the debugger harder.
        class FakeGreenthread(object):
            def link(self, a_callable, *args):
                a_callable(self, *args)

        class FakePool(object):
            def spawn(self, a_callable, *args, **kwargs):
                a_callable(*args, **kwargs)
                return FakeGreenthread()

            def spawn_n(self, a_callable, *args, **kwargs):
                a_callable(*args, **kwargs)

            def waitall(self):
                pass

        def dinky_app(env, start_response):
            start_response("200 OK", [])
            body = '\r\n'.join([
                'got addr: %s %s' % (
                    env.get("REMOTE_ADDR", "<missing>"),
                    env.get("REMOTE_PORT", "<missing>")),
                'on addr: %s %s' % (
                    env.get("SERVER_ADDR", "<missing>"),
                    env.get("SERVER_PORT", "<missing>")),
                'https is %s (scheme %s)' % (
                    env.get("HTTPS", "<missing>"),
                    env.get("wsgi.url_scheme", "<missing>")),
            ]) + '\r\n'
            return [body.encode("utf-8")]

        addr = ('127.0.0.1', 8359)
        fake_tcp_socket = mock.Mock(
            setsockopt=lambda *a: None,
            makefile=lambda mode, bufsize: rfile if 'r' in mode else wfile,
            getsockname=lambda *a: addr
        )
        fake_listen_socket = mock.Mock(
            accept=mock.MagicMock(
                side_effect=[[fake_tcp_socket, addr],
                             # KeyboardInterrupt breaks the WSGI server out of
                             # its infinite accept-process-close loop.
                             KeyboardInterrupt]),
            getsockname=lambda *a: addr)
        del fake_listen_socket.do_handshake

        # If we let the WSGI server close rfile/wfile then we can't access
        # their contents any more.
        with mock.patch.object(wfile, 'close', lambda: None), \
                mock.patch.object(rfile, 'close', lambda: None):
            eventlet.wsgi.server(
                fake_listen_socket, dinky_app,
                protocol=protocol_class,
                custom_pool=FakePool(),
                log_output=False,  # quiet the test run
            )
        return wfile.getvalue()

    def test_request_with_proxy(self):
        bytes_out = self._run_bytes_through_protocol((
            b"PROXY TCP4 192.168.0.1 192.168.0.11 56423 4433\r\n"
            b"GET /someurl HTTP/1.0\r\n"
            b"User-Agent: something or other\r\n"
            b"\r\n"
        ), wsgi.SwiftHttpProxiedProtocol)

        lines = [l for l in bytes_out.split(b"\r\n") if l]
        self.assertEqual(lines[0], b"HTTP/1.1 200 OK")  # sanity check
        self.assertEqual(lines[-3:], [
            b"got addr: 192.168.0.1 56423",
            b"on addr: 192.168.0.11 4433",
            b"https is <missing> (scheme http)",
        ])

    def test_request_with_proxy_https(self):
        bytes_out = self._run_bytes_through_protocol((
            b"PROXY TCP4 192.168.0.1 192.168.0.11 56423 443\r\n"
            b"GET /someurl HTTP/1.0\r\n"
            b"User-Agent: something or other\r\n"
            b"\r\n"
        ), wsgi.SwiftHttpProxiedProtocol)

        lines = [l for l in bytes_out.split(b"\r\n") if l]
        self.assertEqual(lines[0], b"HTTP/1.1 200 OK")  # sanity check
        self.assertEqual(lines[-3:], [
            b"got addr: 192.168.0.1 56423",
            b"on addr: 192.168.0.11 443",
            b"https is on (scheme https)",
        ])

    def test_multiple_requests_with_proxy(self):
        bytes_out = self._run_bytes_through_protocol((
            b"PROXY TCP4 192.168.0.1 192.168.0.11 56423 443\r\n"
            b"GET /someurl HTTP/1.1\r\n"
            b"User-Agent: something or other\r\n"
            b"\r\n"
            b"GET /otherurl HTTP/1.1\r\n"
            b"User-Agent: something or other\r\n"
            b"Connection: close\r\n"
            b"\r\n"
        ), wsgi.SwiftHttpProxiedProtocol)

        lines = bytes_out.split(b"\r\n")
        self.assertEqual(lines[0], b"HTTP/1.1 200 OK")  # sanity check

        # the address in the PROXY line is applied to every request
        addr_lines = [l for l in lines if l.startswith(b"got addr")]
        self.assertEqual(addr_lines, [b"got addr: 192.168.0.1 56423"] * 2)
        addr_lines = [l for l in lines if l.startswith(b"on addr")]
        self.assertEqual(addr_lines, [b"on addr: 192.168.0.11 443"] * 2)
        addr_lines = [l for l in lines if l.startswith(b"https is")]
        self.assertEqual(addr_lines, [b"https is on (scheme https)"] * 2)

    def test_missing_proxy_line(self):
        bytes_out = self._run_bytes_through_protocol((
            # whoops, no PROXY line here
            b"GET /someurl HTTP/1.0\r\n"
            b"User-Agent: something or other\r\n"
            b"\r\n"
        ), wsgi.SwiftHttpProxiedProtocol)

        lines = [l for l in bytes_out.split(b"\r\n") if l]
        self.assertIn(b"400 Invalid PROXY line", lines[0])

    def test_malformed_proxy_lines(self):
        for bad_line in [b'PROXY jojo',
                         b'PROXYjojo a b c d e',
                         b'PROXY a b c d e',  # bad INET protocol and family
                         ]:
            bytes_out = self._run_bytes_through_protocol(
                bad_line, wsgi.SwiftHttpProxiedProtocol)
            lines = [l for l in bytes_out.split(b"\r\n") if l]
            self.assertIn(b"400 Invalid PROXY line", lines[0])

    def test_unknown_client_addr(self):
        # For "UNKNOWN", the rest of the line before the CRLF may be omitted by
        # the sender, and the receiver must ignore anything presented before
        # the CRLF is found.
        for unknown_line in [b'PROXY UNKNOWN',  # mimimal valid unknown
                             b'PROXY UNKNOWNblahblah',  # also valid
                             b'PROXY UNKNOWN a b c d']:
            bytes_out = self._run_bytes_through_protocol((
                unknown_line + (b"\r\n"
                                b"GET /someurl HTTP/1.0\r\n"
                                b"User-Agent: something or other\r\n"
                                b"\r\n")
            ), wsgi.SwiftHttpProxiedProtocol)
            lines = [l for l in bytes_out.split(b"\r\n") if l]
            self.assertIn(b"200 OK", lines[0])

    def test_address_and_environ(self):
        # Make an object we can exercise... note the base class's __init__()
        # does a bunch of work, so we just new up an object like eventlet.wsgi
        # does.
        dummy_env = {'OTHER_ENV_KEY': 'OTHER_ENV_VALUE'}
        mock_protocol = mock.Mock(get_environ=lambda s: dummy_env)
        patcher = mock.patch(
            'swift.common.wsgi.SwiftHttpProtocol', mock_protocol
        )
        self.mock_super = patcher.start()
        self.addCleanup(patcher.stop)

        proto_class = wsgi.SwiftHttpProxiedProtocol
        try:
            proxy_obj = types.InstanceType(proto_class)
        except AttributeError:
            proxy_obj = proto_class.__new__(proto_class)

        # Install some convenience mocks
        proxy_obj.server = Namespace(app=Namespace(logger=mock.Mock()),
                                     url_length_limit=777,
                                     log=mock.Mock())
        proxy_obj.send_error = mock.Mock()

        proxy_obj.rfile = BytesIO(
            b'PROXY TCP4 111.111.111.111 222.222.222.222 111 222'
        )

        assert proxy_obj.handle()

        self.assertEqual(proxy_obj.client_address, ('111.111.111.111', '111'))
        self.assertEqual(proxy_obj.proxy_address, ('222.222.222.222', '222'))
        expected_env = {
            'SERVER_PORT': '222',
            'SERVER_ADDR': '222.222.222.222',
            'OTHER_ENV_KEY': 'OTHER_ENV_VALUE'
        }
        self.assertEqual(proxy_obj.get_environ(), expected_env)


class TestServersPerPortStrategy(unittest.TestCase):
    def setUp(self):
        self.logger = FakeLogger()
        self.conf = {
            'workers': 100,  # ignored
            'user': 'bob',
            'swift_dir': '/jim/cricket',
            'ring_check_interval': '76',
            'bind_ip': '2.3.4.5',
        }
        self.servers_per_port = 3
        self.s1, self.s2 = mock.MagicMock(), mock.MagicMock()
        patcher = mock.patch('swift.common.wsgi.get_socket',
                             side_effect=[self.s1, self.s2])
        self.mock_get_socket = patcher.start()
        self.addCleanup(patcher.stop)
        patcher = mock.patch('swift.common.wsgi.drop_privileges')
        self.mock_drop_privileges = patcher.start()
        self.addCleanup(patcher.stop)
        patcher = mock.patch('swift.common.wsgi.BindPortsCache')
        self.mock_cache_class = patcher.start()
        self.addCleanup(patcher.stop)
        patcher = mock.patch('swift.common.wsgi.os.setsid')
        self.mock_setsid = patcher.start()
        self.addCleanup(patcher.stop)
        patcher = mock.patch('swift.common.wsgi.os.chdir')
        self.mock_chdir = patcher.start()
        self.addCleanup(patcher.stop)
        patcher = mock.patch('swift.common.wsgi.os.umask')
        self.mock_umask = patcher.start()
        self.addCleanup(patcher.stop)

        self.all_bind_ports_for_node = \
            self.mock_cache_class().all_bind_ports_for_node
        self.ports = (6006, 6007)
        self.all_bind_ports_for_node.return_value = set(self.ports)

        self.strategy = wsgi.ServersPerPortStrategy(self.conf, self.logger,
                                                    self.servers_per_port)

    def test_loop_timeout(self):
        # This strategy should loop every ring_check_interval seconds, even if
        # no workers exit.
        self.assertEqual(76, self.strategy.loop_timeout())

        # Check the default
        del self.conf['ring_check_interval']
        self.strategy = wsgi.ServersPerPortStrategy(self.conf, self.logger,
                                                    self.servers_per_port)

        self.assertEqual(15, self.strategy.loop_timeout())

    def test_bind_ports(self):
        self.strategy.do_bind_ports()

        self.assertEqual(set((6006, 6007)), self.strategy.bind_ports)
        self.assertEqual([
            mock.call({'workers': 100,  # ignored
                       'user': 'bob',
                       'swift_dir': '/jim/cricket',
                       'ring_check_interval': '76',
                       'bind_ip': '2.3.4.5',
                       'bind_port': 6006}),
            mock.call({'workers': 100,  # ignored
                       'user': 'bob',
                       'swift_dir': '/jim/cricket',
                       'ring_check_interval': '76',
                       'bind_ip': '2.3.4.5',
                       'bind_port': 6007}),
        ], self.mock_get_socket.mock_calls)
        self.assertEqual(
            6006, self.strategy.port_pid_state.port_for_sock(self.s1))
        self.assertEqual(
            6007, self.strategy.port_pid_state.port_for_sock(self.s2))
        self.assertEqual([mock.call()], self.mock_setsid.mock_calls)
        self.assertEqual([mock.call('/')], self.mock_chdir.mock_calls)
        self.assertEqual([mock.call(0o22)], self.mock_umask.mock_calls)

    def test_bind_ports_ignores_setsid_errors(self):
        self.mock_setsid.side_effect = OSError()
        self.strategy.do_bind_ports()

        self.assertEqual(set((6006, 6007)), self.strategy.bind_ports)
        self.assertEqual([
            mock.call({'workers': 100,  # ignored
                       'user': 'bob',
                       'swift_dir': '/jim/cricket',
                       'ring_check_interval': '76',
                       'bind_ip': '2.3.4.5',
                       'bind_port': 6006}),
            mock.call({'workers': 100,  # ignored
                       'user': 'bob',
                       'swift_dir': '/jim/cricket',
                       'ring_check_interval': '76',
                       'bind_ip': '2.3.4.5',
                       'bind_port': 6007}),
        ], self.mock_get_socket.mock_calls)
        self.assertEqual(
            6006, self.strategy.port_pid_state.port_for_sock(self.s1))
        self.assertEqual(
            6007, self.strategy.port_pid_state.port_for_sock(self.s2))
        self.assertEqual([mock.call()], self.mock_setsid.mock_calls)
        self.assertEqual([mock.call('/')], self.mock_chdir.mock_calls)
        self.assertEqual([mock.call(0o22)], self.mock_umask.mock_calls)

    def test_no_fork_sock(self):
        self.assertIsNone(self.strategy.no_fork_sock())

    def test_new_worker_socks(self):
        self.strategy.do_bind_ports()
        self.all_bind_ports_for_node.reset_mock()

        pid = 88
        got_si = []
        for s, i in self.strategy.new_worker_socks():
            got_si.append((s, i))
            self.strategy.register_worker_start(s, i, pid)
            pid += 1

        self.assertEqual([
            (self.s1, 0), (self.s1, 1), (self.s1, 2),
            (self.s2, 0), (self.s2, 1), (self.s2, 2),
        ], got_si)
        self.assertEqual([
            'Started child %d (PID %d) for port %d' % (0, 88, 6006),
            'Started child %d (PID %d) for port %d' % (1, 89, 6006),
            'Started child %d (PID %d) for port %d' % (2, 90, 6006),
            'Started child %d (PID %d) for port %d' % (0, 91, 6007),
            'Started child %d (PID %d) for port %d' % (1, 92, 6007),
            'Started child %d (PID %d) for port %d' % (2, 93, 6007),
        ], self.logger.get_lines_for_level('notice'))
        self.logger._clear()

        # Steady-state...
        self.assertEqual([], list(self.strategy.new_worker_socks()))
        self.all_bind_ports_for_node.reset_mock()

        # Get rid of servers for ports which disappear from the ring
        self.ports = (6007,)
        self.all_bind_ports_for_node.return_value = set(self.ports)
        self.s1.reset_mock()
        self.s2.reset_mock()

        with mock.patch('swift.common.wsgi.greenio') as mock_greenio:
            self.assertEqual([], list(self.strategy.new_worker_socks()))

        self.assertEqual([
            mock.call(),  # ring_check_interval has passed...
        ], self.all_bind_ports_for_node.mock_calls)
        self.assertEqual([
            mock.call.shutdown_safe(self.s1),
        ], mock_greenio.mock_calls)
        self.assertEqual([
            mock.call.close(),
        ], self.s1.mock_calls)
        self.assertEqual([], self.s2.mock_calls)  # not closed
        self.assertEqual([
            'Closing unnecessary sock for port %d' % 6006,
        ], self.logger.get_lines_for_level('notice'))
        self.logger._clear()

        # Create new socket & workers for new ports that appear in ring
        self.ports = (6007, 6009)
        self.all_bind_ports_for_node.return_value = set(self.ports)
        self.s1.reset_mock()
        self.s2.reset_mock()
        s3 = mock.MagicMock()
        self.mock_get_socket.side_effect = Exception('ack')

        # But first make sure we handle failure to bind to the requested port!
        got_si = []
        for s, i in self.strategy.new_worker_socks():
            got_si.append((s, i))
            self.strategy.register_worker_start(s, i, pid)
            pid += 1

        self.assertEqual([], got_si)
        self.assertEqual([
            'Unable to bind to port %d: %s' % (6009, Exception('ack')),
            'Unable to bind to port %d: %s' % (6009, Exception('ack')),
            'Unable to bind to port %d: %s' % (6009, Exception('ack')),
        ], self.logger.get_lines_for_level('critical'))
        self.logger._clear()

        # Will keep trying, so let it succeed again
        self.mock_get_socket.side_effect = [s3]

        got_si = []
        for s, i in self.strategy.new_worker_socks():
            got_si.append((s, i))
            self.strategy.register_worker_start(s, i, pid)
            pid += 1

        self.assertEqual([
            (s3, 0), (s3, 1), (s3, 2),
        ], got_si)
        self.assertEqual([
            'Started child %d (PID %d) for port %d' % (0, 94, 6009),
            'Started child %d (PID %d) for port %d' % (1, 95, 6009),
            'Started child %d (PID %d) for port %d' % (2, 96, 6009),
        ], self.logger.get_lines_for_level('notice'))
        self.logger._clear()

        # Steady-state...
        self.assertEqual([], list(self.strategy.new_worker_socks()))
        self.all_bind_ports_for_node.reset_mock()

        # Restart a guy who died on us
        self.strategy.register_worker_exit(95)  # server_idx == 1

        got_si = []
        for s, i in self.strategy.new_worker_socks():
            got_si.append((s, i))
            self.strategy.register_worker_start(s, i, pid)
            pid += 1

        self.assertEqual([
            (s3, 1),
        ], got_si)
        self.assertEqual([
            'Started child %d (PID %d) for port %d' % (1, 97, 6009),
        ], self.logger.get_lines_for_level('notice'))
        self.logger._clear()

        # Check log_sock_exit
        self.strategy.log_sock_exit(self.s2, 2)
        self.assertEqual([
            'Child %d (PID %d, port %d) exiting normally' % (
                2, os.getpid(), 6007),
        ], self.logger.get_lines_for_level('notice'))

        # It's ok to register_worker_exit for a PID that's already had its
        # socket closed due to orphaning.
        # This is one of the workers for port 6006 that already got reaped.
        self.assertIsNone(self.strategy.register_worker_exit(89))

    def test_post_fork_hook(self):
        self.strategy.post_fork_hook()

        self.assertEqual([
            mock.call('bob', call_setsid=False),
        ], self.mock_drop_privileges.mock_calls)

    def test_shutdown_sockets(self):
        self.strategy.do_bind_ports()

        with mock.patch('swift.common.wsgi.greenio') as mock_greenio:
            self.strategy.shutdown_sockets()

        self.assertEqual([
            mock.call.shutdown_safe(self.s1),
            mock.call.shutdown_safe(self.s2),
        ], mock_greenio.mock_calls)
        self.assertEqual([
            mock.call.close(),
        ], self.s1.mock_calls)
        self.assertEqual([
            mock.call.close(),
        ], self.s2.mock_calls)


class TestWorkersStrategy(unittest.TestCase):
    def setUp(self):
        self.logger = FakeLogger()
        self.conf = {
            'workers': 2,
            'user': 'bob',
        }
        self.strategy = wsgi.WorkersStrategy(self.conf, self.logger)
        patcher = mock.patch('swift.common.wsgi.get_socket',
                             return_value='abc')
        self.mock_get_socket = patcher.start()
        self.addCleanup(patcher.stop)
        patcher = mock.patch('swift.common.wsgi.drop_privileges')
        self.mock_drop_privileges = patcher.start()
        self.addCleanup(patcher.stop)

    def test_loop_timeout(self):
        # This strategy should sit in the green.os.wait() for a bit (to avoid
        # busy-waiting) but not forever (so the keep-running flag actually
        # gets checked).
        self.assertEqual(0.5, self.strategy.loop_timeout())

    def test_binding(self):
        self.assertIsNone(self.strategy.do_bind_ports())

        self.assertEqual('abc', self.strategy.sock)
        self.assertEqual([
            mock.call(self.conf),
        ], self.mock_get_socket.mock_calls)
        self.assertEqual([
            mock.call('bob'),
        ], self.mock_drop_privileges.mock_calls)

        self.mock_get_socket.side_effect = wsgi.ConfigFilePortError()

        self.assertEqual(
            'bind_port wasn\'t properly set in the config file. '
            'It must be explicitly set to a valid port number.',
            self.strategy.do_bind_ports())

    def test_no_fork_sock(self):
        self.strategy.do_bind_ports()
        self.assertIsNone(self.strategy.no_fork_sock())

        self.conf['workers'] = 0
        self.strategy = wsgi.WorkersStrategy(self.conf, self.logger)
        self.strategy.do_bind_ports()

        self.assertEqual('abc', self.strategy.no_fork_sock())

    def test_new_worker_socks(self):
        self.strategy.do_bind_ports()
        pid = 88
        sock_count = 0
        for s, i in self.strategy.new_worker_socks():
            self.assertEqual('abc', s)
            self.assertIsNone(i)  # unused for this strategy
            self.strategy.register_worker_start(s, 'unused', pid)
            pid += 1
            sock_count += 1

        mypid = os.getpid()
        self.assertEqual([
            'Started child %s from parent %s' % (88, mypid),
            'Started child %s from parent %s' % (89, mypid),
        ], self.logger.get_lines_for_level('notice'))

        self.assertEqual(2, sock_count)
        self.assertEqual([], list(self.strategy.new_worker_socks()))

        sock_count = 0
        self.strategy.register_worker_exit(88)

        self.assertEqual([
            'Removing dead child %s from parent %s' % (88, mypid)
        ], self.logger.get_lines_for_level('error'))

        for s, i in self.strategy.new_worker_socks():
            self.assertEqual('abc', s)
            self.assertIsNone(i)  # unused for this strategy
            self.strategy.register_worker_start(s, 'unused', pid)
            pid += 1
            sock_count += 1

        self.assertEqual(1, sock_count)
        self.assertEqual([
            'Started child %s from parent %s' % (88, mypid),
            'Started child %s from parent %s' % (89, mypid),
            'Started child %s from parent %s' % (90, mypid),
        ], self.logger.get_lines_for_level('notice'))

    def test_post_fork_hook(self):
        # Just don't crash or do something stupid
        self.assertIsNone(self.strategy.post_fork_hook())

    def test_shutdown_sockets(self):
        self.mock_get_socket.return_value = mock.MagicMock()
        self.strategy.do_bind_ports()
        with mock.patch('swift.common.wsgi.greenio') as mock_greenio:
            self.strategy.shutdown_sockets()
        self.assertEqual([
            mock.call.shutdown_safe(self.mock_get_socket.return_value),
        ], mock_greenio.mock_calls)
        self.assertEqual([
            mock.call.close(),
        ], self.mock_get_socket.return_value.mock_calls)

    def test_log_sock_exit(self):
        self.strategy.log_sock_exit('blahblah', 'blahblah')
        my_pid = os.getpid()
        self.assertEqual([
            'Child %d exiting normally' % my_pid,
        ], self.logger.get_lines_for_level('notice'))


class TestWSGIContext(unittest.TestCase):

    def test_app_call(self):
        statuses = ['200 Ok', '404 Not Found']

        def app(env, start_response):
            start_response(statuses.pop(0), [('Content-Length', '3')])
            yield 'Ok\n'

        wc = wsgi.WSGIContext(app)
        r = Request.blank('/')
        it = wc._app_call(r.environ)
        self.assertEqual(wc._response_status, '200 Ok')
        self.assertEqual(''.join(it), 'Ok\n')
        r = Request.blank('/')
        it = wc._app_call(r.environ)
        self.assertEqual(wc._response_status, '404 Not Found')
        self.assertEqual(''.join(it), 'Ok\n')

    def test_app_iter_is_closable(self):

        def app(env, start_response):
            yield ''
            yield ''
            start_response('200 OK', [('Content-Length', '25')])
            yield 'aaaaa'
            yield 'bbbbb'
            yield 'ccccc'
            yield 'ddddd'
            yield 'eeeee'

        wc = wsgi.WSGIContext(app)
        r = Request.blank('/')
        iterable = wc._app_call(r.environ)
        self.assertEqual(wc._response_status, '200 OK')

        iterator = iter(iterable)
        self.assertEqual('aaaaa', next(iterator))
        self.assertEqual('bbbbb', next(iterator))
        iterable.close()
        with self.assertRaises(StopIteration):
            next(iterator)

    def test_update_content_length(self):
        statuses = ['200 Ok']

        def app(env, start_response):
            start_response(statuses.pop(0), [('Content-Length', '30')])
            yield 'Ok\n'

        wc = wsgi.WSGIContext(app)
        r = Request.blank('/')
        it = wc._app_call(r.environ)
        wc.update_content_length(35)
        self.assertEqual(wc._response_status, '200 Ok')
        self.assertEqual(''.join(it), 'Ok\n')
        self.assertEqual(wc._response_headers, [('Content-Length', '35')])


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
class TestPipelineModification(unittest.TestCase):
    def pipeline_modules(self, app):
        # This is rather brittle; it'll break if a middleware stores its app
        # anywhere other than an attribute named "app", but it works for now.
        pipe = []
        for _ in range(1000):
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
                          'swift.common.middleware.listing_formats',
                          'swift.common.middleware.copy',
                          'swift.common.middleware.dlo',
                          'swift.common.middleware.versioned_writes',
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
                          'swift.common.middleware.listing_formats',
                          'swift.common.middleware.copy',
                          'swift.common.middleware.dlo',
                          'swift.common.middleware.versioned_writes',
                          'swift.common.middleware.healthcheck',
                          'swift.proxy.server'])

    def test_proxy_modify_wsgi_pipeline_recommended_pipelines(self):
        to_test = [
            # Version, filter-only pipeline, expected final pipeline
            ('1.4.1',
             'catch_errors healthcheck cache ratelimit tempauth',
             'catch_errors gatekeeper healthcheck memcache'
             ' listing_formats ratelimit tempauth copy dlo versioned_writes'),
            ('1.5.0',
             'catch_errors healthcheck cache ratelimit tempauth proxy-logging',
             'catch_errors gatekeeper healthcheck memcache ratelimit tempauth'
             ' proxy_logging listing_formats copy dlo versioned_writes'),
            ('1.8.0',
             'catch_errors healthcheck proxy-logging cache slo ratelimit'
             ' tempauth container-quotas account-quotas proxy-logging',
             'catch_errors gatekeeper healthcheck proxy_logging memcache'
             ' listing_formats slo ratelimit tempauth copy dlo'
             ' versioned_writes container_quotas account_quotas'
             ' proxy_logging'),
            ('1.9.1',
             'catch_errors healthcheck proxy-logging cache bulk slo ratelimit'
             ' tempauth container-quotas account-quotas proxy-logging',
             'catch_errors gatekeeper healthcheck proxy_logging memcache'
             ' listing_formats bulk slo ratelimit tempauth copy dlo'
             ' versioned_writes container_quotas account_quotas'
             ' proxy_logging'),
            ('1.12.0',
             'catch_errors gatekeeper healthcheck proxy-logging cache'
             ' container_sync bulk slo ratelimit tempauth container-quotas'
             ' account-quotas proxy-logging',
             'catch_errors gatekeeper healthcheck proxy_logging memcache'
             ' listing_formats container_sync bulk slo ratelimit tempauth'
             ' copy dlo versioned_writes container_quotas account_quotas'
             ' proxy_logging'),
            ('1.13.0',
             'catch_errors gatekeeper healthcheck proxy-logging cache'
             ' container_sync bulk slo dlo ratelimit tempauth'
             ' container-quotas account-quotas proxy-logging',
             'catch_errors gatekeeper healthcheck proxy_logging memcache'
             ' listing_formats container_sync bulk slo dlo ratelimit'
             ' tempauth copy versioned_writes container_quotas account_quotas'
             ' proxy_logging'),
            ('1.13.1',
             'catch_errors gatekeeper healthcheck proxy-logging cache'
             ' container_sync bulk tempurl slo dlo ratelimit tempauth'
             ' container-quotas account-quotas proxy-logging',
             'catch_errors gatekeeper healthcheck proxy_logging memcache'
             ' listing_formats container_sync bulk tempurl slo dlo ratelimit'
             ' tempauth copy versioned_writes container_quotas account_quotas'
             ' proxy_logging'),
            ('2.0.0',
             'catch_errors gatekeeper healthcheck proxy-logging cache'
             ' container_sync bulk tempurl ratelimit tempauth container-quotas'
             ' account-quotas slo dlo proxy-logging',
             'catch_errors gatekeeper healthcheck proxy_logging memcache'
             ' listing_formats container_sync bulk tempurl ratelimit tempauth'
             ' copy container_quotas account_quotas slo dlo versioned_writes'
             ' proxy_logging'),
            ('2.4.0',
             'catch_errors gatekeeper healthcheck proxy-logging cache'
             ' container_sync bulk tempurl ratelimit tempauth container-quotas'
             ' account-quotas slo dlo versioned_writes proxy-logging',
             'catch_errors gatekeeper healthcheck proxy_logging memcache'
             ' listing_formats container_sync bulk tempurl ratelimit tempauth'
             ' copy container_quotas account_quotas slo dlo versioned_writes'
             ' proxy_logging'),
            ('2.8.0',
             'catch_errors gatekeeper healthcheck proxy-logging cache'
             ' container_sync bulk tempurl ratelimit tempauth copy'
             ' container-quotas account-quotas slo dlo versioned_writes'
             ' proxy-logging',
             'catch_errors gatekeeper healthcheck proxy_logging memcache'
             ' listing_formats container_sync bulk tempurl ratelimit tempauth'
             ' copy container_quotas account_quotas slo dlo versioned_writes'
             ' proxy_logging'),
            ('2.16.0',
             'catch_errors gatekeeper healthcheck proxy-logging cache'
             ' listing_formats container_sync bulk tempurl ratelimit'
             ' tempauth copy container-quotas account-quotas slo dlo'
             ' versioned_writes proxy-logging',
             'catch_errors gatekeeper healthcheck proxy_logging memcache'
             ' listing_formats container_sync bulk tempurl ratelimit'
             ' tempauth copy container_quotas account_quotas slo dlo'
             ' versioned_writes proxy_logging'),
        ]

        config = """
            [DEFAULT]
            swift_dir = %s

            [pipeline:main]
            pipeline = %s proxy-server

            [app:proxy-server]
            use = egg:swift#proxy
            conn_timeout = 0.2

            [filter:catch_errors]
            use = egg:swift#catch_errors

            [filter:gatekeeper]
            use = egg:swift#gatekeeper

            [filter:healthcheck]
            use = egg:swift#healthcheck

            [filter:proxy-logging]
            use = egg:swift#proxy_logging

            [filter:cache]
            use = egg:swift#memcache

            [filter:listing_formats]
            use = egg:swift#listing_formats

            [filter:container_sync]
            use = egg:swift#container_sync

            [filter:bulk]
            use = egg:swift#bulk

            [filter:tempurl]
            use = egg:swift#tempurl

            [filter:ratelimit]
            use = egg:swift#ratelimit

            [filter:tempauth]
            use = egg:swift#tempauth

            [filter:copy]
            use = egg:swift#copy

            [filter:container-quotas]
            use = egg:swift#container_quotas

            [filter:account-quotas]
            use = egg:swift#account_quotas

            [filter:slo]
            use = egg:swift#slo

            [filter:dlo]
            use = egg:swift#dlo

            [filter:versioned_writes]
            use = egg:swift#versioned_writes
        """
        contents = dedent(config)

        with temptree(['proxy-server.conf']) as t:
            _fake_rings(t)
            for version, pipeline, expected in to_test:
                conf_file = os.path.join(t, 'proxy-server.conf')
                with open(conf_file, 'w') as f:
                    f.write(contents % (t, pipeline))
                app = wsgi.loadapp(conf_file, global_conf={})

                actual = ' '.join(m.rsplit('.', 1)[1]
                                  for m in self.pipeline_modules(app)[:-1])
                self.assertEqual(
                    expected, actual,
                    'Pipeline mismatch for version %s: got\n    %s\n'
                    'but expected\n    %s' % (version, actual, expected))

    def test_proxy_modify_wsgi_pipeline_inserts_versioned_writes(self):
        config = """
        [DEFAULT]
        swift_dir = TEMPDIR

        [pipeline:main]
        pipeline = slo dlo healthcheck proxy-server

        [app:proxy-server]
        use = egg:swift#proxy
        conn_timeout = 0.2

        [filter:healthcheck]
        use = egg:swift#healthcheck

        [filter:dlo]
        use = egg:swift#dlo

        [filter:slo]
        use = egg:swift#slo
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
                          'swift.common.middleware.listing_formats',
                          'swift.common.middleware.copy',
                          'swift.common.middleware.slo',
                          'swift.common.middleware.dlo',
                          'swift.common.middleware.versioned_writes',
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
            'swift.common.middleware.listing_formats',
            'swift.common.middleware.copy',
            'swift.common.middleware.dlo',
            'swift.common.middleware.versioned_writes',
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
            'swift.common.middleware.listing_formats',
            'swift.common.middleware.copy',
            'swift.common.middleware.dlo',
            'swift.common.middleware.versioned_writes',
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
            'swift.common.middleware.listing_formats',
            'swift.common.middleware.copy',
            'swift.common.middleware.dlo',
            'swift.common.middleware.versioned_writes',
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
        proxy_app = app.app.app.app.app.app.app.app
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
        self.assertTrue(isinstance(app, FakeApp))
        app.foo = 'bar'
        self.assertEqual(filtered_app.foo, 'bar')

        # attribute is cached
        app.foo = 'baz'
        self.assertEqual(filtered_app.foo, 'bar')


if __name__ == '__main__':
    unittest.main()
