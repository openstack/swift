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

import configparser
import errno
import json
import logging
import signal
import socket
import struct
import unittest
import os
import eventlet

from collections import defaultdict
from io import BytesIO
from textwrap import dedent

from urllib.parse import quote

from unittest import mock

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
from test.debug_logger import debug_logger
from test.unit import (
    temptree, with_tempdir, write_fake_ring, patch_policies)

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
        fallocate_reserve = 1%

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
            'fallocate_reserve': '1%',
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
        [DEFAULT]
        CONN_timeout = 10
        client_timeout = 1
        [app:main]
        use = egg:swift#object
        conn_timeout = 5
        client_timeout = 2
        CLIENT_TIMEOUT = 3
        """
        contents = dedent(conf_body)
        with open(conf_path, 'w') as f:
            f.write(contents)
        app = wsgi.loadapp(conf_path)
        self.assertIsInstance(app, obj_server.ObjectController)
        # N.B. paste config loading from *file* is already case-sensitive,
        # so, CLIENT_TIMEOUT/client_timeout are unique options
        self.assertEqual(1, app.client_timeout)
        self.assertEqual(5, app.conn_timeout)

    @with_tempdir
    def test_loadapp_from_file_with_duplicate_var(self, tempdir):
        conf_path = os.path.join(tempdir, 'object-server.conf')
        conf_body = """
        [app:main]
        use = egg:swift#object
        client_timeout = 2
        client_timeout = 3
        """
        contents = dedent(conf_body)
        with open(conf_path, 'w') as f:
            f.write(contents)
        with self.assertRaises(
                configparser.DuplicateOptionError) as ctx:
            wsgi.loadapp(conf_path)
        msg = str(ctx.exception)
        self.assertIn('client_timeout', msg)
        self.assertIn('already exists', msg)

    @with_tempdir
    def test_loadapp_from_file_with_global_conf(self, tempdir):
        # verify that global_conf items override conf file DEFAULTS...
        conf_path = os.path.join(tempdir, 'object-server.conf')
        conf_body = """
        [DEFAULT]
        log_name = swift
        [app:main]
        use = egg:swift#object
        log_name = swift-main
        """
        contents = dedent(conf_body)
        with open(conf_path, 'w') as f:
            f.write(contents)
        app = wsgi.loadapp(conf_path)
        self.assertIsInstance(app, obj_server.ObjectController)
        self.assertEqual('swift', app.logger.server)

        app = wsgi.loadapp(conf_path, global_conf={'log_name': 'custom'})
        self.assertIsInstance(app, obj_server.ObjectController)
        self.assertEqual('custom', app.logger.server)

        # and regular section options...
        conf_path = os.path.join(tempdir, 'object-server.conf')
        conf_body = """
        [DEFAULT]
        [app:main]
        use = egg:swift#object
        log_name = swift-main
        """
        contents = dedent(conf_body)
        with open(conf_path, 'w') as f:
            f.write(contents)
        app = wsgi.loadapp(conf_path)
        self.assertIsInstance(app, obj_server.ObjectController)
        self.assertEqual('swift-main', app.logger.server)

        app = wsgi.loadapp(conf_path, global_conf={'log_name': 'custom'})
        self.assertIsInstance(app, obj_server.ObjectController)
        self.assertEqual('custom', app.logger.server)

        # ...but global_conf items do not override conf file 'set' options
        conf_body = """
        [DEFAULT]
        log_name = swift
        [app:main]
        use = egg:swift#object
        set log_name = swift-main
        """
        contents = dedent(conf_body)
        with open(conf_path, 'w') as f:
            f.write(contents)
        app = wsgi.loadapp(conf_path)
        self.assertIsInstance(app, obj_server.ObjectController)
        self.assertEqual('swift-main', app.logger.server)

        app = wsgi.loadapp(conf_path, global_conf={'log_name': 'custom'})
        self.assertIsInstance(app, obj_server.ObjectController)
        self.assertEqual('swift-main', app.logger.server)

    def test_loadapp_from_string(self):
        conf_body = """
        [DEFAULT]
        CONN_timeout = 10
        client_timeout = 1
        [app:main]
        use = egg:swift#object
        conn_timeout = 5
        client_timeout = 2
        """
        app = wsgi.loadapp(wsgi.ConfigString(conf_body))
        self.assertIsInstance(app, obj_server.ObjectController)
        self.assertEqual(1, app.client_timeout)
        self.assertEqual(5, app.conn_timeout)

    @with_tempdir
    def test_loadapp_from_dir(self, tempdir):
        conf_files = {
            'pipeline': """
            [pipeline:main]
            pipeline = tempauth proxy-server
            """,
            'tempauth': """
            [DEFAULT]
            swift_dir = %s
            random_VAR = foo
            [filter:tempauth]
            use = egg:swift#tempauth
            random_var = bar
            """ % tempdir,
            'proxy': """
            [DEFAULT]
            conn_timeout = 5
            client_timeout = 1
            [app:proxy-server]
            use = egg:swift#proxy
            CONN_timeout = 10
            client_timeout = 2
            """,
        }
        _fake_rings(tempdir)
        for filename, conf_body in conf_files.items():
            path = os.path.join(tempdir, filename + '.conf')
            with open(path, 'wt') as fd:
                fd.write(dedent(conf_body))
        app = wsgi.loadapp(tempdir)
        # DEFAULT takes priority (!?)
        self.assertEqual(5, app._pipeline_final_app.conn_timeout)
        self.assertEqual(1, app._pipeline_final_app.client_timeout)
        self.assertEqual('foo', app.app.app.app.conf['random_VAR'])
        self.assertEqual('bar', app.app.app.app.conf['random_var'])

    @with_tempdir
    def test_loadapp_from_dir_with_duplicate_var(self, tempdir):
        conf_files = {
            'pipeline': """
            [pipeline:main]
            pipeline = tempauth proxy-server
            """,
            'tempauth': """
            [DEFAULT]
            swift_dir = %s
            random_VAR = foo
            [filter:tempauth]
            use = egg:swift#tempauth
            random_var = bar
            """ % tempdir,
            'proxy': """
            [app:proxy-server]
            use = egg:swift#proxy
            client_timeout = 2
            CLIENT_TIMEOUT = 1
            conn_timeout = 3
            conn_timeout = 4
            """,
        }
        _fake_rings(tempdir)
        for filename, conf_body in conf_files.items():
            path = os.path.join(tempdir, filename + '.conf')
            with open(path, 'wt') as fd:
                fd.write(dedent(conf_body))
        with self.assertRaises(
                configparser.DuplicateOptionError) as ctx:
            wsgi.loadapp(tempdir)
        msg = str(ctx.exception)
        self.assertIn('conn_timeout', msg)
        self.assertIn('already exists', msg)

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
        self.assertIsInstance(app, expected)
        self.assertIsInstance(app.app, swift.proxy.server.Application)
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
            'cert_file': 'cert.pem',
            'key_file': 'private.key',
        })

        # mocks
        class MockSocket(object):
            def __init__(self):
                self.opts = defaultdict(dict)

            def setsockopt(self, level, optname, value):
                self.opts[level][optname] = value

        def mock_listen(*args, **kwargs):
            return MockSocket()

        class MockSslContext(object):
            _instance = None

            def __init__(self, *args, **kwargs):
                MockSslContext._instance = self
                self.load_cert_chain_args = []

            def wrap_socket(self, sock, *args, **kwargs):
                return sock

            def load_cert_chain(self, *args, **kwargs):
                self.load_cert_chain_args.extend(args)

        # patch
        old_listen = wsgi.listen
        old_ssl_context = wsgi.ssl.SSLContext
        try:
            wsgi.listen = mock_listen
            wsgi.ssl.SSLContext = MockSslContext
            # test
            sock = wsgi.get_socket(conf)
            # assert
            self.assertIsInstance(sock, MockSocket)
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
            expected_args = ['cert.pem', 'private.key']
            self.assertEqual(MockSslContext._instance.load_cert_chain_args,
                             expected_args)

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
            wsgi.ssl.SSLContext = old_ssl_context

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
        keepalive_timeout = 10
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
            with mock.patch('swift.common.wsgi.wsgi') as _wsgi, \
                    mock.patch('swift.common.wsgi.eventlet') as _wsgi_evt:
                conf = wsgi.appconfig(conf_file)
                logger = logging.getLogger('test')
                sock = listen_zero()
                wsgi.run_server(conf, logger, sock,
                                allow_modify_pipeline=False)
        _wsgi_evt.hubs.use_hub.assert_called_with(utils.get_hub())
        _wsgi_evt.debug.hub_exceptions.assert_called_with(False)
        self.assertTrue(_wsgi.server.called)
        args, kwargs = _wsgi.server.call_args
        server_sock, server_app, server_logger = args
        self.assertEqual(sock, server_sock)
        self.assertIsInstance(server_app, swift.proxy.server.Application)
        self.assertIsNone(server_app.watchdog._run_gth)
        self.assertEqual(20, server_app.client_timeout)
        self.assertIsInstance(server_logger, wsgi.NullLogger)
        self.assertTrue('custom_pool' in kwargs)
        self.assertEqual(1000, kwargs['custom_pool'].size)
        self.assertEqual(30, kwargs['socket_timeout'])
        self.assertEqual(10, kwargs['keepalive'])

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
        set client_timeout = 2.5
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
        _eventlet.hubs.use_hub.assert_called_with(utils.get_hub())
        _eventlet.debug.hub_exceptions.assert_called_with(False)
        self.assertTrue(_wsgi.server.called)
        args, kwargs = _wsgi.server.call_args
        server_sock, server_app, server_logger = args
        self.assertEqual(sock, server_sock)
        self.assertIsInstance(server_app, swift.proxy.server.Application)
        self.assertEqual(2.5, server_app.client_timeout)
        self.assertIsInstance(server_logger, wsgi.NullLogger)
        self.assertTrue('custom_pool' in kwargs)
        self.assertEqual(10, kwargs['custom_pool'].size)
        self.assertEqual(2.5, kwargs['socket_timeout'])
        self.assertNotIn('keepalive', kwargs)  # eventlet defaults to True

        proto_class = kwargs['protocol']
        self.assertEqual(proto_class, wsgi.SwiftHttpProxiedProtocol)
        self.assertEqual('HTTP/1.0', proto_class.default_request_version)

    def test_run_server_with_latest_eventlet(self):
        config = """
        [DEFAULT]
        swift_dir = TEMPDIR
        keepalive_timeout = 0

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
        self.assertIs(False, kwargs['keepalive'])

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
                self.assertNotEqual(os.environ['TZ'], '')

        _wsgi_evt.hubs.use_hub.assert_called_with(utils.get_hub())
        _wsgi_evt.debug.hub_exceptions.assert_called_with(False)
        self.assertTrue(_wsgi.server.called)
        args, kwargs = _wsgi.server.call_args
        server_sock, server_app, server_logger = args
        self.assertEqual(sock, server_sock)
        self.assertIsInstance(server_app, swift.proxy.server.Application)
        self.assertIsInstance(server_logger, wsgi.NullLogger)
        self.assertTrue('custom_pool' in kwargs)
        self.assertEqual(30, kwargs['socket_timeout'])
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
        _wsgi_evt.hubs.use_hub.assert_called_with(utils.get_hub())
        _wsgi_evt.debug.hub_exceptions.assert_called_with(True)
        self.assertTrue(mock_server.called)
        args, kwargs = mock_server.call_args
        server_sock, server_app, server_logger = args
        self.assertEqual(sock, server_sock)
        self.assertIsInstance(server_app, swift.proxy.server.Application)
        self.assertEqual(20, server_app.client_timeout)
        self.assertIsNone(server_logger)
        self.assertTrue('custom_pool' in kwargs)
        self.assertEqual(1000, kwargs['custom_pool'].size)
        self.assertEqual(30, kwargs['socket_timeout'])
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
                {'__file__': 'test', 'workers': 0, 'bind_port': 12345},
                'logger',
                'log_name')

        loadapp_conf = []
        to_inject = object()  # replication_timeout injects non-string data

        def _global_conf_callback(preloaded_app_conf, global_conf):
            calls['_global_conf_callback'] += 1
            self.assertEqual(
                preloaded_app_conf,
                {'__file__': 'test', 'workers': 0, 'bind_port': 12345})
            self.assertEqual(global_conf, {'log_name': 'log_name'})
            global_conf['test1'] = to_inject

        def _loadapp(uri, name=None, **kwargs):
            calls['_loadapp'] += 1
            self.assertIn('global_conf', kwargs)
            loadapp_conf.append(kwargs['global_conf'])
            # global_conf_callback hasn't been called yet
            self.assertNotIn('test1', kwargs['global_conf'])

        def _run_server(*args, **kwargs):
            # but by the time that we actually *run* the server, it has
            self.assertEqual(loadapp_conf,
                             [{'log_name': 'log_name', 'test1': to_inject}])

        with mock.patch.object(wsgi, '_initrp', _initrp), \
                mock.patch.object(wsgi, 'get_socket'), \
                mock.patch.object(wsgi, 'drop_privileges'), \
                mock.patch.object(wsgi, 'loadapp', _loadapp), \
                mock.patch.object(wsgi, 'capture_stdio'), \
                mock.patch.object(wsgi, 'run_server', _run_server), \
                mock.patch(
                    'swift.common.wsgi.systemd_notify') as mock_notify, \
                mock.patch('swift.common.utils.eventlet') as _utils_evt:
            wsgi.run_wsgi('conf_file', 'app_section',
                          global_conf_callback=_global_conf_callback)

        self.assertEqual(calls['_global_conf_callback'], 1)
        self.assertEqual(calls['_loadapp'], 1)
        _utils_evt.patcher.monkey_patch.assert_called_with(all=False,
                                                           socket=True,
                                                           select=True,
                                                           thread=True)
        self.assertEqual(mock_notify.mock_calls, [
            mock.call('logger', "STOPPING=1"),
        ])

    def test_run_server_success(self):
        calls = defaultdict(int)

        def _initrp(conf_file, app_section, *args, **kwargs):
            calls['_initrp'] += 1
            return (
                {'__file__': 'test', 'workers': 0, 'bind_port': 12345},
                'logger',
                'log_name')

        def _loadapp(uri, name=None, **kwargs):
            calls['_loadapp'] += 1

        logging.logThreads = 1  # reset to default
        with mock.patch.object(wsgi, '_initrp', _initrp), \
                mock.patch.object(wsgi, 'get_socket'), \
                mock.patch.object(wsgi, 'drop_privileges') as _d_privs, \
                mock.patch.object(wsgi, 'clean_up_daemon_hygiene') as _c_hyg, \
                mock.patch.object(wsgi, 'loadapp', _loadapp), \
                mock.patch.object(wsgi, 'capture_stdio'), \
                mock.patch.object(wsgi, 'run_server'), \
                mock.patch(
                    'swift.common.wsgi.systemd_notify') as mock_notify, \
                mock.patch('swift.common.utils.eventlet') as _utils_evt:
            rc = wsgi.run_wsgi('conf_file', 'app_section')
        self.assertEqual(calls['_initrp'], 1)
        self.assertEqual(calls['_loadapp'], 1)
        self.assertEqual(rc, 0)
        _utils_evt.patcher.monkey_patch.assert_called_with(all=False,
                                                           socket=True,
                                                           select=True,
                                                           thread=True)
        self.assertEqual(mock_notify.mock_calls, [
            mock.call('logger', "STOPPING=1"),
        ])
        # run_wsgi() no longer calls drop_privileges() in the parent process,
        # just clean_up_daemon_hygene()
        self.assertEqual([], _d_privs.mock_calls)
        self.assertEqual([mock.call()], _c_hyg.mock_calls)
        self.assertEqual(0, logging.logThreads)  # fixed in our monkey_patch

    def test_run_server_test_config(self):
        calls = defaultdict(int)

        def _initrp(conf_file, app_section, *args, **kwargs):
            calls['_initrp'] += 1
            return (
                {'__file__': 'test', 'workers': 0, 'bind_port': 12345},
                'logger',
                'log_name')

        def _loadapp(uri, name=None, **kwargs):
            calls['_loadapp'] += 1

        with mock.patch.object(wsgi, '_initrp', _initrp), \
                mock.patch.object(wsgi, 'get_socket') as _get_socket, \
                mock.patch.object(wsgi, 'drop_privileges') as _d_privs, \
                mock.patch.object(wsgi, 'clean_up_daemon_hygiene') as _c_hyg, \
                mock.patch.object(wsgi, 'loadapp', _loadapp), \
                mock.patch.object(wsgi, 'capture_stdio'), \
                mock.patch.object(wsgi, 'run_server'), \
                mock.patch('swift.common.utils.eventlet') as _utils_evt:
            rc = wsgi.run_wsgi('conf_file', 'app_section', test_config=True)
        self.assertEqual(calls['_initrp'], 1)
        self.assertEqual(calls['_loadapp'], 1)
        self.assertEqual(rc, 0)
        _utils_evt.patcher.monkey_patch.assert_called_with(all=False,
                                                           socket=True,
                                                           select=True,
                                                           thread=True)
        # run_wsgi() stops before calling clean_up_daemon_hygene() or
        # creating sockets
        self.assertEqual([], _d_privs.mock_calls)
        self.assertEqual([], _c_hyg.mock_calls)
        self.assertEqual([], _get_socket.mock_calls)

    @mock.patch('swift.common.wsgi.run_server')
    @mock.patch('swift.common.wsgi.WorkersStrategy')
    @mock.patch('swift.common.wsgi.ServersPerPortStrategy')
    def test_run_server_strategy_plumbing(self, mock_per_port, mock_workers,
                                          mock_run_server):
        # Make sure the right strategy gets used in a number of different
        # config cases.

        class StopAtCreatingSockets(Exception):
            '''Dummy exception to make sure we don't actually bind ports'''

        mock_per_port().no_fork_sock.return_value = None
        mock_per_port().new_worker_socks.side_effect = StopAtCreatingSockets
        mock_workers().no_fork_sock.return_value = None
        mock_workers().new_worker_socks.side_effect = StopAtCreatingSockets
        logger = debug_logger()
        stub__initrp = [
            {'__file__': 'test', 'workers': 2, 'bind_port': 12345},  # conf
            logger,
            'log_name',
        ]
        with mock.patch.object(wsgi, '_initrp', return_value=stub__initrp), \
                mock.patch.object(wsgi, 'loadapp'), \
                mock.patch('swift.common.utils.monkey_patch'), \
                mock.patch.object(wsgi, 'capture_stdio'):
            for server_type in ('account-server', 'container-server',
                                'object-server'):
                mock_per_port.reset_mock()
                mock_workers.reset_mock()
                logger._clear()
                with self.assertRaises(StopAtCreatingSockets):
                    wsgi.run_wsgi('conf_file', server_type)
                self.assertEqual([], mock_per_port.mock_calls)
                self.assertEqual([
                    mock.call(stub__initrp[0], logger),
                    mock.call().no_fork_sock(),
                    mock.call().new_worker_socks(),
                ], mock_workers.mock_calls)

            stub__initrp[0]['servers_per_port'] = 3
            for server_type in ('account-server', 'container-server'):
                mock_per_port.reset_mock()
                mock_workers.reset_mock()
                logger._clear()
                with self.assertRaises(StopAtCreatingSockets):
                    wsgi.run_wsgi('conf_file', server_type)
                self.assertEqual([], mock_per_port.mock_calls)
                self.assertEqual([
                    mock.call(stub__initrp[0], logger),
                    mock.call().no_fork_sock(),
                    mock.call().new_worker_socks(),
                ], mock_workers.mock_calls)

            mock_per_port.reset_mock()
            mock_workers.reset_mock()
            logger._clear()
            with self.assertRaises(StopAtCreatingSockets):
                wsgi.run_wsgi('conf_file', 'object-server')
            self.assertEqual([
                mock.call(stub__initrp[0], logger, servers_per_port=3),
                mock.call().no_fork_sock(),
                mock.call().new_worker_socks(),
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

    def test_run_server_bad_bind_port(self):
        def do_test(port):
            calls = defaultdict(lambda: 0)
            logger = debug_logger()

            def _initrp(conf_file, app_section, *args, **kwargs):
                calls['_initrp'] += 1
                return (
                    {'__file__': 'test', 'workers': 0, 'bind_port': port},
                    logger,
                    'log_name')

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
            self.assertEqual(
                ["bind_port wasn't properly set in the config file. "
                 "It must be explicitly set to a valid port number."],
                logger.get_lines_for_level('error')
            )

        do_test('bad')
        do_test('80000')

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


class CommonTestMixin(object):

    @mock.patch('swift.common.wsgi.capture_stdio')
    def test_post_fork_hook(self, mock_capture):
        self.strategy.post_fork_hook()

        self.assertEqual([
            mock.call('bob'),
        ], self.mock_drop_privileges.mock_calls)
        self.assertEqual([
            mock.call(self.logger),
        ], mock_capture.mock_calls)

    def test_stale_pid_loading(self):
        class FakeTime(object):
            def __init__(self, step=10):
                self.patchers = [
                    mock.patch('swift.common.wsgi.time.time',
                               side_effect=self.time),
                    mock.patch('swift.common.wsgi.sleep',
                               side_effect=self.sleep),
                ]
                self.now = 0
                self.step = step
                self.sleeps = []

            def time(self):
                self.now += self.step
                return self.now

            def sleep(self, delta):
                if delta < 0:
                    raise ValueError('cannot sleep negative time: %s' % delta)
                self.now += delta
                self.sleeps.append(delta)

            def __enter__(self):
                for patcher in self.patchers:
                    patcher.start()
                return self

            def __exit__(self, *a):
                for patcher in self.patchers:
                    patcher.stop()

        notify_rfd, notify_wfd = os.pipe()
        state_rfd, state_wfd = os.pipe()
        stale_process_data = {
            "old_pids": {123: 5, 456: 6, 78: 27, 90: 28},
        }
        to_write = json.dumps(stale_process_data).encode('ascii')
        os.write(state_wfd, struct.pack('!I', len(to_write)) + to_write)
        os.close(state_wfd)
        self.assertEqual(self.strategy.reload_pids, {})
        os.environ['__SWIFT_SERVER_NOTIFY_FD'] = str(notify_wfd)
        os.environ['__SWIFT_SERVER_CHILD_STATE_FD'] = str(state_rfd)
        with mock.patch('swift.common.wsgi.capture_stdio'), \
                mock.patch('swift.common.utils.get_ppid') as mock_ppid, \
                mock.patch('os.kill') as mock_kill, FakeTime() as fake_time:
            mock_ppid.side_effect = [
                os.getpid(),
                OSError(errno.ENOENT, "Not there"),
                OSError(errno.EPERM, "Not for you"),
                os.getpid(),
            ]
            self.strategy.signal_ready()
            self.assertEqual(self.strategy.reload_pids,
                             stale_process_data['old_pids'])

            # We spawned our child-killer, but it hasn't been scheduled yet
            self.assertEqual(mock_ppid.mock_calls, [])
            self.assertEqual(mock_kill.mock_calls, [])
            self.assertEqual(fake_time.sleeps, [])

            # *Now* we let it run (with mocks still enabled)
            eventlet.sleep()

        self.assertEqual(str(os.getpid()).encode('ascii'),
                         os.read(notify_rfd, 30))
        os.close(notify_rfd)

        self.assertEqual(mock_kill.mock_calls, [
            mock.call(123, signal.SIGKILL),
            mock.call(90, signal.SIGKILL)])
        self.assertEqual(fake_time.sleeps, [86395, 2])


class TestServersPerPortStrategy(unittest.TestCase, CommonTestMixin):
    def setUp(self):
        self.logger = debug_logger()
        self.conf = {
            'workers': 100,  # ignored
            'user': 'bob',
            'swift_dir': '/jim/cricket',
            'ring_check_interval': '76',
            'bind_ip': '2.3.4.5',
        }
        self.servers_per_port = 3
        self.sockets = [mock.MagicMock() for _ in range(6)]
        patcher = mock.patch('swift.common.wsgi.get_socket',
                             side_effect=self.sockets)
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

    def test_no_fork_sock(self):
        self.assertIsNone(self.strategy.no_fork_sock())

    def test_new_worker_socks(self):
        self.all_bind_ports_for_node.reset_mock()

        pid = 88
        got_si = []
        for s, i in self.strategy.new_worker_socks():
            got_si.append((s, i))
            self.strategy.register_worker_start(s, i, pid)
            pid += 1

        self.assertEqual([
            (self.sockets[0], (6006, 0)),
            (self.sockets[1], (6006, 1)),
            (self.sockets[2], (6006, 2)),
            (self.sockets[3], (6007, 0)),
            (self.sockets[4], (6007, 1)),
            (self.sockets[5], (6007, 2)),
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
        for s in self.sockets:
            s.reset_mock()

        with mock.patch('swift.common.wsgi.greenio') as mock_greenio:
            self.assertEqual([], list(self.strategy.new_worker_socks()))

        self.assertEqual([
            mock.call(),  # ring_check_interval has passed...
        ], self.all_bind_ports_for_node.mock_calls)
        self.assertEqual([
            [mock.call.close()]
            for _ in range(3)
        ], [s.mock_calls for s in self.sockets[:3]])
        self.assertEqual({
            ('shutdown_safe', (self.sockets[0],)),
            ('shutdown_safe', (self.sockets[1],)),
            ('shutdown_safe', (self.sockets[2],)),
        }, {call[:2] for call in mock_greenio.mock_calls})
        self.assertEqual([
            [] for _ in range(3)
        ], [s.mock_calls for s in self.sockets[3:]])  # not closed
        self.assertEqual({
            'Closing unnecessary sock for port %d (child pid %d)' % (6006, p)
            for p in range(88, 91)
        }, set(self.logger.get_lines_for_level('notice')))
        self.logger._clear()

        # Create new socket & workers for new ports that appear in ring
        self.ports = (6007, 6009)
        self.all_bind_ports_for_node.return_value = set(self.ports)
        for s in self.sockets:
            s.reset_mock()
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
        new_sockets = self.mock_get_socket.side_effect = [
            mock.MagicMock() for _ in range(3)]

        got_si = []
        for s, i in self.strategy.new_worker_socks():
            got_si.append((s, i))
            self.strategy.register_worker_start(s, i, pid)
            pid += 1

        self.assertEqual([
            (s, (6009, i)) for i, s in enumerate(new_sockets)
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

        # TODO: check that the socket got cleaned up

        new_socket = mock.MagicMock()
        self.mock_get_socket.side_effect = [new_socket]

        got_si = []
        for s, i in self.strategy.new_worker_socks():
            got_si.append((s, i))
            self.strategy.register_worker_start(s, i, pid)
            pid += 1

        self.assertEqual([
            (new_socket, (6009, 1)),
        ], got_si)
        self.assertEqual([
            'Started child %d (PID %d) for port %d' % (1, 97, 6009),
        ], self.logger.get_lines_for_level('notice'))
        self.logger._clear()

        # Check log_sock_exit
        self.strategy.log_sock_exit(self.sockets[5], (6007, 2))
        self.assertEqual([
            'Child %d (PID %d, port %d) exiting normally' % (
                2, os.getpid(), 6007),
        ], self.logger.get_lines_for_level('notice'))

        # It's ok to register_worker_exit for a PID that's already had its
        # socket closed due to orphaning.
        # This is one of the workers for port 6006 that already got reaped.
        self.assertIsNone(self.strategy.register_worker_exit(89))

    def test_servers_per_port_in_container(self):
        # normally there's no configured ring_ip
        conf = {
            'bind_ip': '1.2.3.4',
        }
        self.strategy = wsgi.ServersPerPortStrategy(conf, self.logger, 1)
        self.assertEqual(self.mock_cache_class.call_args,
                         mock.call('/etc/swift', '1.2.3.4'))
        self.assertEqual({6006, 6007},
                         self.strategy.cache.all_bind_ports_for_node())
        ports = {item[1][0] for item in self.strategy.new_worker_socks()}
        self.assertEqual({6006, 6007}, ports)

        # but in a container we can override it
        conf = {
            'bind_ip': '1.2.3.4',
            'ring_ip': '2.3.4.5'
        }
        self.strategy = wsgi.ServersPerPortStrategy(conf, self.logger, 1)
        # N.B. our fake BindPortsCache always returns {6006, 6007}, but a real
        # BindPortsCache would only return ports for devices that match the ip
        # address in the ring
        self.assertEqual(self.mock_cache_class.call_args,
                         mock.call('/etc/swift', '2.3.4.5'))
        self.assertEqual({6006, 6007},
                         self.strategy.cache.all_bind_ports_for_node())
        ports = {item[1][0] for item in self.strategy.new_worker_socks()}
        self.assertEqual({6006, 6007}, ports)

    def test_shutdown_sockets(self):
        pid = 88
        for s, i in self.strategy.new_worker_socks():
            self.strategy.register_worker_start(s, i, pid)
            pid += 1

        with mock.patch('swift.common.wsgi.greenio') as mock_greenio:
            self.strategy.shutdown_sockets()

        self.assertEqual([
            mock.call.shutdown_safe(s)
            for s in self.sockets
        ], mock_greenio.mock_calls)
        self.assertEqual([
            [mock.call.close()]
            for _ in range(3)
        ], [s.mock_calls for s in self.sockets[:3]])


class TestWorkersStrategy(unittest.TestCase, CommonTestMixin):
    def setUp(self):
        self.logger = debug_logger()
        self.conf = {
            'workers': 2,
            'user': 'bob',
        }
        self.strategy = wsgi.WorkersStrategy(self.conf, self.logger)
        self.mock_socket = mock.Mock()
        patcher = mock.patch('swift.common.wsgi.get_socket',
                             return_value=self.mock_socket)
        self.mock_get_socket = patcher.start()
        self.addCleanup(patcher.stop)
        patcher = mock.patch('swift.common.wsgi.drop_privileges')
        self.mock_drop_privileges = patcher.start()
        self.addCleanup(patcher.stop)
        patcher = mock.patch('swift.common.wsgi.clean_up_daemon_hygiene')
        self.mock_clean_up_daemon_hygene = patcher.start()
        self.addCleanup(patcher.stop)

    def test_loop_timeout(self):
        # This strategy should sit in the green.os.wait() for a bit (to avoid
        # busy-waiting) but not forever (so the keep-running flag actually
        # gets checked).
        self.assertEqual(0.5, self.strategy.loop_timeout())

    def test_no_fork_sock(self):
        self.assertIsNone(self.strategy.no_fork_sock())

        self.conf['workers'] = 0
        self.strategy = wsgi.WorkersStrategy(self.conf, self.logger)

        self.assertIs(self.mock_socket, self.strategy.no_fork_sock())

    def test_new_worker_socks(self):
        pid = 88
        sock_count = 0
        for s, i in self.strategy.new_worker_socks():
            self.assertEqual(self.mock_socket, s)
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
            self.assertEqual(self.mock_socket, s)
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

    def test_shutdown_sockets(self):
        self.mock_get_socket.side_effect = sockets = [
            mock.MagicMock(), mock.MagicMock()]

        pid = 88
        for s, i in self.strategy.new_worker_socks():
            self.strategy.register_worker_start(s, 'unused', pid)
            pid += 1

        with mock.patch('swift.common.wsgi.greenio') as mock_greenio:
            self.strategy.shutdown_sockets()
        self.assertEqual([
            mock.call.shutdown_safe(s)
            for s in sockets
        ], mock_greenio.mock_calls)
        self.assertEqual([
            [mock.call.close()] for _ in range(2)
        ], [s.mock_calls for s in sockets])

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
            yield b'Ok\n'

        wc = wsgi.WSGIContext(app)
        r = Request.blank('/')
        it = wc._app_call(r.environ)
        self.assertEqual(wc._response_status, '200 Ok')
        self.assertEqual(wc._get_status_int(), 200)
        self.assertEqual(b''.join(it), b'Ok\n')
        r = Request.blank('/')
        it = wc._app_call(r.environ)
        self.assertEqual(wc._response_status, '404 Not Found')
        self.assertEqual(wc._get_status_int(), 404)
        self.assertEqual(b''.join(it), b'Ok\n')

    def test_app_iter_is_closable(self):

        def app(env, start_response):
            yield b''
            yield b''
            start_response('200 OK', [('Content-Length', '25')])
            yield b'aaaaa'
            yield b'bbbbb'
            yield b'ccccc'
            yield b'ddddd'
            yield b'eeeee'

        wc = wsgi.WSGIContext(app)
        r = Request.blank('/')
        iterable = wc._app_call(r.environ)
        self.assertEqual(wc._response_status, '200 OK')
        self.assertEqual(wc._get_status_int(), 200)

        iterator = iter(iterable)
        self.assertEqual(b'aaaaa', next(iterator))
        self.assertEqual(b'bbbbb', next(iterator))
        iterable.close()
        with self.assertRaises(StopIteration):
            next(iterator)

    def test_update_content_length(self):
        statuses = ['200 Ok']

        def app(env, start_response):
            start_response(statuses.pop(0), [('Content-Length', '30')])
            yield b'Ok\n'

        wc = wsgi.WSGIContext(app)
        r = Request.blank('/')
        it = wc._app_call(r.environ)
        wc.update_content_length(35)
        self.assertEqual(wc._response_status, '200 Ok')
        self.assertEqual(wc._get_status_int(), 200)
        self.assertEqual(b''.join(it), b'Ok\n')
        self.assertEqual(wc._response_headers, [('Content-Length', '35')])

    def test_app_returns_headers_as_dict_items(self):
        statuses = ['200 Ok']

        def app(env, start_response):
            start_response(statuses.pop(0), {'Content-Length': '3'}.items())
            yield b'Ok\n'

        wc = wsgi.WSGIContext(app)
        r = Request.blank('/')
        it = wc._app_call(r.environ)
        wc._response_headers.append(('X-Trans-Id', 'txn'))
        self.assertEqual(wc._response_status, '200 Ok')
        self.assertEqual(wc._get_status_int(), 200)
        self.assertEqual(b''.join(it), b'Ok\n')
        self.assertEqual(wc._response_headers, [
            ('Content-Length', '3'),
            ('X-Trans-Id', 'txn'),
        ])


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
            if app.__class__.__module__ == \
                    'swift.common.middleware.versioned_writes.legacy':
                pipe.append('swift.common.middleware.versioned_writes')
            else:
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
            self.assertIsInstance(app, exp)
            exp = swift.common.middleware.healthcheck.HealthCheckMiddleware
            self.assertIsInstance(app.app, exp)
            exp = swift.proxy.server.Application
            self.assertIsInstance(app.app.app, exp)
            # Everybody gets a reference to the final app, too
            self.assertIs(app.app.app, app._pipeline_final_app)
            self.assertIs(app.app.app, app._pipeline_request_logging_app)
            self.assertIs(app.app.app, app.app._pipeline_final_app)
            self.assertIs(app.app.app, app.app._pipeline_request_logging_app)
            self.assertIs(app.app.app, app.app.app._pipeline_final_app)
            exp_pipeline = [app, app.app, app.app.app]
            self.assertEqual(exp_pipeline, app._pipeline)
            self.assertEqual(exp_pipeline, app.app._pipeline)
            self.assertEqual(exp_pipeline, app.app.app._pipeline)
            self.assertIs(app._pipeline, app.app._pipeline)
            self.assertIs(app._pipeline, app.app.app._pipeline)

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
            self.assertIsInstance(app, exp)
            exp = swift.proxy.server.Application
            self.assertIsInstance(app.app, exp)

    def test_load_app_request_logging_app(self):
        config = """
        [DEFAULT]
        swift_dir = TEMPDIR

        [pipeline:main]
        pipeline = catch_errors proxy_logging proxy-server

        [app:proxy-server]
        use = egg:swift#proxy
        conn_timeout = 0.2

        [filter:catch_errors]
        use = egg:swift#catch_errors

        [filter:proxy_logging]
        use = egg:swift#proxy_logging
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
                              'swift.common.middleware.proxy_logging',
                              'swift.common.middleware.listing_formats',
                              'swift.common.middleware.copy',
                              'swift.common.middleware.dlo',
                              'swift.common.middleware.versioned_writes',
                              'swift.proxy.server'])

            pipeline = app._pipeline
            logging_app = app._pipeline_request_logging_app
            final_app = app._pipeline_final_app
            # Sanity check -- loadapp returns the start of the pipeline
            self.assertIs(app, pipeline[0])
            # ... and the final_app is the end
            self.assertIs(final_app, pipeline[-1])

            # The logging app is its own special short pipeline
            self.assertEqual(self.pipeline_modules(logging_app), [
                'swift.common.middleware.proxy_logging',
                'swift.proxy.server'])
            self.assertNotIn(logging_app, pipeline)
            self.assertIs(logging_app.app, final_app)

            # All the apps in the main pipeline got decorated identically
            for app in pipeline:
                self.assertIs(app._pipeline, pipeline)
                self.assertIs(app._pipeline_request_logging_app, logging_app)
                self.assertIs(app._pipeline_final_app, final_app)

            # Special logging app got them, too
            self.assertIs(logging_app._pipeline_request_logging_app,
                          logging_app)
            self.assertIs(logging_app._pipeline_final_app, final_app)
            # Though the pipeline's different -- may or may not matter?
            self.assertEqual(logging_app._pipeline, [logging_app, final_app])

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
            user_test_tester = t%%sting .admin

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
                    to_write = contents % (t, pipeline)
                    # Sanity check that the password only has one % in it
                    self.assertIn('t%sting', to_write)
                    f.write(to_write)
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
        proxy_app = app._pipeline_final_app
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
            self.assertIsInstance(app, controller)


if __name__ == '__main__':
    unittest.main()
