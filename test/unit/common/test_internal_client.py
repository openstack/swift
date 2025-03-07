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

import json
from unittest import mock
import unittest
import zlib
import os

from io import BytesIO
from textwrap import dedent

from itertools import zip_longest
from urllib.parse import quote, parse_qsl
from swift.common import exceptions, internal_client, request_helpers, swob
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.storage_policy import StoragePolicy
from swift.common.middleware.proxy_logging import ProxyLoggingMiddleware
from swift.common.middleware.gatekeeper import GatekeeperMiddleware

from test.debug_logger import debug_logger
from test.unit import with_tempdir, write_fake_ring, patch_policies
from test.unit.common.middleware.helpers import FakeSwift, LeakTrackingIter

from eventlet.green.urllib import request as urllib_request
from eventlet.green.http import client as http_client


class FakeConn(object):
    def __init__(self, body=None):
        if body is None:
            body = []
        self.body = body

    def read(self):
        return json.dumps(self.body).encode('ascii')

    def info(self):
        return {}


def not_sleep(seconds):
    pass


def unicode_string(start, length):
    return u''.join([chr(x) for x in range(start, start + length)])


def path_parts():
    account = unicode_string(1000, 4) + ' ' + unicode_string(1100, 4)
    container = unicode_string(2000, 4) + ' ' + unicode_string(2100, 4)
    obj = unicode_string(3000, 4) + ' ' + unicode_string(3100, 4)
    return account, container, obj


def make_path(account, container=None, obj=None):
    path = '/v1/%s' % quote(account.encode('utf-8'))
    if container:
        path += '/%s' % quote(container.encode('utf-8'))
    if obj:
        path += '/%s' % quote(obj.encode('utf-8'))
    return path


def make_path_info(account, container=None, obj=None):
    # FakeSwift keys on PATH_INFO - which is *encoded* but unquoted
    path = '/v1/%s' % '/'.join(
        p for p in (account, container, obj) if p)
    return swob.bytes_to_wsgi(path.encode('utf-8'))


def get_client_app():
    app = FakeSwift()
    client = internal_client.InternalClient({}, 'test', 1, app=app)
    return client, app


class InternalClient(internal_client.InternalClient):
    def __init__(self):
        pass


class GetMetadataInternalClient(internal_client.InternalClient):
    def __init__(self, test, path, metadata_prefix, acceptable_statuses):
        self.test = test
        self.path = path
        self.metadata_prefix = metadata_prefix
        self.acceptable_statuses = acceptable_statuses
        self.get_metadata_called = 0
        self.metadata = 'some_metadata'

    def _get_metadata(self, path, metadata_prefix, acceptable_statuses=None,
                      headers=None, params=None):
        self.get_metadata_called += 1
        self.test.assertEqual(self.path, path)
        self.test.assertEqual(self.metadata_prefix, metadata_prefix)
        self.test.assertEqual(self.acceptable_statuses, acceptable_statuses)
        return self.metadata


class SetMetadataInternalClient(internal_client.InternalClient):
    def __init__(
            self, test, path, metadata, metadata_prefix, acceptable_statuses):
        self.test = test
        self.path = path
        self.metadata = metadata
        self.metadata_prefix = metadata_prefix
        self.acceptable_statuses = acceptable_statuses
        self.set_metadata_called = 0
        self.metadata = 'some_metadata'

    def _set_metadata(
            self, path, metadata, metadata_prefix='',
            acceptable_statuses=None):
        self.set_metadata_called += 1
        self.test.assertEqual(self.path, path)
        self.test.assertEqual(self.metadata_prefix, metadata_prefix)
        self.test.assertEqual(self.metadata, metadata)
        self.test.assertEqual(self.acceptable_statuses, acceptable_statuses)


class IterInternalClient(internal_client.InternalClient):
    def __init__(
            self, test, path, marker, end_marker, prefix, acceptable_statuses,
            items):
        self.test = test
        self.path = path
        self.marker = marker
        self.end_marker = end_marker
        self.prefix = prefix
        self.acceptable_statuses = acceptable_statuses
        self.items = items

    def _iter_items(
            self, path, marker='', end_marker='', prefix='',
            acceptable_statuses=None):
        self.test.assertEqual(self.path, path)
        self.test.assertEqual(self.marker, marker)
        self.test.assertEqual(self.end_marker, end_marker)
        self.test.assertEqual(self.prefix, prefix)
        self.test.assertEqual(self.acceptable_statuses, acceptable_statuses)
        for item in self.items:
            yield item


class TestCompressingfileReader(unittest.TestCase):
    def test_init(self):
        class CompressObj(object):
            def __init__(self, test, *args):
                self.test = test
                self.args = args

            def method(self, *args):
                self.test.assertEqual(self.args, args)
                return self

        try:
            compressobj = CompressObj(
                self, 9, zlib.DEFLATED, -zlib.MAX_WBITS, zlib.DEF_MEM_LEVEL, 0)

            old_compressobj = internal_client.compressobj
            internal_client.compressobj = compressobj.method

            f = BytesIO(b'')

            fobj = internal_client.CompressingFileReader(f)
            self.assertEqual(f, fobj._f)
            self.assertEqual(compressobj, fobj._compressor)
            self.assertEqual(False, fobj.done)
            self.assertEqual(True, fobj.first)
            self.assertEqual(0, fobj.crc32)
            self.assertEqual(0, fobj.total_size)
        finally:
            internal_client.compressobj = old_compressobj

    def test_read(self):
        exp_data = b'abcdefghijklmnopqrstuvwxyz'
        fobj = internal_client.CompressingFileReader(
            BytesIO(exp_data), chunk_size=5)

        d = zlib.decompressobj(16 + zlib.MAX_WBITS)
        data = b''.join(d.decompress(chunk)
                        for chunk in iter(fobj.read, b''))

        self.assertEqual(exp_data, data)

    def test_seek(self):
        exp_data = b'abcdefghijklmnopqrstuvwxyz'
        fobj = internal_client.CompressingFileReader(
            BytesIO(exp_data), chunk_size=5)

        # read a couple of chunks only
        for _ in range(2):
            fobj.read()

        # read whole thing after seek and check data
        fobj.seek(0)
        d = zlib.decompressobj(16 + zlib.MAX_WBITS)
        data = b''.join(d.decompress(chunk)
                        for chunk in iter(fobj.read, b''))
        self.assertEqual(exp_data, data)

    def test_seek_not_implemented_exception(self):
        fobj = internal_client.CompressingFileReader(
            BytesIO(b''), chunk_size=5)
        self.assertRaises(NotImplementedError, fobj.seek, 10)
        self.assertRaises(NotImplementedError, fobj.seek, 0, 10)


class TestInternalClient(unittest.TestCase):

    @mock.patch('swift.common.utils.HASH_PATH_SUFFIX', new=b'endcap')
    @with_tempdir
    def test_load_from_config(self, tempdir):
        conf_path = os.path.join(tempdir, 'interal_client.conf')
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
        account_ring_path = os.path.join(tempdir, 'account.ring.gz')
        write_fake_ring(account_ring_path)
        container_ring_path = os.path.join(tempdir, 'container.ring.gz')
        write_fake_ring(container_ring_path)
        object_ring_path = os.path.join(tempdir, 'object.ring.gz')
        write_fake_ring(object_ring_path)
        logger = debug_logger('test-ic')
        self.assertEqual(logger.get_lines_for_level('warning'), [])
        with patch_policies([StoragePolicy(0, 'legacy', True)]):
            with mock.patch('swift.proxy.server.get_logger',
                            lambda *a, **kw: logger):
                client = internal_client.InternalClient(conf_path, 'test', 1)
            self.assertEqual(client.account_ring,
                             client.app.app.app.account_ring)
            self.assertEqual(client.account_ring.serialized_path,
                             account_ring_path)
            self.assertEqual(client.container_ring,
                             client.app.app.app.container_ring)
            self.assertEqual(client.container_ring.serialized_path,
                             container_ring_path)
            object_ring = client.app.app.app.get_object_ring(0)
            self.assertEqual(client.get_object_ring(0),
                             object_ring)
            self.assertEqual(object_ring.serialized_path,
                             object_ring_path)

    @mock.patch('swift.common.utils.HASH_PATH_SUFFIX', new=b'endcap')
    @with_tempdir
    def test_load_from_config_with_global_conf(self, tempdir):
        account_ring_path = os.path.join(tempdir, 'account.ring.gz')
        write_fake_ring(account_ring_path)
        container_ring_path = os.path.join(tempdir, 'container.ring.gz')
        write_fake_ring(container_ring_path)
        object_ring_path = os.path.join(tempdir, 'object.ring.gz')
        write_fake_ring(object_ring_path)

        # global_conf will override the 'x = y' syntax in conf file...
        conf_path = os.path.join(tempdir, 'internal_client.conf')
        conf_body = """
        [DEFAULT]
        swift_dir = %s
        log_name = conf-file-log-name

        [pipeline:main]
        pipeline = catch_errors cache proxy-server

        [app:proxy-server]
        use = egg:swift#proxy

        [filter:cache]
        use = egg:swift#memcache

        [filter:catch_errors]
        use = egg:swift#catch_errors
        log_name = catch-errors-log-name
        """ % tempdir
        with open(conf_path, 'w') as f:
            f.write(dedent(conf_body))
        global_conf = {'log_name': 'global-conf-log-name'}
        with patch_policies([StoragePolicy(0, 'legacy', True)]):
            client = internal_client.InternalClient(
                conf_path, 'test', 1, global_conf=global_conf)
        self.assertEqual('global-conf-log-name', client.app.logger.server)

        # ...but the 'set x = y' syntax in conf file DEFAULT section will
        # override global_conf
        conf_body = """
        [DEFAULT]
        swift_dir = %s
        set log_name = conf-file-log-name

        [pipeline:main]
        pipeline = catch_errors cache proxy-server

        [app:proxy-server]
        use = egg:swift#proxy

        [filter:cache]
        use = egg:swift#memcache

        [filter:catch_errors]
        use = egg:swift#catch_errors
        log_name = catch-errors-log-name
        """ % tempdir
        with open(conf_path, 'w') as f:
            f.write(dedent(conf_body))
        global_conf = {'log_name': 'global-conf-log-name'}
        with patch_policies([StoragePolicy(0, 'legacy', True)]):
            client = internal_client.InternalClient(
                conf_path, 'test', 1, global_conf=global_conf)
        self.assertEqual('conf-file-log-name', client.app.logger.server)

        # ...and the 'set x = y' syntax in conf file app section will override
        # DEFAULT section and global_conf
        conf_body = """
        [DEFAULT]
        swift_dir = %s
        set log_name = conf-file-log-name

        [pipeline:main]
        pipeline = catch_errors cache proxy-server

        [app:proxy-server]
        use = egg:swift#proxy

        [filter:cache]
        use = egg:swift#memcache

        [filter:catch_errors]
        use = egg:swift#catch_errors
        set log_name = catch-errors-log-name
        """ % tempdir
        with open(conf_path, 'w') as f:
            f.write(dedent(conf_body))
        global_conf = {'log_name': 'global-conf-log-name'}
        with patch_policies([StoragePolicy(0, 'legacy', True)]):
            client = internal_client.InternalClient(
                conf_path, 'test', 1, global_conf=global_conf)
        self.assertEqual('catch-errors-log-name', client.app.logger.server)

    def test_init(self):
        conf_path = 'some_path'
        app = FakeSwift()

        user_agent = 'some_user_agent'
        request_tries = 123

        with mock.patch.object(internal_client, 'loadapp',
                               return_value=app) as mock_loadapp, \
                self.assertRaises(ValueError):
            # First try with a bad arg
            internal_client.InternalClient(
                conf_path, user_agent, request_tries=0)
        mock_loadapp.assert_not_called()

        # if we load it with the gatekeeper middleware then we also get a
        # value error
        gate_keeper_app = GatekeeperMiddleware(app, {})
        gate_keeper_app._pipeline_final_app = app
        gate_keeper_app._pipeline = [gate_keeper_app, app]
        with mock.patch.object(
                internal_client, 'loadapp', return_value=gate_keeper_app) \
                as mock_loadapp, self.assertRaises(ValueError) as err:
            internal_client.InternalClient(
                conf_path, user_agent, request_tries)
        self.assertEqual(
            str(err.exception),
            ('Gatekeeper middleware is not allowed in the InternalClient '
             'proxy pipeline'))

        with mock.patch.object(
                internal_client, 'loadapp', return_value=app) as mock_loadapp:
            client = internal_client.InternalClient(
                conf_path, user_agent, request_tries)

        mock_loadapp.assert_called_once_with(
            conf_path, global_conf=None, allow_modify_pipeline=False)
        self.assertEqual(app, client.app)
        self.assertEqual(user_agent, client.user_agent)
        self.assertEqual(request_tries, client.request_tries)
        self.assertFalse(client.use_replication_network)

        client = internal_client.InternalClient(
            conf_path, user_agent, request_tries, app=app,
            use_replication_network=True)
        self.assertEqual(app, client.app)
        self.assertEqual(user_agent, client.user_agent)
        self.assertEqual(request_tries, client.request_tries)
        self.assertTrue(client.use_replication_network)

        global_conf = {'log_name': 'custom'}
        client = internal_client.InternalClient(
            conf_path, user_agent, request_tries, app=app,
            use_replication_network=True, global_conf=global_conf)
        self.assertEqual(app, client.app)
        self.assertEqual(user_agent, client.user_agent)
        self.assertEqual(request_tries, client.request_tries)
        self.assertTrue(client.use_replication_network)

    def test_init_allow_modify_pipeline(self):
        conf_path = 'some_path'
        app = FakeSwift()
        user_agent = 'some_user_agent'

        with mock.patch.object(internal_client, 'loadapp',
                               return_value=app) as mock_loadapp, \
                self.assertRaises(ValueError) as cm:
            internal_client.InternalClient(
                conf_path, user_agent, 1, allow_modify_pipeline=True)
        mock_loadapp.assert_not_called()
        self.assertIn("'allow_modify_pipeline' is no longer supported",
                      str(cm.exception))

        with mock.patch.object(
                internal_client, 'loadapp', return_value=app) as mock_loadapp:
            internal_client.InternalClient(
                conf_path, user_agent, 1, allow_modify_pipeline=False)
        mock_loadapp.assert_called_once_with(
            conf_path, allow_modify_pipeline=False, global_conf=None)

    def test_gatekeeper_not_loaded(self):
        app = FakeSwift()
        pipeline = [app]

        class RandomMiddleware(object):
            def __init__(self, app):
                self.app = app
                self._pipeline_final_app = app
                self._pipeline = pipeline
                self._pipeline.insert(0, self)

        # if there is no Gatekeeper middleware then it's false
        # just the final app
        self.assertFalse(
            internal_client.InternalClient.check_gatekeeper_not_loaded(app))

        # now with a bunch of middlewares
        app_no_gatekeeper = app
        for i in range(5):
            app_no_gatekeeper = RandomMiddleware(app_no_gatekeeper)
            self.assertFalse(
                internal_client.InternalClient.check_gatekeeper_not_loaded(
                    app_no_gatekeeper))

        # But if we put the gatekeeper on the end, it will be found
        app_with_gatekeeper = GatekeeperMiddleware(app_no_gatekeeper, {})
        pipeline.insert(0, app_with_gatekeeper)
        app_with_gatekeeper._pipeline = pipeline
        with self.assertRaises(ValueError) as err:
            internal_client.InternalClient.check_gatekeeper_not_loaded(
                app_with_gatekeeper)
        self.assertEqual(str(err.exception),
                         ('Gatekeeper middleware is not allowed in the '
                          'InternalClient proxy pipeline'))

        # even if we bury deep into the pipeline
        for i in range(5):
            app_with_gatekeeper = RandomMiddleware(app_with_gatekeeper)
            with self.assertRaises(ValueError) as err:
                internal_client.InternalClient.check_gatekeeper_not_loaded(
                    app_with_gatekeeper)
            self.assertEqual(str(err.exception),
                             ('Gatekeeper middleware is not allowed in the '
                              'InternalClient proxy pipeline'))

    def test_make_request_sets_user_agent(self):
        class FakeApp(FakeSwift):
            def __init__(self, test):
                super(FakeApp, self).__init__()
                self.test = test

            def __call__(self, env, start_response):
                self.test.assertNotIn(
                    'HTTP_X_BACKEND_USE_REPLICATION_NETWORK', env)
                self.test.assertEqual(self.backend_user_agent,
                                      "some_agent")
                start_response('200 Ok', [('Content-Length', '0')])
                return []

        client = internal_client.InternalClient(
            None, 'some_agent', 1, use_replication_network=False,
            app=FakeApp(self))
        client.make_request('GET', '/', {}, (200,))

    def test_make_request_clears_txn_id_after_calling_app(self):
        class InternalClient(internal_client.InternalClient):
            def __init__(self, test, logger):
                def fake_app(env, start_response):
                    self.app.logger.txn_id = 'foo'
                    self.app.logger.debug('Inside of request')
                    start_response('200 Ok', [('Content-Length', '0')])
                    return []

                self.test = test
                self.user_agent = 'some_agent'
                self.app = fake_app
                self.app.logger = logger
                self.request_tries = 1
                self.use_replication_network = False

        logger = debug_logger()
        # Make sure there's no transaction ID set -- other tests may have
        # polluted the logger
        logger.txn_id = None
        logger.debug('Before request')
        client = InternalClient(self, logger)
        client.make_request('GET', '/', {}, (200,))
        logger.debug('After request')
        self.assertEqual([(args[0], kwargs['extra'].get('txn_id'))
                          for args, kwargs in logger.log_dict['debug']], [
            ('Before request', None),
            ('Inside of request', 'foo'),
            ('After request', None),
        ])

    def test_make_request_defaults_replication_network_header(self):
        class FakeApp(FakeSwift):
            def __init__(self, test):
                super(FakeApp, self).__init__()
                self.test = test
                self.expected_header_value = None

            def __call__(self, env, start_response):
                if self.expected_header_value is None:
                    self.test.assertNotIn(
                        'HTTP_X_BACKEND_USE_REPLICATION_NETWORK', env)
                else:
                    hdr_val = env['HTTP_X_BACKEND_USE_REPLICATION_NETWORK']
                    self.test.assertEqual(self.expected_header_value, hdr_val)
                self.test.assertEqual(self.backend_user_agent,
                                      'some_agent')
                start_response('200 Ok', [('Content-Length', '0')])
                return []

        client = internal_client.InternalClient(
            None, 'some_agent', 1, use_replication_network=False,
            app=FakeApp(self))
        client.make_request('GET', '/', {}, (200,))
        # Caller can still override
        client.app.expected_header_value = 'false'
        client.make_request('GET', '/', {
            request_helpers.USE_REPLICATION_NETWORK_HEADER: 'false'}, (200,))
        client.app.expected_header_value = 'true'
        client.make_request('GET', '/', {
            request_helpers.USE_REPLICATION_NETWORK_HEADER: 'true'}, (200,))

        # Switch default behavior
        client.use_replication_network = True

        client.make_request('GET', '/', {}, (200,))
        client.app.expected_header_value = 'false'
        client.make_request('GET', '/', {
            request_helpers.USE_REPLICATION_NETWORK_HEADER: 'false'}, (200,))
        client.app.expected_header_value = 'on'
        client.make_request('GET', '/', {
            request_helpers.USE_REPLICATION_NETWORK_HEADER: 'on'}, (200,))

    def test_make_request_sets_query_string(self):
        captured_envs = []

        class FakeApp(FakeSwift):
            def __call__(self, env, start_response):
                captured_envs.append(env)
                start_response('200 Ok', [('Content-Length', '0')])
                return []

        client = internal_client.InternalClient(
            None, 'some_agent', 1, use_replication_network=False,
            app=FakeApp())
        params = {'param1': 'p1', 'tasty': 'soup'}
        client.make_request('GET', '/', {}, (200,), params=params)
        actual_params = dict(parse_qsl(captured_envs[0]['QUERY_STRING'],
                                       keep_blank_values=True,
                                       strict_parsing=True))
        self.assertEqual(params, actual_params)
        self.assertEqual(client.app._pipeline_final_app.backend_user_agent,
                         'some_agent')

    def test_make_request_retries(self):
        class FakeApp(FakeSwift):
            def __init__(self):
                super(FakeApp, self).__init__()
                self.request_tries = 4
                self.tries = 0

            def __call__(self, env, start_response):
                self.tries += 1
                if self.tries < self.request_tries:
                    start_response(
                        '500 Internal Server Error', [('Content-Length', '0')])
                else:
                    start_response('200 Ok', [('Content-Length', '0')])
                return []

        class InternalClient(internal_client.InternalClient):
            def __init__(self, *args, **kwargs):
                self.test = kwargs.pop('test')
                super(InternalClient, self).__init__(*args, **kwargs)
                self.sleep_called = 0

            def sleep(self, seconds):
                self.sleep_called += 1
                self.test.assertEqual(2 ** (self.sleep_called), seconds)

        client = InternalClient(
            None, 'some_agent', 4, use_replication_network=False,
            app=FakeApp(), test=self)

        old_sleep = internal_client.sleep
        internal_client.sleep = client.sleep

        try:
            client.make_request('GET', '/', {}, (200,))
        finally:
            internal_client.sleep = old_sleep

        self.assertEqual(3, client.sleep_called)
        self.assertEqual(4, client.app.tries)
        self.assertEqual(client.app._pipeline_final_app.backend_user_agent,
                         'some_agent')

    def test_base_request_timeout(self):
        # verify that base_request passes timeout arg on to urlopen
        body = {"some": "content"}

        for timeout in (0.0, 42.0, None):
            mocked_func = 'swift.common.internal_client.urllib_request.urlopen'
            with mock.patch(mocked_func) as mock_urlopen:
                mock_urlopen.side_effect = [FakeConn(body)]
                sc = internal_client.SimpleClient('http://0.0.0.0/')
                _, resp_body = sc.base_request('GET', timeout=timeout)
                mock_urlopen.assert_called_once_with(mock.ANY, timeout=timeout)
                # sanity check
                self.assertEqual(body, resp_body)

    def test_base_full_listing(self):
        body1 = [{'name': 'a'}, {'name': "b"}, {'name': "c"}]
        body2 = [{'name': 'd'}]
        body3 = []

        mocked_func = 'swift.common.internal_client.urllib_request.urlopen'
        with mock.patch(mocked_func) as mock_urlopen:
            mock_urlopen.side_effect = [
                FakeConn(body1), FakeConn(body2), FakeConn(body3)]
            sc = internal_client.SimpleClient('http://0.0.0.0/')
            _, resp_body = sc.base_request('GET', full_listing=True)
        self.assertEqual(body1 + body2, resp_body)
        self.assertEqual(3, mock_urlopen.call_count)
        actual_requests = [call[0][0] for call in mock_urlopen.call_args_list]
        self.assertEqual('/?format=json', actual_requests[0].selector)
        self.assertEqual(
            '/?format=json&marker=c', actual_requests[1].selector)
        self.assertEqual(
            '/?format=json&marker=d', actual_requests[2].selector)

    def test_make_request_method_path_headers(self):
        class FakeApp(FakeSwift):
            def __init__(self):
                super(FakeApp, self).__init__()
                self.env = None

            def __call__(self, env, start_response):
                self.env = env
                start_response('200 Ok', [('Content-Length', '0')])
                return []

        client = internal_client.InternalClient(
            None, 'some_agent', 3, use_replication_network=False,
            app=FakeApp())

        for method in 'GET PUT HEAD'.split():
            client.make_request(method, '/', {}, (200,))
            self.assertEqual(client.app.env['REQUEST_METHOD'], method)

        for path in '/one /two/three'.split():
            client.make_request('GET', path, {'X-Test': path}, (200,))
            self.assertEqual(client.app.env['PATH_INFO'], path)
            self.assertEqual(client.app.env['HTTP_X_TEST'], path)
        self.assertEqual(client.app._pipeline_final_app.backend_user_agent,
                         'some_agent')

    def test_make_request_error_case(self):
        class FakeApp(FakeSwift):
            def __call__(self, env, start_response):
                body = b'fake error response'
                start_response('409 Conflict',
                               [('Content-Length', str(len(body)))])
                return [body]

        final_fake_app = FakeApp()
        fake_app = ProxyLoggingMiddleware(
            final_fake_app, {}, final_fake_app.logger)
        fake_app._pipeline_final_app = final_fake_app

        client = internal_client.InternalClient(
            None, 'some_agent', 3, use_replication_network=False, app=fake_app)
        with self.assertRaises(internal_client.UnexpectedResponse), \
                mock.patch('swift.common.internal_client.sleep'):
            client.make_request('DELETE', '/container', {}, (200,))

        # Since we didn't provide an X-Timestamp, retrying gives us a chance to
        # succeed (assuming the failure was due to clock skew between servers)
        expected = (' HTTP/1.0 409 ',)
        logger = client.app._pipeline_final_app.logger
        loglines = logger.get_lines_for_level('info')
        for expected, logline in zip_longest(expected, loglines):
            if not expected:
                self.fail('Unexpected extra log line: %r' % logline)
            self.assertIn(expected, logline)
        self.assertEqual(client.app.app.backend_user_agent, 'some_agent')

    def test_make_request_acceptable_status_not_2xx(self):
        class FakeApp(FakeSwift):
            def __init__(self):
                super(FakeApp, self).__init__()
                self.closed_paths = []
                self.fully_read_paths = []
                self.resp_status = None

            def __call__(self, env, start_response):
                body = b'fake error response'
                start_response(self.resp_status,
                               [('Content-Length', str(len(body)))])
                return LeakTrackingIter(body, self.closed_paths.append,
                                        self.fully_read_paths.append,
                                        env['PATH_INFO'])

        def do_test(resp_status):
            final_fake_app = FakeApp()
            fake_app = ProxyLoggingMiddleware(
                final_fake_app, {}, final_fake_app.logger)
            fake_app._pipeline_final_app = final_fake_app
            final_fake_app.resp_status = resp_status
            client = internal_client.InternalClient(
                None, "some_agent", 3, use_replication_network=False,
                app=fake_app)
            expected_error = internal_client.UnexpectedResponse
            with self.assertRaises(expected_error) as ctx, \
                    mock.patch('swift.common.internal_client.sleep'):
                # This is obvious strange tests to expect only 400 Bad Request
                # but this test intended to avoid extra body drain if it's
                # correct object body with 2xx.
                client.make_request('GET', '/cont/obj', {}, (400,))
            logger = client.app._pipeline_final_app.logger
            loglines = logger.get_lines_for_level('info')
            self.assertEqual(client.app.app.backend_user_agent, 'some_agent')
            return (client.app._pipeline_final_app.fully_read_paths,
                    client.app._pipeline_final_app.closed_paths,
                    ctx.exception.resp, loglines)

        fully_read_paths, closed_paths, resp, loglines = do_test('200 OK')
        # Since the 200 is considered "properly handled", it won't be retried
        self.assertEqual(fully_read_paths, [])
        self.assertEqual(closed_paths, [])
        # ...and it'll be on us (the caller) to read and close (for example,
        # by using swob.Response's body property)
        self.assertEqual(resp.body, b'fake error response')
        self.assertEqual(fully_read_paths, ['/cont/obj'])
        self.assertEqual(closed_paths, ['/cont/obj'])

        expected = (' HTTP/1.0 200 ', )
        for expected, logline in zip_longest(expected, loglines):
            if not expected:
                self.fail('Unexpected extra log line: %r' % logline)
            self.assertIn(expected, logline)

        fully_read_paths, closed_paths, resp, loglines = do_test(
            '503 Service Unavailable')
        # But since 5xx is neither "properly handled" not likely to include
        # a large body, it will be retried and responses will already be closed
        self.assertEqual(fully_read_paths, ['/cont/obj'] * 3)
        self.assertEqual(closed_paths, ['/cont/obj'] * 3)

        expected = (' HTTP/1.0 503 ', ' HTTP/1.0 503 ', ' HTTP/1.0 503 ', )
        for expected, logline in zip_longest(expected, loglines):
            if not expected:
                self.fail('Unexpected extra log line: %r' % logline)
            self.assertIn(expected, logline)

    def test_make_request_codes(self):
        class FakeApp(FakeSwift):
            def __call__(self, env, start_response):
                start_response('200 Ok', [('Content-Length', '0')])
                return []

        client = internal_client.InternalClient(
            None, 'some_agent', 3, use_replication_network=False,
            app=FakeApp())

        try:
            old_sleep = internal_client.sleep
            internal_client.sleep = not_sleep

            client.make_request('GET', '/', {}, (200,))
            client.make_request('GET', '/', {}, (2,))
            client.make_request('GET', '/', {}, (400, 200))
            client.make_request('GET', '/', {}, (400, 2))

            with self.assertRaises(internal_client.UnexpectedResponse) \
                    as raised:
                client.make_request('GET', '/', {}, (400,))
            self.assertEqual(200, raised.exception.resp.status_int)

            with self.assertRaises(internal_client.UnexpectedResponse) \
                    as raised:
                client.make_request('GET', '/', {}, (201,))
            self.assertEqual(200, raised.exception.resp.status_int)

            with self.assertRaises(internal_client.UnexpectedResponse) \
                    as raised:
                client.make_request('GET', '/', {}, (111,))
            self.assertTrue(str(raised.exception).startswith(
                'Unexpected response'))
            self.assertEqual(client.app._pipeline_final_app.backend_user_agent,
                             'some_agent')
        finally:
            internal_client.sleep = old_sleep

    def test_make_request_calls_fobj_seek_each_try(self):
        class FileObject(object):
            def __init__(self, test):
                self.test = test
                self.seek_called = 0

            def seek(self, offset, whence=0):
                self.seek_called += 1
                self.test.assertEqual(0, offset)
                self.test.assertEqual(0, whence)

        class FakeApp(FakeSwift):
            def __init__(self, status):
                super(FakeApp, self).__init__()
                self.status = status

            def __call__(self, env, start_response):
                start_response(self.status, [('Content-Length', '0')])
                self._calls.append('')
                return []

        def do_test(status, expected_calls):
            fobj = FileObject(self)
            client = internal_client.InternalClient(
                None, 'some_agent', 3, use_replication_network=False,
                app=FakeApp(status))

            with mock.patch.object(internal_client, 'sleep', not_sleep):
                with self.assertRaises(Exception) as exc_mgr:
                    client.make_request('PUT', '/', {}, (2,), fobj)
                self.assertEqual(int(status[:3]),
                                 exc_mgr.exception.resp.status_int)

            self.assertEqual(client.app.call_count, fobj.seek_called)
            self.assertEqual(client.app.call_count, expected_calls)
            self.assertEqual(client.app._pipeline_final_app.backend_user_agent,
                             'some_agent')

        do_test('404 Not Found', 1)
        do_test('503 Service Unavailable', 3)

    def test_make_request_request_exception(self):
        class FakeApp(FakeSwift):
            def __call__(self, env, start_response):
                raise Exception()

        client = internal_client.InternalClient(
            None, 'some_agent', 3, app=FakeApp())
        self.assertEqual(client.app._pipeline_final_app.backend_user_agent,
                         'some_agent')
        try:
            old_sleep = internal_client.sleep
            internal_client.sleep = not_sleep
            self.assertRaises(
                Exception, client.make_request, 'GET', '/', {}, (2,))
        finally:
            internal_client.sleep = old_sleep

    def test_get_metadata(self):
        class Response(object):
            def __init__(self, headers):
                self.headers = headers
                self.status_int = 200

        class InternalClient(internal_client.InternalClient):
            def __init__(self, test, path, resp_headers):
                self.test = test
                self.path = path
                self.resp_headers = resp_headers
                self.make_request_called = 0

            def make_request(
                    self, method, path, headers, acceptable_statuses,
                    body_file=None, params=None):
                self.make_request_called += 1
                self.test.assertEqual('HEAD', method)
                self.test.assertEqual(self.path, path)
                self.test.assertEqual((2,), acceptable_statuses)
                self.test.assertIsNone(body_file)
                return Response(self.resp_headers)

        path = 'some_path'
        metadata_prefix = 'some_key-'
        resp_headers = {
            '%sone' % (metadata_prefix): '1',
            '%sTwo' % (metadata_prefix): '2',
            '%sThree' % (metadata_prefix): '3',
            'some_header-four': '4',
            'Some_header-five': '5',
        }
        exp_metadata = {
            'one': '1',
            'two': '2',
            'three': '3',
        }

        client = InternalClient(self, path, resp_headers)
        metadata = client._get_metadata(path, metadata_prefix)
        self.assertEqual(exp_metadata, metadata)
        self.assertEqual(1, client.make_request_called)

    def test_get_metadata_invalid_status(self):

        class FakeApp(FakeSwift):
            def __call__(self, environ, start_response):
                start_response('404 Not Found', [('x-foo', 'bar')])
                return [b'nope']

        client = internal_client.InternalClient(
            None, 'test', 1, use_replication_network=False, app=FakeApp())
        self.assertRaises(internal_client.UnexpectedResponse,
                          client._get_metadata, 'path')
        metadata = client._get_metadata('path', metadata_prefix='x-',
                                        acceptable_statuses=(4,))
        self.assertEqual(metadata, {'foo': 'bar'})
        self.assertEqual(client.app._pipeline_final_app.backend_user_agent,
                         'test')

    def test_make_path(self):
        account, container, obj = path_parts()
        path = make_path(account, container, obj)

        c = InternalClient()
        self.assertEqual(path, c.make_path(account, container, obj))

    def test_make_path_exception(self):
        c = InternalClient()
        self.assertRaises(ValueError, c.make_path, 'account', None, 'obj')

    def test_iter_items(self):
        class Response(object):
            def __init__(self, status_int, body):
                self.status_int = status_int
                self.body = body

        class InternalClient(internal_client.InternalClient):
            def __init__(self, test, responses):
                self.test = test
                self.responses = responses
                self.make_request_called = 0

            def make_request(
                    self, method, path, headers, acceptable_statuses,
                    body_file=None):
                self.make_request_called += 1
                return self.responses.pop(0)

        exp_items = []
        responses = [Response(200, json.dumps([]).encode('ascii')), ]
        items = []
        client = InternalClient(self, responses)
        for item in client._iter_items('/'):
            items.append(item)
        self.assertEqual(exp_items, items)

        exp_items = []
        responses = []
        for i in range(3):
            data = [
                {'name': 'item%02d' % (2 * i)},
                {'name': 'item%02d' % (2 * i + 1)}]
            responses.append(Response(200, json.dumps(data).encode('ascii')))
            exp_items.extend(data)
        responses.append(Response(204, ''))

        items = []
        client = InternalClient(self, responses)
        for item in client._iter_items('/'):
            items.append(item)
        self.assertEqual(exp_items, items)

    def test_iter_items_with_markers(self):
        class Response(object):
            def __init__(self, status_int, body):
                self.status_int = status_int
                self.body = body.encode('ascii')

        class InternalClient(internal_client.InternalClient):
            def __init__(self, test, paths, responses):
                self.test = test
                self.paths = paths
                self.responses = responses

            def make_request(
                    self, method, path, headers, acceptable_statuses,
                    body_file=None):
                exp_path = self.paths.pop(0)
                self.test.assertEqual(exp_path, path)
                return self.responses.pop(0)

        paths = [
            '/?format=json&marker=start&end_marker=end&prefix=',
            '/?format=json&marker=one%C3%A9&end_marker=end&prefix=',
            '/?format=json&marker=two&end_marker=end&prefix=',
        ]

        responses = [
            Response(200, json.dumps([{
                'name': b'one\xc3\xa9'.decode('utf8')}, ])),
            Response(200, json.dumps([{'name': 'two'}, ])),
            Response(204, ''),
        ]

        items = []
        client = InternalClient(self, paths, responses)
        for item in client._iter_items('/', marker='start', end_marker='end'):
            items.append(item['name'].encode('utf8'))

        self.assertEqual(b'one\xc3\xa9 two'.split(), items)

    def test_iter_items_with_markers_and_prefix(self):
        class Response(object):
            def __init__(self, status_int, body):
                self.status_int = status_int
                self.body = body.encode('ascii')

        class InternalClient(internal_client.InternalClient):
            def __init__(self, test, paths, responses):
                self.test = test
                self.paths = paths
                self.responses = responses

            def make_request(
                    self, method, path, headers, acceptable_statuses,
                    body_file=None):
                exp_path = self.paths.pop(0)
                self.test.assertEqual(exp_path, path)
                return self.responses.pop(0)

        paths = [
            '/?format=json&marker=prefixed_start&end_marker=prefixed_end'
            '&prefix=prefixed_',
            '/?format=json&marker=prefixed_one%C3%A9&end_marker=prefixed_end'
            '&prefix=prefixed_',
            '/?format=json&marker=prefixed_two&end_marker=prefixed_end'
            '&prefix=prefixed_',
        ]

        responses = [
            Response(200, json.dumps([{
                'name': b'prefixed_one\xc3\xa9'.decode('utf8')}, ])),
            Response(200, json.dumps([{'name': 'prefixed_two'}, ])),
            Response(204, ''),
        ]

        items = []
        client = InternalClient(self, paths, responses)
        for item in client._iter_items('/', marker='prefixed_start',
                                       end_marker='prefixed_end',
                                       prefix='prefixed_'):
            items.append(item['name'].encode('utf8'))

        self.assertEqual(b'prefixed_one\xc3\xa9 prefixed_two'.split(), items)

    def test_iter_item_read_response_if_status_is_acceptable(self):
        class Response(object):
            def __init__(self, status_int, body, app_iter):
                self.status_int = status_int
                self.body = body
                self.app_iter = app_iter

        class InternalClient(internal_client.InternalClient):
            def __init__(self, test, responses):
                self.test = test
                self.responses = responses

            def make_request(
                self, method, path, headers, acceptable_statuses,
                    body_file=None):
                resp = self.responses.pop(0)
                if resp.status_int in acceptable_statuses or \
                        resp.status_int // 100 in acceptable_statuses:
                    return resp
                if resp:
                    raise internal_client.UnexpectedResponse(
                        'Unexpected response: %s' % resp.status_int, resp)

        num_list = []

        def generate_resp_body():
            for i in range(1, 5):
                yield str(i).encode('ascii')
                num_list.append(i)

        exp_items = []
        responses = [Response(204, json.dumps([]).encode('ascii'),
                              generate_resp_body())]
        items = []
        client = InternalClient(self, responses)
        for item in client._iter_items('/'):
            items.append(item)
        self.assertEqual(exp_items, items)
        self.assertEqual(len(num_list), 0)

        responses = [Response(300, json.dumps([]).encode('ascii'),
                              generate_resp_body())]
        client = InternalClient(self, responses)
        self.assertRaises(internal_client.UnexpectedResponse,
                          next, client._iter_items('/'))

        exp_items = []
        responses = [Response(404, json.dumps([]).encode('ascii'),
                              generate_resp_body())]
        items = []
        client = InternalClient(self, responses)
        for item in client._iter_items('/'):
            items.append(item)
        self.assertEqual(exp_items, items)
        self.assertEqual(len(num_list), 4)

    def test_set_metadata(self):
        class InternalClient(internal_client.InternalClient):
            def __init__(self, test, path, exp_headers):
                self.test = test
                self.path = path
                self.exp_headers = exp_headers
                self.make_request_called = 0

            def make_request(
                    self, method, path, headers, acceptable_statuses,
                    body_file=None):
                self.make_request_called += 1
                self.test.assertEqual('POST', method)
                self.test.assertEqual(self.path, path)
                self.test.assertEqual(self.exp_headers, headers)
                self.test.assertEqual((2,), acceptable_statuses)
                self.test.assertIsNone(body_file)

        path = 'some_path'
        metadata_prefix = 'some_key-'
        metadata = {
            '%sone' % (metadata_prefix): '1',
            '%stwo' % (metadata_prefix): '2',
            'three': '3',
        }
        exp_headers = {
            '%sone' % (metadata_prefix): '1',
            '%stwo' % (metadata_prefix): '2',
            '%sthree' % (metadata_prefix): '3',
        }

        client = InternalClient(self, path, exp_headers)
        client._set_metadata(path, metadata, metadata_prefix)
        self.assertEqual(1, client.make_request_called)

    def test_iter_containers(self):
        account, container, obj = path_parts()
        path = make_path(account)
        items = '0 1 2'.split()
        marker = 'some_marker'
        end_marker = 'some_end_marker'
        prefix = 'some_prefix'
        acceptable_statuses = 'some_status_list'
        client = IterInternalClient(
            self, path, marker, end_marker, prefix, acceptable_statuses, items)
        ret_items = []
        for container in client.iter_containers(
                account, marker, end_marker, prefix,
                acceptable_statuses=acceptable_statuses):
            ret_items.append(container)
        self.assertEqual(items, ret_items)

    def test_create_account(self):
        account, container, obj = path_parts()
        path = make_path_info(account)
        client, app = get_client_app()
        app.register('PUT', path, swob.HTTPCreated, {})
        client.create_account(account)
        self.assertEqual([('PUT', path, {
            'X-Backend-Allow-Reserved-Names': 'true',
            'Host': 'localhost:80',
            'User-Agent': 'test'
        })], app.calls_with_headers)
        self.assertEqual(app.backend_user_agent, 'test')
        self.assertEqual({}, app.unread_requests)
        self.assertEqual({}, app.unclosed_requests)

    def test_delete_account(self):
        account, container, obj = path_parts()
        path = make_path_info(account)
        client, app = get_client_app()
        app.register('DELETE', path, swob.HTTPNoContent, {})
        client.delete_account(account)
        self.assertEqual(1, len(app._calls))
        self.assertEqual({}, app.unread_requests)
        self.assertEqual({}, app.unclosed_requests)
        self.assertEqual(app.backend_user_agent, 'test')

    def test_get_account_info(self):
        class Response(object):
            def __init__(self, containers, objects):
                self.headers = {
                    'x-account-container-count': containers,
                    'x-account-object-count': objects,
                }
                self.status_int = 200

        class InternalClient(internal_client.InternalClient):
            def __init__(self, test, path, resp):
                self.test = test
                self.path = path
                self.resp = resp

            def make_request(
                    self, method, path, headers, acceptable_statuses,
                    body_file=None):
                self.test.assertEqual('HEAD', method)
                self.test.assertEqual(self.path, path)
                self.test.assertEqual({}, headers)
                self.test.assertEqual((2, 404), acceptable_statuses)
                self.test.assertIsNone(body_file)
                return self.resp

        account, container, obj = path_parts()
        path = make_path(account)
        containers, objects = 10, 100
        client = InternalClient(self, path, Response(containers, objects))
        info = client.get_account_info(account)
        self.assertEqual((containers, objects), info)

    def test_get_account_info_404(self):
        class Response(object):
            def __init__(self):
                self.headers = {
                    'x-account-container-count': 10,
                    'x-account-object-count': 100,
                }
                self.status_int = 404

        class InternalClient(internal_client.InternalClient):
            def __init__(self):
                pass

            def make_path(self, *a, **kw):
                return 'some_path'

            def make_request(self, *a, **kw):
                return Response()

        client = InternalClient()
        info = client.get_account_info('some_account')
        self.assertEqual((0, 0), info)

    def test_get_account_metadata(self):
        account, container, obj = path_parts()
        path = make_path(account)
        acceptable_statuses = 'some_status_list'
        metadata_prefix = 'some_metadata_prefix'
        client = GetMetadataInternalClient(
            self, path, metadata_prefix, acceptable_statuses)
        metadata = client.get_account_metadata(
            account, metadata_prefix, acceptable_statuses)
        self.assertEqual(client.metadata, metadata)
        self.assertEqual(1, client.get_metadata_called)

    def test_get_metadadata_with_acceptable_status(self):
        account, container, obj = path_parts()
        path = make_path_info(account)
        client, app = get_client_app()
        resp_headers = {'some-important-header': 'some value'}
        app.register('GET', path, swob.HTTPOk, resp_headers)
        metadata = client.get_account_metadata(
            account, acceptable_statuses=(2, 4))
        self.assertEqual(metadata['some-important-header'],
                         'some value')
        app.register('GET', path, swob.HTTPNotFound, resp_headers)
        metadata = client.get_account_metadata(
            account, acceptable_statuses=(2, 4))
        self.assertEqual(metadata['some-important-header'],
                         'some value')
        app.register('GET', path, swob.HTTPServerError, resp_headers)
        self.assertRaises(internal_client.UnexpectedResponse,
                          client.get_account_metadata, account,
                          acceptable_statuses=(2, 4))

    def test_set_account_metadata(self):
        account, container, obj = path_parts()
        path = make_path_info(account)
        client, app = get_client_app()
        app.register('POST', path, swob.HTTPAccepted, {})
        client.set_account_metadata(account, {'Color': 'Blue'},
                                    metadata_prefix='X-Account-Meta-')
        self.assertEqual([('POST', path, {
            'X-Backend-Allow-Reserved-Names': 'true',
            'Host': 'localhost:80',
            'X-Account-Meta-Color': 'Blue',
            'User-Agent': 'test'
        })], app.calls_with_headers)
        self.assertEqual({}, app.unread_requests)
        self.assertEqual({}, app.unclosed_requests)
        self.assertEqual(app.backend_user_agent, 'test')

    def test_set_account_metadata_plumbing(self):
        account, container, obj = path_parts()
        path = make_path(account)
        metadata = 'some_metadata'
        metadata_prefix = 'some_metadata_prefix'
        acceptable_statuses = 'some_status_list'
        client = SetMetadataInternalClient(
            self, path, metadata, metadata_prefix, acceptable_statuses)
        client.set_account_metadata(
            account, metadata, metadata_prefix, acceptable_statuses)
        self.assertEqual(1, client.set_metadata_called)

    def test_container_exists(self):
        class Response(object):
            def __init__(self, status_int):
                self.status_int = status_int

        class InternalClient(internal_client.InternalClient):
            def __init__(self, test, path, resp):
                self.test = test
                self.path = path
                self.make_request_called = 0
                self.resp = resp

            def make_request(
                    self, method, path, headers, acceptable_statuses,
                    body_file=None):
                self.make_request_called += 1
                self.test.assertEqual('HEAD', method)
                self.test.assertEqual(self.path, path)
                self.test.assertEqual({}, headers)
                self.test.assertEqual((2, 404), acceptable_statuses)
                self.test.assertIsNone(body_file)
                return self.resp

        account, container, obj = path_parts()
        path = make_path(account, container)

        client = InternalClient(self, path, Response(200))
        self.assertEqual(True, client.container_exists(account, container))
        self.assertEqual(1, client.make_request_called)

        client = InternalClient(self, path, Response(404))
        self.assertEqual(False, client.container_exists(account, container))
        self.assertEqual(1, client.make_request_called)

    def test_create_container(self):
        account, container, obj = path_parts()
        path = make_path_info(account, container)
        client, app = get_client_app()
        app.register('PUT', path, swob.HTTPCreated, {})
        client.create_container(account, container)
        self.assertEqual([('PUT', path, {
            'X-Backend-Allow-Reserved-Names': 'true',
            'Host': 'localhost:80',
            'User-Agent': 'test'
        })], app.calls_with_headers)
        self.assertEqual(app.backend_user_agent, 'test')
        self.assertEqual({}, app.unread_requests)
        self.assertEqual({}, app.unclosed_requests)

    def test_create_container_plumbing(self):
        class InternalClient(internal_client.InternalClient):
            def __init__(self, test, path, headers):
                self.test = test
                self.path = path
                self.headers = headers
                self.make_request_called = 0

            def make_request(
                    self, method, path, headers, acceptable_statuses,
                    body_file=None):
                self.make_request_called += 1
                self.test.assertEqual('PUT', method)
                self.test.assertEqual(self.path, path)
                self.test.assertEqual(self.headers, headers)
                self.test.assertEqual((2,), acceptable_statuses)
                self.test.assertIsNone(body_file)

        account, container, obj = path_parts()
        path = make_path(account, container)
        headers = 'some_headers'
        client = InternalClient(self, path, headers)
        client.create_container(account, container, headers)
        self.assertEqual(1, client.make_request_called)

    def test_delete_container(self):
        account, container, obj = path_parts()
        path = make_path_info(account, container)
        client, app = get_client_app()
        app.register('DELETE', path, swob.HTTPNoContent, {})
        client.delete_container(account, container)
        self.assertEqual(1, len(app._calls))
        self.assertEqual({}, app.unread_requests)
        self.assertEqual({}, app.unclosed_requests)
        self.assertEqual(app.backend_user_agent, 'test')

    def test_delete_container_plumbing(self):
        class InternalClient(internal_client.InternalClient):
            def __init__(self, test, path):
                self.test = test
                self.path = path
                self.make_request_called = 0

            def make_request(
                    self, method, path, headers, acceptable_statuses,
                    body_file=None):
                self.make_request_called += 1
                self.test.assertEqual('DELETE', method)
                self.test.assertEqual(self.path, path)
                self.test.assertEqual({}, headers)
                self.test.assertEqual((2, 404), acceptable_statuses)
                self.test.assertIsNone(body_file)

        account, container, obj = path_parts()
        path = make_path(account, container)
        client = InternalClient(self, path)
        client.delete_container(account, container)
        self.assertEqual(1, client.make_request_called)

    def test_get_container_metadata(self):
        account, container, obj = path_parts()
        path = make_path(account, container)
        metadata_prefix = 'some_metadata_prefix'
        acceptable_statuses = 'some_status_list'
        client = GetMetadataInternalClient(
            self, path, metadata_prefix, acceptable_statuses)
        metadata = client.get_container_metadata(
            account, container, metadata_prefix, acceptable_statuses)
        self.assertEqual(client.metadata, metadata)
        self.assertEqual(1, client.get_metadata_called)

    def test_iter_objects(self):
        account, container, obj = path_parts()
        path = make_path(account, container)
        marker = 'some_maker'
        end_marker = 'some_end_marker'
        prefix = 'some_prefix'
        acceptable_statuses = 'some_status_list'
        items = '0 1 2'.split()
        client = IterInternalClient(
            self, path, marker, end_marker, prefix, acceptable_statuses, items)
        ret_items = []
        for obj in client.iter_objects(
                account, container, marker, end_marker, prefix,
                acceptable_statuses):
            ret_items.append(obj)
        self.assertEqual(items, ret_items)

    def test_set_container_metadata(self):
        account, container, obj = path_parts()
        path = make_path_info(account, container)
        client, app = get_client_app()
        app.register('POST', path, swob.HTTPAccepted, {})
        client.set_container_metadata(account, container, {'Color': 'Blue'},
                                      metadata_prefix='X-Container-Meta-')
        self.assertEqual([('POST', path, {
            'X-Backend-Allow-Reserved-Names': 'true',
            'Host': 'localhost:80',
            'X-Container-Meta-Color': 'Blue',
            'User-Agent': 'test'
        })], app.calls_with_headers)
        self.assertEqual({}, app.unread_requests)
        self.assertEqual({}, app.unclosed_requests)
        self.assertEqual(app.backend_user_agent, 'test')

    def test_set_container_metadata_plumbing(self):
        account, container, obj = path_parts()
        path = make_path(account, container)
        metadata = 'some_metadata'
        metadata_prefix = 'some_metadata_prefix'
        acceptable_statuses = 'some_status_list'
        client = SetMetadataInternalClient(
            self, path, metadata, metadata_prefix, acceptable_statuses)
        client.set_container_metadata(
            account, container, metadata, metadata_prefix, acceptable_statuses)
        self.assertEqual(1, client.set_metadata_called)

    def test_delete_object(self):
        account, container, obj = path_parts()
        path = make_path_info(account, container, obj)
        client, app = get_client_app()
        app.register('DELETE', path, swob.HTTPNoContent, {})
        client.delete_object(account, container, obj)
        self.assertEqual(app.unclosed_requests, {})
        self.assertEqual(1, len(app._calls))
        self.assertEqual({}, app.unread_requests)
        self.assertEqual({}, app.unclosed_requests)
        self.assertEqual(app.backend_user_agent, 'test')

        app.register('DELETE', path, swob.HTTPNotFound, {})
        client.delete_object(account, container, obj)
        self.assertEqual(app.unclosed_requests, {})
        self.assertEqual(2, len(app._calls))
        self.assertEqual({}, app.unread_requests)
        self.assertEqual({}, app.unclosed_requests)

    def test_get_object_metadata(self):
        account, container, obj = path_parts()
        path = make_path(account, container, obj)
        metadata_prefix = 'some_metadata_prefix'
        acceptable_statuses = 'some_status_list'
        client = GetMetadataInternalClient(
            self, path, metadata_prefix, acceptable_statuses)
        metadata = client.get_object_metadata(
            account, container, obj, metadata_prefix,
            acceptable_statuses)
        self.assertEqual(client.metadata, metadata)
        self.assertEqual(1, client.get_metadata_called)

    def test_get_metadata_extra_headers(self):
        class FakeApp(FakeSwift):
            def __call__(self, env, start_response):
                self.req_env = env
                start_response('200 Ok', [('Content-Length', '0')])
                return []

        client = internal_client.InternalClient(
            None, 'some_agent', 3, use_replication_network=False,
            app=FakeApp())
        headers = {'X-Foo': 'bar'}
        client.get_object_metadata('account', 'container', 'obj',
                                   headers=headers)
        self.assertEqual(client.app.req_env['HTTP_X_FOO'], 'bar')

    def test_get_object(self):
        account, container, obj = path_parts()
        path_info = make_path_info(account, container, obj)
        client, app = get_client_app()
        headers = {'foo': 'bar'}
        body = b'some_object_body'
        params = {'symlink': 'get'}
        app.register('GET', path_info, swob.HTTPOk, headers, body)
        req_headers = {'x-important-header': 'some_important_value'}
        status_int, resp_headers, obj_iter = client.get_object(
            account, container, obj, req_headers, params=params)
        self.assertEqual(status_int // 100, 2)
        for k, v in headers.items():
            self.assertEqual(v, resp_headers[k])
        self.assertEqual(b''.join(obj_iter), body)
        self.assertEqual(resp_headers['content-length'], str(len(body)))
        self.assertEqual(app.call_count, 1)
        req_headers.update({
            'host': 'localhost:80',  # from swob.Request.blank
            'user-agent': 'test',  # from IC
            'x-backend-allow-reserved-names': 'true',  # also from IC
        })
        self.assertEqual(app.calls_with_headers, [(
            'GET', path_info + '?symlink=get', HeaderKeyDict(req_headers))])

    def test_iter_object_lines(self):
        class FakeApp(FakeSwift):
            def __init__(self, lines):
                super(FakeApp, self).__init__()
                self.lines = lines

            def __call__(self, env, start_response):
                start_response('200 Ok', [('Content-Length', '0')])
                return [b'%s\n' % x for x in self.lines]

        lines = b'line1 line2 line3'.split()
        client = internal_client.InternalClient(
            None, 'some_agent', 3, use_replication_network=False,
            app=FakeApp(lines))
        ret_lines = []
        for line in client.iter_object_lines('account', 'container', 'object'):
            ret_lines.append(line)
        self.assertEqual(lines, ret_lines)
        self.assertEqual(client.app._pipeline_final_app.backend_user_agent,
                         'some_agent')

    def test_iter_object_lines_compressed_object(self):
        class FakeApp(FakeSwift):
            def __init__(self, lines):
                super(FakeApp, self).__init__()
                self.lines = lines

            def __call__(self, env, start_response):
                start_response('200 Ok', [('Content-Length', '0')])
                return internal_client.CompressingFileReader(
                    BytesIO(b'\n'.join(self.lines)))

        lines = b'line1 line2 line3'.split()
        client = internal_client.InternalClient(
            None, 'some_agent', 3, use_replication_network=False,
            app=FakeApp(lines))
        ret_lines = []
        for line in client.iter_object_lines(
                'account', 'container', 'object.gz'):
            ret_lines.append(line)
        self.assertEqual(lines, ret_lines)

    def test_iter_object_lines_404(self):
        class FakeApp(FakeSwift):
            def __call__(self, env, start_response):
                start_response('404 Not Found', [])
                return [b'one\ntwo\nthree']

        client = internal_client.InternalClient(
            None, 'some_agent', 3, use_replication_network=False,
            app=FakeApp())
        lines = []
        for line in client.iter_object_lines(
                'some_account', 'some_container', 'some_object',
                acceptable_statuses=(2, 404)):
            lines.append(line)
        self.assertEqual([], lines)

    def test_set_object_metadata(self):
        account, container, obj = path_parts()
        path = make_path_info(account, container, obj)
        client, app = get_client_app()
        app.register('POST', path, swob.HTTPAccepted, {})
        client.set_object_metadata(account, container, obj, {'Color': 'Blue'},
                                   metadata_prefix='X-Object-Meta-')
        self.assertEqual([('POST', path, {
            'X-Backend-Allow-Reserved-Names': 'true',
            'Host': 'localhost:80',
            'X-Object-Meta-Color': 'Blue',
            'User-Agent': 'test'
        })], app.calls_with_headers)
        self.assertEqual({}, app.unread_requests)
        self.assertEqual({}, app.unclosed_requests)
        self.assertEqual(app.backend_user_agent, 'test')

    def test_set_object_metadata_plumbing(self):
        account, container, obj = path_parts()
        path = make_path(account, container, obj)
        metadata = 'some_metadata'
        metadata_prefix = 'some_metadata_prefix'
        acceptable_statuses = 'some_status_list'
        client = SetMetadataInternalClient(
            self, path, metadata, metadata_prefix, acceptable_statuses)
        client.set_object_metadata(
            account, container, obj, metadata, metadata_prefix,
            acceptable_statuses)
        self.assertEqual(1, client.set_metadata_called)

    def test_upload_object(self):
        account, container, obj = path_parts()
        path = make_path_info(account, container, obj)
        client, app = get_client_app()
        app.register('PUT', path, swob.HTTPCreated, {})
        client.upload_object(BytesIO(b'fobj'), account, container, obj)
        self.assertEqual([('PUT', path, {
            'Transfer-Encoding': 'chunked',
            'X-Backend-Allow-Reserved-Names': 'true',
            'Host': 'localhost:80',
            'User-Agent': 'test'
        })], app.calls_with_headers)
        self.assertEqual({}, app.unread_requests)
        self.assertEqual({}, app.unclosed_requests)
        self.assertEqual(app.backend_user_agent, 'test')

    def test_upload_object_plumbing(self):
        class InternalClient(internal_client.InternalClient):
            def __init__(self, test, path, headers, fobj):
                self.test = test
                self.use_replication_network = False
                self.path = path
                self.headers = headers
                self.fobj = fobj
                self.make_request_called = 0

            def make_request(
                    self, method, path, headers, acceptable_statuses,
                    body_file=None, params=None):
                self.make_request_called += 1
                self.test.assertEqual(self.path, path)
                exp_headers = dict(self.headers)
                exp_headers['Transfer-Encoding'] = 'chunked'
                self.test.assertEqual(exp_headers, headers)
                self.test.assertEqual(self.fobj, fobj)

        fobj = 'some_fobj'
        account, container, obj = path_parts()
        path = make_path(account, container, obj)
        headers = {'key': 'value'}

        client = InternalClient(self, path, headers, fobj)
        client.upload_object(fobj, account, container, obj, headers)
        self.assertEqual(1, client.make_request_called)

    def test_upload_object_not_chunked(self):
        class InternalClient(internal_client.InternalClient):
            def __init__(self, test, path, headers, fobj):
                self.test = test
                self.path = path
                self.headers = headers
                self.fobj = fobj
                self.make_request_called = 0

            def make_request(
                    self, method, path, headers, acceptable_statuses,
                    body_file=None, params=None):
                self.make_request_called += 1
                self.test.assertEqual(self.path, path)
                exp_headers = dict(self.headers)
                self.test.assertEqual(exp_headers, headers)
                self.test.assertEqual(self.fobj, fobj)

        fobj = 'some_fobj'
        account, container, obj = path_parts()
        path = make_path(account, container, obj)
        headers = {'key': 'value', 'Content-Length': len(fobj)}

        client = InternalClient(self, path, headers, fobj)
        client.upload_object(fobj, account, container, obj, headers)
        self.assertEqual(1, client.make_request_called)


class TestGetAuth(unittest.TestCase):
    @mock.patch.object(urllib_request, 'urlopen')
    @mock.patch.object(urllib_request, 'Request')
    def test_ok(self, request, urlopen):
        def getheader(name):
            d = {'X-Storage-Url': 'url', 'X-Auth-Token': 'token'}
            return d.get(name)
        urlopen.return_value.info.return_value.getheader = getheader

        url, token = internal_client.get_auth(
            'http://127.0.0.1', 'user', 'key')

        self.assertEqual(url, "url")
        self.assertEqual(token, "token")
        request.assert_called_with('http://127.0.0.1')
        request.return_value.add_header.assert_any_call('X-Auth-User', 'user')
        request.return_value.add_header.assert_any_call('X-Auth-Key', 'key')

    def test_invalid_version(self):
        self.assertRaises(SystemExit, internal_client.get_auth,
                          'http://127.0.0.1', 'user', 'key', auth_version=2.0)


class TestSimpleClient(unittest.TestCase):

    def _test_get_head(self, request, urlopen, method):

        mock_time_value = [1401224049.98]

        def mock_time():
            # global mock_time_value
            mock_time_value[0] += 1
            return mock_time_value[0]

        with mock.patch('swift.common.internal_client.time', mock_time):
            # basic request, only url as kwarg
            request.return_value.get_type.return_value = "http"
            urlopen.return_value.read.return_value = b''
            urlopen.return_value.getcode.return_value = 200
            urlopen.return_value.info.return_value = {'content-length': '345'}
            sc = internal_client.SimpleClient(url='http://127.0.0.1')
            logger = debug_logger('test-ic')
            retval = sc.retry_request(
                method, headers={'content-length': '123'}, logger=logger)
            self.assertEqual(urlopen.call_count, 1)
            request.assert_called_with('http://127.0.0.1?format=json',
                                       headers={'content-length': '123'},
                                       data=None)
            self.assertEqual([{'content-length': '345'}, None], retval)
            self.assertEqual(method, request.return_value.get_method())
            self.assertEqual(logger.get_lines_for_level('debug'), [
                '-> 2014-05-27T20:54:11 ' + method +
                ' http://127.0.0.1%3Fformat%3Djson 200 '
                '123 345 1401224050.98 1401224051.98 1.0 -'
            ])

            # Check if JSON is decoded
            urlopen.return_value.read.return_value = b'{}'
            retval = sc.retry_request(method)
            self.assertEqual([{'content-length': '345'}, {}], retval)

            # same as above, now with token
            sc = internal_client.SimpleClient(url='http://127.0.0.1',
                                              token='token')
            retval = sc.retry_request(method)
            request.assert_called_with('http://127.0.0.1?format=json',
                                       headers={'X-Auth-Token': 'token'},
                                       data=None)
            self.assertEqual([{'content-length': '345'}, {}], retval)

            # same as above, now with prefix
            sc = internal_client.SimpleClient(url='http://127.0.0.1',
                                              token='token')
            retval = sc.retry_request(method, prefix="pre_")
            request.assert_called_with(
                'http://127.0.0.1?format=json&prefix=pre_',
                headers={'X-Auth-Token': 'token'}, data=None)
            self.assertEqual([{'content-length': '345'}, {}], retval)

            # same as above, now with container name
            retval = sc.retry_request(method, container='cont')
            request.assert_called_with('http://127.0.0.1/cont?format=json',
                                       headers={'X-Auth-Token': 'token'},
                                       data=None)
            self.assertEqual([{'content-length': '345'}, {}], retval)

            # same as above, now with object name
            retval = sc.retry_request(method, container='cont', name='obj')
            request.assert_called_with('http://127.0.0.1/cont/obj',
                                       headers={'X-Auth-Token': 'token'},
                                       data=None)
            self.assertEqual([{'content-length': '345'}, {}], retval)

    @mock.patch.object(urllib_request, 'urlopen')
    @mock.patch.object(urllib_request, 'Request')
    def test_get(self, request, urlopen):
        self._test_get_head(request, urlopen, 'GET')

    @mock.patch.object(urllib_request, 'urlopen')
    @mock.patch.object(urllib_request, 'Request')
    def test_head(self, request, urlopen):
        self._test_get_head(request, urlopen, 'HEAD')

    @mock.patch.object(urllib_request, 'urlopen')
    @mock.patch.object(urllib_request, 'Request')
    def test_get_with_retries_all_failed(self, request, urlopen):
        # Simulate a failing request, ensure retries done
        request.return_value.get_type.return_value = "http"
        urlopen.side_effect = urllib_request.URLError('')
        sc = internal_client.SimpleClient(url='http://127.0.0.1', retries=1)
        with mock.patch('swift.common.internal_client.sleep') as mock_sleep:
            self.assertRaises(urllib_request.URLError, sc.retry_request, 'GET')
        self.assertEqual(mock_sleep.call_count, 1)
        self.assertEqual(request.call_count, 2)
        self.assertEqual(urlopen.call_count, 2)

    @mock.patch.object(urllib_request, 'urlopen')
    @mock.patch.object(urllib_request, 'Request')
    def test_get_with_retries(self, request, urlopen):
        # First request fails, retry successful
        request.return_value.get_type.return_value = "http"
        mock_resp = mock.MagicMock()
        mock_resp.read.return_value = b''
        mock_resp.info.return_value = {}
        urlopen.side_effect = [urllib_request.URLError(''), mock_resp]
        sc = internal_client.SimpleClient(url='http://127.0.0.1', retries=1,
                                          token='token')

        with mock.patch('swift.common.internal_client.sleep') as mock_sleep:
            retval = sc.retry_request('GET')
        self.assertEqual(mock_sleep.call_count, 1)
        self.assertEqual(request.call_count, 2)
        self.assertEqual(urlopen.call_count, 2)
        request.assert_called_with('http://127.0.0.1?format=json', data=None,
                                   headers={'X-Auth-Token': 'token'})
        self.assertEqual([{}, None], retval)
        self.assertEqual(sc.attempts, 2)

    @mock.patch.object(urllib_request, 'urlopen')
    def test_get_with_retries_param(self, mock_urlopen):
        mock_response = mock.MagicMock()
        mock_response.read.return_value = b''
        mock_response.info.return_value = {}
        mock_urlopen.side_effect = http_client.BadStatusLine('')
        c = internal_client.SimpleClient(url='http://127.0.0.1', token='token')
        self.assertEqual(c.retries, 5)

        # first without retries param
        with mock.patch('swift.common.internal_client.sleep') as mock_sleep:
            self.assertRaises(http_client.BadStatusLine,
                              c.retry_request, 'GET')
        self.assertEqual(mock_sleep.call_count, 5)
        self.assertEqual(mock_urlopen.call_count, 6)
        # then with retries param
        mock_urlopen.reset_mock()
        with mock.patch('swift.common.internal_client.sleep') as mock_sleep:
            self.assertRaises(http_client.BadStatusLine,
                              c.retry_request, 'GET', retries=2)
        self.assertEqual(mock_sleep.call_count, 2)
        self.assertEqual(mock_urlopen.call_count, 3)
        # and this time with a real response
        mock_urlopen.reset_mock()
        mock_urlopen.side_effect = [http_client.BadStatusLine(''),
                                    mock_response]
        with mock.patch('swift.common.internal_client.sleep') as mock_sleep:
            retval = c.retry_request('GET', retries=1)
        self.assertEqual(mock_sleep.call_count, 1)
        self.assertEqual(mock_urlopen.call_count, 2)
        self.assertEqual([{}, None], retval)

    @mock.patch.object(urllib_request, 'urlopen')
    def test_request_with_retries_with_HTTPError(self, mock_urlopen):
        mock_response = mock.MagicMock()
        mock_response.read.return_value = b''
        c = internal_client.SimpleClient(url='http://127.0.0.1', token='token')
        self.assertEqual(c.retries, 5)

        for request_method in 'GET PUT POST DELETE HEAD COPY'.split():
            mock_urlopen.reset_mock()
            mock_urlopen.side_effect = urllib_request.HTTPError(*[None] * 5)
            with mock.patch('swift.common.internal_client.sleep') \
                    as mock_sleep:
                self.assertRaises(exceptions.ClientException,
                                  c.retry_request, request_method, retries=1)
            self.assertEqual(mock_sleep.call_count, 1)
            self.assertEqual(mock_urlopen.call_count, 2)

    @mock.patch.object(urllib_request, 'urlopen')
    def test_request_container_with_retries_with_HTTPError(self,
                                                           mock_urlopen):
        mock_response = mock.MagicMock()
        mock_response.read.return_value = b''
        c = internal_client.SimpleClient(url='http://127.0.0.1', token='token')
        self.assertEqual(c.retries, 5)

        for request_method in 'GET PUT POST DELETE HEAD COPY'.split():
            mock_urlopen.reset_mock()
            mock_urlopen.side_effect = urllib_request.HTTPError(*[None] * 5)
            with mock.patch('swift.common.internal_client.sleep') \
                    as mock_sleep:
                self.assertRaises(exceptions.ClientException,
                                  c.retry_request, request_method,
                                  container='con', retries=1)
            self.assertEqual(mock_sleep.call_count, 1)
            self.assertEqual(mock_urlopen.call_count, 2)

    @mock.patch.object(urllib_request, 'urlopen')
    def test_request_object_with_retries_with_HTTPError(self,
                                                        mock_urlopen):
        mock_response = mock.MagicMock()
        mock_response.read.return_value = b''
        c = internal_client.SimpleClient(url='http://127.0.0.1', token='token')
        self.assertEqual(c.retries, 5)

        for request_method in 'GET PUT POST DELETE HEAD COPY'.split():
            mock_urlopen.reset_mock()
            mock_urlopen.side_effect = urllib_request.HTTPError(*[None] * 5)
            with mock.patch('swift.common.internal_client.sleep') \
                    as mock_sleep:
                self.assertRaises(exceptions.ClientException,
                                  c.retry_request, request_method,
                                  container='con', name='obj', retries=1)
            self.assertEqual(mock_sleep.call_count, 1)
            self.assertEqual(mock_urlopen.call_count, 2)

    @mock.patch.object(urllib_request, 'urlopen')
    def test_delete_object_with_404_no_retry(self, mock_urlopen):
        mock_response = mock.MagicMock()
        mock_response.read.return_value = b''
        err_args = [None, 404, None, None, None]
        mock_urlopen.side_effect = urllib_request.HTTPError(*err_args)

        with mock.patch('swift.common.internal_client.sleep') as mock_sleep, \
                self.assertRaises(exceptions.ClientException) as caught:
            internal_client.delete_object('http://127.0.0.1',
                                          container='con', name='obj')
        self.assertEqual(caught.exception.http_status, 404)
        self.assertEqual(mock_sleep.call_count, 0)
        self.assertEqual(mock_urlopen.call_count, 1)

    @mock.patch.object(urllib_request, 'urlopen')
    def test_delete_object_with_409_no_retry(self, mock_urlopen):
        mock_response = mock.MagicMock()
        mock_response.read.return_value = b''
        err_args = [None, 409, None, None, None]
        mock_urlopen.side_effect = urllib_request.HTTPError(*err_args)

        with mock.patch('swift.common.internal_client.sleep') as mock_sleep, \
                self.assertRaises(exceptions.ClientException) as caught:
            internal_client.delete_object('http://127.0.0.1',
                                          container='con', name='obj')
        self.assertEqual(caught.exception.http_status, 409)
        self.assertEqual(mock_sleep.call_count, 0)
        self.assertEqual(mock_urlopen.call_count, 1)

    def test_proxy(self):
        # check that proxy arg is passed through to the urllib Request
        scheme = 'http'
        proxy_host = '127.0.0.1:80'
        proxy = '%s://%s' % (scheme, proxy_host)
        url = 'https://127.0.0.1:1/a'

        mocked = 'swift.common.internal_client.urllib_request.urlopen'

        # module level methods
        for func in (internal_client.put_object,
                     internal_client.delete_object):
            with mock.patch(mocked) as mock_urlopen:
                mock_urlopen.return_value = FakeConn()
                func(url, container='c', name='o1', contents='', proxy=proxy,
                     timeout=0.1, retries=0)
                self.assertEqual(1, mock_urlopen.call_count)
                args, kwargs = mock_urlopen.call_args
                self.assertEqual(1, len(args))
                self.assertEqual(1, len(kwargs))
                self.assertEqual(0.1, kwargs['timeout'])
                self.assertIsInstance(args[0], urllib_request.Request)
                self.assertEqual(proxy_host, args[0].host)
                # TODO: figure out why this doesn't match `scheme`, whether
                # py2 (where it did) or py3 is messed up, whether we care,
                # and what can be done about it
                self.assertEqual('https', args[0].type)

        # class methods
        content = mock.MagicMock()
        cl = internal_client.SimpleClient(url)
        scenarios = ((cl.get_account, []),
                     (cl.get_container, ['c']),
                     (cl.put_container, ['c']),
                     (cl.put_object, ['c', 'o', content]))
        for scenario in scenarios:
            with mock.patch(mocked) as mock_urlopen:
                mock_urlopen.return_value = FakeConn()
                scenario[0](*scenario[1], proxy=proxy, timeout=0.1)
                self.assertEqual(1, mock_urlopen.call_count)
                args, kwargs = mock_urlopen.call_args
                self.assertEqual(1, len(args))
                self.assertEqual(1, len(kwargs))
                self.assertEqual(0.1, kwargs['timeout'])
                self.assertIsInstance(args[0], urllib_request.Request)
                self.assertEqual(proxy_host, args[0].host)
                # See above
                self.assertEqual('https', args[0].type)


if __name__ == '__main__':
    unittest.main()
