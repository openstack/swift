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
import mock
from StringIO import StringIO
import unittest
from urllib import quote
import zlib
from textwrap import dedent
import os

from test.unit import FakeLogger
import eventlet
from eventlet.green import urllib2
from swift.common import internal_client
from swift.common import swob
from swift.common.storage_policy import StoragePolicy

from test.unit import with_tempdir, write_fake_ring, patch_policies
from test.unit.common.middleware.helpers import FakeSwift


def not_sleep(seconds):
    pass


def unicode_string(start, length):
    return u''.join([unichr(x) for x in xrange(start, start + length)])


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
    return path.encode('utf-8')


def get_client_app():
    app = FakeSwift()
    with mock.patch('swift.common.internal_client.loadapp',
                    new=lambda *args, **kwargs: app):
        client = internal_client.InternalClient({}, 'test', 1)
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
                      headers=None):
        self.get_metadata_called += 1
        self.test.assertEquals(self.path, path)
        self.test.assertEquals(self.metadata_prefix, metadata_prefix)
        self.test.assertEquals(self.acceptable_statuses, acceptable_statuses)
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
        self.test.assertEquals(self.path, path)
        self.test.assertEquals(self.metadata_prefix, metadata_prefix)
        self.test.assertEquals(self.metadata, metadata)
        self.test.assertEquals(self.acceptable_statuses, acceptable_statuses)


class IterInternalClient(internal_client.InternalClient):
    def __init__(
            self, test, path, marker, end_marker, acceptable_statuses, items):
        self.test = test
        self.path = path
        self.marker = marker
        self.end_marker = end_marker
        self.acceptable_statuses = acceptable_statuses
        self.items = items

    def _iter_items(
            self, path, marker='', end_marker='', acceptable_statuses=None):
        self.test.assertEquals(self.path, path)
        self.test.assertEquals(self.marker, marker)
        self.test.assertEquals(self.end_marker, end_marker)
        self.test.assertEquals(self.acceptable_statuses, acceptable_statuses)
        for item in self.items:
            yield item


class TestCompressingfileReader(unittest.TestCase):
    def test_init(self):
        class CompressObj(object):
            def __init__(self, test, *args):
                self.test = test
                self.args = args

            def method(self, *args):
                self.test.assertEquals(self.args, args)
                return self

        try:
            compressobj = CompressObj(
                self, 9, zlib.DEFLATED, -zlib.MAX_WBITS, zlib.DEF_MEM_LEVEL, 0)

            old_compressobj = internal_client.compressobj
            internal_client.compressobj = compressobj.method

            f = StringIO('')

            fobj = internal_client.CompressingFileReader(f)
            self.assertEquals(f, fobj._f)
            self.assertEquals(compressobj, fobj._compressor)
            self.assertEquals(False, fobj.done)
            self.assertEquals(True, fobj.first)
            self.assertEquals(0, fobj.crc32)
            self.assertEquals(0, fobj.total_size)
        finally:
            internal_client.compressobj = old_compressobj

    def test_read(self):
        exp_data = 'abcdefghijklmnopqrstuvwxyz'
        fobj = internal_client.CompressingFileReader(
            StringIO(exp_data), chunk_size=5)

        data = ''
        d = zlib.decompressobj(16 + zlib.MAX_WBITS)
        for chunk in fobj.read():
            data += d.decompress(chunk)

        self.assertEquals(exp_data, data)

    def test_seek(self):
        exp_data = 'abcdefghijklmnopqrstuvwxyz'
        fobj = internal_client.CompressingFileReader(
            StringIO(exp_data), chunk_size=5)

        # read a couple of chunks only
        for _ in range(2):
            fobj.read()

        # read whole thing after seek and check data
        fobj.seek(0)
        data = ''
        d = zlib.decompressobj(16 + zlib.MAX_WBITS)
        for chunk in fobj.read():
            data += d.decompress(chunk)
        self.assertEquals(exp_data, data)

    def test_seek_not_implemented_exception(self):
        fobj = internal_client.CompressingFileReader(
            StringIO(''), chunk_size=5)
        self.assertRaises(NotImplementedError, fobj.seek, 10)
        self.assertRaises(NotImplementedError, fobj.seek, 0, 10)


class TestInternalClient(unittest.TestCase):

    @mock.patch('swift.common.utils.HASH_PATH_SUFFIX', new='endcap')
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
        auto_create_account_prefix = -

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
        with patch_policies([StoragePolicy(0, 'legacy', True)]):
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
            self.assertEquals(client.auto_create_account_prefix, '-')

    def test_init(self):
        class App(object):
            def __init__(self, test, conf_path):
                self.test = test
                self.conf_path = conf_path
                self.load_called = 0

            def load(self, uri, allow_modify_pipeline=True):
                self.load_called += 1
                self.test.assertEquals(conf_path, uri)
                self.test.assertFalse(allow_modify_pipeline)
                return self

        conf_path = 'some_path'
        app = App(self, conf_path)
        old_loadapp = internal_client.loadapp
        internal_client.loadapp = app.load

        user_agent = 'some_user_agent'
        request_tries = 'some_request_tries'

        try:
            client = internal_client.InternalClient(
                conf_path, user_agent, request_tries)
        finally:
            internal_client.loadapp = old_loadapp

        self.assertEquals(1, app.load_called)
        self.assertEquals(app, client.app)
        self.assertEquals(user_agent, client.user_agent)
        self.assertEquals(request_tries, client.request_tries)

    def test_make_request_sets_user_agent(self):
        class InternalClient(internal_client.InternalClient):
            def __init__(self, test):
                self.test = test
                self.app = self.fake_app
                self.user_agent = 'some_agent'
                self.request_tries = 1

            def fake_app(self, env, start_response):
                self.test.assertEquals(self.user_agent, env['HTTP_USER_AGENT'])
                start_response('200 Ok', [('Content-Length', '0')])
                return []

        client = InternalClient(self)
        client.make_request('GET', '/', {}, (200,))

    def test_make_request_retries(self):
        class InternalClient(internal_client.InternalClient):
            def __init__(self, test):
                self.test = test
                self.app = self.fake_app
                self.user_agent = 'some_agent'
                self.request_tries = 4
                self.tries = 0
                self.sleep_called = 0

            def fake_app(self, env, start_response):
                self.tries += 1
                if self.tries < self.request_tries:
                    start_response(
                        '500 Internal Server Error', [('Content-Length', '0')])
                else:
                    start_response('200 Ok', [('Content-Length', '0')])
                return []

            def sleep(self, seconds):
                self.sleep_called += 1
                self.test.assertEquals(2 ** (self.sleep_called), seconds)

        client = InternalClient(self)

        old_sleep = internal_client.sleep
        internal_client.sleep = client.sleep

        try:
            client.make_request('GET', '/', {}, (200,))
        finally:
            internal_client.sleep = old_sleep

        self.assertEquals(3, client.sleep_called)
        self.assertEquals(4, client.tries)

    def test_base_request_timeout(self):
        # verify that base_request passes timeout arg on to urlopen
        body = {"some": "content"}

        class FakeConn(object):
            def read(self):
                return json.dumps(body)

        for timeout in (0.0, 42.0, None):
            mocked_func = 'swift.common.internal_client.urllib2.urlopen'
            with mock.patch(mocked_func) as mock_urlopen:
                mock_urlopen.side_effect = [FakeConn()]
                sc = internal_client.SimpleClient('http://0.0.0.0/')
                _, resp_body = sc.base_request('GET', timeout=timeout)
                mock_urlopen.assert_called_once_with(mock.ANY, timeout=timeout)
                # sanity check
                self.assertEquals(body, resp_body)

    def test_make_request_method_path_headers(self):
        class InternalClient(internal_client.InternalClient):
            def __init__(self):
                self.app = self.fake_app
                self.user_agent = 'some_agent'
                self.request_tries = 3
                self.env = None

            def fake_app(self, env, start_response):
                self.env = env
                start_response('200 Ok', [('Content-Length', '0')])
                return []

        client = InternalClient()

        for method in 'GET PUT HEAD'.split():
            client.make_request(method, '/', {}, (200,))
            self.assertEquals(client.env['REQUEST_METHOD'], method)

        for path in '/one /two/three'.split():
            client.make_request('GET', path, {'X-Test': path}, (200,))
            self.assertEquals(client.env['PATH_INFO'], path)
            self.assertEquals(client.env['HTTP_X_TEST'], path)

    def test_make_request_codes(self):
        class InternalClient(internal_client.InternalClient):
            def __init__(self):
                self.app = self.fake_app
                self.user_agent = 'some_agent'
                self.request_tries = 3

            def fake_app(self, env, start_response):
                start_response('200 Ok', [('Content-Length', '0')])
                return []

        client = InternalClient()

        try:
            old_sleep = internal_client.sleep
            internal_client.sleep = not_sleep

            client.make_request('GET', '/', {}, (200,))
            client.make_request('GET', '/', {}, (2,))
            client.make_request('GET', '/', {}, (400, 200))
            client.make_request('GET', '/', {}, (400, 2))

            try:
                client.make_request('GET', '/', {}, (400,))
            except Exception as err:
                pass
            self.assertEquals(200, err.resp.status_int)
            try:
                client.make_request('GET', '/', {}, (201,))
            except Exception as err:
                pass
            self.assertEquals(200, err.resp.status_int)
            try:
                client.make_request('GET', '/', {}, (111,))
            except Exception as err:
                self.assertTrue(str(err).startswith('Unexpected response'))
            else:
                self.fail("Expected the UnexpectedResponse")
        finally:
            internal_client.sleep = old_sleep

    def test_make_request_calls_fobj_seek_each_try(self):
        class FileObject(object):
            def __init__(self, test):
                self.test = test
                self.seek_called = 0

            def seek(self, offset, whence=0):
                self.seek_called += 1
                self.test.assertEquals(0, offset)
                self.test.assertEquals(0, whence)

        class InternalClient(internal_client.InternalClient):
            def __init__(self):
                self.app = self.fake_app
                self.user_agent = 'some_agent'
                self.request_tries = 3

            def fake_app(self, env, start_response):
                start_response('404 Not Found', [('Content-Length', '0')])
                return []

        fobj = FileObject(self)
        client = InternalClient()

        try:
            old_sleep = internal_client.sleep
            internal_client.sleep = not_sleep
            try:
                client.make_request('PUT', '/', {}, (2,), fobj)
            except Exception as err:
                pass
            self.assertEquals(404, err.resp.status_int)
        finally:
            internal_client.sleep = old_sleep

        self.assertEquals(client.request_tries, fobj.seek_called)

    def test_make_request_request_exception(self):
        class InternalClient(internal_client.InternalClient):
            def __init__(self):
                self.app = self.fake_app
                self.user_agent = 'some_agent'
                self.request_tries = 3

            def fake_app(self, env, start_response):
                raise Exception()

        client = InternalClient()
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
                    body_file=None):
                self.make_request_called += 1
                self.test.assertEquals('HEAD', method)
                self.test.assertEquals(self.path, path)
                self.test.assertEquals((2,), acceptable_statuses)
                self.test.assertEquals(None, body_file)
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
        self.assertEquals(exp_metadata, metadata)
        self.assertEquals(1, client.make_request_called)

    def test_get_metadata_invalid_status(self):
        class FakeApp(object):

            def __call__(self, environ, start_response):
                start_response('404 Not Found', [('x-foo', 'bar')])
                return ['nope']

        class InternalClient(internal_client.InternalClient):
            def __init__(self):
                self.user_agent = 'test'
                self.request_tries = 1
                self.app = FakeApp()

        client = InternalClient()
        self.assertRaises(internal_client.UnexpectedResponse,
                          client._get_metadata, 'path')
        metadata = client._get_metadata('path', metadata_prefix='x-',
                                        acceptable_statuses=(4,))
        self.assertEqual(metadata, {'foo': 'bar'})

    def test_make_path(self):
        account, container, obj = path_parts()
        path = make_path(account, container, obj)

        c = InternalClient()
        self.assertEquals(path, c.make_path(account, container, obj))

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
        responses = [Response(200, json.dumps([])), ]
        items = []
        client = InternalClient(self, responses)
        for item in client._iter_items('/'):
            items.append(item)
        self.assertEquals(exp_items, items)

        exp_items = []
        responses = []
        for i in xrange(3):
            data = [
                {'name': 'item%02d' % (2 * i)},
                {'name': 'item%02d' % (2 * i + 1)}]
            responses.append(Response(200, json.dumps(data)))
            exp_items.extend(data)
        responses.append(Response(204, ''))

        items = []
        client = InternalClient(self, responses)
        for item in client._iter_items('/'):
            items.append(item)
        self.assertEquals(exp_items, items)

    def test_iter_items_with_markers(self):
        class Response(object):
            def __init__(self, status_int, body):
                self.status_int = status_int
                self.body = body

        class InternalClient(internal_client.InternalClient):
            def __init__(self, test, paths, responses):
                self.test = test
                self.paths = paths
                self.responses = responses

            def make_request(
                    self, method, path, headers, acceptable_statuses,
                    body_file=None):
                exp_path = self.paths.pop(0)
                self.test.assertEquals(exp_path, path)
                return self.responses.pop(0)

        paths = [
            '/?format=json&marker=start&end_marker=end',
            '/?format=json&marker=one%C3%A9&end_marker=end',
            '/?format=json&marker=two&end_marker=end',
        ]

        responses = [
            Response(200, json.dumps([{'name': 'one\xc3\xa9'}, ])),
            Response(200, json.dumps([{'name': 'two'}, ])),
            Response(204, ''),
        ]

        items = []
        client = InternalClient(self, paths, responses)
        for item in client._iter_items('/', marker='start', end_marker='end'):
            items.append(item['name'].encode('utf8'))

        self.assertEquals('one\xc3\xa9 two'.split(), items)

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
                self.test.assertEquals('POST', method)
                self.test.assertEquals(self.path, path)
                self.test.assertEquals(self.exp_headers, headers)
                self.test.assertEquals((2,), acceptable_statuses)
                self.test.assertEquals(None, body_file)

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
        self.assertEquals(1, client.make_request_called)

    def test_iter_containers(self):
        account, container, obj = path_parts()
        path = make_path(account)
        items = '0 1 2'.split()
        marker = 'some_marker'
        end_marker = 'some_end_marker'
        acceptable_statuses = 'some_status_list'
        client = IterInternalClient(
            self, path, marker, end_marker, acceptable_statuses, items)
        ret_items = []
        for container in client.iter_containers(
                account, marker, end_marker,
                acceptable_statuses=acceptable_statuses):
            ret_items.append(container)
        self.assertEquals(items, ret_items)

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
                self.test.assertEquals('HEAD', method)
                self.test.assertEquals(self.path, path)
                self.test.assertEquals({}, headers)
                self.test.assertEquals((2, 404), acceptable_statuses)
                self.test.assertEquals(None, body_file)
                return self.resp

        account, container, obj = path_parts()
        path = make_path(account)
        containers, objects = 10, 100
        client = InternalClient(self, path, Response(containers, objects))
        info = client.get_account_info(account)
        self.assertEquals((containers, objects), info)

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
        self.assertEquals((0, 0), info)

    def test_get_account_metadata(self):
        account, container, obj = path_parts()
        path = make_path(account)
        acceptable_statuses = 'some_status_list'
        metadata_prefix = 'some_metadata_prefix'
        client = GetMetadataInternalClient(
            self, path, metadata_prefix, acceptable_statuses)
        metadata = client.get_account_metadata(
            account, metadata_prefix, acceptable_statuses)
        self.assertEquals(client.metadata, metadata)
        self.assertEquals(1, client.get_metadata_called)

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
        path = make_path(account)
        metadata = 'some_metadata'
        metadata_prefix = 'some_metadata_prefix'
        acceptable_statuses = 'some_status_list'
        client = SetMetadataInternalClient(
            self, path, metadata, metadata_prefix, acceptable_statuses)
        client.set_account_metadata(
            account, metadata, metadata_prefix, acceptable_statuses)
        self.assertEquals(1, client.set_metadata_called)

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
                self.test.assertEquals('HEAD', method)
                self.test.assertEquals(self.path, path)
                self.test.assertEquals({}, headers)
                self.test.assertEquals((2, 404), acceptable_statuses)
                self.test.assertEquals(None, body_file)
                return self.resp

        account, container, obj = path_parts()
        path = make_path(account, container)

        client = InternalClient(self, path, Response(200))
        self.assertEquals(True, client.container_exists(account, container))
        self.assertEquals(1, client.make_request_called)

        client = InternalClient(self, path, Response(404))
        self.assertEquals(False, client.container_exists(account, container))
        self.assertEquals(1, client.make_request_called)

    def test_create_container(self):
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
                self.test.assertEquals('PUT', method)
                self.test.assertEquals(self.path, path)
                self.test.assertEquals(self.headers, headers)
                self.test.assertEquals((2,), acceptable_statuses)
                self.test.assertEquals(None, body_file)

        account, container, obj = path_parts()
        path = make_path(account, container)
        headers = 'some_headers'
        client = InternalClient(self, path, headers)
        client.create_container(account, container, headers)
        self.assertEquals(1, client.make_request_called)

    def test_delete_container(self):
        class InternalClient(internal_client.InternalClient):
            def __init__(self, test, path):
                self.test = test
                self.path = path
                self.make_request_called = 0

            def make_request(
                    self, method, path, headers, acceptable_statuses,
                    body_file=None):
                self.make_request_called += 1
                self.test.assertEquals('DELETE', method)
                self.test.assertEquals(self.path, path)
                self.test.assertEquals({}, headers)
                self.test.assertEquals((2, 404), acceptable_statuses)
                self.test.assertEquals(None, body_file)

        account, container, obj = path_parts()
        path = make_path(account, container)
        client = InternalClient(self, path)
        client.delete_container(account, container)
        self.assertEquals(1, client.make_request_called)

    def test_get_container_metadata(self):
        account, container, obj = path_parts()
        path = make_path(account, container)
        metadata_prefix = 'some_metadata_prefix'
        acceptable_statuses = 'some_status_list'
        client = GetMetadataInternalClient(
            self, path, metadata_prefix, acceptable_statuses)
        metadata = client.get_container_metadata(
            account, container, metadata_prefix, acceptable_statuses)
        self.assertEquals(client.metadata, metadata)
        self.assertEquals(1, client.get_metadata_called)

    def test_iter_objects(self):
        account, container, obj = path_parts()
        path = make_path(account, container)
        marker = 'some_maker'
        end_marker = 'some_end_marker'
        acceptable_statuses = 'some_status_list'
        items = '0 1 2'.split()
        client = IterInternalClient(
            self, path, marker, end_marker, acceptable_statuses, items)
        ret_items = []
        for obj in client.iter_objects(
                account, container, marker, end_marker, acceptable_statuses):
            ret_items.append(obj)
        self.assertEquals(items, ret_items)

    def test_set_container_metadata(self):
        account, container, obj = path_parts()
        path = make_path(account, container)
        metadata = 'some_metadata'
        metadata_prefix = 'some_metadata_prefix'
        acceptable_statuses = 'some_status_list'
        client = SetMetadataInternalClient(
            self, path, metadata, metadata_prefix, acceptable_statuses)
        client.set_container_metadata(
            account, container, metadata, metadata_prefix, acceptable_statuses)
        self.assertEquals(1, client.set_metadata_called)

    def test_delete_object(self):
        class InternalClient(internal_client.InternalClient):
            def __init__(self, test, path):
                self.test = test
                self.path = path
                self.make_request_called = 0

            def make_request(
                    self, method, path, headers, acceptable_statuses,
                    body_file=None):
                self.make_request_called += 1
                self.test.assertEquals('DELETE', method)
                self.test.assertEquals(self.path, path)
                self.test.assertEquals({}, headers)
                self.test.assertEquals((2, 404), acceptable_statuses)
                self.test.assertEquals(None, body_file)

        account, container, obj = path_parts()
        path = make_path(account, container, obj)

        client = InternalClient(self, path)
        client.delete_object(account, container, obj)
        self.assertEquals(1, client.make_request_called)

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
        self.assertEquals(client.metadata, metadata)
        self.assertEquals(1, client.get_metadata_called)

    def test_get_metadata_extra_headers(self):
        class InternalClient(internal_client.InternalClient):
            def __init__(self):
                self.app = self.fake_app
                self.user_agent = 'some_agent'
                self.request_tries = 3

            def fake_app(self, env, start_response):
                self.req_env = env
                start_response('200 Ok', [('Content-Length', '0')])
                return []

        client = InternalClient()
        headers = {'X-Foo': 'bar'}
        client.get_object_metadata('account', 'container', 'obj',
                                   headers=headers)
        self.assertEqual(client.req_env['HTTP_X_FOO'], 'bar')

    def test_get_object(self):
        account, container, obj = path_parts()
        path_info = make_path_info(account, container, obj)
        client, app = get_client_app()
        headers = {'foo': 'bar'}
        body = 'some_object_body'
        app.register('GET', path_info, swob.HTTPOk, headers, body)
        req_headers = {'x-important-header': 'some_important_value'}
        status_int, resp_headers, obj_iter = client.get_object(
            account, container, obj, req_headers)
        self.assertEqual(status_int // 100, 2)
        for k, v in headers.items():
            self.assertEqual(v, resp_headers[k])
        self.assertEqual(''.join(obj_iter), body)
        self.assertEqual(resp_headers['content-length'], str(len(body)))
        self.assertEqual(app.call_count, 1)
        req_headers.update({
            'host': 'localhost:80',  # from swob.Request.blank
            'user-agent': 'test',   # from InternalClient.make_request
        })
        self.assertEqual(app.calls_with_headers, [(
            'GET', path_info, swob.HeaderKeyDict(req_headers))])

    def test_iter_object_lines(self):
        class InternalClient(internal_client.InternalClient):
            def __init__(self, lines):
                self.lines = lines
                self.app = self.fake_app
                self.user_agent = 'some_agent'
                self.request_tries = 3

            def fake_app(self, env, start_response):
                start_response('200 Ok', [('Content-Length', '0')])
                return ['%s\n' % x for x in self.lines]

        lines = 'line1 line2 line3'.split()
        client = InternalClient(lines)
        ret_lines = []
        for line in client.iter_object_lines('account', 'container', 'object'):
            ret_lines.append(line)
        self.assertEquals(lines, ret_lines)

    def test_iter_object_lines_compressed_object(self):
        class InternalClient(internal_client.InternalClient):
            def __init__(self, lines):
                self.lines = lines
                self.app = self.fake_app
                self.user_agent = 'some_agent'
                self.request_tries = 3

            def fake_app(self, env, start_response):
                start_response('200 Ok', [('Content-Length', '0')])
                return internal_client.CompressingFileReader(
                    StringIO('\n'.join(self.lines)))

        lines = 'line1 line2 line3'.split()
        client = InternalClient(lines)
        ret_lines = []
        for line in client.iter_object_lines(
                'account', 'container', 'object.gz'):
            ret_lines.append(line)
        self.assertEquals(lines, ret_lines)

    def test_iter_object_lines_404(self):
        class InternalClient(internal_client.InternalClient):
            def __init__(self):
                self.app = self.fake_app
                self.user_agent = 'some_agent'
                self.request_tries = 3

            def fake_app(self, env, start_response):
                start_response('404 Not Found', [])
                return ['one\ntwo\nthree']

        client = InternalClient()
        lines = []
        for line in client.iter_object_lines(
                'some_account', 'some_container', 'some_object',
                acceptable_statuses=(2, 404)):
            lines.append(line)
        self.assertEquals([], lines)

    def test_set_object_metadata(self):
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
        self.assertEquals(1, client.set_metadata_called)

    def test_upload_object(self):
        class InternalClient(internal_client.InternalClient):
            def __init__(self, test, path, headers, fobj):
                self.test = test
                self.path = path
                self.headers = headers
                self.fobj = fobj
                self.make_request_called = 0

            def make_request(
                    self, method, path, headers, acceptable_statuses,
                    body_file=None):
                self.make_request_called += 1
                self.test.assertEquals(self.path, path)
                exp_headers = dict(self.headers)
                exp_headers['Transfer-Encoding'] = 'chunked'
                self.test.assertEquals(exp_headers, headers)
                self.test.assertEquals(self.fobj, fobj)

        fobj = 'some_fobj'
        account, container, obj = path_parts()
        path = make_path(account, container, obj)
        headers = {'key': 'value'}

        client = InternalClient(self, path, headers, fobj)
        client.upload_object(fobj, account, container, obj, headers)
        self.assertEquals(1, client.make_request_called)

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
                    body_file=None):
                self.make_request_called += 1
                self.test.assertEquals(self.path, path)
                exp_headers = dict(self.headers)
                self.test.assertEquals(exp_headers, headers)
                self.test.assertEquals(self.fobj, fobj)

        fobj = 'some_fobj'
        account, container, obj = path_parts()
        path = make_path(account, container, obj)
        headers = {'key': 'value', 'Content-Length': len(fobj)}

        client = InternalClient(self, path, headers, fobj)
        client.upload_object(fobj, account, container, obj, headers)
        self.assertEquals(1, client.make_request_called)


class TestGetAuth(unittest.TestCase):
    @mock.patch('eventlet.green.urllib2.urlopen')
    @mock.patch('eventlet.green.urllib2.Request')
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


mock_time_value = 1401224049.98


def mock_time():
    global mock_time_value
    mock_time_value += 1
    return mock_time_value


class TestSimpleClient(unittest.TestCase):

    @mock.patch('eventlet.green.urllib2.urlopen')
    @mock.patch('eventlet.green.urllib2.Request')
    @mock.patch('swift.common.internal_client.time', mock_time)
    def test_get(self, request, urlopen):
        # basic GET request, only url as kwarg
        request.return_value.get_type.return_value = "http"
        urlopen.return_value.read.return_value = ''
        urlopen.return_value.getcode.return_value = 200
        urlopen.return_value.info.return_value = {'content-length': '345'}
        sc = internal_client.SimpleClient(url='http://127.0.0.1')
        logger = FakeLogger()
        retval = sc.retry_request(
            'GET', headers={'content-length': '123'}, logger=logger)
        self.assertEqual(urlopen.call_count, 1)
        request.assert_called_with('http://127.0.0.1?format=json',
                                   headers={'content-length': '123'},
                                   data=None)
        self.assertEqual([None, None], retval)
        self.assertEqual('GET', request.return_value.get_method())
        self.assertEqual(logger.log_dict['debug'], [(
            ('-> 2014-05-27T20:54:11 GET http://127.0.0.1%3Fformat%3Djson 200 '
             '123 345 1401224050.98 1401224051.98 1.0 -',), {})])

        # Check if JSON is decoded
        urlopen.return_value.read.return_value = '{}'
        retval = sc.retry_request('GET')
        self.assertEqual([None, {}], retval)

        # same as above, now with token
        sc = internal_client.SimpleClient(url='http://127.0.0.1',
                                          token='token')
        retval = sc.retry_request('GET')
        request.assert_called_with('http://127.0.0.1?format=json',
                                   headers={'X-Auth-Token': 'token'},
                                   data=None)
        self.assertEqual([None, {}], retval)

        # same as above, now with prefix
        sc = internal_client.SimpleClient(url='http://127.0.0.1',
                                          token='token')
        retval = sc.retry_request('GET', prefix="pre_")
        request.assert_called_with('http://127.0.0.1?format=json&prefix=pre_',
                                   headers={'X-Auth-Token': 'token'},
                                   data=None)
        self.assertEqual([None, {}], retval)

        # same as above, now with container name
        retval = sc.retry_request('GET', container='cont')
        request.assert_called_with('http://127.0.0.1/cont?format=json',
                                   headers={'X-Auth-Token': 'token'},
                                   data=None)
        self.assertEqual([None, {}], retval)

        # same as above, now with object name
        retval = sc.retry_request('GET', container='cont', name='obj')
        request.assert_called_with('http://127.0.0.1/cont/obj',
                                   headers={'X-Auth-Token': 'token'},
                                   data=None)
        self.assertEqual([None, {}], retval)

    @mock.patch('eventlet.green.urllib2.urlopen')
    @mock.patch('eventlet.green.urllib2.Request')
    def test_get_with_retries_all_failed(self, request, urlopen):
        # Simulate a failing request, ensure retries done
        request.return_value.get_type.return_value = "http"
        urlopen.side_effect = urllib2.URLError('')
        sc = internal_client.SimpleClient(url='http://127.0.0.1', retries=1)
        with mock.patch('swift.common.internal_client.sleep') as mock_sleep:
            self.assertRaises(urllib2.URLError, sc.retry_request, 'GET')
        self.assertEqual(mock_sleep.call_count, 1)
        self.assertEqual(request.call_count, 2)
        self.assertEqual(urlopen.call_count, 2)

    @mock.patch('eventlet.green.urllib2.urlopen')
    @mock.patch('eventlet.green.urllib2.Request')
    def test_get_with_retries(self, request, urlopen):
        # First request fails, retry successful
        request.return_value.get_type.return_value = "http"
        mock_resp = mock.MagicMock()
        mock_resp.read.return_value = ''
        urlopen.side_effect = [urllib2.URLError(''), mock_resp]
        sc = internal_client.SimpleClient(url='http://127.0.0.1', retries=1,
                                          token='token')

        with mock.patch('swift.common.internal_client.sleep') as mock_sleep:
            retval = sc.retry_request('GET')
        self.assertEqual(mock_sleep.call_count, 1)
        self.assertEqual(request.call_count, 2)
        self.assertEqual(urlopen.call_count, 2)
        request.assert_called_with('http://127.0.0.1?format=json', data=None,
                                   headers={'X-Auth-Token': 'token'})
        self.assertEqual([None, None], retval)
        self.assertEqual(sc.attempts, 2)

    @mock.patch('eventlet.green.urllib2.urlopen')
    def test_get_with_retries_param(self, mock_urlopen):
        mock_response = mock.MagicMock()
        mock_response.read.return_value = ''
        mock_urlopen.side_effect = internal_client.httplib.BadStatusLine('')
        c = internal_client.SimpleClient(url='http://127.0.0.1', token='token')
        self.assertEqual(c.retries, 5)

        # first without retries param
        with mock.patch('swift.common.internal_client.sleep') as mock_sleep:
            self.assertRaises(internal_client.httplib.BadStatusLine,
                              c.retry_request, 'GET')
        self.assertEqual(mock_sleep.call_count, 5)
        self.assertEqual(mock_urlopen.call_count, 6)
        # then with retries param
        mock_urlopen.reset_mock()
        with mock.patch('swift.common.internal_client.sleep') as mock_sleep:
            self.assertRaises(internal_client.httplib.BadStatusLine,
                              c.retry_request, 'GET', retries=2)
        self.assertEqual(mock_sleep.call_count, 2)
        self.assertEqual(mock_urlopen.call_count, 3)
        # and this time with a real response
        mock_urlopen.reset_mock()
        mock_urlopen.side_effect = [internal_client.httplib.BadStatusLine(''),
                                    mock_response]
        with mock.patch('swift.common.internal_client.sleep') as mock_sleep:
            retval = c.retry_request('GET', retries=1)
        self.assertEqual(mock_sleep.call_count, 1)
        self.assertEqual(mock_urlopen.call_count, 2)
        self.assertEqual([None, None], retval)

    def test_proxy(self):
        running = True

        def handle(sock):
            while running:
                try:
                    with eventlet.Timeout(0.1):
                        (conn, addr) = sock.accept()
                except eventlet.Timeout:
                    continue
                else:
                    conn.send('HTTP/1.1 503 Server Error')
                    conn.close()
            sock.close()

        sock = eventlet.listen(('', 0))
        port = sock.getsockname()[1]
        proxy = 'http://127.0.0.1:%s' % port
        url = 'https://127.0.0.1:1/a'
        server = eventlet.spawn(handle, sock)
        try:
            headers = {'Content-Length': '0'}
            with mock.patch('swift.common.internal_client.sleep'):
                try:
                    internal_client.put_object(
                        url, container='c', name='o1', headers=headers,
                        contents='', proxy=proxy, timeout=0.1, retries=0)
                except urllib2.HTTPError as e:
                    self.assertEqual(e.code, 503)
                except urllib2.URLError as e:
                    if 'ECONNREFUSED' in str(e):
                        self.fail(
                            "Got %s which probably means the http proxy "
                            "settings were not used" % e)
                    else:
                        raise e
                else:
                    self.fail('Unexpected successful response')
        finally:
            running = False
        server.wait()


if __name__ == '__main__':
    unittest.main()
