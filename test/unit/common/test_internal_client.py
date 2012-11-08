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

import json
from StringIO import StringIO
import unittest
from urllib import quote
import zlib

from swift.common import internal_client


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

    def _get_metadata(self, path, metadata_prefix, acceptable_statuses=None):
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
        fobj = internal_client.CompressingFileReader(StringIO(exp_data))

        data = ''
        d = zlib.decompressobj(16 + zlib.MAX_WBITS)
        for chunk in fobj.read():
            data += d.decompress(chunk)

        self.assertEquals(exp_data, data)


class TestInternalClient(unittest.TestCase):
    def test_init(self):
        class App(object):
            def __init__(self, test, conf_path):
                self.test = test
                self.conf_path = conf_path
                self.load_called = 0

            def load(self, uri):
                self.load_called += 1
                self.test.assertEquals('config:' + conf_path, uri)
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
            except Exception, err:
                exc = err
            self.assertEquals(200, err.resp.status_int)
            try:
                client.make_request('GET', '/', {}, (201,))
            except Exception, err:
                exc = err
            self.assertEquals(200, err.resp.status_int)
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
            except Exception, err:
                exc = err
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
            '%stwo' % (metadata_prefix): '2',
            '%sthree' % (metadata_prefix): '3',
            'some_header-four': '4',
            'some_header-five': '5',
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
        class Response(object):
            def __init__(self):
                self.status_int = 404
                self.headers = {'some_key': 'some_value'}

        class InternalClient(internal_client.InternalClient):
            def __init__(self):
                pass

            def make_request(self, *a, **kw):
                return Response()

        client = InternalClient()
        metadata = client._get_metadata('path')
        self.assertEquals({}, metadata)

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

if __name__ == '__main__':
    unittest.main()
