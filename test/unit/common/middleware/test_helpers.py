# -*- coding: utf-8 -*-
# Copyright (c) 2023 OpenStack Foundation
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
import unittest

from swift.common.storage_policy import POLICIES
from swift.common.swob import Request, HTTPOk, HTTPNotFound, \
    HTTPCreated, HeaderKeyDict
from swift.common import request_helpers as rh
from swift.common.middleware.s3api.utils import sysmeta_header
from test.unit.common.middleware.helpers import FakeSwift


class TestFakeSwift(unittest.TestCase):
    def test_allowed_methods(self):

        def do_test(swift, method, exp_status):
            path = '/v1/a/c/o'
            swift.register(method, path, HTTPOk, {}, None)
            req = Request.blank(path)
            req.method = method
            self.assertEqual(exp_status, req.get_response(swift).status_int)

        for method in ('PUT', 'POST', 'DELETE', 'GET', 'HEAD', 'OPTIONS',
                       'REPLICATE', 'SSYNC', 'UPDATE'):
            do_test(FakeSwift(), method, 200)

        do_test(FakeSwift(), 'TEST', 405)
        do_test(FakeSwift(), 'get', 405)

    def test_not_registered(self):
        swift = FakeSwift()

        def do_test(method):
            req = Request.blank('/v1/a/c/o')
            req.method = method
            with self.assertRaises(KeyError):
                req.get_response(swift)

        do_test('GET')
        do_test('HEAD')
        do_test('POST')
        do_test('PUT')
        do_test('DELETE')

    def test_GET_registered(self):
        # verify that a single registered GET response is sufficient to handle
        # GETs and HEADS, with and without query strings
        swift = FakeSwift()
        swift.register('GET', '/v1/a/c/o', HTTPOk, {'X-Foo': 'Bar'}, b'stuff')

        req = Request.blank('/v1/a/c/o')
        req.method = 'GET'
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual({'Content-Length': '5',
                          'Content-Type': 'text/html; charset=UTF-8',
                          'X-Foo': 'Bar'},
                         resp.headers)
        self.assertEqual(b'stuff', resp.body)
        self.assertEqual(1, swift.call_count)
        self.assertEqual(('GET', '/v1/a/c/o'), swift.calls[-1])

        req = Request.blank('/v1/a/c/o')
        req.method = 'GET'
        req.query_string = 'p=q'
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual({'Content-Length': '5',
                          'Content-Type': 'text/html; charset=UTF-8',
                          'X-Foo': 'Bar'},
                         resp.headers)
        self.assertEqual(b'stuff', resp.body)
        self.assertEqual(2, swift.call_count)
        self.assertEqual(('GET', '/v1/a/c/o?p=q'), swift.calls[-1])

        req = Request.blank('/v1/a/c/o')
        req.method = 'HEAD'
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual({'Content-Type': 'text/html; charset=UTF-8',
                          'X-Foo': 'Bar'},
                         resp.headers)
        self.assertEqual(b'', resp.body)
        self.assertEqual(3, swift.call_count)
        self.assertEqual(('HEAD', '/v1/a/c/o'), swift.calls[-1])

        req = Request.blank('/v1/a/c/o')
        req.method = 'HEAD'
        req.query_string = 'p=q'
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual({'Content-Type': 'text/html; charset=UTF-8',
                          'X-Foo': 'Bar'},
                         resp.headers)
        self.assertEqual(b'', resp.body)
        self.assertEqual(4, swift.call_count)
        self.assertEqual(('HEAD', '/v1/a/c/o?p=q'), swift.calls[-1])

    def test_GET_registered_with_query_string(self):
        # verify that a single registered GET response is sufficient to handle
        # GETs and HEADS, with and without query strings
        swift = FakeSwift()
        swift.register('GET', '/v1/a/c/o?p=q', HTTPOk,
                       {'X-Foo': 'Bar'}, b'stuff')

        req = Request.blank('/v1/a/c/o')
        req.method = 'GET'
        with self.assertRaises(KeyError):
            resp = req.get_response(swift)

        req.query_string = 'p=q'
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual({'Content-Length': '5',
                          'Content-Type': 'text/html; charset=UTF-8',
                          'X-Foo': 'Bar'},
                         resp.headers)
        self.assertEqual(b'stuff', resp.body)
        self.assertEqual(1, swift.call_count)
        self.assertEqual(('GET', '/v1/a/c/o?p=q'), swift.calls[-1])

    def test_GET_and_HEAD_registered(self):
        # verify that a registered HEAD response will be preferred over GET for
        # HEAD request
        swift = FakeSwift()
        swift.register('GET', '/v1/a/c/o', HTTPOk, {'X-Foo': 'Bar'}, b'stuff')
        swift.register('HEAD', '/v1/a/c/o', HTTPNotFound, {}, b'')

        req = Request.blank('/v1/a/c/o')
        req.method = 'GET'
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual({'Content-Length': '5',
                          'Content-Type': 'text/html; charset=UTF-8',
                          'X-Foo': 'Bar'},
                         resp.headers)
        self.assertEqual(b'stuff', resp.body)
        self.assertEqual(1, swift.call_count)
        self.assertEqual(('GET', '/v1/a/c/o'), swift.calls[-1])

        req = Request.blank('/v1/a/c/o')
        req.method = 'HEAD'
        resp = req.get_response(swift)
        self.assertEqual(404, resp.status_int)
        self.assertEqual({'Content-Type': 'text/html; charset=UTF-8'},
                         resp.headers)
        self.assertEqual(b'', resp.body)
        self.assertEqual(2, swift.call_count)
        self.assertEqual(('HEAD', '/v1/a/c/o'), swift.calls[-1])

        req = Request.blank('/v1/a/c/o')
        req.method = 'HEAD'
        req.query_string = 'p=q'
        resp = req.get_response(swift)
        self.assertEqual(404, resp.status_int)
        self.assertEqual({'Content-Type': 'text/html; charset=UTF-8'},
                         resp.headers)
        self.assertEqual(b'', resp.body)
        self.assertEqual(3, swift.call_count)
        self.assertEqual(('HEAD', '/v1/a/c/o?p=q'), swift.calls[-1])

    def test_PUT_uploaded(self):
        # verify an uploaded object is sufficient to handle GETs and HEADS,
        # with and without query strings
        swift = FakeSwift()
        swift.register('PUT', '/v1/a/c/o', HTTPCreated, {}, None)
        req = Request.blank('/v1/a/c/o', body=b'stuff')
        req.method = 'PUT'
        resp = req.get_response(swift)
        self.assertEqual(201, resp.status_int)
        self.assertEqual({'Content-Length': '5',
                          'Etag': 'c13d88cb4cb02003daedb8a84e5d272a',
                          'Content-Type': 'text/html; charset=UTF-8'},
                         resp.headers)
        self.assertEqual(b'', resp.body)
        self.assertEqual(1, swift.call_count)
        self.assertEqual(('PUT', '/v1/a/c/o'), swift.calls[-1])

        req = Request.blank('/v1/a/c/o')
        req.method = 'GET'
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual({'Content-Length': '5',
                          'Content-Type': 'text/html; charset=UTF-8',
                          'Host': 'localhost:80'},
                         resp.headers)
        self.assertEqual(b'stuff', resp.body)
        self.assertEqual(2, swift.call_count)
        self.assertEqual(('GET', '/v1/a/c/o'), swift.calls[-1])

        req = Request.blank('/v1/a/c/o')
        req.method = 'GET'
        req.query_string = 'p=q'
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual({'Content-Length': '5',
                          'Content-Type': 'text/html; charset=UTF-8',
                          'Host': 'localhost:80'},
                         resp.headers)
        self.assertEqual(b'stuff', resp.body)
        self.assertEqual(3, swift.call_count)
        self.assertEqual(('GET', '/v1/a/c/o?p=q'), swift.calls[-1])

        req = Request.blank('/v1/a/c/o')
        req.method = 'HEAD'
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual({'Content-Length': '5',
                          'Content-Type': 'text/html; charset=UTF-8',
                          'Host': 'localhost:80'},
                         resp.headers)
        self.assertEqual(b'', resp.body)
        self.assertEqual(4, swift.call_count)
        self.assertEqual(('HEAD', '/v1/a/c/o'), swift.calls[-1])

        req = Request.blank('/v1/a/c/o')
        req.method = 'HEAD'
        req.query_string = 'p=q'
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual({'Content-Length': '5',
                          'Content-Type': 'text/html; charset=UTF-8',
                          'Host': 'localhost:80'},
                         resp.headers)
        self.assertEqual(b'', resp.body)
        self.assertEqual(5, swift.call_count)
        self.assertEqual(('HEAD', '/v1/a/c/o?p=q'), swift.calls[-1])

    def test_PUT_uploaded_with_query_string(self):
        # verify an uploaded object with query string is sufficient to handle
        # GETs and HEADS, with and without query strings
        swift = FakeSwift()
        swift.register('PUT', '/v1/a/c/o', HTTPCreated, {}, None)
        req = Request.blank('/v1/a/c/o', body=b'stuff')
        req.method = 'PUT'
        req.query_string = 'multipart-manifest=put'
        resp = req.get_response(swift)
        self.assertEqual(201, resp.status_int)
        self.assertEqual({'Content-Length': '5',
                          'Etag': 'c13d88cb4cb02003daedb8a84e5d272a',
                          'Content-Type': 'text/html; charset=UTF-8'},
                         resp.headers)
        self.assertEqual(b'', resp.body)
        self.assertEqual(1, swift.call_count)
        self.assertEqual(('PUT', '/v1/a/c/o?multipart-manifest=put'),
                         swift.calls[-1])
        # note: query string is not included in uploaded key
        self.assertEqual(
            {'/v1/a/c/o': ({'Host': 'localhost:80',
                            'Content-Length': '5'},
                           b'stuff')},
            swift.uploaded)

        req = Request.blank('/v1/a/c/o')
        req.method = 'GET'
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual({'Content-Length': '5',
                          'Content-Type': 'text/html; charset=UTF-8',
                          'Host': 'localhost:80'},
                         resp.headers)
        self.assertEqual(b'stuff', resp.body)
        self.assertEqual(2, swift.call_count)
        self.assertEqual(('GET', '/v1/a/c/o'), swift.calls[-1])

        req = Request.blank('/v1/a/c/o')
        req.method = 'GET'
        req.query_string = 'p=q'  # note: differs from PUT query string
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual({'Content-Length': '5',
                          'Content-Type': 'text/html; charset=UTF-8',
                          'Host': 'localhost:80'},
                         resp.headers)
        self.assertEqual(b'stuff', resp.body)
        self.assertEqual(3, swift.call_count)
        self.assertEqual(('GET', '/v1/a/c/o?p=q'), swift.calls[-1])

        req = Request.blank('/v1/a/c/o')
        req.method = 'HEAD'
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual({'Content-Length': '5',
                          'Content-Type': 'text/html; charset=UTF-8',
                          'Host': 'localhost:80'},
                         resp.headers)
        self.assertEqual(b'', resp.body)
        self.assertEqual(4, swift.call_count)
        self.assertEqual(('HEAD', '/v1/a/c/o'), swift.calls[-1])

        req = Request.blank('/v1/a/c/o')
        req.method = 'HEAD'
        req.query_string = 'p=q'
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual({'Content-Length': '5',
                          'Content-Type': 'text/html; charset=UTF-8',
                          'Host': 'localhost:80'},
                         resp.headers)
        self.assertEqual(b'', resp.body)
        self.assertEqual(5, swift.call_count)
        self.assertEqual(('HEAD', '/v1/a/c/o?p=q'), swift.calls[-1])

    def test_PUT_POST(self):
        # verify an uploaded object is updated by a POST
        swift = FakeSwift()
        swift.register('PUT', '/v1/a/c/o', HTTPCreated, {}, None)
        # Note: the POST must be registered
        swift.register('POST', '/v1/a/c/o', HTTPCreated, {}, None)
        req = Request.blank('/v1/a/c/o', body=b'stuff',
                            headers={'X-Object-Meta-Foo': 'Bar'})
        req.method = 'PUT'
        resp = req.get_response(swift)
        self.assertEqual(201, resp.status_int)
        self.assertEqual({'Content-Length': '5',
                          'Etag': 'c13d88cb4cb02003daedb8a84e5d272a',
                          'Content-Type': 'text/html; charset=UTF-8'},
                         resp.headers)
        self.assertEqual(b'', resp.body)
        self.assertEqual(1, swift.call_count)
        self.assertEqual(('PUT', '/v1/a/c/o'), swift.calls[-1])
        self.assertEqual(
            {'/v1/a/c/o': ({'Host': 'localhost:80',
                            'Content-Length': '5',
                            'X-Object-Meta-Foo': 'Bar'},
                           b'stuff')},
            swift.uploaded)

        # POST should update the uploaded object
        req = Request.blank('/v1/a/c/o', body=b'stuff',
                            headers={'X-Object-Meta-Foo': 'Baz'})
        req.method = 'POST'
        resp = req.get_response(swift)
        self.assertEqual(201, resp.status_int)
        self.assertEqual({'Content-Type': 'text/html; charset=UTF-8'},
                         resp.headers)
        self.assertEqual(b'', resp.body)
        self.assertEqual(2, swift.call_count)
        self.assertEqual(('POST', '/v1/a/c/o'), swift.calls[-1])
        self.assertEqual(
            {'/v1/a/c/o': ({'Host': 'localhost:80',
                            'Content-Length': '5',
                            'X-Object-Meta-Foo': 'Baz'},
                           b'stuff')},
            swift.uploaded)

    def test_PUT_with_query_string_POST(self):
        # verify an uploaded object with query string is updated by a POST
        swift = FakeSwift()
        swift.register('PUT', '/v1/a/c/o', HTTPCreated, {}, None)
        # Note: the POST must be registered
        swift.register('POST', '/v1/a/c/o', HTTPCreated, {}, None)
        req = Request.blank('/v1/a/c/o', body=b'stuff',
                            headers={'X-Object-Meta-Foo': 'Bar'})
        req.method = 'PUT'
        req.query_string = 'p=q'
        resp = req.get_response(swift)
        self.assertEqual(201, resp.status_int)
        self.assertEqual({'Content-Length': '5',
                          'Etag': 'c13d88cb4cb02003daedb8a84e5d272a',
                          'Content-Type': 'text/html; charset=UTF-8'},
                         resp.headers)
        self.assertEqual(b'', resp.body)
        self.assertEqual(1, swift.call_count)
        self.assertEqual(('PUT', '/v1/a/c/o?p=q'), swift.calls[-1])
        # note: query string is not included in uploaded key
        self.assertEqual(
            {'/v1/a/c/o': ({'Host': 'localhost:80',
                            'Content-Length': '5',
                            'X-Object-Meta-Foo': 'Bar'},
                           b'stuff')},
            swift.uploaded)

        # POST without query string should update the uploaded object
        req = Request.blank('/v1/a/c/o', body=b'stuff',
                            headers={'X-Object-Meta-Foo': 'Baz'})
        req.method = 'POST'
        resp = req.get_response(swift)
        self.assertEqual(201, resp.status_int)
        self.assertEqual({'Content-Type': 'text/html; charset=UTF-8'},
                         resp.headers)
        self.assertEqual(b'', resp.body)
        self.assertEqual(2, swift.call_count)
        self.assertEqual(('POST', '/v1/a/c/o'), swift.calls[-1])
        self.assertEqual(
            {'/v1/a/c/o': ({'Host': 'localhost:80',
                            'Content-Length': '5',
                            'X-Object-Meta-Foo': 'Baz'},
                           b'stuff')},
            swift.uploaded)

        # POST with different query string should update the uploaded object
        req = Request.blank('/v1/a/c/o', body=b'stuff',
                            headers={'X-Object-Meta-Foo': 'Bof'})
        req.method = 'POST'
        req.query_string = 'x=y'
        resp = req.get_response(swift)
        self.assertEqual(201, resp.status_int)
        self.assertEqual({'Content-Type': 'text/html; charset=UTF-8'},
                         resp.headers)
        self.assertEqual(b'', resp.body)
        self.assertEqual(3, swift.call_count)
        self.assertEqual(('POST', '/v1/a/c/o?x=y'), swift.calls[-1])
        self.assertEqual(
            {'/v1/a/c/o': ({'Host': 'localhost:80',
                            'Content-Length': '5',
                            'X-Object-Meta-Foo': 'Bof'},
                           b'stuff')},
            swift.uploaded)

        req = Request.blank('/v1/a/c/o')
        req.method = 'GET'
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual({'Content-Length': '5',
                          'Content-Type': 'text/html; charset=UTF-8',
                          'Host': 'localhost:80',
                          'X-Object-Meta-Foo': 'Bof'},
                         resp.headers)
        self.assertEqual(b'stuff', resp.body)
        self.assertEqual(4, swift.call_count)
        self.assertEqual(('GET', '/v1/a/c/o'), swift.calls[-1])

    def test_GET_registered_overrides_uploaded(self):
        swift = FakeSwift()
        swift.register('PUT', '/v1/a/c/o', HTTPCreated, {}, None)
        swift.register('GET', '/v1/a/c/o', HTTPOk, {}, b'not stuff')

        req = Request.blank('/v1/a/c/o', body=b'stuff',
                            headers={'X-Object-Meta-Foo': 'Bar'})
        req.method = 'PUT'
        resp = req.get_response(swift)
        self.assertEqual(201, resp.status_int)
        self.assertEqual({'Content-Length': '5',
                          'Etag': 'c13d88cb4cb02003daedb8a84e5d272a',
                          'Content-Type': 'text/html; charset=UTF-8'},
                         resp.headers)
        self.assertEqual(b'', resp.body)
        self.assertEqual(1, swift.call_count)
        self.assertEqual(('PUT', '/v1/a/c/o'), swift.calls[-1])
        self.assertEqual(
            {'/v1/a/c/o': ({'Host': 'localhost:80',
                            'Content-Length': '5',
                            'X-Object-Meta-Foo': 'Bar'},
                           b'stuff')},
            swift.uploaded)

        req = Request.blank('/v1/a/c/o')
        req.method = 'GET'
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual({'Content-Length': '9',
                          'Content-Type': 'text/html; charset=UTF-8'},
                         resp.headers)
        self.assertEqual(b'not stuff', resp.body)
        self.assertEqual(2, swift.call_count)
        self.assertEqual(('GET', '/v1/a/c/o'), swift.calls[-1])

    def test_range(self):
        swift = FakeSwift()
        swift.register('GET', '/v1/a/c/o', HTTPOk, {}, b'stuff')
        req = Request.blank('/v1/a/c/o', headers={'Range': 'bytes=0-2'})
        resp = req.get_response(swift)
        self.assertEqual(206, resp.status_int)
        self.assertEqual(b'stu', resp.body)
        self.assertEqual('bytes 0-2/5', resp.headers['Content-Range'])
        self.assertEqual('bytes=0-2', req.headers.get('Range'))
        self.assertEqual('bytes=0-2',
                         swift.calls_with_headers[-1].headers.get('Range'))

    def test_range_ignore_range_header(self):
        swift = FakeSwift()
        swift.register('GET', '/v1/a/c/o', HTTPOk, {
            # the value of the matching header doesn't matter
            'X-Object-Sysmeta-Magic': 'False'
        }, b'stuff')
        req = Request.blank('/v1/a/c/o', headers={'Range': 'bytes=0-2'})
        rh.update_ignore_range_header(req, 'X-Object-Sysmeta-Magic')
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual(b'stuff', resp.body)
        self.assertNotIn('Content-Range', resp.headers)
        self.assertEqual('bytes=0-2', req.headers.get('Range'))
        self.assertEqual('bytes=0-2',
                         swift.calls_with_headers[-1].headers.get('Range'))

    def test_range_ignore_range_header_old_swift(self):
        swift = FakeSwift()
        swift.can_ignore_range = False
        swift.register('GET', '/v1/a/c/o', HTTPOk, {
            # the value of the matching header doesn't matter
            'X-Object-Sysmeta-Magic': 'False'
        }, b'stuff')
        req = Request.blank('/v1/a/c/o', headers={'Range': 'bytes=0-2'})
        rh.update_ignore_range_header(req, 'X-Object-Sysmeta-Magic')
        resp = req.get_response(swift)
        self.assertEqual(206, resp.status_int)
        self.assertEqual(b'stu', resp.body)
        self.assertEqual('bytes 0-2/5', resp.headers['Content-Range'])
        self.assertEqual('bytes=0-2', req.headers.get('Range'))
        self.assertEqual('bytes=0-2',
                         swift.calls_with_headers[-1].headers.get('Range'))

    def test_range_ignore_range_header_ignored(self):
        swift = FakeSwift()
        # range is only ignored if registered response has matching metadata
        swift.register('GET', '/v1/a/c/o', HTTPOk, {}, b'stuff')
        req = Request.blank('/v1/a/c/o', headers={'Range': 'bytes=0-2'})
        rh.update_ignore_range_header(req, 'X-Object-Sysmeta-Magic')
        resp = req.get_response(swift)
        self.assertEqual(206, resp.status_int)
        self.assertEqual(b'stu', resp.body)
        self.assertEqual('bytes 0-2/5', resp.headers['Content-Range'])
        self.assertEqual('bytes=0-2', req.headers.get('Range'))
        self.assertEqual('bytes=0-2',
                         swift.calls_with_headers[-1].headers.get('Range'))

    def test_object_GET_updated_with_storage_policy(self):
        swift = FakeSwift()
        swift.register('GET', '/v1/a/c/o', HTTPOk, {}, body=b'stuff')
        req = Request.blank('/v1/a/c/o')
        req.method = 'GET'
        self.assertNotIn('X-Backend-Storage-Policy-Index', req.headers)
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual({'Content-Length': '5',
                          'Content-Type': 'text/html; charset=UTF-8'},
                         resp.headers)
        self.assertEqual(b'stuff', resp.body)
        self.assertEqual(1, swift.call_count)
        self.assertEqual(('GET', '/v1/a/c/o'), swift.calls[0])
        self.assertEqual(('GET', '/v1/a/c/o',
                          {'Host': 'localhost:80'}),  # from swob
                         swift.calls_with_headers[0])
        # default storage policy is applied...
        self.assertEqual(str(int(POLICIES.default)),
                         req.headers.get('X-Backend-Storage-Policy-Index'))

        # register a container with storage policy 99...
        swift.register('HEAD', '/v1/a/c', HTTPOk,
                       {'X-Backend-Storage-Policy-Index': '99'}, None)
        req = Request.blank('/v1/a/c/o')
        req.method = 'GET'
        self.assertNotIn('X-Backend-Storage-Policy-Index', req.headers)
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual({'Content-Length': '5',
                          'Content-Type': 'text/html; charset=UTF-8'},
                         resp.headers)
        self.assertEqual(b'stuff', resp.body)
        self.assertEqual(2, swift.call_count)
        self.assertEqual(('GET', '/v1/a/c/o'), swift.calls[1])
        self.assertEqual(('GET', '/v1/a/c/o',
                          {'Host': 'localhost:80'}),  # from swob
                         swift.calls_with_headers[1])
        self.assertEqual(
            '99', req.headers.get('X-Backend-Storage-Policy-Index'))


class TestFakeSwiftMultipleResponses(unittest.TestCase):

    def test_register_response_is_forever(self):
        swift = FakeSwift()
        swift.register('GET', '/v1/a/c/o',
                       HTTPOk, {'X-Foo': 'Bar'}, b'stuff')
        req = Request.blank('/v1/a/c/o')
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual('Bar', resp.headers['X-Foo'])
        # you can get this response as much as you want
        for i in range(10):
            resp = req.get_response(swift)
            self.assertEqual(200, resp.status_int)
            self.assertEqual('Bar', resp.headers['X-Foo'])

    def test_register_response_is_last_response_wins(self):
        swift = FakeSwift()
        swift.register('GET', '/v1/a/c/o',
                       HTTPOk, {'X-Foo': 'Bar'}, b'stuff')
        req = Request.blank('/v1/a/c/o')
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual('Bar', resp.headers['X-Foo'])

        swift.register('GET', '/v1/a/c/o',
                       HTTPOk, {'X-Foo': 'Baz'}, b'other')
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual('Baz', resp.headers['X-Foo'])
        # you can get this new response as much as you want
        for i in range(10):
            resp = req.get_response(swift)
            self.assertEqual(200, resp.status_int)
            self.assertEqual('Baz', resp.headers['X-Foo'])

    def test_register_next_response_is_last_response_wins(self):
        swift = FakeSwift()
        swift.register(
            'GET', '/v1/a/c/o',
            HTTPOk, {'X-Foo': 'Bar'}, b'stuff')
        swift.register_next_response(
            'GET', '/v1/a/c/o',
            HTTPOk, {'X-Foo': 'Baz'}, b'other')
        req = Request.blank('/v1/a/c/o')

        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual('Bar', resp.headers['X-Foo'])
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual('Baz', resp.headers['X-Foo'])
        # you can get this new response as much as you want
        for i in range(10):
            resp = req.get_response(swift)
            self.assertEqual(200, resp.status_int)
            self.assertEqual('Baz', resp.headers['X-Foo'])

    def test_register_next_response_keeps_current_registered_response(self):
        # we expect test authors will typically 'd register ALL their responses
        # before you start calling FakeSwift
        swift = FakeSwift()
        swift.register(
            'GET', '/v1/a/c/o',
            HTTPOk, {'X-Foo': 'Bar'}, b'stuff')
        req = Request.blank('/v1/a/c/o')

        # we get the registered response, obviously
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual('Bar', resp.headers['X-Foo'])

        # because before calling register_next_response, no resp are consumed
        swift.register_next_response(
            'GET', '/v1/a/c/o',
            HTTPOk, {'X-Foo': 'Baz'}, b'other')

        # so, this is the "current" response, not the *next* response
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual('Bar', resp.headers['X-Foo'])

        # the *next* response is the next response
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual('Baz', resp.headers['X-Foo'])

    def test_register_next_response_first(self):
        # you can just use register_next_response
        swift = FakeSwift()
        swift.register_next_response(
            'GET', '/v1/a/c/o',
            HTTPOk, {'X-Foo': 'Bar'}, b'stuff')
        swift.register_next_response(
            'GET', '/v1/a/c/o',
            HTTPOk, {'X-Foo': 'Baz'}, b'other')
        req = Request.blank('/v1/a/c/o')

        # it works just like you'd called register
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual('Bar', resp.headers['X-Foo'])
        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual('Baz', resp.headers['X-Foo'])
        # you can get this new response as much as you want
        for i in range(10):
            resp = req.get_response(swift)
            self.assertEqual(200, resp.status_int)
            self.assertEqual('Baz', resp.headers['X-Foo'])

    def test_register_resets(self):
        swift = FakeSwift()
        swift.register_next_response(
            'GET', '/v1/a/c/o',
            HTTPOk, {'X-Foo': 'Bar'}, b'stuff')
        req = Request.blank('/v1/a/c/o')

        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual('Bar', resp.headers['X-Foo'])
        # you can get this response as much as you want
        for i in range(10):
            resp = req.get_response(swift)
            self.assertEqual(200, resp.status_int)
            self.assertEqual('Bar', resp.headers['X-Foo'])

        # if you call register mid test you immediately reset the resp
        swift.register(
            'GET', '/v1/a/c/o',
            HTTPOk, {'X-Foo': 'Baz'}, b'other')

        resp = req.get_response(swift)
        self.assertEqual(200, resp.status_int)
        self.assertEqual('Baz', resp.headers['X-Foo'])
        # you can get this new response as much as you want
        for i in range(10):
            resp = req.get_response(swift)
            self.assertEqual(200, resp.status_int)
            self.assertEqual('Baz', resp.headers['X-Foo'])


class TestFakeSwiftStickyHeaders(unittest.TestCase):
    def setUp(self):
        self.swift = FakeSwift()
        self.path = '/v1/AUTH_test/bucket'

    def _check_headers(self, method, path, exp_headers):
        captured_headers = {}

        def start_response(status, resp_headers):
            self.assertEqual(status, '200 OK')
            captured_headers.update(resp_headers)

        env = {'REQUEST_METHOD': method, 'PATH_INFO': path}
        body_iter = self.swift(env, start_response)
        b''.join(body_iter)
        captured_headers.pop('Content-Type')
        self.assertEqual(exp_headers, captured_headers)

    def test_sticky_headers(self):
        sticky_headers = HeaderKeyDict({
            sysmeta_header('container', 'acl'): 'test',
            'x-container-meta-foo': 'bar',
        })
        self.swift.update_sticky_response_headers(self.path, sticky_headers)
        # register a response for this path with no headers
        self.swift.register('GET', self.path, HTTPOk, {}, None)
        self._check_headers('HEAD', self.path, sticky_headers)
        self._check_headers('GET', self.path, sticky_headers)

        # sticky headers are not applied to PUT, POST, DELETE
        self.swift.register('PUT', self.path, HTTPOk, {}, None)
        self._check_headers('PUT', self.path, {})
        self.swift.register('POST', self.path, HTTPOk, {}, None)
        self._check_headers('POST', self.path, {})
        self.swift.register('DELETE', self.path, HTTPOk, {}, None)
        self._check_headers('DELETE', self.path, {})

    def test_sticky_headers_match_path(self):
        other_path = self.path + '-other'
        sticky_headers = HeaderKeyDict({
            sysmeta_header('container', 'acl'): 'test',
            'x-container-meta-foo': 'bar',
        })
        sticky_headers_other = HeaderKeyDict({
            'x-container-meta-foo': 'other',
        })
        self.swift.update_sticky_response_headers(self.path, sticky_headers)
        self.swift.update_sticky_response_headers(other_path,
                                                  sticky_headers_other)
        self.swift.register('GET', self.path, HTTPOk, {}, None)
        self.swift.register('GET', other_path, HTTPOk, {}, None)
        self._check_headers('HEAD', self.path, sticky_headers)
        self._check_headers('GET', other_path, sticky_headers_other)

    def test_sticky_headers_update(self):
        sticky_headers = HeaderKeyDict({
            sysmeta_header('container', 'acl'): 'test',
            'x-container-meta-foo': 'bar'
        })
        exp_headers = sticky_headers.copy()
        self.swift.update_sticky_response_headers(self.path, sticky_headers)
        self.swift.register('HEAD', self.path, HTTPOk, {}, None)
        self._check_headers('HEAD', self.path, exp_headers)

        # check that FakeSwift made a *copy*
        sticky_headers['x-container-meta-foo'] = 'changed'
        self._check_headers('HEAD', self.path, exp_headers)

        # check existing are updated not replaced
        sticky_headers = HeaderKeyDict({
            sysmeta_header('container', 'acl'): 'test-modified',
            'x-container-meta-bar': 'foo'
        })
        exp_headers.update(sticky_headers)
        self.swift.update_sticky_response_headers(self.path, sticky_headers)
        self._check_headers('HEAD', self.path, exp_headers)

    def test_sticky_headers_add_to_response_headers(self):
        sticky_headers = HeaderKeyDict({
            'x-container-meta-foo': 'bar',
        })
        self.swift.update_sticky_response_headers(self.path, sticky_headers)
        # register a response with another header
        self.swift.register('HEAD', self.path, HTTPOk, {
            'x-backend-storage-policy-index': '1',
        }, None)
        self._check_headers('HEAD', self.path, HeaderKeyDict({
            'x-container-meta-foo': 'bar',
            'x-backend-storage-policy-index': '1',
        }))

    def test_sticky_headers_overwritten_by_response_header(self):
        sticky_headers = HeaderKeyDict({
            'x-container-meta-foo': 'bar',
            'x-backend-storage-policy-index': '0',
        })
        self.swift.update_sticky_response_headers(self.path, sticky_headers)
        # register a response with a different value for a sticky header
        self.swift.register('HEAD', self.path, HTTPOk, {
            'x-container-meta-foo': 'different',
        }, None)
        self._check_headers('HEAD', self.path, HeaderKeyDict({
            'x-container-meta-foo': 'different',
            'x-backend-storage-policy-index': '0',
        }))


if __name__ == '__main__':
    unittest.main()
