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

from swift.common.swob import Request, HTTPOk, HTTPNotFound, HTTPCreated
from test.unit.common.middleware.helpers import FakeSwift


class TestFakeSwift(unittest.TestCase):
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
