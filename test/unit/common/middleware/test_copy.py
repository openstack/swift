#!/usr/bin/env python
# Copyright (c) 2015 OpenStack Foundation
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

from unittest import mock
import unittest
import urllib.parse

from swift.common import swob
from swift.common.middleware import copy
from swift.common.storage_policy import POLICIES
from swift.common.swob import Request, HTTPException
from swift.common.utils import closing_if_possible, md5
from test.debug_logger import debug_logger
from test.unit import patch_policies, FakeRing
from test.unit.common.middleware.helpers import FakeSwift
from test.unit.proxy.controllers.test_obj import set_http_connect, \
    PatchedObjControllerApp


class TestCopyConstraints(unittest.TestCase):
    def test_validate_copy_from(self):
        req = Request.blank(
            '/v/a/c/o',
            headers={'x-copy-from': 'c/o2'})
        src_cont, src_obj = copy._check_copy_from_header(req)
        self.assertEqual(src_cont, 'c')
        self.assertEqual(src_obj, 'o2')
        req = Request.blank(
            '/v/a/c/o',
            headers={'x-copy-from': 'c/subdir/o2'})
        src_cont, src_obj = copy._check_copy_from_header(req)
        self.assertEqual(src_cont, 'c')
        self.assertEqual(src_obj, 'subdir/o2')
        req = Request.blank(
            '/v/a/c/o',
            headers={'x-copy-from': '/c/o2'})
        src_cont, src_obj = copy._check_copy_from_header(req)
        self.assertEqual(src_cont, 'c')
        self.assertEqual(src_obj, 'o2')

    def test_validate_bad_copy_from(self):
        req = Request.blank(
            '/v/a/c/o',
            headers={'x-copy-from': 'bad_object'})
        self.assertRaises(HTTPException,
                          copy._check_copy_from_header, req)

    def test_validate_destination(self):
        req = Request.blank(
            '/v/a/c/o',
            headers={'destination': 'c/o2'})
        src_cont, src_obj = copy._check_destination_header(req)
        self.assertEqual(src_cont, 'c')
        self.assertEqual(src_obj, 'o2')
        req = Request.blank(
            '/v/a/c/o',
            headers={'destination': 'c/subdir/o2'})
        src_cont, src_obj = copy._check_destination_header(req)
        self.assertEqual(src_cont, 'c')
        self.assertEqual(src_obj, 'subdir/o2')
        req = Request.blank(
            '/v/a/c/o',
            headers={'destination': '/c/o2'})
        src_cont, src_obj = copy._check_destination_header(req)
        self.assertEqual(src_cont, 'c')
        self.assertEqual(src_obj, 'o2')

    def test_validate_bad_destination(self):
        req = Request.blank(
            '/v/a/c/o',
            headers={'destination': 'bad_object'})
        self.assertRaises(HTTPException,
                          copy._check_destination_header, req)


class TestServerSideCopyMiddleware(unittest.TestCase):
    def setUp(self):
        self.app = FakeSwift()
        self.ssc = copy.filter_factory({})(self.app)
        self.ssc.logger = self.app.logger

    def tearDown(self):
        self.assertEqual(self.app.unclosed_requests, {})

    def call_app(self, req, app=None, expect_exception=False):
        if app is None:
            app = self.app

        self.authorized = []

        def authorize(req):
            self.authorized.append(req)

        if 'swift.authorize' not in req.environ:
            req.environ['swift.authorize'] = authorize

        req.headers.setdefault("User-Agent", "Bruce Wayne")

        status = [None]
        headers = [None]

        def start_response(s, h, ei=None):
            status[0] = s
            headers[0] = h

        body_iter = app(req.environ, start_response)
        body = b''
        caught_exc = None
        try:
            # appease the close-checker
            with closing_if_possible(body_iter):
                for chunk in body_iter:
                    body += chunk
        except Exception as exc:
            if expect_exception:
                caught_exc = exc
            else:
                raise

        if expect_exception:
            return status[0], headers[0], body, caught_exc
        else:
            return status[0], headers[0], body

    def call_ssc(self, req, **kwargs):
        return self.call_app(req, app=self.ssc, **kwargs)

    def assertRequestEqual(self, req, other):
        self.assertEqual(req.method, other.method)
        self.assertEqual(req.path, other.path)

    def test_no_object_in_path_pass_through(self):
        self.app.register('PUT', '/v1/a/c', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c', method='PUT')
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_object_pass_through_methods(self):
        for method in ['DELETE', 'GET', 'HEAD', 'REPLICATE']:
            self.app.register(method, '/v1/a/c/o', swob.HTTPOk, {})
            req = Request.blank('/v1/a/c/o', method=method)
            status, headers, body = self.call_ssc(req)
            self.assertEqual(status, '200 OK')
            self.assertEqual(len(self.authorized), 1)
            self.assertRequestEqual(req, self.authorized[0])
            self.assertNotIn('swift.orig_req_method', req.environ)

    def test_basic_put_with_x_copy_from(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a/c/o2', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o2', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': 'c/o'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        self.assertTrue(('X-Copied-From', 'c/o') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a/c/o2', self.authorized[1].path)
        self.assertEqual(self.app.swift_sources[0], 'SSC')
        self.assertEqual(self.app.swift_sources[1], 'SSC')
        # For basic test cases, assert orig_req_method behavior
        self.assertNotIn('swift.orig_req_method', req.environ)

    def test_static_large_object_manifest(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk,
                          {'X-Static-Large-Object': 'True',
                           'Etag': 'should not be sent'}, 'passed')
        self.app.register('PUT', '/v1/a/c/o2?multipart-manifest=put',
                          swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o2?multipart-manifest=get',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': 'c/o'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        self.assertTrue(('X-Copied-From', 'c/o') in headers)
        self.assertEqual(2, len(self.app.calls))
        self.assertEqual('GET', self.app.calls[0][0])
        get_path, qs = self.app.calls[0][1].split('?')
        params = urllib.parse.parse_qs(qs)
        self.assertDictEqual(
            {'format': ['raw'], 'multipart-manifest': ['get']}, params)
        self.assertEqual(get_path, '/v1/a/c/o')
        self.assertEqual(self.app.calls[1],
                         ('PUT', '/v1/a/c/o2?multipart-manifest=put'))
        req_headers = self.app.headers[1]
        self.assertNotIn('X-Static-Large-Object', req_headers)
        self.assertNotIn('Etag', req_headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a/c/o2', self.authorized[1].path)

    def test_static_large_object(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk,
                          {'X-Static-Large-Object': 'True',
                           'Etag': 'should not be sent'}, 'passed')
        self.app.register('PUT', '/v1/a/c/o2',
                          swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o2',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': 'c/o'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        self.assertTrue(('X-Copied-From', 'c/o') in headers)
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/a/c/o'),
            ('PUT', '/v1/a/c/o2')])
        req_headers = self.app.headers[1]
        self.assertNotIn('X-Static-Large-Object', req_headers)
        self.assertNotIn('Etag', req_headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a/c/o2', self.authorized[1].path)

    def test_basic_put_with_x_copy_from_across_container(self):
        self.app.register('GET', '/v1/a/c1/o1', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a/c2/o2', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c2/o2', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': 'c1/o1'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        self.assertTrue(('X-Copied-From', 'c1/o1') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c1/o1', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a/c2/o2', self.authorized[1].path)

    def test_basic_put_with_x_copy_from_across_container_and_account(self):
        self.app.register('GET', '/v1/a1/c1/o1', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a2/c2/o2', swob.HTTPCreated, {},
                          'passed')
        req = Request.blank('/v1/a2/c2/o2', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': 'c1/o1',
                                     'X-Copy-From-Account': 'a1'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        self.assertTrue(('X-Copied-From', 'c1/o1') in headers)
        self.assertTrue(('X-Copied-From-Account', 'a1') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a1/c1/o1', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a2/c2/o2', self.authorized[1].path)

    def test_copy_non_zero_content_length(self):
        req = Request.blank('/v1/a/c2/o2', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '10',
                                     'X-Copy-From': 'c1/o1'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '400 Bad Request')

    def test_copy_non_zero_content_length_with_account(self):
        req = Request.blank('/v1/a2/c2/o2', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '10',
                                     'X-Copy-From': 'c1/o1',
                                     'X-Copy-From-Account': 'a1'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '400 Bad Request')

    def test_copy_with_slashes_in_x_copy_from(self):
        self.app.register('GET', '/v1/a/c/o/o2', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a/c/o', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': 'c/o/o2'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        self.assertTrue(('X-Copied-From', 'c/o/o2') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o/o2', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a/c/o', self.authorized[1].path)

    def test_copy_with_slashes_in_x_copy_from_and_account(self):
        self.app.register('GET', '/v1/a1/c1/o/o1', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a2/c2/o2', swob.HTTPCreated, {})
        req = Request.blank('/v1/a2/c2/o2', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': 'c1/o/o1',
                                     'X-Copy-From-Account': 'a1'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        self.assertTrue(('X-Copied-From', 'c1/o/o1') in headers)
        self.assertTrue(('X-Copied-From-Account', 'a1') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a1/c1/o/o1', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a2/c2/o2', self.authorized[1].path)

    def test_copy_with_spaces_in_x_copy_from(self):
        self.app.register('GET', '/v1/a/c/o o2', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a/c/o', swob.HTTPCreated, {})
        # space in soure path
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': 'c/o%20o2'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[0]
        self.assertEqual('GET', method)
        self.assertEqual('/v1/a/c/o o2', path)
        self.assertTrue(('X-Copied-From', 'c/o%20o2') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o%20o2', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a/c/o', self.authorized[1].path)

    def test_copy_with_unicode(self):
        self.app.register('GET', '/v1/a/c/\xF0\x9F\x8C\xB4', swob.HTTPOk,
                          {}, 'passed')
        self.app.register('PUT', '/v1/a/c/\xE2\x98\x83', swob.HTTPCreated, {})
        # Just for fun, let's have a mix of properly encoded and not
        req = Request.blank('/v1/a/c/%F0\x9F%8C%B4',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Content-Length': '0',
                                     'Destination': 'c/%E2\x98%83'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[0]
        self.assertEqual('GET', method)
        self.assertEqual('/v1/a/c/\xF0\x9F\x8C\xB4', path)
        self.assertIn(('X-Copied-From', 'c/%F0%9F%8C%B4'), headers)

        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/%F0%9F%8C%B4', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a/c/%E2%98%83', self.authorized[1].path)

    def test_copy_with_spaces_in_x_copy_from_and_account(self):
        self.app.register('GET', '/v1/a/c/o o2', swob.HTTPOk, {}, b'passed')
        self.app.register('PUT', '/v1/a1/c1/o', swob.HTTPCreated, {})
        # space in soure path
        req = Request.blank('/v1/a1/c1/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': 'c/o%20o2',
                                     'X-Copy-From-Account': 'a'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[0]
        self.assertEqual('GET', method)
        self.assertEqual('/v1/a/c/o o2', path)
        self.assertTrue(('X-Copied-From', 'c/o%20o2') in headers)
        self.assertTrue(('X-Copied-From-Account', 'a') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o%20o2', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a1/c1/o', self.authorized[1].path)

    def test_copy_with_leading_slash_in_x_copy_from(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a/c/o', swob.HTTPCreated, {})
        # repeat tests with leading /
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[0]
        self.assertEqual('GET', method)
        self.assertEqual('/v1/a/c/o', path)
        self.assertTrue(('X-Copied-From', 'c/o') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a/c/o', self.authorized[1].path)

    def test_copy_with_leading_slash_in_x_copy_from_and_account(self):
        # repeat tests with leading /
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a1/c1/o', swob.HTTPCreated, {})
        req = Request.blank('/v1/a1/c1/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o',
                                     'X-Copy-From-Account': 'a'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[0]
        self.assertEqual('GET', method)
        self.assertEqual('/v1/a/c/o', path)
        self.assertTrue(('X-Copied-From', 'c/o') in headers)
        self.assertTrue(('X-Copied-From-Account', 'a') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a1/c1/o', self.authorized[1].path)

    def test_copy_with_leading_slash_and_slashes_in_x_copy_from(self):
        self.app.register('GET', '/v1/a/c/o/o2', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a/c/o', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o/o2'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[0]
        self.assertEqual('GET', method)
        self.assertEqual('/v1/a/c/o/o2', path)
        self.assertTrue(('X-Copied-From', 'c/o/o2') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o/o2', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a/c/o', self.authorized[1].path)

    def test_copy_with_leading_slash_and_slashes_in_x_copy_from_acct(self):
        self.app.register('GET', '/v1/a/c/o/o2', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a1/c1/o', swob.HTTPCreated, {})
        req = Request.blank('/v1/a1/c1/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o/o2',
                                     'X-Copy-From-Account': 'a'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[0]
        self.assertEqual('GET', method)
        self.assertEqual('/v1/a/c/o/o2', path)
        self.assertTrue(('X-Copied-From', 'c/o/o2') in headers)
        self.assertTrue(('X-Copied-From-Account', 'a') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o/o2', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a1/c1/o', self.authorized[1].path)

    def test_copy_with_no_object_in_x_copy_from(self):
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '412 Precondition Failed')

    def test_copy_with_no_object_in_x_copy_from_and_account(self):
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c',
                                     'X-Copy-From-Account': 'a'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '412 Precondition Failed')

    def test_copy_with_bad_x_copy_from_account(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o',
                                     'X-Copy-From-Account': '/i/am/bad'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '412 Precondition Failed')

    def test_copy_server_error_reading_source(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPServiceUnavailable, {})
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '503 Service Unavailable')

    def test_copy_server_error_reading_source_and_account(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPServiceUnavailable, {})
        req = Request.blank('/v1/a1/c1/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o',
                                     'X-Copy-From-Account': 'a'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '503 Service Unavailable')
        self.assertEqual(len(self.authorized), 1)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)

    def test_copy_not_found_reading_source(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPNotFound, {})
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '404 Not Found')
        self.assertEqual(len(self.authorized), 1)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)

    def test_copy_not_found_reading_source_and_account(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPNotFound, {})
        req = Request.blank('/v1/a1/c1/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o',
                                     'X-Copy-From-Account': 'a'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '404 Not Found')
        self.assertEqual(len(self.authorized), 1)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)

    def test_copy_with_object_metadata(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a/c/o', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o',
                                     'X-Object-Meta-Ours': 'okay'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[1]
        self.assertEqual('PUT', method)
        self.assertEqual('/v1/a/c/o', path)
        self.assertEqual(req_headers['X-Object-Meta-Ours'], 'okay')
        self.assertTrue(('X-Object-Meta-Ours', 'okay') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a/c/o', self.authorized[1].path)

    def test_copy_with_object_metadata_and_account(self):
        self.app.register('GET', '/v1/a1/c/o', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a/c/o', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o',
                                     'X-Object-Meta-Ours': 'okay',
                                     'X-Copy-From-Account': 'a1'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[1]
        self.assertEqual('PUT', method)
        self.assertEqual('/v1/a/c/o', path)
        self.assertEqual(req_headers['X-Object-Meta-Ours'], 'okay')
        self.assertTrue(('X-Object-Meta-Ours', 'okay') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a1/c/o', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a/c/o', self.authorized[1].path)

    def test_copy_source_larger_than_max_file_size(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {}, "largebody")
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o'})
        with mock.patch('swift.common.middleware.copy.'
                        'MAX_FILE_SIZE', 1):
            status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '413 Request Entity Too Large')
        self.assertEqual(len(self.authorized), 1)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)

    def test_basic_COPY(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {
            'etag': 'is sent'}, 'passed')
        self.app.register('PUT', '/v1/a/c/o-copy', swob.HTTPCreated, {})
        req = Request.blank(
            '/v1/a/c/o', method='COPY',
            headers={'Content-Length': 0,
                     'Destination': 'c/o-copy'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        self.assertTrue(('X-Copied-From', 'c/o') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a/c/o-copy', self.authorized[1].path)
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/a/c/o'),
            ('PUT', '/v1/a/c/o-copy')])
        self.assertIn('etag', self.app.headers[1])
        self.assertEqual(self.app.headers[1]['etag'], 'is sent')
        # For basic test cases, assert orig_req_method behavior
        self.assertEqual(req.environ['swift.orig_req_method'], 'COPY')

    def test_basic_DLO(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {
            'x-object-manifest': 'some/path',
            'etag': 'is not sent'}, 'passed')
        self.app.register('PUT', '/v1/a/c/o-copy', swob.HTTPCreated, {})
        req = Request.blank(
            '/v1/a/c/o', method='COPY',
            headers={'Content-Length': 0,
                     'Destination': 'c/o-copy'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        self.assertTrue(('X-Copied-From', 'c/o') in headers)
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/a/c/o'),
            ('PUT', '/v1/a/c/o-copy')])
        self.assertNotIn('x-object-manifest', self.app.headers[1])
        self.assertNotIn('etag', self.app.headers[1])

    def test_basic_DLO_manifest(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {
            'x-object-manifest': 'some/path',
            'etag': 'is sent'}, 'passed')
        self.app.register('PUT', '/v1/a/c/o-copy', swob.HTTPCreated, {})
        req = Request.blank(
            '/v1/a/c/o?multipart-manifest=get', method='COPY',
            headers={'Content-Length': 0,
                     'Destination': 'c/o-copy'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        self.assertTrue(('X-Copied-From', 'c/o') in headers)
        self.assertEqual(2, len(self.app.calls))
        self.assertEqual('GET', self.app.calls[0][0])
        get_path, qs = self.app.calls[0][1].split('?')
        params = urllib.parse.parse_qs(qs)
        self.assertDictEqual(
            {'format': ['raw'], 'multipart-manifest': ['get']}, params)
        self.assertEqual(get_path, '/v1/a/c/o')
        self.assertEqual(self.app.calls[1], ('PUT', '/v1/a/c/o-copy'))
        self.assertIn('x-object-manifest', self.app.headers[1])
        self.assertEqual(self.app.headers[1]['x-object-manifest'], 'some/path')
        self.assertIn('etag', self.app.headers[1])
        self.assertEqual(self.app.headers[1]['etag'], 'is sent')

    def test_COPY_source_metadata(self):
        source_headers = {
            'x-object-sysmeta-test1': 'copy me',
            'x-object-meta-test2': 'copy me too',
            'x-object-transient-sysmeta-test3': 'ditto',
            'x-object-sysmeta-container-update-override-etag': 'etag val',
            'x-object-sysmeta-container-update-override-size': 'size val',
            'x-object-sysmeta-container-update-override-foo': 'bar',
            'x-delete-at': 'delete-at-time'}

        get_resp_headers = source_headers.copy()
        get_resp_headers['etag'] = 'source etag'
        self.app.register(
            'GET', '/v1/a/c/o', swob.HTTPOk,
            headers=get_resp_headers, body=b'passed')

        def verify_headers(expected_headers, unexpected_headers,
                           actual_headers):
            for k, v in actual_headers:
                if k.lower() in expected_headers:
                    expected_val = expected_headers.pop(k.lower())
                    self.assertEqual(expected_val, v)
                self.assertNotIn(k.lower(), unexpected_headers)
            self.assertFalse(expected_headers)

        # use a COPY request
        self.app.register('PUT', '/v1/a/c/o-copy0', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o', method='COPY',
                            headers={'Content-Length': 0,
                                     'Destination': 'c/o-copy0'})
        status, resp_headers, body = self.call_ssc(req)
        self.assertEqual('201 Created', status)
        verify_headers(source_headers.copy(), [], resp_headers)
        method, path, put_headers = self.app.calls_with_headers[-1]
        self.assertEqual('PUT', method)
        self.assertEqual('/v1/a/c/o-copy0', path)
        verify_headers(source_headers.copy(), [], put_headers.items())
        self.assertIn('etag', put_headers)
        self.assertEqual(put_headers['etag'], 'source etag')

        req = Request.blank('/v1/a/c/o-copy0', method='GET')
        status, resp_headers, body = self.call_ssc(req)
        self.assertEqual('200 OK', status)
        verify_headers(source_headers.copy(), [], resp_headers)

        # use a COPY request with a Range header
        self.app.register('PUT', '/v1/a/c/o-copy1', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o', method='COPY',
                            headers={'Content-Length': 0,
                                     'Destination': 'c/o-copy1',
                                     'Range': 'bytes=1-2'})
        status, resp_headers, body = self.call_ssc(req)
        expected_headers = source_headers.copy()
        unexpected_headers = (
            'x-object-sysmeta-container-update-override-etag',
            'x-object-sysmeta-container-update-override-size',
            'x-object-sysmeta-container-update-override-foo')
        for h in unexpected_headers:
            expected_headers.pop(h)
        self.assertEqual('201 Created', status)
        verify_headers(expected_headers, unexpected_headers, resp_headers)
        method, path, put_headers = self.app.calls_with_headers[-1]
        self.assertEqual('PUT', method)
        self.assertEqual('/v1/a/c/o-copy1', path)
        verify_headers(
            expected_headers, unexpected_headers, put_headers.items())
        # etag should not be copied with a Range request
        self.assertNotIn('etag', put_headers)

        req = Request.blank('/v1/a/c/o-copy1', method='GET')
        status, resp_headers, body = self.call_ssc(req)
        self.assertEqual('200 OK', status)
        verify_headers(expected_headers, unexpected_headers, resp_headers)

        # use a PUT with x-copy-from
        self.app.register('PUT', '/v1/a/c/o-copy2', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o-copy2', method='PUT',
                            headers={'Content-Length': 0,
                                     'X-Copy-From': 'c/o'})
        status, resp_headers, body = self.call_ssc(req)
        self.assertEqual('201 Created', status)
        verify_headers(source_headers.copy(), [], resp_headers)
        method, path, put_headers = self.app.calls_with_headers[-1]
        self.assertEqual('PUT', method)
        self.assertEqual('/v1/a/c/o-copy2', path)
        verify_headers(source_headers.copy(), [], put_headers.items())
        self.assertIn('etag', put_headers)
        self.assertEqual(put_headers['etag'], 'source etag')

        req = Request.blank('/v1/a/c/o-copy2', method='GET')
        status, resp_headers, body = self.call_ssc(req)
        self.assertEqual('200 OK', status)
        verify_headers(source_headers.copy(), [], resp_headers)

        # copy to same path as source
        self.app.register('PUT', '/v1/a/c/o', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o', method='PUT',
                            headers={'Content-Length': 0,
                                     'X-Copy-From': 'c/o'})
        status, resp_headers, body = self.call_ssc(req)
        self.assertEqual('201 Created', status)
        verify_headers(source_headers.copy(), [], resp_headers)
        method, path, put_headers = self.app.calls_with_headers[-1]
        self.assertEqual('PUT', method)
        self.assertEqual('/v1/a/c/o', path)
        verify_headers(source_headers.copy(), [], put_headers.items())
        self.assertIn('etag', put_headers)
        self.assertEqual(put_headers['etag'], 'source etag')

    def test_COPY_no_destination_header(self):
        req = Request.blank(
            '/v1/a/c/o', method='COPY', headers={'Content-Length': 0})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '412 Precondition Failed')
        self.assertEqual(len(self.authorized), 0)

    def test_basic_COPY_account(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a1/c1/o2', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': 'c1/o2',
                                     'Destination-Account': 'a1'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[0]
        self.assertEqual('GET', method)
        self.assertEqual('/v1/a/c/o', path)
        method, path, req_headers = calls[1]
        self.assertEqual('PUT', method)
        self.assertEqual('/v1/a1/c1/o2', path)
        self.assertTrue(('X-Copied-From', 'c/o') in headers)
        self.assertTrue(('X-Copied-From-Account', 'a') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a1/c1/o2', self.authorized[1].path)

    def test_COPY_across_containers(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a/c2/o', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': 'c2/o'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        self.assertTrue(('X-Copied-From', 'c/o') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a/c2/o', self.authorized[1].path)

    def test_COPY_source_with_slashes_in_name(self):
        self.app.register('GET', '/v1/a/c/o/o2', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a/c/o', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o/o2',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': 'c/o'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[1]
        self.assertEqual('PUT', method)
        self.assertEqual('/v1/a/c/o', path)
        self.assertTrue(('X-Copied-From', 'c/o/o2') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o/o2', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a/c/o', self.authorized[1].path)

    def test_COPY_account_source_with_slashes_in_name(self):
        self.app.register('GET', '/v1/a/c/o/o2', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a1/c1/o', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o/o2',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': 'c1/o',
                                     'Destination-Account': 'a1'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[1]
        self.assertEqual('PUT', method)
        self.assertEqual('/v1/a1/c1/o', path)
        self.assertTrue(('X-Copied-From', 'c/o/o2') in headers)
        self.assertTrue(('X-Copied-From-Account', 'a') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o/o2', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a1/c1/o', self.authorized[1].path)

    def test_COPY_destination_leading_slash(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a/c/o', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c/o'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        self.assertTrue(('X-Copied-From', 'c/o') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a/c/o', self.authorized[1].path)

    def test_COPY_account_destination_leading_slash(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a1/c1/o', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c1/o',
                                     'Destination-Account': 'a1'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[1]
        self.assertEqual('PUT', method)
        self.assertEqual('/v1/a1/c1/o', path)
        self.assertTrue(('X-Copied-From', 'c/o') in headers)
        self.assertTrue(('X-Copied-From-Account', 'a') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a1/c1/o', self.authorized[1].path)

    def test_COPY_source_with_slashes_destination_leading_slash(self):
        self.app.register('GET', '/v1/a/c/o/o2', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a/c/o', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o/o2',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c/o'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[1]
        self.assertEqual('PUT', method)
        self.assertEqual('/v1/a/c/o', path)
        self.assertTrue(('X-Copied-From', 'c/o/o2') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o/o2', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a/c/o', self.authorized[1].path)

    def test_COPY_account_source_with_slashes_destination_leading_slash(self):
        self.app.register('GET', '/v1/a/c/o/o2', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a1/c1/o', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o/o2',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c1/o',
                                     'Destination-Account': 'a1'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[1]
        self.assertEqual('PUT', method)
        self.assertEqual('/v1/a1/c1/o', path)
        self.assertTrue(('X-Copied-From', 'c/o/o2') in headers)
        self.assertTrue(('X-Copied-From-Account', 'a') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o/o2', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a1/c1/o', self.authorized[1].path)

    def test_COPY_no_object_in_destination(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': 'c_o'})
        status, headers, body = self.call_ssc(req)

        self.assertEqual(status, '412 Precondition Failed')

    def test_COPY_account_no_object_in_destination(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': 'c_o',
                                     'Destination-Account': 'a1'})
        status, headers, body = self.call_ssc(req)

        self.assertEqual(status, '412 Precondition Failed')

    def test_COPY_account_bad_destination_account(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c/o',
                                     'Destination-Account': '/i/am/bad'})
        status, headers, body = self.call_ssc(req)

        self.assertEqual(status, '412 Precondition Failed')

    def test_COPY_server_error_reading_source(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPServiceUnavailable, {})
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c/o'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '503 Service Unavailable')
        self.assertEqual(len(self.authorized), 1)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)

    def test_COPY_account_server_error_reading_source(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPServiceUnavailable, {})
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c1/o',
                                     'Destination-Account': 'a1'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '503 Service Unavailable')
        self.assertEqual(len(self.authorized), 1)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)

    def test_COPY_not_found_reading_source(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPNotFound, {})
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c/o'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '404 Not Found')
        self.assertEqual(len(self.authorized), 1)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)

    def test_COPY_account_not_found_reading_source(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPNotFound, {})
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c1/o',
                                     'Destination-Account': 'a1'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '404 Not Found')
        self.assertEqual(len(self.authorized), 1)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)

    def test_COPY_with_metadata(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {}, "passed")
        self.app.register('PUT', '/v1/a/c/o', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c/o',
                                     'X-Object-Meta-Ours': 'okay'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[1]
        self.assertEqual('PUT', method)
        self.assertEqual('/v1/a/c/o', path)
        self.assertEqual(req_headers['X-Object-Meta-Ours'], 'okay')
        self.assertTrue(('X-Object-Meta-Ours', 'okay') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a/c/o', self.authorized[1].path)

    def test_COPY_account_with_metadata(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {}, "passed")
        self.app.register('PUT', '/v1/a1/c1/o', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c1/o',
                                     'X-Object-Meta-Ours': 'okay',
                                     'Destination-Account': 'a1'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[1]
        self.assertEqual('PUT', method)
        self.assertEqual('/v1/a1/c1/o', path)
        self.assertEqual(req_headers['X-Object-Meta-Ours'], 'okay')
        self.assertTrue(('X-Object-Meta-Ours', 'okay') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a1/c1/o', self.authorized[1].path)

    def test_COPY_source_zero_content_length(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {}, None)
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c/o'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '413 Request Entity Too Large')
        self.assertEqual(len(self.authorized), 1)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)

    def test_COPY_source_larger_than_max_file_size(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {}, "largebody")
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c/o'})
        with mock.patch('swift.common.middleware.copy.'
                        'MAX_FILE_SIZE', 1):
            status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '413 Request Entity Too Large')
        self.assertEqual(len(self.authorized), 1)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)

    def test_COPY_account_source_zero_content_length(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {}, None)
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c/o',
                                     'Destination-Account': 'a1'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '413 Request Entity Too Large')
        self.assertEqual(len(self.authorized), 1)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)

    def test_COPY_account_source_larger_than_max_file_size(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {}, "largebody")
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c1/o',
                                     'Destination-Account': 'a1'})
        with mock.patch('swift.common.middleware.copy.'
                        'MAX_FILE_SIZE', 1):
            status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '413 Request Entity Too Large')
        self.assertEqual(len(self.authorized), 1)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)

    def test_COPY_newest(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk,
                          {'Last-Modified': '123'}, "passed")
        self.app.register('PUT', '/v1/a/c/o', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c/o'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        self.assertTrue(('X-Copied-From-Last-Modified', '123') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a/c/o', self.authorized[1].path)

    def test_COPY_account_newest(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk,
                          {'Last-Modified': '123'}, "passed")
        self.app.register('PUT', '/v1/a1/c1/o', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c1/o',
                                     'Destination-Account': 'a1'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        self.assertTrue(('X-Copied-From-Last-Modified', '123') in headers)
        self.assertEqual(len(self.authorized), 2)
        self.assertEqual('GET', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)
        self.assertEqual('PUT', self.authorized[1].method)
        self.assertEqual('/v1/a1/c1/o', self.authorized[1].path)

    def test_COPY_in_OPTIONS_response(self):
        self.app.register('OPTIONS', '/v1/a/c/o', swob.HTTPOk,
                          {'Allow': 'GET, PUT'})
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'OPTIONS'}, headers={})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '200 OK')
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[0]
        self.assertEqual('OPTIONS', method)
        self.assertEqual('/v1/a/c/o', path)
        self.assertTrue(('Allow', 'GET, PUT, COPY') in headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertEqual('OPTIONS', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)
        # For basic test cases, assert orig_req_method behavior
        self.assertNotIn('swift.orig_req_method', req.environ)

    def test_COPY_in_OPTIONS_response_CORS(self):
        self.app.register('OPTIONS', '/v1/a/c/o', swob.HTTPOk,
                          {'Allow': 'GET, PUT',
                           'Access-Control-Allow-Methods': 'GET, PUT'})
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'OPTIONS'}, headers={})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '200 OK')
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[0]
        self.assertEqual('OPTIONS', method)
        self.assertEqual('/v1/a/c/o', path)
        self.assertTrue(('Allow', 'GET, PUT, COPY') in headers)
        self.assertTrue(('Access-Control-Allow-Methods',
                         'GET, PUT, COPY') in headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertEqual('OPTIONS', self.authorized[0].method)
        self.assertEqual('/v1/a/c/o', self.authorized[0].path)

    def _test_COPY_source_headers(self, extra_put_headers):
        # helper method to perform a COPY with some metadata headers that
        # should always be sent to the destination
        put_headers = {'Destination': '/c1/o',
                       'X-Object-Meta-Test2': 'added',
                       'X-Object-Sysmeta-Test2': 'added',
                       'X-Object-Transient-Sysmeta-Test2': 'added'}
        put_headers.update(extra_put_headers)
        get_resp_headers = {
            'X-Timestamp': '1234567890.12345',
            'X-Backend-Timestamp': '1234567890.12345',
            'Content-Type': 'text/original',
            'Content-Encoding': 'gzip',
            'Content-Disposition': 'attachment; filename=myfile',
            'X-Object-Meta-Test': 'original',
            'X-Object-Sysmeta-Test': 'original',
            'X-Object-Transient-Sysmeta-Test': 'original',
            'X-Foo': 'Bar'}
        self.app.register(
            'GET', '/v1/a/c/o', swob.HTTPOk, headers=get_resp_headers)
        self.app.register('PUT', '/v1/a/c1/o', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o', method='COPY', headers=put_headers)
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        calls = self.app.calls_with_headers
        self.assertEqual(2, len(calls))
        method, path, req_headers = calls[1]
        self.assertEqual('PUT', method)
        # these headers should always be applied to the destination
        self.assertEqual('added', req_headers.get('X-Object-Meta-Test2'))
        self.assertEqual('added', req_headers.get('X-Object-Sysmeta-Test2'))
        self.assertEqual('added',
                         req_headers.get('X-Object-Transient-Sysmeta-Test2'))
        return req_headers

    def test_COPY_source_headers_no_updates(self):
        # copy should preserve existing metadata if not updated
        req_headers = self._test_COPY_source_headers({})
        self.assertEqual('text/original', req_headers.get('Content-Type'))
        self.assertEqual('gzip', req_headers.get('Content-Encoding'))
        self.assertEqual('attachment; filename=myfile',
                         req_headers.get('Content-Disposition'))
        self.assertEqual('original', req_headers.get('X-Object-Meta-Test'))
        self.assertEqual('original', req_headers.get('X-Object-Sysmeta-Test'))
        self.assertEqual('original',
                         req_headers.get('X-Object-Transient-Sysmeta-Test'))
        self.assertEqual('Bar', req_headers.get('X-Foo'))
        self.assertNotIn('X-Timestamp', req_headers)
        self.assertNotIn('X-Backend-Timestamp', req_headers)

    def test_COPY_source_headers_with_updates(self):
        # copy should apply any updated values to existing metadata
        put_headers = {
            'Content-Type': 'text/not_original',
            'Content-Encoding': 'not_gzip',
            'Content-Disposition': 'attachment; filename=notmyfile',
            'X-Object-Meta-Test': 'not_original',
            'X-Object-Sysmeta-Test': 'not_original',
            'X-Object-Transient-Sysmeta-Test': 'not_original',
            'X-Foo': 'Not Bar'}
        req_headers = self._test_COPY_source_headers(put_headers)
        self.assertEqual('text/not_original', req_headers.get('Content-Type'))
        self.assertEqual('not_gzip', req_headers.get('Content-Encoding'))
        self.assertEqual('attachment; filename=notmyfile',
                         req_headers.get('Content-Disposition'))
        self.assertEqual('not_original', req_headers.get('X-Object-Meta-Test'))
        self.assertEqual('not_original',
                         req_headers.get('X-Object-Sysmeta-Test'))
        self.assertEqual('not_original',
                         req_headers.get('X-Object-Transient-Sysmeta-Test'))
        self.assertEqual('Not Bar', req_headers.get('X-Foo'))
        self.assertNotIn('X-Timestamp', req_headers)
        self.assertNotIn('X-Backend-Timestamp', req_headers)

    def test_COPY_x_fresh_metadata_no_updates(self):
        # existing user metadata should not be copied, sysmeta is copied
        put_headers = {
            'X-Fresh-Metadata': 'true',
            'X-Extra': 'Fresh'}
        req_headers = self._test_COPY_source_headers(put_headers)
        self.assertEqual('text/original', req_headers.get('Content-Type'))
        self.assertEqual('Fresh', req_headers.get('X-Extra'))
        self.assertEqual('original',
                         req_headers.get('X-Object-Sysmeta-Test'))
        self.assertIn('X-Fresh-Metadata', req_headers)
        self.assertNotIn('X-Object-Meta-Test', req_headers)
        self.assertNotIn('X-Object-Transient-Sysmeta-Test', req_headers)
        self.assertNotIn('X-Timestamp', req_headers)
        self.assertNotIn('X-Backend-Timestamp', req_headers)
        self.assertNotIn('Content-Encoding', req_headers)
        self.assertNotIn('Content-Disposition', req_headers)
        self.assertNotIn('X-Foo', req_headers)

    def test_COPY_x_fresh_metadata_with_updates(self):
        # existing user metadata should not be copied, new metadata replaces it
        put_headers = {
            'X-Fresh-Metadata': 'true',
            'Content-Type': 'text/not_original',
            'Content-Encoding': 'not_gzip',
            'Content-Disposition': 'attachment; filename=notmyfile',
            'X-Object-Meta-Test': 'not_original',
            'X-Object-Sysmeta-Test': 'not_original',
            'X-Object-Transient-Sysmeta-Test': 'not_original',
            'X-Foo': 'Not Bar',
            'X-Extra': 'Fresh'}
        req_headers = self._test_COPY_source_headers(put_headers)
        self.assertEqual('Fresh', req_headers.get('X-Extra'))
        self.assertEqual('text/not_original', req_headers.get('Content-Type'))
        self.assertEqual('not_gzip', req_headers.get('Content-Encoding'))
        self.assertEqual('attachment; filename=notmyfile',
                         req_headers.get('Content-Disposition'))
        self.assertEqual('not_original', req_headers.get('X-Object-Meta-Test'))
        self.assertEqual('not_original',
                         req_headers.get('X-Object-Sysmeta-Test'))
        self.assertEqual('not_original',
                         req_headers.get('X-Object-Transient-Sysmeta-Test'))
        self.assertEqual('Not Bar', req_headers.get('X-Foo'))

    def test_COPY_with_single_range(self):
        # verify that source etag is not copied when copying a range
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk,
                          {'etag': 'bogus etag'}, "abcdefghijklmnop")
        self.app.register('PUT', '/v1/a/c1/o', swob.HTTPCreated, {})
        req = swob.Request.blank(
            '/v1/a/c/o', method='COPY',
            headers={'Destination': 'c1/o',
                     'Range': 'bytes=5-10'})

        status, headers, body = self.call_ssc(req)

        self.assertEqual(status, '201 Created')
        calls = self.app.calls_with_headers
        self.assertEqual(2, len(calls))
        method, path, req_headers = calls[1]
        self.assertEqual('PUT', method)
        self.assertEqual('/v1/a/c1/o', path)
        self.assertNotIn('etag', (h.lower() for h in req_headers))
        self.assertEqual('6', req_headers['content-length'])
        req = swob.Request.blank('/v1/a/c1/o', method='GET')
        status, headers, body = self.call_ssc(req)
        self.assertEqual(b'fghijk', body)


@patch_policies(with_ec_default=True)
class TestServerSideCopyMiddlewareWithEC(unittest.TestCase):
    container_info = {
        'status': 200,
        'write_acl': None,
        'read_acl': None,
        'storage_policy': None,
        'sync_key': None,
        'versions': None,
    }

    def setUp(self):
        self.logger = debug_logger('proxy-server')
        self.logger.thread_locals = ('txn1', '127.0.0.2')
        self.app = PatchedObjControllerApp(
            None, account_ring=FakeRing(),
            container_ring=FakeRing(), logger=self.logger)
        self.ssc = copy.filter_factory({})(self.app)
        self.ssc.logger = self.app.logger
        self.policy = POLICIES.default
        self.app.container_info = dict(self.container_info)

    def test_COPY_with_single_range(self):
        req = swob.Request.blank(
            '/v1/a/c/o', method='COPY',
            headers={'Destination': 'c1/o',
                     'Range': 'bytes=5-10'})
        # turn a real body into fragments
        segment_size = self.policy.ec_segment_size
        real_body = (b'asdf' * segment_size)[:-10]

        # split it up into chunks
        chunks = [real_body[x:x + segment_size]
                  for x in range(0, len(real_body), segment_size)]

        # we need only first chunk to rebuild 5-10 range
        fragments = self.policy.pyeclib_driver.encode(chunks[0])
        fragment_payloads = []
        fragment_payloads.append(fragments)

        node_fragments = list(zip(*fragment_payloads))
        self.assertEqual(len(node_fragments),
                         self.policy.object_ring.replicas)  # sanity
        headers = {'X-Object-Sysmeta-Ec-Content-Length': str(len(real_body))}
        responses = [(200, b''.join(node_fragments[i]), headers)
                     for i in range(POLICIES.default.ec_ndata)]
        responses += [(201, b'', {})] * self.policy.object_ring.replicas
        status_codes, body_iter, headers = zip(*responses)
        expect_headers = {
            'X-Obj-Metadata-Footer': 'yes',
            'X-Obj-Multiphase-Commit': 'yes'
        }

        put_hdrs = []

        def capture_conn(host, port, dev, part, method, path, *args, **kwargs):
            if method == 'PUT':
                put_hdrs.append(args[0])

        with set_http_connect(*status_codes, body_iter=body_iter,
                              headers=headers, expect_headers=expect_headers,
                              give_connect=capture_conn):
            resp = req.get_response(self.ssc)

        self.assertEqual(resp.status_int, 201)
        expected_puts = POLICIES.default.ec_ndata + POLICIES.default.ec_nparity
        self.assertEqual(expected_puts, len(put_hdrs))
        for hdrs in put_hdrs:
            # etag should not be copied from source
            self.assertNotIn('etag', (h.lower() for h in hdrs))

    def test_COPY_with_invalid_ranges(self):
        # real body size is segment_size - 10 (just 1 segment)
        segment_size = self.policy.ec_segment_size
        real_body = (b'a' * segment_size)[:-10]

        # range is out of real body but in segment size
        self._test_invalid_ranges('COPY', real_body,
                                  segment_size, '%s-' % (segment_size - 10))
        # range is out of both real body and segment size
        self._test_invalid_ranges('COPY', real_body,
                                  segment_size, '%s-' % (segment_size + 10))

    def _test_invalid_ranges(self, method, real_body, segment_size, req_range):
        # make a request with range starts from more than real size.
        body_etag = md5(real_body, usedforsecurity=False).hexdigest()
        req = swob.Request.blank(
            '/v1/a/c/o', method=method,
            headers={'Destination': 'c1/o',
                     'Range': 'bytes=%s' % (req_range)})

        fragments = self.policy.pyeclib_driver.encode(real_body)
        fragment_payloads = [fragments]

        node_fragments = list(zip(*fragment_payloads))
        self.assertEqual(len(node_fragments),
                         self.policy.object_ring.replicas)  # sanity
        headers = {'X-Object-Sysmeta-Ec-Content-Length': str(len(real_body)),
                   'X-Object-Sysmeta-Ec-Etag': body_etag}
        start = int(req_range.split('-')[0])
        self.assertTrue(start >= 0)  # sanity
        title, exp = swob.RESPONSE_REASONS[416]
        range_not_satisfiable_body = \
            '<html><h1>%s</h1><p>%s</p></html>' % (title, exp)
        range_not_satisfiable_body = range_not_satisfiable_body.encode('ascii')
        if start >= segment_size:
            responses = [(416, range_not_satisfiable_body, headers)
                         for i in range(POLICIES.default.ec_ndata)]
        else:
            responses = [(200, b''.join(node_fragments[i]), headers)
                         for i in range(POLICIES.default.ec_ndata)]
        status_codes, body_iter, headers = zip(*responses)
        expect_headers = {
            'X-Obj-Metadata-Footer': 'yes',
            'X-Obj-Multiphase-Commit': 'yes'
        }
        # TODO possibly use FakeApp here
        with set_http_connect(*status_codes, body_iter=body_iter,
                              headers=headers, expect_headers=expect_headers):
            resp = req.get_response(self.ssc)
        self.assertEqual(resp.status_int, 416)
        self.assertEqual(resp.content_length, len(range_not_satisfiable_body))
        self.assertEqual(resp.body, range_not_satisfiable_body)
        self.assertEqual(resp.etag, body_etag)
        self.assertEqual(resp.headers['Accept-Ranges'], 'bytes')
