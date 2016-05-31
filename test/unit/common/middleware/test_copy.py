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

import time
import mock
import shutil
import tempfile
import unittest
from hashlib import md5
from textwrap import dedent

from swift.common import swob
from swift.common.middleware import copy
from swift.common.storage_policy import POLICIES
from swift.common.swob import Request, HTTPException
from test.unit import patch_policies, debug_logger, FakeMemcache, FakeRing
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
        self.ssc = copy.filter_factory({
            'object_post_as_copy': 'yes',
        })(self.app)
        self.ssc.logger = self.app.logger

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
        body = ''
        caught_exc = None
        try:
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

    def test_object_delete_pass_through(self):
        self.app.register('DELETE', '/v1/a/c/o', swob.HTTPOk, {})
        req = Request.blank('/v1/a/c/o', method='DELETE')
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_POST_as_COPY_simple(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a/c/o', swob.HTTPAccepted, {})
        req = Request.blank('/v1/a/c/o', method='POST')
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '202 Accepted')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_POST_as_COPY_201_return_202(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a/c/o', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o', method='POST')
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '202 Accepted')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_POST_delete_at(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')
        self.app.register('PUT', '/v1/a/c/o', swob.HTTPAccepted, {})
        t = str(int(time.time() + 100))
        req = Request.blank('/v1/a/c/o', method='POST',
                            headers={'Content-Type': 'foo/bar',
                                     'X-Delete-At': t})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '202 Accepted')

        calls = self.app.calls_with_headers
        method, path, req_headers = calls[1]
        self.assertEqual('PUT', method)
        self.assertTrue('X-Delete-At' in req_headers)
        self.assertEqual(req_headers['X-Delete-At'], str(t))
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

    def test_POST_as_COPY_static_large_object(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk,
                          {'X-Static-Large-Object': True}, 'passed')
        self.app.register('PUT', '/v1/a/c/o', swob.HTTPAccepted, {})
        req = Request.blank('/v1/a/c/o', method='POST',
                            headers={})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '202 Accepted')

        calls = self.app.calls_with_headers
        method, path, req_headers = calls[1]
        self.assertEqual('PUT', method)
        self.assertNotIn('X-Static-Large-Object', req_headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])

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

    def test_static_large_object(self):
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk,
                          {'X-Static-Large-Object': 'True'}, 'passed')
        self.app.register('PUT', '/v1/a/c/o2?multipart-manifest=put',
                          swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/o2?multipart-manifest=get',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': 'c/o'})
        status, headers, body = self.call_ssc(req)
        self.assertEqual(status, '201 Created')
        self.assertTrue(('X-Copied-From', 'c/o') in headers)
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[1]
        self.assertEqual('PUT', method)
        self.assertEqual('/v1/a/c/o2?multipart-manifest=put', path)
        self.assertNotIn('X-Static-Large-Object', req_headers)
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

    def test_copy_with_spaces_in_x_copy_from_and_account(self):
        self.app.register('GET', '/v1/a/c/o o2', swob.HTTPOk, {}, 'passed')
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
        try:
            status, headers, body = self.call_ssc(req)
        except HTTPException as resp:
            self.assertEqual("412 Precondition Failed", str(resp))
        else:
            self.fail("Expecting HTTPException.")

    def test_copy_with_no_object_in_x_copy_from_and_account(self):
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c',
                                     'X-Copy-From-Account': 'a'})
        try:
            status, headers, body = self.call_ssc(req)
        except HTTPException as resp:
            self.assertEqual("412 Precondition Failed", str(resp))
        else:
            self.fail("Expecting HTTPException.")

    def test_copy_with_bad_x_copy_from_account(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Content-Length': '0',
                                     'X-Copy-From': '/c/o',
                                     'X-Copy-From-Account': '/i/am/bad'})
        try:
            status, headers, body = self.call_ssc(req)
        except HTTPException as resp:
            self.assertEqual("412 Precondition Failed", str(resp))
        else:
            self.fail("Expecting HTTPException.")

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
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, {}, 'passed')
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
        try:
            status, headers, body = self.call_ssc(req)
        except HTTPException as resp:
            self.assertEqual("412 Precondition Failed", str(resp))
        else:
            self.fail("Expecting HTTPException.")

    def test_COPY_account_no_object_in_destination(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': 'c_o',
                                     'Destination-Account': 'a1'})
        try:
            status, headers, body = self.call_ssc(req)
        except HTTPException as resp:
            self.assertEqual("412 Precondition Failed", str(resp))
        else:
            self.fail("Expecting HTTPException.")

    def test_COPY_account_bad_destination_account(self):
        req = Request.blank('/v1/a/c/o',
                            environ={'REQUEST_METHOD': 'COPY'},
                            headers={'Destination': '/c/o',
                                     'Destination-Account': '/i/am/bad'})
        try:
            status, headers, body = self.call_ssc(req)
        except HTTPException as resp:
            self.assertEqual("412 Precondition Failed", str(resp))
        else:
            self.fail("Expecting HTTPException.")

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


class TestServerSideCopyConfiguration(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_reading_proxy_conf_when_no_middleware_conf_present(self):
        proxy_conf = dedent("""
        [DEFAULT]
        bind_ip = 10.4.5.6

        [pipeline:main]
        pipeline = catch_errors copy ye-olde-proxy-server

        [filter:copy]
        use = egg:swift#copy

        [app:ye-olde-proxy-server]
        use = egg:swift#proxy
        object_post_as_copy = no
        """)

        conffile = tempfile.NamedTemporaryFile()
        conffile.write(proxy_conf)
        conffile.flush()

        ssc = copy.filter_factory({
            '__file__': conffile.name
        })("no app here")

        self.assertEqual(ssc.object_post_as_copy, False)

    def test_middleware_conf_precedence(self):
        proxy_conf = dedent("""
        [DEFAULT]
        bind_ip = 10.4.5.6

        [pipeline:main]
        pipeline = catch_errors copy ye-olde-proxy-server

        [filter:copy]
        use = egg:swift#copy
        object_post_as_copy = no

        [app:ye-olde-proxy-server]
        use = egg:swift#proxy
        object_post_as_copy = yes
        """)

        conffile = tempfile.NamedTemporaryFile()
        conffile.write(proxy_conf)
        conffile.flush()

        ssc = copy.filter_factory({
            'object_post_as_copy': 'no',
            '__file__': conffile.name
        })("no app here")

        self.assertEqual(ssc.object_post_as_copy, False)


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
            None, FakeMemcache(), account_ring=FakeRing(),
            container_ring=FakeRing(), logger=self.logger)
        self.ssc = copy.filter_factory({
            'object_post_as_copy': 'yes',
        })(self.app)
        self.ssc.logger = self.app.logger
        self.policy = POLICIES.default
        self.app.container_info = dict(self.container_info)

    def test_COPY_with_ranges(self):
        req = swob.Request.blank(
            '/v1/a/c/o', method='COPY',
            headers={'Destination': 'c1/o',
                     'Range': 'bytes=5-10'})
        # turn a real body into fragments
        segment_size = self.policy.ec_segment_size
        real_body = ('asdf' * segment_size)[:-10]

        # split it up into chunks
        chunks = [real_body[x:x + segment_size]
                  for x in range(0, len(real_body), segment_size)]

        # we need only first chunk to rebuild 5-10 range
        fragments = self.policy.pyeclib_driver.encode(chunks[0])
        fragment_payloads = []
        fragment_payloads.append(fragments)

        node_fragments = zip(*fragment_payloads)
        self.assertEqual(len(node_fragments),
                         self.policy.object_ring.replicas)  # sanity
        headers = {'X-Object-Sysmeta-Ec-Content-Length': str(len(real_body))}
        responses = [(200, ''.join(node_fragments[i]), headers)
                     for i in range(POLICIES.default.ec_ndata)]
        responses += [(201, '', {})] * self.policy.object_ring.replicas
        status_codes, body_iter, headers = zip(*responses)
        expect_headers = {
            'X-Obj-Metadata-Footer': 'yes',
            'X-Obj-Multiphase-Commit': 'yes'
        }
        with set_http_connect(*status_codes, body_iter=body_iter,
                              headers=headers, expect_headers=expect_headers):
            resp = req.get_response(self.ssc)
        self.assertEqual(resp.status_int, 201)

    def test_COPY_with_invalid_ranges(self):
        # real body size is segment_size - 10 (just 1 segment)
        segment_size = self.policy.ec_segment_size
        real_body = ('a' * segment_size)[:-10]

        # range is out of real body but in segment size
        self._test_invalid_ranges('COPY', real_body,
                                  segment_size, '%s-' % (segment_size - 10))
        # range is out of both real body and segment size
        self._test_invalid_ranges('COPY', real_body,
                                  segment_size, '%s-' % (segment_size + 10))

    def _test_invalid_ranges(self, method, real_body, segment_size, req_range):
        # make a request with range starts from more than real size.
        body_etag = md5(real_body).hexdigest()
        req = swob.Request.blank(
            '/v1/a/c/o', method=method,
            headers={'Destination': 'c1/o',
                     'Range': 'bytes=%s' % (req_range)})

        fragments = self.policy.pyeclib_driver.encode(real_body)
        fragment_payloads = [fragments]

        node_fragments = zip(*fragment_payloads)
        self.assertEqual(len(node_fragments),
                         self.policy.object_ring.replicas)  # sanity
        headers = {'X-Object-Sysmeta-Ec-Content-Length': str(len(real_body)),
                   'X-Object-Sysmeta-Ec-Etag': body_etag}
        start = int(req_range.split('-')[0])
        self.assertTrue(start >= 0)  # sanity
        title, exp = swob.RESPONSE_REASONS[416]
        range_not_satisfiable_body = \
            '<html><h1>%s</h1><p>%s</p></html>' % (title, exp)
        if start >= segment_size:
            responses = [(416, range_not_satisfiable_body, headers)
                         for i in range(POLICIES.default.ec_ndata)]
        else:
            responses = [(200, ''.join(node_fragments[i]), headers)
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
