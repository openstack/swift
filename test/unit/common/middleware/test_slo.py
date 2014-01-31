# -*- coding: utf-8 -*-
# Copyright (c) 2013 OpenStack Foundation
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
import unittest
from copy import deepcopy
from mock import patch
from hashlib import md5
from swift.common import swob, utils
from swift.common.exceptions import ListingIterError, SegmentError
from swift.common.middleware import slo
from swift.common.utils import json, split_path
from swift.common.swob import Request, Response, HTTPException


test_xml_data = '''<?xml version="1.0" encoding="UTF-8"?>
<static_large_object>
<object_segment>
<path>/cont/object</path>
<etag>etagoftheobjectsegment</etag>
<size_bytes>100</size_bytes>
</object_segment>
</static_large_object>
'''
test_json_data = json.dumps([{'path': '/cont/object',
                              'etag': 'etagoftheobjectsegment',
                              'size_bytes': 100}])


def fake_start_response(*args, **kwargs):
    pass


class FakeSwift(object):
    def __init__(self):
        self._calls = []
        self.req_method_paths = []
        self.uploaded = {}
        # mapping of (method, path) --> (response class, headers, body)
        self._responses = {}

    def __call__(self, env, start_response):
        method = env['REQUEST_METHOD']
        path = env['PATH_INFO']
        _, acc, cont, obj = split_path(env['PATH_INFO'], 0, 4,
                                       rest_with_last=True)

        headers = swob.Request(env).headers
        self._calls.append((method, path, headers))

        try:
            resp_class, raw_headers, body = self._responses[(method, path)]
            headers = swob.HeaderKeyDict(raw_headers)
        except KeyError:
            if method == 'HEAD' and ('GET', path) in self._responses:
                resp_class, raw_headers, _ = self._responses[('GET', path)]
                body = None
                headers = swob.HeaderKeyDict(raw_headers)
            elif method == 'GET' and obj and path in self.uploaded:
                resp_class = swob.HTTPOk
                headers, body = self.uploaded[path]
            else:
                raise

        # simulate object PUT
        if method == 'PUT' and obj:
            input = env['wsgi.input'].read()
            etag = md5(input).hexdigest()
            headers.setdefault('Etag', etag)
            headers.setdefault('Content-Length', len(input))

            # keep it for subsequent GET requests later
            self.uploaded[path] = (deepcopy(headers), input)
            if "CONTENT_TYPE" in env:
                self.uploaded[path][0]['Content-Type'] = env["CONTENT_TYPE"]

        req = swob.Request(env)
        # range requests ought to work, hence conditional_response=True
        resp = resp_class(req=req, headers=headers, body=body,
                          conditional_response=True)
        return resp(env, start_response)

    @property
    def calls(self):
        return [(method, path) for method, path, headers in self._calls]

    @property
    def calls_with_headers(self):
        return self._calls

    @property
    def call_count(self):
        return len(self._calls)

    def register(self, method, path, response_class, headers, body):
        self._responses[(method, path)] = (response_class, headers, body)


class SloTestCase(unittest.TestCase):
    def setUp(self):
        self.app = FakeSwift()
        self.slo = slo.filter_factory({})(self.app)
        self.slo.min_segment_size = 1

    def call_app(self, req, app=None, expect_exception=False):
        if app is None:
            app = self.app

        req.headers.setdefault("User-Agent", "Mozzarella Foxfire")

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

    def call_slo(self, req, **kwargs):
        return self.call_app(req, app=self.slo, **kwargs)


class TestSloMiddleware(SloTestCase):

    def setUp(self):
        super(TestSloMiddleware, self).setUp()

        self.app.register(
            'GET', '/', swob.HTTPOk, {}, 'passed')
        self.app.register(
            'PUT', '/', swob.HTTPOk, {}, 'passed')

    def test_handle_multipart_no_obj(self):
        req = Request.blank('/')
        resp_iter = self.slo(req.environ, fake_start_response)
        self.assertEquals(self.app.calls, [('GET', '/')])
        self.assertEquals(''.join(resp_iter), 'passed')

    def test_slo_header_assigned(self):
        req = Request.blank(
            '/v1/a/c/o', headers={'x-static-large-object': "true"},
            environ={'REQUEST_METHOD': 'PUT'})
        resp = ''.join(self.slo(req.environ, fake_start_response))
        self.assert_(
            resp.startswith('X-Static-Large-Object is a reserved header'))

    def test_parse_input(self):
        self.assertRaises(HTTPException, slo.parse_input, 'some non json')
        data = json.dumps(
            [{'path': '/cont/object', 'etag': 'etagoftheobjecitsegment',
              'size_bytes': 100}])
        self.assertEquals('/cont/object',
                          slo.parse_input(data)[0]['path'])


class TestSloPutManifest(SloTestCase):

    def setUp(self):
        super(TestSloPutManifest, self).setUp()

        self.app.register(
            'GET', '/', swob.HTTPOk, {}, 'passed')
        self.app.register(
            'PUT', '/', swob.HTTPOk, {}, 'passed')

        self.app.register(
            'HEAD', '/v1/AUTH_test/cont/object',
            swob.HTTPOk,
            {'Content-Length': '100', 'Etag': 'etagoftheobjectsegment'},
            None)
        self.app.register(
            'HEAD', '/v1/AUTH_test/cont/object\xe2\x99\xa1',
            swob.HTTPOk,
            {'Content-Length': '100', 'Etag': 'etagoftheobjectsegment'},
            None)
        self.app.register(
            'HEAD', '/v1/AUTH_test/cont/small_object',
            swob.HTTPOk,
            {'Content-Length': '10', 'Etag': 'etagoftheobjectsegment'},
            None)
        self.app.register(
            'PUT', '/v1/AUTH_test/c/man', swob.HTTPCreated, {}, None)
        self.app.register(
            'DELETE', '/v1/AUTH_test/c/man', swob.HTTPNoContent, {}, None)

        self.app.register(
            'HEAD', '/v1/AUTH_test/checktest/a_1',
            swob.HTTPOk,
            {'Content-Length': '1', 'Etag': 'a'},
            None)
        self.app.register(
            'HEAD', '/v1/AUTH_test/checktest/badreq',
            swob.HTTPBadRequest, {}, None)
        self.app.register(
            'HEAD', '/v1/AUTH_test/checktest/b_2',
            swob.HTTPOk,
            {'Content-Length': '2', 'Etag': 'b',
             'Last-Modified': 'Fri, 01 Feb 2012 20:38:36 GMT'},
            None)
        self.app.register(
            'GET', '/v1/AUTH_test/checktest/slob',
            swob.HTTPOk,
            {'X-Static-Large-Object': 'true', 'Etag': 'slob-etag'},
            None)

        self.app.register(
            'PUT', '/v1/AUTH_test/checktest/man_3', swob.HTTPCreated, {}, None)

    def test_put_manifest_too_quick_fail(self):
        req = Request.blank('/v1/a/c/o')
        req.content_length = self.slo.max_manifest_size + 1
        try:
            self.slo.handle_multipart_put(req, fake_start_response)
        except HTTPException as e:
            pass
        self.assertEquals(e.status_int, 413)

        with patch.object(self.slo, 'max_manifest_segments', 0):
            req = Request.blank('/v1/a/c/o', body=test_json_data)
            e = None
            try:
                self.slo.handle_multipart_put(req, fake_start_response)
            except HTTPException as e:
                pass
            self.assertEquals(e.status_int, 413)

        with patch.object(self.slo, 'min_segment_size', 1000):
            req = Request.blank('/v1/a/c/o', body=test_json_data)
            try:
                self.slo.handle_multipart_put(req, fake_start_response)
            except HTTPException as e:
                pass
            self.assertEquals(e.status_int, 400)

        req = Request.blank('/v1/a/c/o', headers={'X-Copy-From': 'lala'})
        try:
            self.slo.handle_multipart_put(req, fake_start_response)
        except HTTPException as e:
            pass
        self.assertEquals(e.status_int, 405)

        # ignores requests to /
        req = Request.blank(
            '/?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, body=test_json_data)
        self.assertEquals(
            self.slo.handle_multipart_put(req, fake_start_response),
            ['passed'])

    def test_handle_multipart_put_success(self):
        req = Request.blank(
            '/v1/AUTH_test/c/man?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, headers={'Accept': 'test'},
            body=test_json_data)
        self.assertTrue('X-Static-Large-Object' not in req.headers)

        def my_fake_start_response(*args, **kwargs):
            gen_etag = '"' + md5('etagoftheobjectsegment').hexdigest() + '"'
            self.assertTrue(('Etag', gen_etag) in args[1])

        self.slo(req.environ, my_fake_start_response)
        self.assertTrue('X-Static-Large-Object' in req.headers)

    def test_handle_multipart_put_success_allow_small_last_segment(self):
        with patch.object(self.slo, 'min_segment_size', 50):
            test_json_data = json.dumps([{'path': '/cont/object',
                                          'etag': 'etagoftheobjectsegment',
                                          'size_bytes': 100},
                                         {'path': '/cont/small_object',
                                          'etag': 'etagoftheobjectsegment',
                                          'size_bytes': 10}])
            req = Request.blank(
                '/v1/AUTH_test/c/man?multipart-manifest=put',
                environ={'REQUEST_METHOD': 'PUT'}, headers={'Accept': 'test'},
                body=test_json_data)
            self.assertTrue('X-Static-Large-Object' not in req.headers)
            self.slo(req.environ, fake_start_response)
            self.assertTrue('X-Static-Large-Object' in req.headers)

    def test_handle_multipart_put_success_unicode(self):
        test_json_data = json.dumps([{'path': u'/cont/object\u2661',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 100}])
        req = Request.blank(
            '/v1/AUTH_test/c/man?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, headers={'Accept': 'test'},
            body=test_json_data)
        self.assertTrue('X-Static-Large-Object' not in req.headers)
        self.slo(req.environ, fake_start_response)
        self.assertTrue('X-Static-Large-Object' in req.headers)
        self.assertTrue(req.environ['PATH_INFO'], '/cont/object\xe2\x99\xa1')

    def test_handle_multipart_put_no_xml(self):
        req = Request.blank(
            '/test_good/AUTH_test/c/man?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, headers={'Accept': 'test'},
            body=test_xml_data)
        no_xml = self.slo(req.environ, fake_start_response)
        self.assertEquals(no_xml, ['Manifest must be valid json.'])

    def test_handle_multipart_put_bad_data(self):
        bad_data = json.dumps([{'path': '/cont/object',
                                'etag': 'etagoftheobj',
                                'size_bytes': 'lala'}])
        req = Request.blank(
            '/test_good/AUTH_test/c/man?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, body=bad_data)
        self.assertRaises(HTTPException, self.slo.handle_multipart_put, req,
                          fake_start_response)

        for bad_data in [
                json.dumps([{'path': '/cont', 'etag': 'etagoftheobj',
                             'size_bytes': 100}]),
                json.dumps('asdf'), json.dumps(None), json.dumps(5),
                'not json', '1234', None, '', json.dumps({'path': None}),
                json.dumps([{'path': '/cont/object', 'etag': None,
                             'size_bytes': 12}]),
                json.dumps([{'path': '/cont/object', 'etag': 'asdf',
                             'size_bytes': 'sd'}]),
                json.dumps([{'path': 12, 'etag': 'etagoftheobj',
                             'size_bytes': 100}]),
                json.dumps([{'path': u'/cont/object\u2661',
                             'etag': 'etagoftheobj', 'size_bytes': 100}]),
                json.dumps([{'path': 12, 'size_bytes': 100}]),
                json.dumps([{'path': 12, 'size_bytes': 100}]),
                json.dumps([{'path': None, 'etag': 'etagoftheobj',
                             'size_bytes': 100}])]:
            req = Request.blank(
                '/v1/AUTH_test/c/man?multipart-manifest=put',
                environ={'REQUEST_METHOD': 'PUT'}, body=bad_data)
            self.assertRaises(HTTPException, self.slo.handle_multipart_put,
                              req, fake_start_response)

    def test_handle_multipart_put_check_data(self):
        good_data = json.dumps(
            [{'path': '/checktest/a_1', 'etag': 'a', 'size_bytes': '1'},
             {'path': '/checktest/b_2', 'etag': 'b', 'size_bytes': '2'}])
        req = Request.blank(
            '/v1/AUTH_test/checktest/man_3?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, body=good_data)
        status, headers, body = self.call_slo(req)
        self.assertEquals(self.app.call_count, 3)

        # go behind SLO's back and see what actually got stored
        req = Request.blank(
            '/v1/AUTH_test/checktest/man_3?multipart-manifest=get',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_app(req)
        headers = dict(headers)
        manifest_data = json.loads(body)
        self.assert_(headers['Content-Type'].endswith(';swift_bytes=3'))
        self.assertEquals(len(manifest_data), 2)
        self.assertEquals(manifest_data[0]['hash'], 'a')
        self.assertEquals(manifest_data[0]['bytes'], 1)
        self.assert_(not manifest_data[0]['last_modified'].startswith('2012'))
        self.assert_(manifest_data[1]['last_modified'].startswith('2012'))

    def test_handle_multipart_put_check_data_bad(self):
        bad_data = json.dumps(
            [{'path': '/checktest/a_1', 'etag': 'a', 'size_bytes': '2'},
             {'path': '/checktest/badreq', 'etag': 'a', 'size_bytes': '1'},
             {'path': '/checktest/b_2', 'etag': 'not-b', 'size_bytes': '2'},
             {'path': '/checktest/slob', 'etag': 'not-slob',
              'size_bytes': '2'}])
        req = Request.blank(
            '/v1/AUTH_test/checktest/man?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'Accept': 'application/json'},
            body=bad_data)

        status, headers, body = self.call_slo(req)
        self.assertEquals(self.app.call_count, 5)
        errors = json.loads(body)['Errors']
        self.assertEquals(len(errors), 5)
        self.assertEquals(errors[0][0], '/checktest/a_1')
        self.assertEquals(errors[0][1], 'Size Mismatch')
        self.assertEquals(errors[1][0], '/checktest/badreq')
        self.assertEquals(errors[1][1], '400 Bad Request')
        self.assertEquals(errors[2][0], '/checktest/b_2')
        self.assertEquals(errors[2][1], 'Etag Mismatch')
        self.assertEquals(errors[3][0], '/checktest/slob')
        self.assertEquals(errors[3][1], 'Size Mismatch')
        self.assertEquals(errors[4][0], '/checktest/slob')
        self.assertEquals(errors[4][1], 'Etag Mismatch')


class TestSloDeleteManifest(SloTestCase):

    def setUp(self):
        super(TestSloDeleteManifest, self).setUp()

        _submanifest_data = json.dumps(
            [{'name': '/deltest/b_2', 'hash': 'a', 'bytes': '1'},
             {'name': '/deltest/c_3', 'hash': 'b', 'bytes': '2'}])

        self.app.register(
            'GET', '/v1/AUTH_test/deltest/man_404',
            swob.HTTPNotFound, {}, None)
        self.app.register(
            'GET', '/v1/AUTH_test/deltest/man',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            json.dumps([{'name': '/deltest/gone', 'hash': 'a', 'bytes': '1'},
                        {'name': '/deltest/b_2', 'hash': 'b', 'bytes': '2'}]))
        self.app.register(
            'DELETE', '/v1/AUTH_test/deltest/man',
            swob.HTTPNoContent, {}, None)
        self.app.register(
            'GET', '/v1/AUTH_test/deltest/man-all-there',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            json.dumps([{'name': '/deltest/b_2', 'hash': 'a', 'bytes': '1'},
                        {'name': '/deltest/c_3', 'hash': 'b', 'bytes': '2'}]))
        self.app.register(
            'DELETE', '/v1/AUTH_test/deltest/gone',
            swob.HTTPNotFound, {}, None)
        self.app.register(
            'GET', '/v1/AUTH_test/deltest/a_1',
            swob.HTTPOk, {'Content-Length': '1'}, 'a')
        self.app.register(
            'DELETE', '/v1/AUTH_test/deltest/a_1',
            swob.HTTPNoContent, {}, None)
        self.app.register(
            'DELETE', '/v1/AUTH_test/deltest/b_2',
            swob.HTTPNoContent, {}, None)
        self.app.register(
            'DELETE', '/v1/AUTH_test/deltest/c_3',
            swob.HTTPNoContent, {}, None)
        self.app.register(
            'DELETE', '/v1/AUTH_test/deltest/d_3',
            swob.HTTPNoContent, {}, None)

        self.app.register(
            'GET', '/v1/AUTH_test/deltest/manifest-with-submanifest',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            json.dumps([{'name': '/deltest/a_1',
                         'hash': 'a', 'bytes': '1'},
                        {'name': '/deltest/submanifest', 'sub_slo': True,
                         'hash': 'submanifest-etag',
                         'bytes': len(_submanifest_data)},
                        {'name': '/deltest/d_3',
                         'hash': 'd', 'bytes': '3'}]))
        self.app.register(
            'DELETE', '/v1/AUTH_test/deltest/manifest-with-submanifest',
            swob.HTTPNoContent, {}, None)

        self.app.register(
            'GET', '/v1/AUTH_test/deltest/submanifest',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            _submanifest_data)
        self.app.register(
            'DELETE', '/v1/AUTH_test/deltest/submanifest',
            swob.HTTPNoContent, {}, None)

        self.app.register(
            'GET', '/v1/AUTH_test/deltest/manifest-missing-submanifest',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            json.dumps([{'name': '/deltest/a_1', 'hash': 'a', 'bytes': '1'},
                        {'name': '/deltest/missing-submanifest',
                         'hash': 'a', 'bytes': '2', 'sub_slo': True},
                        {'name': '/deltest/d_3', 'hash': 'd', 'bytes': '3'}]))
        self.app.register(
            'DELETE', '/v1/AUTH_test/deltest/manifest-missing-submanifest',
            swob.HTTPNoContent, {}, None)

        self.app.register(
            'GET', '/v1/AUTH_test/deltest/missing-submanifest',
            swob.HTTPNotFound, {}, None)

        self.app.register(
            'GET', '/v1/AUTH_test/deltest/manifest-badjson',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            "[not {json (at ++++all")

        self.app.register(
            'GET', '/v1/AUTH_test/deltest/manifest-with-unauth-segment',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            json.dumps([{'name': '/deltest/a_1', 'hash': 'a', 'bytes': '1'},
                        {'name': '/deltest-unauth/q_17',
                         'hash': '11', 'bytes': '17'}]))
        self.app.register(
            'DELETE', '/v1/AUTH_test/deltest/manifest-with-unauth-segment',
            swob.HTTPNoContent, {}, None)

        self.app.register(
            'DELETE', '/v1/AUTH_test/deltest-unauth/q_17',
            swob.HTTPUnauthorized, {}, None)

    def test_handle_multipart_delete_man(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/man',
            environ={'REQUEST_METHOD': 'DELETE'})
        self.slo(req.environ, fake_start_response)
        self.assertEquals(self.app.call_count, 1)

    def test_handle_multipart_delete_bad_utf8(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/man\xff\xfe?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_slo(req)
        self.assertEquals(status, '200 OK')
        resp_data = json.loads(body)
        self.assertEquals(resp_data['Response Status'],
                          '412 Precondition Failed')

    def test_handle_multipart_delete_whole_404(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/man_404?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_slo(req)
        resp_data = json.loads(body)
        self.assertEquals(self.app.calls,
                          [('GET', '/v1/AUTH_test/deltest/man_404')])
        self.assertEquals(resp_data['Response Status'], '200 OK')
        self.assertEquals(resp_data['Response Body'], '')
        self.assertEquals(resp_data['Number Deleted'], 0)
        self.assertEquals(resp_data['Number Not Found'], 1)
        self.assertEquals(resp_data['Errors'], [])

    def test_handle_multipart_delete_segment_404(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/man?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_slo(req)
        resp_data = json.loads(body)
        self.assertEquals(self.app.calls,
                          [('GET', '/v1/AUTH_test/deltest/man'),
                           ('DELETE', '/v1/AUTH_test/deltest/gone'),
                           ('DELETE', '/v1/AUTH_test/deltest/b_2'),
                           ('DELETE', '/v1/AUTH_test/deltest/man')])
        self.assertEquals(resp_data['Response Status'], '200 OK')
        self.assertEquals(resp_data['Number Deleted'], 2)
        self.assertEquals(resp_data['Number Not Found'], 1)

    def test_handle_multipart_delete_whole(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/man-all-there?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE'})
        self.call_slo(req)
        self.assertEquals(self.app.calls,
                          [('GET', '/v1/AUTH_test/deltest/man-all-there'),
                           ('DELETE', '/v1/AUTH_test/deltest/b_2'),
                           ('DELETE', '/v1/AUTH_test/deltest/c_3'),
                           ('DELETE', '/v1/AUTH_test/deltest/man-all-there')])

    def test_handle_multipart_delete_nested(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/manifest-with-submanifest?' +
            'multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE'})
        self.call_slo(req)
        self.assertEquals(
            set(self.app.calls),
            set([('GET', '/v1/AUTH_test/deltest/manifest-with-submanifest'),
                 ('GET', '/v1/AUTH_test/deltest/submanifest'),
                 ('DELETE', '/v1/AUTH_test/deltest/a_1'),
                 ('DELETE', '/v1/AUTH_test/deltest/b_2'),
                 ('DELETE', '/v1/AUTH_test/deltest/c_3'),
                 ('DELETE', '/v1/AUTH_test/deltest/submanifest'),
                 ('DELETE', '/v1/AUTH_test/deltest/d_3'),
                 ('DELETE', '/v1/AUTH_test/deltest/' +
                  'manifest-with-submanifest')]))

    def test_handle_multipart_delete_nested_too_many_segments(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/manifest-with-submanifest?' +
            'multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        with patch.object(slo, 'MAX_BUFFERED_SLO_SEGMENTS', 1):
            status, headers, body = self.call_slo(req)
        self.assertEquals(status, '200 OK')
        resp_data = json.loads(body)
        self.assertEquals(resp_data['Response Status'], '400 Bad Request')
        self.assertEquals(resp_data['Response Body'],
                          'Too many buffered slo segments to delete.')

    def test_handle_multipart_delete_nested_404(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/manifest-missing-submanifest' +
            '?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_slo(req)
        resp_data = json.loads(body)
        self.assertEquals(self.app.calls,
                          [('GET', '/v1/AUTH_test/deltest/' +
                            'manifest-missing-submanifest'),
                           ('DELETE', '/v1/AUTH_test/deltest/a_1'),
                           ('GET', '/v1/AUTH_test/deltest/' +
                            'missing-submanifest'),
                           ('DELETE', '/v1/AUTH_test/deltest/d_3'),
                           ('DELETE', '/v1/AUTH_test/deltest/' +
                            'manifest-missing-submanifest')])
        self.assertEquals(resp_data['Response Status'], '200 OK')
        self.assertEquals(resp_data['Response Body'], '')
        self.assertEquals(resp_data['Number Deleted'], 3)
        self.assertEquals(resp_data['Number Not Found'], 1)
        self.assertEquals(resp_data['Errors'], [])

    def test_handle_multipart_delete_nested_401(self):
        self.app.register(
            'GET', '/v1/AUTH_test/deltest/submanifest',
            swob.HTTPUnauthorized, {}, None)

        req = Request.blank(
            ('/v1/AUTH_test/deltest/manifest-with-submanifest' +
             '?multipart-manifest=delete'),
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_slo(req)
        self.assertEquals(status, '200 OK')
        resp_data = json.loads(body)
        self.assertEquals(resp_data['Response Status'], '400 Bad Request')
        self.assertEquals(resp_data['Errors'],
                          [['/deltest/submanifest', '401 Unauthorized']])

    def test_handle_multipart_delete_nested_500(self):
        self.app.register(
            'GET', '/v1/AUTH_test/deltest/submanifest',
            swob.HTTPServerError, {}, None)

        req = Request.blank(
            ('/v1/AUTH_test/deltest/manifest-with-submanifest' +
             '?multipart-manifest=delete'),
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_slo(req)
        self.assertEquals(status, '200 OK')
        resp_data = json.loads(body)
        self.assertEquals(resp_data['Response Status'], '400 Bad Request')
        self.assertEquals(resp_data['Errors'],
                          [['/deltest/submanifest',
                            'Unable to load SLO manifest or segment.']])

    def test_handle_multipart_delete_not_a_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/a_1?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_slo(req)
        resp_data = json.loads(body)
        self.assertEquals(self.app.calls,
                          [('GET', '/v1/AUTH_test/deltest/a_1')])
        self.assertEquals(resp_data['Response Status'], '400 Bad Request')
        self.assertEquals(resp_data['Response Body'], '')
        self.assertEquals(resp_data['Number Deleted'], 0)
        self.assertEquals(resp_data['Number Not Found'], 0)
        self.assertEquals(resp_data['Errors'],
                          [['/deltest/a_1', 'Not an SLO manifest']])

    def test_handle_multipart_delete_bad_json(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/manifest-badjson?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_slo(req)
        resp_data = json.loads(body)
        self.assertEquals(self.app.calls,
                          [('GET', '/v1/AUTH_test/deltest/manifest-badjson')])
        self.assertEquals(resp_data['Response Status'], '400 Bad Request')
        self.assertEquals(resp_data['Response Body'], '')
        self.assertEquals(resp_data['Number Deleted'], 0)
        self.assertEquals(resp_data['Number Not Found'], 0)
        self.assertEquals(resp_data['Errors'],
                          [['/deltest/manifest-badjson',
                            'Unable to load SLO manifest']])

    def test_handle_multipart_delete_401(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/manifest-with-unauth-segment' +
            '?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_slo(req)
        resp_data = json.loads(body)
        self.assertEquals(self.app.calls,
                          [('GET', '/v1/AUTH_test/deltest/' +
                            'manifest-with-unauth-segment'),
                           ('DELETE', '/v1/AUTH_test/deltest/a_1'),
                           ('DELETE', '/v1/AUTH_test/deltest-unauth/q_17'),
                           ('DELETE', '/v1/AUTH_test/deltest/' +
                            'manifest-with-unauth-segment')])
        self.assertEquals(resp_data['Response Status'], '400 Bad Request')
        self.assertEquals(resp_data['Response Body'], '')
        self.assertEquals(resp_data['Number Deleted'], 2)
        self.assertEquals(resp_data['Number Not Found'], 0)
        self.assertEquals(resp_data['Errors'],
                          [['/deltest-unauth/q_17', '401 Unauthorized']])


class TestSloHeadManifest(SloTestCase):
    def setUp(self):
        super(TestSloHeadManifest, self).setUp()

        self._manifest_json = json.dumps([
            {'name': '/gettest/seg01',
             'bytes': '100',
             'hash': 'seg01-hash',
             'content_type': 'text/plain',
             'last_modified': '2013-11-19T11:33:45.137446'},
            {'name': '/gettest/seg02',
             'bytes': '200',
             'hash': 'seg02-hash',
             'content_type': 'text/plain',
             'last_modified': '2013-11-19T11:33:45.137447'}])

        self.app.register(
            'GET', '/v1/AUTH_test/headtest/man',
            swob.HTTPOk, {'Content-Length': str(len(self._manifest_json)),
                          'X-Static-Large-Object': 'true',
                          'Etag': md5(self._manifest_json).hexdigest()},
            self._manifest_json)

    def test_etag_is_hash_of_segment_etags(self):
        req = Request.blank(
            '/v1/AUTH_test/headtest/man',
            environ={'REQUEST_METHOD': 'HEAD'})
        status, headers, body = self.call_slo(req)
        headers = swob.HeaderKeyDict(headers)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers.get('Etag', '').strip("'\""),
                         md5("seg01-hashseg02-hash").hexdigest())
        self.assertEqual(body, '')  # it's a HEAD request, after all


class TestSloGetManifest(SloTestCase):
    def setUp(self):
        super(TestSloGetManifest, self).setUp()

        _bc_manifest_json = json.dumps(
            [{'name': '/gettest/b_10', 'hash': 'b', 'bytes': '10',
              'content_type': 'text/plain'},
             {'name': '/gettest/c_15', 'hash': 'c', 'bytes': '15',
              'content_type': 'text/plain'}])

        # some plain old objects
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/a_5',
            swob.HTTPOk, {'Content-Length': '5',
                          'Etag': 'a'},
            'a' * 5)
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/b_10',
            swob.HTTPOk, {'Content-Length': '10',
                          'Etag': 'b'},
            'b' * 10)
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/c_15',
            swob.HTTPOk, {'Content-Length': '15',
                          'Etag': 'c'},
            'c' * 15)
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/d_20',
            swob.HTTPOk, {'Content-Length': '20',
                          'Etag': 'd'},
            'd' * 20)

        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-bc',
            swob.HTTPOk, {'Content-Type': 'application/json;swift_bytes=25',
                          'X-Static-Large-Object': 'true',
                          'X-Object-Meta-Plant': 'Ficus'},
            _bc_manifest_json)

        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-abcd',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            json.dumps([{'name': '/gettest/a_5', 'hash': 'a',
                         'content_type': 'text/plain', 'bytes': '5'},
                        {'name': '/gettest/manifest-bc', 'sub_slo': True,
                         'content_type': 'application/json;swift_bytes=25',
                         'hash': 'manifest-bc',
                         'bytes': len(_bc_manifest_json)},
                        {'name': '/gettest/d_20', 'hash': 'd',
                         'content_type': 'text/plain', 'bytes': '20'}]))

        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-badjson',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true',
                          'X-Object-Meta-Fish': 'Bass'},
            "[not {json (at ++++all")

    def test_get_manifest_passthrough(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-bc?multipart-manifest=get',
            environ={'REQUEST_METHOD': 'GET',
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertTrue(
            ('Content-Type', 'application/json; charset=utf-8') in headers,
            headers)
        try:
            resp_data = json.loads(body)
        except json.JSONDecodeError:
            resp_data = None

        self.assertEqual(
            resp_data,
            [{'hash': 'b', 'bytes': '10', 'name': '/gettest/b_10',
              'content_type': 'text/plain'},
             {'hash': 'c', 'bytes': '15', 'name': '/gettest/c_15',
              'content_type': 'text/plain'}],
            body)

    def test_get_nonmanifest_passthrough(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/a_5',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(body, 'aaaaa')

    def test_get_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-bc',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)
        headers = swob.HeaderKeyDict(headers)

        manifest_etag = md5("bc").hexdigest()
        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Content-Length'], '25')
        self.assertEqual(headers['Etag'], '"%s"' % manifest_etag)
        self.assertEqual(headers['X-Object-Meta-Plant'], 'Ficus')
        self.assertEqual(body, 'bbbbbbbbbbccccccccccccccc')

        for _, _, hdrs in self.app.calls_with_headers[1:]:
            ua = hdrs.get("User-Agent", "")
            self.assertTrue("SLO MultipartGET" in ua)
            self.assertFalse("SLO MultipartGET SLO MultipartGET" in ua)
        # the first request goes through unaltered
        self.assertFalse(
            "SLO MultipartGET" in self.app.calls_with_headers[0][2])

    def test_get_manifest_with_submanifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)
        headers = swob.HeaderKeyDict(headers)

        manifest_etag = md5("a" + "manifest-bc" + "d").hexdigest()
        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Content-Length'], '50')
        self.assertEqual(headers['Etag'], '"%s"' % manifest_etag)
        self.assertEqual(
            body, 'aaaaabbbbbbbbbbcccccccccccccccdddddddddddddddddddd')

    def test_range_get_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=3-17'})
        status, headers, body = self.call_slo(req)
        headers = swob.HeaderKeyDict(headers)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Content-Length'], '15')
        self.assertTrue('Etag' not in headers)
        self.assertEqual(body, 'aabbbbbbbbbbccc')

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/a_5'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
             ('GET', '/v1/AUTH_test/gettest/b_10'),
             ('GET', '/v1/AUTH_test/gettest/c_15')])

        headers = [c[2] for c in self.app.calls_with_headers]
        self.assertEqual(headers[0].get('Range'), 'bytes=3-17')
        self.assertEqual(headers[1].get('Range'), None)
        self.assertEqual(headers[2].get('Range'), 'bytes=3-')
        self.assertEqual(headers[3].get('Range'), None)
        self.assertEqual(headers[4].get('Range'), None)
        self.assertEqual(headers[5].get('Range'), 'bytes=0-2')

    def test_range_get_manifest_on_segment_boundaries(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=5-29'})
        status, headers, body = self.call_slo(req)
        headers = swob.HeaderKeyDict(headers)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Content-Length'], '25')
        self.assertTrue('Etag' not in headers)
        self.assertEqual(body, 'bbbbbbbbbbccccccccccccccc')

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
             ('GET', '/v1/AUTH_test/gettest/b_10'),
             ('GET', '/v1/AUTH_test/gettest/c_15')])

        headers = [c[2] for c in self.app.calls_with_headers]
        self.assertEqual(headers[0].get('Range'), 'bytes=5-29')
        self.assertEqual(headers[1].get('Range'), None)
        self.assertEqual(headers[2].get('Range'), None)
        self.assertEqual(headers[3].get('Range'), None)
        self.assertEqual(headers[4].get('Range'), None)

    def test_range_get_manifest_first_byte(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=0-0'})
        status, headers, body = self.call_slo(req)
        headers = swob.HeaderKeyDict(headers)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Content-Length'], '1')
        self.assertEqual(body, 'a')

        # Make sure we don't get any objects we don't need, including
        # submanifests.
        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/a_5')])

    def test_range_get_manifest_overlapping_end(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=45-55'})
        status, headers, body = self.call_slo(req)
        headers = swob.HeaderKeyDict(headers)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Content-Length'], '5')
        self.assertEqual(body, 'ddddd')

    def test_range_get_manifest_unsatisfiable(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=100-200'})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '416 Requested Range Not Satisfiable')

    def test_multi_range_get_manifest(self):
        # SLO doesn't support multi-range GETs. The way that you express
        # "unsupported" in HTTP is to return a 200 and the whole entity.
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=0-0,2-2'})
        status, headers, body = self.call_slo(req)
        headers = swob.HeaderKeyDict(headers)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Content-Length'], '50')
        self.assertEqual(
            body, 'aaaaabbbbbbbbbbcccccccccccccccdddddddddddddddddddd')

    def test_get_segment_with_non_ascii_name(self):
        segment_body = u"a møøse once bit my sister".encode("utf-8")
        self.app.register(
            'GET', u'/v1/AUTH_test/ünicode/öbject-segment'.encode('utf-8'),
            swob.HTTPOk, {'Content-Length': str(len(segment_body)),
                          'Etag': "moose"},
            segment_body)

        manifest_json = json.dumps([{'name': u'/ünicode/öbject-segment',
                                     'hash': 'moose',
                                     'content_type': 'text/plain',
                                     'bytes': len(segment_body)}])
        self.app.register(
            'GET', u'/v1/AUTH_test/ünicode/manifest'.encode('utf-8'),
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'Content-Length': str(len(manifest_json)),
                          'X-Static-Large-Object': 'true'},
            manifest_json)

        req = Request.blank(
            '/v1/AUTH_test/ünicode/manifest',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)
        headers = swob.HeaderKeyDict(headers)
        self.assertEqual(status, '200 OK')
        self.assertEqual(body, segment_body)

    def test_get_bogus_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-badjson',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)
        headers = swob.HeaderKeyDict(headers)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Content-Length'], '0')
        self.assertEqual(headers['X-Object-Meta-Fish'], 'Bass')
        self.assertEqual(body, '')

    def test_head_manifest_is_efficient(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'HEAD'})
        status, headers, body = self.call_slo(req)
        headers = swob.HeaderKeyDict(headers)

        manifest_etag = md5("a" + "manifest-bc" + "d").hexdigest()
        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Content-Length'], '50')
        self.assertEqual(headers['Etag'], '"%s"' % manifest_etag)
        self.assertEqual(body, '')
        # Note the lack of recursive descent into manifest-bc. We know the
        # content-length from the outer manifest, so there's no need for any
        # submanifest fetching here, but a naïve implementation might do it
        # anyway.
        self.assertEqual(self.app.calls, [
            ('HEAD', '/v1/AUTH_test/gettest/manifest-abcd'),
            ('GET', '/v1/AUTH_test/gettest/manifest-abcd')])

    def test_recursion_limit(self):
        # man1 points to obj1 and man2, man2 points to obj2 and man3...
        for i in xrange(20):
            self.app.register('GET', '/v1/AUTH_test/gettest/obj%d' % i,
                              swob.HTTPOk, {'Content-Type': 'text/plain',
                                            'Etag': 'hash%d' % i},
                              'body%02d' % i)

        manifest_json = json.dumps([{'name': '/gettest/obj20',
                                     'hash': 'hash20',
                                     'content_type': 'text/plain',
                                     'bytes': '6'}])
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/man%d' % i,
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true',
                          'Etag': 'man%d' % i},
            manifest_json)

        for i in xrange(19, 0, -1):
            manifest_data = [
                {'name': '/gettest/obj%d' % i,
                 'hash': 'hash%d' % i,
                 'bytes': '6',
                 'content_type': 'text/plain'},
                {'name': '/gettest/man%d' % (i + 1),
                 'hash': 'man%d' % (i + 1),
                 'sub_slo': True,
                 'bytes': len(manifest_json),
                 'content_type':
                 'application/json;swift_bytes=%d' % ((21 - i) * 6)}]

            manifest_json = json.dumps(manifest_data)
            self.app.register(
                'GET', '/v1/AUTH_test/gettest/man%d' % i,
                swob.HTTPOk, {'Content-Type': 'application/json',
                              'X-Static-Large-Object': 'true',
                              'Etag': 'man%d' % i},
                manifest_json)

        req = Request.blank(
            '/v1/AUTH_test/gettest/man1',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body, exc = self.call_slo(req, expect_exception=True)
        headers = swob.HeaderKeyDict(headers)

        self.assertTrue(isinstance(exc, ListingIterError))
        # we don't know at header-sending time that things are going to go
        # wrong, so we end up with a 200 and a truncated body
        self.assertEqual(status, '200 OK')
        self.assertEqual(body, ('body01body02body03body04body05' +
                                'body06body07body08body09body10'))
        # make sure we didn't keep asking for segments
        self.assertEqual(self.app.call_count, 20)

    def test_error_fetching_segment(self):
        self.app.register('GET', '/v1/AUTH_test/gettest/c_15',
                          swob.HTTPUnauthorized, {}, None)

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body, exc = self.call_slo(req, expect_exception=True)
        headers = swob.HeaderKeyDict(headers)

        self.assertTrue(isinstance(exc, SegmentError))
        self.assertEqual(status, '200 OK')
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
            ('GET', '/v1/AUTH_test/gettest/a_5'),
            ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
            ('GET', '/v1/AUTH_test/gettest/b_10'),
            # This one has the error, and so is the last one we fetch.
            ('GET', '/v1/AUTH_test/gettest/c_15')])

    def test_error_fetching_submanifest(self):
        self.app.register('GET', '/v1/AUTH_test/gettest/manifest-bc',
                          swob.HTTPUnauthorized, {}, None)
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body, exc = self.call_slo(req, expect_exception=True)

        self.assertTrue(isinstance(exc, ListingIterError))
        self.assertEqual("200 OK", status)
        self.assertEqual("aaaaa", body)
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
            ('GET', '/v1/AUTH_test/gettest/a_5'),
            # This one has the error, and so is the last one we fetch.
            ('GET', '/v1/AUTH_test/gettest/manifest-bc')])

    def test_error_fetching_first_segment_submanifest(self):
        # This differs from the normal submanifest error because this one
        # happens before we've actually sent any response body.
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-a',
            swob.HTTPForbidden, {}, None)
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-manifest-a',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            json.dumps([{'name': '/gettest/manifest-a', 'sub_slo': True,
                         'content_type': 'application/json;swift_bytes=5',
                         'hash': 'manifest-a',
                         'bytes': '12345'}]))

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-manifest-a',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body, exc = self.call_slo(req, expect_exception=True)

        self.assertTrue(isinstance(exc, ListingIterError))
        self.assertEqual('200 OK', status)
        self.assertEqual(body, ' ')

    def test_invalid_json_submanifest(self):
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-bc',
            swob.HTTPOk, {'Content-Type': 'application/json;swift_bytes=25',
                          'X-Static-Large-Object': 'true',
                          'X-Object-Meta-Plant': 'Ficus'},
            "[this {isn't (JSON")

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body, exc = self.call_slo(req, expect_exception=True)

        self.assertTrue(isinstance(exc, ListingIterError))
        self.assertEqual('200 OK', status)
        self.assertEqual(body, 'aaaaa')

    def test_mismatched_etag(self):
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-a-b-badetag-c',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            json.dumps([{'name': '/gettest/a_5', 'hash': 'a',
                         'content_type': 'text/plain', 'bytes': '5'},
                        {'name': '/gettest/b_10', 'hash': 'wrong!',
                         'content_type': 'text/plain', 'bytes': '10'},
                        {'name': '/gettest/c_15', 'hash': 'c',
                         'content_type': 'text/plain', 'bytes': '15'}]))

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-a-b-badetag-c',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body, exc = self.call_slo(req, expect_exception=True)

        self.assertTrue(isinstance(exc, SegmentError))
        self.assertEqual('200 OK', status)
        self.assertEqual(body, 'aaaaa')

    def test_mismatched_size(self):
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-a-b-badetag-c',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            json.dumps([{'name': '/gettest/a_5', 'hash': 'a',
                         'content_type': 'text/plain', 'bytes': '5'},
                        {'name': '/gettest/b_10', 'hash': 'b',
                         'content_type': 'text/plain', 'bytes': '999999'},
                        {'name': '/gettest/c_15', 'hash': 'c',
                         'content_type': 'text/plain', 'bytes': '15'}]))

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-a-b-badetag-c',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body, exc = self.call_slo(req, expect_exception=True)

        self.assertTrue(isinstance(exc, SegmentError))
        self.assertEqual('200 OK', status)
        self.assertEqual(body, 'aaaaa')

    def test_download_takes_too_long(self):
        the_time = [time.time()]

        def mock_time():
            return the_time[0]

        # this is just a convenient place to hang a time jump; there's nothing
        # special about the choice of is_success().
        def mock_is_success(status_int):
            the_time[0] += 7 * 3600
            return status_int // 100 == 2

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'})

        with patch.object(slo, 'time', mock_time):
            with patch.object(slo, 'is_success', mock_is_success):
                status, headers, body, exc = self.call_slo(
                    req, expect_exception=True)

        self.assertTrue(isinstance(exc, SegmentError))
        self.assertEqual(status, '200 OK')
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
            ('GET', '/v1/AUTH_test/gettest/a_5'),
            ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
            ('GET', '/v1/AUTH_test/gettest/b_10'),
            ('GET', '/v1/AUTH_test/gettest/c_15')])


class TestSloBulkLogger(unittest.TestCase):
    def test_reused_logger(self):
        slo_mware = slo.filter_factory({})('fake app')
        self.assertTrue(slo_mware.logger is slo_mware.bulk_deleter.logger)


class TestSloCopyHook(SloTestCase):
    def setUp(self):
        super(TestSloCopyHook, self).setUp()

        self.app.register(
            'GET', '/v1/AUTH_test/c/o', swob.HTTPOk,
            {'Content-Length': '3', 'Etag': 'obj-etag'}, "obj")
        self.app.register(
            'GET', '/v1/AUTH_test/c/man',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            json.dumps([{'name': '/c/o', 'hash': 'obj-etag', 'bytes': '3'}]))

        copy_hook = [None]

        # slip this guy in there to pull out the hook
        def extract_copy_hook(env, sr):
            copy_hook[0] = env['swift.copy_response_hook']
            return self.app(env, sr)

        self.slo = slo.filter_factory({})(extract_copy_hook)

        req = Request.blank('/v1/AUTH_test/c/o',
                            environ={'REQUEST_METHOD': 'GET'})
        self.slo(req.environ, fake_start_response)
        self.copy_hook = copy_hook[0]

        self.assertTrue(self.copy_hook is not None)  # sanity check

    def test_copy_hook_passthrough(self):
        req = Request.blank('/v1/AUTH_test/c/o')
        # no X-Static-Large-Object header, so do nothing
        resp = Response(request=req, status=200)

        modified_resp = self.copy_hook(req, resp)
        self.assertTrue(modified_resp is resp)

    def test_copy_hook_manifest(self):
        req = Request.blank('/v1/AUTH_test/c/o')
        resp = Response(request=req, status=200,
                        headers={"X-Static-Large-Object": "true"},
                        app_iter=[json.dumps([{'name': '/c/o',
                                               'hash': 'obj-etag',
                                               'bytes': '3'}])])

        modified_resp = self.copy_hook(req, resp)
        self.assertTrue(modified_resp is not resp)
        self.assertEqual(modified_resp.etag, md5("obj-etag").hexdigest())


class TestSwiftInfo(unittest.TestCase):
    def setUp(self):
        utils._swift_info = {}
        utils._swift_admin_info = {}

    def test_registered_defaults(self):
        mware = slo.filter_factory({})('have to pass in an app')
        swift_info = utils.get_swift_info()
        self.assertTrue('slo' in swift_info)
        self.assertEqual(swift_info['slo'].get('max_manifest_segments'),
                         mware.max_manifest_segments)
        self.assertEqual(swift_info['slo'].get('min_segment_size'),
                         mware.min_segment_size)
        self.assertEqual(swift_info['slo'].get('max_manifest_size'),
                         mware.max_manifest_size)

if __name__ == '__main__':
    unittest.main()
