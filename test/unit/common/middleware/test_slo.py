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

import unittest
from copy import deepcopy
from mock import patch
from hashlib import md5
from swift.common import swob
from swift.common.middleware import slo
from swift.common.utils import json, split_path
from swift.common.swob import Request, HTTPException


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
        self.calls = []
        self.req_method_paths = []
        self.uploaded = {}
        # mapping of (method, path) --> (response class, headers, body)
        self._responses = {}

    def __call__(self, env, start_response):
        method = env['REQUEST_METHOD']
        path = env['PATH_INFO']
        _, acc, cont, obj = split_path(env['PATH_INFO'], 0, 4,
                                       rest_with_last=True)

        self.calls.append((method, path))

        try:
            resp_class, raw_headers, body = self._responses[(method, path)]
            headers = swob.HeaderKeyDict(raw_headers)
        except KeyError:
            if method == 'GET' and obj and path in self.uploaded:
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

        return resp_class(headers=headers, body=body)(env, start_response)

    @property
    def call_count(self):
        return len(self.calls)

    def register(self, method, path, response_class, headers, body):
        self._responses[(method, path)] = (response_class, headers, body)


class SloTestCase(unittest.TestCase):
    def setUp(self):
        self.app = FakeSwift()
        self.slo = slo.filter_factory({})(self.app)
        self.slo.min_segment_size = 1

    def call_app(self, req, app=None):
        if app is None:
            app = self.app

        status = [None]
        headers = [None]

        def start_response(s, h, ei=None):
            status[0] = s
            headers[0] = h

        body = ''.join(app(req.environ, start_response))
        return status[0], headers[0], body

    def call_slo(self, req):
        return self.call_app(req, app=self.slo)


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
            '/v1/a/c/o', headers={'x-static-large-object': "true"})
        resp = self.slo(req.environ, fake_start_response)
        self.assert_(
            resp[0].startswith('X-Static-Large-Object is a reserved header'))

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
            'HEAD', '/v1/AUTH_test/checktest/slob',
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
        self.assertEquals(self.app.call_count, 4)
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

    def test_handle_multipart_delete_whole_404(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/man_404?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        status, response, body = self.call_slo(req)
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
        status, response, body = self.call_slo(req)
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

    def test_handle_multipart_delete_nested_404(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/manifest-missing-submanifest' +
            '?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        status, response, body = self.call_slo(req)
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

    def test_handle_multipart_delete_not_a_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/a_1?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        status, response, body = self.call_slo(req)
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
        status, response, body = self.call_slo(req)
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
        status, response, body = self.call_slo(req)
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


if __name__ == '__main__':
    unittest.main()
