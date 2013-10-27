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
from mock import patch
from hashlib import md5
from swift.common.middleware import slo
from swift.common.utils import json
from swift.common.swob import Request, Response, HTTPException


class FakeApp(object):
    def __init__(self):
        self.calls = 0
        self.req_method_paths = []

    def __call__(self, env, start_response):
        self.calls += 1
        if env['PATH_INFO'] == '/':
            return Response(status=200, body='passed')(env, start_response)
        if env['PATH_INFO'].startswith('/test_good/'):
            j, v, a, cont, obj = env['PATH_INFO'].split('/')
            if obj == 'a_2':
                return Response(status=400)(env, start_response)
            cont_len = 100
            if obj == 'small_object':
                cont_len = 10
            headers = {'etag': 'etagoftheobjectsegment',
                       'Content-Length': cont_len}
            if obj == 'slob':
                headers['X-Static-Large-Object'] = 'true'
            return Response(status=200, headers=headers)(env, start_response)
        if env['PATH_INFO'].startswith('/test_good_check/'):
            j, v, a, cont, obj = env['PATH_INFO'].split('/')
            etag, size = obj.split('_')
            last_mod = 'Fri, 01 Feb 2012 20:38:36 GMT'
            if obj == 'a_1':
                last_mod = ''
            return Response(
                status=200,
                headers={'etag': etag, 'Last-Modified': last_mod,
                         'Content-Length': size})(env, start_response)
        if env['PATH_INFO'].startswith('/test_get/'):
            good_data = json.dumps(
                [{'name': '/c/a_1', 'hash': 'a', 'bytes': '1'},
                 {'name': '/d/b_2', 'hash': 'b', 'bytes': '2'}])
            return Response(status=200,
                            headers={'X-Static-Large-Object': 'True',
                                     'Content-Type': 'html;swift_bytes=55'},
                            body=good_data)(env, start_response)

        if env['PATH_INFO'].startswith('/test_get_broke_json/'):
            good_data = json.dumps(
                [{'name': '/c/a_1', 'hash': 'a', 'bytes': '1'},
                 {'name': '/d/b_2', 'hash': 'b', 'bytes': '2'}])
            return Response(status=200,
                            headers={'X-Static-Large-Object': 'True'},
                            body=good_data[:-5])(env, start_response)

        if env['PATH_INFO'].startswith('/test_get_bad_json/'):
            bad_data = json.dumps(
                [{'name': '/c/a_1', 'something': 'a', 'bytes': '1'},
                 {'name': '/d/b_2', 'bytes': '2'}])
            return Response(status=200,
                            headers={'X-Static-Large-Object': 'True'},
                            body=bad_data)(env, start_response)

        if env['PATH_INFO'].startswith('/test_get_not_slo/'):
            return Response(status=200, body='lalala')(env, start_response)

        if env['PATH_INFO'].startswith('/test_delete_404/'):
            good_data = json.dumps(
                [{'name': '/c/a_1', 'hash': 'a', 'bytes': '1'},
                 {'name': '/d/b_2', 'hash': 'b', 'bytes': '2'}])
            self.req_method_paths.append((env['REQUEST_METHOD'],
                                          env['PATH_INFO']))
            if env['PATH_INFO'].endswith('/c/man_404'):
                return Response(status=404)(env, start_response)
            if env['PATH_INFO'].endswith('/c/a_1'):
                return Response(status=404)(env, start_response)
            return Response(status=200,
                            headers={'X-Static-Large-Object': 'True'},
                            body=good_data)(env, start_response)

        if env['PATH_INFO'].startswith('/test_delete/'):
            good_data = json.dumps(
                [{'name': '/c/a_1', 'hash': 'a', 'bytes': '1'},
                 {'name': '/d/b_2', 'hash': 'b', 'bytes': '2'}])
            self.req_method_paths.append((env['REQUEST_METHOD'],
                                          env['PATH_INFO']))
            return Response(status=200,
                            headers={'X-Static-Large-Object': 'True'},
                            body=good_data)(env, start_response)

        if env['PATH_INFO'].startswith('/test_delete_nested/'):
            nested_data = json.dumps(
                [{'name': '/b/b_2', 'hash': 'a', 'bytes': '1'},
                 {'name': '/c/c_3', 'hash': 'b', 'bytes': '2'}])
            good_data = json.dumps(
                [{'name': '/a/a_1', 'hash': 'a', 'bytes': '1'},
                 {'name': '/a/sub_nest', 'hash': 'a', 'sub_slo': True,
                  'bytes': len(nested_data)},
                 {'name': '/d/d_3', 'hash': 'b', 'bytes': '2'}])
            self.req_method_paths.append((env['REQUEST_METHOD'],
                                          env['PATH_INFO']))
            if 'sub_nest' in env['PATH_INFO']:
                return Response(status=200,
                                headers={'X-Static-Large-Object': 'True'},
                                body=nested_data)(env, start_response)
            else:
                return Response(status=200,
                                headers={'X-Static-Large-Object': 'True'},
                                body=good_data)(env, start_response)

        if env['PATH_INFO'].startswith('/test_delete_nested_404/'):
            good_data = json.dumps(
                [{'name': '/a/a_1', 'hash': 'a', 'bytes': '1'},
                 {'name': '/a/sub_nest', 'hash': 'a', 'bytes': '2',
                  'sub_slo': True},
                 {'name': '/d/d_3', 'hash': 'b', 'bytes': '3'}])
            self.req_method_paths.append((env['REQUEST_METHOD'],
                                          env['PATH_INFO']))
            if 'sub_nest' in env['PATH_INFO']:
                return Response(status=404)(env, start_response)
            else:
                return Response(status=200,
                                headers={'X-Static-Large-Object': 'True'},
                                body=good_data)(env, start_response)

        if env['PATH_INFO'].startswith('/test_delete_bad_json/'):
            self.req_method_paths.append((env['REQUEST_METHOD'],
                                          env['PATH_INFO']))
            return Response(status=200,
                            headers={'X-Static-Large-Object': 'True'},
                            body='bad json')(env, start_response)

        if env['PATH_INFO'].startswith('/test_delete_bad_man/'):
            self.req_method_paths.append((env['REQUEST_METHOD'],
                                          env['PATH_INFO']))
            return Response(status=200, body='')(env, start_response)

        if env['PATH_INFO'].startswith('/test_delete_401/'):
            good_data = json.dumps(
                [{'name': '/c/a_1', 'hash': 'a', 'bytes': '1'},
                 {'name': '/d/b_2', 'hash': 'b', 'bytes': '2'}])
            self.req_method_paths.append((env['REQUEST_METHOD'],
                                          env['PATH_INFO']))
            if env['PATH_INFO'].endswith('/d/b_2'):
                return Response(status=401)(env, start_response)
            return Response(status=200,
                            headers={'X-Static-Large-Object': 'True'},
                            body=good_data)(env, start_response)

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


class TestStaticLargeObject(unittest.TestCase):

    def setUp(self):
        self.app = FakeApp()
        self.slo = slo.filter_factory({})(self.app)
        self.slo.min_segment_size = 1

    def tearDown(self):
        pass

    def test_handle_multipart_no_obj(self):
        req = Request.blank('/')
        resp_iter = self.slo(req.environ, fake_start_response)
        self.assertEquals(self.app.calls, 1)
        self.assertEquals(''.join(resp_iter), 'passed')

    def test_slo_header_assigned(self):
        req = Request.blank(
            '/v/a/c/o', headers={'x-static-large-object': "true"})
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

        bad_data = json.dumps([{'path': '/cont/object', 'size_bytes': 100}])
        self.assertRaises(HTTPException, slo.parse_input, bad_data)

    def test_put_manifest_too_quick_fail(self):
        req = Request.blank('/v/a/c/o')
        req.content_length = self.slo.max_manifest_size + 1
        try:
            self.slo.handle_multipart_put(req, fake_start_response)
        except HTTPException as e:
            pass
        self.assertEquals(e.status_int, 413)

        with patch.object(self.slo, 'max_manifest_segments', 0):
            req = Request.blank('/v/a/c/o', body=test_json_data)
            e = None
            try:
                self.slo.handle_multipart_put(req, fake_start_response)
            except HTTPException as e:
                pass
            self.assertEquals(e.status_int, 413)

        with patch.object(self.slo, 'min_segment_size', 1000):
            req = Request.blank('/v/a/c/o', body=test_json_data)
            try:
                self.slo.handle_multipart_put(req, fake_start_response)
            except HTTPException as e:
                pass
            self.assertEquals(e.status_int, 400)

        req = Request.blank('/v/a/c/o', headers={'X-Copy-From': 'lala'})
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
            '/test_good/AUTH_test/c/man?multipart-manifest=put',
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
                '/test_good/AUTH_test/c/man?multipart-manifest=put',
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
            '/test_good/AUTH_test/c/man?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, headers={'Accept': 'test'},
            body=test_json_data)
        self.assertTrue('X-Static-Large-Object' not in req.headers)
        self.slo(req.environ, fake_start_response)
        self.assertTrue('X-Static-Large-Object' in req.headers)
        self.assertTrue(req.environ['PATH_INFO'], '/cont/object\xe2\x99\xa4')

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
                json.dumps([{'path': '/c/o', 'etag': None,
                             'size_bytes': 12}]),
                json.dumps([{'path': '/c/o', 'etag': 'asdf',
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
                '/test_good/AUTH_test/c/man?multipart-manifest=put',
                environ={'REQUEST_METHOD': 'PUT'}, body=bad_data)
            self.assertRaises(HTTPException, self.slo.handle_multipart_put,
                              req, fake_start_response)

    def test_handle_multipart_put_check_data(self):
        good_data = json.dumps(
            [{'path': '/c/a_1', 'etag': 'a', 'size_bytes': '1'},
             {'path': '/d/b_2', 'etag': 'b', 'size_bytes': '2'}])
        req = Request.blank(
            '/test_good_check/A/c/man_3?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, body=good_data)
        self.slo.handle_multipart_put(req, fake_start_response)
        self.assertEquals(self.app.calls, 3)
        self.assert_(req.environ['CONTENT_TYPE'].endswith(';swift_bytes=3'))
        manifest_data = json.loads(req.environ['wsgi.input'].read())
        self.assertEquals(len(manifest_data), 2)
        self.assertEquals(manifest_data[0]['hash'], 'a')
        self.assertEquals(manifest_data[0]['bytes'], 1)
        self.assert_(not manifest_data[0]['last_modified'].startswith('2012'))
        self.assert_(manifest_data[1]['last_modified'].startswith('2012'))

    def test_handle_multipart_put_check_data_bad(self):
        bad_data = json.dumps(
            [{'path': '/c/a_1', 'etag': 'a', 'size_bytes': '1'},
             {'path': '/c/a_2', 'etag': 'a', 'size_bytes': '1'},
             {'path': '/d/b_2', 'etag': 'b', 'size_bytes': '2'},
             {'path': '/d/slob', 'etag': 'a', 'size_bytes': '2'}])
        req = Request.blank(
            '/test_good/A/c/man?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'Accept': 'application/json'},
            body=bad_data)
        try:
            self.slo.handle_multipart_put(req, fake_start_response)
        except HTTPException as e:
            self.assertEquals(self.app.calls, 4)
            data = json.loads(e.body)
            errors = data['Errors']
            self.assertEquals(errors[0][0], '/c/a_1')
            self.assertEquals(errors[0][1], 'Size Mismatch')
            self.assertEquals(errors[2][0], '/c/a_2')
            self.assertEquals(errors[2][1], '400 Bad Request')
            self.assertEquals(errors[4][0], '/d/b_2')
            self.assertEquals(errors[4][1], 'Etag Mismatch')
            self.assertEquals(errors[-1][0], '/d/slob')
            self.assertEquals(errors[-1][1], 'Etag Mismatch')
        else:
            self.assert_(False)

    def test_handle_multipart_delete_man(self):
        req = Request.blank(
            '/test_good/A/c/man', environ={'REQUEST_METHOD': 'DELETE'})
        self.slo(req.environ, fake_start_response)
        self.assertEquals(self.app.calls, 1)

    def test_handle_multipart_delete_whole_404(self):
        req = Request.blank(
            '/test_delete_404/A/c/man_404?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        app_iter = self.slo(req.environ, fake_start_response)
        app_iter = list(app_iter)  # iterate through whole response
        resp_data = json.loads(app_iter[0])
        self.assertEquals(self.app.calls, 1)
        self.assertEquals(self.app.req_method_paths,
                          [('GET', '/test_delete_404/A/c/man_404')])
        self.assertEquals(resp_data['Response Status'], '200 OK')
        self.assertEquals(resp_data['Response Body'], '')
        self.assertEquals(resp_data['Number Deleted'], 0)
        self.assertEquals(resp_data['Number Not Found'], 1)
        self.assertEquals(resp_data['Errors'], [])

    def test_handle_multipart_delete_segment_404(self):
        req = Request.blank(
            '/test_delete_404/A/c/man?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        app_iter = self.slo(req.environ, fake_start_response)
        app_iter = list(app_iter)  # iterate through whole response
        resp_data = json.loads(app_iter[0])
        self.assertEquals(self.app.calls, 4)
        self.assertEquals(self.app.req_method_paths,
                          [('GET', '/test_delete_404/A/c/man'),
                           ('DELETE', '/test_delete_404/A/c/a_1'),
                           ('DELETE', '/test_delete_404/A/d/b_2'),
                           ('DELETE', '/test_delete_404/A/c/man')])
        self.assertEquals(resp_data['Response Status'], '200 OK')
        self.assertEquals(resp_data['Number Deleted'], 2)
        self.assertEquals(resp_data['Number Not Found'], 1)

    def test_handle_multipart_delete_whole(self):
        req = Request.blank(
            '/test_delete/A/c/man?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE'})
        app_iter = self.slo(req.environ, fake_start_response)
        list(app_iter)  # iterate through whole response
        self.assertEquals(self.app.calls, 4)
        self.assertEquals(self.app.req_method_paths,
                          [('GET', '/test_delete/A/c/man'),
                           ('DELETE', '/test_delete/A/c/a_1'),
                           ('DELETE', '/test_delete/A/d/b_2'),
                           ('DELETE', '/test_delete/A/c/man')])

    def test_handle_multipart_delete_nested(self):
        req = Request.blank(
            '/test_delete_nested/A/c/man?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE'})
        app_iter = self.slo(req.environ, fake_start_response)
        list(app_iter)  # iterate through whole response
        self.assertEquals(self.app.calls, 8)
        self.assertEquals(
            set(self.app.req_method_paths),
            set([('GET', '/test_delete_nested/A/c/man'),
                 ('GET', '/test_delete_nested/A/a/sub_nest'),
                 ('DELETE', '/test_delete_nested/A/a/a_1'),
                 ('DELETE', '/test_delete_nested/A/b/b_2'),
                 ('DELETE', '/test_delete_nested/A/c/c_3'),
                 ('DELETE', '/test_delete_nested/A/a/sub_nest'),
                 ('DELETE', '/test_delete_nested/A/d/d_3'),
                 ('DELETE', '/test_delete_nested/A/c/man')]))

    def test_handle_multipart_delete_nested_404(self):
        req = Request.blank(
            '/test_delete_nested_404/A/c/man?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        app_iter = self.slo(req.environ, fake_start_response)
        app_iter = list(app_iter)  # iterate through whole response
        resp_data = json.loads(app_iter[0])
        self.assertEquals(self.app.calls, 5)
        self.assertEquals(self.app.req_method_paths,
                          [('GET', '/test_delete_nested_404/A/c/man'),
                           ('DELETE', '/test_delete_nested_404/A/a/a_1'),
                           ('GET', '/test_delete_nested_404/A/a/sub_nest'),
                           ('DELETE', '/test_delete_nested_404/A/d/d_3'),
                           ('DELETE', '/test_delete_nested_404/A/c/man')])
        self.assertEquals(resp_data['Response Status'], '200 OK')
        self.assertEquals(resp_data['Response Body'], '')
        self.assertEquals(resp_data['Number Deleted'], 3)
        self.assertEquals(resp_data['Number Not Found'], 1)
        self.assertEquals(resp_data['Errors'], [])

    def test_handle_multipart_delete_not_a_manifest(self):
        req = Request.blank(
            '/test_delete_bad_man/A/c/man?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        app_iter = self.slo(req.environ, fake_start_response)
        app_iter = list(app_iter)  # iterate through whole response
        resp_data = json.loads(app_iter[0])
        self.assertEquals(self.app.calls, 1)
        self.assertEquals(self.app.req_method_paths,
                          [('GET', '/test_delete_bad_man/A/c/man')])
        self.assertEquals(resp_data['Response Status'], '400 Bad Request')
        self.assertEquals(resp_data['Response Body'], '')
        self.assertEquals(resp_data['Number Deleted'], 0)
        self.assertEquals(resp_data['Number Not Found'], 0)
        self.assertEquals(resp_data['Errors'],
                          [['/c/man', 'Not an SLO manifest']])

    def test_handle_multipart_delete_bad_json(self):
        req = Request.blank(
            '/test_delete_bad_json/A/c/man?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        app_iter = self.slo(req.environ, fake_start_response)
        app_iter = list(app_iter)  # iterate through whole response
        resp_data = json.loads(app_iter[0])
        self.assertEquals(self.app.calls, 1)
        self.assertEquals(self.app.req_method_paths,
                          [('GET', '/test_delete_bad_json/A/c/man')])
        self.assertEquals(resp_data['Response Status'], '400 Bad Request')
        self.assertEquals(resp_data['Response Body'], '')
        self.assertEquals(resp_data['Number Deleted'], 0)
        self.assertEquals(resp_data['Number Not Found'], 0)
        self.assertEquals(resp_data['Errors'],
                          [['/c/man', 'Unable to load SLO manifest']])

    def test_handle_multipart_delete_401(self):
        req = Request.blank(
            '/test_delete_401/A/c/man?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        app_iter = self.slo(req.environ, fake_start_response)
        app_iter = list(app_iter)  # iterate through whole response
        resp_data = json.loads(app_iter[0])
        self.assertEquals(self.app.calls, 4)
        self.assertEquals(self.app.req_method_paths,
                          [('GET', '/test_delete_401/A/c/man'),
                           ('DELETE', '/test_delete_401/A/c/a_1'),
                           ('DELETE', '/test_delete_401/A/d/b_2'),
                           ('DELETE', '/test_delete_401/A/c/man')])
        self.assertEquals(resp_data['Response Status'], '400 Bad Request')
        self.assertEquals(resp_data['Response Body'], '')
        self.assertEquals(resp_data['Number Deleted'], 2)
        self.assertEquals(resp_data['Number Not Found'], 0)
        self.assertEquals(resp_data['Errors'],
                          [['/d/b_2', '401 Unauthorized']])

if __name__ == '__main__':
    unittest.main()
