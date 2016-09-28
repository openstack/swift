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

from six.moves import range

import hashlib
import json
import time
import unittest
from mock import patch
from hashlib import md5
from StringIO import StringIO
from swift.common import swob, utils
from swift.common.exceptions import ListingIterError, SegmentError
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.middleware import slo
from swift.common.swob import Request, HTTPException
from swift.common.utils import quote, closing_if_possible, close_if_possible, \
    parse_content_type, iter_multipart_mime_documents, parse_mime_headers
from test.unit.common.middleware.helpers import FakeSwift


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


def md5hex(s):
    return hashlib.md5(s).hexdigest()


class SloTestCase(unittest.TestCase):
    def setUp(self):
        self.app = FakeSwift()
        slo_conf = {'rate_limit_under_size': '0'}
        self.slo = slo.filter_factory(slo_conf)(self.app)
        self.slo.logger = self.app.logger

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
        self.assertEqual(self.app.calls, [('GET', '/')])
        self.assertEqual(''.join(resp_iter), 'passed')

    def test_slo_header_assigned(self):
        req = Request.blank(
            '/v1/a/c/o', headers={'x-static-large-object': "true"},
            environ={'REQUEST_METHOD': 'PUT'})
        resp = ''.join(self.slo(req.environ, fake_start_response))
        self.assertTrue(
            resp.startswith('X-Static-Large-Object is a reserved header'))

    def test_slo_PUT_env_override(self):
        path = '/v1/a/c/o'
        body = 'manifest body not checked when override flag set'
        resp_status = []

        def start_response(status, headers, *args):
            resp_status.append(status)

        req = Request.blank(
            path, headers={'x-static-large-object': "true"},
            environ={'REQUEST_METHOD': 'PUT', 'swift.slo_override': True},
            body=body)
        self.app.register('PUT', path, swob.HTTPCreated, {})
        resp_iter = self.slo(req.environ, start_response)
        self.assertEqual('', ''.join(resp_iter))
        self.assertEqual(self.app.calls, [('PUT', path)])
        self.assertEqual(body, self.app.uploaded[path][1])
        self.assertEqual(resp_status[0], '201 Created')

    def _put_bogus_slo(self, manifest_text,
                       manifest_path='/v1/a/c/the-manifest'):
        with self.assertRaises(HTTPException) as catcher:
            slo.parse_and_validate_input(manifest_text, manifest_path)
        self.assertEqual(400, catcher.exception.status_int)
        return catcher.exception.body

    def _put_slo(self, manifest_text, manifest_path='/v1/a/c/the-manifest'):
        return slo.parse_and_validate_input(manifest_text, manifest_path)

    def test_bogus_input(self):
        self.assertEqual('Manifest must be valid JSON.\n',
                         self._put_bogus_slo('some non json'))

        self.assertEqual('Manifest must be a list.\n',
                         self._put_bogus_slo('{}'))

        self.assertEqual('Index 0: not a JSON object\n',
                         self._put_bogus_slo('["zombocom"]'))

    def test_bogus_input_bad_keys(self):
        self.assertEqual(
            "Index 0: extraneous keys \"baz\", \"foo\"\n",
            self._put_bogus_slo(json.dumps(
                [{'path': '/cont/object', 'etag': 'etagoftheobjectsegment',
                  'size_bytes': 100,
                  'foo': 'bar', 'baz': 'quux'}])))

    def test_bogus_input_ranges(self):
        self.assertEqual(
            "Index 0: invalid range\n",
            self._put_bogus_slo(json.dumps(
                [{'path': '/cont/object', 'etag': 'blah',
                  'size_bytes': 100, 'range': 'non-range value'}])))

        self.assertEqual(
            "Index 0: multiple ranges (only one allowed)\n",
            self._put_bogus_slo(json.dumps(
                [{'path': '/cont/object', 'etag': 'blah',
                  'size_bytes': 100, 'range': '1-20,30-40'}])))

    def test_bogus_input_unsatisfiable_range(self):
        self.assertEqual(
            "Index 0: unsatisfiable range\n",
            self._put_bogus_slo(json.dumps(
                [{'path': '/cont/object', 'etag': 'blah',
                  'size_bytes': 100, 'range': '8888-9999'}])))

        # since size is optional, we have to be able to defer this check
        segs = self._put_slo(json.dumps(
            [{'path': '/cont/object', 'etag': 'blah',
              'size_bytes': None, 'range': '8888-9999'}]))
        self.assertEqual(1, len(segs))

    def test_bogus_input_path(self):
        self.assertEqual(
            "Index 0: path does not refer to an object. Path must be of the "
            "form /container/object.\n"
            "Index 1: path does not refer to an object. Path must be of the "
            "form /container/object.\n",
            self._put_bogus_slo(json.dumps(
                [{'path': '/cont', 'etag': 'etagoftheobjectsegment',
                  'size_bytes': 100},
                 {'path': '/c-trailing-slash/', 'etag': 'e',
                  'size_bytes': 100},
                 {'path': '/con/obj', 'etag': 'e',
                  'size_bytes': 100},
                 {'path': '/con/obj-trailing-slash/', 'etag': 'e',
                  'size_bytes': 100},
                 {'path': '/con/obj/with/slashes', 'etag': 'e',
                  'size_bytes': 100}])))

    def test_bogus_input_multiple(self):
        self.assertEqual(
            "Index 0: invalid range\nIndex 1: not a JSON object\n",
            self._put_bogus_slo(json.dumps(
                [{'path': '/cont/object', 'etag': 'etagoftheobjectsegment',
                  'size_bytes': 100, 'range': 'non-range value'},
                 None])))

    def test_bogus_input_size_bytes(self):
        self.assertEqual(
            "Index 0: invalid size_bytes\n",
            self._put_bogus_slo(json.dumps(
                [{'path': '/cont/object', 'etag': 'blah', 'size_bytes': "fht"},
                 {'path': '/cont/object', 'etag': 'blah', 'size_bytes': None},
                 {'path': '/cont/object', 'etag': 'blah', 'size_bytes': 100}],
            )))

        self.assertEqual(
            "Index 0: invalid size_bytes\n",
            self._put_bogus_slo(json.dumps(
                [{'path': '/cont/object', 'etag': 'blah', 'size_bytes': []}],
            )))

    def test_bogus_input_self_referential(self):
        self.assertEqual(
            "Index 0: manifest must not include itself as a segment\n",
            self._put_bogus_slo(json.dumps(
                [{'path': '/c/the-manifest', 'etag': 'gate',
                  'size_bytes': 100, 'range': 'non-range value'}])))

    def test_bogus_input_self_referential_non_ascii(self):
        self.assertEqual(
            "Index 0: manifest must not include itself as a segment\n",
            self._put_bogus_slo(
                json.dumps([{'path': u'/c/あ_1',
                             'etag': 'a', 'size_bytes': 1}]),
                manifest_path=quote(u'/v1/a/c/あ_1')))

    def test_bogus_input_self_referential_last_segment(self):
        test_json_data = json.dumps([
            {'path': '/c/seg_1', 'etag': 'a', 'size_bytes': 1},
            {'path': '/c/seg_2', 'etag': 'a', 'size_bytes': 1},
            {'path': '/c/seg_3', 'etag': 'a', 'size_bytes': 1},
            {'path': '/c/the-manifest', 'etag': 'a', 'size_bytes': 1},
        ])
        self.assertEqual(
            "Index 3: manifest must not include itself as a segment\n",
            self._put_bogus_slo(
                test_json_data,
                manifest_path=quote('/v1/a/c/the-manifest')))

    def test_bogus_input_undersize_segment(self):
        self.assertEqual(
            "Index 1: too small; each segment "
            "must be at least 1 byte.\n"
            "Index 2: too small; each segment "
            "must be at least 1 byte.\n",
            self._put_bogus_slo(
                json.dumps([
                    {'path': u'/c/s1', 'etag': 'a', 'size_bytes': 1},
                    {'path': u'/c/s2', 'etag': 'b', 'size_bytes': 0},
                    {'path': u'/c/s3', 'etag': 'c', 'size_bytes': 0},
                    # No error for this one since size_bytes is unspecified
                    {'path': u'/c/s4', 'etag': 'd', 'size_bytes': None},
                    {'path': u'/c/s5', 'etag': 'e', 'size_bytes': 1000}])))

    def test_valid_input(self):
        data = json.dumps(
            [{'path': '/cont/object', 'etag': 'etagoftheobjectsegment',
              'size_bytes': 100}])
        self.assertEqual(
            '/cont/object',
            slo.parse_and_validate_input(data, '/v1/a/cont/man')[0]['path'])

        data = json.dumps(
            [{'path': '/cont/object', 'etag': 'etagoftheobjectsegment',
              'size_bytes': 100, 'range': '0-40'}])
        parsed = slo.parse_and_validate_input(data, '/v1/a/cont/man')
        self.assertEqual('/cont/object', parsed[0]['path'])
        self.assertEqual([(0, 40)], parsed[0]['range'].ranges)

        data = json.dumps(
            [{'path': '/cont/object', 'etag': 'etagoftheobjectsegment',
              'size_bytes': None, 'range': '0-40'}])
        parsed = slo.parse_and_validate_input(data, '/v1/a/cont/man')
        self.assertEqual('/cont/object', parsed[0]['path'])
        self.assertIsNone(parsed[0]['size_bytes'])
        self.assertEqual([(0, 40)], parsed[0]['range'].ranges)


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
            'HEAD', '/v1/AUTH_test/cont/object2',
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
            'HEAD', '/v1/AUTH_test/cont/empty_object',
            swob.HTTPOk,
            {'Content-Length': '0', 'Etag': 'etagoftheobjectsegment'},
            None)
        self.app.register(
            'HEAD', u'/v1/AUTH_test/cont/あ_1',
            swob.HTTPOk,
            {'Content-Length': '1', 'Etag': 'a'},
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

        _manifest_json = json.dumps(
            [{'name': '/checktest/a_5', 'hash': md5hex("a" * 5),
              'content_type': 'text/plain', 'bytes': '5'}])
        self.app.register(
            'GET', '/v1/AUTH_test/checktest/slob',
            swob.HTTPOk,
            {'X-Static-Large-Object': 'true', 'Etag': 'slob-etag',
             'Content-Type': 'cat/picture',
             'Content-Length': len(_manifest_json)},
            _manifest_json)

        self.app.register(
            'PUT', '/v1/AUTH_test/checktest/man_3', swob.HTTPCreated, {}, None)

    def test_put_manifest_too_quick_fail(self):
        req = Request.blank('/v1/a/c/o')
        req.content_length = self.slo.max_manifest_size + 1
        try:
            self.slo.handle_multipart_put(req, fake_start_response)
        except HTTPException as e:
            pass
        self.assertEqual(e.status_int, 413)

        with patch.object(self.slo, 'max_manifest_segments', 0):
            req = Request.blank('/v1/a/c/o', body=test_json_data)
            e = None
            try:
                self.slo.handle_multipart_put(req, fake_start_response)
            except HTTPException as e:
                pass
            self.assertEqual(e.status_int, 413)

        req = Request.blank('/v1/a/c/o', headers={'X-Copy-From': 'lala'})
        try:
            self.slo.handle_multipart_put(req, fake_start_response)
        except HTTPException as e:
            pass
        self.assertEqual(e.status_int, 405)

        # ignores requests to /
        req = Request.blank(
            '/?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, body=test_json_data)
        self.assertEqual(
            list(self.slo.handle_multipart_put(req, fake_start_response)),
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

    def test_handle_multipart_put_disallow_empty_first_segment(self):
        test_json_data = json.dumps([{'path': '/cont/object',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 0},
                                     {'path': '/cont/small_object',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 100}])
        req = Request.blank('/v1/a/c/o', body=test_json_data)
        with self.assertRaises(HTTPException) as catcher:
            self.slo.handle_multipart_put(req, fake_start_response)
        self.assertEqual(catcher.exception.status_int, 400)

    def test_handle_multipart_put_disallow_empty_last_segment(self):
        test_json_data = json.dumps([{'path': '/cont/object',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 100},
                                     {'path': '/cont/small_object',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 0}])
        req = Request.blank('/v1/a/c/o', body=test_json_data)
        with self.assertRaises(HTTPException) as catcher:
            self.slo.handle_multipart_put(req, fake_start_response)
        self.assertEqual(catcher.exception.status_int, 400)

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
        self.assertEqual(no_xml, ['Manifest must be valid JSON.\n'])

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
                json.dumps([{'path': '/c/o', 'etag': 123, 'size_bytes': 100}]),
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
        self.assertEqual(self.app.call_count, 3)

        # go behind SLO's back and see what actually got stored
        req = Request.blank(
            # this string looks weird, but it's just an artifact
            # of FakeSwift
            '/v1/AUTH_test/checktest/man_3?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_app(req)
        headers = dict(headers)
        manifest_data = json.loads(body)
        self.assertTrue(headers['Content-Type'].endswith(';swift_bytes=3'))
        self.assertEqual(len(manifest_data), 2)
        self.assertEqual(manifest_data[0]['hash'], 'a')
        self.assertEqual(manifest_data[0]['bytes'], 1)
        self.assertTrue(
            not manifest_data[0]['last_modified'].startswith('2012'))
        self.assertTrue(manifest_data[1]['last_modified'].startswith('2012'))

    def test_handle_multipart_put_check_data_bad(self):
        bad_data = json.dumps(
            [{'path': '/checktest/a_1', 'etag': 'a', 'size_bytes': '2'},
             {'path': '/checktest/badreq', 'etag': 'a', 'size_bytes': '1'},
             {'path': '/checktest/b_2', 'etag': 'not-b', 'size_bytes': '2'},
             {'path': '/checktest/slob', 'etag': 'not-slob',
              'size_bytes': '12345'}])
        req = Request.blank(
            '/v1/AUTH_test/checktest/man?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'Accept': 'application/json'},
            body=bad_data)

        status, headers, body = self.call_slo(req)
        self.assertEqual(self.app.call_count, 5)
        errors = json.loads(body)['Errors']

        self.assertEqual(len(errors), 5)
        self.assertEqual(errors[0][0], '/checktest/a_1')
        self.assertEqual(errors[0][1], 'Size Mismatch')
        self.assertEqual(errors[1][0], '/checktest/badreq')
        self.assertEqual(errors[1][1], '400 Bad Request')
        self.assertEqual(errors[2][0], '/checktest/b_2')
        self.assertEqual(errors[2][1], 'Etag Mismatch')
        self.assertEqual(errors[3][0], '/checktest/slob')
        self.assertEqual(errors[3][1], 'Size Mismatch')
        self.assertEqual(errors[4][0], '/checktest/slob')
        self.assertEqual(errors[4][1], 'Etag Mismatch')

    def test_handle_multipart_put_skip_size_check(self):
        good_data = json.dumps(
            [{'path': '/checktest/a_1', 'etag': 'a', 'size_bytes': None},
             {'path': '/checktest/b_2', 'etag': 'b', 'size_bytes': None}])
        req = Request.blank(
            '/v1/AUTH_test/checktest/man_3?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, body=good_data)
        status, headers, body = self.call_slo(req)
        self.assertEqual(self.app.call_count, 3)

        # Check that we still populated the manifest properly from our HEADs
        req = Request.blank(
            # this string looks weird, but it's just an artifact
            # of FakeSwift
            '/v1/AUTH_test/checktest/man_3?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_app(req)
        manifest_data = json.loads(body)
        self.assertEqual(1, manifest_data[0]['bytes'])
        self.assertEqual(2, manifest_data[1]['bytes'])

    def test_handle_multipart_put_skip_size_check_still_uses_min_size(self):
        test_json_data = json.dumps([{'path': '/cont/empty_object',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': None},
                                     {'path': '/cont/small_object',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 100}])
        req = Request.blank('/v1/AUTH_test/c/o', body=test_json_data)
        with self.assertRaises(HTTPException) as cm:
            self.slo.handle_multipart_put(req, fake_start_response)
        self.assertEqual(cm.exception.status_int, 400)

    def test_handle_multipart_put_skip_size_check_no_early_bailout(self):
        # The first is too small (it's 0 bytes), and
        # the second has a bad etag. Make sure both errors show up in
        # the response.
        test_json_data = json.dumps([{'path': '/cont/empty_object',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': None},
                                     {'path': '/cont/object2',
                                      'etag': 'wrong wrong wrong',
                                      'size_bytes': 100}])
        req = Request.blank('/v1/AUTH_test/c/o', body=test_json_data)
        with self.assertRaises(HTTPException) as cm:
            self.slo.handle_multipart_put(req, fake_start_response)
        self.assertEqual(cm.exception.status_int, 400)
        self.assertIn('at least 1 byte', cm.exception.body)
        self.assertIn('Etag Mismatch', cm.exception.body)

    def test_handle_multipart_put_skip_etag_check(self):
        good_data = json.dumps(
            [{'path': '/checktest/a_1', 'etag': None, 'size_bytes': 1},
             {'path': '/checktest/b_2', 'etag': None, 'size_bytes': 2}])
        req = Request.blank(
            '/v1/AUTH_test/checktest/man_3?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, body=good_data)
        status, headers, body = self.call_slo(req)
        self.assertEqual(self.app.call_count, 3)

        # Check that we still populated the manifest properly from our HEADs
        req = Request.blank(
            # this string looks weird, but it's just an artifact
            # of FakeSwift
            '/v1/AUTH_test/checktest/man_3?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_app(req)
        manifest_data = json.loads(body)
        self.assertEqual('a', manifest_data[0]['hash'])
        self.assertEqual('b', manifest_data[1]['hash'])

    def test_handle_unsatisfiable_ranges(self):
        bad_data = json.dumps(
            [{'path': '/checktest/a_1', 'etag': None,
              'size_bytes': None, 'range': '1-'}])
        req = Request.blank(
            '/v1/AUTH_test/checktest/man_3?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, body=bad_data)
        with self.assertRaises(HTTPException) as catcher:
            self.slo.handle_multipart_put(req, fake_start_response)
        self.assertEqual(400, catcher.exception.status_int)
        self.assertIn("Unsatisfiable Range", catcher.exception.body)

    def test_handle_multipart_put_success_conditional(self):
        test_json_data = json.dumps([{'path': u'/cont/object',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 100}])
        req = Request.blank(
            '/v1/AUTH_test/c/man?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, headers={'If-None-Match': '*'},
            body=test_json_data)
        status, headers, body = self.call_slo(req)
        self.assertEqual(('201 Created', ''), (status, body))
        self.assertEqual([
            ('HEAD', '/v1/AUTH_test/cont/object'),
            ('PUT', '/v1/AUTH_test/c/man?multipart-manifest=put'),
        ], self.app.calls)
        # HEAD shouldn't be conditional
        self.assertNotIn('If-None-Match', self.app.headers[0])
        # But the PUT should be
        self.assertIn('If-None-Match', self.app.headers[1])
        self.assertEqual('*', self.app.headers[1]['If-None-Match'])

    def test_handle_single_ranges(self):
        good_data = json.dumps(
            [{'path': '/checktest/a_1', 'etag': None,
              'size_bytes': None, 'range': '0-0'},
             {'path': '/checktest/b_2', 'etag': None,
              'size_bytes': 2, 'range': '-1'},
             {'path': '/checktest/b_2', 'etag': None,
              'size_bytes': 2, 'range': '0-0'},
             {'path': '/cont/object', 'etag': None,
              'size_bytes': None, 'range': '10-40'}])
        req = Request.blank(
            '/v1/AUTH_test/checktest/man_3?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, body=good_data)
        status, headers, body = self.call_slo(req)
        expected_etag = '"%s"' % md5('ab:1-1;b:0-0;etagoftheobjectsegment:'
                                     '10-40;').hexdigest()
        self.assertEqual(expected_etag, dict(headers)['Etag'])
        self.assertEqual([
            ('HEAD', '/v1/AUTH_test/checktest/a_1'),
            ('HEAD', '/v1/AUTH_test/checktest/b_2'),  # Only once!
            ('HEAD', '/v1/AUTH_test/cont/object'),
            ('PUT', '/v1/AUTH_test/checktest/man_3?multipart-manifest=put'),
        ], self.app.calls)

        # Check that we still populated the manifest properly from our HEADs
        req = Request.blank(
            # this string looks weird, but it's just an artifact
            # of FakeSwift
            '/v1/AUTH_test/checktest/man_3?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_app(req)
        manifest_data = json.loads(body)
        self.assertEqual('a', manifest_data[0]['hash'])
        self.assertNotIn('range', manifest_data[0])
        self.assertNotIn('segment_bytes', manifest_data[0])

        self.assertEqual('b', manifest_data[1]['hash'])
        self.assertEqual('1-1', manifest_data[1]['range'])

        self.assertEqual('b', manifest_data[2]['hash'])
        self.assertEqual('0-0', manifest_data[2]['range'])

        self.assertEqual('etagoftheobjectsegment', manifest_data[3]['hash'])
        self.assertEqual('10-40', manifest_data[3]['range'])


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
            'DELETE', '/v1/AUTH_test/deltest/man-all-there',
            swob.HTTPNoContent, {}, None)
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
        self.assertEqual(self.app.call_count, 1)

    def test_handle_multipart_delete_bad_utf8(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/man\xff\xfe?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '200 OK')
        resp_data = json.loads(body)
        self.assertEqual(resp_data['Response Status'],
                         '412 Precondition Failed')

    def test_handle_multipart_delete_whole_404(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/man_404?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_slo(req)
        resp_data = json.loads(body)
        self.assertEqual(
            self.app.calls,
            [('GET',
              '/v1/AUTH_test/deltest/man_404?multipart-manifest=get')])
        self.assertEqual(resp_data['Response Status'], '200 OK')
        self.assertEqual(resp_data['Response Body'], '')
        self.assertEqual(resp_data['Number Deleted'], 0)
        self.assertEqual(resp_data['Number Not Found'], 1)
        self.assertEqual(resp_data['Errors'], [])

    def test_handle_multipart_delete_segment_404(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/man?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_slo(req)
        resp_data = json.loads(body)
        self.assertEqual(
            set(self.app.calls),
            set([('GET',
                  '/v1/AUTH_test/deltest/man?multipart-manifest=get'),
                 ('DELETE',
                  '/v1/AUTH_test/deltest/gone?multipart-manifest=delete'),
                 ('DELETE',
                  '/v1/AUTH_test/deltest/b_2?multipart-manifest=delete'),
                 ('DELETE',
                  '/v1/AUTH_test/deltest/man?multipart-manifest=delete')]))
        self.assertEqual(resp_data['Response Status'], '200 OK')
        self.assertEqual(resp_data['Number Deleted'], 2)
        self.assertEqual(resp_data['Number Not Found'], 1)

    def test_handle_multipart_delete_whole(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/man-all-there?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE'})
        self.call_slo(req)
        self.assertEqual(set(self.app.calls), set([
            ('GET',
             '/v1/AUTH_test/deltest/man-all-there?multipart-manifest=get'),
            ('DELETE', '/v1/AUTH_test/deltest/b_2?multipart-manifest=delete'),
            ('DELETE', '/v1/AUTH_test/deltest/c_3?multipart-manifest=delete'),
            ('DELETE', ('/v1/AUTH_test/deltest/' +
                        'man-all-there?multipart-manifest=delete'))]))

    def test_handle_multipart_delete_nested(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/manifest-with-submanifest?' +
            'multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE'})
        self.call_slo(req)
        self.assertEqual(
            set(self.app.calls),
            set([('GET', '/v1/AUTH_test/deltest/' +
                  'manifest-with-submanifest?multipart-manifest=get'),
                 ('GET', '/v1/AUTH_test/deltest/' +
                  'submanifest?multipart-manifest=get'),
                 ('DELETE',
                  '/v1/AUTH_test/deltest/a_1?multipart-manifest=delete'),
                 ('DELETE',
                  '/v1/AUTH_test/deltest/b_2?multipart-manifest=delete'),
                 ('DELETE',
                  '/v1/AUTH_test/deltest/c_3?multipart-manifest=delete'),
                 ('DELETE',
                  '/v1/AUTH_test/deltest/' +
                  'submanifest?multipart-manifest=delete'),
                 ('DELETE',
                  '/v1/AUTH_test/deltest/d_3?multipart-manifest=delete'),
                 ('DELETE',
                  '/v1/AUTH_test/deltest/' +
                  'manifest-with-submanifest?multipart-manifest=delete')]))

    def test_handle_multipart_delete_nested_too_many_segments(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/manifest-with-submanifest?' +
            'multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        with patch.object(slo, 'MAX_BUFFERED_SLO_SEGMENTS', 1):
            status, headers, body = self.call_slo(req)
        self.assertEqual(status, '200 OK')
        resp_data = json.loads(body)
        self.assertEqual(resp_data['Response Status'], '400 Bad Request')
        self.assertEqual(resp_data['Response Body'],
                         'Too many buffered slo segments to delete.')

    def test_handle_multipart_delete_nested_404(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/manifest-missing-submanifest' +
            '?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_slo(req)
        resp_data = json.loads(body)
        self.assertEqual(
            set(self.app.calls),
            set([('GET', '/v1/AUTH_test/deltest/' +
                  'manifest-missing-submanifest?multipart-manifest=get'),
                 ('DELETE', '/v1/AUTH_test/deltest/' +
                  'a_1?multipart-manifest=delete'),
                 ('GET', '/v1/AUTH_test/deltest/' +
                  'missing-submanifest?multipart-manifest=get'),
                 ('DELETE', '/v1/AUTH_test/deltest/' +
                  'd_3?multipart-manifest=delete'),
                 ('DELETE', '/v1/AUTH_test/deltest/' +
                  'manifest-missing-submanifest?multipart-manifest=delete')]))
        self.assertEqual(resp_data['Response Status'], '200 OK')
        self.assertEqual(resp_data['Response Body'], '')
        self.assertEqual(resp_data['Number Deleted'], 3)
        self.assertEqual(resp_data['Number Not Found'], 1)
        self.assertEqual(resp_data['Errors'], [])

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
        self.assertEqual(status, '200 OK')
        resp_data = json.loads(body)
        self.assertEqual(resp_data['Response Status'], '400 Bad Request')
        self.assertEqual(resp_data['Errors'],
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
        self.assertEqual(status, '200 OK')
        resp_data = json.loads(body)
        self.assertEqual(resp_data['Response Status'], '400 Bad Request')
        self.assertEqual(resp_data['Errors'],
                         [['/deltest/submanifest',
                           'Unable to load SLO manifest or segment.']])

    def test_handle_multipart_delete_not_a_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/a_1?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_slo(req)
        resp_data = json.loads(body)
        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/deltest/a_1?multipart-manifest=get')])
        self.assertEqual(resp_data['Response Status'], '400 Bad Request')
        self.assertEqual(resp_data['Response Body'], '')
        self.assertEqual(resp_data['Number Deleted'], 0)
        self.assertEqual(resp_data['Number Not Found'], 0)
        self.assertEqual(resp_data['Errors'],
                         [['/deltest/a_1', 'Not an SLO manifest']])

    def test_handle_multipart_delete_bad_json(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/manifest-badjson?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_slo(req)
        resp_data = json.loads(body)
        self.assertEqual(self.app.calls,
                         [('GET', '/v1/AUTH_test/deltest/' +
                           'manifest-badjson?multipart-manifest=get')])
        self.assertEqual(resp_data['Response Status'], '400 Bad Request')
        self.assertEqual(resp_data['Response Body'], '')
        self.assertEqual(resp_data['Number Deleted'], 0)
        self.assertEqual(resp_data['Number Not Found'], 0)
        self.assertEqual(resp_data['Errors'],
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
        self.assertEqual(
            set(self.app.calls),
            set([('GET', '/v1/AUTH_test/deltest/' +
                  'manifest-with-unauth-segment?multipart-manifest=get'),
                 ('DELETE',
                  '/v1/AUTH_test/deltest/a_1?multipart-manifest=delete'),
                 ('DELETE', '/v1/AUTH_test/deltest-unauth/' +
                  'q_17?multipart-manifest=delete'),
                 ('DELETE', '/v1/AUTH_test/deltest/' +
                  'manifest-with-unauth-segment?multipart-manifest=delete')]))
        self.assertEqual(resp_data['Response Status'], '400 Bad Request')
        self.assertEqual(resp_data['Response Body'], '')
        self.assertEqual(resp_data['Number Deleted'], 2)
        self.assertEqual(resp_data['Number Not Found'], 0)
        self.assertEqual(resp_data['Errors'],
                         [['/deltest-unauth/q_17', '401 Unauthorized']])

    def test_handle_multipart_delete_client_content_type(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/man-all-there?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE', 'CONTENT_TYPE': 'foo/bar'},
            headers={'Accept': 'application/json'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        resp_data = json.loads(body)
        self.assertEqual(resp_data["Number Deleted"], 3)

        self.assertEqual(set(self.app.calls), set([
            ('GET',
             '/v1/AUTH_test/deltest/man-all-there?multipart-manifest=get'),
            ('DELETE', '/v1/AUTH_test/deltest/b_2?multipart-manifest=delete'),
            ('DELETE', '/v1/AUTH_test/deltest/c_3?multipart-manifest=delete'),
            ('DELETE', ('/v1/AUTH_test/deltest/' +
                        'man-all-there?multipart-manifest=delete'))]))


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
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers.get('Etag', '').strip("'\""),
                         md5("seg01-hashseg02-hash").hexdigest())
        self.assertEqual(body, '')  # it's a HEAD request, after all

    def test_etag_matching(self):
        etag = md5("seg01-hashseg02-hash").hexdigest()
        req = Request.blank(
            '/v1/AUTH_test/headtest/man',
            environ={'REQUEST_METHOD': 'HEAD'},
            headers={'If-None-Match': etag})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '304 Not Modified')


class TestSloGetRawManifest(SloTestCase):

    def setUp(self):
        super(TestSloGetRawManifest, self).setUp()

        _bc_manifest_json = json.dumps(
            [{'name': '/gettest/b_10', 'hash': md5hex('b' * 10), 'bytes': '10',
              'content_type': 'text/plain',
              'last_modified': '1970-01-01T00:00:00.000000'},
             {'name': '/gettest/c_15', 'hash': md5hex('c' * 15), 'bytes': '15',
              'content_type': 'text/plain',
              'last_modified': '1970-01-01T00:00:00.000000'},
             {'name': '/gettest/d_10',
              'hash': md5hex(md5hex("e" * 5) + md5hex("f" * 5)), 'bytes': '10',
              'content_type': 'application/json',
              'sub_slo': True,
              'last_modified': '1970-01-01T00:00:00.000000'}])
        self.bc_etag = md5hex(_bc_manifest_json)
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-bc',
            # proxy obj controller removes swift_bytes from content-type
            swob.HTTPOk, {'Content-Type': 'text/plain',
                          'X-Static-Large-Object': 'true',
                          'X-Object-Meta-Plant': 'Ficus',
                          'Etag': md5hex(_bc_manifest_json)},
            _bc_manifest_json)

        _bc_manifest_json_ranges = json.dumps(
            [{'name': '/gettest/b_10', 'hash': md5hex('b' * 10), 'bytes': '10',
              'last_modified': '1970-01-01T00:00:00.000000',
              'content_type': 'text/plain', 'range': '1-99'},
             {'name': '/gettest/c_15', 'hash': md5hex('c' * 15), 'bytes': '15',
              'last_modified': '1970-01-01T00:00:00.000000',
              'content_type': 'text/plain', 'range': '100-200'}])
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-bc-r',
            # proxy obj controller removes swift_bytes from content-type
            swob.HTTPOk, {'Content-Type': 'text/plain',
                          'X-Static-Large-Object': 'true',
                          'X-Object-Meta-Plant': 'Ficus',
                          'Etag': md5hex(_bc_manifest_json_ranges)},
            _bc_manifest_json_ranges)

    def test_get_raw_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-bc'
            '?multipart-manifest=get&format=raw',
            environ={'REQUEST_METHOD': 'GET',
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertTrue(('Etag', self.bc_etag) in headers, headers)
        self.assertTrue(('X-Static-Large-Object', 'true') in headers, headers)
        # raw format should return the actual manifest object content-type
        self.assertIn(('Content-Type', 'text/plain'), headers)

        try:
            resp_data = json.loads(body)
        except ValueError:
            self.fail("Invalid JSON in manifest GET: %r" % body)

        self.assertEqual(
            resp_data,
            [{'etag': md5hex('b' * 10), 'size_bytes': '10',
              'path': '/gettest/b_10'},
             {'etag': md5hex('c' * 15), 'size_bytes': '15',
              'path': '/gettest/c_15'},
             {'etag': md5hex(md5hex("e" * 5) + md5hex("f" * 5)),
              'size_bytes': '10',
              'path': '/gettest/d_10'}])

    def test_get_raw_manifest_passthrough_with_ranges(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-bc-r'
            '?multipart-manifest=get&format=raw',
            environ={'REQUEST_METHOD': 'GET',
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        # raw format should return the actual manifest object content-type
        self.assertIn(('Content-Type', 'text/plain'), headers)
        try:
            resp_data = json.loads(body)
        except ValueError:
            self.fail("Invalid JSON in manifest GET: %r" % body)

        self.assertEqual(
            resp_data,
            [{'etag': md5hex('b' * 10), 'size_bytes': '10',
              'path': '/gettest/b_10', 'range': '1-99'},
             {'etag': md5hex('c' * 15), 'size_bytes': '15',
              'path': '/gettest/c_15', 'range': '100-200'}],
            body)


class TestSloGetManifest(SloTestCase):
    def setUp(self):
        super(TestSloGetManifest, self).setUp()

        # some plain old objects
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/a_5',
            swob.HTTPOk, {'Content-Length': '5',
                          'Etag': md5hex('a' * 5)},
            'a' * 5)
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/b_10',
            swob.HTTPOk, {'Content-Length': '10',
                          'Etag': md5hex('b' * 10)},
            'b' * 10)
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/c_15',
            swob.HTTPOk, {'Content-Length': '15',
                          'Etag': md5hex('c' * 15)},
            'c' * 15)
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/d_20',
            swob.HTTPOk, {'Content-Length': '20',
                          'Etag': md5hex('d' * 20)},
            'd' * 20)
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/e_25',
            swob.HTTPOk, {'Content-Length': '25',
                          'Etag': md5hex('e' * 25)},
            'e' * 25)
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/f_30',
            swob.HTTPOk, {'Content-Length': '30',
                          'Etag': md5hex('f' * 30)},
            'f' * 30)
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/g_35',
            swob.HTTPOk, {'Content-Length': '35',
                          'Etag': md5hex('g' * 35)},
            'g' * 35)
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/h_40',
            swob.HTTPOk, {'Content-Length': '40',
                          'Etag': md5hex('h' * 40)},
            'h' * 40)
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/i_45',
            swob.HTTPOk, {'Content-Length': '45',
                          'Etag': md5hex('i' * 45)},
            'i' * 45)
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/j_50',
            swob.HTTPOk, {'Content-Length': '50',
                          'Etag': md5hex('j' * 50)},
            'j' * 50)
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/k_55',
            swob.HTTPOk, {'Content-Length': '55',
                          'Etag': md5hex('k' * 55)},
            'k' * 55)
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/l_60',
            swob.HTTPOk, {'Content-Length': '60',
                          'Etag': md5hex('l' * 60)},
            'l' * 60)

        _bc_manifest_json = json.dumps(
            [{'name': '/gettest/b_10', 'hash': md5hex('b' * 10), 'bytes': '10',
              'content_type': 'text/plain'},
             {'name': '/gettest/c_15', 'hash': md5hex('c' * 15), 'bytes': '15',
              'content_type': 'text/plain'}])
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-bc',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true',
                          'X-Object-Meta-Plant': 'Ficus',
                          'Etag': md5hex(_bc_manifest_json)},
            _bc_manifest_json)

        _abcd_manifest_json = json.dumps(
            [{'name': '/gettest/a_5', 'hash': md5hex("a" * 5),
              'content_type': 'text/plain', 'bytes': '5'},
             {'name': '/gettest/manifest-bc', 'sub_slo': True,
              'content_type': 'application/json',
              'hash': md5hex(md5hex("b" * 10) + md5hex("c" * 15)),
              'bytes': 25},
             {'name': '/gettest/d_20', 'hash': md5hex("d" * 20),
              'content_type': 'text/plain', 'bytes': '20'}])
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-abcd',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true',
                          'Etag': md5(_abcd_manifest_json).hexdigest()},
            _abcd_manifest_json)

        # A submanifest segment is created using the response headers from a
        # HEAD on the submanifest. That HEAD is passed through SLO which will
        # modify the response content-length to be equal to the size of the
        # submanifest's large object. The swift_bytes value appended to the
        # submanifest's content-type will have been removed. So the sub-slo
        # segment dict that is written to the parent manifest should have the
        # correct bytes and content-type values. However, if somehow the
        # submanifest HEAD response wasn't modified by SLO (maybe
        # historically?) and we ended up with the parent manifest sub-slo entry
        # having swift_bytes appended to it's content-type and the actual
        # submanifest size in its bytes field, then SLO can cope, so we create
        # a deviant manifest to verify that SLO can deal with it.
        _abcd_manifest_json_alt = json.dumps(
            [{'name': '/gettest/a_5', 'hash': md5hex("a" * 5),
              'content_type': 'text/plain', 'bytes': '5'},
             {'name': '/gettest/manifest-bc', 'sub_slo': True,
              'content_type': 'application/json; swift_bytes=25',
              'hash': md5hex(md5hex("b" * 10) + md5hex("c" * 15)),
              'bytes': len(_bc_manifest_json)},
             {'name': '/gettest/d_20', 'hash': md5hex("d" * 20),
              'content_type': 'text/plain', 'bytes': '20'}])
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-abcd-alt',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true',
                          'Etag': md5(_abcd_manifest_json_alt).hexdigest()},
            _abcd_manifest_json_alt)

        _abcdefghijkl_manifest_json = json.dumps(
            [{'name': '/gettest/a_5', 'hash': md5hex("a" * 5),
              'content_type': 'text/plain', 'bytes': '5'},
             {'name': '/gettest/b_10', 'hash': md5hex("b" * 10),
              'content_type': 'text/plain', 'bytes': '10'},
             {'name': '/gettest/c_15', 'hash': md5hex("c" * 15),
              'content_type': 'text/plain', 'bytes': '15'},
             {'name': '/gettest/d_20', 'hash': md5hex("d" * 20),
              'content_type': 'text/plain', 'bytes': '20'},
             {'name': '/gettest/e_25', 'hash': md5hex("e" * 25),
              'content_type': 'text/plain', 'bytes': '25'},
             {'name': '/gettest/f_30', 'hash': md5hex("f" * 30),
              'content_type': 'text/plain', 'bytes': '30'},
             {'name': '/gettest/g_35', 'hash': md5hex("g" * 35),
              'content_type': 'text/plain', 'bytes': '35'},
             {'name': '/gettest/h_40', 'hash': md5hex("h" * 40),
              'content_type': 'text/plain', 'bytes': '40'},
             {'name': '/gettest/i_45', 'hash': md5hex("i" * 45),
              'content_type': 'text/plain', 'bytes': '45'},
             {'name': '/gettest/j_50', 'hash': md5hex("j" * 50),
              'content_type': 'text/plain', 'bytes': '50'},
             {'name': '/gettest/k_55', 'hash': md5hex("k" * 55),
              'content_type': 'text/plain', 'bytes': '55'},
             {'name': '/gettest/l_60', 'hash': md5hex("l" * 60),
              'content_type': 'text/plain', 'bytes': '60'}])
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-abcdefghijkl',
            swob.HTTPOk, {
                'Content-Type': 'application/json',
                'X-Static-Large-Object': 'true',
                'Etag': md5(_abcdefghijkl_manifest_json).hexdigest()},
            _abcdefghijkl_manifest_json)

        self.manifest_abcd_etag = md5hex(
            md5hex("a" * 5) + md5hex(md5hex("b" * 10) + md5hex("c" * 15)) +
            md5hex("d" * 20))

        _bc_ranges_manifest_json = json.dumps(
            [{'name': '/gettest/b_10', 'hash': md5hex('b' * 10),
              'content_type': 'text/plain', 'bytes': '10',
              'range': '4-7'},
             {'name': '/gettest/b_10', 'hash': md5hex('b' * 10),
              'content_type': 'text/plain', 'bytes': '10',
              'range': '2-5'},
             {'name': '/gettest/c_15', 'hash': md5hex('c' * 15),
              'content_type': 'text/plain', 'bytes': '15',
              'range': '0-3'},
             {'name': '/gettest/c_15', 'hash': md5hex('c' * 15),
              'content_type': 'text/plain', 'bytes': '15',
              'range': '11-14'}])
        self.bc_ranges_etag = md5hex(_bc_ranges_manifest_json)
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-bc-ranges',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true',
                          'X-Object-Meta-Plant': 'Ficus',
                          'Etag': self.bc_ranges_etag},
            _bc_ranges_manifest_json)

        _abcd_ranges_manifest_json = json.dumps(
            [{'name': '/gettest/a_5', 'hash': md5hex("a" * 5),
              'content_type': 'text/plain', 'bytes': '5',
              'range': '0-3'},
             {'name': '/gettest/a_5', 'hash': md5hex("a" * 5),
              'content_type': 'text/plain', 'bytes': '5',
              'range': '1-4'},
             {'name': '/gettest/manifest-bc-ranges', 'sub_slo': True,
              'content_type': 'application/json',
              'hash': self.bc_ranges_etag,
              'bytes': 16,
              'range': '8-15'},
             {'name': '/gettest/manifest-bc-ranges', 'sub_slo': True,
              'content_type': 'application/json',
              'hash': self.bc_ranges_etag,
              'bytes': len(_bc_ranges_manifest_json),
              'range': '0-7'},
             {'name': '/gettest/d_20', 'hash': md5hex("d" * 20),
              'content_type': 'text/plain', 'bytes': '20',
              'range': '0-3'},
             {'name': '/gettest/d_20', 'hash': md5hex("d" * 20),
              'content_type': 'text/plain', 'bytes': '20',
              'range': '8-11'}])
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-abcd-ranges',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true',
                          'Etag': md5hex(_abcd_ranges_manifest_json)},
            _abcd_ranges_manifest_json)

        _abcd_subranges_manifest_json = json.dumps(
            [{'name': '/gettest/manifest-abcd-ranges', 'sub_slo': True,
              'hash': md5hex("a" * 8),
              'content_type': 'text/plain', 'bytes': '32',
              'range': '6-10'},
             {'name': '/gettest/manifest-abcd-ranges', 'sub_slo': True,
              'hash': md5hex("a" * 8),
              'content_type': 'text/plain', 'bytes': '32',
              'range': '31-31'},
             {'name': '/gettest/manifest-abcd-ranges', 'sub_slo': True,
              'hash': md5hex("a" * 8),
              'content_type': 'text/plain', 'bytes': '32',
              'range': '14-18'},
             {'name': '/gettest/manifest-abcd-ranges', 'sub_slo': True,
              'hash': md5hex("a" * 8),
              'content_type': 'text/plain', 'bytes': '32',
              'range': '0-0'},
             {'name': '/gettest/manifest-abcd-ranges', 'sub_slo': True,
              'hash': md5hex("a" * 8),
              'content_type': 'text/plain', 'bytes': '32',
              'range': '22-26'}])
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-abcd-subranges',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true',
                          'Etag': md5hex(_abcd_subranges_manifest_json)},
            _abcd_subranges_manifest_json)

        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-badjson',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true',
                          'X-Object-Meta-Fish': 'Bass'},
            "[not {json (at ++++all")

    def tearDown(self):
        self.assertEqual(self.app.unclosed_requests, {})

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
        except ValueError:
            self.fail("Invalid JSON in manifest GET: %r" % body)

        self.assertEqual(
            resp_data,
            [{'hash': md5hex('b' * 10), 'bytes': '10', 'name': '/gettest/b_10',
              'content_type': 'text/plain'},
             {'hash': md5hex('c' * 15), 'bytes': '15', 'name': '/gettest/c_15',
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
        headers = HeaderKeyDict(headers)

        manifest_etag = md5hex(md5hex("b" * 10) + md5hex("c" * 15))
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
        first_ua = self.app.calls_with_headers[0][2].get("User-Agent")
        self.assertFalse(
            "SLO MultipartGET" in first_ua)

    def test_get_manifest_repeated_segments(self):
        _aabbccdd_manifest_json = json.dumps(
            [{'name': '/gettest/a_5', 'hash': md5hex("a" * 5),
              'content_type': 'text/plain', 'bytes': '5'},
             {'name': '/gettest/a_5', 'hash': md5hex("a" * 5),
              'content_type': 'text/plain', 'bytes': '5'},

             {'name': '/gettest/b_10', 'hash': md5hex("b" * 10),
              'content_type': 'text/plain', 'bytes': '10'},
             {'name': '/gettest/b_10', 'hash': md5hex("b" * 10),
              'content_type': 'text/plain', 'bytes': '10'},

             {'name': '/gettest/c_15', 'hash': md5hex("c" * 15),
              'content_type': 'text/plain', 'bytes': '15'},
             {'name': '/gettest/c_15', 'hash': md5hex("c" * 15),
              'content_type': 'text/plain', 'bytes': '15'},

             {'name': '/gettest/d_20', 'hash': md5hex("d" * 20),
              'content_type': 'text/plain', 'bytes': '20'},
             {'name': '/gettest/d_20', 'hash': md5hex("d" * 20),
              'content_type': 'text/plain', 'bytes': '20'}])

        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-aabbccdd',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true',
                          'Etag': md5(_aabbccdd_manifest_json).hexdigest()},
            _aabbccdd_manifest_json)

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-aabbccdd',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '200 OK')
        self.assertEqual(body, (
            'aaaaaaaaaabbbbbbbbbbbbbbbbbbbbcccccccccccccccccccccccccccccc'
            'dddddddddddddddddddddddddddddddddddddddd'))

        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/manifest-aabbccdd'),
            ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get')])

        ranges = [c[2].get('Range') for c in self.app.calls_with_headers]
        self.assertEqual(ranges, [
            None,
            'bytes=0-4,0-4',
            'bytes=0-9,0-9',
            'bytes=0-14,0-14',
            'bytes=0-19,0-19'])

    def test_get_manifest_ratelimiting(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcdefghijkl',
            environ={'REQUEST_METHOD': 'GET'})

        the_time = [time.time()]
        sleeps = []

        def mock_time():
            return the_time[0]

        def mock_sleep(duration):
            sleeps.append(duration)
            the_time[0] += duration

        with patch('time.time', mock_time), \
                patch('eventlet.sleep', mock_sleep), \
                patch.object(self.slo, 'rate_limit_under_size', 999999999), \
                patch.object(self.slo, 'rate_limit_after_segment', 0):
            status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')  # sanity check
        self.assertEqual(sleeps, [2.0, 2.0, 2.0, 2.0, 2.0])

        # give the client the first 4 segments without ratelimiting; we'll
        # sleep less
        del sleeps[:]
        with patch('time.time', mock_time), \
                patch('eventlet.sleep', mock_sleep), \
                patch.object(self.slo, 'rate_limit_under_size', 999999999), \
                patch.object(self.slo, 'rate_limit_after_segment', 4):
            status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')  # sanity check
        self.assertEqual(sleeps, [2.0, 2.0, 2.0])

        # ratelimit segments under 35 bytes; this affects a-f
        del sleeps[:]
        with patch('time.time', mock_time), \
                patch('eventlet.sleep', mock_sleep), \
                patch.object(self.slo, 'rate_limit_under_size', 35), \
                patch.object(self.slo, 'rate_limit_after_segment', 0):
            status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')  # sanity check
        self.assertEqual(sleeps, [2.0, 2.0])

        # ratelimit segments under 36 bytes; this now affects a-g, netting
        # us one more sleep than before
        del sleeps[:]
        with patch('time.time', mock_time), \
                patch('eventlet.sleep', mock_sleep), \
                patch.object(self.slo, 'rate_limit_under_size', 36), \
                patch.object(self.slo, 'rate_limit_after_segment', 0):
            status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')  # sanity check
        self.assertEqual(sleeps, [2.0, 2.0, 2.0])

    def test_if_none_match_matches(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'If-None-Match': self.manifest_abcd_etag})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '304 Not Modified')
        self.assertEqual(headers['Content-Length'], '0')
        self.assertEqual(body, '')

    def test_if_none_match_does_not_match(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'If-None-Match': "not-%s" % self.manifest_abcd_etag})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '200 OK')
        self.assertEqual(
            body, 'aaaaabbbbbbbbbbcccccccccccccccdddddddddddddddddddd')

    def test_if_match_matches(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'If-Match': self.manifest_abcd_etag})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '200 OK')
        self.assertEqual(
            body, 'aaaaabbbbbbbbbbcccccccccccccccdddddddddddddddddddd')

    def test_if_match_does_not_match(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'If-Match': "not-%s" % self.manifest_abcd_etag})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '412 Precondition Failed')
        self.assertEqual(headers['Content-Length'], '0')
        self.assertEqual(body, '')

    def test_if_match_matches_and_range(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'If-Match': self.manifest_abcd_etag,
                     'Range': 'bytes=3-6'})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Content-Length'], '4')
        self.assertEqual(body, 'aabb')

    def test_get_manifest_with_submanifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Content-Length'], '50')
        self.assertEqual(headers['Etag'], '"%s"' % self.manifest_abcd_etag)
        self.assertEqual(
            body, 'aaaaabbbbbbbbbbcccccccccccccccdddddddddddddddddddd')

    def test_get_manifest_with_submanifest_bytes_in_content_type(self):
        # verify correct content-length when the sub-slo segment in the
        # manifest has its actual object content-length appended as swift_bytes
        # to the content-type, and the submanifest length in the bytes field.
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd-alt',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Content-Length'], '50')
        self.assertEqual(headers['Etag'], '"%s"' % self.manifest_abcd_etag)
        self.assertEqual(
            body, 'aaaaabbbbbbbbbbcccccccccccccccdddddddddddddddddddd')

    def test_range_get_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=3-17'})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Content-Length'], '15')
        self.assertTrue('Etag' not in headers)
        self.assertEqual(body, 'aabbbbbbbbbbccc')

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
             ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get')])

        ranges = [c[2].get('Range') for c in self.app.calls_with_headers]
        self.assertEqual(ranges, [
            'bytes=3-17',
            None,
            None,
            'bytes=3-',
            None,
            'bytes=0-2'])
        # we set swift.source for everything but the first request
        self.assertIsNone(self.app.swift_sources[0])
        self.assertEqual(self.app.swift_sources[1:],
                         ['SLO'] * (len(self.app.swift_sources) - 1))

    def test_multiple_ranges_get_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=3-17,20-24,35-999999'})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '206 Partial Content')

        ct, params = parse_content_type(headers['Content-Type'])
        params = dict(params)
        self.assertEqual(ct, 'multipart/byteranges')
        boundary = params.get('boundary')
        self.assertTrue(boundary is not None)

        self.assertEqual(len(body), int(headers['Content-Length']))

        got_mime_docs = []
        for mime_doc_fh in iter_multipart_mime_documents(
                StringIO(body), boundary):
            headers = parse_mime_headers(mime_doc_fh)
            body = mime_doc_fh.read()
            got_mime_docs.append((headers, body))
        self.assertEqual(len(got_mime_docs), 3)

        first_range_headers = got_mime_docs[0][0]
        first_range_body = got_mime_docs[0][1]
        self.assertEqual(first_range_headers['Content-Range'],
                         'bytes 3-17/50')
        self.assertEqual(first_range_headers['Content-Type'],
                         'application/json')
        self.assertEqual(first_range_body, 'aabbbbbbbbbbccc')

        second_range_headers = got_mime_docs[1][0]
        second_range_body = got_mime_docs[1][1]
        self.assertEqual(second_range_headers['Content-Range'],
                         'bytes 20-24/50')
        self.assertEqual(second_range_headers['Content-Type'],
                         'application/json')
        self.assertEqual(second_range_body, 'ccccc')

        third_range_headers = got_mime_docs[2][0]
        third_range_body = got_mime_docs[2][1]
        self.assertEqual(third_range_headers['Content-Range'],
                         'bytes 35-49/50')
        self.assertEqual(third_range_headers['Content-Type'],
                         'application/json')
        self.assertEqual(third_range_body, 'ddddddddddddddd')

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
             ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get')])

        ranges = [c[2].get('Range') for c in self.app.calls_with_headers]
        self.assertEqual(ranges, [
            'bytes=3-17,20-24,35-999999',  # initial GET
            None,                          # re-fetch top-level manifest
            None,                          # fetch manifest-bc as sub-slo
            'bytes=3-',                    # a_5
            None,                          # b_10
            'bytes=0-2,5-9',               # c_15
            'bytes=5-'])                   # d_20
        # we set swift.source for everything but the first request
        self.assertIsNone(self.app.swift_sources[0])
        self.assertEqual(self.app.swift_sources[1:],
                         ['SLO'] * (len(self.app.swift_sources) - 1))

    def test_multiple_ranges_including_suffix_get_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=3-17,-21'})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '206 Partial Content')

        ct, params = parse_content_type(headers['Content-Type'])
        params = dict(params)
        self.assertEqual(ct, 'multipart/byteranges')
        boundary = params.get('boundary')
        self.assertTrue(boundary is not None)

        got_mime_docs = []
        for mime_doc_fh in iter_multipart_mime_documents(
                StringIO(body), boundary):
            headers = parse_mime_headers(mime_doc_fh)
            body = mime_doc_fh.read()
            got_mime_docs.append((headers, body))
        self.assertEqual(len(got_mime_docs), 2)

        first_range_headers = got_mime_docs[0][0]
        first_range_body = got_mime_docs[0][1]
        self.assertEqual(first_range_headers['Content-Range'],
                         'bytes 3-17/50')
        self.assertEqual(first_range_body, 'aabbbbbbbbbbccc')

        second_range_headers = got_mime_docs[1][0]
        second_range_body = got_mime_docs[1][1]
        self.assertEqual(second_range_headers['Content-Range'],
                         'bytes 29-49/50')
        self.assertEqual(second_range_body, 'cdddddddddddddddddddd')

    def test_range_get_includes_whole_manifest(self):
        # If the first range GET results in retrieval of the entire manifest
        # body (which we can detect by looking at Content-Range), then we
        # should not go make a second, non-ranged request just to retrieve the
        # same bytes again.
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=0-999999999'})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(
            body, 'aaaaabbbbbbbbbbcccccccccccccccdddddddddddddddddddd')

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
             ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get')])

    def test_range_get_beyond_manifest(self):
        big = 'e' * 1024 * 1024
        big_etag = md5hex(big)
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/big_seg',
            swob.HTTPOk, {'Content-Type': 'application/foo',
                          'Etag': big_etag}, big)
        big_manifest = json.dumps(
            [{'name': '/gettest/big_seg', 'hash': big_etag,
              'bytes': 1024 * 1024, 'content_type': 'application/foo'}])
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/big_manifest',
            swob.HTTPOk, {'Content-Type': 'application/octet-stream',
                          'X-Static-Large-Object': 'true',
                          'Etag': md5(big_manifest).hexdigest()},
            big_manifest)

        req = Request.blank(
            '/v1/AUTH_test/gettest/big_manifest',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=100000-199999'})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '206 Partial Content')
        count_e = sum(1 if x == 'e' else 0 for x in body)
        self.assertEqual(count_e, 100000)
        self.assertEqual(len(body) - count_e, 0)

        self.assertEqual(
            self.app.calls, [
                # has Range header, gets 416
                ('GET', '/v1/AUTH_test/gettest/big_manifest'),
                # retry the first one
                ('GET', '/v1/AUTH_test/gettest/big_manifest'),
                ('GET',
                 '/v1/AUTH_test/gettest/big_seg?multipart-manifest=get')])

    def test_range_get_bogus_content_range(self):
        # Just a little paranoia; Swift currently sends back valid
        # Content-Range headers, but if somehow someone sneaks an invalid one
        # in there, we'll ignore it.

        def content_range_breaker_factory(app):
            def content_range_breaker(env, start_response):
                req = swob.Request(env)
                resp = req.get_response(app)
                resp.headers['Content-Range'] = 'triscuits'
                return resp(env, start_response)
            return content_range_breaker

        self.slo = slo.filter_factory({})(
            content_range_breaker_factory(self.app))

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=0-999999999'})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(
            body, 'aaaaabbbbbbbbbbcccccccccccccccdddddddddddddddddddd')

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
             ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get')])

    def test_range_get_manifest_on_segment_boundaries(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=5-29'})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Content-Length'], '25')
        self.assertTrue('Etag' not in headers)
        self.assertEqual(body, 'bbbbbbbbbbccccccccccccccc')

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
             ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get')])

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
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Content-Length'], '1')
        self.assertEqual(body, 'a')

        # Make sure we don't get any objects we don't need, including
        # submanifests.
        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get')])

    def test_range_get_manifest_sub_slo(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=25-30'})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)
        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Content-Length'], '6')
        self.assertEqual(body, 'cccccd')

        # Make sure we don't get any objects we don't need, including
        # submanifests.
        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get')])

    def test_range_get_manifest_overlapping_end(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=45-55'})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

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

    def test_get_segment_with_non_ascii_path(self):
        segment_body = u"a møøse once bit my sister".encode("utf-8")
        self.app.register(
            'GET', u'/v1/AUTH_test/ünicode/öbject-segment'.encode('utf-8'),
            swob.HTTPOk, {'Content-Length': str(len(segment_body)),
                          'Etag': md5hex(segment_body)},
            segment_body)

        manifest_json = json.dumps([{'name': u'/ünicode/öbject-segment',
                                     'hash': md5hex(segment_body),
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
        headers = HeaderKeyDict(headers)
        self.assertEqual(status, '200 OK')
        self.assertEqual(body, segment_body)

    def test_get_range_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd-ranges',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Content-Length'], '32')
        self.assertEqual(headers['Content-Type'], 'application/json')
        self.assertEqual(body, 'aaaaaaaaccccccccbbbbbbbbdddddddd')

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd-ranges'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc-ranges'),
             ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get')])

        ranges = [c[2].get('Range') for c in self.app.calls_with_headers]
        self.assertEqual(ranges, [
            None,
            None,
            'bytes=0-3,1-',
            'bytes=0-3,11-',
            'bytes=4-7,2-5',
            'bytes=0-3,8-11'])
        # we set swift.source for everything but the first request
        self.assertIsNone(self.app.swift_sources[0])
        self.assertEqual(self.app.swift_sources[1:],
                         ['SLO'] * (len(self.app.swift_sources) - 1))
        self.assertEqual(md5hex(''.join([
            md5hex('a' * 5), ':0-3;',
            md5hex('a' * 5), ':1-4;',
            self.bc_ranges_etag, ':8-15;',
            self.bc_ranges_etag, ':0-7;',
            md5hex('d' * 20), ':0-3;',
            md5hex('d' * 20), ':8-11;',
        ])), headers['Etag'].strip('"'))

    def test_get_subrange_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd-subranges',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Content-Length'], '17')
        self.assertEqual(headers['Content-Type'], 'application/json')
        self.assertEqual(body, 'aacccdccbbbabbddd')

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd-subranges'),
             ('GET', '/v1/AUTH_test/gettest/manifest-abcd-ranges'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc-ranges'),
             ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get')])

        ranges = [c[2].get('Range') for c in self.app.calls_with_headers]
        self.assertEqual(ranges, [
            None,
            None,
            None,
            'bytes=3-',
            'bytes=0-2',
            'bytes=11-11',
            'bytes=13-',
            'bytes=4-6',
            'bytes=0-0',
            'bytes=4-5',
            'bytes=0-2'])
        # we set swift.source for everything but the first request
        self.assertIsNone(self.app.swift_sources[0])
        self.assertEqual(self.app.swift_sources[1:],
                         ['SLO'] * (len(self.app.swift_sources) - 1))

    def test_range_get_range_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd-ranges',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=7-26'})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Content-Length'], '20')
        self.assertEqual(headers['Content-Type'], 'application/json')
        self.assertNotIn('Etag', headers)
        self.assertEqual(body, 'accccccccbbbbbbbbddd')

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd-ranges'),
             ('GET', '/v1/AUTH_test/gettest/manifest-abcd-ranges'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc-ranges'),
             ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get')])

        ranges = [c[2].get('Range') for c in self.app.calls_with_headers]
        self.assertEqual(ranges, [
            'bytes=7-26',
            None,
            None,
            'bytes=4-',
            'bytes=0-3,11-',
            'bytes=4-7,2-5',
            'bytes=0-2'])
        # we set swift.source for everything but the first request
        self.assertIsNone(self.app.swift_sources[0])
        self.assertEqual(self.app.swift_sources[1:],
                         ['SLO'] * (len(self.app.swift_sources) - 1))

    def test_range_get_subrange_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd-subranges',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=4-12'})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Content-Length'], '9')
        self.assertEqual(headers['Content-Type'], 'application/json')
        self.assertEqual(body, 'cdccbbbab')

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd-subranges'),
             ('GET', '/v1/AUTH_test/gettest/manifest-abcd-subranges'),
             ('GET', '/v1/AUTH_test/gettest/manifest-abcd-ranges'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc-ranges'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get')])

        ranges = [c[2].get('Range') for c in self.app.calls_with_headers]
        self.assertEqual(ranges, [
            'bytes=4-12',
            None,
            None,
            None,
            'bytes=2-2',
            'bytes=11-11',
            'bytes=13-',
            'bytes=4-6',
            'bytes=0-0',
            'bytes=4-4'])
        # we set swift.source for everything but the first request
        self.assertIsNone(self.app.swift_sources[0])
        self.assertEqual(self.app.swift_sources[1:],
                         ['SLO'] * (len(self.app.swift_sources) - 1))

    def test_range_get_includes_whole_range_manifest(self):
        # If the first range GET results in retrieval of the entire manifest
        # body (which we can detect by looking at Content-Range), then we
        # should not go make a second, non-ranged request just to retrieve the
        # same bytes again.
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd-ranges',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=0-999999999'})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Content-Length'], '32')
        self.assertEqual(headers['Content-Type'], 'application/json')
        self.assertEqual(body, 'aaaaaaaaccccccccbbbbbbbbdddddddd')

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd-ranges'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc-ranges'),
             ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get')])

        ranges = [c[2].get('Range') for c in self.app.calls_with_headers]
        self.assertEqual(ranges, [
            'bytes=0-999999999',
            None,
            'bytes=0-3,1-',
            'bytes=0-3,11-',
            'bytes=4-7,2-5',
            'bytes=0-3,8-11'])
        # we set swift.source for everything but the first request
        self.assertIsNone(self.app.swift_sources[0])
        self.assertEqual(self.app.swift_sources[1:],
                         ['SLO'] * (len(self.app.swift_sources) - 1))

    def test_get_bogus_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-badjson',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Content-Length'], '0')
        self.assertEqual(headers['X-Object-Meta-Fish'], 'Bass')
        self.assertEqual(body, '')

    def test_generator_closure(self):
        # Test that the SLO WSGI iterable closes its internal .app_iter when
        # it receives a close() message.
        #
        # This is sufficient to fix a memory leak. The memory leak arises
        # due to cyclic references involving a running generator; a running
        # generator sometimes preventes the GC from collecting it in the
        # same way that an object with a defined __del__ does.
        #
        # There are other ways to break the cycle and fix the memory leak as
        # well; calling .close() on the generator is sufficient, but not
        # necessary. However, having this test is better than nothing for
        # preventing regressions.
        leaks = [0]

        class LeakTracker(object):
            def __init__(self, inner_iter):
                leaks[0] += 1
                self.inner_iter = iter(inner_iter)

            def __iter__(self):
                return self

            def next(self):
                return next(self.inner_iter)

            def close(self):
                leaks[0] -= 1
                close_if_possible(self.inner_iter)

        class LeakTrackingSegmentedIterable(slo.SegmentedIterable):
            def _internal_iter(self, *a, **kw):
                it = super(
                    LeakTrackingSegmentedIterable, self)._internal_iter(
                        *a, **kw)
                return LeakTracker(it)

        status = [None]
        headers = [None]

        def start_response(s, h, ei=None):
            status[0] = s
            headers[0] = h

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET',
                     'HTTP_ACCEPT': 'application/json'})

        # can't self.call_slo() here since we don't want to consume the
        # whole body
        with patch.object(slo, 'SegmentedIterable',
                          LeakTrackingSegmentedIterable):
            app_resp = self.slo(req.environ, start_response)
        self.assertEqual(status[0], '200 OK')  # sanity check
        body_iter = iter(app_resp)
        chunk = next(body_iter)
        self.assertEqual(chunk, 'aaaaa')  # sanity check

        app_resp.close()
        self.assertEqual(0, leaks[0])

    def test_head_manifest_is_efficient(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'HEAD'})
        status, headers, body = self.call_slo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Content-Length'], '50')
        self.assertEqual(headers['Etag'], '"%s"' % self.manifest_abcd_etag)
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
        for i in range(20):
            self.app.register('GET', '/v1/AUTH_test/gettest/obj%d' % i,
                              swob.HTTPOk, {'Content-Type': 'text/plain',
                                            'Etag': md5hex('body%02d' % i)},
                              'body%02d' % i)

        manifest_json = json.dumps([{'name': '/gettest/obj20',
                                     'hash': md5hex('body20'),
                                     'content_type': 'text/plain',
                                     'bytes': '6'}])
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/man%d' % i,
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true',
                          'Etag': 'man%d' % i},
            manifest_json)

        for i in range(19, 0, -1):
            manifest_data = [
                {'name': '/gettest/obj%d' % i,
                 'hash': md5hex('body%02d' % i),
                 'bytes': '6',
                 'content_type': 'text/plain'},
                {'name': '/gettest/man%d' % (i + 1),
                 'hash': 'man%d' % (i + 1),
                 'sub_slo': True,
                 'bytes': len(manifest_json),
                 'content_type': 'application/json'}]

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
        headers = HeaderKeyDict(headers)

        self.assertIsInstance(exc, ListingIterError)
        # we don't know at header-sending time that things are going to go
        # wrong, so we end up with a 200 and a truncated body
        self.assertEqual(status, '200 OK')
        self.assertEqual(body, ('body01body02body03body04body05' +
                                'body06body07body08body09body10'))
        # make sure we didn't keep asking for segments
        self.assertEqual(self.app.call_count, 20)

    def test_sub_slo_recursion(self):
        # man1 points to man2 and obj1, man2 points to man3 and obj2...
        for i in range(11):
            self.app.register('GET', '/v1/AUTH_test/gettest/obj%d' % i,
                              swob.HTTPOk, {'Content-Type': 'text/plain',
                                            'Content-Length': '6',
                                            'Etag': md5hex('body%02d' % i)},
                              'body%02d' % i)

        manifest_json = json.dumps([{'name': '/gettest/obj%d' % i,
                                     'hash': md5hex('body%2d' % i),
                                     'content_type': 'text/plain',
                                     'bytes': '6'}])
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/man%d' % i,
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true',
                          'Etag': 'man%d' % i},
            manifest_json)
        self.app.register(
            'HEAD', '/v1/AUTH_test/gettest/obj%d' % i,
            swob.HTTPOk, {'Content-Length': '6',
                          'Etag': md5hex('body%2d' % i)},
            None)

        for i in range(9, 0, -1):
            manifest_data = [
                {'name': '/gettest/man%d' % (i + 1),
                 'hash': 'man%d' % (i + 1),
                 'sub_slo': True,
                 'bytes': (10 - i) * 6,
                 'content_type': 'application/json'},
                {'name': '/gettest/obj%d' % i,
                 'hash': md5hex('body%02d' % i),
                 'bytes': '6',
                 'content_type': 'text/plain'}]

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
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(body, ('body10body09body08body07body06' +
                                'body05body04body03body02body01'))

        self.assertEqual(self.app.call_count, 20)

    def test_sub_slo_recursion_limit(self):
        # man1 points to man2 and obj1, man2 points to man3 and obj2...
        for i in range(12):
            self.app.register('GET', '/v1/AUTH_test/gettest/obj%d' % i,
                              swob.HTTPOk,
                              {'Content-Type': 'text/plain',
                               'Content-Length': '6',
                               'Etag': md5hex('body%02d' % i)}, 'body%02d' % i)

        manifest_json = json.dumps([{'name': '/gettest/obj%d' % i,
                                     'hash': md5hex('body%2d' % i),
                                     'content_type': 'text/plain',
                                     'bytes': '6'}])
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/man%d' % i,
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true',
                          'Etag': 'man%d' % i},
            manifest_json)
        self.app.register(
            'HEAD', '/v1/AUTH_test/gettest/obj%d' % i,
            swob.HTTPOk, {'Content-Length': '6',
                          'Etag': md5hex('body%2d' % i)},
            None)

        for i in range(11, 0, -1):
            manifest_data = [
                {'name': '/gettest/man%d' % (i + 1),
                 'hash': 'man%d' % (i + 1),
                 'sub_slo': True,
                 'bytes': (12 - i) * 6,
                 'content_type': 'application/json'},
                {'name': '/gettest/obj%d' % i,
                 'hash': md5hex('body%02d' % i),
                 'bytes': '6',
                 'content_type': 'text/plain'}]
            manifest_json = json.dumps(manifest_data)
            self.app.register('GET', '/v1/AUTH_test/gettest/man%d' % i,
                              swob.HTTPOk,
                              {'Content-Type': 'application/json',
                               'X-Static-Large-Object': 'true',
                               'Etag': 'man%d' % i},
                              manifest_json)

        req = Request.blank(
            '/v1/AUTH_test/gettest/man1',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '409 Conflict')
        self.assertEqual(self.app.call_count, 10)
        error_lines = self.slo.logger.get_lines_for_level('error')
        self.assertEqual(len(error_lines), 1)
        self.assertTrue(error_lines[0].startswith(
            'ERROR: An error occurred while retrieving segments'))

    def test_get_with_if_modified_since(self):
        # It's important not to pass the If-[Un]Modified-Since header to the
        # proxy for segment or submanifest GET requests, as it may result in
        # 304 Not Modified responses, and those don't contain any useful data.
        req = swob.Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'If-Modified-Since': 'Wed, 12 Feb 2014 22:24:52 GMT',
                     'If-Unmodified-Since': 'Thu, 13 Feb 2014 23:25:53 GMT'})
        status, headers, body, exc = self.call_slo(req, expect_exception=True)

        for _, _, hdrs in self.app.calls_with_headers[1:]:
            self.assertFalse('If-Modified-Since' in hdrs)
            self.assertFalse('If-Unmodified-Since' in hdrs)

    def test_error_fetching_segment(self):
        self.app.register('GET', '/v1/AUTH_test/gettest/c_15',
                          swob.HTTPUnauthorized, {}, None)

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body, exc = self.call_slo(req, expect_exception=True)
        headers = HeaderKeyDict(headers)

        self.assertIsInstance(exc, SegmentError)
        self.assertEqual(status, '200 OK')
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
            ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
            ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
            # This one has the error, and so is the last one we fetch.
            ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get')])

    def test_error_fetching_submanifest(self):
        self.app.register('GET', '/v1/AUTH_test/gettest/manifest-bc',
                          swob.HTTPUnauthorized, {}, None)
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body, exc = self.call_slo(req, expect_exception=True)

        self.assertIsInstance(exc, ListingIterError)
        self.assertEqual("200 OK", status)
        self.assertEqual("aaaaa", body)
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
            # This one has the error, and so is the last one we fetch.
            ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
            # But we were looking ahead to see if we could combine ranges,
            # so we still get the first segment out
            ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get')])

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
                         'content_type': 'application/json',
                         'hash': 'manifest-a',
                         'bytes': '12345'}]))

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-manifest-a',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('409 Conflict', status)
        error_lines = self.slo.logger.get_lines_for_level('error')
        self.assertEqual(len(error_lines), 1)
        self.assertTrue(error_lines[0].startswith(
            'ERROR: An error occurred while retrieving segments'))

    def test_invalid_json_submanifest(self):
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-bc',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true',
                          'X-Object-Meta-Plant': 'Ficus'},
            "[this {isn't (JSON")

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body, exc = self.call_slo(req, expect_exception=True)

        self.assertIsInstance(exc, ListingIterError)
        self.assertEqual('200 OK', status)
        self.assertEqual(body, 'aaaaa')

    def test_mismatched_etag(self):
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-a-b-badetag-c',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            json.dumps([{'name': '/gettest/a_5', 'hash': md5hex('a' * 5),
                         'content_type': 'text/plain', 'bytes': '5'},
                        {'name': '/gettest/b_10', 'hash': 'wrong!',
                         'content_type': 'text/plain', 'bytes': '10'},
                        {'name': '/gettest/c_15', 'hash': md5hex('c' * 15),
                         'content_type': 'text/plain', 'bytes': '15'}]))

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-a-b-badetag-c',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body, exc = self.call_slo(req, expect_exception=True)

        self.assertIsInstance(exc, SegmentError)
        self.assertEqual('200 OK', status)
        self.assertEqual(body, 'aaaaa')

    def test_mismatched_size(self):
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-a-b-badsize-c',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            json.dumps([{'name': '/gettest/a_5', 'hash': md5hex('a' * 5),
                         'content_type': 'text/plain', 'bytes': '5'},
                        {'name': '/gettest/b_10', 'hash': md5hex('b' * 10),
                         'content_type': 'text/plain', 'bytes': '999999'},
                        {'name': '/gettest/c_15', 'hash': md5hex('c' * 15),
                         'content_type': 'text/plain', 'bytes': '15'}]))

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-a-b-badsize-c',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body, exc = self.call_slo(req, expect_exception=True)

        self.assertIsInstance(exc, SegmentError)
        self.assertEqual('200 OK', status)
        self.assertEqual(body, 'aaaaa')

    def test_first_segment_mismatched_etag(self):
        self.app.register('GET', '/v1/AUTH_test/gettest/manifest-badetag',
                          swob.HTTPOk, {'Content-Type': 'application/json',
                                        'X-Static-Large-Object': 'true'},
                          json.dumps([{'name': '/gettest/a_5',
                                       'hash': 'wrong!',
                                       'content_type': 'text/plain',
                                       'bytes': '5'}]))

        req = Request.blank('/v1/AUTH_test/gettest/manifest-badetag',
                            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('409 Conflict', status)
        error_lines = self.slo.logger.get_lines_for_level('error')
        self.assertEqual(len(error_lines), 1)
        self.assertTrue(error_lines[0].startswith(
            'ERROR: An error occurred while retrieving segments'))

    def test_first_segment_mismatched_size(self):
        self.app.register('GET', '/v1/AUTH_test/gettest/manifest-badsize',
                          swob.HTTPOk, {'Content-Type': 'application/json',
                                        'X-Static-Large-Object': 'true'},
                          json.dumps([{'name': '/gettest/a_5',
                                       'hash': md5hex('a' * 5),
                                       'content_type': 'text/plain',
                                       'bytes': '999999'}]))

        req = Request.blank('/v1/AUTH_test/gettest/manifest-badsize',
                            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('409 Conflict', status)
        error_lines = self.slo.logger.get_lines_for_level('error')
        self.assertEqual(len(error_lines), 1)
        self.assertTrue(error_lines[0].startswith(
            'ERROR: An error occurred while retrieving segments'))

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

        with patch.object(slo, 'is_success', mock_is_success), \
                patch('swift.common.request_helpers.time.time',
                      mock_time), \
                patch('swift.common.request_helpers.is_success',
                      mock_is_success):
            status, headers, body, exc = self.call_slo(
                req, expect_exception=True)

        self.assertIsInstance(exc, SegmentError)
        self.assertEqual(status, '200 OK')
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
            ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
            ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get')])

    def test_first_segment_not_exists(self):
        self.app.register('GET', '/v1/AUTH_test/gettest/not_exists_obj',
                          swob.HTTPNotFound, {}, None)
        self.app.register('GET', '/v1/AUTH_test/gettest/manifest-not-exists',
                          swob.HTTPOk, {'Content-Type': 'application/json',
                                        'X-Static-Large-Object': 'true'},
                          json.dumps([{'name': '/gettest/not_exists_obj',
                                       'hash': md5hex('not_exists_obj'),
                                       'content_type': 'text/plain',
                                       'bytes': '%d' % len('not_exists_obj')
                                       }]))

        req = Request.blank('/v1/AUTH_test/gettest/manifest-not-exists',
                            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('409 Conflict', status)
        error_lines = self.slo.logger.get_lines_for_level('error')
        self.assertEqual(len(error_lines), 1)
        self.assertTrue(error_lines[0].startswith(
            'ERROR: An error occurred while retrieving segments'))


class TestSloBulkLogger(unittest.TestCase):
    def test_reused_logger(self):
        slo_mware = slo.filter_factory({})('fake app')
        self.assertTrue(slo_mware.logger is slo_mware.bulk_deleter.logger)

    def test_passes_through_concurrency(self):
        slo_mware = slo.filter_factory({'delete_concurrency': 5})('fake app')
        self.assertEqual(5, slo_mware.bulk_deleter.delete_concurrency)


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
        self.assertEqual(swift_info['slo'].get('min_segment_size'), 1)
        self.assertEqual(swift_info['slo'].get('max_manifest_size'),
                         mware.max_manifest_size)

if __name__ == '__main__':
    unittest.main()
