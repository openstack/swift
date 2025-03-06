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

import base64
from datetime import datetime
import json
import time
import unittest
import string

from unittest.mock import patch

from io import BytesIO

from swift.common import swob, registry
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.middleware import slo
from swift.common.request_helpers import update_etag_is_at_header
from swift.common.swob import Request, HTTPException, str_to_wsgi, \
    bytes_to_wsgi
from swift.common.utils import quote, closing_if_possible, close_if_possible, \
    parse_content_type, iter_multipart_mime_documents, parse_mime_headers, \
    Timestamp, md5
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
                              'size_bytes': 100}]).encode('ascii')


def fake_start_response(*args, **kwargs):
    pass


def md5hex(s):
    if not isinstance(s, bytes):
        s = s.encode('ascii')
    return md5(s, usedforsecurity=False).hexdigest()


class SloTestCase(unittest.TestCase):

    def setUp(self):
        self.app = FakeSwift()
        slo_conf = {'rate_limit_under_size': '0'}
        self.slo = slo.filter_factory(slo_conf)(self.app)
        self.slo.logger = self.app.logger

    def call_app(self, req, app=None):
        if app is None:
            app = self.app

        req.headers.setdefault("User-Agent", "Mozzarella Foxfire")

        status = [None]
        headers = [None]

        def start_response(s, h, ei=None):
            status[0] = s
            headers[0] = h

        body_iter = app(req.environ, start_response)
        body = b''
        # appease the close-checker
        with closing_if_possible(body_iter):
            for chunk in body_iter:
                body += chunk
        return status[0], headers[0], body

    def call_slo(self, req, **kwargs):
        return self.call_app(req, app=self.slo, **kwargs)


class TestSloMiddleware(SloTestCase):

    def setUp(self):
        super(TestSloMiddleware, self).setUp()

        self.app.register(
            'GET', '/', swob.HTTPOk, {}, b'passed')
        self.app.register(
            'PUT', '/', swob.HTTPOk, {}, b'passed')

    def test_handle_multipart_no_obj(self):
        req = Request.blank('/')
        resp_iter = self.slo(req.environ, fake_start_response)
        self.assertEqual(self.app.calls, [('GET', '/')])
        self.assertEqual(b''.join(resp_iter), b'passed')

    def test_slo_header_assigned(self):
        req = Request.blank(
            '/v1/a/c/o', headers={'x-static-large-object': "true"},
            environ={'REQUEST_METHOD': 'PUT'})
        resp = b''.join(self.slo(req.environ, fake_start_response))
        self.assertTrue(
            resp.startswith(b'X-Static-Large-Object is a reserved header'))

    def test_slo_PUT_env_override(self):
        path = '/v1/a/c/o'
        body = b'manifest body not checked when override flag set'
        resp_status = []

        def start_response(status, headers, *args):
            resp_status.append(status)

        req = Request.blank(
            path, headers={'x-static-large-object': "true"},
            environ={'REQUEST_METHOD': 'PUT', 'swift.slo_override': True},
            body=body)
        self.app.register('PUT', path, swob.HTTPCreated, {})
        resp_iter = self.slo(req.environ, start_response)
        self.assertEqual(b'', b''.join(resp_iter))
        self.assertEqual(self.app.calls, [('PUT', path)])
        self.assertEqual(body, self.app.uploaded[path][1])
        self.assertEqual(resp_status[0], '201 Created')

    def _put_bogus_slo(self, manifest_text,
                       manifest_path='/v1/a/c/the-manifest'):
        with self.assertRaises(HTTPException) as catcher:
            slo.parse_and_validate_input(manifest_text, manifest_path)
        self.assertEqual(400, catcher.exception.status_int)
        return catcher.exception.body.decode('utf-8')

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

        # This also catches typos
        self.assertEqual(
            'Index 0: extraneous keys "egat"\n',
            self._put_bogus_slo(json.dumps(
                [{'path': '/cont/object', 'egat': 'etagoftheobjectsegment',
                  'size_bytes': 100}])))
        self.assertEqual(
            'Index 0: extraneous keys "siez_bytes"\n',
            self._put_bogus_slo(json.dumps(
                [{'path': '/cont/object', 'etag': 'etagoftheobjectsegment',
                  'siez_bytes': 100}])))

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
        ]).encode('ascii')
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

    def test_container_listing(self):
        listing_json = json.dumps([{
            "bytes": 104857600,
            "content_type": "application/x-troff-me",
            "hash": "8de7b0b1551660da51d8d96a53b85531; this=that;"
            "slo_etag=dc9947c2b53a3f55fe20c1394268e216",
            "last_modified": "2018-07-12T03:14:39.532020",
            "name": "test.me"
        }]).encode('ascii')
        self.app.register(
            'GET', '/v1/a/c',
            swob.HTTPOk,
            {'Content-Type': 'application/json',
             'Content-Length': len(listing_json)},
            listing_json)
        req = Request.blank('/v1/a/c', method='GET')
        status, headers, body = self.call_slo(req)
        self.assertEqual(json.loads(body), [{
            "slo_etag": '"dc9947c2b53a3f55fe20c1394268e216"',
            "hash": "8de7b0b1551660da51d8d96a53b85531; this=that",
            "name": "test.me",
            "bytes": 104857600,
            "last_modified": "2018-07-12T03:14:39.532020",
            "content_type": "application/x-troff-me",
        }])


class TestSloPutManifest(SloTestCase):

    def setUp(self):
        super(TestSloPutManifest, self).setUp()

        self.app.register(
            'GET', '/', swob.HTTPOk, {}, b'passed')
        self.app.register(
            'PUT', '/', swob.HTTPOk, {}, b'passed')

        self.app.register(
            'HEAD', '/v1/AUTH_test/cont/missing-object',
            swob.HTTPNotFound, {}, None)
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
            'PUT', '/v1/AUTH_test/c/man', swob.HTTPCreated,
            {'Last-Modified': 'Fri, 01 Feb 2012 20:38:36 GMT'}, None)
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
              'content_type': 'text/plain', 'bytes': '5'}]).encode('ascii')
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
        req = Request.blank('/v1/a/c/o?multipart-manifest=put', method='PUT')
        req.content_length = self.slo.max_manifest_size + 1
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '413 Request Entity Too Large')

        with patch.object(self.slo, 'max_manifest_segments', 0):
            req = Request.blank('/v1/a/c/o?multipart-manifest=put',
                                method='PUT', body=test_json_data)
            status, headers, body = self.call_slo(req)
            self.assertEqual(status, '413 Request Entity Too Large')

        req = Request.blank('/v1/a/c/o?multipart-manifest=put', method='PUT',
                            headers={'X-Copy-From': 'lala'})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '405 Method Not Allowed')

        # we already validated that there are enough path segments in __call__
        for path in ('/', '/v1/', '/v1/a/', '/v1/a/c/'):
            req = Request.blank(
                path + '?multipart-manifest=put',
                environ={'REQUEST_METHOD': 'PUT'}, body=test_json_data)
            with self.assertRaises(ValueError):
                list(self.slo.handle_multipart_put(req, fake_start_response))

            req = Request.blank(
                path.rstrip('/') + '?multipart-manifest=put',
                environ={'REQUEST_METHOD': 'PUT'}, body=test_json_data)
            with self.assertRaises(ValueError):
                list(self.slo.handle_multipart_put(req, fake_start_response))

    def test_handle_multipart_put_success(self):
        override_header = 'X-Object-Sysmeta-Container-Update-Override-Etag'
        headers = {
            'Accept': 'test',
            override_header: '; params=are important',
        }
        req = Request.blank(
            '/v1/AUTH_test/c/man?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, headers=headers,
            body=test_json_data)
        for h in ('X-Static-Large-Object', 'X-Object-Sysmeta-Slo-Etag',
                  'X-Object-Sysmeta-Slo-Size'):
            # Sanity
            self.assertNotIn(h, req.headers)

        status, headers, body = self.call_slo(req)
        gen_etag = '"' + md5hex('etagoftheobjectsegment') + '"'
        self.assertIn(('Etag', gen_etag), headers)
        self.assertIn('X-Static-Large-Object', req.headers)
        self.assertEqual(req.headers['X-Static-Large-Object'], 'True')
        self.assertIn('Etag', req.headers)
        self.assertIn('X-Object-Sysmeta-Slo-Etag', req.headers)
        self.assertIn('X-Object-Sysmeta-Container-Update-Override-Etag',
                      req.headers)
        self.assertEqual(req.headers['X-Object-Sysmeta-Slo-Etag'],
                         gen_etag.strip('"'))
        self.assertEqual(
            req.headers['X-Object-Sysmeta-Container-Update-Override-Etag'],
            '%s; params=are important; slo_etag=%s' % (
                req.headers['Etag'], gen_etag.strip('"')))
        self.assertIn('X-Object-Sysmeta-Slo-Size', req.headers)
        self.assertEqual(req.headers['X-Object-Sysmeta-Slo-Size'], '100')
        self.assertIn('Content-Type', req.headers)
        self.assertTrue(
            req.headers['Content-Type'].endswith(';swift_bytes=100'),
            'Content-Type %r does not end with swift_bytes=100' %
            req.headers['Content-Type'])

    @patch('swift.common.middleware.slo.time')
    def test_handle_multipart_put_fast_heartbeat(self, mock_time):
        mock_time.time.side_effect = [
            0,  # start time
            1,  # first segment's fast
            2,  # second segment's also fast!
        ]
        test_json_data = json.dumps([{'path': u'/cont/object\u2661',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 100},
                                     {'path': '/cont/object',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 100}]).encode('ascii')
        req = Request.blank(
            '/v1/AUTH_test/c/man?multipart-manifest=put&heartbeat=on',
            environ={'REQUEST_METHOD': 'PUT'}, headers={'Accept': 'test'},
            body=test_json_data)

        status, headers, body = self.call_slo(req)
        self.assertEqual('202 Accepted', status)
        headers_found = [h.lower() for h, v in headers]
        self.assertNotIn('etag', headers_found)
        gen_etag = '"' + md5hex('etagoftheobjectsegment' * 2) + '"'
        self.assertTrue(body.startswith(b' \r\n\r\n'),
                        'Expected body to start with single space and two '
                        'blank lines; got %r' % body)
        self.assertIn(b'\nResponse Status: 201 Created\n', body)
        self.assertIn(b'\nResponse Body: \n', body)
        self.assertIn(('\nEtag: %s\n' % gen_etag).encode('ascii'), body)
        self.assertIn(b'\nLast Modified: Fri, 01 Feb 2012 20:38:36 GMT\n',
                      body)

    @patch('swift.common.middleware.slo.time')
    def test_handle_multipart_long_running_put_success(self, mock_time):
        mock_time.time.side_effect = [
            0,  # start time
            1,  # first segment's fast
            20,  # second segment's slow
        ]
        test_json_data = json.dumps([{'path': u'/cont/object\u2661',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 100},
                                     {'path': '/cont/object',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 100}]).encode('ascii')
        req = Request.blank(
            '/v1/AUTH_test/c/man?multipart-manifest=put&heartbeat=on',
            environ={'REQUEST_METHOD': 'PUT'}, headers={'Accept': 'test'},
            body=test_json_data)

        status, headers, body = self.call_slo(req)
        self.assertEqual('202 Accepted', status)
        headers_found = [h.lower() for h, v in headers]
        self.assertNotIn('etag', headers_found)
        gen_etag = '"' + md5hex('etagoftheobjectsegment' * 2) + '"'
        self.assertTrue(body.startswith(b'  \r\n\r\n'),
                        'Expected body to start with two spaces and two '
                        'blank lines; got %r' % body)
        self.assertIn(b'\nResponse Status: 201 Created\n', body)
        self.assertIn(b'\nResponse Body: \n', body)
        self.assertIn(('\nEtag: %s\n' % gen_etag).encode('ascii'), body)
        self.assertIn(b'\nLast Modified: Fri, 01 Feb 2012 20:38:36 GMT\n',
                      body)

    @patch('swift.common.middleware.slo.time')
    def test_handle_multipart_long_running_put_success_json(self, mock_time):
        mock_time.time.side_effect = [
            0,  # start time
            11,  # first segment's slow
            22,  # second segment's also slow
        ]
        test_json_data = json.dumps([{'path': u'/cont/object\u2661',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 100},
                                     {'path': '/cont/object',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 100}]).encode('ascii')
        req = Request.blank(
            '/v1/AUTH_test/c/man?multipart-manifest=put&heartbeat=on',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'Accept': 'application/json'},
            body=test_json_data)

        status, headers, body = self.call_slo(req)
        self.assertEqual('202 Accepted', status)
        headers_found = [h.lower() for h, v in headers]
        self.assertNotIn('etag', headers_found)
        gen_etag = '"' + md5hex('etagoftheobjectsegment' * 2) + '"'
        self.assertTrue(body.startswith(b'   \r\n\r\n'),
                        'Expected body to start with three spaces and two '
                        'blank lines; got %r' % body)
        body = json.loads(body)
        self.assertEqual(body['Response Status'], '201 Created')
        self.assertEqual(body['Response Body'], '')
        self.assertEqual(body['Etag'], gen_etag)
        self.assertEqual(body['Last Modified'],
                         'Fri, 01 Feb 2012 20:38:36 GMT')

    @patch('swift.common.middleware.slo.time')
    def test_handle_multipart_long_running_put_failure(self, mock_time):
        mock_time.time.side_effect = [
            0,  # start time
            1,  # first segment's fast
            20,  # second segment's slow
        ]
        test_json_data = json.dumps([{'path': u'/cont/missing-object',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 100},
                                     {'path': '/cont/object',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 99}]).encode('ascii')
        req = Request.blank(
            '/v1/AUTH_test/c/man?multipart-manifest=put&heartbeat=on',
            environ={'REQUEST_METHOD': 'PUT'}, headers={'Accept': 'test'},
            body=test_json_data)

        status, headers, body = self.call_slo(req)
        self.assertEqual('202 Accepted', status)
        headers_found = [h.lower() for h, v in headers]
        self.assertNotIn('etag', headers_found)
        body = body.split(b'\n')
        self.assertEqual([b'  \r', b'\r'], body[:2],
                         'Expected body to start with two spaces and two '
                         'blank lines; got %r' % b'\n'.join(body))
        self.assertIn(b'Response Status: 400 Bad Request', body[2:5])
        self.assertIn(b'Response Body: Bad Request', body)
        self.assertIn(b'The server could not comply with the request since it '
                      b'is either malformed or otherwise incorrect.', body)
        self.assertFalse(any(line.startswith(b'Etag: ') for line in body))
        self.assertFalse(any(line.startswith(b'Last Modified: ')
                             for line in body))
        self.assertEqual(body[-4], b'Errors:')
        self.assertEqual(sorted(body[-3:-1]), [
            b'/cont/missing-object, 404 Not Found',
            b'/cont/object, Size Mismatch',
        ])
        self.assertEqual(body[-1], b'')

    @patch('swift.common.middleware.slo.time')
    def test_handle_multipart_long_running_put_failure_json(self, mock_time):
        mock_time.time.side_effect = [
            0,  # start time
            11,  # first segment's slow
            22,  # second segment's also slow
        ]
        test_json_data = json.dumps([{'path': u'/cont/object\u2661',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 99},
                                     {'path': '/cont/object',
                                      'etag': 'some other etag',
                                      'size_bytes': 100}]).encode('ascii')
        req = Request.blank(
            '/v1/AUTH_test/c/man?multipart-manifest=put&heartbeat=on',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'Accept': 'application/json'},
            body=test_json_data)

        status, headers, body = self.call_slo(req)
        self.assertEqual('202 Accepted', status)
        headers_found = [h.lower() for h, v in headers]
        self.assertNotIn('etag', headers_found)
        self.assertTrue(body.startswith(b'   \r\n\r\n'),
                        'Expected body to start with three spaces and two '
                        'blank lines; got %r' % body)
        body = json.loads(body)
        self.assertEqual(body['Response Status'], '400 Bad Request')
        self.assertEqual(body['Response Body'], 'Bad Request\nThe server '
                         'could not comply with the request since it is '
                         'either malformed or otherwise incorrect.')
        self.assertNotIn('Etag', body)
        self.assertNotIn('Last Modified', body)
        self.assertEqual(sorted(body['Errors']), [
            ['/cont/object', 'Etag Mismatch'],
            [quote(u'/cont/object\u2661'.encode('utf8')).decode('ascii'),
             'Size Mismatch'],
        ])

    @patch('swift.common.middleware.slo.time')
    def test_handle_multipart_long_running_put_bad_etag_json(self, mock_time):
        mock_time.time.side_effect = [
            0,  # start time
            11,  # first segment's slow
            22,  # second segment's also slow
        ]
        test_json_data = json.dumps([{'path': u'/cont/object\u2661',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 100},
                                     {'path': '/cont/object',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 100}]).encode('ascii')
        req = Request.blank(
            '/v1/AUTH_test/c/man?multipart-manifest=put&heartbeat=on',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'Accept': 'application/json', 'ETag': 'bad etag'},
            body=test_json_data)

        status, headers, body = self.call_slo(req)
        self.assertEqual('202 Accepted', status)
        headers_found = [h.lower() for h, v in headers]
        self.assertNotIn('etag', headers_found)
        self.assertTrue(body.startswith(b'   \r\n\r\n'),
                        'Expected body to start with three spaces and two '
                        'blank lines; got %r' % body)
        body = json.loads(body)
        self.assertEqual(body['Response Status'], '422 Unprocessable Entity')
        self.assertEqual('Unprocessable Entity\nUnable to process the '
                         'contained instructions', body['Response Body'])
        self.assertNotIn('Etag', body)
        self.assertNotIn('Last Modified', body)
        self.assertEqual(body['Errors'], [])

    def test_manifest_put_no_etag_success(self):
        req = Request.blank(
            '/v1/AUTH_test/c/man?multipart-manifest=put',
            method='PUT', body=test_json_data)
        resp = req.get_response(self.slo)
        self.assertEqual(resp.status_int, 201)

    def test_manifest_put_with_etag_success(self):
        req = Request.blank(
            '/v1/AUTH_test/c/man?multipart-manifest=put',
            method='PUT', body=test_json_data)
        req.headers['Etag'] = md5hex('etagoftheobjectsegment')
        resp = req.get_response(self.slo)
        self.assertEqual(resp.status_int, 201)

    def test_manifest_put_with_etag_with_quotes_success(self):
        req = Request.blank(
            '/v1/AUTH_test/c/man?multipart-manifest=put',
            method='PUT', body=test_json_data)
        req.headers['Etag'] = '"%s"' % md5hex('etagoftheobjectsegment')
        resp = req.get_response(self.slo)
        self.assertEqual(resp.status_int, 201)

    def test_manifest_put_bad_etag_fail(self):
        req = Request.blank(
            '/v1/AUTH_test/c/man?multipart-manifest=put',
            method='PUT', body=test_json_data)
        req.headers['Etag'] = md5hex('NOTetagoftheobjectsegment')
        resp = req.get_response(self.slo)
        self.assertEqual(resp.status_int, 422)

    def test_handle_multipart_put_disallow_empty_first_segment(self):
        test_json_data = json.dumps([{'path': '/cont/small_object',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 0},
                                     {'path': '/cont/object',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 100}]).encode('ascii')
        req = Request.blank('/v1/a/c/o?multipart-manifest=put',
                            method='PUT', body=test_json_data)
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '400 Bad Request')

    def test_handle_multipart_put_allow_empty_last_segment(self):
        test_json_data = json.dumps([{'path': '/cont/object',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 100},
                                     {'path': '/cont/empty_object',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 0}]).encode('ascii')
        req = Request.blank('/v1/AUTH_test/c/man?multipart-manifest=put',
                            method='PUT', body=test_json_data)
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '201 Created')

    def test_handle_multipart_put_invalid_data(self):
        def do_test(bad_data):
            test_json_data = json.dumps([{'path': '/cont/object',
                                          'etag': 'etagoftheobjectsegment',
                                          'size_bytes': 100},
                                         {'data': bad_data}]).encode('ascii')
            req = Request.blank('/v1/a/c/o', body=test_json_data)
            with self.assertRaises(HTTPException) as catcher:
                self.slo.handle_multipart_put(req, fake_start_response)
            self.assertEqual(catcher.exception.status_int, 400)

        do_test('invalid')  # insufficient padding
        do_test(12345)
        do_test(0)
        do_test(True)
        do_test(False)
        do_test(None)
        do_test({})
        do_test([])
        # Empties are no good, either
        do_test('')
        do_test('====')

    def test_handle_multipart_put_success_unicode(self):
        test_json_data = json.dumps([{'path': u'/cont/object\u2661',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 100}]).encode('ascii')
        req = Request.blank(
            '/v1/AUTH_test/c/man?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, headers={'Accept': 'test'},
            body=test_json_data)
        self.assertNotIn('X-Static-Large-Object', req.headers)
        self.call_slo(req)
        self.assertIn('X-Static-Large-Object', req.headers)
        self.assertEqual(req.environ['PATH_INFO'], '/v1/AUTH_test/c/man')
        self.assertIn(('HEAD', '/v1/AUTH_test/cont/object\xe2\x99\xa1'),
                      self.app.calls)

    def test_handle_multipart_put_no_xml(self):
        req = Request.blank(
            '/test_good/AUTH_test/c/man?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, headers={'Accept': 'test'},
            body=test_xml_data)
        no_xml = list(self.slo(req.environ, fake_start_response))
        self.assertEqual(no_xml, [b'Manifest must be valid JSON.\n'])

    def test_handle_multipart_put_bad_data(self):
        bad_data = json.dumps([{'path': '/cont/object',
                                'etag': 'etagoftheobj',
                                'size_bytes': 'lala'}])
        req = Request.blank(
            '/test_good/AUTH_test/c/man?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, body=bad_data)
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '400 Bad Request')
        self.assertIn(b'invalid size_bytes', body)

        for bad_data in [
                json.dumps([{'path': '/cont', 'etag': 'etagoftheobj',
                             'size_bytes': 100}]),
                json.dumps('asdf'), json.dumps(None), json.dumps(5),
                'not json', '1234', '', json.dumps({'path': None}),
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
            status, headers, body = self.call_slo(req)
            self.assertEqual(status, '400 Bad Request')

        req = Request.blank(
            '/v1/AUTH_test/c/man?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, body=None)
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '411 Length Required')

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
            '/v1/AUTH_test/checktest/man_3?multipart-manifest=get',
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

        self.assertEqual([
            [u'/checktest/a_1', u'Size Mismatch'],
            [u'/checktest/b_2', u'Etag Mismatch'],
            [u'/checktest/badreq', u'400 Bad Request'],
            [u'/checktest/slob', u'Etag Mismatch'],
            [u'/checktest/slob', u'Size Mismatch'],
        ], sorted(errors))

    def test_handle_multipart_put_skip_size_check(self):
        good_data = json.dumps([
            # Explicit None will skip it
            {'path': '/checktest/a_1', 'etag': 'a', 'size_bytes': None},
            # ...as will omitting it entirely
            {'path': '/checktest/b_2', 'etag': 'b'}])
        req = Request.blank(
            '/v1/AUTH_test/checktest/man_3?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, body=good_data)
        status, headers, body = self.call_slo(req)
        self.assertEqual(self.app.call_count, 3)

        # Check that we still populated the manifest properly from our HEADs
        req = Request.blank(
            '/v1/AUTH_test/checktest/man_3?multipart-manifest=get',
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
                                      'size_bytes': 100}]).encode('ascii')
        req = Request.blank('/v1/AUTH_test/c/o?multipart-manifest=put',
                            method='PUT', body=test_json_data)
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '400 Bad Request')
        self.assertIn(b'Too small; each segment must be at least 1 byte', body)

    def test_handle_multipart_put_skip_size_check_no_early_bailout(self):
        # The first is too small (it's 0 bytes), and
        # the second has a bad etag. Make sure both errors show up in
        # the response.
        test_json_data = json.dumps([{'path': '/cont/empty_object',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': None},
                                     {'path': '/cont/object2',
                                      'etag': 'wrong wrong wrong',
                                      'size_bytes': 100}]).encode('ascii')
        req = Request.blank('/v1/AUTH_test/c/o?multipart-manifest=put',
                            method='PUT', body=test_json_data)
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '400 Bad Request')
        self.assertIn(b'at least 1 byte', body)
        self.assertIn(b'Etag Mismatch', body)

    def test_handle_multipart_put_skip_etag_check(self):
        good_data = json.dumps([
            # Explicit None will skip it
            {'path': '/checktest/a_1', 'etag': None, 'size_bytes': 1},
            # ...as will omitting it entirely
            {'path': '/checktest/b_2', 'size_bytes': 2}])
        req = Request.blank(
            '/v1/AUTH_test/checktest/man_3?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, body=good_data)
        status, headers, body = self.call_slo(req)
        self.assertEqual(self.app.call_count, 3)

        # Check that we still populated the manifest properly from our HEADs
        req = Request.blank(
            '/v1/AUTH_test/checktest/man_3?multipart-manifest=get',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_app(req)
        manifest_data = json.loads(body)
        self.assertEqual('a', manifest_data[0]['hash'])
        self.assertEqual('b', manifest_data[1]['hash'])

    def test_handle_multipart_put_with_manipulator_callback(self):
        def data_inserter(manifest):
            for i in range(len(manifest), -1, -1):
                manifest.insert(i, {'data': 'WA=='})

        good_data = json.dumps([
            {'path': '/checktest/a_1'},
            {'path': '/checktest/b_2'}])
        req = Request.blank(
            '/v1/AUTH_test/checktest/man_3?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT',
                     'swift.callback.slo_manifest_hook': data_inserter},
            body=good_data)
        status, headers, body = self.call_slo(req)
        self.assertEqual(self.app.call_count, 3)

        # Check that we still populated the manifest properly from our HEADs
        req = Request.blank(
            '/v1/AUTH_test/checktest/man_3?multipart-manifest=get',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_app(req)
        manifest_data = json.loads(body)
        self.assertEqual([
            {k: v for k, v in item.items()
             if k in ('name', 'bytes', 'hash', 'data')}
            for item in manifest_data
        ], [
            {'data': 'WA=='},
            {'name': '/checktest/a_1', 'bytes': 1, 'hash': 'a'},
            {'data': 'WA=='},
            {'name': '/checktest/b_2', 'bytes': 2, 'hash': 'b'},
            {'data': 'WA=='},
        ])

    def test_handle_multipart_put_with_validator_callback(self):
        def complainer(manifest):
            return [(item['name'], "Don't wanna") for item in manifest]

        good_data = json.dumps([
            {'path': '/checktest/a_1'},
            {'path': '/checktest/b_2'}])
        req = Request.blank(
            '/v1/AUTH_test/checktest/man_3?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT',
                     'swift.callback.slo_manifest_hook': complainer},
            body=good_data)
        status, headers, body = self.call_slo(req)
        self.assertEqual(self.app.call_count, 2)
        self.assertEqual(status, '400 Bad Request')
        body = body.split(b'\n')
        self.assertIn(b"/checktest/a_1, Don't wanna", body)
        self.assertIn(b"/checktest/b_2, Don't wanna", body)

    def test_handle_unsatisfiable_ranges(self):
        bad_data = json.dumps(
            [{'path': '/checktest/a_1', 'etag': None,
              'size_bytes': None, 'range': '1-'}])
        req = Request.blank(
            '/v1/AUTH_test/checktest/man_3?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, body=bad_data)
        status, headers, body = self.call_slo(req)
        self.assertEqual('400 Bad Request', status)
        self.assertIn(b"Unsatisfiable Range", body)

    def test_handle_multipart_put_success_conditional(self):
        test_json_data = json.dumps([{'path': u'/cont/object',
                                      'etag': 'etagoftheobjectsegment',
                                      'size_bytes': 100}]).encode('ascii')
        req = Request.blank(
            '/v1/AUTH_test/c/man?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, headers={'If-None-Match': '*'},
            body=test_json_data)
        status, headers, body = self.call_slo(req)
        self.assertEqual(('201 Created', b''), (status, body))
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
             {'path': '/checktest/a_1', 'etag': None,
              'size_bytes': None},
             {'path': '/cont/object', 'etag': None,
              'size_bytes': None, 'range': '10-40'}])
        override_header = 'X-Object-Sysmeta-Container-Update-Override-Etag'
        req = Request.blank(
            '/v1/AUTH_test/checktest/man_3?multipart-manifest=put',
            environ={'REQUEST_METHOD': 'PUT'}, body=good_data,
            headers={override_header: 'my custom etag'})
        status, headers, body = self.call_slo(req)
        self.assertEqual(('201 Created', b''), (status, body))
        expected_etag = '"%s"' % md5hex(
            'ab:1-1;b:0-0;aetagoftheobjectsegment:10-40;')
        self.assertEqual(expected_etag, dict(headers)['Etag'])
        self.assertEqual([
            ('HEAD', '/v1/AUTH_test/checktest/a_1'),  # Only once!
            ('HEAD', '/v1/AUTH_test/checktest/b_2'),  # Only once!
            ('HEAD', '/v1/AUTH_test/cont/object'),
        ], sorted(self.app.calls[:-1]))
        self.assertEqual(
            ('PUT', '/v1/AUTH_test/checktest/man_3?multipart-manifest=put'),
            self.app.calls[-1])
        self.assertEqual(
            'my custom etag; slo_etag=%s' % expected_etag.strip('"'),
            self.app.headers[-1].get(override_header))

        # Check that we still populated the manifest properly from our HEADs
        req = Request.blank(
            '/v1/AUTH_test/checktest/man_3?multipart-manifest=get',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_app(req)
        manifest_data = json.loads(body)
        self.assertEqual(len(manifest_data), 5)

        self.assertEqual('a', manifest_data[0]['hash'])
        self.assertNotIn('range', manifest_data[0])

        self.assertEqual('b', manifest_data[1]['hash'])
        self.assertEqual('1-1', manifest_data[1]['range'])

        self.assertEqual('b', manifest_data[2]['hash'])
        self.assertEqual('0-0', manifest_data[2]['range'])

        self.assertEqual('a', manifest_data[3]['hash'])
        self.assertNotIn('range', manifest_data[3])

        self.assertEqual('etagoftheobjectsegment', manifest_data[4]['hash'])
        self.assertEqual('10-40', manifest_data[4]['range'])


class TestSloDeleteManifest(SloTestCase):

    def setUp(self):
        super(TestSloDeleteManifest, self).setUp()

        _submanifest_data = json.dumps(
            [{'name': '/deltest/b_2', 'hash': 'a', 'bytes': '1'},
             {'name': '/deltest/c_3', 'hash': 'b', 'bytes': '2'}])
        _submanifest_data = _submanifest_data.encode('ascii')

        self.app.register(
            'GET', '/v1/AUTH_test/deltest/man_404',
            swob.HTTPNotFound, {}, None)
        self.app.register(
            'GET', '/v1/AUTH_test/deltest/man',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            json.dumps([{'name': '/deltest/gone', 'hash': 'a', 'bytes': '1'},
                        {'name': '/deltest/b_2', 'hash': 'b', 'bytes': '2'}]).
            encode('ascii'))
        self.app.register(
            'DELETE', '/v1/AUTH_test/deltest/man',
            swob.HTTPNoContent, {}, None)
        self.app.register(
            'GET', '/v1/AUTH_test/deltest/man-all-there',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            json.dumps([{'name': '/deltest/b_2', 'hash': 'a', 'bytes': '1'},
                        {'name': '/deltest/c_3', 'hash': 'b', 'bytes': '2'}]).
            encode('ascii'))
        self.app.register(
            'GET', '/v1/AUTH_test-un\xc3\xafcode',
            swob.HTTPOk, {}, None)
        self.app.register(
            'GET', '/v1/AUTH_test-un\xc3\xafcode/deltest', swob.HTTPOk, {
                'X-Container-Read': 'diff read',
                'X-Container-Write': 'diff write',
            }, None)
        self.app.register(
            'GET', '/v1/AUTH_test-un\xc3\xafcode/\xe2\x98\x83', swob.HTTPOk, {
                'X-Container-Read': 'same read',
                'X-Container-Write': 'same write',
            }, None)
        self.app.register(
            'GET', '/v1/AUTH_test-un\xc3\xafcode/deltest/man-all-there',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            json.dumps([
                {'name': u'/\N{SNOWMAN}/b_2', 'hash': 'a', 'bytes': '1'},
                {'name': u'/\N{SNOWMAN}/c_3', 'hash': 'b', 'bytes': '2'},
            ]).encode('ascii'))
        self.app.register(
            'GET', '/v1/AUTH_test-un\xc3\xafcode/\xe2\x98\x83/same-container',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            json.dumps([
                {'name': u'/\N{SNOWMAN}/b_2', 'hash': 'a', 'bytes': '1'},
                {'name': u'/\N{SNOWMAN}/c_3', 'hash': 'b', 'bytes': '2'},
            ]).encode('ascii'))
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
            'DELETE', '/v1/AUTH_test-un\xc3\xafcode/deltest/man-all-there',
            swob.HTTPNoContent, {}, None)
        self.app.register(
            'DELETE',
            '/v1/AUTH_test-un\xc3\xafcode/\xe2\x98\x83/same-container',
            swob.HTTPNoContent, {}, None)
        self.app.register(
            'DELETE', '/v1/AUTH_test-un\xc3\xafcode/\xe2\x98\x83/b_2',
            swob.HTTPNoContent, {}, None)
        self.app.register(
            'DELETE', '/v1/AUTH_test-un\xc3\xafcode/\xe2\x98\x83/c_3',
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
                         'hash': 'd', 'bytes': '3'}]).encode('ascii'))
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
                        {'name': '/deltest/d_3', 'hash': 'd', 'bytes': '3'}]).
            encode('ascii'))
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
            b"[not {json (at ++++all")

        self.app.register(
            'GET', '/v1/AUTH_test/deltest/manifest-with-unauth-segment',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            json.dumps([{'name': '/deltest/a_1', 'hash': 'a', 'bytes': '1'},
                        {'name': '/deltest-unauth/q_17',
                         'hash': '11', 'bytes': '17'}]).encode('ascii'))
        self.app.register(
            'DELETE', '/v1/AUTH_test/deltest/manifest-with-unauth-segment',
            swob.HTTPNoContent, {}, None)

        self.app.register(
            'DELETE', '/v1/AUTH_test/deltest-unauth/q_17',
            swob.HTTPUnauthorized, {}, None)

        self.app.register(
            'GET', '/v1/AUTH_test/deltest/manifest-with-too-many-segs',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            json.dumps([{'name': '/deltest/a_1',
                         'hash': 'a', 'bytes': '1'},
                        {'name': '/deltest/multi-submanifest', 'sub_slo': True,
                         'hash': 'submanifest-etag',
                         'bytes': len(_submanifest_data)},
                        {'name': '/deltest/b_2',
                         'hash': 'b', 'bytes': '1'},
                        {'name': '/deltest/c_3',
                         'hash': 'c', 'bytes': '1'},
                        {'name': '/deltest/d_4',
                         'hash': 'b', 'bytes': '1'},
                        {'name': '/deltest/e_5',
                         'hash': 'c', 'bytes': '1'},
                        {'name': '/deltest/f_6',
                         'hash': 'b', 'bytes': '1'},
                        {'name': '/deltest/g_8',
                         'hash': 'c', 'bytes': '1'},
                        {'name': '/deltest/g_8',
                         'hash': 'c', 'bytes': '1'},
                        {'name': '/deltest/h_9',
                         'hash': 'd', 'bytes': '3'}]))

    def test_handle_multipart_delete_man(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/man',
            environ={'REQUEST_METHOD': 'DELETE'})
        self.slo(req.environ, fake_start_response)
        self.assertEqual(self.app.call_count, 1)

    def test_handle_multipart_delete_bad_utf8(self):
        req = Request.blank(
            b'/v1/AUTH_test/deltest/man\xff\xfe?multipart-manifest=delete',
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
                 ('DELETE', '/v1/AUTH_test/deltest/gone'),
                 ('DELETE', '/v1/AUTH_test/deltest/b_2'),
                 ('DELETE', '/v1/AUTH_test/deltest/man')]))
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
            ('DELETE', '/v1/AUTH_test/deltest/b_2'),
            ('DELETE', '/v1/AUTH_test/deltest/c_3'),
            ('DELETE', ('/v1/AUTH_test/deltest/man-all-there'))]))

    def test_handle_multipart_delete_whole_old_swift(self):
        # behave like pre-2.24.0 swift; initial GET will return just one byte
        self.app.can_ignore_range = False

        req = Request.blank(
            '/v1/AUTH_test/deltest/man-all-there?multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE'})
        self.call_slo(req)
        self.assertEqual(self.app.calls_with_headers[:2], [
            ('GET',
             '/v1/AUTH_test/deltest/man-all-there?multipart-manifest=get',
             {'Host': 'localhost:80',
              'User-Agent': 'Mozzarella Foxfire MultipartDELETE',
              'Range': 'bytes=-1',
              'X-Backend-Ignore-Range-If-Metadata-Present':
              'X-Static-Large-Object',
              'Content-Length': '0'}),
            ('GET',
             '/v1/AUTH_test/deltest/man-all-there?multipart-manifest=get',
             {'Host': 'localhost:80',
              'User-Agent': 'Mozzarella Foxfire MultipartDELETE',
              'Content-Length': '0'}),
        ])
        self.assertEqual(set(self.app.calls), set([
            ('GET',
             '/v1/AUTH_test/deltest/man-all-there?multipart-manifest=get'),
            ('DELETE', '/v1/AUTH_test/deltest/b_2'),
            ('DELETE', '/v1/AUTH_test/deltest/c_3'),
            ('DELETE', ('/v1/AUTH_test/deltest/man-all-there'))]))

    def test_handle_multipart_delete_non_ascii(self):
        unicode_acct = u'AUTH_test-un\u00efcode'
        wsgi_acct = bytes_to_wsgi(unicode_acct.encode('utf-8'))
        req = Request.blank(
            '/v1/%s/deltest/man-all-there?'
            'multipart-manifest=delete' % wsgi_acct,
            environ={'REQUEST_METHOD': 'DELETE'})
        status, _, body = self.call_slo(req)
        self.assertEqual('200 OK', status)
        lines = body.split(b'\n')
        for l in lines:
            parts = l.split(b':')
            if len(parts) == 1:
                continue
            key, value = parts
            if key == 'Response Status':
                delete_status = int(value.split()[0])
                self.assertEqual(200, delete_status)

        self.assertEqual(set(self.app.calls), set([
            ('GET',
             '/v1/%s/deltest/man-all-there'
             '?multipart-manifest=get' % wsgi_acct),
            ('DELETE', '/v1/%s/\xe2\x98\x83/b_2' % wsgi_acct),
            ('DELETE', '/v1/%s/\xe2\x98\x83/c_3' % wsgi_acct),
            ('DELETE', ('/v1/%s/deltest/man-all-there' % wsgi_acct))]))

    def test_handle_multipart_delete_nested(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/manifest-with-submanifest?' +
            'multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE'})
        self.call_slo(req)
        self.assertEqual(
            set(self.app.calls),
            {('GET', '/v1/AUTH_test/deltest/' +
              'manifest-with-submanifest?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/deltest/' +
              'submanifest?multipart-manifest=get'),
             ('DELETE', '/v1/AUTH_test/deltest/a_1'),
             ('DELETE', '/v1/AUTH_test/deltest/b_2'),
             ('DELETE', '/v1/AUTH_test/deltest/c_3'),
             ('DELETE', '/v1/AUTH_test/deltest/submanifest'),
             ('DELETE', '/v1/AUTH_test/deltest/d_3'),
             ('DELETE', '/v1/AUTH_test/deltest/manifest-with-submanifest')})

    def test_handle_multipart_delete_nested_too_many_segments(self):
        req = Request.blank(
            '/v1/AUTH_test/deltest/manifest-with-too-many-segs?' +
            'multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        with patch.object(self.slo, 'max_manifest_segments', 1):
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
        self.assertEqual(set(self.app.calls), {
            ('GET', '/v1/AUTH_test/deltest/' +
             'manifest-missing-submanifest?multipart-manifest=get'),
            ('DELETE', '/v1/AUTH_test/deltest/a_1'),
            ('GET', '/v1/AUTH_test/deltest/' +
             'missing-submanifest?multipart-manifest=get'),
            ('DELETE', '/v1/AUTH_test/deltest/d_3'),
            ('DELETE', '/v1/AUTH_test/deltest/manifest-missing-submanifest'),
        })
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
        self.assertFalse(self.app.unread_requests)

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
                 ('DELETE', '/v1/AUTH_test/deltest/a_1'),
                 ('DELETE', '/v1/AUTH_test/deltest-unauth/q_17'),
                 ('DELETE', '/v1/AUTH_test/deltest/' +
                  'manifest-with-unauth-segment')]))
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
            ('DELETE', '/v1/AUTH_test/deltest/b_2'),
            ('DELETE', '/v1/AUTH_test/deltest/c_3'),
            ('DELETE', '/v1/AUTH_test/deltest/man-all-there')]))

    def test_handle_async_delete_whole_404(self):
        self.slo.allow_async_delete = True
        req = Request.blank(
            '/v1/AUTH_test/deltest/man_404?async=t&multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE',
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_slo(req)
        self.assertEqual('404 Not Found', status)
        self.assertEqual(
            self.app.calls,
            [('GET',
              '/v1/AUTH_test/deltest/man_404?multipart-manifest=get')])

    def test_handle_async_delete_turned_off(self):
        self.slo.allow_async_delete = False
        req = Request.blank(
            '/v1/AUTH_test/deltest/man-all-there?'
            'multipart-manifest=delete&async=on&heartbeat=on',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'Accept': 'application/json'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        resp_data = json.loads(body)
        self.assertEqual(resp_data["Number Deleted"], 3)

        self.assertEqual(set(self.app.calls), set([
            ('GET',
             '/v1/AUTH_test/deltest/man-all-there?multipart-manifest=get'),
            ('DELETE', '/v1/AUTH_test/deltest/b_2'),
            ('DELETE', '/v1/AUTH_test/deltest/c_3'),
            ('DELETE', '/v1/AUTH_test/deltest/man-all-there')]))

    def test_handle_async_delete_whole(self):
        self.slo.allow_async_delete = True
        now = Timestamp(time.time())
        exp_obj_cont = self.slo.expirer_config.get_expirer_container(
            int(now), 'AUTH_test', 'deltest', 'man-all-there')
        self.app.register(
            'UPDATE', '/v1/.expiring_objects/%s' % exp_obj_cont,
            swob.HTTPNoContent, {}, None)
        req = Request.blank(
            '/v1/AUTH_test/deltest/man-all-there'
            '?async=true&multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE'})
        with patch('swift.common.utils.Timestamp.now', return_value=now):
            status, headers, body = self.call_slo(req)
        self.assertEqual('204 No Content', status)
        self.assertEqual(b'', body)
        self.assertEqual(self.app.calls, [
            ('GET',
             '/v1/AUTH_test/deltest/man-all-there?multipart-manifest=get'),
            ('UPDATE', '/v1/.expiring_objects/%s'
                       '?async=true&multipart-manifest=delete' % exp_obj_cont),
            ('DELETE', '/v1/AUTH_test/deltest/man-all-there'
                       '?async=true&multipart-manifest=delete'),
        ])

        for header, expected in (
            ('Content-Type', 'application/json'),
            ('X-Backend-Storage-Policy-Index', '0'),
            ('X-Backend-Allow-Private-Methods', 'True'),
        ):
            self.assertIn(header, self.app.call_list[1].headers)
            value = self.app.call_list[1].headers[header]
            msg = 'Expected %s header to be %r, not %r'
            self.assertEqual(value, expected, msg % (header, expected, value))

        self.assertEqual(json.loads(self.app.call_list[1].body), [
            {'content_type': 'application/async-deleted',
             'created_at': now.internal,
             'deleted': 0,
             'etag': 'd41d8cd98f00b204e9800998ecf8427e',
             'name': '%s-AUTH_test/deltest/b_2' % now.internal,
             'size': 0,
             'storage_policy_index': 0},
            {'content_type': 'application/async-deleted',
             'created_at': now.internal,
             'deleted': 0,
             'etag': 'd41d8cd98f00b204e9800998ecf8427e',
             'name': '%s-AUTH_test/deltest/c_3' % now.internal,
             'size': 0,
             'storage_policy_index': 0},
        ])

    def test_handle_async_delete_non_ascii(self):
        self.slo.allow_async_delete = True
        unicode_acct = u'AUTH_test-un\u00efcode'
        wsgi_acct = bytes_to_wsgi(unicode_acct.encode('utf-8'))
        now = Timestamp(time.time())
        exp_obj_cont = self.slo.expirer_config.get_expirer_container(
            int(now), unicode_acct, 'deltest', 'man-all-there')
        self.app.register(
            'UPDATE', '/v1/.expiring_objects/%s' % exp_obj_cont,
            swob.HTTPNoContent, {}, None)
        authorize_calls = []

        def authorize(req):
            authorize_calls.append((req.method, req.acl))

        req = Request.blank(
            '/v1/%s/deltest/man-all-there?'
            'async=1&multipart-manifest=delete&heartbeat=1' % wsgi_acct,
            environ={'REQUEST_METHOD': 'DELETE', 'swift.authorize': authorize})
        with patch('swift.common.utils.Timestamp.now', return_value=now):
            status, _, body = self.call_slo(req)
        # Every async delete should only need to make 3 requests during the
        # client request/response cycle, so no need to support heart-beating
        self.assertEqual('204 No Content', status)
        self.assertEqual(b'', body)

        self.assertEqual(self.app.calls, [
            ('GET',
             '/v1/%s/deltest/man-all-there?'
             'multipart-manifest=get' % wsgi_acct),
            ('HEAD', '/v1/%s' % wsgi_acct),
            ('HEAD', '/v1/%s/deltest' % wsgi_acct),
            ('HEAD', '/v1/%s/\xe2\x98\x83' % wsgi_acct),
            ('UPDATE',
             '/v1/.expiring_objects/%s'
             '?async=1&heartbeat=1&multipart-manifest=delete' % exp_obj_cont),
            ('DELETE',
             '/v1/%s/deltest/man-all-there'
             '?async=1&heartbeat=1&multipart-manifest=delete' % wsgi_acct),
        ])
        self.assertEqual(authorize_calls, [
            ('GET', None),  # Original GET
            ('DELETE', 'diff write'),
            ('DELETE', 'same write'),
            ('DELETE', None),  # Final DELETE
        ])

        for header, expected in (
            ('Content-Type', 'application/json'),
            ('X-Backend-Storage-Policy-Index', '0'),
            ('X-Backend-Allow-Private-Methods', 'True'),
        ):
            self.assertIn(header, self.app.call_list[-2].headers)
            value = self.app.call_list[-2].headers[header]
            msg = 'Expected %s header to be %r, not %r'
            self.assertEqual(value, expected, msg % (header, expected, value))

        self.assertEqual(json.loads(self.app.call_list[-2].body), [
            {'content_type': 'application/async-deleted',
             'created_at': now.internal,
             'deleted': 0,
             'etag': 'd41d8cd98f00b204e9800998ecf8427e',
             'name': u'%s-%s/\N{SNOWMAN}/b_2' % (now.internal, unicode_acct),
             'size': 0,
             'storage_policy_index': 0},
            {'content_type': 'application/async-deleted',
             'created_at': now.internal,
             'deleted': 0,
             'etag': 'd41d8cd98f00b204e9800998ecf8427e',
             'name': u'%s-%s/\N{SNOWMAN}/c_3' % (now.internal, unicode_acct),
             'size': 0,
             'storage_policy_index': 0},
        ])

    def test_handle_async_delete_non_ascii_same_container(self):
        self.slo.allow_async_delete = True
        unicode_acct = u'AUTH_test-un\u00efcode'
        wsgi_acct = bytes_to_wsgi(unicode_acct.encode('utf-8'))
        now = Timestamp(time.time())
        exp_obj_cont = self.slo.expirer_config.get_expirer_container(
            int(now), unicode_acct, u'\N{SNOWMAN}', 'same-container')
        self.app.register(
            'UPDATE', '/v1/.expiring_objects/%s' % exp_obj_cont,
            swob.HTTPNoContent, {}, None)
        authorize_calls = []

        def authorize(req):
            authorize_calls.append((req.method, req.acl))

        req = Request.blank(
            '/v1/%s/\xe2\x98\x83/same-container?'
            'async=yes&multipart-manifest=delete' % wsgi_acct,
            environ={'REQUEST_METHOD': 'DELETE', 'swift.authorize': authorize})
        with patch('swift.common.utils.Timestamp.now', return_value=now):
            status, _, body = self.call_slo(req)
        self.assertEqual('204 No Content', status)
        self.assertEqual(b'', body)

        self.assertEqual(self.app.calls, [
            ('GET',
             '/v1/%s/\xe2\x98\x83/same-container?'
             'multipart-manifest=get' % wsgi_acct),
            ('HEAD', '/v1/%s' % wsgi_acct),
            ('HEAD', '/v1/%s/\xe2\x98\x83' % wsgi_acct),
            ('UPDATE',
             '/v1/.expiring_objects/%s'
             '?async=yes&multipart-manifest=delete' % exp_obj_cont),
            ('DELETE',
             '/v1/%s/\xe2\x98\x83/same-container'
             '?async=yes&multipart-manifest=delete' % wsgi_acct),
        ])
        self.assertEqual(authorize_calls, [
            ('GET', None),  # Original GET
            ('DELETE', 'same write'),  # Only need one auth check
            ('DELETE', None),  # Final DELETE
        ])

        for header, expected in (
            ('Content-Type', 'application/json'),
            ('X-Backend-Storage-Policy-Index', '0'),
            ('X-Backend-Allow-Private-Methods', 'True'),
        ):
            self.assertIn(header, self.app.call_list[-2].headers)
            value = self.app.call_list[-2].headers[header]
            msg = 'Expected %s header to be %r, not %r'
            self.assertEqual(value, expected, msg % (header, expected, value))

        self.assertEqual(json.loads(self.app.call_list[-2].body), [
            {'content_type': 'application/async-deleted',
             'created_at': now.internal,
             'deleted': 0,
             'etag': 'd41d8cd98f00b204e9800998ecf8427e',
             'name': u'%s-%s/\N{SNOWMAN}/b_2' % (now.internal, unicode_acct),
             'size': 0,
             'storage_policy_index': 0},
            {'content_type': 'application/async-deleted',
             'created_at': now.internal,
             'deleted': 0,
             'etag': 'd41d8cd98f00b204e9800998ecf8427e',
             'name': u'%s-%s/\N{SNOWMAN}/c_3' % (now.internal, unicode_acct),
             'size': 0,
             'storage_policy_index': 0},
        ])

    def test_handle_async_delete_alternative_expirer_config(self):
        # Test that SLO async delete operation will send UPDATE requests to the
        # alternative expirer container when using a non-default account name
        # and container divisor.
        slo_conf = {
            'expiring_objects_account_name': 'exp',
            'expiring_objects_container_divisor': '5400',
        }
        self.slo = slo.filter_factory(slo_conf)(self.app)
        now = Timestamp(time.time())
        exp_obj_cont = self.slo.expirer_config.get_expirer_container(
            int(now), 'AUTH_test', 'deltest', 'man-all-there')
        self.app.register(
            'UPDATE', '/v1/.exp/%s' % exp_obj_cont,
            swob.HTTPNoContent, {}, None)
        req = Request.blank(
            '/v1/AUTH_test/deltest/man-all-there',
            method='DELETE')
        with patch('swift.common.utils.Timestamp.now', return_value=now):
            self.slo.handle_async_delete(req)
        self.assertEqual([
            ('GET', '/v1/AUTH_test/deltest/man-all-there'
             '?multipart-manifest=get'),
            ('UPDATE', '/v1/.exp/%s' % exp_obj_cont),
        ], self.app.calls)

    def test_handle_async_delete_nested(self):
        self.slo.allow_async_delete = True
        req = Request.blank(
            '/v1/AUTH_test/deltest/manifest-with-submanifest' +
            '?async=on&multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE'})
        status, _, body = self.call_slo(req)
        self.assertEqual('400 Bad Request', status)
        self.assertEqual(b'No segments may be large objects.', body)
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/deltest/' +
             'manifest-with-submanifest?multipart-manifest=get')])

    def test_handle_async_delete_too_many_containers(self):
        self.slo.allow_async_delete = True
        self.app.register(
            'GET', '/v1/AUTH_test/deltest/man',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            json.dumps([{'name': '/cont1/a_1', 'hash': 'a', 'bytes': '1'},
                        {'name': '/cont2/b_2', 'hash': 'b', 'bytes': '2'}]).
            encode('ascii'))

        req = Request.blank(
            '/v1/AUTH_test/deltest/man?async=on&multipart-manifest=delete',
            environ={'REQUEST_METHOD': 'DELETE'})
        status, _, body = self.call_slo(req)
        self.assertEqual('400 Bad Request', status)
        expected = b'All segments must be in one container. Found segments in '
        self.assertEqual(expected, body[:len(expected)])
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/deltest/man?multipart-manifest=get')])


class SloGETorHEADTestCase(SloTestCase):
    """
    Any GET or HEAD test-case should exercise legacy manifests written before
    we added etag/size SLO Sysmeta.

    N.B. We used to GET the whole manifest to calculate etag/size, just to
    respond to HEAD requests.
    """

    modern_manifest_headers = True

    def maybe_add_modern_manifest_headers(self, headers, manifest):
        hasher = md5(usedforsecurity=False)
        calculated_size = 0
        for seg_dict in manifest:
            if 'data' in seg_dict:
                raw_data = base64.b64decode(seg_dict['data'])
                segment_etag = md5(raw_data, usedforsecurity=False).hexdigest()
                segment_length = len(raw_data)
            else:
                segment_etag = seg_dict['hash']
                if 'range' in seg_dict:
                    segment_etag += ':%s;' % seg_dict['range']
                    start, end = seg_dict['range'].split('-')
                    segment_length = int(end) - int(start) + 1
                else:
                    segment_length = int(seg_dict['bytes'])
            calculated_size += segment_length
            hasher.update(segment_etag.encode())
        calculated_etag = hasher.hexdigest()
        if self.modern_manifest_headers:
            headers.update({
                'X-Object-Sysmeta-Slo-Etag': calculated_etag,
                'X-Object-Sysmeta-Slo-Size': calculated_size,
            })
        return calculated_etag, calculated_size

    def setUp(self):
        super(SloGETorHEADTestCase, self).setUp()
        self.expected_unread_requests = {}

    def tearDown(self):
        # SloTestCase always has an app
        self.assertEqual(self.app.unclosed_requests, {})
        self.assertEqual(self.app.unread_requests,
                         self.expected_unread_requests)

    def call_slo(self, req):
        # all the tests that inhert from this class were part of a major test
        # refactor in an effort to normalize and strengthen assertions; in
        # general it would probably be reasonable for call_app to return a
        # HeaderKeyDict but at the time was considered unrelated to the
        # GETorHEAD TestCase refactor
        status, raw_headers, body = super(
            SloGETorHEADTestCase, self).call_slo(req)
        headers = HeaderKeyDict(raw_headers)
        self.assertEqual(
            len(raw_headers), len(headers),
            'Got duplicate names in response headers: %r' % raw_headers)
        return status, headers, body

    def _setup_alphabet_objects(self, letters, container='gettest'):
        """
        A common convention in these tests uses segments named a_5, b_10, etc.

        They're nothing special, just small regular objects with names that
        describe their content and size.
        """
        for i, letter in enumerate(string.ascii_lowercase):
            if letter not in letters:
                continue
            size = (i + 1) * 5
            body = letter * size
            path = '/v1/AUTH_test/%s/%s_%s' % (container, letter, size)
            self.app.register('GET', path, swob.HTTPOk, {
                'Content-Length': len(body),
                'Etag': md5hex(body),
            }, body)

    def _setup_manifest(self, name, manifest, extra_headers=None,
                        attr_key=None, container='c', obj_key=None):
        """
        This helper should be used to create manifests that have descriptive
        names you can reference in tests.  Think of manifests as having
        "personalities" - you should get to known them over the course of a few
        test methods; you can share them between TestCases.  When you want to
        test something on a SLO with a slightly different manifest resist the
        temptation to update an existing manifest; just create a new
        personality - and give it a name that recognizes its heritage and
        follows existing patterns - don't worry we won't run out of memory
        having too many similar but different manifests setup in our tests.
        """
        if not attr_key:
            # _setup_manifest attrs will *always* be prefixed with manifest,
            # but you can override the glob in the form manifest_*_<attr>
            attr_key = name.replace('-', '_').lower()
        if obj_key is None:
            # seems reasonable when reading tests to see request paths that
            # clearly specify the object is a manifest; so this is a strong
            # convention.  But if we ever have a bug we can only repro if the
            # manifest doesn't start a literal "manifest-" you can register the
            # object name however you want.
            obj_key = 'manifest-%s' % name
        manifest_json = json.dumps(manifest)
        setattr(self, 'manifest_%s_json_size' % attr_key, len(manifest_json))
        json_md5 = md5hex(manifest_json)
        setattr(self, 'manifest_%s_json_md5' % attr_key, json_md5)
        manifest_headers = {
            'Content-Length': str(len(manifest_json)),
            'X-Static-Large-Object': 'true',
            'Etag': json_md5,
            # In my testing it's not possible to create an SLO manifest that
            # has *no* content-type, both empty or missing Content-Type header
            # on ?multipart-manifest=put result in a default
            # "application/octet-stream" value being stored in the manifest
            # metadata; still I wouldn't assert on this value in these tests,
            # you may not be testing what you think you are - N.B. some tests
            # will override this value with the "extra_headers" param.
            'Content-Type': 'application/octet-stream',
        }
        if extra_headers is not None:
            manifest_headers.update(extra_headers)
        slo_etag, slo_size = self.maybe_add_modern_manifest_headers(
            manifest_headers, manifest)
        setattr(self, 'manifest_%s_slo_etag' % attr_key, slo_etag)
        setattr(self, 'manifest_%s_slo_size' % attr_key, slo_size)
        self.app.register(
            'GET', '/v1/AUTH_test/%s/%s' % (container, obj_key),
            swob.HTTPOk, manifest_headers, manifest_json.encode('ascii'))

    def _setup_manifest_single_segment(self):
        """
        This manifest's segments are all regular objects.
        """
        _single_segment_manifest = [
            {'name': '/gettest/b_50', 'hash': md5hex('b' * 50), 'bytes': '50',
             'content_type': 'text/plain'},
        ]
        self._setup_manifest(
            'single-segment', _single_segment_manifest,
            extra_headers={'X-Object-Meta-Nature': 'Regular'},
            container='gettest')

    def _setup_manifest_zero_byte(self):
        """
        This is a zero-byte manifest.
        """
        _single_segment_manifest = [
            {'name': '/gettest/zero', 'hash': md5hex(''), 'bytes': '0',
             'content_type': 'text/plain'},
        ]
        self._setup_manifest(
            'zero-byte', _single_segment_manifest,
            container='gettest')

    def _setup_manifest_data(self):
        _data_manifest = [
            {
                'data': base64.b64encode(b'123456').decode('ascii')
            }, {
                'name': '/gettest/a_5',
                'hash': md5hex('a' * 5),
                'content_type': 'text/plain',
                'bytes': '5',
            }, {
                'data': base64.b64encode(b'ABCDEF').decode('ascii')
            },
        ]
        self._setup_manifest('data', _data_manifest)

    def _setup_manifest_bc(self):
        """
        This manifest's segments are all regular objects.
        """
        _bc_manifest = [
            {'name': '/gettest/b_10', 'hash': md5hex('b' * 10), 'bytes': '10',
             'content_type': 'text/plain'},
            {'name': '/gettest/c_15', 'hash': md5hex('c' * 15), 'bytes': '15',
             'content_type': 'text/plain'}
        ]
        self._setup_manifest('bc', _bc_manifest, extra_headers={
            # maybe manifest-bc is about some botony research!?
            'X-Object-Meta-Plant': 'Ficus',
        }, container='gettest')

    def _setup_manifest_bc_expires(self):
        """
        This manifest's segments are all regular objects due to expire.
        """
        _bc_expires_manifest = [
            {'name': '/gettest/b_5', 'hash': md5hex('b' * 5), 'bytes': '5',
             'content_type': 'text/plain'},
            {'name': '/gettest/c_10', 'hash': md5hex('c' * 10), 'bytes': '10',
             'content_type': 'text/plain'}
        ]
        self._setup_manifest('bc-expires', _bc_expires_manifest,
                             extra_headers={'X-Object-Meta-Plant':
                                            'Ficus-Expires'},
                             container='gettest')

    def _setup_manifest_abcd(self):
        """
        This manifest uses manifest-bc as a sub-manifest!
        """
        _abcd_manifest = [
            {'name': '/gettest/a_5', 'hash': md5hex("a" * 5),
             'content_type': 'text/plain', 'bytes': '5'},
            {'name': '/gettest/manifest-bc', 'sub_slo': True,
             'content_type': 'application/json',
             # N.B. sub-slo-segments use slo-etag & slo-size
             'hash': self.manifest_bc_slo_etag,
             'bytes': self.manifest_bc_slo_size},
            {'name': '/gettest/d_20', 'hash': md5hex("d" * 20),
             'content_type': 'text/plain', 'bytes': '20'},
        ]
        self._setup_manifest('abcd', _abcd_manifest, extra_headers={
            # apparently this is a HUGE json object?  maybe the plan Ficus data
            # is embeded as base64.
            'Content-Type': 'application/json',
        }, container='gettest')

    def _setup_manifest_abcdefghijkl(self):
        """
        Despite the terrible name, this is just a large manifest of regular
        objects.
        """
        _abcdefghijkl_manifest = [
            {'name': '/gettest/a_5', 'hash': md5hex("a" * 5),
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
             'content_type': 'text/plain', 'bytes': '60'},
        ]
        self._setup_manifest('abcdefghijkl', _abcdefghijkl_manifest,
                             extra_headers={}, container='gettest')

    def _setup_manifest_bc_ranges(self):
        """
        This manifest's segments are range-segments into regular objects.
        """
        _bc_ranges_manifest = [
            {'name': '/gettest/b_10', 'hash': md5hex('b' * 10),
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
             'range': '11-14'},
        ]
        self._setup_manifest('bc-ranges', _bc_ranges_manifest,
                             container='gettest')

    def _setup_manifest_abcd_ranges(self):
        """
        This manifest's range-segments use manifest-bc as sub-manifest!
        """
        _abcd_ranges_manifest = [
            {'name': '/gettest/a_5', 'hash': md5hex("a" * 5),
             'content_type': 'text/plain', 'bytes': '5',
             'range': '0-3'},
            {'name': '/gettest/a_5', 'hash': md5hex("a" * 5),
             'content_type': 'text/plain', 'bytes': '5',
             'range': '1-4'},
            {'name': '/gettest/manifest-bc-ranges', 'sub_slo': True,
             'content_type': 'application/json',
             'hash': self.manifest_bc_ranges_slo_etag,
             'bytes': 16,
             'range': '8-15'},
            {'name': '/gettest/manifest-bc-ranges', 'sub_slo': True,
             'content_type': 'application/json',
             'hash': self.manifest_bc_ranges_slo_etag,
             'bytes': self.manifest_bc_ranges_slo_size,
             'range': '0-7'},
            {'name': '/gettest/d_20', 'hash': md5hex("d" * 20),
             'content_type': 'text/plain', 'bytes': '20',
             'range': '0-3'},
            {'name': '/gettest/d_20', 'hash': md5hex("d" * 20),
             'content_type': 'text/plain', 'bytes': '20',
             'range': '8-11'},
        ]
        self._setup_manifest(
            'abcd-ranges', _abcd_ranges_manifest, extra_headers={
                # apparently this is another HUGE json object?
                'Content-Type': 'application/json',
            }, container='gettest')

    def _setup_manifest_abcd_subranges(self):
        """
        These range-segments use manifest-abcd-ranges as sub-manifests!
        """
        _abcd_subranges_manifest = [
            {'name': '/gettest/manifest-abcd-ranges', 'sub_slo': True,
             'hash': self.manifest_abcd_ranges_slo_etag,
             'bytes': self.manifest_abcd_ranges_slo_size,
             'content_type': 'text/plain',
             'range': '6-10'},
            {'name': '/gettest/manifest-abcd-ranges', 'sub_slo': True,
             'hash': self.manifest_abcd_ranges_slo_etag,
             'bytes': self.manifest_abcd_ranges_slo_size,
             'content_type': 'text/plain',
             'range': '31-31'},
            {'name': '/gettest/manifest-abcd-ranges', 'sub_slo': True,
             'hash': self.manifest_abcd_ranges_slo_etag,
             'bytes': self.manifest_abcd_ranges_slo_size,
             'content_type': 'text/plain',
             'range': '14-18'},
            {'name': '/gettest/manifest-abcd-ranges', 'sub_slo': True,
             'hash': self.manifest_abcd_ranges_slo_etag,
             'bytes': self.manifest_abcd_ranges_slo_size,
             'content_type': 'text/plain',
             'range': '0-0'},
            {'name': '/gettest/manifest-abcd-ranges', 'sub_slo': True,
             'hash': self.manifest_abcd_ranges_slo_etag,
             'bytes': self.manifest_abcd_ranges_slo_size,
             'content_type': 'text/plain',
             'range': '22-26'},
        ]
        self._setup_manifest(
            'abcd-subranges', _abcd_subranges_manifest, extra_headers={
                # apparently this is another HUGE json object?
                'Content-Type': 'application/json',
            }, container='gettest')

    def _setup_manifest_headtest(self):
        """
        This is a unqiue manifest, un-related to the linage of gettest with
        different segments.  AFAIK the segments responses are never registered.
        It also has it's own weird name.
        """
        manifest = [
            {'name': '/gettest/seg01',
             'bytes': '100',
             'hash': 'seg01-hash',
             'content_type': 'text/plain',
             'last_modified': '2013-11-19T11:33:45.137446'},
            {'name': '/gettest/seg02',
             'bytes': '200',
             'hash': 'seg02-hash',
             'content_type': 'text/plain',
             'last_modified': '2013-11-19T11:33:45.137447'},
        ]
        self._setup_manifest('man', manifest, extra_headers={
            'Content-Type': 'test/data',
            'X-Object-Sysmeta-Artisanal-Etag': 'bespoke',
        }, attr_key='headtest', container='headtest', obj_key='man')

    def _setup_manifest_aabbccdd(self):
        """
        This manifest has repeated whole segments, SLO request pattern on a
        manifest like uses multi-range requests to coalesce repated segments
        into a single request!
        """
        _aabbccdd_manifest = [
            {'name': '/gettest/a_5', 'hash': md5hex("a" * 5),
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
             'content_type': 'text/plain', 'bytes': '20'}
        ]
        self._setup_manifest('aabbccdd', _aabbccdd_manifest,
                             container='gettest')


class TestSloHeadOldManifest(SloGETorHEADTestCase):

    modern_manifest_headers = False

    def setUp(self):
        super(TestSloHeadOldManifest, self).setUp()
        self._setup_alphabet_objects('abcd')
        self._setup_manifest_bc()
        self._setup_manifest_abcd()

        self._setup_manifest_headtest()
        # these aliases are not *too* ambiguous, they get a pass
        self.slo_etag = self.manifest_headtest_slo_etag
        self.manifest_json_etag = self.manifest_headtest_json_md5

    def test_etag_is_hash_of_segment_etags(self):
        req = Request.blank(
            '/v1/AUTH_test/headtest/man',
            environ={'REQUEST_METHOD': 'HEAD'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Etag'], '"%s"' % self.slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'], self.manifest_json_etag)
        self.assertEqual(headers['Content-Length'], '300')
        self.assertEqual(headers['Content-Type'], 'test/data')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(body, b'')  # it's a HEAD request, after all

        expected_app_calls = [('HEAD', '/v1/AUTH_test/headtest/man')]
        if not self.modern_manifest_headers:
            expected_app_calls.append(('GET', '/v1/AUTH_test/headtest/man'))
        self.assertEqual(self.app.calls, expected_app_calls)

    def test_get_manifest_passthrough(self):
        req = Request.blank(
            '/v1/AUTH_test/headtest/man?multipart-manifest=get',
            environ={'REQUEST_METHOD': 'HEAD'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Etag'], self.manifest_json_etag)
        self.assertEqual(headers['Content-Type'],
                         'application/json; charset=utf-8')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Object-Sysmeta-Artisanal-Etag'], 'bespoke')
        self.assertNotIn('X-Manifest-Etag', headers)
        self.assertEqual(int(headers['Content-Length']),
                         self.manifest_headtest_json_size)
        self.assertEqual(body, b'')  # it's a HEAD request, after all

        expected_app_calls = [(
            'HEAD', '/v1/AUTH_test/headtest/man?multipart-manifest=get')]
        self.assertEqual(self.app.calls, expected_app_calls)
        # this is only relevant for conditional requests; but SLO will only
        # *add* it on SLO requests, not a multipart-manifest=get request
        self.assertNotIn('X-Backend-Etag-Is-At', self.app.headers[0])

    def test_zero_byte_manifest(self):
        _single_segment_manifest = [
            {'name': '/c/zero', 'hash': md5hex(''), 'bytes': '0',
             'content_type': 'text/plain'},
        ]
        self._setup_manifest('zero-byte', _single_segment_manifest)
        req = Request.blank('/v1/AUTH_test/c/manifest-zero-byte',
                            method='HEAD')
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Etag'],
                         '"%s"' % self.manifest_zero_byte_slo_etag)
        self.assertEqual(headers['Content-Length'], '0')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_zero_byte_json_md5)
        self.assertEqual(body, b'')  # it's a HEAD request, after all

        expected_app_calls = [('HEAD', '/v1/AUTH_test/c/manifest-zero-byte')]
        if not self.modern_manifest_headers:
            expected_app_calls.append((
                'GET', '/v1/AUTH_test/c/manifest-zero-byte'))
        self.assertEqual(self.app.calls, expected_app_calls)

    def test_if_none_match_etag_matching(self):
        req = Request.blank(
            '/v1/AUTH_test/headtest/man',
            environ={'REQUEST_METHOD': 'HEAD'},
            headers={'If-None-Match': self.slo_etag})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '304 Not Modified')
        self.assertEqual(headers['Etag'], '"%s"' % self.slo_etag)
        self.assertEqual(headers['Content-Length'], '0')
        self.assertEqual(headers['Content-Type'], 'test/data')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Manifest-Etag'], self.manifest_json_etag)
        self.assertEqual(headers['X-Object-Sysmeta-Artisanal-Etag'], 'bespoke')

        expected_app_calls = [('HEAD', '/v1/AUTH_test/headtest/man')]
        if not self.modern_manifest_headers:
            expected_app_calls.append(('GET', '/v1/AUTH_test/headtest/man'))
        self.assertEqual(self.app.calls, expected_app_calls)

    def test_if_match_etag_not_matching(self):
        req = Request.blank(
            '/v1/AUTH_test/headtest/man',
            environ={'REQUEST_METHOD': 'HEAD'},
            headers={'If-Match': 'zzz'})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '412 Precondition Failed')
        self.assertEqual(headers['Etag'], '"%s"' % self.slo_etag)
        self.assertEqual(headers['Content-Length'], '0')
        self.assertEqual(headers['Content-Type'], 'test/data')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Manifest-Etag'], self.manifest_json_etag)
        self.assertEqual(headers['X-Object-Sysmeta-Artisanal-Etag'], 'bespoke')

        expected_app_calls = [('HEAD', '/v1/AUTH_test/headtest/man')]
        if not self.modern_manifest_headers:
            expected_app_calls.append(('GET', '/v1/AUTH_test/headtest/man'))
        self.assertEqual(self.app.calls, expected_app_calls)

    def test_if_none_match_etag_matching_with_override(self):
        req = Request.blank(
            '/v1/AUTH_test/headtest/man',
            environ={'REQUEST_METHOD': 'HEAD'},
            headers={
                'If-None-Match': 'bespoke',
                'X-Backend-Etag-Is-At': 'X-Object-Sysmeta-Artisanal-Etag'})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '304 Not Modified')
        # We *are not* responsible for replacing the etag; whoever set
        # x-backend-etag-is-at is responsible
        self.assertEqual(headers['Etag'], '"%s"' % self.slo_etag)
        self.assertEqual(headers['Content-Length'], '0')
        self.assertEqual(headers['Content-Type'], 'test/data')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Manifest-Etag'], self.manifest_json_etag)
        self.assertEqual(headers['X-Object-Sysmeta-Artisanal-Etag'], 'bespoke')

        expected_app_calls = [('HEAD', '/v1/AUTH_test/headtest/man')]
        if not self.modern_manifest_headers:
            expected_app_calls.append(('GET', '/v1/AUTH_test/headtest/man'))
        self.assertEqual(self.app.calls, expected_app_calls)

    def test_if_match_etag_not_matching_with_override(self):
        req = Request.blank(
            '/v1/AUTH_test/headtest/man',
            environ={'REQUEST_METHOD': 'HEAD'},
            headers={
                'If-Match': self.slo_etag,
                'X-Backend-Etag-Is-At': 'X-Object-Sysmeta-Artisanal-Etag'})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '412 Precondition Failed')
        # We *are not* responsible for replacing the etag; whoever set
        # x-backend-etag-is-at is responsible
        self.assertEqual(headers['Etag'], '"%s"' % self.slo_etag)
        self.assertEqual(headers['Content-Length'], '0')
        self.assertEqual(headers['Content-Type'], 'test/data')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Manifest-Etag'], self.manifest_json_etag)
        self.assertEqual(headers['X-Object-Sysmeta-Artisanal-Etag'], 'bespoke')

        expected_app_calls = [('HEAD', '/v1/AUTH_test/headtest/man')]
        if not self.modern_manifest_headers:
            expected_app_calls.append(('GET', '/v1/AUTH_test/headtest/man'))
        self.assertEqual(self.app.calls, expected_app_calls)

    def test_head_manifest_is_efficient(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'HEAD'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Content-Length'], '50')
        self.assertEqual(headers['Etag'], '"%s"' % self.manifest_abcd_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_abcd_json_md5)
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(body, b'')
        expected_calls = [
            ('HEAD', '/v1/AUTH_test/gettest/manifest-abcd'),
        ]
        if not self.modern_manifest_headers:
            # Note we don't call validate first segment on HEAD. We know the
            # slo size/etag from the manifest, so there's no need for any
            # segment or submanifest fetching here, but a naïve implementation
            # might do it anyway.
            expected_calls.append(
                ('GET', '/v1/AUTH_test/gettest/manifest-abcd'))
        self.assertEqual(self.app.calls, expected_calls)


class TestSloHeadManifest(TestSloHeadOldManifest):
    """
    Exercise manifests written after we added etag/size SLO Sysmeta
    """

    modern_manifest_headers = True


class TestSloGetRawManifest(SloGETorHEADTestCase):

    modern_manifest_headers = True

    def setUp(self):
        super(TestSloGetRawManifest, self).setUp()
        self._setup_manifest_raw()
        self._setup_manifest_raw_ranges()

    def _setup_manifest_raw(self):
        """
        This is only used by TestSloGetRawManifest; some segments are
        regular objects and one segment is a sub-slo.
        """
        _raw_manifest = [
            {'name': '/gettest/does_not_exist', 'hash': md5hex('foo'),
             'bytes': '100', 'content_type': 'text/plain',
             'last_modified': '1970-01-01T00:00:00.000000'},
            {'name': '/gettest/not_checked', 'hash': md5hex('bar'),
             'bytes': '303', 'content_type': 'text/plain',
             'last_modified': '1970-01-01T00:00:00.000000'},
            {'name': '/gettest/made_up',
             'hash': md5hex(md5hex("fizz") + md5hex("buzz")), 'bytes': '2099',
             'content_type': 'application/json',
             'sub_slo': True,
             'last_modified': '1970-01-01T00:00:00.000000'}
        ]
        self._setup_manifest('raw', _raw_manifest, extra_headers={
            'Content-Type': 'text/plain',
        }, container='gettest')

    def _setup_manifest_raw_ranges(self):
        """
        This is only used by TestSloGetRawManifest; the segments are
        range-segments into regular objects.
        """
        _raw_ranges_manifest = [
            {'name': '/gettest/does_not_exist', 'hash': md5hex('foo'),
             'bytes': '100', 'last_modified': '1970-01-01T00:00:00.000000',
             'content_type': 'text/plain', 'range': '1-99'},
            {'name': '/gettest/not_checked', 'hash': md5hex('bar'),
             'bytes': '303', 'last_modified': '1970-01-01T00:00:00.000000',
             'content_type': 'text/plain', 'range': '100-200'},
        ]
        self._setup_manifest(
            'raw-ranges', _raw_ranges_manifest, extra_headers={
                'Content-Type': 'text/plain',
            }, container='gettest')

    def test_get_raw_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-raw'
            '?multipart-manifest=get&format=raw',
            environ={'REQUEST_METHOD': 'GET',
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_slo(req)

        expected_body = json.dumps([
            {'etag': md5hex('foo'), 'size_bytes': '100',
             'path': '/gettest/does_not_exist'},
            {'etag': md5hex('bar'), 'size_bytes': '303',
             'path': '/gettest/not_checked'},
            {'etag': md5hex(md5hex("fizz") + md5hex("buzz")),
             'size_bytes': '2099',
             'path': '/gettest/made_up'}], sort_keys=True).encode('utf8')
        expected_etag = md5hex(expected_body)

        self.assertEqual(body, expected_body)
        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Etag'], expected_etag)
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        # raw format should return the actual manifest object content-type
        self.assertEqual(headers['Content-Type'], 'text/plain')
        self.assertNotIn('X-Manifest-Etag', headers)
        self.assertEqual(int(headers['Content-Length']), len(body))

        try:
            json.loads(body)
        except ValueError:
            self.fail("Invalid JSON in manifest GET: %r" % body)

        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/manifest-raw'
             # FakeSwift stable-alpha-sorts params keys
             '?format=raw&multipart-manifest=get'),
        ])

    def test_get_raw_manifest_passthrough_with_ranges(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-raw-ranges'
            '?multipart-manifest=get&format=raw',
            environ={'REQUEST_METHOD': 'GET',
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        # raw format should return the actual manifest object content-type
        self.assertEqual(headers['Content-Type'], 'text/plain')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertNotIn('X-Manifest-Etag', headers)
        self.assertEqual(int(headers['Content-Length']), len(body))
        try:
            resp_data = json.loads(body)
        except ValueError:
            self.fail("Invalid JSON in manifest GET: %r" % body)

        self.assertEqual(
            resp_data,
            [{'etag': md5hex('foo'), 'size_bytes': '100',
              'path': '/gettest/does_not_exist', 'range': '1-99'},
             {'etag': md5hex('bar'), 'size_bytes': '303',
              'path': '/gettest/not_checked', 'range': '100-200'}],
            body)

        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/manifest-raw-ranges'
             # FakeSwift stable-alpha-sorts params keys
             '?format=raw&multipart-manifest=get'),
        ])


class TestSloGetRawOldManifest(TestSloGetRawManifest):

    modern_manifest_headers = False


class TestSloGetManifests(SloGETorHEADTestCase):

    modern_manifest_headers = True

    def setUp(self):
        super(TestSloGetManifests, self).setUp()
        self._setup_alphabet_objects('abcdefghijkl')
        self._setup_manifest_bc()
        self._setup_manifest_abcd()
        self._setup_manifest_abcdefghijkl()

    def test_get_manifest_passthrough(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-bc?multipart-manifest=get',
            environ={'REQUEST_METHOD': 'GET',
                     'HTTP_ACCEPT': 'application/json'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Content-Type'],
                         'application/json; charset=utf-8')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['Etag'], self.manifest_bc_json_md5)
        self.assertEqual(int(headers['Content-Length']),
                         self.manifest_bc_json_size)
        self.assertNotIn('X-Manifest-Etag', headers)
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
        self.assertEqual(headers['Etag'], md5hex(body))

        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/manifest-bc'
             '?multipart-manifest=get'),
        ])

    def test_get_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-bc',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        manifest_etag = md5hex(md5hex("b" * 10) + md5hex("c" * 15))
        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Content-Length'], '25')
        self.assertEqual(headers['Etag'], '"%s"' % manifest_etag)
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Manifest-Etag'], self.manifest_bc_json_md5)
        self.assertEqual(headers['X-Object-Meta-Plant'], 'Ficus')
        self.assertEqual(body, b'bbbbbbbbbbccccccccccccccc')
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
            ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
        ])

        for _, _, hdrs in self.app.calls_with_headers[1:]:
            ua = hdrs.get("User-Agent", "")
            self.assertTrue("SLO MultipartGET" in ua)
            self.assertFalse("SLO MultipartGET SLO MultipartGET" in ua)
        # the first request goes through unaltered
        first_ua = self.app.calls_with_headers[0][2].get("User-Agent")
        self.assertFalse(
            "SLO MultipartGET" in first_ua)

    def test_get_manifest_repeated_segments_uses_multi_range_requests(self):
        self._setup_manifest_aabbccdd()

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-aabbccdd',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(int(headers['Content-Length']),
                         self.manifest_aabbccdd_slo_size)
        self.assertEqual(headers['Etag'],
                         '"%s"' % self.manifest_aabbccdd_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_aabbccdd_json_md5)
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(body, (
            b'aaaaaaaaaabbbbbbbbbbbbbbbbbbbbcccccccccccccccccccccccccccccc'
            b'dddddddddddddddddddddddddddddddddddddddd'))

        # FakeSwift doesn't have any explicit knowledge of multi-range
        # responses; but swob will convert the response to to MIME documents if
        # it's constructed with body=<bytes>, this test would break with
        # registered segment responses who's body was was a list/app_iter
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/manifest-aabbccdd'),
            ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get')])

        ranges = [c[2].get('Range') for c in self.app.calls_with_headers]
        # I won't say it's bad we do this, but it's not obviously only good
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
        self.assertEqual(sleeps, [1.0] * 11)

        # give the client the first 4 segments without ratelimiting; we'll
        # sleep less
        del sleeps[:]
        with patch('time.time', mock_time), \
                patch('eventlet.sleep', mock_sleep), \
                patch.object(self.slo, 'rate_limit_under_size', 999999999), \
                patch.object(self.slo, 'rate_limit_after_segment', 4):
            status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')  # sanity check
        self.assertEqual(sleeps, [1.0] * 7)

        # ratelimit segments under 35 bytes; this affects a-f
        del sleeps[:]
        with patch('time.time', mock_time), \
                patch('eventlet.sleep', mock_sleep), \
                patch.object(self.slo, 'rate_limit_under_size', 35), \
                patch.object(self.slo, 'rate_limit_after_segment', 0):
            status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')  # sanity check
        self.assertEqual(sleeps, [1.0] * 5)

        # ratelimit segments under 36 bytes; this now affects a-g, netting
        # us one more sleep than before
        del sleeps[:]
        with patch('time.time', mock_time), \
                patch('eventlet.sleep', mock_sleep), \
                patch.object(self.slo, 'rate_limit_under_size', 36), \
                patch.object(self.slo, 'rate_limit_after_segment', 0):
            status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')  # sanity check
        self.assertEqual(sleeps, [1.0] * 6)

    def test_get_manifest_with_submanifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Content-Length'], '50')
        self.assertEqual(headers['Etag'], '"%s"' % self.manifest_abcd_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_abcd_json_md5)
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(
            body, b'aaaaabbbbbbbbbbcccccccccccccccdddddddddddddddddddd')

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
             ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get')])

    def test_range_get_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=3-17'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Content-Length'], '15')
        self.assertEqual(headers['Content-Range'], 'bytes 3-17/50')
        self.assertEqual(headers['Etag'], '"%s"' % self.manifest_abcd_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_abcd_json_md5)
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(body, b'aabbbbbbbbbbccc')

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
             ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get')])

        ranges = [c[2].get('Range') for c in self.app.calls_with_headers]
        self.assertEqual(ranges, [
            'bytes=3-17',
            None,
            'bytes=3-',
            None,
            'bytes=0-2'])
        ignore_range_headers = [
            c[2].get('X-Backend-Ignore-Range-If-Metadata-Present')
            for c in self.app.calls_with_headers]
        self.assertEqual(ignore_range_headers, [
            'X-Static-Large-Object',
            None,
            None,
            None,
            None])
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

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Etag'],
                         '"%s"' % self.manifest_abcd_slo_etag)
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_abcd_json_md5)
        self.assertEqual(int(headers['Content-Length']), len(body))

        ct, params = parse_content_type(headers['Content-Type'])
        params = dict(params)
        self.assertEqual(ct, 'multipart/byteranges')
        boundary = params.get('boundary')
        self.assertTrue(boundary is not None)
        boundary = boundary.encode('utf-8')

        self.assertEqual(len(body), int(headers['Content-Length']))
        # this is a multi-range resp
        self.assertNotIn('Content-Range', headers)

        got_mime_docs = []
        for mime_doc_fh in iter_multipart_mime_documents(
                BytesIO(body), boundary):
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
        self.assertEqual(first_range_body, b'aabbbbbbbbbbccc')

        second_range_headers = got_mime_docs[1][0]
        second_range_body = got_mime_docs[1][1]
        self.assertEqual(second_range_headers['Content-Range'],
                         'bytes 20-24/50')
        self.assertEqual(second_range_headers['Content-Type'],
                         'application/json')
        self.assertEqual(second_range_body, b'ccccc')

        third_range_headers = got_mime_docs[2][0]
        third_range_body = got_mime_docs[2][1]
        self.assertEqual(third_range_headers['Content-Range'],
                         'bytes 35-49/50')
        self.assertEqual(third_range_headers['Content-Type'],
                         'application/json')
        self.assertEqual(third_range_body, b'ddddddddddddddd')

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
             ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get')])

        ranges = [c[2].get('Range') for c in self.app.calls_with_headers]
        self.assertEqual(ranges, [
            'bytes=3-17,20-24,35-999999',  # initial GET
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

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Etag'],
                         '"%s"' % self.manifest_abcd_slo_etag)
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_abcd_json_md5)
        self.assertEqual(int(headers['Content-Length']), len(body))

        ct, params = parse_content_type(headers['Content-Type'])
        params = dict(params)
        self.assertEqual(ct, 'multipart/byteranges')
        boundary = params.get('boundary')
        self.assertTrue(boundary is not None)
        boundary = boundary.encode('utf-8')

        got_mime_docs = []
        for mime_doc_fh in iter_multipart_mime_documents(
                BytesIO(body), boundary):
            headers = parse_mime_headers(mime_doc_fh)
            body = mime_doc_fh.read()
            got_mime_docs.append((headers, body))
        self.assertEqual(len(got_mime_docs), 2)

        first_range_headers = got_mime_docs[0][0]
        first_range_body = got_mime_docs[0][1]
        self.assertEqual(first_range_headers['Content-Range'],
                         'bytes 3-17/50')
        self.assertEqual(first_range_body, b'aabbbbbbbbbbccc')

        second_range_headers = got_mime_docs[1][0]
        second_range_body = got_mime_docs[1][1]
        self.assertEqual(second_range_headers['Content-Range'],
                         'bytes 29-49/50')
        self.assertEqual(second_range_body, b'cdddddddddddddddddddd')

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
             ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get')])
        ranges = [c[2].get('Range') for c in self.app.calls_with_headers]
        self.assertEqual(ranges, [
            'bytes=3-17,-21',              # initial GET
            None,                          # fetch manifest-bc as sub-slo
            'bytes=3-',                    # a_5
            None,                          # b_10
            'bytes=0-2,14-',               # c_15
            None])                         # d_20


class TestSloGetOldManifests(TestSloGetManifests):

    modern_manifest_headers = False

    def test_get_manifest_with_submanifest_bytes_in_content_type(self):
        _abcd_alt_manifest = [
            {'name': '/gettest/a_5', 'hash': md5hex("a" * 5),
             'content_type': 'text/plain', 'bytes': '5'},
            {'name': '/gettest/manifest-bc', 'sub_slo': True,
             'hash': self.manifest_bc_slo_etag,
             # if swift 1.9.1 thru 1.12.0 let this sub-slo manifest get created
             # with invalid hash/bytes the sub-request SLO GET will still work
             'content_type': 'application/json; swift_bytes=25',
             'bytes': self.manifest_bc_json_size},
            {'name': '/gettest/d_20', 'hash': md5hex("d" * 20),
             'content_type': 'text/plain', 'bytes': '20'}
        ]
        # N.B. if the _setup_manifest helper is called from a class w/
        # modern_manifest_headers = True the fake sysmeta is calculated from
        # the manifests provided bytes; real modern swift would have rejected
        # the PUT when the HEAD resp showed a size mis-match with sub-slo resp
        self._setup_manifest('abcd-alt', _abcd_alt_manifest,
                             container='gettest')
        # verify correct content-length when the sub-slo segment in the
        # manifest has its actual object content-length appended as swift_bytes
        # to the content-type, and the submanifest length in the bytes field.
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd-alt',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Content-Length'], '50')
        # N.B. we use the same slo-etag hash for the sub-slo segment to
        # calculate the alt-slo-etag
        self.assertEqual(self.manifest_abcd_slo_etag,
                         self.manifest_abcd_alt_slo_etag)
        self.assertEqual(headers['Etag'],
                         '"%s"' % self.manifest_abcd_alt_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_abcd_alt_json_md5)
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(
            body, b'aaaaabbbbbbbbbbcccccccccccccccdddddddddddddddddddd')
        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd-alt'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
             ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get')])


class TestOldSwiftWithRanges(SloGETorHEADTestCase):

    # Proxies have been writing moden slo-sysmeta since 2016; object servers
    # only started ignoring Range headers on x-static-large-object in 2020 and
    # it works on legacy manifests.
    modern_manifest_headers = True

    def setUp(self):
        super(TestOldSwiftWithRanges, self).setUp()
        # old swift didn't know how to ignore range headers and respond with
        # the whole object/manifest when specific metadata was present
        self.app.can_ignore_range = False

        self._setup_alphabet_objects('abcd')
        self._setup_manifest_bc()
        self._setup_manifest_bc_ranges()
        self._setup_manifest_abcd()
        self._setup_manifest_abcd_ranges()
        self._setup_big_manifest()

    def _setup_big_manifest(self):
        big = b'e' * 1024 * 1024
        big_etag = md5hex(big)
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/big_seg',
            swob.HTTPOk, {'Content-Type': 'application/foo',
                          'Etag': big_etag}, big)
        self._setup_manifest('big_man', [
            {'name': '/gettest/big_seg', 'hash': big_etag,
             'bytes': 1024 * 1024, 'content_type': 'application/foo'},
        ], extra_headers={
            'X-Backend-Timestamp': '1234',
        }, container='gettest', obj_key='big_manifest')

    def test_old_swift_range_get_includes_whole_manifest(self):
        # If the first range GET results in retrieval of the entire manifest
        # body (and not because of X-Backend-Ignore-Range-If-Metadata-Present,
        # but because the requested range happened to be sufficient which we
        # detected by looking at the Content-Range response header), then we
        # should not go make a second, non-ranged request just to retrieve the
        # same bytes again.
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=0-999999999'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Etag'],
                         '"%s"' % self.manifest_abcd_slo_etag)
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_abcd_json_md5)
        self.assertEqual(int(headers['Content-Length']),
                         self.manifest_abcd_slo_size)
        self.assertEqual(
            body, b'aaaaabbbbbbbbbbcccccccccccccccdddddddddddddddddddd')

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
             ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get')])

    def test_old_swift_range_get_beyond_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/big_manifest',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=100000-199999'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Etag'],
                         '"%s"' % self.manifest_big_man_slo_etag)
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_big_man_json_md5)
        self.assertEqual(int(headers['Content-Length']), len(body))
        count_e = sum(1 if x == 'e' else 0
                      for x in body.decode('ascii', errors='replace'))
        self.assertEqual(count_e, 100000)
        self.assertEqual(len(body) - count_e, 0)

        self.assertEqual(
            self.app.calls, [
                # has Range header, gets 416
                ('GET', '/v1/AUTH_test/gettest/big_manifest'),
                # old swift can't ignore range request to manifest and we have
                # to refetch; new swift has exactly the same behavior but w/o
                # this extra refetch request as lots of other tests demonstrate
                ('GET', '/v1/AUTH_test/gettest/big_manifest'),
                ('GET',
                 '/v1/AUTH_test/gettest/big_seg?multipart-manifest=get')])
        self.assertEqual('bytes=100000-199999', self.app.headers[0]['Range'])
        self.assertNotIn('Range', self.app.headers[1])
        self.assertEqual('bytes=100000-199999', self.app.headers[2]['Range'])

    def test_old_swift_range_get_beyond_manifest_refetch_fails(self):
        # new swift would have ignored the range and got the whole
        # manifest on the first try and therefore never have attempted
        # this second refetch which fails
        self.app.register_next_response(
            'GET', '/v1/AUTH_test/gettest/big_manifest',
            swob.HTTPNotFound, {}, None)

        req = Request.blank(
            '/v1/AUTH_test/gettest/big_manifest',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=100000-199999'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '503 Service Unavailable')
        self.assertNotIn('X-Static-Large-Object', headers)
        self.assertEqual(self.app.calls, [
            # has Range header, gets 416
            ('GET', '/v1/AUTH_test/gettest/big_manifest'),
            # retry the first one
            ('GET', '/v1/AUTH_test/gettest/big_manifest'),
        ])

    def test_old_swift_range_get_beyond_manifest_refetch_finds_old(self):
        # new swift would have ignored the range and got the whole
        # manifest on the first try and therefore never have attempted
        # this second refetch which is too old
        self.app.register_next_response(
            'GET', '/v1/AUTH_test/gettest/big_manifest',
            swob.HTTPOk, {'X-Backend-Timestamp': '1233'}, [b'small body'])

        req = Request.blank(
            '/v1/AUTH_test/gettest/big_manifest',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=100000-199999'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '503 Service Unavailable')
        self.assertNotIn('X-Static-Large-Object', headers)
        self.assertEqual(self.app.calls, [
            # has Range header, gets 416
            ('GET', '/v1/AUTH_test/gettest/big_manifest'),
            # retry the first one
            ('GET', '/v1/AUTH_test/gettest/big_manifest'),
        ])

    def test_old_swift_range_get_beyond_manifest_refetch_small_non_slo(self):
        # new swift would have ignored the range and got the whole
        # manifest on the first try and therefore never have attempted
        # this second refetch which isn't an SLO
        self.app.register_next_response(
            'GET', '/v1/AUTH_test/gettest/big_manifest',
            swob.HTTPOk, {'X-Backend-Timestamp': '1235'}, [b'small body'])
        req = Request.blank(
            '/v1/AUTH_test/gettest/big_manifest',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=100000-199999'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '416 Requested Range Not Satisfiable')
        self.assertNotIn('X-Static-Large-Object', headers)
        self.assertEqual(self.app.calls, [
            # has Range header, gets 416
            ('GET', '/v1/AUTH_test/gettest/big_manifest'),
            # retry the first one
            ('GET', '/v1/AUTH_test/gettest/big_manifest'),
        ])
        # swob is converting the successful non-slo response to conditional
        # error and closing our unconditionally refetched resp_iter
        self.expected_unread_requests[
            ('GET', '/v1/AUTH_test/gettest/big_manifest')] = 1

    def test_old_swift_range_get_beyond_manifest_refetch_big_non_slo(self):
        # new swift would have ignored the range and got the whole
        # manifest on the first try and therefore never have attempted
        # this second refetch which isn't an SLO
        self.app.register_next_response(
            'GET', '/v1/AUTH_test/gettest/big_manifest',
            swob.HTTPOk, {'X-Backend-Timestamp': '1235'}, [b'x' * 1024 * 1024])

        req = Request.blank(
            '/v1/AUTH_test/gettest/big_manifest',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=100000-199999'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')  # NOT 416 or 206!
        self.assertNotIn('X-Static-Large-Object', headers)
        self.assertEqual(len(body), 1024 * 1024)
        self.assertEqual(body, b'x' * 1024 * 1024)
        self.assertEqual(self.app.calls, [
            # has Range header, gets 416
            ('GET', '/v1/AUTH_test/gettest/big_manifest'),
            # retry the first one
            ('GET', '/v1/AUTH_test/gettest/big_manifest'),
        ])

    def test_old_swift_range_get_beyond_manifest_refetch_tombstone(self):
        # new swift would have ignored the range and got the whole
        # manifest on the first try and therefore never have attempted
        # this second refetch which shows it was deleted
        self.app.register_next_response(
            'GET', '/v1/AUTH_test/gettest/big_manifest',
            swob.HTTPNotFound, {'X-Backend-Timestamp': '1345'}, None)

        req = Request.blank(
            '/v1/AUTH_test/gettest/big_manifest',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=100000-199999'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '404 Not Found')
        self.assertNotIn('X-Static-Large-Object', headers)
        self.assertEqual(self.app.calls, [
            # has Range header, gets 416
            ('GET', '/v1/AUTH_test/gettest/big_manifest'),
            # retry the first one
            ('GET', '/v1/AUTH_test/gettest/big_manifest'),
        ])

    def test_old_swift_range_get_bogus_content_range(self):
        # Just a little paranoia; Swift currently sends back valid
        # Content-Range headers, but if somehow someone sneaks an invalid one
        # in there, we'll ignore it, when sniffing a 206 manifest response.

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

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(
            body, b'aaaaabbbbbbbbbbcccccccccccccccdddddddddddddddddddd')

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             # new swift would have ignored the range and got the whole
             # manifest on the first try and therefore never have attempted to
             # look at Content-Range; new swift has exactly the same behavior
             # but w/o this extra refetch request, however on new swift the
             # broken content-range in the resp isn't intresting or relevant
             ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
             ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get')])

    def test_old_swift_range_get_includes_whole_range_manifest(self):
        # If the first range GET results in retrieval of the entire manifest
        # body (and not because of X-Backend-Ignore-Range-If-Metadata-Present,
        # but because the requested range happened to be sufficient which we
        # detected by looking at the Content-Range response header), then we
        # should not go make a second, non-ranged request just to retrieve the
        # same bytes again.
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd-ranges',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=0-999999999'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Content-Length'], '32')
        self.assertEqual(headers['Content-Type'], 'application/json')
        self.assertEqual(body, b'aaaaaaaaccccccccbbbbbbbbdddddddd')

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


class TestOldSwiftWithRangesOldManifests(TestOldSwiftWithRanges):

    # Proxies have been writing modern slo-sysmeta since 2016; object servers
    # only started ignoring Range headers on x-static-large-object in 2020 and
    # it works on legacy manifests.
    modern_manifest_headers = False


class TestSloRangeRequests(SloGETorHEADTestCase):

    modern_manifest_headers = True

    def setUp(self):
        super(TestSloRangeRequests, self).setUp()
        self._setup_alphabet_objects('abcd')
        self._setup_manifest_bc()
        self._setup_manifest_abcd()
        self._setup_manifest_bc_ranges()
        self._setup_manifest_abcd_ranges()
        self._setup_manifest_abcd_subranges()

    def test_range_get_manifest_on_segment_boundaries(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=5-29'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Content-Length'], '25')
        self.assertEqual(headers['Content-Range'], 'bytes 5-29/50')
        self.assertEqual(headers['Etag'], '"%s"' % self.manifest_abcd_slo_etag)
        self.assertEqual(body, b'bbbbbbbbbbccccccccccccccc')

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
             ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get')])

        headers = [c[2] for c in self.app.calls_with_headers]
        self.assertEqual(headers[0].get('Range'), 'bytes=5-29')
        self.assertIsNone(headers[1].get('Range'))
        self.assertIsNone(headers[2].get('Range'))
        self.assertIsNone(headers[3].get('Range'))

    def test_range_get_manifest_first_byte(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=0-0'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Content-Length'], '1')
        self.assertEqual(headers['Content-Range'], 'bytes 0-0/50')
        self.assertEqual(body, b'a')

        # Make sure we don't get any objects we don't need, including
        # submanifests.
        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get')])

    def test_range_get_manifest_sub_slo(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=25-30'})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Content-Length'], '6')
        self.assertEqual(headers['Content-Range'], 'bytes 25-30/50')
        self.assertEqual(body, b'cccccd')

        # Make sure we don't get any objects we don't need, including
        # submanifests.
        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get')])

    def test_range_get_manifest_overlapping_end(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=45-55'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Content-Length'], '5')
        self.assertEqual(headers['Content-Range'], 'bytes 45-49/50')
        self.assertEqual(body, b'ddddd')

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get')])

    def test_range_get_manifest_unsatisfiable(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=100-200'})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '416 Requested Range Not Satisfiable')

    def test_get_segment_with_non_ascii_path(self):
        segment_body = u"a møøse once bit my sister".encode("utf-8")
        segment_etag = md5(segment_body, usedforsecurity=False).hexdigest()
        path = str_to_wsgi(u'/v1/AUTH_test/ünicode/öbject-segment')
        self.app.register(
            'GET', path,
            swob.HTTPOk, {'Content-Length': str(len(segment_body)),
                          'Etag': segment_etag},
            segment_body)

        manifest_json = json.dumps([{'name': u'/ünicode/öbject-segment',
                                     'hash': segment_etag,
                                     'content_type': 'text/plain',
                                     'bytes': len(segment_body)}])
        path = str_to_wsgi(u'/v1/AUTH_test/ünicode/manifest')
        self.app.register(
            'GET', path,
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'Content-Length': str(len(manifest_json)),
                          'X-Static-Large-Object': 'true'},
            manifest_json.encode('ascii'))

        req = Request.blank(
            str_to_wsgi('/v1/AUTH_test/ünicode/manifest'),
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(body, segment_body)

    def test_get_range_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd-ranges',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Content-Length'], '32')
        self.assertEqual(headers['Content-Type'], 'application/json')
        self.assertEqual(body, b'aaaaaaaaccccccccbbbbbbbbdddddddd')

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
        self.assertEqual(
            self.app.swift_sources[1:],
            ['SLO'] * (len(self.app.swift_sources) - 1)
        )
        self.assertEqual(md5hex(''.join([
            md5hex('a' * 5), ':0-3;',
            md5hex('a' * 5), ':1-4;',
            self.manifest_bc_ranges_slo_etag, ':8-15;',
            self.manifest_bc_ranges_slo_etag, ':0-7;',
            md5hex('d' * 20), ':0-3;',
            md5hex('d' * 20), ':8-11;',
        ])), headers['Etag'].strip('"'))

    def test_get_subrange_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd-subranges',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Content-Length'], '17')
        self.assertEqual(headers['Content-Type'], 'application/json')
        self.assertEqual(body, b'aacccdccbbbabbddd')

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

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Content-Length'], '20')
        self.assertEqual(headers['Content-Range'], 'bytes 7-26/32')
        self.assertEqual(headers['Content-Type'], 'application/json')
        self.assertIn('Etag', headers)
        self.assertEqual(body, b'accccccccbbbbbbbbddd')

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
            'bytes=7-26',
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

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Content-Length'], '9')
        self.assertEqual(headers['Content-Range'], 'bytes 4-12/17')
        self.assertEqual(headers['Content-Type'], 'application/json')
        self.assertEqual(body, b'cdccbbbab')

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd-subranges'),
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


class TestSloRangeRequestsOldManifest(TestSloRangeRequests):

    modern_manifest_headers = False


class TestSloErrors(SloGETorHEADTestCase):

    modern_manifest_headers = True

    def setUp(self):
        super(TestSloErrors, self).setUp()
        self._setup_alphabet_objects('abcd')
        self._setup_manifest_bc()
        self._setup_manifest_abcd()

        self._setup_manifest('badetag', [
            {'name': '/gettest/a_5', 'hash': 'wrong!',
             'content_type': 'text/plain', 'bytes': '5'}
        ], container='gettest')
        self._setup_manifest('badsize', [
            {'name': '/gettest/a_5', 'hash': md5hex('a' * 5),
             'content_type': 'text/plain', 'bytes': '999999'},
        ], container='gettest')

    def test_slo_sysmeta_on_error(self):
        headers = {
            'Content-Type': 'application/octet-stream',
            'X-Static-Large-Object': 'true',
            'X-Object-Meta-Animal': 'Pig',
        }
        if self.modern_manifest_headers:
            headers.update({
                'X-Object-Sysmeta-Slo-Etag': 'badmeta-etag',
                'X-Object-Sysmeta-Slo-Size': '123',
            })
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-badmeta',
            swob.HTTPNotFound, headers)
        req = Request.blank('/v1/AUTH_test/gettest/manifest-badmeta')
        status, headers, body = self.call_slo(req)
        # slo metadata on error response!?  there's a bug somewhere.
        self.assertEqual(status, '500 Internal Error')
        self.assertEqual(body, b'Unable to load SLO manifest')
        self.assertNotIn('X-Object-Meta-Animal', headers)
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/manifest-badmeta')])

    def test_get_bogus_manifest(self):
        headers = {
            'Content-Type': 'application/json',
            'X-Static-Large-Object': 'true',
            'X-Object-Meta-Fish': 'Bass',
        }
        if self.modern_manifest_headers:
            headers.update({
                'X-Object-Sysmeta-Slo-Etag': 'badjson-etag',
                'X-Object-Sysmeta-Slo-Size': '123',
            })
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-badjson',
            swob.HTTPOk, headers,
            "[not {json (at ++++all")

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-badjson',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        # This often (usually?) happens because of an incomplete read -- the
        # proxy app started getting a large manifest and sending it back to
        # SLO, then there was a timeout or something, couldn't resume in time,
        # and we've got just part of a JSON document. Having the client retry
        # seems reasonable
        self.assertEqual(status, '500 Internal Error')
        self.assertEqual(body, b'Unable to load SLO manifest')
        self.assertNotIn('X-Object-Meta-Fish', headers)
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/manifest-badjson')])

    def test_get_invalid_sysmeta_passthrough(self):
        # in an attempt to workaround lp bug#2035158 s3api used to set some
        # invalid slo/s3api sysmeta, we will always have some data stored with
        # empty values for these headers, but they're not SLOs and are missing
        # the X-Static-Large-Object marker sysmeta (thank goodness!)
        self.app.register(
            'GET', '/v1/AUTH_test/bucket+segments/obj/upload-id/1',
            swob.HTTPOk, {
                'X-Object-Sysmeta-S3Api-Acl': "{'some': 'json'}",
                'X-Object-Sysmeta-S3Api-Etag': '',
                'X-Object-Sysmeta-Slo-Etag': '',
                'X-Object-Sysmeta-Slo-Size': '',
                'X-Object-Sysmeta-Swift3-Etag': '',
            }, "any seg created with copy-part")
        req = Request.blank('/v1/AUTH_test/bucket+segments/obj/upload-id/1')
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertNotIn('X-Static-Large-Object', headers)
        self.assertNotIn('X-Manifest-Etag', headers)
        self.assertEqual(int(headers['Content-Length']), len(body))
        self.assertEqual(body, b"any seg created with copy-part")
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/bucket+segments/obj/upload-id/1'),
        ])

    def _do_test_generator_closure(self, leaks):
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
        class LeakTracker(object):
            def __init__(self, inner_iter):
                leaks[0] += 1
                self.inner_iter = iter(inner_iter)

            def __iter__(self):
                return self

            def __next__(self):
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
        return app_resp

    def test_generator_closure(self):
        leaks = [0]
        app_resp = self._do_test_generator_closure(leaks)
        body_iter = iter(app_resp)
        chunk = next(body_iter)
        self.assertEqual(chunk, b'aaaaa')  # sanity check
        app_resp.close()
        self.assertEqual(0, leaks[0])
        # we closed before reading all chunks
        self.expected_unread_requests[
            ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get')] = 1

    def test_generator_closure_iter_app_resp(self):
        # verify that the result of iter(app_resp) has a close method that
        # closes app_resp
        leaks = [0]
        app_resp = self._do_test_generator_closure(leaks)
        body_iter = iter(app_resp)
        chunk = next(body_iter)
        self.assertEqual(chunk, b'aaaaa')  # sanity check
        close_method = getattr(body_iter, 'close', None)
        self.assertIsNotNone(close_method)
        self.assertTrue(callable(close_method))
        close_method()
        self.assertEqual(0, leaks[0])
        # we closed before reading all chunks
        self.expected_unread_requests[
            ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get')] = 1

    def test_recursion_limit(self):
        # man1 points to obj1 and man2, man2 points to obj2 and man3...
        for i in range(20):
            self.app.register('GET', '/v1/AUTH_test/gettest/obj%d' % i,
                              swob.HTTPOk, {'Content-Type': 'text/plain',
                                            'Etag': md5hex('body%02d' % i)},
                              b'body%02d' % i)

        manifest_name = 'man1'
        manifest_data = [{'name': '/gettest/obj20',
                          'hash': md5hex('body20'),
                          'content_type': 'text/plain',
                          'bytes': '6'}]
        self._setup_manifest(manifest_name, manifest_data,
                             container='gettest', obj_key='man1')

        submanifest_bytes = 6
        for i in range(19, 0, -1):
            manifest_name = 'man%d' % i
            manifest_data = [
                {'name': '/gettest/obj%d' % i,
                 'hash': md5hex('body%02d' % i),
                 'bytes': '6',
                 'content_type': 'text/plain'},
                {'data': base64.b64encode(b'-' * 3).decode('ascii')},
                {'name': '/gettest/man%d' % (i + 1),
                 'hash': 'man%d' % (i + 1),
                 'sub_slo': True,
                 'bytes': submanifest_bytes,
                 'content_type': 'application/json'}]
            self._setup_manifest(manifest_name, manifest_data,
                                 container='gettest', obj_key=manifest_name)

            submanifest_bytes += 9

        req = Request.blank(
            '/v1/AUTH_test/gettest/man1',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        # we don't know at header-sending time that things are going to go
        # wrong, so we end up with a 200 and a truncated body
        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Content-Length'], str(9 * 19 + 6))
        self.assertEqual(body, (
            b'body01---body02---body03---body04---body05---'
            b'body06---body07---body08---body09---body10---'))
        # but the error shows up in logs
        self.assertEqual(self.slo.logger.get_lines_for_level('error'), [
            "While processing manifest '/v1/AUTH_test/gettest/man1', "
            "max recursion depth was exceeded"
        ])
        # make sure we didn't keep asking for segments
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/man1'),
            ('GET', '/v1/AUTH_test/gettest/obj1?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/man2'),
            ('GET', '/v1/AUTH_test/gettest/obj2?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/man3'),
            ('GET', '/v1/AUTH_test/gettest/obj3?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/man4'),
            ('GET', '/v1/AUTH_test/gettest/obj4?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/man5'),
            ('GET', '/v1/AUTH_test/gettest/obj5?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/man6'),
            ('GET', '/v1/AUTH_test/gettest/obj6?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/man7'),
            ('GET', '/v1/AUTH_test/gettest/obj7?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/man8'),
            ('GET', '/v1/AUTH_test/gettest/obj8?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/man9'),
            ('GET', '/v1/AUTH_test/gettest/obj9?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/man10'),
            ('GET', '/v1/AUTH_test/gettest/obj10?multipart-manifest=get'),
        ])

    def test_sub_slo_recursion(self):
        # man1 points to man2 and obj1, man2 points to man3 and obj2...
        for i in range(11):
            self.app.register('GET', '/v1/AUTH_test/gettest/obj%d' % i,
                              swob.HTTPOk, {'Content-Type': 'text/plain',
                                            'Content-Length': '6',
                                            'Etag': md5hex('body%02d' % i)},
                              b'body%02d' % i)

        manifest_name = 'man%d' % i
        manifest_data = [{'name': '/gettest/obj%d' % i,
                          'hash': md5hex('body%2d' % i),
                          'content_type': 'text/plain',
                          'bytes': '6'}]
        self._setup_manifest(manifest_name, manifest_data,
                             container='gettest', obj_key=manifest_name)

        self.app.register(
            'HEAD', '/v1/AUTH_test/gettest/obj%d' % i,
            swob.HTTPOk, {'Content-Length': '6',
                          'Etag': md5hex('body%2d' % i)},
            None)

        for i in range(9, 0, -1):
            manifest_name = 'man%d' % i
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
            self._setup_manifest(manifest_name, manifest_data,
                                 container='gettest', obj_key=manifest_name)

        req = Request.blank(
            '/v1/AUTH_test/gettest/man1',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(body, (b'body10body09body08body07body06'
                                b'body05body04body03body02body01'))

        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/man1'),
            ('GET', '/v1/AUTH_test/gettest/man2'),
            ('GET', '/v1/AUTH_test/gettest/man3'),
            ('GET', '/v1/AUTH_test/gettest/man4'),
            ('GET', '/v1/AUTH_test/gettest/man5'),
            ('GET', '/v1/AUTH_test/gettest/man6'),
            ('GET', '/v1/AUTH_test/gettest/man7'),
            ('GET', '/v1/AUTH_test/gettest/man8'),
            ('GET', '/v1/AUTH_test/gettest/man9'),
            ('GET', '/v1/AUTH_test/gettest/man10'),
            ('GET', '/v1/AUTH_test/gettest/obj10?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/obj9?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/obj8?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/obj7?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/obj6?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/obj5?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/obj4?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/obj3?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/obj2?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/obj1?multipart-manifest=get'),
        ])

    def test_sub_slo_recursion_limit(self):
        # man1 points to man2 and obj1, man2 points to man3 and obj2...
        for i in range(12):
            self.app.register('GET', '/v1/AUTH_test/gettest/obj%d' % i,
                              swob.HTTPOk,
                              {'Content-Type': 'text/plain',
                               'Content-Length': '6',
                               'Etag': md5hex('body%02d' % i)},
                              b'body%02d' % i)

        manifest_name = 'man%d' % i
        manifest_data = [{'name': '/gettest/obj%d' % i,
                          'hash': md5hex('body%2d' % i),
                          'content_type': 'text/plain',
                          'bytes': '6'}]
        self._setup_manifest(manifest_name, manifest_data,
                             container='gettest', obj_key=manifest_name)

        self.app.register(
            'HEAD', '/v1/AUTH_test/gettest/obj%d' % i,
            swob.HTTPOk, {'Content-Length': '6',
                          'Etag': md5hex('body%2d' % i)},
            None)

        for i in range(11, 0, -1):
            manifest_name = 'man%d' % i
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
            self._setup_manifest(manifest_name, manifest_data,
                                 container='gettest', obj_key=manifest_name)

        req = Request.blank(
            '/v1/AUTH_test/gettest/man1',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '409 Conflict')
        self.assertNotIn('Etag', headers)
        self.assertNotIn('X-Manifest-Etag', headers)
        self.assertNotIn('X-Static-Large-Object', headers)
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/man1'),
            ('GET', '/v1/AUTH_test/gettest/man2'),
            ('GET', '/v1/AUTH_test/gettest/man3'),
            ('GET', '/v1/AUTH_test/gettest/man4'),
            ('GET', '/v1/AUTH_test/gettest/man5'),
            ('GET', '/v1/AUTH_test/gettest/man6'),
            ('GET', '/v1/AUTH_test/gettest/man7'),
            ('GET', '/v1/AUTH_test/gettest/man8'),
            ('GET', '/v1/AUTH_test/gettest/man9'),
            ('GET', '/v1/AUTH_test/gettest/man10'),
        ])
        self.assertEqual(self.slo.logger.get_lines_for_level('error'), [
            "While processing manifest '/v1/AUTH_test/gettest/man1', "
            "max recursion depth was exceeded"
        ])

    def test_error_fetching_segment(self):
        self.app.register('GET', '/v1/AUTH_test/gettest/c_15',
                          swob.HTTPUnauthorized, {}, None)

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(b"aaaaabbbbbbbbbb", body)
        self.assertEqual(self.slo.logger.get_lines_for_level('error'), [
            'While processing manifest /v1/AUTH_test/gettest/manifest-abcd, '
            'got 401 (<html><h1>Unauthorized</h1><p>This server could not '
            'verif...) while retrieving /v1/AUTH_test/gettest/c_15'
        ])
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
        status, headers, body = self.call_slo(req)

        self.assertEqual("200 OK", status)
        self.assertEqual(b"aaaaa", body)
        self.assertEqual(self.slo.logger.get_lines_for_level('error'), [
            'while fetching /v1/AUTH_test/gettest/manifest-abcd, GET of '
            'submanifest /v1/AUTH_test/gettest/manifest-bc failed with '
            'status 401 (<html><h1>Unauthorized</h1><p>This server could '
            'not verif...)'
        ])
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
            # TODO: skip coalecse until validate to re-order sub-manifest req
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
        self._setup_manifest('manifest-a', [
            {'name': '/gettest/manifest-a', 'sub_slo': True,
             'content_type': 'application/json',
             'hash': 'manifest-a', 'bytes': '12345'},
        ], container='gettest')

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-manifest-a',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('409 Conflict', status)
        self.assertEqual(self.slo.logger.get_lines_for_level('error'), [
            'while fetching /v1/AUTH_test/gettest/manifest-manifest-a, GET '
            'of submanifest /v1/AUTH_test/gettest/manifest-a failed with '
            'status 403 (<html><h1>Forbidden</h1><p>Access was denied to '
            'this reso...)'
        ])

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
        status, headers, body = self.call_slo(req)

        self.assertEqual('200 OK', status)
        self.assertEqual(body, b'aaaaa')
        self.assertEqual(self.slo.logger.get_lines_for_level('error'), [
            'Unable to load SLO manifest: '
            'Expecting value: line 1 column 2 (char 1)',
            'while fetching /v1/AUTH_test/gettest/manifest-abcd, '
            'JSON-decoding of submanifest /v1/AUTH_test/gettest/manifest-bc '
            'failed with 500 Internal Error'
        ])

    def test_mismatched_etag(self):
        self._setup_manifest('a-b-badetag-c', [
            {'name': '/gettest/a_5', 'hash': md5hex('a' * 5),
             'content_type': 'text/plain', 'bytes': '5'},
            {'name': '/gettest/b_10', 'hash': 'wrong!',
             'content_type': 'text/plain', 'bytes': '10'},
            {'name': '/gettest/c_15', 'hash': md5hex('c' * 15),
             'content_type': 'text/plain', 'bytes': '15'},
        ], container='gettest')

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-a-b-badetag-c',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('200 OK', status)
        self.assertEqual(headers['Etag'], '"%s"' %
                         self.manifest_a_b_badetag_c_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_a_b_badetag_c_json_md5)
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(body, b'aaaaa')
        self.assertEqual(self.slo.logger.get_lines_for_level('error'), [
            'Object segment no longer valid: /v1/AUTH_test/gettest/b_10 '
            'etag: 82136b4240d6ce4ea7d03e51469a393b != wrong! or 10 != 10.'
        ])
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/manifest-a-b-badetag-c'),
            ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
        ])
        # we don't drain the segment's resp_iter if validation fails
        self.expected_unread_requests[
            ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get')] = 1

    def test_mismatched_size(self):
        self._setup_manifest('a-b-badsize-c', [
            {'name': '/gettest/a_5', 'hash': md5hex('a' * 5),
             'content_type': 'text/plain', 'bytes': '5'},
            {'name': '/gettest/b_10', 'hash': md5hex('b' * 10),
             'content_type': 'text/plain', 'bytes': '999999'},
            {'name': '/gettest/c_15', 'hash': md5hex('c' * 15),
             'content_type': 'text/plain', 'bytes': '15'},
        ], container='gettest')

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-a-b-badsize-c',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('200 OK', status)
        self.assertEqual(body, b'aaaaa')
        self.assertEqual(self.slo.logger.get_lines_for_level('error'), [
            'Object segment no longer valid: /v1/AUTH_test/gettest/b_10 '
            'etag: 82136b4240d6ce4ea7d03e51469a393b != '
            '82136b4240d6ce4ea7d03e51469a393b or 10 != 999999.'
        ])
        # we don't drain the segment's resp_iter if validation fails
        self.expected_unread_requests[
            ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get')] = 1

    def test_mismatched_checksum(self):
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/a_5',
            swob.HTTPOk, {'Content-Length': '5',
                          'Etag': md5hex('a' * 5)},
            # this segment has invalid content
            'x' * 5)

        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            json.dumps([{'name': '/gettest/b_10', 'hash': md5hex('b' * 10),
                         'content_type': 'text/plain', 'bytes': '10'},
                        {'name': '/gettest/a_5', 'hash': md5hex('a' * 5),
                         'content_type': 'text/plain', 'bytes': '5'},
                        {'name': '/gettest/c_15', 'hash': md5hex('c' * 15),
                         'content_type': 'text/plain', 'bytes': '15'}]))

        req = Request.blank('/v1/AUTH_test/gettest/manifest')
        status, headers, body = self.call_slo(req)

        self.assertEqual('200 OK', status)
        self.assertEqual(body, (b'b' * 10 + b'x' * 5))
        self.assertEqual(self.slo.logger.get_lines_for_level('error'), [
            'Bad MD5 checksum for /v1/AUTH_test/gettest/a_5 as part of '
            '/v1/AUTH_test/gettest/manifest: headers had '
            '594f803b380a41396ed63dca39503542, but object MD5 was '
            'actually fb0e22c79ac75679e9881e6ba183b354',
        ])

    def test_mismatched_length(self):
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/a_5',
            swob.HTTPOk, {'Content-Length': '5',
                          'Etag': md5hex('a' * 5)},
            # this segment comes up short
            [b'a' * 4])

        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest',
            swob.HTTPOk, {'Content-Type': 'application/json',
                          'X-Static-Large-Object': 'true'},
            json.dumps([{'name': '/gettest/b_10', 'hash': md5hex('b' * 10),
                         'content_type': 'text/plain', 'bytes': '10'},
                        {'name': '/gettest/a_5', 'hash': md5hex('a' * 5),
                         'content_type': 'text/plain', 'bytes': '5'},
                        {'name': '/gettest/c_15', 'hash': md5hex('c' * 15),
                         'content_type': 'text/plain', 'bytes': '15'}]))

        req = Request.blank('/v1/AUTH_test/gettest/manifest')
        status, headers, body = self.call_slo(req)

        self.assertEqual('200 OK', status)
        self.assertEqual(body, (b'b' * 10 + b'a' * 4))
        self.assertEqual(self.slo.logger.get_lines_for_level('error'), [
            'Bad response length for /v1/AUTH_test/gettest/a_5 as part of '
            '/v1/AUTH_test/gettest/manifest: headers had 5, but '
            'response length was actually 4',
        ])

    def test_first_segment_mismatched_etag(self):
        req = Request.blank('/v1/AUTH_test/gettest/manifest-badetag')
        status, headers, body = self.call_slo(req)

        self.assertEqual('409 Conflict', status)
        self.assertNotIn('Etag', headers)
        self.assertNotIn('X-Manifest-Etag', headers)
        self.assertNotIn('X-Static-Large-Object', headers)
        self.assertEqual(int(headers['Content-Length']), len(body))
        self.assertIn(b'There was a conflict', body)

        self.assertEqual(self.slo.logger.get_lines_for_level('error'), [
            'Object segment no longer valid: /v1/AUTH_test/gettest/a_5 '
            'etag: 594f803b380a41396ed63dca39503542 != wrong! or 5 != 5.'
        ])
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/manifest-badetag'),
            ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
        ])
        # we don't drain the segment's resp_iter if validation fails
        self.expected_unread_requests[
            ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get')] = 1

    def test_head_does_not_validate_first_segment_mismatched_etag(self):
        req = Request.blank('/v1/AUTH_test/gettest/manifest-badetag',
                            method='HEAD')
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Etag'],
                         '"%s"' % self.manifest_badetag_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_badetag_json_md5)
        self.assertEqual(int(headers['Content-Length']),
                         self.manifest_badetag_slo_size)
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        expected_calls = [
            ('HEAD', '/v1/AUTH_test/gettest/manifest-badetag'),
        ]
        if not self.modern_manifest_headers:
            expected_calls.append(
                ('GET', '/v1/AUTH_test/gettest/manifest-badetag'))
        self.assertEqual(self.app.calls, expected_calls)

    def test_first_segment_mismatched_size(self):
        req = Request.blank('/v1/AUTH_test/gettest/manifest-badsize',
                            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('409 Conflict', status)
        self.assertNotIn('Etag', headers)
        self.assertNotIn('X-Manifest-Etag', headers)
        self.assertNotIn('X-Static-Large-Object', headers)
        self.assertEqual(int(headers['Content-Length']), len(body))
        self.assertIn(b'There was a conflict', body)
        self.assertEqual(self.slo.logger.get_lines_for_level('error'), [
            'Object segment no longer valid: /v1/AUTH_test/gettest/a_5 '
            'etag: 594f803b380a41396ed63dca39503542 != '
            '594f803b380a41396ed63dca39503542 or 5 != 999999.'
        ])
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/manifest-badsize'),
            ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
        ])
        # we don't drain the segment's resp_iter if validation fails
        self.expected_unread_requests[
            ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get')] = 1

    def test_head_does_not_validate_first_segment_mismatched_size(self):
        req = Request.blank('/v1/AUTH_test/gettest/manifest-badsize',
                            method='HEAD')
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Etag'],
                         '"%s"' % self.manifest_badsize_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_badsize_json_md5)
        self.assertEqual(int(headers['Content-Length']),
                         self.manifest_badsize_slo_size)
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        expected_calls = [
            ('HEAD', '/v1/AUTH_test/gettest/manifest-badsize'),
        ]
        if not self.modern_manifest_headers:
            expected_calls.append(
                ('GET', '/v1/AUTH_test/gettest/manifest-badsize'))
        self.assertEqual(self.app.calls, expected_calls)

    @patch('swift.common.request_helpers.time')
    def test_download_takes_too_long(self, mock_time):
        mock_time.time.side_effect = [
            0,  # start time
            10 * 3600,  # a_5
            20 * 3600,  # b_10
            30 * 3600,  # c_15, but then we time out
        ]
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'})

        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(self.slo.logger.get_lines_for_level('error'), [
            'While processing manifest /v1/AUTH_test/gettest/manifest-abcd, '
            'max LO GET time of 86400s exceeded'
        ])
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
            ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
            ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get')])
        # we timeout without reading the whole of the last segment
        self.expected_unread_requests[
            ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get')] = 1

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
        self.assertEqual(self.slo.logger.get_lines_for_level('error'), [
            'While processing manifest /v1/AUTH_test/gettest/'
            'manifest-not-exists, got 404 (<html><h1>Not Found</h1><p>The '
            'resource could not be foun...) while retrieving /v1/AUTH_test/'
            'gettest/not_exists_obj'
        ])

    def test_first_segment_not_available(self):
        self.app.register('GET', '/v1/AUTH_test/gettest/not_avail_obj',
                          swob.HTTPServiceUnavailable, {}, None)
        self.app.register('GET', '/v1/AUTH_test/gettest/manifest-not-avail',
                          swob.HTTPOk, {'Content-Type': 'application/json',
                                        'X-Static-Large-Object': 'true'},
                          json.dumps([{'name': '/gettest/not_avail_obj',
                                       'hash': md5hex('not_avail_obj'),
                                       'content_type': 'text/plain',
                                       'bytes': '%d' % len('not_avail_obj')
                                       }]))

        req = Request.blank('/v1/AUTH_test/gettest/manifest-not-avail',
                            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('503 Service Unavailable', status)
        self.assertEqual(self.slo.logger.get_lines_for_level('error'), [
            'While processing manifest /v1/AUTH_test/gettest/'
            'manifest-not-avail, got 503 (<html><h1>Service Unavailable</h1>'
            '<p>The server is curren...) while retrieving /v1/AUTH_test/'
            'gettest/not_avail_obj'
        ])
        self.assertIn(b'Service Unavailable', body)


class TestSloErrorsOldManifests(TestSloErrors):

    modern_manifest_headers = False


class TestSloDataSegments(SloGETorHEADTestCase):
    # data segments were added some months after modern slo-sysmeta

    def setUp(self):
        super(TestSloDataSegments, self).setUp()
        self._setup_alphabet_objects('ab')

    def test_leading_data_segment(self):
        slo_etag = md5hex(
            md5hex('preamble') +
            md5hex('a' * 5)
        )
        preamble = base64.b64encode(b'preamble')
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-single-preamble',
            swob.HTTPOk,
            {
                'Content-Type': 'application/json',
                'X-Static-Large-Object': 'true'
            },
            json.dumps([{
                'data': preamble.decode('ascii')
            }, {
                'name': '/gettest/a_5',
                'hash': md5hex('a' * 5),
                'content_type': 'text/plain',
                'bytes': '5',
            }])
        )

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-single-preamble',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('200 OK', status)
        self.assertEqual(body, b'preambleaaaaa')
        self.assertEqual(headers['Etag'], '"%s"' % slo_etag)
        self.assertEqual(headers['Content-Length'], '13')

    def test_trailing_data_segment(self):
        slo_etag = md5hex(
            md5hex('a' * 5) +
            md5hex('postamble')
        )
        postamble = base64.b64encode(b'postamble')
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-single-postamble',
            swob.HTTPOk,
            {
                'Content-Type': 'application/json',
                'X-Static-Large-Object': 'true'
            },
            json.dumps([{
                'name': '/gettest/a_5',
                'hash': md5hex('a' * 5),
                'content_type': 'text/plain',
                'bytes': '5',
            }, {
                'data': postamble.decode('ascii')
            }]).encode('ascii')
        )

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-single-postamble',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('200 OK', status)
        self.assertEqual(body, b'aaaaapostamble')
        self.assertEqual(headers['Etag'], '"%s"' % slo_etag)
        self.assertEqual(headers['Content-Length'], '14')

    def test_data_segment_sandwich(self):
        slo_etag = md5hex(
            md5hex('preamble') +
            md5hex('a' * 5) +
            md5hex('postamble')
        )
        preamble = base64.b64encode(b'preamble')
        postamble = base64.b64encode(b'postamble')
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-single-prepostamble',
            swob.HTTPOk,
            {
                'Content-Type': 'application/json',
                'X-Static-Large-Object': 'true'
            },
            json.dumps([{
                'data': preamble.decode('ascii'),
            }, {
                'name': '/gettest/a_5',
                'hash': md5hex('a' * 5),
                'content_type': 'text/plain',
                'bytes': '5',
            }, {
                'data': postamble.decode('ascii')
            }])
        )

        # Test the whole SLO
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-single-prepostamble',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('200 OK', status)
        self.assertEqual(body, b'preambleaaaaapostamble')
        self.assertEqual(headers['Etag'], '"%s"' % slo_etag)
        self.assertEqual(headers['Content-Length'], '22')

        # Test complete preamble only
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-single-prepostamble',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=0-7'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('206 Partial Content', status)
        self.assertEqual(body, b'preamble')

        # Test range within preamble only
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-single-prepostamble',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=1-5'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('206 Partial Content', status)
        self.assertEqual(body, b'reamb')

        # Test complete postamble only
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-single-prepostamble',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=13-21'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('206 Partial Content', status)
        self.assertEqual(body, b'postamble')

        # Test partial pre and postamble
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-single-prepostamble',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=4-16'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('206 Partial Content', status)
        self.assertEqual(body, b'mbleaaaaapost')

        # Test partial preamble and first byte of data
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-single-prepostamble',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=1-8'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('206 Partial Content', status)
        self.assertEqual(body, b'reamblea')

        # Test last byte of segment data and partial postamble
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-single-prepostamble',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=12-16'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('206 Partial Content', status)
        self.assertEqual(body, b'apost')

    def test_bunches_of_data_segments(self):
        slo_etag = md5hex(
            md5hex('ABCDEF') +
            md5hex('a' * 5) +
            md5hex('123456') +
            md5hex('GHIJKL') +
            md5hex('b' * 10) +
            md5hex('7890@#')
        )
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/manifest-multi-prepostamble',
            swob.HTTPOk,
            {
                'Content-Type': 'application/json',
                'X-Static-Large-Object': 'true'
            },
            json.dumps([
                {
                    'data': base64.b64encode(b'ABCDEF').decode('ascii')
                },
                {
                    'name': '/gettest/a_5',
                    'hash': md5hex('a' * 5),
                    'content_type': 'text/plain',
                    'bytes': '5',
                },
                {
                    'data': base64.b64encode(b'123456').decode('ascii')
                },
                {
                    'data': base64.b64encode(b'GHIJKL').decode('ascii')
                },
                {
                    'name': '/gettest/b_10',
                    'hash': md5hex('b' * 10),
                    'content_type': 'text/plain',
                    'bytes': '10',
                },
                {
                    'data': base64.b64encode(b'7890@#').decode('ascii')
                }
            ])
        )

        # Test the whole SLO
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-multi-prepostamble',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('200 OK', status)
        self.assertEqual(body, b'ABCDEFaaaaa123456GHIJKLbbbbbbbbbb7890@#')
        self.assertEqual(headers['Etag'], '"%s"' % slo_etag)
        self.assertEqual(headers['Content-Length'], '39')

        # Test last byte first pre-amble to first byte of second postamble
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-multi-prepostamble',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=5-33'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('206 Partial Content', status)
        self.assertEqual(body, b'Faaaaa123456GHIJKLbbbbbbbbbb7')

        # Test only second complete preamble
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-multi-prepostamble',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=17-22'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('206 Partial Content', status)
        self.assertEqual(body, b'GHIJKL')

        # Test only first complete postamble
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-multi-prepostamble',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=11-16'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('206 Partial Content', status)
        self.assertEqual(body, b'123456')

        # Test only range within first postamble
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-multi-prepostamble',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=12-15'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('206 Partial Content', status)
        self.assertEqual(body, b'2345')

        # Test only range within first postamble and second preamble
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-multi-prepostamble',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=12-18'})
        status, headers, body = self.call_slo(req)

        self.assertEqual('206 Partial Content', status)
        self.assertEqual(body, b'23456GH')


class TestSloConditionalGetOldManifest(SloGETorHEADTestCase):

    modern_manifest_headers = False

    def setUp(self):
        super(TestSloConditionalGetOldManifest, self).setUp()
        self._setup_alphabet_objects('abcd')
        self._setup_manifest_bc()
        self._setup_manifest_abcd()

        # plain object with alt-etag
        num_segments = 2
        alt_seg_info = []
        for i in range(num_segments):
            body = (b'alt_%02d' % i) * 5
            etag = md5hex(body)
            self.app.register(
                'GET', '/v1/AUTH_test/c/alt_%02d' % i,
                swob.HTTPOk, {
                    'Content-Length': len(body),
                    'Etag': etag,
                    'X-Object-Sysmeta-Alt-Etag': 'seg-etag-%02d' % i
                }, body=body)
            alt_seg_info.append((body, etag))

        # s3api is to the left of SLO and writes an alternate etag for
        # conditional requests to match
        self._setup_manifest('alt', [{
            'name': '/c/alt_%02d' % i,
            'bytes': len(body),
            'hash': etag,
            'content_type': 'text/plain',
        } for i, (body, etag) in enumerate(alt_seg_info)], extra_headers={
            'X-Object-Sysmeta-Alt-Etag': '"alt-etag-1"',
        })

        self._setup_manifest('last-modified', [
            {'name': '/gettest/a_5', 'hash': md5hex('a' * 5), 'bytes': '5',
             'content_type': 'text/plain'},
            {'name': '/gettest/c_15', 'hash': md5hex('c' * 15), 'bytes': '15',
             'content_type': 'text/plain'},
        ], extra_headers={
            'Last-Modified': 'Mon, 23 Oct 2023 10:05:32 GMT',
        })

    def test_if_none_match_matches(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            headers={'If-None-Match': self.manifest_abcd_slo_etag})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '304 Not Modified')
        self.assertEqual('"%s"' % self.manifest_abcd_slo_etag, headers['Etag'])
        # conditional errors are always zero-bytes to save client egress
        self.assertEqual('0', headers['Content-Length'])
        self.assertEqual(b'', body)

        expected_app_calls = [('GET', '/v1/AUTH_test/gettest/manifest-abcd')]
        if not self.modern_manifest_headers:
            expected_app_calls.extend([
                # N.B. since manifest didn't match slo_etag => no refetch
                # TODO: skip coalecse until validate to avoid sub-manifest req
                ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
                # for legacy manifests we don't know if swob will return a
                # successful response or conditional error so we validate the
                # first segment to avoid a 2XX when we should 5XX
                ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
            ])
            # when swob decides to error it closes our SegmentedIterable
            # and we don't drain the (possibly large) segment.
            self.expected_unread_requests[('GET', '/v1/AUTH_test/gettest/a_5'
                                           '?multipart-manifest=get')] = 1
        self.assertEqual(self.app.calls, expected_app_calls)
        self.assertEqual(self.app.headers[0].get('X-Backend-Etag-Is-At'),
                         'x-object-sysmeta-slo-etag')
        if not self.modern_manifest_headers:
            for headers in self.app.headers[1:]:
                self.assertNotIn('If-Match', headers)
                self.assertNotIn('X-Backend-Etag-Is-At', headers)

    def test_if_none_match_mismatches(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            headers={'If-None-Match': "not-%s" % self.manifest_abcd_slo_etag})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual('"%s"' % self.manifest_abcd_slo_etag, headers['Etag'])
        self.assertEqual('50', headers['Content-Length'])
        self.assertEqual(
            body, b'aaaaabbbbbbbbbbcccccccccccccccdddddddddddddddddddd')

        expected_app_calls = [
            ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
            ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
            ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get'),
        ]
        self.assertEqual(self.app.calls, expected_app_calls)
        self.assertEqual(self.app.headers[0].get('X-Backend-Etag-Is-At'),
                         'x-object-sysmeta-slo-etag')

    def test_if_none_match_mismatches_json_md5(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            headers={'If-None-Match': self.manifest_abcd_json_md5})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual('"%s"' % self.manifest_abcd_slo_etag, headers['Etag'])
        self.assertEqual('50', headers['Content-Length'])

        expected_app_calls = [
            ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
        ]
        if not self.modern_manifest_headers:
            # w/o modern manifest headers, the json manifest etag responds 304
            # and triggers a refetch!
            expected_app_calls.append(
                ('GET', '/v1/AUTH_test/gettest/manifest-abcd')
            )
        expected_app_calls.extend([
            ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
            ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get'),
        ])
        self.assertEqual(self.app.calls, expected_app_calls)

    def test_if_none_match_matches_alternate_etag(self):
        req = Request.blank(
            '/v1/AUTH_test/c/manifest-alt',
            headers={'If-None-Match': '"alt-etag-1"'})
        update_etag_is_at_header(req, 'X-Object-Sysmeta-Alt-Etag')
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '304 Not Modified')
        # N.B. Etag-Is-At only effects conditional matching, not response Etag
        self.assertEqual('"%s"' % self.manifest_alt_slo_etag, headers['Etag'])
        # ... but the response Sysmeta will be available to wrapping middleware
        self.assertEqual('"alt-etag-1"', headers['X-Object-Sysmeta-Alt-Etag'])
        # conditional errors are always zero-bytes to save client egress
        self.assertEqual('0', headers['Content-Length'])
        self.assertEqual(b'', body)

        expected_app_calls = [('GET', '/v1/AUTH_test/c/manifest-alt')]
        self.assertEqual(
            self.app.headers[0].get('X-Backend-Etag-Is-At'),
            'X-Object-Sysmeta-Alt-Etag,x-object-sysmeta-slo-etag')
        if not self.modern_manifest_headers:
            expected_app_calls.extend([
                # Needed to re-fetch because if-match can't find slo-etag, and
                # has to 304
                ('GET', '/v1/AUTH_test/c/manifest-alt'),
                # for legacy manifests we don't know if swob will return a
                # successful response or conditional error so we validate the
                # first segment to avoid a 2XX when we should 5XX
                ('GET', '/v1/AUTH_test/c/alt_00?multipart-manifest=get'),
            ])
            # when swob decides to error it closes our SegmentedIterable
            # and we don't drain the (possibly large) segment.
            self.expected_unread_requests[('GET', '/v1/AUTH_test/c/alt_00'
                                           '?multipart-manifest=get')] = 1
            for headers in self.app.headers[1:]:
                self.assertNotIn('If-None-Match', headers)
                self.assertNotIn('X-Backend-Etag-Is-At', headers)
        self.assertEqual(self.app.calls, expected_app_calls)

    def test_if_none_match_matches_no_alternate_etag(self):
        # this is similar to test_if_none_match_matches, but serves as a sanity
        # check to test_if_none_match_mismatches_alternate_etag, which appends
        # to etag-is-at
        req = Request.blank(
            '/v1/AUTH_test/c/manifest-alt',
            headers={'If-None-Match': self.manifest_alt_slo_etag})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '304 Not Modified')
        self.assertEqual('"%s"' % self.manifest_alt_slo_etag, headers['Etag'])
        # conditional errors are always zero-bytes to save client egress
        self.assertEqual('0', headers['Content-Length'])
        self.assertEqual(b'', body)

        expected_app_calls = [('GET', '/v1/AUTH_test/c/manifest-alt')]
        self.assertEqual(self.app.headers[0].get('X-Backend-Etag-Is-At'),
                         'x-object-sysmeta-slo-etag')
        if not self.modern_manifest_headers:
            expected_app_calls.extend([
                # N.B. since manifest didn't match slo_etag => no refetch
                # for legacy manifests we don't know if swob will return a
                # successful response or conditional error so we validate the
                # first segment to avoid a 2XX when we should 5XX
                ('GET', '/v1/AUTH_test/c/alt_00?multipart-manifest=get'),
            ])
            for headers in self.app.headers[1:]:
                self.assertNotIn('If-Match', headers)
                self.assertNotIn('X-Backend-Etag-Is-At', headers)
            # for legacy manifests we don't know if swob will return a
            # successful response or conditional error so we validate the first
            # segment to avoid a 2XX when we should 5XX, when swob decides to
            # error it closes our SegmentedIterable and we don't drain the
            # (possibly large) segment.
            self.expected_unread_requests[
                ('GET', '/v1/AUTH_test/c/alt_00'
                 '?multipart-manifest=get')] = 1
        self.assertEqual(self.app.calls, expected_app_calls)

    def test_if_none_match_mismatches_alternate_etag(self):
        req = Request.blank(
            '/v1/AUTH_test/c/manifest-alt',
            headers={'If-None-Match': self.manifest_alt_slo_etag})
        # N.B. SLO request with if-none-match slo_etag would normally respond
        # not modified (see test_if_none_match_matches_no_alternate_etag), but
        # here we provide alt-tag so it doesn't match so the request is success
        update_etag_is_at_header(req, 'X-Object-Sysmeta-Alt-Etag')
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        # N.B. Etag-Is-At only effects conditional matching, not response Etag
        self.assertEqual('"%s"' % self.manifest_alt_slo_etag, headers['Etag'])
        # ... but the response Sysmeta will be available to wrapping middleware
        self.assertEqual('"alt-etag-1"', headers['X-Object-Sysmeta-Alt-Etag'])
        self.assertEqual(self.manifest_alt_slo_size,
                         int(headers['Content-Length']))

        expected_app_calls = [
            ('GET', '/v1/AUTH_test/c/manifest-alt'),
            ('GET', '/v1/AUTH_test/c/alt_00?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/c/alt_01?multipart-manifest=get'),
        ]
        self.assertEqual(self.app.calls, expected_app_calls)
        self.assertEqual(
            self.app.headers[0].get('X-Backend-Etag-Is-At'),
            'X-Object-Sysmeta-Alt-Etag,x-object-sysmeta-slo-etag')
        for headers in self.app.headers[1:]:
            self.assertNotIn('If-None-Match', headers)
            self.assertNotIn('X-Backend-Etag-Is-At', headers)

    def test_if_match_matches(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'If-Match': self.manifest_abcd_slo_etag})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual('"%s"' % self.manifest_abcd_slo_etag, headers['Etag'])
        self.assertEqual('50', headers['Content-Length'])
        self.assertEqual(
            body, b'aaaaabbbbbbbbbbcccccccccccccccdddddddddddddddddddd')

        expected_app_calls = [('GET', '/v1/AUTH_test/gettest/manifest-abcd')]
        if not self.modern_manifest_headers:
            # Manifest never matches -> got back a 412; need to re-fetch
            expected_app_calls.append(
                ('GET', '/v1/AUTH_test/gettest/manifest-abcd'))
        expected_app_calls.extend([
            ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
            ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get'),
        ])
        self.assertEqual(self.app.calls, expected_app_calls)
        self.assertEqual(self.app.headers[0].get('X-Backend-Etag-Is-At'),
                         'x-object-sysmeta-slo-etag')

    def test_if_match_mismatches(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            headers={'If-Match': 'not-%s' % self.manifest_abcd_json_md5})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '412 Precondition Failed')
        self.assertEqual('"%s"' % self.manifest_abcd_slo_etag, headers['Etag'])
        # conditional errors are always zero-bytes to save client egress
        self.assertEqual('0', headers['Content-Length'])
        self.assertEqual(b'', body)

        expected_app_calls = [('GET', '/v1/AUTH_test/gettest/manifest-abcd')]
        if not self.modern_manifest_headers:
            expected_app_calls.extend([
                # Manifest "never" matches -> got back a 412; need to re-fetch
                ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
                # TODO: skip coalecse until validate to avoid sub-manifest req
                ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
                # for legacy manifests we don't know if swob will return a
                # successful response or conditional error so we validate the
                # first segment to avoid a 2XX when we should 5XX
                ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
            ])
            # when swob decides to error it closes our SegmentedIterable
            # and we don't drain the (possibly large) segment.
            self.expected_unread_requests[
                ('GET', '/v1/AUTH_test/gettest/a_5'
                 '?multipart-manifest=get')] = 1
        self.assertEqual(self.app.calls, expected_app_calls)
        self.assertEqual(self.app.headers[0].get('X-Backend-Etag-Is-At'),
                         'x-object-sysmeta-slo-etag')
        if not self.modern_manifest_headers:
            for headers in self.app.headers[1:]:
                self.assertNotIn('If-Match', headers)
                self.assertNotIn('X-Backend-Etag-Is-At', headers)

    def test_if_match_mismatches_manifest_json_md5(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            headers={'If-Match': self.manifest_abcd_json_md5})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '412 Precondition Failed')
        self.assertEqual('"%s"' % self.manifest_abcd_slo_etag, headers['Etag'])
        # 412 is always zero-bytes because client is trying to save egress
        self.assertEqual('0', headers['Content-Length'])
        self.assertEqual(body, b'')

        expected_app_calls = [('GET', '/v1/AUTH_test/gettest/manifest-abcd')]
        self.assertEqual(self.app.headers[0].get('X-Backend-Etag-Is-At'),
                         'x-object-sysmeta-slo-etag')
        if not self.modern_manifest_headers:
            # We *still* verify the first segment
            expected_app_calls.extend([
                # N.B. since manifest matched => no refetch
                # TODO: skip coalecse until validate to avoid sub-manifest req
                ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
                ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
            ])
            # swob conditional errors throw away the SegmentedIterable w/o
            # reading the remaining segments
            self.expected_unread_requests[('GET', '/v1/AUTH_test/gettest/a_5'
                                           '?multipart-manifest=get')] = 1
        self.assertEqual(self.app.calls, expected_app_calls)

    def test_if_match_matches_alternate_etag(self):
        req = Request.blank(
            '/v1/AUTH_test/c/manifest-alt',
            headers={'If-Match': '"alt-etag-1"'})
        update_etag_is_at_header(req, 'X-Object-Sysmeta-Alt-Etag')
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(self.manifest_alt_slo_size,
                         int(headers['Content-Length']))
        # N.B. Etag-Is-At only effects conditional matching, not response Etag
        self.assertEqual('"%s"' % self.manifest_alt_slo_etag, headers['Etag'])
        # ... but the response Sysmeta will be available to wrapping middleware
        self.assertEqual('"alt-etag-1"', headers['X-Object-Sysmeta-Alt-Etag'])

        expected_app_calls = [
            ('GET', '/v1/AUTH_test/c/manifest-alt'),
            ('GET', '/v1/AUTH_test/c/alt_00?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/c/alt_01?multipart-manifest=get'),
        ]
        self.assertEqual(self.app.calls, expected_app_calls)
        self.assertEqual(
            self.app.headers[0].get('X-Backend-Etag-Is-At'),
            'X-Object-Sysmeta-Alt-Etag,x-object-sysmeta-slo-etag')
        self.assertNotIn('X-Backend-Etag-Is-At', self.app.headers[1])

    def test_if_match_mismatches_alternate_etag(self):
        req = Request.blank(
            '/v1/AUTH_test/c/manifest-alt',
            headers={'If-Match': self.manifest_alt_slo_etag})
        update_etag_is_at_header(req, 'X-Object-Sysmeta-Alt-Etag')
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '412 Precondition Failed')
        # N.B. Etag-Is-At only effects conditional matching, not response Etag
        self.assertEqual('"%s"' % self.manifest_alt_slo_etag, headers['Etag'])
        # ... but the response Sysmeta will be available to wrapping middleware
        self.assertEqual('"alt-etag-1"', headers['X-Object-Sysmeta-Alt-Etag'])
        # conditional errors are always zero-bytes to save client egress
        self.assertEqual('0', headers['Content-Length'])
        self.assertEqual(b'', body)
        expected_app_calls = [('GET', '/v1/AUTH_test/c/manifest-alt')]
        self.assertEqual(self.app.headers[0].get('X-Backend-Etag-Is-At'),
                         'X-Object-Sysmeta-Alt-Etag,x-object-sysmeta-slo-etag')
        if not self.modern_manifest_headers:
            expected_app_calls.extend([
                # Needed to re-fetch because if-match can't find slo-etag
                ('GET', '/v1/AUTH_test/c/manifest-alt'),
                # We end up validating the first segment
                ('GET', '/v1/AUTH_test/c/alt_00?multipart-manifest=get'),
            ])
            # for legacy manifests we don't know if swob will return a
            # successful response or conditional error so we validate the first
            # segment to avoid a 2XX when we should 5XX, when swob decides to
            # error it closes our SegmentedIterable and we don't drain the
            # (possibly large) segment.
            self.expected_unread_requests[
                ('GET', '/v1/AUTH_test/c/alt_00'
                 '?multipart-manifest=get')] = 1
            for headers in self.app.headers[1:]:
                self.assertNotIn('If-Match', headers)
                self.assertNotIn('X-Backend-Etag-Is-At', headers)
        self.assertEqual(self.app.calls, expected_app_calls)

    def test_manifest_get_if_none_match_matches(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd?multipart-manifest=get',
            headers={'If-None-Match': self.manifest_abcd_json_md5})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '304 Not Modified')
        self.assertEqual(self.manifest_abcd_json_md5, headers['Etag'])
        # conditional errors are always zero-bytes to save client egress
        self.assertEqual('0', headers['Content-Length'])
        self.assertEqual(b'', body)

        expected_app_calls = [('GET', '/v1/AUTH_test/gettest/manifest-abcd'
                               '?multipart-manifest=get')]

        self.assertEqual(self.app.calls, expected_app_calls)
        self.assertNotIn('X-Backend-Etag-Is-At', self.app.headers[0])

    def test_manifest_get_if_none_match_mismatches(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd?multipart-manifest=get',
            headers={'If-None-Match': "not-%s" % self.manifest_abcd_json_md5})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(self.manifest_abcd_json_md5, headers['Etag'])
        self.assertEqual(self.manifest_abcd_json_size,
                         int(headers['Content-Length']))
        data = json.loads(body)
        self.assertEqual(
            ['/gettest/a_5', '/gettest/manifest-bc', '/gettest/d_20'],
            [s['name'] for s in data])
        self.assertEqual(md5hex(body), self.manifest_abcd_json_md5)

        expected_app_calls = [('GET', '/v1/AUTH_test/gettest/manifest-abcd'
                               '?multipart-manifest=get')]
        self.assertEqual(self.app.calls, expected_app_calls)
        self.assertNotIn('X-Backend-Etag-Is-At', self.app.headers[0])

    def test_manifest_get_if_none_match_matches_alternate_etag(self):
        req = Request.blank(
            '/v1/AUTH_test/c/manifest-alt?multipart-manifest=get',
            headers={'If-None-Match': '"alt-etag-1"'})
        # who would do this for a multipart-manifest=get requests?
        update_etag_is_at_header(req, 'X-Object-Sysmeta-Alt-Etag')
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '304 Not Modified')
        # N.B. Etag-Is-At only effects conditional matching, not response Etag
        self.assertEqual(self.manifest_alt_json_md5, headers['Etag'])
        # ... but the response Sysmeta will be available to wrapping middleware
        self.assertEqual('"alt-etag-1"', headers['X-Object-Sysmeta-Alt-Etag'])
        # conditional errors are always zero-bytes to save client egress
        self.assertEqual('0', headers['Content-Length'])
        self.assertEqual(b'', body)

        expected_app_calls = [('GET', '/v1/AUTH_test/c/manifest-alt'
                               '?multipart-manifest=get')]
        self.assertEqual(self.app.calls, expected_app_calls)
        self.assertEqual(
            self.app.headers[0].get('X-Backend-Etag-Is-At'),
            'X-Object-Sysmeta-Alt-Etag')

    def test_manifest_get_if_none_match_mismatches_alternate_etag(self):
        req = Request.blank(
            '/v1/AUTH_test/c/manifest-alt?multipart-manifest=get',
            headers={'If-None-Match': '"not-alt-etag-1"'})
        # who would do this for a multipart-manifest=get requests?
        update_etag_is_at_header(req, 'X-Object-Sysmeta-Alt-Etag')
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        # N.B. Etag-Is-At only effects conditional matching, not response Etag
        self.assertEqual(self.manifest_alt_json_md5, headers['Etag'])
        # ... but the response Sysmeta will be available to wrapping middleware
        self.assertEqual('"alt-etag-1"', headers['X-Object-Sysmeta-Alt-Etag'])
        data = json.loads(body)
        self.assertEqual(['/c/alt_%02d' % i for i in range(len(data))],
                         [s['name'] for s in data])
        self.assertEqual(md5hex(body), self.manifest_alt_json_md5)

        expected_app_calls = [('GET', '/v1/AUTH_test/c/manifest-alt'
                               '?multipart-manifest=get')]
        self.assertEqual(self.app.calls, expected_app_calls)
        self.assertEqual(
            self.app.headers[0].get('X-Backend-Etag-Is-At'),
            'X-Object-Sysmeta-Alt-Etag')

    def test_manifest_get_if_match_matches(self):
        # use if-match condition and expect to match
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd?multipart-manifest=get',
            headers={'If-Match': self.manifest_abcd_json_md5})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(self.manifest_abcd_json_md5, headers['Etag'])
        expected_app_calls = [
            ('GET',
             '/v1/AUTH_test/gettest/manifest-abcd?multipart-manifest=get')]
        self.assertEqual(self.app.calls, expected_app_calls)
        self.assertNotIn('X-Backend-Etag-Is-At', self.app.headers[0])

    def test_manifest_get_if_match_mismatches(self):
        # use if-match condition and expect to mismatch
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd?multipart-manifest=get',
            headers={'If-Match': self.manifest_abcd_slo_etag})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '412 Precondition Failed')
        self.assertEqual(self.manifest_abcd_json_md5, headers['Etag'])
        # conditional errors are always zero-bytes to save client egress
        self.assertEqual('0', headers['Content-Length'])
        self.assertEqual(b'', body)

        expected_app_calls = [
            ('GET',
             '/v1/AUTH_test/gettest/manifest-abcd?multipart-manifest=get')]
        self.assertEqual(self.app.calls, expected_app_calls)
        self.assertNotIn('X-Backend-Etag-Is-At', self.app.headers[0])

    def test_manifest_get_if_match_matches_alternate_etag(self):
        # use if-match condition with alt-etag and expect to match
        req = Request.blank(
            '/v1/AUTH_test/c/manifest-alt?multipart-manifest=get',
            headers={'If-Match': '"alt-etag-1"'})
        update_etag_is_at_header(req, 'X-Object-Sysmeta-Alt-Etag')
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(self.manifest_alt_json_md5, headers['Etag'])
        expected_app_calls = [
            ('GET', '/v1/AUTH_test/c/manifest-alt?multipart-manifest=get')]
        self.assertEqual(self.app.calls, expected_app_calls)
        self.assertEqual('X-Object-Sysmeta-Alt-Etag',
                         self.app.headers[0]['X-Backend-Etag-Is-At'])

    def test_manifest_get_if_match_mismatches_alternate_etag(self):
        # mis-match alternate etag
        req = Request.blank(
            '/v1/AUTH_test/c/manifest-alt?multipart-manifest=get',
            headers={'If-Match': self.manifest_alt_json_md5})
        update_etag_is_at_header(req, 'X-Object-Sysmeta-Alt-Etag')
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '412 Precondition Failed')
        self.assertEqual(self.manifest_alt_json_md5, headers['Etag'])
        # conditional errors are always zero-bytes to save client egress
        self.assertEqual('0', headers['Content-Length'])
        self.assertEqual(b'', body)

        expected_app_calls = [
            ('GET', '/v1/AUTH_test/c/manifest-alt?multipart-manifest=get')]
        self.assertEqual(self.app.calls, expected_app_calls)
        self.assertEqual('X-Object-Sysmeta-Alt-Etag',
                         self.app.headers[0].get('X-Backend-Etag-Is-At'))

    def test_manifest_get_if_match_mismatches_without_alternate_etag(self):
        # sanity, this is similar to the test_manifest_get_if_match_mismatches
        # but in this case our manifest *has* an alt-etag, but no-one tells the
        # object server to look for it
        req = Request.blank(
            '/v1/AUTH_test/c/manifest-alt?multipart-manifest=get',
            headers={'If-Match': '"alt-etag-1"'})
        # update_etag_is_at_header(req, 'X-Object-Sysmeta-Alt-Etag')
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '412 Precondition Failed')
        self.assertEqual(self.manifest_alt_json_md5, headers['Etag'])
        # conditional errors are always zero-bytes to save client egress
        self.assertEqual('0', headers['Content-Length'])
        self.assertEqual(b'', body)

        expected_app_calls = [
            ('GET', '/v1/AUTH_test/c/manifest-alt?multipart-manifest=get')]
        self.assertEqual(self.app.calls, expected_app_calls)
        self.assertNotIn('X-Backend-Etag-Is-At', self.app.headers[0])

    def test_manifest_get_if_match_mismatches_alternate_etag_miss(self):
        # sanity, this is similar to
        # test_manifest_get_if_match_mismatches_alternate_etag but in this case
        # our manifest doesn't HAVE an alt-etag, so the object server falls
        # back to match with manifest's json_md5
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd?multipart-manifest=get',
            headers={'If-Match': self.manifest_abcd_json_md5})
        update_etag_is_at_header(req, 'X-Object-Sysmeta-Alt-Etag')
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')

    def test_if_match_matches_alternate_etag_non_slo(self):
        # match alternate etag
        req = Request.blank(
            '/v1/AUTH_test/c/alt_00',
            headers={'If-Match': 'seg-etag-00'})
        update_etag_is_at_header(req, 'X-Object-Sysmeta-Alt-Etag')
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '200 OK')
        # N.B. Etag-Is-At only effects conditional matching, not response Etag
        self.assertEqual(md5hex('alt_00' * 5), headers['Etag'])
        # ... but the response Sysmeta will be available to wrapping middleware
        self.assertEqual('seg-etag-00', headers['X-Object-Sysmeta-Alt-Etag'])
        expected_calls = [
            ('GET', '/v1/AUTH_test/c/alt_00'),
        ]
        self.assertEqual(self.app.calls, expected_calls)

    def test_if_match_mismatches_alternate_etag_non_slo(self):
        # mis-match alternate etag
        req = Request.blank(
            '/v1/AUTH_test/c/alt_00',
            headers={'If-Match': md5hex(b'alt_00' * 5)})
        update_etag_is_at_header(req, 'X-Object-Sysmeta-Alt-Etag')
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '412 Precondition Failed')
        # N.B. Etag-Is-At only effects conditional matching, not response Etag
        self.assertEqual(md5hex('alt_00' * 5), headers['Etag'])
        # ... but the response Sysmeta will be available to wrapping middleware
        self.assertEqual('seg-etag-00', headers['X-Object-Sysmeta-Alt-Etag'])
        expected_calls = [
            ('GET', '/v1/AUTH_test/c/alt_00'),
        ]
        self.assertEqual(self.app.calls, expected_calls)

    def test_if_match_matches_alternate_etag_non_slo_after_refetch(self):
        self.app.register_next_response(
            'GET', '/v1/AUTH_test/c/manifest-alt',
            swob.HTTPOk, {'Content-Length': '25',
                          'Etag': md5hex('alt_1' * 5),
                          # N.B. manifest-alt gets overwritten mid-flight!
                          'X-Backend-Timestamp': '2345',
                          'X-Object-Sysmeta-Alt-Etag': 'alt-object-etag'},
            body=b'alt_1' * 5)

        req = Request.blank(
            '/v1/AUTH_test/c/manifest-alt',
            headers={'If-Match': 'alt-object-etag'})
        update_etag_is_at_header(req, 'X-Object-Sysmeta-Alt-Etag')
        status, headers, body = self.call_slo(req)

        expected_app_calls = [('GET', '/v1/AUTH_test/c/manifest-alt')]
        # first request asks for match on alt-etag
        self.assertEqual('alt-object-etag', self.app.headers[0]['If-Match'])
        self.assertIn('X-Object-Sysmeta-Alt-Etag',
                      self.app.headers[0]['X-Backend-Etag-Is-At'])

        if self.modern_manifest_headers:
            # and since the response includes modern sysmeta, slo trusts the
            # 412 w/o refetch
            self.assertEqual(status, '412 Precondition Failed')
            # N.B. if the first repsonse had included a matching
            # alt-object-etag in sysmeta we would have returned 200, see
            # test_if_match_matches_alternate_etag with "alt-etag-1"
            self.assertEqual('"%s"' % self.manifest_alt_slo_etag,
                             headers['Etag'])
            self.assertEqual('"alt-etag-1"',
                             headers['X-Object-Sysmeta-Alt-Etag'])
        else:
            # ... but lacking modern sysmeta, slo will refetch a 412
            expected_app_calls.append(
                ('GET', '/v1/AUTH_test/c/manifest-alt')
            )
            # ... w/o conditionals
            self.assertNotIn('If-Match', self.app.headers[1])
            self.assertNotIn('X-Backend-Etag-Is-At', self.app.headers[1])
            # and the reconstructed swob response will *match*
            self.assertEqual(status, '200 OK')
            # N.B. Etag-Is-At only effects conditional matching,
            # not response Etag
            self.assertEqual(md5hex('alt_1' * 5), headers['Etag'])
            # ... but the response Sysmeta will be available to
            # wrapping middleware
            self.assertEqual('alt-object-etag',
                             headers['X-Object-Sysmeta-Alt-Etag'])

        self.assertEqual(self.app.calls, expected_app_calls)

    def test_if_match_mismatches_alternate_etag_non_slo_after_refetch(self):
        self.app.register_next_response(
            'GET', '/v1/AUTH_test/c/manifest-alt',
            swob.HTTPOk, {'Content-Length': '25',
                          'Etag': md5hex('alt_1' * 5),
                          # N.B. manifest-alt gets overwritten mid-flight!
                          'X-Backend-Timestamp': '2345',
                          'X-Object-Sysmeta-Alt-Etag': 'alt-object-etag'},
            body=b'alt_1' * 5)

        req = Request.blank(
            '/v1/AUTH_test/c/manifest-alt',
            headers={'If-Match': md5hex('alt_1' * 5)})
        update_etag_is_at_header(req, 'X-Object-Sysmeta-Alt-Etag')
        status, headers, body = self.call_slo(req)

        expected_app_calls = [('GET', '/v1/AUTH_test/c/manifest-alt')]
        # first request asks for (mis)match on alt-etag
        self.assertEqual(md5hex('alt_1' * 5), self.app.headers[0]['If-Match'])
        self.assertIn('X-Object-Sysmeta-Alt-Etag',
                      self.app.headers[0]['X-Backend-Etag-Is-At'])

        if self.modern_manifest_headers:
            # and since the response includes modern sysmeta, slo trusts the
            # 412 w/o refetch
            self.assertEqual(status, '412 Precondition Failed')
            # N.B. the first repsonse included an alt-etag in sysmeta (i.e.
            # "alt-etag-1"), it just doesn't match either - see
            # test_if_match_mismatches_alternate_etag
            self.assertEqual('"%s"' % self.manifest_alt_slo_etag,
                             headers['Etag'])
            self.assertEqual('"alt-etag-1"',
                             headers['X-Object-Sysmeta-Alt-Etag'])
        else:
            # ... but lacking modern sysmeta, slo will refetch a 412
            expected_app_calls.append(
                ('GET', '/v1/AUTH_test/c/manifest-alt')
            )
            # ... w/o conditionals
            self.assertNotIn('If-Match', self.app.headers[1])
            self.assertNotIn('X-Backend-Etag-Is-At', self.app.headers[1])
            # and the reconstructed swob response will *not* match
            self.assertEqual(status, '412 Precondition Failed')
            # N.B. Etag-Is-At only effects conditional matching,
            # not response Etag
            self.assertEqual(md5hex('alt_1' * 5), headers['Etag'])
            # ... but the response Sysmeta will be available to
            # wrapping middleware
            self.assertEqual('alt-object-etag',
                             headers['X-Object-Sysmeta-Alt-Etag'])
            # swob is converting the successful non-slo response to conditional
            # error and closing our unconditionally refetched resp_iter
            self.expected_unread_requests[
                ('GET', '/v1/AUTH_test/c/manifest-alt')] = 1

        self.assertEqual(self.app.calls, expected_app_calls)

    def test_if_match_matches_and_range(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'If-Match': self.manifest_abcd_slo_etag,
                     'Range': 'bytes=3-6'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '206 Partial Content')
        self.assertIn('bytes 3-6/50', headers['Content-Range'])
        self.assertEqual('"%s"' % self.manifest_abcd_slo_etag, headers['Etag'])
        self.assertEqual('4', headers['Content-Length'])
        self.assertEqual(body, b'aabb')

        expected_app_calls = [
            ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
        ]
        if not self.modern_manifest_headers:
            # Needed to re-fetch because if-match can't find slo-etag
            expected_app_calls.append(
                ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
            )
        # and then fetch the segments
        expected_app_calls.extend([
            ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
            ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
        ])
        self.assertEqual(self.app.calls, expected_app_calls)
        self.assertEqual(self.app.headers[0].get('X-Backend-Etag-Is-At'),
                         'x-object-sysmeta-slo-etag')

    def test_old_swift_if_match_matches_and_range(self):
        self.app.can_ignore_range = False
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            headers={'If-Match': self.manifest_abcd_slo_etag,
                     'Range': 'bytes=3-6'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual('"%s"' % self.manifest_abcd_slo_etag, headers['Etag'])
        self.assertEqual('4', headers['Content-Length'])
        self.assertEqual(body, b'aabb')

        expected_app_calls = [
            ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
            # new-sytle manifest sysmeta was added 2016, but ignore-range
            # didn't get added until 2020, so both new and old manifest
            # will still require refetch with old-swift
            ('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
            ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
            ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
        ]
        self.assertEqual(self.app.calls, expected_app_calls)
        self.assertEqual(self.app.headers[0].get('X-Backend-Etag-Is-At'),
                         'x-object-sysmeta-slo-etag')

    def test_range_resume_download(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=20-'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(body, b'ccccccccccdddddddddddddddddddd')

    def test_get_with_if_modified_since(self):
        req = swob.Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'If-Modified-Since': 'Wed, 12 Feb 2014 22:24:52 GMT',
                     'If-Unmodified-Since': 'Thu, 13 Feb 2014 23:25:53 GMT'})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Etag'], '"%s"' % self.manifest_abcd_slo_etag)
        self.assertEqual(int(headers['Content-Length']),
                         self.manifest_abcd_slo_size)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_abcd_json_md5)
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(self.slo.logger.get_lines_for_level('error'), [])

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/gettest/manifest-abcd'),
             ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
             ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get')])

        # It's important not to pass the If-[Un]Modified-Since header to the
        # proxy for segment or submanifest GET requests, as it may result in
        # 304 Not Modified responses, and those don't contain any useful data.
        for _, _, hdrs in self.app.calls_with_headers[1:]:
            self.assertNotIn('If-Modified-Since', hdrs)
            self.assertNotIn('If-Unmodified-Since', hdrs)

    def test_if_modified_since_ancient_date(self):
        req = swob.Request.blank(
            '/v1/AUTH_test/c/manifest-last-modified',
            headers={
                'If-Modified-Since': 'Fri, 01 Feb 2012 20:38:36 GMT',
            })
        status, headers, body = self.call_slo(req)
        # oh it's *definately* been modified since then!
        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['Etag'],
                         '"%s"' % self.manifest_last_modified_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_last_modified_json_md5)
        self.assertEqual(int(headers['Content-Length']),
                         self.manifest_last_modified_slo_size)
        self.assertEqual(headers['Last-Modified'],
                         'Mon, 23 Oct 2023 10:05:32 GMT')
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/c/manifest-last-modified'),
            ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
        ])

    def test_if_modified_since_last_modified(self):
        req = swob.Request.blank(
            '/v1/AUTH_test/c/manifest-last-modified',
            headers={
                'If-Modified-Since': 'Mon, 23 Oct 2023 10:05:32 GMT',
            })
        status, headers, body = self.call_slo(req)
        # nope, that was the last time it was changed
        self.assertEqual(status, '304 Not Modified')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['Etag'],
                         '"%s"' % self.manifest_last_modified_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_last_modified_json_md5)
        self.assertEqual(headers['Content-Length'], '0')
        self.assertEqual('Mon, 23 Oct 2023 10:05:32 GMT',
                         headers['Last-Modified'])
        self.assertEqual(b'', body)
        expected_calls = [
            ('GET', '/v1/AUTH_test/c/manifest-last-modified'),
        ]
        if not self.modern_manifest_headers:
            # N.B. legacy manifests must refetch for accurate Etag, and then we
            # validate first segment before lettting swob return the error
            expected_calls.extend([
                ('GET', '/v1/AUTH_test/c/manifest-last-modified'),
                ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
            ])
            # we don't drain the segment's resp_iter if validation fails
            self.expected_unread_requests[
                ('GET', '/v1/AUTH_test/gettest/a_5'
                 '?multipart-manifest=get')] = 1
        self.assertEqual(self.app.calls, expected_calls)

    def test_if_modified_since_now(self):
        now = datetime.now()
        last_modified = now.strftime("%a, %d %b %Y %H:%M:%S %Z")
        req = swob.Request.blank(
            '/v1/AUTH_test/c/manifest-last-modified',
            headers={
                'If-Modified-Since': last_modified,
            })
        status, headers, body = self.call_slo(req)
        # nope, that was the last time it was changed
        self.assertEqual(status, '304 Not Modified')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['Etag'],
                         '"%s"' % self.manifest_last_modified_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_last_modified_json_md5)
        self.assertEqual(headers['Content-Length'], '0')
        self.assertEqual('Mon, 23 Oct 2023 10:05:32 GMT',
                         headers['Last-Modified'])
        self.assertEqual(b'', body)
        expected_calls = [
            ('GET', '/v1/AUTH_test/c/manifest-last-modified'),
        ]
        if not self.modern_manifest_headers:
            # N.B. legacy manifests must refetch for accurate Etag, and then we
            # validate first segment before lettting swob return the error
            expected_calls.extend([
                ('GET', '/v1/AUTH_test/c/manifest-last-modified'),
                ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
            ])
            # we don't drain the segment's resp_iter if validation fails
            self.expected_unread_requests[
                ('GET', '/v1/AUTH_test/gettest/a_5'
                 '?multipart-manifest=get')] = 1
        self.assertEqual(self.app.calls, expected_calls)

    def test_if_unmodified_since_ancient_date(self):
        req = swob.Request.blank(
            '/v1/AUTH_test/c/manifest-last-modified',
            headers={
                'If-Unmodified-Since': 'Fri, 01 Feb 2012 20:38:36 GMT',
            })
        status, headers, body = self.call_slo(req)
        # oh it's *definately* been modified since then!
        self.assertEqual(status, '412 Precondition Failed')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['Etag'],
                         '"%s"' % self.manifest_last_modified_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_last_modified_json_md5)
        self.assertEqual(headers['Content-Length'], '0')
        self.assertEqual('Mon, 23 Oct 2023 10:05:32 GMT',
                         headers['Last-Modified'])
        self.assertEqual(b'', body)
        expected_calls = [
            ('GET', '/v1/AUTH_test/c/manifest-last-modified'),
        ]
        if not self.modern_manifest_headers:
            # N.B. legacy manifests must refetch for accurate Etag, and then we
            # validate first segment before lettting swob return the error
            expected_calls.extend([
                ('GET', '/v1/AUTH_test/c/manifest-last-modified'),
                ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
            ])
            # we don't drain the segment's resp_iter if validation fails
            self.expected_unread_requests[
                ('GET', '/v1/AUTH_test/gettest/a_5'
                 '?multipart-manifest=get')] = 1
        self.assertEqual(self.app.calls, expected_calls)

    def test_if_unmodified_since_last_modified(self):
        req = swob.Request.blank(
            '/v1/AUTH_test/c/manifest-last-modified',
            headers={
                'If-Unmodified-Since': 'Mon, 23 Oct 2023 10:05:32 GMT',
            })
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['Etag'],
                         '"%s"' % self.manifest_last_modified_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_last_modified_json_md5)
        self.assertEqual(int(headers['Content-Length']),
                         self.manifest_last_modified_slo_size)
        self.assertEqual(headers['Last-Modified'],
                         'Mon, 23 Oct 2023 10:05:32 GMT')
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/c/manifest-last-modified'),
            ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
        ])

    def test_if_unmodified_since_now(self):
        now = datetime.now()
        last_modified = now.strftime("%a, %d %b %Y %H:%M:%S %Z")
        req = swob.Request.blank(
            '/v1/AUTH_test/c/manifest-last-modified',
            headers={
                'If-Unmodified-Since': last_modified,
            })
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['Etag'],
                         '"%s"' % self.manifest_last_modified_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_last_modified_json_md5)
        self.assertEqual(int(headers['Content-Length']),
                         self.manifest_last_modified_slo_size)
        self.assertEqual(headers['Last-Modified'],
                         'Mon, 23 Oct 2023 10:05:32 GMT')
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/c/manifest-last-modified'),
            ('GET', '/v1/AUTH_test/gettest/a_5?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
        ])


class TestSloConditionalGetNewManifest(TestSloConditionalGetOldManifest):

    modern_manifest_headers = True


class TestPartNumber(SloGETorHEADTestCase):

    modern_manifest_headers = True

    def setUp(self):
        super(TestPartNumber, self).setUp()
        self._setup_alphabet_objects('bcdj')
        self._setup_manifest_bc()
        self._setup_manifest_abcd()
        self._setup_manifest_abcdefghijkl()
        self._setup_manifest_bc_ranges()
        self._setup_manifest_abcd_ranges()
        self._setup_manifest_abcd_subranges()
        self._setup_manifest_aabbccdd()
        self._setup_manifest_single_segment()
        self._setup_manifest_zero_byte()
        self._setup_manifest_bc_expires()

        # this b_50 object doesn't follow the alphabet convention
        self.app.register(
            'GET', '/v1/AUTH_test/gettest/b_50',
            swob.HTTPPartialContent, {'Content-Length': '50',
                                      'Etag': md5hex('b' * 50)},
            'b' * 50)

        # Setup POST req separately for expiring manifest
        self.app.register('POST',
                          '/v1/AUTH_test/gettest/manifest-bc-expires',
                          swob.HTTPAccepted, {})

        self._setup_manifest_data()

    def test_head_part_number(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-bc?part-number=1',
            environ={'REQUEST_METHOD': 'HEAD'})
        status, headers, body = self.call_slo(req)
        expected_calls = [
            ('HEAD', '/v1/AUTH_test/gettest/manifest-bc?part-number=1'),
            ('GET', '/v1/AUTH_test/gettest/manifest-bc?part-number=1')
        ]

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Etag'], '"%s"' % self.manifest_bc_slo_etag)
        self.assertEqual(headers['Content-Length'], '10')
        self.assertEqual(headers['Content-Range'], 'bytes 0-9/25')
        self.assertEqual(headers['X-Manifest-Etag'], self.manifest_bc_json_md5)
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Parts-Count'], '2')
        self.assertEqual(headers['Content-Type'], 'application/octet-stream')
        self.assertEqual(body, b'')  # it's a HEAD request, after all
        self.assertEqual(headers['X-Object-Meta-Plant'], 'Ficus')
        self.assertEqual(self.app.calls, expected_calls)

    def test_head_part_number_refetch_path(self):
        # verify that any modification of the request path by a downstream
        # middleware is ignored when refetching
        req = Request.blank(
            '/v1/AUTH_test/gettest/mani?part-number=1',
            environ={'REQUEST_METHOD': 'HEAD'})
        captured_calls = []
        orig_call = FakeSwift.__call__

        def pseudo_middleware(app, env, start_response):
            captured_calls.append((env['REQUEST_METHOD'], env['PATH_INFO']))
            # pretend another middleware modified the path
            # note: for convenience, the path "modification" actually results
            # in one of the pre-registered paths
            env['PATH_INFO'] += 'fest-bc'
            return orig_call(app, env, start_response)

        with patch.object(FakeSwift, '__call__', pseudo_middleware):
            status, headers, body = self.call_slo(req)

        # pseudo-middleware gets the original path for the refetch
        self.assertEqual([('HEAD', '/v1/AUTH_test/gettest/mani'),
                          ('GET', '/v1/AUTH_test/gettest/mani')],
                         captured_calls)
        self.assertEqual(status, '206 Partial Content')
        expected_calls = [
            # original path is modified...
            ('HEAD', '/v1/AUTH_test/gettest/manifest-bc?part-number=1'),
            # refetch: the *original* path is modified...
            ('GET', '/v1/AUTH_test/gettest/manifest-bc?part-number=1')
        ]
        self.assertEqual(self.app.calls, expected_calls)

    def test_get_manifest_with_x_open_expired_part_num(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-bc-expires'
            '?multipart-manifest=get',
            environ={'REQUEST_METHOD': 'GET'})

        captured_calls = []
        orig_call = FakeSwift.__call__

        def pseudo_middleware(app, env, start_response):
            captured_calls.append((env['REQUEST_METHOD'], env['PATH_INFO']))
            # pretend another middleware modified the path
            # note: for convenience, the path "modification" actually results
            # in one of the pre-registered paths
            env['PATH_INFO'] += ''
            return orig_call(app, env, start_response)

        with patch.object(FakeSwift, '__call__', pseudo_middleware):
            status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual([('GET',
                           '/v1/AUTH_test/gettest/manifest-bc-expires')],
                         captured_calls)

        t = str(int(time.time()))
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-bc-expires',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Delete-At': t}
        )

        with patch.object(FakeSwift, '__call__', pseudo_middleware):
            status, headers, body = self.call_slo(req)

        self.assertEqual(status, '202 Accepted')

        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-bc-expires?part-number=1',
            environ={'REQUEST_METHOD': 'HEAD'},
            headers={'x-open-expired': 'true'})

        with patch.object(FakeSwift, '__call__', pseudo_middleware):
            status, headers, body = self.call_slo(req)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['Etag'], '"%s"' %
                         self.manifest_bc_expires_slo_etag)
        self.assertEqual(self.app.call_count, 4)
        self.assertTrue(self.app.calls_with_headers[2][2]['X-Open-Expired'])

    def test_get_part_number(self):
        # part number 1 is b_10
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-bc?part-number=1')
        status, headers, body = self.call_slo(req)
        expected_calls = [
            ('GET', '/v1/AUTH_test/gettest/manifest-bc?part-number=1'),
            ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get')
        ]

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Etag'], '"%s"' % self.manifest_bc_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'], self.manifest_bc_json_md5)
        self.assertEqual(headers['Content-Length'], '10')
        self.assertEqual(headers['Content-Range'], 'bytes 0-9/25')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Parts-Count'], '2')
        self.assertEqual(headers['Content-Type'], 'application/octet-stream')
        self.assertEqual(body, b'b' * 10)
        self.assertEqual(headers['X-Object-Meta-Plant'], 'Ficus')
        self.assertEqual(self.app.calls, expected_calls)

        # part number 2 is c_15
        self.app.clear_calls()
        expected_calls = [
            ('GET', '/v1/AUTH_test/gettest/manifest-bc?part-number=2'),
            ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get')
        ]
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-bc?part-number=2')
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Etag'], '"%s"' % self.manifest_bc_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'], self.manifest_bc_json_md5)
        self.assertEqual(headers['Content-Length'], '15')
        self.assertEqual(headers['Content-Range'], 'bytes 10-24/25')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Parts-Count'], '2')
        self.assertEqual(headers['Content-Type'], 'application/octet-stream')
        self.assertEqual(body, b'c' * 15)
        self.assertEqual(headers['X-Object-Meta-Plant'], 'Ficus')
        self.assertEqual(self.app.calls, expected_calls)

        # we now test it with single segment slo
        self.app.clear_calls()
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-single-segment?part-number=1')
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Etag'], '"%s"' %
                         self.manifest_single_segment_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_single_segment_json_md5)
        self.assertEqual(headers['Content-Length'], '50')
        self.assertEqual(headers['Content-Range'], 'bytes 0-49/50')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Object-Meta-Nature'], 'Regular')
        self.assertEqual(headers['X-Parts-Count'], '1')
        self.assertEqual(headers['Content-Type'], 'application/octet-stream')
        expected_calls = [
            ('GET', '/v1/AUTH_test/gettest/manifest-single-segment?'
                    'part-number=1'),
            ('GET', '/v1/AUTH_test/gettest/b_50?multipart-manifest=get')
        ]
        self.assertEqual(self.app.calls, expected_calls)

    def test_get_part_number_sub_slo(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd?part-number=3')
        status, headers, body = self.call_slo(req)
        expected_calls = [
            ('GET', '/v1/AUTH_test/gettest/manifest-abcd?part-number=3'),
            ('GET', '/v1/AUTH_test/gettest/d_20?multipart-manifest=get')
        ]

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Etag'], '"%s"' % self.manifest_abcd_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_abcd_json_md5)
        self.assertEqual(headers['Content-Length'], '20')
        self.assertEqual(headers['Content-Range'], 'bytes 30-49/50')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Parts-Count'], '3')
        self.assertEqual(headers['Content-Type'], 'application/json')
        self.assertEqual(body, b'd' * 20)
        self.assertEqual(self.app.calls, expected_calls)

        self.app.clear_calls()
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd?part-number=2')
        status, headers, body = self.call_slo(req)
        expected_calls = [
            ('GET', '/v1/AUTH_test/gettest/manifest-abcd?part-number=2'),
            ('GET', '/v1/AUTH_test/gettest/manifest-bc'),
            ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get')
        ]

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Etag'], '"%s"' % self.manifest_abcd_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_abcd_json_md5)
        self.assertEqual(headers['Content-Length'], '25')
        self.assertEqual(headers['Content-Range'], 'bytes 5-29/50')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Parts-Count'], '3')
        self.assertEqual(headers['Content-Type'], 'application/json')
        self.assertEqual(body, b'b' * 10 + b'c' * 15)
        self.assertEqual(self.app.calls, expected_calls)

    def test_get_part_number_large_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcdefghijkl?part-number=10')
        status, headers, body = self.call_slo(req)
        expected_calls = [
            ('GET', '/v1/AUTH_test/gettest/manifest-abcdefghijkl?'
                    'part-number=10'),
            ('GET', '/v1/AUTH_test/gettest/j_50?multipart-manifest=get')
        ]

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Etag'], '"%s"' %
                         self.manifest_abcdefghijkl_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_abcdefghijkl_json_md5)
        self.assertEqual(headers['Content-Length'], '50')
        self.assertEqual(headers['Content-Range'], 'bytes 225-274/390')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Parts-Count'], '12')
        self.assertEqual(headers['Content-Type'], 'application/octet-stream')
        self.assertEqual(body, b'j' * 50)
        self.assertEqual(self.app.calls, expected_calls)

    def test_part_number_with_range_segments(self):
        req = Request.blank('/v1/AUTH_test/gettest/manifest-bc-ranges',
                            params={'part-number': 1})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Etag'], '"%s"' %
                         self.manifest_bc_ranges_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_bc_ranges_json_md5)
        self.assertEqual(headers['Content-Length'], '4')
        self.assertEqual(headers['Content-Range'],
                         'bytes 0-3/%s' % self.manifest_bc_ranges_slo_size)
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Parts-Count'], '4')
        self.assertEqual(body, b'b' * 4)
        expected_calls = [
            ('GET', '/v1/AUTH_test/gettest/manifest-bc-ranges?part-number=1'),
            ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get')
        ]
        self.assertEqual(self.app.calls, expected_calls)
        # since the our requested part-number is range-segment we expect Range
        # header on b_10 segment subrequest
        self.assertEqual('bytes=4-7',
                         self.app.call_list[1].headers['Range'])

    def test_part_number_sub_ranges_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd-subranges?part-number=3')

        status, headers, body = self.call_slo(req)
        expected_calls = [
            ('GET', '/v1/AUTH_test/gettest/manifest-abcd-subranges?'
                    'part-number=3'),
            ('GET', '/v1/AUTH_test/gettest/manifest-abcd-ranges'),
            ('GET', '/v1/AUTH_test/gettest/manifest-bc-ranges'),
            ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get'),
            ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get')
        ]

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Etag'], '"%s"' %
                         self.manifest_abcd_subranges_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_abcd_subranges_json_md5)
        self.assertEqual(headers['Content-Length'], '5')
        self.assertEqual(headers['Content-Range'], 'bytes 6-10/17')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Parts-Count'], '5')
        self.assertEqual(headers['Content-Type'], 'application/json')
        self.assertEqual(body, b'c' * 2 + b'b' * 3)
        self.assertEqual(self.app.calls, expected_calls)

    def test_get_part_num_with_repeated_segments(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-aabbccdd?part-number=3',
            environ={'REQUEST_METHOD': 'GET'})

        status, headers, body = self.call_slo(req)
        expected_calls = [
            ('GET', '/v1/AUTH_test/gettest/manifest-aabbccdd?part-number=3'),
            ('GET', '/v1/AUTH_test/gettest/b_10?multipart-manifest=get')
        ]

        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Etag'], '"%s"' %
                         self.manifest_aabbccdd_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_aabbccdd_json_md5)
        self.assertEqual(headers['Content-Length'], '10')
        self.assertEqual(headers['Content-Range'], 'bytes 10-19/100')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Parts-Count'], '8')
        self.assertEqual(headers['Content-Type'], 'application/octet-stream')
        self.assertEqual(body, b'b' * 10)
        self.assertEqual(self.app.calls, expected_calls)

    def test_part_number_zero_invalid(self):
        # part-number query param is 1-indexed, part-number=0 is no joy
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-bc?part-number=0')
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '400 Bad Request')
        self.assertNotIn('Content-Range', headers)
        self.assertNotIn('Etag', headers)
        self.assertNotIn('X-Static-Large-Object', headers)
        self.assertNotIn('X-Parts-Count', headers)
        self.assertEqual(body,
                         b'Part number must be an integer greater than 0')
        expected_calls = [
            ('GET', '/v1/AUTH_test/gettest/manifest-bc?part-number=0')
        ]
        self.assertEqual(expected_calls, self.app.calls)

        self.app.clear_calls()
        self.slo.max_manifest_segments = 3999
        req = Request.blank('/v1/AUTH_test/gettest/manifest-bc',
                            params={'part-number': 0})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '400 Bad Request')
        self.assertNotIn('Content-Range', headers)
        self.assertNotIn('Etag', headers)
        self.assertNotIn('X-Static-Large-Object', headers)
        self.assertNotIn('X-Parts-Count', headers)
        self.assertEqual(body,
                         b'Part number must be an integer greater than 0')
        self.assertEqual(expected_calls, self.app.calls)

    def test_head_part_number_zero_invalid(self):
        # you can HEAD part-number=0 either
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-bc', method='HEAD',
            params={'part-number': 0})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '400 Bad Request')
        self.assertNotIn('Content-Range', headers)
        self.assertNotIn('Etag', headers)
        self.assertNotIn('X-Static-Large-Object', headers)
        self.assertNotIn('X-Parts-Count', headers)
        self.assertEqual(body, b'')  # HEAD response, makes sense
        expected_calls = [
            ('HEAD', '/v1/AUTH_test/gettest/manifest-bc?part-number=0')
        ]
        self.assertEqual(expected_calls, self.app.calls)

    def test_part_number_zero_byte_manifest(self):
        part_num = 1
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-zero-byte?'
            'partNumber=%s' % part_num,
            method='HEAD')
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Etag'],
                         '"%s"' % self.manifest_zero_byte_slo_etag)
        self.assertEqual(headers['Content-Length'], '0')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_zero_byte_json_md5)
        self.assertEqual(body, b'')  # it's a HEAD request, after all

        expected_app_calls = [('HEAD',
                               '/v1/AUTH_test/gettest/manifest-zero-byte?'
                               'partNumber=%s' % part_num)]
        if not self.modern_manifest_headers:
            expected_app_calls.append((
                'GET',
                '/v1/AUTH_test/gettest/manifest-zero-byte?'
                'partNumber=%s' % part_num))
        self.assertEqual(self.app.calls, expected_app_calls)

    def test_part_number_zero_invalid_on_subrange(self):
        # either manifest, doesn't matter, part-number=0 is always invalid
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd-subranges?part-number=0')
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '400 Bad Request')
        self.assertNotIn('Content-Range', headers)
        self.assertNotIn('Etag', headers)
        self.assertNotIn('X-Static-Large-Object', headers)
        self.assertNotIn('X-Parts-Count', headers)
        self.assertEqual(body,
                         b'Part number must be an integer greater than 0')
        expected_calls = [
            ('GET',
             '/v1/AUTH_test/gettest/manifest-abcd-subranges?part-number=0')
        ]
        self.assertEqual(expected_calls, self.app.calls)

    def test_negative_part_number_invalid(self):
        # negative numbers are never any good
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-bc?part-number=-1')
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '400 Bad Request')
        self.assertNotIn('Content-Range', headers)
        self.assertNotIn('Etag', headers)
        self.assertNotIn('X-Static-Large-Object', headers)
        self.assertNotIn('X-Parts-Count', headers)
        self.assertEqual(body,
                         b'Part number must be an integer greater than 0')
        expected_calls = [
            ('GET', '/v1/AUTH_test/gettest/manifest-bc?part-number=-1')
        ]
        self.assertEqual(expected_calls, self.app.calls)

    def test_head_negative_part_number_invalid_on_subrange(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd-subranges', method='HEAD',
            params={'part-number': '-1'})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '400 Bad Request')
        self.assertNotIn('Content-Range', headers)
        self.assertNotIn('Etag', headers)
        self.assertNotIn('X-Static-Large-Object', headers)
        self.assertNotIn('X-Parts-Count', headers)
        self.assertEqual(body, b'')
        expected_calls = [
            ('HEAD',
             '/v1/AUTH_test/gettest/manifest-abcd-subranges?part-number=-1')
        ]
        self.assertEqual(expected_calls, self.app.calls)

    def test_head_non_integer_part_number_invalid(self):
        # some kind of string is bad too
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-bc', method='HEAD',
            params={'part-number': 'foo'})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '400 Bad Request')
        self.assertEqual(body, b'')
        expected_calls = [
            ('HEAD', '/v1/AUTH_test/gettest/manifest-bc?part-number=foo')
        ]
        self.assertEqual(expected_calls, self.app.calls)

    def test_get_non_integer_part_number_invalid(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-bc', params={'part-number': 'foo'})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '400 Bad Request')
        self.assertNotIn('Content-Range', headers)
        self.assertNotIn('Etag', headers)
        self.assertNotIn('X-Static-Large-Object', headers)
        self.assertNotIn('X-Parts-Count', headers)
        self.assertEqual(body, b'Part number must be an integer greater'
                               b' than 0')
        expected_calls = [
            ('GET', '/v1/AUTH_test/gettest/manifest-bc?part-number=foo')
        ]
        self.assertEqual(expected_calls, self.app.calls)

    def test_get_out_of_range_part_number(self):
        # you can't go past the actual number of parts either
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-bc?part-number=4')
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '416 Requested Range Not Satisfiable')
        self.assertEqual(headers['Content-Range'],
                         'bytes */%d' % self.manifest_bc_slo_size)
        self.assertEqual(headers['Etag'], '"%s"' % self.manifest_bc_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'], self.manifest_bc_json_md5)
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Parts-Count'], '2')
        self.assertEqual(int(headers['Content-Length']), len(body))
        self.assertEqual(body, b'The requested part number is not '
                               b'satisfiable')
        self.assertEqual(headers['X-Object-Meta-Plant'], 'Ficus')
        expected_app_calls = [
            ('GET', '/v1/AUTH_test/gettest/manifest-bc?part-number=4'),
        ]
        self.assertEqual(self.app.calls, expected_app_calls)

        self.app.clear_calls()
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-single-segment?part-number=2')
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '416 Requested Range Not Satisfiable')
        self.assertEqual(headers['Content-Range'],
                         'bytes */%d' % self.manifest_single_segment_slo_size)
        self.assertEqual(int(headers['Content-Length']), len(body))
        self.assertEqual(headers['Etag'],
                         '"%s"' % self.manifest_single_segment_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_single_segment_json_md5)
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Parts-Count'], '1')
        self.assertEqual(body, b'The requested part number is not '
                               b'satisfiable')
        self.assertEqual(headers['X-Object-Meta-Nature'], 'Regular')
        expected_app_calls = [
            ('GET', '/v1/AUTH_test/gettest/manifest-single-segment?'
                    'part-number=2'),
        ]
        self.assertEqual(self.app.calls, expected_app_calls)

    def test_head_out_of_range_part_number(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-bc?part-number=4')
        req.method = 'HEAD'
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '416 Requested Range Not Satisfiable')
        self.assertEqual(headers['Content-Range'],
                         'bytes */%d' % self.manifest_bc_slo_size)
        self.assertEqual(headers['Etag'], '"%s"' % self.manifest_bc_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'], self.manifest_bc_json_md5)
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Parts-Count'], '2')
        self.assertEqual(int(headers['Content-Length']), len(body))
        self.assertEqual(body, b'')
        self.assertEqual(headers['X-Object-Meta-Plant'], 'Ficus')
        expected_app_calls = [
            ('HEAD', '/v1/AUTH_test/gettest/manifest-bc?part-number=4'),
            # segments needed
            ('GET', '/v1/AUTH_test/gettest/manifest-bc?part-number=4')
        ]
        self.assertEqual(self.app.calls, expected_app_calls)

    def test_part_number_exceeds_max_manifest_segments_is_ok(self):
        # verify that an existing part can be fetched regardless of the current
        # max_manifest_segments
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-bc?part-number=2')
        self.slo.max_manifest_segments = 1
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Etag'], '"%s"' % self.manifest_bc_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'], self.manifest_bc_json_md5)
        self.assertEqual(headers['Content-Length'], '15')
        self.assertEqual(headers['Content-Range'], 'bytes 10-24/25')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Parts-Count'], '2')
        self.assertEqual(headers['Content-Type'], 'application/octet-stream')
        self.assertEqual(body, b'c' * 15)
        expected_calls = [
            ('GET', '/v1/AUTH_test/gettest/manifest-bc?part-number=2'),
            ('GET', '/v1/AUTH_test/gettest/c_15?multipart-manifest=get')
        ]
        self.assertEqual(self.app.calls, expected_calls)

    def test_part_number_ignored_for_non_slo_object(self):
        # verify that a part-number param is ignored for a non-slo object
        def do_test(query_string):
            self.app.clear_calls()
            req = Request.blank(
                '/v1/AUTH_test/gettest/c_15?%s' % query_string)
            self.slo.max_manifest_segments = 1
            status, headers, body = self.call_slo(req)
            self.assertEqual(status, '200 OK')
            self.assertEqual(headers['Etag'], '%s' % md5hex('c' * 15))
            self.assertEqual(headers['Content-Length'], '15')
            self.assertEqual(body, b'c' * 15)
            self.assertEqual(1, self.app.call_count)
            method, path = self.app.calls[0]
            actual_req = Request.blank(path, method=method)
            self.assertEqual(req.path, actual_req.path)
            self.assertEqual(req.params, actual_req.params)

        do_test('part-number=-1')
        do_test('part-number=0')
        do_test('part-number=1')
        do_test('part-number=2')
        do_test('part-number=foo')
        do_test('part-number=foo&multipart-manifest=get')

    def test_part_number_ignored_for_non_slo_object_with_range(self):
        # verify that a part-number param is ignored for a non-slo object
        def do_test(query_string):
            self.app.clear_calls()
            req = Request.blank(
                '/v1/AUTH_test/gettest/c_15?%s' % query_string,
                headers={'Range': 'bytes=1-2'})
            self.slo.max_manifest_segments = 1
            status, headers, body = self.call_slo(req)
            self.assertEqual(status, '206 Partial Content')
            self.assertEqual(headers['Etag'], '%s' % md5hex('c' * 15))
            self.assertEqual(headers['Content-Length'], '2')
            self.assertEqual(headers['Content-Range'], 'bytes 1-2/15')
            self.assertEqual(body, b'c' * 2)
            self.assertEqual(1, self.app.call_count)
            method, path = self.app.calls[0]
            actual_req = Request.blank(path, method=method)
            self.assertEqual(req.path, actual_req.path)
            self.assertEqual(req.params, actual_req.params)

        do_test('part-number=-1')
        do_test('part-number=0')
        do_test('part-number=1')
        do_test('part-number=2')
        do_test('part-number=foo')
        do_test('part-number=foo&multipart-manifest=get')

    def test_part_number_ignored_for_manifest_get(self):
        def do_test(query_string):
            self.app.clear_calls()
            req = Request.blank(
                '/v1/AUTH_test/gettest/manifest-bc?%s' % query_string)
            self.slo.max_manifest_segments = 1
            status, headers, body = self.call_slo(req)
            self.assertEqual(status, '200 OK')
            self.assertEqual(headers['Etag'], self.manifest_bc_json_md5)
            self.assertEqual(headers['Content-Length'],
                             str(self.manifest_bc_json_size))
            self.assertEqual(headers['X-Static-Large-Object'], 'true')
            self.assertEqual(headers['Content-Type'],
                             'application/json; charset=utf-8')
            self.assertEqual(headers['X-Object-Meta-Plant'], 'Ficus')
            self.assertEqual(1, self.app.call_count)
            method, path = self.app.calls[0]
            actual_req = Request.blank(path, method=method)
            self.assertEqual(req.path, actual_req.path)
            self.assertEqual(req.params, actual_req.params)

        do_test('part-number=-1&multipart-manifest=get')
        do_test('part-number=0&multipart-manifest=get')
        do_test('part-number=1&multipart-manifest=get')
        do_test('part-number=2&multipart-manifest=get')
        do_test('part-number=foo&multipart-manifest=get')

    def test_head_out_of_range_part_number_on_subrange(self):
        # you can't go past the actual number of parts either
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd-subranges',
            method='HEAD',
            params={'part-number': 6})
        expected_calls = [
            ('HEAD', '/v1/AUTH_test/gettest/manifest-abcd-subranges?'
                     'part-number=6'),
            ('GET', '/v1/AUTH_test/gettest/manifest-abcd-subranges?'
                    'part-number=6')]
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '416 Requested Range Not Satisfiable')
        self.assertEqual(headers['Content-Range'],
                         'bytes */%d' % self.manifest_abcd_subranges_slo_size)
        self.assertEqual(headers['Etag'],
                         '"%s"' % self.manifest_abcd_subranges_slo_etag)
        self.assertEqual(headers['X-Manifest-Etag'],
                         self.manifest_abcd_subranges_json_md5)
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Parts-Count'], '5')
        self.assertEqual(int(headers['Content-Length']), len(body))
        self.assertEqual(body, b'')
        self.assertEqual(self.app.calls, expected_calls)

    def test_range_with_part_number_is_error(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd-subranges?part-number=2',
            headers={'Range': 'bytes=4-12'})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '400 Bad Request')
        self.assertNotIn('Content-Range', headers)
        self.assertNotIn('Etag', headers)
        self.assertNotIn('X-Static-Large-Object', headers)
        self.assertNotIn('X-Parts-Count', headers)
        self.assertEqual(body, b'Range requests are not supported with '
                               b'part number queries')
        expected_calls = [
            ('GET',
             '/v1/AUTH_test/gettest/manifest-abcd-subranges?part-number=2')
        ]
        self.assertEqual(expected_calls, self.app.calls)

    def test_head_part_number_subrange(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/manifest-abcd-subranges',
            method='HEAD', params={'part-number': 2})
        status, headers, body = self.call_slo(req)

        # Range header can be ignored in a HEAD request
        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Etag'],
                         '"%s"' % self.manifest_abcd_subranges_slo_etag)
        self.assertEqual(headers['Content-Length'], '1')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['Content-Type'], 'application/json')
        self.assertEqual(headers['X-Parts-Count'], '5')
        self.assertEqual(body, b'')  # it's a HEAD request, after all
        expected_calls = [
            ('HEAD', '/v1/AUTH_test/gettest/manifest-abcd-subranges'
             '?part-number=2'),
            ('GET', '/v1/AUTH_test/gettest/manifest-abcd-subranges'
             '?part-number=2'),
        ]
        self.assertEqual(self.app.calls, expected_calls)

    def test_head_part_number_data_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/c/manifest-data',
            method='HEAD', params={'part-number': 1})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Etag'],
                         '"%s"' % self.manifest_data_slo_etag)
        self.assertEqual(headers['Content-Length'], '6')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Parts-Count'], '3')
        self.assertEqual(body, b'')  # it's a HEAD request, after all
        expected_calls = [
            ('HEAD', '/v1/AUTH_test/c/manifest-data?part-number=1'),
            ('GET', '/v1/AUTH_test/c/manifest-data?part-number=1'),
        ]
        self.assertEqual(self.app.calls, expected_calls)

    def test_get_part_number_data_manifest(self):
        req = Request.blank(
            '/v1/AUTH_test/c/manifest-data',
            params={'part-number': 3})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Etag'],
                         '"%s"' % self.manifest_data_slo_etag)
        self.assertEqual(headers['Content-Length'], '6')
        self.assertEqual(headers['X-Static-Large-Object'], 'true')
        self.assertEqual(headers['X-Parts-Count'], '3')
        self.assertEqual(body, b'ABCDEF')
        expected_calls = [
            ('GET', '/v1/AUTH_test/c/manifest-data?part-number=3'),
        ]
        self.assertEqual(self.app.calls, expected_calls)


class TestPartNumberLegacyManifest(TestPartNumber):

    modern_manifest_headers = False


class TestSloBulkDeleter(unittest.TestCase):
    def test_reused_logger(self):
        slo_mware = slo.filter_factory({})('fake app')
        self.assertTrue(slo_mware.logger is slo_mware.bulk_deleter.logger)

    def test_passes_through_concurrency(self):
        slo_mware = slo.filter_factory({'delete_concurrency': 5})('fake app')
        self.assertEqual(5, slo_mware.bulk_deleter.delete_concurrency)

    def test_uses_big_max_deletes(self):
        slo_mware = slo.filter_factory(
            {'max_manifest_segments': 123456789})('fake app')
        self.assertGreaterEqual(
            slo_mware.bulk_deleter.max_deletes_per_request,
            123456789)


class TestSwiftInfo(unittest.TestCase):
    def setUp(self):
        registry._swift_info = {}
        registry._swift_admin_info = {}

    def test_registered_defaults(self):
        mware = slo.filter_factory({})('have to pass in an app')
        swift_info = registry.get_swift_info()
        self.assertTrue('slo' in swift_info)
        self.assertEqual(swift_info['slo'].get('max_manifest_segments'),
                         mware.max_manifest_segments)
        self.assertEqual(swift_info['slo'].get('min_segment_size'), 1)
        self.assertEqual(swift_info['slo'].get('max_manifest_size'),
                         mware.max_manifest_size)
        self.assertIs(swift_info['slo'].get('allow_async_delete'), True)
        self.assertEqual(1000, mware.max_manifest_segments)
        self.assertEqual(8388608, mware.max_manifest_size)
        self.assertEqual(1048576, mware.rate_limit_under_size)
        self.assertEqual(10, mware.rate_limit_after_segment)
        self.assertEqual(1, mware.rate_limit_segments_per_sec)
        self.assertEqual(10, mware.yield_frequency)
        self.assertEqual(2, mware.concurrency)
        self.assertEqual(2, mware.bulk_deleter.delete_concurrency)
        self.assertIs(True, mware.allow_async_delete)

    def test_registered_non_defaults(self):
        conf = dict(
            max_manifest_segments=500, max_manifest_size=1048576,
            rate_limit_under_size=2097152, rate_limit_after_segment=20,
            rate_limit_segments_per_sec=2, yield_frequency=5, concurrency=1,
            delete_concurrency=3, allow_async_delete='n')
        mware = slo.filter_factory(conf)('have to pass in an app')
        swift_info = registry.get_swift_info()
        self.assertTrue('slo' in swift_info)
        self.assertEqual(swift_info['slo'].get('max_manifest_segments'), 500)
        self.assertEqual(swift_info['slo'].get('min_segment_size'), 1)
        self.assertEqual(swift_info['slo'].get('max_manifest_size'), 1048576)
        self.assertIs(swift_info['slo'].get('allow_async_delete'), False)
        self.assertEqual(500, mware.max_manifest_segments)
        self.assertEqual(1048576, mware.max_manifest_size)
        self.assertEqual(2097152, mware.rate_limit_under_size)
        self.assertEqual(20, mware.rate_limit_after_segment)
        self.assertEqual(2, mware.rate_limit_segments_per_sec)
        self.assertEqual(5, mware.yield_frequency)
        self.assertEqual(1, mware.concurrency)
        self.assertEqual(3, mware.bulk_deleter.delete_concurrency)
        self.assertIs(False, mware.allow_async_delete)


class TestNonSloPassthrough(SloGETorHEADTestCase):

    def setUp(self):
        super(TestNonSloPassthrough, self).setUp()
        self._setup_alphabet_objects('a')

        body = b'big' * 1000
        self.app.register(
            'GET', '/v1/AUTH_test/rangetest/big', swob.HTTPOk, {
                'Content-Length': len(body),
                'Etag': md5hex(body),
            }, body=body)

    def test_get_nonmanifest_passthrough(self):
        req = Request.blank(
            '/v1/AUTH_test/gettest/a_5',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_slo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Etag'], md5hex('a' * 5))
        self.assertEqual(headers['Content-Length'], '5')
        self.assertNotIn('X-Static-Large-Object', headers)
        self.assertNotIn('X-Manifest-Etag', headers)
        self.assertEqual(body, b'aaaaa')
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/gettest/a_5'),
        ])

    def test_non_slo_range_passthrough(self):
        req = Request.blank(
            '/v1/AUTH_test/rangetest/big',
            headers={'Range': 'bytes=0-4'})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '206 Partial Content')
        self.assertEqual(headers['Content-Length'], '5')
        self.assertEqual(headers['Content-Range'], 'bytes 0-4/3000')
        self.assertEqual(body, b'bigbi')
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/rangetest/big'),
        ])

    def test_non_slo_range_unsatisfiable_passthrough(self):
        req = Request.blank(
            '/v1/AUTH_test/rangetest/big',
            headers={'Range': 'bytes=3001-'})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '416 Requested Range Not Satisfiable')
        self.assertEqual(headers['Content-Range'], 'bytes */3000')
        self.assertIn(b'Requested Range Not Satisfiable', body)
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/rangetest/big'),
        ])

    def test_non_slo_multi_range_passthrough(self):
        req = Request.blank(
            '/v1/AUTH_test/rangetest/big',
            headers={'Range': 'bytes=1-2,3-4'})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '206 Partial Content')
        self.assertNotIn('Content-Range', headers)

        ct, params = parse_content_type(headers['Content-Type'])
        self.assertEqual(ct, 'multipart/byteranges')

        params = dict(params)
        boundary = params.get('boundary').encode('utf-8')

        self.assertEqual(len(body), int(headers['Content-Length']))

        got_mime_docs = []
        for mime_doc_fh in iter_multipart_mime_documents(
                BytesIO(body), boundary):
            headers = parse_mime_headers(mime_doc_fh)
            body = mime_doc_fh.read()
            got_mime_docs.append((headers, body))
        self.assertEqual(len(got_mime_docs), 2)

        first_range_headers, first_range_body = got_mime_docs[0]
        self.assertEqual(first_range_headers['Content-Range'],
                         'bytes 1-2/3000')
        self.assertEqual(first_range_body, b'ig')

        second_range_headers, second_range_body = got_mime_docs[1]
        self.assertEqual(second_range_headers['Content-Range'],
                         'bytes 3-4/3000')
        # 012 34 5678
        # big bi gbig
        self.assertEqual(second_range_body, b'bi')
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/rangetest/big'),
        ])

    def test_non_slo_multi_range_partially_satisfiable_passthrough(self):
        req = Request.blank(
            '/v1/AUTH_test/rangetest/big',
            headers={'Range': 'bytes=1-2,3-4,3001-'})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '206 Partial Content')
        self.assertNotIn('Content-Range', headers)
        self.assertEqual(int(headers['Content-Length']), len(body))

        ct, params = parse_content_type(headers['Content-Type'])
        self.assertEqual(ct, 'multipart/byteranges')

        params = dict(params)
        boundary = params.get('boundary').encode('utf-8')

        self.assertEqual(len(body), int(headers['Content-Length']))

        got_mime_docs = []
        for mime_doc_fh in iter_multipart_mime_documents(
                BytesIO(body), boundary):
            headers = parse_mime_headers(mime_doc_fh)
            body = mime_doc_fh.read()
            got_mime_docs.append((headers, body))
        self.assertEqual(len(got_mime_docs), 2)

        first_range_headers, first_range_body = got_mime_docs[0]
        self.assertEqual(first_range_headers['Content-Range'],
                         'bytes 1-2/3000')
        self.assertEqual(first_range_body, b'ig')

        second_range_headers, second_range_body = got_mime_docs[1]
        self.assertEqual(second_range_headers['Content-Range'],
                         'bytes 3-4/3000')
        self.assertEqual(second_range_body, b'bi')
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/rangetest/big'),
        ])

    def test_non_slo_multi_range_unsatisfiable_passthrough(self):
        req = Request.blank(
            '/v1/AUTH_test/rangetest/big',
            headers={'Range': 'bytes=3001-,3005-3010'})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '416 Requested Range Not Satisfiable')
        self.assertEqual(headers['Content-Range'], 'bytes */3000')
        self.assertEqual(int(headers['Content-Length']), len(body))
        self.assertIn(b'Requested Range Not Satisfiable', body)
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/rangetest/big'),
        ])

    def test_non_slo_multi_range_starting_beyond_multipart_resp_length(self):
        req = Request.blank(
            '/v1/AUTH_test/rangetest/big',
            headers={'Range': 'bytes=1000-1002,2000-2002'})
        status, headers, body = self.call_slo(req)
        self.assertEqual(status, '206 Partial Content')
        self.assertNotIn('Content-Range', headers)
        self.assertEqual(int(headers['Content-Length']), len(body))

        ct, params = parse_content_type(headers['Content-Type'])
        self.assertEqual(ct, 'multipart/byteranges')

        params = dict(params)
        boundary = params.get('boundary').encode('utf-8')

        self.assertEqual(len(body), int(headers['Content-Length']))

        got_mime_docs = []
        for mime_doc_fh in iter_multipart_mime_documents(
                BytesIO(body), boundary):
            headers = parse_mime_headers(mime_doc_fh)
            body = mime_doc_fh.read()
            got_mime_docs.append((headers, body))
        self.assertEqual(len(got_mime_docs), 2)

        first_range_headers, first_range_body = got_mime_docs[0]
        self.assertEqual(first_range_headers['Content-Range'],
                         'bytes 1000-1002/3000')
        self.assertEqual(first_range_body, b'igb')

        second_range_headers, second_range_body = got_mime_docs[1]
        self.assertEqual(second_range_headers['Content-Range'],
                         'bytes 2000-2002/3000')
        self.assertEqual(second_range_body, b'gbi')
        self.assertEqual(self.app.calls, [
            ('GET', '/v1/AUTH_test/rangetest/big'),
        ])


class TestRespAttrs(unittest.TestCase):
    def test_init_calculates_is_legacy(self):
        attrs = slo.RespAttrs(True, 123456789.12345,
                              'manifest-etag', 'slo-etag', 999)
        self.assertTrue(attrs.is_slo)
        self.assertEqual(123456789.12345, attrs.timestamp)
        self.assertIsInstance(attrs.timestamp, Timestamp)
        self.assertEqual('manifest-etag', attrs.json_md5)
        self.assertEqual('slo-etag', attrs.slo_etag)
        self.assertEqual(999, attrs.slo_size)
        # we gave it etag and size!
        self.assertTrue(attrs._has_size_and_etag())
        self.assertFalse(attrs.is_legacy)

    def test_init_converts_timestamps_from_strings(self):
        attrs = slo.RespAttrs(True, '123456789.12345',
                              'manifest-etag', 'slo-etag', 999)
        self.assertTrue(attrs.is_slo)
        self.assertEqual(123456789.12345, attrs.timestamp)
        self.assertIsInstance(attrs.timestamp, Timestamp)
        self.assertEqual('manifest-etag', attrs.json_md5)
        self.assertEqual('slo-etag', attrs.slo_etag)
        self.assertEqual(999, attrs.slo_size)
        # we gave it etag and size!
        self.assertTrue(attrs._has_size_and_etag())
        self.assertFalse(attrs.is_legacy)

    def test_default_types(self):
        attrs = slo.RespAttrs(None, None, None, None, None)
        # types are correct, values are default/place-holders
        self.assertTrue(attrs.is_slo is False)  # not None!
        self.assertEqual(0, attrs.timestamp)
        self.assertIsInstance(attrs.timestamp, Timestamp)
        self.assertEqual('', attrs.json_md5)
        self.assertEqual('', attrs.slo_etag)
        self.assertEqual(-1, attrs.slo_size)
        # we didn't provide etag & size
        self.assertFalse(attrs._has_size_and_etag())
        self.assertTrue(attrs.is_legacy)

    def test_init_with_no_sysmeta(self):
        now = Timestamp.now()
        attrs = slo.RespAttrs(True, now.normal, None, None, None)
        self.assertTrue(attrs.is_slo)
        self.assertEqual(now, attrs.timestamp)
        self.assertIsInstance(attrs.timestamp, Timestamp)
        self.assertEqual('', attrs.json_md5)
        self.assertEqual('', attrs.slo_etag)
        self.assertEqual(-1, attrs.slo_size)
        # we didn't provide etag & size
        self.assertFalse(attrs._has_size_and_etag())
        self.assertTrue(attrs.is_legacy)

    def test_init_with_no_sysmeta_offset(self):
        now = Timestamp.now(offset=123)
        attrs = slo.RespAttrs(True, now.internal, None, None, None)
        self.assertTrue(attrs.is_slo)
        self.assertEqual(now, attrs.timestamp)
        self.assertIsInstance(attrs.timestamp, Timestamp)
        self.assertEqual('', attrs.json_md5)
        self.assertEqual('', attrs.slo_etag)
        self.assertEqual(-1, attrs.slo_size)
        # we didn't provide etag & size
        self.assertFalse(attrs._has_size_and_etag())
        self.assertTrue(attrs.is_legacy)

    def test_from_empty_headers(self):
        attrs = slo.RespAttrs.from_headers([])
        self.assertFalse(attrs.is_slo)
        self.assertEqual(0, attrs.timestamp)
        self.assertEqual('', attrs.json_md5)
        self.assertEqual('', attrs.slo_etag)
        self.assertEqual(-1, attrs.slo_size)
        self.assertTrue(attrs.is_legacy)

    def test_from_only_timestamp(self):
        now = Timestamp.now(offset=1)
        attrs = slo.RespAttrs.from_headers(
            [('X-Backend-Timestamp', now.internal),
             ('X-Irrelevant', 'ignored')])
        self.assertFalse(attrs.is_slo)
        self.assertEqual(now, attrs.timestamp)
        self.assertEqual('', attrs.json_md5)
        self.assertEqual('', attrs.slo_etag)
        self.assertEqual(-1, attrs.slo_size)
        self.assertTrue(attrs.is_legacy)

    def test_legacy_slo_sysmeta(self):
        attrs = slo.RespAttrs.from_headers(
            [('X-Backend-Timestamp', '123456789.12345'),
             ('Etag', 'manifest-etag'),
             ('X-Static-lARGE-Object', 'yes')])
        self.assertTrue(attrs.is_slo)
        self.assertEqual(123456789.12345, attrs.timestamp)
        self.assertEqual('manifest-etag', attrs.json_md5)
        self.assertEqual('', attrs.slo_etag)
        self.assertEqual(-1, attrs.slo_size)
        self.assertTrue(attrs.is_legacy)

    def test_partial_modern_sysmeta(self):
        # missing slo etag
        attrs = slo.RespAttrs.from_headers(
            [('X-Backend-Timestamp', '123456789.12345'),
             ('Etag', 'manifest-etag'),
             ('X-Static-lARGE-Object', 'yes'),
             ('x-object-sysmeta-slo-size', '1234')])
        self.assertTrue(attrs.is_slo)
        self.assertEqual(123456789.12345, attrs.timestamp)
        self.assertEqual('manifest-etag', attrs.json_md5)
        self.assertEqual('', attrs.slo_etag)
        self.assertEqual(1234, attrs.slo_size)
        self.assertTrue(attrs.is_legacy)

        # missing slo size
        attrs = slo.RespAttrs.from_headers(
            [('X-Backend-Timestamp', '123456789.12345'),
             ('Etag', 'manifest-etag'),
             ('X-Static-lARGE-Object', 'yes'),
             ('x-object-sysmeta-slo-etag', 'slo-etag')])
        self.assertTrue(attrs.is_slo)
        self.assertEqual(123456789.12345, attrs.timestamp)
        self.assertEqual('manifest-etag', attrs.json_md5)
        self.assertEqual('slo-etag', attrs.slo_etag)
        self.assertEqual(-1, attrs.slo_size)
        self.assertTrue(attrs.is_legacy)

        # missing manifest etag
        attrs = slo.RespAttrs.from_headers(
            [('X-Backend-Timestamp', '123456789.12345'),
             ('X-Static-lARGE-Object', 'yes'),
             ('x-object-sysmeta-slo-size', '1234'),
             ('x-object-sysmeta-slo-etag', 'slo-etag')])
        self.assertTrue(attrs.is_slo)
        self.assertEqual(123456789.12345, attrs.timestamp)
        self.assertEqual('', attrs.json_md5)
        self.assertEqual('slo-etag', attrs.slo_etag)
        self.assertEqual(1234, attrs.slo_size)
        # missing Etag might be some kind of bug, but it has all sysmeta
        self.assertFalse(attrs.is_legacy)

    def test_invalid_sysmeta(self):
        attrs = slo.RespAttrs.from_headers(
            [('X-Backend-Timestamp', '123456789.12345'),
             ('X-Static-lARGE-Object', 'yes'),
             ('x-object-sysmeta-slo-size', 'huge!')])
        self.assertTrue(attrs.is_slo)
        self.assertEqual(123456789.12345, attrs.timestamp)
        self.assertEqual('', attrs.json_md5)
        self.assertEqual('', attrs.slo_etag)
        self.assertEqual(-1, attrs.slo_size)
        self.assertTrue(attrs.is_legacy)

        attrs = slo.RespAttrs.from_headers(
            [('X-Backend-Timestamp', '123456789.12345'),
             ('X-Static-lARGE-Object', 'yes'),
             ('e-TAG', 'wrong!'),
             ('x-object-sysmeta-slo-size', '')])
        self.assertTrue(attrs.is_slo)
        self.assertEqual(123456789.12345, attrs.timestamp)
        self.assertEqual('', attrs.json_md5)
        self.assertEqual('', attrs.slo_etag)
        self.assertEqual(-1, attrs.slo_size)
        self.assertTrue(attrs.is_legacy)

    def test_from_valid_sysmeta(self):
        attrs = slo.RespAttrs.from_headers(
            [('X-Backend-Timestamp', '123456789.12345'),
             ('Etag', 'manifest-etag'),
             ('X-Static-lARGE-Object', 'yes'),
             ('x-object-sysmeta-slo-etag', 'slo-tag'),
             ('x-object-sysmeta-slo-size', '1234')])
        self.assertTrue(attrs.is_slo)
        self.assertEqual(123456789.12345, attrs.timestamp)
        self.assertEqual('manifest-etag', attrs.json_md5)
        self.assertEqual('slo-tag', attrs.slo_etag)
        self.assertEqual(1234, attrs.slo_size)
        self.assertFalse(attrs.is_legacy)

    def test_from_regular_object(self):
        now = Timestamp.now()
        attrs = slo.RespAttrs.from_headers(
            [('X-Backend-Timestamp', now.normal),
             ('Etag', 'object-etag')])
        self.assertFalse(attrs.is_slo)
        self.assertEqual(now, attrs.timestamp)
        # N.B. we only set manifest_etag on slo objects
        self.assertEqual('', attrs.json_md5)
        self.assertEqual('', attrs.slo_etag)
        self.assertEqual(-1, attrs.slo_size)
        self.assertTrue(attrs.is_legacy)

    def test_non_slo_with_sysmeta(self):
        attrs = slo.RespAttrs.from_headers(
            [('X-Backend-Timestamp', '123456789.12345'),
             ('X-Static-lARGE-Object', 'false')])
        self.assertFalse(attrs.is_slo)
        self.assertEqual(123456789.12345, attrs.timestamp)
        self.assertEqual('', attrs.json_md5)
        self.assertEqual('', attrs.slo_etag)
        self.assertEqual(-1, attrs.slo_size)
        self.assertTrue(attrs.is_legacy)

        attrs = slo.RespAttrs.from_headers(
            [('X-Backend-Timestamp', '123456789.12345'),
             ('Etag', 'segment-etag'),
             ('x-object-sysmeta-slo-etag', 'tag'),
             ('x-object-sysmeta-slo-size', '1234')])
        # this is NOT an SLO
        self.assertFalse(attrs.is_slo)
        self.assertEqual('', attrs.json_md5)
        # ... but we set these based on the sysmeta values
        self.assertEqual('tag', attrs.slo_etag)
        self.assertEqual(1234, attrs.slo_size)
        self.assertEqual(123456789.12345, attrs.timestamp)
        # I hope someday a non-slo with slo sysmeta *will* be just a legacy,
        # see lp bug #2035158
        self.assertFalse(attrs.is_legacy)

    def test_from_zero_byte_sysmeta(self):
        attrs = slo.RespAttrs.from_headers([
            ('X-Backend-Timestamp', '1709069771.34178'),
            ('X-Object-Sysmeta-Container-Update-Override-Etag',
             'a1eadf0ca181e87fcbdba2074ce0fd90; '
             's3_etag=59adb24ef3cdbe0297f05b395827453f-1; '
             'slo_etag=74be16979710d4c4e7c6647856088456'),
            ('X-Object-Sysmeta-S3Api-Etag',
             '59adb24ef3cdbe0297f05b395827453f-1'),
            ('X-Object-Sysmeta-S3Api-Upload-Id',
             'NDZlMDBhN2MtNzVmZS00ZTljLTkzN2EtODcwNGQ5OTg4NmQ2'),
            ('X-Object-Sysmeta-Slo-Etag', '74be16979710d4c4e7c6647856088456'),
            ('X-Object-Sysmeta-Slo-Size', '0'),
            ('ETag', 'a1eadf0ca181e87fcbdba2074ce0fd90'),
            ('X-Static-Large-Object', 'True'),
        ])
        self.assertTrue(attrs.is_slo)
        self.assertEqual(1709069771.34178, attrs.timestamp)
        self.assertEqual('a1eadf0ca181e87fcbdba2074ce0fd90', attrs.json_md5)
        self.assertEqual('74be16979710d4c4e7c6647856088456', attrs.slo_etag)
        self.assertEqual(0, attrs.slo_size)
        self.assertFalse(attrs.is_legacy)

    def _legacy_from_headers(self):
        attrs = slo.RespAttrs.from_headers(
            [('X-Backend-Timestamp', '123456789.12345'),
             ('Etag', 'manifest-etag'),
             ('X-Static-lARGE-Object', 'yes')])
        self.assertTrue(attrs.is_slo)
        self.assertEqual(123456789.12345, attrs.timestamp)
        self.assertEqual('manifest-etag', attrs.json_md5)
        self.assertEqual('', attrs.slo_etag)
        self.assertEqual(-1, attrs.slo_size)
        self.assertTrue(attrs.is_legacy)
        return attrs

    def test_update_from_segments(self):
        attrs = self._legacy_from_headers()
        segments = [
            {'hash': 'abc', 'bytes': 2},
            {'hash': 'def', 'bytes': 3},
        ]
        slo._annotate_segments(segments)
        attrs.update_from_segments(segments)

        exp_etag = md5('abcdef'.encode('ascii'), usedforsecurity=False)

        self.assertTrue(attrs.is_slo)
        self.assertEqual(123456789.12345, attrs.timestamp)
        self.assertEqual(exp_etag.hexdigest(), attrs.slo_etag)
        self.assertEqual(5, attrs.slo_size)
        # N.B. it's still a legacy manifest
        self.assertTrue(attrs.is_legacy)

    def test_update_from_segments_with_raw_data(self):
        attrs = self._legacy_from_headers()
        raw_data = b'something'
        segments = [
            {'hash': 'abc', 'bytes': 2},
            {'data': base64.b64encode(raw_data)},
        ]
        slo._annotate_segments(segments)
        attrs.update_from_segments(segments)

        raw_data_checksum = md5(raw_data).hexdigest()
        exp_etag = md5(('abc' + raw_data_checksum).encode('ascii'),
                       usedforsecurity=False)

        self.assertTrue(attrs.is_slo)
        self.assertEqual(123456789.12345, attrs.timestamp)
        self.assertEqual(exp_etag.hexdigest(), attrs.slo_etag)
        self.assertEqual(11, attrs.slo_size)
        # N.B. it's still a legacy manifest
        self.assertTrue(attrs.is_legacy)

    def test_update_from_segments_with_range(self):
        attrs = self._legacy_from_headers()
        segments = [
            {'hash': 'abc', 'bytes': 2},
            {'hash': 'def', 'range': '1-2'},
        ]
        slo._annotate_segments(segments)
        attrs.update_from_segments(segments)

        exp_etag = md5('abcdef:1-2;'.encode('ascii'), usedforsecurity=False)

        self.assertTrue(attrs.is_slo)
        self.assertEqual(123456789.12345, attrs.timestamp)
        self.assertEqual(exp_etag.hexdigest(), attrs.slo_etag)
        self.assertEqual(4, attrs.slo_size)
        # N.B. it's still a legacy manifest
        self.assertTrue(attrs.is_legacy)

    def test_update_from_segments_with_sub_slo(self):
        attrs = self._legacy_from_headers()
        content_type = 'application/octet-stream'
        content_type += ";swift_bytes=%d" % 5
        segments = [
            {'hash': 'abc', 'bytes': 2},
            {'hash': '123', 'sub_slo': True, 'content_type': content_type},
        ]
        slo._annotate_segments(segments)
        attrs.update_from_segments(segments)

        exp_etag = md5('abc123'.encode('ascii'), usedforsecurity=False)

        self.assertTrue(attrs.is_slo)
        self.assertEqual(123456789.12345, attrs.timestamp)
        self.assertEqual(exp_etag.hexdigest(), attrs.slo_etag)
        self.assertEqual(7, attrs.slo_size)
        # N.B. it's still a legacy manifest
        self.assertTrue(attrs.is_legacy)

    def test_update_from_segments_with_sub_slo_range(self):
        attrs = self._legacy_from_headers()
        content_type = 'application/octet-stream'
        content_type += ";swift_bytes=%d" % 5
        segments = [
            {'hash': 'abc', 'bytes': 2},
            {'hash': '123', 'sub_slo': True, 'content_type': content_type,
             'range': '2-4'},
        ]
        slo._annotate_segments(segments)
        attrs.update_from_segments(segments)

        exp_etag = md5('abc123:2-4;'.encode('ascii'), usedforsecurity=False)

        self.assertTrue(attrs.is_slo)
        self.assertEqual(123456789.12345, attrs.timestamp)
        self.assertEqual(exp_etag.hexdigest(), attrs.slo_etag)
        self.assertEqual(5, attrs.slo_size)
        # N.B. it's still a legacy manifest
        self.assertTrue(attrs.is_legacy)

    def test_update_from_segments_not_legacy(self):
        attrs = slo.RespAttrs.from_headers(
            [('X-Backend-Timestamp', '123456789.12345'),
             ('X-Static-lARGE-Object', 'yes'),
             ('x-object-sysmeta-slo-etag', 'tag'),
             ('x-object-sysmeta-slo-size', '1234')])

        segments = 'not even json; does not matter'
        attrs.update_from_segments(segments)

        self.assertTrue(attrs.is_slo)
        self.assertEqual(123456789.12345, attrs.timestamp)
        self.assertEqual('tag', attrs.slo_etag)
        self.assertEqual(1234, attrs.slo_size)
        # N.B. it's still a legacy manifest
        self.assertFalse(attrs.is_legacy)


if __name__ == '__main__':
    unittest.main()
