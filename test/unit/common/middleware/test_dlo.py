# coding: utf-8
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

import hashlib
import json
import mock
import shutil
import tempfile
from textwrap import dedent
import time
import unittest

from swift.common import swob
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.middleware import dlo
from swift.common.utils import closing_if_possible
from test.unit.common.middleware.helpers import FakeSwift


LIMIT = 'swift.common.constraints.CONTAINER_LISTING_LIMIT'


def md5hex(s):
    return hashlib.md5(s).hexdigest()


class DloTestCase(unittest.TestCase):
    def call_dlo(self, req, app=None):
        if app is None:
            app = self.dlo

        req.headers.setdefault("User-Agent", "Soap Opera")

        status = [None]
        headers = [None]

        def start_response(s, h, ei=None):
            status[0] = s
            headers[0] = h

        body_iter = app(req.environ, start_response)
        body = ''
        # appease the close-checker
        with closing_if_possible(body_iter):
            for chunk in body_iter:
                body += chunk
        return status[0], headers[0], body

    def setUp(self):
        self.app = FakeSwift()
        self.dlo = dlo.filter_factory({
            # don't slow down tests with rate limiting
            'rate_limit_after_segment': '1000000',
        })(self.app)
        self.dlo.logger = self.app.logger
        self.app.register(
            'GET', '/v1/AUTH_test/c/seg_01',
            swob.HTTPOk, {'Content-Length': '5', 'Etag': md5hex("aaaaa")},
            'aaaaa')
        self.app.register(
            'GET', '/v1/AUTH_test/c/seg_02',
            swob.HTTPOk, {'Content-Length': '5', 'Etag': md5hex("bbbbb")},
            'bbbbb')
        self.app.register(
            'GET', '/v1/AUTH_test/c/seg_03',
            swob.HTTPOk, {'Content-Length': '5', 'Etag': md5hex("ccccc")},
            'ccccc')
        self.app.register(
            'GET', '/v1/AUTH_test/c/seg_04',
            swob.HTTPOk, {'Content-Length': '5', 'Etag': md5hex("ddddd")},
            'ddddd')
        self.app.register(
            'GET', '/v1/AUTH_test/c/seg_05',
            swob.HTTPOk, {'Content-Length': '5', 'Etag': md5hex("eeeee")},
            'eeeee')

        # an unrelated object (not seg*) to test the prefix matching
        self.app.register(
            'GET', '/v1/AUTH_test/c/catpicture.jpg',
            swob.HTTPOk, {'Content-Length': '9',
                          'Etag': md5hex("meow meow meow meow")},
            'meow meow meow meow')

        self.app.register(
            'GET', '/v1/AUTH_test/mancon/manifest',
            swob.HTTPOk, {'Content-Length': '17', 'Etag': 'manifest-etag',
                          'X-Object-Manifest': 'c/seg'},
            'manifest-contents')

        lm = '2013-11-22T02:42:13.781760'
        ct = 'application/octet-stream'
        segs = [{"hash": md5hex("aaaaa"), "bytes": 5,
                 "name": "seg_01", "last_modified": lm, "content_type": ct},
                {"hash": md5hex("bbbbb"), "bytes": 5,
                 "name": "seg_02", "last_modified": lm, "content_type": ct},
                {"hash": md5hex("ccccc"), "bytes": 5,
                 "name": "seg_03", "last_modified": lm, "content_type": ct},
                {"hash": md5hex("ddddd"), "bytes": 5,
                 "name": "seg_04", "last_modified": lm, "content_type": ct},
                {"hash": md5hex("eeeee"), "bytes": 5,
                 "name": "seg_05", "last_modified": lm, "content_type": ct}]

        full_container_listing = segs + [{"hash": "cats-etag", "bytes": 9,
                                          "name": "catpicture.jpg",
                                          "last_modified": lm,
                                          "content_type": "application/png"}]
        self.app.register(
            'GET', '/v1/AUTH_test/c',
            swob.HTTPOk, {'Content-Type': 'application/json; charset=utf-8'},
            json.dumps(full_container_listing))
        self.app.register(
            'GET', '/v1/AUTH_test/c?prefix=seg',
            swob.HTTPOk, {'Content-Type': 'application/json; charset=utf-8'},
            json.dumps(segs))

        # This is to let us test multi-page container listings; we use the
        # trailing underscore to send small (pagesize=3) listings.
        #
        # If you're testing against this, be sure to mock out
        # CONTAINER_LISTING_LIMIT to 3 in your test.
        self.app.register(
            'GET', '/v1/AUTH_test/mancon/manifest-many-segments',
            swob.HTTPOk, {'Content-Length': '7', 'Etag': 'etag-manyseg',
                          'X-Object-Manifest': 'c/seg_'},
            'manyseg')
        self.app.register(
            'GET', '/v1/AUTH_test/c?prefix=seg_',
            swob.HTTPOk, {'Content-Type': 'application/json; charset=utf-8'},
            json.dumps(segs[:3]))
        self.app.register(
            'GET', '/v1/AUTH_test/c?prefix=seg_&marker=seg_03',
            swob.HTTPOk, {'Content-Type': 'application/json; charset=utf-8'},
            json.dumps(segs[3:]))

        # Here's a manifest with 0 segments
        self.app.register(
            'GET', '/v1/AUTH_test/mancon/manifest-no-segments',
            swob.HTTPOk, {'Content-Length': '7', 'Etag': 'noseg',
                          'X-Object-Manifest': 'c/noseg_'},
            'noseg')
        self.app.register(
            'GET', '/v1/AUTH_test/c?prefix=noseg_',
            swob.HTTPOk, {'Content-Type': 'application/json; charset=utf-8'},
            json.dumps([]))


class TestDloPutManifest(DloTestCase):
    def setUp(self):
        super(TestDloPutManifest, self).setUp()
        self.app.register(
            'PUT', '/v1/AUTH_test/c/m',
            swob.HTTPCreated, {}, None)

    def test_validating_x_object_manifest(self):
        exp_okay = ["c/o",
                    "c/obj/with/slashes",
                    "c/obj/with/trailing/slash/",
                    "c/obj/with//multiple///slashes////adjacent"]
        exp_bad = ["",
                   "/leading/slash",
                   "double//slash",
                   "container-only",
                   "whole-container/",
                   "c/o?short=querystring",
                   "c/o?has=a&long-query=string"]

        got_okay = []
        got_bad = []
        for val in (exp_okay + exp_bad):
            req = swob.Request.blank("/v1/AUTH_test/c/m",
                                     environ={'REQUEST_METHOD': 'PUT'},
                                     headers={"X-Object-Manifest": val})
            status, _, _ = self.call_dlo(req)
            if status.startswith("201"):
                got_okay.append(val)
            else:
                got_bad.append(val)

        self.assertEqual(exp_okay, got_okay)
        self.assertEqual(exp_bad, got_bad)

    def test_validation_watches_manifests_with_slashes(self):
        self.app.register(
            'PUT', '/v1/AUTH_test/con/w/x/y/z',
            swob.HTTPCreated, {}, None)

        req = swob.Request.blank(
            "/v1/AUTH_test/con/w/x/y/z", environ={'REQUEST_METHOD': 'PUT'},
            headers={"X-Object-Manifest": 'good/value'})
        status, _, _ = self.call_dlo(req)
        self.assertEqual(status, "201 Created")

        req = swob.Request.blank(
            "/v1/AUTH_test/con/w/x/y/z", environ={'REQUEST_METHOD': 'PUT'},
            headers={"X-Object-Manifest": '/badvalue'})
        status, _, _ = self.call_dlo(req)
        self.assertEqual(status, "400 Bad Request")

    def test_validation_ignores_containers(self):
        self.app.register(
            'PUT', '/v1/a/c',
            swob.HTTPAccepted, {}, None)
        req = swob.Request.blank(
            "/v1/a/c", environ={'REQUEST_METHOD': 'PUT'},
            headers={"X-Object-Manifest": "/superbogus/?wrong=in&every=way"})
        status, _, _ = self.call_dlo(req)
        self.assertEqual(status, "202 Accepted")

    def test_validation_ignores_accounts(self):
        self.app.register(
            'PUT', '/v1/a',
            swob.HTTPAccepted, {}, None)
        req = swob.Request.blank(
            "/v1/a", environ={'REQUEST_METHOD': 'PUT'},
            headers={"X-Object-Manifest": "/superbogus/?wrong=in&every=way"})
        status, _, _ = self.call_dlo(req)
        self.assertEqual(status, "202 Accepted")


class TestDloHeadManifest(DloTestCase):
    def test_head_large_object(self):
        expected_etag = '"%s"' % md5hex(
            md5hex("aaaaa") + md5hex("bbbbb") + md5hex("ccccc") +
            md5hex("ddddd") + md5hex("eeeee"))
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'HEAD'})
        status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)
        self.assertEqual(headers["Etag"], expected_etag)
        self.assertEqual(headers["Content-Length"], "25")

    def test_head_large_object_too_many_segments(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest-many-segments',
                                 environ={'REQUEST_METHOD': 'HEAD'})
        with mock.patch(LIMIT, 3):
            status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)

        # etag is manifest's etag
        self.assertEqual(headers["Etag"], "etag-manyseg")
        self.assertIsNone(headers.get("Content-Length"))

    def test_head_large_object_no_segments(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest-no-segments',
                                 environ={'REQUEST_METHOD': 'HEAD'})
        status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)
        self.assertEqual(headers["Etag"], '"%s"' % md5hex(""))
        self.assertEqual(headers["Content-Length"], "0")

        # one request to HEAD the manifest
        # one request for the first page of listings
        # *zero* requests for the second page of listings
        self.assertEqual(
            self.app.calls,
            [('HEAD', '/v1/AUTH_test/mancon/manifest-no-segments'),
             ('GET', '/v1/AUTH_test/c?prefix=noseg_')])


class TestDloGetManifest(DloTestCase):
    def tearDown(self):
        self.assertEqual(self.app.unclosed_requests, {})

    def test_get_manifest(self):
        expected_etag = '"%s"' % md5hex(
            md5hex("aaaaa") + md5hex("bbbbb") + md5hex("ccccc") +
            md5hex("ddddd") + md5hex("eeeee"))
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)
        self.assertEqual(headers["Etag"], expected_etag)
        self.assertEqual(headers["Content-Length"], "25")
        self.assertEqual(body, 'aaaaabbbbbcccccdddddeeeee')

        for _, _, hdrs in self.app.calls_with_headers[1:]:
            ua = hdrs.get("User-Agent", "")
            self.assertTrue("DLO MultipartGET" in ua)
            self.assertFalse("DLO MultipartGET DLO MultipartGET" in ua)
        # the first request goes through unaltered
        self.assertFalse(
            "DLO MultipartGET" in self.app.calls_with_headers[0][2])

        # we set swift.source for everything but the first request
        self.assertEqual(self.app.swift_sources,
                         [None, 'DLO', 'DLO', 'DLO', 'DLO', 'DLO', 'DLO'])

    def test_get_non_manifest_passthrough(self):
        req = swob.Request.blank('/v1/AUTH_test/c/catpicture.jpg',
                                 environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_dlo(req)
        self.assertEqual(body, "meow meow meow meow")

    def test_get_non_object_passthrough(self):
        self.app.register('GET', '/info', swob.HTTPOk,
                          {}, 'useful stuff here')
        req = swob.Request.blank('/info',
                                 environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_dlo(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(body, 'useful stuff here')
        self.assertEqual(self.app.call_count, 1)

    def test_get_manifest_passthrough(self):
        # reregister it with the query param
        self.app.register(
            'GET', '/v1/AUTH_test/mancon/manifest?multipart-manifest=get',
            swob.HTTPOk, {'Content-Length': '17', 'Etag': 'manifest-etag',
                          'X-Object-Manifest': 'c/seg'},
            'manifest-contents')
        req = swob.Request.blank(
            '/v1/AUTH_test/mancon/manifest',
            environ={'REQUEST_METHOD': 'GET',
                     'QUERY_STRING': 'multipart-manifest=get'})
        status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)
        self.assertEqual(headers["Etag"], "manifest-etag")
        self.assertEqual(body, "manifest-contents")

    def test_error_passthrough(self):
        self.app.register(
            'GET', '/v1/AUTH_test/gone/404ed',
            swob.HTTPNotFound, {}, None)
        req = swob.Request.blank('/v1/AUTH_test/gone/404ed',
                                 environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_dlo(req)
        self.assertEqual(status, '404 Not Found')

    def test_get_range(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=8-17'})
        status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)
        self.assertEqual(status, "206 Partial Content")
        self.assertEqual(headers["Content-Length"], "10")
        self.assertEqual(body, "bbcccccddd")
        expected_etag = '"%s"' % md5hex(
            md5hex("aaaaa") + md5hex("bbbbb") + md5hex("ccccc") +
            md5hex("ddddd") + md5hex("eeeee"))
        self.assertEqual(headers.get("Etag"), expected_etag)

    def test_get_range_on_segment_boundaries(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=10-19'})
        status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)
        self.assertEqual(status, "206 Partial Content")
        self.assertEqual(headers["Content-Length"], "10")
        self.assertEqual(body, "cccccddddd")

    def test_get_range_first_byte(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=0-0'})
        status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)
        self.assertEqual(status, "206 Partial Content")
        self.assertEqual(headers["Content-Length"], "1")
        self.assertEqual(body, "a")

    def test_get_range_last_byte(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=24-24'})
        status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)
        self.assertEqual(status, "206 Partial Content")
        self.assertEqual(headers["Content-Length"], "1")
        self.assertEqual(body, "e")

    def test_get_range_overlapping_end(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=18-30'})
        status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)
        self.assertEqual(status, "206 Partial Content")
        self.assertEqual(headers["Content-Length"], "7")
        self.assertEqual(headers["Content-Range"], "bytes 18-24/25")
        self.assertEqual(body, "ddeeeee")

    def test_get_range_unsatisfiable(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=25-30'})
        status, headers, body = self.call_dlo(req)
        self.assertEqual(status, "416 Requested Range Not Satisfiable")
        expected_headers = (
            ('Accept-Ranges', 'bytes'),
            ('Content-Range', 'bytes */25'),
        )
        for header_value_pair in expected_headers:
            self.assertIn(header_value_pair, headers)

    def test_get_range_many_segments_satisfiable(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest-many-segments',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=3-12'})
        with mock.patch(LIMIT, 3):
            status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)
        self.assertEqual(status, "206 Partial Content")
        self.assertEqual(headers["Content-Length"], "10")
        # The /15 here indicates that this is a 15-byte object. DLO can't tell
        # if there are more segments or not without fetching more container
        # listings, though, so we just go with the sum of the lengths of the
        # segments we can see. In an ideal world, this would be "bytes 3-12/*"
        # to indicate that we don't know the full object length. However, RFC
        # 2616 section 14.16 explicitly forbids us from doing that:
        #
        #   A response with status code 206 (Partial Content) MUST NOT include
        #   a Content-Range field with a byte-range-resp-spec of "*".
        #
        # Since the truth is forbidden, we lie.
        self.assertEqual(headers["Content-Range"], "bytes 3-12/15")
        self.assertEqual(body, "aabbbbbccc")

        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/mancon/manifest-many-segments'),
             ('GET', '/v1/AUTH_test/c?prefix=seg_'),
             ('GET', '/v1/AUTH_test/c/seg_01?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/c/seg_02?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/c/seg_03?multipart-manifest=get')])

    def test_get_range_many_segments_satisfiability_unknown(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest-many-segments',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=10-22'})
        with mock.patch(LIMIT, 3):
            status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)
        self.assertEqual(status, "200 OK")
        # this requires multiple pages of container listing, so we can't send
        # a Content-Length header
        self.assertIsNone(headers.get("Content-Length"))
        self.assertEqual(body, "aaaaabbbbbcccccdddddeeeee")

    def test_get_suffix_range(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=-40'})
        status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)
        self.assertEqual(status, "206 Partial Content")
        self.assertEqual(headers["Content-Length"], "25")
        self.assertEqual(body, "aaaaabbbbbcccccdddddeeeee")

    def test_get_suffix_range_many_segments(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest-many-segments',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=-5'})
        with mock.patch(LIMIT, 3):
            status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)
        self.assertEqual(status, "200 OK")
        self.assertIsNone(headers.get("Content-Length"))
        self.assertIsNone(headers.get("Content-Range"))
        self.assertEqual(body, "aaaaabbbbbcccccdddddeeeee")

    def test_get_multi_range(self):
        # DLO doesn't support multi-range GETs. The way that you express that
        # in HTTP is to return a 200 response containing the whole entity.
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest-many-segments',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=5-9,15-19'})
        with mock.patch(LIMIT, 3):
            status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)
        self.assertEqual(status, "200 OK")
        self.assertIsNone(headers.get("Content-Length"))
        self.assertIsNone(headers.get("Content-Range"))
        self.assertEqual(body, "aaaaabbbbbcccccdddddeeeee")

    def test_if_match_matches(self):
        manifest_etag = '"%s"' % md5hex(
            md5hex("aaaaa") + md5hex("bbbbb") + md5hex("ccccc") +
            md5hex("ddddd") + md5hex("eeeee"))
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'If-Match': manifest_etag})

        status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Content-Length'], '25')
        self.assertEqual(body, 'aaaaabbbbbcccccdddddeeeee')

    def test_if_match_does_not_match(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'If-Match': 'not it'})

        status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '412 Precondition Failed')
        self.assertEqual(headers['Content-Length'], '0')
        self.assertEqual(body, '')

    def test_if_none_match_matches(self):
        manifest_etag = '"%s"' % md5hex(
            md5hex("aaaaa") + md5hex("bbbbb") + md5hex("ccccc") +
            md5hex("ddddd") + md5hex("eeeee"))
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'If-None-Match': manifest_etag})

        status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '304 Not Modified')
        self.assertEqual(headers['Content-Length'], '0')
        self.assertEqual(body, '')

    def test_if_none_match_does_not_match(self):
        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'If-None-Match': 'not it'})

        status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '200 OK')
        self.assertEqual(headers['Content-Length'], '25')
        self.assertEqual(body, 'aaaaabbbbbcccccdddddeeeee')

    def test_get_with_if_modified_since(self):
        # It's important not to pass the If-[Un]Modified-Since header to the
        # proxy for segment GET requests, as it may result in 304 Not Modified
        # responses, and those don't contain segment data.
        req = swob.Request.blank(
            '/v1/AUTH_test/mancon/manifest',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'If-Modified-Since': 'Wed, 12 Feb 2014 22:24:52 GMT',
                     'If-Unmodified-Since': 'Thu, 13 Feb 2014 23:25:53 GMT'})
        status, headers, body = self.call_dlo(req)

        for _, _, hdrs in self.app.calls_with_headers[1:]:
            self.assertFalse('If-Modified-Since' in hdrs)
            self.assertFalse('If-Unmodified-Since' in hdrs)

    def test_error_fetching_first_segment(self):
        self.app.register(
            'GET', '/v1/AUTH_test/c/seg_01',
            swob.HTTPForbidden, {}, None)

        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_dlo(req)
        self.assertEqual(status, "409 Conflict")
        self.assertEqual(self.dlo.logger.get_lines_for_level('error'), [
            'While processing manifest /v1/AUTH_test/mancon/manifest, '
            'got 403 while retrieving /v1/AUTH_test/c/seg_01',
        ])

    def test_error_fetching_second_segment(self):
        self.app.register(
            'GET', '/v1/AUTH_test/c/seg_02',
            swob.HTTPForbidden, {}, None)

        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, "200 OK")
        self.assertEqual(''.join(body), "aaaaa")  # first segment made it out
        self.assertEqual(self.dlo.logger.get_lines_for_level('error'), [
            'While processing manifest /v1/AUTH_test/mancon/manifest, '
            'got 403 while retrieving /v1/AUTH_test/c/seg_02',
        ])

    def test_error_listing_container_first_listing_request(self):
        self.app.register(
            'GET', '/v1/AUTH_test/c?prefix=seg_',
            swob.HTTPNotFound, {}, None)

        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest-many-segments',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=-5'})
        with mock.patch(LIMIT, 3):
            status, headers, body = self.call_dlo(req)
        self.assertEqual(status, "404 Not Found")

    def test_error_listing_container_second_listing_request(self):
        self.app.register(
            'GET', '/v1/AUTH_test/c?prefix=seg_&marker=seg_03',
            swob.HTTPNotFound, {}, None)

        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest-many-segments',
                                 environ={'REQUEST_METHOD': 'GET'},
                                 headers={'Range': 'bytes=-5'})
        with mock.patch(LIMIT, 3):
            status, headers, body = self.call_dlo(req)
        self.assertEqual(status, "200 OK")
        self.assertEqual(body, "aaaaabbbbbccccc")

    def test_error_listing_container_HEAD(self):
        self.app.register(
            'GET', '/v1/AUTH_test/c?prefix=seg_',
            # for example, if a manifest refers to segments in another
            # container, but the user is accessing the manifest via a
            # container-level tempurl key
            swob.HTTPUnauthorized, {}, None)

        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest-many-segments',
                                 environ={'REQUEST_METHOD': 'HEAD'})
        with mock.patch(LIMIT, 3):
            status, headers, body = self.call_dlo(req)
        self.assertEqual(status, "401 Unauthorized")
        self.assertEqual(body, b"")

    def test_mismatched_etag_fetching_second_segment(self):
        self.app.register(
            'GET', '/v1/AUTH_test/c/seg_02',
            swob.HTTPOk, {'Content-Length': '5', 'Etag': md5hex("bbbbb")},
            'bbWRONGbb')

        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, "200 OK")
        self.assertEqual(''.join(body), "aaaaabbWRONGbb")  # stop after error

    def test_etag_comparison_ignores_quotes(self):
        # a little future-proofing here in case we ever fix this in swob
        self.app.register(
            'HEAD', '/v1/AUTH_test/mani/festo',
            swob.HTTPOk, {'Content-Length': '0', 'Etag': 'blah',
                          'X-Object-Manifest': 'c/quotetags'}, None)
        self.app.register(
            'GET', '/v1/AUTH_test/c?prefix=quotetags',
            swob.HTTPOk, {'Content-Type': 'application/json; charset=utf-8'},
            json.dumps([{"hash": "\"abc\"", "bytes": 5, "name": "quotetags1",
                         "last_modified": "2013-11-22T02:42:14.261620",
                         "content-type": "application/octet-stream"},
                        {"hash": "def", "bytes": 5, "name": "quotetags2",
                         "last_modified": "2013-11-22T02:42:14.261620",
                         "content-type": "application/octet-stream"}]))

        req = swob.Request.blank('/v1/AUTH_test/mani/festo',
                                 environ={'REQUEST_METHOD': 'HEAD'})
        status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)
        self.assertEqual(headers["Etag"],
                         '"' + hashlib.md5("abcdef").hexdigest() + '"')

    def test_object_prefix_quoting(self):
        self.app.register(
            'GET', '/v1/AUTH_test/man/accent',
            swob.HTTPOk, {'Content-Length': '0', 'Etag': 'blah',
                          'X-Object-Manifest': u'c/é'.encode('utf-8')}, None)

        segs = [{"hash": md5hex("AAAAA"), "bytes": 5, "name": u"é1"},
                {"hash": md5hex("AAAAA"), "bytes": 5, "name": u"é2"}]
        self.app.register(
            'GET', '/v1/AUTH_test/c?prefix=%C3%A9',
            swob.HTTPOk, {'Content-Type': 'application/json'},
            json.dumps(segs))

        self.app.register(
            'GET', '/v1/AUTH_test/c/\xC3\xa91',
            swob.HTTPOk, {'Content-Length': '5', 'Etag': md5hex("AAAAA")},
            "AAAAA")
        self.app.register(
            'GET', '/v1/AUTH_test/c/\xC3\xA92',
            swob.HTTPOk, {'Content-Length': '5', 'Etag': md5hex("BBBBB")},
            "BBBBB")

        req = swob.Request.blank('/v1/AUTH_test/man/accent',
                                 environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_dlo(req)
        self.assertEqual(status, "200 OK")
        self.assertEqual(body, "AAAAABBBBB")

    def test_get_taking_too_long(self):
        the_time = [time.time()]

        def mock_time():
            return the_time[0]

        # this is just a convenient place to hang a time jump
        def mock_is_success(status_int):
            the_time[0] += 9 * 3600
            return status_int // 100 == 2

        req = swob.Request.blank(
            '/v1/AUTH_test/mancon/manifest',
            environ={'REQUEST_METHOD': 'GET'})

        with mock.patch('swift.common.request_helpers.time.time',
                        mock_time), \
                mock.patch('swift.common.request_helpers.is_success',
                           mock_is_success), \
                mock.patch.object(dlo, 'is_success', mock_is_success):
            status, headers, body = self.call_dlo(req)

        self.assertEqual(status, '200 OK')
        self.assertEqual(body, 'aaaaabbbbbccccc')

    def test_get_oversize_segment(self):
        # If we send a Content-Length header to the client, it's based on the
        # container listing. If a segment gets bigger by the time we get to it
        # (like if a client uploads a bigger segment w/the same name), we need
        # to not send anything beyond the length we promised. Also, we should
        # probably raise an exception.

        # This is now longer than the original seg_03+seg_04+seg_05 combined
        self.app.register(
            'GET', '/v1/AUTH_test/c/seg_03',
            swob.HTTPOk, {'Content-Length': '20', 'Etag': 'seg03-etag'},
            'cccccccccccccccccccc')

        req = swob.Request.blank(
            '/v1/AUTH_test/mancon/manifest',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '200 OK')  # sanity check
        self.assertEqual(headers.get('Content-Length'), '25')  # sanity check
        self.assertEqual(body, 'aaaaabbbbbccccccccccccccc')
        self.assertEqual(
            self.app.calls,
            [('GET', '/v1/AUTH_test/mancon/manifest'),
             ('GET', '/v1/AUTH_test/c?prefix=seg'),
             ('GET', '/v1/AUTH_test/c/seg_01?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/c/seg_02?multipart-manifest=get'),
             ('GET', '/v1/AUTH_test/c/seg_03?multipart-manifest=get')])

    def test_get_undersize_segment(self):
        # If we send a Content-Length header to the client, it's based on the
        # container listing. If a segment gets smaller by the time we get to
        # it (like if a client uploads a smaller segment w/the same name), we
        # need to raise an exception so that the connection will be closed by
        # the WSGI server. Otherwise, the WSGI server will be waiting for the
        # next request, the client will still be waiting for the rest of the
        # response, and nobody will be happy.

        # Shrink it by a single byte
        self.app.register(
            'GET', '/v1/AUTH_test/c/seg_03',
            swob.HTTPOk, {'Content-Length': '4', 'Etag': md5hex("cccc")},
            'cccc')

        req = swob.Request.blank(
            '/v1/AUTH_test/mancon/manifest',
            environ={'REQUEST_METHOD': 'GET'})
        status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '200 OK')  # sanity check
        self.assertEqual(headers.get('Content-Length'), '25')  # sanity check
        self.assertEqual(body, 'aaaaabbbbbccccdddddeeeee')

    def test_get_undersize_segment_range(self):
        # Shrink it by a single byte
        self.app.register(
            'GET', '/v1/AUTH_test/c/seg_03',
            swob.HTTPOk, {'Content-Length': '4', 'Etag': md5hex("cccc")},
            'cccc')

        req = swob.Request.blank(
            '/v1/AUTH_test/mancon/manifest',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Range': 'bytes=0-14'})
        status, headers, body = self.call_dlo(req)
        headers = HeaderKeyDict(headers)

        self.assertEqual(status, '206 Partial Content')  # sanity check
        self.assertEqual(headers.get('Content-Length'), '15')  # sanity check
        self.assertEqual(body, 'aaaaabbbbbcccc')

    def test_get_with_auth_overridden(self):
        auth_got_called = [0]

        def my_auth(req):
            auth_got_called[0] += 1
            return None

        req = swob.Request.blank('/v1/AUTH_test/mancon/manifest',
                                 environ={'REQUEST_METHOD': 'GET',
                                          'swift.authorize': my_auth})
        status, headers, body = self.call_dlo(req)
        self.assertTrue(auth_got_called[0] > 1)


class TestDloConfiguration(unittest.TestCase):
    """
    For backwards compatibility, we will read a couple of values out of the
    proxy's config section if we don't have any config values.
    """

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_skip_defaults_if_configured(self):
        # The presence of even one config value in our config section means we
        # won't go looking for the proxy config at all.
        proxy_conf = dedent("""
        [DEFAULT]
        bind_ip = 10.4.5.6

        [pipeline:main]
        pipeline = catch_errors dlo ye-olde-proxy-server

        [filter:catch_errors]
        use = egg:swift#catch_errors

        [filter:dlo]
        use = egg:swift#dlo
        max_get_time = 3600

        [app:ye-olde-proxy-server]
        use = egg:swift#proxy
        rate_limit_segments_per_sec = 7
        rate_limit_after_segment = 13
        max_get_time = 2900
        """)

        conffile = tempfile.NamedTemporaryFile()
        conffile.write(proxy_conf)
        conffile.flush()

        mware = dlo.filter_factory({
            'max_get_time': '3600',
            '__file__': conffile.name
        })("no app here")

        self.assertEqual(1, mware.rate_limit_segments_per_sec)
        self.assertEqual(10, mware.rate_limit_after_segment)
        self.assertEqual(3600, mware.max_get_time)

    def test_finding_defaults_from_file(self):
        # If DLO has no config vars, go pull them from the proxy server's
        # config section
        proxy_conf = dedent("""
        [DEFAULT]
        bind_ip = 10.4.5.6

        [pipeline:main]
        pipeline = catch_errors dlo ye-olde-proxy-server

        [filter:catch_errors]
        use = egg:swift#catch_errors

        [filter:dlo]
        use = egg:swift#dlo

        [app:ye-olde-proxy-server]
        use = egg:swift#proxy
        rate_limit_after_segment = 13
        set max_get_time = 2900
        """)

        conffile = tempfile.NamedTemporaryFile()
        conffile.write(proxy_conf)
        conffile.flush()

        mware = dlo.filter_factory({
            '__file__': conffile.name
        })("no app here")

        self.assertEqual(1, mware.rate_limit_segments_per_sec)
        self.assertEqual(13, mware.rate_limit_after_segment)
        self.assertEqual(2900, mware.max_get_time)

    def test_finding_defaults_from_dir(self):
        # If DLO has no config vars, go pull them from the proxy server's
        # config section
        proxy_conf1 = dedent("""
        [DEFAULT]
        bind_ip = 10.4.5.6

        [pipeline:main]
        pipeline = catch_errors dlo ye-olde-proxy-server
        """)

        proxy_conf2 = dedent("""
        [filter:catch_errors]
        use = egg:swift#catch_errors

        [filter:dlo]
        use = egg:swift#dlo

        [app:ye-olde-proxy-server]
        use = egg:swift#proxy
        rate_limit_after_segment = 13
        max_get_time = 2900
        """)

        conf_dir = self.tmpdir

        conffile1 = tempfile.NamedTemporaryFile(dir=conf_dir, suffix='.conf')
        conffile1.write(proxy_conf1)
        conffile1.flush()

        conffile2 = tempfile.NamedTemporaryFile(dir=conf_dir, suffix='.conf')
        conffile2.write(proxy_conf2)
        conffile2.flush()

        mware = dlo.filter_factory({
            '__file__': conf_dir
        })("no app here")

        self.assertEqual(1, mware.rate_limit_segments_per_sec)
        self.assertEqual(13, mware.rate_limit_after_segment)
        self.assertEqual(2900, mware.max_get_time)


if __name__ == '__main__':
    unittest.main()
