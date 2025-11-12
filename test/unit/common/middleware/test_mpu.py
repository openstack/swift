# Copyright (c) 2024 Nvidia
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
import hmac
import json
import unittest
import urllib
import urllib.parse
from copy import deepcopy

from unittest import mock

from swift.common import swob, registry
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.middleware import mpu
from swift.common.middleware.mpu import MPUMiddleware, \
    normalize_part_number, MPUSession, BaseMPUHandler, MPUEtagHasher, \
    byteranges_parts_iter
from swift.common.object_ref import ObjectRef, HistoryId, UploadId
from swift.common.swob import Request, HTTPOk, HTTPNotFound, HTTPCreated, \
    HTTPAccepted, HTTPServiceUnavailable, HTTPPreconditionFailed, \
    HTTPException, HTTPBadRequest, HTTPNoContent, HTTPInternalServerError, \
    HTTPConflict, HTTPPartialContent, HTTPRequestedRangeNotSatisfiable
from swift.common.utils import md5, quote, Timestamp, MD5_OF_EMPTY_STRING, \
    param_str_from_dict
from test.debug_logger import debug_logger
from test.unit import make_timestamp_iter
from test.unit.common.middleware.helpers import FakeSwift


class TestModuleFunctions(unittest.TestCase):
    def test_normalize_part_number(self):
        self.assertEqual('000001', normalize_part_number(1))
        self.assertEqual('000011', normalize_part_number('11'))
        self.assertEqual('000111', normalize_part_number('000111'))

    def test_byteranges_parts_iter(self):
        parts = [{'size': 10}, {'size': 11}, {'size': 12}]
        byte_ranges = [(0, -1)]
        it = byteranges_parts_iter(parts, byte_ranges)
        self.assertEqual([], list(it))

        byte_ranges = [(0, 0)]
        it = byteranges_parts_iter(parts, byte_ranges)
        self.assertEqual([(parts[0], 0, 0)], list(it))

        byte_ranges = [(0, 9)]
        it = byteranges_parts_iter(parts, byte_ranges)
        self.assertEqual([(parts[0], 0, 9)], list(it))

        byte_ranges = [(0, 10)]
        it = byteranges_parts_iter(parts, byte_ranges)
        self.assertEqual([(parts[0], 0, 9), (parts[1], 0, 0)], list(it))

        byte_ranges = [(9, 10)]
        it = byteranges_parts_iter(parts, byte_ranges)
        self.assertEqual([(parts[0], 9, 9), (parts[1], 0, 0)], list(it))

        byte_ranges = [(9, 20)]
        it = byteranges_parts_iter(parts, byte_ranges)
        self.assertEqual([(parts[0], 9, 9), (parts[1], 0, 10)], list(it))

        byte_ranges = [(9, 21)]
        it = byteranges_parts_iter(parts, byte_ranges)
        self.assertEqual(
            [(parts[0], 9, 9), (parts[1], 0, 10), (parts[2], 0, 0)], list(it))

        byte_ranges = [(9, 99)]
        it = byteranges_parts_iter(parts, byte_ranges)
        self.assertEqual(
            [(parts[0], 9, 9), (parts[1], 0, 10), (parts[2], 0, 11)], list(it))

        byte_ranges = [(32, 99)]
        it = byteranges_parts_iter(parts, byte_ranges)
        self.assertEqual([(parts[2], 11, 11)], list(it))

        byte_ranges = [(33, 99)]
        it = byteranges_parts_iter(parts, byte_ranges)
        self.assertEqual([], list(it))

        byte_ranges = [(0, 9), (11, 15)]
        it = byteranges_parts_iter(parts, byte_ranges)
        self.assertEqual([(parts[0], 0, 9), (parts[1], 1, 5)], list(it))

        byte_ranges = [(25, 26), (26, 28)]
        it = byteranges_parts_iter(parts, byte_ranges)
        self.assertEqual([(parts[2], 4, 5), (parts[2], 5, 7)], list(it))


class TestMPUSession(unittest.TestCase):
    def test_init(self):
        sess = MPUSession('mysession', Timestamp(123.45678))
        self.assertEqual('mysession', sess.name)
        self.assertEqual(Timestamp(123.45678), sess.meta_timestamp)
        self.assertEqual(Timestamp(123.45678), sess.data_timestamp)
        self.assertEqual('application/x-mpu-session-created',
                         sess.content_type)
        self.assertEqual({}, sess.headers)
        self.assertTrue(sess.is_active)
        self.assertFalse(sess.is_aborted)
        self.assertFalse(sess.is_completed)

    def test_init_non_default(self):
        headers = {'X-Object-Sysmeta-Mpu-Fruit': 'apple'}
        sess = MPUSession('mysession', Timestamp(123.45678),
                          content_type='application/x-mpu-session-aborted',
                          headers=headers)
        self.assertEqual('mysession', sess.name)
        self.assertEqual(Timestamp(123.45678), sess.meta_timestamp)
        self.assertEqual(Timestamp(123.45678), sess.data_timestamp)
        self.assertEqual('application/x-mpu-session-aborted',
                         sess.content_type)
        self.assertEqual(headers, sess.headers)
        self.assertFalse(sess.is_active)
        self.assertTrue(sess.is_aborted)
        self.assertFalse(sess.is_completed)

    def test_get_manifest_headers(self):
        headers = {
            'X-Object-Sysmeta-Mpu-History-Id': '-null-&$&9999987654.99999',
            'X-Object-Sysmeta-Mpu-User-X-Object-Meta-Fruit': 'apple',
            'X-Object-Sysmeta-Mpu-Content-Type': 'user/type',
            'X-Object-Sysmeta-Mpu-User-Content-Disposition': 'attachment',
            'X-Object-Sysmeta-Mpu-User-Content-Encoding': 'none',
            'X-Object-Sysmeta-Mpu-User-Content-Language': 'en-US',
            'X-Object-Sysmeta-Mpu-User-Cache-Control': 'no-cache',
            'X-Object-Sysmeta-Mpu-User-Expires':
                'Wed, 25 Dec 2024 04:04:04 GMT',
        }
        sess = MPUSession('mysession', Timestamp(123.45678), headers=headers)
        exp = {
            'X-Object-Meta-Fruit': 'apple',
            'Content-Disposition': 'attachment',
            'Content-Encoding': 'none',
            'Content-Language': 'en-US',
            'Cache-Control': 'no-cache',
            'Expires': 'Wed, 25 Dec 2024 04:04:04 GMT',
            'Content-Type': 'user/type',
        }
        self.assertEqual(exp, sess.get_manifest_headers())

    def test_from_user_headers(self):
        headers = {
            'X-Object-Sysmeta-Mpu-History-Id': '-null-&$&9999987654.99999',
            'x-object-meta-fruit': 'apple',
            'x-timestamp': '12345',
            'content-type': 'user/type',
            'Content-Disposition': 'attachment',
            'Content-Encoding': 'none',
            'Content-Language': 'en-US',
            'Cache-Control': 'no-cache',
            'Expires': 'Wed, 25 Dec 2024 04:04:04 GMT',
        }
        sess = MPUSession.from_user_headers('mysession', headers=headers)
        self.assertEqual(Timestamp(12345), sess.meta_timestamp)
        self.assertEqual(Timestamp(12345), sess.data_timestamp)
        self.assertEqual('application/x-mpu-session-created',
                         sess.content_type)
        exp_sess_headers = {
            'X-Object-Sysmeta-Mpu-History-Id': '-null-&$&9999987654.99999',
            'X-Object-Sysmeta-Mpu-Content-Type': 'user/type',
            'X-Object-Sysmeta-Mpu-User-X-Object-Meta-Fruit': 'apple',
            'X-Object-Sysmeta-Mpu-User-Content-Disposition': 'attachment',
            'X-Object-Sysmeta-Mpu-User-Content-Encoding': 'none',
            'X-Object-Sysmeta-Mpu-User-Content-Language': 'en-US',
            'X-Object-Sysmeta-Mpu-User-Cache-Control': 'no-cache',
            'X-Object-Sysmeta-Mpu-User-Expires':
                'Wed, 25 Dec 2024 04:04:04 GMT',
        }
        self.assertEqual(exp_sess_headers, sess.headers)
        self.assertFalse(sess.is_aborted)
        self.assertFalse(sess.is_completed)

        exp_put_headers = {
            'Content-Length': '0',
            'Content-Type': 'application/x-mpu-session-created',
            'X-Timestamp': '0000012345.00000',
            'X-Object-Sysmeta-Mpu-History-Id': '-null-&$&9999987654.99999',
            'X-Object-Sysmeta-Mpu-Content-Type': 'user/type',
            'X-Object-Sysmeta-Mpu-User-X-Object-Meta-Fruit': 'apple',
            'X-Object-Sysmeta-Mpu-User-Content-Disposition': 'attachment',
            'X-Object-Sysmeta-Mpu-User-Content-Encoding': 'none',
            'X-Object-Sysmeta-Mpu-User-Content-Language': 'en-US',
            'X-Object-Sysmeta-Mpu-User-Cache-Control': 'no-cache',
            'X-Object-Sysmeta-Mpu-User-Expires':
                'Wed, 25 Dec 2024 04:04:04 GMT',
        }
        self.assertEqual(exp_put_headers, sess.get_put_headers())
        exp_post_headers = {
            'Content-Type': 'application/x-mpu-session-created',
            'X-Timestamp': '0000012345.00000'}
        self.assertEqual(exp_post_headers, sess.get_post_headers())

    def test_from_session_headers(self):
        headers = HeaderKeyDict({
            'Content-Length': '0',
            'Content-Type': 'application/x-mpu-session-aborted',
            'X-Backend-Data-Timestamp': '0000012345.00000',
            'X-Timestamp': '0000067890.00000',
            'X-Object-Sysmeta-Mpu-History-Id': '-null-&$&9999987654.99999',
            'X-Object-Sysmeta-Mpu-Content-Type': 'user/type',
            'X-Object-Sysmeta-Mpu-User-X-Object-Meta-Fruit': 'apple',
            'X-Object-Sysmeta-Mpu-User-Content-Disposition': 'attachment',
            'X-Object-Sysmeta-Mpu-User-Content-Encoding': 'none',
            'X-Object-Sysmeta-Mpu-User-Content-Language': 'en-US',
            'X-Object-Sysmeta-Mpu-User-Cache-Control': 'no-cache',
            'X-Object-Sysmeta-Mpu-User-Expires':
                'Wed, 25 Dec 2024 04:04:04 GMT',
        })
        sess = MPUSession.from_session_headers('mysession', headers)
        self.assertEqual(Timestamp(67890), sess.meta_timestamp)
        self.assertEqual(Timestamp(12345), sess.data_timestamp)
        self.assertEqual('application/x-mpu-session-aborted',
                         sess.content_type)
        exp_post_headers = {
            'Content-Type': 'application/x-mpu-session-aborted',
            'X-Timestamp': '0000067890.00000'}
        self.assertEqual(exp_post_headers, sess.get_post_headers())
        self.assertEqual(12345.0, sess.history_id.timestamp)
        self.assertTrue(sess.history_id.null)

    def test_set_completed(self):
        sess = MPUSession('mysession', Timestamp(123.45678))
        exp_post_headers = {
            'Content-Type': 'application/x-mpu-session-created',
            'X-Timestamp': '0000000123.45678'}
        self.assertEqual(exp_post_headers, sess.get_post_headers())
        self.assertTrue(sess.is_active)
        self.assertFalse(sess.is_completed)

        sess.set_completed(Timestamp(345))
        exp_post_headers = {
            'Content-Type': 'application/x-mpu-session-completed',
            'X-Timestamp': '0000000345.00000'}
        self.assertEqual(exp_post_headers, sess.get_post_headers())
        self.assertFalse(sess.is_active)
        self.assertTrue(sess.is_completed)

    def test_set_completing(self):
        sess = MPUSession('mysession', Timestamp(123.45678))
        exp_post_headers = {
            'Content-Type': 'application/x-mpu-session-created',
            'X-Timestamp': '0000000123.45678'}
        self.assertEqual(exp_post_headers, sess.get_post_headers())
        self.assertTrue(sess.is_active)
        self.assertFalse(sess.is_completing)

        sess.set_completing(Timestamp(345))
        exp_post_headers = {
            'Content-Type': 'application/x-mpu-session-completing',
            'X-Timestamp': '0000000345.00000'}
        self.assertEqual(exp_post_headers, sess.get_post_headers())
        self.assertTrue(sess.is_active)
        self.assertTrue(sess.is_completing)

    def test_set_aborted(self):
        sess = MPUSession('mysession', Timestamp(123.45678))
        exp_post_headers = {
            'Content-Type': 'application/x-mpu-session-created',
            'X-Timestamp': '0000000123.45678'}
        self.assertEqual(exp_post_headers, sess.get_post_headers())
        self.assertTrue(sess.is_active)
        self.assertFalse(sess.is_aborted)

        sess.set_aborted(Timestamp(345))
        exp_post_headers = {
            'Content-Type': 'application/x-mpu-session-aborted',
            'X-Timestamp': '0000000345.00000'}
        self.assertEqual(exp_post_headers, sess.get_post_headers())
        self.assertFalse(sess.is_active)
        self.assertTrue(sess.is_aborted)


class BaseTestMPUMiddleware(unittest.TestCase):
    # TODO: assert 'X-Backend-Allow-Reserved-Names' in backend requests
    def setUp(self):
        self.app = FakeSwift()
        self.ts_iter = make_timestamp_iter()
        self.ts_now = next(self.ts_iter)
        self.signing_key = b'key'
        self.obj_name = 'o'
        self.debug_logger = debug_logger()
        self.exp_calls = []
        self._setup_user_ac_info_requests()
        self.mw_conf = {
            'upload_id_key':
                base64.b64encode(self.signing_key).decode('ascii'),
        }
        self.mw = MPUMiddleware(self.app, self.mw_conf,
                                logger=self.debug_logger)

    @property
    def upload_id(self):
        return UploadId(self.ts_now)

    @property
    def session_ref(self):
        return ObjectRef(self.obj_name, self.upload_id)

    @property
    def session_name(self):
        return self.session_ref.serialize()

    @property
    def session_name_wsgi(self):
        return swob.str_to_wsgi(self.session_ref.serialize())

    @property
    def history_id(self):
        return HistoryId(Timestamp.max(), null=True)

    @property
    def history_ref(self):
        return ObjectRef(self.obj_name, self.history_id)

    @property
    def version_name(self):
        return self.history_ref.serialize()

    @property
    def external_upload_id(self):
        return self.mw.externalize_upload_id(
            '/v1/a/c/' + quote(self.obj_name), self.upload_id)

    def _setup_user_ac_info_requests(self):
        ac_info_calls = [('HEAD', '/v1/a', HTTPOk, {}),
                         ('HEAD', '/v1/a/c', HTTPOk, {})]
        for call in ac_info_calls:
            self.app.register(*call)
        self.exp_calls.extend(ac_info_calls)

    def _setup_mpu_existence_check_call(
            self, ts_session, ts_meta=None, extra_headers=None):
        ts_meta = ts_meta or next(self.ts_iter)
        headers = HeaderKeyDict({
            'X-Timestamp': ts_meta.internal,
            'Content-Type': 'application/x-mpu-session-created',
            'X-Backend-Data-Timestamp': ts_session.internal,
            'X-Object-Sysmeta-Mpu-History-Id': self.history_id.serialize(),
            'X-Object-Sysmeta-Mpu-User-X-Object-Meta-Foo': 'blah',
            'X-Object-Sysmeta-Mpu-Content-Type': 'application/test',
        })
        headers.update(extra_headers or {})
        call = ('HEAD',
                '/v1/.a/\x00mpu_sessions\x00c/%s' % self.session_name_wsgi,
                HTTPOk, headers)
        self.app.register(*call)
        self.exp_calls.append(call)


class TestBaseMpuHandler(BaseTestMPUMiddleware):
    def test_make_subrequest(self):
        req = Request.blank(path='/v1/a/c/%s?orig=blah' % quote(self.obj_name),
                            headers={'Content-Type': 'foo'}, )
        self.assertEqual({'Host': 'localhost:80',
                          'Content-Type': 'foo'},
                         dict(req.headers))
        self.assertEqual({'orig': 'blah'}, req.params)

        handler = BaseMPUHandler(self.mw, req)
        subreq = handler.make_subrequest(
            'POST', 'new/path',
            headers={'X-Extra': 'extra'},
            params={'added': 'test'})

        self.assertEqual({'Host': 'localhost:80',
                          'User-Agent': 'Swift',
                          'X-Backend-Allow-Reserved-Names': 'true',
                          'X-Extra': 'extra'},
                         dict(subreq.headers))
        self.assertEqual({'added': 'test'}, subreq.params)

    def test_translate_error_response_400(self):
        req = Request.blank('/v1/a/c/o')
        resp = HTTPBadRequest()
        handler = BaseMPUHandler(self.mw, req)
        actual = handler.translate_error_response(resp)
        self.assertIsNot(resp, actual)
        self.assertIsInstance(actual, HTTPException)
        self.assertEqual(400, actual.status_int)
        self.assertIs(req, actual.request)

    def test_translate_error_response_503(self):
        req = Request.blank('/v1/a/c/o')
        resp = HTTPServiceUnavailable()
        handler = BaseMPUHandler(self.mw, req)
        actual = handler.translate_error_response(resp)
        self.assertIsNot(resp, actual)
        self.assertIsInstance(actual, HTTPException)
        self.assertEqual(503, actual.status_int)
        self.assertIs(req, actual.request)

    def test_translate_error_response_404(self):
        req = Request.blank('/v1/a/c/o')
        resp = HTTPNotFound()
        handler = BaseMPUHandler(self.mw, req)
        actual = handler.translate_error_response(resp)
        self.assertIsNot(resp, actual)
        self.assertIsInstance(actual, HTTPException)
        self.assertEqual(503, actual.status_int)
        self.assertIs(req, actual.request)

    def test_translate_error_response_567(self):
        req = Request.blank('/v1/a/c/o')
        resp = HTTPException()
        resp.status_int = 567
        handler = BaseMPUHandler(self.mw, req)
        actual = handler.translate_error_response(resp)
        self.assertIsNot(resp, actual)
        self.assertIsInstance(actual, HTTPException)
        self.assertEqual(503, actual.status_int)
        self.assertIs(req, actual.request)


class TestMPUMiddleware(BaseTestMPUMiddleware):
    # TODO: make all these test pass with a utf8 obj name
    def setUp(self):
        super().setUp()
        self.sample_in_progress_session_listing = [
            # in progress
            {'name': ObjectRef(
                'obj1',
                UploadId(next(self.ts_iter))).serialize(),
             'hash': 'etag',
             'bytes': 0,
             'content_type': 'application/x-mpu-session-created',
             'last_modified': '1970-01-01T00:00:00.000000'},
            {'name': ObjectRef(
                'obj2',
                UploadId(next(self.ts_iter))).serialize(),
             'hash': 'etag',
             'bytes': 0,
             'content_type': 'application/x-mpu-session-created',
             'last_modified': '1970-01-01T00:00:00.000000'},
            {'name': ObjectRef(
                'obj2\N{SNOWMAN}',
                UploadId(next(self.ts_iter))).serialize(),
             'hash': 'etag',
             'bytes': 0,
             'content_type': 'application/x-mpu-session-created',
             'last_modified': '1970-01-01T00:00:00.000000'},
            {'name': ObjectRef(
                'obj2\N{SNOWMAN}',
                UploadId(next(self.ts_iter))).serialize(),
             'hash': 'etag',
             'bytes': 0,
             'content_type': 'application/x-mpu-session-created',
             'last_modified': '1970-01-01T00:00:00.000000'},
        ]
        self.sample_all_session_listing = \
            self.sample_in_progress_session_listing + [
                # aborted
                {'name': ObjectRef(
                    'obj33\N{SNOWMAN}',
                    UploadId(next(self.ts_iter))).serialize(),
                 'hash': 'etag',
                 'bytes': 0,
                 'content_type': 'application/x-mpu-session-aborted',
                 'last_modified': '1970-01-01T00:00:00.000000'},
                # completed
                {'name': ObjectRef(
                    'obj33\N{SNOWMAN}',
                    UploadId(next(self.ts_iter))).serialize(),
                 'hash': 'etag',
                 'bytes': 0,
                 'content_type': 'application/x-mpu-session-completed',
                 'last_modified': '1970-01-01T00:00:00.000000'},
            ]

        self.sample_part1_body = b'1' * 5 * 1024 * 1024
        self.sample_part2_body = b'2' * 99

    def _make_parts_check_calls(self, manifest):
        """
        Return argument tuples for FakeSwift call registrations for HEAD
        requests to parts in the given manifest.

        :param manifest: a list of part dicts
        :return: a list of argument tuples for FakeSwift call registrations
        """
        calls = [
            ('HEAD',
             swob.str_to_wsgi('/v1/%s' % part['path']),
             HTTPOk,
             {'X-Backend-Timestamp': part['timestamp'],
              'Content-Length': part['size'],
              'ETag': '%s' % part['etag']})
            for part in manifest]
        return calls

    def _make_sample_part_dicts(self, part_numbers=None):
        """
        Return a list of dictionaries, one for each part number, that is
        equivalent to an internal manifest. The list can be passed to
        _make_parts_check_calls.
        """
        part_numbers = part_numbers or [1, 2]
        return [
            {'path': 'a/\x00mpu_parts\x00c/%s/%06d'
                     % (self.session_ref.serialize(), part_numbers[0]),
             'part_number': part_numbers[0],
             'etag': md5(self.sample_part1_body).hexdigest(),
             'timestamp': next(self.ts_iter).internal,
             'size': len(self.sample_part1_body)},
            {'path': 'a/\x00mpu_parts\x00c/%s/%06d'
                     % (self.session_ref.serialize(), part_numbers[1]),
             'part_number': part_numbers[1],
             'etag': md5(self.sample_part2_body).hexdigest(),
             'timestamp': next(self.ts_iter).internal,
             'size': len(self.sample_part2_body)}
        ]

    @staticmethod
    def _user_manifest_for_part_dicts(part_dicts):
        """
        Transform an internal manifest to the equivalent abbreviated user
        manifest.

        :param part_dicts: a list of part dicts
        :return: a list of abbreviated part dicts
        """
        return [
            dict(part_number=part_dict['part_number'], etag=part_dict['etag'])
            for part_dict in part_dicts
        ]

    def _make_sample_user_manifest(self):
        return self._user_manifest_for_part_dicts(
            self._make_sample_part_dicts()
        )

    def _calculate_mpu_etag(self, user_manifest):
        hasher = MPUEtagHasher()
        for item in user_manifest:
            hasher.update(item['etag'])
        return hasher.etag

    def _make_manifest_resp_hdrs(self, manifest):
        mpu_size = sum(p['size'] for p in manifest)
        manifest_body = json.dumps(manifest).encode('ascii')
        return {
            'Content-Type': 'application/test',
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
            'X-Timestamp': '1727184150.29665',
            'Accept-Ranges': 'bytes',
            'Content-Length': str(len(manifest_body)),
            'X-Manifest-Etag': 'b871773cf02434d498517245c7b88c11',
            'X-Trans-Id': 'test-txn-id',
            'X-Openstack-Request-Id': 'test-txn-id',
            'Date': 'Tue, 24 Sep 2024 13:22:30 GMT',
            'Etag': '"de64a5af184e6f732a26328c13d4ab25"',
            'X-Object-Sysmeta-Mpu-Etag': 'mpu-etag',
            'X-Object-Sysmeta-Mpu-Size': str(mpu_size),
            'X-Object-Sysmeta-Mpu-Parts-Count': '2',
            'X-Object-Sysmeta-Mpu-Max-Manifest-Part': '2',
            'X-Object-Sysmeta-Mpu-Upload-Id': self.upload_id.serialize(),
        }

    def _setup_mpu_create_requests(self):
        """
        Register FakeSwift calls to handle account and container requests
        during a CreateMpu request.
        """
        registered = [
            ('HEAD', '/v1/.a/\x00mpu_sessions\x00c', HTTPNotFound, {}),
            ('PUT', '/v1/.a/\x00mpu_sessions\x00c', HTTPCreated, {}),
            ('HEAD', '/v1/a/\x00mpu_parts\x00c', HTTPNotFound, {}),
            ('PUT', '/v1/a/\x00mpu_parts\x00c', HTTPCreated, {}),
            ('HEAD', '/v1/.a/\x00history\x00c', HTTPNotFound, {}),
            ('PUT', '/v1/.a/\x00history\x00c', HTTPCreated, {}),
            ('POST', '/v1/a/c', HTTPAccepted, {}),
        ]
        for call in registered:
            self.app.register(*call)
        expected = [
            ('HEAD', '/v1/a'),
            ('HEAD', '/v1/a/c'),
        ]
        expected += [call[:2] for call in registered]
        return expected

    def test_filter_factory_default_conf(self):
        app = object()
        mw = mpu.filter_factory({})(app)
        self.assertIsInstance(mw, MPUMiddleware)
        self.assertIs(app, mw.app)
        self.assertEqual(10000, mw.max_part_number)
        self.assertEqual(5242880, mw.min_part_size)
        self.assertEqual(983, mw.max_name_length)
        self.assertEqual({'max_part_number': 10000,
                          'min_part_size': 5242880,
                          'max_name_length': 983},
                         registry.get_swift_info().get('mpu'))

    def test_filter_factory_custom_conf(self):
        def do_test(conf):
            app = object()
            mw = mpu.filter_factory(conf)(app)
            self.assertIsInstance(mw, MPUMiddleware)
            self.assertIs(app, mw.app)
            self.assertEqual(999, mw.max_part_number)
            self.assertEqual(1048576, mw.min_part_size)
            self.assertEqual(983, mw.max_name_length)
            self.assertEqual({'max_part_number': 999,
                              'min_part_size': 1048576,
                              'max_name_length': 983},
                             registry.get_swift_info().get('mpu'))

        do_test({'min_part_size': 1048576,
                 'max_part_number': 999})
        do_test({'min_part_size': '1048576',
                 'max_part_number': '999'})

    def test_filter_factory_invalid_conf(self):
        def do_test(conf):
            with self.assertRaises(ValueError):
                mpu.filter_factory(conf)(object())

        do_test({'min_part_size': 0})
        do_test({'min_part_size': '0'})
        do_test({'min_part_size': '-1'})

    def test_externalize_internalize_upload_id(self):
        orig_obj_id = UploadId(Timestamp(123.45678))
        self.assertEqual('0000000123.45678&$', orig_obj_id.serialize())
        ext_id_str = self.mw.externalize_upload_id('/v1/a/c/o', orig_obj_id)
        exp_tag = hmac.new(
            self.mw.upload_id_signing_key,
            b'a/c/o0000000123.45678&$', 'sha256').hexdigest()
        self.assertEqual(
            base64.b64encode(
                (orig_obj_id.serialize() + exp_tag).encode('utf-8')
            ).decode('utf-8'), ext_id_str)

        internalized_obj_id = self.mw.internalize_upload_id(
            '/v1/a/c/o', ext_id_str)
        self.assertEqual(str(orig_obj_id), str(internalized_obj_id))
        self.assertEqual('0000000123.45678&$', str(internalized_obj_id))

    def test_get_upload_id_param_absent(self):
        req = Request.blank('/v1/a/c/o')
        self.assertIsNone(self.mw.get_valid_upload_id(req))

    def test_get_upload_id(self):
        def do_test(path):
            ts = Timestamp.now()
            quoted_path = quote(path)
            upload_id = UploadId(ts)
            req = Request.blank(quoted_path)
            ext_upload_id = self.mw.externalize_upload_id(req.path, upload_id)
            req.params = {'upload-id': ext_upload_id}
            req_upload_id = self.mw.get_valid_upload_id(req)
            self.assertEqual(upload_id, req_upload_id)
            self.assertEqual(ts, req_upload_id.timestamp)

        do_test('/v1/a/c/o')
        do_test('/v1/a/c/o\N{SNOWMAN}')

    def test_get_upload_id_invalid(self):
        def do_test_bad_value(value):
            with self.assertRaises(HTTPException) as cm:
                req = Request.blank('/v1/a/c/o', params={'upload-id': value})
                self.mw.get_valid_upload_id(req)
            self.assertEqual(400, cm.exception.status_int)

        do_test_bad_value('')
        do_test_bad_value(None)
        do_test_bad_value('my-uuid')
        do_test_bad_value('my-uuid:')
        do_test_bad_value(':%s' % Timestamp.now().internal)
        do_test_bad_value('my-uuid:xyz')

    def _do_test_create_mpu(self, req_headers):
        expected = self._setup_mpu_create_requests()
        registered_calls = [
            ('PUT', '/v1/.a/\x00mpu_sessions\x00c/%s' % self.session_name_wsgi,
             HTTPCreated, {}),
            ('PUT', '/v1/a/\x00mpu_parts\x00c/%s/' % self.session_name_wsgi,
             HTTPCreated, {})
        ]
        for call in registered_calls:
            self.app.register(*call)
            expected += [call[:2]]
        req = Request.blank('/v1/a/c/%s?uploads=true' % quote(self.obj_name),
                            headers=req_headers)
        req.method = 'POST'
        with mock.patch('swift.common.utils.Timestamp.now',
                        return_value=self.ts_now):
            resp = req.get_response(self.mw)
        self.assertEqual(202, resp.status_int)
        self.assertIs(req, resp.request)
        exp_id = base64.urlsafe_b64decode(
            self.external_upload_id.encode('utf-8'))
        actual_id = base64.urlsafe_b64decode(
            resp.headers.get('X-Upload-Id').encode('utf-8'))
        self.assertEqual(exp_id, actual_id)
        self.assertEqual(expected, self.app.calls)

    def test_create_mpu(self):
        req_headers = {'X-Object-Meta-Foo': 'blah'}
        self._do_test_create_mpu(req_headers)
        self.assertEqual(
            {'Content-Length': '0',
             'Content-Type': 'application/x-mpu-session-created',
             'X-Timestamp': self.ts_now.internal,
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Object-Sysmeta-Mpu-History-Id': self.history_id.serialize(),
             'X-Object-Sysmeta-Mpu-User-X-Object-Meta-Foo': 'blah',
             'X-Object-Sysmeta-Mpu-Content-Type': 'application/octet-stream'},
            self.app.headers[-2])
        # TODO: assert the lifeline object PUT headers

    def test_create_mpu_utf8(self):
        self.obj_name = 'o\N{SNOWMAN}x'
        req_headers = {'X-Object-Meta-Foo': 'blah'}
        self._do_test_create_mpu(req_headers)
        self.assertEqual(
            {'Content-Length': '0',
             'Content-Type': 'application/x-mpu-session-created',
             'X-Timestamp': self.ts_now.internal,
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Object-Sysmeta-Mpu-History-Id': self.history_id.serialize(),
             'X-Object-Sysmeta-Mpu-User-X-Object-Meta-Foo': 'blah',
             'X-Object-Sysmeta-Mpu-Content-Type': 'application/octet-stream'},
            self.app.headers[-2])

    def test_create_mpu_name_too_long(self):
        mpu_name = 'x' * 1024
        req = Request.blank('/v1/a/c/%s?uploads=true' % mpu_name)
        req.method = 'POST'
        resp = req.get_response(self.mw)
        self.assertEqual(400, resp.status_int)
        self.assertEqual(b'MPU object name length of 1024 longer than 983',
                         resp.body)

    def test_create_mpu_with_content_type(self):
        headers = {'X-Object-Meta-Foo': 'blah',
                   'content-Type': 'application/test'}
        self._do_test_create_mpu(headers)
        self.assertEqual(
            {'Content-Length': '0',
             'Content-Type': 'application/x-mpu-session-created',
             'X-Timestamp': mock.ANY,
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Object-Sysmeta-Mpu-History-Id': self.history_id.serialize(),
             'X-Object-Sysmeta-Mpu-User-X-Object-Meta-Foo': 'blah',
             'X-Object-Sysmeta-Mpu-Content-Type': 'application/test'},
            self.app.headers[-2])

    def test_create_mpu_detects_content_type(self):
        self.obj_name = 'o.html'
        headers = {'X-Object-Meta-Foo': 'blah'}
        self._do_test_create_mpu(headers)
        self.assertEqual(
            {'Content-Length': '0',
             'Content-Type': 'application/x-mpu-session-created',
             'X-Timestamp': mock.ANY,
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Object-Sysmeta-Mpu-History-Id': self.history_id.serialize(),
             'X-Object-Sysmeta-Mpu-User-X-Object-Meta-Foo': 'blah',
             'X-Object-Sysmeta-Mpu-Content-Type': 'text/html'},
            self.app.headers[-2])

    def test_create_mpu_with_x_timestamp(self):
        expected = self._setup_mpu_create_requests()
        req_headers = {'X-Object-Meta-Foo': 'blah',
                       'X-Timestamp': self.ts_now.internal}
        registered_calls = [
            ('PUT', '/v1/.a/\x00mpu_sessions\x00c/%s' % self.session_name_wsgi,
             HTTPCreated, {}),
            ('PUT', '/v1/a/\x00mpu_parts\x00c/%s/' % self.session_name_wsgi,
             HTTPCreated, {})
        ]
        for call in registered_calls:
            self.app.register(*call)
            expected += [call[:2]]
        req = Request.blank('/v1/a/c/%s?uploads=true' % self.obj_name,
                            headers=req_headers)
        req.method = 'POST'
        # note: no mocking Timestamp.now for ObjectId because request
        # timestamp is used
        resp = req.get_response(self.mw)
        self.assertEqual(202, resp.status_int)
        self.assertIs(req, resp.request)
        self.assertIn('X-Upload-Id', resp.headers)
        self.assertEqual(expected, self.app.calls)
        self.assertEqual(
            {'Content-Length': '0',
             'Content-Type': 'application/x-mpu-session-created',
             'X-Timestamp': self.ts_now.internal,
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Object-Sysmeta-Mpu-History-Id': self.history_id.serialize(),
             'X-Object-Sysmeta-Mpu-User-X-Object-Meta-Foo': 'blah',
             'X-Object-Sysmeta-Mpu-Content-Type': 'application/octet-stream'},
            self.app.headers[-2])

    def test_create_mpu_existing_resource_containers(self):
        user_container_headers = {
            'X-Container-Sysmeta-Mpu-Parts-Container-0':
                swob.wsgi_quote('\x00mpu_parts\x00c'),
            'X-Container-Sysmeta-History-Container':
                swob.wsgi_quote('\x00history\x00c')
        }
        registered = [
            ('HEAD', '/v1/a', HTTPOk, {}),
            ('HEAD', '/v1/a/c', HTTPNoContent, user_container_headers),
            ('HEAD', '/v1/.a/\x00mpu_sessions\x00c', HTTPNoContent, {}),
            ('HEAD', '/v1/a/\x00mpu_parts\x00c', HTTPNoContent, {}),
            ('HEAD', '/v1/.a/\x00history\x00c', HTTPNoContent, {}),
            ('PUT', '/v1/.a/\x00mpu_sessions\x00c/%s' % self.session_name_wsgi,
             HTTPCreated, {}),
            ('PUT', '/v1/a/\x00mpu_parts\x00c/%s/' % self.session_name_wsgi,
             HTTPCreated, {})
        ]
        for call in registered:
            self.app.register(*call)
        expected = [call[:2] for call in registered]
        req = Request.blank('/v1/a/c/o?uploads=true',
                            headers={'X-Timestamp': self.ts_now.internal})
        req.method = 'POST'
        resp = req.get_response(self.mw)
        self.assertEqual(202, resp.status_int)
        self.assertIs(req, resp.request)
        self.assertIn('X-Upload-Id', resp.headers)
        self.assertEqual(expected, self.app.calls)

    def test_create_mpu_fails_to_create_parts_container(self):
        expected = self._setup_mpu_create_requests()[:-3]
        # replace previously registered call
        self.app.register(
            'PUT', '/v1/a/\x00mpu_parts\x00c', HTTPInternalServerError, {})
        req = Request.blank('/v1/a/c/%s?uploads=true' % self.obj_name)
        req.method = 'POST'
        resp = req.get_response(self.mw)
        self.assertEqual(500, resp.status_int)
        self.assertEqual(b'Error creating MPU resource container', resp.body)
        self.assertIs(req, resp.request)
        self.assertEqual(expected, self.app.calls)

    def test_create_mpu_fails_to_create_sessions_container(self):
        expected = self._setup_mpu_create_requests()[:-5]
        # replace previously registered call
        self.app.register(
            'PUT', '/v1/.a/\x00mpu_sessions\x00c', HTTPInternalServerError, {})
        req = Request.blank('/v1/a/c/%s?uploads=true' % self.obj_name)
        req.method = 'POST'
        resp = req.get_response(self.mw)
        self.assertEqual(500, resp.status_int)
        self.assertEqual(b'Error creating MPU resource container', resp.body)
        self.assertIs(req, resp.request)
        self.assertEqual(expected, self.app.calls)

    def test_create_mpu_fails_to_post_to_user_container(self):
        expected = self._setup_mpu_create_requests()
        # replace previously registered call
        self.app.register(
            'POST', '/v1/a/c', HTTPInternalServerError, {})
        req = Request.blank('/v1/a/c/%s?uploads=true' % self.obj_name)
        req.method = 'POST'
        resp = req.get_response(self.mw)
        self.assertEqual(500, resp.status_int)
        self.assertEqual(b'Error writing MPU resource metadata', resp.body)
        self.assertIs(req, resp.request)
        self.assertEqual(expected, self.app.calls)

    def test_create_mpu_fails_to_create_session(self):
        expected = self._setup_mpu_create_requests()
        registered_calls = [
            ('PUT', '/v1/.a/\x00mpu_sessions\x00c/%s' % self.session_name_wsgi,
             HTTPServiceUnavailable, {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
            expected += [call[:2]]
        req = Request.blank('/v1/a/c/%s?uploads=true' % self.obj_name,
                            headers={'X-Timestamp': self.ts_now.internal})
        req.method = 'POST'
        with mock.patch('swift.common.utils.Timestamp.now',
                        return_value=self.ts_now):
            resp = req.get_response(self.mw)
        self.assertEqual(503, resp.status_int)
        self.assertIs(req, resp.request)
        exp_body = b''.join(HTTPServiceUnavailable()({}, lambda *args: None))
        self.assertEqual(exp_body, resp.body)
        self.assertEqual(expected, self.app.calls)

    def test_list_uploads(self):
        registered_calls = [
            ('GET', '/v1/.a/\x00mpu_sessions\x00c', HTTPOk, {},
             json.dumps(self.sample_all_session_listing).encode('ascii')),
            ('GET', '/v1/.a/\x00mpu_sessions\x00c', HTTPOk, {},
             json.dumps([]).encode('ascii'))
        ]
        self.app.register(*registered_calls[0])
        self.app.register_next_response(*registered_calls[1])
        req = Request.blank('/v1/a/c?uploads')
        req.method = 'GET'
        resp = req.get_response(self.mw)
        self.assertEqual(200, resp.status_int)

        exp_listing = [
            dict(item,
                 name=ref.user_name,
                 upload_id=self.mw.externalize_upload_id(
                     '/v1/a/c/' + quote(ref.user_name),
                     UploadId.parse(ref.obj_id)))
            for ref, item in [
                (ObjectRef.parse(_item['name']), _item)
                for _item in self.sample_in_progress_session_listing]]
        self.assertEqual(exp_listing, json.loads(resp.body))

        expected = [call[:2] for call in self.exp_calls]
        self.assertEqual(expected, self.app.calls[:2])
        self.assertEqual(4, len(self.app.calls), self.app.calls)
        expected = [call[:2] for call in self.exp_calls]
        self.assertEqual(expected, self.app.calls[:2])

        # first backend listing
        self.assertEqual('GET', self.app.calls[2][0])
        parsed_path = urllib.parse.urlparse(self.app.calls[2][1])
        self.assertEqual('/v1/.a/\x00mpu_sessions\x00c', parsed_path.path)
        unquoted_params = dict(urllib.parse.parse_qsl(parsed_path.query,
                                                      keep_blank_values=True))
        self.assertEqual({}, unquoted_params)
        # second backend listing
        self.assertEqual('GET', self.app.calls[3][0])
        parsed_path = urllib.parse.urlparse(self.app.calls[3][1])
        self.assertEqual('/v1/.a/\x00mpu_sessions\x00c', parsed_path.path)
        unquoted_params = dict(urllib.parse.parse_qsl(parsed_path.query,
                                                      keep_blank_values=True))
        self.assertEqual(
            {'marker': self.sample_all_session_listing[-1]['name']},
            unquoted_params)

    def test_list_uploads_limit(self):
        registered_calls = [
            ('GET', '/v1/.a/\x00mpu_sessions\x00c', HTTPOk, {},
             json.dumps(self.sample_all_session_listing).encode('ascii')),
        ]
        self.app.register(*registered_calls[0])
        req = Request.blank('/v1/a/c?uploads&limit=2')
        req.method = 'GET'
        resp = req.get_response(self.mw)
        self.assertEqual(200, resp.status_int)

        exp_listing = [
            dict(item,
                 name=ref.user_name,
                 upload_id=self.mw.externalize_upload_id(
                     '/v1/a/c/' + quote(ref.user_name),
                     UploadId.parse(ref.obj_id)))
            for ref, item in [
                (ObjectRef.parse(_item['name']), _item)
                for _item in self.sample_in_progress_session_listing[:2]]]
        self.assertEqual(exp_listing, json.loads(resp.body))

        expected = [call[:2] for call in self.exp_calls]
        self.assertEqual(expected, self.app.calls[:2])
        self.assertEqual(3, len(self.app.calls), self.app.calls)
        expected = [call[:2] for call in self.exp_calls]
        self.assertEqual(expected, self.app.calls[:2])

        # first backend listing
        self.assertEqual('GET', self.app.calls[2][0])
        parsed_path = urllib.parse.urlparse(self.app.calls[2][1])
        self.assertEqual('/v1/.a/\x00mpu_sessions\x00c', parsed_path.path)
        unquoted_params = dict(urllib.parse.parse_qsl(parsed_path.query,
                                                      keep_blank_values=True))
        self.assertEqual({}, unquoted_params)

    def test_list_uploads_with_prefix(self):
        prefix = 'obj2'
        registered_calls = [
            ('GET', '/v1/.a/\x00mpu_sessions\x00c', HTTPOk, {},
             json.dumps(self.sample_all_session_listing[1:]).encode('ascii')),
            ('GET', '/v1/.a/\x00mpu_sessions\x00c', HTTPOk, {},
             json.dumps([]).encode('ascii'))
        ]
        self.app.register(*registered_calls[0])
        self.app.register_next_response(*registered_calls[1])
        req = Request.blank('/v1/a/c?uploads&prefix=%s&ignored=x' % prefix)
        req.method = 'GET'
        resp = req.get_response(self.mw)
        self.assertEqual(200, resp.status_int)

        # expected listing does not include the aborted session
        exp_listing = [
            dict(item,
                 name=ref.user_name,
                 upload_id=self.mw.externalize_upload_id(
                     '/v1/a/c/' + quote(ref.user_name),
                     UploadId.parse(ref.obj_id)))
            for ref, item in [
                (ObjectRef.parse(_item['name']), _item)
                for _item in self.sample_in_progress_session_listing[1:]]]
        self.assertEqual(exp_listing, json.loads(resp.body))

        self.assertEqual(4, len(self.app.calls), self.app.calls)
        expected = [call[:2] for call in self.exp_calls]
        self.assertEqual(expected, self.app.calls[:2])

        # first backend listing
        self.assertEqual('GET', self.app.calls[2][0])
        parsed_path = urllib.parse.urlparse(self.app.calls[2][1])
        self.assertEqual('/v1/.a/\x00mpu_sessions\x00c', parsed_path.path)
        unquoted_params = dict(urllib.parse.parse_qsl(parsed_path.query,
                                                      keep_blank_values=True))
        self.assertEqual({'prefix': str(ObjectRef(prefix))}, unquoted_params)
        # second backend listing
        self.assertEqual('GET', self.app.calls[3][0])
        parsed_path = urllib.parse.urlparse(self.app.calls[3][1])
        self.assertEqual('/v1/.a/\x00mpu_sessions\x00c', parsed_path.path)
        unquoted_params = dict(urllib.parse.parse_qsl(parsed_path.query,
                                                      keep_blank_values=True))
        self.assertEqual(
            {'marker': self.sample_all_session_listing[-1]['name'],
             'prefix': str(ObjectRef(prefix))},
            unquoted_params)

    def test_list_uploads_with_marker_and_no_upload_id_marker(self):
        marker = 'obj1'
        registered_calls = [
            ('GET', '/v1/.a/\x00mpu_sessions\x00c', HTTPOk, {},
             json.dumps(self.sample_all_session_listing[1:]).encode('ascii')),
            ('GET', '/v1/.a/\x00mpu_sessions\x00c', HTTPOk, {},
             json.dumps([]).encode('ascii'))
        ]
        self.app.register(*registered_calls[0])
        self.app.register_next_response(*registered_calls[1])
        req = Request.blank('/v1/a/c?uploads&marker=%s&ignored=x' % marker)
        req.method = 'GET'
        resp = req.get_response(self.mw)
        self.assertEqual(200, resp.status_int)
        exp_listing = [
            dict(item,
                 name=ref.user_name,
                 upload_id=self.mw.externalize_upload_id(
                     '/v1/a/c/' + quote(ref.user_name),
                     UploadId.parse(ref.obj_id)))
            for ref, item in [
                (ObjectRef.parse(_item['name']), _item)
                for _item in self.sample_in_progress_session_listing[1:]]]
        self.assertEqual(exp_listing, json.loads(resp.body))

        self.assertEqual(4, len(self.app.calls), self.app.calls)
        expected = [call[:2] for call in self.exp_calls]
        self.assertEqual(expected, self.app.calls[:2])

        # first backend listing
        self.assertEqual('GET', self.app.calls[2][0])
        parsed_path = urllib.parse.urlparse(self.app.calls[2][1])
        self.assertEqual('/v1/.a/\x00mpu_sessions\x00c', parsed_path.path)
        unquoted_params = dict(urllib.parse.parse_qsl(parsed_path.query,
                                                      keep_blank_values=True))
        self.assertEqual(
            {'marker': ObjectRef(
                marker, UploadId(Timestamp.max())).serialize()},
            unquoted_params)
        # second backend listing
        self.assertEqual('GET', self.app.calls[3][0])
        parsed_path = urllib.parse.urlparse(self.app.calls[3][1])
        self.assertEqual('/v1/.a/\x00mpu_sessions\x00c', parsed_path.path)
        unquoted_params = dict(urllib.parse.parse_qsl(parsed_path.query,
                                                      keep_blank_values=True))
        self.assertEqual(
            {'marker': self.sample_all_session_listing[-1]['name']},
            unquoted_params)

    def test_list_uploads_with_marker_and_upload_id_marker(self):
        registered_calls = [
            ('GET', '/v1/.a/\x00mpu_sessions\x00c', HTTPOk, {},
             json.dumps(self.sample_all_session_listing[3:]).encode('ascii')),
            ('GET', '/v1/.a/\x00mpu_sessions\x00c', HTTPOk, {},
             json.dumps([]).encode('ascii'))
        ]
        self.app.register(*registered_calls[0])
        self.app.register_next_response(*registered_calls[1])
        marker = quote('obj2\N{SNOWMAN}')
        marker_ref = ObjectRef.parse(
            self.sample_all_session_listing[2]['name'])
        upload_id_marker = self.mw.externalize_upload_id(
            '/v1/a/c/' + quote(marker_ref.user_name),
            UploadId.parse(marker_ref.obj_id))
        req = Request.blank(
            '/v1/a/c?uploads&marker=%s&upload-id-marker=%s&ignored=x'
            % (marker, upload_id_marker))
        req.method = 'GET'
        resp = req.get_response(self.mw)
        self.assertEqual(200, resp.status_int, resp.body)

        # first backend listing
        call = self.app.call_list[2]
        self.assertEqual('GET', call.method)
        parsed_path = urllib.parse.urlparse(self.app.calls[2][1])
        self.assertEqual('/v1/.a/\x00mpu_sessions\x00c', parsed_path.path)
        unquoted_params = dict(urllib.parse.parse_qsl(parsed_path.query,
                                                      keep_blank_values=True))
        self.assertEqual(
            {'marker': marker_ref.serialize()},
            unquoted_params)
        # second backend listing
        call = self.app.call_list[3]
        self.assertEqual('GET', call.method)
        parsed_path = urllib.parse.urlparse(self.app.calls[3][1])
        self.assertEqual('/v1/.a/\x00mpu_sessions\x00c', parsed_path.path)
        unquoted_params = dict(urllib.parse.parse_qsl(parsed_path.query,
                                                      keep_blank_values=True))
        self.assertEqual(
            {'marker': self.sample_all_session_listing[-1]['name']},
            unquoted_params)

    def test_list_uploads_with_no_marker_and_upload_id_marker(self):
        registered_calls = [
            ('GET', '/v1/.a/\x00mpu_sessions\x00c', HTTPOk, {},
             json.dumps(self.sample_all_session_listing).encode('ascii')),
            ('GET', '/v1/.a/\x00mpu_sessions\x00c', HTTPOk, {},
             json.dumps([]).encode('ascii'))
        ]
        self.app.register(*registered_calls[0])
        self.app.register_next_response(*registered_calls[1])
        req = Request.blank(
            '/v1/a/c?uploads&upload-id-marker=123&ignored=x')
        req.method = 'GET'
        resp = req.get_response(self.mw)
        self.assertEqual(200, resp.status_int)

        # first backend listing
        self.assertEqual('GET', self.app.calls[2][0])
        parsed_path = urllib.parse.urlparse(self.app.calls[2][1])
        self.assertEqual('/v1/.a/\x00mpu_sessions\x00c', parsed_path.path)
        params = dict(urllib.parse.parse_qsl(parsed_path.query,
                      keep_blank_values=True))
        self.assertFalse(params)
        # second backend listing
        self.assertEqual('GET', self.app.calls[3][0])
        parsed_path = urllib.parse.urlparse(self.app.calls[3][1])
        self.assertEqual('/v1/.a/\x00mpu_sessions\x00c', parsed_path.path)
        params = dict(urllib.parse.parse_qsl(parsed_path.query,
                      keep_blank_values=True))
        self.assertEqual(
            {'marker': self.sample_all_session_listing[-1]['name']},
            params)

    def test_list_uploads_subrequest_503(self):
        registered_calls = [
            ('GET', '/v1/.a/\x00mpu_sessions\x00c',
             HTTPServiceUnavailable, {}, None),
        ]
        self.app.register(*registered_calls[0])
        req = Request.blank('/v1/a/c?uploads')
        req.method = 'GET'
        resp = req.get_response(self.mw)
        self.assertEqual(503, resp.status_int)
        self.assertIs(req, resp.request)
        exp_body = b''.join(HTTPServiceUnavailable()({}, lambda *args: None))
        self.assertEqual(exp_body, resp.body)

    def test_list_uploads_subrequest_404(self):
        registered_calls = [
            ('GET', '/v1/.a/\x00mpu_sessions\x00c',
             HTTPNotFound, {}, None),
        ]
        self.app.register(*registered_calls[0])
        req = Request.blank('/v1/a/c?uploads')
        req.method = 'GET'
        resp = req.get_response(self.mw)
        self.assertEqual(404, resp.status_int)
        self.assertIs(req, resp.request)
        exp_body = b''.join(HTTPNotFound()({}, lambda *args: None))
        self.assertEqual(exp_body, resp.body)

    def _do_test_upload_part(self, part_str, session_ctype):
        self.app.clear_calls()
        ts_session = next(self.ts_iter)
        extra_hdrs = {'Content-Type': session_ctype}
        self._setup_mpu_existence_check_call(
            ts_session, extra_headers=extra_hdrs)
        registered_calls = [
            ('PUT',
             '/v1/a/\x00mpu_parts\x00c/%s/000001' % self.session_name,
             HTTPCreated, {})]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank(
            '/v1/a/c/o?upload-id=%s&part-number=%s'
            % (self.external_upload_id, part_str),
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'Etag': 'test-etag',
                     'X-Delete-At': next(self.ts_iter).normal,  # ignored
                     'X-Delete-After': '345',  # ignored
                     'Transfer-Encoding': 'test-encoding'},
            body=b'testing')

        ts_now = next(self.ts_iter)
        with mock.patch('swift.common.utils.Timestamp.now',
                        return_value=ts_now):
            resp = req.get_response(self.mw)
        self.assertEqual(201, resp.status_int, resp.body)
        self.assertEqual(b'', resp.body)
        exp_etag = md5(b'testing', usedforsecurity=False).hexdigest()
        self.assertEqual('"%s"' % exp_etag, resp.headers.get('Etag'))
        self.assertEqual('7', resp.headers['Content-Length'])
        expected = [call[:2] for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)
        actual_put_hdrs = self.app.headers[-1]
        exp_put_hdrs = {'Content-Length': '7',
                        'Host': 'localhost:80',
                        'User-Agent': 'Swift',
                        'Etag': 'test-etag',
                        'Transfer-Encoding': 'test-encoding',
                        'X-Backend-Allow-Reserved-Names': 'true',
                        'X-Timestamp': ts_now.normal}
        self.assertEqual(exp_put_hdrs, actual_put_hdrs)

    def test_upload_part(self):
        self._do_test_upload_part('1', 'application/x-mpu-session-created')

    def test_upload_part_padded_digits(self):
        self._do_test_upload_part(
            '000001', 'application/x-mpu-session-created')

    def test_upload_part_session_completing(self):
        self._do_test_upload_part('1', 'application/x-mpu-session-completing')

    def test_upload_part_subrequest_404(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        registered_calls = [
            ('PUT',
             '/v1/a/\x00mpu_parts\x00c/%s/000001' % self.session_name,
             HTTPNotFound, {})
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank(
            '/v1/a/c/o?upload-id=%s&part-number=1' % self.external_upload_id,
            environ={'REQUEST_METHOD': 'PUT'},
            body=b'testing')
        with mock.patch('swift.common.utils.Timestamp.now',
                        return_value=next(self.ts_iter)):
            resp = req.get_response(self.mw)
        self.assertEqual(503, resp.status_int, resp.body)
        exp_body = b''.join(HTTPServiceUnavailable()({}, lambda *args: None))
        self.assertEqual(exp_body, resp.body)

    def test_upload_part_subrequest_503(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        registered_calls = [
            ('PUT',
             '/v1/a/\x00mpu_parts\x00c/%s/000001' % self.session_name,
             HTTPServiceUnavailable, {})
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank(
            '/v1/a/c/o?upload-id=%s&part-number=1' % self.external_upload_id,
            environ={'REQUEST_METHOD': 'PUT'},
            body=b'testing')
        with mock.patch('swift.common.utils.Timestamp.now',
                        return_value=next(self.ts_iter)):
            resp = req.get_response(self.mw)
        self.assertEqual(503, resp.status_int, resp.body)
        exp_body = b''.join(HTTPServiceUnavailable()({}, lambda *args: None))
        self.assertEqual(exp_body, resp.body)

    def test_upload_part_session_aborted(self):
        ts_session = next(self.ts_iter)
        extra_hdrs = {'Content-Type': 'application/x-mpu-session-aborted'}
        self._setup_mpu_existence_check_call(
            ts_session, extra_headers=extra_hdrs)
        req = Request.blank(
            '/v1/a/c/o?upload-id=%s&part-number=1' % self.external_upload_id,
            environ={'REQUEST_METHOD': 'PUT'},
            body=b'testing')
        with mock.patch('swift.common.utils.Timestamp.now',
                        return_value=next(self.ts_iter)):
            resp = req.get_response(self.mw)
        self.assertEqual(404, resp.status_int, resp.body)
        self.assertEqual(b'No such upload-id', resp.body)

    def test_upload_part_session_not_found(self):
        # session not created or completed
        ts_session = next(self.ts_iter)
        registered_calls = [
            ('HEAD',
             '/v1/.a/\x00mpu_sessions\x00c/%s' % self.session_name_wsgi,
             HTTPNotFound,
             {'X-Backend-Delete-Timestamp': ts_session.internal}
             ),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank(
            '/v1/a/c/o?upload-id=%s&part-number=1' % self.external_upload_id,
            environ={'REQUEST_METHOD': 'PUT'},
            body=b'testing')
        resp = req.get_response(self.mw)
        self.assertEqual(404, resp.status_int)
        self.assertEqual(b'No such upload-id', resp.body)

    def _do_test_list_parts(self, session_ctype):
        ts_session = next(self.ts_iter)
        extra_hdrs = {'Content-Type': session_ctype}
        self._setup_mpu_existence_check_call(
            ts_session, extra_headers=extra_hdrs)
        listing = [{'name': '%s/%06d' % (self.session_name, i),
                    'hash': 'etag%d' % i,
                    'bytes': i,
                    'content_type': 'text/plain',
                    'last_modified': '1970-01-01T00:00:00.000000'}
                   for i in range(3)]
        registered_calls = [
            ('GET',
             '/v1/a/\x00mpu_parts\x00c',
             HTTPOk,
             {'X-Container-Object-Count': '123',
              'X-Container-Bytes-Used': '999999',
              'X-Storage-Policy': 'policy-0'},
             json.dumps(listing).encode('ascii'))
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.external_upload_id)
        with mock.patch('swift.common.utils.Timestamp.now',
                        return_value=next(self.ts_iter)):
            resp = req.get_response(self.mw)
        self.assertEqual(200, resp.status_int, resp.body)
        expected = [call[:2] for call in self.exp_calls] + [
            ('GET', '/v1/a/\x00mpu_parts\x00c?marker=%s&prefix=%s'
             % (quote(self.session_name + '/', safe=''),
                quote(self.session_name + '/', safe=''))),
        ]
        self.assertEqual(expected, self.app.calls)
        exp_listing = [{'name': 'o/%s/%06d' % (self.external_upload_id, i),
                        'hash': 'etag%d' % i,
                        'bytes': i,
                        'content_type': 'text/plain',
                        'last_modified': '1970-01-01T00:00:00.000000'}
                       for i in range(3)]
        self.assertEqual(exp_listing, json.loads(resp.body))
        self.assertEqual({'Content-Length': str(len(resp.body)),
                          'Content-Type': 'application/json; charset=utf-8',
                          'X-Storage-Policy': 'policy-0'},
                         resp.headers)

    def test_list_parts_in_progress(self):
        self._do_test_list_parts(
            session_ctype='application/x-mpu-session-created')

    def test_list_parts_completing(self):
        self._do_test_list_parts(
            session_ctype='application/x-mpu-session-completing')

    def test_list_parts_forwards_params(self):
        # this test doesn't care about the listing content, it's just verifying
        # the request parameter forwarding
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        listing = []
        registered_calls = [
            ('GET',
             '/v1/a/\x00mpu_parts\x00c',
             HTTPOk,
             {'X-Container-Object-Count': '123',
              'X-Container-Bytes-Used': '999999',
              'X-Storage-Policy': 'policy-0'},
             json.dumps(listing).encode('ascii'))
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank(
            '/v1/a/c/o?upload-id=%s&part-number-marker=1&marker=ignored'
            % self.external_upload_id)
        with mock.patch('swift.common.utils.Timestamp.now',
                        return_value=next(self.ts_iter)):
            resp = req.get_response(self.mw)
        self.assertEqual(200, resp.status_int)
        self.assertEqual([], json.loads(resp.body))
        self.assertEqual(4, len(self.app.calls), self.app.calls)
        expected = [call[:2] for call in self.exp_calls]
        self.assertEqual(expected, self.app.calls[:3])
        self.assertEqual('GET', self.app.calls[3][0])
        parsed_path = urllib.parse.urlparse(self.app.calls[3][1])
        self.assertEqual('/v1/a/\x00mpu_parts\x00c', parsed_path.path)
        params = dict(urllib.parse.parse_qsl(parsed_path.query,
                      keep_blank_values=True))
        self.assertEqual({'marker': '%s/000001' % self.session_name,
                          'prefix': '%s/' % self.session_name},
                         params)

    def test_list_parts_subrequest_404(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        registered_calls = [
            ('GET',
             '/v1/a/\x00mpu_parts\x00c',
             HTTPNotFound, {})

        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.external_upload_id)
        with mock.patch('swift.common.utils.Timestamp.now',
                        return_value=next(self.ts_iter)):
            resp = req.get_response(self.mw)
        self.assertEqual(503, resp.status_int)
        self.assertIs(req, resp.request)
        exp_body = b''.join(HTTPServiceUnavailable()({}, lambda *args: None))
        self.assertEqual(exp_body, resp.body)
        self.assertEqual({'Content-Length': str(len(resp.body)),
                          'Content-Type': 'text/html; charset=UTF-8'},
                         resp.headers)
        expected = [call[:2] for call in self.exp_calls] + [
            ('GET', '/v1/a/\x00mpu_parts\x00c?marker=%s&prefix=%s'
             % (quote(self.session_name + '/', safe=''),
                quote(self.session_name + '/', safe=''))),
        ]
        self.assertEqual(expected, self.app.calls)

    def test_list_parts_subrequest_503(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        registered_calls = [
            ('GET',
             '/v1/a/\x00mpu_parts\x00c',
             HTTPServiceUnavailable, {})
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.external_upload_id)
        with mock.patch('swift.common.utils.Timestamp.now',
                        return_value=next(self.ts_iter)):
            resp = req.get_response(self.mw)
        self.assertEqual(503, resp.status_int)
        exp_body = b''.join(HTTPServiceUnavailable()({}, lambda *args: None))
        self.assertEqual(exp_body, resp.body)
        self.assertEqual({'Content-Length': str(len(resp.body)),
                          'Content-Type': 'text/html; charset=UTF-8'},
                         resp.headers)
        expected = [call[:2] for call in self.exp_calls] + [
            ('GET', '/v1/a/\x00mpu_parts\x00c?marker=%s&prefix=%s'
             % (quote(self.session_name + '/', safe=''),
                quote(self.session_name + '/', safe=''))),
        ]
        self.assertEqual(expected, self.app.calls)

    def _do_test_complete_mpu(self):
        ts_session = next(self.ts_iter)
        ts_session.offset = 123
        part_dicts = self._make_sample_part_dicts()
        user_manifest = self._user_manifest_for_part_dicts(part_dicts)
        self._setup_mpu_existence_check_call(ts_session)
        ts_part1, ts_part2, ts_complete = [next(self.ts_iter)
                                           for _ in range(3)]
        registered_calls = [
            ('POST',
             '/v1/.a/\x00mpu_sessions\x00c/%s' % self.session_name_wsgi,
             HTTPOk, {}),
        ]
        registered_calls += self._make_parts_check_calls(part_dicts)
        registered_calls += [
            ('PUT',
             '/v1/a/c/%s'
             % swob.str_to_wsgi(self.obj_name),
             HTTPCreated,
             {'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT'},
             b'ignored manifest put resp body'),
            ('POST',
             '/v1/.a/\x00mpu_sessions\x00c/%s' % self.session_name_wsgi,
             HTTPAccepted, {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored',
                    'X-Object-Sysmeta-Container-Update-Override-Etag':
                        'ignored-etag;foo=ignored'}
        req = Request.blank('/v1/a/c/%s?upload-id=%s'
                            % (quote(self.obj_name), self.external_upload_id),
                            headers=req_hdrs)
        req.method = 'POST'
        exp_mpu_etag = self._calculate_mpu_etag(user_manifest)
        req.body = json.dumps(user_manifest)
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(202, resp.status_int, resp_body)
        resp_dict = json.loads(resp_body)
        self.assertEqual(
            {'Response Status': '201 Created',
             'Errors': [],
             'Etag': exp_mpu_etag,
             'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT'},
            resp_dict)
        expected = [call[:2]
                    for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

        session_post = self.app.call_list[3]
        self.assertEqual(
            {'Content-Type': 'application/x-mpu-session-completing',
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Timestamp': ts_complete.internal},
            dict(session_post.headers))

        actual_manifest_body = self.app.uploaded.get(
            '/v1/a/c/%s' % swob.str_to_wsgi(self.obj_name))[1]
        self.assertEqual(part_dicts, json.loads(actual_manifest_body))
        # the session timestamp's offset is preserved...
        exp_put_ts = Timestamp(ts_session,
                               offset=ts_complete.raw - ts_session.raw + 123)
        exp_mpu_link = quote(
            '\x00mpu_parts\x00c/%s' % self.session_ref.serialize())
        manifest_put = self.app.call_list[-2]
        exp_systags = {
            'relic_id': quote(self.upload_id.serialize()),
            'child_container': quote('\x00mpu_parts\x00c'),
            'child': quote(self.session_name) + '/',
        }
        self.assertEqual(
            {'Accept': 'application/json',
             'Content-Length': str(len(actual_manifest_body)),
             'Content-Type': 'application/test',
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Object-Meta-Foo': 'blah',
             'X-Object-Sysmeta-Allow-Reserved-Names': 'true',
             'X-Object-Sysmeta-Container-Update-Override-Etag':
                 '%s; mpu_etag=%s; mpu_link=%s'
                 % (exp_mpu_etag, exp_mpu_etag, exp_mpu_link),
             'X-Object-Sysmeta-Container-Update-Override-Size': '5242979',
             'X-Object-Sysmeta-Container-Update-Override-Systags':
                param_str_from_dict(exp_systags),
             'X-Object-Sysmeta-Mpu-Manifest': 'true',
             'X-Object-Sysmeta-Mpu-Etag': exp_mpu_etag,
             'X-Object-Sysmeta-Mpu-Size': '5242979',
             'X-Object-Sysmeta-Mpu-Parts-Count': '2',
             'X-Object-Sysmeta-Mpu-Upload-Id': self.upload_id.serialize(),
             'X-Object-Sysmeta-Mpu-Max-Manifest-Part': '2',
             'X-Timestamp': exp_put_ts.internal},
            dict(manifest_put.headers))

        exp_updates = [{
            "op": "PUT",
            "account": ".a",
            "container": "\x00history\x00c",
            "obj": self.obj_name + '\x00null',
            "headers": {
                "x-size": "0",
                "x-content-type": "application/x-phony;swift_source=mpu",
                'x-systags': param_str_from_dict(exp_systags), }
        }]
        self.assertEqual(exp_updates,
                         manifest_put.env.get('swift.container_updates'))

        session_post = self.app.call_list[-1]
        self.assertEqual(
            {'Content-Type': 'application/x-mpu-session-completed',
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Timestamp': mock.ANY},
            session_post.headers)

    def test_complete_mpu(self):
        self._do_test_complete_mpu()

    def test_complete_mpu_utf8(self):
        self.obj_name = '\N{SNOWMAN}'
        self._do_test_complete_mpu()

    def test_complete_mpu_with_gaps_in_parts_is_ok(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        ts_part1, ts_part3, ts_complete = [next(self.ts_iter)
                                           for _ in range(3)]
        # referenced part numbers are not required to be contiguous...
        part_dicts = self._make_sample_part_dicts([1, 3])
        user_manifest = self._user_manifest_for_part_dicts(part_dicts)
        registered_calls = [
            ('POST',
             '/v1/.a/\x00mpu_sessions\x00c/%s' % self.session_name_wsgi,
             HTTPOk, {})]
        registered_calls += self._make_parts_check_calls(part_dicts)
        registered_calls += [
            ('PUT',
             '/v1/a/c/o',
             HTTPCreated,
             {'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT'},
             b'ignored manifest put resp body'),
            ('POST',
             '/v1/.a/\x00mpu_sessions\x00c/%s' % self.session_name_wsgi,
             HTTPAccepted, {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.external_upload_id,
                            headers=req_hdrs)
        req.method = 'POST'
        exp_mpu_etag = self._calculate_mpu_etag(user_manifest)
        req.body = json.dumps(user_manifest)
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(202, resp.status_int)
        resp_dict = json.loads(resp_body)
        self.assertEqual(
            {'Response Status': '201 Created',
             'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
             'Etag': exp_mpu_etag,
             'Errors': []},
            resp_dict)
        expected = [call[:2]
                    for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

        session_post = self.app.call_list[3]
        self.assertEqual(
            {'Content-Type': 'application/x-mpu-session-completing',
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Timestamp': ts_complete.internal},
            session_post.headers)

        actual_manifest_body = self.app.uploaded.get('/v1/a/c/o')[1]
        self.assertEqual(
            part_dicts,
            json.loads(actual_manifest_body))
        exp_put_ts = Timestamp(ts_session,
                               offset=ts_complete.raw - ts_session.raw)
        exp_mpu_link = swob.wsgi_quote(
            '\x00mpu_parts\x00c/%s' % self.session_name)
        manifest_put = self.app.call_list[-2]
        exp_systags = {
            'relic_id': quote(self.upload_id.serialize()),
            'child_container': quote('\x00mpu_parts\x00c'),
            'child': quote(self.session_name) + '/',
        }
        self.assertEqual(
            {'Accept': 'application/json',
             'Content-Length': str(len(actual_manifest_body)),
             'Content-Type': 'application/test',
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Object-Meta-Foo': 'blah',
             'X-Object-Sysmeta-Allow-Reserved-Names': 'true',
             'X-Object-Sysmeta-Container-Update-Override-Etag':
                 '%s; mpu_etag=%s; mpu_link=%s'
                 % (exp_mpu_etag, exp_mpu_etag, exp_mpu_link),
             'X-Object-Sysmeta-Container-Update-Override-Size': '5242979',
             'X-Object-Sysmeta-Container-Update-Override-Systags':
                param_str_from_dict(exp_systags),
             'X-Object-Sysmeta-Mpu-Manifest': 'true',
             'X-Object-Sysmeta-Mpu-Etag': exp_mpu_etag,
             'X-Object-Sysmeta-Mpu-Size': '5242979',
             'X-Object-Sysmeta-Mpu-Parts-Count': '2',
             'X-Object-Sysmeta-Mpu-Upload-Id': self.upload_id.serialize(),
             'X-Object-Sysmeta-Mpu-Max-Manifest-Part': '3',
             'X-Timestamp': exp_put_ts.internal},
            manifest_put.headers)
        exp_updates = [{
            "op": "PUT",
            "account": ".a",
            "container": "\x00history\x00c",
            "obj": self.obj_name + '\x00null',
            "headers": {
                "x-size": "0",
                "x-content-type": "application/x-phony;swift_source=mpu",
                'x-systags': param_str_from_dict(exp_systags), }
        }]
        self.assertEqual(exp_updates,
                         manifest_put.env.get('swift.container_updates'))

        session_post = self.app.call_list[-1]
        self.assertEqual(
            {'Content-Type': 'application/x-mpu-session-completed',
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Timestamp': mock.ANY},
            session_post.headers)

    def test_complete_mpu_fails_to_set_completing_state(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        ts_complete = next(self.ts_iter)
        registered_calls = [
            ('POST',
             '/v1/.a/\x00mpu_sessions\x00c/%s' % self.session_name_wsgi,
             HTTPNotFound, {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.external_upload_id,
                            headers=req_hdrs)
        req.method = 'POST'
        user_manifest = self._make_sample_user_manifest()
        req.body = json.dumps(user_manifest)
        resp = req.get_response(self.mw)
        self.assertEqual(503, resp.status_int)
        exp_body = b''.join(HTTPServiceUnavailable()({}, lambda *args: None))
        self.assertEqual(exp_body, resp.body)
        expected = [call[:2]
                    for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def do_complete(self, manifest_body):
        self.app.clear_calls()
        ts_complete = next(self.ts_iter)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank(
            '/v1/a/c/o?upload-id=%s' % self.external_upload_id,
            headers=req_hdrs)
        req.method = 'POST'
        req.body = manifest_body
        resp = req.get_response(self.mw)
        expected = [call[:2] for call in self.exp_calls]
        self.assertEqual(expected, self.app.calls)
        return resp

    def test_complete_mpu_manifest_no_parts(self):
        resp = self.do_complete(b"[]")
        self.assertEqual(400, resp.status_int)
        self.assertEqual(b'Manifest must have at least one part.\n',
                         resp.body)

    def test_complete_mpu_manifest_too_many_parts(self):
        resp = self.do_complete(json.dumps(
            [{'part_number': i + 1, 'etag': MD5_OF_EMPTY_STRING}
             for i in range(10001)]))
        self.assertEqual(400, resp.status_int)
        self.assertEqual(
            b'Index 10000: part_number must be at most 10000.\n'
            b'Manifest must have at most 10000 parts.\n',
            resp.body)

    def test_complete_mpu_manifest_invalid_json(self):
        resp = self.do_complete(b"[{123: 'foo'}]")
        self.assertEqual(400, resp.status_int)
        self.assertEqual(b'Manifest must be valid JSON.\n',
                         resp.body)

    def test_complete_mpu_manifest_not_a_list(self):
        resp = self.do_complete(json.dumps({'part_number': 2}))
        self.assertEqual(400, resp.status_int)
        self.assertEqual(b'Manifest must be a list.\n',
                         resp.body)

    def test_complete_mpu_manifest_not_list_of_dicts(self):
        resp = self.do_complete(json.dumps([[]]))
        self.assertEqual(400, resp.status_int)
        self.assertEqual(b'Index 0: not a JSON object.\n',
                         resp.body)

    def test_complete_mpu_manifest_part_missing_etag(self):
        resp = self.do_complete(json.dumps([{'part_number': 2}]))
        self.assertEqual(400, resp.status_int)
        self.assertEqual(b'Index 0: expected keys to include etag.\n',
                         resp.body)

    def test_complete_mpu_manifest_part_missing_part_number(self):
        resp = self.do_complete(json.dumps([{'etag': 'a' * 32}]))
        self.assertEqual(400, resp.status_int)
        self.assertEqual(b'Index 0: expected keys to include part_number.\n',
                         resp.body)

    def test_complete_mpu_manifest_parts_missing_etag_and_part_number(self):
        resp = self.do_complete(json.dumps([{'etag': 'a' * 32},
                                            {'part_number': 2}]))
        self.assertEqual(400, resp.status_int)
        self.assertEqual(b'Index 0: expected keys to include part_number.\n'
                         b'Index 1: expected keys to include etag.\n',
                         resp.body)

    def test_complete_mpu_manifest_parts_misordered(self):
        resp = self.do_complete(json.dumps([
            {'etag': 'b' * 32, 'part_number': 2},
            {'etag': 'a' * 32, 'part_number': 1}
        ]))
        self.assertEqual(400, resp.status_int)
        self.assertEqual(
            b'Index 1: part_number 1 must be greater than previous 2.\n',
            resp.body)

    def test_complete_mpu_manifest_part_number_too_large(self):
        resp = self.do_complete(json.dumps([
            {'etag': 'b' * 32, 'part_number': 2},
            {'etag': 'a' * 32, 'part_number': 10001}
        ]))
        self.assertEqual(400, resp.status_int)
        self.assertEqual(
            b'Index 1: part_number must be at most 10000.\n',
            resp.body)

    def test_complete_mpu_manifest_too_long(self):
        resp = self.do_complete('x' * (self.mw.max_manifest_size + 1))
        self.assertEqual(413, resp.status_int)
        self.assertEqual(b'MultipartComplete body > 720000 bytes.\n',
                         resp.body)

    def test_complete_mpu_manifest_put_not_success(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        ts_complete = next(self.ts_iter)
        part_dicts = self._make_sample_part_dicts()
        user_manifest = self._user_manifest_for_part_dicts(part_dicts)
        registered_calls = [
            ('POST',
             '/v1/.a/\x00mpu_sessions\x00c/%s' % self.session_name_wsgi,
             HTTPOk, {}),
        ]
        registered_calls += self._make_parts_check_calls(part_dicts)
        registered_calls += [
            ('PUT',
             '/v1/a/c/o',
             HTTPServiceUnavailable, {}, None),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.external_upload_id,
                            headers=req_hdrs)
        req.method = 'POST'
        req.body = json.dumps(user_manifest)
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(202, resp.status_int)
        resp_dict = json.loads(resp_body)
        self.assertEqual(
            {'Response Status': '503 Service Unavailable',
             'Response Body': b''.join(HTTPServiceUnavailable()(
                 {}, lambda *args: None)).decode('utf8'),
             'Errors': []},
            resp_dict)
        expected = [call[:2]
                    for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_complete_mpu_problem_segments(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        ts_complete = next(self.ts_iter)
        part_dicts = self._make_sample_part_dicts()
        user_manifest = self._user_manifest_for_part_dicts(part_dicts)
        bad_part_dicts = deepcopy(part_dicts)
        bad_part_dicts[0]['size'] = 123  # too small
        bad_part_dicts[1]['etag'] = 'bad etag'
        registered_calls = [
            ('POST',
             '/v1/.a/\x00mpu_sessions\x00c/%s' % self.session_name_wsgi,
             HTTPOk, {}),
        ]
        registered_calls += self._make_parts_check_calls(bad_part_dicts)
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.external_upload_id,
                            headers=req_hdrs)
        req.method = 'POST'
        req.body = json.dumps(user_manifest)
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(202, resp.status_int, resp.body)
        resp_dict = json.loads(resp_body)
        self.assertEqual({
            'Response Status': '400 Bad Request',
            'Response Body': '',
            'Errors': [
                ['1', 'Upload part too small: '
                      'part must be at least 5242880 bytes.'],
                ['2', 'Etag Mismatch']]
        }, resp_dict)
        expected = [call[:2]
                    for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_complete_mpu_session_aborted(self):
        ts_session = next(self.ts_iter)
        extra_hdrs = {'Content-Type': 'application/x-mpu-session-aborted'}
        self._setup_mpu_existence_check_call(ts_session,
                                             extra_headers=extra_hdrs)
        # the mpu was not previously completed...
        registered_calls = [
            ('HEAD', '/v1/a/c/%s' % self.obj_name, HTTPNotFound, {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        ts_complete = next(self.ts_iter)
        req_hdrs = {'X-Timestamp': ts_complete.internal}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.external_upload_id,
                            headers=req_hdrs)
        req.method = 'POST'
        user_manifest = self._make_sample_user_manifest()
        req.body = json.dumps(user_manifest)
        resp = req.get_response(self.mw)
        self.assertEqual(404, resp.status_int)
        self.assertEqual(b'No such upload-id', resp.body)
        expected = [call[:2] for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_complete_mpu_session_completed(self):
        ts_session = next(self.ts_iter)
        extra_hdrs = {'Content-Type': 'application/x-mpu-session-completed'}
        self._setup_mpu_existence_check_call(ts_session,
                                             extra_headers=extra_hdrs)
        user_manifest = self._make_sample_user_manifest()
        exp_mpu_etag = self._calculate_mpu_etag(user_manifest)
        # the mpu was previously completed...
        head_resp_hdrs = {
            'X-Object-Sysmeta-Mpu-Upload-Id': self.upload_id.serialize(),
            'X-Object-Sysmeta-Mpu-Etag': exp_mpu_etag,
            'X-Object-Sysmeta-Mpu-Parts-Count': '99',
            'X-Object-Sysmeta-Mpu-Max-Manifest-Part': '123',
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
        }
        registered_calls = [
            ('HEAD', '/v1/a/c/%s' % self.obj_name, HTTPOk, head_resp_hdrs),
        ]
        for call in registered_calls:
            self.app.register(*call)
        ts_complete = next(self.ts_iter)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.external_upload_id,
                            headers=req_hdrs)
        req.method = 'POST'
        req.body = json.dumps(user_manifest)
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(202, resp.status_int)
        resp_dict = json.loads(resp_body)
        self.assertEqual(
            {'Response Status': '201 Created',
             'Etag': exp_mpu_etag,
             'Response Body': '',
             'Last Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
             'Errors': [],
             },
            resp_dict)
        expected = [call[:2] for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_complete_mpu_session_not_found_user_object_not_found(self):
        ts_complete = next(self.ts_iter)
        registered_calls = [
            ('HEAD',
             '/v1/.a/\x00mpu_sessions\x00c/%s' % self.session_name_wsgi,
             HTTPNotFound, {}),
            ('HEAD', '/v1/a/c/o', HTTPNotFound, {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.external_upload_id,
                            headers=req_hdrs)
        req.method = 'POST'
        req.body = json.dumps(self._make_sample_user_manifest())
        resp = req.get_response(self.mw)
        self.assertEqual(404, resp.status_int)
        self.assertEqual(b'No such upload-id', resp.body)
        expected = [call[:2] for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_complete_mpu_session_not_found_user_object_not_same_etag(self):
        ts_complete = next(self.ts_iter)
        user_manifest = self._make_sample_user_manifest()
        user_obj_head_resp_headers = {
            'Content-Type': 'application/test',
            'X-Manifest-Etag': 'slo-manifest-etag',
            'Etag': '"slo-etag"',
            'X-Object-Sysmeta-Mpu-Etag': 'not-the-same-mpu-etag',
            'X-Object-Sysmeta-Mpu-Upload-Id': self.upload_id.serialize(),
        }

        registered_calls = [
            ('HEAD',
             '/v1/.a/\x00mpu_sessions\x00c/%s' % self.session_name_wsgi,
             HTTPNotFound, {}),
            ('HEAD', '/v1/a/c/o', HTTPOk, user_obj_head_resp_headers),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.external_upload_id,
                            headers=req_hdrs)
        req.method = 'POST'
        req.body = json.dumps(user_manifest)
        resp = req.get_response(self.mw)
        self.assertEqual(404, resp.status_int)
        self.assertEqual(b'No such upload-id', resp.body)
        expected = [call[:2] for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_complete_mpu_session_not_found_user_object_not_same_id(self):
        ts_complete = next(self.ts_iter)
        user_manifest = self._make_sample_user_manifest()
        exp_mpu_etag = self._calculate_mpu_etag(user_manifest)
        user_obj_head_resp_headers = {
            'Content-Type': 'application/test',
            'X-Manifest-Etag': 'slo-manifest-etag',
            'Etag': '"slo-etag"',
            'X-Object-Sysmeta-Mpu-Etag': exp_mpu_etag,
            'X-Object-Sysmeta-Mpu-Upload-Id': 'not-the-same-upload-id',
        }

        registered_calls = [
            ('HEAD',
             '/v1/.a/\x00mpu_sessions\x00c/%s' % self.session_name_wsgi,
             HTTPNotFound, {}),
            ('HEAD', '/v1/a/c/o', HTTPOk, user_obj_head_resp_headers),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.external_upload_id,
                            headers=req_hdrs)
        req.method = 'POST'
        req.body = json.dumps(user_manifest)
        resp = req.get_response(self.mw)
        self.assertEqual(404, resp.status_int)
        self.assertEqual(b'No such upload-id', resp.body)
        expected = [call[:2] for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_complete_mpu_session_not_found_user_object_linked(self):
        ts_complete = next(self.ts_iter)
        user_manifest = self._make_sample_user_manifest()
        exp_mpu_etag = self._calculate_mpu_etag(user_manifest)
        user_obj_head_resp_headers = {
            'Content-Type': 'application/test',
            'X-Manifest-Etag': 'slo-manifest-etag',
            'Etag': '"slo-etag"',
            'X-Object-Sysmeta-Mpu-Etag': exp_mpu_etag,
            'X-Object-Sysmeta-Mpu-Upload-Id': self.upload_id.serialize(),
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
        }

        registered_calls = [
            ('HEAD',
             '/v1/.a/\x00mpu_sessions\x00c/%s' % self.session_name_wsgi,
             HTTPNotFound, {}),
            ('HEAD', '/v1/a/c/o', HTTPOk, user_obj_head_resp_headers),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.external_upload_id,
                            headers=req_hdrs)
        req.method = 'POST'
        req.body = json.dumps(user_manifest)
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(202, resp.status_int)
        resp_dict = json.loads(resp_body)
        self.assertEqual(
            {'Response Status': '201 Created',
             'Etag': exp_mpu_etag,
             'Response Body': '',
             'Last Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
             'Errors': [],
             },
            resp_dict)
        expected = [call[:2] for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_abort_mpu(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        ts_abort = next(self.ts_iter)
        registered_calls = [
            ('HEAD', '/v1/a/c/o', HTTPOk, {}),
            ('POST',
             '/v1/.a/\x00mpu_sessions\x00c/%s' % self.session_name_wsgi,
             HTTPOk, {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_abort.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.external_upload_id,
                            headers=req_hdrs)
        req.method = 'DELETE'
        resp = req.get_response(self.mw)
        self.assertEqual(b'', resp.body)
        self.assertEqual(204, resp.status_int)
        expected = [call[:2] for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)
        session_post_hdrs = self.app.headers[4]
        self.assertEqual(
            {'Content-Type': 'application/x-mpu-session-aborted',
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Timestamp': ts_abort.internal},
            session_post_hdrs)

    def test_abort_mpu_but_manifest_in_user_namespace(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        ts_abort = next(self.ts_iter)
        registered_calls = [
            ('HEAD', '/v1/a/c/o', HTTPOk,
             {'X-Object-Sysmeta-Mpu-Upload-Id': self.upload_id.serialize()}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_abort.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.external_upload_id,
                            headers=req_hdrs)
        req.method = 'DELETE'
        resp = req.get_response(self.mw)
        self.assertEqual(409, resp.status_int)
        exp_body = b''.join(HTTPConflict()({}, lambda *args: None))
        self.assertEqual(exp_body, resp.body)
        expected = [call[:2] for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_abort_mpu_fails_to_update_session(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        ts_abort = next(self.ts_iter)
        registered_calls = [
            ('HEAD', '/v1/a/c/o', HTTPOk, {}),
            ('POST',
             '/v1/.a/\x00mpu_sessions\x00c/%s' % self.session_name_wsgi,
             HTTPNotFound, {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_abort.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.external_upload_id,
                            headers=req_hdrs)
        req.method = 'DELETE'
        resp = req.get_response(self.mw)
        self.assertEqual(503, resp.status_int)
        exp_body = b''.join(HTTPServiceUnavailable()({}, lambda *args: None))
        self.assertEqual(exp_body, resp.body)
        expected = [call[:2] for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_abort_mpu_session_completing(self):
        ts_session = next(self.ts_iter)
        extra_hdrs = {'Content-Type': 'application/x-mpu-session-completing'}
        self._setup_mpu_existence_check_call(ts_session,
                                             extra_headers=extra_hdrs)
        ts_abort = next(self.ts_iter)
        registered_calls = [
            ('HEAD', '/v1/a/c/o', HTTPOk, {}),
            ('POST',
             '/v1/.a/\x00mpu_sessions\x00c/%s' % self.session_name_wsgi,
             HTTPOk, {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_abort.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.external_upload_id,
                            headers=req_hdrs)
        req.method = 'DELETE'
        resp = req.get_response(self.mw)
        self.assertEqual(b'', resp.body)
        self.assertEqual(204, resp.status_int)
        expected = [call[:2] for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)
        session_post_hdrs = self.app.headers[4]
        # verify that state is copied across to new POST
        self.assertEqual(
            {'Content-Type': 'application/x-mpu-session-aborted',
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Timestamp': ts_abort.internal},
            session_post_hdrs)

    def test_abort_mpu_session_aborted(self):
        # verify it's ok to abort an already aborted session
        ts_session = next(self.ts_iter)
        extra_hdrs = {'Content-Type': 'application/x-mpu-session-aborted'}
        self._setup_mpu_existence_check_call(ts_session,
                                             extra_headers=extra_hdrs)
        ts_abort = next(self.ts_iter)
        req_hdrs = {'X-Timestamp': ts_abort.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.external_upload_id,
                            headers=req_hdrs)
        req.method = 'DELETE'
        resp = req.get_response(self.mw)
        self.assertEqual(b'', resp.body)
        self.assertEqual(204, resp.status_int)
        expected = [call[:2] for call in self.exp_calls]
        self.assertEqual(expected, self.app.calls)

    def test_abort_mpu_session_completed(self):
        ts_session = next(self.ts_iter)
        extra_hdrs = {'Content-Type': 'application/x-mpu-session-completed'}
        self._setup_mpu_existence_check_call(ts_session,
                                             extra_headers=extra_hdrs)
        ts_abort = next(self.ts_iter)
        req_hdrs = {'X-Timestamp': ts_abort.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.external_upload_id,
                            headers=req_hdrs)
        req.method = 'DELETE'
        resp = req.get_response(self.mw)
        self.assertEqual(204, resp.status_int)
        self.assertEqual(b'', resp.body)
        expected = [call[:2] for call in self.exp_calls]
        self.assertEqual(expected, self.app.calls)

    def test_abort_mpu_session_not_found(self):
        ts_abort = next(self.ts_iter)
        registered_calls = [
            ('HEAD',
             '/v1/.a/\x00mpu_sessions\x00c/%s' % self.session_name_wsgi,
             HTTPNotFound, {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_abort.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.external_upload_id,
                            headers=req_hdrs)
        req.method = 'DELETE'
        resp = req.get_response(self.mw)
        self.assertEqual(b'', resp.body)
        self.assertEqual(204, resp.status_int)
        expected = [call[:2] for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_container_listing(self):
        mpu_link = swob.wsgi_quote(
            '\x00mpu_manifests\x00cont/%s' % self.session_name_wsgi)
        listing = [
            # MPU
            {'name': 'a-mpu',
             'bytes': 10485760,  # SLO fixes this up from swift_bytes
             'hash': '%s; '
                     'other_mw_etag=banana; '
                     'mpu_etag=my-mpu-etag; '
                     'mpu_link=%s'
                     % (MD5_OF_EMPTY_STRING, mpu_link),
             'content_type': 'application/test',
             'last_modified': '2024-09-10T14:16:00.579190',
             },
            # the following shouldn't be modified...
            # symlink
            {'name': 'b-symlink',
             'bytes': 0,
             'hash': '%s' % MD5_OF_EMPTY_STRING,
             'content_type': 'application/test',
             'last_modified': '2024-09-10T14:16:00.579190',
             'symlink_etag': 'symlink-etag',
             'symlink_bytes': 10485760,
             'symlink_path': '/v1/a/b/c'},
            # SLO
            {'name': 'c-slo',
             'bytes': 0,
             'hash': '%s' % MD5_OF_EMPTY_STRING,
             'content_type': 'application/test',
             'last_modified': '2024-09-10T14:16:00.579190',
             'slo_etag': 'my-slo-etag'},
            # plain old object
            {'name': 'd-obj',
             'hash': 'my-etag',
             'bytes': 123.45678,
             'content_type': "text/plain",
             'last_modified': '1970-01-01T00:00:01.000000'},
        ]
        resp_body = json.dumps(listing).encode('ascii')
        parts_container = '\x00mpu_parts\x00cont'
        get_resp_hdrs = {
            'X-Container-Sysmeta-Mpu-Parts-Container-0':
                swob.wsgi_quote(parts_container),
            'X-Container-Object-Count': '4',
            'X-Container-Bytes-Used': '123',
        }
        registered = [
            ('HEAD', '/v1/a', swob.HTTPOk, {}),
            ('HEAD', '/v1/a/cont', swob.HTTPOk, {}),
            ('GET', '/v1/a/cont', swob.HTTPCreated,
             get_resp_hdrs, resp_body),
        ]
        for call in registered:
            self.app.register(*call)
        req = Request.blank('/v1/a/cont')
        req.method = 'GET'
        resp = req.get_response(self.mw)
        self.assertEqual(201, resp.status_int)
        actual = json.loads(b''.join(resp.app_iter))
        expected_listing = [
            {'name': 'a-mpu',
             'bytes': 10485760,
             'hash': 'my-mpu-etag; other_mw_etag=banana',
             'content_type': 'application/test',
             'last_modified': '2024-09-10T14:16:00.579190',
             }] + listing[1:]
        self.assertEqual(expected_listing, actual)
        exp_hdrs = {
            'Content-Length': str(len(json.dumps(expected_listing))),
            'Content-Type': 'text/html; charset=UTF-8',
            'X-Container-Bytes-Used': '123',
            'X-Container-Object-Count': '4',
            'X-Container-Sysmeta-Mpu-Parts-Container-0': '%00mpu_parts%00cont'
        }
        self.assertEqual(exp_hdrs, resp.headers)
        exp_calls = [call[:2] for call in registered]
        self.assertEqual(exp_calls, self.app.calls)

    def test_container_listing_no_mpu(self):
        listing = [
            # plain old object
            {'name': 'd-obj',
             'hash': 'my-etag',
             'bytes': 123.45678,
             'content_type': "text/plain",
             'last_modified': '1970-01-01T00:00:01.000000'},
        ]
        resp_body = json.dumps(listing).encode('ascii')
        get_resp_hdrs = {
            'X-Container-Object-Count': '1',
            'X-Container-Bytes-Used': '123',
        }
        registered = [
            ('HEAD', '/v1/a', swob.HTTPOk, {}),
            ('HEAD', '/v1/a/cont', swob.HTTPOk, {}),
            ('GET', '/v1/a/cont', swob.HTTPCreated,
             get_resp_hdrs, resp_body),
        ]
        for call in registered:
            self.app.register(*call)
        req = Request.blank('/v1/a/cont')
        req.method = 'GET'
        resp = req.get_response(self.mw)
        self.assertEqual(201, resp.status_int)
        exp_hdrs = {
            'Content-Length': str(len(resp_body)),
            'Content-Type': 'text/html; charset=UTF-8',
            'X-Container-Bytes-Used': '123',
            'X-Container-Object-Count': '1',
        }
        self.assertEqual(exp_hdrs, resp.headers)
        actual = json.loads(b''.join(resp.app_iter))
        self.assertEqual(listing, actual)
        exp_calls = [call[:2] for call in registered]
        self.assertEqual(exp_calls, self.app.calls)

    def test_post_mpu(self):
        post_resp_headers = {
            'Content-Type': 'text/html; charset=UTF-8',
            'Content-Length': '101',
            'X-Trans-Id': 'test-txn-id',
            'X-Openstack-Request-Id': 'test-txn-id',
            'Date': 'Tue, 24 Sep 2024 13:22:30 GMT',
            'X-Object-Sysmeta-Mpu-Etag': 'mpu-etag',
            'X-Object-Sysmeta-Mpu-Parts-Count': '2',
            'X-Object-Sysmeta-Mpu-Upload-Id': str(self.upload_id),
        }
        post_resp_body = b''.join(HTTPAccepted()({}, lambda *args: None))
        registered_calls = [
            ('HEAD', '/v1/a', swob.HTTPOk, {}),
            ('HEAD', '/v1/a/c', swob.HTTPOk, {}),
            ('POST', '/v1/a/c/' + quote(self.obj_name),
             swob.HTTPAccepted,
             post_resp_headers,
             post_resp_body),
        ]
        for call in registered_calls:
            self.app.register(*call)
        post_req_headers = {
            'X-Object-Meta-Foo': 'Bar',
            'Content-Type': 'application/test2',
            'X-Timestamp': '1727184152.29665',
        }
        req = Request.blank('/v1/a/c/' + quote(self.obj_name),
                            headers=post_req_headers)
        req.method = 'POST'
        resp = req.get_response(self.mw)
        self.assertEqual(202, resp.status_int)
        expected = [call[:2] for call in registered_calls]
        self.assertEqual(expected, self.app.calls)
        self.assertEqual(post_resp_body, resp.body)
        exp_resp_headers = {
            'Content-Type': 'text/html; charset=UTF-8',
            'Content-Length': str(len(post_resp_body)),
            'X-Trans-Id': 'test-txn-id',
            'X-Openstack-Request-Id': 'test-txn-id',
            'Date': 'Tue, 24 Sep 2024 13:22:30 GMT',
            'X-Object-Sysmeta-Mpu-Etag': 'mpu-etag',
            'X-Object-Sysmeta-Mpu-Parts-Count': '2',
            'X-Object-Sysmeta-Mpu-Upload-Id': str(self.upload_id),
        }
        self.assertEqual(exp_resp_headers, resp.headers)
        self.assertEqual({'Content-Type': 'application/test2',
                          'Host': 'localhost:80',
                          'X-Object-Meta-Foo': 'Bar',
                          'X-Timestamp': '1727184152.29665'},
                         self.app.headers[-1])

    def _do_test_get_head_mpu(self, method):
        part_dicts = self._make_sample_part_dicts()
        mpu_size = sum(p['size'] for p in part_dicts)
        manifest_body = json.dumps(part_dicts).encode('ascii')
        manifest_get_resp_headers = {
            'Content-Type': 'application/test',
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
            'X-Timestamp': '1727184150.29665',
            'Accept-Ranges': 'bytes',
            'Content-Length': len(manifest_body),
            'X-Manifest-Etag': 'b871773cf02434d498517245c7b88c11',
            'X-Trans-Id': 'test-txn-id',
            'X-Openstack-Request-Id': 'test-txn-id',
            'Date': 'Tue, 24 Sep 2024 13:22:30 GMT',
            'Etag': '"de64a5af184e6f732a26328c13d4ab25"',
            'X-Object-Sysmeta-Mpu-Etag': 'mpu-etag',
            'X-Object-Sysmeta-Mpu-Size': str(mpu_size),
            'X-Object-Sysmeta-Mpu-Parts-Count': '2',
            'X-Object-Sysmeta-Mpu-Max-Manifest-Part': '2',
            'X-Object-Sysmeta-Mpu-Upload-Id': self.upload_id.serialize(),
        }
        registered_calls = [
            ('HEAD', '/v1/a', swob.HTTPOk, {}),
            ('HEAD', '/v1/a/c', swob.HTTPOk, {}),
            (method, '/v1/a/c/o', swob.HTTPOk, manifest_get_resp_headers,
             json.dumps(part_dicts)),
        ]
        if method == 'GET':
            registered_calls += [
                # TODO: the multipart-manifest=get is due to SegmentedIterable
                ('GET',
                 '/v1/a/\x00mpu_parts\x00c/%s/000001?multipart-manifest=get'
                 % self.session_name_wsgi,
                 HTTPOk,
                 {'X-Backend-Timestamp': part_dicts[0]['timestamp'],
                  'Content-Length': str(part_dicts[0]['size']),
                  'ETag': '%s' % part_dicts[0]['etag']},
                 self.sample_part1_body),
                ('GET',
                 '/v1/a/\x00mpu_parts\x00c/%s/000002?multipart-manifest=get'
                 % self.session_name_wsgi,
                 HTTPOk,
                 {'X-Backend-Timestamp': part_dicts[1]['timestamp'],
                  'Content-Length': str(part_dicts[1]['size']),
                  'ETag': '"%s"' % part_dicts[1]['etag']},
                 self.sample_part2_body),
            ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank('/v1/a/c/' + quote(self.obj_name))
        req.method = method
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(200, resp.status_int)
        exp_calls = [call[:2] for call in registered_calls]
        self.assertEqual(exp_calls, self.app.calls)
        exp_resp_headers = {
            'Content-Type': 'application/test',
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
            'X-Timestamp': '1727184150.29665',
            'Accept-Ranges': 'bytes',
            'Content-Length': str(mpu_size),
            'X-Trans-Id': 'test-txn-id',
            'X-Openstack-Request-Id': 'test-txn-id',
            'Date': 'Tue, 24 Sep 2024 13:22:30 GMT',
            'Etag': '"mpu-etag"',
            'X-Parts-Count': '2',
            'X-Upload-Id': self.external_upload_id,
        }
        self.assertEqual(exp_resp_headers, resp.headers)
        return resp_body

    def test_get_mpu(self):
        resp_body = self._do_test_get_head_mpu('GET')
        self.assertEqual(self.sample_part1_body + self.sample_part2_body,
                         resp_body)
        self.assertEqual(
            {'Host': 'localhost:80',
             'X-Backend-Ignore-Range-If-Metadata-Present':
                 'x-object-sysmeta-mpu-manifest',
             'X-Backend-Etag-Is-At': 'x-object-sysmeta-mpu-etag'},
            self.app.call_list[-3].headers)
        self.assertEqual({'Host': 'localhost:80',
                          'X-Backend-Allow-Reserved-Names': 'true',
                          'User-Agent': 'MPU GET'},
                         self.app.call_list[-2].headers)
        self.assertEqual({'Host': 'localhost:80',
                          'X-Backend-Allow-Reserved-Names': 'true',
                          'User-Agent': 'MPU GET'},
                         self.app.call_list[-1].headers)

    def test_head_mpu(self):
        resp_body = self._do_test_get_head_mpu('HEAD')
        self.assertEqual(b'', resp_body)
        self.assertEqual(
            {'Host': 'localhost:80',
             'X-Backend-Ignore-Range-If-Metadata-Present':
                 'x-object-sysmeta-mpu-manifest',
             'X-Backend-Etag-Is-At': 'x-object-sysmeta-mpu-etag'},
            self.app.call_list[-1].headers)

    def _do_test_head_mpu_304(self, method):
        # A conditional HEAD whose backend returns 304 should have
        # X-Object-Sysmeta-Mpu-Etag translated to Etag.
        mpu_size = 5 * 1024 * 1024 + 99
        manifest_resp_headers = {
            'Content-Length': '0',
            'Content-Type': 'application/test',
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
            'X-Timestamp': '1727184150.29665',
            'Accept-Ranges': 'bytes',
            'X-Trans-Id': 'test-txn-id',
            'X-Openstack-Request-Id': 'test-txn-id',
            'Date': 'Tue, 24 Sep 2024 13:22:30 GMT',
            'Etag': '"de64a5af184e6f732a26328c13d4ab25"',
            'X-Object-Sysmeta-Mpu-Etag': 'mpu-etag',
            'X-Object-Sysmeta-Mpu-Size': str(mpu_size),
            'X-Object-Sysmeta-Mpu-Parts-Count': '2',
            'X-Object-Sysmeta-Mpu-Max-Manifest-Part': '2',
            'X-Object-Sysmeta-Mpu-Upload-Id': self.upload_id.serialize(),
        }
        registered_calls = [
            ('HEAD', '/v1/a', swob.HTTPOk, {}),
            ('HEAD', '/v1/a/c', swob.HTTPOk, {}),
            (method, '/v1/a/c/o',
             swob.HTTPNotModified, manifest_resp_headers, None),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank('/v1/a/c/' + quote(self.obj_name),
                            method=method,
                            headers={'If-None-Match': '"mpu-etag"'})
        resp = req.get_response(self.mw)
        self.assertEqual(b'', resp.body)
        self.assertEqual(304, resp.status_int)
        exp_calls = [call[:2] for call in registered_calls]
        self.assertEqual(exp_calls, self.app.calls)
        self.assertEqual('"mpu-etag"', resp.headers.get('Etag'))
        self.assertEqual('Tue, 24 Sep 2024 13:22:30 GMT',
                         resp.headers.get('Date'))
        self.assertEqual('0', resp.headers.get('content-length'))

    def test_head_mpu_304(self):
        self._do_test_head_mpu_304('HEAD')

    def test_get_mpu_304(self):
        self._do_test_head_mpu_304('GET')

    def test_get_mpu_bad_manifest_503(self):
        manifest_body = b'baloney'
        manifest_get_resp_headers = {
            'Content-Type': 'application/test',
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
            'X-Timestamp': '1727184150.29665',
            'Accept-Ranges': 'bytes',
            'Content-Length': str(len(manifest_body)),
            'X-Manifest-Etag': 'b871773cf02434d498517245c7b88c11',
            'X-Trans-Id': 'test-txn-id',
            'X-Openstack-Request-Id': 'test-txn-id',
            'Date': 'Tue, 24 Sep 2024 13:22:30 GMT',
            'Etag': '"de64a5af184e6f732a26328c13d4ab25"',
            'X-Object-Sysmeta-Mpu-Etag': 'mpu-etag',
            'X-Object-Sysmeta-Mpu-Size': str(len(manifest_body)),
            'X-Object-Sysmeta-Mpu-Parts-Count': '2',
            'X-Object-Sysmeta-Mpu-Max-Manifest-Part': '2',
            'X-Object-Sysmeta-Mpu-Upload-Id': self.upload_id.serialize(),
        }
        registered_calls = [
            ('HEAD', '/v1/a', swob.HTTPOk, {}),
            ('HEAD', '/v1/a/c', swob.HTTPOk, {}),
            ('GET', '/v1/a/c/o',
             swob.HTTPOk, manifest_get_resp_headers,
             manifest_body),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank(
            '/v1/a/c/' + quote(self.obj_name))
        req.method = 'GET'
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(503, resp.status_int)
        exp_calls = [call[:2] for call in registered_calls]
        self.assertEqual(exp_calls, self.app.calls)
        exp_body = b''.join(HTTPServiceUnavailable()({}, lambda *args: None))
        self.assertEqual(exp_body, resp_body)
        exp_resp_headers = {
            'Content-Type': 'text/html; charset=UTF-8',
            'Content-Length': str(len(exp_body)),
        }
        self.assertEqual(exp_resp_headers, resp.headers)

    def test_get_mpu_with_part_number(self):
        part_dicts = self._make_sample_part_dicts()
        manifest_body = json.dumps(part_dicts).encode('ascii')
        manifest_get_resp_hdrs = self._make_manifest_resp_hdrs(part_dicts)
        part2_body = '2' * part_dicts[1]['size']
        registered_calls = [
            ('HEAD', '/v1/a', swob.HTTPOk, {}),
            ('HEAD', '/v1/a/c', swob.HTTPOk, {}),
            ('GET', '/v1/a/c/o?part-number=2',
             swob.HTTPOk, manifest_get_resp_hdrs,
             manifest_body),
            # TODO: the multipart-manifest=get is due to SegmentedIterable
            ('GET',
             '/v1/a/\x00mpu_parts\x00c/%s/000002?multipart-manifest=get'
             % self.session_name_wsgi,
             HTTPOk,
             {'X-Backend-Timestamp': part_dicts[1]['timestamp'],
              'Content-Length': str(part_dicts[1]['size']),
              'ETag': '%s' % part_dicts[1]['etag']},
             part2_body),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank(
            '/v1/a/c/' + quote(self.obj_name) + '?part-number=2')
        req.method = 'GET'
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(206, resp.status_int, resp_body)
        exp_calls = [call[:2] for call in registered_calls]
        self.assertEqual(exp_calls, self.app.calls)
        exp_resp_headers = {
            'Content-Type': 'application/test',
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
            'X-Timestamp': '1727184150.29665',
            'Accept-Ranges': 'bytes',
            'Content-Length': str(part_dicts[1]['size']),
            'Content-Range': 'bytes 5242880-5242978/5242979',
            'X-Trans-Id': 'test-txn-id',
            'X-Openstack-Request-Id': 'test-txn-id',
            'Date': 'Tue, 24 Sep 2024 13:22:30 GMT',
            'Etag': '"mpu-etag"',
            'X-Parts-Count': '2',
            'X-Upload-Id': self.external_upload_id,
        }
        self.assertEqual(exp_resp_headers, resp.headers)
        self.assertEqual(
            {'Host': 'localhost:80',
             'X-Backend-Etag-Is-At': 'x-object-sysmeta-mpu-etag',
             'X-Backend-Ignore-Range-If-Metadata-Present':
                 'x-object-sysmeta-mpu-manifest'},
            self.app.call_list[-2].headers)
        self.assertEqual({'Host': 'localhost:80',
                          'X-Backend-Allow-Reserved-Names': 'true',
                          'User-Agent': 'MPU GET'},
                         self.app.call_list[-1].headers)
        self.assertEqual(part2_body.encode('utf8'), resp_body)

    def test_get_mpu_with_part_number_greater_than_actual(self):
        part_dicts = self._make_sample_part_dicts()
        manifest_body = json.dumps(part_dicts).encode('ascii')
        manifest_get_resp_headers = self._make_manifest_resp_hdrs(part_dicts)
        registered_calls = [
            ('HEAD', '/v1/a', swob.HTTPOk, {}),
            ('HEAD', '/v1/a/c', swob.HTTPOk, {}),
            ('GET', '/v1/a/c/o?part-number=99',
             swob.HTTPOk, manifest_get_resp_headers,
             manifest_body),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank(
            '/v1/a/c/' + quote(self.obj_name) + '?part-number=99')
        req.method = 'GET'
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(416, resp.status_int, resp_body)
        exp_calls = [call[:2] for call in registered_calls]
        self.assertEqual(exp_calls, self.app.calls)
        exp_body = b''.join(
            HTTPRequestedRangeNotSatisfiable()({}, lambda *args: None))
        self.assertEqual(exp_body, resp_body)
        exp_resp_headers = {
            'Content-Length': str(len(exp_body)),
            'Content-Type': 'text/html; charset=UTF-8',
            'X-Parts-Count': '2'}
        self.assertEqual(exp_resp_headers, resp.headers)

    def test_head_mpu_with_part_number(self):
        part_dicts = self._make_sample_part_dicts()
        mpu_size = sum(p['size'] for p in part_dicts)
        manifest_body = json.dumps(part_dicts).encode('ascii')
        manifest_get_resp_headers = {
            'Content-Type': 'application/test',
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
            'X-Timestamp': '1727184150.29665',
            'Accept-Ranges': 'bytes',
            'Content-Length': str(len(manifest_body)),
            'X-Manifest-Etag': 'b871773cf02434d498517245c7b88c11',
            'X-Trans-Id': 'test-txn-id',
            'X-Openstack-Request-Id': 'test-txn-id',
            'Date': 'Tue, 24 Sep 2024 13:22:30 GMT',
            'Etag': '"de64a5af184e6f732a26328c13d4ab25"',
            'X-Object-Sysmeta-Mpu-Etag': 'mpu-etag',
            'X-Object-Sysmeta-Mpu-Size': str(mpu_size),
            'X-Object-Sysmeta-Mpu-Parts-Count': '2',
            'X-Object-Sysmeta-Mpu-Max-Manifest-Part': '2',
            'X-Object-Sysmeta-Mpu-Upload-Id': self.upload_id.serialize(),
        }
        registered_calls = [
            ('HEAD', '/v1/a', swob.HTTPOk, {}),
            ('HEAD', '/v1/a/c', swob.HTTPOk, {}),
            ('HEAD', '/v1/a/c/o?part-number=1',
             swob.HTTPOk, manifest_get_resp_headers,
             None),
            ('HEAD', '/v1/a', swob.HTTPOk, {}),
            ('HEAD', '/v1/a/c', swob.HTTPOk, {}),
            ('GET', '/v1/a/c/o?part-number=1',
             swob.HTTPOk, manifest_get_resp_headers,
             manifest_body),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank(
            '/v1/a/c/' + quote(self.obj_name) + '?part-number=1')
        req.method = 'HEAD'
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(206, resp.status_int, resp_body)
        exp_calls = [call[:2] for call in registered_calls]
        self.assertEqual(exp_calls, self.app.calls)
        exp_resp_headers = {
            'Content-Type': 'application/test',
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
            'X-Timestamp': '1727184150.29665',
            'Accept-Ranges': 'bytes',
            'Content-Length': str(5 * 1024 * 1024),
            'Content-Range': 'bytes 0-5242879/5242979',
            'X-Trans-Id': 'test-txn-id',
            'X-Openstack-Request-Id': 'test-txn-id',
            'Date': 'Tue, 24 Sep 2024 13:22:30 GMT',
            'Etag': '"mpu-etag"',
            'X-Parts-Count': '2',
            'X-Upload-Id': self.external_upload_id,
        }
        self.assertEqual(exp_resp_headers, resp.headers)
        exp_manifest_get_headers = {
            'Host': 'localhost:80',
            'X-Backend-Etag-Is-At': 'x-object-sysmeta-mpu-etag',
            'X-Backend-Ignore-Range-If-Metadata-Present':
                'x-object-sysmeta-mpu-manifest'}
        self.assertEqual(
            exp_manifest_get_headers, self.app.call_list[2].headers)
        self.assertEqual(
            exp_manifest_get_headers, self.app.call_list[5].headers)
        self.assertEqual(b'', resp_body)

    def test_head_mpu_with_range_ignored(self):
        part_dicts = self._make_sample_part_dicts()
        mpu_size = sum(p['size'] for p in part_dicts)
        manifest_body = json.dumps(part_dicts).encode('ascii')
        manifest_get_resp_headers = {
            'Content-Type': 'application/test',
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
            'X-Timestamp': '1727184150.29665',
            'Accept-Ranges': 'bytes',
            'Content-Length': str(len(manifest_body)),
            'X-Manifest-Etag': 'b871773cf02434d498517245c7b88c11',
            'X-Trans-Id': 'test-txn-id',
            'X-Openstack-Request-Id': 'test-txn-id',
            'Date': 'Tue, 24 Sep 2024 13:22:30 GMT',
            'Etag': '"de64a5af184e6f732a26328c13d4ab25"',
            'X-Object-Sysmeta-Mpu-Etag': 'mpu-etag',
            'X-Object-Sysmeta-Mpu-Size': str(mpu_size),
            'X-Object-Sysmeta-Mpu-Parts-Count': '2',
            'X-Object-Sysmeta-Mpu-Max-Manifest-Part': '2',
            'X-Object-Sysmeta-Mpu-Upload-Id': self.upload_id.serialize(),
        }
        registered_calls = [
            ('HEAD', '/v1/a', swob.HTTPOk, {}),
            ('HEAD', '/v1/a/c', swob.HTTPOk, {}),
            ('HEAD', '/v1/a/c/o',
             swob.HTTPOk, manifest_get_resp_headers,
             None),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank(
            '/v1/a/c/' + quote(self.obj_name))
        req.method = 'HEAD'
        req.headers['Range'] = 'bytes=1-10'
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(200, resp.status_int, resp_body)
        exp_calls = [call[:2] for call in registered_calls]
        self.assertEqual(exp_calls, self.app.calls)
        exp_resp_headers = {
            'Content-Type': 'application/test',
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
            'X-Timestamp': '1727184150.29665',
            'Accept-Ranges': 'bytes',
            'Content-Length': str(mpu_size),
            'X-Trans-Id': 'test-txn-id',
            'X-Openstack-Request-Id': 'test-txn-id',
            'Date': 'Tue, 24 Sep 2024 13:22:30 GMT',
            'Etag': '"mpu-etag"',
            'X-Parts-Count': '2',
            'X-Upload-Id': self.external_upload_id,
        }
        self.assertEqual(exp_resp_headers, resp.headers)
        exp_manifest_get_headers = {
            'Host': 'localhost:80',
            'Range': 'bytes=1-10',
            'X-Backend-Etag-Is-At': 'x-object-sysmeta-mpu-etag',
            'X-Backend-Ignore-Range-If-Metadata-Present':
                'x-object-sysmeta-mpu-manifest'}
        self.assertEqual(
            exp_manifest_get_headers, self.app.call_list[2].headers)
        self.assertEqual(b'', resp_body)

    def test_get_mpu_with_range_subset_manifest(self):
        part_dicts = self._make_sample_part_dicts()
        mpu_size = sum(p['size'] for p in part_dicts)
        manifest_body = json.dumps(part_dicts).encode('ascii')
        manifest_get_resp_headers = {
            'Content-Type': 'application/test',
            'X-Object-Sysmeta-Mpu-Manifest': 'True',
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
            'X-Timestamp': '1727184150.29665',
            'Accept-Ranges': 'bytes',
            'Content-Length': str(len(manifest_body)),
            'X-Manifest-Etag': 'b871773cf02434d498517245c7b88c11',
            'X-Trans-Id': 'test-txn-id',
            'X-Openstack-Request-Id': 'test-txn-id',
            'Date': 'Tue, 24 Sep 2024 13:22:30 GMT',
            'Etag': '"de64a5af184e6f732a26328c13d4ab25"',
            'X-Object-Sysmeta-Mpu-Etag': 'mpu-etag',
            'X-Object-Sysmeta-Mpu-Size': str(mpu_size),
            'X-Object-Sysmeta-Mpu-Parts-Count': '2',
            'X-Object-Sysmeta-Mpu-Max-Manifest-Part': '2',
            'X-Object-Sysmeta-Mpu-Upload-Id': self.upload_id.serialize(),
        }
        registered_calls = [
            ('HEAD', '/v1/a', swob.HTTPOk, {}),
            ('HEAD', '/v1/a/c', swob.HTTPOk, {}),
            ('GET', '/v1/a/c/o',
             swob.HTTPOk, manifest_get_resp_headers,
             manifest_body),
            # TODO: the multipart-manifest=get is due to SegmentedIterable
            ('GET',
             '/v1/a/\x00mpu_parts\x00c/%s/000001?multipart-manifest=get'
             % self.session_name_wsgi,
             HTTPOk,
             {'X-Backend-Timestamp': part_dicts[0]['timestamp'],
              'Content-Length': str(part_dicts[0]['size']),
              'ETag': '%s' % part_dicts[0]['etag']},
             'part1 body'),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank(
            '/v1/a/c/' + quote(self.obj_name),
            headers={'Range': 'bytes=1-10'},
        )
        req.method = 'GET'
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(206, resp.status_int)
        exp_calls = [call[:2] for call in registered_calls]
        self.assertEqual(exp_calls, self.app.calls)
        exp_resp_headers = {
            'Content-Type': 'application/test',
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
            'X-Timestamp': '1727184150.29665',
            'Accept-Ranges': 'bytes',
            'Content-Length': '10',
            'Content-Range': 'bytes 1-10/5242979',
            'X-Trans-Id': 'test-txn-id',
            'X-Openstack-Request-Id': 'test-txn-id',
            'Date': 'Tue, 24 Sep 2024 13:22:30 GMT',
            'Etag': '"mpu-etag"',
            'X-Parts-Count': '2',
            'X-Upload-Id': self.external_upload_id,
        }
        self.assertEqual(exp_resp_headers, resp.headers)
        self.assertEqual(
            {'Host': 'localhost:80',
             'Range': 'bytes=1-10',
             'X-Backend-Etag-Is-At': 'x-object-sysmeta-mpu-etag',
             'X-Backend-Ignore-Range-If-Metadata-Present':
                 'x-object-sysmeta-mpu-manifest'},
            self.app.call_list[-2].headers)
        self.assertEqual({'Host': 'localhost:80',
                          'X-Backend-Allow-Reserved-Names': 'true',
                          'User-Agent': 'MPU GET',
                          'Range': 'bytes=1-10'},
                         self.app.call_list[-1].headers)
        self.assertEqual(b'art1 body', resp_body)

    def test_get_mpu_with_range_spans_parts(self):
        part_dicts = self._make_sample_part_dicts()
        mpu_size = sum(p['size'] for p in part_dicts)
        manifest_body = json.dumps(part_dicts).encode('ascii')
        manifest_get_resp_headers = {
            'Content-Type': 'application/test',
            'X-Object-Sysmeta-Mpu-Manifest': 'True',
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
            'X-Timestamp': '1727184150.29665',
            'Accept-Ranges': 'bytes',
            'Content-Length': str(len(manifest_body)),
            'X-Manifest-Etag': 'b871773cf02434d498517245c7b88c11',
            'X-Trans-Id': 'test-txn-id',
            'X-Openstack-Request-Id': 'test-txn-id',
            'Date': 'Tue, 24 Sep 2024 13:22:30 GMT',
            'Etag': '"de64a5af184e6f732a26328c13d4ab25"',
            'X-Object-Sysmeta-Mpu-Etag': 'mpu-etag',
            'X-Object-Sysmeta-Mpu-Size': str(mpu_size),
            'X-Object-Sysmeta-Mpu-Parts-Count': '2',
            'X-Object-Sysmeta-Mpu-Max-Manifest-Part': '2',
            'X-Object-Sysmeta-Mpu-Upload-Id': self.upload_id.serialize(),
        }
        registered_calls = [
            ('HEAD', '/v1/a', swob.HTTPOk, {}),
            ('HEAD', '/v1/a/c', swob.HTTPOk, {}),
            ('GET', '/v1/a/c/o',
             swob.HTTPOk, manifest_get_resp_headers,
             manifest_body),
            # TODO: the multipart-manifest=get is due to SegmentedIterable
            ('GET',
             '/v1/a/\x00mpu_parts\x00c/%s/000001?multipart-manifest=get'
             % self.session_name_wsgi,
             HTTPPartialContent,
             {'X-Backend-Timestamp': part_dicts[0]['timestamp'],
              'Content-Length': str(part_dicts[0]['size']),
              'Content-Range': 'bytes 5242875-5242879/5242880',
              'ETag': '%s' % part_dicts[0]['etag']},
             b'part1'),
            ('GET',
             '/v1/a/\x00mpu_parts\x00c/%s/000002?multipart-manifest=get'
             % self.session_name_wsgi,
             HTTPOk,
             {'X-Backend-Timestamp': part_dicts[1]['timestamp'],
              'Content-Length': str(part_dicts[1]['size']),
              'Content-Range': 'bytes 0-4/99',
              'ETag': '%s' % part_dicts[1]['etag']},
             b'part2'),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank(
            '/v1/a/c/' + quote(self.obj_name),
            headers={'Range': 'bytes=5242875-5242884'},
        )
        req.method = 'GET'
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(206, resp.status_int, resp_body)
        exp_calls = [call[:2] for call in registered_calls]
        self.assertEqual(exp_calls, self.app.calls)
        exp_resp_headers = {
            'Content-Type': 'application/test',
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
            'X-Timestamp': '1727184150.29665',
            'Accept-Ranges': 'bytes',
            'Content-Length': '10',
            'Content-Range': 'bytes 5242875-5242884/5242979',
            'X-Trans-Id': 'test-txn-id',
            'X-Openstack-Request-Id': 'test-txn-id',
            'Date': 'Tue, 24 Sep 2024 13:22:30 GMT',
            'Etag': '"mpu-etag"',
            'X-Parts-Count': '2',
            'X-Upload-Id': self.external_upload_id,
        }
        self.assertEqual(exp_resp_headers, resp.headers)
        self.assertEqual(
            {'Host': 'localhost:80',
             'Range': 'bytes=5242875-5242884',
             'X-Backend-Etag-Is-At': 'x-object-sysmeta-mpu-etag',
             'X-Backend-Ignore-Range-If-Metadata-Present':
                 'x-object-sysmeta-mpu-manifest'},
            self.app.call_list[-3].headers)
        self.assertEqual({'Host': 'localhost:80',
                          'X-Backend-Allow-Reserved-Names': 'true',
                          'User-Agent': 'MPU GET',
                          'Range': 'bytes=5242875-'},
                         self.app.call_list[-2].headers)
        self.assertEqual({'Host': 'localhost:80',
                          'X-Backend-Allow-Reserved-Names': 'true',
                          'User-Agent': 'MPU GET',
                          'Range': 'bytes=0-4'},
                         self.app.call_list[-1].headers)
        self.assertEqual(b'part1part2', resp_body)

    def test_get_mpu_412(self):
        manifest_get_resp_headers = {
            'Content-Type': 'application/test',
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
            'X-Timestamp': '1727184150.29665',
            'Accept-Ranges': 'bytes',
            'Content-Length': '4',
            'X-Manifest-Etag': 'b871773cf02434d498517245c7b88c11',
            'X-Trans-Id': 'test-txn-id',
            'X-Openstack-Request-Id': 'test-txn-id',
            'Date': 'Tue, 24 Sep 2024 13:22:30 GMT',
            'Etag': '"de64a5af184e6f732a26328c13d4ab25"',
            'X-Object-Sysmeta-Mpu-Etag': 'mpu-etag',
            'X-Object-Sysmeta-Mpu-Parts-Count': '2',
            'X-Object-Sysmeta-Mpu-Max-Manifest-Part': '2',
            'X-Object-Sysmeta-Mpu-Upload-Id': self.upload_id.serialize(),
        }
        registered_calls = [
            ('HEAD', '/v1/a', swob.HTTPOk, {}),
            ('HEAD', '/v1/a/c', swob.HTTPOk, {}),
            ('GET', '/v1/a/c/o',
             swob.HTTPPreconditionFailed, manifest_get_resp_headers,
             None),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank('/v1/a/c/' + quote(self.obj_name))
        req.method = 'GET'
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(412, resp.status_int)
        exp_calls = [call[:2] for call in registered_calls]
        self.assertEqual(exp_calls, self.app.calls)
        # TODO: the functional test asserts that the body is empty ?!
        exp_body = b''.join(HTTPPreconditionFailed()({}, lambda *args: None))
        self.assertEqual(exp_body, resp_body)
        exp_resp_headers = dict(manifest_get_resp_headers)
        exp_resp_headers['Content-Length'] = str(len(exp_body))
        exp_resp_headers['Etag'] = '"mpu-etag"'

        self.assertEqual(exp_resp_headers, resp.headers)
        self.assertEqual(
            {'Host': 'localhost:80',
             'X-Backend-Ignore-Range-If-Metadata-Present':
                 'x-object-sysmeta-mpu-manifest',
             'X-Backend-Etag-Is-At': 'x-object-sysmeta-mpu-etag'},
            self.app.call_list[-1].headers)

    def _do_test_get_head_not_mpu(self, method):
        get_resp_headers = {
            'Content-Type': 'application/test',
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
            'X-Timestamp': '1727184150.29665',
            'Accept-Ranges': 'bytes',
            'Content-Length': '4',
            'X-Manifest-Etag': 'b871773cf02434d498517245c7b88c11',
            'X-Trans-Id': 'test-txn-id',
            'X-Openstack-Request-Id': 'test-txn-id',
            'Date': 'Tue, 24 Sep 2024 13:22:30 GMT',
            'Etag': '"de64a5af184e6f732a26328c13d4ab25"',
        }
        self.app.register(method, '/v1/a/c/o', swob.HTTPOk, get_resp_headers,
                          b'test')
        exp_calls = [call[:2] for call in self.exp_calls] + \
                    [(method, '/v1/a/c/o')]
        req = Request.blank('/v1/a/c/o')
        req.method = method
        resp = req.get_response(self.mw)
        self.assertEqual(200, resp.status_int)
        self.assertEqual(exp_calls, self.app.calls)
        self.assertEqual(get_resp_headers, resp.headers)
        return resp

    def test_get_not_mpu(self):
        resp = self._do_test_get_head_not_mpu('GET')
        self.assertEqual(b'test', resp.body)

    def test_head_not_mpu(self):
        resp = self._do_test_get_head_not_mpu('HEAD')
        self.assertEqual(b'', resp.body)

    def test_put_not_mpu(self):
        registered = [
            ('HEAD', '/v1/a', swob.HTTPOk, {}),
            ('HEAD', '/v1/a/c', swob.HTTPOk, {}),
            ('HEAD', '/v1/.a/\x00history\x00c', HTTPCreated, {}),
            ('PUT', '/v1/a/c/o', swob.HTTPCreated, {}, b'')]
        [self.app.register(*call) for call in registered]
        exp_calls = [call[:2] for call in registered]
        req = Request.blank('/v1/a/c/o',
                            headers={'X-Timestamp': '12345.00000'})
        req.method = 'PUT'
        resp = req.get_response(self.mw)
        self.assertEqual(201, resp.status_int)
        self.assertEqual(exp_calls, self.app.calls)
        put_call = self.app.call_list[-1]
        self.assertEqual({'Host': 'localhost:80',
                          'X-Timestamp': mock.ANY},
                         dict(put_call.headers))
        self.assertIn('swift.container_updates', put_call.env)
        self.assertEqual(
            [{'op': 'PUT',
              'account': '.a',
              'container': '\x00history\x00c',
              'headers': {
                  'x-content-type': 'application/x-phony;swift_source=mpu',
                  'x-size': '0'},
              'obj': self.obj_name + '\x00null'}],
            put_call.env['swift.container_updates'])

    def test_delete_not_mpu(self):
        registered = [
            ('HEAD', '/v1/a', swob.HTTPOk, {}),
            ('HEAD', '/v1/a/c', swob.HTTPOk, {}),
            ('HEAD', '/v1/.a/\x00history\x00c', HTTPNoContent, {}),
            ('DELETE', '/v1/a/c/o', swob.HTTPNoContent, {}, b'')]
        [self.app.register(*call) for call in registered]
        exp_calls = [call[:2] for call in registered]
        req = Request.blank('/v1/a/c/o',
                            headers={'X-Timestamp': '12345.00000'})
        req.method = 'DELETE'
        resp = req.get_response(self.mw)
        self.assertEqual(204, resp.status_int)
        self.assertEqual(exp_calls, self.app.calls)
        delete_call = self.app.call_list[-1]
        self.assertEqual({'Host': 'localhost:80',
                          'X-Timestamp': mock.ANY},
                         dict(delete_call.headers))
        self.assertIn('swift.container_updates', delete_call.env)
        self.assertEqual(
            [{'op': 'DELETE',
              'account': '.a',
              'container': '\x00history\x00c',
              'headers': {
                  'x-content-type': 'application/x-phony;swift_source=mpu',
                  'x-size': '0'},
              'obj': self.obj_name + '\x00null'}],
            delete_call.env['swift.container_updates'])


class TestMpuMiddlewareErrors(BaseTestMPUMiddleware):
    def setUp(self):
        super().setUp()

    def _make_upload_part_req(self):
        return Request.blank(
            '/v1/a/c/o?upload-id=%s&part-number=1'
            % self.external_upload_id,
            method='PUT')

    def _make_list_parts_req(self):
        return Request.blank(
            '/v1/a/c/o?upload-id=%s' % self.external_upload_id,
            environ={'REQUEST_METHOD': 'GET'})

    def _make_complete_upload_req(self):
        return Request.blank(
            '/v1/a/c/o?upload-id=%s' % self.external_upload_id,
            method='POST',
            body=json.dumps(
                [{'part_number': 1, 'etag': MD5_OF_EMPTY_STRING}]
            ).encode('ascii'))

    def _make_abort_upload_req(self):
        return Request.blank(
            '/v1/a/c/o?upload-id=%s' % self.external_upload_id,
            method='DELETE')

    def _make_list_uploads_req(self):
        return Request.blank(
            '/v1/a/c?uploads=true',
            method='GET')

    def _make_create_upload_req(self):
        return Request.blank(
            '/v1/a/c/o?uploads=true',
            method='POST')

    def test_api_requests_invalid_upload_id(self):
        def do_test(ext_upload_id_str):
            req = Request.blank(
                '/v1/a/c/%s?upload-id=%s&part-number=1'
                % (quote(self.obj_name), ext_upload_id_str),
                environ={'REQUEST_METHOD': 'PUT'})
            return req.get_response(self.mw)

        self.app.register('HEAD', '/v1/a/c', HTTPNotFound, {})
        # sanity check - mpu id is valid
        resp = do_test(self.external_upload_id)
        self.assertEqual(404, resp.status_int, resp.body)

        resp = do_test('')
        self.assertEqual(400, resp.status_int, resp.body)
        resp = do_test(self.external_upload_id[:-1])
        self.assertEqual(400, resp.status_int, resp.body)
        resp = do_test(self.external_upload_id[:-4] + '====')
        self.assertEqual(400, resp.status_int, resp.body)
        resp = do_test(self.external_upload_id[:64] + 'a' * 64)
        self.assertEqual(400, resp.status_int, resp.body)
        # mpu id belongs to different path
        bad_upload_id = self.mw.externalize_upload_id(
            '/v1/a/other_c/' + quote(self.obj_name), self.upload_id)
        resp = do_test(bad_upload_id)
        self.assertEqual(400, resp.status_int, resp.body)

    def test_api_requests_user_container_not_found(self):
        self.app.register('HEAD', '/v1/a/c', HTTPNotFound, {})

        def do_test(req):
            self.app.clear_calls()
            resp = req.get_response(self.mw)
            self.assertEqual(404, resp.status_int)
            self.assertEqual([call[:2] for call in self.exp_calls],
                             self.app.calls)

        do_test(self._make_create_upload_req())
        do_test(self._make_list_uploads_req())
        do_test(self._make_upload_part_req())
        do_test(self._make_list_parts_req())
        do_test(self._make_complete_upload_req())
        do_test(self._make_abort_upload_req())

    def test_api_requests_user_container_unavailable(self):
        self.app.register('HEAD', '/v1/a/c', HTTPServiceUnavailable, {})

        def do_test(req):
            self.app.clear_calls()
            resp = req.get_response(self.mw)
            self.assertEqual(503, resp.status_int)
            self.assertEqual([call[:2] for call in self.exp_calls],
                             self.app.calls)

        do_test(self._make_create_upload_req())
        do_test(self._make_list_uploads_req())
        do_test(self._make_upload_part_req())
        do_test(self._make_list_parts_req())
        do_test(self._make_complete_upload_req())
        do_test(self._make_abort_upload_req())

    def test_api_requests_user_unexpected_error(self):
        self.app.register('HEAD', '/v1/a/c', HTTPPreconditionFailed, {})

        def do_test(req):
            self.app.clear_calls()
            resp = req.get_response(self.mw)
            self.assertEqual(503, resp.status_int)
            self.assertEqual([call[:2] for call in self.exp_calls],
                             self.app.calls)

        do_test(self._make_create_upload_req())
        do_test(self._make_list_uploads_req())
        do_test(self._make_upload_part_req())
        do_test(self._make_list_parts_req())
        do_test(self._make_complete_upload_req())
        do_test(self._make_abort_upload_req())

    def test_session_requests_with_earlier_timestamp(self):
        ts_older = next(self.ts_iter)
        ts_session_data = next(self.ts_iter)
        ts_newer = next(self.ts_iter)
        # request timestamp must be greater than this session timestamp...
        ts_session_meta = next(self.ts_iter)

        self._setup_mpu_existence_check_call(
            ts_session_data, ts_meta=ts_session_meta)

        def do_test(req, ts_req):
            req.headers['X-Timestamp'] = ts_req.internal
            resp = req.get_response(self.mw)
            self.assertEqual(
                409, resp.status_int,
                '%s %s %s %s' % (req.method, req.path, req.params, resp.body))

        do_test(self._make_upload_part_req(), ts_older)
        do_test(self._make_list_parts_req(), ts_older)
        do_test(self._make_complete_upload_req(), ts_older)
        do_test(self._make_abort_upload_req(), ts_older)

        do_test(self._make_upload_part_req(), ts_session_data)
        do_test(self._make_list_parts_req(), ts_session_data)
        do_test(self._make_complete_upload_req(), ts_session_data)
        do_test(self._make_abort_upload_req(), ts_session_data)

        do_test(self._make_upload_part_req(), ts_newer)
        do_test(self._make_list_parts_req(), ts_newer)
        do_test(self._make_complete_upload_req(), ts_newer)
        do_test(self._make_abort_upload_req(), ts_newer)

        do_test(self._make_upload_part_req(), ts_session_meta)
        do_test(self._make_list_parts_req(), ts_session_meta)
        do_test(self._make_complete_upload_req(), ts_session_meta)
        do_test(self._make_abort_upload_req(), ts_session_meta)
