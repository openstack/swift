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
import binascii
import contextlib
import json
import unittest
import urllib

from unittest import mock

from swift.common import swob, registry
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.middleware import mpu
from swift.common.middleware.mpu import MPUMiddleware, MPUId, \
    get_req_upload_id, normalize_part_number, \
    MPUSession, BaseMPUHandler, MPUEtagHasher
from swift.common.swob import Request, HTTPOk, HTTPNotFound, HTTPCreated, \
    HTTPAccepted, HTTPServiceUnavailable, HTTPPreconditionFailed, \
    HTTPException, HTTPBadRequest, wsgi_quote, HTTPNoContent, \
    HTTPInternalServerError, HTTPConflict
from swift.common.utils import md5, quote, Timestamp, MD5_OF_EMPTY_STRING
from test.debug_logger import debug_logger
from swift.proxy.controllers.base import ResponseCollection, ResponseData
from test.unit import make_timestamp_iter
from test.unit.common.middleware.helpers import FakeSwift


@contextlib.contextmanager
def mock_generate_unique_id(fake_id):
    with mock.patch('swift.common.middleware.mpu.generate_unique_id',
                    return_value=fake_id):
        yield


class TestModuleFunctions(unittest.TestCase):
    def test_get_upload_id(self):
        req = Request.blank('/v1/a/c/o')
        self.assertIsNone(get_req_upload_id(req))

        ts = Timestamp.now()
        mpu_id = MPUId.create(req.path, ts)
        req = Request.blank('/v1/a/c/o', params={'upload-id': str(mpu_id)})
        req_upload_id = get_req_upload_id(req)
        self.assertEqual(mpu_id, req_upload_id)
        self.assertEqual(ts, req_upload_id.timestamp)

        ts = Timestamp.now(offset=123)
        mpu_id = MPUId.create(req.path, ts)
        req = Request.blank('/v1/a/c/o', params={'upload-id': str(mpu_id)})
        req_upload_id = get_req_upload_id(req)
        self.assertEqual(mpu_id, req_upload_id)
        self.assertEqual(ts, req_upload_id.timestamp)

    def test_get_upload_id_invalid(self):
        def do_test_bad_value(value):
            with self.assertRaises(HTTPException) as cm:
                req = Request.blank('/v1/a/c/o', params={'upload-id': value})
                get_req_upload_id(req)
            self.assertEqual(400, cm.exception.status_int)

        do_test_bad_value('')
        do_test_bad_value(None)
        do_test_bad_value('my-uuid')
        do_test_bad_value('my-uuid:')
        do_test_bad_value(':%s' % Timestamp.now().internal)
        do_test_bad_value('my-uuid:xyz')

    def test_normalize_part_number(self):
        self.assertEqual('000001', normalize_part_number(1))
        self.assertEqual('000011', normalize_part_number('11'))
        self.assertEqual('000111', normalize_part_number('000111'))


class TestMPUId(unittest.TestCase):
    def test_create(self):
        timestamp = Timestamp.now()
        with mock.patch('swift.common.middleware.mpu.generate_unique_id',
                        return_value='my-uuid'):
            with mock.patch('swift.common.middleware.mpu.hash_path',
                            return_value='my-hash-path'):
                mpu_id = MPUId.create('/v1/a/c/o', timestamp)
        self.assertEqual('my-uuid', mpu_id.uuid)
        self.assertEqual(timestamp, mpu_id.timestamp)
        self.assertEqual('my-hash-path', mpu_id.tag)
        self.assertEqual('%s~my-uuid~my-hash-path' % timestamp.internal,
                         str(mpu_id))
        # Python 3.7 updates from using RFC 2396 to RFC 3986 to quote URL
        # strings. Now, "~" is included in the set of unreserved characters.
        self.assertEqual(str(mpu_id),
                         urllib.parse.quote(str(mpu_id), safe='~'))

    def test_unique(self):
        timestamp = Timestamp.now()
        mpu_id1 = MPUId.create('/v1/a/c/o', timestamp)
        mpu_id2 = MPUId.create('/v1/a/c/o', timestamp)
        self.assertEqual(mpu_id1.timestamp, mpu_id2.timestamp)
        self.assertNotEqual(mpu_id1, mpu_id2)
        self.assertNotEqual(mpu_id1.uuid, mpu_id2.uuid)

    def test_parse(self):
        timestamp = Timestamp.now()
        mpu_id_str = '%s~my-uuid~my-hash-path' % timestamp.internal
        mpu_id = MPUId.parse(mpu_id_str)
        self.assertEqual('my-uuid', mpu_id.uuid)
        self.assertEqual(timestamp, mpu_id.timestamp)
        self.assertEqual('my-hash-path', mpu_id.tag)
        self.assertEqual(mpu_id_str, str(mpu_id))

    def test_parse_checks_tag(self):
        timestamp = Timestamp.now()
        path1 = '/v1/a/c/o1'
        path2 = '/v1/a/c/o2'
        mpu_id1 = MPUId.create(path1, timestamp)
        mpu_id2 = MPUId.create(path2, timestamp)
        self.assertNotEqual(mpu_id1.tag, mpu_id2.tag)

        self.assertEqual(mpu_id1, MPUId.parse(str(mpu_id1), path=path1))
        self.assertEqual(mpu_id2, MPUId.parse(str(mpu_id2), path=path2))

        with self.assertRaises(ValueError):
            MPUId.parse(str(mpu_id1), path=path2)
        with self.assertRaises(ValueError):
            MPUId.parse(str(mpu_id2), path=path1)

    def test_create_parse(self):
        timestamp = Timestamp.now()
        mpu_id1 = MPUId.create('/v1/a/c/o', timestamp)
        mpu_id2 = MPUId.parse(str(mpu_id1))
        self.assertEqual(timestamp, mpu_id2.timestamp)
        self.assertEqual(mpu_id1.uuid, mpu_id2.uuid)
        self.assertEqual(str(mpu_id1), str(mpu_id2))

    def test_eq(self):
        timestamp = Timestamp.now()
        mpu_id1a = MPUId.create('/v1/a/c/o', timestamp)
        mpu_id1b = MPUId.parse(str(mpu_id1a))
        self.assertEqual(mpu_id1a, mpu_id1b)
        self.assertEqual(mpu_id1a, str(mpu_id1b))

        mpu_id2 = MPUId.create('/v1/a/c/o', timestamp)
        self.assertNotEqual(mpu_id1a, mpu_id2)

    def test_max(self):
        self.assertEqual(
            '9999999999.99999'
            '~zzzzzzzzzzzzzzzzzzzzzzzz'
            '~ffffffffffffffffffffffffffffffff', str(MPUId.max()))
        self.assertLess(str(MPUId.create('/v1/a/c/o', Timestamp.now())),
                        str(MPUId.max()))


class TestMPUSession(unittest.TestCase):
    def test_init(self):
        sess = MPUSession('mysession', Timestamp(123))
        self.assertEqual('mysession', sess.name)
        self.assertEqual(Timestamp(123), sess.meta_timestamp)
        self.assertEqual(Timestamp(123), sess.data_timestamp)
        self.assertEqual('application/x-mpu-session-created',
                         sess.content_type)
        self.assertEqual({}, sess.headers)
        self.assertTrue(sess.is_active)
        self.assertFalse(sess.is_aborted)
        self.assertFalse(sess.is_completed)

    def test_init_non_default(self):
        headers = {'X-Object-Sysmeta-Mpu-Fruit': 'apple'}
        sess = MPUSession('mysession', Timestamp(123),
                          content_type='application/x-mpu-session-aborted',
                          headers=headers)
        self.assertEqual('mysession', sess.name)
        self.assertEqual(Timestamp(123), sess.meta_timestamp)
        self.assertEqual(Timestamp(123), sess.data_timestamp)
        self.assertEqual('application/x-mpu-session-aborted',
                         sess.content_type)
        self.assertEqual(headers, sess.headers)
        self.assertFalse(sess.is_active)
        self.assertTrue(sess.is_aborted)
        self.assertFalse(sess.is_completed)

    def test_get_manifest_headers(self):
        headers = {
            'X-Object-Sysmeta-Mpu-User-X-Object-Meta-Fruit': 'apple',
            'X-Object-Sysmeta-Mpu-Content-Type': 'user/type',
            'X-Object-Sysmeta-Mpu-User-Content-Disposition': 'attachment',
            'X-Object-Sysmeta-Mpu-User-Content-Encoding': 'none',
            'X-Object-Sysmeta-Mpu-User-Content-Language': 'en-US',
            'X-Object-Sysmeta-Mpu-User-Cache-Control': 'no-cache',
            'X-Object-Sysmeta-Mpu-User-Expires':
                'Wed, 25 Dec 2024 04:04:04 GMT',
        }
        sess = MPUSession('mysession', Timestamp(123), headers=headers)
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

    def test_set_completed(self):
        sess = MPUSession('mysession', Timestamp(123))
        exp_post_headers = {
            'Content-Type': 'application/x-mpu-session-created',
            'X-Timestamp': '0000000123.00000'}
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
        sess = MPUSession('mysession', Timestamp(123))
        exp_post_headers = {
            'Content-Type': 'application/x-mpu-session-created',
            'X-Timestamp': '0000000123.00000'}
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
        sess = MPUSession('mysession', Timestamp(123))
        exp_post_headers = {
            'Content-Type': 'application/x-mpu-session-created',
            'X-Timestamp': '0000000123.00000'}
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
        self.mpu_name = 'o'
        self.mpu_name = 'o'
        self.mpu_path = '/v1/a/c/o'
        self.mpu_id = self._make_mpu_id(self.mpu_path)
        self.debug_logger = debug_logger()
        self.exp_calls = []
        self._setup_user_ac_info_requests()
        self.mw = MPUMiddleware(self.app, {}, logger=self.debug_logger)

    @property
    def sess_name(self):
        return '\x00%s/%s' % (self.mpu_name, self.mpu_id)

    def _make_mpu_id(self, path):
        return MPUId.create(path, next(self.ts_iter))

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
            'X-Object-Sysmeta-Mpu-User-X-Object-Meta-Foo': 'blah',
            'X-Object-Sysmeta-Mpu-Content-Type': 'application/test',
        })
        headers.update(extra_headers or {})
        call = ('HEAD', '/v1/a/\x00mpu_sessions\x00c/%s' % self.sess_name,
                HTTPOk,
                headers)
        self.app.register(*call)
        self.exp_calls.append(call)


class TestBaseMpuHandler(BaseTestMPUMiddleware):
    def test_make_subrequest(self):
        req = Request.blank(path='%s?orig=blah' % self.mpu_path,
                            headers={'Content-Type': 'foo'},)
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
    def setUp(self):
        super(TestMPUMiddleware, self).setUp()
        self.sample_in_progress_session_listing = [
            # in progress
            {'name': '\x00obj1/%s' % self._make_mpu_id('/v1/a/c/obj1'),
             'hash': 'etag',
             'bytes': 0,
             'content_type': 'application/x-mpu-session-created',
             'last_modified': '1970-01-01T00:00:00.000000'},
            {'name': '\x00obj1/%s' % self._make_mpu_id('/v1/a/c/obj1'),
             'hash': 'etag',
             'bytes': 0,
             'content_type': 'application/x-mpu-session-created',
             'last_modified': '1970-01-01T00:00:00.000000'},
            {'name': '\x00obj2\N{SNOWMAN}/%s'
                     % self._make_mpu_id('/v1/a/c/obj2\N{SNOWMAN}'),
             'hash': 'etag',
             'bytes': 0,
             'content_type': 'application/x-mpu-session-created',
             'last_modified': '1970-01-01T00:00:00.000000'},
        ]
        self.sample_all_session_listing = \
            self.sample_in_progress_session_listing + [
                # aborted
                {'name': '\x00obj3/%s' % self._make_mpu_id('/v1/a/c/obj3'),
                 'hash': 'etag',
                 'bytes': 0,
                 'content_type': 'application/x-mpu-session-aborted',
                 'last_modified': '1970-01-01T00:00:00.000000'},
                # completed
                {'name': '\x00obj3/%s' % self._make_mpu_id('/v1/a/c/obj3'),
                 'hash': 'etag',
                 'bytes': 0,
                 'content_type': 'application/x-mpu-session-completed',
                 'last_modified': '1970-01-01T00:00:00.000000'},
            ]

    def _setup_mpu_create_requests(self):
        registered = [
            ('HEAD', '/v1/a/\x00mpu_sessions\x00c', HTTPNotFound, {}),
            ('PUT', '/v1/a/\x00mpu_sessions\x00c', HTTPCreated, {}),
            ('HEAD', '/v1/a/\x00mpu_parts\x00c', HTTPNotFound, {}),
            ('PUT', '/v1/a/\x00mpu_parts\x00c', HTTPCreated, {}),
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
        self.assertEqual(933, mw.max_name_length)
        self.assertEqual({'max_part_number': 10000,
                          'min_part_size': 5242880,
                          'max_name_length': 933},
                         registry.get_swift_info().get('mpu'))

    def test_filter_factory_custom_conf(self):
        def do_test(conf):
            app = object()
            mw = mpu.filter_factory(conf)(app)
            self.assertIsInstance(mw, MPUMiddleware)
            self.assertIs(app, mw.app)
            self.assertEqual(999, mw.max_part_number)
            self.assertEqual(1048576, mw.min_part_size)
            self.assertEqual(933, mw.max_name_length)
            self.assertEqual({'max_part_number': 999,
                              'min_part_size': 1048576,
                              'max_name_length': 933},
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

    def _do_test_create_mpu(self, req_headers):
        expected = self._setup_mpu_create_requests()
        self.app.register(
            'PUT', '/v1/a/\x00mpu_sessions\x00c/%s' % self.sess_name,
            HTTPCreated, {})
        expected.append(('PUT',
                         '/v1/a/\x00mpu_sessions\x00c/%s' % self.sess_name))
        req = Request.blank('/v1/a/c/%s?uploads=true' % self.mpu_name,
                            headers=req_headers)
        req.method = 'POST'
        with mock.patch('swift.common.middleware.mpu.MPUId.create',
                        return_value=self.mpu_id):
            resp = req.get_response(self.mw)
        self.assertEqual(202, resp.status_int)
        self.assertIs(req, resp.request)
        self.assertIn('X-Upload-Id', resp.headers)
        self.assertEqual(expected, self.app.calls)

    def test_create_mpu(self):
        req_headers = {'X-Object-Meta-Foo': 'blah'}
        ts_now = Timestamp.now()
        with mock.patch('swift.common.utils.Timestamp.now',
                        return_value=ts_now):
            self._do_test_create_mpu(req_headers)
        self.assertEqual(
            {'Content-Length': '0',
             'Content-Type': 'application/x-mpu-session-created',
             'X-Timestamp': ts_now.internal,
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Object-Sysmeta-Mpu-User-X-Object-Meta-Foo': 'blah',
             'X-Object-Sysmeta-Mpu-Content-Type': 'application/octet-stream'},
            self.app.headers[-1])

    def test_create_mpu_name_too_long(self):
        mpu_name = 'x' * 1024
        req = Request.blank('/v1/a/c/%s?uploads=true' % mpu_name)
        req.method = 'POST'
        resp = req.get_response(self.mw)
        self.assertEqual(400, resp.status_int)
        self.assertEqual(b'MPU object name length of 1024 longer than 933',
                         resp.body)

    def test_create_mpu_with_x_timestamp(self):
        ts_now = Timestamp(1234567.123)
        req_headers = {'X-Object-Meta-Foo': 'blah',
                       'X-Timestamp': ts_now.internal}
        self._do_test_create_mpu(req_headers)
        self.assertEqual(
            {'Content-Length': '0',
             'Content-Type': 'application/x-mpu-session-created',
             'X-Timestamp': ts_now.internal,
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Object-Sysmeta-Mpu-User-X-Object-Meta-Foo': 'blah',
             'X-Object-Sysmeta-Mpu-Content-Type': 'application/octet-stream'},
            self.app.headers[-1])

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
             'X-Object-Sysmeta-Mpu-User-X-Object-Meta-Foo': 'blah',
             'X-Object-Sysmeta-Mpu-Content-Type': 'application/test'},
            self.app.headers[-1])

    def test_create_mpu_detects_content_type(self):
        self.mpu_name = 'o.html'
        headers = {'X-Object-Meta-Foo': 'blah'}
        self._do_test_create_mpu(headers)
        self.assertEqual(
            {'Content-Length': '0',
             'Content-Type': 'application/x-mpu-session-created',
             'X-Timestamp': mock.ANY,
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Object-Sysmeta-Mpu-User-X-Object-Meta-Foo': 'blah',
             'X-Object-Sysmeta-Mpu-Content-Type': 'text/html'},
            self.app.headers[-1])

    def test_create_mpu_existing_resource_containers(self):
        user_container_headers = {
            'X-Container-Sysmeta-Mpu-Parts-Container-0':
                wsgi_quote('\x00mpu_parts\x00c')
        }
        registered = [
            ('HEAD', '/v1/a', HTTPOk, {}),
            ('HEAD', '/v1/a/c', HTTPOk, user_container_headers),
            ('HEAD', '/v1/a/\x00mpu_sessions\x00c', HTTPOk, {}),
            ('HEAD', '/v1/a/\x00mpu_parts\x00c', HTTPNoContent, {}),
            ('PUT', '/v1/a/\x00mpu_sessions\x00c/\x00o/%s' % self.mpu_id,
             HTTPCreated, {})
        ]
        for call in registered:
            self.app.register(*call)
        expected = [call[:2] for call in registered]
        req = Request.blank('/v1/a/c/o?uploads=true')
        req.method = 'POST'
        with mock.patch('swift.common.middleware.mpu.MPUId.create',
                        return_value=self.mpu_id):
            resp = req.get_response(self.mw)
        self.assertEqual(202, resp.status_int)
        self.assertIs(req, resp.request)
        self.assertIn('X-Upload-Id', resp.headers)
        self.assertEqual(expected, self.app.calls)

    def test_create_mpu_fails_to_create_parts_container(self):
        expected = self._setup_mpu_create_requests()
        # replace previously registered call
        self.app.register(
            'PUT', '/v1/a/\x00mpu_parts\x00c', HTTPInternalServerError, {})
        req = Request.blank('/v1/a/c/%s?uploads=true' % self.mpu_name)
        req.method = 'POST'
        resp = req.get_response(self.mw)
        self.assertEqual(500, resp.status_int)
        self.assertEqual(b'Error creating MPU resource container', resp.body)
        self.assertIs(req, resp.request)
        self.assertEqual(expected[:-1], self.app.calls)

    def test_create_mpu_fails_to_create_sessions_container(self):
        expected = self._setup_mpu_create_requests()
        # replace previously registered call
        self.app.register(
            'PUT', '/v1/a/\x00mpu_sessions\x00c', HTTPInternalServerError, {})
        req = Request.blank('/v1/a/c/%s?uploads=true' % self.mpu_name)
        req.method = 'POST'
        resp = req.get_response(self.mw)
        self.assertEqual(500, resp.status_int)
        self.assertEqual(b'Error creating MPU resource container', resp.body)
        self.assertIs(req, resp.request)
        self.assertEqual(expected[:-3], self.app.calls)

    def test_create_mpu_fails_to_post_to_user_container(self):
        expected = self._setup_mpu_create_requests()
        # replace previously registered call
        self.app.register(
            'POST', '/v1/a/c', HTTPInternalServerError, {})
        req = Request.blank('/v1/a/c/%s?uploads=true' % self.mpu_name)
        req.method = 'POST'
        resp = req.get_response(self.mw)
        self.assertEqual(500, resp.status_int)
        self.assertEqual(b'Error writing MPU resource metadata', resp.body)
        self.assertIs(req, resp.request)
        self.assertEqual(expected, self.app.calls)

    def test_create_mpu_fails_to_create_session(self):
        expected = self._setup_mpu_create_requests()
        self.app.register(
            'PUT', '/v1/a/\x00mpu_sessions\x00c/%s' % self.sess_name,
            HTTPServiceUnavailable, {})
        expected.append(('PUT',
                         '/v1/a/\x00mpu_sessions\x00c/%s' % self.sess_name))
        req = Request.blank('/v1/a/c/%s?uploads=true' % self.mpu_name)
        req.method = 'POST'
        with mock.patch('swift.common.middleware.mpu.MPUId.create',
                        return_value=self.mpu_id):
            resp = req.get_response(self.mw)
        self.assertEqual(503, resp.status_int)
        self.assertIs(req, resp.request)
        exp_body = b''.join(HTTPServiceUnavailable()({}, lambda *args: None))
        self.assertEqual(exp_body, resp.body)
        self.assertEqual(expected, self.app.calls)

    def test_list_uploads(self):
        registered_calls = [
            ('GET', '/v1/a/\x00mpu_sessions\x00c', HTTPOk, {},
             json.dumps(self.sample_all_session_listing).encode('ascii')),
            ('GET', '/v1/a/\x00mpu_sessions\x00c', HTTPOk, {},
             json.dumps([]).encode('ascii'))
        ]
        self.app.register(*registered_calls[0])
        self.app.register_next_response(*registered_calls[1])
        req = Request.blank('/v1/a/c?uploads')
        req.method = 'GET'
        resp = req.get_response(self.mw)
        self.assertEqual(200, resp.status_int)

        exp_listing = [dict(item,
                            name=item['name'][1:].split('/', 1)[0],
                            upload_id=item['name'][1:].split('/', 1)[1])
                       for item in self.sample_in_progress_session_listing]
        self.assertEqual(exp_listing, json.loads(resp.body))
        expected = [call[:2] for call in self.exp_calls]
        self.assertEqual(expected, self.app.calls[:2])
        self.assertEqual(4, len(self.app.calls), self.app.calls)
        expected = [call[:2] for call in self.exp_calls]
        self.assertEqual(expected, self.app.calls[:2])

        # first backend listing
        self.assertEqual('GET', self.app.calls[2][0])
        parsed_path = urllib.parse.urlparse(self.app.calls[2][1])
        self.assertEqual('/v1/a/\x00mpu_sessions\x00c', parsed_path.path)
        params = dict(urllib.parse.parse_qsl(parsed_path.query,
                      keep_blank_values=True))
        self.assertEqual({}, params)
        # second backend listing
        self.assertEqual('GET', self.app.calls[3][0])
        parsed_path = urllib.parse.urlparse(self.app.calls[3][1])
        self.assertEqual('/v1/a/\x00mpu_sessions\x00c', parsed_path.path)
        params = dict(urllib.parse.parse_qsl(parsed_path.query,
                      keep_blank_values=True))
        self.assertEqual(
            {'marker': quote(self.sample_all_session_listing[-1]['name'])},
            params)

    def test_list_uploads_limit(self):
        registered_calls = [
            ('GET', '/v1/a/\x00mpu_sessions\x00c', HTTPOk, {},
             json.dumps(self.sample_all_session_listing).encode('ascii')),
        ]
        self.app.register(*registered_calls[0])
        req = Request.blank('/v1/a/c?uploads&limit=2')
        req.method = 'GET'
        resp = req.get_response(self.mw)
        self.assertEqual(200, resp.status_int)

        exp_listing = [dict(item,
                            name=item['name'][1:].split('/', 1)[0],
                            upload_id=item['name'][1:].split('/', 1)[1])
                       for item in self.sample_in_progress_session_listing[:2]]
        self.assertEqual(exp_listing, json.loads(resp.body))
        expected = [call[:2] for call in self.exp_calls]
        self.assertEqual(expected, self.app.calls[:2])
        self.assertEqual(3, len(self.app.calls), self.app.calls)
        expected = [call[:2] for call in self.exp_calls]
        self.assertEqual(expected, self.app.calls[:2])

        # first backend listing
        self.assertEqual('GET', self.app.calls[2][0])
        parsed_path = urllib.parse.urlparse(self.app.calls[2][1])
        self.assertEqual('/v1/a/\x00mpu_sessions\x00c', parsed_path.path)
        params = dict(urllib.parse.parse_qsl(parsed_path.query,
                      keep_blank_values=True))
        self.assertEqual({}, params)

    def test_list_uploads_with_prefix(self):
        registered_calls = [
            ('GET', '/v1/a/\x00mpu_sessions\x00c', HTTPOk, {},
             json.dumps(self.sample_all_session_listing).encode('ascii')),
            ('GET', '/v1/a/\x00mpu_sessions\x00c', HTTPOk, {},
             json.dumps([]).encode('ascii'))
        ]
        self.app.register(*registered_calls[0])
        self.app.register_next_response(*registered_calls[1])
        req = Request.blank(
            '/v1/a/c?uploads&prefix=bar&ignored=x')
        req.method = 'GET'
        resp = req.get_response(self.mw)
        self.assertEqual(200, resp.status_int)
        self.assertEqual(4, len(self.app.calls), self.app.calls)
        expected = [call[:2] for call in self.exp_calls]
        self.assertEqual(expected, self.app.calls[:2])

        # first backend listing
        self.assertEqual('GET', self.app.calls[2][0])
        parsed_path = urllib.parse.urlparse(self.app.calls[2][1])
        self.assertEqual('/v1/a/\x00mpu_sessions\x00c', parsed_path.path)
        params = dict(urllib.parse.parse_qsl(parsed_path.query,
                      keep_blank_values=True))
        self.assertEqual({'prefix': '\x00bar'}, params)
        # second backend listing
        self.assertEqual('GET', self.app.calls[3][0])
        parsed_path = urllib.parse.urlparse(self.app.calls[3][1])
        self.assertEqual('/v1/a/\x00mpu_sessions\x00c', parsed_path.path)
        params = dict(urllib.parse.parse_qsl(parsed_path.query,
                      keep_blank_values=True))
        self.assertEqual(
            {'marker': quote(self.sample_all_session_listing[-1]['name']),
             'prefix': '\x00bar'},
            params)

    def test_list_uploads_with_marker_and_no_upload_id_marker(self):
        registered_calls = [
            ('GET', '/v1/a/\x00mpu_sessions\x00c', HTTPOk, {},
             json.dumps(self.sample_all_session_listing).encode('ascii')),
            ('GET', '/v1/a/\x00mpu_sessions\x00c', HTTPOk, {},
             json.dumps([]).encode('ascii'))
        ]
        self.app.register(*registered_calls[0])
        self.app.register_next_response(*registered_calls[1])
        req = Request.blank(
            '/v1/a/c?uploads&marker=foo&prefix=bar&ignored=x')
        req.method = 'GET'
        resp = req.get_response(self.mw)
        self.assertEqual(200, resp.status_int)
        self.assertEqual(4, len(self.app.calls), self.app.calls)
        expected = [call[:2] for call in self.exp_calls]
        self.assertEqual(expected, self.app.calls[:2])

        # first backend listing
        self.assertEqual('GET', self.app.calls[2][0])
        parsed_path = urllib.parse.urlparse(self.app.calls[2][1])
        self.assertEqual('/v1/a/\x00mpu_sessions\x00c', parsed_path.path)
        params = dict(urllib.parse.parse_qsl(parsed_path.query,
                      keep_blank_values=True))
        self.assertEqual({'marker': '\x00foo/9999999999.99999'
                                    '~zzzzzzzzzzzzzzzzzzzzzzzz'
                                    '~ffffffffffffffffffffffffffffffff',
                          'prefix': '\x00bar'},
                         params)
        # second backend listing
        self.assertEqual('GET', self.app.calls[3][0])
        parsed_path = urllib.parse.urlparse(self.app.calls[3][1])
        self.assertEqual('/v1/a/\x00mpu_sessions\x00c', parsed_path.path)
        params = dict(urllib.parse.parse_qsl(parsed_path.query,
                      keep_blank_values=True))
        self.assertEqual(
            {'marker': quote(self.sample_all_session_listing[-1]['name']),
             'prefix': '\x00bar'},
            params)

    def test_list_uploads_with_marker_and_upload_id_marker(self):
        registered_calls = [
            ('GET', '/v1/a/\x00mpu_sessions\x00c', HTTPOk, {},
             json.dumps(self.sample_all_session_listing).encode('ascii')),
            ('GET', '/v1/a/\x00mpu_sessions\x00c', HTTPOk, {},
             json.dumps([]).encode('ascii'))
        ]
        self.app.register(*registered_calls[0])
        self.app.register_next_response(*registered_calls[1])
        req = Request.blank(
            '/v1/a/c?uploads&marker=foo&upload-id-marker=123&ignored=x')
        req.method = 'GET'
        resp = req.get_response(self.mw)
        self.assertEqual(200, resp.status_int)

        # first backend listing
        self.assertEqual('GET', self.app.calls[2][0])
        parsed_path = urllib.parse.urlparse(self.app.calls[2][1])
        self.assertEqual('/v1/a/\x00mpu_sessions\x00c', parsed_path.path)
        params = dict(urllib.parse.parse_qsl(parsed_path.query,
                      keep_blank_values=True))
        self.assertEqual({'marker': '\x00foo/123'}, params)
        # second backend listing
        self.assertEqual('GET', self.app.calls[3][0])
        parsed_path = urllib.parse.urlparse(self.app.calls[3][1])
        self.assertEqual('/v1/a/\x00mpu_sessions\x00c', parsed_path.path)
        params = dict(urllib.parse.parse_qsl(parsed_path.query,
                      keep_blank_values=True))
        self.assertEqual(
            {'marker': quote(self.sample_all_session_listing[-1]['name'])},
            params)

    def test_list_uploads_with_no_marker_and_upload_id_marker(self):
        registered_calls = [
            ('GET', '/v1/a/\x00mpu_sessions\x00c', HTTPOk, {},
             json.dumps(self.sample_all_session_listing).encode('ascii')),
            ('GET', '/v1/a/\x00mpu_sessions\x00c', HTTPOk, {},
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
        self.assertEqual('/v1/a/\x00mpu_sessions\x00c', parsed_path.path)
        params = dict(urllib.parse.parse_qsl(parsed_path.query,
                      keep_blank_values=True))
        self.assertFalse(params)
        # second backend listing
        self.assertEqual('GET', self.app.calls[3][0])
        parsed_path = urllib.parse.urlparse(self.app.calls[3][1])
        self.assertEqual('/v1/a/\x00mpu_sessions\x00c', parsed_path.path)
        params = dict(urllib.parse.parse_qsl(parsed_path.query,
                      keep_blank_values=True))
        self.assertEqual(
            {'marker': quote(self.sample_all_session_listing[-1]['name'])},
            params)

    def test_list_uploads_subrequest_503(self):
        registered_calls = [
            ('GET', '/v1/a/\x00mpu_sessions\x00c',
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
            ('GET', '/v1/a/\x00mpu_sessions\x00c',
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
             '/v1/a/\x00mpu_parts\x00c/%s/000001' % self.sess_name,
             HTTPCreated, {})]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank(
            '/v1/a/c/o?upload-id=%s&part-number=%s' % (self.mpu_id, part_str),
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
        self.assertEqual(201, resp.status_int)
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
             '/v1/a/\x00mpu_parts\x00c/%s/000001' % self.sess_name,
             HTTPNotFound,
             {})
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank(
            '/v1/a/c/o?upload-id=%s&part-number=1' % self.mpu_id,
            environ={'REQUEST_METHOD': 'PUT'},
            body=b'testing')
        with mock.patch('swift.common.utils.Timestamp.now',
                        return_value=next(self.ts_iter)):
            resp = req.get_response(self.mw)
        self.assertEqual(503, resp.status_int)
        exp_body = b''.join(HTTPServiceUnavailable()({}, lambda *args: None))
        self.assertEqual(exp_body, resp.body)

    def test_upload_part_subrequest_503(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        registered_calls = [
            ('PUT',
             '/v1/a/\x00mpu_parts\x00c/%s/000001' % self.sess_name,
             HTTPServiceUnavailable,
             {})
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank(
            '/v1/a/c/o?upload-id=%s&part-number=1' % self.mpu_id,
            environ={'REQUEST_METHOD': 'PUT'},
            body=b'testing')
        with mock.patch('swift.common.utils.Timestamp.now',
                        return_value=next(self.ts_iter)):
            resp = req.get_response(self.mw)
        self.assertEqual(503, resp.status_int)
        exp_body = b''.join(HTTPServiceUnavailable()({}, lambda *args: None))
        self.assertEqual(exp_body, resp.body)

    def test_upload_part_session_aborted(self):
        ts_session = next(self.ts_iter)
        extra_hdrs = {'Content-Type': 'application/x-mpu-session-aborted'}
        self._setup_mpu_existence_check_call(
            ts_session, extra_headers=extra_hdrs)
        req = Request.blank(
            '/v1/a/c/o?upload-id=%s&part-number=1' % self.mpu_id,
            environ={'REQUEST_METHOD': 'PUT'},
            body=b'testing')
        with mock.patch('swift.common.utils.Timestamp.now',
                        return_value=next(self.ts_iter)):
            resp = req.get_response(self.mw)
        self.assertEqual(404, resp.status_int)
        self.assertEqual(b'No such upload-id', resp.body)

    def test_upload_part_session_not_found(self):
        # session not created or completed
        ts_session = next(self.ts_iter)
        registered_calls = [
            ('HEAD',
             '/v1/a/\x00mpu_sessions\x00c/%s' % self.sess_name,
             HTTPNotFound,
             {'X-Backend-Delete-Timestamp': ts_session.internal}
             ),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank(
            '/v1/a/c/o?upload-id=%s&part-number=1' % self.mpu_id,
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
        listing = [{'name': '%s/%06d' % (self.sess_name, i),
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
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id)
        with mock.patch('swift.common.utils.Timestamp.now',
                        return_value=next(self.ts_iter)):
            resp = req.get_response(self.mw)
        self.assertEqual(200, resp.status_int)
        expected = [call[:2] for call in self.exp_calls] + [
            ('GET', '/v1/a/\x00mpu_parts\x00c?prefix=%s'
             % quote('%s' % self.sess_name, safe='')),
        ]
        self.assertEqual(expected, self.app.calls)
        exp_listing = [{'name': 'o/%s/%06d' % (self.mpu_id, i),
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
            % self.mpu_id)
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
        self.assertEqual({'marker': '%s/000001' % self.sess_name,
                          'prefix': '%s' % self.sess_name},
                         params)

    def test_list_parts_subrequest_404(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        registered_calls = [
            ('GET',
             '/v1/a/\x00mpu_parts\x00c',
             HTTPNotFound,
             {})
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id)
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
            ('GET', '/v1/a/\x00mpu_parts\x00c?prefix=%s'
             % quote('%s' % self.sess_name, safe='')),
        ]
        self.assertEqual(expected, self.app.calls)

    def test_list_parts_subrequest_503(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        registered_calls = [
            ('GET',
             '/v1/a/\x00mpu_parts\x00c',
             HTTPServiceUnavailable,
             {})
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id)
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
            ('GET', '/v1/a/\x00mpu_parts\x00c?prefix=%s'
             % quote('%s' % self.sess_name, safe='')),
        ]
        self.assertEqual(expected, self.app.calls)

    def test_complete_mpu(self):
        ts_session = next(self.ts_iter)
        ts_session.offset = 123
        self._setup_mpu_existence_check_call(ts_session)
        ts_complete = next(self.ts_iter)
        put_slo_resp_body = {'Response Status': '201 Created',
                             'Etag': 'slo-etag'}
        registered_calls = [
            ('POST',
             '/v1/a/\x00mpu_sessions\x00c/%s' % self.sess_name,
             HTTPOk,
             {}),
            # SLO heartbeat response is 202...
            ('PUT',
             '/v1/a/c/o?heartbeat=on&multipart-manifest=put',
             HTTPAccepted,
             {},
             json.dumps(put_slo_resp_body).encode('ascii')),
            ('POST',
             '/v1/a/\x00mpu_sessions\x00c/%s' % self.sess_name,
             HTTPAccepted,
             {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored',
                    'X-Object-Sysmeta-Container-Update-Override-Etag':
                        'ignored-etag;foo=ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
                            headers=req_hdrs)
        req.method = 'POST'
        mpu_manifest = [
            {'part_number': 1, 'etag': 'a' * 32},
            {'part_number': 2, 'etag': 'b' * 32},
        ]
        mpu_etag_hasher = md5(usedforsecurity=False)
        for part in mpu_manifest:
            mpu_etag_hasher.update(binascii.a2b_hex(part['etag']))
        exp_mpu_etag = mpu_etag_hasher.hexdigest() + '-2'
        req.body = json.dumps(mpu_manifest)
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(202, resp.status_int)
        resp_dict = json.loads(resp_body)
        self.assertEqual(
            {"Response Status": "201 Created",
             "Etag": exp_mpu_etag},
            resp_dict)
        expected = [call[:2]
                    for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

        session_hdrs = self.app.headers[3]
        self.assertEqual(
            {'Content-Type': 'application/x-mpu-session-completing',
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Timestamp': ts_complete.internal},
            session_hdrs)

        actual_manifest_body = self.app.uploaded.get('/v1/a/c/o')[1]
        self.assertEqual(
            [{"path": "\x00mpu_parts\x00c/%s/000001" % self.sess_name,
              "etag": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
             {"path": "\x00mpu_parts\x00c/%s/000002" % self.sess_name,
              "etag": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}],
            json.loads(actual_manifest_body))
        # the session timestamp's offset is preserved...
        exp_put_ts = Timestamp(ts_session,
                               offset=ts_complete.raw - ts_session.raw + 123)
        manifest_hdrs = self.app.headers[4]
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
                 '%s; mpu_etag=%s' % (exp_mpu_etag, exp_mpu_etag),
             'X-Object-Sysmeta-Container-Update-Override-Size': '0',
             'X-Object-Sysmeta-Mpu-Etag': exp_mpu_etag,
             'X-Object-Sysmeta-Mpu-Parts-Count': '2',
             'X-Object-Sysmeta-Mpu-Upload-Id': str(self.mpu_id),
             'X-Object-Sysmeta-Mpu-Max-Manifest-Part': '2',
             'X-Timestamp': exp_put_ts.internal},
            manifest_hdrs)

        session_hdrs = self.app.headers[5]
        self.assertEqual(
            {'Content-Type': 'application/x-mpu-session-completed',
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Timestamp': mock.ANY},
            session_hdrs)

    def test_complete_mpu_with_gaps_in_parts(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        ts_complete = next(self.ts_iter)
        put_slo_resp_body = {'Response Status': '201 Created',
                             'Etag': 'slo-etag'}
        registered_calls = [
            ('POST',
             '/v1/a/\x00mpu_sessions\x00c/%s' % self.sess_name,
             HTTPOk,
             {}),
            # SLO heartbeat response is 202...
            ('PUT',
             '/v1/a/c/o?heartbeat=on&multipart-manifest=put',
             HTTPAccepted,
             {},
             json.dumps(put_slo_resp_body).encode('ascii')),
            ('POST',
             '/v1/a/\x00mpu_sessions\x00c/%s' % self.sess_name,
             HTTPAccepted,
             {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
                            headers=req_hdrs)
        req.method = 'POST'
        # referenced part numbers are not contiguous
        mpu_manifest = [
            {'part_number': 1, 'etag': 'a' * 32},
            {'part_number': 3, 'etag': 'b' * 32},
        ]
        mpu_etag_hasher = md5(usedforsecurity=False)
        for part in mpu_manifest:
            mpu_etag_hasher.update(binascii.a2b_hex(part['etag']))
        exp_mpu_etag = mpu_etag_hasher.hexdigest() + '-2'
        req.body = json.dumps(mpu_manifest)
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(202, resp.status_int)
        resp_dict = json.loads(resp_body)
        self.assertEqual(
            {"Response Status": "201 Created",
             "Etag": exp_mpu_etag},
            resp_dict)
        expected = [call[:2]
                    for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

        session_hdrs = self.app.headers[3]
        self.assertEqual(
            {'Content-Type': 'application/x-mpu-session-completing',
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Timestamp': ts_complete.internal},
            session_hdrs)

        actual_manifest_body = self.app.uploaded.get('/v1/a/c/o')[1]
        self.assertEqual(
            [{"path": "\x00mpu_parts\x00c/%s/000001" % self.sess_name,
              "etag": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
             {"path": "\x00mpu_parts\x00c/%s/000003" % self.sess_name,
              "etag": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}],
            json.loads(actual_manifest_body))
        exp_put_ts = Timestamp(ts_session,
                               offset=ts_complete.raw - ts_session.raw)
        manifest_hdrs = self.app.headers[4]
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
                 '%s; mpu_etag=%s' % (exp_mpu_etag, exp_mpu_etag),
             'X-Object-Sysmeta-Container-Update-Override-Size': '0',
             'X-Object-Sysmeta-Mpu-Etag': exp_mpu_etag,
             'X-Object-Sysmeta-Mpu-Parts-Count': '2',
             'X-Object-Sysmeta-Mpu-Upload-Id': str(self.mpu_id),
             'X-Object-Sysmeta-Mpu-Max-Manifest-Part': '3',
             'X-Timestamp': exp_put_ts.internal},
            manifest_hdrs)

        session_hdrs = self.app.headers[5]
        self.assertEqual(
            {'Content-Type': 'application/x-mpu-session-completed',
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Timestamp': mock.ANY},
            session_hdrs)

    def test_complete_mpu_fails_to_set_completing_state(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        ts_complete = next(self.ts_iter)
        registered_calls = [
            ('POST',
             '/v1/a/\x00mpu_sessions\x00c/\x00o/%s' % self.mpu_id,
             HTTPNotFound,
             {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
                            headers=req_hdrs)
        req.method = 'POST'
        mpu_manifest = [
            {'part_number': 1, 'etag': 'a' * 32},
            {'part_number': 2, 'etag': 'b' * 32},
        ]
        mpu_etag_hasher = md5(usedforsecurity=False)
        for part in mpu_manifest:
            mpu_etag_hasher.update(binascii.a2b_hex(part['etag']))
        req.body = json.dumps(mpu_manifest)
        resp = req.get_response(self.mw)
        self.assertEqual(503, resp.status_int)
        exp_body = b''.join(HTTPServiceUnavailable()({}, lambda *args: None))
        self.assertEqual(exp_body, resp.body)
        expected = [call[:2]
                    for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_complete_mpu_bad_manifest(self):

        def do_complete(manifest_body):
            self.app.clear_calls()
            ts_complete = next(self.ts_iter)
            req_hdrs = {'X-Timestamp': ts_complete.internal,
                        'Content-Type': 'ignored'}
            req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
                                headers=req_hdrs)
            req.method = 'POST'
            req.body = manifest_body
            resp = req.get_response(self.mw)
            expected = [call[:2] for call in self.exp_calls]
            self.assertEqual(expected, self.app.calls)
            self.assertEqual(400, resp.status_int)
            return resp

        resp = do_complete(b"[]")
        self.assertEqual(b'Manifest must have at least one part.\n',
                         resp.body)

        resp = do_complete(json.dumps(
            [{'part_number': i + 1, 'etag': MD5_OF_EMPTY_STRING}
             for i in range(10001)]))
        self.assertEqual(b'Manifest must have at most 10000 parts.\n',
                         resp.body)

        resp = do_complete(b"[{123: 'foo'}]")
        self.assertEqual(b'Manifest must be valid JSON.\n',
                         resp.body)

        resp = do_complete(json.dumps({'part_number': 2}))
        self.assertEqual(b'Manifest must be a list.\n',
                         resp.body)

        resp = do_complete(json.dumps([[]]))
        self.assertEqual(b'Index 0: not a JSON object.\n',
                         resp.body)

        resp = do_complete(json.dumps([{'part_number': 2}]))
        self.assertEqual(b'Index 0: expected keys to include etag.\n',
                         resp.body)

        resp = do_complete(json.dumps([{'etag': 'a' * 32}]))
        self.assertEqual(b'Index 0: expected keys to include part_number.\n',
                         resp.body)

        resp = do_complete(json.dumps([{'etag': 'a' * 32},
                                       {'part_number': 2}]))
        self.assertEqual(b'Index 0: expected keys to include part_number.\n'
                         b'Index 1: expected keys to include etag.\n',
                         resp.body)

    def test_complete_mpu_manifest_put_not_success(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        ts_complete = next(self.ts_iter)
        registered_calls = [
            ('POST', '/v1/a/\x00mpu_sessions\x00c/%s' % self.sess_name,
             HTTPOk,
             {}),
            ('PUT', '/v1/a/c/o?heartbeat=on&multipart-manifest=put',
             HTTPServiceUnavailable, {}, None),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
                            headers=req_hdrs)
        req.method = 'POST'
        req.body = json.dumps([
            {'part_number': 1, 'etag': 'a' * 32},
            {'part_number': 2, 'etag': 'b' * 32},
        ])
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(202, resp.status_int)
        resp_dict = json.loads(resp_body)
        self.assertEqual(
            {'Response Status': '503 Service Unavailable',
             'Response Body': b''.join(HTTPServiceUnavailable()(
                 {}, lambda *args: None)).decode('utf8')},
            resp_dict)
        expected = [call[:2]
                    for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_complete_mpu_manifest_put_returns_problem_segments(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        ts_complete = next(self.ts_iter)
        put_slo_resp_body = {
            'Response Status': '400 Bad Request',
            'Response Body': 'Bad Request\nThe server could not comply with '
                             'the request since it is either malformed or '
                             'otherwise incorrect.',
            'Errors': [
                ["\x00mpu_parts\x00c/\x00o/test-id/1", '404 Not Found'],
                ["\x00mpu_parts\x00c/\x00o/test-id/2", 'Etag Mismatch'],
            ]
        }
        registered_calls = [
            ('POST', '/v1/a/\x00mpu_sessions\x00c/%s' % self.sess_name,
             HTTPOk, {}),
            ('PUT', '/v1/a/c/o?heartbeat=on&multipart-manifest=put',
             HTTPAccepted, {}, json.dumps(put_slo_resp_body).encode('ascii')),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
                            headers=req_hdrs)
        req.method = 'POST'
        req.body = json.dumps([
            {'part_number': 1, 'etag': 'a' * 32},
            {'part_number': 2, 'etag': 'b' * 32},
        ])
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(202, resp.status_int)
        resp_dict = json.loads(resp_body)
        self.assertEqual({
            'Response Status': '400 Bad Request',
            'Errors': [['1', '404 Not Found'], ['2', 'Etag Mismatch']]
        }, resp_dict)
        expected = [call[:2]
                    for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_complete_mpu_manifest_put_returns_bad_json(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        ts_complete = next(self.ts_iter)
        registered_calls = [
            ('POST', '/v1/a/\x00mpu_sessions\x00c/%s' % self.sess_name,
             HTTPOk,
             {}),
            ('PUT', '/v1/a/c/o?heartbeat=on&multipart-manifest=put',
             HTTPAccepted, {}, '{123: "NOT JSON"}'),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
                            headers=req_hdrs)
        req.method = 'POST'
        req.body = json.dumps([
            {'part_number': 1, 'etag': 'a' * 32},
            {'part_number': 2, 'etag': 'b' * 32},
        ])
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(202, resp.status_int)
        resp_dict = json.loads(resp_body)
        self.assertEqual({
            'Response Status': '503 Service Unavailable',
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
            ('HEAD', '/v1/a/c/%s' % self.mpu_name, HTTPNotFound, {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        ts_complete = next(self.ts_iter)
        req_hdrs = {'X-Timestamp': ts_complete.internal}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
                            headers=req_hdrs)
        req.method = 'POST'
        req.body = json.dumps([
            {'part_number': 1, 'etag': 'a' * 32},
            {'part_number': 2, 'etag': 'b' * 32},
        ])
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
        manifest = [
            {'part_number': 1, 'etag': 'a' * 32},
            {'part_number': 2, 'etag': 'b' * 32},
        ]
        hasher = MPUEtagHasher()
        for item in manifest:
            hasher.update(item['etag'])
        exp_mpu_etag = hasher.etag
        # the mpu was previously completed...
        head_resp_hdrs = {
            'X-Object-Sysmeta-Mpu-Upload-Id': self.mpu_id,
            'X-Object-Sysmeta-Mpu-Etag': exp_mpu_etag,
            'X-Object-Sysmeta-Mpu-Parts-Count': '99',
            'X-Object-Sysmeta-Mpu-Max-Manifest-Part': '123',
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
        }
        registered_calls = [
            ('HEAD', '/v1/a/c/%s' % self.mpu_name, HTTPOk, head_resp_hdrs),
        ]
        for call in registered_calls:
            self.app.register(*call)
        ts_complete = next(self.ts_iter)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
                            headers=req_hdrs)
        req.method = 'POST'
        req.body = json.dumps(manifest)
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
            ('HEAD', '/v1/a/\x00mpu_sessions\x00c/%s' % self.sess_name,
             HTTPNotFound, {}),
            ('HEAD', '/v1/a/c/o', HTTPNotFound, {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
                            headers=req_hdrs)
        req.method = 'POST'
        req.body = json.dumps([
            {'part_number': 1, 'etag': 'a' * 32},
            {'part_number': 2, 'etag': 'b' * 32},
        ])
        resp = req.get_response(self.mw)
        self.assertEqual(404, resp.status_int)
        self.assertEqual(b'No such upload-id', resp.body)
        expected = [call[:2] for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_complete_mpu_session_not_found_user_object_not_same_etag(self):
        ts_complete = next(self.ts_iter)
        manifest = [
            {'part_number': 1, 'etag': 'a' * 32},
            {'part_number': 2, 'etag': 'b' * 32},
        ]
        mpu_etag_hasher = MPUEtagHasher()
        for item in manifest:
            mpu_etag_hasher.update(item['etag'])
        user_obj_head_resp_headers = {
            'Content-Type': 'application/test',
            'X-Static-Large-Object': 'True',
            'X-Manifest-Etag': 'slo-manifest-etag',
            'Etag': '"slo-etag"',
            'X-Object-Sysmeta-Mpu-Etag': 'not-the-same-mpu-etag',
            'X-Object-Sysmeta-Mpu-Upload-Id': self.mpu_id,
        }

        registered_calls = [
            ('HEAD', '/v1/a/\x00mpu_sessions\x00c/%s' % self.sess_name,
             HTTPNotFound, {}),
            ('HEAD', '/v1/a/c/o', HTTPOk, user_obj_head_resp_headers),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
                            headers=req_hdrs)
        req.method = 'POST'
        req.body = json.dumps(manifest)
        resp = req.get_response(self.mw)
        self.assertEqual(404, resp.status_int)
        self.assertEqual(b'No such upload-id', resp.body)
        expected = [call[:2] for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_complete_mpu_session_not_found_user_object_not_same_id(self):
        ts_complete = next(self.ts_iter)
        manifest = [
            {'part_number': 1, 'etag': 'a' * 32},
            {'part_number': 2, 'etag': 'b' * 32},
        ]
        mpu_etag_hasher = MPUEtagHasher()
        for item in manifest:
            mpu_etag_hasher.update(item['etag'])
        user_obj_head_resp_headers = {
            'Content-Type': 'application/test',
            'X-Static-Large-Object': 'True',
            'X-Manifest-Etag': 'slo-manifest-etag',
            'Etag': '"slo-etag"',
            'X-Object-Sysmeta-Mpu-Etag': mpu_etag_hasher.etag,
            'X-Object-Sysmeta-Mpu-Upload-Id': 'not-the-same-upload-id',
        }

        registered_calls = [
            ('HEAD', '/v1/a/\x00mpu_sessions\x00c/%s' % self.sess_name,
             HTTPNotFound, {}),
            ('HEAD', '/v1/a/c/o', HTTPOk, user_obj_head_resp_headers),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
                            headers=req_hdrs)
        req.method = 'POST'
        req.body = json.dumps(manifest)
        resp = req.get_response(self.mw)
        self.assertEqual(404, resp.status_int)
        self.assertEqual(b'No such upload-id', resp.body)
        expected = [call[:2] for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_complete_mpu_session_not_found_user_object_linked(self):
        ts_complete = next(self.ts_iter)
        manifest = [
            {'part_number': 1, 'etag': 'a' * 32},
            {'part_number': 2, 'etag': 'b' * 32},
        ]
        mpu_etag_hasher = MPUEtagHasher()
        for item in manifest:
            mpu_etag_hasher.update(item['etag'])
        user_obj_head_resp_headers = {
            'Content-Type': 'application/test',
            'X-Static-Large-Object': 'True',
            'X-Manifest-Etag': 'slo-manifest-etag',
            'Etag': '"slo-etag"',
            'X-Object-Sysmeta-Mpu-Etag': mpu_etag_hasher.etag,
            'X-Object-Sysmeta-Mpu-Upload-Id': self.mpu_id,
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
        }

        registered_calls = [
            ('HEAD', '/v1/a/\x00mpu_sessions\x00c/%s' % self.sess_name,
             HTTPNotFound, {}),
            ('HEAD', '/v1/a/c/o', HTTPOk, user_obj_head_resp_headers),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
                            headers=req_hdrs)
        req.method = 'POST'
        req.body = json.dumps(manifest)
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(202, resp.status_int)
        resp_dict = json.loads(resp_body)
        self.assertEqual(
            {'Response Status': '201 Created',
             'Etag': mpu_etag_hasher.etag,
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
             '/v1/a/\x00mpu_sessions\x00c/%s' % self.sess_name,
             HTTPOk,
             {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_abort.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
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
             {'X-Object-Sysmeta-Mpu-Upload-Id': self.mpu_id}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_abort.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
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
             '/v1/a/\x00mpu_sessions\x00c/\x00o/%s' % self.mpu_id,
             HTTPNotFound,
             {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_abort.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
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
             '/v1/a/\x00mpu_sessions\x00c/\x00o/%s' % self.mpu_id,
             HTTPOk,
             {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_abort.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
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
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
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
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
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
            ('HEAD', '/v1/a/\x00mpu_sessions\x00c/%s' % self.sess_name,
             HTTPNotFound, {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_abort.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
                            headers=req_hdrs)
        req.method = 'DELETE'
        resp = req.get_response(self.mw)
        self.assertEqual(b'', resp.body)
        self.assertEqual(204, resp.status_int)
        expected = [call[:2] for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_mpu_async_cleanup_DELETE(self):
        backend_headers = {'x-object-sysmeta-mpu-upload-id': self.mpu_id}
        backend_responses = ResponseCollection([
            ResponseData(204, headers=backend_headers)
        ])
        env_updates = {'swift.backend_responses': backend_responses}
        registered_calls = [
            ('DELETE', '/v1/a/c/o', swob.HTTPNoContent, {}, None,
             env_updates),
            ('PUT',
             '/v1/a/\x00mpu_parts\x00c/%s/marker-deleted'
             % self.sess_name,
             HTTPAccepted,
             backend_headers)
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank('/v1/a/c/o')
        req.method = 'DELETE'
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(204, resp.status_int)
        self.assertEqual(b'', resp_body)
        exp_calls = [call[:2] for call in registered_calls]
        self.assertEqual(exp_calls, self.app.calls)

    def test_mpu_async_cleanup_DELETE_versioning_enabled(self):
        # if *any* of the backend responses indicates that a version was
        # created then none of the uploads are cleaned up :(
        # TODO: cleanup uploads that were not preserved as a version
        mpu_id_alt = MPUId.create(self.mpu_path, next(self.ts_iter))
        backend_headers = {
            'x-object-sysmeta-mpu-upload-id': self.mpu_id,
            'x-object-version-id': 'my-version-id',
        }
        alt_backend_headers = {
            'x-object-sysmeta-mpu-upload-id': mpu_id_alt,
        }

        backend_responses = ResponseCollection([
            ResponseData(201, headers=backend_headers),
            ResponseData(201, headers=alt_backend_headers),
        ])
        env_updates = {'swift.backend_responses': backend_responses}
        self.app.register('DELETE', '/v1/a/c/o', swob.HTTPNoContent,
                          backend_headers, None, env_updates=env_updates)
        req = Request.blank('/v1/a/c/o')
        req.method = 'DELETE'
        resp = req.get_response(self.mw)
        self.assertEqual(b'', b''.join(resp.app_iter))
        self.assertEqual(204, resp.status_int)
        exp_calls = [('DELETE', '/v1/a/c/o')]
        self.assertEqual(exp_calls, self.app.calls)

    def test_mpu_async_cleanup_DELETE_specific_version(self):
        # mpu IS cleaned up after deleting a specific version
        backend_headers = {
            'x-object-sysmeta-mpu-upload-id': self.mpu_id,
            'x-object-version-id': 'my-version-id',
            'x-object-current-version-id': 'null',
        }

        backend_responses = ResponseCollection([
            ResponseData(201, headers=backend_headers),
        ])
        env_updates = {'swift.backend_responses': backend_responses}
        registered_calls = [
            ('DELETE', '/v1/a/c/o',
             swob.HTTPNoContent, backend_headers, None, env_updates),
            ('PUT',
             '/v1/a/\x00mpu_parts\x00c/\x00o/%s/marker-deleted'
             % self.mpu_id, HTTPAccepted, {})
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank('/v1/a/c/o')
        req.method = 'DELETE'
        resp = req.get_response(self.mw)
        self.assertEqual(b'', b''.join(resp.app_iter))
        self.assertEqual(204, resp.status_int)
        exp_calls = [call[:2] for call in registered_calls]
        self.assertEqual(exp_calls, self.app.calls)

    def test_mpu_async_cleanup_PUT(self):
        backend_headers = {'x-object-sysmeta-mpu-upload-id': self.mpu_id}
        backend_responses = ResponseCollection([
            ResponseData(201, headers=backend_headers)
        ])
        env_updates = {'swift.backend_responses': backend_responses}
        self.app.register('PUT', '/v1/a/c/o', swob.HTTPCreated,
                          backend_headers, None, env_updates=env_updates)
        self.app.register(
            'PUT',
            '/v1/a/\x00mpu_parts\x00c/%s/marker-deleted'
            % self.sess_name,
            HTTPAccepted,
            {})
        req = Request.blank('/v1/a/c/o')
        req.method = 'PUT'
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(201, resp.status_int)
        self.assertEqual(b'', resp_body)
        exp_calls = [
            ('PUT', '/v1/a/c/o'),
            ('PUT',
             '/v1/a/\x00mpu_parts\x00c/%s/marker-deleted'
             % self.sess_name),
        ]
        self.assertEqual(exp_calls, self.app.calls)

    def test_mpu_async_cleanup_PUT_mixed_backend_responses(self):
        # verify bad resp headers are ignored and multiple mpu's are cleaned up
        mpu_id_alt = self._make_mpu_id('/v1/a/c/o')
        self.assertNotEqual(self.mpu_id, mpu_id_alt)  # sanity check
        backend_responses = ResponseCollection([
            ResponseData(
                201, headers={}),  # no upload id
            ResponseData(
                201, headers={'x-object-sysmeta-mpu-upload-id': 'bad id'}),
            ResponseData(
                201, headers={'x-object-sysmeta-mpu-upload-id': mpu_id_alt}),
            ResponseData(
                201, headers={'x-object-sysmeta-mpu-upload-id': self.mpu_id}),
        ])
        env_updates = {'swift.backend_responses': backend_responses}
        self.app.register('PUT', '/v1/a/c/o', swob.HTTPCreated, {}, None,
                          env_updates=env_updates)
        self.app.register(
            'PUT',
            '/v1/a/\x00mpu_parts\x00c/%s/marker-deleted'
            % self.sess_name,
            HTTPAccepted,
            {})
        self.app.register(
            'PUT',
            '/v1/a/\x00mpu_parts\x00c/\x00o/%s/marker-deleted'
            % mpu_id_alt,
            HTTPAccepted,
            {})
        req = Request.blank('/v1/a/c/o')
        req.method = 'PUT'
        resp = req.get_response(self.mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(201, resp.status_int)
        self.assertEqual(b'', resp_body)
        self.assertEqual(3, len(self.app.calls))
        self.assertEqual(('PUT', '/v1/a/c/o'), self.app.calls[0])
        exp_calls = sorted([
            ('PUT',
             '/v1/a/\x00mpu_parts\x00c/%s/marker-deleted'
             % self.sess_name),
            ('PUT',
             '/v1/a/\x00mpu_parts\x00c/\x00o/%s/marker-deleted'
             % mpu_id_alt),
        ])
        self.assertEqual(exp_calls, sorted(self.app.calls[1:]))

    def test_container_listing(self):
        listing = [
            # MPU
            {'name': 'a-mpu',
             'bytes': 10485760,  # SLO fixes this up from swift_bytes
             'hash': '%s; '
                     'other_mw_etag=banana; '
                     'mpu_etag=my-mpu-etag '
                     % MD5_OF_EMPTY_STRING,
             'content_type': 'application/test',
             'last_modified': '2024-09-10T14:16:00.579190',
             'slo_etag': 'my-slo-etag'
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
             'bytes': 123,
             'content_type': "text/plain",
             'last_modified': '1970-01-01T00:00:01.000000'},
        ]
        resp_body = json.dumps(listing).encode('ascii')
        parts_container = '\x00mpu_parts\x00cont'
        get_resp_hdrs = {
            'X-Container-Sysmeta-Mpu-Parts-Container-0':
                wsgi_quote(parts_container),
            'X-Container-Object-Count': '4',
            'X-Container-Bytes-Used': '123',
        }
        head_resp_hdrs = {
            'X-Container-Object-Count': '2',
            'X-Container-Bytes-Used': '12341234',
        }
        registered = [
            ('GET', '/v1/a/cont', swob.HTTPCreated,
             get_resp_hdrs, resp_body),
            ('HEAD', '/v1/a/\x00mpu_parts\x00cont', swob.HTTPNoContent,
             head_resp_hdrs, resp_body),
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
            'X-Container-Bytes-Used': '12341357',
            'X-Container-Mpu-Parts-Bytes-Used': '12341234',
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
             'bytes': 123,
             'content_type': "text/plain",
             'last_modified': '1970-01-01T00:00:01.000000'},
        ]
        resp_body = json.dumps(listing).encode('ascii')
        get_resp_hdrs = {
            'X-Container-Object-Count': '1',
            'X-Container-Bytes-Used': '123',
        }
        registered = [
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
            'X-Object-Sysmeta-Mpu-Upload-Id': str(self.mpu_id),
        }
        post_resp_body = b''.join(HTTPAccepted()({}, lambda *args: None))
        registered_calls = [
            ('POST', self.mpu_path,
             swob.HTTPAccepted,
             post_resp_headers,
             post_resp_body),
        ]
        for call in registered_calls:
            self.app.register(*call)
        post_req_headers = {
            'X-Object-Meta-Foo': 'Bar',
            'Content-Type': 'application/test2',
            'X-Timestamp': '1727184152.29655',
        }
        req = Request.blank(self.mpu_path, headers=post_req_headers)
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
            'X-Object-Sysmeta-Mpu-Upload-Id': str(self.mpu_id),
        }
        self.assertEqual(exp_resp_headers, resp.headers)
        self.assertEqual({'Content-Type': 'application/test2',
                          'Host': 'localhost:80',
                          'X-Object-Meta-Foo': 'Bar',
                          'X-Timestamp': '1727184152.29655'},
                         self.app.headers[0])

    def _do_test_get_head_mpu(self, method):
        get_resp_headers = {
            'Content-Type': 'application/test',
            'X-Static-Large-Object': 'True',
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
            'X-Timestamp': '1727184150.29655',
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
            'X-Object-Sysmeta-Mpu-Upload-Id': str(self.mpu_id),
        }
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, get_resp_headers,
                          b'test')
        req = Request.blank(self.mpu_path)
        req.method = method
        resp = req.get_response(self.mw)
        self.assertEqual(200, resp.status_int)
        self.assertEqual(1, len(self.app.calls))
        self.assertEqual((method, self.mpu_path), self.app.calls[0])
        exp_resp_headers = {
            'Content-Type': 'application/test',
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
            'X-Timestamp': '1727184150.29655',
            'Accept-Ranges': 'bytes',
            'Content-Length': '4',
            'X-Trans-Id': 'test-txn-id',
            'X-Openstack-Request-Id': 'test-txn-id',
            'Date': 'Tue, 24 Sep 2024 13:22:30 GMT',
            'Etag': '"mpu-etag"',
            'X-Parts-Count': '2',
            'X-Upload-Id': str(self.mpu_id),
        }
        self.assertEqual(exp_resp_headers, resp.headers)
        self.assertEqual({'Host': 'localhost:80',
                          'X-Backend-Etag-Is-At': 'x-object-sysmeta-mpu-etag'},
                         self.app.call_list[0].headers)
        return resp

    def test_get_mpu(self):
        resp = self._do_test_get_head_mpu('GET')
        self.assertEqual(b'test', resp.body)

    def test_head_mpu(self):
        resp = self._do_test_get_head_mpu('HEAD')
        self.assertEqual(b'', resp.body)

    def _do_test_get_head_not_mpu(self, method):
        get_resp_headers = {
            'Content-Type': 'application/test',
            'X-Static-Large-Object': 'True',
            'Last-Modified': 'Tue, 24 Sep 2024 13:22:31 GMT',
            'X-Timestamp': '1727184150.29655',
            'Accept-Ranges': 'bytes',
            'Content-Length': '4',
            'X-Manifest-Etag': 'b871773cf02434d498517245c7b88c11',
            'X-Trans-Id': 'test-txn-id',
            'X-Openstack-Request-Id': 'test-txn-id',
            'Date': 'Tue, 24 Sep 2024 13:22:30 GMT',
            'Etag': '"de64a5af184e6f732a26328c13d4ab25"',
        }
        self.app.register('GET', '/v1/a/c/o', swob.HTTPOk, get_resp_headers,
                          b'test')
        req = Request.blank('/v1/a/c/o')
        req.method = method
        resp = req.get_response(self.mw)
        self.assertEqual(200, resp.status_int)
        self.assertEqual(1, len(self.app.calls))
        self.assertEqual((method, '/v1/a/c/o'), self.app.calls[0])
        self.assertEqual(get_resp_headers, resp.headers)
        return resp

    def test_get_not_mpu(self):
        resp = self._do_test_get_head_not_mpu('GET')
        self.assertEqual(b'test', resp.body)

    def test_head_not_mpu(self):
        resp = self._do_test_get_head_mpu('HEAD')
        self.assertEqual(b'', resp.body)


class TestMpuMiddlewareErrors(BaseTestMPUMiddleware):
    def setUp(self):
        super(TestMpuMiddlewareErrors, self).setUp()
        self.session_requests = [
            # upload part
            Request.blank('/v1/a/c/o?upload-id=%s&part-number=1' % self.mpu_id,
                          environ={'REQUEST_METHOD': 'PUT'}),
            # list parts
            Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
                          environ={'REQUEST_METHOD': 'GET'}),
            # complete upload
            Request.blank(
                '/v1/a/c/o?upload-id=%s' % self.mpu_id,
                environ={'REQUEST_METHOD': 'POST'},
                body=json.dumps(
                    [{'part_number': 1, 'etag': MD5_OF_EMPTY_STRING}]
                ).encode('ascii')),
            # abort upload
            Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
                          environ={'REQUEST_METHOD': 'DELETE'}),
        ]
        self.requests = self.session_requests + [
            # list uploads
            Request.blank('/v1/a/c?uploads=true',
                          environ={'REQUEST_METHOD': 'GET'}),
            # create upload
            Request.blank('/v1/a/c/o?uploads=true',
                          environ={'REQUEST_METHOD': 'POST'}),
        ]

    def test_api_requests_invalid_upload_id(self):
        self.app.register('HEAD', '/v1/a/c', HTTPNotFound, {})
        # sanity check - mpu id is valid
        req = Request.blank(
            '%s?upload-id=%s&part-number=1' % (self.mpu_path, self.mpu_id),
            environ={'REQUEST_METHOD': 'PUT'})
        resp = req.get_response(MPUMiddleware(self.app, {}))
        self.assertEqual(404, resp.status_int)

        def test_bad_mpu_id(bad_id):
            req = Request.blank(
                '%s?upload-id=%s&part-number=1' % (self.mpu_path, bad_id),
                environ={'REQUEST_METHOD': 'PUT'})
            resp = req.get_response(MPUMiddleware(self.app, {}))
            self.assertEqual(400, resp.status_int, bad_id)
            self.assertEqual(b'Invalid upload-id', resp.body)

        test_bad_mpu_id('')
        test_bad_mpu_id(str(self.mpu_id).split('~')[0])
        test_bad_mpu_id(str(self.mpu_id)[:-1])
        test_bad_mpu_id(str(self.mpu_id) + '~foo')
        test_bad_mpu_id(reversed(str(self.mpu_id)))
        # mpu id belongs to different path
        test_bad_mpu_id(str(self._make_mpu_id('/v1/a/c/o2')))

    def test_api_requests_user_container_not_found(self):
        self.app.register('HEAD', '/v1/a/c', HTTPNotFound, {})
        for req in self.requests:
            self.app.clear_calls()
            resp = req.get_response(self.mw)
            self.assertEqual(404, resp.status_int)
            self.assertEqual([call[:2] for call in self.exp_calls],
                             self.app.calls)

    def test_api_requests_user_container_unavailable(self):
        self.app.register('HEAD', '/v1/a/c', HTTPServiceUnavailable, {})
        for req in self.requests:
            self.app.clear_calls()
            resp = req.get_response(self.mw)
            self.assertEqual(503, resp.status_int)
            self.assertEqual([call[:2] for call in self.exp_calls],
                             self.app.calls)

    def test_api_requests_user_unexpected_error(self):
        self.app.register('HEAD', '/v1/a/c', HTTPPreconditionFailed, {})
        for req in self.requests:
            self.app.clear_calls()
            resp = req.get_response(self.mw)
            self.assertEqual(503, resp.status_int)
            self.assertEqual([call[:2] for call in self.exp_calls],
                             self.app.calls)

    def test_session_requests_with_earlier_timestamp(self):
        ts_older = next(self.ts_iter)
        ts_session = next(self.ts_iter)
        ts_newer = next(self.ts_iter)
        ts_meta = next(self.ts_iter)

        self._setup_mpu_existence_check_call(ts_session, ts_meta=ts_meta)
        for req in self.session_requests:
            req.headers['X-Timestamp'] = ts_older.internal
            resp = req.get_response(self.mw)
            self.assertEqual(
                409, resp.status_int,
                '%s %s %s %s' % (req.method, req.path, req.params, resp.body))

        for req in self.session_requests:
            req.headers['X-Timestamp'] = ts_newer.internal
            resp = req.get_response(self.mw)
            self.assertEqual(
                409, resp.status_int,
                '%s %s %s %s' % (req.method, req.path, req.params, resp.body))

        for req in self.session_requests:
            req.headers['X-Timestamp'] = ts_meta.internal
            resp = req.get_response(self.mw)
            self.assertEqual(
                409, resp.status_int,
                '%s %s %s %s' % (req.method, req.path, req.params, resp.body))
