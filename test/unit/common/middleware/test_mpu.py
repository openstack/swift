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
import mock

from swift.common import swob
from swift.common.middleware.mpu import MPUMiddleware, MPUId, get_upload_id
from swift.common.swob import Request, HTTPOk, HTTPNotFound, HTTPCreated, \
    HTTPAccepted, HTTPNoContent, HTTPServiceUnavailable, HTTPPreconditionFailed
from swift.common.utils import md5, quote, Timestamp
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
        req = Request.blank('/v1/a/c')
        self.assertIsNone(get_upload_id(req))

        ts = Timestamp.now()
        mpu_id = MPUId.create(ts)
        req = Request.blank('/v1/a/c', params={'upload-id': str(mpu_id)})
        self.assertEqual(mpu_id, get_upload_id(req))

        def do_test_bad_value(value):
            with self.assertRaises(ValueError):
                req = Request.blank('/v1/a/c', params={'upload-id': value})
                get_upload_id(req)

        do_test_bad_value('')
        do_test_bad_value(None)
        do_test_bad_value('my-uuid')
        do_test_bad_value('my-uuid_')
        do_test_bad_value('_%s' % ts.internal)
        do_test_bad_value('my-uuid_xyz')


class TestMPUId(unittest.TestCase):
    def test_create(self):
        timestamp = Timestamp.now()
        with mock.patch('swift.common.middleware.mpu.generate_unique_id',
                        return_value='my-uuid'):
            mpu_id = MPUId.create(timestamp)
        self.assertEqual('my-uuid', mpu_id.uuid)
        self.assertEqual(timestamp, mpu_id.timestamp)
        self.assertEqual('%s_my-uuid' % timestamp.internal, str(mpu_id))

    def test_unique(self):
        timestamp = Timestamp.now()
        mpu_id1 = MPUId.create(timestamp)
        mpu_id2 = MPUId.create(timestamp)
        self.assertEqual(mpu_id1.timestamp, mpu_id2.timestamp)
        self.assertNotEqual(mpu_id1, mpu_id2)
        self.assertNotEqual(mpu_id1.uuid, mpu_id2.uuid)

    def test_parse(self):
        timestamp = Timestamp.now()
        mpu_id = MPUId.parse('%s_my-uuid' % timestamp.internal)
        self.assertEqual('my-uuid', mpu_id.uuid)
        self.assertEqual(timestamp, mpu_id.timestamp)
        self.assertEqual('%s_my-uuid' % timestamp.internal, str(mpu_id))

    def test_create_parse(self):
        timestamp = Timestamp.now()
        mpu_id1 = MPUId.create(timestamp)
        mpu_id2 = MPUId.parse(str(mpu_id1))
        self.assertEqual(timestamp, mpu_id2.timestamp)
        self.assertEqual(mpu_id1.uuid, mpu_id2.uuid)
        self.assertEqual(str(mpu_id1), str(mpu_id2))

    def test_eq(self):
        timestamp = Timestamp.now()
        mpu_id1a = MPUId.create(timestamp)
        mpu_id1b = MPUId.parse(str(mpu_id1a))
        self.assertEqual(mpu_id1a, mpu_id1b)
        self.assertEqual(mpu_id1a, str(mpu_id1b))

        mpu_id2 = MPUId.create(timestamp)
        self.assertNotEqual(mpu_id1a, mpu_id2)


class BaseTestMPUMiddleware(unittest.TestCase):
    # TODO: assert 'X-Backend-Allow-Reserved-Names' in backend requests
    def setUp(self):
        self.app = FakeSwift()
        self.ts_iter = make_timestamp_iter()
        self.id_iter = iter(MPUId.create(t) for t in make_timestamp_iter())
        self.mpu_id = next(self.id_iter)
        self.debug_logger = debug_logger()
        self.exp_calls = []
        self._setup_user_ac_info_requests()
        self.mw = MPUMiddleware(self.app, {}, logger=self.debug_logger)

    def _setup_user_ac_info_requests(self):
        ac_info_calls = [('HEAD', '/v1/a', HTTPOk, {}),
                         ('HEAD', '/v1/a/c', HTTPOk, {})]
        for call in ac_info_calls:
            self.app.register(*call)
        self.exp_calls.extend(ac_info_calls)


class TestMPUMiddleware(BaseTestMPUMiddleware):
    def setUp(self):
        super(TestMPUMiddleware, self).setUp()

    def _setup_mpu_create_requests(self):
        self.app.register(
            'HEAD', '/v1/a/\x00mpu_sessions\x00c', HTTPNotFound, {})
        self.app.register(
            'PUT', '/v1/a/\x00mpu_sessions\x00c', HTTPCreated, {})
        self.app.register(
            'HEAD', '/v1/a/\x00mpu_manifests\x00c', HTTPNotFound, {})
        self.app.register(
            'PUT', '/v1/a/\x00mpu_manifests\x00c', HTTPCreated, {})
        self.app.register(
            'HEAD', '/v1/a/\x00mpu_parts\x00c', HTTPNotFound, {})
        self.app.register(
            'PUT', '/v1/a/\x00mpu_parts\x00c', HTTPCreated, {})
        expected = [
            ('HEAD', '/v1/a'),
            ('HEAD', '/v1/a/c'),
            ('HEAD', '/v1/a/\x00mpu_sessions\x00c'),
            ('PUT', '/v1/a/\x00mpu_sessions\x00c'),
            ('HEAD', '/v1/a/\x00mpu_manifests\x00c'),
            ('PUT', '/v1/a/\x00mpu_manifests\x00c'),
            ('HEAD', '/v1/a/\x00mpu_parts\x00c'),
            ('PUT', '/v1/a/\x00mpu_parts\x00c'),
        ]
        return expected

    def _setup_mpu_existence_check_call(self, ts_session):
        ts_other = next(self.ts_iter)
        call = ('HEAD', '/v1/a/\x00mpu_sessions\x00c/\x00o/%s' % self.mpu_id,
                HTTPOk,
                {'X-Timestamp': ts_other.internal,
                 'Content-Type': 'application/x-mpu',
                 'X-Backend-Data-Timestamp': ts_session.internal,
                 'X-Object-Transient-Sysmeta-Mpu-State': 'created',
                 'X-Object-Sysmeta-Mpu-X-Object-Meta-Foo': 'blah',
                 'X-Object-Sysmeta-Mpu-Has-Content-Type': 'yes',
                 'X-Object-Sysmeta-Mpu-Content-Type': 'application/test',
                 })
        self.app.register(*call)
        self.exp_calls.append(call)

    def _do_test_create_mpu(self, req_headers):
        expected = self._setup_mpu_create_requests()
        self.app.register(
            'PUT', '/v1/a/\x00mpu_sessions\x00c/\x00o/%s' % self.mpu_id,
            HTTPCreated, {})
        expected.append(('PUT',
                         '/v1/a/\x00mpu_sessions\x00c/\x00o/%s' % self.mpu_id))
        req = Request.blank('/v1/a/c/o?uploads=true', headers=req_headers)
        req.method = 'POST'
        with mock.patch('swift.common.middleware.mpu.MPUId.create',
                        return_value=self.mpu_id):
            resp = req.get_response(self.mw)
        self.assertEqual(200, resp.status_int)
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
             'Content-Type': 'application/x-mpu',
             'X-Timestamp': ts_now.internal,
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Object-Transient-Sysmeta-Mpu-State': 'created',
             'X-Object-Sysmeta-Mpu-X-Object-Meta-Foo': 'blah',
             'X-Object-Sysmeta-Mpu-Has-Content-Type': 'no'},
            self.app.headers[-1])

    def test_create_mpu_with_x_timestamp(self):
        ts_now = Timestamp(1234567.123)
        req_headers = {'X-Object-Meta-Foo': 'blah',
                       'X-Timestamp': ts_now.internal}
        self._do_test_create_mpu(req_headers)
        self.assertEqual(
            {'Content-Length': '0',
             'Content-Type': 'application/x-mpu',
             'X-Timestamp': ts_now.internal,
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Object-Transient-Sysmeta-Mpu-State': 'created',
             'X-Object-Sysmeta-Mpu-X-Object-Meta-Foo': 'blah',
             'X-Object-Sysmeta-Mpu-Has-Content-Type': 'no'},
            self.app.headers[-1])

    def test_create_mpu_with_content_type(self):
        headers = {'X-Object-Meta-Foo': 'blah',
                   'content-Type': 'application/test'}
        self._do_test_create_mpu(headers)
        self.assertEqual(
            {'Content-Length': '0',
             'Content-Type': 'application/x-mpu',
             'X-Timestamp': mock.ANY,
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Object-Transient-Sysmeta-Mpu-State': 'created',
             'X-Object-Sysmeta-Mpu-X-Object-Meta-Foo': 'blah',
             'X-Object-Sysmeta-Mpu-Has-Content-Type': 'yes',
             'X-Object-Sysmeta-Mpu-Content-Type': 'application/test'},
            self.app.headers[-1])

    def test_list_mpus(self):
        listing = [
            # in progress
            {'name': '\x00obj1/%s' % next(self.id_iter),
             'hash': 'etag',
             'bytes': 0,
             'content_type': 'application/x-mpu',
             'last_modified': '1970-01-01T00:00:00.000000'},
            {'name': '\x00obj1/%s' % next(self.id_iter),
             'hash': 'etag',
             'bytes': 0,
             'content_type': 'application/x-mpu',
             'last_modified': '1970-01-01T00:00:00.000000'},
            {'name': '\x00obj2/%s' % next(self.id_iter),
             'hash': 'etag',
             'bytes': 0,
             'content_type': 'application/x-mpu',
             'last_modified': '1970-01-01T00:00:00.000000'},
            # aborted
            {'name': '\x00obj3/%s' % next(self.id_iter),
             'hash': 'etag',
             'bytes': 0,
             'content_type': 'application/x-mpu-aborted',
             'last_modified': '1970-01-01T00:00:00.000000'},
        ]
        exp_listing = [dict(item, name=item['name'][1:])
                       for item in listing[:3]]
        registered_calls = [('GET', '/v1/a/\x00mpu_sessions\x00c', HTTPOk, {},
                             json.dumps(listing).encode('ascii'))]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank('/v1/a/c?uploads')
        req.method = 'GET'
        resp = req.get_response(self.mw)
        self.assertEqual(200, resp.status_int)
        expected = [call[:2] for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)
        self.assertEqual(exp_listing, json.loads(resp.body))

    def test_upload_part(self):
        ts_session = next(self.ts_iter)
        ts_part = next(self.ts_iter)
        registered_calls = [
            ('HEAD',
             '/v1/a/\x00mpu_sessions\x00c/\x00o/%s' % self.mpu_id,
             HTTPOk,
             {'X-Timestamp': ts_session.internal,
              'Content-Type': 'application/x-mpu', }),
            ('PUT',
             '/v1/a/\x00mpu_parts\x00c/\x00o/%s/1' % self.mpu_id,
             HTTPCreated,
             {'X-Timestamp': ts_part.internal})]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank(
            '/v1/a/c/o?upload-id=%s&part-number=1' % self.mpu_id)
        req.method = 'PUT'
        req.body = b'testing'
        mw = MPUMiddleware(self.app, {})
        resp = req.get_response(mw)
        self.assertEqual(200, resp.status_int)
        self.assertEqual(md5(b'testing', usedforsecurity=False).hexdigest(),
                         resp.headers.get('Etag'))
        self.assertEqual('7', resp.headers['Content-Length'])
        expected = [call[:2] for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_list_parts(self):
        ts_session = next(self.ts_iter)
        listing = [{'name': '\x00o/%s/%d' % (self.mpu_id, i),
                    'hash': 'etag%d' % i,
                    'bytes': i,
                    'content_type': 'text/plain',
                    'last_modified': '1970-01-01T00:00:00.000000'}
                   for i in range(3)]
        registered_calls = [
            ('HEAD',
             '/v1/a/\x00mpu_sessions\x00c/\x00o/%s' % self.mpu_id,
             HTTPOk,
             {'X-Timestamp': ts_session.internal,
              'Content-Type': 'application/x-mpu'}),
            ('GET', '/v1/a/\x00mpu_parts\x00c',
             HTTPOk,
             {},
             json.dumps(listing).encode('ascii'))
        ]
        for call in registered_calls:
            self.app.register(*call)
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id)
        req.method = 'GET'
        mw = MPUMiddleware(self.app, {}, logger=self.debug_logger)
        resp = req.get_response(mw)
        self.assertEqual(200, resp.status_int)
        expected = [call[:2] for call in self.exp_calls] + [
            ('HEAD', '/v1/a/\x00mpu_sessions\x00c/\x00o/%s' % self.mpu_id),
            ('GET', '/v1/a/\x00mpu_parts\x00c?prefix=%s'
             % quote('\x00o/%s' % self.mpu_id, safe='')),
        ]
        self.assertEqual(expected, self.app.calls)
        exp_listing = [{'name': 'o/%s/%d' % (self.mpu_id, i),
                        'hash': 'etag%d' % i,
                        'bytes': i,
                        'content_type': 'text/plain',
                        'last_modified': '1970-01-01T00:00:00.000000'}
                       for i in range(3)]
        self.assertEqual(exp_listing, json.loads(resp.body))

    def test_complete_mpu(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        ts_complete = next(self.ts_iter)
        put_slo_resp_body = {'Response Status': '201 Created',
                             'Etag': 'slo-etag'}
        registered_calls = [
            ('POST',
             '/v1/a/\x00mpu_sessions\x00c/\x00o/%s' % self.mpu_id,
             HTTPOk,
             {}),
            # SLO heartbeat response is 202...
            ('PUT',
             '/v1/a/\x00mpu_manifests\x00c/\x00o/%s?'
             'heartbeat=on&multipart-manifest=put' % self.mpu_id,
             HTTPAccepted,
             {},
             json.dumps(put_slo_resp_body).encode('ascii')),
            ('PUT', '/v1/a/c/o', HTTPCreated, {}),
            ('DELETE',
             '/v1/a/\x00mpu_sessions\x00c/\x00o/%s' % self.mpu_id,
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
        mpu_manifest = [
            {'part_number': 1, 'etag': 'a' * 32},
            {'part_number': 2, 'etag': 'b' * 32},
        ]
        mpu_etag_hasher = md5(usedforsecurity=False)
        for part in mpu_manifest:
            mpu_etag_hasher.update(binascii.a2b_hex(part['etag']))
        exp_mpu_etag = mpu_etag_hasher.hexdigest() + '-2'
        req.body = json.dumps(mpu_manifest)
        mw = MPUMiddleware(self.app, {})
        resp = req.get_response(mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(200, resp.status_int)
        resp_dict = json.loads(resp_body)
        self.assertEqual(
            {"Response Status": "201 Created", "Etag": "slo-etag"}, resp_dict)
        expected = [call[:2]
                    for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

        session_hdrs = self.app.headers[3]
        self.assertEqual(
            {'Content-Type': 'application/x-mpu',
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Timestamp': ts_complete.internal,
             'X-Object-Transient-Sysmeta-Mpu-State': 'completing'},
            session_hdrs)

        actual_manifest_body = self.app.uploaded.get(
            '/v1/a/\x00mpu_manifests\x00c/\x00o/%s' % self.mpu_id)[1]
        self.assertEqual(
            [{"path": "\x00mpu_parts\x00c/\x00o/%s/1" % self.mpu_id,
              "etag": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
             {"path": "\x00mpu_parts\x00c/\x00o/%s/2" % self.mpu_id,
              "etag": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}],
            json.loads(actual_manifest_body))
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
             'X-Object-Sysmeta-Mpu-Etag': exp_mpu_etag,
             'X-Object-Sysmeta-Mpu-Parts-Count': '2',
             'X-Object-Sysmeta-Mpu-Upload-Id': str(self.mpu_id),
             'X-Timestamp': ts_session.internal},
            manifest_hdrs)

        actual_mpu_body = self.app.uploaded.get('/v1/a/c/o')[1]
        self.assertEqual(b'', actual_mpu_body)
        mpu_hdrs = self.app.headers[5]
        self.assertEqual(
            {'Content-Length': '0',
             'Content-Type': 'application/test',
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Object-Sysmeta-Allow-Reserved-Names': 'true',
             'X-Object-Sysmeta-Mpu-Upload-Id': str(self.mpu_id),
             'X-Symlink-Target':
                 '\x00mpu_manifests\x00c/\x00o/%s' % self.mpu_id,
             # note: FakeApp doesn't call-back to the MPU middleware slo
             # callback handler so mpu_bytes show as 0
             'X-Object-Sysmeta-Container-Update-Override-Etag':
                 '%s; mpu_etag=%s; mpu_bytes=0' % (exp_mpu_etag, exp_mpu_etag),
             'X-Timestamp': ts_session.internal},
            mpu_hdrs)

    def test_complete_mpu_symlink_put_fails(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        ts_complete = next(self.ts_iter)
        put_resp_body = {'Response Status': '201 Created',
                         'Etag': 'slo-etag'}
        registered_calls = [
            ('POST', '/v1/a/\x00mpu_sessions\x00c/\x00o/%s' % self.mpu_id,
             HTTPOk,
             {}),
            # SLO heartbeat response is 202...
            ('PUT', '/v1/a/\x00mpu_manifests\x00c/\x00o/%s?'
                    'heartbeat=on&multipart-manifest=put' % self.mpu_id,
             HTTPAccepted,
             {},
             json.dumps(put_resp_body).encode('ascii')),
            ('PUT', '/v1/a/c/o', HTTPNotFound, {}),
            # note: no DELETE
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
        mw = MPUMiddleware(self.app, {})
        resp = req.get_response(mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(200, resp.status_int)
        resp_dict = json.loads(resp_body)
        self.assertEqual({"Response Status": "404 Not Found"},
                         resp_dict)
        expected = [call[:2]
                    for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_complete_mpu_manifest_put_not_success(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        ts_complete = next(self.ts_iter)
        registered_calls = [
            ('POST', '/v1/a/\x00mpu_sessions\x00c/\x00o/%s' % self.mpu_id,
             HTTPOk,
             {}),
            # SLO heartbeat response is 202...
            ('PUT', '/v1/a/\x00mpu_manifests\x00c/\x00o/%s?'
                    'heartbeat=on&multipart-manifest=put' % self.mpu_id,
             HTTPServiceUnavailable, {},
             'Service Unavailable\nThe server is currently unavailable. '
             'Please try again at a later time.'),
            # note: no symlink PUT, no DELETE
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
        mw = MPUMiddleware(self.app, {})
        resp = req.get_response(mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(200, resp.status_int)
        resp_dict = json.loads(resp_body)
        self.assertEqual(
            {'Response Status': '503 Service Unavailable',
             'Response Body': 'Service Unavailable\nThe server is currently '
                              'unavailable. Please try again at a later time.'
             }, resp_dict)
        expected = [call[:2]
                    for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_complete_mpu_manifest_put_returns_problem_segments(self):
        ts_session = next(self.ts_iter)
        self._setup_mpu_existence_check_call(ts_session)
        ts_complete = next(self.ts_iter)
        put_resp_body = {
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
            ('POST', '/v1/a/\x00mpu_sessions\x00c/\x00o/%s' % self.mpu_id,
             HTTPOk,
             {}),
            # SLO heartbeat response is 202...
            ('PUT', '/v1/a/\x00mpu_manifests\x00c/\x00o/%s?'
                    'heartbeat=on&multipart-manifest=put' % self.mpu_id,
             HTTPAccepted, {}, json.dumps(put_resp_body).encode('ascii')),
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
        mw = MPUMiddleware(self.app, {}, logger=self.debug_logger)
        resp = req.get_response(mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(200, resp.status_int)
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
            ('POST', '/v1/a/\x00mpu_sessions\x00c/\x00o/%s' % self.mpu_id,
             HTTPOk,
             {}),
            # SLO heartbeat response is 202...
            ('PUT', '/v1/a/\x00mpu_manifests\x00c/\x00o/%s?'
                    'heartbeat=on&multipart-manifest=put' % self.mpu_id,
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
        mw = MPUMiddleware(self.app, {}, logger=self.debug_logger)
        resp = req.get_response(mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(200, resp.status_int)
        resp_dict = json.loads(resp_body)
        self.assertEqual({
            'Response Status': '503 Service Unavailable',
        }, resp_dict)
        expected = [call[:2]
                    for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_complete_mpu_session_aborted(self):
        ts_session = next(self.ts_iter)
        ts_aborted = next(self.ts_iter)
        ts_complete = next(self.ts_iter)
        registered_calls = [
            ('HEAD',
             '/v1/a/\x00mpu_sessions\x00c/\x00o/%s' % self.mpu_id,
             HTTPOk,
             {'X-Timestamp': ts_aborted.internal,
              'Content-Type': 'application/x-mpu-aborted',
              'X-Backend-Data-Timestamp': ts_session.internal,
              'X-Object-Transient-Sysmeta-Mpu-State': 'created',
              }),
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
        mw = MPUMiddleware(self.app, {})
        resp = req.get_response(mw)
        b''.join(resp.app_iter)
        self.assertEqual(409, resp.status_int)
        expected = [call[:2] for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_complete_mpu_session_deleted(self):
        ts_complete = next(self.ts_iter)
        registered_calls = [
            ('HEAD', '/v1/a/\x00mpu_sessions\x00c/\x00o/%s' % self.mpu_id,
             HTTPNotFound, {}),
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
        mw = MPUMiddleware(self.app, {})
        resp = req.get_response(mw)
        b''.join(resp.app_iter)
        self.assertEqual(404, resp.status_int)
        expected = [call[:2] for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def _do_test_abort_mpu(self, extra_session_resp_headers):
        ts_session = next(self.ts_iter)
        ts_other = next(self.ts_iter)
        ts_abort = next(self.ts_iter)
        session_resp_headers = {
            'X-Timestamp': ts_other.internal,
            'Content-Type': 'application/x-mpu',
            'X-Backend-Data-Timestamp': ts_session.internal,
            'X-Object-Transient-Sysmeta-Mpu-State': 'created',
            'X-Object-Sysmeta-Mpu-X-Object-Meta-Foo': 'blah',
            'X-Object-Sysmeta-Mpu-Has-Content-Type': 'yes',
            'X-Object-Sysmeta-Mpu-Content-Type': 'application/test',
        }
        if extra_session_resp_headers:
            session_resp_headers.update(extra_session_resp_headers)
        registered_calls = [
            ('HEAD',
             '/v1/a/\x00mpu_sessions\x00c/\x00o/%s' % self.mpu_id,
             HTTPOk,
             session_resp_headers),
            ('HEAD', '/v1/a/c/o', HTTPOk, session_resp_headers),
            ('POST',
             '/v1/a/\x00mpu_sessions\x00c/\x00o/%s' % self.mpu_id,
             HTTPOk,
             {}),
            ('PUT',
             '/v1/a/\x00mpu_manifests\x00c/\x00o/%s/marker-aborted'
             % self.mpu_id,
             HTTPCreated,
             {}),
            ('DELETE',
             '/v1/a/\x00mpu_sessions\x00c/\x00o/%s' % self.mpu_id,
             HTTPNoContent,
             {})
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_abort.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
                            headers=req_hdrs)
        req.method = 'DELETE'
        mw = MPUMiddleware(self.app, {})
        resp = req.get_response(mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(204, resp.status_int)
        self.assertEqual(b'', resp_body)
        expected = [call[:2] for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

        session_hdrs = self.app.headers[4]
        return session_hdrs, ts_abort

    def test_abort_mpu(self):
        extra_hdrs = {'X-Object-Transient-Sysmeta-Mpu-State': 'created'}
        session_post_hdrs, ts_abort = self._do_test_abort_mpu(extra_hdrs)
        self.assertEqual(
            {'Content-Type': 'application/x-mpu-aborted',
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Timestamp': ts_abort.internal,
             'X-Object-Transient-Sysmeta-Mpu-State': 'created'},
            session_post_hdrs)

    def test_abort_mpu_session_completing(self):
        # verify that state is copied across to new POST
        extra_hdrs = {'X-Object-Transient-Sysmeta-Mpu-State': 'completing'}
        session_post_hdrs, ts_abort = self._do_test_abort_mpu(extra_hdrs)
        self.assertEqual(
            {'Content-Type': 'application/x-mpu-aborted',
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Timestamp': ts_abort.internal,
             'X-Object-Transient-Sysmeta-Mpu-State': 'completing'},
            session_post_hdrs)

    def test_abort_mpu_session_aborted(self):
        # verify it's ok to abort an already aborted session
        extra_hdrs = {'Content-Type': 'application/x-mpu-aborted'}
        session_post_hdrs, ts_abort = self._do_test_abort_mpu(extra_hdrs)
        self.assertEqual(
            {'Content-Type': 'application/x-mpu-aborted',
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Timestamp': ts_abort.internal,
             'X-Object-Transient-Sysmeta-Mpu-State': 'created'},
            session_post_hdrs)

    def test_abort_mpu_session_deleted(self):
        ts_abort = next(self.ts_iter)
        registered_calls = [
            ('HEAD', '/v1/a/\x00mpu_sessions\x00c/\x00o/%s' % self.mpu_id,
             HTTPNotFound, {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_abort.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
                            headers=req_hdrs)
        req.method = 'POST'
        req.body = json.dumps([
            {'part_number': 1, 'etag': 'a' * 32},
            {'part_number': 2, 'etag': 'b' * 32},
        ])
        mw = MPUMiddleware(self.app, {})
        resp = req.get_response(mw)
        b''.join(resp.app_iter)
        self.assertEqual(404, resp.status_int)
        expected = [call[:2] for call in self.exp_calls + registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_mpu_async_cleanup_DELETE(self):
        backend_responses = ResponseCollection([ResponseData(
            204, headers={'x-object-sysmeta-mpu-upload-id': self.mpu_id})
        ])
        env_updates = {'swift.backend_responses': backend_responses}
        self.app.register('DELETE', '/v1/a/c/o', swob.HTTPNoContent, {}, None,
                          env_updates=env_updates)
        self.app.register(
            'PUT',
            '/v1/a/\x00mpu_manifests\x00c/\x00o/%s/marker-deleted'
            % self.mpu_id,
            HTTPAccepted,
            {})
        req = Request.blank('/v1/a/c/o')
        req.method = 'DELETE'
        mw = MPUMiddleware(self.app, {})
        resp = req.get_response(mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(204, resp.status_int)
        self.assertEqual(b'', resp_body)
        exp_calls = [
            ('DELETE', '/v1/a/c/o'),
            ('PUT',
             '/v1/a/\x00mpu_manifests\x00c/\x00o/%s/marker-deleted'
             % self.mpu_id),
        ]
        self.assertEqual(exp_calls, self.app.calls)

    def test_mpu_async_cleanup_PUT(self):
        backend_responses = ResponseCollection([ResponseData(
            201, headers={'x-object-sysmeta-mpu-upload-id': self.mpu_id})
        ])
        env_updates = {'swift.backend_responses': backend_responses}
        self.app.register('PUT', '/v1/a/c/o', swob.HTTPCreated, {}, None,
                          env_updates=env_updates)
        self.app.register(
            'PUT',
            '/v1/a/\x00mpu_manifests\x00c/\x00o/%s/marker-deleted'
            % self.mpu_id,
            HTTPAccepted,
            {})
        req = Request.blank('/v1/a/c/o')
        req.method = 'PUT'
        mw = MPUMiddleware(self.app, {})
        resp = req.get_response(mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(201, resp.status_int)
        self.assertEqual(b'', resp_body)
        exp_calls = [
            ('PUT', '/v1/a/c/o'),
            ('PUT',
             '/v1/a/\x00mpu_manifests\x00c/\x00o/%s/marker-deleted'
             % self.mpu_id),
        ]
        self.assertEqual(exp_calls, self.app.calls)

    def test_mpu_async_cleanup_PUT_mixed_backend_responses(self):
        # verify bad resp headers are ignored and multiple mpu's are cleaned up
        mpu_id_alt = MPUId.create(next(self.ts_iter))
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
            '/v1/a/\x00mpu_manifests\x00c/\x00o/%s/marker-deleted'
            % self.mpu_id,
            HTTPAccepted,
            {})
        self.app.register(
            'PUT',
            '/v1/a/\x00mpu_manifests\x00c/\x00o/%s/marker-deleted'
            % mpu_id_alt,
            HTTPAccepted,
            {})
        req = Request.blank('/v1/a/c/o')
        req.method = 'PUT'
        mw = MPUMiddleware(self.app, {})
        resp = req.get_response(mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(201, resp.status_int)
        self.assertEqual(b'', resp_body)
        self.assertEqual(3, len(self.app.calls))
        self.assertEqual(('PUT', '/v1/a/c/o'), self.app.calls[0])
        exp_calls = sorted([
            ('PUT',
             '/v1/a/\x00mpu_manifests\x00c/\x00o/%s/marker-deleted'
             % self.mpu_id),
            ('PUT',
             '/v1/a/\x00mpu_manifests\x00c/\x00o/%s/marker-deleted'
             % mpu_id_alt),
        ])
        self.assertEqual(exp_calls, sorted(self.app.calls[1:]))


class TestMpuMiddlewareErrors(BaseTestMPUMiddleware):
    def setUp(self):
        super(TestMpuMiddlewareErrors, self).setUp()
        self.requests = [
            # list uploads
            Request.blank('/v1/a/c?uploads=true',
                          environ={'REQUEST_METHOD': 'GET'}),
            # create upload
            Request.blank('/v1/a/c/o?uploads=true',
                          environ={'REQUEST_METHOD': 'POST'}),
            # upload part
            Request.blank('/v1/a/c/o?upload-id=%s&part-number=1' % self.mpu_id,
                          environ={'REQUEST_METHOD': 'PUT'}),
            # list parts
            Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
                          environ={'REQUEST_METHOD': 'GET'}),
            # complete upload
            Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
                          environ={'REQUEST_METHOD': 'POST'}),
            # abort upload
            Request.blank('/v1/a/c/o?upload-id=%s' % self.mpu_id,
                          environ={'REQUEST_METHOD': 'DELETE'}),
        ]

    def test_api_requests_user_container_not_found(self):
        self.app.register('HEAD', '/v1/a/c', HTTPNotFound, {})
        for req in self.requests:
            self.app.clear_calls()
            resp = req.get_response(MPUMiddleware(self.app, {}))
            self.assertEqual(404, resp.status_int)
            self.assertEqual([call[:2] for call in self.exp_calls],
                             self.app.calls)

    def test_api_requests_user_container_unavailable(self):
        self.app.register('HEAD', '/v1/a/c', HTTPServiceUnavailable, {})
        for req in self.requests:
            self.app.clear_calls()
            resp = req.get_response(MPUMiddleware(self.app, {}))
            self.assertEqual(503, resp.status_int)
            self.assertEqual([call[:2] for call in self.exp_calls],
                             self.app.calls)

    def test_api_requests_user_unexpected_error(self):
        self.app.register('HEAD', '/v1/a/c', HTTPPreconditionFailed, {})
        for req in self.requests:
            self.app.clear_calls()
            resp = req.get_response(MPUMiddleware(self.app, {}))
            self.assertEqual(503, resp.status_int)
            self.assertEqual([call[:2] for call in self.exp_calls],
                             self.app.calls)
