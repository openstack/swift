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
import contextlib
import json
import unittest
import mock

from swift.common import swob
from swift.common.middleware.mpu import MPUMiddleware
from swift.common.swob import Request, HTTPOk, HTTPNotFound, HTTPCreated, \
    HTTPAccepted, HTTPNoContent
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


class TestMPUMiddleware(unittest.TestCase):
    # TODO: assert 'X-Backend-Allow-Reserved-Names' in backend requests
    def setUp(self):
        self.app = FakeSwift()
        self.ts_iter = make_timestamp_iter()
        self.debug_logger = debug_logger()

    def _setup_mpu_create_requests(self):
        self.app.register('HEAD', '/v1/a', HTTPOk, {})
        self.app.register('HEAD', '/v1/a/c', HTTPOk, {})
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

    def _do_test_create_mpu(self, req_headers):
        expected = self._setup_mpu_create_requests()
        self.app.register(
            'PUT', '/v1/a/\x00mpu_sessions\x00c/\x00o/test-id',
            HTTPCreated, {})
        expected.append(('PUT', '/v1/a/\x00mpu_sessions\x00c/\x00o/test-id'))
        req = Request.blank('/v1/a/c/o?uploads=true', headers=req_headers)
        req.method = 'POST'
        with mock_generate_unique_id('test-id'):
            mw = MPUMiddleware(self.app, {})
            resp = req.get_response(mw)
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
        # TODO: include aborted mpu session in listing
        listing = [{'name': '\x00o/test-id-%d' % i, 'hash': 'etag',
                    'bytes': 0,
                    'content_type': 'application/mpu',
                    'last_modified': '1970-01-01T00:00:00.000000'}
                   for i in range(3)]
        self.app.register('GET', '/v1/a/\x00mpu_sessions\x00c', HTTPOk, {},
                          body=json.dumps(listing).encode('ascii'))
        req = Request.blank('/v1/a/c?uploads')
        req.method = 'GET'
        mw = MPUMiddleware(self.app, {}, logger=self.debug_logger)
        resp = req.get_response(mw)
        self.assertEqual(200, resp.status_int)
        expected = [
            ('GET', '/v1/a/\x00mpu_sessions\x00c'),
        ]
        self.assertEqual(expected, self.app.calls)
        exp_listing = [{'name': 'o/test-id-%d' % i, 'hash': 'etag',
                        'bytes': 0,
                        'content_type': 'application/mpu',
                        'last_modified': '1970-01-01T00:00:00.000000'}
                       for i in range(3)]
        self.assertEqual(exp_listing, json.loads(resp.body))

    def test_upload_part(self):
        ts_session = next(self.ts_iter)
        ts_part = next(self.ts_iter)
        self.app.register(
            'HEAD', '/v1/a/\x00mpu_sessions\x00c/\x00o/test-id', HTTPOk,
            {'X-Timestamp': ts_session.internal,
             'Content-Type': 'application/x-mpu',
             })
        self.app.register(
            'PUT', '/v1/a/\x00mpu_parts\x00c/\x00o/test-id/1', HTTPCreated,
            {'X-Timestamp': ts_part.internal})
        req = Request.blank('/v1/a/c/o?upload-id=test-id&part-number=1')
        req.method = 'PUT'
        req.body = b'testing'
        mw = MPUMiddleware(self.app, {})
        resp = req.get_response(mw)
        self.assertEqual(200, resp.status_int)
        self.assertEqual(md5(b'testing', usedforsecurity=False).hexdigest(),
                         resp.headers.get('Etag'))
        self.assertEqual('7', resp.headers['Content-Length'])
        expected = [('HEAD', '/v1/a/\x00mpu_sessions\x00c/\x00o/test-id'),
                    ('PUT', '/v1/a/\x00mpu_parts\x00c/\x00o/test-id/1')]
        self.assertEqual(expected, self.app.calls)

    def test_list_parts(self):
        ts_session = next(self.ts_iter)
        self.app.register(
            'HEAD', '/v1/a/\x00mpu_sessions\x00c/\x00o/test-id', HTTPOk,
            {'X-Timestamp': ts_session.internal,
             'Content-Type': 'application/x-mpu'})
        listing = [{'name': '\x00o/test-id/%d' % i, 'hash': 'etag%d' % i,
                    'bytes': i,
                    'content_type': 'text/plain',
                    'last_modified': '1970-01-01T00:00:00.000000'}
                   for i in range(3)]
        self.app.register('GET', '/v1/a/\x00mpu_parts\x00c', HTTPOk, {},
                          body=json.dumps(listing).encode('ascii'))
        req = Request.blank('/v1/a/c/o?upload-id=test-id')
        req.method = 'GET'
        mw = MPUMiddleware(self.app, {}, logger=self.debug_logger)
        resp = req.get_response(mw)
        self.assertEqual(200, resp.status_int)
        expected = [
            ('HEAD', '/v1/a/\x00mpu_sessions\x00c/\x00o/test-id'),
            ('GET', '/v1/a/\x00mpu_parts\x00c?prefix=%s'
             % quote('\x00o/test-id', safe='')),
        ]
        self.assertEqual(expected, self.app.calls)
        exp_listing = [{'name': 'o/test-id/%d' % i, 'hash': 'etag%d' % i,
                        'bytes': i,
                        'content_type': 'text/plain',
                        'last_modified': '1970-01-01T00:00:00.000000'}
                       for i in range(3)]
        self.assertEqual(exp_listing, json.loads(resp.body))

    def test_complete_mpu(self):
        ts_session = next(self.ts_iter)
        ts_other = next(self.ts_iter)
        ts_complete = next(self.ts_iter)
        registered_calls = [
            ('HEAD', '/v1/a/\x00mpu_sessions\x00c/\x00o/test-id', HTTPOk,
             {'X-Timestamp': ts_other.internal,
              'Content-Type': 'application/x-mpu',
              'X-Backend-Data-Timestamp': ts_session.internal,
              'X-Object-Transient-Sysmeta-Mpu-State': 'created',
              'X-Object-Sysmeta-Mpu-X-Object-Meta-Foo': 'blah',
              'X-Object-Sysmeta-Mpu-Has-Content-Type': 'yes',
              'X-Object-Sysmeta-Mpu-Content-Type': 'application/test',
              }),
            ('POST', '/v1/a/\x00mpu_sessions\x00c/\x00o/test-id', HTTPOk, {}),
            # SLO heartbeat response is 202...
            ('PUT', '/v1/a/\x00mpu_manifests\x00c/\x00o/test-id?'
                    'heartbeat=on&multipart-manifest=put',
             HTTPAccepted, {}),
            ('PUT', '/v1/a/c/o', HTTPCreated, {}),
            ('DELETE', '/v1/a/\x00mpu_sessions\x00c/\x00o/test-id',
             HTTPAccepted, {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=test-id', headers=req_hdrs)
        req.method = 'POST'
        req.body = json.dumps([
            {'part_number': 1, 'etag': 'a' * 32},
            {'part_number': 2, 'etag': 'b' * 32},
        ])
        mw = MPUMiddleware(self.app, {})
        resp = req.get_response(mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(200, resp.status_int)
        self.assertEqual(b'', resp_body)
        expected = [call[:2] for call in registered_calls]
        self.assertEqual(expected, self.app.calls)

        session_hdrs = self.app.headers[1]
        self.assertEqual(
            {'Content-Type': 'application/x-mpu',
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Timestamp': ts_complete.internal,
             'X-Object-Transient-Sysmeta-Mpu-State': 'completing'},
            session_hdrs)

        actual_manifest_body = self.app.uploaded.get(
            '/v1/a/\x00mpu_manifests\x00c/\x00o/test-id')[1]
        self.assertEqual([{"path": "\x00mpu_parts\x00c/\x00o/test-id/1",
                           "etag": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
                          {"path": "\x00mpu_parts\x00c/\x00o/test-id/2",
                           "etag": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}],
                         json.loads(actual_manifest_body))
        manifest_hdrs = self.app.headers[2]
        self.assertEqual(
            {'Accept': 'application/json',
             'Content-Length': '196',
             'Content-Type': 'application/test',
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Object-Meta-Foo': 'blah',
             'X-Object-Sysmeta-Allow-Reserved-Names': 'true',
             'X-Object-Sysmeta-Mpu-Manifest-Etag':
                 '8039a288fc27346c4ebd08f17995e8bc',
             'X-Object-Sysmeta-Mpu-Parts-Count': '2',
             'X-Object-Sysmeta-Mpu-Upload-Id': 'test-id',
             'X-Timestamp': ts_session.internal},
            manifest_hdrs)

        actual_mpu_body = self.app.uploaded.get('/v1/a/c/o')[1]
        self.assertEqual(b'', actual_mpu_body)
        mpu_hdrs = self.app.headers[3]
        self.assertEqual(
            {'Content-Length': '0',
             'Host': 'localhost:80',
             'User-Agent': 'Swift',
             'X-Backend-Allow-Reserved-Names': 'true',
             'X-Object-Sysmeta-Allow-Reserved-Names': 'true',
             'X-Object-Sysmeta-Mpu-Upload-Id': 'test-id',
             'X-Symlink-Target': '\x00mpu_manifests\x00c/\x00o/test-id',
             'X-Timestamp': ts_session.internal},
            mpu_hdrs)

    def test_complete_mpu_session_aborted(self):
        ts_session = next(self.ts_iter)
        ts_aborted = next(self.ts_iter)
        ts_complete = next(self.ts_iter)
        registered_calls = [
            ('HEAD', '/v1/a/\x00mpu_sessions\x00c/\x00o/test-id', HTTPOk,
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
        req = Request.blank('/v1/a/c/o?upload-id=test-id', headers=req_hdrs)
        req.method = 'POST'
        req.body = json.dumps([
            {'part_number': 1, 'etag': 'a' * 32},
            {'part_number': 2, 'etag': 'b' * 32},
        ])
        mw = MPUMiddleware(self.app, {})
        resp = req.get_response(mw)
        b''.join(resp.app_iter)
        self.assertEqual(409, resp.status_int)
        expected = [call[:2] for call in registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_complete_mpu_session_deleted(self):
        ts_complete = next(self.ts_iter)
        registered_calls = [
            ('HEAD', '/v1/a/\x00mpu_sessions\x00c/\x00o/test-id',
             HTTPNotFound, {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_complete.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=test-id', headers=req_hdrs)
        req.method = 'POST'
        req.body = json.dumps([
            {'part_number': 1, 'etag': 'a' * 32},
            {'part_number': 2, 'etag': 'b' * 32},
        ])
        mw = MPUMiddleware(self.app, {})
        resp = req.get_response(mw)
        b''.join(resp.app_iter)
        self.assertEqual(404, resp.status_int)
        expected = [call[:2] for call in registered_calls]
        self.assertEqual(expected, self.app.calls)

    def _do_test_abort_mpu(self, extra_session_resp_headers):
        ts_session = next(self.ts_iter)
        ts_other = next(self.ts_iter)
        ts_abort = next(self.ts_iter)
        session_resp_headers = {
            'X-Timestamp': ts_other.internal,
            'Content-Type': 'application/mpu',
            'X-Backend-Data-Timestamp': ts_session.internal,
            'X-Object-Transient-Sysmeta-Mpu-State': 'created',
            'X-Object-Sysmeta-Mpu-X-Object-Meta-Foo': 'blah',
            'X-Object-Sysmeta-Mpu-Has-Content-Type': 'yes',
            'X-Object-Sysmeta-Mpu-Content-Type': 'application/test',
        }
        if extra_session_resp_headers:
            session_resp_headers.update(extra_session_resp_headers)
        registered_calls = [
            ('HEAD', '/v1/a/\x00mpu_sessions\x00c/\x00o/test-id', HTTPOk,
             session_resp_headers),
            ('HEAD', '/v1/a/c/o', HTTPOk, session_resp_headers),
            ('POST', '/v1/a/\x00mpu_sessions\x00c/\x00o/test-id', HTTPOk, {}),
            ('PUT',
             '/v1/a/\x00mpu_manifests\x00c/\x00o/test-id/marker-aborted',
             HTTPCreated, {}),
            ('DELETE', '/v1/a/\x00mpu_sessions\x00c/\x00o/test-id',
             HTTPNoContent, {})
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_abort.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=test-id', headers=req_hdrs)
        req.method = 'DELETE'
        mw = MPUMiddleware(self.app, {})
        resp = req.get_response(mw)
        resp_body = b''.join(resp.app_iter)
        self.assertEqual(204, resp.status_int)
        self.assertEqual(b'', resp_body)
        expected = [call[:2] for call in registered_calls]
        self.assertEqual(expected, self.app.calls)

        session_hdrs = self.app.headers[2]
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
            ('HEAD', '/v1/a/\x00mpu_sessions\x00c/\x00o/test-id',
             HTTPNotFound, {}),
        ]
        for call in registered_calls:
            self.app.register(*call)
        req_hdrs = {'X-Timestamp': ts_abort.internal,
                    'Content-Type': 'ignored'}
        req = Request.blank('/v1/a/c/o?upload-id=test-id', headers=req_hdrs)
        req.method = 'POST'
        req.body = json.dumps([
            {'part_number': 1, 'etag': 'a' * 32},
            {'part_number': 2, 'etag': 'b' * 32},
        ])
        mw = MPUMiddleware(self.app, {})
        resp = req.get_response(mw)
        b''.join(resp.app_iter)
        self.assertEqual(404, resp.status_int)
        expected = [call[:2] for call in registered_calls]
        self.assertEqual(expected, self.app.calls)

    def test_mpu_async_cleanup_DELETE(self):
        backend_responses = ResponseCollection([ResponseData(
            204, headers={'x-object-sysmeta-mpu-upload-id': 'test-id'})
        ])
        env_updates = {'swift.backend_responses': backend_responses}
        self.app.register('DELETE', '/v1/a/c/o', swob.HTTPNoContent, {}, None,
                          env_updates=env_updates)
        self.app.register(
            'PUT', '/v1/a/\x00mpu_manifests\x00c/\x00o/test-id/marker-deleted',
            HTTPAccepted, {})
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
             '/v1/a/\x00mpu_manifests\x00c/\x00o/test-id/marker-deleted'),
        ]
        self.assertEqual(exp_calls, self.app.calls)

    def test_mpu_async_cleanup_PUT(self):
        backend_responses = ResponseCollection([ResponseData(
            201, headers={'x-object-sysmeta-mpu-upload-id': 'test-id'})
        ])
        env_updates = {'swift.backend_responses': backend_responses}
        self.app.register('PUT', '/v1/a/c/o', swob.HTTPCreated, {}, None,
                          env_updates=env_updates)
        self.app.register(
            'PUT', '/v1/a/\x00mpu_manifests\x00c/\x00o/test-id/marker-deleted',
            HTTPAccepted, {})
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
             '/v1/a/\x00mpu_manifests\x00c/\x00o/test-id/marker-deleted'),
        ]
        self.assertEqual(exp_calls, self.app.calls)
