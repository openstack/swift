# Copyright (c) 2026 NVIDIA
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

import io
import json
from unittest import mock
import unittest

from contextlib import redirect_stdout
from urllib.parse import quote

from swift.cli import mpu_tool
from swift.common.internal_client import UnexpectedResponse
from swift.common.middleware.mpu import (
    MPU_SESSION_CREATED_CONTENT_TYPE,
    MPU_SESSION_COMPLETED_CONTENT_TYPE,
    make_mpu_hidden_account_name,
    make_sessions_container_name,
    make_parts_container_name,
)

# serialized ObjectRef names: <user_name>/<upload_id>[/<tail>]
# obj1 has a lifeline, two parts and a session; obj3 has a part but no session;
# obj2 has a session but no parts. The lifeline object has a trailing '/' (an
# empty part number suffix) and sorts before the numbered parts.
LIFELINE_OBJ1 = {'name': 'obj1/UPLOAD1/', 'bytes': 0, 'hash': 'l1',
                 'content_type': 'application/x-mpu-marker',
                 'last_modified': '2026-07-30T00:00:01.500000'}
PARTS_OBJ1 = [
    {'name': 'obj1/UPLOAD1/000001', 'bytes': 1024, 'hash': 'p1',
     'content_type': 'application/octet-stream',
     'last_modified': '2026-07-30T00:00:02.000000'},
    {'name': 'obj1/UPLOAD1/000002', 'bytes': 2048, 'hash': 'p2',
     'content_type': 'application/octet-stream',
     'last_modified': '2026-07-30T00:00:03.000000'},
]
PART_OBJ3 = {'name': 'obj3/UPLOAD3/000001', 'bytes': 4096, 'hash': 'p3',
             'content_type': 'application/octet-stream',
             'last_modified': '2026-07-30T00:00:04.000000'}
PART_ITEMS = [LIFELINE_OBJ1] + PARTS_OBJ1 + [PART_OBJ3]
SESSION_ITEMS = [
    {'name': 'obj1/UPLOAD1', 'bytes': 0, 'hash': 'h1',
     'content_type': MPU_SESSION_CREATED_CONTENT_TYPE,
     'last_modified': '2026-07-30T00:00:00.000000'},
    {'name': 'obj2/UPLOAD2', 'bytes': 0, 'hash': 'h2',
     'content_type': MPU_SESSION_COMPLETED_CONTENT_TYPE,
     'last_modified': '2026-07-30T00:00:01.000000'},
]
# metadata returned by a HEAD on the completed obj1 manifest in the user
# container; obj2 and obj3 have no manifest object yet (HEAD -> 404).
MANIFEST_META = {
    'content-length': '3072',
    'etag': 'aaaa-2',
    'x-object-sysmeta-mpu-upload-id': 'UPLOAD1',
    'x-object-sysmeta-mpu-etag': 'realetag',
    'x-object-sysmeta-mpu-parts-count': '2',
}


class TestMpuList(unittest.TestCase):
    def setUp(self):
        self.account = 'AUTH_test'
        self.container = 'c'
        self.hidden_account = make_mpu_hidden_account_name(self.account)
        self.sessions_container = make_sessions_container_name(self.container)
        self.parts_container = make_parts_container_name(self.container)

    def _run(self, resource, items):
        buf = io.StringIO()
        fake_client = mock.MagicMock()
        fake_client.iter_objects.return_value = iter(items)
        with mock.patch.object(mpu_tool, '_make_internal_client',
                               return_value=fake_client):
            with redirect_stdout(buf):
                rv = mpu_tool.main(
                    ['list', self.account, self.container, resource])
        return fake_client, buf.getvalue(), rv

    def test_list_parts(self):
        items = [
            {'name': 'obj/UID/000001', 'bytes': 1024,
             'content_type': 'application/octet-stream'},
            {'name': 'obj/UID/000002', 'bytes': 2048,
             'content_type': 'application/octet-stream'},
        ]
        client, out, rv = self._run('parts', items)

        self.assertEqual(0, rv)
        lines = out.splitlines()
        self.assertEqual(2, len(lines))
        self.assertIn('obj/UID/000001', lines[0])
        self.assertIn('application/octet-stream', lines[0])
        self.assertIn('obj/UID/000002', lines[1])

    def test_list_sessions(self):
        items = [
            {'name': 'obj/UID', 'bytes': 0,
             'content_type': MPU_SESSION_CREATED_CONTENT_TYPE},
        ]
        client, out, rv = self._run('sessions', items)

        self.assertEqual(0, rv)
        self.assertIn('obj/UID', out)
        self.assertIn(MPU_SESSION_CREATED_CONTENT_TYPE, out)

    def test_list_uses_correct_hidden_container_for_parts(self):
        client, _, _ = self._run('parts', [])
        client.iter_objects.assert_called_once_with(
            self.hidden_account, self.parts_container,
            headers=mpu_tool.RESERVED_NAMES_HEADER)

    def test_list_uses_correct_hidden_container_for_sessions(self):
        client, _, _ = self._run('sessions', [])
        client.iter_objects.assert_called_once_with(
            self.hidden_account, self.sessions_container,
            headers=mpu_tool.RESERVED_NAMES_HEADER)

    def test_list_empty(self):
        client, out, rv = self._run('parts', [])
        self.assertEqual(0, rv)
        self.assertEqual('', out)

    def test_list_name_and_content_type_tab_separated(self):
        items = [{'name': 'a/b/1', 'bytes': 0,
                  'content_type': 'application/octet-stream'}]
        _, out, _ = self._run('parts', items)
        self.assertEqual('a/b/1\tapplication/octet-stream\n', out)


class TestMpuInfo(unittest.TestCase):
    def setUp(self):
        self.account = 'AUTH_test'
        self.container = 'c'
        self.hidden_account = make_mpu_hidden_account_name(self.account)
        self.sessions_container = make_sessions_container_name(self.container)
        self.parts_container = make_parts_container_name(self.container)

    def _run(self, extra_args, part_items=None, session_items=None):
        part_items = PART_ITEMS if part_items is None else part_items
        session_items = SESSION_ITEMS if session_items is None else \
            session_items
        fake_client = mock.MagicMock()

        def fake_iter_objects(account, container, marker='', end_marker='',
                              headers=None):
            self.assertEqual(account, self.hidden_account)
            self.assertEqual(headers, mpu_tool.RESERVED_NAMES_HEADER)
            if container == self.sessions_container:
                return iter(session_items)
            elif container == self.parts_container:
                return iter(part_items)
            raise AssertionError('unexpected container %r' % container)

        def fake_get_object_metadata(account, container, obj, headers=None):
            # the manifest HEAD targets the *user* account and container
            self.assertEqual(account, self.account)
            self.assertEqual(container, self.container)
            self.assertEqual(headers, mpu_tool.INCLUDE_MPU_SYSMETA_HEADER)
            if obj == 'obj1':
                return dict(MANIFEST_META)
            raise UnexpectedResponse('404 Not Found', None)

        fake_client.iter_objects.side_effect = fake_iter_objects
        fake_client.get_object_metadata.side_effect = fake_get_object_metadata
        argv = ['info', self.account, self.container] + extra_args
        buf = io.StringIO()
        with mock.patch.object(mpu_tool, '_make_internal_client',
                               return_value=fake_client) as mock_make:
            with redirect_stdout(buf):
                rv = mpu_tool.main(argv)
        self.assertEqual(0, rv)
        return mock_make, fake_client, buf.getvalue()

    def test_default_json_grouped_by_object_and_upload(self):
        _, _, out = self._run([])
        data = json.loads(out)
        # object order: parts objects first (obj1, obj3), then the session-only
        # object (obj2) appended after; raw listing fields preserved. Within
        # each object parts and sessions are grouped per upload id. obj1 has a
        # manifest object in the user container attached to its UPLOAD1 group;
        # obj2 and obj3 do not. The lifeline object is a part with no part
        # number suffix and appears in the object's parts list.
        self.assertEqual(data, {
            'user_container': quote('%s/%s' % (self.account, self.container)),
            'parts_container': quote(
                '%s/%s' % (self.hidden_account, self.parts_container)),
            'sessions_container': quote(
                '%s/%s' % (self.hidden_account, self.sessions_container)),
            'objects': [
                {'object': 'obj1', 'uploads': [
                    {'upload_id': 'UPLOAD1',
                     'parts': [LIFELINE_OBJ1] + PARTS_OBJ1,
                     'sessions': [SESSION_ITEMS[0]],
                     'manifest': MANIFEST_META}]},
                {'object': 'obj3', 'uploads': [
                    {'upload_id': 'UPLOAD3',
                     'parts': [PART_OBJ3],
                     'sessions': []}]},
                {'object': 'obj2', 'uploads': [
                    {'upload_id': 'UPLOAD2',
                     'parts': [],
                     'sessions': [SESSION_ITEMS[1]]}]},
            ]})

    def test_summary_json_shape(self):
        objects = mpu_tool.group_by_object(PART_ITEMS, SESSION_ITEMS,
                                           full=False, limit=10)
        self.assertEqual(objects, [
            {'object': 'obj1', 'uploads': [
                {'upload_id': 'UPLOAD1',
                 'parts': [{'part_number': '', 'size': 0},
                           {'part_number': '000001', 'size': 1024},
                           {'part_number': '000002', 'size': 2048}],
                 'sessions': [{'state': 'created'}]}]},
            {'object': 'obj3', 'uploads': [
                {'upload_id': 'UPLOAD3',
                 'parts': [{'part_number': '000001', 'size': 4096}],
                 'sessions': []}]},
            {'object': 'obj2', 'uploads': [
                {'upload_id': 'UPLOAD2',
                 'parts': [],
                 'sessions': [{'state': 'completed'}]}]},
        ])

    def test_summary_text_output(self):
        _, _, out = self._run(['--summary'])
        # the url-quoted '<account>/<container>' paths are shown at the top
        self.assertIn(
            'user_container: %s' % quote(
                '%s/%s' % (self.account, self.container)), out)
        self.assertIn(
            'parts_container: %s' % quote(
                '%s/%s' % (self.hidden_account, self.parts_container)), out)
        self.assertIn(
            'sessions_container: %s' % quote(
                '%s/%s' % (self.hidden_account, self.sessions_container)), out)
        # object name is the top-level heading, upload id is the next level
        self.assertIn('obj1:', out)
        self.assertIn('  upload_id=UPLOAD1:', out)
        # parts are shown as a count of suffixed + unsuffixed (lifeline) parts
        self.assertIn('    Parts: 2 + 1', out)
        self.assertNotIn('part_number=', out)
        self.assertIn('    Sessions (1):', out)
        self.assertIn('      state=created', out)
        self.assertIn('obj3:', out)
        self.assertIn('  upload_id=UPLOAD3:', out)
        self.assertIn('    Parts: 1 + 0', out)
        self.assertIn('obj2:', out)
        self.assertIn('  upload_id=UPLOAD2:', out)
        self.assertIn('    Parts: 0 + 0', out)
        self.assertIn('      state=completed', out)
        # object grouping order is obj1, obj3, obj2
        self.assertLess(out.index('obj1:'), out.index('obj3:'))
        self.assertLess(out.index('obj3:'), out.index('obj2:'))
        # obj1's UPLOAD1 group has a manifest (mpu sysmeta subset)
        self.assertIn('    Manifest:', out)
        self.assertIn('      upload-id=UPLOAD1', out)
        self.assertIn('parts-count=2', out)
        self.assertEqual(1, out.count('    Manifest:'))
        # user-facing summary does not leak the raw hash field
        self.assertNotIn('hash=', out)

    def test_multiple_upload_ids_grouped_per_object(self):
        # obj1 has two concurrent uploads, UPLOAD1 and UPLOAD1B
        parts = [
            {'name': 'obj1/UPLOAD1/000001', 'bytes': 1, 'content_type': 'x'},
            {'name': 'obj1/UPLOAD1B/000001', 'bytes': 2, 'content_type': 'x'},
        ]
        sessions = [
            {'name': 'obj1/UPLOAD1', 'bytes': 0,
             'content_type': MPU_SESSION_CREATED_CONTENT_TYPE},
            {'name': 'obj1/UPLOAD1B', 'bytes': 0,
             'content_type': MPU_SESSION_CREATED_CONTENT_TYPE},
        ]
        objects = mpu_tool.group_by_object(parts, sessions, full=False,
                                           limit=10)
        self.assertEqual(1, len(objects))
        self.assertEqual('obj1', objects[0]['object'])
        uploads = objects[0]['uploads']
        self.assertEqual(['UPLOAD1', 'UPLOAD1B'],
                         [u['upload_id'] for u in uploads])
        self.assertEqual([{'part_number': '000001', 'size': 1}],
                         uploads[0]['parts'])
        self.assertEqual([{'state': 'created'}], uploads[0]['sessions'])
        self.assertEqual([{'part_number': '000001', 'size': 2}],
                         uploads[1]['parts'])

    def test_manifest_creates_upload_group_when_no_parts(self):
        # a completed upload whose parts and session are already gone still
        # surfaces via its manifest, in a new upload group
        fake_client = mock.MagicMock()
        fake_client.iter_objects.side_effect = \
            lambda account, container, marker='', end_marker='', \
            headers=None: iter([])
        fake_client.get_object_metadata.side_effect = \
            lambda account, container, obj, headers=None: dict(MANIFEST_META)
        objects = [{'object': 'obj1', 'uploads': []}]
        mpu_tool.add_manifests(fake_client, self.account, self.container,
                               objects, full=True)
        self.assertEqual(objects, [
            {'object': 'obj1', 'uploads': [
                {'upload_id': 'UPLOAD1', 'parts': [], 'sessions': [],
                 'manifest': MANIFEST_META}]}])

    def test_client_constructed_with_defaults(self):
        mock_make, _, _ = self._run([])
        mock_make.assert_called_once_with(
            '/etc/swift/internal-client.conf', 'Swift MPU Tool', 3)

    def test_empty_listings(self):
        fake_client = mock.MagicMock()
        fake_client.iter_objects.side_effect = \
            lambda account, container, marker='', end_marker='', \
            headers=None: iter([])
        buf = io.StringIO()
        with mock.patch.object(mpu_tool, '_make_internal_client',
                               return_value=fake_client):
            with redirect_stdout(buf):
                rv = mpu_tool.main(['info', self.account, self.container])
        self.assertEqual(0, rv)
        self.assertEqual(json.loads(buf.getvalue()), {
            'user_container': quote('%s/%s' % (self.account, self.container)),
            'parts_container': quote(
                '%s/%s' % (self.hidden_account, self.parts_container)),
            'sessions_container': quote(
                '%s/%s' % (self.hidden_account, self.sessions_container)),
            'objects': []})

    def test_malformed_name_falls_back_to_raw(self):
        fake_client = mock.MagicMock()

        def fake_iter_objects(account, container, marker='', end_marker='',
                              headers=None):
            if container == self.parts_container:
                # a value that cannot be parsed should not abort the listing
                return iter([{'name': 'lonely', 'content_type': 'x',
                              'bytes': 0}])
            return iter([])

        fake_client.iter_objects.side_effect = fake_iter_objects
        fake_client.get_object_metadata.side_effect = \
            UnexpectedResponse('404 Not Found', None)
        buf = io.StringIO()
        with mock.patch.object(mpu_tool, '_make_internal_client',
                               return_value=fake_client):
            with mock.patch.object(mpu_tool.ObjectRef, 'parse',
                                   side_effect=ValueError('bad')):
                with redirect_stdout(buf):
                    rv = mpu_tool.main(
                        ['info', self.account, self.container, '--summary'])
        self.assertEqual(0, rv)
        out = buf.getvalue()
        # the raw name is used as the object name, the upload id is None, and
        # the unparseable part is counted as a part with no suffix rather than
        # aborting the listing
        self.assertIn('lonely:', out)
        self.assertIn('  upload_id=None:', out)
        self.assertIn('    Parts: 0 + 1', out)

    def test_no_object_arg_uses_empty_listing_range(self):
        _, fake_client, _ = self._run([])
        self.assertEqual(2, len(fake_client.iter_objects.call_args_list))
        for call in fake_client.iter_objects.call_args_list:
            self.assertEqual(call[1]['marker'], '')
            self.assertEqual(call[1]['end_marker'], '')

    def test_object_arg_restricts_listing_range(self):
        _, fake_client, _ = self._run(['obj1'])
        marker, end_marker = mpu_tool.object_listing_range('obj1')
        # sanity check the computed range: entries 'obj1/...' fall between them
        self.assertEqual((marker, end_marker), ('obj1/', 'obj10'))
        self.assertLess(marker, 'obj1/UPLOAD1')
        self.assertLess('obj1/UPLOAD1/000009', end_marker)
        self.assertGreater('obj2/UPLOAD', end_marker)
        # both the parts and sessions listings are bounded to the object
        self.assertEqual(2, len(fake_client.iter_objects.call_args_list))
        for call in fake_client.iter_objects.call_args_list:
            self.assertEqual(call[1]['marker'], marker)
            self.assertEqual(call[1]['end_marker'], end_marker)

    def test_limit_caps_objects_and_stops_iterating(self):
        consumed = []

        def big_parts():
            for i in range(20):
                name = 'obj%02d/UPLOAD/000001' % i
                consumed.append(name)
                yield {'name': name, 'bytes': i,
                       'content_type': 'application/octet-stream'}

        _, _, out = self._run(
            ['--limit', '3', '--summary'],
            part_items=big_parts(), session_items=iter([]))
        # object headings end with ':'; the container info lines do not
        headings = [line for line in out.splitlines()
                    if line.endswith(':') and not line.startswith(' ')]
        self.assertEqual(headings, ['obj00:', 'obj01:', 'obj02:'])
        # iteration stopped early rather than consuming the whole listing
        self.assertLess(len(consumed), 20)

    def test_config_load_error_exits(self):
        with mock.patch.object(mpu_tool, '_make_internal_client',
                               side_effect=IOError('no such file')):
            with self.assertRaises(SystemExit) as cm:
                mpu_tool.main(['info', self.account, self.container])
        self.assertIn('no such file', str(cm.exception))

    def test_no_subcommand_prints_help_and_exits(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            rv = mpu_tool.main([])
        self.assertEqual(1, rv)
        self.assertIn('info', buf.getvalue())


class TestMpuMake(unittest.TestCase):
    def setUp(self):
        self.account = 'AUTH_test'
        self.container = 'c'
        self.object = 'myobj'

    def _fake_resp(self, status_int=200, headers=None, body=b''):
        resp = mock.MagicMock()
        resp.status_int = status_int
        resp.headers = headers or {}
        resp.body = body
        return resp

    def _complete_body(self, status='201 Created', etag='abc-1'):
        result = {'Response Status': status, 'Etag': etag,
                  'Response Body': ''}
        return b' \r\n\r\n' + json.dumps(result).encode('ascii')

    def _run(self, extra_args, make_request_side_effect):
        buf = io.StringIO()
        mock_client = mock.MagicMock()
        mock_client.make_request.side_effect = make_request_side_effect
        with mock.patch.object(mpu_tool, '_make_internal_client',
                               return_value=mock_client) as mock_make:
            with redirect_stdout(buf):
                rv = mpu_tool.main(
                    ['make', self.account, self.container, self.object]
                    + extra_args)
        return mock_make, mock_client, buf.getvalue(), rv

    def test_make_happy_path(self):
        session_resp = self._fake_resp(202, {'X-Upload-Id': 'UPLOAD1'})
        part_resp = self._fake_resp(201, {'Etag': '"abc123"'})
        complete_resp = self._fake_resp(202, {}, self._complete_body())

        _, _, out, rv = self._run([], [session_resp, part_resp, complete_resp])

        self.assertEqual(0, rv)
        self.assertIn('UPLOAD1', out)
        self.assertIn('abc123', out)
        self.assertIn('201 Created', out)
        self.assertIn('abc-1', out)

    def test_make_verifies_three_requests(self):
        session_resp = self._fake_resp(202, {'X-Upload-Id': 'UID'})
        part_resp = self._fake_resp(201, {'Etag': '"e1"'})
        complete_resp = self._fake_resp(202, {}, self._complete_body())

        _, mock_client, _, _ = self._run(
            [], [session_resp, part_resp, complete_resp])
        calls = mock_client.make_request.call_args_list
        self.assertEqual(3, len(calls))

        # call 1: POST ?uploads to create session
        method, path, headers, statuses = calls[0][0]
        self.assertEqual('POST', method)
        self.assertIn(self.object, path)
        self.assertEqual({'uploads': ''}, calls[0][1].get('params'))

        # call 2: PUT with upload-id and part-number to upload part
        method, path, headers, statuses = calls[1][0]
        self.assertEqual('PUT', method)
        params = calls[1][1].get('params', {})
        self.assertEqual('UID', params.get('upload-id'))
        self.assertEqual('1', params.get('part-number'))
        body_file = calls[1][1].get('body_file')
        self.assertIsNotNone(body_file)
        body = body_file.read()
        self.assertEqual(mpu_tool.PART_SIZE, len(body))
        self.assertEqual(b'test' * (mpu_tool.PART_SIZE // 4), body)

        # call 3: POST ?upload-id to complete upload with JSON manifest
        method, path, headers, statuses = calls[2][0]
        self.assertEqual('POST', method)
        params = calls[2][1].get('params', {})
        self.assertEqual('UID', params.get('upload-id'))
        self.assertNotIn('part-number', params)
        manifest = json.loads(calls[2][1]['body_file'].read())
        self.assertEqual([{'part_number': 1, 'etag': 'e1'}], manifest)

    def test_make_creates_container_on_404_and_retries(self):
        session_404 = self._fake_resp(404)
        container_put = self._fake_resp(201)
        session_resp = self._fake_resp(202, {'X-Upload-Id': 'UID'})
        part_resp = self._fake_resp(201, {'Etag': '"e1"'})
        complete_resp = self._fake_resp(202, {}, self._complete_body())

        _, mock_client, out, rv = self._run(
            [], [session_404, container_put, session_resp,
                 part_resp, complete_resp])

        self.assertEqual(0, rv)
        self.assertIn('container not found', out)
        calls = mock_client.make_request.call_args_list
        # POST(404), PUT container, POST retry, PUT part, POST complete
        self.assertEqual(5, len(calls))
        method, path, _, _ = calls[0][0]
        self.assertEqual('POST', method)  # first attempt
        method, path, _, _ = calls[1][0]
        self.assertEqual('PUT', method)   # container creation
        self.assertNotIn(self.object, path)
        method, _, _, _ = calls[2][0]
        self.assertEqual('POST', method)  # retry

    def test_make_container_create_fails(self):
        session_404 = self._fake_resp(404)
        mock_client = mock.MagicMock()
        mock_client.make_request.side_effect = [
            session_404,
            UnexpectedResponse('403 Forbidden', None),
        ]
        with mock.patch.object(mpu_tool, '_make_internal_client',
                               return_value=mock_client):
            with self.assertRaises(SystemExit) as cm:
                mpu_tool.main(
                    ['make', self.account, self.container, self.object])
        self.assertIn('403', str(cm.exception))

    def test_make_session_create_fails(self):
        mock_client = mock.MagicMock()
        mock_client.make_request.side_effect = \
            UnexpectedResponse('503 Service Unavailable', None)
        with mock.patch.object(mpu_tool, '_make_internal_client',
                               return_value=mock_client):
            with self.assertRaises(SystemExit) as cm:
                mpu_tool.main(
                    ['make', self.account, self.container, self.object])
        self.assertIn('503', str(cm.exception))

    def test_make_no_upload_id_in_response(self):
        session_resp = self._fake_resp(202, {})
        mock_client = mock.MagicMock()
        mock_client.make_request.return_value = session_resp
        with mock.patch.object(mpu_tool, '_make_internal_client',
                               return_value=mock_client):
            with self.assertRaises(SystemExit) as cm:
                mpu_tool.main(
                    ['make', self.account, self.container, self.object])
        self.assertIn('X-Upload-Id', str(cm.exception))

    def test_make_part_upload_fails(self):
        session_resp = self._fake_resp(202, {'X-Upload-Id': 'UID'})
        mock_client = mock.MagicMock()
        mock_client.make_request.side_effect = [
            session_resp,
            UnexpectedResponse('503 Service Unavailable', None),
        ]
        with mock.patch.object(mpu_tool, '_make_internal_client',
                               return_value=mock_client):
            with self.assertRaises(SystemExit) as cm:
                mpu_tool.main(
                    ['make', self.account, self.container, self.object])
        self.assertIn('503', str(cm.exception))

    def test_make_complete_fails_in_body(self):
        session_resp = self._fake_resp(202, {'X-Upload-Id': 'UID'})
        part_resp = self._fake_resp(201, {'Etag': '"e1"'})
        error_body = (b' \r\n\r\n' + json.dumps({
            'Response Status': '400 Bad Request',
            'Response Body': 'Etag Mismatch',
        }).encode('ascii'))
        complete_resp = self._fake_resp(202, {}, error_body)
        mock_client = mock.MagicMock()
        mock_client.make_request.side_effect = [
            session_resp, part_resp, complete_resp]
        with mock.patch.object(mpu_tool, '_make_internal_client',
                               return_value=mock_client):
            with self.assertRaises(SystemExit) as cm:
                mpu_tool.main(
                    ['make', self.account, self.container, self.object])
        self.assertIn('400 Bad Request', str(cm.exception))
        self.assertIn('Etag Mismatch', str(cm.exception))

    def test_make_client_defaults(self):
        session_resp = self._fake_resp(202, {'X-Upload-Id': 'UID'})
        part_resp = self._fake_resp(201, {'Etag': '"e1"'})
        complete_resp = self._fake_resp(202, {}, self._complete_body())

        mock_make, _, _, _ = self._run(
            [], [session_resp, part_resp, complete_resp])
        mock_make.assert_called_once_with(
            '/etc/swift/internal-client.conf', 'Swift MPU Tool', 3)


if __name__ == '__main__':
    unittest.main()
