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
import os
import shutil
import unittest
import random
from tempfile import mkdtemp
import mock

from swift.common.internal_client import InternalClient
from swift.common.middleware.mpu import MPU_DELETED_MARKER_SUFFIX, \
    MPU_ABORTED_MARKER_SUFFIX, MPU_MARKER_CONTENT_TYPE
from swift.common.middleware.s3api.utils import unique_id
from swift.common.request_helpers import get_reserved_name
from swift.common.swob import Request, HTTPOk, HTTPNoContent, HTTPCreated, \
    HTTPNotFound
from swift.common.utils import md5
from swift.container.backend import ContainerBroker
from swift.container.mpu_auditor import MpuAuditor, extract_upload_prefix, \
    yield_item_batches
from swift.container.server import ContainerController
from test.debug_logger import debug_logger
from test.unit import make_timestamp_iter, EMPTY_ETAG
from test.unit.common.middleware.helpers import FakeSwift


class BaseTestMpuAuditor(unittest.TestCase):
    user_container = 'c'
    audit_container = get_reserved_name('mpu_manifests', user_container)

    def setUp(self):
        self.tempdir = mkdtemp()
        self.ts_iter = make_timestamp_iter()
        self.logger = debug_logger('sharder-test')
        self.obj_name = 'obj'
        hash_ = md5(self.audit_container.encode('utf-8'),
                    usedforsecurity=False).hexdigest()
        datadir = os.path.join(
            self.tempdir, 'sda1', 'containers', 'p', hash_[-3:], hash_)
        filename = hash_ + '.db'
        self.db_file = os.path.join(datadir, filename)
        self.broker = ContainerBroker(
            self.db_file, account='a', container=self.audit_container,
            logger=self.logger)
        self.broker.initialize(put_timestamp=float(next(self.ts_iter)))
        self.server = ContainerController({'mount_check': False,
                                           'devices': self.tempdir})

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def make_request(self, req):
        with mock.patch.object(self.server, '_get_container_broker',
                               return_value=self.broker):
            return req.get_response(self.server)

    def put_objects(self, objects, shuffle_order=True):
        # PUT object updates to container in random order
        if shuffle_order:
            # don't mutate the passed-in list!
            objects = random.sample(objects, k=len(objects))
        for obj in objects:
            headers = {'name': obj['name'],
                       'x-timestamp': obj['created_at'],
                       'x-content-type': obj['content_type'],
                       'x-etag': obj['etag'],
                       'x-size': obj['size']}
            req = Request.blank(
                '/sda1/p/a/%s/%s' % (self.audit_container, obj['name']),
                method='PUT', headers=headers)
            resp = self.make_request(req)
            self.assertEqual(201, resp.status_int, resp.body)
            # On py2 the broker has a nasty habit of re-ordering batches of
            # pending updates before merging; to make tests more deterministic,
            # use get_info() to flush the pending file after every PUT
            self.broker.get_info()

    def delete_objects(self, objects):
        ts = next(self.ts_iter)
        for obj in objects:
            headers = {'name': obj['name'],
                       'x-timestamp': ts.internal}
            req = Request.blank(
                '/sda1/p/a/%s/%s' % (self.audit_container, obj['name']),
                method='DELETE', headers=headers)
            resp = self.make_request(req)
            self.assertEqual(204, resp.status_int)

    def _create_manifest_spec(self):
        upload_id = unique_id()
        manifest_name = '/'.join([get_reserved_name(self.obj_name), upload_id])
        manifest_spec = {'name': manifest_name,
                         'created_at': next(self.ts_iter).normal,
                         'content_type': 'text/plain',
                         'etag': 'etag_1',
                         'size': 1024,
                         }
        return upload_id, manifest_spec

    def _create_marker_spec(self, upload, timestamp,
                            marker_type=MPU_DELETED_MARKER_SUFFIX):
        return {'name': '/'.join([upload, marker_type]),
                'created_at': timestamp.normal,
                'content_type': MPU_MARKER_CONTENT_TYPE,
                'etag': EMPTY_ETAG,
                'size': 0,
                }

    def _assert_context(self, exp):
        broker_id = self.broker.get_info()['id']
        ctxt_key = 'X-Container-Sysmeta-Mpu-Auditor-Context-' + broker_id
        self.assertIn(ctxt_key, self.broker.metadata)
        ctxt_value = json.loads(self.broker.metadata[ctxt_key][0])
        for k, v in exp.items():
            self.assertEqual(v, ctxt_value[k])

    def _check_broker_rows(self, expected_items):
        rows = self.broker.get_objects(include_deleted=False)
        self.assertEqual(sorted([o['name'] for o in expected_items]),
                         sorted([row['name'] for row in rows]))

    def _check_deleted_broker_rows(self, expected_items):
        rows = self.broker.get_objects(include_deleted=True)
        self.assertEqual(sorted([o['name'] for o in expected_items]),
                         sorted([row['name'] for row in rows]))


class TestModuleFunctions(BaseTestMpuAuditor):
    def test_extract_upload_prefix(self):
        self.assertEqual('obj/upload-id',
                         extract_upload_prefix('obj/upload-id'))
        self.assertEqual('obj/upload-id',
                         extract_upload_prefix('obj/upload-id/manifest'))
        self.assertEqual('obj/upload-id',
                         extract_upload_prefix('obj/upload-id/marker-deleted'))

    def test_yield_item_batches(self):
        items = [self._create_manifest_spec()[1] for i in range(123)]
        self.put_objects(items, shuffle_order=False)
        self._check_broker_rows(items)

        # stop at max batches
        actual = [b for b in yield_item_batches(self.broker, 0, 2, 50)]
        self.assertEqual(2, len(actual))
        self.assertEqual([it['name'] for it in items[:50]],
                         [a['name'] for a in actual[0]])
        self.assertEqual([it['name'] for it in items[50:100]],
                         [a['name'] for a in actual[1]])
        self.assertEqual(1, actual[0][0]['ROWID'])
        self.assertEqual(100, actual[-1][-1]['ROWID'])

        # stop at end
        actual = [b for b in yield_item_batches(self.broker, 0, 1000, 100)]
        self.assertEqual(2, len(actual))
        self.assertEqual([it['name'] for it in items[:100]],
                         [a['name'] for a in actual[0]])
        self.assertEqual([it['name'] for it in items[100:]],
                         [a['name'] for a in actual[1]])
        self.assertEqual(1, actual[0][0]['ROWID'])
        self.assertEqual(123, actual[-1][-1]['ROWID'])

        # start at start_row
        actual = [b for b in yield_item_batches(self.broker, 114, 100, 10)]
        self.assertEqual(1, len(actual))
        self.assertEqual([it['name'] for it in items[114:]],
                         [a['name'] for a in actual[0]])
        self.assertEqual(115, actual[0][0]['ROWID'])
        self.assertEqual(123, actual[-1][-1]['ROWID'])


class TestMpuAuditorMPU(BaseTestMpuAuditor):
    parts_container = get_reserved_name('mpu_parts', 'c')

    @contextlib.contextmanager
    def _mock_internal_client(self, registered_calls):
        fake_swift = FakeSwift()
        for call in registered_calls:
            fake_swift.register(*call)
        fake_ic = InternalClient(None, 'test-ic', 1, app=fake_swift)

        with mock.patch('swift.container.mpu_auditor.InternalClient',
                        return_value=fake_ic):
            yield fake_swift

    def test_audit_cycle_continues_past_error(self):
        upload_1, manifest_1 = self._create_manifest_spec()
        upload_2, manifest_2 = self._create_manifest_spec()
        calls = []

        def mock_audit_item(auditor, item):
            calls.append(item)
            if len(calls) == 1:
                raise Exception('boom')

        self.put_objects([manifest_1, manifest_2], shuffle_order=False)
        with self._mock_internal_client([]):
            auditor = MpuAuditor({}, debug_logger('test'))
        with mock.patch(
                'swift.container.mpu_auditor.BaseMpuBrokerAuditor._audit_item',
                mock_audit_item):
            auditor.audit(self.broker)
        self.assertEqual(2, len(calls))
        self.assertEqual([manifest_1['name'], manifest_2['name']],
                         [item.name for item in calls])
        warning_lines = auditor.logger.get_lines_for_level('warning')
        self.assertEqual(1, len(warning_lines))
        self.assertIn('boom', warning_lines[0])

    def test_audit_delete_marker(self):
        upload_1, manifest_1 = self._create_manifest_spec()
        upload_2, manifest_2 = self._create_manifest_spec()
        t_marker = next(self.ts_iter)
        marker = self._create_marker_spec(manifest_1['name'], t_marker)
        items = [manifest_1, manifest_2, marker]
        self.put_objects(items)
        rows = self.broker.get_objects(include_deleted=False)
        self.assertEqual(sorted([o['name'] for o in items]),
                         sorted([row['name'] for row in rows]))

        manifest_1_path = '/v1/a/%s/%s' % (self.audit_container,
                                           manifest_1['name'])
        manifest_marker_path = manifest_1_path + '/marker-deleted'
        part_path = '%s/%s/1' % (self.parts_container, manifest_1['name'])
        part_marker_path = '%s/%s/marker-deleted' % (self.parts_container,
                                                     manifest_1['name'])
        manifest_body = json.dumps([{'name': part_path}]).encode('ascii')

        registered_calls = [
            ('GET', manifest_1_path + '?multipart-manifest=get', HTTPOk, {},
             manifest_body),
            ('DELETE', '/v1/a/' + part_path, HTTPNoContent, {}),
            ('PUT', '/v1/a/' + part_marker_path, HTTPCreated, {}),
            ('DELETE', manifest_1_path, HTTPNoContent, {}),
            ('DELETE', manifest_marker_path, HTTPNoContent, {}),
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            auditor = MpuAuditor({}, debug_logger('test'))
        auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self._check_broker_rows(items)
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 3})

    def test_audit_abort_marker_linked(self):
        upload_id, manifest = self._create_manifest_spec()
        t_marker = next(self.ts_iter)
        marker = self._create_marker_spec(
            manifest['name'], t_marker,
            marker_type=MPU_ABORTED_MARKER_SUFFIX)
        items = [marker, manifest]
        self.put_objects(items)
        manifest_rel_path = '%s/%s' % (self.audit_container, manifest['name'])
        manifest_path = '/v1/a/%s' % manifest_rel_path
        manifest_marker_path = manifest_path + '/marker-aborted'

        registered_calls = [
            ('HEAD', '/v1/a/%s/%s' % (self.user_container, self.obj_name),
             HTTPOk, {'x-symlink-target': manifest_rel_path}),
            ('DELETE', manifest_marker_path, HTTPNoContent, {}),
        ]

        with self._mock_internal_client(registered_calls) as fake_swift:
            auditor = MpuAuditor({}, debug_logger('test'))
        auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self._check_broker_rows(items)
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 2})

    def test_audit_abort_marker_linked_manifest_delayed(self):
        upload_id, manifest = self._create_manifest_spec()
        t_marker = next(self.ts_iter)
        marker = self._create_marker_spec(
            manifest['name'], t_marker,
            marker_type=MPU_ABORTED_MARKER_SUFFIX)
        # manifest update not yet in DB
        self.put_objects([marker])
        manifest_rel_path = '%s/%s' % (self.audit_container, manifest['name'])
        manifest_path = '/v1/a/%s' % manifest_rel_path
        manifest_marker_path = manifest_path + '/marker-aborted'

        registered_calls = [
            ('HEAD', '/v1/a/%s/%s' % (self.user_container, self.obj_name),
             HTTPOk, {'x-symlink-target': manifest_rel_path}),
            ('DELETE', manifest_marker_path, HTTPNoContent, {}),
        ]

        with self._mock_internal_client(registered_calls) as fake_swift:
            auditor = MpuAuditor({}, debug_logger('test'))
        auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self._check_broker_rows([marker])
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 1})

        # insert manifest PUT update and marker DELETE update
        self.delete_objects([marker])
        self.put_objects([manifest])
        fake_swift.clear_calls()
        auditor.audit(self.broker)

        # marker already deleted so just a HEAD to check user object
        expected_calls = [call[:2] for call in registered_calls[:1]]
        self.assertEqual(expected_calls, fake_swift.calls)
        self._check_broker_rows([manifest])
        self._check_deleted_broker_rows([marker])
        self._assert_context({'last_audit_row': 3})

    def _do_test_audit_abort_marker_not_linked(
            self, manifest_spec, resp_type, headers):
        t_marker = next(self.ts_iter)
        marker = self._create_marker_spec(
            manifest_spec['name'], t_marker,
            marker_type=MPU_ABORTED_MARKER_SUFFIX)
        # manifest was not PUT
        items = [marker]
        self.put_objects(items)
        manifest_1_path = '/v1/a/%s/%s' % (self.audit_container,
                                           manifest_spec['name'])
        manifest_marker_path = manifest_1_path + '/marker-aborted'
        part_marker_path = '%s/%s/marker-deleted' % (self.parts_container,
                                                     manifest_spec['name'])

        registered_calls = [
            ('HEAD', '/v1/a/%s/%s' % (self.user_container, self.obj_name),
             resp_type, headers),
            ('GET', manifest_1_path + '?multipart-manifest=get', HTTPNotFound,
             {}),
            ('PUT', '/v1/a/' + part_marker_path, HTTPCreated, {}),
            ('DELETE', manifest_1_path, HTTPNotFound, {}),
            ('DELETE', manifest_marker_path, HTTPNoContent, {}),
        ]

        with self._mock_internal_client(registered_calls) as fake_swift:
            auditor = MpuAuditor({}, debug_logger('test'))
        auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self._check_broker_rows(items)
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 1})

    def test_audit_abort_marker_obj_not_found(self):
        upload_id, manifest_spec = self._create_manifest_spec()
        self._do_test_audit_abort_marker_not_linked(manifest_spec,
                                                    HTTPNotFound, {})

    def test_audit_abort_marker_obj_exists_not_symlink(self):
        upload_id, manifest_spec = self._create_manifest_spec()
        self._do_test_audit_abort_marker_not_linked(manifest_spec, HTTPOk, {})

    def test_audit_abort_marker_obj_exists_symlink_not_linked(self):
        upload_id, manifest_spec = self._create_manifest_spec()
        rel_manifest_path = '%s/%s' % (self.audit_container, 'other_manifest')
        headers = {'x-symlink-target': rel_manifest_path}
        self._do_test_audit_abort_marker_not_linked(manifest_spec, HTTPOk,
                                                    headers)


class TestMpuAuditorSLO(BaseTestMpuAuditor):
    user_container = 'c'
    audit_container = user_container + '+segments'

    def _create_upload_parts(self, num_parts):
        upload_id = unique_id()
        upload = 'obj/' + upload_id
        parts = [{'name': '%s/%d' % (upload, i),
                  'created_at': next(self.ts_iter).normal,
                  'content_type': 'text/plain',
                  'etag': 'etag_1',
                  'size': 1024,
                  } for i in range(num_parts)]
        return upload, parts

    def test(self):
        upload_1, parts_1 = self._create_upload_parts(3)
        upload_2, parts_2 = self._create_upload_parts(2)
        t_marker = next(self.ts_iter)
        marker = self._create_marker_spec(upload_1, t_marker)
        # NB: not all parts are inserted into DB yet
        items = parts_1[:2] + parts_2 + [marker]
        self.put_objects(items)
        rows = self.broker.get_objects(include_deleted=False)
        self.assertEqual(sorted([o['name'] for o in items]),
                         sorted([o['name'] for o in rows]))

        part_paths = ['/v1/a/%s/%s' % (self.audit_container, part['name'])
                      for part in parts_1]
        registered_calls = [
            ('DELETE', part_path, HTTPNoContent, {})
            for part_path in part_paths]
        marker_path = '/v1/a/%s/%s' % (self.audit_container, marker['name'])
        registered_calls.append(('DELETE', marker_path, HTTPNoContent, {}))
        fake_swift = FakeSwift()
        for call in registered_calls:
            fake_swift.register(*call)
        fake_ic = InternalClient(None, 'test-ic', 1, app=fake_swift)

        with mock.patch('swift.container.mpu_auditor.InternalClient',
                        return_value=fake_ic):
            auditor = MpuAuditor({}, debug_logger('test'))
        auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls
                          if call[1] != part_paths[2]]
        self.assertEqual(expected_calls, fake_swift.calls)

        rows = self.broker.get_objects(include_deleted=False)
        self.assertEqual(sorted([o['name'] for o in items]),
                         sorted([row['name'] for row in rows]))
        rows = self.broker.get_objects(include_deleted=True)
        self.assertEqual([], rows)
        self._assert_context({'last_audit_row': 5})

        # remove deleted object rows and add another orphaned upload part to
        # the DB...
        self.delete_objects(parts_1[:2] + [marker])
        self.put_objects(parts_1[2:])
        rows = self.broker.get_objects(include_deleted=False)
        self.assertEqual(sorted([o['name'] for o in parts_1[2:] + parts_2]),
                         sorted([row['name'] for row in rows]))

        fake_swift.clear_calls()
        with mock.patch('swift.container.mpu_auditor.InternalClient',
                        return_value=fake_ic):
            auditor = MpuAuditor({}, debug_logger('test'))
        auditor.audit(self.broker)

        expected_calls = [registered_calls[2][:2]]
        self.assertEqual(expected_calls, fake_swift.calls)
        rows = self.broker.get_objects(include_deleted=False)
        self.assertEqual(sorted([o['name'] for o in parts_1[2:] + parts_2]),
                         sorted([row['name'] for row in rows]))
        # 3 rows deleted, 1 row added
        self._assert_context({'last_audit_row': 9})
