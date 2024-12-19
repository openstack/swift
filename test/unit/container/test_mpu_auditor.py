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
import time
import unittest
import random
from tempfile import mkdtemp
import mock

from swift.common.internal_client import InternalClient
from swift.common.middleware.mpu import MPU_DELETED_MARKER_SUFFIX, \
    MPU_MARKER_CONTENT_TYPE, MPUId, \
    MPU_SESSION_CREATED_CONTENT_TYPE, MPU_SESSION_COMPLETED_CONTENT_TYPE, \
    MPU_SESSION_ABORTED_CONTENT_TYPE, MPU_SESSION_COMPLETING_CONTENT_TYPE
from swift.common.request_helpers import get_reserved_name
from swift.common.swob import Request, HTTPOk, HTTPNoContent, \
    HTTPNotFound, HTTPAccepted, HTTPServerError
from swift.common.utils import md5, Timestamp, decode_timestamps, \
    encode_timestamps, MD5_OF_EMPTY_STRING
from swift.container.backend import ContainerBroker
from swift.container.mpu_auditor import MpuAuditor, \
    extract_upload_prefix, yield_item_batches, BaseMpuAuditor, \
    MpuAuditorConfig
from swift.container.server import ContainerController
from test.debug_logger import debug_logger
from test.unit import make_timestamp_iter
from test.unit.common.middleware.helpers import FakeSwift


class BaseTestMpuAuditor(unittest.TestCase):
    user_container = 'c'
    audit_container = 'test-container'

    def setUp(self):
        self.tempdir = mkdtemp()
        self.ts_iter = make_timestamp_iter()
        self.logger = debug_logger('mpu-auditor-test')
        self.fake_statsd_client = self.logger.logger.statsd_client
        self.account = 'a'
        self.obj_name = 'obj'
        self.obj_path = '/v1/%s/%s/%s' % (self.account,
                                          self.user_container,
                                          self.obj_name)
        self.audit_container_path = '/v1/%s/%s' % (self.account,
                                                   self.audit_container)
        self.mpu_id = self._make_mpu_id(self.obj_path)
        hash_ = md5(self.audit_container.encode('utf-8'),
                    usedforsecurity=False).hexdigest()
        datadir = os.path.join(
            self.tempdir, 'sda1', 'containers', 'p', hash_[-3:], hash_)
        filename = hash_ + '.db'
        self.db_file = os.path.join(datadir, filename)
        self.broker = ContainerBroker(
            self.db_file,
            account=self.account,
            container=self.audit_container,
            logger=self.logger)
        self.broker.initialize(put_timestamp=float(next(self.ts_iter)))
        self.server = ContainerController({'mount_check': False,
                                           'devices': self.tempdir})
        self.upload_name = self.session_name = '/'.join(
            [get_reserved_name(self.obj_name),
             str(self.mpu_id)])

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def _make_mpu_id(self, path, ts=None):
        return MPUId.create(path, ts or next(self.ts_iter))

    @contextlib.contextmanager
    def _mock_internal_client(self, registered_calls=None):
        fake_swift = FakeSwift()
        for call in registered_calls or []:
            fake_swift.register(*call)
        fake_ic = InternalClient(None, 'test-ic', 1, app=fake_swift)

        with mock.patch('swift.container.mpu_auditor.InternalClient',
                        return_value=fake_ic):
            yield fake_swift

    def make_request(self, req):
        with mock.patch.object(self.server, '_get_container_broker',
                               return_value=self.broker):
            return req.get_response(self.server)

    def put_object(self, obj):
        ts_data, ts_ctype, ts_meta = decode_timestamps(obj['created_at'])
        headers = {
            'x-timestamp': ts_data.internal,
            'x-content-type-timestamp': ts_ctype.internal,
            'x-meta-timestamp': ts_meta.internal,
            'x-content-type': obj['content_type'],
            'x-etag': obj['etag'],
            'x-size': obj['size']
        }
        req = Request.blank(
            '/sda1/p/a/%s/%s' % (self.audit_container, obj['name']),
            method='PUT', headers=headers)
        resp = self.make_request(req)
        self.assertEqual(201, resp.status_int, resp.body)
        # On py2 the broker has a nasty habit of re-ordering batches of
        # pending updates before merging; to make tests more deterministic,
        # use get_info() to flush the pending file after every PUT
        self.broker.get_info()

    def put_objects(self, objects, shuffle_order=True):
        # PUT object updates to container in random order
        if shuffle_order:
            # don't mutate the passed-in list!
            objects = random.sample(objects, k=len(objects))
        for obj in objects:
            self.put_object(obj)

    def delete_object(self, obj):
        ts = next(self.ts_iter)
        headers = {'name': obj['name'],
                   'x-timestamp': ts.internal}
        req = Request.blank(
            '/sda1/p/a/%s/%s' % (self.audit_container, obj['name']),
            method='DELETE', headers=headers)
        resp = self.make_request(req)
        self.assertEqual(204, resp.status_int)

    def delete_objects(self, objects):
        for obj in objects:
            self.delete_object(obj)

    def _create_item(self, name, ts_data, ts_ctype=None, ts_meta=None,
                     ctype='text/plain', size=0, etag=''):
        created_at = encode_timestamps(ts_data, ts_ctype, ts_meta)
        item = {
            'name': str(name),
            'created_at': created_at,
            'content_type': ctype,
            'etag': etag,
            'size': size,
            'deleted': 0,
            'storage_policy_index': 0,
        }
        return item

    def _create_marker_spec(self, upload, ts_data=None):
        ts_data = ts_data or next(self.ts_iter)
        marker_name = '/'.join([upload, MPU_DELETED_MARKER_SUFFIX])
        return self._create_item(marker_name,
                                 ts_data,
                                 ctype=MPU_MARKER_CONTENT_TYPE,
                                 etag=MD5_OF_EMPTY_STRING)

    def _create_session(
            self, ctype, ts_data=None, ts_ctype=None, ts_meta=None):
        ts_data = ts_data or self.ts_data
        return self._create_item(
            self.session_name, ts_data=ts_data, ts_ctype=ts_ctype,
            ts_meta=ts_meta, ctype=ctype)

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
        return rows

    def _check_deleted_broker_rows(self, expected_items):
        rows = self.broker.get_objects(include_deleted=True)
        self.assertEqual(sorted([o['name'] for o in expected_items]),
                         sorted([row['name'] for row in rows]))
        return rows


class TestModuleFunctions(BaseTestMpuAuditor):
    def test_extract_upload_prefix(self):
        prefix = 'obj/' + str(self.mpu_id)
        self.assertEqual(prefix,
                         extract_upload_prefix(prefix))
        self.assertEqual(prefix,
                         extract_upload_prefix(prefix + '/000001'))
        self.assertEqual(prefix,
                         extract_upload_prefix(prefix + '/marker-deleted'))

    def _setup_for_yield_item_batches(self):
        items = [self._create_item(i, next(self.ts_iter))
                 for i in range(123)]

        self.put_objects(items, shuffle_order=False)
        self._check_broker_rows(items)
        return items

    def test_yield_item_batches_stops_at_max_batches(self):
        items = self._setup_for_yield_item_batches()
        actual = [b for b in yield_item_batches(self.broker, 0, 2, 50)]
        self.assertEqual(2, len(actual))
        self.assertEqual([it['name'] for it in items[:50]],
                         [a['name'] for a in actual[0]])
        self.assertEqual([it['name'] for it in items[50:100]],
                         [a['name'] for a in actual[1]])
        self.assertEqual(1, actual[0][0]['ROWID'])
        self.assertEqual(100, actual[-1][-1]['ROWID'])

    def test_yield_item_batches_stops_at_end_of_items(self):
        items = self._setup_for_yield_item_batches()
        actual = [b for b in yield_item_batches(self.broker, 0, 1000, 100)]
        self.assertEqual(2, len(actual))
        self.assertEqual([it['name'] for it in items[:100]],
                         [a['name'] for a in actual[0]])
        self.assertEqual([it['name'] for it in items[100:]],
                         [a['name'] for a in actual[1]])
        self.assertEqual(1, actual[0][0]['ROWID'])
        self.assertEqual(123, actual[-1][-1]['ROWID'])

    def test_yield_item_batches_starts_at_start_row(self):
        items = self._setup_for_yield_item_batches()
        actual = [b for b in yield_item_batches(self.broker, 114, 100, 10)]
        self.assertEqual(1, len(actual))
        self.assertEqual([it['name'] for it in items[114:]],
                         [a['name'] for a in actual[0]])
        self.assertEqual(115, actual[0][0]['ROWID'])
        self.assertEqual(123, actual[-1][-1]['ROWID'])


class TestBaseMpuBrokerAuditor(BaseTestMpuAuditor):
    # test abstract auditor behavior not specific to the type of resource
    # being audited
    def test_audit_stats(self):
        items = [self._create_item('obj/%s' % i, next(self.ts_iter))
                 for i in range(2)]
        self.put_objects(items)
        marker = dict(items[0],
                      created_at=next(self.ts_iter).internal,
                      name=items[0]['name'] + '/marker-deleted',
                      content_type='application/x-mpu-marker')
        self.put_objects([marker])
        self.assertEqual(3, self.broker.get_max_row())  # sanity check
        calls = []

        def mock_audit_item(auditor, item, upload):
            calls.append(item)
            if item.name == items[1]['name']:
                raise ValueError('kaboom')
            return False

        fake_ic = InternalClient(None, 'test-ic', 1, app=FakeSwift())
        auditor = BaseMpuAuditor(
            MpuAuditorConfig({}), fake_ic, self.logger, self.broker)
        with mock.patch(
                'swift.container.mpu_auditor.BaseMpuAuditor._audit_item',
                mock_audit_item):
            auditor.audit()
        exp_stats = {'processed': 3, 'audited': 1, 'skipped': 1, 'errors': 1}
        log_lines = auditor.logger.get_lines_for_level('info')
        self.assertEqual(1, len(log_lines), log_lines)
        self.assertIn('processed=3, audited=1, skipped=1, errors=1',
                      log_lines[0])
        exp_metrics = dict(('base.%s' % k, v) for k, v in exp_stats.items())
        self.assertEqual(exp_metrics,
                         self.fake_statsd_client.get_increment_counts())

    def test_audit_stops_at_max_row_other_items_merged(self):
        items = [self._create_item(i, next(self.ts_iter))
                 for i in range(10)]
        other_items = [self._create_item(i + 10, next(self.ts_iter))
                       for i in range(10)]
        self.put_objects(items)
        self.assertEqual(10, self.broker.get_max_row())  # sanity check

        calls = []

        def mock_audit_item(auditor, item, upload):
            # merge another item into the db
            self.put_objects([other_items[len(calls)]])
            calls.append(item)
            return False

        fake_ic = InternalClient(None, 'test-ic', 1, app=FakeSwift())
        auditor = BaseMpuAuditor(
            MpuAuditorConfig({}), fake_ic, self.logger, self.broker)
        with mock.patch(
                'swift.container.mpu_auditor.BaseMpuAuditor._audit_item',
                mock_audit_item):
            auditor.audit()

        self.assertEqual(10, len(calls))
        self._check_broker_rows(items + other_items)
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 10})
        self.assertEqual(20, self.broker.get_max_row())

    def test_audit_stops_at_max_row_audited_items_deferred(self):
        old_ts_iter = make_timestamp_iter(-10000)
        items = [self._create_item(i, next(old_ts_iter))
                 for i in range(10)]
        self.put_objects(items)
        self.assertEqual(10, self.broker.get_max_row())  # sanity check
        calls = []

        def mock_audit_item(auditor, item, upload):
            # defer item to be revisited
            auditor._bump_item(item)
            calls.append(item)
            return True

        fake_ic = InternalClient(None, 'test-ic', 1, app=FakeSwift())
        auditor = BaseMpuAuditor(
            MpuAuditorConfig({}), fake_ic, self.logger, self.broker)
        with mock.patch(
                'swift.container.mpu_auditor.BaseMpuAuditor._audit_item',
                mock_audit_item):
            auditor.audit()

        self.assertEqual(10, len(calls))
        rows = self._check_broker_rows(items)
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 10})
        self.assertEqual(20, self.broker.get_max_row())
        name_to_item = dict((item['name'], item) for item in items)
        for row in rows:
            ts_data, ts_ctype, ts_meta = decode_timestamps(row['created_at'])
            exp = name_to_item[row['name']]
            self.assertEqual(exp['created_at'], ts_data)
            self.assertEqual(exp['created_at'], ts_ctype)
            self.assertLess(exp['created_at'], ts_meta)

    def test_deferred_item_content_type_modified(self):
        # verify that when the auditor defers an item by moving it to a new row
        # it does not prevent a subsequent update of the item's content_type
        # using an earlier content_type_timestamp.
        old_ts_iter = make_timestamp_iter(-10000)
        item = self._create_item('x', next(old_ts_iter))
        self.put_objects([item])
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        calls = []

        def mock_audit_item(auditor, item, upload):
            # defer item to be revisited
            auditor._bump_item(item)
            calls.append(item)
            return True

        fake_ic = InternalClient(None, 'test-ic', 1, app=FakeSwift())
        auditor = BaseMpuAuditor(
            MpuAuditorConfig({}), fake_ic, self.logger, self.broker)
        with mock.patch(
                'swift.container.mpu_auditor.BaseMpuAuditor._audit_item',
                mock_audit_item):
            auditor.audit()

        self.assertEqual(1, len(calls))
        rows = self._check_broker_rows([item])
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 1})
        self.assertEqual(2, self.broker.get_max_row())
        row_id_2 = rows[0]
        ts_data, ts_ctype, bumped_ts_meta = decode_timestamps(
            row_id_2['created_at'])
        self.assertEqual(item['created_at'], ts_data)
        self.assertEqual(item['created_at'], ts_ctype)
        self.assertLess(item['created_at'], bumped_ts_meta)
        self.assertEqual('text/plain', row_id_2['content_type'])

        # after the auditor has deferred the item, simulate an object update
        # modifying the content_type with a timestamp *older* than the
        # deferred item's meta_timestamp...
        ts_ctype_modified = next(old_ts_iter)
        self.assertLess(float(ts_ctype_modified), bumped_ts_meta)
        updated_ts = encode_timestamps(Timestamp(item['created_at']),
                                       ts_ctype_modified,
                                       ts_ctype_modified)
        updated_item = dict(item,
                            created_at=updated_ts,
                            content_type='modified')
        self.put_objects([updated_item])
        rows = self._check_broker_rows([item])
        self.assertEqual(3, self.broker.get_max_row())
        row_id_3 = rows[0]
        ts_data, ts_ctype, ts_meta = decode_timestamps(row_id_3['created_at'])
        self.assertEqual(item['created_at'], ts_data)
        self.assertEqual(ts_ctype_modified, ts_ctype)
        self.assertEqual(ts_meta, bumped_ts_meta)
        self.assertEqual('modified', row_id_3['content_type'])

    def test_audit_cycle_continues_past_error(self):
        item1 = self._create_item(1, next(self.ts_iter))
        item2 = self._create_item(2, next(self.ts_iter))
        calls = []

        def mock_audit_item(auditor, item, upload):
            calls.append(item)
            if len(calls) == 1:
                raise Exception('boom')
            return False

        self.put_objects([item1, item2], shuffle_order=False)
        self.assertEqual(2, self.broker.get_max_row())  # sanity check
        fake_ic = InternalClient(None, 'test-ic', 1, app=FakeSwift())
        auditor = BaseMpuAuditor(
            MpuAuditorConfig({}), fake_ic, self.logger, self.broker)
        with mock.patch(
                'swift.container.mpu_auditor.BaseMpuAuditor._audit_item',
                mock_audit_item):
            auditor.audit()
        self.assertEqual(2, len(calls))
        self.assertEqual([item1['name'], item2['name']],
                         [item.name for item in calls])
        log_lines = auditor.logger.get_lines_for_level('error')
        self.assertEqual(1, len(log_lines))
        self.assertIn('Error while auditing ', log_lines[0])
        self.assertIn('boom', log_lines[0])
        self._assert_context({'last_audit_row': 2})
        self.assertEqual(2, self.broker.get_max_row())


class TestMpuAuditorParts(BaseTestMpuAuditor):
    audit_container = get_reserved_name(
        'mpu_parts', BaseTestMpuAuditor.user_container)

    def setUp(self):
        super(TestMpuAuditorParts, self).setUp()
        self.part_prefix_path = '/v1/a/%s/%s' % (
            self.audit_container, self.upload_name)
        self.part_marker_path = self.part_prefix_path + '/marker-deleted'
        self.parts = [self._create_part_spec(i) for i in range(1, 6)]
        self.ts_marker = next(self.ts_iter)
        self.marker = self._create_marker_spec(
            self.upload_name, ts_data=self.ts_marker)

    def _create_part_spec(self, part_number, ts_data=None):
        ts_data = ts_data or next(self.ts_iter)
        part_name = '/'.join([self.upload_name, '%06d' % part_number])
        return self._create_item(part_name,
                                 ts_data,
                                 ctype='application/octet-stream',
                                 size=123456)

    def test_audit_delete_marker(self):
        items = [self.marker] + self.parts
        self.put_objects(items, shuffle_order=True)
        registered_calls = [
            ('DELETE',
             '/'.join([self.audit_container_path, part['name']]),
             HTTPNoContent,
             {})
            for part in self.parts
        ]
        registered_calls.append(
            ('DELETE', self.part_marker_path, HTTPNoContent, {}))
        with self._mock_internal_client(registered_calls) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(next(self.ts_iter))):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self._check_broker_rows(items)
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 6})

    def test_audit_delete_marker_previously_deleted(self):
        marker = dict(self.marker, deleted=1)
        self.delete_object(marker)
        self.put_objects(self.parts, shuffle_order=True)
        registered_calls = [
            ('DELETE',
             '/'.join([self.audit_container_path, part['name']]),
             HTTPNoContent,
             {})
            for part in self.parts
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(next(self.ts_iter))):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self._check_broker_rows(self.parts)
        self._check_deleted_broker_rows([marker])
        self._assert_context({'last_audit_row': 6})

    def test_audit_delete_marker_some_parts_previously_deleted(self):
        self.delete_objects(self.parts[:3])
        self.put_objects([self.marker] + self.parts[3:])
        self._check_deleted_broker_rows(self.parts[:3])  # sanity check
        registered_calls = [
            ('DELETE',
             '/'.join([self.audit_container_path, part['name']]),
             HTTPNoContent,
             {})
            for part in self.parts[3:]
        ]
        registered_calls.append(
            ('DELETE', self.part_marker_path, HTTPNoContent, {}))
        with self._mock_internal_client(registered_calls) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(next(self.ts_iter))):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self._check_broker_rows([self.marker] + self.parts[3:])
        self._check_deleted_broker_rows(self.parts[:3])
        self._assert_context({'last_audit_row': 6})

    def test_audit_delete_marker_parts_not_found(self):
        items = [self.marker] + self.parts
        self.put_objects(items, shuffle_order=True)
        registered_calls = [
            ('DELETE',
             '/'.join([self.audit_container_path, part['name']]),
             HTTPNotFound,
             {})
            for part in self.parts
        ]
        registered_calls.append(
            ('DELETE', self.part_marker_path, HTTPNoContent, {}))
        with self._mock_internal_client(registered_calls) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(next(self.ts_iter))):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self._check_broker_rows(items)
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 6})

    def test_audit_delete_marker_some_part_deletes_fail(self):
        items = [self.marker] + self.parts
        self.put_objects(items, shuffle_order=True)
        self.assertEqual(6, self.broker.get_max_row())  # sanity check
        registered_calls = [
            ('DELETE',
             '/'.join([self.audit_container_path, part['name']]),
             HTTPNoContent,
             {})
            for part in self.parts[:2]
        ]
        registered_calls += [
            ('DELETE',
             '/'.join([self.audit_container_path, part['name']]),
             HTTPServerError,
             {})
            for part in self.parts[2:]
        ]
        ts_now = next(self.ts_iter)
        with self._mock_internal_client(registered_calls) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(ts_now)):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self._assert_context({'last_audit_row': 6})
        self.assertEqual(7, self.broker.get_max_row())  # sanity check
        rows = self._check_broker_rows(items)
        self._check_deleted_broker_rows([])
        # check that marker was bumped to a new row...
        exp_marker_created_at = encode_timestamps(
            self.ts_marker, self.ts_marker, ts_now)
        exp_marker = dict(self.marker, created_at=exp_marker_created_at)
        self.assertEqual(exp_marker, rows[-1])

    def test_audit_delete_marker_part_update_delayed(self):
        # delete marker row is in db but part row is not yet in db;
        # initially only the marker row is in DB
        self.put_objects([self.marker])
        self.assertEqual(1, self.broker.get_max_row())  # sanity check

        registered_calls = [
            ('DELETE', self.part_marker_path, HTTPNoContent, {}),
        ]

        with self._mock_internal_client(registered_calls) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(next(self.ts_iter))):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)
        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self._check_broker_rows([self.marker])
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 1})
        self.assertEqual(1, self.broker.get_max_row())

        # insert parts PUT update and marker DELETE update into container;
        self.delete_objects([self.marker])
        self.put_objects(self.parts[:1])
        self.assertEqual(3, self.broker.get_max_row())  # sanity check
        registered_calls = [
            ('DELETE', '%s/%s' % (self.part_prefix_path, '000001'),
             HTTPNotFound, {}),
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(next(self.ts_iter))):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)
        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self._check_broker_rows(self.parts[:1])
        self._check_deleted_broker_rows([self.marker])
        self._assert_context({'last_audit_row': 3})
        self.assertEqual(3, self.broker.get_max_row())


class TestMpuAuditorSessions(BaseTestMpuAuditor):
    audit_container = get_reserved_name(
        'mpu_sessions', BaseTestMpuAuditor.user_container)

    def setUp(self):
        super(TestMpuAuditorSessions, self).setUp()
        self.ts_data = Timestamp(time.time())
        self.session_path = '/'.join(
            [self.audit_container_path,
             self.session_name])
        self.part_marker_path = '/'.join([
            '', 'v1', self.account,
            get_reserved_name('mpu_parts', self.user_container),
            self.session_name, MPU_DELETED_MARKER_SUFFIX])

    def test_audit_in_progress_session(self):
        # verify that an in progress session is passed over
        session = self._create_session(MPU_SESSION_CREATED_CONTENT_TYPE)
        items = [session]
        self.put_objects(items)
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        with self._mock_internal_client():
            auditor = MpuAuditor({}, self.logger)
            auditor.audit(self.broker)

        self.assertEqual(1, self.broker.get_max_row())  # no new row
        rows = self._check_broker_rows(items)
        self.assertEqual(self.ts_data, rows[0]['created_at'])
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 1})

    def test_audit_completing_session_is_recent(self):
        # verify that a recent completing session is deferred for revisit; the
        # ctype_timestamp is the significant time
        ts_now = Timestamp.now()
        ts_data = Timestamp(float(ts_now) - 3699)
        ts_ctype = Timestamp(float(ts_now) - 123)  # recent
        ts_meta = Timestamp(float(ts_now) - 4)
        session = self._create_session(
            MPU_SESSION_COMPLETING_CONTENT_TYPE, ts_data=ts_data,
            ts_ctype=ts_ctype, ts_meta=ts_meta)
        items = [session]
        self.put_objects(items)
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        registered_calls = [
            ('HEAD', self.obj_path, HTTPOk, {}),
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(ts_now)):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self.assertEqual(2, self.broker.get_max_row())  # new row
        rows = self._check_broker_rows(items)
        # update meta_timestamp
        self.assertEqual((ts_data, ts_ctype, ts_now),
                         decode_timestamps(rows[0]['created_at']))
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 1})

    def test_audit_completing_session_is_old(self):
        # verify that an old completing session is passed over; the
        # ctype_timestamp is the significant time
        ts_now = Timestamp.now()
        ts_data = Timestamp(float(ts_now) - 3699)
        ts_ctype = Timestamp(float(ts_now) - 3601)  # old
        ts_meta = ts_now
        session = self._create_session(
            MPU_SESSION_COMPLETING_CONTENT_TYPE, ts_data=ts_data,
            ts_ctype=ts_ctype, ts_meta=ts_meta)
        items = [session]
        self.put_objects(items)
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        user_obj_resp_hdrs = {}
        registered_calls = [
            ('HEAD', self.obj_path, HTTPOk, user_obj_resp_hdrs),
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(ts_now)):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self.assertEqual(1, self.broker.get_max_row())  # no new rows
        rows = self._check_broker_rows(items)
        # no change to session timestamps
        self.assertEqual((ts_data, ts_ctype, ts_meta),
                         decode_timestamps(rows[0]['created_at']))
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 1})

    def test_audit_completing_session_is_old_error_checking_user_obj(self):
        # verify that an old completing session is not passed over if the
        # user namespace check failed
        ts_now = Timestamp.now()
        ts_data = Timestamp(float(ts_now) - 3699)
        session = self._create_session(MPU_SESSION_COMPLETING_CONTENT_TYPE,
                                       ts_data=ts_data)
        items = [session]
        self.put_objects(items)
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        registered_calls = [
            ('HEAD', self.obj_path, HTTPServerError, {}),
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(ts_now)):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self.assertEqual(2, self.broker.get_max_row())
        rows = self._check_broker_rows(items)
        self.assertNotEqual(ts_data.internal, rows[0]['created_at'])
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 1})

    def test_audit_completing_session_and_user_obj_is_mpu(self):
        # verify that a completing session that is found to be completed is
        # removed
        session = self._create_session(MPU_SESSION_COMPLETING_CONTENT_TYPE)
        items = [session]
        self.put_objects(items)
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        user_obj_resp_hdrs = {'x-object-sysmeta-mpu-upload-id': self.mpu_id}
        registered_calls = [
            ('HEAD', self.obj_path, HTTPOk, user_obj_resp_hdrs),
            ('DELETE', self.session_path, HTTPAccepted, {}),
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            auditor = MpuAuditor({}, self.logger)
        auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self.assertEqual(1, self.broker.get_max_row())  # no new rows
        self._check_broker_rows(items)
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 1})

    def test_audit_completing_session_is_completed_but_delete_fails(self):
        # verify that a completing session that is found to be completed
        # is bumped if session delete fails
        ts_data = next(self.ts_iter)
        session = self._create_session(MPU_SESSION_COMPLETING_CONTENT_TYPE,
                                       ts_data=ts_data)
        items = [session]
        self.put_objects(items)
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        user_obj_resp_hdrs = {'x-object-sysmeta-mpu-upload-id': self.mpu_id}
        registered_calls = [
            ('HEAD', self.obj_path, HTTPOk, user_obj_resp_hdrs),
            ('DELETE', self.session_path, HTTPServerError, {}),
        ]
        ts_now = next(self.ts_iter)
        with self._mock_internal_client(registered_calls) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(ts_now)):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self.assertEqual(2, self.broker.get_max_row())
        rows = self._check_broker_rows(items)
        # both ctype_timestamp and meta_timestamp have been bumped
        exp_session_created_at = encode_timestamps(ts_data, ts_now, ts_now)
        exp_session = dict(session, created_at=exp_session_created_at,
                           content_type=MPU_SESSION_COMPLETED_CONTENT_TYPE)
        self.assertEqual(exp_session, rows[-1])
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 1})

    def test_audit_completed_session(self):
        # verify that a completed session is removed
        session = self._create_session(MPU_SESSION_COMPLETED_CONTENT_TYPE)
        items = [session]
        self.put_objects(items)
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        registered_calls = [
            ('DELETE', self.session_path, HTTPNoContent, {})
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            auditor = MpuAuditor({}, self.logger)
        auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self.assertEqual(1, self.broker.get_max_row())  # no new rows
        self._check_broker_rows(items)
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 1})

    def test_audit_aborted_session_and_user_obj_is_mpu(self):
        # verify that an aborted session is cleaned up if mpu exists
        session = self._create_session(MPU_SESSION_ABORTED_CONTENT_TYPE)
        items = [session]
        self.put_objects(items)
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        user_obj_resp_hdrs = {'x-object-sysmeta-mpu-upload-id': self.mpu_id}
        registered_calls = [
            ('HEAD', self.obj_path, HTTPOk, user_obj_resp_hdrs),
            ('DELETE', self.session_path, HTTPAccepted, {})
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            auditor = MpuAuditor({}, self.logger)
        auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self.assertEqual(1, self.broker.get_max_row())  # no new rows
        self._check_broker_rows(items)
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 1})

    def test_audit_aborted_session_is_not_yet_purged(self):
        # verify that an aborted session younger than purge_delay is deferred
        ts_now = Timestamp.now()
        ts_data = Timestamp(float(ts_now) - 3699)
        session = self._create_session(MPU_SESSION_ABORTED_CONTENT_TYPE,
                                       ts_data=ts_data)
        items = [session]
        self.put_objects(items)
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        registered_calls = [
            ('HEAD', self.obj_path, HTTPOk, {}),
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=float(ts_now)):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self.assertEqual(2, self.broker.get_max_row())  # new row
        rows = self._check_broker_rows(items)
        self.assertEqual(
            encode_timestamps(ts_data, ts_data, ts_now),
            rows[0]['created_at'])
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 1})

    def test_audit_aborted_session_error_checking_user_obj(self):
        session = self._create_session(MPU_SESSION_ABORTED_CONTENT_TYPE)
        items = [session]
        self.put_objects(items)
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        registered_calls = [
            ('HEAD', self.obj_path, HTTPServerError, {}),  # error!
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            auditor = MpuAuditor({'mpu_aborted_purge_delay': 0},
                                 self.logger)
            auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self.assertEqual(2, self.broker.get_max_row())  # new row
        self._check_broker_rows(items)
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 1})

    def test_audit_aborted_session_is_purged(self):
        # verify that an aborted session older than purge_delay is cleaned up
        session = self._create_session(MPU_SESSION_ABORTED_CONTENT_TYPE)
        items = [session]
        self.put_objects(items)
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        registered_calls = [
            ('HEAD', self.obj_path, HTTPOk, {}),
            ('PUT', self.part_marker_path, HTTPOk, {}),
            ('DELETE', self.session_path, HTTPAccepted, {})
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            auditor = MpuAuditor({'mpu_aborted_purge_delay': 0},
                                 self.logger)
            auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self.assertEqual(1, self.broker.get_max_row())  # no new rows
        self._check_broker_rows(items)
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 1})

    def test_audit_aborted_session_error_putting_part_delete_marker(self):
        # verify that an aborted session older than purge_delay is cleaned up
        session = self._create_session(MPU_SESSION_ABORTED_CONTENT_TYPE)
        items = [session]
        self.put_objects(items)
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        registered_calls = [
            ('HEAD', self.obj_path, HTTPOk, {}),
            ('PUT', self.part_marker_path, HTTPServerError, {}),
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            auditor = MpuAuditor({'mpu_aborted_purge_delay': 0},
                                 self.logger)
            auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self.assertEqual(2, self.broker.get_max_row())
        self._check_broker_rows(items)
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 1})

    def test_audit_aborted_session_error_deleting_session(self):
        # verify that an aborted session older than purge_delay is cleaned up
        session = self._create_session(MPU_SESSION_ABORTED_CONTENT_TYPE)
        items = [session]
        self.put_objects(items)
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        registered_calls = [
            ('HEAD', self.obj_path, HTTPOk, {}),
            ('PUT', self.part_marker_path, HTTPOk, {}),
            ('DELETE', self.session_path, HTTPServerError, {})
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            auditor = MpuAuditor({'mpu_aborted_purge_delay': 0},
                                 self.logger)
            auditor.audit(self.broker)

        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self.assertEqual(2, self.broker.get_max_row())
        self._check_broker_rows(items)
        self._check_deleted_broker_rows([])
        self._assert_context({'last_audit_row': 1})

    def test_audit_aborted_session_is_eventually_purged(self):
        # verify that an aborted session younger than purge_delay is deferred,
        # but is *eventually* considered old enough to purge
        now = float(time.time())
        ts_data = Timestamp(now - 3600)
        ts_ctype = Timestamp(now - 2001)
        session = self._create_session(MPU_SESSION_ABORTED_CONTENT_TYPE,
                                       ts_data=ts_data, ts_ctype=ts_ctype)
        items = [session]
        self.put_objects(items)
        self.assertEqual(1, self.broker.get_max_row())  # sanity check
        registered_calls = [
            ('HEAD', self.obj_path, HTTPOk, {}),
        ]
        exp_max_row = 1
        for audit_time_offset in [0, 1000, 8600]:
            audit_time = now + audit_time_offset
            with self._mock_internal_client(registered_calls) as fake_swift:
                with mock.patch('swift.container.mpu_auditor.time.time',
                                return_value=audit_time):
                    auditor = MpuAuditor({}, self.logger)
                    auditor.audit(self.broker)
            self._assert_context({'last_audit_row': exp_max_row})
            exp_max_row += 1
            self.assertEqual(exp_max_row, self.broker.get_max_row(),
                             'audit_time_offset=%s' % audit_time_offset)
            rows = self._check_broker_rows(items)
            # it's just the meta_timestamp that is bumped...
            self.assertEqual(
                encode_timestamps(ts_data,
                                  ts_ctype,
                                  Timestamp(audit_time)),
                rows[0]['created_at'],
                'audit_time_offset=%s' % audit_time_offset)

        # this time the session will be purged...
        audit_time = now + 86400  # purge_delay in the future
        self.assertGreater(audit_time, float(ts_ctype) + 86400)  # sanity check
        registered_calls = [
            ('HEAD', self.obj_path, HTTPOk, {}),
            ('PUT', self.part_marker_path, HTTPOk, {}),
            ('DELETE', self.session_path, HTTPAccepted, {})
        ]
        with self._mock_internal_client(registered_calls) as fake_swift:
            with mock.patch('swift.container.mpu_auditor.time.time',
                            return_value=audit_time):
                auditor = MpuAuditor({}, self.logger)
                auditor.audit(self.broker)
        expected_calls = [call[:2] for call in registered_calls]
        self.assertEqual(expected_calls, fake_swift.calls)
        self._assert_context({'last_audit_row': exp_max_row})
        self.assertEqual(exp_max_row, self.broker.get_max_row())
        # row has NOT been bumped...
        rows = self._check_broker_rows(items)
        self.assertEqual(
            encode_timestamps(ts_data,
                              ts_ctype,
                              Timestamp(now + 8600)),
            rows[0]['created_at'])


class TestMpuAuditorSLO(BaseTestMpuAuditor):
    user_container = 'c'
    audit_container = user_container + '+segments'

    def _create_upload_parts(self, num_parts):
        ts = next(self.ts_iter)
        mpu_id = self._make_mpu_id(self.obj_path, ts)
        upload = '%s/%s' % (self.obj_name, mpu_id)
        parts = [{'name': '%s/%d' % (upload, i),
                  'created_at': ts.normal,
                  'content_type': 'text/plain',
                  'etag': 'etag_1',
                  'size': 1024,
                  } for i in range(num_parts)]
        return upload, parts

    def test(self):
        upload_1, parts_1 = self._create_upload_parts(3)
        upload_2, parts_2 = self._create_upload_parts(2)
        marker = self._create_marker_spec(upload_1)
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
            auditor = MpuAuditor({}, self.logger)
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
            auditor = MpuAuditor({}, self.logger)
        auditor.audit(self.broker)

        expected_calls = [registered_calls[2][:2]]
        self.assertEqual(expected_calls, fake_swift.calls)
        rows = self.broker.get_objects(include_deleted=False)
        self.assertEqual(sorted([o['name'] for o in parts_1[2:] + parts_2]),
                         sorted([row['name'] for row in rows]))
        # 3 rows deleted, 1 row added
        self._assert_context({'last_audit_row': 9})
